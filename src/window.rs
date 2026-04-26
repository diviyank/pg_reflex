//! Window function query decomposition.
//!
//! Splits a query with window functions into two parts:
//! 1. A "base query" without window functions → becomes a sub-IMV (incrementally maintained)
//! 2. A "window view" that applies window functions to the sub-IMV → becomes a VIEW
//!
//! This approach reuses all existing IVM infrastructure. The base query can be an
//! aggregate (GROUP BY) or passthrough query — both are handled by existing code.
//! The VIEW computes window functions at read time over the sub-IMV's small result set.

use crate::sql_analyzer::SqlAnalysis;

/// Result of decomposing a windowed query.
pub struct WindowDecomposition {
    /// SQL for the base sub-IMV (everything except window functions)
    pub base_query: String,
    /// SELECT items for the window VIEW (references base columns + window expressions)
    pub view_select: String,
}

/// Decompose a query with window functions into a base query and window view components.
///
/// Given:
/// ```sql
/// SELECT city, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
/// FROM orders GROUP BY city
/// ```
///
/// Produces:
/// - base_query: `SELECT city, SUM(amount) AS total FROM orders GROUP BY city`
/// - view_select: `city, total, RANK() OVER (ORDER BY total DESC) AS rnk`
///
/// The window expressions have aggregate references (e.g., `SUM(amount)`) replaced
/// with their aliases (e.g., `total`) since the VIEW reads from the sub-IMV.
pub fn decompose_window_query(analysis: &SqlAnalysis) -> WindowDecomposition {
    // Separate SELECT items into base (non-window) and window items
    let mut base_items: Vec<String> = Vec::new();
    let mut window_items: Vec<String> = Vec::new();
    // Build a mapping: aggregate_expr → alias (for rewriting OVER clauses)
    let mut agg_to_alias: Vec<(String, String)> = Vec::new();

    for col in &analysis.select_columns {
        if col.is_window {
            window_items.push(format_select_item(col));
        } else {
            base_items.push(format_select_item(col));
            // Track aggregate-to-alias mapping for OVER clause rewriting
            if col.aggregate.is_some() {
                if let Some(ref alias) = col.alias {
                    agg_to_alias.push((col.expr_sql.clone(), alias.clone()));
                }
            }
        }
    }

    // Build the base query: non-window SELECT items + FROM + WHERE + GROUP BY + HAVING
    let base_select = base_items.join(", ");
    let mut base_query = format!("SELECT {} FROM {}", base_select, analysis.from_clause_sql);
    if let Some(ref wc) = analysis.where_clause {
        base_query.push_str(&format!(" WHERE {}", wc));
    }
    if !analysis.group_by_columns.is_empty() {
        base_query.push_str(&format!(
            " GROUP BY {}",
            analysis.group_by_columns.join(", ")
        ));
    }
    if let Some(ref hc) = analysis.having_clause {
        base_query.push_str(&format!(" HAVING {}", hc));
    }

    // Build the VIEW select: base column names + rewritten window expressions
    let mut view_items: Vec<String> = Vec::new();

    // Add base column references (just the output names, not the expressions)
    for col in &analysis.select_columns {
        if col.is_window {
            continue;
        }
        let col_ref = if let Some(ref alias) = col.alias {
            format!("\"{}\"", alias)
        } else {
            // For simple column references, use the expression directly
            col.expr_sql.clone()
        };
        view_items.push(col_ref);
    }

    // Add window expressions with aggregate references rewritten to aliases
    for col in &analysis.select_columns {
        if !col.is_window {
            continue;
        }
        let mut expr = col.expr_sql.clone();
        // Replace aggregate expressions with their aliases in the window expression.
        // Sort by length descending to avoid partial matches (e.g., "SUM(amount)" before "amount").
        let mut sorted_mappings = agg_to_alias.clone();
        sorted_mappings.sort_by_key(|m| std::cmp::Reverse(m.0.len()));
        for (agg_expr, alias) in &sorted_mappings {
            expr = expr.replace(agg_expr, alias);
        }
        if let Some(ref alias) = col.alias {
            view_items.push(format!("{} AS \"{}\"", expr, alias));
        } else {
            view_items.push(expr);
        }
    }

    WindowDecomposition {
        base_query,
        view_select: view_items.join(", "),
    }
}

/// Format a SelectColumn as a SQL SELECT item (expr + optional alias).
fn format_select_item(col: &crate::sql_analyzer::SelectColumn) -> String {
    if let Some(ref alias) = col.alias {
        format!("{} AS {}", col.expr_sql, alias)
    } else {
        col.expr_sql.clone()
    }
}
