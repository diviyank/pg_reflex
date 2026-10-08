//! Window function query decomposition.
//!
//! Splits a query with window functions into two parts:
//! 1. A "base query" without window functions → becomes a sub-IMV (incrementally maintained)
//! 2. A "window view" that applies window functions to the sub-IMV → becomes a VIEW
//!
//! This approach reuses all existing IVM infrastructure. The base query can be an
//! aggregate (GROUP BY) or passthrough query — both are handled by existing code.
//! The VIEW computes window functions at read time over the sub-IMV's small result set.

use crate::sql_analyzer::{folded, is_window_call, resolve_aggregate_call, SqlAnalysis};
use sqlparser::ast::{Expr, Query, Select, SelectItem, Visit, Visitor};
use std::collections::HashSet;
use std::ops::ControlFlow;

fn window_call(expr: &Expr) -> Option<&Expr> {
    let call = match expr {
        Expr::Cast { expr: inner, .. } => inner.as_ref(),
        other => other,
    };
    is_window_call(call).then_some(call)
}

fn selected_aggregate(expr: &Expr) -> Option<&sqlparser::ast::Function> {
    let call = match expr {
        Expr::Cast { expr: inner, .. } => inner.as_ref(),
        other => other,
    };
    match call {
        Expr::Function(f) if f.over.is_none() && f.filter.is_none() => {
            resolve_aggregate_call(f).map(|_| f)
        }
        _ => None,
    }
}

/// What a window expression reads that the window view cannot find in the base
/// sub-IMV, as a rejection reason.
enum UnreadableReference {
    Column(String),
    QualifiedColumn(String),
    Aggregate(String),
}

struct WindowReferences<'a> {
    base_columns: &'a HashSet<String>,
    aliased_aggregates: &'a HashSet<String>,
    enclosing_queries: usize,
    enclosing_aggregates: usize,
    unreadable: Option<UnreadableReference>,
}

impl WindowReferences<'_> {
    fn is_aggregate_of_the_base(expr: &Expr) -> bool {
        matches!(expr, Expr::Function(f) if f.over.is_none() && resolve_aggregate_call(f).is_some())
    }
}

impl Visitor for WindowReferences<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.enclosing_queries += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.enclosing_queries -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        let read_by_the_view = self.enclosing_queries == 0 && self.enclosing_aggregates == 0;
        let unreadable = match expr {
            _ if !read_by_the_view => None,
            Expr::Identifier(column) if !self.base_columns.contains(&folded(column)) => {
                Some(UnreadableReference::Column(column.to_string()))
            }
            Expr::CompoundIdentifier(_) => {
                Some(UnreadableReference::QualifiedColumn(expr.to_string()))
            }
            aggregate
                if Self::is_aggregate_of_the_base(aggregate)
                    && !self.aliased_aggregates.contains(&aggregate.to_string()) =>
            {
                Some(UnreadableReference::Aggregate(aggregate.to_string()))
            }
            _ => None,
        };
        if Self::is_aggregate_of_the_base(expr) {
            self.enclosing_aggregates += 1;
        }
        if unreadable.is_some() {
            self.unreadable = unreadable;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if Self::is_aggregate_of_the_base(expr) {
            self.enclosing_aggregates -= 1;
        }
        ControlFlow::Continue(())
    }
}

/// The window view computes each window expression over the base sub-IMV, so a window
/// expression can read only what the base projects: a column selected bare under its
/// own name, or an aggregate selected with an alias. Returns the rejection reason for
/// the first reference that is neither. A wildcard projects every column, so such a
/// query is not checked.
pub fn unreadable_window_reference(select: &Select) -> Option<String> {
    let mut items: Vec<(&Expr, Option<String>)> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => items.push((expr, None)),
            SelectItem::ExprWithAlias { expr, alias } => items.push((expr, Some(folded(alias)))),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
        }
    }
    let base_items = || items.iter().filter(|(expr, _)| window_call(expr).is_none());
    let base_columns: HashSet<String> = base_items()
        .filter_map(|(expr, alias)| {
            let column = match expr {
                Expr::Identifier(column) => folded(column),
                Expr::CompoundIdentifier(parts) => folded(parts.last()?),
                _ => return None,
            };
            alias
                .as_ref()
                .is_none_or(|alias| *alias == column)
                .then_some(column)
        })
        .collect();
    let aliased_aggregates: HashSet<String> = base_items()
        .filter(|(_, alias)| alias.is_some())
        .filter_map(|(expr, _)| selected_aggregate(expr).map(|f| f.to_string()))
        .collect();

    let mut references = WindowReferences {
        base_columns: &base_columns,
        aliased_aggregates: &aliased_aggregates,
        enclosing_queries: 0,
        enclosing_aggregates: 0,
        unreadable: None,
    };
    for (expr, _) in items.iter().filter(|(expr, _)| window_call(expr).is_some()) {
        let _ = expr.visit(&mut references);
    }
    let cte_remedy = "if it must be renamed or qualified, compute it in a CTE and apply the \
                      window function in the outer SELECT over that CTE";
    references.unreadable.map(|unreadable| match unreadable {
        UnreadableReference::Column(column) => format!(
            "A window function reads the column '{column}', which the SELECT list does not \
             project bare under its own name: the window is computed over the IMV's other \
             output columns. Fix: add {column} to the SELECT list; {cte_remedy}."
        ),
        UnreadableReference::QualifiedColumn(column) => format!(
            "A window function reads '{column}' with a qualifier, which the IMV's output \
             columns do not carry: the window is computed over them. Fix: write the bare name \
             of a column of the SELECT list; {cte_remedy}."
        ),
        UnreadableReference::Aggregate(aggregate) => format!(
            "A window function reads the aggregate {aggregate}, which the SELECT list does not \
             hold under an alias: the window is computed over the IMV's other output columns. \
             Fix: select it with an alias beside the window function ({aggregate} AS total)."
        ),
    })
}

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
