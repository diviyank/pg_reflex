use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::sql_analyzer::{AggregateKind, SqlAnalysis};

/// A column in the intermediate (unlogged) table storing partial aggregate state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntermediateColumn {
    /// Column name in intermediate table (e.g., "__sum_salary")
    pub name: String,
    /// PostgreSQL type (e.g., "NUMERIC", "BIGINT")
    pub pg_type: String,
    /// Aggregate function to use in the base_query (e.g., "SUM")
    pub source_aggregate: String,
    /// Argument expression from the original query (e.g., "salary")
    pub source_arg: String,
}

/// Mapping from intermediate columns to the final output column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndQueryMapping {
    /// SQL expression reading from the intermediate table (e.g., "__sum_salary / NULLIF(__count_salary, 0)")
    pub intermediate_expr: String,
    /// The user-facing output alias
    pub output_alias: String,
    /// The original aggregate type (e.g., "AVG")
    pub aggregate_type: String,
    /// Optional cast to apply in the end query (e.g., "BIGINT" from SUM(x)::BIGINT)
    #[serde(default)]
    pub cast_type: Option<String>,
}

/// Complete plan for how to decompose a query into intermediate + final stages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationPlan {
    pub group_by_columns: Vec<String>,
    pub intermediate_columns: Vec<IntermediateColumn>,
    pub end_query_mappings: Vec<EndQueryMapping>,
    pub has_distinct: bool,
    pub needs_ivm_count: bool,
    /// For DISTINCT without GROUP BY: the projected columns used as group keys.
    pub distinct_columns: Vec<String>,
    /// True when query has no GROUP BY, no aggregates, no DISTINCT.
    /// Passthrough IMVs skip the intermediate table and modify the target directly.
    pub is_passthrough: bool,
    /// Column names in the passthrough SELECT list (used for incremental DELETE/UPDATE matching).
    #[serde(default)]
    pub passthrough_columns: Vec<String>,
    /// Per-source-table column mappings for passthrough DELETE/UPDATE.
    /// Key: source table name. Value: vec of (target_col, source_col) pairs.
    /// For the key-owner table, target_col == source_col.
    /// For secondary (joined) tables, derived from JOIN conditions.
    #[serde(default)]
    pub passthrough_key_mappings: std::collections::HashMap<String, Vec<(String, String)>>,
    /// Rewritten HAVING clause (aggregate refs replaced with intermediate column names).
    #[serde(default)]
    pub having_clause: Option<String>,
}

/// Sanitize a SQL expression to be used as part of a column name.
/// Replaces dots, parens, spaces with underscores.
fn sanitize_for_col_name(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect::<String>()
        .to_lowercase()
}

/// Detect aggregate kind from a function name (mirrors sql_analyzer::detect_aggregate).
fn detect_aggregate(func_name: &str) -> Option<AggregateKind> {
    match func_name.to_uppercase().as_str() {
        "SUM" => Some(AggregateKind::Sum),
        "COUNT" => Some(AggregateKind::Count),
        "AVG" => Some(AggregateKind::Avg),
        "MIN" => Some(AggregateKind::Min),
        "MAX" => Some(AggregateKind::Max),
        "BOOL_OR" => Some(AggregateKind::BoolOr),
        _ => None,
    }
}

/// Recursively collect (aggregate_kind, arg_string) pairs from a HAVING expression.
fn collect_having_aggregates(expr: &Expr, out: &mut Vec<(AggregateKind, String)>) {
    match expr {
        Expr::Function(f) => {
            let func_name = f.name.to_string();
            if let Some(kind) = detect_aggregate(&func_name) {
                // Check for COUNT(*)
                if let FunctionArguments::List(list) = &f.args {
                    if list.args.len() == 1
                        && matches!(
                            &list.args[0],
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                        )
                    {
                        out.push((AggregateKind::CountStar, "*".to_string()));
                    } else if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) =
                        list.args.first()
                    {
                        out.push((kind, arg_expr.to_string()));
                    }
                }
            }
            // Also recurse into function args (for nested expressions)
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        collect_having_aggregates(e, out);
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_having_aggregates(left, out);
            collect_having_aggregates(right, out);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            collect_having_aggregates(inner, out);
        }
        Expr::Nested(inner) => {
            collect_having_aggregates(inner, out);
        }
        _ => {}
    }
}

/// Build an AggregationPlan from a SqlAnalysis.
pub fn plan_aggregation(analysis: &SqlAnalysis) -> AggregationPlan {
    let mut intermediate_columns = Vec::new();
    let mut end_query_mappings = Vec::new();
    let mut count_distinct_columns: Vec<String> = Vec::new();

    // (Mixed COUNT(DISTINCT) + other aggregates validation is done in lib.rs)

    for col in &analysis.select_columns {
        if col.is_passthrough {
            // Passthrough columns (GROUP BY cols) are handled separately as group keys
            continue;
        }

        let Some(ref agg) = col.aggregate else {
            continue;
        };

        let arg = col
            .aggregate_arg
            .as_deref()
            .unwrap_or("*");
        let arg_sanitized = sanitize_for_col_name(arg);

        // Determine the user-facing alias for the output
        let output_alias = col
            .alias
            .clone()
            .unwrap_or_else(|| col.expr_sql.clone());

        let cast_type = col.cast_type.clone();

        match agg {
            AggregateKind::Sum => {
                let sum_col = format!("__sum_{}", arg_sanitized);
                let count_col = format!("__nonnull_count_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: sum_col.clone(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "SUM".to_string(),
                    source_arg: arg.to_string(),
                });
                // Companion COUNT(col) tracks non-NULL contributors.
                // When this drops to 0, SUM should be NULL (not 0).
                // Only add if not already present from another aggregate.
                if !intermediate_columns.iter().any(|ic| ic.name == count_col) {
                    intermediate_columns.push(IntermediateColumn {
                        name: count_col.clone(),
                        pg_type: "BIGINT".to_string(),
                        source_aggregate: "COUNT".to_string(),
                        source_arg: arg.to_string(),
                    });
                }
                // End query: CASE WHEN non-null count > 0 THEN sum END (returns NULL when all values are NULL)
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: format!("CASE WHEN \"{}\" > 0 THEN \"{}\" END", count_col, sum_col),
                    output_alias,
                    aggregate_type: "SUM".to_string(),
                    cast_type,
                });
            }
            AggregateKind::Count => {
                let col_name = format!("__count_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: col_name.clone(),
                    pg_type: "BIGINT".to_string(),
                    source_aggregate: "COUNT".to_string(),
                    source_arg: arg.to_string(),
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: col_name,
                    output_alias,
                    aggregate_type: "COUNT".to_string(),
                    cast_type,
                });
            }
            AggregateKind::CountStar => {
                let col_name = "__count_star".to_string();
                intermediate_columns.push(IntermediateColumn {
                    name: col_name.clone(),
                    pg_type: "BIGINT".to_string(),
                    source_aggregate: "COUNT".to_string(),
                    source_arg: "*".to_string(),
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: col_name,
                    output_alias,
                    aggregate_type: "COUNT".to_string(),
                    cast_type,
                });
            }
            AggregateKind::Avg => {
                // AVG decomposes to SUM + COUNT
                let sum_col = format!("__sum_{}", arg_sanitized);
                let count_col = format!("__count_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: sum_col.clone(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "SUM".to_string(),
                    source_arg: arg.to_string(),
                });
                intermediate_columns.push(IntermediateColumn {
                    name: count_col.clone(),
                    pg_type: "BIGINT".to_string(),
                    source_aggregate: "COUNT".to_string(),
                    source_arg: arg.to_string(),
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: format!(
                        "{} / NULLIF({}, 0)",
                        sum_col, count_col
                    ),
                    output_alias,
                    aggregate_type: "AVG".to_string(),
                    cast_type,
                });
            }
            AggregateKind::Min => {
                let col_name = format!("__min_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: col_name.clone(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "MIN".to_string(),
                    source_arg: arg.to_string(),
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: col_name,
                    output_alias,
                    aggregate_type: "MIN".to_string(),
                    cast_type,
                });
            }
            AggregateKind::Max => {
                let col_name = format!("__max_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: col_name.clone(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "MAX".to_string(),
                    source_arg: arg.to_string(),
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: col_name,
                    output_alias,
                    aggregate_type: "MAX".to_string(),
                    cast_type,
                });
            }
            AggregateKind::BoolOr => {
                let col_name = format!("__bool_or_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: col_name.clone(),
                    pg_type: "BOOLEAN".to_string(),
                    source_aggregate: "BOOL_OR".to_string(),
                    source_arg: arg.to_string(),
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: col_name,
                    output_alias,
                    aggregate_type: "BOOL_OR".to_string(),
                    cast_type,
                });
            }
            AggregateKind::CountDistinct => {
                // COUNT(DISTINCT val): the intermediate uses (grp, val) as compound key.
                // The end_query does COUNT(*) per original GROUP BY.
                // We add `val` to distinct_columns (which extends the intermediate key).
                // No extra intermediate aggregate column needed — just __ivm_count.
                count_distinct_columns.push(arg.to_string());
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: "COUNT(*)".to_string(),
                    output_alias,
                    aggregate_type: "COUNT".to_string(),
                    cast_type,
                });
            }
        }
    }

    // Auto-add intermediate columns for aggregates referenced in HAVING but not in SELECT
    if let Some(ref having_str) = analysis.having_clause {
        let parse_result = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(having_str)
            .and_then(|mut p| p.parse_expr());
        if let Ok(having_expr) = parse_result {
            let mut having_aggs = Vec::new();
            collect_having_aggregates(&having_expr, &mut having_aggs);
            for (kind, arg) in having_aggs {
                let arg_sanitized = sanitize_for_col_name(&arg);
                match kind {
                    AggregateKind::Sum => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__sum_{}", arg_sanitized),
                            pg_type: "NUMERIC".to_string(),
                            source_aggregate: "SUM".to_string(),
                            source_arg: arg,
                        });
                    }
                    AggregateKind::Count => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__count_{}", arg_sanitized),
                            pg_type: "BIGINT".to_string(),
                            source_aggregate: "COUNT".to_string(),
                            source_arg: arg,
                        });
                    }
                    AggregateKind::CountStar => {
                        intermediate_columns.push(IntermediateColumn {
                            name: "__count_star".to_string(),
                            pg_type: "BIGINT".to_string(),
                            source_aggregate: "COUNT".to_string(),
                            source_arg: "*".to_string(),
                        });
                    }
                    AggregateKind::Avg => {
                        // AVG needs both SUM and COUNT
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__sum_{}", arg_sanitized),
                            pg_type: "NUMERIC".to_string(),
                            source_aggregate: "SUM".to_string(),
                            source_arg: arg.clone(),
                        });
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__count_{}", arg_sanitized),
                            pg_type: "BIGINT".to_string(),
                            source_aggregate: "COUNT".to_string(),
                            source_arg: arg,
                        });
                    }
                    AggregateKind::Min => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__min_{}", arg_sanitized),
                            pg_type: "NUMERIC".to_string(),
                            source_aggregate: "MIN".to_string(),
                            source_arg: arg,
                        });
                    }
                    AggregateKind::Max => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__max_{}", arg_sanitized),
                            pg_type: "NUMERIC".to_string(),
                            source_aggregate: "MAX".to_string(),
                            source_arg: arg,
                        });
                    }
                    AggregateKind::BoolOr => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__bool_or_{}", arg_sanitized),
                            pg_type: "BOOLEAN".to_string(),
                            source_aggregate: "BOOL_OR".to_string(),
                            source_arg: arg,
                        });
                    }
                    AggregateKind::CountDistinct => {
                        // COUNT(DISTINCT) in HAVING is not supported yet
                    }
                }
            }
        }
    }

    // Deduplicate intermediate columns by name (e.g., SUM(x) and AVG(x) both need __sum_x)
    let mut seen_names = std::collections::HashSet::new();
    intermediate_columns.retain(|col| seen_names.insert(col.name.clone()));

    let is_passthrough = analysis.group_by_columns.is_empty()
        && intermediate_columns.is_empty()
        && !analysis.has_distinct;

    // __ivm_count for reference counting (not needed for passthrough)
    let needs_ivm_count = !is_passthrough;

    // For DISTINCT without GROUP BY, the passthrough columns become distinct columns.
    // For COUNT(DISTINCT val), the distinct column extends the intermediate key.
    let mut distinct_columns = if analysis.has_distinct && analysis.group_by_columns.is_empty() {
        analysis
            .select_columns
            .iter()
            .filter(|c| c.is_passthrough)
            .map(|c| c.expr_sql.clone())
            .collect()
    } else {
        Vec::new()
    };
    distinct_columns.extend(count_distinct_columns);

    // For passthrough queries, collect column names for incremental DELETE/UPDATE
    let passthrough_columns = if is_passthrough {
        analysis
            .select_columns
            .iter()
            .map(|c| {
                let name = c.alias.as_deref().unwrap_or(&c.expr_sql);
                crate::query_decomposer::bare_column_name(name).to_string()
            })
            .collect()
    } else {
        Vec::new()
    };

    AggregationPlan {
        group_by_columns: analysis.group_by_columns.clone(),
        intermediate_columns,
        end_query_mappings,
        has_distinct: analysis.has_distinct,
        needs_ivm_count,
        distinct_columns,
        is_passthrough,
        passthrough_columns,
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: analysis.having_clause.clone(),
    }
}

#[cfg(test)]
#[path = "tests/unit_aggregation.rs"]
mod tests;
