use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::sql_analyzer::{detect_aggregate, AggregateKind, SqlAnalysis};

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
    /// Source columns known to be NOT NULL at IMV creation time.
    /// Used to skip companion __nonnull_count columns for SUM aggregates.
    #[serde(default)]
    pub not_null_columns: std::collections::HashSet<String>,
    /// Mapping from GROUP BY expression to user-facing alias for output columns.
    /// E.g., "COALESCE(t1.grp, t2.grp)" -> "grp" when the SELECT has `... AS grp`.
    /// Only populated when the alias differs from the normalized expression name.
    #[serde(default)]
    pub group_by_aliases: std::collections::HashMap<String, String>,
    /// Output column order matching the user's original SELECT.
    /// Each entry is either "gb:col_expr" (GROUP BY column) or "agg:alias" (aggregate/derived).
    /// Used to generate target DDL and end_query with columns in the user's expected order.
    #[serde(default)]
    pub output_column_order: Vec<String>,
}

impl AggregationPlan {
    /// Remove __nonnull_count_* companion columns for SUM aggregates where the source
    /// column is NOT NULL. When a column can't be NULL, SUM can never produce NULL
    /// (empty group case is handled by __ivm_count), so the companion count is redundant.
    pub fn optimize_not_null_sums(&mut self, not_null_columns: &std::collections::HashSet<String>) {
        let to_remove: std::collections::HashSet<String> = self
            .intermediate_columns
            .iter()
            .filter(|ic| ic.source_aggregate == "SUM" && not_null_columns.contains(&ic.source_arg))
            .map(|ic| {
                let arg_sanitized = ic
                    .source_arg
                    .chars()
                    .map(|c| {
                        if c.is_alphanumeric() || c == '_' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect::<String>()
                    .to_lowercase();
                format!("__nonnull_count_{}", arg_sanitized)
            })
            .collect();

        if to_remove.is_empty() {
            return;
        }

        // Remove companion count columns from intermediate
        self.intermediate_columns
            .retain(|ic| !to_remove.contains(&ic.name));

        // Update end_query_mappings: replace CASE WHEN __nonnull_count > 0 THEN __sum END
        // with just the __sum reference (the WHERE __ivm_count > 0 filter or sentinel
        // CASE WHEN wrapper in generate_end_query handles the empty-group case).
        for mapping in &mut self.end_query_mappings {
            if mapping.aggregate_type == "SUM" {
                for count_name in &to_remove {
                    let old_prefix = format!("CASE WHEN \"{}\" > 0 THEN ", count_name);
                    if mapping.intermediate_expr.starts_with(&old_prefix) {
                        if let Some(sum_ref) = mapping
                            .intermediate_expr
                            .strip_prefix(&old_prefix)
                            .and_then(|r| r.strip_suffix(" END"))
                        {
                            mapping.intermediate_expr = sum_ref.to_string();
                        }
                    }
                }
            }
        }

        self.not_null_columns = not_null_columns.clone();
    }
}

/// Sanitize a SQL expression to be used as part of a column name.
/// Strips quotes, replaces non-identifier chars with underscores, collapses runs,
/// and truncates with a hash suffix if too long for PostgreSQL's 63-char limit.
pub fn sanitize_for_col_name(s: &str) -> String {
    // Strip quotes but keep dots/table qualifiers for uniqueness
    let stripped = s.replace('"', "");

    let sanitized: String = stripped
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .to_lowercase();

    // Collapse multiple underscores and trim
    let mut collapsed = String::with_capacity(sanitized.len());
    let mut prev_underscore = false;
    for c in sanitized.chars() {
        if c == '_' {
            if !prev_underscore {
                collapsed.push(c);
            }
            prev_underscore = true;
        } else {
            collapsed.push(c);
            prev_underscore = false;
        }
    }
    let result = collapsed.trim_matches('_').to_string();

    // Truncate to avoid PostgreSQL's 63-char identifier limit.
    // Leave room for prefixes like "__nonnull_count_" (max 18 chars).
    if result.len() > 44 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        result.hash(&mut hasher);
        let hash = hasher.finish();
        format!("{}_{:x}", &result[..36], hash)
    } else {
        result
    }
}

// detect_aggregate is imported from sql_analyzer

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

/// Rewrite an aggregate-derived expression: extract constituent aggregates as
/// intermediate columns and replace them with intermediate column references.
///
/// Returns (rewritten_expression, new_intermediate_columns).
fn rewrite_aggregate_derived_expr(
    expr_sql: &str,
    existing_intermediates: &[IntermediateColumn],
) -> (String, Vec<IntermediateColumn>) {
    let mut new_intermediates = Vec::new();

    // Parse the expression
    let parsed = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(expr_sql)
        .and_then(|mut p| p.parse_expr());
    let Ok(expr) = parsed else {
        return (expr_sql.to_string(), new_intermediates);
    };

    let rewritten = rewrite_expr_aggregates(&expr, existing_intermediates, &mut new_intermediates);
    (rewritten, new_intermediates)
}

/// Recursively rewrite an expression, replacing aggregate function calls with
/// intermediate column references (e.g., SUM(x) -> "__sum_x").
fn rewrite_expr_aggregates(
    expr: &Expr,
    existing: &[IntermediateColumn],
    new_cols: &mut Vec<IntermediateColumn>,
) -> String {
    match expr {
        Expr::Function(f) if f.over.is_none() => {
            let func_name = f.name.to_string();
            if let Some(kind) = detect_aggregate(&func_name) {
                let arg = first_arg_string(f);
                let arg_sanitized = sanitize_for_col_name(&arg);
                let col_name = match kind {
                    AggregateKind::Sum => format!("__sum_{}", arg_sanitized),
                    AggregateKind::Count | AggregateKind::CountStar => {
                        format!("__count_{}", arg_sanitized)
                    }
                    AggregateKind::CountDistinct => {
                        format!("__count_distinct_{}", arg_sanitized)
                    }
                    AggregateKind::Min => format!("__min_{}", arg_sanitized),
                    AggregateKind::Max => format!("__max_{}", arg_sanitized),
                    AggregateKind::BoolOr => format!("__bool_or_{}", arg_sanitized),
                    AggregateKind::Avg => format!("__sum_{}", arg_sanitized),
                };
                // Add intermediate column if not already present
                let all_names: Vec<&str> = existing
                    .iter()
                    .chain(new_cols.iter())
                    .map(|ic| ic.name.as_str())
                    .collect();
                if !all_names.contains(&col_name.as_str()) {
                    let (source_agg, source_arg) = match kind {
                        AggregateKind::Avg => ("SUM".to_string(), arg.clone()),
                        AggregateKind::CountStar => ("COUNT".to_string(), "*".to_string()),
                        _ => (func_name.to_uppercase(), arg.clone()),
                    };
                    new_cols.push(IntermediateColumn {
                        name: col_name.clone(),
                        pg_type: match kind {
                            AggregateKind::Count
                            | AggregateKind::CountStar
                            | AggregateKind::CountDistinct => "BIGINT".to_string(),
                            AggregateKind::BoolOr => "BOOLEAN".to_string(),
                            _ => "NUMERIC".to_string(),
                        },
                        source_aggregate: source_agg,
                        source_arg,
                    });
                }
                return format!("\"{}\"", col_name);
            }
            // Not an aggregate function — recursively rewrite arguments
            // (handles COALESCE(SUM(x), 0), GREATEST(SUM(a), SUM(b)), etc.)
            let mut rewritten_args = Vec::new();
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                            rewritten_args.push(rewrite_expr_aggregates(e, existing, new_cols));
                        }
                        other => rewritten_args.push(other.to_string()),
                    }
                }
            }
            format!("{}({})", f.name, rewritten_args.join(", "))
        }
        Expr::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                rewrite_expr_aggregates(left, existing, new_cols),
                op,
                rewrite_expr_aggregates(right, existing, new_cols)
            )
        }
        Expr::UnaryOp { op, expr: inner } => {
            format!(
                "{} {}",
                op,
                rewrite_expr_aggregates(inner, existing, new_cols)
            )
        }
        Expr::Nested(inner) => {
            format!("({})", rewrite_expr_aggregates(inner, existing, new_cols))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut s = "CASE".to_string();
            if let Some(op) = operand {
                s.push_str(&format!(
                    " {}",
                    rewrite_expr_aggregates(op, existing, new_cols)
                ));
            }
            for case_when in conditions {
                s.push_str(&format!(
                    " WHEN {} THEN {}",
                    rewrite_expr_aggregates(&case_when.condition, existing, new_cols),
                    rewrite_expr_aggregates(&case_when.result, existing, new_cols)
                ));
            }
            if let Some(el) = else_result {
                s.push_str(&format!(
                    " ELSE {}",
                    rewrite_expr_aggregates(el, existing, new_cols)
                ));
            }
            s.push_str(" END");
            s
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            format!(
                "{}::{}",
                rewrite_expr_aggregates(inner, existing, new_cols),
                data_type
            )
        }
        other => other.to_string(),
    }
}

/// Extract the first argument of a function as a string.
fn first_arg_string(f: &Function) -> String {
    if let FunctionArguments::List(list) = &f.args {
        if let Some(arg) = list.args.first() {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => return "*".to_string(),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => return e.to_string(),
                FunctionArg::Unnamed(expr) => return expr.to_string(),
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                    return arg.to_string()
                }
            }
        }
    }
    "*".to_string()
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

        // Handle aggregate-derived expressions (e.g., CASE WHEN SUM(x) > 0 THEN ...)
        if col.is_aggregate_derived {
            let output_alias = col.alias.clone().unwrap_or_else(|| col.expr_sql.clone());
            let (rewritten, new_intermediates) =
                rewrite_aggregate_derived_expr(&col.expr_sql, &intermediate_columns);
            intermediate_columns.extend(new_intermediates);
            end_query_mappings.push(EndQueryMapping {
                intermediate_expr: rewritten,
                output_alias,
                aggregate_type: "DERIVED".to_string(),
                cast_type: col.cast_type.clone(),
            });
            continue;
        }

        let Some(ref agg) = col.aggregate else {
            continue;
        };

        let arg = col.aggregate_arg.as_deref().unwrap_or("*");
        let arg_sanitized = sanitize_for_col_name(arg);

        // Determine the user-facing alias for the output
        let output_alias = col.alias.clone().unwrap_or_else(|| col.expr_sql.clone());

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
                    intermediate_expr: format!(
                        "CASE WHEN \"{}\" > 0 THEN \"{}\" END",
                        count_col, sum_col
                    ),
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
                    intermediate_expr: format!("{} / NULLIF({}, 0)", sum_col, count_col),
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
                // The end_query counts non-NULL distinct values per original GROUP BY
                // using COUNT(val). COUNT(val) (not COUNT(*)) matches Postgres
                // semantics — COUNT(DISTINCT val) ignores NULLs.
                count_distinct_columns.push(arg.to_string());
                let arg_norm = crate::query_decomposer::normalized_column_name(arg);
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: format!("COUNT(\"{}\")", arg_norm),
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

    // Passthrough SELECT columns that aren't in GROUP BY (e.g., EXTRACT(WEEK FROM col)
    // when col is in GROUP BY) are valid in PostgreSQL due to functional dependency.
    // Add them to group_by_columns so they become intermediate table keys.
    // Only applies when there IS a GROUP BY clause (not for DISTINCT-only queries).
    let mut group_by_columns = analysis.group_by_columns.clone();
    if !is_passthrough && !analysis.group_by_columns.is_empty() {
        let gb_norms: Vec<String> = group_by_columns
            .iter()
            .map(|gb| crate::query_decomposer::normalized_column_name(gb))
            .collect();
        let extra_cols: Vec<String> = analysis
            .select_columns
            .iter()
            .filter(|col| {
                if !col.is_passthrough {
                    return false;
                }
                let norm = crate::query_decomposer::normalized_column_name(&col.expr_sql);
                let alias_norm = col
                    .alias
                    .as_deref()
                    .map(crate::query_decomposer::normalized_column_name);
                // Skip if normalized expression or alias already matches a GROUP BY column
                !gb_norms
                    .iter()
                    .any(|gn| *gn == norm || alias_norm.as_deref().is_some_and(|an| gn == an))
            })
            .map(|col| col.expr_sql.clone())
            .collect();
        group_by_columns.extend(extra_cols);
    }

    // Build GROUP BY aliases: map expression -> user alias when they differ
    let mut group_by_aliases = std::collections::HashMap::new();
    for gb in &group_by_columns {
        if let Some(sc) = analysis.select_columns.iter().find(|sc| {
            if !sc.is_passthrough {
                return false;
            }
            // Exact match first, then normalized (handles table.col vs col)
            sc.expr_sql == *gb
                || crate::query_decomposer::normalized_column_name(&sc.expr_sql)
                    == crate::query_decomposer::normalized_column_name(gb)
        }) {
            if let Some(ref alias) = sc.alias {
                let norm_gb = crate::query_decomposer::normalized_column_name(gb);
                let norm_alias = crate::query_decomposer::normalized_column_name(alias);
                if norm_gb != norm_alias {
                    group_by_aliases.insert(gb.clone(), alias.clone());
                }
            }
        }
    }

    // Build output_column_order from the user's SELECT to preserve column ordering.
    // Each entry is "gb:<expr>" for GROUP BY columns or "agg:<alias>" for aggregates/derived.
    let output_column_order: Vec<String> = analysis
        .select_columns
        .iter()
        .filter_map(|col| {
            if col.is_window {
                Some(format!(
                    "agg:{}",
                    col.alias.as_deref().unwrap_or(&col.expr_sql)
                ))
            } else if col.is_passthrough {
                // Resolve to the matching GROUP BY expression so that keys are consistent
                // with group_by_aliases (e.g. SELECT table.col matches GROUP BY col).
                let norm = crate::query_decomposer::normalized_column_name(&col.expr_sql);
                let gb_key = group_by_columns
                    .iter()
                    .find(|g| crate::query_decomposer::normalized_column_name(g) == norm)
                    .cloned()
                    .unwrap_or_else(|| col.expr_sql.clone());
                Some(format!("gb:{}", gb_key))
            } else if col.aggregate.is_some() || col.is_aggregate_derived {
                Some(format!(
                    "agg:{}",
                    col.alias.as_deref().unwrap_or(&col.expr_sql)
                ))
            } else {
                None
            }
        })
        .collect();

    AggregationPlan {
        group_by_columns,
        intermediate_columns,
        end_query_mappings,
        has_distinct: analysis.has_distinct,
        needs_ivm_count,
        distinct_columns,
        is_passthrough,
        passthrough_columns,
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: analysis.having_clause.clone(),
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases,
        output_column_order,
    }
}

#[cfg(test)]
#[path = "tests/unit_aggregation.rs"]
mod tests;
