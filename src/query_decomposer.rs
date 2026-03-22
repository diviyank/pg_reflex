use crate::aggregation::{AggregationPlan, IntermediateColumn};
use crate::sql_analyzer::SqlAnalysis;
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Split a potentially schema-qualified name into (Option<schema>, name).
/// "my_view" -> (None, "my_view")
/// "myschema.my_view" -> (Some("myschema"), "my_view")
pub fn split_qualified_name(name: &str) -> (Option<&str>, &str) {
    match name.find('.') {
        Some(pos) => (Some(&name[..pos]), &name[pos + 1..]),
        None => (None, name),
    }
}

/// Quote a potentially schema-qualified name for use in SQL DDL/DML.
/// "my_view" -> "\"my_view\""
/// "myschema.my_view" -> "\"myschema\".\"my_view\""
pub fn quote_identifier(name: &str) -> String {
    let (schema, tbl) = split_qualified_name(name);
    match schema {
        Some(s) => format!("\"{}\".\"{}\"", s, tbl),
        None => format!("\"{}\"", tbl),
    }
}

/// Name of the intermediate (unlogged) table for a given view.
/// For schema-qualified names, the intermediate table is in the same schema.
pub fn intermediate_table_name(view_name: &str) -> String {
    let (schema, name) = split_qualified_name(view_name);
    let int_name = format!("__reflex_intermediate_{}", name);
    match schema {
        Some(s) => format!("\"{}\".\"{}\"", s, int_name),
        None => int_name,
    }
}

/// Strip table alias/qualifier from a column expression.
/// E.g., "d.dept_name" -> "dept_name", "city" -> "city"
pub fn bare_column_name(col: &str) -> &str {
    col.rsplit('.').next().unwrap_or(col)
}


/// Replace a SQL identifier with another, respecting word boundaries.
/// Only replaces when the match is NOT part of a longer identifier
/// (i.e., the character before/after is not alphanumeric or `_`).
pub fn replace_identifier(sql: &str, old_name: &str, new_name: &str) -> String {
    if old_name.is_empty() {
        return sql.to_string();
    }
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let old_bytes = old_name.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + old_bytes.len() <= bytes.len() && &bytes[i..i + old_bytes.len()] == old_bytes {
            let before_ok =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            let after_pos = i + old_bytes.len();
            let after_ok = after_pos >= bytes.len()
                || !(bytes[after_pos].is_ascii_alphanumeric() || bytes[after_pos] == b'_');
            if before_ok && after_ok {
                result.push_str(new_name);
                i += old_bytes.len();
                continue;
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}



/// Generate the base query: source data -> intermediate table.
///
/// This query is stored in the reference table and at trigger time the source table
/// names are replaced with transition table names (delta).
///
/// The base_query keeps the original FROM clause (with JOINs, aliases, etc.)
/// and uses the original GROUP BY expressions. But it aliases the group-by columns
/// to their bare names so the intermediate table can use them.
pub fn generate_base_query(analysis: &SqlAnalysis, plan: &AggregationPlan) -> String {
    let mut select_parts: Vec<String> = Vec::new();

    // For aggregates without GROUP BY: add sentinel column for intermediate PK
    let needs_sentinel = plan.group_by_columns.is_empty()
        && plan.distinct_columns.is_empty()
        && !plan.intermediate_columns.is_empty();
    if needs_sentinel {
        select_parts.push("0 AS __reflex_group".to_string());
    }

    // Group by columns: in the base query we keep the original expression
    // but alias to the bare column name for the intermediate table
    for col in &plan.group_by_columns {
        let bare = bare_column_name(col);
        if bare != col {
            select_parts.push(format!("{} AS \"{}\"", col, bare));
        } else {
            select_parts.push(col.clone());
        }
    }

    // DISTINCT columns (for DISTINCT without GROUP BY)
    for col in &plan.distinct_columns {
        let bare = bare_column_name(col);
        if bare != col {
            select_parts.push(format!("{} AS \"{}\"", col, bare));
        } else {
            select_parts.push(col.clone());
        }
    }

    // Intermediate aggregate columns
    for ic in &plan.intermediate_columns {
        if ic.source_arg == "*" {
            select_parts.push(format!("{}(*) AS \"{}\"", ic.source_aggregate, ic.name));
        } else {
            select_parts.push(format!(
                "{}({}) AS \"{}\"",
                ic.source_aggregate, ic.source_arg, ic.name
            ));
        }
    }

    // Always add __ivm_count for reference counting
    if plan.needs_ivm_count {
        select_parts.push("COUNT(*) AS __ivm_count".to_string());
    }

    let select_clause = select_parts.join(", ");
    let from_clause = &analysis.from_clause_sql;

    let mut query = format!("SELECT {} FROM {}", select_clause, from_clause);

    if let Some(ref where_clause) = analysis.where_clause {
        query.push_str(&format!(" WHERE {}", where_clause));
    }

    // Group by: use the original group_by_columns expressions (with table qualifiers)
    // because the FROM clause defines those aliases.
    // For DISTINCT without GROUP BY, group by all passthrough columns.
    let group_cols = if plan.group_by_columns.is_empty() && plan.has_distinct {
        analysis
            .select_columns
            .iter()
            .filter(|c| c.is_passthrough)
            .map(|c| c.expr_sql.clone())
            .collect::<Vec<_>>()
    } else {
        plan.group_by_columns.clone()
    };

    if !group_cols.is_empty() {
        query.push_str(&format!(" GROUP BY {}", group_cols.join(", ")));
    }

    query
}

/// Generate the end query: intermediate table -> target table.
///
/// Rewrite a HAVING clause, replacing aggregate function calls with intermediate column refs.
/// E.g., "SUM(amount) > 1000" → "\"__sum_amount\" > 1000"
pub fn rewrite_having(having: &str, plan: &AggregationPlan) -> Option<String> {
    let expr = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(having)
        .and_then(|mut p| p.parse_expr())
        .ok()?;
    Some(rewrite_having_expr(&expr, &plan.intermediate_columns))
}

/// Recursively transform a HAVING expression AST, replacing aggregate functions
/// with references to intermediate table columns.
fn rewrite_having_expr(expr: &Expr, columns: &[IntermediateColumn]) -> String {
    match expr {
        Expr::Function(f) => rewrite_aggregate_call(f, columns),
        Expr::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                rewrite_having_expr(left, columns),
                op,
                rewrite_having_expr(right, columns)
            )
        }
        Expr::UnaryOp { op, expr: inner } => {
            format!("{} {}", op, rewrite_having_expr(inner, columns))
        }
        Expr::Nested(inner) => {
            format!("({})", rewrite_having_expr(inner, columns))
        }
        other => other.to_string(),
    }
}

/// Rewrite a single aggregate function call to its intermediate column reference.
fn rewrite_aggregate_call(f: &Function, columns: &[IntermediateColumn]) -> String {
    let func_name = f.name.to_string().to_uppercase();

    // Check for COUNT(*)
    if func_name == "COUNT" {
        if let FunctionArguments::List(list) = &f.args {
            if list.args.len() == 1
                && matches!(
                    &list.args[0],
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                )
            {
                // COUNT(*) → __count_star
                return "\"__count_star\"".to_string();
            }
        }
    }

    // Extract argument string
    let arg_str = if let FunctionArguments::List(list) = &f.args {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) = list.args.first() {
            Some(arg_expr.to_string())
        } else {
            None
        }
    } else {
        None
    };

    let Some(arg) = arg_str else {
        return f.to_string(); // Can't rewrite, pass through
    };

    // AVG(x) → __sum_x / NULLIF(__count_x, 0)
    if func_name == "AVG" {
        let sanitized = sanitize_for_col_name(&arg);
        let sum_col = format!("__sum_{}", sanitized);
        let count_col = format!("__count_{}", sanitized);
        return format!("\"{}\" / NULLIF(\"{}\", 0)", sum_col, count_col);
    }

    // SUM/COUNT/MIN/MAX → find matching intermediate column
    for col in columns {
        if col.source_aggregate.to_uppercase() == func_name && col.source_arg == arg {
            return format!("\"{}\"", col.name);
        }
    }

    // Fallback: pass through as-is
    f.to_string()
}

fn sanitize_for_col_name(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect::<String>()
        .to_lowercase()
}

/// Uses bare column names since the intermediate table has no table qualifiers.
pub fn generate_end_query(view_name: &str, plan: &AggregationPlan) -> String {
    let table = intermediate_table_name(view_name);
    let mut select_parts: Vec<String> = Vec::new();

    // Group by columns (bare names, since intermediate table uses bare column names)
    for col in &plan.group_by_columns {
        let bare = bare_column_name(col);
        select_parts.push(format!("\"{}\"", bare));
    }

    // For DISTINCT without GROUP BY, add the distinct columns
    for col in &plan.distinct_columns {
        let bare = bare_column_name(col);
        select_parts.push(format!("\"{}\"", bare));
    }

    // End query aggregate expressions
    for mapping in &plan.end_query_mappings {
        select_parts.push(format!(
            "{} AS \"{}\"",
            mapping.intermediate_expr, mapping.output_alias
        ));
    }

    let select_clause = select_parts.join(", ");
    let mut query = format!("SELECT {} FROM {}", select_clause, table);

    // Filter out groups with zero reference count.
    // This ensures deleted groups disappear from the target.
    if plan.needs_ivm_count {
        query.push_str(" WHERE __ivm_count > 0");
    }

    // Apply HAVING clause (rewritten to use intermediate column names)
    if let Some(ref having) = plan.having_clause {
        if let Some(rewritten) = rewrite_having(having, plan) {
            if plan.needs_ivm_count {
                query.push_str(&format!(" AND ({})", rewritten));
            } else {
                query.push_str(&format!(" WHERE ({})", rewritten));
            }
        }
    }

    query
}

/// Serialize the aggregation plan as JSON for the reference table.
pub fn generate_aggregations_json(plan: &AggregationPlan) -> String {
    serde_json::to_string(plan).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::plan_aggregation;
    use crate::sql_analyzer::analyze;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn decompose(sql: &str) -> (SqlAnalysis, AggregationPlan) {
        let parsed = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let analysis = analyze(&parsed).unwrap();
        let plan = plan_aggregation(&analysis);
        (analysis, plan)
    }

    #[test]
    fn test_base_query_simple_sum() {
        let (analysis, plan) =
            decompose("SELECT city, SUM(amount) AS total FROM orders GROUP BY city");
        let base = generate_base_query(&analysis, &plan);
        assert!(base.contains("SUM(amount)"));
        assert!(base.contains("__sum_amount"));
        assert!(base.contains("GROUP BY city"));
        assert!(base.contains("FROM orders"));
        assert!(base.contains("COUNT(*) AS __ivm_count"));
    }

    #[test]
    fn test_base_query_with_avg() {
        let (analysis, plan) =
            decompose("SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept");
        let base = generate_base_query(&analysis, &plan);
        assert!(base.contains("SUM(salary)"));
        assert!(base.contains("__sum_salary"));
        assert!(base.contains("COUNT(salary)"));
        assert!(base.contains("__count_salary"));
        assert!(base.contains("GROUP BY dept"));
    }

    #[test]
    fn test_end_query_avg() {
        let (_analysis, plan) =
            decompose("SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept");
        let end = generate_end_query("test_view", &plan);
        assert!(end.contains("__reflex_intermediate_test_view"));
        assert!(end.contains("__sum_salary / NULLIF(__count_salary, 0)"));
        assert!(end.contains("AS \"avg_sal\""));
    }

    #[test]
    fn test_base_query_distinct() {
        let (analysis, plan) = decompose("SELECT DISTINCT country FROM orders");
        let base = generate_base_query(&analysis, &plan);
        assert!(base.contains("COUNT(*) AS __ivm_count"));
        assert!(base.contains("GROUP BY country"));
    }

    #[test]
    fn test_end_query_distinct() {
        let (_analysis, plan) = decompose("SELECT DISTINCT country FROM orders");
        let end = generate_end_query("countries_view", &plan);
        assert!(end.contains("__ivm_count > 0"));
    }

    #[test]
    fn test_base_query_with_where() {
        let (analysis, plan) = decompose(
            "SELECT city, COUNT(*) AS cnt FROM emp WHERE active = true GROUP BY city",
        );
        let base = generate_base_query(&analysis, &plan);
        assert!(base.contains("WHERE active = true"));
    }

    #[test]
    fn test_aggregations_json_valid() {
        let (_analysis, plan) =
            decompose("SELECT city, SUM(amount) AS total FROM orders GROUP BY city");
        let json = generate_aggregations_json(&plan);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_object());
        assert!(parsed["group_by_columns"].is_array());
    }

    #[test]
    fn test_intermediate_table_name() {
        assert_eq!(
            intermediate_table_name("my_view"),
            "__reflex_intermediate_my_view"
        );
    }

    #[test]
    fn test_bare_column_name() {
        assert_eq!(bare_column_name("d.dept_name"), "dept_name");
        assert_eq!(bare_column_name("city"), "city");
        assert_eq!(bare_column_name("schema.table.col"), "col");
    }

    #[test]
    fn test_base_query_with_alias() {
        let (analysis, plan) = decompose(
            "SELECT a.city, SUM(b.amount) AS total FROM emp a JOIN sales b ON a.id = b.emp_id GROUP BY a.city",
        );
        let base = generate_base_query(&analysis, &plan);
        // The base query should alias a.city to just "city" for the intermediate table
        assert!(base.contains("a.city AS \"city\""));
    }

    #[test]
    fn test_end_query_uses_bare_names() {
        let (_analysis, plan) = decompose(
            "SELECT a.city, SUM(b.amount) AS total FROM emp a JOIN sales b ON a.id = b.emp_id GROUP BY a.city",
        );
        let end = generate_end_query("test_view", &plan);
        assert!(end.contains("\"city\""));
        assert!(!end.contains("a.city"));
    }

    #[test]
    fn test_replace_identifier_basic() {
        let result = replace_identifier("SELECT * FROM regional WHERE x > 1", "regional", "my_view__cte_regional");
        assert!(result.contains("my_view__cte_regional"));
        assert!(!result.contains(" regional "));
    }

    #[test]
    fn test_replace_identifier_no_partial_match() {
        let result = replace_identifier("SELECT * FROM regional_backup", "regional", "replaced");
        // Should NOT replace inside "regional_backup"
        assert!(result.contains("regional_backup"));
        assert!(!result.contains("replaced "));
    }

    #[test]
    fn test_replace_identifier_multiple_occurrences() {
        let result = replace_identifier(
            "SELECT a.x FROM regional a JOIN regional b ON a.id = b.id",
            "regional",
            "new_tbl",
        );
        assert_eq!(result.matches("new_tbl").count(), 2);
    }

    #[test]
    fn test_split_qualified_name() {
        assert_eq!(split_qualified_name("my_view"), (None, "my_view"));
        assert_eq!(
            split_qualified_name("myschema.my_view"),
            (Some("myschema"), "my_view")
        );
    }

    #[test]
    fn test_quote_identifier_unqualified() {
        assert_eq!(quote_identifier("my_view"), "\"my_view\"");
    }

    #[test]
    fn test_quote_identifier_qualified() {
        assert_eq!(
            quote_identifier("myschema.my_view"),
            "\"myschema\".\"my_view\""
        );
    }

    #[test]
    fn test_intermediate_table_name_qualified() {
        assert_eq!(
            intermediate_table_name("myschema.my_view"),
            "\"myschema\".\"__reflex_intermediate_my_view\""
        );
    }

    #[test]
    fn test_rewrite_having_simple_sum() {
        let plan = decompose("SELECT city, SUM(amount) AS total FROM emp GROUP BY city").1;
        let result = rewrite_having("SUM(amount) > 1000", &plan).unwrap();
        assert!(result.contains("__sum_amount"), "Got: {}", result);
        assert!(result.contains("> 1000"), "Got: {}", result);
    }

    #[test]
    fn test_rewrite_having_count_star() {
        let plan = decompose("SELECT city, COUNT(*) AS cnt FROM emp GROUP BY city").1;
        let result = rewrite_having("COUNT(*) > 5", &plan).unwrap();
        assert!(result.contains("__count_star"), "Got: {}", result);
    }

    #[test]
    fn test_rewrite_having_avg() {
        let plan = decompose("SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept").1;
        let result = rewrite_having("AVG(salary) > 50000", &plan).unwrap();
        assert!(result.contains("__sum_salary"), "Got: {}", result);
        assert!(result.contains("NULLIF"), "Got: {}", result);
        assert!(result.contains("__count_salary"), "Got: {}", result);
    }

    #[test]
    fn test_rewrite_having_complex() {
        let plan =
            decompose("SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM emp GROUP BY city")
                .1;
        let result = rewrite_having("SUM(amount) > COUNT(*) * 2", &plan).unwrap();
        assert!(result.contains("__sum_amount"), "Got: {}", result);
        assert!(result.contains("__count_star"), "Got: {}", result);
    }

    mod proptest_tests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// bare_column_name strips table qualifier: "tbl.col" -> "col"
            #[test]
            fn bare_strips_qualifier(
                tbl in "[a-z]{1,10}",
                col in "[a-z]{1,10}",
            ) {
                let qualified = format!("{}.{}", tbl, col);
                assert_eq!(bare_column_name(&qualified), col);
            }

            /// bare_column_name is identity for unqualified names
            #[test]
            fn bare_identity_for_unqualified(col in "[a-z_][a-z0-9_]{0,15}") {
                assert_eq!(bare_column_name(&col), col);
            }

            /// replace_identifier never replaces partial matches
            #[test]
            fn replace_no_partial(
                word in "[a-z]{2,8}",
                suffix in "[a-z]{1,5}",
            ) {
                let longer = format!("{}{}", word, suffix);
                let sql = format!("SELECT {} FROM {}", longer, longer);
                let result = replace_identifier(&sql, &word, "REPLACED");
                // The longer word should NOT be replaced
                assert!(
                    result.contains(&longer),
                    "Partial match should not be replaced: word='{}', longer='{}', result='{}'",
                    word, longer, result
                );
            }
        }
    }
}
