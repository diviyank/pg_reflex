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
    let (analysis, plan) = decompose("SELECT city, SUM(amount) AS total FROM orders GROUP BY city");
    let base = generate_base_query(&analysis, &plan);
    assert!(base.contains("SUM(amount)"));
    assert!(base.contains("__sum_amount"));
    assert!(base.contains("GROUP BY city"));
    assert!(base.contains("FROM orders"));
    assert!(base.contains("COUNT(*) AS __ivm_count"));
}

#[test]
fn test_base_query_with_avg() {
    let (analysis, plan) = decompose("SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept");
    let base = generate_base_query(&analysis, &plan);
    assert!(base.contains("SUM(salary)"));
    assert!(base.contains("__sum_salary"));
    assert!(base.contains("COUNT(salary)"));
    assert!(base.contains("__count_salary"));
    assert!(base.contains("GROUP BY dept"));
}

#[test]
fn test_end_query_avg() {
    let (_analysis, plan) = decompose("SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept");
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
    let (analysis, plan) =
        decompose("SELECT city, COUNT(*) AS cnt FROM emp WHERE active = true GROUP BY city");
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
        "\"__reflex_intermediate_my_view\""
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
    let result = replace_identifier(
        "SELECT * FROM regional WHERE x > 1",
        "regional",
        "my_view__cte_regional",
    );
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

// A CTE sub-IMV source is stored in `froms` already quoted (the body rewrite
// injects `quote_identifier(cte_imv_name)` to preserve case). The deferred
// staging-name builder must not re-quote the already-quoted schema, or it
// emits `""schema""` — a zero-length delimited identifier PG rejects.
#[test]
fn test_staging_delta_table_name_quoted_source() {
    assert_eq!(
        staging_delta_table_name("\"rfx\".\"v__cte_lsb\""),
        "\"rfx\".\"__reflex_delta_rfx_v__cte_lsb\""
    );
}

#[test]
fn qualify_in_same_schema_handles_quoted_source() {
    // Regression lock: a quoted qualified source must yield a single-quoted
    // schema qualifier, never an empty `""`. Exercised via staging_delta_table_name,
    // which attaches the schema via qualify_in_same_schema.
    use crate::query_decomposer::staging_delta_table_name;
    let out = staging_delta_table_name("\"schema\".\"v__cte_a\"");
    assert!(out.starts_with("\"schema\".\""), "got {out:?}");
    assert!(!out.contains("\"\""));
}

#[test]
fn test_staging_delta_table_name_bare_source() {
    assert_eq!(
        staging_delta_table_name("rfx.ss"),
        "\"rfx\".\"__reflex_delta_rfx_ss\""
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
        decompose("SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM emp GROUP BY city").1;
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

#[test]
fn canonical_source_unquotes_both_components() {
    use crate::query_decomposer::canonical_source;
    assert_eq!(
        canonical_source("schema.table"),
        (Some("schema".to_string()), "table".to_string())
    );
    assert_eq!(
        canonical_source("\"schema\".\"view__cte_x\""),
        (Some("schema".to_string()), "view__cte_x".to_string())
    );
    assert_eq!(canonical_source("bare"), (None, "bare".to_string()));
    assert_eq!(
        canonical_source("\"Mixed\""),
        (None, "Mixed".to_string())
    );
}

#[test]
fn name_builders_never_emit_doubled_or_empty_quotes() {
    use crate::query_decomposer::{
        sanitized_source_suffix, staging_delta_table_name, transition_new_table_name,
        transition_old_table_name,
    };
    for src in ["schema.table", "\"schema\".\"view__cte_x\"", "bare", "\"Mixed\""] {
        let outputs = vec![
            ("sanitized_source_suffix", sanitized_source_suffix(src)),
            ("staging_delta_table_name", staging_delta_table_name(src)),
            ("transition_new_table_name", transition_new_table_name(src)),
            ("transition_old_table_name", transition_old_table_name(src)),
        ];
        for (fn_name, out) in outputs {
            assert!(
                !out.contains("\"\""),
                "doubled quote in {fn_name}({src:?}) = {out:?}"
            );
        }
    }
}
