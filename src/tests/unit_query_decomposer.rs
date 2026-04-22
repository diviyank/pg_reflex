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
fn test_replace_source_with_delta_qualified_and_standalone() {
    let result = replace_source_with_delta(
        "SELECT t.col, t.x FROM t WHERE t.col > 1",
        "t",
        "(SUBQ)",
        "__dt",
    );
    assert!(
        result.contains("__dt.col"),
        "qualified .col should get alias prefix: {}",
        result
    );
    assert!(
        result.contains("__dt.x"),
        "qualified .x should get alias prefix: {}",
        result
    );
    assert!(
        result.contains("FROM (SUBQ) AS __dt"),
        "standalone src should become subquery+alias: {}",
        result
    );
    assert!(
        !result.contains(" t.col"),
        "original qualified form must be gone: {}",
        result
    );
}

#[test]
fn test_replace_source_with_delta_no_partial_match() {
    let result = replace_source_with_delta(
        "SELECT other_t.col, t_suffix.x FROM t",
        "t",
        "(SUBQ)",
        "__dt",
    );
    assert!(
        result.contains("other_t.col"),
        "similar prefix identifier must not be rewritten: {}",
        result
    );
    assert!(
        result.contains("t_suffix.x"),
        "similar suffix identifier must not be rewritten: {}",
        result
    );
    assert!(
        result.contains("FROM (SUBQ) AS __dt"),
        "standalone src should still be replaced: {}",
        result
    );
}

#[test]
fn test_replace_source_with_delta_consumes_as_alias() {
    // When the source has a user-alias in FROM/JOIN, the delta subquery must
    // adopt that alias (not emit `AS __dt AS ol` which is invalid SQL).
    let result = replace_source_with_delta(
        "SELECT ol.x FROM order_line AS ol WHERE ol.y > 1",
        "order_line",
        "(SUBQ)",
        "__dt",
    );
    assert!(
        result.contains("FROM (SUBQ) AS ol"),
        "user alias must be preserved: {}",
        result
    );
    assert!(
        !result.contains("AS __dt"),
        "default alias must be dropped when user alias exists: {}",
        result
    );
    assert!(result.contains("ol.x"));
    assert!(result.contains("ol.y"));
}

#[test]
fn test_replace_source_with_delta_consumes_bare_alias() {
    // Same thing for the no-AS form: `FROM table alias`.
    let result = replace_source_with_delta(
        "SELECT lib.x FROM location_inventory_baseline lib WHERE lib.y > 1",
        "location_inventory_baseline",
        "(SUBQ)",
        "__dt",
    );
    assert!(
        result.contains("FROM (SUBQ) AS lib"),
        "bare user alias must be consumed: {}",
        result
    );
    assert!(
        !result.contains("AS __dt lib"),
        "must not emit double alias: {}",
        result
    );
}

#[test]
fn test_replace_source_with_delta_consumes_alias_in_join() {
    let result = replace_source_with_delta(
        "SELECT o.x, ol.y FROM \"order\" AS o JOIN order_line AS ol ON ol.oid = o.id",
        "order_line",
        "(SUBQ)",
        "__dt",
    );
    assert!(
        result.contains("JOIN (SUBQ) AS ol ON"),
        "JOIN alias must be consumed: {}",
        result
    );
    assert!(
        !result.contains("AS __dt AS ol"),
        "must not emit double alias in JOIN: {}",
        result
    );
}

#[test]
fn test_replace_source_with_delta_no_alias_still_defaults() {
    // Sanity check: when source has no alias, we still add `AS __dt`.
    let result = replace_source_with_delta(
        "SELECT orders.x FROM orders WHERE orders.y > 1",
        "orders",
        "(SUBQ)",
        "__dt",
    );
    assert!(
        result.contains("FROM (SUBQ) AS __dt"),
        "default alias used when no user alias: {}",
        result
    );
    assert!(result.contains("__dt.x"));
    assert!(result.contains("__dt.y"));
}

// Bug #12: the `AS <ident>` branch of consume_table_alias accepted any
// identifier — including reserved keywords like SELECT — as an alias. The
// bare-identifier branch already rejected follow-keywords; the AS branch
// must apply the same guard.
#[test]
fn test_replace_source_with_delta_rejects_reserved_word_as_alias() {
    // `FROM orders AS SELECT …` is invalid SQL. pg_reflex must NOT adopt
    // `SELECT` as the subquery's alias (which would silently swallow the
    // reserved keyword and push a confusing mis-parse downstream). Instead
    // it must emit the default alias so the planner sees the malformed
    // `AS __dt AS SELECT …` and rejects it with a clear error.
    let result = replace_source_with_delta(
        "SELECT x FROM orders AS SELECT y, z",
        "orders",
        "(SUBQ)",
        "__dt",
    );
    // The default alias must be emitted (i.e., SELECT not consumed as alias).
    assert!(
        result.contains("FROM (SUBQ) AS __dt"),
        "must fall back to default alias rather than consuming SELECT: {}",
        result
    );
}

#[test]
fn test_replace_source_with_delta_rejects_follow_keyword_as_alias() {
    // Same principle for follow-keywords under AS form: `FROM orders AS JOIN …`.
    let result = replace_source_with_delta(
        "SELECT x FROM orders AS JOIN other ON orders.id = other.id",
        "orders",
        "(SUBQ)",
        "__dt",
    );
    assert!(
        result.contains("FROM (SUBQ) AS __dt"),
        "must fall back to default alias rather than consuming JOIN: {}",
        result
    );
}

#[test]
fn test_replace_source_with_delta_only_qualified() {
    // If every reference is qualified (no standalone FROM <src>), the
    // helper should still rewrite all of them to use the alias prefix.
    let result = replace_source_with_delta(
        "SELECT t.a, t.b FROM other_table JOIN t ON other_table.id = t.id",
        "t",
        "(SUBQ)",
        "__dt",
    );
    assert!(result.contains("__dt.a"), "rewrite a: {}", result);
    assert!(result.contains("__dt.b"), "rewrite b: {}", result);
    assert!(result.contains("__dt.id"), "rewrite id in ON: {}", result);
    assert!(
        result.contains("JOIN (SUBQ) AS __dt ON"),
        "JOIN position rewritten: {}",
        result
    );
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
