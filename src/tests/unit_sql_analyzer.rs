use super::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn parse_and_analyze(sql: &str) -> SqlAnalysis {
    let parsed = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    analyze(&parsed).unwrap()
}

#[test]
fn test_simple_group_by() {
    let a = parse_and_analyze("SELECT city, SUM(salary) FROM emp GROUP BY city");
    assert_eq!(a.group_by_columns, vec!["city"]);
    assert_eq!(a.sources, vec!["emp"]);
    assert_eq!(a.select_columns.len(), 2);
    assert!(a.select_columns[0].is_passthrough);
    assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Sum));
}

#[test]
fn test_avg_detection() {
    let a = parse_and_analyze("SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept");
    assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Avg));
    assert_eq!(a.select_columns[1].alias.as_deref(), Some("avg_sal"));
    assert_eq!(a.select_columns[1].aggregate_arg.as_deref(), Some("salary"));
}

#[test]
fn test_multiple_aggregates() {
    let a = parse_and_analyze(
        "SELECT city, SUM(amount) AS total, COUNT(id) AS cnt FROM orders GROUP BY city",
    );
    assert_eq!(a.select_columns.len(), 3);
    assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Sum));
    assert_eq!(a.select_columns[2].aggregate, Some(AggregateKind::Count));
}

#[test]
fn test_distinct_detected() {
    let a = parse_and_analyze("SELECT DISTINCT country FROM orders");
    assert!(a.has_distinct);
    assert_eq!(a.select_columns.len(), 1);
    assert!(a.select_columns[0].is_passthrough);
}

#[test]
fn test_where_clause() {
    let a = parse_and_analyze("SELECT city, COUNT(*) FROM emp WHERE active = true GROUP BY city");
    assert!(a.where_clause.is_some());
    assert!(a.where_clause.unwrap().contains("active"));
}

#[test]
fn test_join_extraction() {
    let a = parse_and_analyze(
        "SELECT a.city, SUM(b.amount) FROM emp a JOIN sales b ON a.id = b.emp_id GROUP BY a.city",
    );
    assert_eq!(a.sources.len(), 2);
    assert_eq!(a.joins.len(), 1);
    assert_eq!(a.joins[0].join_type, "INNER");
    assert!(a.joins[0].condition_sql.is_some());
}

#[test]
fn test_count_star() {
    let a = parse_and_analyze("SELECT city, COUNT(*) FROM emp GROUP BY city");
    assert_eq!(
        a.select_columns[1].aggregate,
        Some(AggregateKind::CountStar)
    );
    assert_eq!(a.select_columns[1].aggregate_arg.as_deref(), Some("*"));
}

#[test]
fn test_passthrough_columns() {
    let a = parse_and_analyze("SELECT id, name, SUM(amount) FROM orders GROUP BY id, name");
    assert!(a.select_columns[0].is_passthrough);
    assert!(a.select_columns[1].is_passthrough);
    assert!(!a.select_columns[2].is_passthrough);
    assert_eq!(a.group_by_columns, vec!["id", "name"]);
}

#[test]
fn test_min_max_detection() {
    let a = parse_and_analyze("SELECT city, MIN(salary), MAX(salary) FROM emp GROUP BY city");
    assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Min));
    assert_eq!(a.select_columns[2].aggregate, Some(AggregateKind::Max));
}

#[test]
fn test_cte_extracted() {
    let a = parse_and_analyze(
        "WITH regional AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region) SELECT region, total FROM regional",
    );
    assert!(a.unsupported_reason().is_none()); // Non-recursive CTE is now supported
    assert_eq!(a.ctes.len(), 1);
    assert_eq!(a.ctes[0].alias, "regional");
    assert!(a.ctes[0].query_sql.contains("SUM"));
}

#[test]
fn test_cte_extraction_multiple() {
    let a = parse_and_analyze(
        "WITH a AS (SELECT id, COUNT(*) AS cnt FROM t1 GROUP BY id), \
         b AS (SELECT id, SUM(cnt) AS total FROM a GROUP BY id) \
         SELECT * FROM b",
    );
    assert_eq!(a.ctes.len(), 2);
    assert_eq!(a.ctes[0].alias, "a");
    assert_eq!(a.ctes[1].alias, "b");
}

#[test]
fn test_recursive_cte_rejected() {
    let a = parse_and_analyze(
        "WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM nums WHERE n < 10) SELECT * FROM nums",
    );
    assert!(a.has_recursive_cte);
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_unsupported_limit() {
    let a = parse_and_analyze("SELECT * FROM emp LIMIT 10");
    assert!(a.unsupported_reason().is_some());
    assert!(a.has_limit);
}

#[test]
fn test_unsupported_order_by() {
    let a = parse_and_analyze("SELECT * FROM emp ORDER BY id");
    assert!(a.unsupported_reason().is_some());
    assert!(a.has_order_by);
}

#[test]
fn test_window_detected() {
    let a = parse_and_analyze("SELECT id, SUM(amount) OVER (PARTITION BY city) FROM orders");
    assert!(
        a.unsupported_reason().is_none(),
        "Window functions should no longer be unsupported"
    );
    assert!(a.has_window_function);
    // The window column should be flagged
    let win_col = a.select_columns.iter().find(|c| c.is_window);
    assert!(win_col.is_some(), "Should detect window function in SELECT");
}

#[test]
fn test_multiple_queries_error() {
    let parsed = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 1; SELECT 2").unwrap();
    assert!(matches!(
        analyze(&parsed),
        Err(SqlAnalysisError::MultipleQueries(2))
    ));
}

#[test]
fn test_not_select_error() {
    let parsed = Parser::parse_sql(&PostgreSqlDialect {}, "CREATE TABLE t (id INT)").unwrap();
    assert!(matches!(
        analyze(&parsed),
        Err(SqlAnalysisError::NotASelectQuery)
    ));
}

#[test]
fn test_from_clause_sql() {
    let a = parse_and_analyze("SELECT a.x FROM emp a JOIN sales b ON a.id = b.eid");
    assert!(!a.from_clause_sql.is_empty());
}

#[test]
fn test_having_clause() {
    let a = parse_and_analyze(
        "SELECT city, COUNT(*) AS cnt FROM emp GROUP BY city HAVING COUNT(*) > 5",
    );
    assert!(a.having_clause.is_some());
}

#[test]
fn test_cast_aggregate_detected() {
    let a =
        parse_and_analyze("SELECT city, SUM(amount)::BIGINT AS total FROM orders GROUP BY city");
    assert_eq!(a.select_columns.len(), 2);
    assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Sum));
    assert_eq!(a.select_columns[1].aggregate_arg.as_deref(), Some("amount"));
    assert_eq!(a.select_columns[1].alias.as_deref(), Some("total"));
}

#[test]
fn test_multiple_cast_aggregates() {
    let a = parse_and_analyze(
        "SELECT grp, SUM(a)::BIGINT AS sa, COUNT(*)::INT AS cnt FROM t GROUP BY grp",
    );
    assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Sum));
    assert_eq!(
        a.select_columns[2].aggregate,
        Some(AggregateKind::CountStar)
    );
}

#[test]
fn test_malformed_sql_parse_error() {
    let result = Parser::parse_sql(&PostgreSqlDialect {}, "SELEC broken garbage !!!");
    assert!(result.is_err(), "Malformed SQL should fail to parse");
}

#[test]
fn test_table_aliases() {
    let a = parse_and_analyze(
        "SELECT s.product_id, s.amount, p.name FROM sales s JOIN products p ON s.product_id = p.id",
    );
    assert_eq!(a.table_aliases.get("s").map(String::as_str), Some("sales"));
    assert_eq!(
        a.table_aliases.get("p").map(String::as_str),
        Some("products")
    );
}

#[test]
fn test_table_aliases_schema_qualified() {
    let a = parse_and_analyze(
        "SELECT s.id FROM alp.sales_simulation s JOIN dim.products p ON s.product_id = p.id",
    );
    assert_eq!(
        a.table_aliases.get("s").map(String::as_str),
        Some("alp.sales_simulation")
    );
    assert_eq!(
        a.table_aliases.get("p").map(String::as_str),
        Some("dim.products")
    );
}

// ========================================================================
// Unsupported feature detection tests
// ========================================================================

#[test]
fn test_detect_lateral_join() {
    let a = parse_and_analyze(
        "SELECT t.id, s.val FROM t, LATERAL (SELECT val FROM t2 WHERE t2.id = t.id) s",
    );
    assert!(a.has_lateral_join);
    assert!(a.unsupported_reason().is_some());
    assert!(a.unsupported_reason().unwrap().contains("LATERAL"));
}

#[test]
fn test_detect_distinct_on() {
    let a = parse_and_analyze("SELECT DISTINCT ON (city) city, val FROM t");
    assert!(a.has_distinct_on);
    assert!(!a.has_distinct, "DISTINCT ON should not set has_distinct");
    assert!(a.unsupported_reason().is_some());
    assert!(a.unsupported_reason().unwrap().contains("DISTINCT ON"));
}

#[test]
fn test_detect_grouping_sets() {
    let a = parse_and_analyze("SELECT city, SUM(val) FROM t GROUP BY GROUPING SETS ((city), ())");
    assert!(a.has_grouping_sets);
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_detect_cube() {
    let a = parse_and_analyze("SELECT city, state, SUM(val) FROM t GROUP BY CUBE (city, state)");
    assert!(a.has_grouping_sets);
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_detect_rollup() {
    let a = parse_and_analyze("SELECT city, SUM(val) FROM t GROUP BY ROLLUP (city)");
    assert!(a.has_grouping_sets);
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_detect_filter_clause() {
    let a = parse_and_analyze("SELECT city, COUNT(*) FILTER (WHERE active) FROM t GROUP BY city");
    assert!(a.has_filter_clause);
    // FILTER is now supported via CASE WHEN rewrite
    assert!(
        a.unsupported_reason().is_none(),
        "FILTER should be supported, got: {:?}",
        a.unsupported_reason()
    );
}

#[test]
fn test_detect_within_group() {
    let a = parse_and_analyze(
        "SELECT city, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) FROM t GROUP BY city",
    );
    assert!(a.has_within_group);
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_detect_tablesample() {
    let a = parse_and_analyze("SELECT * FROM t TABLESAMPLE BERNOULLI (10)");
    assert!(a.has_tablesample);
    assert!(a.unsupported_reason().is_some());
    assert!(a.unsupported_reason().unwrap().contains("TABLESAMPLE"));
}

#[test]
fn test_detect_nondeterministic_select() {
    let a = parse_and_analyze("SELECT NOW(), city FROM t GROUP BY city");
    assert!(a.has_nondeterministic_select);
    assert!(a.unsupported_reason().is_some());

    let b = parse_and_analyze("SELECT RANDOM(), id FROM t");
    assert!(b.has_nondeterministic_select);
}

#[test]
fn test_detect_unsupported_aggregate_string_agg() {
    let a = parse_and_analyze("SELECT city, STRING_AGG(name, ', ') FROM t GROUP BY city");
    assert!(!a.unsupported_aggregates.is_empty());
    assert!(a.unsupported_aggregates.contains(&"STRING_AGG".to_string()));
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_detect_unsupported_aggregate_array_agg() {
    let a = parse_and_analyze("SELECT city, ARRAY_AGG(val) FROM t GROUP BY city");
    assert!(a.unsupported_aggregates.contains(&"ARRAY_AGG".to_string()));
}

#[test]
fn test_detect_unsupported_aggregate_stddev() {
    let a = parse_and_analyze("SELECT city, STDDEV(val) FROM t GROUP BY city");
    assert!(a.unsupported_aggregates.contains(&"STDDEV".to_string()));
}

#[test]
fn test_detect_scalar_subquery() {
    let a = parse_and_analyze("SELECT (SELECT MAX(x) FROM t2), city FROM t GROUP BY city");
    assert!(a.has_scalar_subquery);
    // Scalar subqueries are now allowed (evaluated at trigger time as static values)
    assert!(
        a.unsupported_reason().is_none(),
        "Scalar subqueries should be allowed, got: {:?}",
        a.unsupported_reason()
    );
}

#[test]
fn test_scalar_subquery_in_where() {
    let a = parse_and_analyze(
        "SELECT city, SUM(val) FROM t WHERE year >= (SELECT MAX(year) FROM t2) GROUP BY city",
    );
    assert!(a.has_scalar_subquery);
    assert!(a.unsupported_reason().is_none());
}

#[test]
fn test_supported_aggregates_not_flagged() {
    let a = parse_and_analyze(
        "SELECT city, SUM(val), COUNT(*), AVG(val), MIN(val), MAX(val), BOOL_OR(flag) \
         FROM t GROUP BY city",
    );
    assert!(
        a.unsupported_aggregates.is_empty(),
        "Supported aggregates should not be flagged: {:?}",
        a.unsupported_aggregates
    );
    assert!(!a.has_filter_clause);
    assert!(!a.has_within_group);
    assert!(!a.has_nondeterministic_select);
    assert!(
        a.unsupported_reason().is_none(),
        "Query with only supported features should pass: {:?}",
        a.unsupported_reason()
    );
}

#[test]
fn test_regular_functions_not_flagged_as_aggregates() {
    // UPPER, LOWER, COALESCE etc. are scalar functions, not aggregates
    let a = parse_and_analyze("SELECT UPPER(name), COALESCE(val, 0) FROM t");
    assert!(
        a.unsupported_aggregates.is_empty(),
        "Regular scalar functions should not be flagged: {:?}",
        a.unsupported_aggregates
    );
}

#[test]
fn test_multiple_unsupported_aggregates() {
    let a = parse_and_analyze(
        "SELECT city, STRING_AGG(name, ','), ARRAY_AGG(val), STDDEV(val) FROM t GROUP BY city",
    );
    assert_eq!(a.unsupported_aggregates.len(), 3);
}

// ========================================================================
// FILTER clause tests
// ========================================================================

#[test]
fn test_filter_sum() {
    let a = parse_and_analyze(
        "SELECT city, SUM(amount) FILTER (WHERE active) AS active_total FROM t GROUP BY city",
    );
    assert!(a.has_filter_clause, "FILTER should be detected");
    assert!(
        a.unsupported_reason().is_none(),
        "FILTER should no longer be rejected: {:?}",
        a.unsupported_reason()
    );
    let col = &a.select_columns[1];
    assert_eq!(col.aggregate, Some(AggregateKind::Sum));
    let arg = col.aggregate_arg.as_deref().unwrap();
    assert!(
        arg.contains("CASE") && arg.contains("WHEN") && arg.contains("active"),
        "SUM FILTER arg should be rewritten to CASE WHEN, got: {}",
        arg
    );
    assert!(
        arg.contains("amount"),
        "Rewritten arg should contain original column: {}",
        arg
    );
}

#[test]
fn test_filter_count_star() {
    let a = parse_and_analyze(
        "SELECT city, COUNT(*) FILTER (WHERE active) AS active_cnt FROM t GROUP BY city",
    );
    assert!(a.unsupported_reason().is_none());
    let col = &a.select_columns[1];
    // COUNT(*) FILTER → COUNT(CASE WHEN active THEN 1 END)
    assert_eq!(
        col.aggregate,
        Some(AggregateKind::Count),
        "COUNT(*) FILTER should become Count (not CountStar), got: {:?}",
        col.aggregate
    );
    let arg = col.aggregate_arg.as_deref().unwrap();
    assert!(
        arg.contains("CASE") && arg.contains("1"),
        "COUNT(*) FILTER arg should be CASE WHEN ... THEN 1 END, got: {}",
        arg
    );
}

#[test]
fn test_filter_count_col() {
    let a = parse_and_analyze(
        "SELECT city, COUNT(val) FILTER (WHERE active) AS cnt FROM t GROUP BY city",
    );
    assert!(a.unsupported_reason().is_none());
    let col = &a.select_columns[1];
    assert_eq!(col.aggregate, Some(AggregateKind::Count));
    let arg = col.aggregate_arg.as_deref().unwrap();
    assert!(
        arg.contains("CASE") && arg.contains("val"),
        "COUNT(col) FILTER arg should be CASE WHEN ... THEN val END, got: {}",
        arg
    );
}

#[test]
fn test_filter_avg() {
    let a = parse_and_analyze(
        "SELECT city, AVG(salary) FILTER (WHERE active) AS avg_sal FROM t GROUP BY city",
    );
    assert!(a.unsupported_reason().is_none());
    let col = &a.select_columns[1];
    assert_eq!(col.aggregate, Some(AggregateKind::Avg));
    let arg = col.aggregate_arg.as_deref().unwrap();
    assert!(
        arg.contains("CASE") && arg.contains("salary"),
        "AVG FILTER arg should be rewritten, got: {}",
        arg
    );
}

#[test]
fn test_filter_min_max() {
    let a = parse_and_analyze(
        "SELECT city, MIN(val) FILTER (WHERE active) AS lo, MAX(val) FILTER (WHERE active) AS hi FROM t GROUP BY city",
    );
    assert!(a.unsupported_reason().is_none());
    assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Min));
    assert_eq!(a.select_columns[2].aggregate, Some(AggregateKind::Max));
    for col in &a.select_columns[1..] {
        let arg = col.aggregate_arg.as_deref().unwrap();
        assert!(
            arg.contains("CASE") && arg.contains("val"),
            "MIN/MAX FILTER arg should be rewritten, got: {}",
            arg
        );
    }
}

#[test]
fn test_filter_mixed_with_regular() {
    let a = parse_and_analyze(
        "SELECT city, SUM(amount) AS total, COUNT(*) FILTER (WHERE active) AS active_cnt FROM t GROUP BY city",
    );
    assert!(a.unsupported_reason().is_none());
    // Regular SUM — not rewritten
    let sum_col = &a.select_columns[1];
    assert_eq!(sum_col.aggregate, Some(AggregateKind::Sum));
    assert_eq!(sum_col.aggregate_arg.as_deref(), Some("amount"));
    // COUNT(*) FILTER — rewritten
    let cnt_col = &a.select_columns[2];
    assert_eq!(cnt_col.aggregate, Some(AggregateKind::Count));
    let arg = cnt_col.aggregate_arg.as_deref().unwrap();
    assert!(
        arg.contains("CASE"),
        "FILTER aggregate should be rewritten, got: {}",
        arg
    );
}

#[test]
fn test_filter_expr_captured() {
    let a = parse_and_analyze(
        "SELECT city, SUM(amount) FILTER (WHERE status = 'active') AS s FROM t GROUP BY city",
    );
    let col = &a.select_columns[1];
    assert!(col.filter_expr.is_some(), "filter_expr should be captured");
    let filter = col.filter_expr.as_deref().unwrap();
    assert!(
        filter.contains("status") && filter.contains("active"),
        "filter_expr should contain the original predicate, got: {}",
        filter
    );
}

// ========================================================================
// DISTINCT ON tests
// ========================================================================

#[test]
fn test_distinct_on_columns_captured() {
    let a = parse_and_analyze(
        "SELECT DISTINCT ON (city) city, name, val FROM t ORDER BY city, val DESC",
    );
    assert!(a.has_distinct_on);
    assert!(
        !a.distinct_on_columns.is_empty(),
        "distinct_on_columns should be captured"
    );
    assert_eq!(a.distinct_on_columns, vec!["city"]);
}

#[test]
fn test_distinct_on_multi_columns_captured() {
    let a = parse_and_analyze(
        "SELECT DISTINCT ON (city, dept) city, dept, name FROM t ORDER BY city, dept, name",
    );
    assert!(a.has_distinct_on);
    assert_eq!(a.distinct_on_columns, vec!["city", "dept"]);
}

#[test]
fn test_distinct_on_order_by_captured() {
    let a = parse_and_analyze(
        "SELECT DISTINCT ON (city) city, name, val FROM t ORDER BY city, val DESC",
    );
    assert!(!a.order_by_exprs.is_empty(), "ORDER BY should be captured");
    // Should contain something like "city" and "val DESC"
    let joined = a.order_by_exprs.join(", ");
    assert!(
        joined.contains("city"),
        "ORDER BY should contain 'city': {}",
        joined
    );
    assert!(
        joined.contains("val"),
        "ORDER BY should contain 'val': {}",
        joined
    );
}

#[test]
fn test_distinct_on_no_longer_rejected() {
    let a = parse_and_analyze(
        "SELECT DISTINCT ON (city) city, name, val FROM t ORDER BY city, val DESC",
    );
    assert!(
        a.unsupported_reason().is_none(),
        "DISTINCT ON should be supported, got: {:?}",
        a.unsupported_reason()
    );
}

#[test]
fn test_order_by_still_rejected_without_distinct_on() {
    let a = parse_and_analyze("SELECT * FROM t ORDER BY id");
    assert!(a.unsupported_reason().is_some());
    assert!(a.unsupported_reason().unwrap().contains("ORDER BY"));
}

// ========================================================================
// Bug fix tests: aggregate-derived expressions (CASE + SUM)
// ========================================================================

#[test]
fn test_case_with_sum_detected_as_aggregate_derived() {
    let a = parse_and_analyze(
        "SELECT grp, CASE WHEN SUM(x) = 0 THEN 0 ELSE SUM(x) END AS val FROM t GROUP BY grp",
    );
    let col = &a.select_columns[1];
    assert!(
        col.is_aggregate_derived,
        "CASE containing SUM should be marked as aggregate_derived"
    );
    assert!(
        !col.is_passthrough,
        "Aggregate-derived column should not be passthrough"
    );
    assert!(
        col.aggregate.is_none(),
        "Aggregate-derived column should not have a direct aggregate kind"
    );
}

#[test]
fn test_nested_case_with_multiple_aggregates() {
    let a = parse_and_analyze(
        "SELECT grp, CASE WHEN SUM(a) = 0 AND SUM(b) = 0 THEN 0 WHEN SUM(b) = 0 THEN 2 ELSE SUM(a) / SUM(b) END AS zscore FROM t GROUP BY grp",
    );
    let col = &a.select_columns[1];
    assert!(col.is_aggregate_derived);
    assert!(!col.is_passthrough);
}

#[test]
fn test_simple_sum_not_aggregate_derived() {
    let a = parse_and_analyze("SELECT grp, SUM(x) AS total FROM t GROUP BY grp");
    let col = &a.select_columns[1];
    assert!(
        !col.is_aggregate_derived,
        "Simple SUM should not be aggregate_derived"
    );
    assert!(col.aggregate.is_some());
}

#[test]
fn test_expr_contains_aggregate_function() {
    use crate::sql_analyzer::expr_contains_aggregate;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let expr = Parser::new(&PostgreSqlDialect {})
        .try_with_sql("CASE WHEN SUM(x) > 0 THEN SUM(x) ELSE 0 END")
        .unwrap()
        .parse_expr()
        .unwrap();
    assert!(expr_contains_aggregate(&expr));

    let expr2 = Parser::new(&PostgreSqlDialect {})
        .try_with_sql("x + y * 2")
        .unwrap()
        .parse_expr()
        .unwrap();
    assert!(!expr_contains_aggregate(&expr2));
}

// ------------------------------------------------------------------
// imv_relevant_columns — per-source set of columns referenced outside
// the WHERE clause. Drives the trigger filter-aware spurious-skip.
// ------------------------------------------------------------------

fn relevant_cols(a: &SqlAnalysis, source: &str) -> Vec<String> {
    a.imv_relevant_columns
        .get(source)
        .map(|s| s.iter().cloned().collect())
        .unwrap_or_default()
}

#[test]
fn imv_relevant_skips_where_only_columns() {
    // status appears only in WHERE — it must NOT be IMV-relevant.
    let a = parse_and_analyze("SELECT id, qty FROM orders WHERE status IN ('open', 'paid')");
    let cols = relevant_cols(&a, "orders");
    assert_eq!(cols, vec!["id", "qty"]);
    assert!(!cols.contains(&"status".to_string()));
}

#[test]
fn imv_relevant_includes_where_col_if_also_projected() {
    // status is BOTH in WHERE and in SELECT → must be IMV-relevant.
    let a = parse_and_analyze(
        "SELECT status, COUNT(*) FROM orders WHERE status <> 'cancelled' GROUP BY status",
    );
    let cols = relevant_cols(&a, "orders");
    assert!(cols.contains(&"status".to_string()));
}

#[test]
fn imv_relevant_with_join_and_alias() {
    // Customer-shape: GROUP BY a join key, WHERE a filter-only column.
    let a = parse_and_analyze(
        "SELECT dp.assortment_id, SUM(s.qty) \
         FROM yse.sales_simulation s \
         JOIN yse.demand_planning dp ON s.dem_plan_id = dp.id \
         WHERE dp.status IN ('validated', 'current') \
         GROUP BY dp.assortment_id",
    );
    let dp_cols = relevant_cols(&a, "yse.demand_planning");
    let ss_cols = relevant_cols(&a, "yse.sales_simulation");
    // dp.status is only in WHERE → filter-only.
    assert!(!dp_cols.contains(&"status".to_string()));
    // dp.id and dp.assortment_id are used in JOIN / GROUP BY → relevant.
    assert!(dp_cols.contains(&"id".to_string()));
    assert!(dp_cols.contains(&"assortment_id".to_string()));
    // s.dem_plan_id (JOIN) and s.qty (aggregate arg) → relevant.
    assert!(ss_cols.contains(&"dem_plan_id".to_string()));
    assert!(ss_cols.contains(&"qty".to_string()));
}

#[test]
fn imv_relevant_includes_having_columns() {
    // count_status appears in HAVING — kept as relevant.
    let a = parse_and_analyze(
        "SELECT city, COUNT(*) FROM emp WHERE dept='eng' GROUP BY city HAVING COUNT(distinct city) > 1",
    );
    let cols = relevant_cols(&a, "emp");
    assert!(cols.contains(&"city".to_string()));
    assert!(!cols.contains(&"dept".to_string()));
}

#[test]
fn imv_relevant_aggregate_argument_is_relevant() {
    let a = parse_and_analyze("SELECT city, SUM(salary) FROM emp GROUP BY city");
    let cols = relevant_cols(&a, "emp");
    assert!(cols.contains(&"city".to_string()));
    assert!(cols.contains(&"salary".to_string()));
}

#[test]
fn imv_relevant_using_clause_columns() {
    let a = parse_and_analyze(
        "SELECT t.x, COUNT(*) FROM t JOIN u USING (k) WHERE t.f='y' GROUP BY t.x",
    );
    // Both t and u should have `k` as IMV-relevant.
    let t_cols = relevant_cols(&a, "t");
    let u_cols = relevant_cols(&a, "u");
    assert!(t_cols.contains(&"k".to_string()));
    assert!(u_cols.contains(&"k".to_string()));
    // t.f is filter-only.
    assert!(!t_cols.contains(&"f".to_string()));
    // t.x is projected/grouped.
    assert!(t_cols.contains(&"x".to_string()));
}

#[test]
fn imv_relevant_empty_when_select_star() {
    // Wildcard projection means we'd need the catalog to enumerate
    // columns. Returning an empty map disables the optimization safely.
    let a = parse_and_analyze("SELECT * FROM orders WHERE status='open'");
    assert!(a.imv_relevant_columns.is_empty());
}

#[test]
fn imv_relevant_empty_when_query_has_cte() {
    // CTE refs cross-link to source columns via projection; static
    // attribution is unsafe. Return empty.
    let a = parse_and_analyze(
        "WITH x AS (SELECT id, status FROM orders) \
         SELECT id FROM x WHERE status='open'",
    );
    assert!(a.imv_relevant_columns.is_empty());
}

#[test]
fn imv_relevant_single_source_bare_idents_attributed() {
    // Bare column refs in a single-source query attribute to that source.
    let a = parse_and_analyze("SELECT id, qty FROM orders WHERE status='open'");
    let cols = relevant_cols(&a, "orders");
    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"qty".to_string()));
    assert!(!cols.contains(&"status".to_string()));
}

#[test]
fn imv_relevant_computed_group_by_extracts_underlying_columns() {
    // GROUP BY date_trunc('day', ts) — underlying ts must be relevant.
    let a = parse_and_analyze(
        "SELECT date_trunc('day', ts) AS d, COUNT(*) FROM events \
         WHERE kind='login' GROUP BY date_trunc('day', ts)",
    );
    let cols = relevant_cols(&a, "events");
    assert!(cols.contains(&"ts".to_string()));
    assert!(!cols.contains(&"kind".to_string()));
}

#[test]
fn imv_relevant_subquery_in_where_does_not_leak() {
    // A scalar subquery inside WHERE references its own table — the refs
    // belong inside WHERE (excluded) and to a relation NOT in `sources`
    // (no leak into the relevant set for `orders`).
    let a = parse_and_analyze(
        "SELECT id, qty FROM orders \
         WHERE created_at >= (SELECT cutoff FROM config)",
    );
    let cols = relevant_cols(&a, "orders");
    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"qty".to_string()));
    // created_at lives only in WHERE → filter-only.
    assert!(!cols.contains(&"created_at".to_string()));
    // cutoff belongs to `config`, which is not a real source for `orders`.
    assert!(!cols.contains(&"cutoff".to_string()));
}

// ------------------------------------------------------------------
// imv_relevant_where — per-source restricted WHERE conjuncts with
// alias prefixes stripped.
// ------------------------------------------------------------------

#[test]
fn imv_relevant_where_single_source_no_alias() {
    let a = parse_and_analyze("SELECT id, qty FROM orders WHERE status='open'");
    let w = a
        .imv_relevant_where
        .get("orders")
        .cloned()
        .unwrap_or_default();
    assert_eq!(w, "status = 'open'");
}

#[test]
fn imv_relevant_where_strips_aliases() {
    let a = parse_and_analyze(
        "SELECT o.id, SUM(o.qty) FROM orders o WHERE o.status='open' GROUP BY o.id",
    );
    let w = a
        .imv_relevant_where
        .get("orders")
        .cloned()
        .unwrap_or_default();
    assert_eq!(w, "status = 'open'");
}

#[test]
fn imv_relevant_where_keeps_single_source_conjuncts_only() {
    // Customer-shape: WHERE clause restricted to ONE source's columns in a
    // multi-source IMV. The conjunct lands in dp's bucket.
    let a = parse_and_analyze(
        "SELECT dp.assortment_id, SUM(s.qty) \
         FROM yse.sales_simulation s \
         JOIN yse.demand_planning dp ON s.dem_plan_id = dp.id \
         WHERE dp.status IN ('validated', 'current') \
         GROUP BY dp.assortment_id",
    );
    let dp_w = a
        .imv_relevant_where
        .get("yse.demand_planning")
        .cloned()
        .unwrap_or_default();
    assert!(dp_w.contains("status IN"));
    assert!(!dp_w.contains("dp."));
    // sales_simulation has no own conjunct.
    assert!(!a.imv_relevant_where.contains_key("yse.sales_simulation"));
}

#[test]
fn imv_relevant_where_drops_cross_source_conjuncts() {
    // Conjunct references both `dp` and `s` — drop it (cannot evaluate
    // against a single transition table).
    let a = parse_and_analyze(
        "SELECT dp.assortment_id, SUM(s.qty) \
         FROM yse.sales_simulation s \
         JOIN yse.demand_planning dp ON s.dem_plan_id = dp.id \
         WHERE dp.status = 'open' AND s.qty > 0 AND dp.id = s.dem_plan_id \
         GROUP BY dp.assortment_id",
    );
    let dp_w = a
        .imv_relevant_where
        .get("yse.demand_planning")
        .cloned()
        .unwrap_or_default();
    let s_w = a
        .imv_relevant_where
        .get("yse.sales_simulation")
        .cloned()
        .unwrap_or_default();
    // Each source keeps its own conjunct.
    assert!(dp_w.contains("status = 'open'"));
    assert!(s_w.contains("qty > 0"));
    // The cross-source conjunct (`dp.id = s.dem_plan_id`) is dropped.
    assert!(!dp_w.contains("dem_plan_id"));
    assert!(!s_w.contains("dem_plan_id"));
}

#[test]
fn imv_relevant_where_multiple_conjuncts_same_source_joined_by_and() {
    let a = parse_and_analyze("SELECT id, qty FROM orders WHERE status='open' AND qty > 0");
    let w = a
        .imv_relevant_where
        .get("orders")
        .cloned()
        .unwrap_or_default();
    assert!(w.contains("status = 'open'"));
    assert!(w.contains("qty > 0"));
    assert!(w.contains(" AND "));
}

#[test]
fn imv_relevant_where_empty_when_no_where_clause() {
    let a = parse_and_analyze("SELECT id, qty FROM orders");
    assert!(a.imv_relevant_where.is_empty());
}

#[test]
fn imv_relevant_where_empty_with_cte() {
    let a = parse_and_analyze(
        "WITH x AS (SELECT id, status FROM orders) \
         SELECT id FROM x WHERE status='open'",
    );
    assert!(a.imv_relevant_where.is_empty());
}

#[test]
fn imv_relevant_where_drops_disjunctions_within_conjunct() {
    // OR is NOT split — we treat the whole `(a OR b)` as one conjunct.
    // If all refs go to one source, kept. Here both refs are to `orders`
    // so it stays.
    let a = parse_and_analyze("SELECT id FROM orders WHERE (status='open' OR status='paid')");
    let w = a
        .imv_relevant_where
        .get("orders")
        .cloned()
        .unwrap_or_default();
    assert!(w.contains("status = 'open'"));
    assert!(w.contains("status = 'paid'"));
}

#[test]
fn is_bare_column_reference_accepts_identifier() {
    assert!(is_bare_column_reference("region"));
    assert!(is_bare_column_reference("\"region\""));
}

#[test]
fn is_bare_column_reference_accepts_compound_identifier() {
    assert!(is_bare_column_reference("t.region"));
    assert!(is_bare_column_reference("\"t\".\"region\""));
}

#[test]
fn is_bare_column_reference_rejects_function_call() {
    assert!(!is_bare_column_reference("DATE_TRUNC('month', d)"));
    assert!(!is_bare_column_reference("UPPER(region)"));
    assert!(!is_bare_column_reference("COALESCE(a, b)"));
}

#[test]
fn is_bare_column_reference_rejects_arithmetic() {
    assert!(!is_bare_column_reference("a + b"));
    assert!(!is_bare_column_reference("region || '_x'"));
}

#[test]
fn is_bare_column_reference_rejects_cast() {
    assert!(!is_bare_column_reference("region::TEXT"));
    assert!(!is_bare_column_reference("CAST(region AS TEXT)"));
}

#[test]
fn is_bare_column_reference_rejects_garbage() {
    assert!(!is_bare_column_reference(""));
    assert!(!is_bare_column_reference("SELECT 1"));
}

mod proptest_tests {
    use super::*;
    use proptest::prelude::*;

    /// Generate a random supported aggregate expression
    fn supported_agg_strategy() -> impl Strategy<Value = (&'static str, String)> {
        prop_oneof![
            Just(("SUM", "SUM(val)".to_string())),
            Just(("COUNT", "COUNT(val)".to_string())),
            Just(("COUNT", "COUNT(*)".to_string())),
            Just(("AVG", "AVG(val)".to_string())),
            Just(("MIN", "MIN(val)".to_string())),
            Just(("MAX", "MAX(val)".to_string())),
            Just(("BOOL_OR", "BOOL_OR(flag)".to_string())),
        ]
    }

    /// Generate a random unsupported aggregate name
    fn unsupported_agg_strategy() -> impl Strategy<Value = &'static str> {
        prop_oneof![
            Just("STRING_AGG"),
            Just("ARRAY_AGG"),
            Just("JSON_AGG"),
            Just("JSONB_AGG"),
            Just("STDDEV"),
            Just("VARIANCE"),
            Just("BOOL_AND"),
            Just("EVERY"),
            Just("BIT_AND"),
            Just("BIT_OR"),
            Just("MODE"),
        ]
    }

    proptest! {
        /// Any query using only supported aggregates should pass validation
        #[test]
        fn supported_sql_passes_validation(
            agg1 in supported_agg_strategy(),
            agg2 in supported_agg_strategy(),
            has_where in any::<bool>(),
        ) {
            let where_clause = if has_where { " WHERE val > 0" } else { "" };
            let sql = format!(
                "SELECT grp, {} AS a1, {} AS a2 FROM tbl{} GROUP BY grp",
                agg1.1, agg2.1, where_clause
            );
            let a = parse_and_analyze(&sql);
            prop_assert!(a.unsupported_reason().is_none(),
                "Supported query should pass: {} => {:?}", sql, a.unsupported_reason());
        }

        /// Any query using an unsupported aggregate should be detected
        #[test]
        fn unsupported_aggregate_always_detected(
            agg_name in unsupported_agg_strategy(),
        ) {
            // STRING_AGG needs two args, others need one
            let expr = if agg_name == "STRING_AGG" {
                format!("{}(name, ',')", agg_name)
            } else {
                format!("{}(val)", agg_name)
            };
            let sql = format!(
                "SELECT grp, {} AS a FROM tbl GROUP BY grp",
                expr
            );
            let a = parse_and_analyze(&sql);
            prop_assert!(!a.unsupported_aggregates.is_empty(),
                "{} should be detected as unsupported in: {}", agg_name, sql);
        }

        /// Non-deterministic functions are always detected
        #[test]
        fn nondeterministic_always_detected(
            func in prop_oneof![
                Just("NOW()"),
                Just("RANDOM()"),
                Just("CURRENT_TIMESTAMP"),
                Just("CLOCK_TIMESTAMP()"),
                Just("GEN_RANDOM_UUID()"),
            ],
        ) {
            let sql = format!("SELECT {}, grp FROM tbl GROUP BY grp", func);
            let a = parse_and_analyze(&sql);
            prop_assert!(a.has_nondeterministic_select,
                "{} should be detected as non-deterministic in: {}", func, sql);
        }
    }
}

fn rejection_of(sql: &str) -> Option<String> {
    parse_and_analyze(sql).unsupported_reason()
}

#[test]
fn distinct_aggregate_rejected_in_every_spelling() {
    for agg in [
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "BOOL_OR",
        "sum",
        "pg_catalog.sum",
        "\"sum\"",
    ] {
        for open in [
            "(DISTINCT ",
            " (DISTINCT ",
            "( DISTINCT ",
            "(\n  DISTINCT ",
            "(/**/DISTINCT ",
            "(distinct ",
        ] {
            let sql = format!("SELECT g, {agg}{open}v) AS s FROM t GROUP BY g");
            let reason = rejection_of(&sql);
            assert!(
                reason
                    .as_deref()
                    .is_some_and(|r| r.contains("DISTINCT modifier")),
                "not rejected: {sql} -> {reason:?}"
            );
        }
    }
}

#[test]
fn distinct_aggregate_rejected_in_every_position() {
    for sql in [
        "SELECT g, SUM( DISTINCT v) + 1 AS s FROM t GROUP BY g",
        "SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING SUM( DISTINCT v) > 3",
        "WITH c AS (SELECT g, SUM( DISTINCT v) AS s FROM t GROUP BY g) SELECT g, s FROM c",
        "SELECT g, SUM(v) AS s FROM t GROUP BY g UNION ALL SELECT g, SUM( DISTINCT v) FROM u GROUP BY g",
        "SELECT g, CASE WHEN AVG( DISTINCT v) > 0 THEN 1 ELSE 0 END AS f FROM t GROUP BY g",
        "SELECT g, SUM(v) AS s FROM t WHERE v > (SELECT MAX( DISTINCT v) FROM u) GROUP BY g",
    ] {
        assert!(
            rejection_of(sql).is_some_and(|r| r.contains("DISTINCT modifier")),
            "not rejected: {sql}"
        );
    }
}

#[test]
fn count_distinct_and_literals_are_not_rejected() {
    for sql in [
        "SELECT g, COUNT(DISTINCT v) AS c FROM t GROUP BY g",
        "SELECT g, COUNT( DISTINCT v) AS c FROM t GROUP BY g",
        "SELECT g, pg_catalog.count(DISTINCT v) AS c FROM t GROUP BY g",
        "SELECT g, COUNT(DISTINCT v) FILTER (WHERE v > 0) AS c FROM t GROUP BY g",
        "SELECT g, 'SUM(DISTINCT x)' AS d, SUM(v) AS s FROM t GROUP BY g",
        "SELECT g, SUM(v) AS s FROM t WHERE g <> 'max (distinct' GROUP BY g",
        "SELECT DISTINCT g FROM t",
    ] {
        let reason = rejection_of(sql);
        assert!(
            !reason
                .as_deref()
                .is_some_and(|r| r.contains("DISTINCT modifier")),
            "wrongly rejected: {sql} -> {reason:?}"
        );
    }
}

#[test]
fn distinct_aggregate_in_window_call_rejected() {
    for sql in [
        "SELECT g, v, SUM(DISTINCT v) OVER () AS s FROM t",
        "SELECT g, v, SUM( DISTINCT v) OVER (PARTITION BY g) AS s FROM t",
    ] {
        assert!(
            rejection_of(sql).is_some_and(|r| r.contains("DISTINCT modifier")),
            "not rejected: {sql}"
        );
    }
}

#[test]
fn nested_count_distinct_rejected_and_top_level_accepted() {
    for sql in [
        "SELECT g, COUNT(DISTINCT v) + 1 AS c FROM t GROUP BY g",
        "SELECT g, COALESCE(COUNT(DISTINCT v), 0) AS c FROM t GROUP BY g",
        "SELECT g, CASE WHEN COUNT(DISTINCT v) > 1 THEN 1 ELSE 0 END AS c FROM t GROUP BY g",
    ] {
        assert!(
            rejection_of(sql)
                .is_some_and(|r| r.contains("COUNT(DISTINCT) nested inside an expression")),
            "not rejected: {sql}"
        );
    }
    for sql in [
        "SELECT g, COUNT(DISTINCT v) AS c FROM t GROUP BY g",
        "SELECT g, COUNT(DISTINCT v)::int AS c FROM t GROUP BY g",
        "SELECT g, SUM(v) + (SELECT COUNT(DISTINCT x) FROM u) AS c FROM t GROUP BY g",
    ] {
        assert_eq!(rejection_of(sql), None, "wrongly rejected: {sql}");
    }
}

#[test]
fn clock_and_session_functions_rejected_under_any_spelling() {
    for call in [
        "now()",
        "pg_catalog.now()",
        "\"now\"()",
        "transaction_timestamp()",
        "statement_timestamp()",
        "clock_timestamp()",
        "timeofday()",
        "LOCALTIMESTAMP",
        "LOCALTIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "age(ts)",
        "current_user",
        "session_user",
        "current_role",
        "current_setting('x')",
        "current_schema()",
        "current_database()",
        "txid_current()",
        "pg_current_xact_id()",
        "random()",
        "gen_random_uuid()",
    ] {
        for sql in [
            format!("SELECT id, {call} AS x FROM t"),
            format!("SELECT g, COUNT(*) AS c FROM t WHERE ts::text <> ({call})::text GROUP BY g"),
            format!("WITH c AS (SELECT id, {call} AS x FROM t) SELECT id, x FROM c"),
            format!("SELECT a.id FROM t a JOIN u b ON b.id = a.id AND b.x::text <> ({call})::text"),
        ] {
            assert!(
                parse_and_analyze(&sql)
                    .unsupported_reason()
                    .is_some_and(|r| r.contains("Non-deterministic")),
                "not rejected: {sql}"
            );
        }
    }
}

#[test]
fn deterministic_lookalikes_are_not_rejected() {
    for sql in [
        "SELECT id, age(ts, '2020-01-01'::timestamp) AS a FROM t",
        "SELECT id, \"user\" FROM t",
        "SELECT t.user FROM t",
        "SELECT id, 'now()' AS s FROM t",
        "SELECT id, upper(g) AS u, length(g) AS l, concat(g, 'x') AS c FROM t",
        "SELECT id, date_trunc('day', ts) AS d FROM t",
    ] {
        assert!(
            !parse_and_analyze(sql)
                .unsupported_reason()
                .is_some_and(|r| r.contains("Non-deterministic")),
            "wrongly rejected: {sql}"
        );
    }
}

#[test]
fn timezone_sensitive_functions_are_listed() {
    let a = parse_and_analyze(
        "SELECT date_trunc('day', ts) AS d, to_char(ts, 'YYYY') AS y, upper(g) AS u FROM t",
    );
    assert_eq!(
        a.timezone_sensitive_functions(),
        vec!["date_trunc", "to_char"]
    );
    let fixed =
        parse_and_analyze("SELECT date_trunc('day', ts, 'UTC') AS d, length(g) AS l FROM t");
    assert!(fixed.timezone_sensitive_functions().is_empty());
}

#[test]
fn called_functions_exclude_window_calls() {
    let a =
        parse_and_analyze("SELECT id, row_number() OVER (ORDER BY id) AS rn, upper(g) AS u FROM t");
    let names: Vec<&str> = a.called_functions.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["upper"]);
}

#[test]
fn keyword_pseudo_functions_are_recorded_once_without_arguments() {
    let a = parse_and_analyze(
        "SELECT id, CURRENT_ROLE AS r, current_role AS r2, CURRENT_SCHEMA AS s FROM t",
    );
    let names: Vec<(&str, usize)> = a
        .called_functions
        .iter()
        .map(|f| (f.name.as_str(), f.arg_count))
        .collect();
    assert_eq!(names, vec![("current_role", 0), ("current_schema", 0)]);
}

#[test]
fn qualified_column_named_like_a_keyword_is_not_a_call() {
    let a = parse_and_analyze("SELECT t.user, t.id FROM t");
    assert!(a.called_functions.is_empty(), "{:?}", a.called_functions);
}

#[test]
fn called_functions_carry_bare_names_and_argument_counts() {
    let a = parse_and_analyze(
        "SELECT pg_catalog.upper(g) AS u, \"Lower\"(g) AS l, age(ts) AS a1, age(ts, ts) AS a2, upper(g) AS u2 FROM t",
    );
    let names: Vec<(&str, usize)> = a
        .called_functions
        .iter()
        .map(|f| (f.name.as_str(), f.arg_count))
        .collect();
    assert_eq!(
        names,
        vec![("upper", 1), ("lower", 1), ("age", 1), ("age", 2)]
    );
}

fn bare_name_of_call(call: &str) -> String {
    let parsed =
        Parser::parse_sql(&PostgreSqlDialect {}, &format!("SELECT {call}() FROM t")).unwrap();
    let Statement::Query(query) = &parsed[0] else {
        panic!("not a query")
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("not a select")
    };
    let SelectItem::UnnamedExpr(Expr::Function(f)) = &select.projection[0] else {
        panic!("not a call")
    };
    bare_function_name(&f.name)
}

#[test]
fn bare_function_name_is_the_last_name_part_lower_cased() {
    for (call, expected) in [
        ("\"a.b\"", "a.b"),
        ("s.\"a.b\"", "a.b"),
        ("pg_catalog.now", "now"),
        ("\"Now\"", "now"),
        ("\"s.x\".\"Sum\"", "sum"),
        ("upper", "upper"),
    ] {
        assert_eq!(bare_name_of_call(call), expected, "{call}");
    }
}

#[test]
fn extract_is_timezone_sensitive_without_being_a_called_function() {
    let a = parse_and_analyze("SELECT EXTRACT(dow FROM ts) AS d FROM t");
    assert_eq!(a.timezone_sensitive_functions(), vec!["extract"]);
    assert!(a.called_functions.is_empty(), "{:?}", a.called_functions);
    assert_eq!(a.unsupported_reason(), None);

    let both = parse_and_analyze(
        "SELECT g, SUM(v) AS s FROM t WHERE EXTRACT(hour FROM ts) > 3 AND to_char(ts, 'D') = '1' GROUP BY g",
    );
    assert_eq!(
        both.timezone_sensitive_functions(),
        vec!["extract", "to_char"]
    );

    let none = parse_and_analyze("SELECT id, upper(g) AS u FROM t");
    assert!(none.timezone_sensitive_functions().is_empty());
}

#[test]
fn backend_connection_and_transaction_identity_functions_rejected() {
    for call in [
        "SYSTEM_USER",
        "system_user",
        "pg_backend_pid()",
        "inet_client_addr()",
        "inet_client_port()",
        "inet_server_addr()",
        "inet_server_port()",
        "pg_current_xact_id_if_assigned()",
        "txid_current_if_assigned()",
        "pg_postmaster_start_time()",
        "pg_conf_load_time()",
        "pg_catalog.pg_backend_pid()",
    ] {
        for sql in [
            format!("SELECT id, {call} AS x FROM t"),
            format!("SELECT g, COUNT(*) AS c FROM t WHERE ts::text <> ({call})::text GROUP BY g"),
        ] {
            assert!(
                parse_and_analyze(&sql)
                    .unsupported_reason()
                    .is_some_and(|r| r.contains("Non-deterministic")),
                "not rejected: {sql}"
            );
        }
    }
    assert_eq!(
        parse_and_analyze("SELECT id, \"system_user\" FROM t").unsupported_reason(),
        None
    );
}

#[test]
fn every_keyword_pseudo_function_is_a_clock_or_session_function() {
    for keyword in KEYWORD_PSEUDO_FUNCTIONS {
        assert!(
            is_clock_or_session_function(&CalledFunction {
                name: keyword.to_string(),
                arg_count: 0,
            }),
            "{keyword}"
        );
    }
}

#[test]
fn bare_function_name_folds_ascii_only() {
    assert_eq!(bare_name_of_call("\"zz_ÉVOL\""), "zz_Évol");
    assert_eq!(bare_name_of_call("zz_ÈVol2"), "zz_Èvol2");
}

#[test]
fn nested_queries_are_detected_wherever_they_appear() {
    for sql in [
        "WITH c AS (SELECT id FROM t) SELECT id FROM c",
        "SELECT id FROM (SELECT id FROM t) d",
        "SELECT id FROM t WHERE v > (SELECT max(v) FROM u)",
        "SELECT id FROM t WHERE g IN (SELECT g FROM u)",
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
        "SELECT id, (SELECT max(v) FROM u) AS m FROM t",
    ] {
        assert!(parse_and_analyze(sql).has_nested_query, "{sql}");
    }
    for sql in [
        "SELECT id FROM t",
        "SELECT g, SUM(v) FROM t JOIN u ON u.id = t.id WHERE v > 1 GROUP BY g HAVING SUM(v) > 0",
        "SELECT id FROM t UNION ALL SELECT id FROM u",
    ] {
        assert!(!parse_and_analyze(sql).has_nested_query, "{sql}");
    }
}

#[test]
fn function_references_carry_the_schema_as_postgres_resolves_it() {
    let a = parse_and_analyze(
        "SELECT upper(g), pg_catalog.upper(g), S1.f(v), \"S1\".f(v), db.\"Sch\".\"F\"(v), s1.f(v, v) FROM t",
    );
    let references: Vec<(Option<&str>, &str)> = a
        .function_references
        .iter()
        .map(|r| (r.schema.as_deref(), r.name.as_str()))
        .collect();
    assert_eq!(
        references,
        vec![
            (None, "upper"),
            (Some("pg_catalog"), "upper"),
            (Some("s1"), "f"),
            (Some("S1"), "f"),
            (Some("Sch"), "f"),
        ]
    );
}

#[test]
fn keyword_pseudo_functions_are_unqualified_references() {
    let a = parse_and_analyze("SELECT id, CURRENT_DATE AS d FROM t");
    assert_eq!(
        a.function_references,
        vec![FunctionReference {
            schema: None,
            name: "current_date".to_string()
        }]
    );
}

#[test]
fn clock_names_match_only_unqualified_or_pg_catalog_calls() {
    for sql in [
        "SELECT id, now() AS x FROM t",
        "SELECT id, pg_catalog.now() AS x FROM t",
        "SELECT id, PG_CATALOG.NOW() AS x FROM t",
        "SELECT id, \"pg_catalog\".\"now\"() AS x FROM t",
        "SELECT id, db.pg_catalog.now() AS x FROM t",
        "SELECT id, CURRENT_TIMESTAMP AS x FROM t",
        "SELECT id, LOCALTIME AS x FROM t",
    ] {
        assert!(parse_and_analyze(sql).has_nondeterministic_select, "{sql}");
    }
    for sql in [
        "SELECT id, s1.now() AS x FROM t",
        "SELECT id, s1.age(v) AS x FROM t",
        "SELECT id, \"PG_CATALOG\".random() AS x FROM t",
    ] {
        let a = parse_and_analyze(sql);
        assert!(!a.has_nondeterministic_select, "{sql}");
        assert!(a.nondeterministic_functions.is_empty(), "{sql}");
        assert_eq!(a.function_references.len(), 1, "{sql}");
    }
}

#[test]
fn nondeterministic_rejection_names_each_matched_function_once() {
    let a = parse_and_analyze(
        "SELECT id, age(ts) AS a, now() AS n, pg_catalog.now() AS n2, s1.random() AS r FROM t",
    );
    assert_eq!(a.nondeterministic_functions, vec!["age", "now"]);
    assert!(a
        .unsupported_reason()
        .is_some_and(|r| r.contains("Fix: remove age, now from the IMV query")));
}

fn relevant_where(a: &SqlAnalysis, source: &str) -> String {
    a.imv_relevant_where
        .get(source)
        .cloned()
        .unwrap_or_default()
}

#[test]
fn columns_of_a_cross_source_conjunct_are_relevant() {
    let a = parse_and_analyze(
        "SELECT m.id, j.label FROM s m JOIN t j ON j.g = m.g WHERE m.v > j.w AND m.kind = 'x'",
    );
    assert_eq!(relevant_cols(&a, "s"), vec!["g", "id", "v"]);
    assert_eq!(relevant_cols(&a, "t"), vec!["g", "label", "w"]);
    assert_eq!(relevant_where(&a, "s"), "kind = 'x'");
}

#[test]
fn columns_of_an_unqualified_conjunct_in_a_join_are_relevant() {
    let a = parse_and_analyze("SELECT m.id FROM s m JOIN t j ON j.g = m.g WHERE flag = 1");
    assert!(relevant_cols(&a, "s").contains(&"flag".to_string()));
    assert!(relevant_cols(&a, "t").contains(&"flag".to_string()));
    assert!(a.imv_relevant_where.is_empty());
}

#[test]
fn correlated_subquery_conjunct_is_not_a_source_filter() {
    for query in [
        "SELECT m.id FROM s m WHERE NOT EXISTS (SELECT 1 FROM t x WHERE x.g = m.g) OR m.w IS NULL",
        "SELECT m.id FROM s m WHERE m.v > (SELECT count(*) FROM t x WHERE x.g = m.g)",
        "SELECT s.id FROM s WHERE NOT EXISTS (SELECT 1 FROM t WHERE t.g = s.g) OR s.w IS NULL",
    ] {
        let a = parse_and_analyze(query);
        assert!(a.imv_relevant_where.is_empty(), "{query}");
        let cols = relevant_cols(&a, "s");
        assert!(cols.contains(&"g".to_string()), "{query}");
    }
}

#[test]
fn columns_only_a_correlated_subquery_reads_are_relevant() {
    let a =
        parse_and_analyze("SELECT m.id FROM s m WHERE EXISTS (SELECT 1 FROM t x WHERE x.v = m.v)");
    assert_eq!(relevant_cols(&a, "s"), vec!["id", "v"]);
    assert!(a.imv_relevant_where.is_empty());
}

#[test]
fn uncorrelated_subquery_conjunct_stays_a_source_filter() {
    let a = parse_and_analyze(
        "SELECT id, qty FROM orders WHERE g IN (SELECT g FROM config) OR created_at >= (SELECT cutoff FROM config)",
    );
    assert_eq!(
        relevant_where(&a, "orders"),
        "g IN (SELECT g FROM config) OR created_at >= (SELECT cutoff FROM config)"
    );
    assert_eq!(relevant_cols(&a, "orders"), vec!["id", "qty"]);
}

#[test]
fn filter_on_a_null_extended_table_is_kept_only_when_the_where_rejects_null_rows() {
    let filter_of = |from: &str, condition: &str| {
        let a = parse_and_analyze(&format!(
            "SELECT d.kind, COUNT(*) FROM {from} WHERE {condition} GROUP BY d.kind"
        ));
        (relevant_where(&a, "fact"), relevant_cols(&a, "fact"))
    };
    let left = "dim d LEFT JOIN fact f ON d.kind = f.kind";
    for admitting in [
        "f.v > 0 OR f.v IS NULL",
        "f.kind IS NULL",
        "COALESCE(f.v, 1) > 0",
        "f.v IS DISTINCT FROM 1",
        "NOT (f.v > 0)",
        "f.v + 1 > 0",
        "f.v IS NOT TRUE",
    ] {
        let (filter, columns) = filter_of(left, admitting);
        assert_eq!(filter, "", "{admitting}");
        assert!(
            columns.contains(&"v".to_string()) || admitting.contains("f.kind"),
            "{admitting}"
        );
    }
    for rejecting in [
        "f.v > 0",
        "0 < f.v",
        "(f.v <> 3)",
        "f.v IN (1, 2)",
        "f.v NOT IN (1, 2)",
        "f.v BETWEEN 1 AND 9",
        "f.v IS NOT NULL",
        "f.v = d.w",
    ] {
        let (filter, _) = filter_of(left, &format!("{rejecting} AND abs(f.v) < 100"));
        assert!(filter.contains("abs(v) < 100"), "{rejecting}: {filter}");
    }
    for (from, null_extends_fact) in [
        ("dim d JOIN fact f ON d.kind = f.kind", false),
        ("fact f LEFT JOIN dim d ON d.kind = f.kind", false),
        ("fact f RIGHT JOIN dim d ON d.kind = f.kind", true),
        ("dim d RIGHT JOIN fact f ON d.kind = f.kind", false),
        ("dim d FULL JOIN fact f ON d.kind = f.kind", true),
        ("fact f FULL OUTER JOIN dim d ON d.kind = f.kind", true),
        (
            "fact f JOIN other o ON o.kind = f.kind RIGHT JOIN dim d ON d.kind = f.kind",
            true,
        ),
        ("dim d LEFT OUTER JOIN fact f ON d.kind = f.kind", true),
    ] {
        let (filter, columns) = filter_of(from, "f.v > 0 OR f.v IS NULL");
        assert_eq!(filter.is_empty(), null_extends_fact, "{from}");
        assert_eq!(
            columns.contains(&"v".to_string()),
            null_extends_fact,
            "{from}"
        );
    }
}

#[test]
fn filter_on_the_preserved_table_of_an_outer_join_is_kept() {
    let a = parse_and_analyze(
        "SELECT d.kind, COUNT(*) FROM dim d LEFT JOIN fact f ON d.kind = f.kind \
         WHERE d.label <> 'q' OR d.label IS NULL GROUP BY d.kind",
    );
    assert_eq!(relevant_where(&a, "dim"), "label <> 'q' OR label IS NULL");
    assert_eq!(relevant_cols(&a, "dim"), vec!["kind"]);
}

#[test]
fn null_rejection_resolves_a_qualifier_through_the_alias_that_hides_a_table_name() {
    let a = parse_and_analyze(
        "SELECT fact.kind, COUNT(*) FROM dim AS fact LEFT JOIN fact AS f ON fact.kind = f.kind \
         WHERE fact.kind > '' AND (f.v > 0 OR f.v IS NULL) GROUP BY fact.kind",
    );
    assert_eq!(relevant_where(&a, "fact"), "");
    assert!(relevant_cols(&a, "fact").contains(&"v".to_string()));
    assert_eq!(relevant_where(&a, "dim"), "kind > ''");

    let a = parse_and_analyze(
        "SELECT d.kind, COUNT(*) FROM dim d LEFT JOIN fact f ON d.kind = f.kind \
         WHERE fact.v > 0 AND abs(f.v) < 9 GROUP BY d.kind",
    );
    assert_eq!(relevant_where(&a, "fact"), "");
}

#[test]
fn null_rejection_accepts_the_name_of_an_unaliased_table_and_no_unqualified_column() {
    let filter_of = |condition: &str| {
        let a = parse_and_analyze(&format!(
            "SELECT d.kind, COUNT(*) FROM dim d LEFT JOIN s.fact ON d.kind = fact.kind \
             WHERE {condition} GROUP BY d.kind"
        ));
        relevant_where(&a, "s.fact")
    };
    assert!(filter_of("fact.v > 0 AND abs(fact.v) < 9").contains("abs(v) < 9"));
    assert_eq!(filter_of("v > 0 AND abs(fact.v) < 9"), "");
    assert_eq!(filter_of("d.kind > '' AND abs(fact.v) < 9"), "");
}
