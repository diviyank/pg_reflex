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
    let a =
        parse_and_analyze("SELECT city, COUNT(*) FROM emp WHERE active = true GROUP BY city");
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
    let a = parse_and_analyze(
        "SELECT city, MIN(salary), MAX(salary) FROM emp GROUP BY city",
    );
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
    assert!(a.unsupported_reason().is_none(), "Window functions should no longer be unsupported");
    assert!(a.has_window_function);
    // The window column should be flagged
    let win_col = a.select_columns.iter().find(|c| c.is_window);
    assert!(win_col.is_some(), "Should detect window function in SELECT");
}

#[test]
fn test_multiple_queries_error() {
    let parsed =
        Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 1; SELECT 2").unwrap();
    assert!(matches!(
        analyze(&parsed),
        Err(SqlAnalysisError::MultipleQueries(2))
    ));
}

#[test]
fn test_not_select_error() {
    let parsed =
        Parser::parse_sql(&PostgreSqlDialect {}, "CREATE TABLE t (id INT)").unwrap();
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
    let a = parse_and_analyze(
        "SELECT city, SUM(amount)::BIGINT AS total FROM orders GROUP BY city",
    );
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
    assert_eq!(a.select_columns[2].aggregate, Some(AggregateKind::CountStar));
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
    assert_eq!(a.table_aliases.get("p").map(String::as_str), Some("products"));
}

#[test]
fn test_table_aliases_schema_qualified() {
    let a = parse_and_analyze(
        "SELECT s.id FROM alp.sales_simulation s JOIN dim.products p ON s.product_id = p.id",
    );
    assert_eq!(a.table_aliases.get("s").map(String::as_str), Some("alp.sales_simulation"));
    assert_eq!(a.table_aliases.get("p").map(String::as_str), Some("dim.products"));
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
    let a = parse_and_analyze(
        "SELECT DISTINCT ON (city) city, val FROM t",
    );
    assert!(a.has_distinct_on);
    assert!(!a.has_distinct, "DISTINCT ON should not set has_distinct");
    assert!(a.unsupported_reason().is_some());
    assert!(a.unsupported_reason().unwrap().contains("DISTINCT ON"));
}

#[test]
fn test_detect_grouping_sets() {
    let a = parse_and_analyze(
        "SELECT city, SUM(val) FROM t GROUP BY GROUPING SETS ((city), ())",
    );
    assert!(a.has_grouping_sets);
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_detect_cube() {
    let a = parse_and_analyze(
        "SELECT city, state, SUM(val) FROM t GROUP BY CUBE (city, state)",
    );
    assert!(a.has_grouping_sets);
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_detect_rollup() {
    let a = parse_and_analyze(
        "SELECT city, SUM(val) FROM t GROUP BY ROLLUP (city)",
    );
    assert!(a.has_grouping_sets);
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_detect_filter_clause() {
    let a = parse_and_analyze(
        "SELECT city, COUNT(*) FILTER (WHERE active) FROM t GROUP BY city",
    );
    assert!(a.has_filter_clause);
    assert!(a.unsupported_reason().is_some());
    assert!(a.unsupported_reason().unwrap().contains("FILTER"));
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
    let a = parse_and_analyze(
        "SELECT * FROM t TABLESAMPLE BERNOULLI (10)",
    );
    assert!(a.has_tablesample);
    assert!(a.unsupported_reason().is_some());
    assert!(a.unsupported_reason().unwrap().contains("TABLESAMPLE"));
}

#[test]
fn test_detect_nondeterministic_select() {
    let a = parse_and_analyze(
        "SELECT NOW(), city FROM t GROUP BY city",
    );
    assert!(a.has_nondeterministic_select);
    assert!(a.unsupported_reason().is_some());

    let b = parse_and_analyze(
        "SELECT RANDOM(), id FROM t",
    );
    assert!(b.has_nondeterministic_select);
}

#[test]
fn test_detect_unsupported_aggregate_string_agg() {
    let a = parse_and_analyze(
        "SELECT city, STRING_AGG(name, ', ') FROM t GROUP BY city",
    );
    assert!(!a.unsupported_aggregates.is_empty());
    assert!(a.unsupported_aggregates.contains(&"STRING_AGG".to_string()));
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_detect_unsupported_aggregate_array_agg() {
    let a = parse_and_analyze(
        "SELECT city, ARRAY_AGG(val) FROM t GROUP BY city",
    );
    assert!(a.unsupported_aggregates.contains(&"ARRAY_AGG".to_string()));
}

#[test]
fn test_detect_unsupported_aggregate_stddev() {
    let a = parse_and_analyze(
        "SELECT city, STDDEV(val) FROM t GROUP BY city",
    );
    assert!(a.unsupported_aggregates.contains(&"STDDEV".to_string()));
}

#[test]
fn test_detect_scalar_subquery() {
    let a = parse_and_analyze(
        "SELECT (SELECT MAX(x) FROM t2), city FROM t GROUP BY city",
    );
    assert!(a.has_scalar_subquery);
    assert!(a.unsupported_reason().is_some());
}

#[test]
fn test_supported_aggregates_not_flagged() {
    let a = parse_and_analyze(
        "SELECT city, SUM(val), COUNT(*), AVG(val), MIN(val), MAX(val), BOOL_OR(flag) \
         FROM t GROUP BY city",
    );
    assert!(a.unsupported_aggregates.is_empty(),
        "Supported aggregates should not be flagged: {:?}", a.unsupported_aggregates);
    assert!(!a.has_filter_clause);
    assert!(!a.has_within_group);
    assert!(!a.has_nondeterministic_select);
    assert!(a.unsupported_reason().is_none(),
        "Query with only supported features should pass: {:?}", a.unsupported_reason());
}

#[test]
fn test_regular_functions_not_flagged_as_aggregates() {
    // UPPER, LOWER, COALESCE etc. are scalar functions, not aggregates
    let a = parse_and_analyze(
        "SELECT UPPER(name), COALESCE(val, 0) FROM t",
    );
    assert!(a.unsupported_aggregates.is_empty(),
        "Regular scalar functions should not be flagged: {:?}", a.unsupported_aggregates);
}

#[test]
fn test_multiple_unsupported_aggregates() {
    let a = parse_and_analyze(
        "SELECT city, STRING_AGG(name, ','), ARRAY_AGG(val), STDDEV(val) FROM t GROUP BY city",
    );
    assert_eq!(a.unsupported_aggregates.len(), 3);
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
