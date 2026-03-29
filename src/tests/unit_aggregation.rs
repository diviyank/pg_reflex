use super::*;
use crate::sql_analyzer::analyze;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn plan_from_sql(sql: &str) -> AggregationPlan {
    let parsed = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    let analysis = analyze(&parsed).unwrap();
    plan_aggregation(&analysis)
}

#[test]
fn test_sum_single_column() {
    let plan = plan_from_sql("SELECT city, SUM(amount) FROM orders GROUP BY city");
    assert_eq!(plan.group_by_columns, vec!["city"]);
    // SUM produces 2 intermediate columns: __sum_amount + __nonnull_count_amount
    assert_eq!(plan.intermediate_columns.len(), 2);
    assert_eq!(plan.intermediate_columns[0].name, "__sum_amount");
    assert_eq!(plan.intermediate_columns[0].source_aggregate, "SUM");
    assert_eq!(plan.intermediate_columns[1].name, "__nonnull_count_amount");
    assert_eq!(plan.intermediate_columns[1].source_aggregate, "COUNT");
    assert_eq!(plan.end_query_mappings.len(), 1);
    // End query uses CASE WHEN non-null count > 0 THEN sum END
    assert!(plan.end_query_mappings[0].intermediate_expr.contains("CASE WHEN"));
}

#[test]
fn test_avg_produces_sum_and_count() {
    let plan = plan_from_sql("SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept");
    assert_eq!(plan.group_by_columns, vec!["dept"]);
    // AVG produces 2 intermediate columns: __sum_salary and __count_salary
    assert_eq!(plan.intermediate_columns.len(), 2);
    assert_eq!(plan.intermediate_columns[0].name, "__sum_salary");
    assert_eq!(plan.intermediate_columns[0].source_aggregate, "SUM");
    assert_eq!(plan.intermediate_columns[1].name, "__count_salary");
    assert_eq!(plan.intermediate_columns[1].source_aggregate, "COUNT");
    // End query expression uses division
    assert_eq!(plan.end_query_mappings.len(), 1);
    assert!(plan.end_query_mappings[0]
        .intermediate_expr
        .contains("NULLIF"));
    assert_eq!(plan.end_query_mappings[0].output_alias, "avg_sal");
}

#[test]
fn test_distinct_produces_ivm_count() {
    let plan = plan_from_sql("SELECT DISTINCT country FROM orders");
    assert!(plan.has_distinct);
    assert!(plan.needs_ivm_count);
    // DISTINCT with no aggregates: only __ivm_count in intermediate
    assert_eq!(plan.intermediate_columns.len(), 0);
}

#[test]
fn test_multiple_aggregates_plan() {
    let plan = plan_from_sql(
        "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt, MAX(price) AS max_p FROM orders GROUP BY city",
    );
    assert_eq!(plan.group_by_columns, vec!["city"]);
    // SUM -> 2 cols (sum + nonnull_count), COUNT(*) -> 1 col, MAX -> 1 col = 4 intermediate columns
    assert_eq!(plan.intermediate_columns.len(), 4);
    assert_eq!(plan.end_query_mappings.len(), 3);
    assert_eq!(plan.end_query_mappings[0].output_alias, "total");
    assert_eq!(plan.end_query_mappings[1].output_alias, "cnt");
    assert_eq!(plan.end_query_mappings[2].output_alias, "max_p");
}

#[test]
fn test_count_star_plan() {
    let plan = plan_from_sql("SELECT city, COUNT(*) FROM emp GROUP BY city");
    assert_eq!(plan.intermediate_columns.len(), 1);
    assert_eq!(plan.intermediate_columns[0].name, "__count_star");
    assert_eq!(plan.intermediate_columns[0].pg_type, "BIGINT");
}

#[test]
fn test_min_max_plan() {
    let plan =
        plan_from_sql("SELECT city, MIN(salary) AS lo, MAX(salary) AS hi FROM emp GROUP BY city");
    assert_eq!(plan.intermediate_columns.len(), 2);
    assert_eq!(plan.intermediate_columns[0].name, "__min_salary");
    assert_eq!(plan.intermediate_columns[0].source_aggregate, "MIN");
    assert_eq!(plan.intermediate_columns[1].name, "__max_salary");
    assert_eq!(plan.intermediate_columns[1].source_aggregate, "MAX");
}

mod proptest_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// AVG always produces both SUM and COUNT intermediate columns
        #[test]
        fn avg_always_produces_sum_and_count(suffix in "[a-z]{1,10}") {
            let col = format!("col_{}", suffix);
            let sql = format!(
                "SELECT grp, AVG({}) AS avg_val FROM tbl GROUP BY grp",
                col
            );
            let plan = plan_from_sql(&sql);
            let has_sum = plan.intermediate_columns.iter().any(|ic| {
                ic.source_aggregate == "SUM" && ic.source_arg == col
            });
            let has_count = plan.intermediate_columns.iter().any(|ic| {
                ic.source_aggregate == "COUNT" && ic.source_arg == col
            });
            assert!(has_sum, "AVG({}) must produce SUM intermediate column", col);
            assert!(has_count, "AVG({}) must produce COUNT intermediate column", col);
        }

        /// Every supported aggregate produces at least one intermediate column
        #[test]
        fn every_aggregate_produces_intermediate(
            agg_kind in prop_oneof![
                Just(("SUM", "SUM(val)")),
                Just(("COUNT", "COUNT(val)")),
                Just(("COUNT", "COUNT(*)")),
                Just(("MIN", "MIN(val)")),
                Just(("MAX", "MAX(val)")),
                Just(("BOOL_OR", "BOOL_OR(flag)")),
            ],
        ) {
            let sql = format!(
                "SELECT grp, {} AS agg_val FROM tbl GROUP BY grp",
                agg_kind.1
            );
            let plan = plan_from_sql(&sql);
            prop_assert!(!plan.intermediate_columns.is_empty(),
                "{} should produce intermediate columns", agg_kind.0);
        }

        /// Multiple aggregates produce at least as many intermediate columns
        #[test]
        fn multiple_aggregates_produce_multiple_intermediates(
            suffix in "[a-z]{1,5}",
        ) {
            let col = format!("v_{}", suffix);
            let sql = format!(
                "SELECT grp, SUM({col}) AS s, COUNT({col}) AS c, MIN({col}) AS lo, MAX({col}) AS hi FROM tbl GROUP BY grp",
                col = col,
            );
            let plan = plan_from_sql(&sql);
            // SUM + COUNT + MIN + MAX = at least 4 intermediate columns
            prop_assert!(plan.intermediate_columns.len() >= 4,
                "4 aggregates should produce >= 4 intermediates, got {}", plan.intermediate_columns.len());
        }

        /// Passthrough queries (no GROUP BY, no aggregates) have no intermediate columns
        #[test]
        fn passthrough_has_no_intermediates(suffix in "[a-z]{1,5}") {
            let col = format!("col_{}", suffix);
            let sql = format!(
                "SELECT {}, id FROM tbl",
                col
            );
            let plan = plan_from_sql(&sql);
            prop_assert!(plan.is_passthrough,
                "Query without GROUP BY or aggregates should be passthrough");
            prop_assert!(plan.intermediate_columns.is_empty(),
                "Passthrough should have no intermediate columns");
        }
    }
}
