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
    assert!(plan.end_query_mappings[0]
        .intermediate_expr
        .contains("CASE WHEN"));
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

// ========================================================================
// Bug fix tests: EXTRACT expression auto-added to GROUP BY
// ========================================================================

#[test]
fn test_extract_auto_added_to_group_by() {
    // EXTRACT(WEEK FROM d) is not in GROUP BY but d is — should be auto-added
    let plan = plan_from_sql(
        "SELECT d, EXTRACT(WEEK FROM d) AS week, EXTRACT(ISOYEAR FROM d) AS isoyear, SUM(qty) FROM t GROUP BY d",
    );
    assert!(
        plan.group_by_columns
            .contains(&"EXTRACT(WEEK FROM d)".to_string()),
        "EXTRACT(WEEK FROM d) should be auto-added to group_by_columns: {:?}",
        plan.group_by_columns
    );
    assert!(
        plan.group_by_columns
            .contains(&"EXTRACT(ISOYEAR FROM d)".to_string()),
        "EXTRACT(ISOYEAR FROM d) should be auto-added to group_by_columns: {:?}",
        plan.group_by_columns
    );
}

#[test]
fn test_explicit_group_by_not_duplicated() {
    // When EXTRACT is already in GROUP BY, it should not be added twice
    let plan = plan_from_sql(
        "SELECT EXTRACT(MONTH FROM d) AS month, SUM(qty) FROM t GROUP BY EXTRACT(MONTH FROM d)",
    );
    let count = plan
        .group_by_columns
        .iter()
        .filter(|c| c.contains("EXTRACT"))
        .count();
    assert_eq!(
        count, 1,
        "Should not duplicate GROUP BY entries: {:?}",
        plan.group_by_columns
    );
}

// ========================================================================
// Bug fix tests: CASE+aggregate derived expressions
// ========================================================================

#[test]
fn test_case_sum_produces_intermediate_columns() {
    let plan = plan_from_sql(
        "SELECT grp, CASE WHEN SUM(x) = 0 THEN 0 ELSE SUM(x) END AS val FROM t GROUP BY grp",
    );
    // Should have intermediate columns for SUM(x)
    assert!(
        plan.intermediate_columns
            .iter()
            .any(|ic| ic.source_aggregate == "SUM" && ic.source_arg == "x"),
        "CASE+SUM should produce SUM intermediate column: {:?}",
        plan.intermediate_columns
    );
    // Should have an end_query_mapping with DERIVED type
    assert!(
        plan.end_query_mappings
            .iter()
            .any(|m| m.aggregate_type == "DERIVED"),
        "CASE+SUM should produce a DERIVED end_query_mapping: {:?}",
        plan.end_query_mappings
    );
}

#[test]
fn test_case_sum_end_query_references_intermediate() {
    let plan = plan_from_sql(
        "SELECT grp, CASE WHEN SUM(a) = 0 THEN 0 ELSE SUM(a) / SUM(b) END AS ratio FROM t GROUP BY grp",
    );
    let derived = plan
        .end_query_mappings
        .iter()
        .find(|m| m.aggregate_type == "DERIVED")
        .expect("Should have DERIVED mapping");
    assert!(
        derived.intermediate_expr.contains("__sum_a"),
        "Derived expr should reference __sum_a: {}",
        derived.intermediate_expr
    );
    assert!(
        derived.intermediate_expr.contains("__sum_b"),
        "Derived expr should reference __sum_b: {}",
        derived.intermediate_expr
    );
    assert_eq!(derived.output_alias, "ratio");
}

// Bug #2: aggregates appearing only in HAVING (not in SELECT) must still
// produce intermediate columns so DELETE can recompute MIN/MAX/BOOL_OR
// and HAVING evaluates against fresh state, not stale pre-delete values.
#[test]
fn test_having_only_max_creates_intermediate_column() {
    let plan =
        plan_from_sql("SELECT grp, SUM(x) AS total FROM t GROUP BY grp HAVING MAX(amount) > 100");
    let has_max = plan
        .intermediate_columns
        .iter()
        .any(|ic| ic.source_aggregate == "MAX" && ic.source_arg == "amount");
    assert!(
        has_max,
        "HAVING-only MAX(amount) must add __max_amount to intermediate_columns. \
         Got: {:?}",
        plan.intermediate_columns
            .iter()
            .map(|ic| (&ic.name, &ic.source_aggregate))
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_having_only_min_creates_intermediate_column() {
    let plan = plan_from_sql("SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING MIN(price) < 10");
    let has_min = plan
        .intermediate_columns
        .iter()
        .any(|ic| ic.source_aggregate == "MIN" && ic.source_arg == "price");
    assert!(has_min, "HAVING-only MIN(price) must add __min_price");
}

#[test]
fn test_having_only_bool_or_creates_intermediate_column() {
    let plan = plan_from_sql("SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING BOOL_OR(active)");
    // Algebraic BOOL_OR: two BIGINT SUM counter columns instead of one BOOLEAN column.
    let has_true_count = plan
        .intermediate_columns
        .iter()
        .any(|ic| ic.name == "__bool_or_active_true_count" && ic.source_aggregate == "SUM");
    let has_nonnull_count = plan
        .intermediate_columns
        .iter()
        .any(|ic| ic.name == "__bool_or_active_nonnull_count" && ic.source_aggregate == "SUM");
    assert!(
        has_true_count,
        "HAVING BOOL_OR(active) must add __bool_or_active_true_count (SUM BIGINT): {:?}",
        plan.intermediate_columns
            .iter()
            .map(|ic| (&ic.name, &ic.source_aggregate))
            .collect::<Vec<_>>()
    );
    assert!(
        has_nonnull_count,
        "HAVING BOOL_OR(active) must add __bool_or_active_nonnull_count (SUM BIGINT): {:?}",
        plan.intermediate_columns
            .iter()
            .map(|ic| (&ic.name, &ic.source_aggregate))
            .collect::<Vec<_>>()
    );
    let has_raw = plan
        .intermediate_columns
        .iter()
        .any(|ic| ic.source_aggregate == "BOOL_OR");
    assert!(
        !has_raw,
        "algebraic BOOL_OR must not produce a raw BOOL_OR intermediate column"
    );
}

#[test]
fn test_plan_bool_or_emits_two_counter_columns() {
    let plan = plan_from_sql("SELECT grp, BOOL_OR(flag) AS has_any FROM t GROUP BY grp");
    let true_col = plan
        .intermediate_columns
        .iter()
        .find(|ic| ic.name == "__bool_or_flag_true_count");
    let nonnull_col = plan
        .intermediate_columns
        .iter()
        .find(|ic| ic.name == "__bool_or_flag_nonnull_count");
    assert!(
        true_col.is_some(),
        "BOOL_OR must emit __bool_or_flag_true_count: {:?}",
        plan.intermediate_columns
    );
    assert!(
        nonnull_col.is_some(),
        "BOOL_OR must emit __bool_or_flag_nonnull_count: {:?}",
        plan.intermediate_columns
    );
    assert_eq!(
        true_col.unwrap().pg_type,
        "BIGINT",
        "true_count must be BIGINT"
    );
    assert_eq!(
        nonnull_col.unwrap().pg_type,
        "BIGINT",
        "nonnull_count must be BIGINT"
    );
    assert_eq!(
        true_col.unwrap().source_aggregate,
        "SUM",
        "true_count uses SUM aggregate"
    );
    assert_eq!(
        nonnull_col.unwrap().source_aggregate,
        "SUM",
        "nonnull_count uses SUM aggregate"
    );
    assert_eq!(plan.end_query_mappings.len(), 1);
    assert_eq!(plan.end_query_mappings[0].aggregate_type, "BOOL_OR");
    assert_eq!(plan.end_query_mappings[0].output_alias, "has_any");
}

#[test]
fn test_plan_bool_or_end_query_mapping_uses_case_expression() {
    let plan = plan_from_sql("SELECT grp, BOOL_OR(flag) AS has_any FROM t GROUP BY grp");
    let mapping = &plan.end_query_mappings[0];
    assert!(
        mapping.intermediate_expr.contains("CASE WHEN"),
        "BOOL_OR end query must use CASE expression: {}",
        mapping.intermediate_expr
    );
    assert!(
        mapping
            .intermediate_expr
            .contains("__bool_or_flag_nonnull_count"),
        "CASE expression must reference nonnull_count: {}",
        mapping.intermediate_expr
    );
    assert!(
        mapping
            .intermediate_expr
            .contains("__bool_or_flag_true_count"),
        "CASE expression must reference true_count: {}",
        mapping.intermediate_expr
    );
}

#[test]
fn test_plan_bool_or_no_raw_bool_or_aggregate() {
    let plan = plan_from_sql("SELECT grp, BOOL_OR(flag) FROM t GROUP BY grp");
    assert!(
        !plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.source_aggregate == "BOOL_OR"),
        "BOOL_OR must not produce a raw BOOL_OR aggregate column (algebraic only): {:?}",
        plan.intermediate_columns
    );
}

#[test]
fn test_having_only_max_is_recomputed_on_delete() {
    // This is the actual correctness assertion: build_min_max_recompute_sql
    // must include the HAVING-only MAX column in its SET list, so DELETE
    // that removes the current max triggers a fresh rescan.
    use crate::trigger::build_min_max_recompute_sql;
    let plan =
        plan_from_sql("SELECT grp, SUM(x) AS total FROM t GROUP BY grp HAVING MAX(amount) > 100");
    let orig_base = "SELECT grp AS \"grp\", SUM(x) AS \"__sum_x\", MAX(amount) AS \"__max_amount\", COUNT(*) AS __ivm_count FROM t GROUP BY grp";
    let sql = build_min_max_recompute_sql("__reflex_intermediate_v", &plan, orig_base, None);
    let sql = sql.expect("HAVING-only MAX must produce a recompute SQL");
    assert!(
        sql.contains("\"__max_amount\" = __src.\"__max_amount\""),
        "HAVING-only MAX(amount) must be recomputed on delete: {}",
        sql
    );
}

// ========================================================================
// 1.4.6 — Item 5: drop redundant __nonnull_count intermediate columns
// ========================================================================
// Cases handled by `optimize_not_null_sums`:
//   (a) SUM(bare_col) where bare_col is NOT NULL in source (existing).
//   (b) BOOL_OR(X) where X is structurally non-null — e.g. `X IS NOT NULL`,
//       `X IS NULL`. The `__bool_or_*_nonnull_count` is always == __ivm_count.
//   (c) SUM(Y * COALESCE(Z, non_null_lit)) where SUM(Y) is also tracked —
//       same nullability profile as Y, so the new nonnull_count duplicates
//       SUM(Y)'s nonnull_count. End-query refs are redirected; the duplicate
//       column is dropped.

use crate::aggregation::{AggregationPlan, EndQueryMapping, IntermediateColumn};

fn plan_with_bool_or_of_is_not_null() -> AggregationPlan {
    AggregationPlan {
        group_by_columns: vec!["grp".to_string()],
        intermediate_columns: vec![
            IntermediateColumn {
                name: "__bool_or_x_is_not_null_true_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (x IS NOT NULL) THEN 1 ELSE 0 END".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__bool_or_x_is_not_null_nonnull_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (x IS NOT NULL) IS NOT NULL THEN 1 ELSE 0 END".to_string(),
                topk_k: None,
            },
        ],
        end_query_mappings: vec![EndQueryMapping {
            intermediate_expr:
                "CASE WHEN \"__bool_or_x_is_not_null_nonnull_count\" > 0 THEN \"__bool_or_x_is_not_null_true_count\" > 0 ELSE NULL END"
                    .to_string(),
            output_alias: "has_x".to_string(),
            aggregate_type: "BOOL_OR".to_string(),
            cast_type: None,
        }],
        has_distinct: false,
        needs_ivm_count: true,
        distinct_columns: vec![],
        is_passthrough: false,
        passthrough_columns: vec![],
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: None,
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases: std::collections::HashMap::new(),
        output_column_order: vec![],
        imv_relevant_columns: std::collections::HashMap::new(),
        imv_relevant_where: std::collections::HashMap::new(),
        source_join_keys: std::collections::HashMap::new(),
    }
}

#[test]
fn test_optimize_drops_bool_or_nonnull_count_when_inner_is_is_not_null() {
    let mut plan = plan_with_bool_or_of_is_not_null();
    plan.optimize_not_null_sums(&std::collections::HashSet::new());

    assert!(
        !plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.name == "__bool_or_x_is_not_null_nonnull_count"),
        "BOOL_OR's nonnull_count must be dropped when inner is `X IS NOT NULL`: {:?}",
        plan.intermediate_columns
    );
    assert!(
        plan.intermediate_columns
            .iter()
            .any(|ic| ic.name == "__bool_or_x_is_not_null_true_count"),
        "BOOL_OR's true_count must be kept"
    );
    let mapping = &plan.end_query_mappings[0];
    assert!(
        !mapping
            .intermediate_expr
            .contains("__bool_or_x_is_not_null_nonnull_count"),
        "end_query must no longer reference the dropped nonnull_count: {}",
        mapping.intermediate_expr
    );
    assert!(
        mapping
            .intermediate_expr
            .contains("__bool_or_x_is_not_null_true_count"),
        "end_query must still reference true_count: {}",
        mapping.intermediate_expr
    );
    assert!(
        !mapping.intermediate_expr.contains("CASE WHEN"),
        "end_query CASE must be flattened to just `true_count > 0`: {}",
        mapping.intermediate_expr
    );
}

#[test]
fn test_optimize_keeps_bool_or_nonnull_count_when_inner_is_arbitrary() {
    // BOOL_OR(flag) — flag is a plain column that may itself be NULL.
    // The nonnull_count is needed to distinguish "all-null" from "all-false".
    let mut plan = plan_with_bool_or_of_is_not_null();
    plan.intermediate_columns[0].source_arg = "CASE WHEN (flag) THEN 1 ELSE 0 END".to_string();
    plan.intermediate_columns[1].source_arg =
        "CASE WHEN (flag) IS NOT NULL THEN 1 ELSE 0 END".to_string();
    plan.optimize_not_null_sums(&std::collections::HashSet::new());

    assert!(
        plan.intermediate_columns
            .iter()
            .any(|ic| ic.name == "__bool_or_x_is_not_null_nonnull_count"),
        "nullable inner arg must NOT drop nonnull_count: {:?}",
        plan.intermediate_columns
    );
}

fn plan_with_sum_and_multiplied_coalesce() -> AggregationPlan {
    AggregationPlan {
        group_by_columns: vec!["grp".to_string()],
        intermediate_columns: vec![
            IntermediateColumn {
                name: "__sum_qty".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "qty".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__nonnull_count_qty".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "COUNT".to_string(),
                source_arg: "qty".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__sum_qty_coalesce_price_0".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "qty * COALESCE(price, 0)".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__nonnull_count_qty_coalesce_price_0".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "COUNT".to_string(),
                source_arg: "qty * COALESCE(price, 0)".to_string(),
                topk_k: None,
            },
        ],
        end_query_mappings: vec![
            EndQueryMapping {
                intermediate_expr:
                    "CASE WHEN \"__nonnull_count_qty\" > 0 THEN \"__sum_qty\" END".to_string(),
                output_alias: "total_qty".to_string(),
                aggregate_type: "SUM".to_string(),
                cast_type: None,
            },
            EndQueryMapping {
                intermediate_expr:
                    "CASE WHEN \"__nonnull_count_qty_coalesce_price_0\" > 0 THEN \"__sum_qty_coalesce_price_0\" END"
                        .to_string(),
                output_alias: "turnover".to_string(),
                aggregate_type: "SUM".to_string(),
                cast_type: None,
            },
        ],
        has_distinct: false,
        needs_ivm_count: true,
        distinct_columns: vec![],
        is_passthrough: false,
        passthrough_columns: vec![],
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: None,
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases: std::collections::HashMap::new(),
        output_column_order: vec![],
        imv_relevant_columns: std::collections::HashMap::new(),
        imv_relevant_where: std::collections::HashMap::new(),
        source_join_keys: std::collections::HashMap::new(),
    }
}

#[test]
fn test_optimize_dedups_nonnull_count_for_multiplied_coalesce() {
    let mut plan = plan_with_sum_and_multiplied_coalesce();
    plan.optimize_not_null_sums(&std::collections::HashSet::new());

    // The duplicate nonnull_count for `qty * COALESCE(price, 0)` must be dropped.
    assert!(
        !plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.name == "__nonnull_count_qty_coalesce_price_0"),
        "duplicate nonnull_count for `qty * COALESCE(price, 0)` must be dropped: {:?}",
        plan.intermediate_columns
    );
    // The canonical nonnull_count for plain `qty` is kept.
    assert!(
        plan.intermediate_columns
            .iter()
            .any(|ic| ic.name == "__nonnull_count_qty"),
        "canonical nonnull_count for qty must be kept"
    );
    // Both __sum_* columns are kept (they hold different values).
    assert!(
        plan.intermediate_columns
            .iter()
            .any(|ic| ic.name == "__sum_qty"),
        "__sum_qty must be kept"
    );
    assert!(
        plan.intermediate_columns
            .iter()
            .any(|ic| ic.name == "__sum_qty_coalesce_price_0"),
        "__sum_qty_coalesce_price_0 must be kept (different aggregate value)"
    );
    // The end_query mapping for `turnover` is rewritten to use the canonical nonnull_count.
    let turnover = plan
        .end_query_mappings
        .iter()
        .find(|m| m.output_alias == "turnover")
        .expect("turnover mapping must remain");
    assert!(
        !turnover
            .intermediate_expr
            .contains("__nonnull_count_qty_coalesce_price_0"),
        "end_query must no longer reference the dropped column: {}",
        turnover.intermediate_expr
    );
    assert!(
        turnover.intermediate_expr.contains("__nonnull_count_qty"),
        "end_query for `turnover` must redirect to the canonical __nonnull_count_qty: {}",
        turnover.intermediate_expr
    );
}

#[test]
fn test_optimize_dedups_propagates_when_left_is_not_null() {
    // SUM(qty) where qty is NOT NULL → SUM(qty)'s nonnull_count is dropped
    // (existing behavior). For SUM(qty * COALESCE(price, 0)), the
    // nullability profile == qty's, which is NOT NULL. So the multiplier's
    // nonnull_count is ALSO dropped (no need to redirect since it's
    // unconditionally true).
    let mut plan = plan_with_sum_and_multiplied_coalesce();
    let mut not_null = std::collections::HashSet::new();
    not_null.insert("qty".to_string());
    plan.optimize_not_null_sums(&not_null);

    assert!(
        !plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.name == "__nonnull_count_qty"),
        "naked qty nonnull_count must be dropped (qty is NOT NULL): {:?}",
        plan.intermediate_columns
    );
    assert!(
        !plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.name == "__nonnull_count_qty_coalesce_price_0"),
        "multiplied nonnull_count must also be dropped (qty is NOT NULL): {:?}",
        plan.intermediate_columns
    );
    let turnover = plan
        .end_query_mappings
        .iter()
        .find(|m| m.output_alias == "turnover")
        .unwrap();
    assert!(
        !turnover.intermediate_expr.contains("CASE WHEN"),
        "end_query CASE must be flattened to just the sum reference: {}",
        turnover.intermediate_expr
    );
    assert!(
        turnover
            .intermediate_expr
            .contains("__sum_qty_coalesce_price_0"),
        "end_query must reference __sum_qty_coalesce_price_0 directly: {}",
        turnover.intermediate_expr
    );
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
