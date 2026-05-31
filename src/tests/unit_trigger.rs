use super::*;
use crate::aggregation::{AggregationPlan, EndQueryMapping, IntermediateColumn};
use crate::schema_builder::build_trigger_ddls;

fn simple_plan() -> AggregationPlan {
    AggregationPlan {
        group_by_columns: vec!["city".to_string()],
        intermediate_columns: vec![IntermediateColumn {
            name: "__sum_amount".to_string(),
            pg_type: "NUMERIC".to_string(),
            source_aggregate: "SUM".to_string(),
            source_arg: "amount".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![EndQueryMapping {
            intermediate_expr: "__sum_amount".to_string(),
            output_alias: "total".to_string(),
            aggregate_type: "SUM".to_string(),
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    }
}

#[test]
fn test_build_merge_add() {
    let plan = simple_plan();
    let delta = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"__reflex_new_v\" GROUP BY city";
    let sql = build_merge_sql("__reflex_intermediate_v", delta, &plan, DeltaOp::Add);
    assert!(sql.contains("MERGE INTO __reflex_intermediate_v AS t"));
    assert!(sql.contains("t.\"city\" IS NOT DISTINCT FROM d.\"city\""));
    assert!(sql.contains("COALESCE(t.\"__sum_amount\", 0) + COALESCE(d.\"__sum_amount\", 0)"));
    assert!(sql.contains("COALESCE(t.__ivm_count, 0) + COALESCE(d.__ivm_count, 0)"));
    assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
}

#[test]
fn test_build_merge_subtract() {
    let plan = simple_plan();
    let delta = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"__reflex_old_v\" GROUP BY city";
    let sql = build_merge_sql("__reflex_intermediate_v", delta, &plan, DeltaOp::Subtract);
    assert!(sql.contains("COALESCE(t.\"__sum_amount\", 0) - COALESCE(d.\"__sum_amount\", 0)"));
    assert!(sql.contains("COALESCE(t.__ivm_count, 0) - COALESCE(d.__ivm_count, 0)"));
    // Subtract should NOT have WHEN NOT MATCHED
    assert!(!sql.contains("WHEN NOT MATCHED"));
}

#[test]
fn test_build_merge_min_add() {
    let plan = AggregationPlan {
        group_by_columns: vec!["city".to_string()],
        intermediate_columns: vec![IntermediateColumn {
            name: "__min_price".to_string(),
            pg_type: "NUMERIC".to_string(),
            source_aggregate: "MIN".to_string(),
            source_arg: "price".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let delta = "SELECT city, MIN(price) AS \"__min_price\", COUNT(*) AS __ivm_count FROM src GROUP BY city";
    let sql = build_merge_sql("intermediate", delta, &plan, DeltaOp::Add);
    assert!(sql.contains("LEAST(t.\"__min_price\", d.\"__min_price\")"));
}

#[test]
fn test_build_upsert_min_subtract_sets_null() {
    let plan = AggregationPlan {
        group_by_columns: vec!["city".to_string()],
        intermediate_columns: vec![IntermediateColumn {
            name: "__min_price".to_string(),
            pg_type: "NUMERIC".to_string(),
            source_aggregate: "MIN".to_string(),
            source_arg: "price".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let delta = "SELECT city, MIN(price) FROM src GROUP BY city";
    let sql = build_merge_sql("intermediate", delta, &plan, DeltaOp::Subtract);
    assert!(sql.contains("\"__min_price\" = NULL"));
}

#[test]
fn test_min_max_recompute_sql() {
    let plan = AggregationPlan {
        group_by_columns: vec!["city".to_string()],
        intermediate_columns: vec![
            IntermediateColumn {
                name: "__min_price".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "MIN".to_string(),
                source_arg: "price".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__sum_amount".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "amount".to_string(),
                topk_k: None,
            },
        ],
        end_query_mappings: vec![],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let orig_base = "SELECT city AS \"city\", MIN(price) AS \"__min_price\", SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let sql = build_min_max_recompute_sql("intermediate", &plan, orig_base, None);
    assert!(sql.is_some());
    let sql = sql.unwrap();
    assert!(
        sql.contains("UPDATE intermediate"),
        "UPDATE target: {}",
        sql
    );
    assert!(
        sql.contains("FROM (SELECT city AS"),
        "recompute source is the original base_query as subquery: {}",
        sql
    );
    assert!(
        sql.contains("\"__min_price\" = __src.\"__min_price\""),
        "SET targets intermediate column name, reads from __src: {}",
        sql
    );
    assert!(
        sql.contains("IS NOT DISTINCT FROM"),
        "join on group keys uses NULL-safe comparison: {}",
        sql
    );
    assert!(
        sql.contains("\"__min_price\" IS NULL"),
        "WHERE only targets MIN-nulled groups: {}",
        sql
    );
    // SUM column must not be in the SET list (only MIN/MAX/BOOL_OR are recomputed).
    assert!(
        !sql.contains("\"__sum_amount\" ="),
        "SUM column must not be recomputed: {}",
        sql
    );
}

#[test]
fn test_min_max_recompute_sql_handles_join_aliases() {
    // After algebraic BOOL_OR (#1): the old join-alias scalar-subquery bug
    // (journal/2026-04-21_min_max_recompute_bug.md) is no longer reachable —
    // BOOL_OR now emits two BIGINT SUM counter columns (algebraic +/-) so
    // build_min_max_recompute_sql never sees a BOOL_OR column.
    // This test verifies that a plan with algebraic BOOL_OR counters produces no recompute.
    let plan = AggregationPlan {
        group_by_columns: vec!["product_id".to_string()],
        intermediate_columns: vec![
            IntermediateColumn {
                name: "__bool_or_caav_product_id_is_not_null_true_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (caav.product_id IS NOT NULL) THEN 1 ELSE 0 END".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__bool_or_caav_product_id_is_not_null_nonnull_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (caav.product_id IS NOT NULL) IS NOT NULL THEN 1 ELSE 0 END"
                    .to_string(),
                topk_k: None,
            },
        ],
        end_query_mappings: vec![],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let orig_base = "SELECT s.product_id AS \"product_id\", SUM(CASE WHEN (caav.product_id IS NOT NULL) THEN 1 ELSE 0 END) AS \"__bool_or_caav_product_id_is_not_null_true_count\", COUNT(*) AS __ivm_count FROM sales_simulation s LEFT JOIN current_assortment_activity caav ON caav.product_id = s.product_id GROUP BY s.product_id";
    let sql = build_min_max_recompute_sql("intermediate", &plan, orig_base, None);
    assert!(
        sql.is_none(),
        "algebraic BOOL_OR (SUM counters) must not trigger recompute: {:?}",
        sql
    );
}

#[test]
fn test_no_min_max_recompute_for_sum_only() {
    let plan = simple_plan();
    let orig_base = "SELECT city, SUM(amount), COUNT(*) FROM orders GROUP BY city";
    let sql = build_min_max_recompute_sql("intermediate", &plan, orig_base, None);
    assert!(sql.is_none());
}

// ========================================================================
// Theme 1 (1.2.0): affected-groups-scoped MIN/MAX recompute
// ========================================================================

fn min_only_plan() -> AggregationPlan {
    AggregationPlan {
        group_by_columns: vec!["city".to_string()],
        intermediate_columns: vec![IntermediateColumn {
            name: "__min_price".to_string(),
            pg_type: "NUMERIC".to_string(),
            source_aggregate: "MIN".to_string(),
            source_arg: "price".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    }
}

#[test]
fn test_min_max_recompute_scoped_to_affected_groups_when_provided() {
    // When an affected-groups table is passed, the orig_base_query subquery
    // must be filtered down to only the groups that appear in that table.
    // Without this filter, the recompute re-aggregates the full source on
    // every retraction — the cliff that makes stock_chart IMVs unusable.
    let plan = min_only_plan();
    let orig_base = "SELECT city AS \"city\", MIN(price) AS \"__min_price\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let sql = build_min_max_recompute_sql(
        "intermediate",
        &plan,
        orig_base,
        Some("__reflex_affected_v"),
    )
    .expect("MIN plan must produce recompute SQL");
    assert!(
        sql.contains("__reflex_affected_v"),
        "recompute SQL must reference the affected-groups table: {}",
        sql
    );
    // The filter should restrict orig_base_query to groups present in the affected table.
    assert!(
        sql.contains("\"city\"") && sql.contains("IN (SELECT"),
        "recompute SQL must include an IN-filter on the group key(s) referencing the affected table: {}",
        sql
    );
}

#[test]
fn test_min_max_recompute_no_affected_filter_when_none_passed() {
    // Backward-compatible path: when no affected-groups table is available
    // (e.g. no-GROUP-BY sentinel case), the emitted SQL must not reference
    // any affected-groups table.
    let plan = min_only_plan();
    let orig_base = "SELECT city AS \"city\", MIN(price) AS \"__min_price\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let sql = build_min_max_recompute_sql("intermediate", &plan, orig_base, None)
        .expect("MIN plan must produce recompute SQL");
    assert!(
        !sql.contains("__reflex_affected"),
        "recompute SQL must NOT reference affected-groups table when none provided: {}",
        sql
    );
}

#[test]
fn test_min_max_recompute_affected_filter_uses_multiple_group_columns() {
    // Compound-key groups: the IN-filter must reference both group columns.
    let plan = AggregationPlan {
        group_by_columns: vec!["region".to_string(), "product".to_string()],
        intermediate_columns: vec![IntermediateColumn {
            name: "__min_price".to_string(),
            pg_type: "NUMERIC".to_string(),
            source_aggregate: "MIN".to_string(),
            source_arg: "price".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let orig_base = "SELECT region AS \"region\", product AS \"product\", MIN(price) AS \"__min_price\", COUNT(*) AS __ivm_count FROM orders GROUP BY region, product";
    let sql = build_min_max_recompute_sql(
        "intermediate",
        &plan,
        orig_base,
        Some("__reflex_affected_v"),
    )
    .expect("MIN plan must produce recompute SQL");
    assert!(
        sql.contains("\"region\"") && sql.contains("\"product\""),
        "compound-key filter must reference both group columns: {}",
        sql
    );
    assert!(
        sql.contains("__reflex_affected_v"),
        "compound-key filter must reference affected table: {}",
        sql
    );
}

#[test]
fn test_min_max_recompute_skips_affected_filter_for_sentinel_plan() {
    // No GROUP BY (sentinel/single-row aggregate) case: there's only one
    // intermediate row so the recompute naturally targets it. An affected-
    // groups filter on an empty grp_cols list would produce invalid SQL.
    let plan = AggregationPlan {
        group_by_columns: vec![],
        intermediate_columns: vec![IntermediateColumn {
            name: "__min_price".to_string(),
            pg_type: "NUMERIC".to_string(),
            source_aggregate: "MIN".to_string(),
            source_arg: "price".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let orig_base = "SELECT MIN(price) AS \"__min_price\", COUNT(*) AS __ivm_count FROM orders";
    let sql = build_min_max_recompute_sql(
        "intermediate",
        &plan,
        orig_base,
        Some("__reflex_affected_v"),
    )
    .expect("MIN plan must produce recompute SQL");
    assert!(
        !sql.contains("__reflex_affected_v"),
        "sentinel (no GROUP BY) plan must not inject affected-groups filter: {}",
        sql
    );
}

#[test]
fn test_replace_source_with_transition_schema_qualified() {
    let base_query = "SELECT sales_simulation.product_id, SUM(amount) FROM alp.sales_simulation INNER JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id GROUP BY sales_simulation.product_id";
    let result = replace_source_with_transition(
        base_query,
        "alp.sales_simulation",
        "__reflex_new_alp_sales_simulation",
    );
    // FROM clause should be replaced
    assert!(
        result.contains("\"__reflex_new_alp_sales_simulation\""),
        "FROM clause not replaced"
    );
    // Column qualifiers should be replaced
    assert!(
        !result.contains(" sales_simulation.product_id"),
        "Column qualifier not replaced: {}",
        result
    );
    assert!(
        !result.contains(" sales_simulation.dem_plan_id"),
        "JOIN qualifier not replaced: {}",
        result
    );
    // Other tables should NOT be replaced
    assert!(
        result.contains("alp.demand_planning"),
        "Other tables should not be affected"
    );
    assert!(
        result.contains("demand_planning.id"),
        "Other table qualifiers should not be affected"
    );
}

#[test]
fn test_replace_source_with_transition_alias_equals_bare_source_name() {
    // Parity with replace_source_with_delta gap 2: when the user has aliased
    // the schema-qualified source with its bare name (`FROM alp.t AS t`), the
    // alias hides the underlying table's own name in PG. Without the
    // pre-pass strip, step 2 of replace_source_with_transition rewrites
    // `t.col` qualifiers to `"__reflex_new_alp_t".col`, but PG raises 42P01
    // because the alias hides that name. The fix strips the redundant alias
    // first so both the FROM and the qualifiers collapse to the bare table.
    let base = "SELECT t.col FROM alp.t AS t WHERE t.col = 1";
    let result = replace_source_with_transition(base, "alp.t", "__reflex_new_alp_t");
    assert!(
        result.contains("FROM \"__reflex_new_alp_t\""),
        "FROM rewritten to transition table: {}",
        result
    );
    // The colliding alias must be stripped — otherwise PG raises 42P01 on the
    // rewritten qualifiers.
    assert!(
        !result.contains(" AS t "),
        "redundant alias must be stripped: {}",
        result
    );
    // Both qualifier occurrences should now reference the transition table.
    assert_eq!(
        result.matches("\"__reflex_new_alp_t\".col").count(),
        2,
        "both `t.col` qualifiers should resolve to the transition table: {}",
        result
    );
}

#[test]
fn test_replace_source_with_transition_alias_equals_bare_source_name_no_as() {
    // Bare-alias form `FROM alp.t t` — same collision.
    let base = "SELECT t.col FROM alp.t t WHERE t.col = 1";
    let result = replace_source_with_transition(base, "alp.t", "__reflex_new_alp_t");
    assert!(
        result.contains("FROM \"__reflex_new_alp_t\""),
        "FROM rewritten: {}",
        result
    );
    // The bare alias text must be consumed, not left dangling as `"…" t`.
    assert!(
        !result.contains("\"__reflex_new_alp_t\" t "),
        "consumed bare alias must not be left in output: {}",
        result
    );
    assert_eq!(
        result.matches("\"__reflex_new_alp_t\".col").count(),
        2,
        "both qualifiers point at transition table: {}",
        result
    );
}

#[test]
fn test_replace_source_with_transition_distinct_user_alias_preserved() {
    // Counter-test: when the user picks an alias that DOES NOT collide with
    // the bare-source name (`FROM alp.t AS my_t`), the alias must be
    // preserved as-is and qualifier rewriting must not touch `my_t.col`.
    let base = "SELECT my_t.col FROM alp.t AS my_t WHERE my_t.col = 1";
    let result = replace_source_with_transition(base, "alp.t", "__reflex_new_alp_t");
    assert!(
        result.contains("FROM \"__reflex_new_alp_t\" AS my_t"),
        "distinct user alias must be preserved: {}",
        result
    );
    assert!(
        result.contains("my_t.col"),
        "distinct alias qualifiers must not be rewritten: {}",
        result
    );
}

#[test]
fn test_replace_source_with_transition_unqualified() {
    let base_query = "SELECT city, SUM(amount) FROM orders GROUP BY city";
    let result = replace_source_with_transition(base_query, "orders", "__reflex_new_orders");
    assert!(result.contains("\"__reflex_new_orders\""));
    assert!(!result.contains(" orders "));
}

// ========================================================================
// Bug fix tests: quoted identifiers in trigger names
// ========================================================================

#[test]
fn test_update_trigger_emits_filter_aware_skip_block() {
    // The UPDATE trigger function body must contain the filter-aware skip:
    // - reads imv_relevant_columns and imv_relevant_where from aggregations
    // - applies the predicate to both __reflex_old_<src> and __reflex_new_<src>
    // - EXCEPT-ALL multiset compare → CONTINUE if both directions empty
    let ddls = build_trigger_ddls("public.orders");
    // ddls[2] is the UPDATE function (INSERT, DELETE, UPDATE, TRUNCATE order).
    let upd = &ddls[2];
    assert!(
        upd.contains("CREATE OR REPLACE FUNCTION") && upd.contains("AFTER UPDATE"),
        "Index 2 should be the UPDATE trigger DDL"
    );
    assert!(
        upd.contains("imv_relevant_columns"),
        "UPDATE body must reference imv_relevant_columns: {}",
        &upd[..upd.len().min(2000)]
    );
    assert!(
        upd.contains("imv_relevant_where"),
        "UPDATE body must reference imv_relevant_where"
    );
    assert!(
        upd.contains("EXCEPT ALL"),
        "UPDATE body must EXCEPT-ALL compare old vs new projections"
    );
    assert!(
        upd.contains("__reflex_old_public_orders") && upd.contains("__reflex_new_public_orders"),
        "UPDATE body must reference BOTH transition tables for the skip check"
    );
    // INSERT and DELETE bodies must NOT contain the skip block —
    // the optimization only fires on UPDATE.
    let ins = &ddls[0];
    let del = &ddls[1];
    assert!(
        !ins.contains("imv_relevant_columns"),
        "INSERT trigger should not have the skip block"
    );
    assert!(
        !del.contains("imv_relevant_columns"),
        "DELETE trigger should not have the skip block"
    );
}

#[test]
fn test_trigger_ddl_quoted_table_name() {
    // Tables with reserved-word names like "order" should not break trigger naming
    let ddls = build_trigger_ddls("alp.\"order\"");
    for ddl in &ddls {
        // Trigger function names should NOT contain literal quote characters
        assert!(
            !ddl.contains("__reflex_ins_trigger_on_alp_\"order\""),
            "Trigger function name should not contain quotes: {}",
            &ddl[..ddl.len().min(200)]
        );
        // Should contain the clean name
        assert!(
            ddl.contains("__reflex_") && ddl.contains("_on_alp_order"),
            "Trigger should use stripped name 'alp_order': {}",
            &ddl[..ddl.len().min(200)]
        );
        // The source table reference in SQL strings should still use the quoted form
        assert!(
            ddl.contains("ON alp.\"order\""),
            "Trigger DDL should reference the original table with quotes"
        );
    }
}

#[test]
fn test_trigger_ddl_unquoted_table_name_unchanged() {
    let ddls = build_trigger_ddls("public.sales");
    for ddl in &ddls {
        assert!(
            ddl.contains("_on_public_sales"),
            "Unquoted table names should work normally"
        );
    }
}

// ========================================================================
// Bug #1: Identifier truncation on long source names
// ========================================================================
//
// `__reflex_new_<src>` / `__reflex_old_<src>` / `__reflex_delta_<src>`
// identifiers must fit in PG's 63-char NAMEDATALEN. Two distinct long
// source names sharing the same 63-char prefix would otherwise collapse
// into the same staging/transition table → silent data corruption.

fn extract_quoted_identifiers(ddl: &str) -> Vec<&str> {
    ddl.split('"').skip(1).step_by(2).collect()
}

#[test]
fn test_build_trigger_ddls_long_source_name_no_truncation() {
    // 55-char source name → naive `__reflex_old_<src>` = 68 chars, > 63.
    let long_src = "demand_planning_characteristics_reflex__cte_sales_stats";
    assert_eq!(long_src.len(), 55);
    let ddls = build_trigger_ddls(long_src);
    for ddl in &ddls {
        for ident in extract_quoted_identifiers(ddl) {
            assert!(
                ident.len() <= 63,
                "quoted identifier > 63 chars risks PG silent truncation: `{}` ({} chars)",
                ident,
                ident.len()
            );
        }
    }
}

#[test]
fn test_build_trigger_ddls_distinct_long_sources_do_not_collide() {
    // Two source names that share a 50+ char prefix must yield DISTINCT
    // transition-table identifiers after truncation. Under naive format!
    // both would truncate to the same 63-char prefix and silently merge.
    let src_a = "demand_planning_characteristics_reflex__cte_sales_stats";
    let src_b = "demand_planning_characteristics_reflex__cte_sales_daily";

    let collect_reflex_idents = |src: &str| -> std::collections::HashSet<String> {
        let ddls = build_trigger_ddls(src);
        let mut out = std::collections::HashSet::new();
        for ddl in &ddls {
            for ident in extract_quoted_identifiers(ddl) {
                if ident.starts_with("__reflex_new_") || ident.starts_with("__reflex_old_") {
                    out.insert(ident.to_string());
                }
            }
        }
        out
    };

    let idents_a = collect_reflex_idents(src_a);
    let idents_b = collect_reflex_idents(src_b);
    assert!(
        !idents_a.is_empty(),
        "expected at least one __reflex_* ident"
    );
    for ident in &idents_a {
        assert!(
            !idents_b.contains(ident),
            "distinct source names must produce distinct transition identifiers, `{}` appeared in both",
            ident
        );
    }
}

#[test]
fn test_build_staging_table_ddl_long_source_name_no_truncation() {
    use crate::schema_builder::build_staging_table_ddl;
    let long_src = "demand_planning_characteristics_reflex__cte_sales_stats";
    let ddl = build_staging_table_ddl(long_src);
    let first_ident = ddl
        .split('"')
        .nth(1)
        .expect("staging DDL missing quoted name");
    assert!(
        first_ident.len() <= 63,
        "staging delta table name > 63 chars: `{}` ({} chars)",
        first_ident,
        first_ident.len()
    );

    let other = "demand_planning_characteristics_reflex__cte_sales_daily";
    let ddl2 = build_staging_table_ddl(other);
    let other_ident = ddl2
        .split('"')
        .nth(1)
        .expect("staging DDL missing quoted name");
    assert_ne!(
        first_ident, other_ident,
        "distinct sources must produce distinct staging delta identifiers"
    );
}

// Bug #3: COUNT(DISTINCT nullable_col) extends the intermediate key with
// `nullable_col`. The subtract path's MERGE must join on the compound key
// using `IS NOT DISTINCT FROM` (NULL-safe), not bare `=`, otherwise NULL
// rows never match and orphan counter rows accumulate.
#[test]
fn test_build_merge_count_distinct_nullable_uses_null_safe_join() {
    use crate::aggregation::{AggregationPlan, EndQueryMapping, IntermediateColumn};
    let plan = AggregationPlan {
        group_by_columns: vec!["grp".to_string()],
        intermediate_columns: vec![IntermediateColumn {
            name: "__count_distinct_maybe_null".to_string(),
            pg_type: "BIGINT".to_string(),
            source_aggregate: "COUNT".to_string(),
            source_arg: "*".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![EndQueryMapping {
            intermediate_expr: "COUNT(*)".to_string(),
            output_alias: "cnt".to_string(),
            aggregate_type: "COUNT".to_string(),
            cast_type: None,
        }],
        has_distinct: false,
        needs_ivm_count: true,
        // COUNT(DISTINCT maybe_null) adds the distinct column to the key.
        distinct_columns: vec!["maybe_null".to_string()],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let delta = "SELECT grp, maybe_null, COUNT(*) AS __ivm_count FROM src GROUP BY grp, maybe_null";

    for op in [DeltaOp::Add, DeltaOp::Subtract] {
        let sql = build_merge_sql("intermediate", delta, &plan, op);
        // Both group key and distinct key must be joined null-safe.
        assert!(
            sql.contains("t.\"grp\" IS NOT DISTINCT FROM d.\"grp\""),
            "group key must be null-safe in {:?} MERGE: {}",
            op as u8,
            sql
        );
        assert!(
            sql.contains("t.\"maybe_null\" IS NOT DISTINCT FROM d.\"maybe_null\""),
            "DISTINCT key must be null-safe in {:?} MERGE — otherwise a row with \
             maybe_null = NULL never matches: {}",
            op as u8,
            sql
        );
        // The ON clause must NOT use bare `=` on the distinct key.
        assert!(
            !sql.contains("t.\"maybe_null\" = d.\"maybe_null\""),
            "bare `=` on nullable DISTINCT key leaves orphan rows: {}",
            sql
        );
    }
}

#[test]
fn test_build_deferred_trigger_ddls_long_source_name_no_truncation() {
    use crate::schema_builder::build_deferred_trigger_ddls;
    let long_src = "demand_planning_characteristics_reflex__cte_sales_stats";
    let cols = vec!["id".to_string(), "amount".to_string()];
    let ddls = build_deferred_trigger_ddls(long_src, &cols);
    for ddl in &ddls {
        for ident in extract_quoted_identifiers(ddl) {
            assert!(
                ident.len() <= 63,
                "deferred-trigger quoted identifier > 63 chars: `{}` ({} chars)",
                ident,
                ident.len()
            );
        }
    }
}

// ========================================================================
// #3 — DO-block gate for targeted refresh
// ========================================================================

#[test]
fn test_build_delta_sql_uses_scratch_table_for_group_by_imv() {
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q =
        "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_test_view\"";
    let sql = reflex_build_delta_sql(
        "test_view",
        "orders",
        "DELETE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert!(
        sql.contains("TRUNCATE \"__reflex_scratch_test_view\""),
        "targeted DELETE must TRUNCATE the scratch table: {}",
        &sql[..sql.len().min(400)]
    );
    assert!(
        sql.contains("USING \"__reflex_scratch_test_view\""),
        "MERGE must read from scratch table, not inline subquery: {}",
        &sql[..sql.len().min(400)]
    );
    assert!(
        !sql.contains("USING (SELECT"),
        "MERGE must never reference a transition table via inline subquery: {}",
        &sql[..sql.len().min(400)]
    );
    assert!(
        sql.contains("INSERT INTO \"__reflex_affected_test_view\" SELECT"),
        "affected groups must be populated from scratch: {}",
        &sql[..sql.len().min(400)]
    );
    assert!(
        !sql.contains("INSERT INTO \"__reflex_affected_test_view\" SELECT DISTINCT"),
        "scratch is one row per group key; DISTINCT is wasted work: {}",
        &sql[..sql.len().min(400)]
    );
}

// 2026-05-15 — Item α: ensure intermediate gets ANALYZE'd after the MERGE
// modifies ~|scratch| rows. Without fresh stats the planner picks
// pathological plans for downstream target-sync and dead-cleanup
// statements (12+ min on 100K groups vs ~2 s expected). The MERGE's
// non-dispatch caller (push_materialized_merge_and_affected) emits the
// ANALYZE immediately after the MERGE, before the affected-INSERT and
// downstream EXISTS-based statements.
#[test]
fn test_intermediate_analyzed_after_merge_in_non_dispatch_path() {
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    // end_q without GROUP BY → takes the non-dispatch grouped target-sync path
    // (intermediate is already pre-aggregated; end_query is a pure projection
    // with WHERE __ivm_count > 0 — the shape of real SOP-style IMVs).
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_test_view\" WHERE __ivm_count > 0";
    let intermediate = "\"__reflex_intermediate_test_view\"";
    let analyze_marker = format!("ANALYZE {}", intermediate);
    for op in ["INSERT", "DELETE", "UPDATE"].iter() {
        let sql = reflex_build_delta_sql(
            "test_view",
            "orders",
            op,
            base_q,
            end_q,
            Some(agg_json.as_str()),
            base_q,
        );
        let merge_marker = format!("MERGE INTO {}", intermediate);
        let merge_pos = sql.find(&merge_marker).unwrap_or_else(|| {
            panic!(
                "MERGE statement must be present (op={}): {}",
                op,
                &sql[..sql.len().min(2000)]
            )
        });
        // ANALYZE on intermediate must appear after MERGE.
        let after_merge = &sql[merge_pos..];
        assert!(
            after_merge.contains(&analyze_marker),
            "ANALYZE on intermediate must appear after MERGE (op={}): {}",
            op,
            &after_merge[..after_merge.len().min(800)]
        );
    }
}

#[test]
// 2026-05-15 — Item α: ensure the affected table gets ANALYZE'd after the
// scratch-fed INSERT, so the planner has fresh row counts for the
// downstream dead-cleanup DELETE and target sync EXISTS lookups.
// Without ANALYZE, TRUNCATE clobbers reltuples → planner picks NestedLoop
// with SeqScan on intermediate per affected row (pathological at scale).
fn test_affected_table_analyzed_after_scratch_fed_insert() {
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", COUNT(\"amount\") AS total FROM \"__reflex_intermediate_test_view\" WHERE __ivm_count > 0 GROUP BY \"city\"";
    for op in ["INSERT", "DELETE", "UPDATE"].iter() {
        let sql = reflex_build_delta_sql(
            "test_view",
            "orders",
            op,
            base_q,
            end_q,
            Some(agg_json.as_str()),
            base_q,
        );
        let affected = "\"__reflex_affected_test_view\"";
        let analyze_marker = format!("ANALYZE {}", affected);
        let affected_insert = format!("INSERT INTO {}", affected);
        let analyze_pos = sql.find(&analyze_marker).unwrap_or_else(|| {
            panic!(
                "must emit ANALYZE on affected table after insert (op={}): {}",
                op,
                &sql[..sql.len().min(800)]
            )
        });
        let insert_pos = sql.find(&affected_insert).unwrap_or_else(|| {
            panic!(
                "affected INSERT must be present (op={}): {}",
                op,
                &sql[..sql.len().min(800)]
            )
        });
        assert!(
            insert_pos < analyze_pos,
            "ANALYZE must follow the affected INSERT (op={}): insert at {} analyze at {}",
            op,
            insert_pos,
            analyze_pos
        );
    }
}

#[test]
fn test_affected_insert_from_scratch_omits_distinct() {
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q =
        "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_test_view\"";

    for op in ["INSERT", "DELETE", "UPDATE"].iter() {
        let sql = reflex_build_delta_sql(
            "test_view",
            "orders",
            op,
            base_q,
            end_q,
            Some(agg_json.as_str()),
            base_q,
        );
        let affected_insert_marker = "INSERT INTO \"__reflex_affected_test_view\"";
        let mut search = sql.as_str();
        while let Some(pos) = search.find(affected_insert_marker) {
            let tail = &search[pos..];
            let line_end = tail.find('\n').unwrap_or(tail.len());
            let stmt_head = &tail[..line_end.min(200)];
            // Affected INSERT pulls from the scratch table for grouped IMVs;
            // scratch is pre-aggregated to one row per group, so DISTINCT is
            // redundant. Outer-join secondary path is the only exception and
            // pulls from `(delta_q)` not `__reflex_scratch_…`.
            if stmt_head.contains("__reflex_scratch_test_view") {
                assert!(
                    !stmt_head.contains("SELECT DISTINCT"),
                    "affected INSERT from scratch must NOT use DISTINCT (op={}): {}",
                    op,
                    stmt_head
                );
            }
            search = &search[pos + affected_insert_marker.len()..];
        }
    }
}

#[test]
fn test_build_delta_sql_end_query_group_by_uses_scratch_table() {
    // end_query_has_group_by: targeted refresh via scratch table (no DO block).
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", COUNT(\"amount\") AS total FROM \"__reflex_intermediate_test_view\" WHERE __ivm_count > 0 GROUP BY \"city\"";
    let sql = reflex_build_delta_sql(
        "test_view",
        "orders",
        "DELETE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert!(
        sql.contains("TRUNCATE \"__reflex_scratch_test_view\""),
        "end_query_has_group_by branch must TRUNCATE scratch: {}",
        &sql[..sql.len().min(600)]
    );
    assert!(
        !sql.contains("USING (SELECT"),
        "MERGE must never use inline transition-table subquery: {}",
        &sql[..sql.len().min(600)]
    );
    // The target INSERT (into test_view) must have the null-safe filter before GROUP BY.
    let insert_pos = sql
        .find("INSERT INTO \"test_view\"")
        .expect("target INSERT must be present");
    let tail = &sql[insert_pos..];
    let filter_pos = tail
        .find("IS NOT DISTINCT FROM")
        .expect("null-safe filter must be in target INSERT");
    let group_by_pos = tail
        .find("GROUP BY")
        .expect("GROUP BY must be in target INSERT");
    assert!(
        filter_pos < group_by_pos,
        "null-safe filter must appear before GROUP BY in target INSERT: {}",
        &tail[..tail.len().min(400)]
    );
}

#[test]
fn test_build_delta_sql_scratch_used_for_sentinel_case() {
    // No group-by columns: scratch table is still used for MERGE materialization.
    let mut plan = simple_plan();
    plan.group_by_columns = vec![];
    plan.distinct_columns = vec![];
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders";
    let end_q = "SELECT \"__sum_amount\" AS total FROM \"__reflex_intermediate_test_view\"";
    let sql = reflex_build_delta_sql(
        "test_view",
        "orders",
        "INSERT",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert!(
        sql.contains("TRUNCATE \"__reflex_scratch_test_view\""),
        "no-group INSERT must still use scratch table: {}",
        &sql[..sql.len().min(400)]
    );
    assert!(
        !sql.contains("USING (SELECT"),
        "MERGE must never use inline transition-table subquery: {}",
        &sql[..sql.len().min(400)]
    );
}

#[test]
fn test_build_delta_sql_dead_cleanup_emitted_as_statement() {
    // needs_ivm_count=true + DELETE: dead-group cleanup is a plain statement, not wrapped in a DO block.
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q =
        "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_test_view\"";
    let sql = reflex_build_delta_sql(
        "test_view",
        "orders",
        "DELETE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    let cleanup_pos = sql
        .find("__ivm_count <= 0")
        .expect("dead cleanup must be present for DELETE with needs_ivm_count");
    let target_delete_pos = sql
        .find("DELETE FROM \"test_view\"")
        .expect("target DELETE must be present");
    assert!(
        cleanup_pos < target_delete_pos,
        "dead cleanup must precede target DELETE (both are plain statements): {}",
        &sql[..sql.len().min(600)]
    );
    assert!(
        !sql.contains("DO $reflex_refresh$"),
        "dead cleanup must not be wrapped in a DO block: {}",
        &sql[..sql.len().min(400)]
    );
}

// ── Phase A (#5): inject_affected_filter_before_group_by + targeted end-query refresh ──

#[test]
fn test_build_delta_sql_splice_injects_filter_before_group_by() {
    // COUNT(DISTINCT val) GROUP BY grp: end_query reads from intermediate with GROUP BY.
    // After #5, this emits a DO-gated targeted refresh with filter spliced before GROUP BY.
    let plan = AggregationPlan {
        group_by_columns: vec!["grp".to_string()],
        intermediate_columns: vec![IntermediateColumn {
            name: "__ivm_count".to_string(),
            pg_type: "BIGINT".to_string(),
            source_aggregate: "COUNT".to_string(),
            source_arg: "*".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![EndQueryMapping {
            intermediate_expr: "COUNT(\"val\")".to_string(),
            output_alias: "cd".to_string(),
            aggregate_type: "COUNT".to_string(),
            cast_type: None,
        }],
        has_distinct: true,
        needs_ivm_count: true,
        distinct_columns: vec!["val".to_string()],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q =
        "SELECT \"grp\", \"val\", COUNT(*) AS __ivm_count FROM src GROUP BY \"grp\", \"val\"";
    let end_q = "SELECT \"grp\", COUNT(\"val\") AS cd FROM \"__reflex_intermediate_test_view\" WHERE __ivm_count > 0 GROUP BY \"grp\"";
    let sql = reflex_build_delta_sql(
        "test_view",
        "src",
        "DELETE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );

    assert!(
        sql.contains("TRUNCATE \"__reflex_scratch_test_view\""),
        "targeted splice must use scratch table: {}",
        &sql[..sql.len().min(600)]
    );
    assert!(
        !sql.contains("USING (SELECT"),
        "MERGE must never use inline transition-table subquery: {}",
        &sql[..sql.len().min(600)]
    );
    // The target INSERT (into test_view) must have the null-safe filter spliced before GROUP BY.
    let insert_pos = sql
        .find("INSERT INTO \"test_view\"")
        .expect("target INSERT must be present");
    let tail = &sql[insert_pos..];
    let filter_pos = tail
        .find("IS NOT DISTINCT FROM")
        .expect("null-safe filter must appear in target INSERT");
    let group_by_pos = tail
        .find("GROUP BY")
        .expect("GROUP BY must be in target INSERT");
    assert!(
        filter_pos < group_by_pos,
        "filter must precede GROUP BY in target INSERT: {}",
        &tail[..tail.len().min(500)]
    );
}

#[test]
fn test_build_delta_sql_splice_falls_back_when_no_group_by_cols() {
    // When plan.group_by_columns is empty but end_query has GROUP BY, fall back to full rebuild.
    let mut plan = simple_plan();
    plan.group_by_columns = vec![];
    plan.distinct_columns = vec![];
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT COUNT(*) AS __ivm_count FROM orders";
    let end_q = "SELECT some_col, COUNT(*) AS cd FROM orders GROUP BY some_col";
    let sql = reflex_build_delta_sql(
        "test_view",
        "orders",
        "DELETE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );

    assert!(
        !sql.contains("DO $reflex_refresh$"),
        "no-output-group-cols must fall back to full rebuild (no DO block): {}",
        &sql[..sql.len().min(400)]
    );
    assert!(
        sql.contains("DELETE FROM"),
        "full-rebuild fallback must contain DELETE FROM: {}",
        &sql[..sql.len().min(400)]
    );
    assert!(
        !sql.contains("USING (SELECT"),
        "MERGE must never use inline transition-table subquery: {}",
        &sql[..sql.len().min(400)]
    );
}

#[test]
fn test_splice_helper_handles_having_clause() {
    let input =
        "SELECT grp, COUNT(val) FROM int WHERE __ivm_count > 0 GROUP BY grp HAVING COUNT(val) > 0";
    let result = inject_affected_filter_before_group_by(
        input,
        &["\"grp\"".to_string()],
        "aff_tbl",
        "int",
        &std::collections::HashSet::new(),
    );
    let spliced = result.expect("should succeed when GROUP BY present");

    let filter_pos = spliced.find("EXISTS").expect("filter must be present");
    let group_by_pos = spliced
        .find("GROUP BY")
        .expect("GROUP BY must be preserved");
    let having_pos = spliced.find("HAVING").expect("HAVING must be preserved");

    assert!(
        filter_pos < group_by_pos,
        "filter must precede GROUP BY: {}",
        spliced
    );
    assert!(
        group_by_pos < having_pos,
        "GROUP BY must precede HAVING: {}",
        spliced
    );
}

#[test]
fn test_splice_helper_returns_none_when_no_group_by() {
    let result = inject_affected_filter_before_group_by(
        "SELECT COUNT(val) FROM int WHERE __ivm_count > 0",
        &["\"grp\"".to_string()],
        "aff_tbl",
        "int",
        &std::collections::HashSet::new(),
    );
    assert!(
        result.is_none(),
        "helper must return None when no GROUP BY marker found"
    );
}

#[test]
fn test_build_delta_sql_splice_uses_distinct_projection_for_compound_key() {
    // COUNT(DISTINCT val) GROUP BY grp: the intermediate key is (grp, val),
    // but the filter for the target must project down to output group cols only (grp, not val).
    let plan = AggregationPlan {
        group_by_columns: vec!["grp".to_string()],
        intermediate_columns: vec![IntermediateColumn {
            name: "__ivm_count".to_string(),
            pg_type: "BIGINT".to_string(),
            source_aggregate: "COUNT".to_string(),
            source_arg: "*".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![EndQueryMapping {
            intermediate_expr: "COUNT(\"val\")".to_string(),
            output_alias: "cd".to_string(),
            aggregate_type: "COUNT".to_string(),
            cast_type: None,
        }],
        has_distinct: true,
        needs_ivm_count: true,
        distinct_columns: vec!["val".to_string()],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q =
        "SELECT \"grp\", \"val\", COUNT(*) AS __ivm_count FROM src GROUP BY \"grp\", \"val\"";
    let end_q = "SELECT \"grp\", COUNT(\"val\") AS cd FROM \"__reflex_intermediate_test_view\" WHERE __ivm_count > 0 GROUP BY \"grp\"";
    let sql = reflex_build_delta_sql(
        "test_view",
        "src",
        "DELETE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );

    // Filter in the INSERT splice must reference "grp" (output group col).
    assert!(
        sql.contains("\"grp\" IS NOT DISTINCT FROM __a.\"grp\""),
        "splice filter must use output group col grp: {}",
        &sql[..sql.len().min(600)]
    );
    // Filter must NOT reference "val" in the target INSERT (distinct col, not an output group col).
    let insert_pos = sql
        .find("INSERT INTO \"test_view\"")
        .expect("target INSERT must be present");
    let insert_tail = &sql[insert_pos..];
    assert!(
        !insert_tail.contains("\"val\" IS NOT DISTINCT FROM"),
        "splice filter must NOT include the distinct column val: {}",
        &insert_tail[..insert_tail.len().min(500)]
    );
}

// ── Phase B (#1): Algebraic BOOL_OR ──

#[test]
fn test_build_merge_sql_bool_or_algebraic_subtract() {
    // Algebraic BOOL_OR emits two BIGINT SUM counter columns.
    // Subtract must use COALESCE arithmetic, not NULL assignment.
    let plan = AggregationPlan {
        group_by_columns: vec!["grp".to_string()],
        intermediate_columns: vec![
            IntermediateColumn {
                name: "__bool_or_flag_true_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (flag) THEN 1 ELSE 0 END".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__bool_or_flag_nonnull_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (flag) IS NOT NULL THEN 1 ELSE 0 END".to_string(),
                topk_k: None,
            },
        ],
        end_query_mappings: vec![],
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let delta = "SELECT grp, SUM(CASE WHEN (flag) THEN 1 ELSE 0 END) AS \"__bool_or_flag_true_count\", SUM(CASE WHEN (flag) IS NOT NULL THEN 1 ELSE 0 END) AS \"__bool_or_flag_nonnull_count\", COUNT(*) AS __ivm_count FROM src GROUP BY grp";
    let sql = build_merge_sql("intermediate", delta, &plan, DeltaOp::Subtract);

    assert!(
        sql.contains(
            "COALESCE(t.\"__bool_or_flag_true_count\", 0) - COALESCE(d.\"__bool_or_flag_true_count\", 0)"
        ),
        "BOOL_OR true_count must use algebraic subtract: {}",
        sql
    );
    assert!(
        sql.contains(
            "COALESCE(t.\"__bool_or_flag_nonnull_count\", 0) - COALESCE(d.\"__bool_or_flag_nonnull_count\", 0)"
        ),
        "BOOL_OR nonnull_count must use algebraic subtract: {}",
        sql
    );
    // Must NOT use NULL assignment (old non-algebraic behavior)
    assert!(
        !sql.contains("__bool_or_flag_true_count\" = NULL"),
        "BOOL_OR counter must not be set to NULL: {}",
        sql
    );
    assert!(
        !sql.contains("__bool_or_flag_nonnull_count\" = NULL"),
        "BOOL_OR counter must not be set to NULL: {}",
        sql
    );
}

#[test]
fn test_build_delta_sql_bool_or_has_no_recompute() {
    // Algebraic BOOL_OR: no MIN/MAX/BOOL_OR recompute step emitted on DELETE.
    use crate::aggregation::{AggregationPlan, EndQueryMapping, IntermediateColumn};
    let plan = AggregationPlan {
        group_by_columns: vec!["grp".to_string()],
        intermediate_columns: vec![
            IntermediateColumn {
                name: "__bool_or_flag_true_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (flag) THEN 1 ELSE 0 END".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__bool_or_flag_nonnull_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (flag) IS NOT NULL THEN 1 ELSE 0 END".to_string(),
                topk_k: None,
            },
        ],
        end_query_mappings: vec![EndQueryMapping {
            intermediate_expr: "CASE WHEN \"__bool_or_flag_nonnull_count\" > 0 THEN \"__bool_or_flag_true_count\" > 0 ELSE NULL END".to_string(),
            output_alias: "has_any".to_string(),
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
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT grp, SUM(CASE WHEN (flag) THEN 1 ELSE 0 END) AS \"__bool_or_flag_true_count\", SUM(CASE WHEN (flag) IS NOT NULL THEN 1 ELSE 0 END) AS \"__bool_or_flag_nonnull_count\", COUNT(*) AS __ivm_count FROM t GROUP BY grp";
    let end_q = "SELECT \"grp\", CASE WHEN \"__bool_or_flag_nonnull_count\" > 0 THEN \"__bool_or_flag_true_count\" > 0 ELSE NULL END AS has_any FROM \"__reflex_intermediate_test_view\" WHERE __ivm_count > 0";
    let sql = reflex_build_delta_sql(
        "test_view",
        "t",
        "DELETE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );

    // No UPDATE ... SET for recompute (which would contain the col names in SET form)
    assert!(
        !sql.contains("UPDATE __reflex_intermediate_test_view SET"),
        "algebraic BOOL_OR must not emit a recompute UPDATE: {}",
        &sql[..sql.len().min(600)]
    );
}

fn passthrough_plan(source: &str) -> AggregationPlan {
    let mut mappings = std::collections::HashMap::new();
    mappings.insert(
        source.to_string(),
        vec![("city".to_string(), "city".to_string())],
    );
    AggregationPlan {
        group_by_columns: vec![],
        intermediate_columns: vec![],
        end_query_mappings: vec![],
        has_distinct: false,
        needs_ivm_count: false,
        distinct_columns: vec![],
        is_passthrough: true,
        passthrough_columns: vec!["city".to_string()],
        passthrough_key_mappings: mappings,
        having_clause: None,
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases: std::collections::HashMap::new(),
        output_column_order: vec![],
        imv_relevant_columns: std::collections::HashMap::new(),
        imv_relevant_where: std::collections::HashMap::new(),
        source_join_keys: std::collections::HashMap::new(),
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    }
}

/// Split generated delta SQL into its constituent statements the same way the
/// trigger body does (`string_to_array(_, '\n--<<REFLEX_SEP>>--\n')`).
fn split_reflex_sep(sql: &str) -> Vec<&str> {
    sql.split("\n--<<REFLEX_SEP>>--\n").collect()
}

/// A statement is "sanctioned" to touch a transition table iff it's a plain
/// `INSERT INTO "__reflex_{scratch|pt_new|pt_old}_*" SELECT * FROM "__reflex_{new|old}_*"`.
/// Everything else referencing `__reflex_new_*` / `__reflex_old_*` is the
/// SIGABRT pattern and must be rejected by the generator guard.
fn is_sanctioned_scratch_populate(stmt: &str) -> bool {
    let t = stmt.trim_start();
    t.starts_with("INSERT INTO \"__reflex_scratch_")
        || t.starts_with("INSERT INTO \"__reflex_pt_new_")
        || t.starts_with("INSERT INTO \"__reflex_pt_old_")
}

fn assert_no_transition_leaks(sql: &str, context: &str) {
    for stmt in split_reflex_sep(sql) {
        let has_new = stmt.contains("\"__reflex_new_");
        let has_old = stmt.contains("\"__reflex_old_");
        if !has_new && !has_old {
            continue;
        }
        assert!(
            is_sanctioned_scratch_populate(stmt),
            "{context}: transition table leaked into unsanctioned statement:\n{stmt}"
        );
    }
}

#[test]
fn test_passthrough_insert_materializes_via_pt_new_scratch() {
    let plan = passthrough_plan("chain_l1");
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, total, cnt FROM chain_l1";

    let sql = reflex_build_delta_sql(
        "chain_l2",
        "chain_l1",
        "INSERT",
        base_q,
        "",
        Some(agg_json.as_str()),
        base_q,
    );

    assert!(
        sql.contains("TRUNCATE \"__reflex_pt_new_chain_l2_chain_l1\""),
        "INSERT must TRUNCATE the new-side pt scratch: {sql}"
    );
    assert!(
        sql.contains(
            "INSERT INTO \"__reflex_pt_new_chain_l2_chain_l1\" SELECT * FROM \"__reflex_new_chain_l1\""
        ),
        "INSERT must populate pt_new scratch from new transition: {sql}"
    );
    assert_no_transition_leaks(&sql, "passthrough INSERT");
}

#[test]
fn test_passthrough_delete_reads_pt_old_scratch_not_transition() {
    let plan = passthrough_plan("chain_l1");
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, total, cnt FROM chain_l1";

    let sql = reflex_build_delta_sql(
        "chain_l2",
        "chain_l1",
        "DELETE",
        base_q,
        "",
        Some(agg_json.as_str()),
        base_q,
    );

    assert!(
        sql.contains("INSERT INTO \"__reflex_pt_old_chain_l2_chain_l1\""),
        "DELETE must populate pt_old scratch: {sql}"
    );
    assert!(
        sql.contains("FROM \"__reflex_pt_old_chain_l2_chain_l1\""),
        "DELETE WHERE IN subquery must read from pt_old scratch: {sql}"
    );
    assert_no_transition_leaks(&sql, "passthrough DELETE");
}

#[test]
fn test_passthrough_update_materializes_both_sides() {
    let plan = passthrough_plan("chain_l1");
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, total, cnt FROM chain_l1";

    let sql = reflex_build_delta_sql(
        "chain_l2",
        "chain_l1",
        "UPDATE",
        base_q,
        "",
        Some(agg_json.as_str()),
        base_q,
    );

    assert!(
        sql.contains("\"__reflex_pt_new_chain_l2_chain_l1\""),
        "UPDATE must use pt_new for the insert phase: {sql}"
    );
    assert!(
        sql.contains("\"__reflex_pt_old_chain_l2_chain_l1\""),
        "UPDATE must use pt_old for the delete phase: {sql}"
    );
    assert_no_transition_leaks(&sql, "passthrough UPDATE");
}

// 1.4.6 — the UPDATE path emits the high-selectivity dispatch DO block.
// INSERT and DELETE intentionally stay on the inline MERGE path: an earlier
// attempt to extend dispatch to those branches regressed bulk filter flips
// on db_clone alp by ~70 % (the scratch-then-decide ordering wastes 50–100 s
// of scratch fill on 8.9 M-row promoted directional flips when dispatch
// then picks reconcile). See journal/2026-05-15_dispatch_wiring_revert.md
// for the decision log and the next-step plan.
#[test]
fn test_dispatch_block_emitted_only_for_update() {
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", COUNT(\"amount\") AS total FROM \"__reflex_intermediate_test_view\" WHERE __ivm_count > 0 GROUP BY \"city\"";

    let sql_upd = reflex_build_delta_sql(
        "test_view",
        "orders",
        "UPDATE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert!(
        sql_upd.contains("DO $reflex_dispatch$"),
        "UPDATE must emit the dispatch DO block: {}",
        &sql_upd[..sql_upd.len().min(2000)]
    );
    assert!(
        sql_upd.contains("reflex_reconcile"),
        "UPDATE dispatch block must reference reflex_reconcile: {}",
        &sql_upd[..sql_upd.len().min(2000)]
    );

    for op in ["INSERT", "DELETE"].iter() {
        let sql = reflex_build_delta_sql(
            "test_view",
            "orders",
            op,
            base_q,
            end_q,
            Some(agg_json.as_str()),
            base_q,
        );
        assert!(
            !sql.contains("DO $reflex_dispatch$"),
            "op={} must NOT emit dispatch — wasted-scratch regression. \
             If you re-enable, validate on benchmarks/bench_user_query_workloads_v3.sql \
             against db_clone alp first.",
            op,
        );
    }
}

// 1.4.6 — the UPDATE dispatch DO block must consult __reflex_ivm_reference
// for the per-IMV wipe_threshold override before falling back to the GUC and
// the compiled default. INSERT/DELETE intentionally stay off dispatch (see
// test above), so we only assert this on UPDATE.
#[test]
fn test_update_dispatch_block_reads_per_imv_threshold_override() {
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", COUNT(\"amount\") AS total FROM \"__reflex_intermediate_test_view\" WHERE __ivm_count > 0 GROUP BY \"city\"";
    let sql = reflex_build_delta_sql(
        "test_view",
        "orders",
        "UPDATE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert!(
        sql.contains("wipe_threshold"),
        "UPDATE dispatch block must read wipe_threshold column from \
         __reflex_ivm_reference: {}",
        &sql[..sql.len().min(2000)]
    );
    assert!(
        sql.contains("__reflex_ivm_reference"),
        "UPDATE dispatch block must reference __reflex_ivm_reference: {}",
        &sql[..sql.len().min(2000)]
    );
}

/// Regression guard: the aggregate branch must also keep transition tables
/// confined to sanctioned scratch-populate statements (Phase B's fix).
///
/// 1.4.5 note: `orig_base_query` MUST be the live-source version (it's
/// what's stored in `__reflex_ivm_reference.base_query` and what
/// `schema_builder.rs` passes through). The high-selectivity dispatch
/// block emitted in 1.4.5 embeds `orig_base_query` in a
/// TRUNCATE+rebuild branch, which is fine in production because
/// `orig_base_query` doesn't contain transition tables. The test fixture
/// now passes a realistic live-source query (no transition refs) and
/// only constructs a transition-substituted variant for the `base_query`
/// param if a test exercises that codegen path internally.
#[test]
fn test_aggregate_delta_sql_has_no_transition_leaks() {
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    // Live-source query (stored in __reflex_ivm_reference). The
    // trigger codegen substitutes transition tables for `t` internally
    // when building the scratch INSERT.
    let live_base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"t\" GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_v\" WHERE __ivm_count > 0";

    for op in ["INSERT", "DELETE", "UPDATE"] {
        let sql = reflex_build_delta_sql(
            "v",
            "t",
            op,
            live_base_q,
            end_q,
            Some(agg_json.as_str()),
            live_base_q,
        );
        assert_no_transition_leaks(&sql, &format!("aggregate {op}"));
    }
}

/// O2 cache: identical inputs must produce identical SQL whether served from
/// cache or built fresh. Differing inputs must produce distinct outputs.
#[test]
fn test_delta_sql_cache_consistency() {
    use crate::trigger::reset_delta_sql_cache;

    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_cache_v\"";

    reset_delta_sql_cache();
    let cold = reflex_build_delta_sql(
        "cache_v",
        "orders",
        "INSERT",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    let warm = reflex_build_delta_sql(
        "cache_v",
        "orders",
        "INSERT",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert_eq!(cold, warm, "cache hit must match cache miss byte-for-byte");

    reset_delta_sql_cache();
    let rebuilt = reflex_build_delta_sql(
        "cache_v",
        "orders",
        "INSERT",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert_eq!(cold, rebuilt, "rebuild after reset must match original");

    let other_op = reflex_build_delta_sql(
        "cache_v",
        "orders",
        "DELETE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert_ne!(
        cold, other_op,
        "different op key must miss the cache and produce different SQL"
    );

    let mut plan2 = simple_plan();
    plan2.group_by_columns = vec!["region".to_string()];
    let agg_json2 = serde_json::to_string(&plan2).unwrap();
    let other_plan = reflex_build_delta_sql(
        "cache_v",
        "orders",
        "INSERT",
        base_q,
        end_q,
        Some(agg_json2.as_str()),
        base_q,
    );
    assert_ne!(
        cold, other_plan,
        "different aggregations_json must miss cache"
    );
}

// =============================================================================
// Regression: null_safe_in must qualify outer column references.
//
// The bug (caught 2026-05-13, would have shipped in 1.4.4 unreleased): the
// generated EXISTS filter emitted bare unqualified outer column refs like
// `"id" = __a."id"`. Postgres's name resolution prefers the inner subquery
// scope when a column is unambiguously present there, so the predicate
// degenerated to `__a.id = __a.id` (a one-time TRUE filter) for every
// affected-vs-target / affected-vs-intermediate match. Every UPDATE/DELETE
// on a grouped IMV silently became a full refresh.
//
// These tests assert on the SHAPE of the generated SQL: the EXISTS predicate
// must qualify the outer-side column with the outer table (or subquery
// alias) so Postgres binds to the outer scope even when both sides share a
// column name. We verify both the simple-aligned case (no GROUP BY aliasing)
// AND the aliased case (`SELECT col AS alias` where target column name
// differs from intermediate column name) — the latter is the customer-shape
// that surfaced the bug.
// =============================================================================

/// Plain GROUP BY IMV with NO alias (target col == intermediate col). The
/// generated EXISTS still has to qualify the outer because the inner-scope
/// rule fires whenever names match, not only when they differ.
#[test]
fn test_null_safe_in_qualifies_outer_when_target_col_matches_intermediate() {
    let plan = simple_plan(); // group_by = ["city"], no aliases
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_test_view\" WHERE __ivm_count > 0";
    let sql = reflex_build_delta_sql(
        "test_view",
        "orders",
        "UPDATE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );

    // Locate the DELETE FROM target — must NOT contain the buggy bare
    // `"city" = __a."city"` pattern. Outer must be qualified with the
    // target table identifier.
    let del_pos = sql
        .find("DELETE FROM \"test_view\"")
        .expect("target DELETE must be present");
    let del_tail = &sql[del_pos..];
    let next_stmt = del_tail
        .find("\n--<<REFLEX_SEP>>--\n")
        .unwrap_or(del_tail.len());
    let delete_stmt = &del_tail[..next_stmt];

    assert!(
        !delete_stmt.contains("WHERE \"city\" ="),
        "buggy unqualified outer: DELETE filter must NOT use bare `\"city\" = __a.\"city\"` (resolves to inner __a scope). Got: {}",
        delete_stmt
    );
    assert!(
        delete_stmt.contains("\"test_view\".\"city\""),
        "target DELETE must qualify outer col as `\"test_view\".\"city\"` so name resolution binds to target. Got: {}",
        delete_stmt
    );
}

/// Customer-shape: IMV aliases a GROUP BY column (`dp.id AS dem_plan_id`).
/// Target carries `dem_plan_id`; intermediate / affected carry `id`. The
/// generated DELETE FROM target must reference `target."dem_plan_id"` (NOT
/// `target."id"` — which doesn't exist on target).
#[test]
fn test_null_safe_in_handles_aliased_group_by_column() {
    let mut plan = AggregationPlan {
        group_by_columns: vec!["dp.id".to_string()],
        intermediate_columns: vec![IntermediateColumn {
            name: "__sum_qty".to_string(),
            pg_type: "NUMERIC".to_string(),
            source_aggregate: "SUM".to_string(),
            source_arg: "qty".to_string(),
            topk_k: None,
        }],
        end_query_mappings: vec![EndQueryMapping {
            intermediate_expr: "__sum_qty".to_string(),
            output_alias: "total_qty".to_string(),
            aggregate_type: "SUM".to_string(),
            cast_type: None,
        }],
        has_distinct: false,
        needs_ivm_count: true,
        distinct_columns: vec![],
        is_passthrough: false,
        passthrough_columns: vec![],
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: None,
        not_null_columns: ["id"].iter().map(|s| s.to_string()).collect(),
        group_by_aliases: std::collections::HashMap::new(),
        output_column_order: vec![],
        imv_relevant_columns: std::collections::HashMap::new(),
        imv_relevant_where: std::collections::HashMap::new(),
        source_join_keys: std::collections::HashMap::new(),
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
    };
    plan.group_by_aliases
        .insert("dp.id".to_string(), "dem_plan_id".to_string());
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT dp.id AS \"id\", SUM(ss.qty) AS \"__sum_qty\", COUNT(*) AS __ivm_count FROM ss JOIN dp ON dp.id = ss.dp_id GROUP BY dp.id";
    let end_q = "SELECT \"id\" AS \"dem_plan_id\", \"__sum_qty\" AS \"total_qty\" FROM \"__reflex_intermediate_aliased_view\" WHERE __ivm_count > 0";
    let sql = reflex_build_delta_sql(
        "aliased_view",
        "dp",
        "UPDATE",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );

    let del_pos = sql
        .find("DELETE FROM \"aliased_view\"")
        .expect("target DELETE must be present");
    let next_sep = sql[del_pos..]
        .find("\n--<<REFLEX_SEP>>--\n")
        .unwrap_or(sql.len() - del_pos);
    let delete_stmt = &sql[del_pos..del_pos + next_sep];

    // The outer column reference in the target DELETE must be the *target*
    // column name (`dem_plan_id`), NOT the intermediate column name (`id`)
    // — the target table doesn't have an `id` column.
    assert!(
        delete_stmt.contains("\"aliased_view\".\"dem_plan_id\""),
        "target DELETE must reference target's `dem_plan_id` column (aliased from dp.id). Got: {}",
        delete_stmt
    );
    assert!(
        !delete_stmt.contains("\"aliased_view\".\"id\""),
        "target DELETE must NOT reference `aliased_view.\"id\"` (target has no `id` col). Got: {}",
        delete_stmt
    );
    // And the affected (__a) side must use the intermediate name (`id`),
    // since the affected table is populated from intermediate naming.
    assert!(
        delete_stmt.contains("__a.\"id\""),
        "affected-side reference must use intermediate naming (`__a.\"id\"`). Got: {}",
        delete_stmt
    );

    // The same generated SQL must also have the intermediate dead-cleanup
    // DELETE qualified with the intermediate table — not bare `"id" = __a."id"`.
    let int_pos = sql
        .find("DELETE FROM \"__reflex_intermediate_aliased_view\"")
        .expect("intermediate DELETE must be present");
    let int_next = sql[int_pos..]
        .find("\n--<<REFLEX_SEP>>--\n")
        .unwrap_or(sql.len() - int_pos);
    let int_stmt = &sql[int_pos..int_pos + int_next];
    assert!(
        int_stmt.contains("\"__reflex_intermediate_aliased_view\".\"id\""),
        "intermediate DELETE must qualify outer col with intermediate table. Got: {}",
        int_stmt
    );

    // The INSERT INTO target via end_query (FROM intermediate) appends the
    // EXISTS filter — its outer is the intermediate, so the appended
    // predicate must qualify with the intermediate table.
    let ins_pos = sql
        .find("INSERT INTO \"aliased_view\"")
        .expect("target INSERT must be present");
    let ins_tail = &sql[ins_pos..];
    let ins_next = ins_tail
        .find("\n--<<REFLEX_SEP>>--\n")
        .unwrap_or(ins_tail.len());
    let ins_stmt = &ins_tail[..ins_next];
    assert!(
        ins_stmt.contains("\"__reflex_intermediate_aliased_view\".\"id\""),
        "target INSERT's appended EXISTS filter must qualify with the intermediate table (end_query's FROM). Got: {}",
        ins_stmt
    );
}

/// Sentinel: scan every EXISTS-on-__a predicate in the generated SQL and
/// verify NONE of them use a bare unqualified outer column reference. This
/// is the property test — even if someone refactors callers later, this
/// keeps the buggy `"col" = __a."col"` shape out of trigger codegen.
#[test]
fn test_null_safe_in_no_unqualified_outer_column_anywhere() {
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_no_unqual_view\" WHERE __ivm_count > 0";

    for op in ["INSERT", "DELETE", "UPDATE"] {
        let sql = reflex_build_delta_sql(
            "no_unqual_view",
            "orders",
            op,
            base_q,
            end_q,
            Some(agg_json.as_str()),
            base_q,
        );
        // The buggy shape is `WHERE "<col>" <op> __a."<col>"` — a bare quoted
        // identifier on the outer side, no `<qualifier>.` prefix. The fixed
        // shape always has `<qualifier>."<col>" <op> __a."<col>"`. We walk
        // every EXISTS clause and require that the byte just BEFORE every
        // outer-side `"<col>"` is `.` (the qualifier separator) — not space
        // or quote or AND. We identify "outer side" as a quoted identifier
        // immediately followed by an operator and `__a.`.
        let mut cursor = 0;
        while let Some(rel) = sql[cursor..].find("EXISTS (SELECT 1 FROM ") {
            let abs = cursor + rel;
            let close = sql[abs..].find(')').expect("EXISTS has closing paren");
            let exists_clause = &sql[abs..abs + close + 1];

            // Find every `__a."` occurrence; for each, walk backwards through
            // the predicate to the outer-side `"<col>"` token.
            let mut search_pos = 0;
            while let Some(rel_a) = exists_clause[search_pos..].find(" __a.\"") {
                let abs_a = search_pos + rel_a;
                // Before __a.<col> there is one of `=` or `IS NOT DISTINCT FROM`.
                // Walk back: skip whitespace, then the operator, then ws, then
                // the outer column token which must end with `"`.
                let before = &exists_clause[..abs_a];
                // Find the operator
                let op_end = before.trim_end().len();
                let head = &before[..op_end];
                let op_start = head
                    .rfind(|c: char| c == '=' || c.is_ascii_uppercase())
                    .map(|i| {
                        // Walk back to start of operator token
                        let bytes = head.as_bytes();
                        let mut k = i;
                        while k > 0
                            && (bytes[k - 1].is_ascii_uppercase()
                                || bytes[k - 1] == b' '
                                || bytes[k - 1] == b'='
                                || bytes[k - 1] == b'M')
                        {
                            k -= 1;
                        }
                        // Skip past trailing whitespace
                        while k < head.len() && (head.as_bytes()[k] == b' ') {
                            k += 1;
                        }
                        k
                    })
                    .unwrap_or(op_end);
                // The outer-side "<col>" ends just before op_start (after
                // whitespace). Find the closing `"` of the outer col token.
                let pre_op = head[..op_start].trim_end();
                assert!(
                    pre_op.ends_with('"'),
                    "op={}: expected outer-side to end with closing quote at byte {} in: {}",
                    op,
                    op_start,
                    exists_clause
                );
                // Find the matching opening `"` and look at the char before it.
                let pre_op_no_close = &pre_op[..pre_op.len() - 1];
                let open_q = pre_op_no_close
                    .rfind('"')
                    .expect("outer col has opening quote");
                let preceding = if open_q == 0 {
                    ' '
                } else {
                    pre_op_no_close.as_bytes()[open_q - 1] as char
                };
                assert_eq!(
                    preceding, '.',
                    "op={}: outer-side `\"col\"` is NOT preceded by `.` (qualifier separator). \
                     This is the 2026-05-13 null_safe_in bug pattern: unqualified outer col \
                     resolves to inner __a scope. Got predicate: {}",
                    op, exists_clause
                );
                search_pos = abs_a + 1;
            }
            cursor = abs + 1;
        }
    }
}

// =============================================================================
// 1.4.6 — INSERT_PROMOTED fast path for Item α OUT→IN flips, gated on
// `plan.source_join_keys` (the JOIN-mapping metadata that confirms the
// trigger source's identity uniquely determines the affected intermediate
// group keys). Re-enable of the earlier-reverted optimization, this time
// with the safety gate that the dd_combo regression exposed.
//
// Contract:
//   * UPDATE trigger body emits 'INSERT_PROMOTED' for OUT→IN.
//   * reflex_build_delta_sql checks plan.source_join_keys.contains(source) —
//     if YES → bulk INSERT + skip target DELETE.
//     if NO  → fall back to MERGE (the standard INSERT path).
//   * Regular INSERT triggers stay on op='INSERT' (no promotion).
// =============================================================================

#[test]
fn test_update_trigger_body_promotes_out_in_to_insert_promoted() {
    let ddls = build_trigger_ddls("orders");
    let combined = ddls.join("\n");
    assert!(
        combined.contains("'INSERT_PROMOTED'"),
        "UPDATE trigger body must emit 'INSERT_PROMOTED' for Item α OUT→IN. Got: {}",
        &combined[..combined.len().min(4000)]
    );
    // DELETE promotion stays on op='DELETE' for now — bulk-DELETE wiring is
    // a separate change.
    assert!(
        combined.contains("'DELETE'"),
        "UPDATE trigger body must still emit 'DELETE' for IN→OUT. Got: {}",
        &combined[..combined.len().min(4000)]
    );
}

fn plan_with_join_keys() -> AggregationPlan {
    let mut p = simple_plan();
    // Pretend the IMV's source `orders` has a JOIN-secondary mapping to
    // the intermediate's `city` GROUP BY column via source col `loc_id`.
    let mut sjk = std::collections::HashMap::new();
    sjk.insert(
        "orders".to_string(),
        vec![("city".to_string(), "loc_id".to_string())],
    );
    p.source_join_keys = sjk;
    p
}

#[test]
fn test_insert_promoted_uses_bulk_insert_when_source_join_keys_present() {
    let plan = plan_with_join_keys();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_pv\" WHERE __ivm_count > 0";
    let sql = reflex_build_delta_sql(
        "pv",
        "orders",
        "INSERT_PROMOTED",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert!(
        !sql.contains("MERGE INTO"),
        "INSERT_PROMOTED with source_join_keys must NOT emit MERGE. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    assert!(
        sql.contains("INSERT INTO \"__reflex_intermediate_pv\""),
        "bulk-INSERT must target the intermediate directly. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    assert!(
        sql.contains("SELECT * FROM \"__reflex_scratch_pv\""),
        "bulk-INSERT must source from scratch directly. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    assert!(
        !sql.contains("DELETE FROM \"pv\""),
        "INSERT_PROMOTED with safety gate satisfied must skip target DELETE. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    assert!(
        sql.contains("INSERT INTO \"pv\""),
        "INSERT_PROMOTED must still INSERT into target. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    // No dead-cleanup either (we only added rows).
    assert!(
        !sql.contains("__ivm_count <= 0"),
        "INSERT_PROMOTED must not emit dead-cleanup DELETE. Got: {}",
        &sql[..sql.len().min(2000)]
    );
}

#[test]
fn test_insert_promoted_falls_back_to_merge_when_no_source_join_keys() {
    // Safety gate: no mapping → bulk-INSERT is unsafe → behave like the
    // standard 'INSERT' path (MERGE + target DELETE+INSERT).
    let plan = simple_plan(); // empty source_join_keys
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_pv\" WHERE __ivm_count > 0";
    let sql = reflex_build_delta_sql(
        "pv",
        "orders",
        "INSERT_PROMOTED",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert!(
        sql.contains("MERGE INTO"),
        "INSERT_PROMOTED without source_join_keys must fall back to MERGE. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    assert!(
        sql.contains("DELETE FROM \"pv\""),
        "fallback path must still emit target DELETE+INSERT. Got: {}",
        &sql[..sql.len().min(2000)]
    );
}

// Path C — EXPLAIN-based fanout dispatch wires only into the UPDATE trigger
// body (the only place Item α can promote to INSERT_PROMOTED). Verify it's
// (a) present in UPDATE bodies, (b) absent from INSERT/DELETE/TRUNCATE,
// (c) gated on `_directional_op = 'INSERT_PROMOTED'`.
#[test]
fn test_update_trigger_emits_path_c_dispatch() {
    let ddls = build_trigger_ddls("orders");
    let upd = ddls
        .iter()
        .find(|d| d.contains("AFTER UPDATE ON"))
        .expect("UPDATE trigger DDL must exist");
    assert!(
        upd.contains("reflex_build_path_c_explain_sql"),
        "UPDATE trigger body must call reflex_build_path_c_explain_sql (Path C). Got: {}",
        &upd[..upd.len().min(4000)]
    );
    assert!(
        upd.contains("_directional_op = 'INSERT_PROMOTED'"),
        "Path C block must be gated on INSERT_PROMOTED. Got: {}",
        &upd[..upd.len().min(4000)]
    );
    assert!(
        upd.contains("EXPLAIN (FORMAT JSON)"),
        "Path C must run EXPLAIN locally in the trigger. Got: {}",
        &upd[..upd.len().min(4000)]
    );
}

#[test]
fn test_ins_del_trunc_trigger_bodies_do_not_emit_path_c() {
    let ddls = build_trigger_ddls("orders");
    for d in &ddls {
        if d.contains("AFTER INSERT ON")
            || d.contains("AFTER DELETE ON")
            || d.contains("AFTER TRUNCATE ON")
        {
            assert!(
                !d.contains("reflex_build_path_c_explain_sql"),
                "Only the UPDATE trigger should emit Path C. Got non-UPDATE body: {}",
                &d[..d.len().min(2000)]
            );
        }
    }
}

// Regression — Path C body must NOT derive its int/scratch/target table names by
// `split_part(_rec.name, '.', 1|2)`. That pattern silently breaks on bare IMV
// names (no schema prefix): `split_part('foo', '.', 2)` returns the empty
// string, so the constructed `"__reflex_intermediate_"` (with a trailing
// underscore and no view part) is not a real relation and the trigger throws
// "relation does not exist" inside the UPDATE path. The fix is to delegate to
// a Rust helper that handles bare and qualified names uniformly (same way
// `intermediate_table_name`, `delta_scratch_table_name`, and
// `quote_identifier` do).
#[test]
fn test_path_c_block_does_not_split_part_imv_name() {
    let ddls = build_trigger_ddls("orders");
    let upd = ddls
        .iter()
        .find(|d| d.contains("AFTER UPDATE ON"))
        .expect("UPDATE trigger DDL must exist");
    assert!(
        !upd.contains("split_part(_rec.name"),
        "Path C must not parse the IMV name with split_part — it breaks on bare \
         (un-qualified) names. Replace with a Rust-side name helper. Got: {}",
        &upd[..upd.len().min(4000)]
    );
}

// Regression — Path C body must NOT construct derived relation names by raw
// string concatenation of `'__reflex_intermediate_' || _pc_view` (or analogous
// for scratch / target). Every other site in the codebase routes through
// `safe_identifier`, which truncates and appends an 8-hex hash when the
// formatted name exceeds PG's 63-char NAMEDATALEN. A raw concat therefore
// silently diverges from the actual stored relation name as soon as the IMV
// name pushes `__reflex_intermediate_<bare>` past 63 chars — manifesting as
// "relation does not exist" inside the UPDATE path. The user-reported symptom
// ("hash result differs with/without the schema prefix") is a consequence of
// this: schema-qualified IMVs happen to land short enough to skip the hash,
// bare names of the same view body cross the threshold.
#[test]
fn test_path_c_block_does_not_concat_raw_reflex_names() {
    let ddls = build_trigger_ddls("orders");
    let upd = ddls
        .iter()
        .find(|d| d.contains("AFTER UPDATE ON"))
        .expect("UPDATE trigger DDL must exist");
    for prefix in &[
        "'__reflex_intermediate_'",
        "'__reflex_scratch_'",
        "'\".\"__reflex_intermediate_'",
        "'\".\"__reflex_scratch_'",
    ] {
        assert!(
            !upd.contains(prefix),
            "Path C must not build derived relation names by raw concat of {} — \
             that bypasses safe_identifier's 63-char hash and breaks for long \
             view names. Use a Rust-side name helper instead. Got: {}",
            prefix,
            &upd[..upd.len().min(4000)]
        );
    }
}

#[test]
fn test_regular_insert_still_uses_merge() {
    // op='INSERT' (not promoted) → always MERGE, regardless of metadata.
    let plan = plan_with_join_keys();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_pv\" WHERE __ivm_count > 0";
    let sql = reflex_build_delta_sql(
        "pv",
        "orders",
        "INSERT",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    assert!(
        sql.contains("MERGE INTO"),
        "Regular INSERT must keep MERGE — new fact rows may aggregate into existing groups. Got: {}",
        &sql[..sql.len().min(2000)]
    );
}

// =============================================================================
// 1.4.6 — DELETE_PROMOTED / bulk-DELETE wiring.
//
// For Item α IN→OUT (and regular DELETE) on safety-gated sources: skip the
// scratch fill JOIN, emit two indexed DELETEs (intermediate + target)
// against transition_old via the source_join_keys mapping.
// =============================================================================

#[test]
fn test_update_trigger_body_promotes_in_out_to_delete_promoted() {
    let ddls = build_trigger_ddls("orders");
    let combined = ddls.join("\n");
    assert!(
        combined.contains("'DELETE_PROMOTED'"),
        "UPDATE trigger body must emit 'DELETE_PROMOTED' for Item α IN→OUT. Got: {}",
        &combined[..combined.len().min(4000)]
    );
}

#[test]
fn test_delete_promoted_uses_bulk_delete_when_safety_gate_satisfied() {
    let plan = plan_with_join_keys();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_pv\" WHERE __ivm_count > 0";
    let sql = reflex_build_delta_sql(
        "pv",
        "orders",
        "DELETE_PROMOTED",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    // No MERGE, no scratch fill
    assert!(
        !sql.contains("MERGE INTO"),
        "bulk-DELETE must NOT emit MERGE. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    assert!(
        !sql.contains("__reflex_scratch_pv"),
        "bulk-DELETE must NOT touch scratch table. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    // Indexed DELETE on intermediate, sourcing from transition_old
    assert!(
        sql.contains("DELETE FROM \"__reflex_intermediate_pv\" WHERE \"city\" IN (SELECT \"loc_id\" FROM \"__reflex_old_orders\")"),
        "bulk-DELETE must emit indexed DELETE on intermediate. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    // Indexed DELETE on target
    assert!(
        sql.contains(
            "DELETE FROM \"pv\" WHERE \"city\" IN (SELECT \"loc_id\" FROM \"__reflex_old_orders\")"
        ),
        "bulk-DELETE must emit indexed DELETE on target. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    // No dead-cleanup either (we removed rows directly)
    assert!(
        !sql.contains("__ivm_count <= 0"),
        "bulk-DELETE must NOT emit dead-cleanup. Got: {}",
        &sql[..sql.len().min(2000)]
    );
}

#[test]
fn test_delete_promoted_falls_back_to_merge_when_no_source_join_keys() {
    let plan = simple_plan(); // empty source_join_keys
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_pv\" WHERE __ivm_count > 0";
    let sql = reflex_build_delta_sql(
        "pv",
        "orders",
        "DELETE_PROMOTED",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    // Falls back to standard DELETE path: MERGE Subtract + dead-cleanup + target sync.
    assert!(
        sql.contains("MERGE INTO"),
        "DELETE_PROMOTED without source_join_keys must fall back to MERGE Subtract. Got: {}",
        &sql[..sql.len().min(2000)]
    );
    assert!(
        sql.contains("__ivm_count <= 0"),
        "fallback path must still emit dead-cleanup. Got: {}",
        &sql[..sql.len().min(2000)]
    );
}

// =============================================================================
// 1.4.6 — Path B pre-scratch dispatch in the trigger function body.
//
// Before invoking reflex_build_delta_sql, the trigger function estimates
// |transition rows| / |source reltuples|. If this exceeds the per-IMV /
// session / compiled threshold, it dispatches to reflex_reconcile and
// CONTINUEs to the next IMV — saving the entire scratch-fill cost on
// genuinely sweeping mutations.
// =============================================================================

#[test]
fn test_trigger_function_body_emits_path_b_dispatch() {
    let ddls = build_trigger_ddls("orders");
    let combined = ddls.join("\n");
    // Three triggers (ins, del, upd) — each must carry the Path B block.
    assert!(
        combined.contains("Path B: dispatching"),
        "trigger function body must emit Path B dispatch (look for the RAISE DEBUG marker). Got: {}",
        &combined[..combined.len().min(4000)]
    );
    assert!(
        combined.contains("PERFORM public.reflex_reconcile"),
        "Path B block must call reflex_reconcile on dispatch. Got: {}",
        &combined[..combined.len().min(4000)]
    );
    // EXCEPTION WHEN OTHERS THEN NULL is the safe-fallback guard for the
    // pre-scratch dispatch — if any catalog query fails (source dropped,
    // brand-new table without reltuples), we silently fall through to the
    // standard codegen rather than aborting the trigger.
    assert!(
        combined.contains("EXCEPTION WHEN OTHERS THEN NULL"),
        "Path B block must have a safe-fallback EXCEPTION handler. Got: {}",
        &combined[..combined.len().min(4000)]
    );
}

#[test]
fn test_path_b_reads_per_imv_threshold_chain() {
    let ddls = build_trigger_ddls("orders");
    let combined = ddls.join("\n");
    // The threshold chain mirrors the existing UPDATE post-scratch
    // dispatch: per-IMV wipe_threshold column → reflex.wipe_threshold GUC
    // → compiled default 0.5.
    assert!(
        combined.contains("SELECT wipe_threshold INTO _pre_per_imv"),
        "Path B must read the per-IMV wipe_threshold column. Got: {}",
        &combined[..combined.len().min(4000)]
    );
    assert!(
        combined.contains("current_setting('reflex.wipe_threshold', true)::NUMERIC, 0.5"),
        "Path B must fall through GUC then compiled default. Got: {}",
        &combined[..combined.len().min(4000)]
    );
}

#[test]
fn test_delete_promoted_falls_back_when_end_query_has_group_by() {
    // Target rows aren't 1:1 with intermediate (further GROUP BY in end_query)
    // → bulk-DELETE on intermediate could leave target rows stale.
    let plan = plan_with_join_keys();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let end_q = "SELECT \"region\", SUM(\"__sum_amount\") AS total FROM \"__reflex_intermediate_pv\" WHERE __ivm_count > 0 GROUP BY \"region\"";
    let sql = reflex_build_delta_sql(
        "pv",
        "orders",
        "DELETE_PROMOTED",
        base_q,
        end_q,
        Some(agg_json.as_str()),
        base_q,
    );
    // Must NOT take bulk path
    assert!(
        sql.contains("MERGE INTO"),
        "DELETE_PROMOTED with end_query GROUP BY must fall back to MERGE. Got: {}",
        &sql[..sql.len().min(2000)]
    );
}

#[cfg(test)]
mod delta_sql_snapshots {
    use crate::trigger::reflex_build_delta_sql;

    // Minimal AggregationPlan JSON for an aggregate IMV: GROUP BY `region`, SUM(qty).
    // Two real sources to enable the OJS detection.
    const AGG_JSON_TWO_SOURCES: &str = r#"{
        "is_passthrough": false,
        "group_by_columns": ["region"],
        "group_by_aliases": {},
        "intermediate_columns": [
            {"name":"__sum_qty","pg_type":"NUMERIC","source_aggregate":"SUM","source_arg":"qty","topk_k":null}
        ],
        "needs_ivm_count": true,
        "has_distinct": false,
        "end_query_mappings": [
            {"intermediate_expr":"__sum_qty","output_alias":"qty","aggregate_type":"SUM","cast_type":null}
        ],
        "distinct_columns": [],
        "passthrough_columns": [],
        "passthrough_key_mappings": {},
        "imv_relevant_columns": {},
        "imv_relevant_where": {},
        "source_join_keys": {},
        "not_null_columns": [],
        "output_column_order": [],
        "partition_columns": [],
        "partition_strategy": "",
        "anchor_source": "",
        "partition_join_paths": {},
        "having_clause": null
    }"#;

    // Same as AGG_JSON_TWO_SOURCES but with the GROUP BY key written QUALIFIED
    // (`o.region`), matching what the real planner stores for a join query. The
    // qualifier lets the outer-join-secondary recompute classify the key as
    // primary-side (stable) and scope by it.
    const AGG_JSON_LEFT_JOIN_QUALIFIED: &str = r#"{
        "is_passthrough": false,
        "group_by_columns": ["o.region"],
        "group_by_aliases": {},
        "intermediate_columns": [
            {"name":"__sum_qty","pg_type":"NUMERIC","source_aggregate":"SUM","source_arg":"qty","topk_k":null}
        ],
        "needs_ivm_count": true,
        "has_distinct": false,
        "end_query_mappings": [
            {"intermediate_expr":"__sum_qty","output_alias":"qty","aggregate_type":"SUM","cast_type":null}
        ],
        "distinct_columns": [],
        "passthrough_columns": [],
        "passthrough_key_mappings": {},
        "imv_relevant_columns": {},
        "imv_relevant_where": {},
        "source_join_keys": {},
        "not_null_columns": [],
        "output_column_order": [],
        "partition_columns": [],
        "partition_strategy": "",
        "anchor_source": "",
        "partition_join_paths": {},
        "having_clause": null
    }"#;

    // Minimal passthrough plan with a per-source unique-key mapping.
    const PASSTHROUGH_JSON_WITH_MAPPING: &str = r#"{
        "is_passthrough": true,
        "group_by_columns": [],
        "group_by_aliases": {},
        "intermediate_columns": [],
        "needs_ivm_count": false,
        "has_distinct": false,
        "end_query_mappings": [],
        "distinct_columns": [],
        "passthrough_columns": ["id"],
        "passthrough_key_mappings": {"orders":[["id","id"]]},
        "imv_relevant_columns": {},
        "imv_relevant_where": {},
        "source_join_keys": {},
        "not_null_columns": ["id"],
        "output_column_order": [],
        "partition_columns": [],
        "partition_strategy": "",
        "anchor_source": "",
        "partition_join_paths": {},
        "having_clause": null
    }"#;

    // Single-source aggregate plan (no OJS, no self-join)
    const AGG_JSON_SINGLE_SOURCE: &str = r#"{
        "is_passthrough": false,
        "group_by_columns": ["region"],
        "group_by_aliases": {},
        "intermediate_columns": [
            {"name":"__sum_qty","pg_type":"NUMERIC","source_aggregate":"SUM","source_arg":"qty","topk_k":null}
        ],
        "needs_ivm_count": true,
        "has_distinct": false,
        "end_query_mappings": [
            {"intermediate_expr":"__sum_qty","output_alias":"qty","aggregate_type":"SUM","cast_type":null}
        ],
        "distinct_columns": [],
        "passthrough_columns": [],
        "passthrough_key_mappings": {},
        "imv_relevant_columns": {},
        "imv_relevant_where": {},
        "source_join_keys": {},
        "not_null_columns": ["region"],
        "output_column_order": [],
        "partition_columns": [],
        "partition_strategy": "",
        "anchor_source": "",
        "partition_join_paths": {},
        "having_clause": null
    }"#;

    #[test]
    fn snapshot_self_join_insert_aggregate() {
        // Self-join: source_table appears twice in base_query → full-refresh path
        let base_q = "SELECT a.region, SUM(a.qty) AS qty FROM sales a JOIN sales b ON a.id = b.parent_id GROUP BY a.region";
        let end_q = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v",
            "sales",
            "INSERT",
            base_q,
            end_q,
            Some(AGG_JSON_TWO_SOURCES),
            base_q,
        );
        insta::assert_snapshot!("self_join_insert_aggregate", sql);
    }

    #[test]
    fn snapshot_self_join_delete_passthrough() {
        let base_q = "SELECT a.id, a.qty FROM sales a JOIN sales b ON a.id = b.parent_id";
        let sql = reflex_build_delta_sql(
            "v",
            "sales",
            "DELETE",
            base_q,
            "",
            Some(PASSTHROUGH_JSON_WITH_MAPPING),
            base_q,
        );
        insta::assert_snapshot!("self_join_delete_passthrough", sql);
    }

    #[test]
    fn snapshot_outer_join_secondary_delete_aggregate() {
        // `customers` is secondary in a LEFT JOIN, DELETE on customers. The group
        // key `o.region` is from the primary side, so the recompute scopes by it
        // (qualified so the codegen can tell it is not secondary-derived).
        let base_q = "SELECT o.region, SUM(o.qty) AS qty FROM orders o LEFT JOIN customers c ON c.id = o.customer_id GROUP BY o.region";
        let end_q = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v",
            "customers",
            "DELETE",
            base_q,
            end_q,
            Some(AGG_JSON_LEFT_JOIN_QUALIFIED),
            base_q,
        );
        insta::assert_snapshot!("outer_join_secondary_delete_aggregate", sql);
    }

    #[test]
    fn snapshot_passthrough_insert() {
        let base_q = "SELECT id, qty FROM orders";
        let sql = reflex_build_delta_sql(
            "v",
            "orders",
            "INSERT",
            base_q,
            "",
            Some(PASSTHROUGH_JSON_WITH_MAPPING),
            base_q,
        );
        insta::assert_snapshot!("passthrough_insert", sql);
    }

    #[test]
    fn snapshot_aggregate_insert() {
        let base_q = "SELECT region, SUM(qty) AS qty FROM sales GROUP BY region";
        let end_q = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v",
            "sales",
            "INSERT",
            base_q,
            end_q,
            Some(AGG_JSON_SINGLE_SOURCE),
            base_q,
        );
        insta::assert_snapshot!("aggregate_insert", sql);
    }

    #[test]
    fn snapshot_aggregate_delete() {
        let base_q = "SELECT region, SUM(qty) AS qty FROM sales GROUP BY region";
        let end_q = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v",
            "sales",
            "DELETE",
            base_q,
            end_q,
            Some(AGG_JSON_SINGLE_SOURCE),
            base_q,
        );
        insta::assert_snapshot!("aggregate_delete", sql);
    }

    #[test]
    fn snapshot_aggregate_update_with_dispatch() {
        // UPDATE on a grouped aggregate without MIN/MAX → pending_dispatch path
        let base_q = "SELECT region, SUM(qty) AS qty FROM sales GROUP BY region";
        let end_q = "SELECT region, qty FROM __reflex_int_v GROUP BY region";
        let sql = reflex_build_delta_sql(
            "v",
            "sales",
            "UPDATE",
            base_q,
            end_q,
            Some(AGG_JSON_SINGLE_SOURCE),
            base_q,
        );
        insta::assert_snapshot!("aggregate_update_dispatch", sql);
    }

    #[test]
    fn snapshot_aggregate_epilogue_no_group_by() {
        // Aggregate IMV with no GROUP BY (global aggregate) → else branch of epilogue
        let plan = r#"{
            "is_passthrough": false,
            "group_by_columns": [],
            "group_by_aliases": {},
            "intermediate_columns": [
                {"name":"__sum_qty","pg_type":"NUMERIC","source_aggregate":"SUM","source_arg":"qty","topk_k":null}
            ],
            "needs_ivm_count": false,
            "has_distinct": false,
            "end_query_mappings": [
                {"intermediate_expr":"__sum_qty","output_alias":"qty","aggregate_type":"SUM","cast_type":null}
            ],
            "distinct_columns": [], "passthrough_columns": [], "passthrough_key_mappings": {},
            "imv_relevant_columns": {}, "imv_relevant_where": {}, "source_join_keys": {}, "not_null_columns": [],
            "output_column_order": [],
            "partition_columns": [], "partition_strategy": "", "anchor_source": "", "partition_join_paths": {},
            "having_clause": null
        }"#;
        let base_q = "SELECT SUM(qty) AS qty FROM sales";
        let end_q = "SELECT qty FROM __reflex_int_v";
        let sql = reflex_build_delta_sql("v", "sales", "INSERT", base_q, end_q, Some(plan), base_q);
        insta::assert_snapshot!("aggregate_epilogue_no_group_by", sql);
    }
}
