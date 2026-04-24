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
            },
            IntermediateColumn {
                name: "__sum_amount".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "amount".to_string(),
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
    };
    let orig_base = "SELECT city AS \"city\", MIN(price) AS \"__min_price\", SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM orders GROUP BY city";
    let sql = build_min_max_recompute_sql("intermediate", &plan, orig_base);
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
            },
            IntermediateColumn {
                name: "__bool_or_caav_product_id_is_not_null_nonnull_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (caav.product_id IS NOT NULL) IS NOT NULL THEN 1 ELSE 0 END"
                    .to_string(),
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
    };
    let orig_base = "SELECT s.product_id AS \"product_id\", SUM(CASE WHEN (caav.product_id IS NOT NULL) THEN 1 ELSE 0 END) AS \"__bool_or_caav_product_id_is_not_null_true_count\", COUNT(*) AS __ivm_count FROM sales_simulation s LEFT JOIN current_assortment_activity caav ON caav.product_id = s.product_id GROUP BY s.product_id";
    let sql = build_min_max_recompute_sql("intermediate", &plan, orig_base);
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
    let sql = build_min_max_recompute_sql("intermediate", &plan, orig_base);
    assert!(sql.is_none());
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
    let ddls = build_deferred_trigger_ddls(long_src);
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
        sql.contains("INSERT INTO \"__reflex_affected_test_view\" SELECT DISTINCT"),
        "affected groups must be populated from scratch: {}",
        &sql[..sql.len().min(400)]
    );
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
    let result = inject_affected_filter_before_group_by(input, &["\"grp\"".to_string()], "aff_tbl");
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
            },
            IntermediateColumn {
                name: "__bool_or_flag_nonnull_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (flag) IS NOT NULL THEN 1 ELSE 0 END".to_string(),
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
            },
            IntermediateColumn {
                name: "__bool_or_flag_nonnull_count".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "CASE WHEN (flag) IS NOT NULL THEN 1 ELSE 0 END".to_string(),
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

/// Regression guard: the aggregate branch must also keep transition tables
/// confined to sanctioned scratch-populate statements (Phase B's fix).
#[test]
fn test_aggregate_delta_sql_has_no_transition_leaks() {
    let plan = simple_plan();
    let agg_json = serde_json::to_string(&plan).unwrap();
    let base_q = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"__reflex_new_t\" GROUP BY city";
    let end_q = "SELECT \"city\", \"__sum_amount\" AS total FROM \"__reflex_intermediate_v\" WHERE __ivm_count > 0";

    for op in ["INSERT", "DELETE", "UPDATE"] {
        let sql =
            reflex_build_delta_sql("v", "t", op, base_q, end_q, Some(agg_json.as_str()), base_q);
        assert_no_transition_leaks(&sql, &format!("aggregate {op}"));
    }
}
