use super::*;
use crate::aggregation::{AggregationPlan, EndQueryMapping, IntermediateColumn};

fn sample_plan() -> AggregationPlan {
    AggregationPlan {
        group_by_columns: vec!["city".to_string()],
        intermediate_columns: vec![
            IntermediateColumn {
                name: "__sum_amount".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "amount".to_string(),
                topk_k: None,
            },
            IntermediateColumn {
                name: "__count_star".to_string(),
                pg_type: "BIGINT".to_string(),
                source_aggregate: "COUNT".to_string(),
                source_arg: "*".to_string(),
                topk_k: None,
            },
        ],
        end_query_mappings: vec![
            EndQueryMapping {
                intermediate_expr: "__sum_amount".to_string(),
                output_alias: "total".to_string(),
                aggregate_type: "SUM".to_string(),
                cast_type: None,
            },
            EndQueryMapping {
                intermediate_expr: "__count_star".to_string(),
                output_alias: "cnt".to_string(),
                aggregate_type: "COUNT".to_string(),
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
    }
}

fn sample_types() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("city".to_string(), "TEXT".to_string());
    m.insert("amount".to_string(), "NUMERIC".to_string());
    m
}

#[test]
fn test_intermediate_table_ddl() {
    let plan = sample_plan();
    let types = sample_types();
    let ddl = build_intermediate_table_ddl("test_view", &plan, &types, false).unwrap();
    assert!(ddl.contains("CREATE UNLOGGED TABLE"));
    assert!(ddl.contains("__reflex_intermediate_test_view"));
    assert!(ddl.contains("\"city\" TEXT"));
    assert!(ddl.contains("\"__sum_amount\" NUMERIC DEFAULT 0"));
    assert!(ddl.contains("\"__count_star\" BIGINT DEFAULT 0"));
    assert!(ddl.contains("__ivm_count BIGINT DEFAULT 0"));
    // No PRIMARY KEY — we use a hash index created separately via build_indexes_ddl
    assert!(!ddl.contains("PRIMARY KEY"));
}

/// HOT-update optimization: MERGE WHEN MATCHED UPDATE rewrites only the
/// aggregate columns (none of which are indexed). With fillfactor=70, those
/// updates stay on the same heap page → HOT fires → zero index writes per
/// touched row. Bench (perftest A→C, 47K rows): 691ms → 75ms (9×).
#[test]
fn test_intermediate_table_ddl_has_fillfactor_70() {
    let plan = sample_plan();
    let types = sample_types();
    let ddl = build_intermediate_table_ddl("test_view", &plan, &types, false).unwrap();
    assert!(
        ddl.contains("fillfactor=70"),
        "intermediate table must set fillfactor=70 to enable HOT updates on MERGE: {}",
        ddl
    );
}

#[test]
fn test_target_table_ddl_has_fillfactor_70() {
    let plan = sample_plan();
    let types = sample_types();
    let ddl = build_target_table_ddl("test_view", &plan, &types, false);
    assert!(
        ddl.contains("fillfactor=70"),
        "target table must set fillfactor=70 to enable HOT updates on MERGE: {}",
        ddl
    );
}

#[test]
fn test_intermediate_table_ddl_logged() {
    let plan = sample_plan();
    let types = sample_types();
    let ddl = build_intermediate_table_ddl("test_view", &plan, &types, true).unwrap();
    assert!(ddl.contains("CREATE TABLE"));
    assert!(!ddl.contains("UNLOGGED"));
}

#[test]
fn test_target_table_ddl() {
    let plan = sample_plan();
    let types = sample_types();
    let ddl = build_target_table_ddl("test_view", &plan, &types, false);
    assert!(ddl.contains("CREATE UNLOGGED TABLE"));
    assert!(ddl.contains("\"test_view\""));
    assert!(ddl.contains("\"city\" TEXT"));
    assert!(ddl.contains("\"total\" NUMERIC"));
    assert!(ddl.contains("\"cnt\" BIGINT"));
}

#[test]
fn test_target_table_ddl_logged() {
    let plan = sample_plan();
    let types = sample_types();
    let ddl = build_target_table_ddl("test_view", &plan, &types, true);
    assert!(ddl.contains("CREATE TABLE"));
    assert!(!ddl.contains("UNLOGGED"));
}

#[test]
fn test_trigger_ddls_format() {
    let ddls = build_trigger_ddls("orders");
    assert_eq!(ddls.len(), 4);
    // INSERT trigger: references transition table directly, loops over IMVs
    assert!(ddls[0].contains("AFTER INSERT ON orders"));
    assert!(ddls[0].contains("REFERENCING NEW TABLE AS"));
    assert!(ddls[0].contains("reflex_build_delta_sql"));
    assert!(ddls[0].contains("'INSERT'"));
    assert!(ddls[0].contains("FOR _rec IN"));
    assert!(ddls[0].contains("__reflex_ins_trigger_on_orders"));
    // No temp table copy (transition tables used directly)
    assert!(!ddls[0].contains("CREATE TEMP TABLE"));
    // DELETE trigger
    assert!(ddls[1].contains("AFTER DELETE ON orders"));
    assert!(ddls[1].contains("'DELETE'"));
    // UPDATE trigger
    assert!(ddls[2].contains("AFTER UPDATE ON orders"));
    assert!(ddls[2].contains("'UPDATE'"));
    // TRUNCATE trigger
    assert!(ddls[3].contains("AFTER TRUNCATE ON orders"));
    assert!(ddls[3].contains("reflex_build_truncate_sql"));
    assert!(ddls[3].contains("FOR _rec IN"));
}

#[test]
fn test_indexes_ddl_multiple_group_by() {
    let mut plan = sample_plan();
    plan.group_by_columns = vec!["city".to_string(), "year".to_string()];
    let indexes = build_indexes_ddl("test_view", &plan);
    // Composite UNIQUE intermediate index + target index. No per-column
    // single-col indexes (dropped in 1.4.4 — bench-proved vestigial because
    // every pg_reflex query path uses the full composite key).
    assert_eq!(indexes.len(), 2);
    assert!(indexes[0].contains("idx__reflex_int_"));
    assert!(!indexes[0].contains("USING hash")); // multi-column uses B-tree
                                                 // 1.4.4: composite intermediate index is now UNIQUE NULLS NOT DISTINCT so
                                                 // (a) the MERGE planner can pick it over a hash-join + seq-scan plan, and
                                                 // (b) the one-row-per-group invariant the MERGE codegen assumes is
                                                 // defensively enforced.
    assert!(
        indexes[0].contains("CREATE UNIQUE INDEX"),
        "multi-col composite must be UNIQUE: {}",
        indexes[0]
    );
    assert!(
        indexes[0].contains("NULLS NOT DISTINCT"),
        "multi-col composite must allow NULL group keys to coexist as one group: {}",
        indexes[0]
    );
    assert!(indexes[0].contains("\"city\", \"year\""));
    assert!(indexes[1].contains("idx__reflex_target_"));
}

/// 1.4.4: assert NO per-column indexes are emitted on the intermediate
/// table for multi-group-by IMVs. The composite UNIQUE NULLS NOT DISTINCT
/// btree at index[0] covers every MERGE/EXISTS/JOIN query path
/// pg_reflex emits (every code path uses the full group-by key). The
/// individual indexes pre-1.4.4 added ~480ms of index maintenance per
/// 47K-row UPDATE cycle on rb.fcast (bench: A 691ms → B 208ms).
#[test]
fn test_indexes_ddl_no_per_column_intermediate_indexes() {
    let mut plan = sample_plan();
    plan.group_by_columns = vec!["city".to_string(), "year".to_string(), "month".to_string()];
    let indexes = build_indexes_ddl("test_view", &plan);
    // Should be: composite intermediate + target. No per-column.
    assert_eq!(
        indexes.len(),
        2,
        "expected only composite + target indexes, got {:?}",
        indexes
    );
    for (i, idx) in indexes.iter().enumerate() {
        assert!(
            !idx.contains("idx__reflex_test_view_0")
                && !idx.contains("idx__reflex_test_view_1")
                && !idx.contains("idx__reflex_test_view_2"),
            "index[{}] looks like a per-column index that 1.4.4 dropped: {}",
            i,
            idx
        );
    }
}

#[test]
fn test_indexes_ddl_single_group_by_stays_non_unique_hash() {
    // Single-column groups keep using a hash index — hash indexes don't
    // support uniqueness constraints in PG, and `=` lookups (the only access
    // pattern MERGE needs for a single col) are optimally served by hash.
    let plan = sample_plan();
    let indexes = build_indexes_ddl("test_view", &plan);
    assert_eq!(indexes.len(), 2);
    assert!(indexes[0].contains("USING hash"));
    assert!(
        !indexes[0].contains("UNIQUE"),
        "single-col hash must not be UNIQUE (PG hash indexes don't support it): {}",
        indexes[0]
    );
    assert!(indexes[0].contains("\"city\""));
}

#[test]
fn test_indexes_ddl_single_group_by() {
    let plan = sample_plan();
    let indexes = build_indexes_ddl("test_view", &plan);
    // hash index on intermediate (single column) + target table index
    assert_eq!(indexes.len(), 2);
    assert!(indexes[0].contains("USING hash"));
    assert!(indexes[0].contains("\"city\""));
    assert!(indexes[1].contains("idx__reflex_target_"));
}

#[test]
fn test_no_intermediate_for_passthrough() {
    let plan = AggregationPlan {
        group_by_columns: vec![],
        intermediate_columns: vec![],
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
    let types = HashMap::new();
    assert!(build_intermediate_table_ddl("test_view", &plan, &types, false).is_none());
}

#[test]
fn test_resolve_column_type() {
    let mut types = HashMap::new();
    types.insert("emp.salary".to_string(), "integer".to_string());
    types.insert("name".to_string(), "varchar".to_string());

    assert_eq!(resolve_column_type("emp.salary", &types, "TEXT"), "integer");
    assert_eq!(resolve_column_type("salary", &types, "TEXT"), "integer");
    assert_eq!(resolve_column_type("name", &types, "TEXT"), "varchar");
    assert_eq!(resolve_column_type("unknown", &types, "TEXT"), "NUMERIC");
}

// ========================================================================
// #12a — deferred UPDATE trigger body must check where_predicate
// ========================================================================

#[test]
fn test_deferred_upd_body_contains_where_predicate_check() {
    let ddls = build_deferred_trigger_ddls("orders");
    let upd_ddl = &ddls[2];
    assert!(
        upd_ddl.contains("_rec.where_predicate IS NOT NULL"),
        "deferred UPDATE DDL must check where_predicate before proceeding: {}",
        &upd_ddl[..upd_ddl.len().min(400)]
    );
    let pred_pos = upd_ddl
        .find("where_predicate")
        .expect("where_predicate must appear in UPDATE DDL");
    let lock_pos = upd_ddl
        .find("pg_advisory_xact_lock")
        .expect("advisory lock must appear in UPDATE DDL");
    assert!(
        pred_pos < lock_pos,
        "predicate check must come before pg_advisory_xact_lock"
    );
}

#[test]
fn test_deferred_upd_body_declares_pred_match() {
    let ddls = build_deferred_trigger_ddls("orders");
    let upd_ddl = &ddls[2];
    assert!(
        upd_ddl.contains("_pred_match BOOLEAN"),
        "deferred UPDATE DDL DECLARE section must include _pred_match BOOLEAN: {}",
        &upd_ddl[..upd_ddl.len().min(400)]
    );
}

// ── Phase B (#1): Algebraic BOOL_OR schema ──

#[test]
fn test_intermediate_ddl_bool_or_emits_bigint_counters() {
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
    };
    let types = HashMap::new();
    let ddl = build_intermediate_table_ddl("test_view", &plan, &types, false).unwrap();
    assert!(
        ddl.contains("\"__bool_or_flag_true_count\" BIGINT DEFAULT 0"),
        "true_count must be BIGINT DEFAULT 0: {}",
        ddl
    );
    assert!(
        ddl.contains("\"__bool_or_flag_nonnull_count\" BIGINT DEFAULT 0"),
        "nonnull_count must be BIGINT DEFAULT 0: {}",
        ddl
    );
    assert!(
        !ddl.contains("BOOLEAN"),
        "algebraic BOOL_OR must not produce a BOOLEAN intermediate column: {}",
        ddl
    );
}
