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
        imv_relevant_columns: std::collections::HashMap::new(),
        imv_relevant_where: std::collections::HashMap::new(),
        source_join_keys: std::collections::HashMap::new(),
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
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

/// Partitioned parents must never be UNLOGGED: PG18 rejects the combination
/// outright ("partitioned tables cannot be unlogged") and pre-PG18 silently
/// ignored the keyword.  Storage mode is carried by the partition children
/// (see `build_partition_child_ddl_pair`).
#[test]
fn test_intermediate_partitioned_parent_is_never_unlogged() {
    let mut plan = sample_plan();
    plan.partition_columns = vec!["city".to_string()];
    plan.partition_strategy = "LIST".to_string();
    let types = sample_types();
    let ddl = build_intermediate_table_ddl("test_view", &plan, &types, false).unwrap();
    assert!(
        !ddl.contains("UNLOGGED"),
        "partitioned intermediate parent must not carry UNLOGGED: {}",
        ddl
    );
    assert!(ddl.contains("PARTITION BY"));
}

#[test]
fn test_target_partitioned_parent_is_never_unlogged() {
    let mut plan = sample_plan();
    plan.partition_columns = vec!["city".to_string()];
    plan.partition_strategy = "LIST".to_string();
    let types = sample_types();
    let ddl = build_target_table_ddl("test_view", &plan, &types, false);
    assert!(
        !ddl.contains("UNLOGGED"),
        "partitioned target parent must not carry UNLOGGED: {}",
        ddl
    );
    assert!(ddl.contains("PARTITION BY"));
}

#[test]
fn test_trigger_ddls_format() {
    let ddls = build_trigger_ddls("orders");
    // 4 trigger DDL blocks + 4 member registration blocks = 8 total
    assert_eq!(ddls.len(), 8);
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
    // Member registration blocks (indices 4-7)
    assert!(ddls[4].contains("ALTER EXTENSION pg_reflex ADD FUNCTION"));
    assert!(ddls[5].contains("ALTER EXTENSION pg_reflex ADD FUNCTION"));
    assert!(ddls[6].contains("ALTER EXTENSION pg_reflex ADD FUNCTION"));
    assert!(ddls[7].contains("ALTER EXTENSION pg_reflex ADD FUNCTION"));
}

#[test]
fn test_deferred_flush_dispatch_is_transaction_local() {
    // Regression: the COMMIT-time dispatcher is a FOR EACH ROW constraint
    // trigger, so NEW.source_table is exactly the source the *current*
    // transaction staged. It must flush only that source. The previous body
    // looped `SELECT DISTINCT source_table` over the shared, cross-schema
    // public.__reflex_deferred_pending queue, so a commit in one schema would
    // flush — and advisory-lock + TRUNCATE-lock the staging table of — sources
    // owned by other, independent schemas.
    let ddls = build_deferred_flush_ddl();
    let dispatch_fn = ddls
        .iter()
        .find(|d| d.contains("FUNCTION public.__reflex_deferred_flush_fn"))
        .expect("deferred flush function DDL present");
    assert!(
        dispatch_fn.contains("NEW.source_table"),
        "dispatcher must flush only NEW.source_table: {dispatch_fn}"
    );
    assert!(
        !dispatch_fn.contains("SELECT DISTINCT source_table"),
        "dispatcher must not scan the global pending queue: {dispatch_fn}"
    );
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
        imv_relevant_columns: std::collections::HashMap::new(),
        imv_relevant_where: std::collections::HashMap::new(),
        source_join_keys: std::collections::HashMap::new(),
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
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
    let cols = vec!["id".to_string(), "amount".to_string()];
    let ddls = build_deferred_trigger_ddls("orders", &cols);
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
    let cols = vec!["id".to_string(), "amount".to_string()];
    let ddls = build_deferred_trigger_ddls("orders", &cols);
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
        imv_relevant_columns: std::collections::HashMap::new(),
        imv_relevant_where: std::collections::HashMap::new(),
        source_join_keys: std::collections::HashMap::new(),
        partition_columns: Vec::new(),
        partition_strategy: String::new(),
        anchor_source: String::new(),
        partition_join_paths: std::collections::HashMap::new(),
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

// ========================================================================
// Item α (2026-05-15) — Directional UPDATE dispatch.
//
// The UPDATE trigger function body must contain a directional probe that
// reads both transition tables (`__reflex_old_<src>` / `__reflex_new_<src>`),
// applies the IMV's relevant WHERE predicate (when known), and routes to
// reflex_build_delta_sql with op='INSERT' (NEW-only), 'DELETE' (OLD-only),
// or 'UPDATE' (both). The probe is gated on the IMV having a non-empty
// `imv_relevant_columns[source]` entry (same gate as the 1.4.5 filter-aware
// spurious-skip block).
// ========================================================================

#[test]
fn test_upd_body_declares_directional_probe_locals() {
    let ddls = build_trigger_ddls("orders");
    let upd_ddl = &ddls[2];
    assert!(
        upd_ddl.contains("_old_has BOOLEAN"),
        "UPDATE trigger DECLARE section must include _old_has: {}",
        &upd_ddl[..upd_ddl.len().min(800)]
    );
    assert!(
        upd_ddl.contains("_new_has BOOLEAN"),
        "UPDATE trigger DECLARE section must include _new_has: {}",
        &upd_ddl[..upd_ddl.len().min(800)]
    );
    assert!(
        upd_ddl.contains("_directional_op TEXT"),
        "UPDATE trigger DECLARE section must include _directional_op TEXT: {}",
        &upd_ddl[..upd_ddl.len().min(800)]
    );
}

#[test]
fn test_upd_body_probes_both_transition_tables() {
    let ddls = build_trigger_ddls("orders");
    let upd_ddl = &ddls[2];
    // The probe must reference both transition tables. Their canonical
    // names are formed via transition_*_table_name(source).
    assert!(
        upd_ddl.contains("__reflex_old_orders"),
        "UPDATE trigger directional probe must read __reflex_old_orders: {}",
        &upd_ddl[..upd_ddl.len().min(2000)]
    );
    assert!(
        upd_ddl.contains("__reflex_new_orders"),
        "UPDATE trigger directional probe must read __reflex_new_orders: {}",
        &upd_ddl[..upd_ddl.len().min(2000)]
    );
}

#[test]
fn test_upd_body_passes_directional_op_to_build_delta_sql() {
    let ddls = build_trigger_ddls("orders");
    let upd_ddl = &ddls[2];
    // The reflex_build_delta_sql call in the UPDATE trigger must pass
    // _directional_op (the runtime-chosen op) — NOT the literal 'UPDATE'.
    // INSERT and DELETE triggers continue to pass their literal op.
    assert!(
        upd_ddl.contains("reflex_build_delta_sql"),
        "UPDATE trigger must still call reflex_build_delta_sql: {}",
        &upd_ddl[..upd_ddl.len().min(800)]
    );
    // Find the build_delta_sql call site and inspect its op argument.
    let call_idx = upd_ddl.find("reflex_build_delta_sql").unwrap();
    let after_call = &upd_ddl[call_idx..];
    assert!(
        after_call.contains("_directional_op"),
        "UPDATE trigger must pass _directional_op to reflex_build_delta_sql: {}",
        &after_call[..after_call.len().min(400)]
    );
}

#[test]
fn test_ins_body_still_passes_literal_op() {
    // INSERT and DELETE triggers are single-direction by construction and
    // must NOT exercise the directional dispatch *logic* (no probe block,
    // no assignment to _directional_op). The DECLARE section may carry the
    // unused locals — that is a body_core artifact and harmless.
    let ddls = build_trigger_ddls("orders");
    let ins_ddl = &ddls[0];
    let del_ddl = &ddls[1];
    let ins_call_idx = ins_ddl.find("reflex_build_delta_sql").unwrap();
    let ins_after_call = &ins_ddl[ins_call_idx..];
    assert!(
        ins_after_call.contains("'INSERT'"),
        "INSERT trigger must pass 'INSERT' literal to reflex_build_delta_sql: {}",
        &ins_after_call[..ins_after_call.len().min(400)]
    );
    assert!(
        !ins_ddl.contains("_directional_op :="),
        "INSERT trigger must NOT contain probe logic (_directional_op assignment): {}",
        &ins_ddl[..ins_ddl.len().min(1000)]
    );
    let del_call_idx = del_ddl.find("reflex_build_delta_sql").unwrap();
    let del_after_call = &del_ddl[del_call_idx..];
    assert!(
        del_after_call.contains("'DELETE'"),
        "DELETE trigger must pass 'DELETE' literal to reflex_build_delta_sql: {}",
        &del_after_call[..del_after_call.len().min(400)]
    );
    assert!(
        !del_ddl.contains("_directional_op :="),
        "DELETE trigger must NOT contain probe logic (_directional_op assignment): {}",
        &del_ddl[..del_ddl.len().min(1000)]
    );
}

#[test]
fn test_upd_body_directional_probe_gated_on_relevant_columns() {
    // The probe must be gated on `imv_relevant_columns[source]` having
    // entries — the same gate used by the 1.4.5 filter-aware spurious-skip
    // block. IMVs without that metadata (CTE-using, SELECT * passthrough)
    // fall through to today's UPDATE path.
    let ddls = build_trigger_ddls("orders");
    let upd_ddl = &ddls[2];
    assert!(
        upd_ddl.contains("imv_relevant_columns"),
        "UPDATE trigger probe must check imv_relevant_columns gate: {}",
        &upd_ddl[..upd_ddl.len().min(2000)]
    );
}

#[test]
fn test_upd_body_directional_probe_uses_relevant_where_when_present() {
    // When imv_relevant_where[source] is set, the probe must apply that
    // WHERE clause to the transition tables. When empty/NULL, the probe
    // checks raw transition-table non-emptiness.
    let ddls = build_trigger_ddls("orders");
    let upd_ddl = &ddls[2];
    assert!(
        upd_ddl.contains("imv_relevant_where"),
        "UPDATE trigger probe must read imv_relevant_where: {}",
        &upd_ddl[..upd_ddl.len().min(2000)]
    );
}

#[test]
fn test_upd_body_probe_after_filter_skip() {
    // The directional probe must come AFTER the filter-aware spurious-skip
    // block (which can CONTINUE the loop if NEW≡OLD post-filter). No
    // point probing direction on a multiset we are about to skip entirely.
    // We look for the assignment `_directional_op :=` (the probe code),
    // not the DECLARE-section token.
    let ddls = build_trigger_ddls("orders");
    let upd_ddl = &ddls[2];
    let skip_pos = upd_ddl
        .find("EXCEPT ALL")
        .expect("filter-skip block must appear in UPDATE DDL");
    let probe_pos = upd_ddl
        .find("_directional_op :=")
        .expect("directional probe assignment must appear in UPDATE DDL");
    assert!(
        skip_pos < probe_pos,
        "filter-skip block (EXCEPT ALL) must precede directional probe (_directional_op := …): \
         skip at {} but probe at {}",
        skip_pos,
        probe_pos
    );
}

// ========================================================================
// MIN/MAX type resolution for qualified columns (bug fix)
// ========================================================================

#[test]
fn test_target_table_qualified_max_timestamptz() {
    // Reproduces the bug: qualified MAX(e.ts) where ts is TIMESTAMPTZ
    // The intermediate column __max_e_ts is built with source_arg="e.ts"
    // and resolved correctly to TIMESTAMPTZ.
    // The target column must also be TIMESTAMPTZ, not NUMERIC.
    let mut plan = sample_plan();
    plan.group_by_columns = vec!["grp".to_string()];
    plan.intermediate_columns = vec![IntermediateColumn {
        name: "__max_e_ts".to_string(),
        pg_type: "NUMERIC".to_string(), // Default, will be resolved
        source_aggregate: "MAX".to_string(),
        source_arg: "e.ts".to_string(),
        topk_k: None,
    }];
    plan.end_query_mappings = vec![EndQueryMapping {
        intermediate_expr: "__max_e_ts".to_string(),
        output_alias: "max_ts".to_string(),
        aggregate_type: "MAX".to_string(),
        cast_type: None,
    }];

    let mut types = sample_types();
    types.insert("e.ts".to_string(), "TIMESTAMPTZ".to_string());
    types.insert("grp".to_string(), "TEXT".to_string());

    let ddl = build_target_table_ddl("test_view", &plan, &types, false);

    // The target column max_ts must be TIMESTAMPTZ, not NUMERIC
    assert!(
        ddl.contains("\"max_ts\" TIMESTAMPTZ"),
        "target column max_ts must resolve to TIMESTAMPTZ, not NUMERIC: {}",
        ddl
    );
    assert!(
        !ddl.contains("\"max_ts\" NUMERIC"),
        "target column max_ts must NOT be NUMERIC: {}",
        ddl
    );
}

#[test]
fn test_target_table_qualified_min_date() {
    let mut plan = sample_plan();
    plan.group_by_columns = vec!["grp".to_string()];
    plan.intermediate_columns = vec![IntermediateColumn {
        name: "__min_e_order_date".to_string(),
        pg_type: "NUMERIC".to_string(),
        source_aggregate: "MIN".to_string(),
        source_arg: "e.order_date".to_string(),
        topk_k: None,
    }];
    plan.end_query_mappings = vec![EndQueryMapping {
        intermediate_expr: "__min_e_order_date".to_string(),
        output_alias: "min_date".to_string(),
        aggregate_type: "MIN".to_string(),
        cast_type: None,
    }];

    let mut types = sample_types();
    types.insert("e.order_date".to_string(), "DATE".to_string());
    types.insert("grp".to_string(), "TEXT".to_string());

    let ddl = build_target_table_ddl("test_view", &plan, &types, false);

    // The target column min_date must be DATE, not NUMERIC
    assert!(
        ddl.contains("\"min_date\" DATE"),
        "target column min_date must resolve to DATE, not NUMERIC: {}",
        ddl
    );
    assert!(
        !ddl.contains("\"min_date\" NUMERIC"),
        "target column min_date must NOT be NUMERIC: {}",
        ddl
    );
}

#[test]
fn test_target_table_qualified_max_text() {
    let mut plan = sample_plan();
    plan.group_by_columns = vec!["grp".to_string()];
    plan.intermediate_columns = vec![IntermediateColumn {
        name: "__max_e_code".to_string(),
        pg_type: "NUMERIC".to_string(),
        source_aggregate: "MAX".to_string(),
        source_arg: "e.code".to_string(),
        topk_k: None,
    }];
    plan.end_query_mappings = vec![EndQueryMapping {
        intermediate_expr: "__max_e_code".to_string(),
        output_alias: "max_code".to_string(),
        aggregate_type: "MAX".to_string(),
        cast_type: None,
    }];

    let mut types = sample_types();
    types.insert("e.code".to_string(), "TEXT".to_string());
    types.insert("grp".to_string(), "TEXT".to_string());

    let ddl = build_target_table_ddl("test_view", &plan, &types, false);

    // The target column max_code must be TEXT, not NUMERIC
    assert!(
        ddl.contains("\"max_code\" TEXT"),
        "target column max_code must resolve to TEXT, not NUMERIC: {}",
        ddl
    );
    assert!(
        !ddl.contains("\"max_code\" NUMERIC"),
        "target column max_code must NOT be NUMERIC: {}",
        ddl
    );
}

#[test]
fn test_target_table_unqualified_max_still_works() {
    // Regression test: unqualified MAX(ts) should still work
    // (this always worked because stripping __max_ leaves "ts" which resolves)
    let mut plan = sample_plan();
    plan.group_by_columns = vec!["grp".to_string()];
    plan.intermediate_columns = vec![IntermediateColumn {
        name: "__max_ts".to_string(),
        pg_type: "NUMERIC".to_string(),
        source_aggregate: "MAX".to_string(),
        source_arg: "ts".to_string(),
        topk_k: None,
    }];
    plan.end_query_mappings = vec![EndQueryMapping {
        intermediate_expr: "__max_ts".to_string(),
        output_alias: "max_ts".to_string(),
        aggregate_type: "MAX".to_string(),
        cast_type: None,
    }];

    let mut types = sample_types();
    types.insert("ts".to_string(), "TIMESTAMPTZ".to_string());
    types.insert("grp".to_string(), "TEXT".to_string());

    let ddl = build_target_table_ddl("test_view", &plan, &types, false);

    // The target column max_ts must be TIMESTAMPTZ (backward compatible)
    assert!(
        ddl.contains("\"max_ts\" TIMESTAMPTZ"),
        "target column max_ts (unqualified) must resolve to TIMESTAMPTZ: {}",
        ddl
    );
}

#[test]
fn test_flush_fn_is_public_qualified_and_member_registered() {
    let ddls = crate::schema_builder::build_deferred_flush_ddl();
    let joined = ddls.join("\n");
    assert!(
        joined.contains("CREATE OR REPLACE FUNCTION public.__reflex_deferred_flush_fn()"),
        "flush fn must be created in public: {joined}"
    );
    assert!(
        joined.contains("EXECUTE FUNCTION public.__reflex_deferred_flush_fn()"),
        "constraint trigger must bind the public copy: {joined}"
    );
    assert!(
        joined.contains("ALTER EXTENSION pg_reflex ADD FUNCTION public.__reflex_deferred_flush_fn()"),
        "flush fn must self-register as an extension member: {joined}"
    );
}

#[test]
fn test_immediate_per_source_fns_are_public_qualified_and_member_registered() {
    let ddls = crate::schema_builder::build_trigger_ddls("myschema.orders");
    let joined = ddls.join("\n");
    for op in ["ins", "del", "upd", "trunc"] {
        let fname = format!("__reflex_{op}_trigger_on_myschema_orders");
        assert!(
            joined.contains(&format!("CREATE OR REPLACE FUNCTION public.{fname}()")),
            "{op} fn must be created in public: {joined}"
        );
        assert!(
            joined.contains(&format!("EXECUTE FUNCTION public.{fname}()")),
            "{op} trigger must bind the public copy: {joined}"
        );
        assert!(
            joined.contains(&format!("ALTER EXTENSION pg_reflex ADD FUNCTION public.{fname}()")),
            "{op} fn must self-register as a member: {joined}"
        );
    }
}

#[test]
fn test_deferred_per_source_fns_are_public_qualified_and_member_registered() {
    let cols = vec!["id".to_string(), "amount".to_string()];
    let ddls = crate::schema_builder::build_deferred_trigger_ddls("myschema.orders", &cols);
    let joined = ddls.join("\n");
    for op in ["ins", "del", "upd", "trunc"] {
        let fname = format!("__reflex_{op}_trigger_on_myschema_orders");
        assert!(
            joined.contains(&format!("CREATE OR REPLACE FUNCTION public.{fname}()")),
            "{op} fn must be created in public: {joined}"
        );
        assert!(
            joined.contains(&format!("EXECUTE FUNCTION public.{fname}()")),
            "{op} trigger must bind the public copy: {joined}"
        );
        assert!(
            joined.contains(&format!("ALTER EXTENSION pg_reflex ADD FUNCTION public.{fname}()")),
            "{op} fn must self-register as a member: {joined}"
        );
    }
}
