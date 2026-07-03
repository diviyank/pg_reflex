//! Pure-Rust unit tests for `crate::partition`.
//!
//! These cover the input parser, helper name generation, and
//! `substitute_identifier`.  The catalog-touching paths
//! (`introspect_partition_descriptor`, `reflex_sync_partitions_impl`,
//! `reflex_reconcile_partition_impl`) are exercised by `#[pg_test]`
//! integration tests in `pg_test_partition.rs`.

use super::*;

#[test]
fn test_parse_partition_by_input_none() {
    assert!(parse_partition_by_input(None).is_empty());
}

#[test]
fn test_parse_partition_by_input_empty_vec() {
    assert!(parse_partition_by_input(Some(vec![])).is_empty());
}

#[test]
fn test_parse_partition_by_input_lowercases_and_trims() {
    let v = parse_partition_by_input(Some(vec![
        Some("  Region  ".to_string()),
        Some("DEPT".to_string()),
    ]));
    assert_eq!(v, vec!["region".to_string(), "dept".to_string()]);
}

#[test]
fn test_parse_partition_by_input_drops_null_and_empty() {
    let v = parse_partition_by_input(Some(vec![
        None,
        Some("".to_string()),
        Some("col".to_string()),
        Some("   ".to_string()),
    ]));
    assert_eq!(v, vec!["col".to_string()]);
}

#[test]
fn test_build_partition_by_clause_list() {
    let clause = build_partition_by_clause("LIST", &["region".to_string()]);
    assert_eq!(clause, r#"PARTITION BY LIST ("region")"#);
}

#[test]
fn test_build_partition_by_clause_range_multi() {
    let clause = build_partition_by_clause("range", &["year".to_string(), "month".to_string()]);
    assert_eq!(clause, r#"PARTITION BY RANGE ("year", "month")"#);
}

#[test]
fn test_intermediate_child_name() {
    let n = intermediate_child_name("sales_view", "orders_p_north");
    assert_eq!(n, "__reflex_intermediate_sales_view_orders_p_north");
}

#[test]
fn test_intermediate_child_name_schema_qualified() {
    // schema qualification is stripped — children live inside the IMV's schema
    let n = intermediate_child_name("alp.sales_view", "orders_p1");
    assert_eq!(n, "__reflex_intermediate_sales_view_orders_p1");
}

#[test]
fn test_target_child_name() {
    let n = target_child_name("sales_view", "orders_p1");
    assert_eq!(n, "sales_view_orders_p1");
}

#[test]
fn test_substitute_identifier_simple() {
    let r = substitute_identifier("region = 'NORTH'", "region", "'NORTH'");
    assert_eq!(r, "'NORTH' = 'NORTH'");
}

#[test]
fn test_substitute_identifier_case_insensitive_match() {
    let r = substitute_identifier("Region = 'X'", "region", "'X'");
    assert_eq!(r, "'X' = 'X'");
}

#[test]
fn test_substitute_identifier_preserves_unrelated_words() {
    let r = substitute_identifier("(region_id = 'X' AND regional = 1)", "region", "REPLACED");
    // region_id and regional are different identifiers
    assert_eq!(r, "(region_id = 'X' AND regional = 1)");
}

#[test]
fn test_substitute_identifier_handles_constraint_def_shape() {
    // Realistic pg_get_partition_constraintdef output:
    let def = "((region IS NOT NULL) AND (region = 'NORTH'::text))";
    let r = substitute_identifier(def, "region", "'NORTH'::text");
    assert_eq!(
        r,
        "(('NORTH'::text IS NOT NULL) AND ('NORTH'::text = 'NORTH'::text))"
    );
}

#[test]
fn test_sql_literal_text_doubles_single_quotes() {
    assert_eq!(sql_literal_text("o'reilly"), "'o''reilly'");
}

#[test]
fn test_sync_result_message_basic() {
    let r = SyncResult {
        added_intermediate: 2,
        added_target: 2,
        dropped_intermediate: 0,
        dropped_target: 0,
        preserved_orphans: vec![],
    };
    assert_eq!(r.into_message(), "sync: +2 intermediate, +2 target");
}

#[test]
fn test_sync_result_message_with_drops() {
    let r = SyncResult {
        added_intermediate: 1,
        added_target: 1,
        dropped_intermediate: 1,
        dropped_target: 1,
        preserved_orphans: vec![],
    };
    assert_eq!(
        r.into_message(),
        "sync: +1 intermediate, +1 target, -1 intermediate, -1 target"
    );
}

#[test]
fn test_sync_result_message_with_orphans() {
    let r = SyncResult {
        added_intermediate: 0,
        added_target: 0,
        dropped_intermediate: 0,
        dropped_target: 0,
        preserved_orphans: vec!["x".to_string(), "y".to_string()],
    };
    assert!(r.into_message().contains("preserved orphans: x, y"));
}

#[test]
fn test_swap_partition_name_shape() {
    let n = swap_partition_name("sales_view", "int", "orders_p1");
    assert_eq!(n, "__reflex_swap_int_sales_view_orders_p1");
    let n = swap_partition_name("alp.sales_view", "tgt", "orders_p2");
    assert_eq!(n, "__reflex_swap_tgt_sales_view_orders_p2");
}

#[test]
fn test_build_swap_partition_ddl_shape_aggregate_logged() {
    // Aggregate IMV in LOGGED storage mode — swap creates both intermediate
    // and target swap children, fills both, ATTACHes both.
    let src = PartitionChild {
        bare_name: "orders_p_north".to_string(),
        bound_expr: "FOR VALUES IN ('NORTH')".to_string(),
    };
    let ddl = build_swap_partition_ddl(
        "sales_view",
        &src,
        "((region IS NOT NULL) AND (region = 'NORTH'::text))",
        "((region IS NOT NULL) AND (region = 'NORTH'::text))",
        false,
        "SELECT region, SUM(amount) AS s FROM orders GROUP BY region",
        "SELECT region, s FROM __reflex_intermediate_sales_view",
    );
    // CREATE statements clone via INCLUDING ALL and target the *old* children.
    assert!(ddl.create_swap_int.starts_with("CREATE TABLE "));
    assert!(!ddl.create_swap_int.contains("UNLOGGED"));
    assert!(ddl.create_swap_int.contains(
        "(LIKE \"public\".\"__reflex_intermediate_sales_view_orders_p_north\" INCLUDING ALL)"
    ));
    assert!(ddl
        .create_swap_int
        .contains("\"__reflex_swap_int_sales_view_orders_p_north\""));
    assert!(ddl
        .create_swap_tgt
        .contains("\"__reflex_swap_tgt_sales_view_orders_p_north\""));
    // Fill statements use the right query.
    let fill_int = ddl.fill_swap_int.as_deref().unwrap();
    assert!(fill_int.contains("SELECT region, SUM(amount)"));
    assert!(fill_int.contains("WHERE (((region IS NOT NULL)"));
    assert!(ddl
        .fill_swap_tgt
        .contains("__reflex_intermediate_sales_view"));
    // CHECK constraints to short-circuit ATTACH validation.
    assert!(ddl
        .check_int
        .as_deref()
        .unwrap()
        .contains("ADD CONSTRAINT __reflex_swap_check"));
    assert!(ddl
        .check_tgt
        .as_deref()
        .unwrap()
        .contains("ADD CONSTRAINT __reflex_swap_check"));
    // (DETACH/ATTACH are built parent-aware in execute_partition_swap_for_child,
    // not in build_swap_partition_ddl — see SwapPartitionDdl doc.)
    // Drop old + rename to canonical names.
    assert!(ddl
        .drop_old_int
        .contains("DROP TABLE \"public\".\"__reflex_intermediate_sales_view_orders_p_north\""));
    assert!(ddl
        .rename_int
        .contains("RENAME TO \"__reflex_intermediate_sales_view_orders_p_north\""));
}

#[test]
fn test_build_swap_partition_ddl_passthrough_skips_intermediate() {
    // Passthrough IMV — end_query is empty.  No intermediate fill / DDL
    // dependence beyond the standard pair (but caller still calls
    // build_swap_partition_ddl which produces the int CREATE; runtime
    // skips it because end_query is empty).
    let src = PartitionChild {
        bare_name: "p1".to_string(),
        bound_expr: "FOR VALUES IN ('A')".to_string(),
    };
    let ddl = build_swap_partition_ddl(
        "view",
        &src,
        "(region = 'A')",
        "(region = 'A')",
        true,
        "SELECT * FROM tbl",
        "",
    );
    // UNLOGGED requested → emitted.
    assert!(ddl.create_swap_int.contains("CREATE UNLOGGED TABLE"));
    assert!(ddl.create_swap_tgt.contains("CREATE UNLOGGED TABLE"));
    // No intermediate fill for passthrough.
    assert!(ddl.fill_swap_int.is_none());
    // Target fill uses base_query (passthrough).
    assert!(ddl.fill_swap_tgt.contains("SELECT * FROM tbl"));
}

#[test]
fn test_build_swap_partition_ddl_empty_constraint_skips_check() {
    let src = PartitionChild {
        bare_name: "p1".to_string(),
        bound_expr: "FOR VALUES IN ('A')".to_string(),
    };
    let ddl = build_swap_partition_ddl(
        "view",
        &src,
        "",
        "",
        false,
        "SELECT * FROM tbl",
        "SELECT * FROM int",
    );
    assert!(ddl.check_int.is_none());
    assert!(ddl.check_tgt.is_none());
    assert!(ddl.drop_check_int.is_none());
    assert!(ddl.drop_check_tgt.is_none());
}

#[test]
fn test_build_swap_partition_ddl_range_bound() {
    // RANGE bounds work the same way — the FOR VALUES … is opaque text
    // to the DDL builder.
    let src = PartitionChild {
        bare_name: "p_2026".to_string(),
        bound_expr: "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')".to_string(),
    };
    let ddl = build_swap_partition_ddl(
        "v",
        &src,
        "(d >= '2026-01-01' AND d < '2027-01-01')",
        "(d >= '2026-01-01' AND d < '2027-01-01')",
        false,
        "SELECT d, SUM(x) FROM t GROUP BY d",
        "SELECT d, s FROM __reflex_intermediate_v",
    );
    // The RANGE bound flows into ATTACH (built parent-aware in the executor);
    // the builder carries the range predicate via the pre-ATTACH CHECK that
    // lets PG skip its validation scan.
    assert!(ddl
        .check_int
        .as_deref()
        .unwrap()
        .contains("d >= '2026-01-01' AND d < '2027-01-01'"));
    assert!(ddl
        .check_tgt
        .as_deref()
        .unwrap()
        .contains("d >= '2026-01-01' AND d < '2027-01-01'"));
}

#[test]
fn test_node_ddl_internal_node_has_sub_partition_by() {
    let node = PartitionNode {
        bare_name: "ss_172".to_string(),
        oid: 0,
        parent_bare: "ss".to_string(), // == anchor root
        bound_expr: "FOR VALUES IN ('172')".to_string(),
        sub_strategy: Some("RANGE".to_string()),
        sub_columns: vec!["order_date".to_string()],
        depth: 1,
    };
    let (_int, tgt) = build_partition_node_ddl_pair("fcst", &node, "ss", true);
    assert_eq!(
        tgt,
        r#"CREATE TABLE IF NOT EXISTS "public"."fcst_ss_172" PARTITION OF "public"."fcst" FOR VALUES IN ('172') PARTITION BY RANGE ("order_date")"#
    );
}

#[test]
fn test_node_ddl_leaf_under_internal_parent_is_unlogged() {
    let node = PartitionNode {
        bare_name: "ss_172_2025_01".to_string(),
        oid: 0,
        parent_bare: "ss_172".to_string(), // not the root → parent is an IMV child
        bound_expr: "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')".to_string(),
        sub_strategy: None,
        sub_columns: vec![],
        depth: 2,
    };
    let (_int, tgt) = build_partition_node_ddl_pair("fcst", &node, "ss", true);
    assert_eq!(
        tgt,
        r#"CREATE UNLOGGED TABLE IF NOT EXISTS "public"."fcst_ss_172_2025_01" PARTITION OF "public"."fcst_ss_172" FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"#
    );
}

#[test]
fn test_classify_partition_diff() {
    let snapshot = vec![
        ("c_jan".to_string(), 100u32),
        ("c_feb".to_string(), 200u32),
        ("c_mar".to_string(), 300u32),
    ];
    let current = vec![
        ("c_jan".to_string(), 100u32), // unchanged
        ("c_feb".to_string(), 999u32), // oid changed -> swap
        ("c_apr".to_string(), 400u32), // new -> attach
                                       // c_mar gone -> drop
    ];
    let mut got = classify_partition_diff(&snapshot, &current);
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got,
        vec![
            ("c_apr".to_string(), PartitionDiffAction::AttachNew),
            ("c_feb".to_string(), PartitionDiffAction::SwapFill),
            ("c_mar".to_string(), PartitionDiffAction::Drop),
        ]
    );
}

#[test]
fn test_partition_node_has_depth_field() {
    // A leaf node constructed directly carries an absolute tree-depth.
    let n = PartitionNode {
        bare_name: "ss_172".to_string(),
        oid: 1,
        parent_bare: "ss".to_string(),
        bound_expr: "FOR VALUES IN (172)".to_string(),
        sub_strategy: None,
        sub_columns: vec![],
        depth: 1,
    };
    assert_eq!(n.depth, 1);
}

fn node(bare: &str, parent: &str, depth: usize, sub: Option<&str>) -> PartitionNode {
    PartitionNode {
        bare_name: bare.to_string(),
        oid: 0,
        parent_bare: parent.to_string(),
        bound_expr: "FOR VALUES IN (1)".to_string(),
        sub_strategy: sub.map(|s| s.to_string()),
        sub_columns: if sub.is_some() {
            vec!["order_date".to_string()]
        } else {
            vec![]
        },
        depth,
    }
}

#[test]
fn test_truncate_drops_below_depth_and_demotes_boundary() {
    // ss_172 (internal, depth 1) -> ss_172_2025_01 (leaf, depth 2)
    let tree = vec![
        node("ss_172", "ss", 1, Some("RANGE")),
        node("ss_172_2025_01", "ss_172", 2, None),
    ];
    let out = truncate_partition_tree(tree, 1);
    // Only the depth-1 node remains, demoted to a leaf.
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].bare_name, "ss_172");
    assert!(
        out[0].sub_strategy.is_none(),
        "boundary node must be demoted to leaf"
    );
    assert!(
        out[0].sub_columns.is_empty(),
        "boundary node must drop sub_columns"
    );
}

#[test]
fn test_truncate_full_depth_is_noop() {
    let tree = vec![
        node("ss_172", "ss", 1, Some("RANGE")),
        node("ss_172_2025_01", "ss_172", 2, None),
    ];
    let out = truncate_partition_tree(tree.clone(), 2);
    assert_eq!(out.len(), 2);
    // Internal node keeps its sub-partitioning.
    assert_eq!(out[0].sub_strategy.as_deref(), Some("RANGE"));
    assert_eq!(out[1].bare_name, "ss_172_2025_01");
}

#[test]
fn test_truncate_depth_beyond_tree_is_noop() {
    let tree = vec![node("ss_172", "ss", 1, None)];
    let out = truncate_partition_tree(tree, 5);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].sub_strategy, None);
}

#[test]
fn test_leaf_ancestor_chain_root_first() {
    // ss_172 (depth1, internal) -> ss_172_2025_01 (depth2, leaf)
    let tree = vec![
        node("ss_172", "ss", 1, Some("RANGE")),
        node("ss_172_2025_01", "ss_172", 2, None),
    ];
    // Ancestor chain of the leaf, root-first, EXCLUDING the leaf itself.
    let chain = leaf_ancestor_chain(&tree, "ss_172_2025_01");
    assert_eq!(chain, vec!["ss_172".to_string()]);
}

#[test]
fn test_leaf_ancestor_chain_of_top_level_leaf_is_empty() {
    let tree = vec![node("ss_a", "ss", 1, None)];
    assert!(leaf_ancestor_chain(&tree, "ss_a").is_empty());
}

#[test]
fn test_ancestor_at_depth_picks_correct_level() {
    // chain root-first for a depth-3 leaf: [lvl1, lvl2]; leaf itself is lvl3.
    let chain = vec!["p_172".to_string(), "p_172_2025".to_string()];
    assert_eq!(
        ancestor_bare_at_depth(&chain, "p_172_2025_03", 1).as_deref(),
        Some("p_172")
    );
    assert_eq!(
        ancestor_bare_at_depth(&chain, "p_172_2025_03", 2).as_deref(),
        Some("p_172_2025")
    );
    assert_eq!(
        ancestor_bare_at_depth(&chain, "p_172_2025_03", 3).as_deref(),
        Some("p_172_2025_03")
    );
    assert_eq!(
        ancestor_bare_at_depth(&chain, "p_172_2025_03", 9).as_deref(),
        Some("p_172_2025_03")
    );
}

#[test]
fn shape_mismatch_detects_leaf_where_partitioned_expected() {
    // Node expects partitioned (sub_strategy Some), existing child is plain 'r'.
    assert!(partition_shape_mismatch(true, Some('r')));
    // Node expects partitioned, existing child already partitioned 'p' -> ok.
    assert!(!partition_shape_mismatch(true, Some('p')));
    // Node expects leaf, existing child is partitioned 'p' -> mismatch.
    assert!(partition_shape_mismatch(false, Some('p')));
    // Node expects leaf, existing child is plain 'r' -> ok.
    assert!(!partition_shape_mismatch(false, Some('r')));
    // No existing child (None) -> never a mismatch (nothing to heal).
    assert!(!partition_shape_mismatch(true, None));
    assert!(!partition_shape_mismatch(false, None));
}

#[test]
fn f7_detect_divergence_flags_missing_and_oid_change() {
    let snap = vec![("a".to_string(), 1u32), ("b".to_string(), 2u32)];
    let live = vec![
        ("a".to_string(), 1u32),
        ("b".to_string(), 9u32),
        ("c".to_string(), 3u32),
    ];
    let mut d = detect_snapshot_live_divergence(&snap, &live);
    d.sort();
    assert_eq!(d, vec!["b".to_string(), "c".to_string()]); // b oid changed, c new
}

#[test]
fn f7_detect_divergence_flags_missing_from_live() {
    // Test case: snapshot has entries absent from live
    let snap = vec![("a".to_string(), 1u32), ("z".to_string(), 5u32)];
    let live = vec![("a".to_string(), 1u32)];
    let mut d = detect_snapshot_live_divergence(&snap, &live);
    d.sort();
    assert_eq!(d, vec!["z".to_string()]); // z missing from live
}
