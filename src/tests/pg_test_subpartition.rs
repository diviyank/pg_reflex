// Integration tests for multi-level (sub-partition) source support.
// Plan: plans/sub_partitioning_impl_plan.md. Included from src/lib.rs tests module.

// Helper: count target-side partition children of an IMV (any depth).
fn imv_child_count(view: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM pg_inherits i \
         JOIN pg_class p ON p.oid = i.inhparent \
         WHERE p.relname = '{}'",
        view
    ))
    .unwrap()
    .unwrap()
}

// Helper: is `child` itself partitioned (an internal node)?
fn is_partitioned_rel(child: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = '{}')",
        child
    ))
    .unwrap()
    .unwrap()
}

#[pg_test]
fn pg_subpart_tree_walk_lists_all_levels() {
    Spi::run(
        "CREATE TABLE ss (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    )
    .expect("root");
    Spi::run("CREATE TABLE ss_172 PARTITION OF ss FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run(
        "CREATE TABLE ss_172_2025_01 PARTITION OF ss_172 \
         FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')",
    )
    .expect("range leaf 1");
    Spi::run(
        "CREATE TABLE ss_172_2025_02 PARTITION OF ss_172 \
         FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')",
    )
    .expect("range leaf 2");

    // 3 descendant nodes total: ss_172 (internal), and its two leaves.
    let n = Spi::get_one::<i64>(
        "SELECT tests.crate_test_list_partition_tree('public.ss'::text)",
    );
    assert_eq!(n.unwrap().unwrap(), 3);
}

#[pg_test]
fn pg_subpart_create_mirrors_full_tree() {
    Spi::run(
        "CREATE TABLE ss2 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, \
         product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)",
    )
    .expect("root");
    Spi::run("CREATE TABLE ss2_172 PARTITION OF ss2 FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE ss2_172_2025_01 PARTITION OF ss2_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");
    Spi::run(
        "INSERT INTO ss2 (dem_plan_id, order_date, product_id, qty) \
         VALUES (172, '2025-01-15', 5, 10)",
    )
    .expect("seed");

    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst2', \
            'SELECT dem_plan_id, order_date, product_id, qty FROM ss2', \
            'dem_plan_id,order_date,product_id', NULL, NULL, NULL, \
            ARRAY['dem_plan_id','order_date'] )",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");

    // Target tree: internal fcst2_ss2_172 must itself be partitioned (RANGE).
    let sub_strat = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'fcst2_ss2_172'",
    )
    .expect("sub strat query")
    .expect("sub strat");
    assert_eq!(sub_strat, "r", "fcst2_ss2_172 should be RANGE sub-partitioned");

    // Leaf mirror exists and holds the seeded row.
    let leaf_qty = Spi::get_one::<i32>(
        "SELECT qty FROM fcst2_ss2_172_2025_01 WHERE product_id = 5",
    )
    .expect("leaf query")
    .expect("qty");
    assert_eq!(leaf_qty, 10);
}

#[pg_test]
fn pg_subpart_rejects_sublevel_column_not_in_unique_key() {
    Spi::run(
        "CREATE TABLE ss3 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    )
    .expect("root");
    Spi::run("CREATE TABLE ss3_172 PARTITION OF ss3 FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE ss3_172_2025_01 PARTITION OF ss3_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");

    // unique_key omits order_date (a sub-level partition key) -> must be rejected
    // with a clean error string (NOT a panic / PG hard error).
    // Declare both levels explicitly so level-2 validation catches the missing order_date in unique_key.
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst3', \
            'SELECT dem_plan_id, qty FROM ss3', \
            'dem_plan_id', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        r.starts_with("ERROR") && r.contains("order_date"),
        "expected rejection naming order_date, got: {r}"
    );
}

#[pg_test]
fn pg_subpart_explicit_shallow_partition_by_creates_single_level() {
    Spi::run(
        "CREATE TABLE ssd (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, \
         product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)",
    )
    .expect("root");
    Spi::run("CREATE TABLE ssd_172 PARTITION OF ssd FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE ssd_172_2025_01 PARTITION OF ssd_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");
    Spi::run(
        "INSERT INTO ssd (dem_plan_id, order_date, product_id, qty) VALUES (172, '2025-01-15', 5, 10)",
    )
    .expect("seed");

    // order_date is projected only via a COALESCE-like rename — declare
    // partition_by:[dem_plan_id] so we mirror ONLY the dem_plan_id level.
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_shallow', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, product_id, qty FROM ssd', \
            'dem_plan_id,order_date,product_id', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "creation failed: {}", r);

    // Exactly ONE target child (the dem_plan_id=172 leaf), and it is NOT
    // itself partitioned (no order_date sub-level mirrored).
    assert_eq!(imv_child_count("fcst_shallow"), 1);
    assert!(!is_partitioned_rel("fcst_shallow_ssd_172"),
        "dem_plan_id leaf must be a plain table, not sub-partitioned");

    // Data is correct.
    let n = Spi::get_one::<i64>("SELECT count(*)::int8 FROM fcst_shallow").unwrap().unwrap();
    assert_eq!(n, 1);
}

#[pg_test]
fn pg_subpart_sync_creates_new_leaf_and_drops_orphan() {
    Spi::run("CREATE TABLE ss4 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss4_172 PARTITION OF ss4 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss4_172_2025_01 PARTITION OF ss4_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("leaf1");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst4', 'SELECT dem_plan_id, order_date, product_id, qty FROM ss4', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("c").expect("c");

    // Attach a brand-new month leaf on the source, then sync.
    Spi::run("CREATE TABLE ss4_172_2025_02 PARTITION OF ss4_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("leaf2");
    let _ = Spi::get_one::<String>("SELECT reflex_sync_partitions('fcst4', TRUE)").expect("sync").expect("sync");

    let exists = Spi::get_one::<bool>(
        "SELECT to_regclass('public.fcst4_ss4_172_2025_02') IS NOT NULL",
    ).expect("q").expect("b");
    assert!(exists, "new month leaf should be mirrored after sync");

    // Drop a source leaf, sync with drop_orphans -> IMV leaf dropped.
    Spi::run("DROP TABLE ss4_172_2025_01").expect("drop source leaf");
    let _ = Spi::get_one::<String>("SELECT reflex_sync_partitions('fcst4', TRUE)").expect("sync2").expect("sync2");
    let gone = Spi::get_one::<bool>(
        "SELECT to_regclass('public.fcst4_ss4_172_2025_01') IS NULL",
    ).expect("q2").expect("b2");
    assert!(gone, "orphan IMV leaf should be dropped after sync");
}

#[pg_test]
fn pg_subpart_reconcile_leaf_swaps_only_that_leaf() {
    Spi::run("CREATE TABLE ss5 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss5_172 PARTITION OF ss5 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss5_172_2025_01 PARTITION OF ss5_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("leaf1");
    Spi::run("CREATE TABLE ss5_172_2025_02 PARTITION OF ss5_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("leaf2");
    Spi::run("INSERT INTO ss5 VALUES (172,'2025-01-15',5,10),(172,'2025-02-15',5,20)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst5','SELECT dem_plan_id, order_date, product_id, qty FROM ss5', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("c").expect("c");

    // Mutate Jan data directly on the source leaf (stand-in for a swap), then
    // reconcile just that source leaf by name.
    Spi::run("UPDATE ss5_172_2025_01 SET qty = 999 WHERE product_id = 5").expect("mutate jan");
    let r = Spi::get_one::<String>(
        "SELECT reflex_reconcile_partition('fcst5', '', 'ss5_172_2025_01')",
    ).expect("reconcile").expect("reconcile");
    assert!(!r.starts_with("ERROR"), "reconcile: {r}");

    let jan = Spi::get_one::<i32>("SELECT qty FROM fcst5 WHERE order_date = '2025-01-15'").expect("q").expect("jan");
    let feb = Spi::get_one::<i32>("SELECT qty FROM fcst5 WHERE order_date = '2025-02-15'").expect("q").expect("feb");
    assert_eq!(jan, 999, "Jan leaf reconciled");
    assert_eq!(feb, 20, "Feb leaf untouched");
}

#[pg_test]
fn pg_subpart_reconcile_internal_node_swaps_all_leaves() {
    Spi::run("CREATE TABLE ss6 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss6_172 PARTITION OF ss6 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss6_172_2025_01 PARTITION OF ss6_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::run("CREATE TABLE ss6_172_2025_02 PARTITION OF ss6_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("l2");
    Spi::run("INSERT INTO ss6 VALUES (172,'2025-01-15',5,10),(172,'2025-02-15',5,20)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst6','SELECT dem_plan_id, order_date, product_id, qty FROM ss6', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("c").expect("c");

    // Mutate both month leaves, then reconcile the WHOLE dem_plan_id internal
    // node by source name -> resolution expands it to all its leaves.
    Spi::run("UPDATE ss6_172_2025_01 SET qty = 111").expect("m1");
    Spi::run("UPDATE ss6_172_2025_02 SET qty = 222").expect("m2");
    let r = Spi::get_one::<String>("SELECT reflex_reconcile_partition('fcst6', '', 'ss6_172')").expect("rec").expect("rec");
    assert!(!r.starts_with("ERROR"), "{r}");
    let jan = Spi::get_one::<i32>("SELECT qty FROM fcst6 WHERE order_date='2025-01-15'").expect("q").expect("j");
    let feb = Spi::get_one::<i32>("SELECT qty FROM fcst6 WHERE order_date='2025-02-15'").expect("q").expect("f");
    assert_eq!((jan, feb), (111, 222));
}

#[pg_test]
fn pg_subpart_catalog_tables_exist() {
    let snap = Spi::get_one::<bool>("SELECT to_regclass('public.__reflex_source_partition_snapshot') IS NOT NULL").expect("q").expect("b");
    let pend = Spi::get_one::<bool>("SELECT to_regclass('public.__reflex_partition_pending') IS NOT NULL").expect("q").expect("b");
    assert!(snap && pend, "snapshot={snap} pending={pend}");
}

#[pg_test]
fn pg_subpart_snapshot_seeded_at_create() {
    Spi::run("CREATE TABLE ss7 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss7_172 PARTITION OF ss7 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss7_172_2025_01 PARTITION OF ss7_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("leaf");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst7','SELECT dem_plan_id, order_date, product_id, qty FROM ss7', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("c");

    // Snapshot holds the source's single leaf.
    let cnt = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_source_partition_snapshot \
         WHERE child_name = 'ss7_172_2025_01'",
    ).expect("q").expect("c");
    assert_eq!(cnt, 1);
}

#[pg_test]
fn pg_subpart_flush_applies_attach() {
    Spi::run("CREATE TABLE ss8 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss8_172 PARTITION OF ss8 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss8_172_2025_01 PARTITION OF ss8_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::run("INSERT INTO ss8 VALUES (172,'2025-01-15',5,10)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst8','SELECT dem_plan_id, order_date, product_id, qty FROM ss8', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("c").expect("c");

    // Build a fresh Feb leaf as a standalone table and ATTACH it (a swap of a
    // new partition). The pre-existing DDL event trigger auto-syncs structure;
    // the flush is what fills the data.
    Spi::run("CREATE TABLE ss8_172_2025_02 (LIKE ss8 INCLUDING ALL)").expect("staging");
    Spi::run("INSERT INTO ss8_172_2025_02 VALUES (172,'2025-02-15',5,20)").expect("fill staging");
    Spi::run("ALTER TABLE ss8_172 ATTACH PARTITION ss8_172_2025_02 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("attach");

    let r = Spi::get_one::<String>("SELECT reflex_flush_partition_source('public.ss8')").expect("flush").expect("flush");
    assert!(!r.starts_with("ERROR"), "flush: {r}");
    let feb = Spi::get_one::<i32>("SELECT qty FROM fcst8 WHERE order_date='2025-02-15'").expect("q").expect("feb");
    assert_eq!(feb, 20, "attached Feb leaf flushed into IMV");
    // Jan still present.
    let jan = Spi::get_one::<i32>("SELECT qty FROM fcst8 WHERE order_date='2025-01-15'").expect("q").expect("jan");
    assert_eq!(jan, 10);
}

#[pg_test]
fn pg_subpart_event_trigger_enqueues_source_not_reflex_owned() {
    Spi::run("CREATE TABLE ss9 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss9_172 PARTITION OF ss9 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss9_172_2025_01 PARTITION OF ss9_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst9','SELECT dem_plan_id, order_date, product_id, qty FROM ss9', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("c");

    // Clear any enqueue from create-time DDL.
    Spi::run("TRUNCATE public.__reflex_partition_pending").expect("clear");

    // Attach a new sub-leaf on the source -> event trigger must enqueue the ROOT 'public.ss9'.
    Spi::run("CREATE TABLE ss9_172_2025_02 PARTITION OF ss9_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("attach");
    let enq = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root = 'public.ss9'",
    ).expect("q").expect("c");
    assert_eq!(enq, 1, "source root should be enqueued");

    // pg_reflex's own IMV partition (fcst9_*) must NOT be enqueued.
    let bad = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root LIKE '%fcst9%'",
    ).expect("q").expect("c");
    assert_eq!(bad, 0, "reflex-owned tables must never be enqueued");
}

/// Adding a brand-new TOP-LEVEL sub-partitioned branch to a multi-level
/// source and ATTACHing it fires the ddl_command_end event trigger with
/// `_parent = the root`, which auto-syncs the IMV against the full source
/// tree — mirroring the new internal (sub-partitioned) node AND its leaf in
/// one shot, with no manual `reflex_sync_partitions`.
#[pg_test]
fn pg_subpart_attach_toplevel_branch_autosyncs_full_subtree_via_event_trigger() {
    Spi::run("CREATE TABLE sb (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE sb_172 PARTITION OF sb FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE sb_172_2025_01 PARTITION OF sb_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("leaf");
    Spi::run("INSERT INTO sb VALUES (172,'2025-01-15',5,10)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('sbv','SELECT dem_plan_id, order_date, product_id, qty FROM sb', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("c").expect("c");

    // Build the new branch (dem_plan_id=173) standalone, fill it, then ATTACH
    // at the top level. The top-level ATTACH is the event-trigger's immediate
    // auto-sync surface.
    Spi::run("CREATE TABLE sb_173 (LIKE sb INCLUDING ALL) PARTITION BY RANGE (order_date)").expect("branch");
    Spi::run("CREATE TABLE sb_173_2025_01 PARTITION OF sb_173 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("branch leaf");
    Spi::run("ALTER TABLE sb ATTACH PARTITION sb_173 FOR VALUES IN (173)").expect("attach branch");

    // Mirror internal node exists and is itself RANGE sub-partitioned.
    let sub_strat = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'sbv_sb_173'",
    ).expect("q").expect("s");
    assert_eq!(sub_strat, "r", "new branch mirror must be RANGE sub-partitioned");

    // Mirror leaf exists — auto-synced by the event trigger, no manual sync.
    let leaf = Spi::get_one::<bool>(
        "SELECT to_regclass('public.sbv_sb_173_2025_01') IS NOT NULL",
    ).expect("q").expect("b");
    assert!(leaf, "new branch leaf must be auto-mirrored by the event trigger");
}

/// The production scenario that exposed the missing-intermediate bug: a
/// MULTI-LEVEL *passthrough* IMV (no intermediate table) driven through global
/// `reflex_reconcile`. Every leaf must rebuild via the per-child swap path
/// without the reconcile branch touching the absent
/// `__reflex_intermediate_<view>` relation.
#[pg_test]
fn pg_subpart_global_reconcile_passthrough_multilevel() {
    Spi::run("CREATE TABLE sc (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE sc_172 PARTITION OF sc FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE sc_172_2025_01 PARTITION OF sc_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::run("CREATE TABLE sc_172_2025_02 PARTITION OF sc_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("l2");
    Spi::run("INSERT INTO sc VALUES (172,'2025-01-15',5,10),(172,'2025-02-15',5,20)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('scv','SELECT dem_plan_id, order_date, product_id, qty FROM sc', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("c").expect("c");

    // Precondition: passthrough → no intermediate table.
    let int_tables = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname = '__reflex_intermediate_scv'",
    ).expect("q").expect("c");
    assert_eq!(int_tables, 0, "passthrough IMV must have no intermediate table");

    // Drift both leaves, then global-reconcile.
    Spi::run("UPDATE scv SET qty = -1").expect("drift");
    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('scv')").expect("rec").expect("res");
    assert_eq!(res, "RECONCILED");

    let jan = Spi::get_one::<i32>("SELECT qty FROM scv WHERE order_date='2025-01-15'").expect("q").expect("j");
    let feb = Spi::get_one::<i32>("SELECT qty FROM scv WHERE order_date='2025-02-15'").expect("q").expect("f");
    assert_eq!((jan, feb), (10, 20));
}

#[pg_test]
fn pg_subpart_detach_remove_drops_imv_leaf_via_flush() {
    Spi::run("CREATE TABLE ssa (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ssa_172 PARTITION OF ssa FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ssa_172_2025_01 PARTITION OF ssa_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l1");
    Spi::run("CREATE TABLE ssa_172_2025_02 PARTITION OF ssa_172 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("l2");
    Spi::run("INSERT INTO ssa VALUES (172,'2025-01-15',5,10),(172,'2025-02-15',5,20)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcsta','SELECT dem_plan_id, order_date, product_id, qty FROM ssa', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("c");

    // Detach + drop the Jan source leaf (removal). The DETACH fires the event
    // trigger which enqueues the root 'public.ssa'; the flush oid-diff sees the
    // Jan leaf gone and DROPs the matching IMV leaf.
    Spi::run("ALTER TABLE ssa_172 DETACH PARTITION ssa_172_2025_01").expect("detach");
    Spi::run("DROP TABLE ssa_172_2025_01").expect("drop");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush").expect("flush");

    let jan_gone = Spi::get_one::<bool>("SELECT to_regclass('public.fcsta_ssa_172_2025_01') IS NULL").expect("q").expect("b");
    assert!(jan_gone, "Jan IMV leaf dropped");
    let feb = Spi::get_one::<i32>("SELECT qty FROM fcsta WHERE order_date='2025-02-15'").expect("q").expect("feb");
    assert_eq!(feb, 20, "Feb untouched");
}

/// Mix of CTE decomposition + sub-partitions + passthrough — the
/// `sop_forecast_view` shape. A CTE passthrough over a SUB-partitioned
/// (LIST → RANGE) base, partitioned on the LIST key and sub-partitioned on the
/// bare-projected RANGE key, decomposes into a passthrough `__cte_` sub-IMV
/// that inherits the full sub-partition tree and so has NO intermediate table.
/// Driving the sub-IMV and the parent through global `reflex_reconcile` must
/// rebuild every leaf without the reconcile branch touching an absent
/// `__reflex_intermediate_*` relation.
#[pg_test]
fn pg_subpart_cte_passthrough_global_reconcile() {
    Spi::run(
        "CREATE TABLE ctm (dp_id BIGINT NOT NULL, item INT NOT NULL, d DATE NOT NULL, \
         qty NUMERIC, PRIMARY KEY (dp_id, item, d)) PARTITION BY LIST (dp_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ctm_1 PARTITION OF ctm FOR VALUES IN (1) PARTITION BY RANGE (d)").expect("list");
    Spi::run("CREATE TABLE ctm_1_jan PARTITION OF ctm_1 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("jan");
    Spi::run("CREATE TABLE ctm_1_feb PARTITION OF ctm_1 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("feb");
    Spi::run("INSERT INTO ctm VALUES (1,1,'2025-01-15',10),(1,2,'2025-02-15',20)").expect("seed");

    // Six-arg overload (no partition_by) → auto-mirror infers dp_id (LIST) and
    // mirrors the full LIST → RANGE sub-tree, the path production uses for CTE
    // chains. (The 7-arg overload takes a NON-NULL text[]; passing NULL there
    // panics in pgrx arg conversion, so auto-mirror must use the 6-arg form.)
    let msg = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'ctm_av', \
            'WITH base AS (SELECT dp_id, item, d, qty FROM ctm) \
             SELECT dp_id, item, d, qty FROM base', \
            'dp_id,item,d', 'UNLOGGED', 'DEFERRED', NULL \
         )",
    ).expect("create call").expect("create result");
    assert!(!msg.starts_with("ERROR"), "create failed: {msg}");

    // The decomposed `__cte_base` sub-IMV is a passthrough over a sub-partitioned
    // base → no intermediate table, and IS sub-partitioned (RANGE under LIST).
    let int_count = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname LIKE '__reflex_intermediate_ctm_av%'",
    ).expect("q").expect("c");
    assert_eq!(int_count, 0, "CTE passthrough chain must have no intermediate table");

    let sub_substrat = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid \
         WHERE c.relname = 'ctm_av__cte_base_ctm_1'",
    ).expect("q").expect("s");
    assert_eq!(sub_substrat, "r", "sub-IMV internal node must be RANGE sub-partitioned");

    // Reconcile the passthrough sub-IMV directly — the exact relation type that
    // crashed in production (sub-partitioned passthrough, no intermediate).
    Spi::run("UPDATE ctm_av__cte_base SET qty = -1").expect("drift sub");
    let r1 = Spi::get_one::<&str>("SELECT reflex_reconcile('ctm_av__cte_base')").expect("rec sub").expect("res");
    assert_eq!(r1, "RECONCILED", "sub-IMV reconcile");
    let jan = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT qty FROM ctm_av__cte_base WHERE item = 1 AND d = '2025-01-15'",
    ).expect("q").expect("v");
    let feb = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT qty FROM ctm_av__cte_base WHERE item = 2 AND d = '2025-02-15'",
    ).expect("q").expect("v");
    assert_eq!((jan.to_string(), feb.to_string()), ("10".into(), "20".into()), "sub-IMV leaves rebuilt");

    // Reconcile the parent (also passthrough + sub-partitioned) over the sub-IMV.
    Spi::run("UPDATE ctm_av SET qty = -999").expect("drift parent");
    let r2 = Spi::get_one::<&str>("SELECT reflex_reconcile('ctm_av')").expect("rec parent").expect("res");
    assert_eq!(r2, "RECONCILED", "parent reconcile");
    let pjan = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT qty FROM ctm_av WHERE item = 1 AND d = '2025-01-15'",
    ).expect("q").expect("v");
    assert_eq!(pjan.to_string(), "10", "parent Jan leaf rebuilt");
}

/// Mixed shape (CTE + sub-partition + passthrough) under incremental DML.
/// INSERT / UPDATE / DELETE on the sub-partitioned base must propagate through
/// the decomposed `__cte_base` sub-IMV and into the integrated parent, with
/// IMMEDIATE maintenance (synchronous within the txn).
#[pg_test]
fn pg_subpart_cte_passthrough_dml() {
    Spi::run("CREATE TABLE cdml (dp_id BIGINT NOT NULL, item INT NOT NULL, d DATE NOT NULL, qty NUMERIC, PRIMARY KEY (dp_id,item,d)) PARTITION BY LIST (dp_id)").expect("root");
    Spi::run("CREATE TABLE cdml_1 PARTITION OF cdml FOR VALUES IN (1) PARTITION BY RANGE (d)").expect("list");
    Spi::run("CREATE TABLE cdml_1_jan PARTITION OF cdml_1 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("jan");
    Spi::run("CREATE TABLE cdml_1_feb PARTITION OF cdml_1 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("feb");
    Spi::run("INSERT INTO cdml VALUES (1,1,'2025-01-15',10)").expect("seed");
    let msg = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'cdml_av', \
            'WITH base AS (SELECT dp_id, item, d, qty FROM cdml) SELECT dp_id, item, d, qty FROM base', \
            'dp_id,item,d', 'UNLOGGED', 'IMMEDIATE', NULL)",
    ).expect("c").expect("c");
    assert!(!msg.starts_with("ERROR"), "create: {msg}");

    // INSERT into a different leaf — propagates to sub-IMV and parent.
    Spi::run("INSERT INTO cdml VALUES (1,2,'2025-02-15',20)").expect("insert");
    let ins_sub = Spi::get_one::<pgrx::AnyNumeric>("SELECT qty FROM cdml_av__cte_base WHERE item=2 AND d='2025-02-15'").expect("q").expect("v");
    let ins_par = Spi::get_one::<pgrx::AnyNumeric>("SELECT qty FROM cdml_av WHERE item=2 AND d='2025-02-15'").expect("q").expect("v");
    assert_eq!((ins_sub.to_string(), ins_par.to_string()), ("20".into(), "20".into()), "INSERT propagated");

    // UPDATE a value — propagates.
    Spi::run("UPDATE cdml SET qty = 99 WHERE dp_id=1 AND item=1 AND d='2025-01-15'").expect("update");
    let upd_par = Spi::get_one::<pgrx::AnyNumeric>("SELECT qty FROM cdml_av WHERE item=1 AND d='2025-01-15'").expect("q").expect("v");
    assert_eq!(upd_par.to_string(), "99", "UPDATE propagated to parent");

    // DELETE a row — removed from both sub-IMV and parent.
    Spi::run("DELETE FROM cdml WHERE dp_id=1 AND item=1 AND d='2025-01-15'").expect("delete");
    let del_sub = Spi::get_one::<i64>("SELECT count(*) FROM cdml_av__cte_base WHERE item=1 AND d='2025-01-15'").expect("q").expect("c");
    let del_par = Spi::get_one::<i64>("SELECT count(*) FROM cdml_av WHERE item=1 AND d='2025-01-15'").expect("q").expect("c");
    assert_eq!((del_sub, del_par), (0, 0), "DELETE propagated");

    // Final parent state matches a fresh recompute of the base over the source.
    let drift = Spi::get_one::<i64>(
        "SELECT count(*) FROM ( \
            (SELECT dp_id,item,d,qty FROM cdml_av EXCEPT SELECT dp_id,item,d,qty FROM cdml) \
            UNION ALL \
            (SELECT dp_id,item,d,qty FROM cdml EXCEPT SELECT dp_id,item,d,qty FROM cdml_av)) x",
    ).expect("q").expect("c");
    assert_eq!(drift, 0, "parent must equal recompute after DML");
}

/// Mixed shape under a TOP-LEVEL partition attach + detach on the base. The
/// attach fires the ddl_command_end event trigger which cascades the auto-sync
/// down the CTE chain; subsequent DML into the new branch propagates to the
/// parent. Detaching + dropping the branch and flushing removes its rows.
#[pg_test]
fn pg_subpart_cte_passthrough_toplevel_attach_detach() {
    Spi::run("CREATE TABLE cat (dp_id BIGINT NOT NULL, item INT NOT NULL, d DATE NOT NULL, qty NUMERIC, PRIMARY KEY (dp_id,item,d)) PARTITION BY LIST (dp_id)").expect("root");
    Spi::run("CREATE TABLE cat_1 PARTITION OF cat FOR VALUES IN (1) PARTITION BY RANGE (d)").expect("list");
    Spi::run("CREATE TABLE cat_1_jan PARTITION OF cat_1 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("jan");
    Spi::run("INSERT INTO cat VALUES (1,1,'2025-01-15',10)").expect("seed");
    Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'cat_av', \
            'WITH base AS (SELECT dp_id, item, d, qty FROM cat) SELECT dp_id, item, d, qty FROM base', \
            'dp_id,item,d', 'UNLOGGED', 'IMMEDIATE', NULL)",
    ).expect("c").expect("c");

    // Build + ATTACH a new top-level branch (dp_id=2), itself sub-partitioned.
    Spi::run("CREATE TABLE cat_2 (LIKE cat INCLUDING ALL) PARTITION BY RANGE (d)").expect("branch");
    Spi::run("CREATE TABLE cat_2_jan PARTITION OF cat_2 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("branch leaf");
    Spi::run("ALTER TABLE cat ATTACH PARTITION cat_2 FOR VALUES IN (2)").expect("attach");

    // The top-level attach auto-syncs the DIRECT dependent (the CTE sub-IMV);
    // nested event triggers do not cascade further, so the parent over the
    // sub-IMV is synced down the chain explicitly (idempotent).
    let sub_branch = Spi::get_one::<bool>("SELECT to_regclass('public.cat_av__cte_base_cat_2') IS NOT NULL").expect("q").expect("b");
    assert!(sub_branch, "sub-IMV must mirror the new top-level branch via auto-sync");
    let _ = Spi::get_one::<String>("SELECT reflex_sync_partitions('cat_av__cte_base')").expect("sync sub").expect("s");
    let _ = Spi::get_one::<String>("SELECT reflex_sync_partitions('cat_av')").expect("sync par").expect("s");

    // DML into the new branch propagates to the parent (the INSERT succeeding
    // proves the parent now has the dp_id=2 partition — otherwise the cascade
    // raises "no partition of relation cat_av found for row").
    Spi::run("INSERT INTO cat VALUES (2,1,'2025-01-20',77)").expect("insert new branch");
    let par = Spi::get_one::<pgrx::AnyNumeric>("SELECT qty FROM cat_av WHERE dp_id=2 AND item=1").expect("q").expect("v");
    assert_eq!(par.to_string(), "77", "DML into attached branch reached parent");

    // DETACH + drop the branch on the source, then flush — branch rows leave the chain.
    Spi::run("ALTER TABLE cat DETACH PARTITION cat_2").expect("detach");
    Spi::run("DROP TABLE cat_2").expect("drop branch");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partition_source('public.cat')").expect("flush").expect("flush");
    let _ = Spi::get_one::<String>("SELECT reflex_sync_partitions('cat_av__cte_base', TRUE)").expect("sync sub").expect("s");
    let _ = Spi::get_one::<String>("SELECT reflex_sync_partitions('cat_av', TRUE)").expect("sync par").expect("s");
    let gone = Spi::get_one::<bool>("SELECT to_regclass('public.cat_av__cte_base_cat_2') IS NULL").expect("q").expect("b");
    assert!(gone, "sub-IMV branch mirror dropped after detach+sync");
}

/// Mixed shape under SUB-LEVEL partition changes: attaching a new month leaf
/// and swapping a leaf (detach old, attach a freshly-built table with mutated
/// data) on the base both flow through the event-trigger enqueue + flush, and
/// the data lands in the integrated parent via the CTE chain.
#[pg_test]
fn pg_subpart_cte_passthrough_sublevel_attach_swap() {
    Spi::run("CREATE TABLE csw (dp_id BIGINT NOT NULL, item INT NOT NULL, d DATE NOT NULL, qty NUMERIC, PRIMARY KEY (dp_id,item,d)) PARTITION BY LIST (dp_id)").expect("root");
    Spi::run("CREATE TABLE csw_1 PARTITION OF csw FOR VALUES IN (1) PARTITION BY RANGE (d)").expect("list");
    Spi::run("CREATE TABLE csw_1_jan PARTITION OF csw_1 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("jan");
    Spi::run("INSERT INTO csw VALUES (1,1,'2025-01-15',10)").expect("seed");
    Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'csw_av', \
            'WITH base AS (SELECT dp_id, item, d, qty FROM csw) SELECT dp_id, item, d, qty FROM base', \
            'dp_id,item,d', 'UNLOGGED', 'IMMEDIATE', NULL)",
    ).expect("c").expect("c");

    // SUB-LEVEL attach: a brand-new Feb leaf built standalone, filled, attached.
    // The DETACH/ATTACH enqueues the source root; flush oid-diffs and fills the
    // matching leaf on the DIRECT mirror (the CTE sub-IMV).
    Spi::run("CREATE TABLE csw_1_feb (LIKE csw INCLUDING ALL)").expect("stage feb");
    Spi::run("INSERT INTO csw_1_feb VALUES (1,2,'2025-02-15',20)").expect("fill feb");
    Spi::run("ALTER TABLE csw_1 ATTACH PARTITION csw_1_feb FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("attach feb");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partition_source('public.csw')").expect("flush1").expect("f");
    let feb_sub = Spi::get_one::<pgrx::AnyNumeric>("SELECT qty FROM csw_av__cte_base WHERE item=2 AND d='2025-02-15'").expect("q").expect("v");
    assert_eq!(feb_sub.to_string(), "20", "attached sub-leaf flushed into the CTE sub-IMV");

    // SUB-LEVEL swap: detach Jan, attach a freshly-built Jan with mutated qty.
    Spi::run("ALTER TABLE csw_1 DETACH PARTITION csw_1_jan").expect("detach jan");
    Spi::run("CREATE TABLE csw_1_jan_new (LIKE csw INCLUDING ALL)").expect("stage jan");
    Spi::run("INSERT INTO csw_1_jan_new VALUES (1,1,'2025-01-15',1000)").expect("fill jan");
    Spi::run("DROP TABLE csw_1_jan").expect("drop old jan");
    Spi::run("ALTER TABLE csw_1 ATTACH PARTITION csw_1_jan_new FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("attach jan");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partition_source('public.csw')").expect("flush2").expect("f");
    let jan_sub = Spi::get_one::<pgrx::AnyNumeric>("SELECT qty FROM csw_av__cte_base WHERE item=1 AND d='2025-01-15'").expect("q").expect("v");
    assert_eq!(jan_sub.to_string(), "1000", "swapped sub-leaf data reached the CTE sub-IMV");

    // The CTE sub-IMV (the direct mirror) equals a fresh recompute of the base.
    let sub_drift = Spi::get_one::<i64>(
        "SELECT count(*) FROM ( \
            (SELECT dp_id,item,d,qty FROM csw_av__cte_base EXCEPT SELECT dp_id,item,d,qty FROM csw) \
            UNION ALL \
            (SELECT dp_id,item,d,qty FROM csw EXCEPT SELECT dp_id,item,d,qty FROM csw_av__cte_base)) x",
    ).expect("q").expect("c");
    assert_eq!(sub_drift, 0, "sub-IMV must equal recompute after sub-level changes");

    // Propagate the structural changes one level up: reconcile the parent over
    // the freshly-mirrored sub-IMV. It must then equal the recompute too.
    let r = Spi::get_one::<&str>("SELECT reflex_reconcile('csw_av')").expect("rec").expect("res");
    assert_eq!(r, "RECONCILED", "parent reconcile after sub-level changes");
    let par_drift = Spi::get_one::<i64>(
        "SELECT count(*) FROM ( \
            (SELECT dp_id,item,d,qty FROM csw_av EXCEPT SELECT dp_id,item,d,qty FROM csw) \
            UNION ALL \
            (SELECT dp_id,item,d,qty FROM csw EXCEPT SELECT dp_id,item,d,qty FROM csw_av)) x",
    ).expect("q").expect("c");
    assert_eq!(par_drift, 0, "parent must equal recompute after reconcile");
}

// Differential oracle: a deterministic sequence of leaf swaps (detach old,
// attach a freshly-built table with mutated data, flush) must leave the IMV
// byte-for-byte equal to a fresh recompute of the base query over the source.
// Uses the freshly-built-table attach form so the partition oid changes and
// the snapshot oid-diff fires (the documented supported path).
#[pg_test]
fn pg_fuzz_subpartition_swap_sequence_matches_recompute() {
    Spi::run("CREATE TABLE fz (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE fz_1 PARTITION OF fz FOR VALUES IN (1) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE fz_1_2025_01 PARTITION OF fz_1 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("m1");
    Spi::run("CREATE TABLE fz_1_2025_02 PARTITION OF fz_1 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").expect("m2");
    Spi::run("CREATE TABLE fz_1_2025_03 PARTITION OF fz_1 FOR VALUES FROM ('2025-03-01') TO ('2025-04-01')").expect("m3");
    Spi::run("INSERT INTO fz SELECT 1, make_date(2025, (g % 3) + 1, 10), g, g * 10 FROM generate_series(1,30) g").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fzv','SELECT dem_plan_id, order_date, product_id, qty FROM fz', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("c").expect("c");

    for m in 1..=3 {
        let lo = format!("2025-0{}-01", m);
        let hi = format!("2025-0{}-01", m + 1);
        Spi::run(&format!("ALTER TABLE fz_1 DETACH PARTITION fz_1_2025_0{}", m)).expect("detach");
        Spi::run(&format!("CREATE TABLE fz_1_2025_0{}_new (LIKE fz INCLUDING ALL)", m)).expect("stage");
        Spi::run(&format!(
            "INSERT INTO fz_1_2025_0{m}_new SELECT dem_plan_id, order_date, product_id, qty + 1000 FROM fz_1_2025_0{m}",
            m = m
        )).expect("fill");
        Spi::run(&format!("DROP TABLE fz_1_2025_0{}", m)).expect("dropold");
        Spi::run(&format!(
            "ALTER TABLE fz_1 ATTACH PARTITION fz_1_2025_0{m}_new FOR VALUES FROM ('{lo}') TO ('{hi}')",
            m = m, lo = lo, hi = hi
        )).expect("attach");
        let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush").expect("flush");
    }

    let drift = Spi::get_one::<i64>(
        "SELECT count(*) FROM ( \
            (SELECT dem_plan_id, order_date, product_id, qty FROM fzv \
             EXCEPT SELECT dem_plan_id, order_date, product_id, qty FROM fz) \
            UNION ALL \
            (SELECT dem_plan_id, order_date, product_id, qty FROM fz \
             EXCEPT SELECT dem_plan_id, order_date, product_id, qty FROM fzv) \
         ) d",
    ).expect("oracle").expect("count");
    assert_eq!(drift, 0, "IMV diverged from source recompute after swap sequence");
}

#[pg_test]
fn pg_subpart_explicit_two_level_opts_into_subpartitioning() {
    Spi::run(
        "CREATE TABLE sstwo (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE sstwo_172 PARTITION OF sstwo FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE sstwo_172_2025_01 PARTITION OF sstwo_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");
    Spi::run("INSERT INTO sstwo VALUES (172, '2025-01-15', 10)").expect("seed");

    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst_deep', \
            'SELECT dem_plan_id, order_date, qty FROM sstwo', \
            'dem_plan_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("create call").expect("create result");
    assert!(!r.starts_with("ERROR"), "create failed: {}", r);

    assert!(is_partitioned_rel("fcst_deep_sstwo_172"),
        "with explicit 2-level partition_by, the dem_plan_id node must sub-partition");
}

#[pg_test]
fn pg_subpart_auto_prune_stops_at_non_projected_sublevel() {
    Spi::run(
        "CREATE TABLE ssauto (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ssauto_172 PARTITION OF ssauto FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("list child");
    Spi::run("CREATE TABLE ssauto_172_2025_01 PARTITION OF ssauto_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')")
        .expect("leaf");
    Spi::run("INSERT INTO ssauto VALUES (172, '2025-01-15', 10)").expect("seed");

    // 6-arg overload (no partition_by) → auto-mirror. Passing NULL for the
    // 7-arg text[] partition_by is ambiguous/panics, so auto-mirror must use
    // the 6-arg form with concrete storage/mode to disambiguate the overload.
    let r = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm('fcst_auto', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, qty FROM ssauto', \
            'dem_plan_id,order_date', 'UNLOGGED', 'IMMEDIATE', NULL)",
    ).expect("create call").expect("create result");
    assert!(!r.starts_with("ERROR"), "create failed: {}", r);

    assert!(!is_partitioned_rel("fcst_auto_ssauto_172"),
        "auto-mirror must prune the order_date sub-level (not bare-projected)");
    assert_eq!(imv_child_count("fcst_auto"), 1);
}

#[pg_test]
fn pg_subpart_shallow_imv_persists_partition_depth() {
    Spi::run(
        "CREATE TABLE ssp (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    ).expect("root");
    Spi::run("CREATE TABLE ssp_172 PARTITION OF ssp FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("c");
    Spi::run("CREATE TABLE ssp_172_2025_01 PARTITION OF ssp_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("l");
    Spi::run("INSERT INTO ssp VALUES (172, '2025-01-15', 1)").expect("seed");
    let r = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm('fcst_depth', \
            'SELECT dem_plan_id, COALESCE(order_date, order_date) AS order_date, qty FROM ssp', \
            'dem_plan_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    ).expect("c").expect("r");
    assert!(!r.starts_with("ERROR"), "create failed: {r}");

    let d = Spi::get_one::<i32>(
        "SELECT partition_depth FROM public.__reflex_ivm_reference WHERE name = 'fcst_depth'",
    ).unwrap();
    assert_eq!(d, Some(1));
}
