// Integration tests for multi-level (sub-partition) source support.
// Plan: plans/sub_partitioning_impl_plan.md. Included from src/lib.rs tests module.

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
            ARRAY['dem_plan_id'] )",
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
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst3', \
            'SELECT dem_plan_id, qty FROM ss3', \
            'dem_plan_id', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        r.starts_with("ERROR") && r.contains("order_date"),
        "expected rejection naming order_date, got: {r}"
    );
}

#[pg_test]
fn pg_subpart_sync_creates_new_leaf_and_drops_orphan() {
    Spi::run("CREATE TABLE ss4 (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("root");
    Spi::run("CREATE TABLE ss4_172 PARTITION OF ss4 FOR VALUES IN (172) PARTITION BY RANGE (order_date)").expect("list");
    Spi::run("CREATE TABLE ss4_172_2025_01 PARTITION OF ss4_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").expect("leaf1");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('fcst4', 'SELECT dem_plan_id, order_date, product_id, qty FROM ss4', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
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
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
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
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id'])",
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
