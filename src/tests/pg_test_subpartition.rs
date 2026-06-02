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
