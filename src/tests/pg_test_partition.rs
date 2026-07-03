// Integration tests for partitioned IMV support (plans/partitioning_2.md).
// Included from src/lib.rs `tests` module via include!.

#[pg_test]
fn pg_part_aggregate_explicit_list_partition() {
    Spi::run(
        "CREATE TABLE part_orders_l (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)",
    )
    .expect("create partitioned source");
    Spi::run("CREATE TABLE part_orders_l_north PARTITION OF part_orders_l FOR VALUES IN ('NORTH')")
        .expect("child north");
    Spi::run("CREATE TABLE part_orders_l_south PARTITION OF part_orders_l FOR VALUES IN ('SOUTH')")
        .expect("child south");
    Spi::run(
        "INSERT INTO part_orders_l (id, region, amount) VALUES \
         (1, 'NORTH', 100), (2, 'NORTH', 200), (3, 'SOUTH', 50)",
    )
    .expect("seed");

    // partition_by => ARRAY['region'] for an aggregate IMV grouped by region
    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'part_agg_view', \
            'SELECT region, SUM(amount) AS total FROM part_orders_l GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create partitioned IMV call")
    .expect("create partitioned IMV result");
    assert!(
        !create_result.starts_with("ERROR"),
        "create returned: {create_result}"
    );

    // Target is partitioned LIST (region)
    let strategy = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid \
         WHERE c.relname = 'part_agg_view'",
    )
    .expect("strategy query")
    .expect("strategy returned");
    assert_eq!(strategy, "l", "target should be LIST partitioned");

    // Two children exist on target
    let child_count = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'part_agg_view'",
    )
    .expect("child count")
    .expect("count");
    assert_eq!(child_count, 2, "target should have 2 children");

    // Intermediate is also partitioned with 2 children
    let int_child_count = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = '__reflex_intermediate_part_agg_view'",
    )
    .expect("int child count")
    .expect("count");
    assert_eq!(int_child_count, 2, "intermediate should have 2 children");

    // Initial materialization is correct
    let north_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_agg_view WHERE region = 'NORTH'",
    )
    .expect("north query")
    .expect("north total");
    assert_eq!(north_total.to_string(), "300");
    let south_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_agg_view WHERE region = 'SOUTH'",
    )
    .expect("south query")
    .expect("south total");
    assert_eq!(south_total.to_string(), "50");

    // Catalog metadata correct
    let strategy_str = Spi::get_one::<String>(
        "SELECT partition_strategy FROM public.__reflex_ivm_reference WHERE name = 'part_agg_view'",
    )
    .expect("catalog query")
    .expect("strategy");
    assert_eq!(strategy_str, "LIST");
    let part_cols: Vec<String> = Spi::get_one::<Vec<String>>(
        "SELECT partition_columns FROM public.__reflex_ivm_reference WHERE name = 'part_agg_view'",
    )
    .expect("catalog query")
    .expect("part_cols");
    assert_eq!(part_cols, vec!["region".to_string()]);
}

#[pg_test]
fn pg_part_partition_by_not_in_group_by_errors() {
    Spi::run("CREATE TABLE part_err1 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_err1_n PARTITION OF part_err1 FOR VALUES IN ('N')").expect("child");
    Spi::run("INSERT INTO part_err1 (id, region, amount) VALUES (1, 'N', 1)").expect("seed");

    // partition_by => ARRAY['amount'] — but amount is NOT in GROUP BY.
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'part_err1_v', \
            'SELECT region, SUM(amount) AS total FROM part_err1 GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['amount'] \
         )",
    )
    .expect("call")
    .expect("result");
    assert!(
        r.starts_with("ERROR: [reflex-unsupported] partition_by column 'amount' is not in GROUP BY"),
        "got: {r}"
    );
}

#[pg_test]
fn pg_part_partition_by_anchor_not_partitioned_errors() {
    // Flat (non-partitioned) source — partition_by on a column should error
    Spi::run("CREATE TABLE part_err2 (id BIGINT, region TEXT NOT NULL, amount NUMERIC)").expect("create");
    Spi::run("INSERT INTO part_err2 (id, region, amount) VALUES (1, 'N', 1)").expect("seed");

    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'part_err2_v', \
            'SELECT region, SUM(amount) AS total FROM part_err2 GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("call")
    .expect("result");
    assert!(
        r.contains("not partitioned LIST/RANGE"),
        "expected partition-anchor error, got: {r}"
    );
}

#[pg_test]
fn pg_part_auto_mirror_aggregate_partition_col_in_group_by() {
    Spi::run("CREATE TABLE part_auto_a (id BIGINT, dept TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (dept)").expect("create");
    Spi::run("CREATE TABLE part_auto_a_e PARTITION OF part_auto_a FOR VALUES IN ('eng')").expect("e");
    Spi::run("CREATE TABLE part_auto_a_s PARTITION OF part_auto_a FOR VALUES IN ('sales')").expect("s");
    Spi::run("INSERT INTO part_auto_a (id, dept, amount) VALUES (1,'eng',10),(2,'sales',5)").expect("seed");

    // No partition_by passed — auto-mirror because dept is in GROUP BY.
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_auto_a_v', \
            'SELECT dept, SUM(amount) AS total FROM part_auto_a GROUP BY dept' \
         )",
    )
    .expect("create");

    let strategy = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid \
         WHERE c.relname = 'part_auto_a_v'",
    )
    .expect("strategy")
    .expect("v");
    assert_eq!(strategy, "l");

    let part_cols: Vec<String> = Spi::get_one::<Vec<String>>(
        "SELECT partition_columns FROM public.__reflex_ivm_reference WHERE name = 'part_auto_a_v'",
    )
    .expect("catalog")
    .expect("cols");
    assert_eq!(part_cols, vec!["dept".to_string()]);
}

#[pg_test]
fn pg_part_auto_mirror_join_shared_partition_key() {
    // A partitioned source joined to a second source that ALSO owns the
    // partition column (the join key itself). The anchor must disambiguate
    // to the partitioned source instead of erroring on "multiple sources own
    // partition column", which previously left the parent with zero children
    // and made the seeding INSERT fail with "no partition of relation found".
    Spi::run("CREATE TABLE part_jk_s (dem_plan_id BIGINT, product_id BIGINT, qty NUMERIC) PARTITION BY LIST (dem_plan_id)").expect("create s");
    Spi::run("CREATE TABLE part_jk_s_p1 PARTITION OF part_jk_s FOR VALUES IN (1, 2)").expect("p1");
    Spi::run("CREATE TABLE part_jk_s_p2 PARTITION OF part_jk_s FOR VALUES IN (7057)").expect("p2");
    Spi::run("INSERT INTO part_jk_s VALUES (1,100,5),(2,101,7),(7057,102,9)").expect("seed s");
    Spi::run("CREATE TABLE part_jk_d (dem_plan_id BIGINT PRIMARY KEY, status TEXT)").expect("create d");
    Spi::run("INSERT INTO part_jk_d VALUES (1,'a'),(2,'a'),(7057,'a')").expect("seed d");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_jk_v', \
            'SELECT s.dem_plan_id, s.product_id, s.qty FROM part_jk_s s \
             JOIN part_jk_d d ON d.dem_plan_id = s.dem_plan_id', \
            'dem_plan_id,product_id' \
         )",
    )
    .expect("create");

    let strategy = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'part_jk_v'",
    )
    .expect("strategy")
    .expect("v");
    assert_eq!(strategy, "l");

    let children = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = 'part_jk_v'::regclass",
    )
    .expect("children query")
    .expect("count");
    assert_eq!(children, 2, "both source partitions must be mirrored");

    let n = Spi::get_one::<i64>("SELECT count(*) FROM part_jk_v")
        .expect("count query")
        .expect("n");
    assert_eq!(n, 3, "all source rows must be seeded across partitions");
}

#[pg_test]
fn pg_part_auto_mirror_skipped_when_col_not_in_group_by() {
    Spi::run("CREATE TABLE part_skip_a (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_skip_a_n PARTITION OF part_skip_a FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO part_skip_a (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    // No partition_by passed; region NOT in GROUP BY → no auto-mirror.
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_skip_a_v', \
            'SELECT id, SUM(amount) AS total FROM part_skip_a GROUP BY id' \
         )",
    )
    .expect("create");

    let is_part = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid \
         WHERE c.relname = 'part_skip_a_v'",
    )
    .expect("query")
    .expect("count");
    assert_eq!(is_part, 0, "IMV should not be partitioned");

    // Catalog metadata absent
    let part_cols: Option<Vec<String>> = Spi::get_one::<Vec<String>>(
        "SELECT partition_columns FROM public.__reflex_ivm_reference WHERE name = 'part_skip_a_v'",
    )
    .expect("catalog");
    assert!(part_cols.is_none(), "catalog partition_columns should be NULL");
}

#[pg_test]
fn pg_part_sync_partitions_adds_new_child() {
    Spi::run("CREATE TABLE part_sync_a (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_sync_a_n PARTITION OF part_sync_a FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO part_sync_a (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_sync_a_v', \
            'SELECT region, SUM(amount) AS total FROM part_sync_a GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create");

    // Add a new source partition. Since 1.6.0 the ddl_command_end event
    // trigger auto-syncs the IMV at CREATE TABLE … PARTITION OF time, so the
    // IMV already has the matching partition when control returns.
    Spi::run("CREATE TABLE part_sync_a_s PARTITION OF part_sync_a FOR VALUES IN ('S')")
        .expect("new partition");

    let after_auto = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'part_sync_a_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(after_auto, 2, "auto-sync at CREATE PARTITION OF should add the IMV child");

    // Manual sync is now idempotent — no further changes.
    let msg = Spi::get_one::<String>("SELECT reflex_sync_partitions('part_sync_a_v')")
        .expect("call")
        .expect("msg");
    assert!(msg.contains("+0 intermediate"), "manual sync should be a no-op, got: {msg}");
    assert!(msg.contains("+0 target"),       "manual sync should be a no-op, got: {msg}");
}

/// Regression: a partitioned PASSTHROUGH IMV (no aggregation, needs_ivm_count
/// = false) has NO intermediate table — `intermediate_column_spec` returns
/// None, so `__reflex_intermediate_<view>` is never created. Both the
/// create-time child loop and `reflex_sync_partitions` must skip every
/// intermediate-child DDL; otherwise they raise `relation
/// "…__reflex_intermediate_<view>" does not exist`. Mirrors prod
/// alp.sop_forecast_view, where ALTER TABLE … DETACH/ATTACH PARTITION failed.
#[pg_test]
fn pg_part_passthrough_imv_without_intermediate_syncs_target_only() {
    Spi::run("CREATE TABLE part_pt_s (dem_plan_id BIGINT, product_id BIGINT, qty NUMERIC) PARTITION BY LIST (dem_plan_id)").expect("create s");
    Spi::run("CREATE TABLE part_pt_s_p1 PARTITION OF part_pt_s FOR VALUES IN (1)").expect("p1");
    Spi::run("INSERT INTO part_pt_s VALUES (1, 100, 5)").expect("seed");

    // Passthrough IMV (no GROUP BY/aggregate) with a sound unique key → no
    // ivm-count intermediate. The source already has a partition at create
    // time, so the create-time child loop runs against the (absent) intermediate.
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_pt_v', \
            'SELECT dem_plan_id, product_id, qty FROM part_pt_s', \
            'dem_plan_id,product_id', NULL, NULL, NULL, \
            ARRAY['dem_plan_id'] \
         )",
    )
    .expect("create passthrough partitioned IMV");

    let int_tables = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname = '__reflex_intermediate_part_pt_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(int_tables, 0, "passthrough IMV must have no intermediate table");

    let children_after_create = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits WHERE inhparent = 'part_pt_v'::regclass",
    )
    .expect("q")
    .expect("c");
    assert_eq!(
        children_after_create, 1,
        "target must mirror the source partition present at create time"
    );

    // Adding a source partition fires the ddl_command_end auto-sync.
    Spi::run("CREATE TABLE part_pt_s_p2 PARTITION OF part_pt_s FOR VALUES IN (7057)")
        .expect("attach p2 triggers auto-sync without error");

    let children_after_attach = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits WHERE inhparent = 'part_pt_v'::regclass",
    )
    .expect("q")
    .expect("c");
    assert_eq!(
        children_after_attach, 2,
        "auto-sync must mirror the new source partition onto the target"
    );

    // Explicit sync is clean and idempotent — no intermediate work attempted.
    let msg = Spi::get_one::<String>("SELECT reflex_sync_partitions('part_pt_v')")
        .expect("sync")
        .expect("msg");
    assert_eq!(
        msg, "sync: +0 intermediate, +0 target",
        "manual sync must be a no-op, got: {msg}"
    );
}

#[pg_test]
fn pg_part_sync_partitions_drops_orphans_by_default() {
    Spi::run("CREATE TABLE part_drop_a (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_drop_a_n PARTITION OF part_drop_a FOR VALUES IN ('N')").expect("p1");
    Spi::run("CREATE TABLE part_drop_a_s PARTITION OF part_drop_a FOR VALUES IN ('S')").expect("p2");
    Spi::run("INSERT INTO part_drop_a (id, region, amount) VALUES (1, 'N', 10),(2,'S',20)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_drop_a_v', \
            'SELECT region, SUM(amount) AS total FROM part_drop_a GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create");

    // Detach + drop one source partition
    Spi::run("ALTER TABLE part_drop_a DETACH PARTITION part_drop_a_s").expect("detach");
    Spi::run("DROP TABLE part_drop_a_s").expect("drop");

    // Sync with default drop_orphans=true
    let msg = Spi::get_one::<String>("SELECT reflex_sync_partitions('part_drop_a_v')")
        .expect("call")
        .expect("msg");
    assert!(msg.contains("-1 intermediate"), "got: {msg}");
    assert!(msg.contains("-1 target"), "got: {msg}");

    // Only the N partition remains
    let after = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'part_drop_a_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(after, 1);
}

#[pg_test]
fn pg_part_sync_partitions_preserves_orphans_when_opt_out() {
    Spi::run("CREATE TABLE part_keep_a (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_keep_a_n PARTITION OF part_keep_a FOR VALUES IN ('N')").expect("p1");
    Spi::run("CREATE TABLE part_keep_a_s PARTITION OF part_keep_a FOR VALUES IN ('S')").expect("p2");
    Spi::run("INSERT INTO part_keep_a (id, region, amount) VALUES (1, 'N', 10),(2,'S',20)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_keep_a_v', \
            'SELECT region, SUM(amount) AS total FROM part_keep_a GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create");

    Spi::run("ALTER TABLE part_keep_a DETACH PARTITION part_keep_a_s").expect("detach");
    Spi::run("DROP TABLE part_keep_a_s").expect("drop");

    let msg = Spi::get_one::<String>("SELECT reflex_sync_partitions('part_keep_a_v', false)")
        .expect("call")
        .expect("msg");
    assert!(msg.contains("preserved orphans"), "got: {msg}");

    // The orphan IMV partitions are still present (2 instead of 1)
    let after = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'part_keep_a_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(after, 2, "orphan target should be preserved");
}

#[pg_test]
fn pg_part_reconcile_partition_rebuilds_only_one_child() {
    Spi::run("CREATE TABLE part_rec_a (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_rec_a_n PARTITION OF part_rec_a FOR VALUES IN ('N')").expect("p1");
    Spi::run("CREATE TABLE part_rec_a_s PARTITION OF part_rec_a FOR VALUES IN ('S')").expect("p2");
    Spi::run("INSERT INTO part_rec_a (id, region, amount) VALUES (1, 'N', 10),(2,'S',20)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_rec_a_v', \
            'SELECT region, SUM(amount) AS total FROM part_rec_a GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create");

    // Manually corrupt N's target row (simulate drift) — UPDATE target child
    Spi::run("UPDATE part_rec_a_v SET total = 999 WHERE region = 'N'").expect("corrupt");

    // Trigger partition-scoped reconcile for 'N' only
    let msg = Spi::get_one::<String>("SELECT reflex_reconcile_partition('part_rec_a_v', 'N')")
        .expect("call")
        .expect("msg");
    assert!(
        msg.starts_with("RECONCILED partitions"),
        "expected RECONCILED, got: {msg}"
    );

    // After: N is correct (10), S is unchanged (20)
    let n_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_rec_a_v WHERE region = 'N'",
    )
    .expect("q")
    .expect("n");
    assert_eq!(n_total.to_string(), "10");
    let s_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_rec_a_v WHERE region = 'S'",
    )
    .expect("q")
    .expect("s");
    assert_eq!(s_total.to_string(), "20");
}

#[pg_test]
fn pg_part_unpartitioned_imv_unchanged_byte_for_byte() {
    // Non-regression: a plain unpartitioned IMV is created via the legacy path.
    // No partition columns on the catalog row; target is not partitioned.
    Spi::run("CREATE TABLE part_npreg (id BIGINT, val TEXT, amount NUMERIC)")
        .expect("create");
    Spi::run("INSERT INTO part_npreg (id, val, amount) VALUES (1, 'a', 10),(2,'b',20)")
        .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_npreg_v', \
            'SELECT val, SUM(amount) AS total FROM part_npreg GROUP BY val' \
         )",
    )
    .expect("create");

    let is_part = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid \
         WHERE c.relname IN ('part_npreg_v', '__reflex_intermediate_part_npreg_v')",
    )
    .expect("q")
    .expect("c");
    assert_eq!(is_part, 0, "unpartitioned IMV must not become partitioned");

    let part_cols: Option<Vec<String>> = Spi::get_one::<Vec<String>>(
        "SELECT partition_columns FROM public.__reflex_ivm_reference WHERE name = 'part_npreg_v'",
    )
    .expect("catalog");
    assert!(part_cols.is_none());
}

/// Reconcile on a partitioned IMV must work via the existing
/// `reflex_reconcile` (TRUNCATE on parent cascades to children).  Sync runs
/// at entry too — newly attached source partitions are picked up.
#[pg_test]
fn pg_part_reconcile_after_attach_picks_up_new_partition() {
    Spi::run("CREATE TABLE part_rec2 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_rec2_n PARTITION OF part_rec2 FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO part_rec2 (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_rec2_v', \
            'SELECT region, SUM(amount) AS total FROM part_rec2 GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create");

    // Source: attach a new partition.  Per design (PP §Cons #2), the user
    // MUST call `reflex_sync_partitions` BEFORE inserting into a new source
    // partition — otherwise the trigger would route the row to a missing
    // IMV partition and ERROR.  Reconcile then re-syncs + rebuilds.
    Spi::run("CREATE TABLE part_rec2_s PARTITION OF part_rec2 FOR VALUES IN ('S')").expect("p2");
    Spi::run("SELECT reflex_sync_partitions('part_rec2_v')").expect("sync");
    Spi::run("INSERT INTO part_rec2 (id, region, amount) VALUES (2, 'S', 50)").expect("seed2");

    // Full reconcile (will re-sync at entry, then rebuild)
    let r = Spi::get_one::<&str>("SELECT reflex_reconcile('part_rec2_v')")
        .expect("call")
        .expect("res");
    assert_eq!(r, "RECONCILED");

    // Verify both regions are correctly materialized after reconcile
    let n_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_rec2_v WHERE region = 'N'",
    )
    .expect("q")
    .expect("n");
    assert_eq!(n_total.to_string(), "10");
    let s_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_rec2_v WHERE region = 'S'",
    )
    .expect("q")
    .expect("s");
    assert_eq!(s_total.to_string(), "50");
}

#[pg_test]
fn pg_part_drop_imv_drops_all_partitions() {
    Spi::run("CREATE TABLE part_drop_imv (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_drop_imv_n PARTITION OF part_drop_imv FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO part_drop_imv (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_drop_imv_v', \
            'SELECT region, SUM(amount) AS total FROM part_drop_imv GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create");
    Spi::run("SELECT drop_reflex_ivm('part_drop_imv_v')").expect("drop");

    let exists = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname IN \
         ('part_drop_imv_v', 'part_drop_imv_v_part_drop_imv_n', \
          '__reflex_intermediate_part_drop_imv_v', '__reflex_intermediate_part_drop_imv_v_part_drop_imv_n')",
    )
    .expect("q")
    .expect("c");
    assert_eq!(
        exists, 0,
        "drop_reflex_ivm should remove parent + all partition children"
    );
}

/// Phase B (plans/partitioning_3.md §2): explicit partition_by on a
/// computed GROUP BY expression (e.g. DATE_TRUNC) must be rejected.  The
/// trigger codegen needs to find the partition key on transition tables
/// by bare reference; computed GROUP BY expressions would require
/// re-evaluating the function on every transition row.
#[pg_test]
fn pg_part_explicit_computed_partition_by_errors() {
    Spi::run(
        "CREATE TABLE part_comp (id BIGINT, d DATE, amount NUMERIC) PARTITION BY LIST (d)",
    )
    .expect("create");
    Spi::run("CREATE TABLE part_comp_p1 PARTITION OF part_comp FOR VALUES IN ('2026-01-01')")
        .expect("p1");
    Spi::run("INSERT INTO part_comp VALUES (1, '2026-01-01', 10)").expect("seed");

    // partition_by names the *alias* `month` which maps to DATE_TRUNC(...) —
    // computed, should be rejected.
    let res = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'part_comp_v', \
            'SELECT DATE_TRUNC(''month'', d) AS month, SUM(amount) AS total \
             FROM part_comp GROUP BY DATE_TRUNC(''month'', d)', \
            NULL, NULL, NULL, NULL, \
            ARRAY['month'] \
         )",
    )
    .expect("call")
    .expect("res");
    assert!(
        res.contains("ERROR")
            && (res.contains("computed") || res.contains("bare")),
        "expected computed-GROUP-BY rejection, got: {res}"
    );
}

/// Phase B: explicit partition_by on a bare GROUP BY column is accepted.
#[pg_test]
fn pg_part_explicit_bare_partition_by_accepted() {
    Spi::run(
        "CREATE TABLE part_bare (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    Spi::run("CREATE TABLE part_bare_p1 PARTITION OF part_bare FOR VALUES IN ('A')").expect("p1");
    Spi::run("INSERT INTO part_bare VALUES (1, 'A', 10)").expect("seed");

    let res = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'part_bare_v', \
            'SELECT region, SUM(amount) AS total FROM part_bare GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("call")
    .expect("res");
    assert!(
        res.starts_with("CREATE REFLEX") || res.contains("OK"),
        "expected acceptance, got: {res}"
    );
}

/// Global `reflex_reconcile` on a partitioned IMV uses the per-child
/// swap path instead of TRUNCATE+INSERT on the parent.  Verifies:
///   * Every partition is rebuilt correctly.
///   * No `__reflex_swap_*` leftover after the call (rename worked).
///   * Drift on multiple children is cleared.
#[pg_test]
fn pg_part_global_reconcile_swaps_each_partition() {
    Spi::run(
        "CREATE TABLE part_grec (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    for r in ["A", "B", "C"] {
        Spi::run(&format!(
            "CREATE TABLE part_grec_{r} PARTITION OF part_grec FOR VALUES IN ('{r}')"
        ))
        .expect("p");
    }
    Spi::run("INSERT INTO part_grec VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)")
        .expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_grec_v', \
            'SELECT region, SUM(amount) AS total FROM part_grec GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("imv");

    // Drift every child.
    Spi::run("UPDATE part_grec_v SET total = -1 WHERE region IN ('A', 'B', 'C')")
        .expect("drift");

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('part_grec_v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");

    for (r, expected) in [("A", "10"), ("B", "20"), ("C", "30")] {
        let q = format!("SELECT total FROM part_grec_v WHERE region = '{r}'");
        let v = Spi::get_one::<pgrx::AnyNumeric>(&q)
            .expect("q")
            .expect("v");
        assert_eq!(v.to_string(), expected, "region {r}");
    }

    // No swap tables remain.
    let leftover = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class \
         WHERE relkind = 'r' AND relname LIKE '__reflex_swap_%_part_grec_v_%'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(leftover, 0);
}

/// Regression: global `reflex_reconcile` on a partitioned *passthrough* IMV
/// (no intermediate table) must rebuild every child without touching the
/// absent `__reflex_intermediate_<view>` relation.  Before the fix the
/// partitioned reconcile branch unconditionally ran
/// `ANALYZE __reflex_intermediate_<view>`, raising 42P01 for passthrough IMVs
/// and aborting the whole reconcile.
#[pg_test]
fn pg_part_global_reconcile_passthrough_no_intermediate() {
    Spi::run(
        "CREATE TABLE part_grecp (dem_plan_id BIGINT, product_id BIGINT, qty NUMERIC) \
         PARTITION BY LIST (dem_plan_id)",
    )
    .expect("create");
    for id in [1, 2, 3] {
        Spi::run(&format!(
            "CREATE TABLE part_grecp_{id} PARTITION OF part_grecp FOR VALUES IN ({id})"
        ))
        .expect("p");
    }
    Spi::run("INSERT INTO part_grecp VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30)")
        .expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_grecp_v', \
            'SELECT dem_plan_id, product_id, qty FROM part_grecp', \
            'dem_plan_id,product_id', NULL, NULL, NULL, \
            ARRAY['dem_plan_id'] \
         )",
    )
    .expect("imv");

    // Precondition: passthrough → no intermediate table exists.
    let int_tables = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname = '__reflex_intermediate_part_grecp_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(int_tables, 0, "passthrough IMV must have no intermediate table");

    // Drift every child, then global-reconcile.
    Spi::run("UPDATE part_grecp_v SET qty = -1").expect("drift");

    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('part_grecp_v')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");

    for (id, expected) in [(1, "10"), (2, "20"), (3, "30")] {
        let q = format!("SELECT qty FROM part_grecp_v WHERE dem_plan_id = {id}");
        let v = Spi::get_one::<pgrx::AnyNumeric>(&q).expect("q").expect("v");
        assert_eq!(v.to_string(), expected, "dem_plan_id {id}");
    }
}

/// Cascade post-swap: a partitioned parent IMV with a partitioned child
/// IMV.  When the parent's partition is reconciled via the atomic swap,
/// the cascade picks up the new partition data correctly.
#[pg_test]
fn pg_part_cascade_after_swap_sees_new_data() {
    Spi::run(
        "CREATE TABLE part_csw (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    Spi::run("CREATE TABLE part_csw_a PARTITION OF part_csw FOR VALUES IN ('A')").expect("p");
    Spi::run("CREATE TABLE part_csw_b PARTITION OF part_csw FOR VALUES IN ('B')").expect("p");
    Spi::run("INSERT INTO part_csw VALUES (1, 'A', 10), (2, 'B', 5)").expect("seed");

    let c1 = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'part_csw_p', \
            'SELECT region, SUM(amount) AS total FROM part_csw GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("p_imv")
    .expect("p_imv_res");
    assert!(!c1.starts_with("ERROR"), "parent imv: {c1}");

    // Child IMV: aggregate so we get an intermediate (passthrough chains
    // surface other complexity unrelated to the swap-cascade path).
    let c2 = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'part_csw_c', \
            'SELECT region, SUM(total) AS doubled FROM part_csw_p GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("c_imv")
    .expect("c_imv_res");
    assert!(!c2.starts_with("ERROR"), "child imv: {c2}");

    // Corrupt the parent's A row, then reconcile A.  Cascade must
    // propagate the rebuilt parent into the child.
    Spi::run("UPDATE part_csw_p SET total = 999 WHERE region = 'A'").expect("corrupt");
    Spi::run("UPDATE part_csw_c SET doubled = 9999 WHERE region = 'A'").expect("corrupt2");

    let msg = Spi::get_one::<String>("SELECT reflex_reconcile_partition('part_csw_p', 'A')")
        .expect("rec")
        .expect("msg");
    assert!(msg.starts_with("RECONCILED"), "{msg}");

    // Parent's A = 10 (rebuilt from source).
    let a_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM part_csw_p WHERE region = 'A'",
    )
    .expect("q")
    .expect("a");
    assert_eq!(a_total.to_string(), "10");

    // Child's A = parent's A total = 10 (rebuilt via cascade).
    let c_doubled = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT doubled FROM part_csw_c WHERE region = 'A'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(c_doubled.to_string(), "10", "child should reflect rebuilt parent");
}

/// Scoped cascade: a partitioned parent IMV with a NON-partitioned child IMV
/// that groups by the parent's partition key (the `forecast_dp_year_agg`
/// shape). Reconciling ONE parent partition must rebuild only that key's child
/// groups — it must NOT full-rebuild every key. Proven by corrupting an
/// unrelated key's child row and asserting it survives a reconcile of a
/// different partition (a full reflex_reconcile of the child would overwrite
/// it back to the correct value).
#[pg_test]
fn pg_part_cascade_scoped_to_reconciled_key() {
    Spi::run(
        "CREATE TABLE part_scope (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    Spi::run("CREATE TABLE part_scope_a PARTITION OF part_scope FOR VALUES IN ('A')").expect("pa");
    Spi::run("CREATE TABLE part_scope_b PARTITION OF part_scope FOR VALUES IN ('B')").expect("pb");
    Spi::run("INSERT INTO part_scope VALUES (1, 'A', 10), (2, 'B', 5)").expect("seed");

    // Parent IMV: partitioned by region.
    let p = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'part_scope_p', \
            'SELECT region, SUM(amount) AS total FROM part_scope GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("p_imv")
    .expect("p_imv_res");
    assert!(!p.starts_with("ERROR"), "parent imv: {p}");

    // Child IMV: aggregate grouped by region but EXPLICITLY non-partitioned
    // (ARRAY[]::text[] forces unpartitioned on a partitioned source).
    let c = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'part_scope_c', \
            'SELECT region, SUM(total) AS s FROM part_scope_p GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY[]::text[] \
         )",
    )
    .expect("c_imv")
    .expect("c_imv_res");
    assert!(!c.starts_with("ERROR"), "child imv: {c}");

    // Corrupt both child groups directly. A is the partition we reconcile
    // (must be rebuilt -> fixed). B is unrelated (scoped reconcile must NOT
    // touch it).
    Spi::run("UPDATE part_scope_c SET s = 8888 WHERE region = 'A'").expect("corrupt a");
    Spi::run("UPDATE part_scope_c SET s = 9999 WHERE region = 'B'").expect("corrupt b");

    let msg = Spi::get_one::<String>("SELECT reflex_reconcile_partition('part_scope_p', 'A')")
        .expect("rec")
        .expect("msg");
    assert!(msg.starts_with("RECONCILED"), "{msg}");

    // A was reconciled -> child A rebuilt from the parent (= 10).
    let a = Spi::get_one::<pgrx::AnyNumeric>("SELECT s FROM part_scope_c WHERE region = 'A'")
        .expect("qa")
        .expect("a");
    assert_eq!(a.to_string(), "10", "child A must be rebuilt by the cascade");

    // B was NOT reconciled -> a scoped cascade leaves it untouched (still 9999).
    // The buggy full reflex_reconcile would overwrite B back to 5.
    let b = Spi::get_one::<pgrx::AnyNumeric>("SELECT s FROM part_scope_c WHERE region = 'B'")
        .expect("qb")
        .expect("b");
    assert_eq!(
        b.to_string(),
        "9999",
        "reconciling partition A must NOT rebuild unrelated key B (scoped cascade)"
    );
}

/// Scoped cascade through the FLUSH path (the production forecast-push path):
/// `reflex_flush_partition_source` passes a source partition, not keys, so the
/// affected key set is DERIVED from the reconciled parent. Attaching a new key
/// must scope the cascade into a non-partitioned aggregate dependent to that
/// key only — leaving unrelated keys untouched. Guards the derivation against
/// silently scoping to the wrong key (which the DO-block EXCEPTION cannot
/// catch).
#[pg_test]
fn pg_part_flush_cascade_scoped_to_attached_key() {
    Spi::run(
        "CREATE TABLE fsc_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    Spi::run("CREATE TABLE fsc_src_n PARTITION OF fsc_src FOR VALUES IN ('N')").expect("pn");
    Spi::run("INSERT INTO fsc_src VALUES (1, 'N', 10)").expect("seed");

    let p = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'fsc_p', \
            'SELECT region, SUM(amount) AS total FROM fsc_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("p_imv")
    .expect("p_imv_res");
    assert!(!p.starts_with("ERROR"), "parent imv: {p}");

    let c = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'fsc_c', \
            'SELECT region, SUM(total) AS s FROM fsc_p GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY[]::text[] \
         )",
    )
    .expect("c_imv")
    .expect("c_imv_res");
    assert!(!c.starts_with("ERROR"), "child imv: {c}");

    // Corrupt the EXISTING, unrelated key N's child group.
    Spi::run("UPDATE fsc_c SET s = 7777 WHERE region = 'N'").expect("corrupt n");

    // Attach a NEW partition S with data — the key the flush will process.
    Spi::run("CREATE TABLE fsc_src_s (id BIGINT, region TEXT NOT NULL, amount NUMERIC)")
        .expect("detached child");
    Spi::run("INSERT INTO fsc_src_s VALUES (2, 'S', 50)").expect("child data");
    Spi::run("ALTER TABLE fsc_src ATTACH PARTITION fsc_src_s FOR VALUES IN ('S')").expect("attach");

    // Flush: reconciles S in the parent and cascades, scoped to the derived key S.
    Spi::run("SELECT reflex_flush_partition_source('fsc_src')").expect("flush");

    // S now flows into the child (scoped reconcile of the newly attached key).
    let s = Spi::get_one::<pgrx::AnyNumeric>("SELECT s FROM fsc_c WHERE region = 'S'")
        .expect("qs")
        .expect("s");
    assert_eq!(s.to_string(), "50", "attached key S must appear in the child");

    // N is unrelated to the flushed key S -> derived-key scoping must leave it
    // untouched (corruption survives). A full reconcile would reset it to 10.
    let n = Spi::get_one::<pgrx::AnyNumeric>("SELECT s FROM fsc_c WHERE region = 'N'")
        .expect("qn")
        .expect("n");
    assert_eq!(
        n.to_string(),
        "7777",
        "flushing key S must NOT rebuild unrelated key N (derived-key scoped cascade)"
    );
}

/// Cascade dedup: one flush that reconciles MULTIPLE leaves of a single
/// partitioned parent IMV must cascade into a non-co-partitioned aggregate
/// dependent ONCE — not once per reconciled leaf (the `forecast_dp_year_agg`
/// per-month-push redundancy: 12 monthly leaves of one plan would otherwise
/// rebuild that plan's aggregate slice 12×). Proven by counting `INSERT`
/// statements on the child's target via a statement-level trigger: a single
/// flush attaching two non-empty leaves must produce exactly one cascade
/// `INSERT`, while still landing both keys correctly.
#[pg_test]
fn pg_part_flush_cascade_fires_once_for_multileaf() {
    Spi::run(
        "CREATE TABLE fdd_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    Spi::run("CREATE TABLE fdd_src_n PARTITION OF fdd_src FOR VALUES IN ('N')").expect("pn");
    Spi::run("INSERT INTO fdd_src VALUES (1, 'N', 10)").expect("seed");

    let p = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'fdd_p', \
            'SELECT region, SUM(amount) AS total FROM fdd_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("p_imv")
    .expect("p_imv_res");
    assert!(!p.starts_with("ERROR"), "parent imv: {p}");

    let c = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'fdd_c', \
            'SELECT region, SUM(total) AS s FROM fdd_p GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY[]::text[] \
         )",
    )
    .expect("c_imv")
    .expect("c_imv_res");
    assert!(!c.starts_with("ERROR"), "child imv: {c}");

    // Count INSERT statements on the child target. Only the cascade writes to
    // fdd_c (the parent's swap touches its own children, not fdd_c), so this
    // counts cascade firings for the flush.
    Spi::run("CREATE TABLE fdd_casc_count (n INT)").expect("counter table");
    Spi::run("INSERT INTO fdd_casc_count VALUES (0)").expect("counter seed");
    Spi::run(
        "CREATE FUNCTION fdd_casc_bump() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN UPDATE fdd_casc_count SET n = n + 1; RETURN NULL; END $$",
    )
    .expect("counter fn");
    Spi::run(
        "CREATE TRIGGER fdd_casc_trg AFTER INSERT ON fdd_c \
         FOR EACH STATEMENT EXECUTE FUNCTION fdd_casc_bump()",
    )
    .expect("counter trigger");

    // Attach TWO new non-empty leaves in one batch -> one flush, two reconciled
    // parent nodes, two cascade firings without dedup.
    Spi::run("CREATE TABLE fdd_src_a (id BIGINT, region TEXT NOT NULL, amount NUMERIC)")
        .expect("a child");
    Spi::run("INSERT INTO fdd_src_a VALUES (2, 'A', 50)").expect("a data");
    Spi::run("ALTER TABLE fdd_src ATTACH PARTITION fdd_src_a FOR VALUES IN ('A')").expect("attach a");
    Spi::run("CREATE TABLE fdd_src_b (id BIGINT, region TEXT NOT NULL, amount NUMERIC)")
        .expect("b child");
    Spi::run("INSERT INTO fdd_src_b VALUES (3, 'B', 70)").expect("b data");
    Spi::run("ALTER TABLE fdd_src ATTACH PARTITION fdd_src_b FOR VALUES IN ('B')").expect("attach b");

    // Reset the counter so only the flush's cascade INSERTs are measured.
    Spi::run("UPDATE fdd_casc_count SET n = 0").expect("reset");

    Spi::run("SELECT reflex_flush_partition_source('fdd_src')").expect("flush");

    // Both keys must land correctly in the child (dedup must not lose work).
    let a = Spi::get_one::<pgrx::AnyNumeric>("SELECT s FROM fdd_c WHERE region = 'A'")
        .expect("qa")
        .expect("a");
    assert_eq!(a.to_string(), "50", "attached key A must appear in the child");
    let b = Spi::get_one::<pgrx::AnyNumeric>("SELECT s FROM fdd_c WHERE region = 'B'")
        .expect("qb")
        .expect("b");
    assert_eq!(b.to_string(), "70", "attached key B must appear in the child");

    // The cascade must have fired exactly once for the whole flush.
    let fires = Spi::get_one::<i32>("SELECT n FROM fdd_casc_count")
        .expect("count")
        .expect("count_res");
    assert_eq!(
        fires, 1,
        "cascade into the non-co-partitioned dependent must fire ONCE per flush, \
         not once per reconciled leaf"
    );
}

/// Idempotent recovery: when a swap is left orphaned (simulated by
/// manually creating a `__reflex_swap_*` table), the next reconcile
/// drops it cleanly.
#[pg_test]
fn pg_part_reconcile_drops_orphan_swap_tables() {
    Spi::run(
        "CREATE TABLE part_orph (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    Spi::run("CREATE TABLE part_orph_a PARTITION OF part_orph FOR VALUES IN ('A')").expect("p");
    Spi::run("INSERT INTO part_orph VALUES (1, 'A', 10)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_orph_v', \
            'SELECT region, SUM(amount) AS total FROM part_orph GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create_imv");

    // Manually create an orphan swap table to simulate a prior failure.
    Spi::run(
        "CREATE TABLE \"__reflex_swap_int_part_orph_v_part_orph_a\" \
         (LIKE \"__reflex_intermediate_part_orph_v_part_orph_a\" INCLUDING ALL)",
    )
    .expect("orphan");

    // Reconcile should clean up the orphan and succeed.
    let msg = Spi::get_one::<String>("SELECT reflex_reconcile_partition('part_orph_v', 'A')")
        .expect("rec")
        .expect("msg");
    assert!(
        msg.starts_with("RECONCILED"),
        "expected reconcile success, got: {msg}"
    );

    // Orphan should be gone.
    let exists = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname = '__reflex_swap_int_part_orph_v_part_orph_a'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(exists, 0, "orphan swap table should have been dropped");
}

#[pg_test]
fn pg_part_passthrough_auto_mirror_when_projected() {
    Spi::run("CREATE TABLE part_pt_a (id BIGINT, region TEXT NOT NULL, val TEXT) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_pt_a_n PARTITION OF part_pt_a FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO part_pt_a (id, region, val) VALUES (1, 'N', 'hello')").expect("seed");

    // Passthrough projecting region — auto-mirror should pick up partitioning
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_pt_a_v', \
            'SELECT id, region, val FROM part_pt_a' \
         )",
    )
    .expect("create");

    let strategy = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid \
         WHERE c.relname = 'part_pt_a_v'",
    )
    .expect("strategy")
    .expect("s");
    assert_eq!(strategy, "l");

    // Data populated correctly
    let val = Spi::get_one::<String>("SELECT val FROM part_pt_a_v WHERE id = 1")
        .expect("q")
        .expect("v");
    assert_eq!(val, "hello");
}

// ---------------------------------------------------------------------------
// 1.6.0: event-trigger-driven auto-sync of IMV partitions when the source
// partition tree changes (ALTER TABLE … ATTACH/DETACH PARTITION, or
// CREATE TABLE … PARTITION OF source). The trigger lives in src/lib.rs.
// ---------------------------------------------------------------------------

#[pg_test]
fn pg_part_event_trigger_create_partition_of_auto_syncs() {
    Spi::run(
        "CREATE TABLE ev_src_a (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE ev_src_a_n PARTITION OF ev_src_a FOR VALUES IN ('N')")
        .expect("p1");
    Spi::run("INSERT INTO ev_src_a (id, region, amount) VALUES (1, 'N', 10)")
        .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'ev_src_a_v', \
            'SELECT region, SUM(amount) AS total FROM ev_src_a GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create imv");

    let before = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'ev_src_a_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(before, 1);

    // CREATE TABLE … PARTITION OF should fire the event trigger and auto-sync.
    Spi::run("CREATE TABLE ev_src_a_s PARTITION OF ev_src_a FOR VALUES IN ('S')")
        .expect("new partition");

    let after = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'ev_src_a_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(after, 2, "IMV should have a matching partition after CREATE PARTITION OF");

    // INSERT into the new partition value must propagate to the IMV.
    Spi::run("INSERT INTO ev_src_a (id, region, amount) VALUES (2, 'S', 25)")
        .expect("insert routed to new partition");
    let total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM ev_src_a_v WHERE region = 'S'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(total.to_string(), "25");
}

#[pg_test]
fn pg_part_event_trigger_attach_partition_auto_syncs() {
    Spi::run(
        "CREATE TABLE ev_at_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE ev_at_src_n PARTITION OF ev_at_src FOR VALUES IN ('N')")
        .expect("p1");
    Spi::run("INSERT INTO ev_at_src (id, region, amount) VALUES (1, 'N', 10)")
        .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'ev_at_src_v', \
            'SELECT region, SUM(amount) AS total FROM ev_at_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create imv");

    // Build a detached child table first, then ATTACH PARTITION.
    Spi::run("CREATE TABLE ev_at_src_e (id BIGINT, region TEXT NOT NULL, amount NUMERIC)")
        .expect("detached child");
    Spi::run(
        "ALTER TABLE ev_at_src ATTACH PARTITION ev_at_src_e FOR VALUES IN ('E')",
    )
    .expect("attach");

    let after = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'ev_at_src_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(after, 2, "IMV should have a matching partition after ATTACH");
}

#[pg_test]
fn pg_part_event_trigger_skips_unpartitioned_imv() {
    Spi::run(
        "CREATE TABLE ev_skip_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE ev_skip_src_n PARTITION OF ev_skip_src FOR VALUES IN ('N')")
        .expect("p1");
    Spi::run("INSERT INTO ev_skip_src (id, region, amount) VALUES (1, 'N', 10)")
        .expect("seed");

    // Aggregate IMV that does NOT project the partition column (region) — so
    // auto-mirror is skipped and the IMV stays unpartitioned. The event
    // trigger must skip auto-sync for this IMV.
    Spi::run(
        "SELECT create_reflex_ivm( \
            'ev_skip_src_v', \
            'SELECT SUM(amount) AS total FROM ev_skip_src' \
         )",
    )
    .expect("create non-partitioned imv");

    let is_partitioned_before = Spi::get_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_partitioned_table pt \
                        JOIN pg_class c ON c.oid = pt.partrelid \
                        WHERE c.relname = 'ev_skip_src_v')",
    )
    .expect("q")
    .expect("v");
    assert!(
        !is_partitioned_before,
        "IMV without projected partition column must not auto-mirror"
    );

    // CREATE PARTITION OF on source must not fail (auto-sync should skip
    // because the IMV is unpartitioned).
    Spi::run("CREATE TABLE ev_skip_src_s PARTITION OF ev_skip_src FOR VALUES IN ('S')")
        .expect("new source partition should not break the event trigger");

    let is_partitioned_after = Spi::get_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_partitioned_table pt \
                        JOIN pg_class c ON c.oid = pt.partrelid \
                        WHERE c.relname = 'ev_skip_src_v')",
    )
    .expect("q")
    .expect("v");
    assert!(!is_partitioned_after, "non-partitioned IMV should stay unpartitioned");
}

/// Incremental partition delta (plans/2026-06-11): attaching a partition whose
/// rows BELONG in an unpartitioned IMV must be maintained incrementally — as the
/// bulk INSERT it semantically is — NOT by a full TRUNCATE+rebuild reconcile.
/// Observable: the IMV target's `relfilenode` is reassigned by TRUNCATE but
/// stable across an incremental MERGE/INSERT.
#[pg_test]
fn pg_part_attach_matching_partition_maintains_unpartitioned_imv_incrementally() {
    Spi::run(
        "CREATE TABLE iamp_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE iamp_src_n PARTITION OF iamp_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO iamp_src (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    // Unpartitioned passthrough IMV, single source, filter keeps regions N and S.
    Spi::run(
        "SELECT create_reflex_ivm( \
            'iamp_v', \
            'SELECT id, amount FROM iamp_src WHERE region IN (''N'',''S'')', \
            'id' \
         )",
    )
    .expect("create imv");

    let before_cnt =
        Spi::get_one::<i64>("SELECT count(*) FROM iamp_v").expect("q").expect("c");
    assert_eq!(before_cnt, 1, "baseline: only the N row");
    let before_node = Spi::get_one::<i64>(
        "SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'iamp_v'",
    )
    .expect("q")
    .expect("n");

    // Attach a NEW partition 'S' WITH data — these rows belong in the IMV.
    Spi::run("CREATE TABLE iamp_src_s (id BIGINT, region TEXT NOT NULL, amount NUMERIC)")
        .expect("detached child");
    Spi::run("INSERT INTO iamp_src_s (id, region, amount) VALUES (2, 'S', 50)").expect("child data");
    Spi::run("ALTER TABLE iamp_src ATTACH PARTITION iamp_src_s FOR VALUES IN ('S')")
        .expect("attach");

    // Drive the deferred flush (the COMMIT-time constraint trigger doesn't fire
    // inside the test transaction).
    Spi::run("SELECT reflex_flush_partition_source('iamp_src')").expect("flush");

    let after_cnt =
        Spi::get_one::<i64>("SELECT count(*) FROM iamp_v").expect("q").expect("c");
    assert_eq!(
        after_cnt, 2,
        "matching attached partition rows must appear in the IMV"
    );

    let after_node = Spi::get_one::<i64>(
        "SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'iamp_v'",
    )
    .expect("q")
    .expect("n");
    assert_eq!(
        before_node, after_node,
        "attach must be maintained incrementally, not by a full TRUNCATE reconcile"
    );
}

/// Incremental partition delta (plans/2026-06-11): attaching a partition whose
/// rows are entirely rejected by the IMV's WHERE filter is a no-op — the IMV must
/// be neither reconciled nor written. Observable: target relfilenode stable +
/// content unchanged.
#[pg_test]
fn pg_part_attach_irrelevant_partition_is_noop_for_unpartitioned_imv() {
    Spi::run(
        "CREATE TABLE ian_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE ian_src_n PARTITION OF ian_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO ian_src (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    // Filter keeps ONLY region 'N'.
    Spi::run(
        "SELECT create_reflex_ivm('ian_v', 'SELECT id, amount FROM ian_src WHERE region = ''N''', 'id')",
    )
    .expect("create imv");

    let before_node = Spi::get_one::<i64>(
        "SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'ian_v'",
    )
    .expect("q")
    .expect("n");

    // Attach a NEW partition 'S' WITH data — all rows are filtered out.
    Spi::run("CREATE TABLE ian_src_s (id BIGINT, region TEXT NOT NULL, amount NUMERIC)")
        .expect("child");
    Spi::run("INSERT INTO ian_src_s (id, region, amount) VALUES (2, 'S', 50)").expect("child data");
    Spi::run("ALTER TABLE ian_src ATTACH PARTITION ian_src_s FOR VALUES IN ('S')").expect("attach");
    Spi::run("SELECT reflex_flush_partition_source('ian_src')").expect("flush");

    let after_cnt = Spi::get_one::<i64>("SELECT count(*) FROM ian_v").expect("q").expect("c");
    assert_eq!(after_cnt, 1, "filtered-out partition must not change IMV content");
    let after_node = Spi::get_one::<i64>(
        "SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'ian_v'",
    )
    .expect("q")
    .expect("n");
    assert_eq!(
        before_node, after_node,
        "an irrelevant partition attach must not reconcile (no TRUNCATE) the IMV"
    );
}

/// Incremental partition delta (plans/2026-06-11): the headline payoff — an
/// irrelevant attach produces no write to the base IMV, so the trigger-driven
/// cascade never fires and a downstream child IMV is left completely untouched.
#[pg_test]
fn pg_part_attach_irrelevant_partition_does_not_cascade_to_child() {
    Spi::run(
        "CREATE TABLE iac_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE iac_src_n PARTITION OF iac_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO iac_src (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    // Base unpartitioned IMV filtered to region 'N'; child aggregate on top of it.
    Spi::run(
        "SELECT create_reflex_ivm('iac_base', 'SELECT id, amount FROM iac_src WHERE region = ''N''', 'id')",
    )
    .expect("create base imv");
    Spi::run(
        "SELECT create_reflex_ivm('iac_child', 'SELECT count(*) AS c, sum(amount) AS s FROM iac_base')",
    )
    .expect("create child imv");

    let child_node_before = Spi::get_one::<i64>(
        "SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'iac_child'",
    )
    .expect("q")
    .expect("n");

    // Attach an irrelevant partition 'S' with data.
    Spi::run("CREATE TABLE iac_src_s (id BIGINT, region TEXT NOT NULL, amount NUMERIC)")
        .expect("child tbl");
    Spi::run("INSERT INTO iac_src_s (id, region, amount) VALUES (2, 'S', 50)").expect("child data");
    Spi::run("ALTER TABLE iac_src ATTACH PARTITION iac_src_s FOR VALUES IN ('S')").expect("attach");
    Spi::run("SELECT reflex_flush_partition_source('iac_src')").expect("flush");

    // Child content correct (still only the N row counted)...
    let child_c = Spi::get_one::<i64>("SELECT c FROM iac_child").expect("q").expect("c");
    assert_eq!(child_c, 1, "child must still reflect only the in-filter row");
    // ...and the cascade never touched the child target (no TRUNCATE/rebuild).
    let child_node_after = Spi::get_one::<i64>(
        "SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'iac_child'",
    )
    .expect("q")
    .expect("n");
    assert_eq!(
        child_node_before, child_node_after,
        "irrelevant attach must not cascade a rebuild to downstream child IMVs"
    );
}

/// Incremental partition delta (plans/2026-06-11): DETACH of an in-filter
/// partition removes its rows from the unpartitioned IMV via a DELETE delta.
#[pg_test]
fn pg_part_detach_in_filter_partition_removes_rows_incrementally() {
    Spi::run(
        "CREATE TABLE idd_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE idd_src_n PARTITION OF idd_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("CREATE TABLE idd_src_s PARTITION OF idd_src FOR VALUES IN ('S')").expect("p2");
    Spi::run("INSERT INTO idd_src (id, region, amount) VALUES (1, 'N', 10), (2, 'S', 50)")
        .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('idd_v', 'SELECT id, amount FROM idd_src WHERE region IN (''N'',''S'')', 'id')",
    )
    .expect("create imv");
    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM idd_v").expect("q").expect("c"),
        2,
        "baseline: both rows"
    );

    Spi::run("ALTER TABLE idd_src DETACH PARTITION idd_src_s").expect("detach");
    Spi::run("SELECT reflex_flush_partition_source('idd_src')").expect("flush");

    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM idd_v").expect("q").expect("c"),
        1,
        "detached partition's rows must be removed"
    );
    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM idd_v WHERE id = 2").expect("q").expect("c"),
        0,
        "the S row (id=2) must be gone"
    );
}

/// Incremental partition delta (plans/2026-06-11): creating an EMPTY new
/// partition is a no-op — empty transition produces no write.
#[pg_test]
fn pg_part_attach_empty_partition_is_noop() {
    Spi::run(
        "CREATE TABLE iep_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE iep_src_n PARTITION OF iep_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO iep_src (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('iep_v', 'SELECT id, amount FROM iep_src WHERE region IN (''N'',''S'')', 'id')",
    )
    .expect("create imv");

    let before_node = Spi::get_one::<i64>(
        "SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'iep_v'",
    )
    .expect("q")
    .expect("n");

    // CREATE an empty new partition (no data).
    Spi::run("CREATE TABLE iep_src_s PARTITION OF iep_src FOR VALUES IN ('S')").expect("p2");
    Spi::run("SELECT reflex_flush_partition_source('iep_src')").expect("flush");

    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM iep_v").expect("q").expect("c"),
        1,
        "empty partition must not change content"
    );
    let after_node = Spi::get_one::<i64>(
        "SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'iep_v'",
    )
    .expect("q")
    .expect("n");
    assert_eq!(before_node, after_node, "empty partition must not reconcile the IMV");
}

/// Incremental partition delta (plans/2026-06-11): an AGGREGATE unpartitioned IMV
/// folds an attached partition's rows into its groups incrementally; result
/// matches a from-scratch recomputation (oracle).
#[pg_test]
fn pg_part_attach_matching_partition_updates_aggregate_imv() {
    Spi::run(
        "CREATE TABLE iag_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE iag_src_n PARTITION OF iag_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO iag_src (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('iag_v', 'SELECT sum(amount) AS total FROM iag_src WHERE region IN (''N'',''S'')')",
    )
    .expect("create imv");
    assert_eq!(
        Spi::get_one::<pgrx::AnyNumeric>("SELECT total FROM iag_v").expect("q").expect("t").to_string(),
        "10"
    );

    Spi::run("CREATE TABLE iag_src_s (id BIGINT, region TEXT NOT NULL, amount NUMERIC)").expect("child");
    Spi::run("INSERT INTO iag_src_s (id, region, amount) VALUES (2, 'S', 50)").expect("child data");
    Spi::run("ALTER TABLE iag_src ATTACH PARTITION iag_src_s FOR VALUES IN ('S')").expect("attach");
    Spi::run("SELECT reflex_flush_partition_source('iag_src')").expect("flush");

    // Oracle: incremental result equals a fresh recomputation.
    let imv_total = Spi::get_one::<pgrx::AnyNumeric>("SELECT total FROM iag_v")
        .expect("q")
        .expect("t")
        .to_string();
    let fresh_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT sum(amount) FROM iag_src WHERE region IN ('N','S')",
    )
    .expect("q")
    .expect("t")
    .to_string();
    assert_eq!(imv_total, fresh_total, "aggregate IMV must match fresh recomputation");
    assert_eq!(imv_total, "60");
}

/// Incremental partition delta (plans/2026-06-11): a JOIN IMV (multi-source, so
/// empty where_predicate) on a partitioned source still applies an attached
/// partition correctly — the delta joins the transition to the dimension table.
/// Oracle: incremental result equals a fresh recomputation.
#[pg_test]
fn pg_part_attach_partition_join_imv_matches_fresh() {
    Spi::run(
        "CREATE TABLE ijn_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE ijn_src_n PARTITION OF ijn_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("CREATE TABLE ijn_dim (id BIGINT, label TEXT)").expect("dim");
    Spi::run("INSERT INTO ijn_dim (id, label) VALUES (1, 'a'), (2, 'b')").expect("dim seed");
    Spi::run("INSERT INTO ijn_src (id, region, amount) VALUES (1, 'N', 10)").expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm('ijn_v', \
            'SELECT s.id, s.amount, d.label FROM ijn_src s JOIN ijn_dim d ON d.id = s.id', 'id')",
    )
    .expect("create join imv");

    Spi::run("CREATE TABLE ijn_src_s (id BIGINT, region TEXT NOT NULL, amount NUMERIC)")
        .expect("child");
    Spi::run("INSERT INTO ijn_src_s (id, region, amount) VALUES (2, 'S', 50)").expect("child data");
    Spi::run("ALTER TABLE ijn_src ATTACH PARTITION ijn_src_s FOR VALUES IN ('S')").expect("attach");
    Spi::run("SELECT reflex_flush_partition_source('ijn_src')").expect("flush");

    // Oracle: the IMV exactly equals a from-scratch join.
    let diff = Spi::get_one::<i64>(
        "SELECT count(*) FROM ( \
            (SELECT * FROM ijn_v \
               EXCEPT ALL SELECT s.id, s.amount, d.label FROM ijn_src s JOIN ijn_dim d ON d.id = s.id) \
            UNION ALL \
            (SELECT s.id, s.amount, d.label FROM ijn_src s JOIN ijn_dim d ON d.id = s.id \
               EXCEPT ALL SELECT * FROM ijn_v) \
         ) __oracle",
    )
    .expect("q")
    .expect("c");
    assert_eq!(diff, 0, "join IMV must equal a fresh recomputation after attach");
    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM ijn_v").expect("q").expect("c"),
        2,
        "both joined rows present"
    );
}

#[pg_test]
fn pg_part_event_trigger_detach_keeps_orphan_partition() {
    // drop_orphans=FALSE is the safety default for auto-sync — DETACH on the
    // source must NOT delete the IMV partition (it may still hold data the
    // operator wants to query).
    Spi::run(
        "CREATE TABLE ev_det_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE ev_det_src_n PARTITION OF ev_det_src FOR VALUES IN ('N')")
        .expect("p1");
    Spi::run("CREATE TABLE ev_det_src_s PARTITION OF ev_det_src FOR VALUES IN ('S')")
        .expect("p2");
    Spi::run("INSERT INTO ev_det_src (id, region, amount) VALUES (1, 'N', 10),(2,'S',20)")
        .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'ev_det_src_v', \
            'SELECT region, SUM(amount) AS total FROM ev_det_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create imv");

    Spi::run("ALTER TABLE ev_det_src DETACH PARTITION ev_det_src_s")
        .expect("detach");

    // The IMV's S partition is preserved (drop_orphans=FALSE). The operator
    // can still call reflex_sync_partitions(view, true) to drop it.
    let imv_part_count = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'ev_det_src_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(imv_part_count, 2, "auto-sync should NOT drop orphans");
}

// CTE partition propagation tests (Task 2, Part A)

#[pg_test]
fn pg_part_cte_partition_propagation_basic() {
    // Setup: partitioned source with LIST(region)
    Spi::run(
        "CREATE TABLE cte_part_src1 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE cte_part_src1_n PARTITION OF cte_part_src1 FOR VALUES IN ('N')")
        .expect("north partition");
    Spi::run("CREATE TABLE cte_part_src1_s PARTITION OF cte_part_src1 FOR VALUES IN ('S')")
        .expect("south partition");
    Spi::run(
        "INSERT INTO cte_part_src1 (id, region, amount) VALUES \
         (1, 'N', 100), (2, 'N', 200), (3, 'S', 50)",
    )
    .expect("seed data");

    // Create IMV with CTE that outputs region (partition column).
    // partition_by=[region] should propagate to the CTE sub-IMV.
    let result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'cte_part_main', \
            'WITH regional_totals AS ( \
                SELECT region, SUM(amount) AS total FROM cte_part_src1 GROUP BY region \
             ) \
             SELECT region, total FROM regional_totals', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        !result.starts_with("ERROR"),
        "CTE with partition column in output should succeed: {result}"
    );

    // Verify the CTE sub-IMV is partitioned.
    // Expected name: cte_part_main__cte_regional_totals
    let cte_sub_imv_partstrat = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid \
         WHERE c.relname LIKE 'cte_part_main__cte_regional%'",
    );
    match cte_sub_imv_partstrat {
        Ok(Some(strat)) => {
            assert_eq!(strat, "l", "CTE sub-IMV should be LIST partitioned");
        }
        Ok(None) => {
            panic!("CTE sub-IMV not found or not partitioned");
        }
        Err(e) => {
            panic!("Failed to query partitioned_table: {e}");
        }
    }

    // Verify CTE sub-IMV has 2 children (N, S)
    let cte_children = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname LIKE 'cte_part_main__cte_regional%'",
    )
    .expect("child count query")
    .expect("count");
    assert_eq!(cte_children, 2, "CTE sub-IMV should have 2 partition children");

    // CRITICAL: Verify the PARENT (main) IMV is also partitioned.
    // This ensures that partition_by is actually passed to the main-body re-entry,
    // not dropped (regression guard for Problem 1).
    let main_imv_partstrat = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid \
         WHERE c.relname = 'cte_part_main'",
    );
    match main_imv_partstrat {
        Ok(Some(strat)) => {
            assert_eq!(strat, "l", "Main IMV should be LIST partitioned on region");
        }
        Ok(None) => {
            panic!("REGRESSION: Main IMV 'cte_part_main' not found or not partitioned — partition_by was silently dropped");
        }
        Err(e) => {
            panic!("Failed to query main IMV partitioning: {e}");
        }
    }

    // Verify initial data is correct in the main IMV
    let n_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cte_part_main WHERE region = 'N'",
    )
    .expect("n query")
    .expect("n total");
    assert_eq!(n_total.to_string(), "300");

    let s_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM cte_part_main WHERE region = 'S'",
    )
    .expect("s query")
    .expect("s total");
    assert_eq!(s_total.to_string(), "50");
}

#[pg_test]
fn pg_part_cte_partition_no_propagation_when_col_not_in_output() {
    // Setup: partitioned source with LIST(region)
    Spi::run(
        "CREATE TABLE cte_part_src2 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE cte_part_src2_x PARTITION OF cte_part_src2 FOR VALUES IN ('X')")
        .expect("child");
    Spi::run("INSERT INTO cte_part_src2 (id, region, amount) VALUES (1, 'X', 50)")
        .expect("seed");

    // Create IMV with CTE that does NOT output region (partition column).
    // User requests partition_by=[region], but CTE doesn't output it.
    // This is now correctly detected: partition_by is NOT propagated to the CTE sub-IMV
    // (since 'region' is not in its output), but IS passed to the main body,
    // which then fails validation because 'region' is not a source column.
    let result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'cte_no_prop', \
            'WITH totals AS ( \
                SELECT SUM(amount) AS total FROM cte_part_src2 \
             ) \
             SELECT total FROM totals', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        result.starts_with("ERROR"),
        "Creating an IMV with partition_by on a column not in CTE outputs should fail: {result}"
    );
    assert!(
        result.contains("no source table owns partition column"),
        "Error should indicate partition column not found in sources: {result}"
    );
}

#[pg_test]
fn pg_part_cte_multiple_partitions_same_key() {
    // Two separate parent IMVs with the same-named CTE should NOT collide.
    Spi::run(
        "CREATE TABLE cte_part_src3 (id BIGINT, dept TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (dept)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE cte_part_src3_e PARTITION OF cte_part_src3 FOR VALUES IN ('eng')")
        .expect("eng");
    Spi::run("CREATE TABLE cte_part_src3_s PARTITION OF cte_part_src3 FOR VALUES IN ('sales')")
        .expect("sales");
    Spi::run(
        "INSERT INTO cte_part_src3 (id, dept, amount) VALUES (1, 'eng', 100), (2, 'sales', 200)",
    )
    .expect("seed");

    // First IMV: parent1
    let r1 = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'parent1_cte_test', \
            'WITH dept_totals AS ( \
                SELECT dept, SUM(amount) AS total FROM cte_part_src3 GROUP BY dept \
             ) \
             SELECT dept, total FROM dept_totals', \
            NULL, NULL, NULL, NULL, \
            ARRAY['dept'] \
         )",
    )
    .expect("create1 call")
    .expect("create1 result");
    assert!(!r1.starts_with("ERROR"), "parent1 creation should succeed: {r1}");

    // Second IMV: parent2 with same CTE alias (dept_totals)
    let r2 = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'parent2_cte_test', \
            'WITH dept_totals AS ( \
                SELECT dept, SUM(amount) AS total FROM cte_part_src3 GROUP BY dept \
             ) \
             SELECT dept, total FROM dept_totals', \
            NULL, NULL, NULL, NULL, \
            ARRAY['dept'] \
         )",
    )
    .expect("create2 call")
    .expect("create2 result");
    assert!(!r2.starts_with("ERROR"), "parent2 creation should succeed: {r2}");

    // Both should have independent sub-IMVs with distinct names.
    // parent1__cte_dept_totals should exist and be partitioned
    let p1_exists = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname = 'parent1_cte_test__cte_dept_totals'",
    )
    .expect("p1 query")
    .expect("count");
    assert_eq!(p1_exists, 1, "parent1 CTE sub-IMV should exist");

    // parent2__cte_dept_totals should exist and be partitioned
    let p2_exists = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname = 'parent2_cte_test__cte_dept_totals'",
    )
    .expect("p2 query")
    .expect("count");
    assert_eq!(p2_exists, 1, "parent2 CTE sub-IMV should exist");

    // Both should work independently
    let p1_eng = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM parent1_cte_test WHERE dept = 'eng'",
    )
    .expect("p1 query")
    .expect("total");
    assert_eq!(p1_eng.to_string(), "100");

    let p2_eng = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM parent2_cte_test WHERE dept = 'eng'",
    )
    .expect("p2 query")
    .expect("total");
    assert_eq!(p2_eng.to_string(), "100");
}

#[pg_test]
fn pg_part_cte_nested_ctes_naming_collision_safety() {
    // Test that nested CTEs (CTEs that reference other CTEs) don't cause
    // naming collisions due to the parent-prefixing strategy.
    // A CTE sub-IMV is named: parent_imv_name__cte_cte_alias
    // When a CTE sources from another CTE, that second CTE's sub-IMV is ALSO
    // prefixed with the same parent, ensuring uniqueness.

    Spi::run(
        "CREATE TABLE nested_cte_src (id BIGINT, category TEXT NOT NULL, value NUMERIC) \
         PARTITION BY LIST (category)",
    )
    .expect("create source");
    Spi::run(
        "CREATE TABLE nested_cte_src_a PARTITION OF nested_cte_src FOR VALUES IN ('a')",
    )
    .expect("a");
    Spi::run(
        "CREATE TABLE nested_cte_src_b PARTITION OF nested_cte_src FOR VALUES IN ('b')",
    )
    .expect("b");
    Spi::run(
        "INSERT INTO nested_cte_src (id, category, value) VALUES \
         (1, 'a', 100), (2, 'a', 200), (3, 'b', 300), (4, 'b', 400)",
    )
    .expect("seed");

    // Create an IMV with nested CTEs:
    // - level_1_cte: sources from the table
    // - level_2_cte: sources from level_1_cte
    // - main: sources from level_2_cte
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'nested_ivm', \
            'WITH level_1_cte AS ( \
                SELECT category, SUM(value) AS total FROM nested_cte_src GROUP BY category \
             ), \
             level_2_cte AS ( \
                SELECT category, total FROM level_1_cte WHERE total > 200 \
             ) \
             SELECT category, total FROM level_2_cte', \
            NULL, NULL, NULL, NULL, \
            ARRAY['category'] \
         )",
    )
    .expect("create call")
    .expect("result");
    assert!(!r.starts_with("ERROR"), "nested CTE IMV creation should succeed: {r}");

    // Verify that both CTE sub-IMVs were created with correct parent-prefixed names
    let level1_exists = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname = 'nested_ivm__cte_level_1_cte'",
    )
    .expect("level1 query")
    .expect("count");
    assert_eq!(level1_exists, 1, "level_1_cte sub-IMV should exist");

    let level2_exists = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname = 'nested_ivm__cte_level_2_cte'",
    )
    .expect("level2 query")
    .expect("count");
    assert_eq!(level2_exists, 1, "level_2_cte sub-IMV should exist");

    // Verify partitioning was propagated correctly
    let level1_children = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits \
         WHERE inhrelid IN (SELECT oid FROM pg_class WHERE relname LIKE 'nested_ivm__cte_level_1_cte%')",
    )
    .expect("level1 children query")
    .expect("count");
    assert!(
        level1_children >= 2,
        "level_1_cte sub-IMV should be partitioned: {level1_children} children"
    );

    // Verify data is correct
    let total_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM nested_ivm WHERE category = 'a'",
    )
    .expect("query")
    .expect("total");
    assert_eq!(total_a.to_string(), "300", "category a total should be 300 (100+200)");

    let total_b = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM nested_ivm WHERE category = 'b'",
    )
    .expect("query")
    .expect("total");
    assert_eq!(total_b.to_string(), "700", "category b total should be 700 (300+400)");
}

#[pg_test]
fn partitioned_imv_over_quoted_cte_source_creates_cleanly() {
    // Reproduces Bug 5: ambiguous partition anchor when a CTE sub-IMV source is
    // stored double-quoted. The create must succeed and pick a single anchor.
    Spi::run("CREATE TABLE pq_base (id int, g int, m numeric) PARTITION BY RANGE (g);").unwrap();
    Spi::run("CREATE TABLE pq_base_1 PARTITION OF pq_base FOR VALUES FROM (1) TO (100);").unwrap();
    Spi::run("CREATE TABLE pq_base_2 PARTITION OF pq_base FOR VALUES FROM (100) TO (200);").unwrap();
    Spi::run("INSERT INTO pq_base (id, g, m) VALUES (1, 50, 10.5), (2, 150, 20.5);").unwrap();

    let _body = "WITH agg AS (SELECT g, SUM(m) AS s FROM pq_base GROUP BY g) \
                SELECT g, s FROM agg";
    let msg = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'pq_imv', \
            'WITH agg AS (SELECT g, SUM(m) AS s FROM pq_base GROUP BY g) SELECT g, s FROM agg', \
            NULL, NULL, NULL, NULL, \
            ARRAY['g'] \
         )",
    )
    .expect("create partitioned IMV over CTE")
    .expect("create result");
    assert!(msg.starts_with("CREATE REFLEX") || msg.contains(crate::REFLEX_UNSUPPORTED_TAG),
            "unexpected: {msg}");
}

/// Anchor disambiguation: when a CTE joins a base partitioned table to a
/// partition-inheriting sub-IMV, both own the partition column AND both are
/// partitioned. The anchor — whose partition children we physically mirror —
/// must be the base table, not the reflex-generated `__cte_` intermediate.
/// Before the fix this failed with "multiple sources own partition column …
/// ambiguous", blocking the whole IMV.
#[pg_test]
fn pg_part_anchor_prefers_base_over_cte_intermediate() {
    Spi::run(
        "CREATE TABLE anc_fact (dp_id BIGINT NOT NULL, item INT NOT NULL, amount NUMERIC, \
         PRIMARY KEY (dp_id, item)) PARTITION BY LIST (dp_id)",
    )
    .expect("create fact");
    Spi::run("CREATE TABLE anc_fact_1 PARTITION OF anc_fact FOR VALUES IN (1)").expect("p1");
    Spi::run("CREATE TABLE anc_fact_2 PARTITION OF anc_fact FOR VALUES IN (2)").expect("p2");
    Spi::run("INSERT INTO anc_fact VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)").expect("seed");

    let msg = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'anc_av', \
            'WITH bounds AS ( \
                 SELECT dp_id, MAX(amount) AS mx FROM anc_fact GROUP BY dp_id \
             ), joined AS ( \
                 SELECT f.dp_id, f.item, f.amount FROM anc_fact f \
                 JOIN bounds b ON f.dp_id = b.dp_id \
             ) \
             SELECT dp_id, SUM(amount) AS total FROM joined GROUP BY dp_id', \
            NULL, NULL, NULL, NULL, \
            ARRAY['dp_id'] \
         )",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        !msg.starts_with("ERROR"),
        "anchor disambiguation should let the partitioned CTE IMV create, got: {msg}"
    );

    let total_1 = Spi::get_one::<pgrx::AnyNumeric>("SELECT total FROM anc_av WHERE dp_id = 1")
        .expect("q1")
        .expect("v1");
    assert_eq!(total_1.to_string(), "30", "dp_id=1 total");
    let total_2 = Spi::get_one::<pgrx::AnyNumeric>("SELECT total FROM anc_av WHERE dp_id = 2")
        .expect("q2")
        .expect("v2");
    assert_eq!(total_2.to_string(), "30", "dp_id=2 total");
}

/// Co-partitioned FULL OUTER JOIN: both sources are partitioned on the SAME
/// column and joined on it, so the result partition key is
/// COALESCE(l.dp_id, r.dp_id) and NEITHER side is uniquely "the" anchor. The
/// single-anchor resolver rejected this with "multiple sources own partition
/// column … ambiguous", blocking the whole IMV (this is the shape of
/// forecast_analysis_view: forecast_sales FULL JOIN history_sales on dem_plan_id).
/// The IMV must create AND preserve the FULL JOIN's outer rows from BOTH sides.
#[pg_test]
fn pg_part_anchor_copartitioned_full_join() {
    Spi::run(
        "CREATE TABLE fj_l (dp_id BIGINT NOT NULL, item INT NOT NULL, lval NUMERIC, \
         PRIMARY KEY (dp_id, item)) PARTITION BY LIST (dp_id)",
    )
    .expect("create left");
    Spi::run("CREATE TABLE fj_l_1 PARTITION OF fj_l FOR VALUES IN (1)").expect("l1");
    Spi::run("CREATE TABLE fj_l_2 PARTITION OF fj_l FOR VALUES IN (2)").expect("l2");
    Spi::run(
        "CREATE TABLE fj_r (dp_id BIGINT NOT NULL, item INT NOT NULL, rval NUMERIC, \
         PRIMARY KEY (dp_id, item)) PARTITION BY LIST (dp_id)",
    )
    .expect("create right");
    Spi::run("CREATE TABLE fj_r_1 PARTITION OF fj_r FOR VALUES IN (1)").expect("r1");
    Spi::run("CREATE TABLE fj_r_2 PARTITION OF fj_r FOR VALUES IN (2)").expect("r2");

    // Identical dp_id universe ({1,2}); the outer rows differ by `item` so the
    // FULL JOIN yields a matched row, a left-only row, and a right-only row all
    // inside partition dp_id=1.
    Spi::run("INSERT INTO fj_l VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)").expect("seed l");
    Spi::run("INSERT INTO fj_r VALUES (1, 1, 100), (1, 3, 300), (2, 1, 300)").expect("seed r");

    let msg = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'fj_av', \
            'SELECT COALESCE(l.dp_id, r.dp_id) AS dp_id, \
                    COALESCE(l.item, r.item) AS item, \
                    l.lval, r.rval \
             FROM fj_l l FULL JOIN fj_r r \
               ON l.dp_id = r.dp_id AND l.item = r.item', \
            'dp_id,item', 'UNLOGGED', 'DEFERRED', NULL, \
            ARRAY['dp_id'] \
         )",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        !msg.starts_with("ERROR"),
        "co-partitioned FULL JOIN should create, got: {msg}"
    );

    // Partition dp_id=1 must hold matched (item 1) + left-only (item 2) +
    // right-only (item 3): outer rows on BOTH sides survive.
    let n = Spi::get_one::<i64>("SELECT count(*) FROM fj_av WHERE dp_id = 1")
        .expect("count q")
        .expect("count v");
    assert_eq!(n, 3, "dp_id=1 must keep matched + left-only + right-only rows");
    let right_only = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT rval FROM fj_av WHERE dp_id = 1 AND item = 3",
    )
    .expect("right-only q")
    .expect("right-only present");
    assert_eq!(right_only.to_string(), "300", "right-only row preserved");
    let left_only = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT lval FROM fj_av WHERE dp_id = 1 AND item = 2",
    )
    .expect("left-only q")
    .expect("left-only present");
    assert_eq!(left_only.to_string(), "20", "left-only row preserved");
}

/// Incremental soundness for the co-partitioned FULL JOIN: a right-only INSERT
/// on the NON-anchor side, in a partition the anchor has NO rows for, must
/// still reach the IMV. The buggy single-anchor JOIN-path
/// (`JOIN anchor a ON a.dp_id = t.dp_id`) would find no anchor row for that
/// dp_id and silently drop the delta. The fix gives a co-partitioned source no
/// JOIN-path, so it falls through to Path B and the row propagates.
#[pg_test]
fn pg_part_copartitioned_full_join_right_only_delta_propagates() {
    Spi::run(
        "CREATE TABLE fjd_l (dp_id BIGINT NOT NULL, item INT NOT NULL, lval NUMERIC, \
         PRIMARY KEY (dp_id, item)) PARTITION BY LIST (dp_id)",
    )
    .expect("create left");
    Spi::run("CREATE TABLE fjd_l_1 PARTITION OF fjd_l FOR VALUES IN (1)").expect("l1");
    Spi::run("CREATE TABLE fjd_l_2 PARTITION OF fjd_l FOR VALUES IN (2)").expect("l2");
    Spi::run(
        "CREATE TABLE fjd_r (dp_id BIGINT NOT NULL, item INT NOT NULL, rval NUMERIC, \
         PRIMARY KEY (dp_id, item)) PARTITION BY LIST (dp_id)",
    )
    .expect("create right");
    Spi::run("CREATE TABLE fjd_r_1 PARTITION OF fjd_r FOR VALUES IN (1)").expect("r1");
    Spi::run("CREATE TABLE fjd_r_2 PARTITION OF fjd_r FOR VALUES IN (2)").expect("r2");

    // Anchor (lexicographically first = fjd_l) has rows ONLY in dp_id=1. dp_id=2
    // is empty on the anchor side at create time.
    Spi::run("INSERT INTO fjd_l VALUES (1, 1, 10)").expect("seed l");
    Spi::run("INSERT INTO fjd_r VALUES (1, 1, 100)").expect("seed r");

    let msg = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'fjd_av', \
            'SELECT COALESCE(l.dp_id, r.dp_id) AS dp_id, \
                    COALESCE(l.item, r.item) AS item, \
                    l.lval, r.rval \
             FROM fjd_l l FULL JOIN fjd_r r \
               ON l.dp_id = r.dp_id AND l.item = r.item', \
            'dp_id,item', 'LOGGED', 'IMMEDIATE', NULL, \
            ARRAY['dp_id'] \
         )",
    )
    .expect("create call")
    .expect("create result");
    assert!(!msg.starts_with("ERROR"), "create failed: {msg}");

    // Right-only delta into a dp_id the anchor never had rows for. The anchor
    // JOIN-path would miss partition 2 entirely; Path B must catch it.
    Spi::run("INSERT INTO fjd_r VALUES (2, 7, 700)").expect("right-only delta");

    let rval = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT rval FROM fjd_av WHERE dp_id = 2 AND item = 7",
    )
    .expect("delta q")
    .expect("right-only delta must reach the IMV");
    assert_eq!(rval.to_string(), "700", "right-only delta propagated to IMV");
}

/// The forecast_analysis_view shape: the FULL JOIN's two sides are reflex
/// `__cte_` INTERMEDIATES (not base tables), each a passthrough over a
/// partitioned source so each inherits the partition column. The top-level
/// anchor resolution then has TWO partitioned owners and ZERO base owners —
/// the `owners_on_col` branch, distinct from the base-table case above. This
/// is the exact branch forecast_analysis_view exercises
/// (__cte_forecast_sales FULL JOIN __cte_history_sales on dem_plan_id).
#[pg_test]
fn pg_part_copartitioned_full_join_of_cte_intermediates() {
    Spi::run(
        "CREATE TABLE cti_l (dp_id BIGINT NOT NULL, item INT NOT NULL, lval NUMERIC, \
         PRIMARY KEY (dp_id, item)) PARTITION BY LIST (dp_id)",
    )
    .expect("create left base");
    Spi::run("CREATE TABLE cti_l_1 PARTITION OF cti_l FOR VALUES IN (1)").expect("l1");
    Spi::run("CREATE TABLE cti_l_2 PARTITION OF cti_l FOR VALUES IN (2)").expect("l2");
    Spi::run(
        "CREATE TABLE cti_r (dp_id BIGINT NOT NULL, item INT NOT NULL, rval NUMERIC, \
         PRIMARY KEY (dp_id, item)) PARTITION BY LIST (dp_id)",
    )
    .expect("create right base");
    Spi::run("CREATE TABLE cti_r_1 PARTITION OF cti_r FOR VALUES IN (1)").expect("r1");
    Spi::run("CREATE TABLE cti_r_2 PARTITION OF cti_r FOR VALUES IN (2)").expect("r2");

    Spi::run("INSERT INTO cti_l VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)").expect("seed l");
    Spi::run("INSERT INTO cti_r VALUES (1, 1, 100), (1, 3, 300), (2, 1, 300)").expect("seed r");

    // Each CTE is a passthrough over a partitioned base, so decomposition makes
    // `cti_av__cte_lc` / `cti_av__cte_rc` partitioned sub-IMVs; the top-level
    // FULL JOIN then sees two partitioned `__cte_` owners and no base owner.
    let msg = Spi::get_one::<&str>(
        "SELECT create_reflex_ivm( \
            'cti_av', \
            'WITH lc AS (SELECT dp_id, item, lval FROM cti_l), \
                  rc AS (SELECT dp_id, item, rval FROM cti_r) \
             SELECT COALESCE(l.dp_id, r.dp_id) AS dp_id, \
                    COALESCE(l.item, r.item) AS item, \
                    l.lval, r.rval \
             FROM lc l FULL JOIN rc r \
               ON l.dp_id = r.dp_id AND l.item = r.item', \
            'dp_id,item', 'UNLOGGED', 'DEFERRED', NULL, \
            ARRAY['dp_id'] \
         )",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        !msg.starts_with("ERROR"),
        "co-partitioned FULL JOIN of CTE intermediates should create, got: {msg}"
    );

    let n = Spi::get_one::<i64>("SELECT count(*) FROM cti_av WHERE dp_id = 1")
        .expect("count q")
        .expect("count v");
    assert_eq!(n, 3, "dp_id=1 must keep matched + left-only + right-only rows");
}

/// gap 2 proof: an aggregate, LIST-partitioned IMV in DEFERRED mode whose flush
/// makes one partition "hot" must drive reflex_reconcile_partition (DETACH/ATTACH)
/// from inside the per-IMV savepoint DO-block at COMMIT — and stay correct.
///
/// Decision point for the plan's Task 9 contingency: if DDL-at-commit is unsafe
/// inside the deferred flush, this test exposes it (lock/subtransaction error or
/// a wrong result). It passing means the swap escalation is safe in DEFERRED for
/// both the aggregate and (by parity) the passthrough dispatch.
#[pg_test]
fn pg_part_deferred_hot_swap_aggregate_is_correct() {
    Spi::run("CREATE TABLE dhs_src (region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("src");
    Spi::run("CREATE TABLE dhs_a PARTITION OF dhs_src FOR VALUES IN ('A')").expect("pa");
    Spi::run("CREATE TABLE dhs_b PARTITION OF dhs_src FOR VALUES IN ('B')").expect("pb");
    Spi::run("INSERT INTO dhs_src SELECT 'A', g FROM generate_series(1,50) g").expect("seedA");
    Spi::run("INSERT INTO dhs_src SELECT 'B', g FROM generate_series(1,50) g").expect("seedB");
    let sql = "SELECT region, sum(amount) AS s, count(*) AS c FROM dhs_src GROUP BY region";
    let res = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('dhsv', '{}', 'region', NULL, 'DEFERRED', NULL, ARRAY['region'])",
        sql.replace('\'', "''")
    )).expect("create call").expect("create result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");
    // Force a very low wipe threshold so any sizeable touch is "hot".
    Spi::run("UPDATE public.__reflex_ivm_reference SET wipe_threshold = 0.01 WHERE name = 'dhsv'").expect("thr");
    Spi::run("ANALYZE dhs_src").expect("analyze");
    assert_imv_correct("dhsv", sql);

    // Bulk update partition A → hot → swap path at commit.
    Spi::run("UPDATE dhs_src SET amount = amount + 1000 WHERE region = 'A'").expect("bulk A");
    Spi::run("SELECT reflex_flush_deferred('dhs_src')").expect("flush");
    assert_imv_correct("dhsv", sql);
}

/// audit #2: partitioned passthrough UPDATE — correctness across one leaf and a
/// near-full (hot) leaf, in DEFERRED mode. Exercises the hybrid dispatch:
/// cold leaves get keyed delete/insert, the hot leaf is atomic-swapped, and the
/// cold body must exclude the swapped leaf (no double-processing).
#[pg_test]
fn pg_part_passthrough_update_dispatch_deferred_correct() {
    Spi::run("CREATE TABLE ppd_src (dem_plan_id INT NOT NULL, product_id INT NOT NULL, qty INT) PARTITION BY LIST (dem_plan_id)").expect("src");
    Spi::run("CREATE TABLE ppd_1 PARTITION OF ppd_src FOR VALUES IN (1)").expect("p1");
    Spi::run("CREATE TABLE ppd_2 PARTITION OF ppd_src FOR VALUES IN (2)").expect("p2");
    Spi::run("CREATE TABLE ppd_3 PARTITION OF ppd_src FOR VALUES IN (3)").expect("p3");
    Spi::run("INSERT INTO ppd_src SELECT (g%3)+1, g, g FROM generate_series(1,90) g").expect("seed");
    let sql = "SELECT dem_plan_id, product_id, qty FROM ppd_src";
    let res = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('ppdv', '{}', 'dem_plan_id,product_id', NULL, 'DEFERRED', NULL, ARRAY['dem_plan_id'])",
        sql.replace('\'', "''")
    )).expect("create call").expect("create result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");
    Spi::run("ANALYZE ppd_src").expect("analyze");
    assert_imv_correct("ppdv", sql);

    // 1 leaf, low selectivity → keyed cold path.
    Spi::run("UPDATE ppd_src SET qty = qty + 1 WHERE dem_plan_id = 1 AND product_id = 1").expect("u1");
    Spi::run("SELECT reflex_flush_deferred('ppd_src')").expect("f1");
    assert_imv_correct("ppdv", sql);

    // near-full leaf (hot) — low threshold forces the atomic swap path.
    Spi::run("UPDATE public.__reflex_ivm_reference SET wipe_threshold = 0.01 WHERE name = 'ppdv'").expect("thr");
    Spi::run("UPDATE ppd_src SET qty = qty + 100 WHERE dem_plan_id = 2").expect("u2");
    Spi::run("SELECT reflex_flush_deferred('ppd_src')").expect("f2");
    assert_imv_correct("ppdv", sql);

    // mixed: one hot leaf + one cold single-row touch in the same flush.
    Spi::run("UPDATE ppd_src SET qty = qty + 7 WHERE dem_plan_id = 3").expect("u3-hot");
    Spi::run("UPDATE ppd_src SET qty = qty + 1 WHERE dem_plan_id = 1 AND product_id = 4").expect("u3-cold");
    Spi::run("SELECT reflex_flush_deferred('ppd_src')").expect("f3");
    assert_imv_correct("ppdv", sql);
}

/// Component 4: RANGE-partitioned AGGREGATE IMV — per-child dispatch correctness.
/// One statement touches many rows in Q1 (hot child → swap) plus a couple in Q2
/// (cold). The cold-exclusion filter MUST drop rows of the hot CHILD (via child
/// OID), not specific values — a value-array filter would re-process the hot
/// child's non-representative rows that the swap already rebuilt.
#[pg_test]
fn pg_part_range_aggregate_dispatch_correct() {
    Spi::run("CREATE TABLE rga_src (d DATE NOT NULL, region TEXT, amount NUMERIC) PARTITION BY RANGE (d)").expect("src");
    Spi::run("CREATE TABLE rga_q1 PARTITION OF rga_src FOR VALUES FROM ('2026-01-01') TO ('2026-04-01')").expect("q1");
    Spi::run("CREATE TABLE rga_q2 PARTITION OF rga_src FOR VALUES FROM ('2026-04-01') TO ('2026-07-01')").expect("q2");
    Spi::run("INSERT INTO rga_src SELECT '2026-02-15'::date + (g % 30), 'r'||(g%3), g FROM generate_series(1,90) g").expect("seedq1");
    Spi::run("INSERT INTO rga_src SELECT '2026-05-15'::date + (g % 30), 'r'||(g%3), g FROM generate_series(1,90) g").expect("seedq2");
    let sql = "SELECT d, sum(amount) AS s, count(*) AS c FROM rga_src GROUP BY d";
    let res = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('rgav', '{}', 'd', NULL, NULL, NULL, ARRAY['d'])",
        sql.replace('\'', "''")
    )).expect("create call").expect("create result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");
    Spi::run("ANALYZE rga_src").expect("analyze");
    assert_imv_correct("rgav", sql);

    // Touch many rows in Q1 (hot) + a couple in Q2 (cold) in one statement.
    Spi::run("UPDATE public.__reflex_ivm_reference SET wipe_threshold = 0.01 WHERE name = 'rgav'").expect("thr");
    Spi::run("UPDATE rga_src SET amount = amount + 1 WHERE d < '2026-04-01' OR d = '2026-05-15'").expect("bulk");
    assert_imv_correct("rgav", sql);
}

/// Component 4: RANGE-partitioned PASSTHROUGH IMV — keyed prune + per-child
/// classification correctness in DEFERRED mode. A hot Q1 child is swapped while
/// a single cold Q2 row is keyed-maintained in the same flush; the RANGE
/// cold-exclusion (by child name) must drop exactly the swapped child's rows.
#[pg_test]
fn pg_part_range_passthrough_dispatch_correct() {
    Spi::run("CREATE TABLE rgp_src (d DATE NOT NULL, id INT NOT NULL, qty INT) PARTITION BY RANGE (d)").expect("src");
    Spi::run("CREATE TABLE rgp_q1 PARTITION OF rgp_src FOR VALUES FROM ('2026-01-01') TO ('2026-04-01')").expect("q1");
    Spi::run("CREATE TABLE rgp_q2 PARTITION OF rgp_src FOR VALUES FROM ('2026-04-01') TO ('2026-07-01')").expect("q2");
    Spi::run("INSERT INTO rgp_src SELECT '2026-02-10'::date + (g%30), g, g FROM generate_series(1,60) g").expect("s1");
    Spi::run("INSERT INTO rgp_src SELECT '2026-05-10'::date + (g%30), 1000+g, g FROM generate_series(1,60) g").expect("s2");
    let sql = "SELECT d, id, qty FROM rgp_src";
    let res = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('rgpv', '{}', 'd,id', NULL, 'DEFERRED', NULL, ARRAY['d'])",
        sql.replace('\'', "''")
    )).expect("create call").expect("create result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");
    Spi::run("ANALYZE rgp_src").expect("analyze");
    assert_imv_correct("rgpv", sql);

    Spi::run("UPDATE public.__reflex_ivm_reference SET wipe_threshold = 0.01 WHERE name = 'rgpv'").expect("thr");
    Spi::run("UPDATE rgp_src SET qty = qty + 1 WHERE d < '2026-04-01'").expect("hot q1");
    Spi::run("UPDATE rgp_src SET qty = qty + 5 WHERE id = 1001").expect("cold q2 single");
    Spi::run("SELECT reflex_flush_deferred('rgp_src')").expect("flush");
    assert_imv_correct("rgpv", sql);
}

/// Fix #2: a reconcile failure on one pending root must NOT prevent a healthy
/// root from being reconciled and drained. Before the fix, the whole flush
/// aborts on the first failing root and drains nothing.
#[pg_test]
fn flush_isolates_failing_root_from_healthy_root() {
    // Healthy partitioned source + IMV.
    Spi::run("CREATE TABLE iso_ok (region text, amount int) PARTITION BY LIST (region)").unwrap();
    Spi::run("CREATE TABLE iso_ok_us PARTITION OF iso_ok FOR VALUES IN ('us')").unwrap();
    Spi::run("INSERT INTO iso_ok VALUES ('us', 10)").unwrap();
    Spi::run(
        "SELECT create_reflex_ivm('iso_ok_imv', \
         'SELECT region, sum(amount) AS total FROM iso_ok GROUP BY region', \
         'region', 'UNLOGGED', 'IMMEDIATE', NULL, ARRAY['region'])",
    ).unwrap();

    // Broken partitioned source + IMV: we corrupt the IMV after creation so the
    // per-partition reconcile of this root throws, WITHOUT removing the registry
    // row or its source triggers (the root must still enqueue on ATTACH).
    // Dropping the whole target table is intercepted by the sql_drop event
    // trigger, which fully unregisters the IMV — so instead we drop only the
    // aggregate output column from the target: the registry/triggers survive
    // (target identity unchanged, never a tracked source), but reconcile's
    // INSERT of (region, total) into a now-`total`-less target child errors hard.
    Spi::run("CREATE TABLE iso_bad (region text, amount int) PARTITION BY LIST (region)").unwrap();
    Spi::run("CREATE TABLE iso_bad_us PARTITION OF iso_bad FOR VALUES IN ('us')").unwrap();
    Spi::run("INSERT INTO iso_bad VALUES ('us', 1)").unwrap();
    Spi::run(
        "SELECT create_reflex_ivm('iso_bad_imv', \
         'SELECT region, sum(amount) AS total FROM iso_bad GROUP BY region', \
         'region', 'UNLOGGED', 'IMMEDIATE', NULL, ARRAY['region'])",
    ).unwrap();
    // Corrupt: strip the aggregate output column so reconcile errors hard.
    Spi::run("ALTER TABLE public.iso_bad_imv DROP COLUMN total").unwrap();

    // Attach NEW data partitions to BOTH sources (event trigger enqueues both,
    // creates empty structure, no data fill yet).
    Spi::run("CREATE TABLE iso_ok_eu (region text, amount int)").unwrap();
    Spi::run("INSERT INTO iso_ok_eu VALUES ('eu', 100)").unwrap();
    Spi::run("ALTER TABLE iso_ok ATTACH PARTITION iso_ok_eu FOR VALUES IN ('eu')").unwrap();

    Spi::run("CREATE TABLE iso_bad_eu (region text, amount int)").unwrap();
    Spi::run("INSERT INTO iso_bad_eu VALUES ('eu', 200)").unwrap();
    Spi::run("ALTER TABLE iso_bad ATTACH PARTITION iso_bad_eu FOR VALUES IN ('eu')").unwrap();

    // Drain ALL pending roots. Must not raise even though iso_bad fails.
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").unwrap();

    // Healthy root reconciled + drained.
    let ok_eu = Spi::get_one::<i64>("SELECT total FROM iso_ok_imv WHERE region = 'eu'")
        .unwrap()
        .unwrap_or(-1);
    assert_eq!(ok_eu, 100, "healthy root must be reconciled despite a sibling failing");
    let ok_pending = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root LIKE '%iso_ok%'",
    ).unwrap().unwrap_or(-1);
    assert_eq!(ok_pending, 0, "healthy root must be drained from the pending queue");

    // Broken root left pending for retry (NOT silently dropped).
    let bad_pending = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root LIKE '%iso_bad%'",
    ).unwrap().unwrap_or(-1);
    assert_eq!(bad_pending, 1, "failing root must remain pending, not block others");
}

/// Root-cause pin (contrast / working baseline): DETACH of an IRRELEVANT
/// partition whose child still EXISTS at flush time must be a no-op — the
/// DELETE delta's pred-check probes the surviving child, finds no rows pass the
/// IMV filter, and SKIPs (no reconcile → stable relfilenode).
#[pg_test]
fn pg_part_detach_irrelevant_partition_not_dropped_is_noop() {
    Spi::run(
        "CREATE TABLE ddn_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE ddn_src_n PARTITION OF ddn_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("CREATE TABLE ddn_src_s PARTITION OF ddn_src FOR VALUES IN ('S')").expect("p2");
    Spi::run("INSERT INTO ddn_src (id, region, amount) VALUES (1, 'N', 10), (2, 'S', 50)")
        .expect("seed");
    Spi::run("SELECT create_reflex_ivm('ddn_v', 'SELECT id, amount FROM ddn_src WHERE region = ''N''', 'id')")
        .expect("create imv");

    let before_node =
        Spi::get_one::<i64>("SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'ddn_v'")
            .expect("q")
            .expect("n");

    // DETACH the irrelevant 'S' partition but DO NOT drop it.
    Spi::run("ALTER TABLE ddn_src DETACH PARTITION ddn_src_s").expect("detach");
    Spi::run("SELECT reflex_flush_partition_source('ddn_src')").expect("flush");

    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM ddn_v").expect("q").expect("c"),
        1,
        "content unchanged (S was never in the filter)"
    );
    let after_node =
        Spi::get_one::<i64>("SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'ddn_v'")
            .expect("q")
            .expect("n");
    assert_eq!(
        before_node, after_node,
        "detaching an irrelevant (still-existing) partition must not reconcile the IMV"
    );
}

/// Root-cause pin (the reported regression): DETACH-then-DROP of an IRRELEVANT
/// partition in the same transaction. By flush time the child is gone, so the
/// DELETE-delta path cannot probe it and currently force-reconciles — the
/// expensive full TRUNCATE+rebuild + downstream cascade the optimization was
/// meant to avoid. It must instead be a no-op (stable relfilenode), exactly as
/// the not-dropped case above.
#[pg_test]
fn pg_part_detach_then_drop_irrelevant_partition_is_noop() {
    Spi::run(
        "CREATE TABLE ddd_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE ddd_src_n PARTITION OF ddd_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("CREATE TABLE ddd_src_s PARTITION OF ddd_src FOR VALUES IN ('S')").expect("p2");
    Spi::run("INSERT INTO ddd_src (id, region, amount) VALUES (1, 'N', 10), (2, 'S', 50)")
        .expect("seed");
    Spi::run("SELECT create_reflex_ivm('ddd_v', 'SELECT id, amount FROM ddd_src WHERE region = ''N''', 'id')")
        .expect("create imv");

    let before_node =
        Spi::get_one::<i64>("SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'ddd_v'")
            .expect("q")
            .expect("n");

    // DETACH then DROP the irrelevant 'S' partition before the flush runs.
    Spi::run("ALTER TABLE ddd_src DETACH PARTITION ddd_src_s").expect("detach");
    Spi::run("DROP TABLE ddd_src_s").expect("drop");
    Spi::run("SELECT reflex_flush_partition_source('ddd_src')").expect("flush");

    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM ddd_v").expect("q").expect("c"),
        1,
        "content unchanged (S was never in the filter)"
    );
    let after_node =
        Spi::get_one::<i64>("SELECT relfilenode::BIGINT FROM pg_class WHERE relname = 'ddd_v'")
            .expect("q")
            .expect("n");
    assert_eq!(
        before_node, after_node,
        "detach-then-drop of an irrelevant partition must not reconcile the IMV"
    );
}

/// Correctness guard: DETACH-then-DROP of an IN-FILTER partition must still
/// remove its rows. The bound probe finds the partition relevant, so it cannot
/// build a DELETE delta (child gone) and must reconcile — yielding correct
/// (shrunken) content.
#[pg_test]
fn pg_part_detach_then_drop_in_filter_partition_removes_rows() {
    Spi::run(
        "CREATE TABLE drr_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE drr_src_n PARTITION OF drr_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("CREATE TABLE drr_src_s PARTITION OF drr_src FOR VALUES IN ('S')").expect("p2");
    Spi::run("INSERT INTO drr_src (id, region, amount) VALUES (1, 'N', 10), (2, 'S', 50)")
        .expect("seed");
    Spi::run("SELECT create_reflex_ivm('drr_v', 'SELECT id, amount FROM drr_src WHERE region = ''N''', 'id')")
        .expect("create imv");
    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM drr_v").expect("q").expect("c"),
        1,
        "baseline: only the N row is kept"
    );

    // DETACH then DROP the in-filter 'N' partition before the flush.
    Spi::run("ALTER TABLE drr_src DETACH PARTITION drr_src_n").expect("detach");
    Spi::run("DROP TABLE drr_src_n").expect("drop");
    Spi::run("SELECT reflex_flush_partition_source('drr_src')").expect("flush");

    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM drr_v").expect("q").expect("c"),
        0,
        "the dropped in-filter partition's rows must be gone (reconciled)"
    );
}

/// Soundness guard: when the IMV filter references a NON-partition-key column,
/// the bound probe (which exposes only the key column) MUST NOT be trusted to
/// prove a no-op — it must reconcile. Here the dropped partition held a row that
/// passes `amount > 100`, so a wrong skip would leave stale data.
#[pg_test]
fn pg_part_detach_then_drop_nonkey_predicate_reconciles() {
    Spi::run(
        "CREATE TABLE dnk_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create source");
    Spi::run("CREATE TABLE dnk_src_n PARTITION OF dnk_src FOR VALUES IN ('N')").expect("p1");
    Spi::run("CREATE TABLE dnk_src_s PARTITION OF dnk_src FOR VALUES IN ('S')").expect("p2");
    Spi::run("INSERT INTO dnk_src (id, region, amount) VALUES (1, 'N', 500), (2, 'S', 50)")
        .expect("seed");
    Spi::run("SELECT create_reflex_ivm('dnk_v', 'SELECT id, amount FROM dnk_src WHERE amount > 100', 'id')")
        .expect("create imv");
    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM dnk_v").expect("q").expect("c"),
        1,
        "baseline: id=1 (amount 500) is kept"
    );

    // The N partition held the only passing row; detach+drop it.
    Spi::run("ALTER TABLE dnk_src DETACH PARTITION dnk_src_n").expect("detach");
    Spi::run("DROP TABLE dnk_src_n").expect("drop");
    Spi::run("SELECT reflex_flush_partition_source('dnk_src')").expect("flush");

    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM dnk_v").expect("q").expect("c"),
        0,
        "non-key-column filter must reconcile (not wrongly skip), removing the dropped row"
    );
}

#[pg_test]
fn f7_snapshot_heal_repopulates_after_truncate() {
    // Build a partitioned source + IMV so a snapshot row set exists.
    Spi::run(
        "CREATE TABLE f7_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)",
    )
    .expect("create partitioned source");
    Spi::run("CREATE TABLE f7_src_north PARTITION OF f7_src FOR VALUES IN ('NORTH')")
        .expect("create north partition");
    Spi::run("CREATE TABLE f7_src_south PARTITION OF f7_src FOR VALUES IN ('SOUTH')")
        .expect("create south partition");
    Spi::run("INSERT INTO f7_src (id, region, amount) VALUES (1, 'NORTH', 100), (2, 'SOUTH', 50)")
        .expect("seed data");

    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'f7_v', \
            'SELECT region, SUM(amount) AS total FROM f7_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create IMV call")
    .expect("create IMV result");
    assert!(
        !create_result.starts_with("ERROR"),
        "create should succeed: {create_result}"
    );

    // Verify snapshot was populated.
    let snap_count_before = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_source_partition_snapshot WHERE source_root = 'public.f7_src'",
    )
    .expect("count before")
    .expect("count");
    assert!(snap_count_before > 0, "snapshot should have rows after creation");

    // Truncate the snapshot to simulate divergence.
    Spi::run("DELETE FROM public.__reflex_source_partition_snapshot WHERE source_root = 'public.f7_src'")
        .expect("truncate snapshot");

    let snap_count_after_delete = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_source_partition_snapshot WHERE source_root = 'public.f7_src'",
    )
    .expect("count after delete")
    .expect("count");
    assert_eq!(snap_count_after_delete, 0, "snapshot should be empty after delete");

    // Call the heal function.
    let msg = Spi::get_one::<String>(
        "SELECT reflex_refresh_partition_snapshot_if_diverged('public.f7_src')",
    )
    .expect("heal call")
    .expect("heal result");
    assert!(msg.starts_with("HEALED"), "should report healing: {msg}");

    // Verify snapshot was repopulated.
    let snap_count_healed = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_source_partition_snapshot WHERE source_root = 'public.f7_src'",
    )
    .expect("count after heal")
    .expect("count");
    assert!(snap_count_healed > 0, "snapshot should be repopulated from live tree");
    assert_eq!(snap_count_healed, snap_count_before, "snapshot count should match original");
}

#[pg_test]
fn f9_reconcile_succeeds_with_debug_off() {
    Spi::run("CREATE TABLE f9_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)")
        .expect("create partitioned source");
    Spi::run("CREATE TABLE f9_src_n PARTITION OF f9_src FOR VALUES IN ('N')")
        .expect("partition N");
    Spi::run("INSERT INTO f9_src (id, region, amount) VALUES (1, 'N', 10)")
        .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'f9_v', \
            'SELECT region, SUM(amount) AS total FROM f9_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create partitioned IMV");

    let status = Spi::get_one::<String>("SELECT reflex_reconcile('f9_v')")
        .expect("reconcile call")
        .expect("reconcile result");
    assert_eq!(status, "RECONCILED", "reconcile must succeed with debug GUC off");
}

#[pg_test]
fn f1_rearm_after_failed_flush_reattempts() {
    Spi::run("INSERT INTO public.__reflex_partition_pending (source_root) VALUES ('t.wedged')").unwrap();
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending (source_root) VALUES ('t.wedged') \
         ON CONFLICT (source_root) DO UPDATE \
           SET enqueued_at = statement_timestamp(), \
               attempts = public.__reflex_partition_pending.attempts + 1",
    ).unwrap();
    let attempts = Spi::get_one::<i32>(
        "SELECT attempts FROM public.__reflex_partition_pending WHERE source_root='t.wedged'",
    ).unwrap().unwrap();
    assert_eq!(attempts, 1, "second enqueue must bump attempts (re-arm)");
}

#[pg_test]
fn f1_failed_drain_records_last_error() {
    Spi::run("INSERT INTO public.__reflex_partition_pending (source_root) VALUES ('t.boom')").unwrap();
    Spi::run(
        "UPDATE public.__reflex_partition_pending \
            SET last_error = left('simulated shape drift', 2000) \
          WHERE source_root='t.boom'",
    ).unwrap();
    let err = Spi::get_one::<String>(
        "SELECT last_error FROM public.__reflex_partition_pending WHERE source_root='t.boom'",
    ).unwrap().unwrap();
    assert!(err.contains("shape drift"));
}

/// Safety test: ensure reconcile never drops a live-backed child partition.
/// This test MUST always pass before and after the fix.
#[pg_test]
fn f3_swap_never_drops_live_backed_child() {
    // 1. Create partitioned source with two partitions
    Spi::run(
        "CREATE TABLE f3_safe_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)"
    ).expect("create partitioned source");
    Spi::run(
        "CREATE TABLE f3_safe_src_n PARTITION OF f3_safe_src FOR VALUES IN ('N')"
    ).expect("partition N");
    Spi::run(
        "CREATE TABLE f3_safe_src_s PARTITION OF f3_safe_src FOR VALUES IN ('S')"
    ).expect("partition S");
    Spi::run(
        "INSERT INTO f3_safe_src (id, region, amount) VALUES (1, 'N', 100), (2, 'S', 200)"
    ).expect("seed");

    // 2. Create partitioned aggregate IMV
    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'f3_safe_v', \
            'SELECT region, SUM(amount) AS total FROM f3_safe_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )"
    ).expect("create IMV call").expect("create IMV result");
    assert!(!create_result.starts_with("ERROR"), "create failed: {}", create_result);

    // 3. Verify both target children exist and have data
    let n_count_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f3_safe_v WHERE region = 'N'"
    ).expect("q").expect("c");
    let s_count_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f3_safe_v WHERE region = 'S'"
    ).expect("q").expect("c");
    assert_eq!(n_count_before, 1, "N partition should have 1 row");
    assert_eq!(s_count_before, 1, "S partition should have 1 row");

    // 4. Count target children before reconcile
    let tgt_child_count_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'f3_safe_v'"
    ).expect("q").expect("c");
    assert_eq!(tgt_child_count_before, 2, "should have 2 target children before reconcile");

    // 5. Run reconcile (should not drop any live-backed children)
    let recon_result = Spi::get_one::<String>(
        "SELECT reflex_reconcile('f3_safe_v')"
    ).expect("reconcile call").expect("result");
    assert_eq!(recon_result, "RECONCILED", "reconcile must succeed");

    // 6. Verify no children were dropped
    let tgt_child_count_after = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'f3_safe_v'"
    ).expect("q").expect("c");
    assert_eq!(tgt_child_count_after, 2, "reconcile must preserve live-backed children");

    // 7. Verify data integrity (row counts preserved or repopulated correctly)
    let n_count_after = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f3_safe_v WHERE region = 'N'"
    ).expect("q").expect("c");
    let s_count_after = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f3_safe_v WHERE region = 'S'"
    ).expect("q").expect("c");
    assert_eq!(n_count_after, 1, "N partition should still have 1 row after reconcile");
    assert_eq!(s_count_after, 1, "S partition should still have 1 row after reconcile");
}

/// Red test: reproduce the orphan-overlap issue and verify it's healed.
/// Before fix: reconcile aborts with "would overlap partition".
/// After fix: reconcile succeeds and the partition is repopulated.
#[pg_test]
fn f3_swap_heals_overlapping_orphan() {
    // 1. Create partitioned source with one partition
    Spi::run(
        "CREATE TABLE f3_orphan_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)"
    ).expect("create partitioned source");
    Spi::run(
        "CREATE TABLE f3_orphan_src_x PARTITION OF f3_orphan_src FOR VALUES IN ('X')"
    ).expect("partition X");
    Spi::run(
        "INSERT INTO f3_orphan_src (id, region, amount) VALUES (1, 'X', 111)"
    ).expect("seed");

    // 2. Create partitioned aggregate IMV
    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'f3_orphan_v', \
            'SELECT region, SUM(amount) AS total FROM f3_orphan_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )"
    ).expect("create IMV call").expect("create IMV result");
    assert!(!create_result.starts_with("ERROR"), "create failed: {}", create_result);

    // 3. Verify X partition populated
    let x_count_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f3_orphan_v WHERE region = 'X'"
    ).expect("q").expect("c");
    assert_eq!(x_count_before, 1, "X partition should have 1 row");

    // 4. DETACH source partition X with drop_orphans=false (default)
    //    This leaves the IMV child as an orphan with live bounds
    Spi::run(
        "ALTER TABLE f3_orphan_src DETACH PARTITION f3_orphan_src_x"
    ).expect("detach X");
    Spi::run("DROP TABLE f3_orphan_src_x").expect("drop X");

    // 5. Re-create the source partition X with fresh data
    Spi::run(
        "CREATE TABLE f3_orphan_src_x PARTITION OF f3_orphan_src FOR VALUES IN ('X')"
    ).expect("recreate partition X");
    Spi::run(
        "INSERT INTO f3_orphan_src (id, region, amount) VALUES (2, 'X', 222)"
    ).expect("seed fresh X data");

    // 6. Attempt reconcile — should now heal the overlapping orphan
    //    BEFORE fix: aborts with "would overlap partition"
    //    AFTER fix: succeeds and fills X with fresh data
    let recon_result = Spi::get_one::<String>(
        "SELECT reflex_reconcile_partition('f3_orphan_v', 'X')"
    ).expect("reconcile call").expect("result");

    // Check for success (should not contain error about overlap)
    // The result will start with "RECONCILED" on success, possibly with partition details
    assert!(!recon_result.contains("would overlap"),
        "reconcile must not fail with overlap error (got: {})", recon_result);
    assert!(recon_result.starts_with("RECONCILED"),
        "reconcile must succeed (got: {})", recon_result);

    // 7. Verify the partition was repopulated with fresh data
    let x_count_after = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f3_orphan_v WHERE region = 'X'"
    ).expect("q").expect("c");
    // After reconcile, we should have the fresh data from the re-created source
    // (the old orphan was dropped during swap, new one populated from base_query)
    assert!(x_count_after >= 1, "X partition should be repopulated after reconcile (got {})", x_count_after);

    // 8. Verify the total value is correct (should be the fresh value 222)
    let x_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM f3_orphan_v WHERE region = 'X'"
    ).expect("q").expect("v");
    assert_eq!(x_total.to_string(), "222", "X partition total should be fresh value (222)");
}
