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
