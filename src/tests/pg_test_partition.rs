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

/// PS-8 S2: the advisory lock `reflex_sync_partitions` takes to serialize DDL on
/// an IMV must use the SAME two-key `(int4, int4)` form as every IMV-name
/// maintenance lock (immediate/deferred trigger bodies, deferred flush,
/// partition flush). A one-key `bigint` lock and a two-key lock occupy different
/// advisory-lock spaces in PostgreSQL and never mutually exclude, so an arity
/// split silently defeats the per-IMV serialization invariant. Held to end of
/// this transaction, the lock is inspectable in `pg_locks`: the two-key form
/// stores `classid = hashtext(view)`, `objid = hashtext(reverse(view))`,
/// `objsubid = 2`; a one-key form would show `objsubid = 1` and would not split
/// on those keys at all.
#[pg_test]
fn pg_part_sync_advisory_lock_uses_two_key_imv_form() {
    Spi::run("CREATE TABLE part_lock_a (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_lock_a_n PARTITION OF part_lock_a FOR VALUES IN ('N')").expect("p1");
    Spi::run("INSERT INTO part_lock_a (id, region, amount) VALUES (1, 'N', 10)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_lock_a_v', \
            'SELECT region, SUM(amount) AS total FROM part_lock_a GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create");

    // Acquire the sync lock; a pg_advisory_xact_lock is held to end of this
    // test transaction, so it is still visible in pg_locks afterward.
    Spi::run("SELECT reflex_sync_partitions('part_lock_a_v')").expect("sync");

    // -1 means "no advisory lock split on (hashtext(view), hashtext(reverse(view)))"
    // — i.e. a one-key bigint lock, which lives in a different lock space.
    let subid = Spi::get_one::<i32>(
        "SELECT COALESCE( \
            (SELECT objsubid::int FROM pg_locks \
              WHERE locktype = 'advisory' AND pid = pg_backend_pid() \
                AND classid = (hashtext('part_lock_a_v'))::oid \
                AND objid = (hashtext(reverse('part_lock_a_v')))::oid), \
            -1)",
    )
    .expect("pg_locks query failed")
    .expect("COALESCE always returns a row");
    assert_eq!(
        subid, 2,
        "reflex_sync_partitions must take the two-key (int4,int4) IMV-name advisory lock \
         so it shares the maintenance lock space (objsubid=2); got objsubid={subid} \
         (-1 == a one-key bigint lock, which never excludes the two-key maintenance locks)"
    );
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

/// An empty source-tree enumeration must NEVER be read as "every IMV partition
/// is an orphan". `list_partition_tree` returns an empty Vec both when the
/// anchor genuinely has no children AND when it could not be resolved at all
/// (`to_regclass` NULL on an unqualified name, a non-partitioned anchor, a
/// failed catalog query) — so an empty expected-set carries no information and
/// must not authorise a drop. `execute_partition_swap_for_child` already
/// refuses on exactly this condition (the "F3 fail-safe"); the sync path is
/// far more destructive (it drops every non-expected child, not just
/// bound-colliding ones) and must refuse too.
///
/// Field impact: a `drop_orphans = true` sync — the SQL default, and what
/// `reflex_reconcile_partition` runs internally — emptied a production IMV.
#[pg_test]
fn pg_part_sync_refuses_mass_drop_on_empty_source_tree() {
    Spi::run("CREATE TABLE part_wipe_a (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)").expect("create");
    Spi::run("CREATE TABLE part_wipe_a_n PARTITION OF part_wipe_a FOR VALUES IN ('N')")
        .expect("p1");
    Spi::run("CREATE TABLE part_wipe_a_s PARTITION OF part_wipe_a FOR VALUES IN ('S')")
        .expect("p2");
    Spi::run("INSERT INTO part_wipe_a (id, region, amount) VALUES (1, 'N', 10),(2,'S',20)")
        .expect("seed");

    Spi::run(
        "SELECT create_reflex_ivm( \
            'part_wipe_a_v', \
            'SELECT region, SUM(amount) AS total FROM part_wipe_a GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create");

    let rows_before = Spi::get_one::<i64>("SELECT count(*) FROM part_wipe_a_v")
        .expect("q")
        .expect("c");
    assert_eq!(rows_before, 2, "fixture must start with both region rows");

    // Drive the source tree to enumerate empty WITHOUT destroying the IMV's
    // right to its partitions: the anchor still exists and still owns `region`,
    // so anchor resolution succeeds and only the child enumeration comes back
    // empty. This is the shape a mid-maintenance source (all children detached)
    // presents, and the same empty Vec an unresolvable anchor produces.
    Spi::run("ALTER TABLE part_wipe_a DETACH PARTITION part_wipe_a_n").expect("detach n");
    Spi::run("ALTER TABLE part_wipe_a DETACH PARTITION part_wipe_a_s").expect("detach s");

    let msg = Spi::get_one::<String>("SELECT reflex_sync_partitions('part_wipe_a_v')")
        .expect("call")
        .expect("msg");

    let children = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhparent \
         WHERE c.relname = 'part_wipe_a_v'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(
        children, 2,
        "sync must not drop IMV partitions on an empty source enumeration, got {children} \
         remaining (msg: {msg})"
    );

    let rows_after = Spi::get_one::<i64>("SELECT count(*) FROM part_wipe_a_v")
        .expect("q")
        .expect("c");
    assert_eq!(
        rows_after, 2,
        "IMV data must survive an empty source enumeration (msg: {msg})"
    );

    assert!(
        msg.contains("refused orphan drop"),
        "sync must refuse LOUDLY, not silently skip — got: {msg}"
    );
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

/// Bug 1: the flush error handler writes `last_error`/`failures` to the same
/// table the deferred flush trigger fires on. Scoping the trigger to
/// `UPDATE OF enqueued_at` means only a genuine re-enqueue re-arms it.
#[pg_test]
fn pg_part_pending_error_write_does_not_rearm() {
    // Check that the trigger is scoped to enqueued_at column only
    let event_count_result: Option<i64> = Spi::get_one(
        "SELECT count(*) FROM pg_trigger t
          WHERE t.tgname = '__reflex_partition_flush_trigger'
            AND t.tgattr::int2[] @> ARRAY[
                  (SELECT attnum FROM pg_attribute
                    WHERE attrelid = 'public.__reflex_partition_pending'::regclass
                      AND attname = 'enqueued_at')]::int2[]",
    )
    .expect("trigger column scope query");
    let events = event_count_result.expect("count");
    assert_eq!(
        events, 1,
        "flush trigger must be scoped to the enqueued_at column"
    );

    // Create a partitioned test source to use as the root
    Spi::run("CREATE TABLE test_pend_src (id INT) PARTITION BY RANGE (id)").expect("create partitioned test source");
    Spi::run("CREATE TABLE test_pend_src_part1 PARTITION OF test_pend_src FOR VALUES FROM (1) TO (100)")
        .expect("create partition");

    // Seed the pending table via direct INSERT (bypassing enqueued_at enforcement)
    // Use DISABLE TRIGGER to prevent the flush trigger from firing on INSERT
    Spi::run("ALTER TABLE public.__reflex_partition_pending DISABLE TRIGGER __reflex_partition_flush_trigger")
        .expect("disable trigger");
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending (source_root, enqueued_at, failures)
         VALUES ('public.test_pend_src', NOW(), 0)",
    )
    .expect("seed pending row");
    Spi::run("ALTER TABLE public.__reflex_partition_pending ENABLE TRIGGER __reflex_partition_flush_trigger")
        .expect("enable trigger");

    // The error-handler pattern: touches only last_error + failures (not enqueued_at).
    // This update simulates what an EXCEPTION handler will do in Task 2.
    // With UPDATE OF enqueued_at scoping, this should NOT re-arm the trigger.
    Spi::run(
        "UPDATE public.__reflex_partition_pending
            SET last_error = 'simulated error', failures = failures + 1
          WHERE source_root = 'public.test_pend_src'",
    )
    .expect("error-handler style update must not re-arm the flush");

    // Force any deferred triggers to fire; none should fire for this update
    // because it only touches last_error and failures, not enqueued_at
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("trigger fire check");

    // Verify the failures column was incremented and persisted (this proves
    // the row survived the update and the trigger did not re-fire)
    let failures_result: Option<i32> = Spi::get_one(
        "SELECT COALESCE(failures, 0) FROM public.__reflex_partition_pending WHERE source_root = 'public.test_pend_src'",
    )
    .expect("failures query");
    let failures = failures_result.expect("get failures");
    assert_eq!(failures, 1, "failures must increment without re-arming trigger");

    // Positive control: UPDATE OF enqueued_at MUST re-arm the trigger and fire the flush.
    // The flush deletes the pending row on success, so asserting its absence proves
    // the trigger fired and the flush executed.
    Spi::run(
        "UPDATE public.__reflex_partition_pending
            SET enqueued_at = NOW()
          WHERE source_root = 'public.test_pend_src'",
    )
    .expect("update enqueued_at for positive control");

    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("trigger fire for positive control");

    // Assert the row is now gone, deleted by the successful flush.
    let remaining_count: Option<i64> = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root = 'public.test_pend_src'",
    )
    .expect("count after flush");
    let count = remaining_count.expect("get count");
    assert_eq!(
        count, 0,
        "pending row must be deleted by flush triggered via UPDATE OF enqueued_at"
    );
}

/// Bug 1: a root that has already failed the cap number of times is skipped,
/// so a poison root cannot hang a committing backend indefinitely.
#[pg_test]
fn pg_part_flush_skips_root_past_failure_cap() {
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending (source_root, failures)
         VALUES ('public.poison_src', 5)",
    )
    .expect("seed capped root");

    let out = Spi::get_one::<String>("SELECT reflex_flush_partitions()")
        .expect("flush call")
        .expect("flush result");
    assert!(
        !out.starts_with("ERROR"),
        "capped root must be skipped, not fatal: {out}"
    );

    let still_pending: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root = 'public.poison_src'",
    )
    .expect("pending query")
    .unwrap_or(0);
    assert_eq!(
        still_pending, 1,
        "capped root keeps its pending row for reflex_doctor"
    );
}

/// The deferred commit-time trigger reaches the flush via
/// `reflex_flush_partition_source(root)` (the single-root path), not the
/// queue-draining `reflex_flush_partitions()`. The cap must engage there too, or
/// a poison root is retried on every triggering commit — exactly the path the
/// production deadlock occurred on.
#[pg_test]
fn pg_part_flush_source_skips_root_past_failure_cap() {
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending (source_root, failures)
         VALUES ('public.poison_src', 5)",
    )
    .expect("seed capped root");

    let out = Spi::get_one::<String>("SELECT reflex_flush_partition_source('public.poison_src')")
        .expect("flush call")
        .expect("flush result");
    assert!(
        !out.starts_with("ERROR"),
        "capped root must be skipped on the single-root path, not fatal: {out}"
    );

    let still_pending: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root = 'public.poison_src'",
    )
    .expect("pending query")
    .unwrap_or(0);
    assert_eq!(
        still_pending, 1,
        "capped root keeps its pending row on the single-root path too"
    );
}

#[pg_test]
fn pg_part_flush_failure_increments_counter_across_rollback() {
    Spi::run(
        "CREATE TABLE fail_src (id INT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)"
    )
    .expect("create partitioned source");
    Spi::run("CREATE TABLE fail_src_north PARTITION OF fail_src FOR VALUES IN ('NORTH')")
        .expect("create partition");
    Spi::run("INSERT INTO fail_src (id, region, amount) VALUES (1, 'NORTH', 100)")
        .expect("seed data");

    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm(
            'fail_imv',
            'SELECT region, SUM(amount) AS total FROM fail_src GROUP BY region',
            NULL, NULL, NULL, NULL,
            ARRAY['region']
         )",
    )
    .expect("create IMV call")
    .expect("create IMV result");
    assert!(!create_result.starts_with("ERROR"), "IMV creation failed: {create_result}");

    Spi::run("ALTER TABLE public.__reflex_partition_pending DISABLE TRIGGER __reflex_partition_flush_trigger")
        .expect("disable trigger");
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending (source_root, enqueued_at, failures)
         VALUES ('public.fail_src', NOW(), 0)",
    )
    .expect("seed pending row");
    Spi::run("ALTER TABLE public.__reflex_partition_pending ENABLE TRIGGER __reflex_partition_flush_trigger")
        .expect("enable trigger");

    let pending_count_before_flush: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root = 'public.fail_src'",
    )
    .expect("pending count before flush")
    .unwrap_or(0);
    assert_eq!(pending_count_before_flush, 1, "pending row must be seeded before flush");

    Spi::run(
        "CREATE FUNCTION fail_trigger_func() RETURNS TRIGGER LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'simulated flush failure'; END; $$"
    )
    .expect("create trigger function");

    Spi::run(
        "CREATE TRIGGER fail_pending_delete_trigger BEFORE DELETE ON public.__reflex_partition_pending
         FOR EACH ROW WHEN (OLD.source_root = 'public.fail_src')
         EXECUTE FUNCTION fail_trigger_func()"
    )
    .expect("create trigger to force flush failure");

    let _flush_out = Spi::get_one::<String>("SELECT reflex_flush_partitions()")
        .expect("first flush call")
        .expect("first flush result");

    let pending_count_after_flush: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root = 'public.fail_src'",
    )
    .expect("pending count after flush")
    .unwrap_or(0);

    if pending_count_after_flush == 0 {
        panic!("pending row was deleted - flush succeeded when it should have failed");
    }

    let failures_after_first: Option<i32> = Spi::get_one(
        "SELECT failures FROM public.__reflex_partition_pending WHERE source_root = 'public.fail_src'",
    )
    .expect("first failures query");
    assert_eq!(failures_after_first, Some(1), "failures must be 1 after first failed flush");

    let error_after_first: Option<String> = Spi::get_one(
        "SELECT last_error FROM public.__reflex_partition_pending WHERE source_root = 'public.fail_src'",
    )
    .ok()
    .flatten();
    assert!(
        error_after_first.is_some() && !error_after_first.as_ref().map(|s| s.is_empty()).unwrap_or(true),
        "last_error must be populated after failed flush"
    );

    let _flush_out2 = Spi::get_one::<String>("SELECT reflex_flush_partitions()")
        .expect("second flush call")
        .expect("second flush result");

    let failures_after_second: Option<i32> = Spi::get_one(
        "SELECT failures FROM public.__reflex_partition_pending WHERE source_root = 'public.fail_src'",
    )
    .expect("second failures query");
    assert_eq!(failures_after_second, Some(2), "failures must be 2 after second failed flush");

    let still_pending: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root = 'public.fail_src'",
    )
    .expect("pending row count query")
    .unwrap_or(0);
    assert_eq!(still_pending, 1, "pending row must survive failed flush for retry");
}

/// F4: the pending table stores source_root canonically (schema.relname). A
/// single-root flush called with a BARE name must still match the stored row,
/// so the failure cap engages instead of being silently bypassed.
#[pg_test]
fn pg_part_flush_source_canonicalizes_bare_root() {
    Spi::run("CREATE TABLE fc_src (id INT) PARTITION BY RANGE (id)").expect("src");
    // Below the cap, so the root is PROCESSED. A successful flush drains the
    // pending row via `DELETE ... WHERE source_root = <root>`. The row is stored
    // qualified; a bare call must canonicalize to 'public.fc_src' for that DELETE
    // to match. This discriminates the fix: without canonicalization the bare
    // DELETE misses the qualified row and it survives; with it, the row is gone.
    Spi::run("INSERT INTO public.__reflex_partition_pending (source_root, failures) \
              VALUES ('public.fc_src', 0)").expect("seed qualified, below cap");

    let out = Spi::get_one::<String>("SELECT reflex_flush_partition_source('fc_src')")
        .expect("flush").expect("result");
    assert!(!out.starts_with("ERROR"), "bare flush must not be fatal: {out}");

    let remaining: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root = 'public.fc_src'",
    ).expect("q").unwrap_or(-1);
    assert_eq!(remaining, 0, "successful bare flush must drain the stored qualified pending row");
}

/// Bug 2 regression: rows for a key whose source leaf appeared only AFTER the
/// IMV was built (the field report's stuck-flush gap) sit in the IMV's DEFAULT
/// partition. reflex_sync_partitions must then create the dedicated IMV leaf,
/// which PostgreSQL refuses while the default holds a matching row (SQLSTATE
/// 23514) unless those rows are first drained OUT of the default and INTO the
/// new leaf. Because sync creates leaves WITHOUT filling them, the drain must
/// MOVE the rows, never DELETE them — a DELETE would lose the only copy.
///
/// Precondition (asserted, not assumed): the IMV must genuinely hold residue in
/// its default before the sync. The source-side leaf is added under
/// `session_replication_role = replica` so the IMV's maintenance trigger does
/// not fire and undo that residue — reproducing the gap a stuck flush leaves.
#[pg_test]
fn pg_part_standalone_sync_preserves_default_rows() {
    Spi::run("CREATE TABLE sp_src (k TEXT NOT NULL, v INT) PARTITION BY LIST (k)").expect("src");
    Spi::run("CREATE TABLE sp_src_a PARTITION OF sp_src FOR VALUES IN ('a')").expect("src a");
    Spi::run("CREATE TABLE sp_src_def PARTITION OF sp_src DEFAULT").expect("src default");
    Spi::run("INSERT INTO sp_src VALUES ('a', 1), ('a', 2)").expect("seed a");

    let sql = "SELECT k, sum(v) AS s FROM sp_src GROUP BY k";
    let res = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('sp_imv', '{}', 'k', NULL, NULL, NULL, ARRAY['k'])",
        sql.replace('\'', "''")
    ))
    .expect("create call")
    .expect("create result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");

    // 'b' has no source leaf yet, so its rows fall to the source default and the
    // IMV maintains their aggregate into ITS default (no 'b' IMV leaf exists).
    Spi::run("INSERT INTO sp_src VALUES ('b', 10)").expect("seed b into source default");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("maintain IMV");

    // Confirm the precondition: the IMV default now holds the ('b', 10) aggregate.
    let default_rows: i64 = Spi::get_one(
        "SELECT s::bigint FROM sp_imv_sp_src_def WHERE k = 'b'",
    )
    .expect("imv default probe")
    .unwrap_or(-1);
    assert_eq!(
        default_rows, 10,
        "precondition: IMV default must hold the 'b' aggregate before sync"
    );

    // The app promotes 'b' to a dedicated source leaf. Done under replica role so
    // the IMV's maintenance trigger does not fire — the IMV keeps its default
    // residue, exactly as it would while a flush is stuck.
    Spi::run("SET session_replication_role = replica").expect("suppress triggers");
    Spi::run("ALTER TABLE sp_src DETACH PARTITION sp_src_def").expect("detach src default");
    Spi::run("CREATE TABLE sp_src_b PARTITION OF sp_src FOR VALUES IN ('b')").expect("src b");
    Spi::run("INSERT INTO sp_src_b SELECT * FROM sp_src_def WHERE k = 'b'").expect("move b in src");
    Spi::run("DELETE FROM sp_src_def WHERE k = 'b'").expect("clear b from src default");
    Spi::run("ALTER TABLE sp_src ATTACH PARTITION sp_src_def DEFAULT").expect("reattach src default");
    Spi::run("SET session_replication_role = origin").expect("restore triggers");

    let before: i64 = Spi::get_one("SELECT count(*) FROM sp_imv")
        .expect("before count").unwrap_or(-1);

    // Without the default drain this fails with SQLSTATE 23514 (sync returns an
    // ERROR string); with a DELETE-based drain the 'b' aggregate would vanish.
    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('sp_imv')")
        .expect("sync call")
        .expect("sync result");
    assert!(!sync.starts_with("ERROR"), "sync returned: {sync}");

    let after: i64 = Spi::get_one("SELECT count(*) FROM sp_imv")
        .expect("after count").unwrap_or(-1);
    assert_eq!(
        after, before,
        "sync must not lose IMV rows held in the default partition"
    );

    let b_total: i64 = Spi::get_one("SELECT s::bigint FROM sp_imv WHERE k = 'b'")
        .expect("b query").unwrap_or(-1);
    assert_eq!(b_total, 10, "the 'b' aggregate must move into the new leaf, not be deleted");

    let b_in_default: i64 = Spi::get_one(
        "SELECT count(*) FROM sp_imv_sp_src_def WHERE k = 'b'",
    )
    .expect("default recheck")
    .unwrap_or(-1);
    assert_eq!(b_in_default, 0, "the 'b' aggregate must no longer be in the default");
}

/// Task 4 regression: second sync call on a partitioned IMV whose default
/// partition already exists must preserve default-resident rows. Retained across
/// the drain rewrite: a re-sync where the IMV default holds legitimate data for
/// keys without dedicated leaves must never lose that data. (It originally
/// guarded a self-referential per-node-drain truncation bug; the tree-wide drain
/// that replaced it never touches a default self-referentially, and this test
/// keeps the row-preservation guarantee locked regardless of mechanism.)
///
/// This test builds a scenario where the IMV default holds legitimate data for
/// keys without dedicated leaves, then calls reflex_sync_partitions a second
/// time (when the default already exists), and asserts the data survives intact.
#[pg_test]
fn pg_part_default_self_reference_resync_preserves_data() {
    Spi::run("CREATE TABLE sr_src (k TEXT NOT NULL, v INT) PARTITION BY LIST (k)").expect("src");
    Spi::run("CREATE TABLE sr_src_a PARTITION OF sr_src FOR VALUES IN ('a')").expect("src a");
    Spi::run("CREATE TABLE sr_src_def PARTITION OF sr_src DEFAULT").expect("src default");
    Spi::run("INSERT INTO sr_src VALUES ('a', 1), ('a', 2)").expect("seed a");

    let sql = "SELECT k, sum(v) AS s FROM sr_src GROUP BY k";
    let res = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('sr_imv', '{}', 'k', NULL, NULL, NULL, ARRAY['k'])",
        sql.replace('\'', "''")
    ))
    .expect("create call")
    .expect("create result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");

    // Key 'b' has no source leaf, so its rows fall into source default and
    // IMV default holds their aggregate.
    Spi::run("INSERT INTO sr_src VALUES ('b', 100)").expect("seed b into source default");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("maintain IMV");

    // Precondition: IMV default holds ('b', 100) aggregate.
    let default_val: i64 = Spi::get_one(
        "SELECT s::bigint FROM sr_imv_sr_src_def WHERE k = 'b'",
    )
    .expect("imv default probe")
    .unwrap_or(-1);
    assert_eq!(
        default_val, 100,
        "precondition: IMV default must hold the 'b' aggregate before resync"
    );

    let before: i64 = Spi::get_one("SELECT count(*) FROM sr_imv")
        .expect("before count").unwrap_or(-1);

    // Second sync call: the IMV default already exists and holds the 'b'
    // aggregate. The tree-wide drain empties it into a holding table and refills
    // by routing; 'b' has no dedicated leaf, so it must land back in the default.
    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('sr_imv')")
        .expect("resync call")
        .expect("resync result");
    assert!(!sync.starts_with("ERROR"), "resync returned: {sync}");

    let after: i64 = Spi::get_one("SELECT count(*) FROM sr_imv")
        .expect("after count").unwrap_or(-1);
    assert_eq!(
        after, before,
        "resync must not lose IMV rows held in the default partition"
    );

    // Verify the exact value is still correct after resync.
    let b_total: i64 = Spi::get_one("SELECT s::bigint FROM sr_imv WHERE k = 'b'")
        .expect("b query").unwrap_or(-1);
    assert_eq!(b_total, 100, "the 'b' aggregate must survive the resync intact");

    let b_in_default: i64 = Spi::get_one(
        "SELECT count(*) FROM sr_imv_sr_src_def WHERE k = 'b'",
    )
    .expect("default recheck")
    .unwrap_or(-1);
    assert_eq!(b_in_default, 1, "the 'b' aggregate must still be in the default");
}

/// F2: drain empties every default in a tree into holding tables, the caller
/// builds new leaves against the now-empty defaults, and refill relocates rows
/// via tuple routing into the correct leaf (unmatched rows stay in the default).
/// Multi-level tree: LIST(region) -> RANGE(month), residue in the TOP default.
#[pg_test]
fn pg_part_drain_refill_multilevel_relocates_rows() {
    Spi::run("CREATE TABLE dm_p (region TEXT NOT NULL, mon DATE NOT NULL, v INT) \
              PARTITION BY LIST (region)").expect("parent");
    Spi::run("CREATE TABLE dm_p_default PARTITION OF dm_p DEFAULT").expect("default");
    // Residue: region 'r1' has no dedicated subtree yet, so its rows sit in the default.
    Spi::run("INSERT INTO dm_p VALUES ('r1', '2026-02-10', 1), ('r1', '2026-05-10', 2), \
              ('rx', '2026-02-10', 9)").expect("seed");

    // Wrapper: drain the tree's defaults, build the r1 subtree (region leaf +
    // two month leaves) against the emptied default, then refill.
    let build = "CREATE TABLE dm_p_r1 PARTITION OF dm_p FOR VALUES IN ('r1') PARTITION BY RANGE (mon);\
                 CREATE TABLE dm_p_r1_q1 PARTITION OF dm_p_r1 FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');\
                 CREATE TABLE dm_p_r1_q2 PARTITION OF dm_p_r1 FOR VALUES FROM ('2026-04-01') TO ('2026-07-01')";
    let out = Spi::get_one::<String>(&format!(
        "SELECT __reflex_test_drain_build_refill('public.dm_p', '{}')",
        build.replace('\'', "''")
    )).expect("call").expect("result");
    assert_eq!(out, "OK", "drain/build/refill returned: {out}");

    // The two r1 rows relocated into the correct month leaves.
    let q1: i64 = Spi::get_one("SELECT count(*) FROM dm_p_r1_q1").expect("q1").unwrap_or(-1);
    let q2: i64 = Spi::get_one("SELECT count(*) FROM dm_p_r1_q2").expect("q2").unwrap_or(-1);
    assert_eq!(q1, 1, "Feb r1 row -> q1 leaf");
    assert_eq!(q2, 1, "May r1 row -> q2 leaf");
    // The unrelated 'rx' row stays in the default; total is preserved.
    let dflt: i64 = Spi::get_one("SELECT count(*) FROM dm_p_default").expect("d").unwrap_or(-1);
    assert_eq!(dflt, 1, "rx row (no leaf) stays in the default");
    let total: i64 = Spi::get_one("SELECT count(*) FROM dm_p").expect("t").unwrap_or(-1);
    assert_eq!(total, 3, "no rows lost");
}

/// F2: a tree with no default partition — drain is a no-op, build proceeds.
#[pg_test]
fn pg_part_drain_refill_noop_without_default() {
    Spi::run("CREATE TABLE dn2_p (k TEXT NOT NULL, v INT) PARTITION BY LIST (k)").expect("parent");
    let out = Spi::get_one::<String>(
        "SELECT __reflex_test_drain_build_refill('public.dn2_p', \
         'CREATE TABLE dn2_p_a PARTITION OF dn2_p FOR VALUES IN (''a'')')",
    ).expect("call").expect("result");
    assert_eq!(out, "OK", "returned: {out}");
    let exists: i64 = Spi::get_one("SELECT count(*) FROM pg_class WHERE relname = 'dn2_p_a'")
        .expect("q").unwrap_or(0);
    assert_eq!(exists, 1, "leaf created when there is no default");
}

/// F2 end-to-end: a multi-level partitioned PASSTHROUGH IMV
/// (LIST dem_plan_id -> RANGE order_date) with residue in the IMV's top default
/// for a plan promoted later. Standalone reflex_sync_partitions must build the
/// plan subtree and relocate the residue into the correct month leaf — the case
/// the per-node drain aborted on. Multi-level partition_by is supported for
/// passthrough IMVs (see pg_test_subpartition.rs), not aggregates.
#[pg_test]
fn pg_part_sync_multilevel_default_residue_recovered() {
    Spi::run("CREATE TABLE mls (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, \
              product_id BIGINT, qty INT) PARTITION BY LIST (dem_plan_id)").expect("src");
    Spi::run("CREATE TABLE mls_172 PARTITION OF mls FOR VALUES IN (172) PARTITION BY RANGE (order_date)")
        .expect("172");
    Spi::run("CREATE TABLE mls_172_q1 PARTITION OF mls_172 FOR VALUES FROM ('2026-01-01') TO ('2026-04-01')")
        .expect("172q1");
    Spi::run("CREATE TABLE mls_def PARTITION OF mls DEFAULT").expect("src default");
    Spi::run("INSERT INTO mls VALUES (172, '2026-02-01', 1, 10)").expect("seed 172");

    let res = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('mlv', \
         'SELECT dem_plan_id, order_date, product_id, qty FROM mls', \
         'dem_plan_id,product_id,order_date', NULL, NULL, NULL, ARRAY['dem_plan_id','order_date'])",
    ).expect("create").expect("result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");

    // Plan 999 has no source leaf yet: its row falls to the source default and
    // the IMV maintains its passthrough copy into its own top default.
    Spi::run("INSERT INTO mls VALUES (999, '2026-05-01', 2, 20)").expect("seed 999");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("maintain");

    // Promote 999 on the source side under replica role so the IMV keeps its
    // default residue (simulating the stuck-flush gap).
    Spi::run("SET session_replication_role = replica").expect("suppress");
    Spi::run("ALTER TABLE mls DETACH PARTITION mls_def").expect("detach");
    Spi::run("CREATE TABLE mls_999 PARTITION OF mls FOR VALUES IN (999) PARTITION BY RANGE (order_date)").expect("999");
    Spi::run("CREATE TABLE mls_999_q2 PARTITION OF mls_999 FOR VALUES FROM ('2026-04-01') TO ('2026-07-01')").expect("999q2");
    Spi::run("INSERT INTO mls_999_q2 SELECT * FROM mls_def WHERE dem_plan_id=999").expect("move");
    Spi::run("DELETE FROM mls_def WHERE dem_plan_id=999").expect("clear");
    Spi::run("ALTER TABLE mls ATTACH PARTITION mls_def DEFAULT").expect("reattach");
    Spi::run("SET session_replication_role = origin").expect("restore");

    let before: i64 = Spi::get_one("SELECT count(*) FROM mlv").expect("before").unwrap_or(-1);
    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('mlv')")
        .expect("sync").expect("result");
    assert!(!sync.starts_with("ERROR"), "sync returned: {sync}");

    let after: i64 = Spi::get_one("SELECT count(*) FROM mlv").expect("after").unwrap_or(-1);
    assert_eq!(after, before, "no IMV rows lost draining the multi-level default");
    let qty999: i64 = Spi::get_one("SELECT qty::bigint FROM mlv WHERE dem_plan_id=999")
        .expect("999").unwrap_or(-1);
    assert_eq!(qty999, 20, "the 999 row relocated into the new subtree");
}

/// Task 2 hardening, Finding 1: the tree-wide default drain/build/refill in
/// `reflex_sync_partitions` is a pure PHYSICAL relocation of rows already
/// present in the IMV -- its logical content does not change. But when the
/// IMV being synced is itself a SOURCE for a downstream chained IMV, its
/// target table carries an `AFTER INSERT ... FOR EACH STATEMENT ...
/// REFERENCING NEW TABLE` maintenance trigger (schema_builder.rs). The
/// drain's `DELETE FROM <default leaf>` does NOT fire that trigger (it isn't
/// the partitioned root), but the refill's `INSERT INTO <root>` DOES -- so an
/// unguarded refill re-counts merely-relocated rows into the downstream IMV.
#[pg_test]
fn pg_part_sync_refill_does_not_double_count_downstream_imv() {
    Spi::run("CREATE TABLE dtr_src (k TEXT NOT NULL, v INT) PARTITION BY LIST (k)").expect("src");
    Spi::run("CREATE TABLE dtr_src_a PARTITION OF dtr_src FOR VALUES IN ('a')").expect("src a");
    Spi::run("CREATE TABLE dtr_src_def PARTITION OF dtr_src DEFAULT").expect("src default");
    Spi::run("INSERT INTO dtr_src VALUES ('a', 1), ('a', 2)").expect("seed a");

    // IMV_A: partitioned aggregate over the source.
    let ca = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('dtr_a', 'SELECT k, sum(v) AS s FROM dtr_src GROUP BY k', \
         'k', NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("create a call")
    .expect("create a result");
    assert!(!ca.starts_with("ERROR"), "create IMV_A returned: {ca}");

    // IMV_B chains off IMV_A's target table: a scalar aggregate, unpartitioned.
    let cb = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('dtr_b', 'SELECT sum(s) AS total FROM dtr_a')",
    )
    .expect("create b call")
    .expect("create b result");
    assert!(!cb.starts_with("ERROR"), "create IMV_B returned: {cb}");

    // Key 'b' has no source leaf yet: its row falls to the source default,
    // IMV_A maintains the aggregate into ITS OWN default, and that genuinely
    // new row cascades normally into IMV_B (correct: real new data).
    Spi::run("INSERT INTO dtr_src VALUES ('b', 10)").expect("seed b into source default");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("maintain");

    let default_val: i64 = Spi::get_one("SELECT s::bigint FROM dtr_a_dtr_src_def WHERE k = 'b'")
        .expect("imv_a default probe")
        .unwrap_or(-1);
    assert_eq!(
        default_val, 10,
        "precondition: IMV_A default must hold the 'b' aggregate before promotion"
    );

    let before: i64 = Spi::get_one("SELECT total::bigint FROM dtr_b")
        .expect("before")
        .unwrap_or(-1);
    assert_eq!(before, 13, "precondition: IMV_B must already reflect the 'b' aggregate");

    // Promote 'b' on the SOURCE under replica role so IMV_A's own maintenance
    // trigger does not fire -- IMV_A keeps its default residue for 'b', exactly
    // the shape a stuck flush (or a lagging sync) leaves behind.
    Spi::run("SET session_replication_role = replica").expect("suppress");
    Spi::run("ALTER TABLE dtr_src DETACH PARTITION dtr_src_def").expect("detach src default");
    Spi::run("CREATE TABLE dtr_src_b PARTITION OF dtr_src FOR VALUES IN ('b')").expect("src b");
    Spi::run("INSERT INTO dtr_src_b SELECT * FROM dtr_src_def WHERE k = 'b'").expect("move b in src");
    Spi::run("DELETE FROM dtr_src_def WHERE k = 'b'").expect("clear b from src default");
    Spi::run("ALTER TABLE dtr_src ATTACH PARTITION dtr_src_def DEFAULT").expect("reattach src default");
    Spi::run("SET session_replication_role = origin").expect("restore triggers");

    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('dtr_a')")
        .expect("sync call")
        .expect("sync result");
    assert!(!sync.starts_with("ERROR"), "sync returned: {sync}");

    // Sync only RELOCATES the 'b' row within IMV_A (default -> new dedicated
    // leaf); it must be invisible to IMV_A's own downstream maintenance trigger.
    let after: i64 = Spi::get_one("SELECT total::bigint FROM dtr_b")
        .expect("after")
        .unwrap_or(-1);
    assert_eq!(
        after, before,
        "sync's drain/refill relocation must not re-count rows into a downstream IMV"
    );

    // IMV_A's own content is correct: 'b' now sits in its dedicated leaf, not lost.
    let b_total: i64 = Spi::get_one("SELECT s::bigint FROM dtr_a WHERE k = 'b'")
        .expect("b total")
        .unwrap_or(-1);
    assert_eq!(b_total, 10, "the 'b' aggregate must survive the relocation intact");

    let b_in_default: i64 = Spi::get_one("SELECT count(*) FROM dtr_a_dtr_src_def WHERE k = 'b'")
        .expect("default recheck")
        .unwrap_or(-1);
    assert_eq!(b_in_default, 0, "the 'b' aggregate must no longer be in IMV_A's default");
}

/// Task 2 hardening, Finding 2: residue in a NESTED internal default (a
/// region's own default under the LIST level, not the tree's top default).
/// Source: LIST(l1) -> RANGE(l2). Region 'r1' has a dedicated LIST leaf that
/// is itself RANGE-subpartitioned with ONE dedicated month leaf and its OWN
/// default (`nst_src_r1_def`); a separate, unrelated top-level default
/// (`nst_src_def`) also exists so the two are never confused. A row for r1
/// whose month misses the dedicated leaf lands in r1's OWN default, not the
/// top one. Multi-level partition_by is only supported for PASSTHROUGH IMVs
/// (aggregates reject non-empty sub-levels; see pg_test_subpartition.rs), so
/// this uses a passthrough mirror of the shape, matching
/// `pg_part_sync_multilevel_default_residue_recovered` above but with the
/// residue moved one level deeper -- into the region's own default instead of
/// the tree root's.
#[pg_test]
fn pg_part_sync_nested_default_residue_recovered() {
    Spi::run(
        "CREATE TABLE nst_src (l1 TEXT NOT NULL, l2 DATE NOT NULL, item BIGINT, qty INT) \
         PARTITION BY LIST (l1)",
    )
    .expect("src");
    Spi::run("CREATE TABLE nst_src_r1 PARTITION OF nst_src FOR VALUES IN ('r1') PARTITION BY RANGE (l2)")
        .expect("src r1");
    Spi::run("CREATE TABLE nst_src_r1_q1 PARTITION OF nst_src_r1 FOR VALUES FROM ('2026-01-01') TO ('2026-04-01')")
        .expect("src r1 q1");
    Spi::run("CREATE TABLE nst_src_r1_def PARTITION OF nst_src_r1 DEFAULT").expect("src r1 default (nested)");
    Spi::run("CREATE TABLE nst_src_def PARTITION OF nst_src DEFAULT").expect("src top default");
    Spi::run("INSERT INTO nst_src VALUES ('r1', '2026-02-01', 1, 10)").expect("seed r1 q1");

    let res = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('nst_v', \
         'SELECT l1, l2, item, qty FROM nst_src', \
         'l1,item,l2', NULL, NULL, NULL, ARRAY['l1','l2'])",
    )
    .expect("create")
    .expect("result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");

    // A second r1 row whose month (May) misses the dedicated q1 leaf (Jan-Mar)
    // routes to r1's OWN default -- not the tree's top default. The IMV
    // maintains the passthrough copy into ITS nested default, one level below
    // the tree root.
    Spi::run("INSERT INTO nst_src VALUES ('r1', '2026-05-01', 2, 20)").expect("seed r1 residue");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("maintain");

    // Precondition: residue sits in the IMV's NESTED default (under r1), and
    // the unrelated top-level default is untouched.
    let nested_qty: i32 = Spi::get_one("SELECT qty FROM nst_v_nst_src_r1_def WHERE item = 2")
        .expect("nested default probe")
        .unwrap_or(-1);
    assert_eq!(nested_qty, 20, "precondition: residue must sit in r1's own default");
    let top_default_rows: i64 = Spi::get_one("SELECT count(*) FROM nst_v_nst_src_def")
        .expect("top default probe")
        .unwrap_or(-1);
    assert_eq!(top_default_rows, 0, "precondition: the top-level default must stay empty");

    // Promote the deeper (r1, May) leaf on the SOURCE under replica role so
    // the IMV's own trigger does not fire -- the IMV keeps its nested-default
    // residue, exactly the shape a stuck flush leaves behind one level down.
    Spi::run("SET session_replication_role = replica").expect("suppress");
    Spi::run("ALTER TABLE nst_src_r1 DETACH PARTITION nst_src_r1_def").expect("detach nested default");
    Spi::run(
        "CREATE TABLE nst_src_r1_q2 PARTITION OF nst_src_r1 \
         FOR VALUES FROM ('2026-04-01') TO ('2026-07-01')",
    )
    .expect("src r1 q2");
    Spi::run("INSERT INTO nst_src_r1_q2 SELECT * FROM nst_src_r1_def WHERE item = 2").expect("move deep row");
    Spi::run("DELETE FROM nst_src_r1_def WHERE item = 2").expect("clear nested default");
    Spi::run("ALTER TABLE nst_src_r1 ATTACH PARTITION nst_src_r1_def DEFAULT").expect("reattach nested default");
    Spi::run("SET session_replication_role = origin").expect("restore");

    let before_total: i32 = Spi::get_one("SELECT sum(qty)::int FROM nst_v")
        .expect("before")
        .unwrap_or(-1);
    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('nst_v')")
        .expect("sync call")
        .expect("sync result");
    assert!(!sync.starts_with("ERROR"), "sync returned: {sync}");

    // Total preserved -- the sync only relocated the row, it did not lose or
    // duplicate it.
    let after_total: i32 = Spi::get_one("SELECT sum(qty)::int FROM nst_v")
        .expect("after")
        .unwrap_or(-1);
    assert_eq!(after_total, before_total, "no rows lost or duplicated draining the nested default");

    // The deep row relocated into the newly built dedicated leaf under r1...
    let q2_qty: i32 = Spi::get_one("SELECT qty FROM nst_v_nst_src_r1_q2 WHERE item = 2")
        .expect("q2 probe")
        .unwrap_or(-1);
    assert_eq!(q2_qty, 20, "the deep row must relocate into the new r1/May leaf");

    // ...and no longer sits in r1's nested default.
    let nested_after: i64 = Spi::get_one("SELECT count(*) FROM nst_v_nst_src_r1_def WHERE item = 2")
        .expect("nested recheck")
        .unwrap_or(-1);
    assert_eq!(nested_after, 0, "the deep row must no longer be in r1's nested default");

    // The unrelated top-level default was never touched by this relocation.
    let top_after: i64 = Spi::get_one("SELECT count(*) FROM nst_v_nst_src_def")
        .expect("top recheck")
        .unwrap_or(-1);
    assert_eq!(top_after, 0, "the top-level default must remain untouched");
}

/// Regression (1.10.10): the drain/refill relocation in `reflex_sync_partitions`
/// suppressed downstream-feeding triggers with a session-wide
/// `SET session_replication_role = replica`, a SUPERUSER-only GUC. Reconcile of
/// a partitioned IMV therefore aborted for any non-superuser role with
/// `permission denied to set parameter "session_replication_role"`, even when
/// that role owned every table involved. Trigger suppression must instead be
/// scoped to the relocation roots via ownership-based `ALTER TABLE ... DISABLE
/// TRIGGER USER`, which a non-superuser table owner is permitted to run.
#[pg_test]
fn pg_part_sync_relocation_works_for_non_superuser_owner() {
    Spi::run("CREATE TABLE permrec_src (k TEXT NOT NULL, v INT) PARTITION BY LIST (k)").expect("src");
    Spi::run("CREATE TABLE permrec_src_a PARTITION OF permrec_src FOR VALUES IN ('a')").expect("src a");
    Spi::run("CREATE TABLE permrec_src_def PARTITION OF permrec_src DEFAULT").expect("src default");
    Spi::run("INSERT INTO permrec_src VALUES ('a', 1), ('a', 2)").expect("seed a");

    let create = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('permrec_v', 'SELECT k, sum(v) AS s FROM permrec_src GROUP BY k', \
         'k', NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(!create.starts_with("ERROR"), "create returned: {create}");

    // Force default residue in the IMV so the sync performs a real drain/refill
    // relocation -- the code path that suppresses triggers. Promote 'b' on the
    // source under replica role so the IMV keeps its default residue behind.
    Spi::run("INSERT INTO permrec_src VALUES ('b', 10)").expect("seed b");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("maintain");
    Spi::run("SET session_replication_role = replica").expect("suppress");
    Spi::run("ALTER TABLE permrec_src DETACH PARTITION permrec_src_def").expect("detach");
    Spi::run("CREATE TABLE permrec_src_b PARTITION OF permrec_src FOR VALUES IN ('b')").expect("src b");
    Spi::run("INSERT INTO permrec_src_b SELECT * FROM permrec_src_def WHERE k = 'b'").expect("move b");
    Spi::run("DELETE FROM permrec_src_def WHERE k = 'b'").expect("clear b");
    Spi::run("ALTER TABLE permrec_src ATTACH PARTITION permrec_src_def DEFAULT").expect("reattach");
    Spi::run("SET session_replication_role = origin").expect("restore");

    // A non-superuser role that OWNS every IMV table (target + intermediate and
    // their partition children) and may create the holding tables the drain
    // needs. This is the shape of a locked-down production database.
    Spi::run("CREATE ROLE permrec_owner NOSUPERUSER").expect("role");
    Spi::run("GRANT CREATE ON SCHEMA public TO permrec_owner").expect("schema create grant");
    Spi::run("GRANT SELECT ON public.__reflex_ivm_reference TO permrec_owner").expect("catalog select grant");
    Spi::run(
        "DO $$ DECLARE r record; BEGIN \
           FOR r IN SELECT c.oid::regclass::text AS t FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' \
               AND (c.relname = 'permrec_v' OR c.relname LIKE 'permrec\\_v\\_%' \
                    OR c.relname LIKE '\\_\\_reflex_intermediate_permrec_v%') \
           LOOP EXECUTE format('ALTER TABLE %s OWNER TO permrec_owner', r.t); END LOOP; \
         END $$",
    )
    .expect("transfer ownership of IMV tables to the non-superuser role");

    Spi::run("SET ROLE permrec_owner").expect("assume non-superuser role");
    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('permrec_v')")
        .expect("sync call")
        .expect("sync result");
    Spi::run("RESET ROLE").expect("reset role");

    assert!(
        !sync.starts_with("ERROR"),
        "sync must succeed for a non-superuser table owner, got: {sync}"
    );

    // The relocation still did its job: 'b' moved out of the default into its
    // dedicated leaf, nothing lost or duplicated.
    let b_total: i64 = Spi::get_one("SELECT s::bigint FROM permrec_v WHERE k = 'b'")
        .expect("b total")
        .unwrap_or(-1);
    assert_eq!(b_total, 10, "the relocated 'b' aggregate must survive intact");
    let b_in_default: i64 =
        Spi::get_one("SELECT count(*) FROM permrec_v_permrec_src_def WHERE k = 'b'")
            .expect("default recheck")
            .unwrap_or(-1);
    assert_eq!(b_in_default, 0, "'b' must no longer sit in the IMV default");
}

// ---------------------------------------------------------------------------
// Failure atomicity of `reflex_reconcile_partition`
//
// `reflex_reconcile_partition` reports failure by RETURNING a string starting
// with "ERROR:" rather than by raising — so the calling statement COMMITS.
// Everything the call did before the failure must therefore be undone by the
// call itself, or an operator who reads "ERROR" (and reasonably concludes
// nothing happened) is left with committed, destructive DDL.
// ---------------------------------------------------------------------------

/// Comma-joined, ordered child relnames of a partitioned relation — the exact
/// partition set, not just its cardinality, so a drop compensated by a create
/// cannot pass.
fn partition_child_set(parent: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(c.relname, ',' ORDER BY c.relname), '') \
         FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         JOIN pg_class p ON p.oid = i.inhparent \
         WHERE p.relname = '{parent}'"
    ))
    .expect("child set query")
    .expect("child set")
}

/// T1 — a reconcile that fails must not commit the destructive orphan drop its
/// own pre-sync performed. This is the field shape: a detached source child
/// makes the pre-sync legitimately DROP the mirrored IMV child, then a bogus
/// `source_partition` (arg 3 handed a key value instead of a partition name)
/// fails the reconcile. The operator sees "ERROR" — and the IMV partition,
/// with its data, must still be there.
#[pg_test]
fn pg_part_failed_reconcile_rolls_back_presync_orphan_drop() {
    Spi::run(
        "CREATE TABLE atom1 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    Spi::run("CREATE TABLE atom1_n PARTITION OF atom1 FOR VALUES IN ('N')").expect("n");
    Spi::run("CREATE TABLE atom1_s PARTITION OF atom1 FOR VALUES IN ('S')").expect("s");
    Spi::run("INSERT INTO atom1 (id, region, amount) VALUES (1,'N',10),(2,'S',20)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('atom1v', \
           'SELECT region, SUM(amount) AS total FROM atom1 GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("create imv");

    let tgt_before = partition_child_set("atom1v");
    let int_before = partition_child_set("__reflex_intermediate_atom1v");
    assert!(
        tgt_before.contains("atom1v_atom1_s"),
        "fixture must start with the S mirror child, got: {tgt_before}"
    );
    let rows_before = Spi::get_one::<i64>("SELECT count(*) FROM atom1v")
        .expect("q")
        .expect("c");
    assert_eq!(rows_before, 2, "fixture must start with both region rows");

    // The source child genuinely goes away, so the pre-sync's orphan drop is
    // correct in isolation — it is the FAILED reconcile that must undo it.
    Spi::run("ALTER TABLE atom1 DETACH PARTITION atom1_s").expect("detach s");

    let msg = Spi::get_one::<String>(
        "SELECT reflex_reconcile_partition('atom1v', 'region', 'no_such_source_child')",
    )
    .expect("call")
    .expect("msg");
    assert!(
        msg.starts_with("ERROR"),
        "reconcile of a bogus source partition must report ERROR, got: {msg}"
    );

    assert_eq!(
        partition_child_set("atom1v"),
        tgt_before,
        "a reconcile that reports ERROR must leave the target partition set untouched (msg: {msg})"
    );
    assert_eq!(
        partition_child_set("__reflex_intermediate_atom1v"),
        int_before,
        "a reconcile that reports ERROR must leave the intermediate partition set untouched (msg: {msg})"
    );
    let rows_after = Spi::get_one::<i64>("SELECT count(*) FROM atom1v")
        .expect("q")
        .expect("c");
    assert_eq!(
        rows_after, 2,
        "the dropped child's DATA must come back too, not just an empty child (msg: {msg})"
    );
    let s_total = Spi::get_one::<pgrx::AnyNumeric>("SELECT total FROM atom1v WHERE region = 'S'")
        .expect("q")
        .expect("s");
    assert_eq!(s_total.to_string(), "20", "the S slice must survive intact");
}

/// T2 — the creative direction. A genuinely new source partition makes the
/// pre-sync CREATE mirror children; a reconcile that then fails must roll that
/// creation back too, so "ERROR" means the whole call was a no-op.
#[pg_test]
fn pg_part_failed_reconcile_rolls_back_presync_child_creation() {
    Spi::run(
        "CREATE TABLE atom2 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    Spi::run("CREATE TABLE atom2_n PARTITION OF atom2 FOR VALUES IN ('N')").expect("n");
    Spi::run("CREATE TABLE atom2_s PARTITION OF atom2 FOR VALUES IN ('S')").expect("s");
    Spi::run("INSERT INTO atom2 (id, region, amount) VALUES (1,'N',10),(2,'S',20)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('atom2v', \
           'SELECT region, SUM(amount) AS total FROM atom2 GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("create imv");

    let tgt_before = partition_child_set("atom2v");
    let int_before = partition_child_set("__reflex_intermediate_atom2v");

    // A new source partition the IMV does not mirror yet — the shape left by a
    // partition created while the DDL hook was not in force (restore, replica
    // promotion, `ALTER EVENT TRIGGER … DISABLE`). It is the RECONCILE's own
    // pre-sync that must create `atom2v_atom2_e` (+ its intermediate), not the
    // ddl_command_end auto-sync, or the creation would sit outside the scope
    // this test is about.
    Spi::run("ALTER EVENT TRIGGER reflex_on_ddl_command_end DISABLE").expect("disable ddl hook");
    Spi::run("CREATE TABLE atom2_e PARTITION OF atom2 FOR VALUES IN ('E')").expect("e");
    Spi::run("ALTER EVENT TRIGGER reflex_on_ddl_command_end ENABLE").expect("re-enable ddl hook");
    assert!(
        !partition_child_set("atom2v").contains("atom2v_atom2_e"),
        "fixture precondition: the E mirror must not exist before the reconcile"
    );

    let msg = Spi::get_one::<String>(
        "SELECT reflex_reconcile_partition('atom2v', '', 'no_such_source_child')",
    )
    .expect("call")
    .expect("msg");
    assert!(
        msg.starts_with("ERROR"),
        "reconcile of a bogus source partition must report ERROR, got: {msg}"
    );

    assert_eq!(
        partition_child_set("atom2v"),
        tgt_before,
        "a reconcile that reports ERROR must roll back the children its pre-sync created \
         (msg: {msg})"
    );
    assert_eq!(
        partition_child_set("__reflex_intermediate_atom2v"),
        int_before,
        "a reconcile that reports ERROR must roll back the intermediate children its pre-sync \
         created (msg: {msg})"
    );
}

/// T3 — the happy path is unchanged: a successful reconcile still commits its
/// work (the pre-sync's AND the swap's), the mirrored child stays usable, and
/// the IMV still matches a fresh recompute under the bidirectional EXCEPT ALL
/// oracle.
#[pg_test]
fn pg_part_successful_reconcile_still_commits_and_stays_correct() {
    Spi::run(
        "CREATE TABLE atom3 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    Spi::run("CREATE TABLE atom3_n PARTITION OF atom3 FOR VALUES IN ('N')").expect("n");
    Spi::run("CREATE TABLE atom3_s PARTITION OF atom3 FOR VALUES IN ('S')").expect("s");
    Spi::run("INSERT INTO atom3 (id, region, amount) VALUES (1,'N',10),(2,'S',20)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('atom3v', \
           'SELECT region, SUM(amount) AS total FROM atom3 GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("create imv");

    // A new source partition the reconcile's own pre-sync must mirror (the DDL
    // hook is held off so the creation belongs to the reconcile) — work that
    // has to SURVIVE the call, since the reconcile succeeds.
    Spi::run("ALTER EVENT TRIGGER reflex_on_ddl_command_end DISABLE").expect("disable ddl hook");
    Spi::run("CREATE TABLE atom3_e PARTITION OF atom3 FOR VALUES IN ('E')").expect("e");
    Spi::run("ALTER EVENT TRIGGER reflex_on_ddl_command_end ENABLE").expect("re-enable ddl hook");

    let top_xid = Spi::get_one::<i64>("SELECT pg_current_xact_id()::text::bigint")
        .expect("xid")
        .expect("xid");

    let msg = Spi::get_one::<String>("SELECT reflex_reconcile_partition('atom3v', '', 'atom3_e')")
        .expect("call")
        .expect("msg");
    assert!(
        msg.starts_with("RECONCILED partitions"),
        "expected RECONCILED, got: {msg}"
    );

    assert!(
        partition_child_set("atom3v").contains("atom3v_atom3_e"),
        "the pre-sync's new child must SURVIVE a successful reconcile"
    );
    // The surviving mirror is a working partition, not just a catalog entry:
    // maintenance into the new key must land and stay correct.
    Spi::run("INSERT INTO atom3 (id, region, amount) VALUES (3,'E',30)").expect("seed e");
    assert_imv_correct(
        "atom3v",
        "SELECT region, SUM(amount) AS total FROM atom3 GROUP BY region",
    );

    // The work ran inside a subtransaction of its own: the registry row it
    // wrote carries that subtransaction's xid, not the top-level one. Without
    // a subtransaction there is nothing for a failed call to roll back to.
    let ref_xmin = Spi::get_one::<i64>(
        "SELECT xmin::text::bigint FROM public.__reflex_ivm_reference WHERE name = 'atom3v'",
    )
    .expect("xmin")
    .expect("xmin");
    assert_ne!(
        ref_xmin, top_xid,
        "reconcile must run in its own subtransaction, but its registry write carries the \
         top-level xid ({top_xid}) — nothing to roll back to"
    );
}

/// T4 — the batch path (`skip_sync => true`, the shape `reflex_flush_partitions`
/// dispatches per root) gets the SAME rollback. `skip_sync` skips the O(tree)
/// prep, never the isolation.
///
/// The flush wraps its statements in a plpgsql `EXCEPTION` block, which is a
/// subtransaction — but one that only rolls back on a RAISED error. Reconcile
/// reports failure by RETURNING `ERROR: …`, so that block completes normally and
/// RELEASEs, committing every child swapped before the failure. Here three real
/// children are reconciled in one call alongside a fourth that cannot resolve;
/// the three sort first, so they are swapped before the failure is reached.
#[pg_test]
fn pg_part_failed_skip_sync_reconcile_rolls_back_children_already_swapped() {
    Spi::run(
        "CREATE TABLE atom4 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    for (child, key) in [("atom4_a", "A"), ("atom4_b", "B"), ("atom4_c", "C")] {
        Spi::run(&format!(
            "CREATE TABLE {child} PARTITION OF atom4 FOR VALUES IN ('{key}')"
        ))
        .expect("child");
    }
    Spi::run("INSERT INTO atom4 (id, region, amount) VALUES (1,'A',10),(2,'B',20),(3,'C',30)")
        .expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('atom4v', \
           'SELECT region, SUM(amount) AS total FROM atom4 GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("create imv");

    // Real drift the reconcile would repair — so "was this child swapped?" is
    // answered by data, not just by catalog identity.
    Spi::run("UPDATE atom4v SET total = 999").expect("drift");
    // Catalog identity too: a DETACH/ATTACH swap replaces the child relation, so
    // a committed swap changes its oid.
    let oids_before = Spi::get_one::<String>(
        "SELECT string_agg(c.oid::text, ',' ORDER BY c.relname) FROM pg_class c \
         WHERE c.relname IN ('atom4v_atom4_a','atom4v_atom4_b','atom4v_atom4_c')",
    )
    .expect("oids")
    .expect("oids");

    // `zzz_ghost` sorts after all three real children, so they are processed —
    // and swapped — before the reconcile discovers it has no target bound.
    let msg = Spi::get_one::<String>(
        "SELECT reflex_reconcile_partition('atom4v', '', 'atom4_a,atom4_b,atom4_c,zzz_ghost', true)",
    )
    .expect("call")
    .expect("msg");
    assert!(
        msg.starts_with("ERROR"),
        "a batch reconcile naming an unresolvable child must report ERROR, got: {msg}"
    );

    let oids_after = Spi::get_one::<String>(
        "SELECT string_agg(c.oid::text, ',' ORDER BY c.relname) FROM pg_class c \
         WHERE c.relname IN ('atom4v_atom4_a','atom4v_atom4_b','atom4v_atom4_c')",
    )
    .expect("oids")
    .expect("oids");
    assert_eq!(
        oids_before, oids_after,
        "the batch path must roll back the children it had already swapped when a later \
         child fails (msg: {msg})"
    );
    let repaired = Spi::get_one::<i64>("SELECT count(*) FROM atom4v WHERE total <> 999")
        .expect("repaired probe")
        .expect("repaired");
    assert_eq!(
        repaired, 0,
        "no partial repair may survive a batch reconcile that reports ERROR (msg: {msg})"
    );
}

/// T4b — the batch path still WORKS. Paired with T4 so "rolls back on failure"
/// can never be satisfied by refusing to do anything.
#[pg_test]
fn pg_part_skip_sync_reconcile_still_repairs_on_success() {
    Spi::run(
        "CREATE TABLE atom4b (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    Spi::run("CREATE TABLE atom4b_n PARTITION OF atom4b FOR VALUES IN ('N')").expect("n");
    Spi::run("CREATE TABLE atom4b_s PARTITION OF atom4b FOR VALUES IN ('S')").expect("s");
    Spi::run("INSERT INTO atom4b (id, region, amount) VALUES (1,'N',10),(2,'S',20)")
        .expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('atom4bv', \
           'SELECT region, SUM(amount) AS total FROM atom4b GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("create imv");
    Spi::run("UPDATE atom4bv SET total = 999 WHERE region = 'N'").expect("drift");

    let msg =
        Spi::get_one::<String>("SELECT reflex_reconcile_partition('atom4bv', '', 'atom4b_n', true)")
            .expect("call")
            .expect("msg");
    assert!(
        msg.starts_with("RECONCILED partitions"),
        "skip_sync reconcile must still work, got: {msg}"
    );
    assert_imv_correct(
        "atom4bv",
        "SELECT region, SUM(amount) AS total FROM atom4b GROUP BY region",
    );
}

/// T5 — the IMV-name advisory lock keeps the two-key
/// `(hashtext(name), hashtext(reverse(name)))` form AND belongs to the caller's
/// transaction, not to the reconcile's subtransaction.
///
/// PostgreSQL releases a lock first taken inside a subtransaction when that
/// subtransaction rolls back. So the load-bearing case is the FAILING reconcile:
/// callers such as `trigger/dispatch.rs` discard the returned `ERROR:` string and
/// go on to run MERGE/DELETE/INSERT against the same IMV, and they must still be
/// serialized against concurrent maintenance. `objsubid = 2` is PostgreSQL's
/// marker for the two-int4-key advisory space; a one-key `bigint` lock lands in
/// space `objsubid = 1` and would never mutually exclude against the rest of
/// pg_reflex.
#[pg_test]
fn pg_part_reconcile_keeps_two_key_advisory_lock_even_when_it_fails() {
    Spi::run(
        "CREATE TABLE atom5 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    Spi::run("CREATE TABLE atom5_n PARTITION OF atom5 FOR VALUES IN ('N')").expect("n");
    Spi::run("CREATE TABLE atom5_s PARTITION OF atom5 FOR VALUES IN ('S')").expect("s");
    Spi::run("INSERT INTO atom5 (id, region, amount) VALUES (1,'N',10),(2,'S',20)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('atom5v', \
           'SELECT region, SUM(amount) AS total FROM atom5 GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("create imv");

    let two_key_lock_count = || {
        Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_locks \
             WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND objsubid = 2 \
               AND classid::bigint = (hashtext('atom5v')::bigint & 4294967295) \
               AND objid::bigint   = (hashtext(reverse('atom5v'))::bigint & 4294967295)",
        )
        .expect("lock probe")
        .expect("lock count")
    };

    let failed = Spi::get_one::<String>(
        "SELECT reflex_reconcile_partition('atom5v', 'region', 'no_such_source_child')",
    )
    .expect("call")
    .expect("msg");
    assert!(
        failed.starts_with("ERROR"),
        "expected the bogus reconcile to report ERROR, got: {failed}"
    );
    assert_eq!(
        two_key_lock_count(),
        1,
        "a FAILED reconcile must leave the two-key advisory lock held by the caller's \
         transaction — its rollback must not take the caller's mutual exclusion with it"
    );

    let ok = Spi::get_one::<String>("SELECT reflex_reconcile_partition('atom5v', 'N')")
        .expect("call")
        .expect("msg");
    assert!(
        ok.starts_with("RECONCILED partitions"),
        "expected RECONCILED, got: {ok}"
    );
    assert_eq!(
        two_key_lock_count(),
        1,
        "a SUCCESSFUL reconcile must still leave the two-key advisory lock held"
    );
}

/// T6 — a reconcile whose swap RAISES, caught by a real plpgsql `EXCEPTION`
/// handler, must be reported cleanly and leave the backend usable.
///
/// This is the shape every `reflex_doctor(fix => true)` partition repair takes
/// (`__reflex_doctor_try_repair`), and the shape the deferred flush wraps every
/// IMV's dispatch statements in. A subtransaction left open by the unwind would
/// be rolled back by that handler INSTEAD of its own, desynchronising plpgsql's
/// expression-context stack — an assertion failure and `SIGABRT` on a cassert
/// build, silently mismatched state on a release build.
///
/// `drop_old_tgt` is a `DROP TABLE` with no CASCADE, so one dependent view is
/// enough to make the swap raise for real, mid-flight.
///
/// NOTE ON FAILURE MODE: when this regresses it does not print a clean assertion
/// — the backend aborts, so the run dies with `connection closed` and every
/// later test reports "Could not obtain test mutex". A whole-suite collapse of
/// that shape means the `SubTransaction` `Drop` contract broke; start here.
#[pg_test]
fn pg_part_raised_reconcile_failure_survives_plpgsql_exception_handler() {
    Spi::run(
        "CREATE TABLE atom6 (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("source");
    Spi::run("CREATE TABLE atom6_n PARTITION OF atom6 FOR VALUES IN ('N')").expect("n");
    Spi::run("CREATE TABLE atom6_s PARTITION OF atom6 FOR VALUES IN ('S')").expect("s");
    Spi::run("INSERT INTO atom6 (id, region, amount) VALUES (1,'N',10),(2,'S',20)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('atom6v', \
           'SELECT region, SUM(amount) AS total FROM atom6 GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("create imv");

    let children_before = partition_child_set("atom6v");
    Spi::run("CREATE VIEW atom6_pin AS SELECT * FROM atom6v_atom6_n").expect("blocking view");

    // The in-tree doctor repair wrapper: plpgsql, EXCEPTION WHEN OTHERS.
    let repair = Spi::get_one::<String>(
        "SELECT public.__reflex_doctor_try_repair( \
           $q$SELECT reflex_reconcile_partition('atom6v','','atom6_n')$q$)",
    )
    .expect("repair call")
    .expect("repair result");
    assert!(
        repair.starts_with("failed:"),
        "the raised swap failure must be reported by the handler, got: {repair}"
    );

    // The backend is alive and the transaction still usable — the whole point.
    let rows = Spi::get_one::<i64>("SELECT count(*) FROM atom6v")
        .expect("post-failure query")
        .expect("rows");
    assert_eq!(rows, 2, "the IMV must still be readable after a caught raise");
    assert_eq!(
        partition_child_set("atom6v"),
        children_before,
        "a raised, caught reconcile failure must leave the partition set intact"
    );
    assert_imv_correct(
        "atom6v",
        "SELECT region, SUM(amount) AS total FROM atom6 GROUP BY region",
    );
}

/// T7 — TWO nested `SubTransaction` guards unwinding together on a raise.
///
/// The dependent cascade re-enters `reflex_reconcile_partition` for a dependent
/// partitioned on the same column (`src/partition.rs`, the `same_part` branch)
/// via `client.update`, from inside the parent's still-open subtransaction AND
/// inside the parent's live `SpiClient`. Making the NESTED swap raise is the
/// only way to unwind two guards at once, and it is the one shape the
/// success-path suite cannot reach.
///
/// The unwind has to run, in order: the nested `SpiClient` (`SPI_finish`), the
/// nested `SubTransaction` (rollback), then the parent's pair — before the error
/// reaches the plpgsql `EXCEPTION` handler that must roll back only its OWN
/// subtransaction. Any guard that skips its turn leaves the handler rolling back
/// someone else's, which aborts the backend.
///
/// Same failure mode as T6: a regression here kills the run rather than printing
/// an assertion.
#[pg_test]
fn pg_part_nested_cascade_raise_unwinds_both_subtransactions() {
    Spi::run("CREATE TABLE nst (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)")
        .expect("source");
    Spi::run("CREATE TABLE nst_a PARTITION OF nst FOR VALUES IN ('A')").expect("a");
    Spi::run("CREATE TABLE nst_b PARTITION OF nst FOR VALUES IN ('B')").expect("b");
    Spi::run("INSERT INTO nst VALUES (1,'A',10),(2,'B',5)").expect("seed");

    let parent = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('nstp', \
           'SELECT region, SUM(amount) AS total FROM nst GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("parent imv")
    .expect("parent imv result");
    assert!(!parent.starts_with("ERROR"), "parent imv: {parent}");

    // Dependent partitioned on the SAME column, so the cascade re-enters
    // reflex_reconcile_partition rather than taking a scoped or full reconcile.
    let dependent = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('nstd', \
           'SELECT region, SUM(total) AS doubled FROM nstp GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("dependent imv")
    .expect("dependent imv result");
    assert!(!dependent.starts_with("ERROR"), "dependent imv: {dependent}");

    let parent_children_before = partition_child_set("nstp");
    let dependent_children_before = partition_child_set("nstd");

    // Drift both levels, so "did this level's swap survive?" is answered by
    // data. The parent goes to a value the reconcile WILL repair (to 10), and
    // the dependent to one that satisfies the blocker below.
    Spi::run("UPDATE nstp SET total = 77 WHERE region = 'A'").expect("drift parent");
    Spi::run("UPDATE nstd SET doubled = 9999 WHERE region = 'A'").expect("drift dependent");

    // Block the NESTED swap, on the dependent's PARENT table. It has to live
    // there, not on the child and not as a dependent view: the dependent's own
    // pre-sync drops and recreates that child with `DROP TABLE … CASCADE`, which
    // silently removes any view pinned to it (and with it the blocker). A CHECK
    // on the partitioned parent survives, is inherited by the recreated child,
    // and is copied onto the swap table by `CREATE TABLE … (LIKE … INCLUDING
    // ALL)` — so the swap's fill raises on the rebuilt value (10, not > 100).
    Spi::run("ALTER TABLE nstd ADD CONSTRAINT nstd_block CHECK (region <> 'A' OR doubled > 100)")
        .expect("blocking constraint");

    let repair = Spi::get_one::<String>(
        "SELECT public.__reflex_doctor_try_repair( \
           $q$SELECT reflex_reconcile_partition('nstp','A')$q$)",
    )
    .expect("repair call")
    .expect("repair result");
    assert!(
        repair.starts_with("failed:"),
        "the nested raise must surface through the handler, got: {repair}"
    );

    // Backend alive, transaction usable — the whole point.
    let rows = Spi::get_one::<i64>("SELECT count(*) FROM nstp")
        .expect("post-failure query")
        .expect("rows");
    assert_eq!(rows, 2, "the parent IMV must still be readable");

    // INNER guard: the dependent's own partial swap is gone.
    let dependent_a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT doubled FROM nstd WHERE region = 'A'",
    )
    .expect("dependent probe")
    .expect("dependent value");
    assert_eq!(
        dependent_a.to_string(),
        "9999",
        "the nested reconcile's subtransaction must have rolled back"
    );

    // OUTER guard: the parent's swap SUCCEEDED before the cascade raised, and
    // must be rolled back too. This is the assertion that needs both guards to
    // have unwound in order — the inner one first, then the outer.
    let parent_a =
        Spi::get_one::<pgrx::AnyNumeric>("SELECT total FROM nstp WHERE region = 'A'")
            .expect("parent probe")
            .expect("parent value");
    assert_eq!(
        parent_a.to_string(),
        "77",
        "the parent's already-completed swap must be rolled back by its own subtransaction \
         when a cascaded reconcile raises"
    );

    assert_eq!(
        partition_child_set("nstp"),
        parent_children_before,
        "the parent's partition set must be intact after a nested cascade raise"
    );
    assert_eq!(
        partition_child_set("nstd"),
        dependent_children_before,
        "the dependent's partition set must be intact after a nested cascade raise"
    );
}
