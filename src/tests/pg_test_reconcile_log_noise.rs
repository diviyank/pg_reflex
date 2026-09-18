// `reflex_reconcile` output noise (untreated_bugs/2026-09-18_reconcile_log_noise.md).
//
// Only lines that need action should reach the client. A test event trigger
// records every `CREATE TABLE` that created nothing: a `CREATE TABLE IF NOT
// EXISTS` of an existing relation reaches `ddl_command_end` with no collected
// command, and is what prints `relation … already exists, skipping`.

fn rln_record_skipped_creates() {
    lock_shared_fixtures();
    Spi::run("CREATE TABLE rln_ddl_log (tag TEXT)").expect("ddl log");
    Spi::run(
        "CREATE FUNCTION rln_log_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$ \
         BEGIN \
           IF NOT EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands()) THEN \
             INSERT INTO public.rln_ddl_log VALUES (tg_tag); \
           END IF; \
         END $$",
    )
    .expect("ddl log function");
    Spi::run(
        "CREATE EVENT TRIGGER rln_log_ddl ON ddl_command_end \
         WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION rln_log_ddl()",
    )
    .expect("ddl log trigger");
}

fn rln_skipped_creates() -> i64 {
    Spi::get_one::<i64>("SELECT count(*)::int8 FROM rln_ddl_log")
        .expect("ddl log count")
        .unwrap_or(-1)
}

const RLN_PARENT_SQL: &str = "SELECT k, bucket, SUM(amt) AS total FROM rln_s GROUP BY k, bucket";
const RLN_DEPENDENT_SQL: &str = "SELECT k, SUM(total) AS t FROM rln_p GROUP BY k";

/// A partitioned aggregate IMV `rln_p` over three leaves, read by `rln_d`.
fn rln_partitioned_with_dependent() {
    lock_shared_fixtures();
    Spi::run(
        "CREATE TABLE rln_s (k TEXT NOT NULL, bucket INT NOT NULL, amt NUMERIC) \
         PARTITION BY LIST (k)",
    )
    .expect("source");
    for (leaf, key) in [("rln_s_a", "A"), ("rln_s_b", "B"), ("rln_s_c", "C")] {
        Spi::run(&format!(
            "CREATE TABLE {leaf} PARTITION OF rln_s FOR VALUES IN ('{key}')"
        ))
        .expect("leaf");
    }
    Spi::run(
        "INSERT INTO rln_s SELECT v.k, (g % 5), (g % 97)::numeric \
           FROM generate_series(1, 300) g CROSS JOIN (VALUES ('A'), ('B'), ('C')) v(k)",
    )
    .expect("seed");
    let parent = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('rln_p', '{}', NULL, NULL, NULL, NULL, ARRAY['k'])",
        RLN_PARENT_SQL
    ))
    .expect("create parent")
    .unwrap_or_default();
    assert!(!parent.starts_with("ERROR"), "parent create returned: {parent}");
    let dependent = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('rln_d', '{}')",
        RLN_DEPENDENT_SQL
    ))
    .expect("create dependent")
    .unwrap_or_default();
    assert!(!dependent.starts_with("ERROR"), "dependent create returned: {dependent}");
}

/// Item 1: a reconcile of an up-to-date partition tree must not re-issue a
/// `CREATE TABLE IF NOT EXISTS` per existing child.
#[pg_test]
fn rln_reconcile_of_a_synced_tree_creates_no_partition() {
    rln_partitioned_with_dependent();
    rln_record_skipped_creates();

    let result = Spi::get_one::<String>("SELECT reflex_reconcile('rln_p')")
        .expect("reconcile")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "reconcile returned: {result}");
    assert_eq!(
        Spi::get_one::<i64>(
            "SELECT count(*)::int8 FROM pg_inherits WHERE inhparent = 'rln_p'::regclass"
        )
        .expect("children")
        .unwrap_or(-1),
        3,
        "precondition: the IMV keeps one child per source leaf"
    );
    assert_eq!(
        rln_skipped_creates(),
        0,
        "no CREATE TABLE of an existing child"
    );
    assert_imv_correct("rln_p", RLN_PARENT_SQL);
    assert_imv_correct("rln_d", RLN_DEPENDENT_SQL);
}

/// Item 1 control: a new source leaf still gets its IMV child.
#[pg_test]
fn rln_new_source_leaf_is_still_mirrored() {
    rln_partitioned_with_dependent();
    Spi::run("CREATE TABLE rln_s_d PARTITION OF rln_s FOR VALUES IN ('D')").expect("new leaf");
    Spi::run("INSERT INTO rln_s VALUES ('D', 1, 7)").expect("row in new leaf");
    let result = Spi::get_one::<String>("SELECT reflex_reconcile('rln_p')")
        .expect("reconcile")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "reconcile returned: {result}");
    assert_eq!(
        Spi::get_one::<i64>(
            "SELECT count(*)::int8 FROM pg_inherits WHERE inhparent = 'rln_p'::regclass"
        )
        .expect("children")
        .unwrap_or(-1),
        4,
        "the new leaf is mirrored"
    );
    assert_imv_correct("rln_p", RLN_PARENT_SQL);
}

/// Item 2: the cross-source guard's batch marker is created once per
/// transaction, not re-issued by every later flush of the batch.
#[pg_test]
fn rln_cross_source_marker_is_created_once_per_batch() {
    lock_shared_fixtures();
    Spi::run("CREATE TABLE rln_xa (id INT PRIMARY KEY, g INT, m NUMERIC)").unwrap();
    Spi::run("CREATE TABLE rln_xb (id INT PRIMARY KEY, g INT, w NUMERIC)").unwrap();
    Spi::run("INSERT INTO rln_xa VALUES (1,1,10),(2,1,20),(3,2,30)").unwrap();
    Spi::run("INSERT INTO rln_xb VALUES (1,1,100),(2,2,200)").unwrap();
    let sql = "SELECT rln_xa.g AS g, SUM(rln_xa.m) AS sm, SUM(rln_xb.w) AS sw \
               FROM rln_xa JOIN rln_xb ON rln_xb.g = rln_xa.g GROUP BY rln_xa.g";
    crate::create_reflex_ivm("rln_xj", sql, None, None, Some("DEFERRED"), None);
    rln_record_skipped_creates();

    Spi::run("INSERT INTO rln_xa VALUES (4,1,5)").unwrap();
    Spi::run("INSERT INTO rln_xb VALUES (3,1,50)").unwrap();
    Spi::run("SELECT reflex_flush_deferred('rln_xa')").expect("flush xa");
    Spi::run("SELECT reflex_flush_deferred('rln_xb')").expect("flush xb");
    assert_eq!(
        Spi::get_one::<bool>(
            "SELECT to_regclass('pg_temp.__reflex_deferred_reconciled_batch') IS NOT NULL"
        )
        .expect("marker probe")
        .unwrap_or(false),
        true,
        "precondition: the cross-source guard engaged"
    );
    assert_eq!(
        rln_skipped_creates(),
        0,
        "the batch marker is not re-created by a later flush"
    );
    assert_imv_correct("rln_xj", sql);
}

/// Item 3: the sync's own trigger toggle on the IMV root is not a source change,
/// so a dependent does not raise the alter alarm (here: its `error` policy).
#[pg_test]
fn rln_reconcile_with_a_dependent_passes_the_error_alter_policy() {
    rln_partitioned_with_dependent();
    Spi::run("SET LOCAL pg_reflex.alter_source_policy = 'error'").expect("policy");
    let result = Spi::get_one::<String>("SELECT reflex_reconcile('rln_p')")
        .expect("reconcile under the error policy")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "reconcile returned: {result}");
    assert_imv_correct("rln_p", RLN_PARENT_SQL);
    assert_imv_correct("rln_d", RLN_DEPENDENT_SQL);
}

/// Item 3 control: the suppression covers pg_reflex's own statement only. A
/// user's ALTER of the relation the sync just toggled, later in the same
/// transaction, still raises the alarm. A passthrough IMV has no intermediate
/// and its dependent is not partitioned (no nested auto-sync), so the IMV root
/// is the last relation the sync toggles.
#[pg_test(
    error = "pg_reflex: ALTER blocked by pg_reflex.alter_source_policy='error' on tracked source(s); affected: public.swi_imv -> rln_swi_dep"
)]
fn rln_user_alter_after_sync_still_alarms() {
    swi_build_fixture("validated");
    let dependent = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('rln_swi_dep', \
           'SELECT product_id, sum(qty) AS q FROM swi_imv GROUP BY product_id')",
    )
    .expect("create dependent")
    .unwrap_or_default();
    assert!(!dependent.starts_with("ERROR"), "dependent create returned: {dependent}");
    let synced = Spi::get_one::<String>("SELECT reflex_sync_partitions('swi_imv', FALSE)")
        .expect("sync")
        .unwrap_or_default();
    assert!(!synced.starts_with("ERROR"), "sync returned: {synced}");
    Spi::run("SET LOCAL pg_reflex.alter_source_policy = 'error'").expect("policy");
    Spi::run("ALTER TABLE swi_imv ALTER COLUMN qty SET STATISTICS 200").expect("user alter");
}

/// Item 1 guard: a sync that drops a bound-collision orphan also drops, by
/// CASCADE, children it had already seen; those must still be created. Here
/// DP 471 is repartitioned while the auto-sync is off, moving its February leaf
/// under a new list child, so the next sync drops the old mirror of 471 and
/// with it the mirror of the February leaf.
#[pg_test]
fn rln_child_dropped_with_an_orphan_is_recreated() {
    swi_build_fixture("validated");
    Spi::run("ALTER EVENT TRIGGER reflex_on_ddl_command_end DISABLE").expect("pause auto-sync");
    Spi::run("ALTER TABLE swi_ss DETACH PARTITION swi_ss_471").expect("detach 471");
    Spi::run("ALTER TABLE swi_ss_471 DETACH PARTITION swi_ss_471_feb").expect("detach feb");
    Spi::run(
        "CREATE TABLE swi_ss_471b PARTITION OF swi_ss FOR VALUES IN (471) \
         PARTITION BY RANGE (order_date)",
    )
    .expect("new 471 list child");
    Spi::run(
        "ALTER TABLE swi_ss_471b ATTACH PARTITION swi_ss_471_feb \
         FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
    )
    .expect("move feb under it");
    Spi::run("ALTER EVENT TRIGGER reflex_on_ddl_command_end ENABLE").expect("resume auto-sync");

    let synced = Spi::get_one::<String>("SELECT reflex_sync_partitions('swi_imv', FALSE)")
        .expect("sync")
        .unwrap_or_default();
    assert!(!synced.starts_with("ERROR"), "sync returned: {synced}");
    let feb_parent = Spi::get_one::<String>(
        "SELECT inhparent::regclass::text FROM pg_inherits \
          WHERE inhrelid = to_regclass('swi_imv_swi_ss_471_feb')",
    )
    .expect("feb mirror query");
    assert_eq!(
        feb_parent.as_deref(),
        Some("swi_imv_swi_ss_471b"),
        "the February mirror must exist under the new list child"
    );
    let result = Spi::get_one::<String>("SELECT reflex_reconcile('swi_imv')")
        .expect("reconcile")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "reconcile returned: {result}");
    assert_eq!(ish_swi_diverging_rows(), 0, "the IMV equals its query");
}

