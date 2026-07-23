
#[pg_test]
fn test_truncate_clears_imv() {
    Spi::run("CREATE TABLE trunc_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO trunc_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .expect("seed");

    crate::create_reflex_ivm(
        "trunc_view",
        "SELECT grp, SUM(val) AS total FROM trunc_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Verify data exists
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM trunc_view")
        .expect("q").expect("v");
    assert_eq!(count, 2);

    // TRUNCATE source → IMV should be empty
    Spi::run("TRUNCATE trunc_src").expect("truncate");

    let count_after = Spi::get_one::<i64>("SELECT COUNT(*) FROM trunc_view")
        .expect("q").expect("v");
    assert_eq!(count_after, 0);

    // Intermediate should also be empty
    let int_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM __reflex_intermediate_trunc_view",
    ).expect("q").expect("v");
    assert_eq!(int_count, 0);
}

#[pg_test]
fn test_truncate_then_reinsert() {
    Spi::run("CREATE TABLE trunc2_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO trunc2_src (grp, val) VALUES ('x', 100)")
        .expect("seed");

    crate::create_reflex_ivm(
        "trunc2_view",
        "SELECT grp, SUM(val) AS total FROM trunc2_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    Spi::run("TRUNCATE trunc2_src").expect("truncate");
    Spi::run("INSERT INTO trunc2_src (grp, val) VALUES ('y', 500)")
        .expect("reinsert");

    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM trunc2_view")
        .expect("q").expect("v");
    assert_eq!(count, 1);

    let y_total = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM trunc2_view WHERE grp = 'y'",
    ).expect("q").expect("v");
    assert_eq!(y_total.to_string(), "500");
}

#[pg_test]
fn test_reconcile_fixes_drift() {
    Spi::run("CREATE TABLE recon_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO recon_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .expect("seed");

    crate::create_reflex_ivm(
        "recon_view",
        "SELECT grp, SUM(val) AS total FROM recon_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Corrupt the intermediate table by zeroing out a value
    Spi::run("UPDATE __reflex_intermediate_recon_view SET \"__sum_val\" = 0 WHERE grp = 'a'")
        .expect("corrupt");

    // Target is now stale — verify corruption propagated
    // (target reflects intermediate, not source)

    // Reconcile should fix it
    let result = crate::reflex_reconcile("recon_view");
    assert_eq!(result, "RECONCILED");

    // Verify data matches expected
    let a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM recon_view WHERE grp = 'a'",
    ).expect("q").expect("v");
    assert_eq!(a.to_string(), "10");

    let b = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM recon_view WHERE grp = 'b'",
    ).expect("q").expect("v");
    assert_eq!(b.to_string(), "20");
}

#[pg_test]
fn test_reconcile_passthrough() {
    Spi::run("CREATE TABLE recon_pt_src (id SERIAL, name TEXT)")
        .expect("create table");
    Spi::run("INSERT INTO recon_pt_src (name) VALUES ('Alice'), ('Bob')")
        .expect("seed");

    crate::create_reflex_ivm("recon_pt_view", "SELECT id, name FROM recon_pt_src", None, None, None, None);

    // Manually delete a row from target (corrupt)
    Spi::run("DELETE FROM recon_pt_view WHERE name = 'Alice'").expect("corrupt");
    let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM recon_pt_view")
        .expect("q").expect("v");
    assert_eq!(count, 1);

    // Reconcile should restore
    let result = crate::reflex_reconcile("recon_pt_view");
    assert_eq!(result, "RECONCILED");

    let count_after = Spi::get_one::<i64>("SELECT COUNT(*) FROM recon_pt_view")
        .expect("q").expect("v");
    assert_eq!(count_after, 2);
}

#[pg_test]
fn test_reconcile_aggregate() {
    Spi::run(
        "CREATE TABLE recon_agg_src (id SERIAL, grp TEXT, val NUMERIC)",
    )
    .expect("create table");
    Spi::run(
        "INSERT INTO recon_agg_src (grp, val) VALUES ('A', 10), ('A', 20), ('B', 30)",
    )
    .expect("seed");
    crate::create_reflex_ivm(
        "recon_agg_view",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM recon_agg_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    // Corrupt intermediate table
    Spi::run(
        "UPDATE __reflex_intermediate_recon_agg_view SET \"__sum_val\" = 999 WHERE \"grp\" = 'A'",
    )
    .expect("corrupt intermediate");
    // Reconcile should fix it
    let result = crate::reflex_reconcile("recon_agg_view");
    assert_eq!(result, "RECONCILED");
    let a = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM recon_agg_view WHERE grp = 'A'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a.to_string(), "30", "After reconcile, SUM should be 10+20=30");
}

#[pg_test]
fn test_refresh_imv_depending_on() {
    Spi::run("CREATE TABLE rdep_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)")
        .expect("create table");
    Spi::run("INSERT INTO rdep_src (grp, val) VALUES ('X', 10), ('Y', 20)")
        .expect("seed");

    // Create two IMVs on the same source
    crate::create_reflex_ivm(
        "rdep_v1",
        "SELECT grp, SUM(val) AS total FROM rdep_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    crate::create_reflex_ivm(
        "rdep_v2",
        "SELECT grp, COUNT(*) AS cnt FROM rdep_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Corrupt both by directly modifying intermediate tables
    Spi::run("UPDATE __reflex_intermediate_rdep_v1 SET \"__sum_val\" = 999 WHERE \"grp\" = 'X'")
        .expect("corrupt v1");
    Spi::run("UPDATE __reflex_intermediate_rdep_v2 SET \"__count_star\" = 999 WHERE \"grp\" = 'X'")
        .expect("corrupt v2");

    // Refresh all IMVs depending on rdep_src
    let result = crate::refresh_imv_depending_on("rdep_src");
    assert!(result.contains("2"), "Should refresh 2 IMVs, got: {}", result);

    // Verify both are fixed
    let v1 = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM rdep_v1 WHERE grp = 'X'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(v1.to_string(), "10", "v1 should be fixed after refresh");

    let v2 = Spi::get_one::<i64>(
        "SELECT cnt FROM rdep_v2 WHERE grp = 'X'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(v2, 1, "v2 should be fixed after refresh");
}

#[pg_test]
fn test_scheduled_reconcile_runs_stale_imvs() {
    Spi::run("CREATE TABLE sched_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO sched_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .expect("seed");

    crate::create_reflex_ivm(
        "sched_v1",
        "SELECT grp, SUM(val) AS total FROM sched_src GROUP BY grp",
        None, None, None,
        None,
    );
    crate::create_reflex_ivm(
        "sched_v2",
        "SELECT grp, COUNT(*) AS cnt FROM sched_src GROUP BY grp",
        None, None, None,
        None,
    );

    // Force every registry row to look "stale": null out last_update_date.
    Spi::run("UPDATE public.__reflex_ivm_reference SET last_update_date = NULL WHERE name LIKE 'sched_%'")
        .expect("null last_update");

    // Corrupt the intermediate of v1 to verify the scheduled reconcile actually rebuilds.
    Spi::run("UPDATE __reflex_intermediate_sched_v1 SET \"__sum_val\" = 999 WHERE \"grp\" = 'a'")
        .expect("corrupt intermediate");
    Spi::run("UPDATE sched_v1 SET total = 999 WHERE grp = 'a'")
        .expect("corrupt target");

    let reconciled: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM reflex_scheduled_reconcile(0) WHERE name LIKE 'sched_%' AND status = 'RECONCILED'",
    ).expect("q").expect("v");
    assert!(reconciled >= 2, "both stale IMVs should reconcile, got {}", reconciled);

    let v1_total: pgrx::AnyNumeric = Spi::get_one(
        "SELECT total FROM sched_v1 WHERE grp = 'a'",
    ).expect("q").expect("v");
    assert_eq!(v1_total.to_string(), "10", "v1 should be reconciled back to source value");
}

#[pg_test]
fn test_scheduled_reconcile_skips_fresh_imvs() {
    Spi::run("CREATE TABLE sched_fresh_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO sched_fresh_src (grp, val) VALUES ('x', 1)").expect("seed");

    crate::create_reflex_ivm(
        "sched_fresh_view",
        "SELECT grp, SUM(val) AS total FROM sched_fresh_src GROUP BY grp",
        None, None, None,
        None,
    );

    // last_update_date is set to now() at IMV creation. With a 60-minute
    // threshold, the row should be considered fresh and skipped.
    let scanned: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM reflex_scheduled_reconcile(60) WHERE name = 'sched_fresh_view'",
    ).expect("q").expect("v");
    assert_eq!(scanned, 0, "fresh IMV should not be reconciled");
}

/// PS-7 gap 4 — every IMV reconciled in one scheduled batch must get a DISTINCT
/// `last_update_date`. Before the fix `reconcile_one` stamped `NOW()`
/// (transaction start), so an entire batch shared one identical timestamp
/// (field-observed across four schemas at once) and the column could not date an
/// individual rebuild. `clock_timestamp()` stamps wall-clock at statement time,
/// so two IMVs reconciled back-to-back differ.
#[pg_test]
fn pg_scheduled_reconcile_stamps_distinct_timestamps_per_imv() {
    Spi::run("CREATE TABLE ps7_ts_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO ps7_ts_src (grp, val) VALUES ('a', 10), ('b', 20)").expect("seed");

    crate::create_reflex_ivm(
        "ps7_ts_v1",
        "SELECT grp, SUM(val) AS total FROM ps7_ts_src GROUP BY grp",
        None, None, None, None,
    );
    crate::create_reflex_ivm(
        "ps7_ts_v2",
        "SELECT grp, COUNT(*) AS cnt FROM ps7_ts_src GROUP BY grp",
        None, None, None, None,
    );

    // Backdate so both are candidates (CURRENT_TIMESTAMP is transaction start
    // inside a test, so a row touched this transaction never looks stale).
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET last_update_date = TIMESTAMP '2001-01-01 00:00:00' \
          WHERE name LIKE 'ps7_ts_%'",
    )
    .expect("backdate");

    let reconciled: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM reflex_scheduled_reconcile(0) \
          WHERE name LIKE 'ps7_ts_%' AND status = 'RECONCILED'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(reconciled, 2, "both IMVs should reconcile");

    let distinct_timestamps: i64 = Spi::get_one(
        "SELECT COUNT(DISTINCT last_update_date)::BIGINT \
           FROM public.__reflex_ivm_reference WHERE name LIKE 'ps7_ts_%'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        distinct_timestamps, 2,
        "each IMV in the batch must get its own last_update_date (clock_timestamp), \
         not one shared NOW() for the whole transaction"
    );
}

/// PS-7 gaps 1-2 — `batch_size` makes `reflex_scheduled_reconcile` a resumable
/// driver: a bounded call attempts at most `batch_size` IMVs, reports how many
/// candidates it deferred via `remaining`, and a second call CONTINUES with the
/// next IMV rather than restarting. Resumability falls out of the age gate — a
/// reconciled IMV's clock_timestamp() write lands after `CURRENT_TIMESTAMP -
/// max_age`, so it drops out of the candidate set on the next call.
///
/// The gate MUST be positive here. With `max_age = 0` this test would be a
/// false-green: `#[pg_test]` runs in one transaction, so `CURRENT_TIMESTAMP` is
/// frozen at transaction start while `clock_timestamp()` advances past it —
/// making the reconciled row drop out for a reason that does NOT hold in
/// production (where each call is its own transaction with a later
/// `CURRENT_TIMESTAMP`). Backdating to 2001 and gating on `max_age = 60` makes
/// the drop-out genuine: a just-reconciled (~now) row is never older than
/// `CURRENT_TIMESTAMP - 60min` whether the clock is frozen or not.
#[pg_test]
fn pg_scheduled_reconcile_batch_size_is_resumable() {
    Spi::run("CREATE TABLE ps7_rz_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO ps7_rz_src (grp, val) VALUES ('a', 1), ('b', 2)").expect("seed");

    crate::create_reflex_ivm(
        "ps7_rz_a",
        "SELECT grp, SUM(val) AS total FROM ps7_rz_src GROUP BY grp",
        None, None, None, None,
    );
    crate::create_reflex_ivm(
        "ps7_rz_b",
        "SELECT grp, COUNT(*) AS cnt FROM ps7_rz_src GROUP BY grp",
        None, None, None, None,
    );

    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET last_update_date = TIMESTAMP '2001-01-01 00:00:00' \
          WHERE name LIKE 'ps7_rz_%'",
    )
    .expect("backdate");

    // First bounded call: attempt exactly one, defer the other.
    Spi::run(
        "CREATE TEMP TABLE ps7_rz_call1 AS \
           SELECT * FROM reflex_scheduled_reconcile(60, 1)",
    )
    .expect("call1");

    let attempted1: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM ps7_rz_call1 WHERE name LIKE 'ps7_rz_%'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(attempted1, 1, "batch_size=1 attempts exactly one IMV");

    let remaining1: i64 =
        Spi::get_one("SELECT MAX(remaining) FROM ps7_rz_call1").expect("q").expect("v");
    assert_eq!(remaining1, 1, "one candidate must be reported as deferred to a future call");

    let first_name: String =
        Spi::get_one("SELECT name FROM ps7_rz_call1 WHERE name LIKE 'ps7_rz_%'")
            .expect("q")
            .expect("v");
    assert_eq!(
        first_name, "ps7_rz_a",
        "with equal last_update_date the name tiebreaker orders ps7_rz_a first"
    );

    // Second call continues — the reconciled first IMV is now fresh (~now) and
    // is no longer older than CURRENT_TIMESTAMP - 60min, so it drops out.
    Spi::run(
        "CREATE TEMP TABLE ps7_rz_call2 AS \
           SELECT * FROM reflex_scheduled_reconcile(60, 1)",
    )
    .expect("call2");

    let second_name: String =
        Spi::get_one("SELECT name FROM ps7_rz_call2 WHERE name LIKE 'ps7_rz_%'")
            .expect("q")
            .expect("v");
    assert_eq!(
        second_name, "ps7_rz_b",
        "second call must continue with the next IMV, not restart on the first"
    );

    let remaining2: i64 =
        Spi::get_one("SELECT MAX(remaining) FROM ps7_rz_call2").expect("q").expect("v");
    assert_eq!(remaining2, 0, "sweep complete after the second call");
}

/// PS-7 gap 3 — `target_schema` scopes the scan to one tenant. An IMV in another
/// schema must be untouched (its `last_update_date` unchanged), so single-tenant
/// recovery does not pay for all 415 schemas.
#[pg_test]
fn pg_scheduled_reconcile_target_schema_isolates_tenants() {
    Spi::run("CREATE SCHEMA ps7_sa").expect("schema a");
    Spi::run("CREATE SCHEMA ps7_sb").expect("schema b");
    Spi::run("CREATE TABLE ps7_sa.src (id SERIAL, grp TEXT, val NUMERIC)").expect("src a");
    Spi::run("CREATE TABLE ps7_sb.src (id SERIAL, grp TEXT, val NUMERIC)").expect("src b");
    Spi::run("INSERT INTO ps7_sa.src (grp, val) VALUES ('a', 1)").expect("seed a");
    Spi::run("INSERT INTO ps7_sb.src (grp, val) VALUES ('a', 1)").expect("seed b");

    crate::create_reflex_ivm(
        "ps7_sa.v",
        "SELECT grp, SUM(val) AS total FROM ps7_sa.src GROUP BY grp",
        None, None, None, None,
    );
    crate::create_reflex_ivm(
        "ps7_sb.v",
        "SELECT grp, SUM(val) AS total FROM ps7_sb.src GROUP BY grp",
        None, None, None, None,
    );

    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET last_update_date = TIMESTAMP '2001-01-01 00:00:00' \
          WHERE target_schema IN ('ps7_sa', 'ps7_sb')",
    )
    .expect("backdate");

    // Reconcile only schema a.
    let attempted: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM reflex_scheduled_reconcile(0, 0, 'ps7_sa') \
          WHERE status = 'RECONCILED'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(attempted, 1, "only schema a's IMV should be reconciled");

    let a_fresh: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM public.__reflex_ivm_reference \
          WHERE target_schema = 'ps7_sa' \
            AND last_update_date > TIMESTAMP '2001-01-01 00:00:00'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(a_fresh, 1, "schema a IMV must be reconciled (timestamp advanced)");

    let b_untouched: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM public.__reflex_ivm_reference \
          WHERE target_schema = 'ps7_sb' \
            AND last_update_date = TIMESTAMP '2001-01-01 00:00:00'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        b_untouched, 1,
        "schema b IMV must be untouched by a schema-a-scoped scan"
    );
}

/// PS-7 gap 1 — durability boundary the batched-driver model provides IN-PROCESS:
/// a per-IMV SOFT failure does not halt the batch or discard the siblings'
/// rebuilds, and the failing candidate is reported HONESTLY (PS-1 error
/// propagation is not swallowed). Cross-call COMMIT durability (a bounded batch
/// that finishes under statement_timeout persists across pg_cron ticks) is a
/// deployment property of "each call is its own transaction" and cannot be
/// exercised by the transaction-wrapped test harness.
///
/// The failure is injected as a registry row whose name `validate_view_name`
/// rejects — the only in-transaction soft-error injection that does not abort the
/// whole transaction (a broken real IMV hard-errors via unwrap_or_report). It
/// exercises the same driver loop that real soft failures (partition-swap Err,
/// PS-1 generated-child failure) travel through. The bad name sorts FIRST
/// (ASCII '-' < '_'), so the two real IMVs are reconciled AFTER the failure.
#[pg_test]
fn pg_scheduled_reconcile_soft_failure_does_not_stop_the_batch() {
    Spi::run("CREATE TABLE ps7_du_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO ps7_du_src (grp, val) VALUES ('a', 1), ('b', 2)").expect("seed");

    crate::create_reflex_ivm(
        "ps7_du_a",
        "SELECT grp, SUM(val) AS total FROM ps7_du_src GROUP BY grp",
        None, None, None, None,
    );
    crate::create_reflex_ivm(
        "ps7_du_b",
        "SELECT grp, COUNT(*) AS cnt FROM ps7_du_src GROUP BY grp",
        None, None, None, None,
    );

    // Inject a candidate whose name validate_view_name rejects (contains '-').
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference (name, graph_depth, last_update_date) \
         VALUES ('ps7-du-bad', 1, TIMESTAMP '2001-01-01 00:00:00')",
    )
    .expect("inject bad candidate");

    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET last_update_date = TIMESTAMP '2001-01-01 00:00:00' \
          WHERE name IN ('ps7_du_a', 'ps7_du_b')",
    )
    .expect("backdate");

    Spi::run(
        "CREATE TEMP TABLE ps7_du_call AS SELECT * FROM reflex_scheduled_reconcile(0)",
    )
    .expect("call");

    let bad_reported: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM ps7_du_call \
          WHERE name = 'ps7-du-bad' AND status LIKE 'ERROR%'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(bad_reported, 1, "the failing candidate must be reported with its ERROR status");

    let siblings_ok: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM ps7_du_call \
          WHERE name IN ('ps7_du_a', 'ps7_du_b') AND status = 'RECONCILED'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        siblings_ok, 2,
        "both real IMVs must reconcile despite the earlier soft failure in the batch"
    );

    let siblings_fresh: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM public.__reflex_ivm_reference \
          WHERE name IN ('ps7_du_a', 'ps7_du_b') \
            AND last_update_date > TIMESTAMP '2001-01-01 00:00:00'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(siblings_fresh, 2, "siblings' rebuilds must survive the failure (timestamps advanced)");
}

/// PS-7 regression — default-arg pg_cron usage is behaviour-preserving:
/// `reflex_scheduled_reconcile(max_age)` alone (batch_size defaults to 0 =
/// unlimited, target_schema defaults to '' = all schemas) still reconciles every
/// stale IMV in ONE call and reports `remaining = 0`, so existing monitoring that
/// reads a single successful call as "sweep complete" stays correct.
#[pg_test]
fn pg_scheduled_reconcile_default_args_are_behaviour_preserving() {
    Spi::run("CREATE TABLE ps7_dflt_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO ps7_dflt_src (grp, val) VALUES ('a', 1), ('b', 2)").expect("seed");

    crate::create_reflex_ivm(
        "ps7_dflt_a",
        "SELECT grp, SUM(val) AS total FROM ps7_dflt_src GROUP BY grp",
        None, None, None, None,
    );
    crate::create_reflex_ivm(
        "ps7_dflt_b",
        "SELECT grp, COUNT(*) AS cnt FROM ps7_dflt_src GROUP BY grp",
        None, None, None, None,
    );

    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET last_update_date = TIMESTAMP '2001-01-01 00:00:00' \
          WHERE name LIKE 'ps7_dflt_%'",
    )
    .expect("backdate");

    Spi::run(
        "CREATE TEMP TABLE ps7_dflt_call AS SELECT * FROM reflex_scheduled_reconcile(0)",
    )
    .expect("call");

    let reconciled: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM ps7_dflt_call \
          WHERE name LIKE 'ps7_dflt_%' AND status = 'RECONCILED'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(reconciled, 2, "default args must reconcile all stale IMVs in one call");

    let remaining: i64 =
        Spi::get_one("SELECT MAX(remaining) FROM ps7_dflt_call").expect("q").expect("v");
    assert_eq!(remaining, 0, "unlimited default batch reports 0 remaining = sweep complete");
}

/// F5 diagnosis: reflex_rebuild_imv on a partitioned IMV whose authoritative
/// source is in ignore_sources. This tests whether the partition stays empty
/// (F6 interaction) or fills (unexpected skip-existing-children bug).
///
/// Repro scenario:
/// 1. Create a partitioned anchor source A
/// 2. Create an authoritative source B (non-partitioned)
/// 3. Create a partitioned IMV joining A and B, partitioned by A's key,
///    with B in ignore_sources (B not incrementally maintained)
/// 4. Populate B for a specific key while the IMV partition for that key is
///    left empty (bypass normal maintenance)
/// 5. Call reflex_rebuild_imv and observe whether the partition fills
///
/// Expected (Branch A, F6 interaction): Partition stays empty because B is the
/// authoritative source but is in ignore_sources, so rebuild anchors on A and
/// never re-derives keys fed only by B.
#[pg_test]
fn f5_rebuild_imv_ignore_sources_anchor_repro() {
    // 1. Create a partitioned anchor source (A) partitioned by region
    Spi::run(
        "CREATE TABLE f5_anchor (id BIGINT NOT NULL, region TEXT NOT NULL, val INT) \
         PARTITION BY LIST (region)"
    ).expect("create anchor");

    Spi::run(
        "CREATE TABLE f5_anchor_n PARTITION OF f5_anchor FOR VALUES IN ('NORTH')"
    ).expect("create anchor north");

    Spi::run(
        "CREATE TABLE f5_anchor_s PARTITION OF f5_anchor FOR VALUES IN ('SOUTH')"
    ).expect("create anchor south");

    // 2. Create an authoritative source B (non-partitioned)
    Spi::run(
        "CREATE TABLE f5_auth (auth_id BIGINT NOT NULL, region TEXT NOT NULL, auth_val INT)"
    ).expect("create auth");

    // 3. Seed anchor with one region, but auth with both
    Spi::run(
        "INSERT INTO f5_anchor (id, region, val) VALUES (1, 'NORTH', 100)"
    ).expect("seed anchor");

    Spi::run(
        "INSERT INTO f5_auth (auth_id, region, auth_val) VALUES \
         (10, 'NORTH', 1000), \
         (20, 'SOUTH', 2000)"
    ).expect("seed auth");

    // 4. Create a partitioned IMV joining A and B, partitioned by region,
    //    with B in ignore_sources (so B is NOT incrementally maintained)
    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'f5_view', \
            'SELECT a.region, COUNT(*) AS cnt FROM f5_anchor a JOIN f5_auth b ON a.region = b.region GROUP BY a.region', \
            NULL, NULL, NULL, 'f5_auth', \
            ARRAY['region'] \
         )"
    ).expect("create IMV call").expect("create IMV result");

    assert!(
        !create_result.starts_with("ERROR"),
        "create IMV failed: {create_result}"
    );

    // 5. Verify that partition for SOUTH is empty (because anchor has no SOUTH rows)
    let south_count_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f5_view WHERE region = 'SOUTH'"
    ).expect("query before").expect("count before");
    assert_eq!(south_count_before, 0, "SOUTH partition should start empty");

    // 6. Call reflex_rebuild_imv and capture result
    let rebuild_result = Spi::get_one::<String>(
        "SELECT reflex_rebuild_imv('f5_view')"
    ).expect("rebuild call").expect("rebuild result");

    pgrx::notice!("pg_test: rebuild returned: {}", rebuild_result);

    // 7. Check if SOUTH partition still empty or got filled
    //    EXPECTED (Branch A): stays empty because f5_auth is in ignore_sources,
    //    so rebuild only touches anchor's children (NORTH), never fetches
    //    new rows from f5_auth for SOUTH.
    let south_count_after = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f5_view WHERE region = 'SOUTH'"
    ).expect("query after").expect("count after");

    // Assert the OBSERVED behavior: partition stays empty
    // This is the durable regression lock documenting F6 interaction.
    assert_eq!(
        south_count_after, 0,
        "F5 diagnosis: SOUTH partition stayed empty after rebuild (F6 interaction: \
         ignore_sources source is not the reconcile anchor, so rebuild never re-derives \
         keys fed only by that source)"
    );
}
