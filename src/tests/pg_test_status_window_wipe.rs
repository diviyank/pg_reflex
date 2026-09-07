// STEP 0 FALSIFICATION — 2026-09 omc incident (sop_forecast_view / DP 471).
//
// The field report blames a non-atomic flush ("the DELETE stays applied when
// the INSERT fails"). The competing hypothesis under test here is that NO
// failure occurred at all: a partition-DDL-triggered rebuild re-evaluated the
// IMV's `WHERE dp.status IN (...)` while the DP sat in a status the list omits
// (creating_sop / finalizing / merging / refreshing_views), rebuilt the slice
// to zero rows, and COMMITTED successfully — with `demand_planning` in
// `ignore_sources`, nothing recomputes when the status returns to validated.
//
// Shape mirrors production: source partitioned LIST(dem_plan_id) -> RANGE
// (order_date), DEFERRED IMV partitioned on [dem_plan_id, order_date], the
// status-carrying table listed in ignore_sources, and a db-bus-style month
// swap (detached build -> DETACH/ATTACH/DROP/RENAME) as the DDL event.

fn swi_imv_rows() -> i64 {
    Spi::get_one::<i64>("SELECT count(*)::int8 FROM swi_imv")
        .unwrap()
        .unwrap()
}

fn swi_imv_rows_for(dp: i64) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM swi_imv WHERE dem_plan_id = {dp}"
    ))
    .unwrap()
    .unwrap()
}

fn swi_build_fixture(status: &str) {
    Spi::run("CREATE TABLE swi_dp (id BIGINT PRIMARY KEY, status TEXT NOT NULL)").expect("dp");
    Spi::run(&format!(
        "INSERT INTO swi_dp VALUES (471, '{status}'), (9, 'validated')"
    ))
    .expect("seed dp");

    Spi::run(
        "CREATE TABLE swi_ss (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, \
         product_id BIGINT NOT NULL, location_id BIGINT NOT NULL, qty INT NOT NULL) \
         PARTITION BY LIST (dem_plan_id)",
    )
    .expect("ss root");
    Spi::run(
        "CREATE TABLE swi_ss_471 PARTITION OF swi_ss FOR VALUES IN (471) \
         PARTITION BY RANGE (order_date)",
    )
    .expect("dp471 list child");
    Spi::run(
        "CREATE TABLE swi_ss_471_feb PARTITION OF swi_ss_471 \
         FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
    )
    .expect("feb leaf");
    Spi::run(
        "CREATE TABLE swi_ss_471_mar PARTITION OF swi_ss_471 \
         FOR VALUES FROM ('2026-03-01') TO ('2026-04-01')",
    )
    .expect("mar leaf");
    Spi::run(
        "CREATE TABLE swi_ss_9 PARTITION OF swi_ss FOR VALUES IN (9) \
         PARTITION BY RANGE (order_date)",
    )
    .expect("dp9 list child");
    Spi::run(
        "CREATE TABLE swi_ss_9_feb PARTITION OF swi_ss_9 \
         FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
    )
    .expect("dp9 feb leaf");

    Spi::run(
        "INSERT INTO swi_ss VALUES \
           (471, '2026-02-10', 1, 1, 10), \
           (471, '2026-02-20', 2, 1, 11), \
           (471, '2026-03-10', 1, 1, 20), \
           (9,   '2026-02-10', 1, 1, 5)",
    )
    .expect("seed ss");

    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('swi_imv', \
           'SELECT ss.dem_plan_id, ss.order_date, ss.product_id, ss.location_id, ss.qty \
              FROM swi_ss ss JOIN swi_dp dp ON dp.id = ss.dem_plan_id \
             WHERE dp.status IN (''validated'', ''draft'')', \
           'dem_plan_id,order_date,product_id,location_id', 'UNLOGGED', 'DEFERRED', \
           'swi_dp', ARRAY['dem_plan_id','order_date'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");
}

/// db-bus `insert_replace_range_partitioned_data`: build the replacement month
/// as a DETACHED relation, then DETACH/ATTACH/DROP/RENAME so a NEW oid carries
/// the canonical name. This is the DDL pg_reflex observes at the incident's
/// 14:05.
fn swi_month_swap() {
    Spi::run(
        "CREATE TABLE swi_ss_471_feb_new (LIKE swi_ss_471_feb INCLUDING DEFAULTS)",
    )
    .expect("build detached");
    Spi::run(
        "INSERT INTO swi_ss_471_feb_new VALUES \
           (471, '2026-02-10', 1, 1, 99), \
           (471, '2026-02-20', 2, 1, 11)",
    )
    .expect("fill detached");
    Spi::run("ALTER TABLE swi_ss_471 DETACH PARTITION swi_ss_471_feb").expect("detach old");
    Spi::run(
        "ALTER TABLE swi_ss_471 ATTACH PARTITION swi_ss_471_feb_new \
         FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
    )
    .expect("attach new");
    Spi::run("DROP TABLE swi_ss_471_feb").expect("drop old");
    Spi::run("ALTER TABLE swi_ss_471_feb_new RENAME TO swi_ss_471_feb").expect("rename");
}

fn swi_registry_health() -> (bool, Option<String>) {
    let stale = Spi::get_one::<bool>(
        "SELECT COALESCE(known_stale, FALSE) FROM public.__reflex_ivm_reference \
         WHERE name = 'swi_imv'",
    )
    .unwrap()
    .unwrap_or(false);
    let err = Spi::get_one::<String>(
        "SELECT last_error FROM public.__reflex_ivm_reference WHERE name = 'swi_imv'",
    )
    .unwrap_or(None);
    (stale, err)
}

/// CONTROL: the same month swap with the DP in an INCLUDED status must keep the
/// IMV complete. If this fails, the swap itself is the culprit and the status
/// window is irrelevant.
///
/// Also the mutation-check discriminator for the event log's write-suppression
/// guard: the swap changes the slice's CONTENT (qty is rewritten) but not its
/// ROW COUNT, so this is the "must stay silent" case — a rebuild that changes
/// nothing worth logging must leave no `__reflex_event_log` row.
#[pg_test]
fn swi_control_swap_with_included_status_keeps_rows() {
    swi_build_fixture("validated");
    assert_eq!(swi_imv_rows_for(471), 3, "seeded DP 471 rows");

    swi_month_swap();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain partition queue");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush");

    let (stale, err) = swi_registry_health();
    assert_eq!(
        swi_imv_rows_for(471),
        3,
        "CONTROL: swap under an included status must preserve the slice \
         (known_stale={stale}, last_error={err:?})"
    );
    assert_eq!(
        swi_event_rows("swi_imv"),
        0,
        "a swap that doesn't change the slice's row count must write nothing"
    );
}

/// THE HYPOTHESIS: DP status moves to a value the view's IN list omits (the
/// `creating_sop` window of the SP run). `swi_dp` is in ignore_sources, so the
/// status change itself changes nothing. Then the month swap fires the
/// partition path, which rebuilds from base_query — and the predicate now
/// excludes the whole DP.
///
/// This confirms the defect, it does not fix it: the predicate exclusion is a
/// base-db-owned bug (spec §3 B1 — invert `sop_forecast_view`'s status
/// predicate) that this plan does not touch. What this plan (Task 3) adds is
/// the durable `__reflex_event_log` row asserted in
/// `swi_silent_wipe_leaves_an_event_log_row` — the slice still shrinks, but it
/// no longer does so silently.
#[pg_test]
fn swi_status_window_swap_wipes_slice_silently() {
    swi_build_fixture("validated");
    assert_eq!(swi_imv_rows_for(471), 3, "seeded DP 471 rows");

    Spi::run("UPDATE swi_dp SET status = 'creating_sop' WHERE id = 471").expect("SP window opens");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("flush ignored-source change");
    assert_eq!(
        swi_imv_rows_for(471),
        3,
        "ignore_sources contract: a status change alone must not touch the IMV"
    );

    swi_month_swap();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain partition queue");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush");

    let after_471 = swi_imv_rows_for(471);
    let after_all = swi_imv_rows();
    let (stale, _err) = swi_registry_health();

    assert!(
        after_471 < 3,
        "the excluded-status window lets the swap rebuild the slice smaller \
         (dp471_rows={after_471} total_rows={after_all})"
    );
    assert!(
        !stale,
        "the swap succeeds outright — this is not a caught failure, so \
         known_stale is never set; that is exactly why the event log (not \
         known_stale) is the artefact that catches it"
    );
}

/// Does the status returning to `validated` heal it? With `ignore_sources`
/// naming the status table, nothing should fire.
///
/// This is the same base-db-owned defect (spec §3 B1) as the probe above:
/// `swi_dp` is in `ignore_sources`, so pg_reflex never re-evaluates the base
/// query on a plain status UPDATE, and the already-shrunk slice stays shrunk.
/// Not a bug in this plan to fix — recorded here so the healing gap stays
/// pinned by a green test rather than a panic.
#[pg_test]
fn swi_status_return_does_not_heal() {
    swi_build_fixture("validated");
    Spi::run("UPDATE swi_dp SET status = 'creating_sop' WHERE id = 471").expect("window opens");
    swi_month_swap();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush");
    let during = swi_imv_rows_for(471);

    Spi::run("UPDATE swi_dp SET status = 'validated' WHERE id = 471").expect("window closes");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain 2");
    let after = swi_imv_rows_for(471);

    assert!(during < 3, "precondition: the swap already shrunk the slice");
    assert_eq!(
        after, during,
        "ignore_sources means the status returning to validated does not \
         re-evaluate the base query, so the slice does not heal on its own"
    );
}

// ---------------------------------------------------------------------------
// RE-VALIDATION of the three defects claimed from code reading (D1/D2/D3).
// Each probe panics with its measurements so the run itself is the evidence.
// ---------------------------------------------------------------------------

fn dfx_build() {
    Spi::run("CREATE TABLE dfx_src (id BIGINT, val TEXT)").expect("src");
    Spi::run("INSERT INTO dfx_src VALUES (1, 'a')").expect("seed");
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('dfx_imv', 'SELECT id, val FROM dfx_src', \
         'id', 'UNLOGGED', 'DEFERRED')",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("initial flush");
}

fn dfx_state() -> (i64, bool, Option<String>, i64, i64) {
    let imv_rows = Spi::get_one::<i64>("SELECT count(*)::int8 FROM dfx_imv")
        .unwrap()
        .unwrap();
    let stale = Spi::get_one::<bool>(
        "SELECT COALESCE(known_stale, FALSE) FROM public.__reflex_ivm_reference \
         WHERE name = 'dfx_imv'",
    )
    .unwrap()
    .unwrap_or(false);
    let err = Spi::get_one::<String>(
        "SELECT last_error FROM public.__reflex_ivm_reference WHERE name = 'dfx_imv'",
    )
    .unwrap_or(None);
    let delta_rows = Spi::get_one::<i64>("SELECT count(*)::int8 FROM __reflex_delta_dfx_src")
        .unwrap_or(Some(-1))
        .unwrap_or(-1);
    let pending = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM public.__reflex_deferred_pending \
         WHERE source_table LIKE '%dfx_src%'",
    )
    .unwrap()
    .unwrap();
    (imv_rows, stale, err, delta_rows, pending)
}

/// D1: a caught per-IMV flush failure must mark the IMV stale. The staged delta
/// stays discarded on purpose — it is per-SOURCE and shared with every other IMV
/// reading that source, so replaying it would re-apply to IMVs that already
/// succeeded (a merge-add, i.e. silent double-counting, for aggregates). The
/// repair path is reflex_reconcile, which clears known_stale.
#[pg_test]
fn dfx_d1_failed_flush_marks_imv_stale() {
    dfx_build();

    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("flush (failure is caught)");

    let (rows, stale, err, delta_rows, _pending) = dfx_state();
    assert!(stale, "a caught flush failure must set known_stale");
    assert!(
        err.is_some_and(|e| e.contains("23505")),
        "last_error must carry the SQLSTATE of the failure"
    );
    assert_eq!(rows, 1, "the failed subtransaction must not have modified the IMV");
    assert_eq!(delta_rows, 0, "the shared per-source delta is still discarded");

    let reason = Spi::get_one::<String>(
        "SELECT stale_reason FROM public.__reflex_ivm_reference WHERE name = 'dfx_imv'",
    )
    .unwrap();
    assert!(
        reason.is_some_and(|r| r.contains("flush")),
        "stale_reason must say what failed"
    );
}

/// A5 — `pg_reflex.flush_failure_policy`. Default behaviour is unchanged: the
/// caller's transaction survives a per-IMV flush failure (it degrades to a
/// WARNING, exactly as D1 pins). Nothing in the suite sets this GUC, so this
/// also stands as the "nothing regressed" control for the two tests below.
#[pg_test]
fn dfx_policy_defaults_to_warn() {
    dfx_build();
    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("default policy must not abort");
    let (_, stale, _, _, _) = dfx_state();
    assert!(stale, "warn still marks the IMV stale");
}

/// Under 'error' the failure reaches the caller instead of being swallowed.
///
/// A `#[pg_test]` body is itself one transaction, and cannot host this
/// scenario directly. Confirmed empirically: `client.update` (in
/// `reflex_flush_deferred`) calls `pg_sys::SPI_execute` with no sigsetjmp of
/// its own, so the uncaught ERROR raised inside the fail_hard DO block
/// longjmps straight past every Rust frame in between — neither
/// `std::panic::catch_unwind` around the triggering `Spi::run` nor a PL/pgSQL
/// `EXCEPTION` block wrapped around it catches it — landing on the
/// pgrx-tests CLIENT's own top-level query and failing the whole test
/// instead of letting it observe the outcome. This is exactly the in-harness
/// limitation the task brief anticipated, and exactly why
/// `src/tests/pg_test_partition_attach_locks.rs:63-80` drives its own
/// transaction-aborting scenario through a REMOTE `dblink` worker instead —
/// only a genuinely separate session can abort without taking this one down.
///
/// The worker fires the flush as its own bare autocommit statement (no
/// explicit `BEGIN`): the duplicate-key insert's own implicit COMMIT is what
/// drains the deferred trigger, so a failure there aborts exactly that one
/// statement's transaction — proof the flush failure reaches (and aborts)
/// the caller. `dblink_exec(..., fail_on_error := false)` reports that
/// failure back to THIS session as a string instead of raising here, so the
/// probe itself needs no exception handling of its own.
#[pg_test]
fn dfx_policy_error_aborts_the_caller() {
    const DBNAME: &str = "reflex_flushpolicy_probe";
    probe_db_open(DBNAME);
    worker_exec(
        "CREATE TABLE dpx_src (id BIGINT, val TEXT); \
         INSERT INTO dpx_src VALUES (1, 'a'); \
         DO $mk$ BEGIN PERFORM create_reflex_ivm('dpx_imv', 'SELECT id, val FROM dpx_src', \
             'id', 'UNLOGGED', 'DEFERRED'); END $mk$",
    );
    worker_exec("SET pg_reflex.flush_failure_policy = 'error'");

    let outcome = Spi::get_one::<String>(&format!(
        "SELECT dblink_exec('reflex_lock_worker', {}, false)",
        sql_lit("INSERT INTO dpx_src VALUES (2, 'x'), (2, 'y')")
    ))
    .expect("dblink_exec call")
    .expect("dblink_exec result");

    worker_exec(
        "DROP TABLE IF EXISTS dpx_src CASCADE; DROP TABLE IF EXISTS dpx_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'dpx_imv'",
    );
    probe_db_close(DBNAME);

    assert!(
        outcome.to_uppercase().contains("ERROR"),
        "under 'error' the flush failure must propagate to (and abort) the caller \
         (dblink_exec returned: {outcome})"
    );
}

/// An unrecognised value must not silently disable the guard.
#[pg_test]
fn dfx_policy_invalid_value_falls_back_to_warn() {
    dfx_build();
    Spi::run("SET pg_reflex.flush_failure_policy = 'banana'").expect("set policy");
    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("invalid value falls back to warn");
    let (_, stale, _, _, _) = dfx_state();
    assert!(stale, "fallback must still mark the IMV stale");
}

/// D2: the evidence must survive until something actually repairs the IMV.
#[pg_test]
fn dfx_d2_successful_flush_preserves_last_error_while_stale() {
    dfx_build();
    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("failing flush");

    Spi::run("INSERT INTO dfx_src VALUES (3, 'z')").expect("clean insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("successful flush");

    let (_, stale, err, _, _) = dfx_state();
    assert!(stale, "a later success must not clear known_stale");
    assert!(
        err.is_some(),
        "a later success must not erase last_error while the IMV is still stale"
    );

    Spi::run("DELETE FROM dfx_src WHERE id = 2 AND val = 'y'").expect("remove the duplicate");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("flush the fix");

    // reconcile is the repair path, and it is what clears both.
    let r = Spi::get_one::<String>("SELECT reflex_reconcile('dfx_imv')")
        .expect("reconcile call")
        .expect("reconcile result");
    assert!(!r.starts_with("ERROR"), "reconcile returned: {r}");

    let (_, stale_after, err_after, _, _) = dfx_state();
    assert!(!stale_after, "reconcile must clear known_stale");
    assert!(err_after.is_none(), "reconcile must clear last_error");
}

/// D3: the status view must not report a planner estimate for an IMV that
/// carries an anomaly. A healthy IMV keeps the O(1) estimate.
#[pg_test]
fn dfx_d3_status_row_count_exact_under_anomaly() {
    dfx_build();
    Spi::run("ANALYZE dfx_imv").expect("analyze so reltuples > 0");

    let (rc, est) = Spi::get_two::<i64, bool>(
        "SELECT row_count, is_estimate FROM reflex_ivm_status() WHERE name = 'dfx_imv'",
    )
    .expect("status query");
    assert_eq!(rc, Some(1), "healthy IMV reports its row count");
    assert_eq!(est, Some(true), "healthy IMV uses the O(1) estimate");

    // Force a caught flush failure -> known_stale, then diverge the estimate.
    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("failing flush");
    Spi::run("DELETE FROM dfx_imv").expect("empty the target behind the estimate's back");

    let (rc2, est2) = Spi::get_two::<i64, bool>(
        "SELECT row_count, is_estimate FROM reflex_ivm_status() WHERE name = 'dfx_imv'",
    )
    .expect("status query 2");
    assert_eq!(rc2, Some(0), "an IMV carrying an anomaly must report the exact count");
    assert_eq!(est2, Some(false), "and must say the number is not an estimate");
}

/// NB2: `reflex_ivm_status` is the primary observability entry point. A
/// missing `__reflex_event_log` (a missed migration on an upgraded install)
/// must not turn it into a casualty of the anomaly signal it added — the
/// call must fall back to the known_stale/last_error/estimate determination
/// instead of erroring for every IMV.
#[pg_test]
fn dfx_status_survives_missing_event_log_table() {
    dfx_build();
    Spi::run("ANALYZE dfx_imv").expect("analyze so reltuples > 0");
    Spi::run("ALTER TABLE public.__reflex_event_log RENAME TO __reflex_event_log_hidden")
        .expect("simulate a missed migration");

    let (rc, est) = Spi::get_two::<i64, bool>(
        "SELECT row_count, is_estimate FROM reflex_ivm_status() WHERE name = 'dfx_imv'",
    )
    .expect("status must not error when the event log is missing");
    assert_eq!(rc, Some(1), "a healthy IMV still reports its row count");
    assert_eq!(
        est,
        Some(true),
        "no known_stale/last_error means still the O(1) estimate, event log or not"
    );
}

// ---------------------------------------------------------------------------
// A4: the durable maintenance event log. A slice-changing rebuild or a caught
// flush failure must leave a row; an ordinary successful flush must not.
// ---------------------------------------------------------------------------

fn swi_event_rows(imv: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM public.__reflex_event_log WHERE imv_name = '{imv}'"
    ))
    .unwrap()
    .unwrap()
}

/// The incident, now observable. A slice rebuild that empties a non-empty slice
/// commits successfully — that is legitimate given the predicate — but it must
/// no longer do so without leaving a trace.
#[pg_test]
fn swi_silent_wipe_leaves_an_event_log_row() {
    swi_build_fixture("validated");
    assert_eq!(swi_imv_rows_for(471), 3, "seeded DP 471 rows");

    Spi::run("UPDATE swi_dp SET status = 'creating_sop' WHERE id = 471").expect("SP window");
    swi_month_swap();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain partition queue");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush");

    assert!(
        swi_imv_rows_for(471) < 3,
        "precondition: the slice was rebuilt smaller (this is the incident)"
    );

    let (before, after, reason) = Spi::get_three::<i64, i64, String>(
        "SELECT rows_before, rows_after, trigger_reason FROM public.__reflex_event_log \
         WHERE imv_name = 'swi_imv' AND event = 'rebuild' ORDER BY id DESC LIMIT 1",
    )
    .expect("event log query");
    assert!(before.unwrap_or(0) > after.unwrap_or(-1), "the row must record the shrink");
    assert_eq!(reason.as_deref(), Some("partition_swap"));
}

/// I2, partition side: unlike the plpgsql DO block in deferred.rs, the swap's
/// event-log INSERT runs via a bare `client.update` with no surrounding
/// EXCEPTION to catch a hard Postgres ERROR (which longjmps straight past a
/// `Result`). A missing `__reflex_event_log` must not turn a legitimate,
/// successful partition swap into a failure — the existence check must skip
/// the write instead.
#[pg_test]
fn swi_silent_wipe_still_swaps_without_event_log_table() {
    swi_build_fixture("validated");
    // Renamed, not dropped: the table is an extension member, and a real DROP
    // pulls in extension-membership dependency handling that isn't the point
    // of this test. A rename is enough to make `public.__reflex_event_log`
    // unresolvable — exactly the "missing table" shape an upgraded install
    // whose migration missed it would present.
    Spi::run("ALTER TABLE public.__reflex_event_log RENAME TO __reflex_event_log_hidden")
        .expect("simulate a missed migration");

    Spi::run("UPDATE swi_dp SET status = 'creating_sop' WHERE id = 471").expect("SP window");
    swi_month_swap();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain partition queue");
    let flush = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush");
    assert!(
        !flush.unwrap_or_default().starts_with("ERROR"),
        "the swap itself must succeed even though it can't log"
    );

    assert!(
        swi_imv_rows_for(471) < 3,
        "the swap must still have rebuilt the slice — logging is an observer, not a gate"
    );
}

/// I1 convergence: a 'rebuild' row must force the exact count (not the O(1)
/// estimate) until the target is re-analyzed — and it MUST clear afterward,
/// or the remedy the incident's stale_reason prescribes (reconcile, which
/// ANALYZEs the target) would deepen the condition instead of repairing it.
/// This is the point of the finding: without this test the ANALYZE-recency
/// scoping is unverified.
#[pg_test]
fn swi_rebuild_anomaly_clears_after_analyze() {
    swi_build_fixture("validated");
    Spi::run("UPDATE swi_dp SET status = 'creating_sop' WHERE id = 471").expect("SP window");
    swi_month_swap();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain partition queue");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush");
    assert_eq!(
        swi_event_rows("swi_imv"),
        1,
        "precondition: the wipe logged exactly one rebuild row"
    );

    let (rc, est) = Spi::get_two::<i64, bool>(
        "SELECT row_count, is_estimate FROM reflex_ivm_status() WHERE name = 'swi_imv'",
    )
    .expect("status before analyze");
    assert_eq!(
        est,
        Some(false),
        "a rebuild row newer than the last ANALYZE must force the exact count"
    );
    assert!(rc.is_some(), "row_count must still be reported while forced exact");

    Spi::run("ANALYZE swi_imv").expect("analyze the target");
    // pg_stat_all_tables is snapshotted once per transaction; the whole test
    // runs inside pg_test's single wrapping transaction, so without this the
    // ANALYZE above is invisible to the next query in THIS test only — a
    // harness artifact, not something a real caller (a fresh statement in a
    // fresh transaction) would ever need.
    Spi::run("SELECT pg_stat_clear_snapshot()").expect("clear stats snapshot");
    let (_, est_after) = Spi::get_two::<i64, bool>(
        "SELECT row_count, is_estimate FROM reflex_ivm_status() WHERE name = 'swi_imv'",
    )
    .expect("status after analyze");
    assert_eq!(
        est_after,
        Some(true),
        "ANALYZE-ing the target (what reconcile does) must clear the rebuild \
         anomaly and restore the O(1) estimate — this is what makes reconcile \
         an actual repair instead of a remedy that deepens its own finding"
    );
}

/// A caught flush failure writes an error row alongside the staleness mark.
#[pg_test]
fn dfx_failed_flush_writes_event_log_row() {
    dfx_build();
    assert_eq!(swi_event_rows("dfx_imv"), 0, "no events on a healthy IMV");

    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("failing flush");

    let (event, sqlstate) = Spi::get_two::<String, String>(
        "SELECT event, sqlstate FROM public.__reflex_event_log \
         WHERE imv_name = 'dfx_imv' ORDER BY id DESC LIMIT 1",
    )
    .expect("event log query");
    assert_eq!(event.as_deref(), Some("error"));
    assert_eq!(sqlstate.as_deref(), Some("23505"));
}

/// I2: a logging side effect must never be able to break the operation it
/// observes. If `__reflex_event_log` is missing (the realistic trigger: an
/// upgraded install whose migration missed the table), the EXCEPTION
/// branch's own INSERT would otherwise raise "relation does not exist" from
/// INSIDE the handler — uncaught by it — aborting the whole cascade and
/// rolling back the known_stale/last_error UPDATE two statements above. The
/// nested BEGIN…EXCEPTION WHEN OTHERS THEN NULL around that INSERT must
/// prevent that: known_stale/last_error must still be recorded exactly as
/// they would be with the table present.
#[pg_test]
fn dfx_failed_flush_marks_stale_even_without_event_log_table() {
    dfx_build();
    // Renamed, not dropped: the table is an extension member, and a real DROP
    // pulls in extension-membership dependency handling that isn't the point
    // of this test. A rename is enough to make `public.__reflex_event_log`
    // unresolvable — exactly the "missing table" shape an upgraded install
    // whose migration missed it would present.
    Spi::run("ALTER TABLE public.__reflex_event_log RENAME TO __reflex_event_log_hidden")
        .expect("simulate a missed migration");

    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("failing flush must not abort");

    let (rows, stale, err, _, _) = dfx_state();
    assert!(
        stale,
        "known_stale must still be set even though the event-log INSERT can't land"
    );
    assert!(
        err.is_some_and(|e| e.contains("23505")),
        "last_error must still carry the real failure, not be lost to a missing table"
    );
    assert_eq!(rows, 1, "the failed subtransaction must still not have modified the IMV");
}

/// An unremarkable successful flush must NOT write a row — the log is for
/// anomalies and slice-changing rebuilds, not for every commit.
#[pg_test]
fn dfx_clean_flush_writes_no_event_log_row() {
    dfx_build();
    Spi::run("INSERT INTO dfx_src VALUES (4, 'q')").expect("clean insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("clean flush");
    assert_eq!(swi_event_rows("dfx_imv"), 0, "a clean flush writes nothing");
}

/// Pruning is operator-driven and returns what it removed.
#[pg_test]
fn dfx_prune_event_log_removes_only_old_rows() {
    dfx_build();
    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("failing flush");
    assert_eq!(swi_event_rows("dfx_imv"), 1);

    let kept = Spi::get_one::<i64>("SELECT reflex_prune_event_log('30 days')")
        .expect("prune")
        .expect("prune result");
    assert_eq!(kept, 0, "nothing is 30 days old yet");
    assert_eq!(swi_event_rows("dfx_imv"), 1, "recent rows survive");

    Spi::run(
        "UPDATE public.__reflex_event_log SET at = now() - INTERVAL '40 days' \
         WHERE imv_name = 'dfx_imv'",
    )
    .expect("age the row");
    let removed = Spi::get_one::<i64>("SELECT reflex_prune_event_log('30 days')")
        .expect("prune 2")
        .expect("prune 2 result");
    assert_eq!(removed, 1);
    assert_eq!(swi_event_rows("dfx_imv"), 0);
}
