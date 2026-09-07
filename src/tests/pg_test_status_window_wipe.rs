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
}

/// THE HYPOTHESIS: DP status moves to a value the view's IN list omits (the
/// `creating_sop` window of the SP run). `swi_dp` is in ignore_sources, so the
/// status change itself changes nothing. Then the month swap fires the
/// partition path, which rebuilds from base_query — and the predicate now
/// excludes the whole DP.
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
    let (stale, err) = swi_registry_health();

    // Not an assertion of desired behaviour — this is the probe. Print the
    // observed state so the run itself is the evidence.
    panic!(
        "PROBE RESULT: dp471_rows={after_471} total_rows={after_all} \
         known_stale={stale} last_error={err:?}"
    );
}

/// Does the status returning to `validated` heal it? With `ignore_sources`
/// naming the status table, nothing should fire.
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

    panic!("PROBE RESULT: during_window={during} after_return_to_validated={after}");
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

/// D1: a caught per-IMV flush failure must not leave the IMV silently diverged.
/// Claim under test: `last_error` is set but `known_stale` stays FALSE, and the
/// staged delta is discarded anyway — so the lost rows can never be replayed.
#[pg_test]
fn dfx_d1_failed_flush_discards_delta_without_marking_stale() {
    dfx_build();
    let (rows0, _, _, _, _) = dfx_state();

    // Two rows sharing the IMV's unique key: the flush INSERT must violate
    // __reflex_uk_dfx_imv. The source itself has no such constraint.
    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("flush (failure is caught)");

    let (rows1, stale, err, delta_rows, pending) = dfx_state();
    panic!(
        "D1 PROBE: rows_before={rows0} rows_after={rows1} (source now has 3) \
         known_stale={stale} last_error={err:?} staged_delta_rows={delta_rows} \
         pending_rows={pending}"
    );
}

/// D2: the evidence of D1 must survive until something repairs the IMV.
/// Claim under test: the next SUCCESSFUL flush nulls `last_error`, erasing the
/// only trace, while the IMV stays diverged.
#[pg_test]
fn dfx_d2_successful_flush_erases_last_error() {
    dfx_build();
    Spi::run("INSERT INTO dfx_src VALUES (2, 'x'), (2, 'y')").expect("dup insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("failing flush");
    let (_, stale_a, err_a, _, _) = dfx_state();

    // A later, perfectly valid change to an unrelated key.
    Spi::run("INSERT INTO dfx_src VALUES (3, 'z')").expect("clean insert");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("successful flush");
    let (rows_b, stale_b, err_b, _, _) = dfx_state();

    panic!(
        "D2 PROBE: after_failure(known_stale={stale_a}, last_error={err_a:?}) \
         after_success(rows={rows_b}, known_stale={stale_b}, last_error={err_b:?}) \
         — source holds 4 rows, 3 distinct keys"
    );
}

/// D3: `reflex_ivm_status.row_count` must not report a comfortable number for a
/// table that has been emptied. Claim under test: it returns `pg_class.reltuples`
/// whenever that is > 0, so a slice wipe that ANALYZEs only the child leaves the
/// status view reporting the pre-incident count.
#[pg_test]
fn dfx_d3_status_row_count_reports_stale_estimate() {
    swi_build_fixture("validated");
    Spi::run("ANALYZE swi_imv").expect("analyze parent — the pre-incident state");

    let reported_before = Spi::get_one::<i64>(
        "SELECT row_count FROM reflex_ivm_status() WHERE name = 'swi_imv'",
    )
    .unwrap()
    .unwrap();

    Spi::run("UPDATE swi_dp SET status = 'creating_sop' WHERE id = 471").expect("window");
    swi_month_swap();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush");

    let reported_after = Spi::get_one::<i64>(
        "SELECT row_count FROM reflex_ivm_status() WHERE name = 'swi_imv'",
    )
    .unwrap()
    .unwrap();
    let actual = Spi::get_one::<i64>("SELECT count(*)::int8 FROM swi_imv")
        .unwrap()
        .unwrap();

    panic!(
        "D3 PROBE: status.row_count_before={reported_before} \
         status.row_count_after={reported_after} actual_count={actual}"
    );
}
