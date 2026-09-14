// Healing after a change to an ignored source.
//
// `ignore_sources` keeps writes to a source from maintaining the IMV. When the
// query filters on that source, a change to it — `demand_planning.status`
// leaving and re-entering the included set — leaves the IMV permanently wrong for
// the affected slice: the 2026-09 incident. Since 1.11.4 a change that maps to an
// IMV partition key queues that key; `reflex_heal_ignored_sources`,
// `reflex_scheduled_reconcile` and `reflex_doctor(fix => TRUE)` rebuild only those
// partitions. The write itself stays cheap, and the IMV reports stale until healed.
//
// Fixture and swap helpers come from pg_test_status_window_wipe.rs.

fn ish_queued(imv: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM public.__reflex_heal_pending WHERE imv_name = '{imv}'"
    ))
    .expect("queue query")
    .unwrap_or(-1)
}

fn ish_swi_diverging_rows() -> i64 {
    Spi::get_one::<i64>(
        "WITH q AS ( \
           SELECT ss.dem_plan_id, ss.order_date, ss.product_id, ss.location_id, ss.qty \
             FROM swi_ss ss JOIN swi_dp dp ON dp.id = ss.dem_plan_id \
            WHERE dp.status IN ('validated', 'draft')), \
         v AS (SELECT dem_plan_id, order_date, product_id, location_id, qty FROM swi_imv) \
         SELECT (SELECT count(*) FROM (SELECT * FROM q EXCEPT ALL SELECT * FROM v) a) \
              + (SELECT count(*) FROM (SELECT * FROM v EXCEPT ALL SELECT * FROM q) b)",
    )
    .expect("oracle")
    .unwrap_or(-1)
}

fn ish_status_stale(imv: &str) -> (bool, Option<String>) {
    let stale = Spi::get_one::<bool>(&format!(
        "SELECT known_stale FROM reflex_ivm_status() WHERE name = '{imv}'"
    ))
    .expect("status")
    .expect("status row");
    let reason = Spi::get_one::<String>(&format!(
        "SELECT stale_reason FROM reflex_ivm_status() WHERE name = '{imv}'"
    ))
    .expect("status reason");
    (stale, reason)
}

/// The incident shape end to end: the slice empties during the excluded-status
/// window, the status returns, and a sweep restores exactly what the query says.
fn ish_wipe_then_restore_status() {
    swi_build_fixture("validated");
    Spi::run("UPDATE swi_dp SET status = 'creating_sop' WHERE id = 471").expect("window opens");
    swi_month_swap();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("drain partition queue");
    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()").expect("flush");
    assert!(swi_imv_rows_for(471) < 3, "precondition: the swap emptied the slice");

    Spi::run("UPDATE swi_dp SET status = 'validated' WHERE id = 471").expect("window closes");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time triggers");
}

#[pg_test]
fn ish_status_return_heals_after_sweep() {
    ish_wipe_then_restore_status();

    let queued: i64 = Spi::get_one(
        "SELECT count(*)::int8 FROM public.__reflex_heal_pending \
          WHERE imv_name = 'swi_imv' AND partition_key = '471'",
    )
    .expect("queue query")
    .unwrap_or(0);
    assert_eq!(queued, 1, "the status change must queue exactly the affected DP");
    assert!(
        swi_imv_rows_for(471) < 3,
        "queue + sweep: the status UPDATE itself must not rebuild anything"
    );

    let (stale, reason) = ish_status_stale("swi_imv");
    assert!(stale, "a queued heal means the IMV is known not to match its query");
    let reason = reason.unwrap_or_default();
    assert!(
        reason.contains("swi_dp") && reason.contains("reflex_heal_ignored_sources"),
        "stale_reason must name the ignored source and the remedy: {reason}"
    );

    let result = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()")
        .expect("heal")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "heal returned: {result}");

    assert_eq!(swi_imv_rows_for(471), 3, "the healed slice holds every row the query returns");
    assert_eq!(ish_swi_diverging_rows(), 0, "the IMV must equal its query exactly");
    assert_eq!(ish_queued("swi_imv"), 0, "a successful heal drains its queue rows");
    assert!(
        !ish_status_stale("swi_imv").0,
        "running the printed remedy must retire the stale report"
    );
}

#[pg_test]
fn ish_update_of_an_unreferenced_column_queues_nothing() {
    swi_build_fixture("validated");
    Spi::run("ALTER TABLE swi_dp ADD COLUMN note TEXT").expect("add unreferenced column");

    Spi::run("UPDATE swi_dp SET note = 'renamed' WHERE id = 471").expect("unrelated update");
    assert_eq!(
        ish_queued("swi_imv"),
        0,
        "a column the IMV never reads cannot change it, so it must not cost a rebuild"
    );

    Spi::run("UPDATE swi_dp SET status = 'draft' WHERE id = 471").expect("relevant update");
    assert_eq!(ish_queued("swi_imv"), 1, "control: a referenced column does queue the key");
}

#[pg_test]
fn ish_scheduled_reconcile_drains_the_heal_queue() {
    ish_wipe_then_restore_status();
    assert_eq!(ish_queued("swi_imv"), 1, "precondition: heal queued");

    // A 60-minute age gate leaves the freshly built IMV itself out of the sweep,
    // so only the heal drain can restore the slice.
    let _ = Spi::get_one::<i64>("SELECT count(*)::int8 FROM reflex_scheduled_reconcile(60)")
        .expect("scheduled reconcile");

    assert_eq!(ish_queued("swi_imv"), 0, "the sweep must drain queued heals");
    assert_eq!(swi_imv_rows_for(471), 3, "and rebuild the queued slice");
    assert_eq!(ish_swi_diverging_rows(), 0, "exactly");
}

#[pg_test]
fn ish_doctor_reports_and_fixes_a_queued_heal() {
    ish_wipe_then_restore_status();

    let action = Spi::get_one::<String>(
        "SELECT action FROM reflex_doctor() WHERE check_id = 'F14' AND object = 'swi_imv' LIMIT 1",
    )
    .expect("doctor report")
    .expect("a queued heal must be reported");
    assert!(
        action.contains("reflex_heal_ignored_sources('swi_imv')"),
        "the action must be the converging remedy: {action}"
    );

    let outcome = Spi::get_one::<String>(
        "SELECT outcome FROM reflex_doctor(NULL, TRUE) \
          WHERE check_id = 'F14' AND object = 'swi_imv' LIMIT 1",
    )
    .expect("doctor fix")
    .expect("fix row");
    assert_eq!(outcome, "fixed", "doctor(fix) must run the heal");
    assert_eq!(swi_imv_rows_for(471), 3, "and the slice must be restored");
}

#[pg_test]
fn ish_unmappable_ignored_source_gets_no_heal_trigger() {
    Spi::run("CREATE TABLE ish_region (id INT PRIMARY KEY, label TEXT NOT NULL)").expect("region");
    Spi::run("INSERT INTO ish_region VALUES (1, 'north'), (2, 'south')").expect("seed region");
    Spi::run(
        "CREATE TABLE ish_fact (part INT NOT NULL, region_id INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (part)",
    )
    .expect("fact");
    Spi::run("CREATE TABLE ish_fact_1 PARTITION OF ish_fact FOR VALUES IN (1)").expect("leaf");
    Spi::run("INSERT INTO ish_fact VALUES (1, 1, 10), (1, 2, 20)").expect("seed fact");

    // `region_id` is not a partition column, so a region change cannot be scoped
    // to partitions: no heal, the pre-1.11.4 ignore contract holds.
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('ish_imv', \
           'SELECT f.part, f.region_id, f.v, r.label \
              FROM ish_fact f JOIN ish_region r ON r.id = f.region_id', \
           'part,region_id', 'UNLOGGED', 'DEFERRED', '!ish_region', ARRAY['part'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");

    let triggers: i64 = Spi::get_one(
        "SELECT count(*)::int8 FROM pg_trigger \
          WHERE tgrelid = 'ish_region'::regclass AND tgname LIKE '__reflex_heal%'",
    )
    .expect("trigger query")
    .unwrap_or(-1);
    assert_eq!(triggers, 0, "an unmappable ignored source must not get heal triggers");

    Spi::run("UPDATE ish_region SET label = 'n' WHERE id = 1").expect("region update");
    assert_eq!(ish_queued("ish_imv"), 0, "and must queue nothing");
}

#[pg_test]
fn ish_drop_clears_the_heal_queue() {
    swi_build_fixture("validated");
    Spi::run("UPDATE swi_dp SET status = 'draft' WHERE id = 471").expect("relevant update");
    assert_eq!(ish_queued("swi_imv"), 1, "precondition: heal queued");

    let _ = Spi::get_one::<String>("SELECT drop_reflex_ivm('swi_imv')").expect("drop");
    assert_eq!(ish_queued("swi_imv"), 0, "a dropped IMV must not leave heal rows behind");

    Spi::run("UPDATE swi_dp SET status = 'validated' WHERE id = 471")
        .expect("the heal trigger must tolerate an ignored source with no remaining IMV");
}

