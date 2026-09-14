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


// Adversarial review of the heal (2026-09-14): one test per confirmed finding.

fn ish_heal_triggers_on(table: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT count(*)::int8 FROM pg_trigger \
          WHERE tgrelid = '{table}'::regclass AND tgname LIKE '__reflex_heal%'"
    ))
    .expect("trigger query")
    .unwrap_or(-1)
}

fn ish_create(name: &str, sql: &str, key: &str, ignored: &str, partition_by: &str) {
    let r = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{name}', '{sql}', '{key}', 'UNLOGGED', 'DEFERRED', \
           '{ignored}', ARRAY[{partition_by}])",
        sql = sql.replace('\'', "''")
    ))
    .expect("create call")
    .expect("create result");
    assert!(!r.starts_with("ERROR"), "create returned: {r}");
}

fn ish_diverging(imv: &str, cols: &str, query: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "WITH q AS ({query}), v AS (SELECT {cols} FROM {imv}) \
         SELECT (SELECT count(*) FROM (SELECT * FROM q EXCEPT ALL SELECT * FROM v) a) \
              + (SELECT count(*) FROM (SELECT * FROM v EXCEPT ALL SELECT * FROM q) b)"
    ))
    .expect("oracle")
    .unwrap_or(-1)
}

fn ish_subxact_state() -> String {
    Spi::run("SELECT pg_stat_clear_snapshot()").expect("clear stats snapshot");
    Spi::get_one::<String>(
        "SELECT (x.subxact_count, x.subxact_overflowed)::text \
           FROM pg_stat_get_backend_idset() s, pg_stat_get_backend_subxact(s) x \
          WHERE pg_stat_get_backend_pid(s) = pg_backend_pid()",
    )
    .expect("subxact query")
    .expect("own backend row")
}

/// F1: a second, unaliased read of the ignored source (here in NOT EXISTS) can
/// change partitions other than the changed row's key, so it must not be mapped.
#[pg_test]
fn ish_second_use_of_the_ignored_source_is_not_mapped() {
    swi_build_fixture("validated");
    ish_create(
        "ish_nx_imv",
        "SELECT ss.dem_plan_id, ss.order_date, ss.product_id, ss.location_id, ss.qty \
           FROM swi_ss ss JOIN swi_dp dp ON dp.id = ss.dem_plan_id \
          WHERE dp.status IN ('validated', 'draft') \
            AND NOT EXISTS (SELECT 1 FROM swi_dp WHERE swi_dp.status = 'frozen')",
        "dem_plan_id,order_date,product_id,location_id",
        "!swi_dp",
        "'dem_plan_id','order_date'",
    );
    let mapped = Spi::get_one::<bool>(
        "SELECT COALESCE(aggregations->'ignore_heal_keys', '{}'::jsonb) <> '{}'::jsonb \
           FROM public.__reflex_ivm_reference WHERE name = 'ish_nx_imv'",
    )
    .expect("mapping query")
    .expect("registry row");
    assert!(!mapped, "a source read twice cannot be scoped to one partition key");

    Spi::run("UPDATE swi_dp SET status = 'frozen' WHERE id = 9").expect("freeze another DP");
    assert_eq!(ish_queued("ish_nx_imv"), 0, "so nothing may claim a heal for it");
}

/// F2: a partition key containing a comma must heal its own partition.
#[pg_test]
fn ish_key_with_a_comma_heals_its_partition() {
    Spi::run("CREATE TABLE ish_kd (code TEXT PRIMARY KEY, status TEXT NOT NULL)").expect("kd");
    Spi::run("INSERT INTO ish_kd VALUES ('a,b', 'validated'), ('c', 'validated')").expect("seed kd");
    Spi::run(
        "CREATE TABLE ish_kf (code TEXT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (code)",
    )
    .expect("kf");
    Spi::run("CREATE TABLE ish_kf_ab PARTITION OF ish_kf FOR VALUES IN ('a,b')").expect("ab");
    Spi::run("CREATE TABLE ish_kf_c PARTITION OF ish_kf FOR VALUES IN ('c')").expect("c");
    Spi::run("INSERT INTO ish_kf VALUES ('a,b', 1, 10), ('c', 1, 20)").expect("seed kf");
    let query = "SELECT f.code, f.d, f.v FROM ish_kf f JOIN ish_kd x ON x.code = f.code \
                  WHERE x.status = 'validated'";
    ish_create("ish_k_imv", query, "code,d", "!ish_kd", "'code'");

    Spi::run("UPDATE ish_kd SET status = 'draft' WHERE code = 'a,b'").expect("exclude a,b");
    assert_eq!(ish_queued("ish_k_imv"), 1, "precondition: key queued");
    let result = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()")
        .expect("heal")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "heal returned: {result}");
    assert_eq!(
        ish_diverging("ish_k_imv", "code, d, v", query),
        0,
        "the healed IMV must equal its query"
    );
}

/// F3: an ignored-source column whose type differs from the partition column's
/// (numeric vs bigint) renders keys the partition cannot parse: no mapping.
#[pg_test]
fn ish_key_type_mismatch_is_not_mapped() {
    Spi::run("CREATE TABLE ish_nd (id NUMERIC PRIMARY KEY, status TEXT NOT NULL)").expect("nd");
    Spi::run("INSERT INTO ish_nd VALUES (5.0, 'validated')").expect("seed nd");
    Spi::run(
        "CREATE TABLE ish_nf (pid BIGINT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (pid)",
    )
    .expect("nf");
    Spi::run("CREATE TABLE ish_nf_5 PARTITION OF ish_nf FOR VALUES IN (5)").expect("leaf");
    Spi::run("INSERT INTO ish_nf VALUES (5, 1, 10)").expect("seed nf");
    ish_create(
        "ish_n_imv",
        "SELECT f.pid, f.d, f.v FROM ish_nf f JOIN ish_nd x ON x.id = f.pid \
          WHERE x.status = 'validated'",
        "pid,d",
        "!ish_nd",
        "'pid'",
    );
    assert_eq!(ish_heal_triggers_on("ish_nd"), 0, "mismatched key types get no heal");
}

/// F3: a heal that raises is recorded on its own queue rows; it neither aborts
/// the other IMVs' heals nor the sweeps that run it.
#[pg_test]
fn ish_a_failing_heal_is_recorded_and_does_not_abort_the_sweep() {
    swi_build_fixture("validated");
    Spi::run("CREATE TABLE ish_od (id BIGINT PRIMARY KEY, status TEXT NOT NULL)").expect("od");
    Spi::run("INSERT INTO ish_od VALUES (1, 'validated')").expect("seed od");
    Spi::run(
        "CREATE TABLE ish_of (pid INT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (pid)",
    )
    .expect("of");
    Spi::run("CREATE TABLE ish_of_1 PARTITION OF ish_of FOR VALUES IN (1)").expect("leaf");
    Spi::run("INSERT INTO ish_of VALUES (1, 1, 10)").expect("seed of");
    ish_create(
        "ish_o_imv",
        "SELECT f.pid, f.d, f.v FROM ish_of f JOIN ish_od x ON x.id = f.pid \
          WHERE x.status = 'validated'",
        "pid,d",
        "!ish_od",
        "'pid'",
    );
    // Out of range for the int4 partition column: the partition match raises.
    Spi::run("INSERT INTO ish_od VALUES (3000000000, 'validated')").expect("queue a bad key");
    Spi::run("UPDATE swi_dp SET status = 'draft' WHERE id = 471").expect("queue a good key");
    assert_eq!(ish_queued("ish_o_imv"), 1, "precondition: bad key queued");
    assert_eq!(ish_queued("swi_imv"), 1, "precondition: good key queued");

    let result = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()")
        .expect("the heal must report a failure, not raise it")
        .unwrap_or_default();
    assert!(result.starts_with("ERROR"), "the failed heal must be reported: {result}");
    assert_eq!(ish_queued("swi_imv"), 0, "the other IMV is still healed");
    let recorded = Spi::get_one::<bool>(
        "SELECT bool_and(last_error IS NOT NULL) FROM public.__reflex_heal_pending \
          WHERE imv_name = 'ish_o_imv'",
    )
    .expect("last_error query")
    .unwrap_or(false);
    assert!(recorded, "the failure must be recorded on the failing IMV's rows");

    Spi::get_one::<i64>("SELECT count(*)::int8 FROM reflex_scheduled_reconcile(60)")
        .expect("the scheduled sweep must survive a failing heal");
    Spi::get_one::<i64>("SELECT count(*)::int8 FROM reflex_doctor(NULL, TRUE)")
        .expect("doctor(fix) must survive a failing heal");
}

/// F4: DDL on the ignored source must never make writes to it fail. The IMV is
/// reported stale instead, since its changes can no longer be mapped.
#[pg_test]
fn ish_renamed_watched_column_keeps_the_source_writable() {
    swi_build_fixture("validated");
    Spi::run("ALTER TABLE swi_dp RENAME COLUMN status TO state").expect("rename watched column");
    Spi::run("UPDATE swi_dp SET state = 'draft' WHERE id = 471").expect("update after rename");
    Spi::run("INSERT INTO swi_dp VALUES (11, 'validated')").expect("insert after rename");

    let (stale, reason) = ish_status_stale("swi_imv");
    assert!(stale, "the IMV can no longer be healed and must say so");
    let reason = reason.unwrap_or_default();
    assert!(
        reason.contains("can no longer be healed"),
        "stale_reason must explain the lost heal: {reason}"
    );

    Spi::run("ALTER TABLE swi_dp RENAME COLUMN id TO plan_id").expect("rename mapped column");
    Spi::run("DELETE FROM swi_dp WHERE plan_id = 11").expect("delete after renaming the key");
}

/// F5: a role that may write the ignored source but holds no pg_reflex grants
/// keeps writing it, and its changes are still queued.
#[pg_test]
fn ish_writer_without_reflex_grants_can_write_the_ignored_source() {
    swi_build_fixture("validated");
    Spi::run("CREATE ROLE ish_app").expect("role");
    Spi::run("GRANT USAGE ON SCHEMA public TO ish_app").expect("schema usage");
    Spi::run("GRANT SELECT, INSERT, UPDATE, DELETE ON swi_dp TO ish_app").expect("table grants");

    Spi::run("SET ROLE ish_app").expect("become the app role");
    let update = Spi::run("UPDATE swi_dp SET status = 'draft' WHERE id = 471");
    Spi::run("RESET ROLE").expect("reset role");
    update.expect("the app role must still be able to write the ignored source");

    assert_eq!(ish_queued("swi_imv"), 1, "and its change must still be queued");
}

/// F6: queuing must not consume a subtransaction per statement; 64 of them
/// overflow the backend's subxid cache.
#[pg_test]
fn ish_status_updates_consume_no_subtransactions() {
    swi_build_fixture("validated");
    let before = ish_subxact_state();
    Spi::run(
        "DO $$ BEGIN FOR i IN 1..70 LOOP \
           UPDATE swi_dp SET status = CASE WHEN i % 2 = 0 THEN 'draft' ELSE 'validated' END \
            WHERE id = 9; \
         END LOOP; END $$",
    )
    .expect("70 status updates");
    assert_eq!(ish_queued("swi_imv"), 1, "precondition: the updates queued the key");
    assert_eq!(
        ish_subxact_state(),
        before,
        "the heal trigger must not open subtransactions"
    );
}

/// F9: keys queued for a disabled IMV wait for it; they fail nothing meanwhile.
#[pg_test]
fn ish_a_disabled_imv_does_not_fail_the_sweep() {
    swi_build_fixture("validated");
    Spi::run("UPDATE swi_dp SET status = 'draft' WHERE id = 471").expect("queue");
    Spi::run("UPDATE public.__reflex_ivm_reference SET enabled = FALSE WHERE name = 'swi_imv'")
        .expect("disable");

    let result = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()")
        .expect("heal")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "a disabled IMV is not a failed heal: {result}");
    let failed = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM reflex_doctor(NULL, TRUE) \
          WHERE check_id = 'F14' AND outcome LIKE 'failed%'",
    )
    .expect("doctor")
    .unwrap_or(-1);
    assert_eq!(failed, 0, "F14 must not report a failure it cannot clear");

    Spi::run("UPDATE public.__reflex_ivm_reference SET enabled = TRUE WHERE name = 'swi_imv'")
        .expect("enable");
    assert_eq!(ish_queued("swi_imv"), 1, "the keys wait for the IMV to be enabled");
}

/// F10: TRUNCATE has no transition table to scope; the IMV must report stale
/// with a remedy that clears it.
#[pg_test]
fn ish_truncate_of_the_ignored_source_marks_the_imv_stale() {
    swi_build_fixture("validated");
    Spi::run("TRUNCATE swi_dp").expect("truncate ignored source");

    let (stale, reason) = ish_status_stale("swi_imv");
    assert!(stale, "a truncated ignored source leaves the IMV wrong");
    let reason = reason.unwrap_or_default();
    assert!(
        reason.contains("reflex_reconcile('swi_imv')"),
        "stale_reason must prescribe the reconcile: {reason}"
    );

    let result = Spi::get_one::<String>("SELECT reflex_reconcile('swi_imv')")
        .expect("reconcile")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "reconcile returned: {result}");
    assert_eq!(ish_swi_diverging_rows(), 0, "the remedy restores the IMV");
    assert!(!ish_status_stale("swi_imv").0, "and retires the report");
}
