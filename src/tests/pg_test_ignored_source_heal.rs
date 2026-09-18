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
    ish_gated_fixture();
    Spi::run("UPDATE ish_gd SET status = 'boom' WHERE id = 1").expect("queue a key whose heal raises");
    Spi::run("UPDATE swi_dp SET status = 'draft' WHERE id = 471").expect("queue a good key");
    assert_eq!(ish_queued("ish_g_imv"), 1, "precondition: raising key queued");
    assert_eq!(ish_queued("swi_imv"), 1, "precondition: good key queued");

    let result = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()")
        .expect("the heal must report a failure, not raise it")
        .unwrap_or_default();
    assert!(result.starts_with("ERROR"), "the failed heal must be reported: {result}");
    assert_eq!(ish_queued("swi_imv"), 0, "the other IMV is still healed");
    let recorded = Spi::get_one::<bool>(
        "SELECT bool_and(last_error IS NOT NULL) FROM public.__reflex_heal_pending \
          WHERE imv_name = 'ish_g_imv'",
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

// Re-review of the fix round (2026-09-14): one test per confirmed finding.

/// An IMV over `ish_gd` whose query calls `ish_gate`, which raises for status
/// `boom` (a plain error) and `cancel` (a query cancel). Only a heal evaluates
/// it on those statuses: `ish_gd` is ignored.
fn ish_gated_fixture() {
    Spi::run(
        "CREATE FUNCTION ish_gate(status TEXT) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$ \
         BEGIN \
           IF status = 'boom' THEN RAISE EXCEPTION 'ish gate boom'; END IF; \
           IF status = 'cancel' THEN \
             RAISE EXCEPTION 'ish cancel probe' USING ERRCODE = 'query_canceled'; \
           END IF; \
           RETURN status = 'validated'; \
         END $$",
    )
    .expect("gate function");
    Spi::run("CREATE TABLE ish_gd (id BIGINT PRIMARY KEY, status TEXT NOT NULL)").expect("gd");
    Spi::run("INSERT INTO ish_gd VALUES (1, 'validated'), (2, 'validated')").expect("seed gd");
    Spi::run(
        "CREATE TABLE ish_gf (pid BIGINT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (pid)",
    )
    .expect("gf");
    Spi::run("CREATE TABLE ish_gf_1 PARTITION OF ish_gf FOR VALUES IN (1)").expect("leaf 1");
    Spi::run("CREATE TABLE ish_gf_2 PARTITION OF ish_gf FOR VALUES IN (2)").expect("leaf 2");
    Spi::run("INSERT INTO ish_gf VALUES (1, 1, 10), (2, 1, 20)").expect("seed gf");
    ish_create(
        "ish_g_imv",
        "SELECT f.pid, f.d, f.v FROM ish_gf f JOIN ish_gd x ON x.id = f.pid \
          WHERE ish_gate(x.status)",
        "pid,d",
        "!ish_gd",
        "'pid'",
    );
}

/// N1: the heal must never run user-defined code as anyone but the writer. A
/// type owner's cast of a watched column records who runs it.
#[pg_test]
fn ish_heal_trigger_runs_no_user_code_as_another_role() {
    swi_build_fixture("validated");
    Spi::run("CREATE TABLE ish_probe (who TEXT)").expect("probe");
    Spi::run("GRANT INSERT ON ish_probe TO PUBLIC").expect("probe grant");
    Spi::run("CREATE TYPE ish_mood AS ENUM ('ok', 'sad')").expect("type");
    Spi::run(
        "CREATE FUNCTION ish_mood_text(ish_mood) RETURNS TEXT LANGUAGE plpgsql AS $$ \
         BEGIN INSERT INTO public.ish_probe VALUES (current_user); RETURN format('%s', $1); END $$",
    )
    .expect("cast function");
    Spi::run("CREATE CAST (ish_mood AS TEXT) WITH FUNCTION ish_mood_text(ish_mood)").expect("cast");
    Spi::run("ALTER TABLE swi_dp ADD COLUMN mood ish_mood NOT NULL DEFAULT 'ok'").expect("column");
    ish_create(
        "ish_mood_imv",
        "SELECT ss.dem_plan_id, ss.order_date, ss.product_id, ss.location_id, ss.qty \
           FROM swi_ss ss JOIN swi_dp dp ON dp.id = ss.dem_plan_id \
          WHERE dp.status IN ('validated', 'draft') AND dp.mood = 'ok'",
        "dem_plan_id,order_date,product_id,location_id",
        "!swi_dp",
        "'dem_plan_id','order_date'",
    );
    Spi::run("CREATE ROLE ish_writer").expect("role");
    Spi::run("GRANT USAGE ON SCHEMA public TO ish_writer").expect("schema usage");
    Spi::run("GRANT SELECT, UPDATE ON swi_dp TO ish_writer").expect("table grants");

    Spi::run("SET ROLE ish_writer").expect("become the writer");
    let update = Spi::run("UPDATE swi_dp SET mood = 'sad' WHERE id = 471");
    Spi::run("RESET ROLE").expect("reset role");
    update.expect("the writer can update the ignored source");

    assert_eq!(ish_queued("ish_mood_imv"), 1, "the watched-column change is queued");
    let foreign = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM ish_probe WHERE who <> 'ish_writer'",
    )
    .expect("probe query")
    .unwrap_or(-1);
    assert_eq!(foreign, 0, "user-defined code must not run with another role's rights");
}

/// N2: a `char(n)` key keeps its padding in text form and matches no `text`
/// partition; only compatible key types are mapped.
#[pg_test]
fn ish_char_key_against_text_partition_is_not_mapped() {
    Spi::run("CREATE TABLE ish_bd (code CHAR(5) PRIMARY KEY, status TEXT NOT NULL)").expect("bd");
    Spi::run("INSERT INTO ish_bd VALUES ('ab', 'validated'), ('cd', 'validated')").expect("seed bd");
    Spi::run(
        "CREATE TABLE ish_bf (code TEXT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (code)",
    )
    .expect("bf");
    Spi::run("CREATE TABLE ish_bf_ab PARTITION OF ish_bf FOR VALUES IN ('ab')").expect("ab");
    Spi::run("CREATE TABLE ish_bf_cd PARTITION OF ish_bf FOR VALUES IN ('cd')").expect("cd");
    Spi::run("INSERT INTO ish_bf VALUES ('ab', 1, 10), ('cd', 1, 20)").expect("seed bf");
    ish_create(
        "ish_b_imv",
        "SELECT f.code, f.d, f.v FROM ish_bf f JOIN ish_bd x ON x.code = f.code \
          WHERE x.status = 'validated'",
        "code,d",
        "!ish_bd",
        "'code'",
    );
    assert_eq!(ish_heal_triggers_on("ish_bd"), 0, "char(n) against text gets no heal");
}

/// N3: a key no row of the partition column's type can hold (an int8 id beyond
/// an int4 partition column) has nothing to rebuild; it drains.
#[pg_test]
fn ish_key_outside_the_partition_type_drains() {
    Spi::run("CREATE TABLE ish_od (id BIGINT PRIMARY KEY, status TEXT NOT NULL)").expect("od");
    Spi::run("INSERT INTO ish_od VALUES (1, 'validated')").expect("seed od");
    Spi::run(
        "CREATE TABLE ish_of (pid INT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (pid)",
    )
    .expect("of");
    Spi::run("CREATE TABLE ish_of_1 PARTITION OF ish_of FOR VALUES IN (1)").expect("leaf");
    Spi::run("INSERT INTO ish_of VALUES (1, 1, 10)").expect("seed of");
    let query = "SELECT f.pid, f.d, f.v FROM ish_of f JOIN ish_od x ON x.id = f.pid \
                  WHERE x.status = 'validated'";
    ish_create("ish_o_imv", query, "pid,d", "!ish_od", "'pid'");

    Spi::run("INSERT INTO ish_od VALUES (3000000000, 'validated')").expect("id beyond int4");
    assert_eq!(ish_queued("ish_o_imv"), 1, "precondition: key queued");
    let result = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()")
        .expect("heal")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "an impossible key is not a failure: {result}");
    assert_eq!(ish_queued("ish_o_imv"), 0, "it drains");
    assert!(!ish_status_stale("ish_o_imv").0, "and the IMV reports fresh");
    assert_eq!(ish_diverging("ish_o_imv", "pid, d, v", query), 0, "which it is");
}

/// N4: the cascade to a dependent IMV must not lose a key's exact text.
#[pg_test]
fn ish_key_with_edge_whitespace_heals_dependents() {
    Spi::run("CREATE TABLE ish_wd (code TEXT PRIMARY KEY, status TEXT NOT NULL)").expect("wd");
    Spi::run("INSERT INTO ish_wd VALUES (' a', 'validated'), ('c', 'validated')").expect("seed wd");
    Spi::run(
        "CREATE TABLE ish_wf (code TEXT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (code)",
    )
    .expect("wf");
    Spi::run("CREATE TABLE ish_wf_a PARTITION OF ish_wf FOR VALUES IN (' a')").expect("a");
    Spi::run("CREATE TABLE ish_wf_c PARTITION OF ish_wf FOR VALUES IN ('c')").expect("c");
    Spi::run("INSERT INTO ish_wf VALUES (' a', 1, 10), (' a', 2, 11), ('c', 1, 20)").expect("seed");
    ish_create(
        "ish_w_imv",
        "SELECT f.code, f.d, f.v FROM ish_wf f JOIN ish_wd x ON x.code = f.code \
          WHERE x.status = 'validated'",
        "code,d",
        "!ish_wd",
        "'code'",
    );
    let r = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('ish_w_dep', \
           'SELECT code, sum(v) AS s, count(*) AS n FROM ish_w_imv GROUP BY code', \
           NULL, 'UNLOGGED', 'DEFERRED', NULL, ARRAY['code'])",
    )
    .expect("dependent create call")
    .expect("dependent create result");
    assert!(!r.starts_with("ERROR"), "dependent create returned: {r}");

    Spi::run("UPDATE ish_wd SET status = 'draft' WHERE code = ' a'").expect("exclude ' a'");
    let result = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()")
        .expect("heal")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "heal returned: {result}");
    assert_eq!(
        ish_diverging(
            "ish_w_dep",
            "code, s, n",
            "SELECT code, sum(v) AS s, count(*) AS n FROM ish_w_imv GROUP BY code"
        ),
        0,
        "the dependent must follow its healed parent"
    );
}

fn ish_registry_ctid(imv: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT ctid::text FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
    ))
    .expect("ctid query")
    .expect("registry row")
}

/// N5: writes to an ignored source whose watched column went missing must not
/// write the registry row; every writer would serialize (and deadlock) on it.
#[pg_test]
fn ish_missing_column_leaves_the_registry_row_alone() {
    swi_build_fixture("validated");
    Spi::run("ALTER TABLE swi_dp RENAME COLUMN status TO state").expect("rename watched column");
    let before = ish_registry_ctid("swi_imv");
    Spi::run("UPDATE swi_dp SET state = 'draft' WHERE id = 471").expect("update");
    Spi::run("UPDATE swi_dp SET state = 'draft' WHERE id = 9").expect("update another row");
    Spi::run("INSERT INTO swi_dp VALUES (12, 'validated')").expect("insert");
    assert_eq!(
        ish_registry_ctid("swi_imv"),
        before,
        "the writes must not update the IMV's registry row"
    );
    assert!(ish_status_stale("swi_imv").0, "the lost heal is still reported");
    let doctor_rows = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM reflex_doctor() WHERE check_id = 'F14' AND object = 'swi_imv'",
    )
    .expect("doctor")
    .unwrap_or(0);
    assert_eq!(doctor_rows, 1, "and reflex_doctor reports it");
}

/// N6: a query cancel (or statement_timeout) during a heal must stop the call,
/// not be recorded as one IMV's failure while the sweep carries on.
#[pg_test(error = "ish cancel probe")]
fn ish_a_cancel_during_a_heal_is_not_swallowed() {
    ish_gated_fixture();
    Spi::run("UPDATE ish_gd SET status = 'cancel' WHERE id = 1").expect("queue");
    let _ = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()");
}

fn ish_registry_stale_reason(imv: &str) -> Option<String> {
    Spi::get_one::<String>(&format!(
        "SELECT stale_reason FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
    ))
    .expect("stale_reason query")
}

/// N7: a TRUNCATE adds its reason to an IMV already stale for another cause.
#[pg_test]
fn ish_truncate_keeps_an_existing_stale_reason() {
    swi_build_fixture("validated");
    Spi::run("INSERT INTO swi_ss VALUES (471, '2026-02-10', 1, 1, 10)").expect("duplicate row");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("failing deferred flush");
    let earlier = ish_registry_stale_reason("swi_imv")
        .expect("precondition: the failed flush left a stale_reason");

    Spi::run("TRUNCATE swi_dp").expect("truncate ignored source");
    let reason = ish_registry_stale_reason("swi_imv").unwrap_or_default();
    assert!(reason.contains(&earlier), "the earlier reason must survive: {reason}");
    assert!(reason.contains("truncated"), "and the truncate is added: {reason}");
}

/// N7: a TRUNCATE stamps `stale_since` like every other staleness writer.
#[pg_test]
fn ish_truncate_stamps_stale_since() {
    swi_build_fixture("validated");
    Spi::run("TRUNCATE swi_dp").expect("truncate ignored source");
    let stamped = Spi::get_one::<bool>(
        "SELECT stale_since IS NOT NULL FROM public.__reflex_ivm_reference WHERE name = 'swi_imv'",
    )
    .expect("stale_since query")
    .unwrap_or(false);
    assert!(stamped, "stale_since must be set");
}

/// N1: a type owner's cast to text must neither run inside the heal trigger nor
/// decide whether a watched column changed (a constant cast would hide it).
#[pg_test]
fn ish_type_owner_cast_neither_runs_nor_hides_a_change() {
    swi_build_fixture("validated");
    Spi::run("CREATE TABLE ish_cast_probe (who TEXT)").expect("probe");
    Spi::run("GRANT INSERT ON ish_cast_probe TO PUBLIC").expect("probe grant");
    Spi::run("CREATE TYPE ish_tone AS ENUM ('ok', 'sad')").expect("type");
    Spi::run(
        "CREATE FUNCTION ish_tone_text(ish_tone) RETURNS TEXT LANGUAGE plpgsql AS $$ \
         BEGIN INSERT INTO public.ish_cast_probe VALUES (current_user); RETURN 'tone'; END $$",
    )
    .expect("cast function");
    Spi::run("CREATE CAST (ish_tone AS TEXT) WITH FUNCTION ish_tone_text(ish_tone)").expect("cast");
    Spi::run("ALTER TABLE swi_dp ADD COLUMN tone ish_tone NOT NULL DEFAULT 'ok'").expect("column");
    ish_create(
        "ish_tone_imv",
        "SELECT ss.dem_plan_id, ss.order_date, ss.product_id, ss.location_id, ss.qty \
           FROM swi_ss ss JOIN swi_dp dp ON dp.id = ss.dem_plan_id \
          WHERE dp.status IN ('validated', 'draft') AND dp.tone = 'ok'",
        "dem_plan_id,order_date,product_id,location_id",
        "!swi_dp",
        "'dem_plan_id','order_date'",
    );

    Spi::run("UPDATE swi_dp SET tone = 'sad' WHERE id = 471").expect("update");
    assert_eq!(ish_queued("ish_tone_imv"), 1, "the change is seen through the type's output");
    let ran = Spi::get_one::<i64>("SELECT count(*)::int8 FROM ish_cast_probe")
        .expect("probe query")
        .unwrap_or(-1);
    assert_eq!(ran, 0, "the heal trigger must not evaluate the type owner's cast");
}

/// N1: a key of a user-defined type is not mapped: rendering it would consult
/// that type's owner-defined casts.
#[pg_test]
fn ish_user_typed_key_is_not_mapped() {
    Spi::run("CREATE TYPE ish_ek AS ENUM ('a', 'b')").expect("type");
    Spi::run("CREATE TABLE ish_ed (code ish_ek PRIMARY KEY, status TEXT NOT NULL)").expect("ed");
    Spi::run("INSERT INTO ish_ed VALUES ('a', 'validated'), ('b', 'validated')").expect("seed ed");
    Spi::run(
        "CREATE TABLE ish_ef (code ish_ek NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (code)",
    )
    .expect("ef");
    Spi::run("CREATE TABLE ish_ef_a PARTITION OF ish_ef FOR VALUES IN ('a')").expect("a");
    Spi::run("CREATE TABLE ish_ef_b PARTITION OF ish_ef FOR VALUES IN ('b')").expect("b");
    Spi::run("INSERT INTO ish_ef VALUES ('a', 1, 10), ('b', 1, 20)").expect("seed ef");
    ish_create(
        "ish_e_imv",
        "SELECT f.code, f.d, f.v FROM ish_ef f JOIN ish_ed x ON x.code = f.code \
          WHERE x.status = 'validated'",
        "code,d",
        "!ish_ed",
        "'code'",
    );
    assert_eq!(ish_heal_triggers_on("ish_ed"), 0, "a user-typed key gets no heal");
}

/// X1: every heal function is a trigger function, which no role can call
/// directly; a callable SECURITY DEFINER helper ran a caller-chosen relation's
/// code with pg_reflex's rights.
#[pg_test]
fn ish_no_heal_function_is_directly_callable() {
    let callable = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM pg_proc \
          WHERE starts_with(proname, '__reflex_heal') AND prorettype <> 'trigger'::regtype",
    )
    .expect("pg_proc query")
    .unwrap_or(-1);
    assert_eq!(callable, 0, "a heal function other than the trigger is callable");
    let triggers = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM pg_proc \
          WHERE proname = '__reflex_heal_on_ignored_change' AND prorettype = 'trigger'::regtype",
    )
    .expect("pg_proc query")
    .unwrap_or(-1);
    assert_eq!(triggers, 1, "control: the heal trigger function exists");
}

/// An IMV over `ish_nf` keeping the partitions whose `ish_nd.note` is NULL.
fn ish_null_note_fixture() -> &'static str {
    Spi::run("CREATE TABLE ish_nd (id BIGINT PRIMARY KEY, note TEXT)").expect("nd");
    Spi::run("INSERT INTO ish_nd VALUES (1, NULL), (2, NULL)").expect("seed nd");
    Spi::run(
        "CREATE TABLE ish_nf (pid BIGINT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (pid)",
    )
    .expect("nf");
    Spi::run("CREATE TABLE ish_nf_1 PARTITION OF ish_nf FOR VALUES IN (1)").expect("leaf 1");
    Spi::run("CREATE TABLE ish_nf_2 PARTITION OF ish_nf FOR VALUES IN (2)").expect("leaf 2");
    Spi::run("INSERT INTO ish_nf VALUES (1, 1, 10), (2, 1, 20)").expect("seed nf");
    let query = "SELECT f.pid, f.d, f.v FROM ish_nf f JOIN ish_nd x ON x.id = f.pid \
                  WHERE x.note IS NULL";
    ish_create("ish_n_imv", query, "pid,d", "!ish_nd", "'pid'");
    query
}

/// X2: a watched column moving between NULL and '' changes the IMV, so it must
/// be queued; both render as '' through `format('%s')`.
#[pg_test]
fn ish_watched_column_null_to_empty_is_queued() {
    let query = ish_null_note_fixture();
    Spi::run("UPDATE ish_nd SET note = '' WHERE id = 1").expect("NULL to ''");
    assert_eq!(ish_queued("ish_n_imv"), 1, "the NULL-to-'' change must be queued");
    assert!(ish_status_stale("ish_n_imv").0, "and reported stale until healed");

    let result = Spi::get_one::<String>("SELECT reflex_heal_ignored_sources()")
        .expect("heal")
        .unwrap_or_default();
    assert!(!result.starts_with("ERROR"), "heal returned: {result}");
    assert_eq!(ish_diverging("ish_n_imv", "pid, d, v", query), 0, "the heal restores the IMV");

    Spi::run("UPDATE ish_nd SET note = NULL WHERE id = 1").expect("'' to NULL");
    assert_eq!(ish_queued("ish_n_imv"), 1, "the reverse change must be queued too");
}

/// X4: `TRUNCATE ONLY` of an inheritance parent empties the parent while its
/// children keep rows; the IMV is still wrong and must report stale.
#[pg_test]
fn ish_truncate_only_of_an_inheritance_parent_marks_the_imv_stale() {
    swi_build_fixture("validated");
    Spi::run("CREATE TABLE swi_dp_extra () INHERITS (swi_dp)").expect("inheritance child");
    Spi::run("INSERT INTO swi_dp_extra VALUES (5, 'draft')").expect("child row");

    Spi::run("TRUNCATE ONLY swi_dp").expect("truncate only the parent");
    let (stale, reason) = ish_status_stale("swi_imv");
    assert!(stale, "the parent's rows are gone from the query, so the IMV is wrong");
    assert!(
        reason.unwrap_or_default().contains("truncated"),
        "the reason names the truncate"
    );
}

/// X3: writing the ignored source must need nothing beyond the privileges on
/// that source, even where schema `public` is closed to ordinary roles.
#[pg_test]
fn ish_writer_without_public_schema_usage_can_write_the_ignored_source() {
    Spi::run("CREATE SCHEMA ish_app").expect("schema");
    Spi::run("CREATE TABLE ish_app.hd (id BIGINT PRIMARY KEY, status TEXT NOT NULL)").expect("hd");
    Spi::run("INSERT INTO ish_app.hd VALUES (1, 'validated'), (2, 'validated')").expect("seed hd");
    Spi::run(
        "CREATE TABLE ish_app.hf (pid BIGINT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (pid)",
    )
    .expect("hf");
    Spi::run("CREATE TABLE ish_app.hf_1 PARTITION OF ish_app.hf FOR VALUES IN (1)").expect("1");
    Spi::run("CREATE TABLE ish_app.hf_2 PARTITION OF ish_app.hf FOR VALUES IN (2)").expect("2");
    Spi::run("INSERT INTO ish_app.hf VALUES (1, 1, 10), (2, 1, 20)").expect("seed hf");
    ish_create(
        "ish_app.h_imv",
        "SELECT f.pid, f.d, f.v FROM ish_app.hf f JOIN ish_app.hd x ON x.id = f.pid \
          WHERE x.status = 'validated'",
        "pid,d",
        "!ish_app.hd",
        "'pid'",
    );
    assert_eq!(ish_heal_triggers_on("ish_app.hd"), 4, "precondition: the source is healed");

    Spi::run("CREATE ROLE ish_hardened").expect("role");
    Spi::run("GRANT USAGE ON SCHEMA ish_app TO ish_hardened").expect("schema usage");
    Spi::run("GRANT SELECT, INSERT, UPDATE, DELETE ON ish_app.hd TO ish_hardened").expect("grants");
    Spi::run("REVOKE ALL ON SCHEMA public FROM PUBLIC").expect("harden public");

    Spi::run("SET ROLE ish_hardened").expect("become the writer");
    let update = Spi::run("UPDATE ish_app.hd SET status = 'draft' WHERE id = 1");
    Spi::run("RESET ROLE").expect("reset role");
    update.expect("the writer must be able to write the ignored source");

    let queued = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM public.__reflex_heal_pending WHERE imv_name LIKE '%h_imv'",
    )
    .expect("queue query")
    .unwrap_or(-1);
    assert_eq!(queued, 1, "and its change must still be queued");
}

/// A key column retyped to a user type after install must not have its owner's
/// cast run by the heal trigger, and the IMV must report that it can no longer
/// be healed.
#[pg_test]
fn ish_key_retyped_to_a_user_type_runs_no_user_code_and_reports_unhealable() {
    Spi::run("CREATE TABLE ish_kd (code TEXT PRIMARY KEY, status TEXT NOT NULL)").expect("kd");
    Spi::run("INSERT INTO ish_kd VALUES ('a', 'validated'), ('b', 'validated')").expect("seed kd");
    Spi::run(
        "CREATE TABLE ish_kf (code TEXT NOT NULL, d INT NOT NULL, v INT NOT NULL) \
         PARTITION BY LIST (code)",
    )
    .expect("kf");
    Spi::run("CREATE TABLE ish_kf_a PARTITION OF ish_kf FOR VALUES IN ('a')").expect("a");
    Spi::run("CREATE TABLE ish_kf_b PARTITION OF ish_kf FOR VALUES IN ('b')").expect("b");
    Spi::run("INSERT INTO ish_kf VALUES ('a', 1, 10), ('b', 1, 20)").expect("seed kf");
    ish_create(
        "ish_k_imv",
        "SELECT f.code, f.d, f.v FROM ish_kf f JOIN ish_kd x ON x.code = f.code \
          WHERE x.status = 'validated'",
        "code,d",
        "!ish_kd",
        "'code'",
    );
    assert_eq!(ish_heal_triggers_on("ish_kd"), 4, "precondition: the source is healed");
    assert!(!ish_status_stale("ish_k_imv").0, "precondition: fresh");

    Spi::run("CREATE TABLE ish_key_probe (who TEXT)").expect("probe");
    Spi::run("GRANT INSERT ON ish_key_probe TO PUBLIC").expect("probe grant");
    Spi::run("CREATE TYPE ish_kk AS ENUM ('a', 'b')").expect("type");
    Spi::run(
        "CREATE FUNCTION ish_kk_json(ish_kk) RETURNS json LANGUAGE plpgsql AS $$ \
         BEGIN INSERT INTO public.ish_key_probe VALUES (current_user); \
               RETURN to_json($1::text); END $$",
    )
    .expect("cast function");
    Spi::run("CREATE CAST (ish_kk AS json) WITH FUNCTION ish_kk_json(ish_kk)").expect("cast");
    Spi::run("ALTER TABLE ish_kd ALTER COLUMN code TYPE ish_kk USING code::ish_kk")
        .expect("retype the key");
    Spi::run("CREATE ROLE ish_key_writer").expect("role");
    Spi::run("GRANT USAGE ON SCHEMA public TO ish_key_writer").expect("schema usage");
    Spi::run("GRANT SELECT, UPDATE ON ish_kd TO ish_key_writer").expect("table grants");

    Spi::run("SET ROLE ish_key_writer").expect("become the writer");
    let update = Spi::run("UPDATE ish_kd SET status = 'draft' WHERE code = 'a'");
    Spi::run("RESET ROLE").expect("reset role");
    update.expect("the writer can update the ignored source");

    let foreign = Spi::get_one::<i64>(
        "SELECT count(*)::int8 FROM ish_key_probe WHERE who <> 'ish_key_writer'",
    )
    .expect("probe query")
    .unwrap_or(-1);
    assert_eq!(foreign, 0, "user-defined code must not run with another role's rights");
    let (stale, reason) = ish_status_stale("ish_k_imv");
    assert!(stale, "a change the heal cannot queue leaves the IMV wrong");
    assert!(
        reason.unwrap_or_default().contains("ish_kd"),
        "the reason names the ignored source"
    );
}
