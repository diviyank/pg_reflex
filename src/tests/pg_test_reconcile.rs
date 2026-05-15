
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

#[pg_test]
fn test_reconcile_preserves_unlogged_persistence() {
    Spi::run("CREATE TABLE rec_unlog_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO rec_unlog_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .expect("seed");
    crate::create_reflex_ivm(
        "rec_unlog_view",
        "SELECT grp, SUM(val) AS total FROM rec_unlog_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Defaults: intermediate UNLOGGED, target UNLOGGED.
    let int_persist_before: &str = Spi::get_one(
        "SELECT relpersistence::TEXT FROM pg_class WHERE relname = '__reflex_intermediate_rec_unlog_view'",
    ).expect("q").expect("v");
    assert_eq!(int_persist_before, "u", "intermediate must default to UNLOGGED");

    let tgt_persist_before: &str = Spi::get_one(
        "SELECT relpersistence::TEXT FROM pg_class WHERE relname = 'rec_unlog_view'",
    ).expect("q").expect("v");
    assert_eq!(tgt_persist_before, "u", "target must default to UNLOGGED");

    assert_eq!(crate::reflex_reconcile("rec_unlog_view"), "RECONCILED");

    let int_persist_after: &str = Spi::get_one(
        "SELECT relpersistence::TEXT FROM pg_class WHERE relname = '__reflex_intermediate_rec_unlog_view'",
    ).expect("q").expect("v");
    assert_eq!(int_persist_after, "u", "intermediate persistence must be preserved after reconcile");

    let tgt_persist_after: &str = Spi::get_one(
        "SELECT relpersistence::TEXT FROM pg_class WHERE relname = 'rec_unlog_view'",
    ).expect("q").expect("v");
    assert_eq!(tgt_persist_after, "u", "target persistence must be preserved after reconcile");
}

#[pg_test]
fn test_reconcile_preserves_fillfactor_70() {
    Spi::run("CREATE TABLE rec_ff_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO rec_ff_src (grp, val) VALUES ('a', 1)").expect("seed");
    crate::create_reflex_ivm(
        "rec_ff_view",
        "SELECT grp, SUM(val) AS total FROM rec_ff_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    assert_eq!(crate::reflex_reconcile("rec_ff_view"), "RECONCILED");

    let int_opts: Option<Vec<Option<String>>> = Spi::get_one(
        "SELECT reloptions FROM pg_class WHERE relname = '__reflex_intermediate_rec_ff_view'",
    ).expect("q");
    let has_ff70_int = int_opts
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .any(|s| s == "fillfactor=70");
    assert!(has_ff70_int, "intermediate must keep fillfactor=70 after reconcile");

    let tgt_opts: Option<Vec<Option<String>>> = Spi::get_one(
        "SELECT reloptions FROM pg_class WHERE relname = 'rec_ff_view'",
    ).expect("q");
    let has_ff70_tgt = tgt_opts
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .any(|s| s == "fillfactor=70");
    assert!(has_ff70_tgt, "target must keep fillfactor=70 after reconcile");
}

#[pg_test]
fn test_reconcile_preserves_reflex_indexes() {
    Spi::run("CREATE TABLE rec_idx_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO rec_idx_src (grp, val) VALUES ('a', 1), ('b', 2)").expect("seed");
    crate::create_reflex_ivm(
        "rec_idx_view",
        "SELECT grp, SUM(val) AS total FROM rec_idx_src GROUP BY grp",
        None, None, None, None,
    );

    let int_idx_before: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM pg_indexes \
         WHERE tablename = '__reflex_intermediate_rec_idx_view' AND indexname LIKE 'idx__reflex_int_%'",
    ).expect("q").expect("v");
    assert!(int_idx_before >= 1, "reflex intermediate index must exist before reconcile");

    let tgt_idx_before: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM pg_indexes \
         WHERE tablename = 'rec_idx_view' AND indexname LIKE 'idx__reflex_target_%'",
    ).expect("q").expect("v");
    assert!(tgt_idx_before >= 1, "reflex target index must exist before reconcile");

    assert_eq!(crate::reflex_reconcile("rec_idx_view"), "RECONCILED");

    let int_idx_after: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM pg_indexes \
         WHERE tablename = '__reflex_intermediate_rec_idx_view' AND indexname LIKE 'idx__reflex_int_%'",
    ).expect("q").expect("v");
    assert_eq!(int_idx_after, int_idx_before, "reflex intermediate indexes count must match after reconcile");

    let tgt_idx_after: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM pg_indexes \
         WHERE tablename = 'rec_idx_view' AND indexname LIKE 'idx__reflex_target_%'",
    ).expect("q").expect("v");
    assert_eq!(tgt_idx_after, tgt_idx_before, "reflex target indexes count must match after reconcile");
}

#[pg_test]
fn test_reconcile_preserves_user_target_index() {
    Spi::run("CREATE TABLE rec_uidx_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO rec_uidx_src (grp, val) VALUES ('a', 1), ('b', 2)").expect("seed");
    crate::create_reflex_ivm(
        "rec_uidx_view",
        "SELECT grp, SUM(val) AS total FROM rec_uidx_src GROUP BY grp",
        None, None, None, None,
    );
    Spi::run("CREATE INDEX my_custom_total_idx ON rec_uidx_view (total)")
        .expect("create user index");

    let user_idx_before: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM pg_indexes \
         WHERE tablename = 'rec_uidx_view' AND indexname = 'my_custom_total_idx'",
    ).expect("q").expect("v");
    assert_eq!(user_idx_before, 1);

    assert_eq!(crate::reflex_reconcile("rec_uidx_view"), "RECONCILED");

    let user_idx_after: i64 = Spi::get_one(
        "SELECT COUNT(*)::BIGINT FROM pg_indexes \
         WHERE tablename = 'rec_uidx_view' AND indexname = 'my_custom_total_idx'",
    ).expect("q").expect("v");
    assert_eq!(user_idx_after, 1, "user-created target index must survive reconcile");
}

#[pg_test]
fn test_reconcile_swaps_physical_tables() {
    Spi::run("CREATE TABLE rec_oid_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO rec_oid_src (grp, val) VALUES ('a', 1)").expect("seed");
    crate::create_reflex_ivm(
        "rec_oid_view",
        "SELECT grp, SUM(val) AS total FROM rec_oid_src GROUP BY grp",
        None, None, None, None,
    );

    let int_oid_before: pgrx::pg_sys::Oid = Spi::get_one(
        "SELECT oid FROM pg_class WHERE relname = '__reflex_intermediate_rec_oid_view'",
    ).expect("q").expect("v");

    assert_eq!(crate::reflex_reconcile("rec_oid_view"), "RECONCILED");

    let int_oid_after: pgrx::pg_sys::Oid = Spi::get_one(
        "SELECT oid FROM pg_class WHERE relname = '__reflex_intermediate_rec_oid_view'",
    ).expect("q").expect("v");

    assert_ne!(
        int_oid_before, int_oid_after,
        "CTAS+rename should swap the physical intermediate table (OID changes)"
    );
}

#[pg_test]
fn test_reconcile_aggregate_then_incremental_update() {
    Spi::run("CREATE TABLE rec_inc_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO rec_inc_src (grp, val) VALUES ('A', 10), ('B', 20)").expect("seed");
    crate::create_reflex_ivm(
        "rec_inc_view",
        "SELECT grp, SUM(val) AS total FROM rec_inc_src GROUP BY grp",
        None, None, None, None,
    );

    assert_eq!(crate::reflex_reconcile("rec_inc_view"), "RECONCILED");

    Spi::run("INSERT INTO rec_inc_src (grp, val) VALUES ('A', 5)").expect("post insert");

    let a: pgrx::AnyNumeric = Spi::get_one("SELECT total FROM rec_inc_view WHERE grp = 'A'")
        .expect("q").expect("v");
    assert_eq!(a.to_string(), "15", "incremental UPDATE after reconcile must work");
}
