
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

    crate::create_reflex_ivm("recon_pt_view", "SELECT id, name FROM recon_pt_src", None, None, None);

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
    );
    crate::create_reflex_ivm(
        "rdep_v2",
        "SELECT grp, COUNT(*) AS cnt FROM rdep_src GROUP BY grp",
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
