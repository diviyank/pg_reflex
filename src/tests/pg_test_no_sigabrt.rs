// Regression test: MERGE inside a trigger must not crash the backend with SIGABRT.
//
// Root cause: MERGE INTO ... USING (<SELECT FROM transition_table>) executed via EXECUTE
// inside a PL/pgSQL trigger body trips a PostgreSQL internal assertion in cassert builds,
// causing abort(). Fix: materialize the delta into a scratch table first, then MERGE FROM
// the scratch table (a plain relation, not a transition table).
//
// In a cassert-enabled build (the environment used by `cargo pgrx test`), any SIGABRT
// would terminate the test process immediately — surviving these DML operations IS the test.

#[pg_test]
fn test_trigger_fired_merge_does_not_crash_backend() {
    Spi::run("CREATE TABLE sigabrt_t (city TEXT, amount INT)").expect("create table");
    Spi::run("INSERT INTO sigabrt_t VALUES ('paris', 10), ('berlin', 20), ('paris', 30)")
        .expect("seed");

    let r = crate::create_reflex_ivm(
        "sigabrt_v",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS __ivm_count FROM sigabrt_t GROUP BY city",
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW", "create IMV: {}", r);

    // INSERT — fires the trigger, which must execute MERGE from scratch (not inline subquery).
    Spi::run("INSERT INTO sigabrt_t VALUES ('london', 5)").expect("INSERT");
    assert_imv_correct(
        "sigabrt_v",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS __ivm_count FROM sigabrt_t GROUP BY city",
    );

    // UPDATE — exercises the UPDATE path of the trigger.
    Spi::run("UPDATE sigabrt_t SET amount = amount + 1 WHERE city = 'paris'").expect("UPDATE");
    assert_imv_correct(
        "sigabrt_v",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS __ivm_count FROM sigabrt_t GROUP BY city",
    );

    // DELETE — exercises the DELETE path with dead-group cleanup.
    Spi::run("DELETE FROM sigabrt_t WHERE city = 'london'").expect("DELETE");
    assert_imv_correct(
        "sigabrt_v",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS __ivm_count FROM sigabrt_t GROUP BY city",
    );
}

// Regression test for chained IMVs: a passthrough IMV whose source is another IMV
// previously crashed (SIGABRT) because its trigger — fired nested from the upstream
// aggregate's DELETE/INSERT target refresh — EXECUTE'd DML that read transition
// tables (both `DELETE ... WHERE IN (SELECT ... FROM __reflex_old_L1)` and
// `INSERT ... SELECT ... FROM __reflex_new_L1`). The fix materializes both
// transitions into per-(IMV, source) UNLOGGED scratches before any downstream DML.
#[pg_test]
fn test_chained_passthrough_does_not_crash_backend() {
    Spi::run("CREATE TABLE chain_src (city TEXT, amount INT)").expect("create src");
    Spi::run("INSERT INTO chain_src VALUES ('paris', 10), ('berlin', 20)").expect("seed");

    let r1 = crate::create_reflex_ivm(
        "chain_l1",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM chain_src GROUP BY city",
        None,
        None,
        None,
    );
    assert_eq!(r1, "CREATE REFLEX INCREMENTAL VIEW", "create L1: {}", r1);

    let r2 = crate::create_reflex_ivm(
        "chain_l2",
        "SELECT city, total, cnt FROM chain_l1",
        Some("city"),
        None,
        None,
    );
    assert_eq!(r2, "CREATE REFLEX INCREMENTAL VIEW", "create L2: {}", r2);

    // All four nested-trigger paths that used to crash:
    Spi::run("INSERT INTO chain_src VALUES ('london', 5)").expect("INSERT new group");
    assert_imv_correct(
        "chain_l2",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM chain_src GROUP BY city",
    );

    Spi::run("UPDATE chain_src SET amount = amount + 100 WHERE city = 'paris'")
        .expect("UPDATE same group");
    assert_imv_correct(
        "chain_l2",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM chain_src GROUP BY city",
    );

    Spi::run("UPDATE chain_src SET city = 'north' WHERE city = 'berlin'")
        .expect("UPDATE group change");
    assert_imv_correct(
        "chain_l2",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM chain_src GROUP BY city",
    );

    Spi::run("DELETE FROM chain_src WHERE city = 'london'").expect("DELETE");
    assert_imv_correct(
        "chain_l2",
        "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM chain_src GROUP BY city",
    );
}
