// Operator entry points (`reflex_rebuild_imv`, `reflex_reconcile`) must hold an
// IMV's advisory lock before writing its `__reflex_ivm_reference` row, the order
// the COMMIT-time flush uses. Each test runs in a private database so the
// sessions really commit and block on each other (helpers of
// `pg_test_partition_attach_locks.rs` and `pg_test_rebuild_commit.rs`).

const LO_IMV_SQL: &str = "SELECT k, v FROM lo_src";

fn lo_connect(connection: &str, dbname: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect({}, {})",
        sql_lit(connection),
        sql_lit(&conninfo_for(dbname))
    ))
    .unwrap_or_else(|e| panic!("connect {connection}: {e}"))
    .expect("connect NULL");
}

fn lo_disconnect(connection: &str) {
    let _ = Spi::get_one::<String>(&format!(
        "SELECT dblink_disconnect({})",
        sql_lit(connection)
    ));
}

fn lo_send(connection: &str, sql: &str) {
    Spi::run(&format!(
        "SELECT dblink_send_query({}, {})",
        sql_lit(connection),
        sql_lit(sql)
    ))
    .unwrap_or_else(|e| panic!("send on {connection}: {e}"));
}

/// Result text of an async call, or the error text when the call failed.
fn lo_result(connection: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT COALESCE((SELECT r FROM dblink_get_result({}, false) AS t(r text)), \
                         'ERROR: no result row (call failed)')",
        sql_lit(connection)
    ))
    .expect("get result")
    .unwrap_or_default()
}

/// Status of a statement run on the worker without raising on failure.
fn lo_worker_status(sql: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_exec('reflex_lock_worker', {}, false)",
        sql_lit(sql)
    ))
    .expect("worker status")
    .unwrap_or_default()
}

fn lo_await_lock_wait(query: &str) -> bool {
    Spi::run(
        "CREATE OR REPLACE FUNCTION pg_temp.lo_await_lock_wait(q text) RETURNS bool \
         LANGUAGE plpgsql AS $fn$ \
         BEGIN \
           FOR i IN 1..200 LOOP \
             PERFORM pg_stat_clear_snapshot(); \
             IF EXISTS (SELECT 1 FROM pg_stat_activity \
                        WHERE query = q AND wait_event_type = 'Lock') THEN RETURN TRUE; END IF; \
             PERFORM pg_sleep(0.05); \
           END LOOP; \
           RETURN FALSE; \
         END $fn$",
    )
    .expect("poll fn");
    Spi::get_one::<bool>(&format!(
        "SELECT pg_temp.lo_await_lock_wait({})",
        sql_lit(query)
    ))
    .expect("poll")
    .unwrap_or(false)
}

/// The worker holds IMV `lo_v`'s advisory lock in an open transaction while an
/// operator call on a second connection queues behind it; the worker then writes
/// the IMV's registry row, as a COMMIT-time flush does under that lock. The write
/// must not wait on the queued operator. The IMV has a dependent so the rebuild
/// takes the IMV's advisory lock (a dependent-less rebuild never does).
fn lo_operator_waits_while_worker_writes_registry_row(
    dbname: &str,
    operator_call: &str,
    with_dependent: bool,
    expected_result: &str,
) {
    probe_db_open(dbname);
    worker_exec("CREATE TABLE lo_src (k INT PRIMARY KEY, v INT)");
    worker_exec("INSERT INTO lo_src SELECT g, g FROM generate_series(1, 100) g");
    rbc_create_deferred("lo_v", LO_IMV_SQL, "k");
    if with_dependent {
        rbc_create_deferred("lo_dependent", "SELECT k, v FROM lo_v", "k");
    }
    lo_connect("lo_operator", dbname);

    worker_exec("BEGIN");
    rbc_select("pg_advisory_xact_lock(hashtext('lo_v'), hashtext(reverse('lo_v')))");
    lo_send("lo_operator", operator_call);
    let operator_queued = lo_await_lock_wait(operator_call);
    let registry_write = lo_worker_status(
        "SET lock_timeout = '2s'; \
         UPDATE public.__reflex_ivm_reference SET last_update_date = now() WHERE name = 'lo_v'",
    );
    worker_exec("COMMIT");
    let operator_result = lo_result("lo_operator");

    assert!(operator_queued, "the operator was never observed blocked");
    assert_eq!(
        registry_write, "UPDATE 1",
        "the lock holder could not write the IMV's registry row: the queued operator holds it"
    );
    assert!(
        operator_result.starts_with(expected_result),
        "operator returned {operator_result}"
    );
    assert_eq!(rbc_mismatch("lo_v", LO_IMV_SQL), 0);
    if with_dependent {
        assert_eq!(rbc_mismatch("lo_dependent", LO_IMV_SQL), 0);
    }
    lo_disconnect("lo_operator");
    probe_db_close(dbname);
}

#[pg_test]
fn pg_lock_order_rebuild_imv_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(
        "reflex_lo_rebuild",
        "SELECT reflex_rebuild_imv('lo_v')",
        true,
        "RECONCILED",
    );
}

/// The invariant holds regardless of dependents: the lock comes from
/// `stamp_targeted_recovery`, not from the dependent-propagating rebuild.
#[pg_test]
fn pg_lock_order_rebuild_imv_without_dependents_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(
        "reflex_lo_nodep",
        "SELECT reflex_rebuild_imv('lo_v')",
        false,
        "RECONCILED",
    );
}

#[pg_test]
fn pg_lock_order_setter_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(
        "reflex_lo_setter",
        "SELECT reflex_set_wipe_floor_rows('lo_v', 100)",
        false,
        "OK",
    );
}

#[pg_test]
fn pg_lock_order_reconcile_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(
        "reflex_lo_reconcile",
        "SELECT reflex_reconcile('lo_v')",
        true,
        "RECONCILED",
    );
}

/// Chain `lo_u` -> `lo_w`, both DEFERRED; two operators rebuild one node each
/// while a writer commits a change to both nodes' sources. No session may be a
/// deadlock victim and both IMVs must end correct.
#[pg_test]
fn pg_lock_order_two_operators_and_writer_no_deadlock() {
    const DBNAME: &str = "reflex_lo_chain";
    const W_SQL: &str = "SELECT u.k, u.v + b.w AS s FROM lo_u u JOIN lo_b b ON b.k = u.k";
    const W_FRESH: &str = "SELECT u.k, u.v + b.w FROM lo_src u JOIN lo_b b ON b.k = u.k";
    probe_db_open(DBNAME);
    worker_exec("CREATE TABLE lo_src (k INT PRIMARY KEY, v INT)");
    worker_exec("CREATE TABLE lo_b (k INT PRIMARY KEY, w INT)");
    worker_exec("INSERT INTO lo_src SELECT g, g FROM generate_series(1, 20000) g");
    worker_exec("INSERT INTO lo_b SELECT g, 2 * g FROM generate_series(1, 20000) g");
    rbc_create_deferred("lo_u", "SELECT k, v FROM lo_src", "k");
    rbc_create_deferred("lo_w", W_SQL, "k");
    lo_connect("lo_operator_u", DBNAME);
    lo_connect("lo_operator_w", DBNAME);

    lo_send("lo_operator_u", "SELECT reflex_rebuild_imv('lo_u')");
    lo_send("lo_operator_w", "SELECT reflex_rebuild_imv('lo_w')");
    let writer = lo_worker_status(
        "BEGIN; \
         INSERT INTO lo_src VALUES (900001, 1); \
         INSERT INTO lo_b VALUES (900001, 1); \
         UPDATE lo_src SET v = v + 1 WHERE k <= 50; \
         COMMIT",
    );
    let result_u = lo_result("lo_operator_u");
    let result_w = lo_result("lo_operator_w");

    assert_eq!(writer, "COMMIT", "writer transaction failed");
    assert_eq!(result_u, "RECONCILED", "operator on lo_u: {result_u}");
    assert_eq!(result_w, "RECONCILED", "operator on lo_w: {result_w}");
    assert_eq!(rbc_mismatch("lo_u", "SELECT k, v FROM lo_src"), 0);
    assert_eq!(rbc_mismatch("lo_w", W_FRESH), 0);
    lo_disconnect("lo_operator_u");
    lo_disconnect("lo_operator_w");
    probe_db_close(DBNAME);
}
