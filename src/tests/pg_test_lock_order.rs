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

const LO_IGNORING_IMV_SQL: &str = "SELECT s.k, s.v FROM lo_src s JOIN lo_dim d ON d.k = s.k";

/// One operator entry point under test: the call, the shape of the IMV it runs
/// on, and the prefix of the result a healthy call returns.
struct LoCase {
    dbname: &'static str,
    operator_call: &'static str,
    with_dependent: bool,
    ignores_dimension: bool,
    expected_result: &'static str,
}

impl LoCase {
    fn imv_sql(&self) -> &'static str {
        if self.ignores_dimension {
            LO_IGNORING_IMV_SQL
        } else {
            LO_IMV_SQL
        }
    }

    fn create_imv(&self) {
        worker_exec("CREATE TABLE lo_src (k INT PRIMARY KEY, v INT)");
        worker_exec("INSERT INTO lo_src SELECT g, g FROM generate_series(1, 100) g");
        if self.ignores_dimension {
            worker_exec("CREATE TABLE lo_dim (k INT PRIMARY KEY)");
            worker_exec("INSERT INTO lo_dim SELECT g FROM generate_series(1, 100) g");
            let created = rbc_select(&format!(
                "create_reflex_ivm('lo_v', {}, 'k', 'UNLOGGED', 'DEFERRED', '!lo_dim')",
                sql_lit(self.imv_sql())
            ));
            assert!(!created.starts_with("ERROR"), "create lo_v: {created}");
        } else {
            rbc_create_deferred("lo_v", self.imv_sql(), "k");
        }
        if self.with_dependent {
            rbc_create_deferred("lo_dependent", "SELECT k, v FROM lo_v", "k");
        }
    }
}

/// The worker holds IMV `lo_v`'s advisory lock in an open transaction while an
/// operator call on a second connection queues behind it; the worker then writes
/// the IMV's registry row, as a COMMIT-time flush does under that lock. The write
/// must not wait on the queued operator. A rebuild only takes the IMV's own lock
/// from its entry point, so cases cover IMVs with and without dependents.
fn lo_operator_waits_while_worker_writes_registry_row(case: &LoCase) {
    probe_db_open(case.dbname);
    case.create_imv();
    lo_connect("lo_operator", case.dbname);

    worker_exec("BEGIN");
    rbc_select("pg_advisory_xact_lock(hashtext('lo_v'), hashtext(reverse('lo_v')))");
    lo_send("lo_operator", case.operator_call);
    let operator_queued = lo_await_lock_wait(case.operator_call);
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
        operator_result.starts_with(case.expected_result),
        "operator returned {operator_result}"
    );
    assert_eq!(rbc_mismatch("lo_v", case.imv_sql()), 0);
    if case.with_dependent {
        assert_eq!(rbc_mismatch("lo_dependent", LO_IMV_SQL), 0);
    }
    lo_disconnect("lo_operator");
    probe_db_close(case.dbname);
}

#[pg_test]
fn pg_lock_order_rebuild_imv_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(&LoCase {
        dbname: "reflex_lo_rebuild",
        operator_call: "SELECT reflex_rebuild_imv('lo_v')",
        with_dependent: true,
        ignores_dimension: false,
        expected_result: "RECONCILED",
    });
}

/// The invariant holds regardless of dependents: the lock comes from
/// `stamp_targeted_recovery`, not from the dependent-propagating rebuild.
#[pg_test]
fn pg_lock_order_rebuild_imv_without_dependents_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(&LoCase {
        dbname: "reflex_lo_nodep",
        operator_call: "SELECT reflex_rebuild_imv('lo_v')",
        with_dependent: false,
        ignores_dimension: false,
        expected_result: "RECONCILED",
    });
}

#[pg_test]
fn pg_lock_order_reconcile_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(&LoCase {
        dbname: "reflex_lo_reconcile",
        operator_call: "SELECT reflex_reconcile('lo_v')",
        with_dependent: true,
        ignores_dimension: false,
        expected_result: "RECONCILED",
    });
}

#[pg_test]
fn pg_lock_order_reconcile_two_arg_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(&LoCase {
        dbname: "reflex_lo_reconcile2",
        operator_call: "SELECT reflex_reconcile('lo_v', TRUE)",
        with_dependent: true,
        ignores_dimension: false,
        expected_result: "RECONCILED",
    });
}

#[pg_test]
fn pg_lock_order_set_wipe_floor_rows_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(&LoCase {
        dbname: "reflex_lo_floor",
        operator_call: "SELECT reflex_set_wipe_floor_rows('lo_v', 100)",
        with_dependent: false,
        ignores_dimension: false,
        expected_result: "OK",
    });
}

#[pg_test]
fn pg_lock_order_set_wipe_threshold_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(&LoCase {
        dbname: "reflex_lo_threshold",
        operator_call: "SELECT reflex_set_wipe_threshold('lo_v', 0.5)",
        with_dependent: false,
        ignores_dimension: false,
        expected_result: "OK",
    });
}

#[pg_test]
fn pg_lock_order_set_partition_dispatch_cost_cap_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(&LoCase {
        dbname: "reflex_lo_costcap",
        operator_call: "SELECT reflex_set_partition_dispatch_cost_cap('lo_v', 1000)",
        with_dependent: false,
        ignores_dimension: false,
        expected_result: "OK",
    });
}

#[pg_test]
fn pg_lock_order_ack_ignore_source_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(&LoCase {
        dbname: "reflex_lo_ack",
        operator_call: "SELECT reflex_ack_ignore_source('lo_v', 'lo_dim')",
        with_dependent: false,
        ignores_dimension: true,
        expected_result: "ACKNOWLEDGED",
    });
}

#[pg_test]
fn pg_lock_order_rebuild_imv_metadata_takes_lock_before_registry_row() {
    lo_operator_waits_while_worker_writes_registry_row(&LoCase {
        dbname: "reflex_lo_metadata",
        operator_call: "SELECT reflex_rebuild_imv_metadata('lo_v')",
        with_dependent: false,
        ignores_dimension: false,
        expected_result: "pg_reflex: rebuilt metadata",
    });
}

/// Chain `lo_u` -> `lo_w`. The worker holds both locks upstream first, the order
/// the flush takes them, while one operator per node queues behind it. Once both
/// are provably queued the worker writes both registry rows; neither write may
/// wait on a queued operator. Both operators then finish without a deadlock and
/// both IMVs are correct.
#[pg_test]
fn pg_lock_order_chain_operators_queue_behind_flush_in_graph_order() {
    const DBNAME: &str = "reflex_lo_chain";
    const W_SQL: &str = "SELECT u.k, u.v + b.w AS s FROM lo_u u JOIN lo_b b ON b.k = u.k";
    const W_FRESH: &str = "SELECT u.k, u.v + b.w FROM lo_src u JOIN lo_b b ON b.k = u.k";
    const OPERATOR_U: &str = "SELECT reflex_rebuild_imv('lo_u')";
    const OPERATOR_W: &str = "SELECT reflex_rebuild_imv('lo_w')";
    probe_db_open(DBNAME);
    worker_exec("CREATE TABLE lo_src (k INT PRIMARY KEY, v INT)");
    worker_exec("CREATE TABLE lo_b (k INT PRIMARY KEY, w INT)");
    worker_exec("INSERT INTO lo_src SELECT g, g FROM generate_series(1, 100) g");
    worker_exec("INSERT INTO lo_b SELECT g, 2 * g FROM generate_series(1, 100) g");
    rbc_create_deferred("lo_u", "SELECT k, v FROM lo_src", "k");
    rbc_create_deferred("lo_w", W_SQL, "k");
    lo_connect("lo_operator_u", DBNAME);
    lo_connect("lo_operator_w", DBNAME);

    worker_exec("BEGIN");
    rbc_select("pg_advisory_xact_lock(hashtext('lo_u'), hashtext(reverse('lo_u')))");
    rbc_select("pg_advisory_xact_lock(hashtext('lo_w'), hashtext(reverse('lo_w')))");
    lo_send("lo_operator_u", OPERATOR_U);
    lo_send("lo_operator_w", OPERATOR_W);
    let both_queued = lo_await_lock_wait(OPERATOR_U) && lo_await_lock_wait(OPERATOR_W);
    let registry_writes = lo_worker_status(
        "SET lock_timeout = '2s'; \
         UPDATE public.__reflex_ivm_reference SET last_update_date = now() \
          WHERE name IN ('lo_u', 'lo_w')",
    );
    worker_exec("COMMIT");
    let result_u = lo_result("lo_operator_u");
    let result_w = lo_result("lo_operator_w");

    assert!(both_queued, "both operators were not observed blocked");
    assert_eq!(
        registry_writes, "UPDATE 2",
        "the flush-order lock holder could not write the chain's registry rows"
    );
    assert_eq!(result_u, "RECONCILED", "operator on lo_u: {result_u}");
    assert_eq!(result_w, "RECONCILED", "operator on lo_w: {result_w}");
    assert_eq!(rbc_mismatch("lo_u", "SELECT k, v FROM lo_src"), 0);
    assert_eq!(rbc_mismatch("lo_w", W_FRESH), 0);
    lo_disconnect("lo_operator_u");
    lo_disconnect("lo_operator_w");
    probe_db_close(DBNAME);
}
