// An error caught inside the partition flush, called in the transaction or run
// at COMMIT, leaves the root pending, flags its IMVs and their readers stale,
// and lets the COMMIT succeed; the next flush brings every IMV back to its
// query. Every scenario commits, so a worker session drives it through
// `dblink` in a private database, and a holder session keeps a lock when the
// error is a lock timeout or a deadlock.
//
// The transaction of each scenario is one statement, so it commits with the
// transaction in the implicit state. Under `BEGIN ... COMMIT`, a deferred
// trigger whose subtransaction rolls back at COMMIT trips an assertion of
// PostgreSQL itself (`RollbackAndReleaseCurrentSubTransaction` does not list
// `TBLOCK_END`), which aborts the backends of assert-enabled builds such as
// the ones these tests run on, with or without pg_reflex. Builds without
// assertions run the same code with no check there.

const CFE_WORKER: &str = "cfe_worker";
const CFE_HOLDER: &str = "cfe_holder";

#[derive(Clone, Copy, Debug)]
enum CfeError {
    RefusedDelete,
    CheckViolation,
    ReaderCheckViolation,
    LockTimeout,
    Deadlock,
}

impl CfeError {
    fn is_transient(self) -> bool {
        matches!(self, CfeError::LockTimeout | CfeError::Deadlock)
    }

    fn message_marker(self) -> &'static str {
        match self {
            CfeError::RefusedDelete => "cfe refused",
            CfeError::CheckViolation => "cfe_q_small",
            CfeError::ReaderCheckViolation => "cfe_sq_small",
            CfeError::LockTimeout => "lock timeout",
            CfeError::Deadlock => "deadlock",
        }
    }
}

const CFE_IMVS: [(&str, &str); 3] = [
    ("tv", "SELECT plan, k, q FROM tt"),
    ("tr", "SELECT plan, COUNT(*) AS c, SUM(q) AS sq FROM tt GROUP BY plan"),
    ("tu", "SELECT plan, COUNT(*) AS c, SUM(q) AS sq FROM tt GROUP BY plan"),
];

fn cfe_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn cfe_conninfo(dbname: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT 'host=' || split_part(current_setting('unix_socket_directories'), ',', 1) \
              || ' port=' || current_setting('port') \
              || ' dbname=' || {} \
              || ' user=' || current_user",
        cfe_lit(dbname)
    ))
    .expect("conninfo")
    .expect("conninfo NULL")
}

fn cfe_admin_exec(sql: &str) {
    let here = Spi::get_one::<String>("SELECT current_database()::text")
        .expect("current_database")
        .expect("current_database NULL");
    Spi::get_one::<String>(&format!(
        "SELECT dblink_exec({}, {})",
        cfe_lit(&cfe_conninfo(&here)),
        cfe_lit(sql)
    ))
    .unwrap_or_else(|e| panic!("admin <{sql}>: {e}"));
}

fn cfe_connect(connection: &str, dbname: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect({}, {})",
        cfe_lit(connection),
        cfe_lit(&cfe_conninfo(dbname))
    ))
    .unwrap_or_else(|e| panic!("connect {connection}: {e}"));
}

fn cfe_exec_on(connection: &str, sql: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_exec({}, {})",
        cfe_lit(connection),
        cfe_lit(sql)
    ))
    .unwrap_or_else(|e| panic!("{connection} <{sql}>: {e}"));
}

fn cfe_exec(sql: &str) {
    cfe_exec_on(CFE_WORKER, sql);
}

fn cfe_select(sql: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT v FROM dblink({}, {}) AS t(v text)",
        cfe_lit(CFE_WORKER),
        cfe_lit(&format!("SELECT ({sql})::text"))
    ))
    .unwrap_or_else(|e| panic!("worker select <{sql}>: {e}"))
    .unwrap_or_default()
}

fn cfe_pid(connection: &str) -> i32 {
    Spi::get_one::<i32>(&format!(
        "SELECT p FROM dblink({}, 'SELECT pg_backend_pid()') AS t(p int)",
        cfe_lit(connection)
    ))
    .expect("pid")
    .expect("pid NULL")
}

/// Fails the test, naming the crash, when the server process of the worker
/// died; a new connection shows whether the server came back.
fn cfe_assert_worker_alive(dbname: &str, outcome: &Result<(), String>) {
    if let Err(message) = outcome {
        let fresh = Spi::get_one::<String>(&format!(
            "SELECT v FROM dblink({}, 'SELECT 1::text') AS t(v text)",
            cfe_lit(&cfe_conninfo(dbname))
        ));
        assert!(
            !message.contains("terminated") && !message.contains("closed the connection"),
            "the server process running the transaction died: {message} (new connection: {fresh:?})"
        );
    }
}

fn cfe_error_message() -> String {
    Spi::get_one::<String>(&format!("SELECT dblink_error_message({})", cfe_lit(CFE_WORKER)))
        .expect("dblink_error_message")
        .unwrap_or_default()
}

/// Runs `sql` on the worker as one committed transaction.
fn cfe_commit(sql: &str) -> Result<(), String> {
    let status = Spi::get_one::<String>(&format!(
        "SELECT dblink_exec({}, {}, false)",
        cfe_lit(CFE_WORKER),
        cfe_lit(sql)
    ))
    .expect("dblink_exec")
    .unwrap_or_default();
    if status == "ERROR" {
        Err(cfe_error_message())
    } else {
        Ok(())
    }
}

/// Waits until `pid` waits on a lock.
fn cfe_await_lock_wait(pid: i32) {
    for _ in 0..600 {
        let waiting = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = {pid} AND NOT granted)"
        ))
        .expect("pg_locks")
        .unwrap_or(false);
        if waiting {
            return;
        }
        Spi::run("SELECT pg_sleep(0.05)").expect("sleep");
    }
    panic!("process {pid} never waited on a lock");
}

/// Runs `sql` on the worker as one committed transaction while the holder,
/// which keeps a lock the flush needs, waits on a lock the transaction took:
/// the worker's wait closes the cycle and the worker is the one to detect it.
fn cfe_commit_into_deadlock(sql: &str, holder_wait: &str) -> Result<(), String> {
    let worker_pid = cfe_pid(CFE_WORKER);
    let holder_pid = cfe_pid(CFE_HOLDER);
    cfe_exec("SET deadlock_timeout = '2s'");
    Spi::run(&format!(
        "SELECT dblink_send_query({}, {})",
        cfe_lit(CFE_WORKER),
        cfe_lit(sql)
    ))
    .expect("send worker");
    cfe_await_lock_wait(worker_pid);
    Spi::run(&format!(
        "SELECT dblink_send_query({}, {})",
        cfe_lit(CFE_HOLDER),
        cfe_lit(holder_wait)
    ))
    .expect("send holder");
    cfe_await_lock_wait(holder_pid);
    let status = Spi::get_one::<String>(&format!(
        "SELECT s FROM dblink_get_result({}, false) AS t(s text)",
        cfe_lit(CFE_WORKER)
    ))
    .expect("worker result")
    .unwrap_or_default();
    let outcome = if status == "ERROR" { Err(cfe_error_message()) } else { Ok(()) };
    let _ = Spi::run(&format!(
        "SELECT * FROM dblink_get_result({}, false) AS t(s text)",
        cfe_lit(CFE_WORKER)
    ));
    let _ = Spi::run(&format!(
        "SELECT * FROM dblink_get_result({}, false) AS t(s text)",
        cfe_lit(CFE_HOLDER)
    ));
    let _ = Spi::run(&format!(
        "SELECT * FROM dblink_get_result({}, false) AS t(s text)",
        cfe_lit(CFE_HOLDER)
    ));
    outcome
}

/// Source `tt`, LIST-partitioned on `plan`, read by the partitioned IMV `tv`,
/// its reader `tr` and the unpartitioned IMV `tu`, all in `mode`.
fn cfe_fixture(dbname: &str, mode: &str) {
    Spi::run("CREATE EXTENSION IF NOT EXISTS dblink").expect("dblink");
    cfe_admin_exec(&format!("DROP DATABASE IF EXISTS {dbname} WITH (FORCE)"));
    cfe_admin_exec(&format!("CREATE DATABASE {dbname}"));
    cfe_connect(CFE_WORKER, dbname);
    cfe_exec("CREATE EXTENSION pg_reflex");
    cfe_exec("CREATE TABLE tt (plan INT NOT NULL, k INT NOT NULL, q INT NOT NULL) PARTITION BY LIST (plan)");
    cfe_exec("CREATE TABLE tt_p1 PARTITION OF tt FOR VALUES IN (1)");
    cfe_exec("CREATE TABLE tt_p2 PARTITION OF tt FOR VALUES IN (2)");
    cfe_exec("INSERT INTO tt SELECT p, k, p * 10 + k FROM generate_series(1, 2) p, generate_series(1, 3) k");
    for (name, sql, key, partition_by) in [
        ("tv", "SELECT s.plan, s.k, s.q FROM tt AS s", "'plan, k'", ", NULL, ARRAY['plan']"),
        ("tr", "SELECT plan, COUNT(*) AS c, SUM(q) AS sq FROM tv GROUP BY plan", "NULL", ""),
        ("tu", "SELECT plan, COUNT(*) AS c, SUM(q) AS sq FROM tt GROUP BY plan", "NULL", ""),
    ] {
        assert_eq!(
            cfe_select(&format!(
                "create_reflex_ivm({}, {}, {key}, NULL, {}{partition_by})",
                cfe_lit(name),
                cfe_lit(sql),
                cfe_lit(mode)
            )),
            "CREATE REFLEX INCREMENTAL VIEW",
            "creating {name}"
        );
    }
}

fn cfe_close(dbname: &str) {
    for connection in [CFE_HOLDER, CFE_WORKER] {
        Spi::run(&format!(
            "SELECT dblink_disconnect({0}) WHERE {0} = ANY (dblink_get_connections())",
            cfe_lit(connection)
        ))
        .expect("disconnect");
    }
    cfe_admin_exec(&format!("DROP DATABASE IF EXISTS {dbname} WITH (FORCE)"));
}

/// The partition change of the scenario: a detach, or for the CHECK a swap of
/// `tt_p2` by a copy whose `q` the IMV's constraint refuses.
fn cfe_partition_change(error: CfeError) -> &'static str {
    match error {
        CfeError::CheckViolation | CfeError::ReaderCheckViolation => {
            "CREATE TABLE tt_p2n (LIKE tt INCLUDING ALL); \
             INSERT INTO tt_p2n SELECT plan, k, q + 10000 FROM tt_p2; \
             ALTER TABLE tt DETACH PARTITION tt_p2; \
             ALTER TABLE tt ATTACH PARTITION tt_p2n FOR VALUES IN (2);"
        }
        _ => "ALTER TABLE tt DETACH PARTITION tt_p2;",
    }
}

fn cfe_install_obstacle(error: CfeError, dbname: &str) {
    match error {
        CfeError::RefusedDelete => {
            cfe_exec(
                "CREATE FUNCTION cfe_refuse() RETURNS TRIGGER LANGUAGE plpgsql AS \
                 $$ BEGIN RAISE EXCEPTION 'cfe refused'; END $$",
            );
            cfe_exec("CREATE TRIGGER cfe_refuse BEFORE DELETE ON tv FOR EACH STATEMENT EXECUTE FUNCTION cfe_refuse()");
        }
        CfeError::CheckViolation => {
            cfe_exec("ALTER TABLE tv ADD CONSTRAINT cfe_q_small CHECK (q < 10000)");
        }
        CfeError::ReaderCheckViolation => {
            cfe_exec("ALTER TABLE tr ADD CONSTRAINT cfe_sq_small CHECK (sq < 10000)");
        }
        CfeError::LockTimeout | CfeError::Deadlock => {
            cfe_connect(CFE_HOLDER, dbname);
            cfe_exec_on(CFE_HOLDER, "SET deadlock_timeout = '60s'");
            cfe_exec_on(CFE_HOLDER, "BEGIN");
            cfe_exec_on(CFE_HOLDER, "LOCK TABLE tu IN SHARE MODE");
            if matches!(error, CfeError::LockTimeout) {
                cfe_exec("SET lock_timeout = '300ms'");
            }
        }
    }
}

fn cfe_remove_obstacle(error: CfeError) {
    match error {
        CfeError::RefusedDelete => cfe_exec("DROP TRIGGER cfe_refuse ON tv"),
        CfeError::CheckViolation => cfe_exec("ALTER TABLE tv DROP CONSTRAINT cfe_q_small"),
        CfeError::ReaderCheckViolation => cfe_exec("ALTER TABLE tr DROP CONSTRAINT cfe_sq_small"),
        CfeError::LockTimeout | CfeError::Deadlock => {
            cfe_exec_on(CFE_HOLDER, "ROLLBACK");
            cfe_exec("RESET lock_timeout");
            cfe_exec("RESET deadlock_timeout");
        }
    }
}

fn cfe_mismatches(imv: &str, fresh: &str) -> i64 {
    cfe_select(&format!(
        "SELECT count(*) FROM ((SELECT * FROM {imv} EXCEPT ALL ({fresh})) \
         UNION ALL (({fresh}) EXCEPT ALL SELECT * FROM {imv})) x"
    ))
    .parse()
    .expect("mismatch count")
}

/// One scenario: the partition change, the flush called in the transaction
/// when `flush_in_transaction`, then COMMIT, with `error` raised in the flush.
fn cfe_scenario(error: CfeError, mode: &str, flush_in_transaction: bool) {
    let dbname = format!(
        "reflex_cfe_{}_{}_{}",
        format!("{error:?}").to_lowercase(),
        mode.to_lowercase(),
        if flush_in_transaction { "flush" } else { "commit" }
    );
    let context = format!("{error:?} {mode} flush_in_transaction={flush_in_transaction}");
    cfe_fixture(&dbname, mode);
    cfe_install_obstacle(error, &dbname);

    let explicit_flush = if flush_in_transaction {
        "PERFORM reflex_flush_partitions();"
    } else {
        ""
    };
    let transaction = format!(
        "DO $cfe$ BEGIN {} {explicit_flush} END $cfe$",
        cfe_partition_change(error)
    );
    let outcome = match error {
        CfeError::Deadlock => cfe_commit_into_deadlock(&transaction, "LOCK TABLE tt_p2 IN ACCESS SHARE MODE"),
        _ => cfe_commit(&transaction),
    };
    cfe_assert_worker_alive(&dbname, &outcome);
    assert!(outcome.is_ok(), "{context}: the COMMIT fails: {outcome:?}");

    let pending = cfe_select(
        "(SELECT failures || '|' || COALESCE(last_error, '') FROM public.__reflex_partition_pending)",
    );
    let (failures, last_error) = pending
        .split_once('|')
        .unwrap_or_else(|| panic!("{context}: the root is not left pending: <{pending}>"));
    assert!(
        last_error.contains(error.message_marker()),
        "{context}: the pending root records <{last_error}>"
    );
    let failures: i32 = failures.parse().expect("failures");
    if error.is_transient() {
        assert_eq!(failures, 0, "{context}: a transient error counts toward the suspension");
    } else {
        assert!(failures >= 1, "{context}: the failure is not counted");
    }
    for (imv, _) in CFE_IMVS {
        assert_eq!(
            cfe_select(&format!(
                "(SELECT COALESCE(known_stale, FALSE) FROM public.__reflex_ivm_reference WHERE name = '{imv}')"
            )),
            "true",
            "{context}: {imv} is not flagged stale"
        );
    }

    cfe_remove_obstacle(error);
    let retry = cfe_select("reflex_flush_partitions()");
    assert!(!retry.starts_with("ERROR"), "{context}: the retry fails: {retry}");
    assert_eq!(
        cfe_select("(SELECT count(*) FROM public.__reflex_partition_pending)"),
        "0",
        "{context}: the retry leaves the root pending"
    );
    for (imv, fresh) in CFE_IMVS {
        assert_eq!(cfe_mismatches(imv, fresh), 0, "{context}: {imv} differs from its query after the retry");
    }
    cfe_close(&dbname);
}

fn cfe_matrix(error: CfeError) {
    for mode in ["IMMEDIATE", "DEFERRED"] {
        for flush_in_transaction in [false, true] {
            cfe_scenario(error, mode, flush_in_transaction);
        }
    }
}

#[pg_test]
fn pg_cfe_refused_delete_in_the_flush_leaves_the_server_up() {
    cfe_matrix(CfeError::RefusedDelete);
}

#[pg_test]
fn pg_cfe_check_violation_in_the_flush_leaves_the_server_up() {
    cfe_matrix(CfeError::CheckViolation);
}

#[pg_test]
fn pg_cfe_check_violation_in_the_refresh_of_a_reader_leaves_the_server_up() {
    cfe_matrix(CfeError::ReaderCheckViolation);
}

#[pg_test]
fn pg_cfe_lock_timeout_in_the_flush_leaves_the_server_up() {
    cfe_matrix(CfeError::LockTimeout);
}

#[pg_test]
fn pg_cfe_deadlock_in_the_flush_leaves_the_server_up() {
    cfe_matrix(CfeError::Deadlock);
}

/// The COMMIT-time flush of a DEFERRED IMV catches an error the same way: the
/// IMV is flagged stale and the COMMIT succeeds.
#[pg_test]
fn pg_cfe_check_violation_in_the_deferred_flush_leaves_the_server_up() {
    const DB: &str = "reflex_cfe_deferred_flush";
    const FRESH: &str = "SELECT g, SUM(v) AS sv FROM cd GROUP BY g";
    Spi::run("CREATE EXTENSION IF NOT EXISTS dblink").expect("dblink");
    cfe_admin_exec(&format!("DROP DATABASE IF EXISTS {DB} WITH (FORCE)"));
    cfe_admin_exec(&format!("CREATE DATABASE {DB}"));
    cfe_connect(CFE_WORKER, DB);
    cfe_exec("CREATE EXTENSION pg_reflex");
    cfe_exec("CREATE TABLE cd (id INT PRIMARY KEY, g INT NOT NULL, v INT NOT NULL)");
    cfe_exec("INSERT INTO cd SELECT i, i % 3, i FROM generate_series(1, 30) i");
    assert_eq!(
        cfe_select(&format!(
            "create_reflex_ivm('cdv', {}, NULL, NULL, 'DEFERRED')",
            cfe_lit(FRESH)
        )),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    cfe_exec("ALTER TABLE cdv ADD CONSTRAINT cfe_sv_small CHECK (sv < 10000)");

    let outcome = cfe_commit("INSERT INTO cd VALUES (100, 1, 20000)");
    cfe_assert_worker_alive(DB, &outcome);
    assert!(outcome.is_ok(), "the COMMIT fails: {outcome:?}");
    assert_eq!(
        cfe_select("(SELECT COALESCE(known_stale, FALSE) FROM public.__reflex_ivm_reference WHERE name = 'cdv')"),
        "true",
        "cdv is not flagged stale"
    );

    cfe_exec("ALTER TABLE cdv DROP CONSTRAINT cfe_sv_small");
    let retry = cfe_select("reflex_reconcile('cdv')");
    assert!(!retry.starts_with("ERROR"), "the reconcile fails: {retry}");
    assert_eq!(cfe_mismatches("cdv", FRESH), 0, "cdv differs from its query after the reconcile");
    cfe_close(DB);
}
