// Attaching a NEW source partition must never take AccessExclusive on the live
// IMV root: readers of the IMV — including readers pruning to a completely
// unrelated partition — must stay unblocked for the whole transaction (the
// inline `ddl_command_end` sync AND the COMMIT-time reconcile it precedes).
//
// A `#[pg_test]` body is one transaction that is rolled back, which cannot host
// this scenario: the IMV root would be created by the very transaction under
// test (so it already holds AccessExclusive on it from the CREATE, masking the
// lock under test), and nothing it does is visible to a second session. Both
// lock tests therefore drive a REMOTE `dblink` session: it builds the fixture,
// opens a transaction, attaches the source partition, and commits — while this
// session observes `pg_locks` and reads the IMV with a `lock_timeout`. The
// remote session drops its fixture after COMMIT, so nothing is left behind.

fn sql_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn dblink_conninfo() -> String {
    Spi::get_one::<String>(
        "SELECT 'host=' || split_part(current_setting('unix_socket_directories'), ',', 1) \
              || ' port=' || current_setting('port') \
              || ' dbname=' || current_database() \
              || ' user=' || current_user",
    )
    .expect("conninfo query")
    .expect("conninfo NULL")
}

fn setup_dblink() {
    Spi::run("CREATE EXTENSION IF NOT EXISTS dblink").expect("dblink extension");
}

/// Open the named remote worker session that plays the role of the application
/// backend doing the partition add.
fn worker_connect() {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('reflex_lock_worker', {})",
        sql_lit(&dblink_conninfo())
    ))
    .expect("worker connect")
    .expect("worker connect NULL");
}

fn worker_disconnect() {
    let _ = Spi::get_one::<String>("SELECT dblink_disconnect('reflex_lock_worker')");
}

/// Run a statement (or `;`-separated batch returning no rows) on the worker.
fn worker_exec(sql: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_exec('reflex_lock_worker', {})",
        sql_lit(sql)
    ))
    .unwrap_or_else(|e| panic!("worker_exec failed for <{}>: {}", sql, e));
}

fn relation_oid(rel: &str) -> i64 {
    Spi::get_one::<i64>(&format!("SELECT to_regclass({})::oid::int8", sql_lit(rel)))
        .expect("oid query")
        .expect("oid NULL")
}

fn worker_pid() -> i32 {
    Spi::get_one::<i32>(
        "SELECT p FROM dblink('reflex_lock_worker', 'SELECT pg_backend_pid()') AS t(p int)",
    )
    .expect("worker pid")
    .expect("worker pid NULL")
}

/// Lock modes the worker backend currently holds on `rel`.
fn worker_lock_modes(rel_oid: i64, pid: i32) -> Vec<String> {
    Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(mode, ',' ORDER BY mode), '') FROM pg_locks \
         WHERE pid = {} AND locktype = 'relation' \
           AND relation = {} AND granted",
        pid, rel_oid
    ))
    .expect("pg_locks query")
    .expect("pg_locks NULL")
    .split(',')
    .filter(|s| !s.is_empty())
    .map(|s| s.to_string())
    .collect()
}

/// Read `query` from a THROWAWAY session with `lock_timeout = 2s`, returning
/// -1 when the read blocked — the freeze under test.
///
/// The reader must not be this session: a `SELECT` here would hold
/// `AccessShareLock` on the IMV for the rest of the test transaction, and the
/// worker's fixture `DROP TABLE` would then deadlock against it. `dblink` with
/// an inline conninfo opens and closes its connection per call, so it leaves no
/// lock behind. The plpgsql EXCEPTION block confines the lock-timeout error to
/// a subtransaction so the test reports it instead of aborting.
fn read_with_lock_timeout(query: &str) -> i64 {
    Spi::run(
        "CREATE OR REPLACE FUNCTION pg_temp.__reflex_timed_read(conn text, q text) \
         RETURNS bigint LANGUAGE plpgsql AS $fn$ \
         DECLARE n bigint; \
         BEGIN \
           SELECT c INTO n FROM dblink(conn, q) AS t(c bigint); \
           RETURN n; \
         EXCEPTION WHEN OTHERS THEN RETURN -1; \
         END $fn$",
    )
    .expect("timed read function");
    let conn = format!("{} options=-c\\ lock_timeout=2000", dblink_conninfo());
    Spi::get_one::<i64>(&format!(
        "SELECT pg_temp.__reflex_timed_read({}, {})",
        sql_lit(&conn),
        sql_lit(query)
    ))
    .expect("timed read call")
    .expect("timed read NULL")
}

fn assert_root_lock_shape(modes: &[String], what: &str) {
    assert!(
        !modes.iter().any(|m| m == "AccessExclusiveLock"),
        "{}: the IMV root went AccessExclusiveLock and holds it to commit, \
         freezing every reader of the IMV (locks held: {:?})",
        what,
        modes
    );
    assert!(
        modes.iter().any(|m| m == "ShareUpdateExclusiveLock"),
        "{}: the IMV root shows no ShareUpdateExclusiveLock (locks held: {:?}) — \
         the new mirror partition was not added by ALTER TABLE ... ATTACH PARTITION",
        what,
        modes
    );
}

// ---------------------------------------------------------------------------
// T1 — mirror depth 2
// ---------------------------------------------------------------------------

#[pg_test]
fn attach_new_partition_never_locks_imv_root_depth2() {
    setup_dblink();
    worker_connect();
    worker_exec(
        "CREATE TABLE la2_src (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY LIST (k); \
         CREATE TABLE la2_src_1 PARTITION OF la2_src FOR VALUES IN (1) PARTITION BY RANGE (d); \
         CREATE TABLE la2_src_1_m1 PARTITION OF la2_src_1 \
             FOR VALUES FROM ('2025-01-01') TO ('2025-02-01'); \
         CREATE TABLE la2_src_1_m2 PARTITION OF la2_src_1 \
             FOR VALUES FROM ('2025-02-01') TO ('2025-03-01'); \
         INSERT INTO la2_src SELECT 1, '2025-01-15'::date, g FROM generate_series(1, 500) g; \
         INSERT INTO la2_src SELECT 1, '2025-02-15'::date, g FROM generate_series(1, 500) g; \
         DO $mk$ BEGIN PERFORM create_reflex_ivm('la2_imv', 'SELECT k, d, v FROM la2_src', \
             'k,d,v', NULL, NULL, NULL, ARRAY['k','d']); END $mk$",
    );
    // A brand-new, pre-populated top-level branch with 3 monthly leaves.
    worker_exec(
        "CREATE TABLE la2_src_5 (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY RANGE (d); \
         CREATE TABLE la2_src_5_m1 PARTITION OF la2_src_5 \
             FOR VALUES FROM ('2025-01-01') TO ('2025-02-01'); \
         CREATE TABLE la2_src_5_m2 PARTITION OF la2_src_5 \
             FOR VALUES FROM ('2025-02-01') TO ('2025-03-01'); \
         CREATE TABLE la2_src_5_m3 PARTITION OF la2_src_5 \
             FOR VALUES FROM ('2025-03-01') TO ('2025-04-01'); \
         INSERT INTO la2_src_5 SELECT 5, '2025-01-10'::date, g FROM generate_series(1, 400) g; \
         INSERT INTO la2_src_5 SELECT 5, '2025-02-10'::date, g FROM generate_series(1, 400) g; \
         INSERT INTO la2_src_5 SELECT 5, '2025-03-10'::date, g FROM generate_series(1, 400) g",
    );
    let pid = worker_pid();
    let imv_oid = relation_oid("la2_imv");

    let baseline = read_with_lock_timeout("SELECT count(*) FROM la2_imv WHERE k = 1");

    worker_exec("BEGIN");
    worker_exec("ALTER TABLE la2_src ATTACH PARTITION la2_src_5 FOR VALUES IN (5)");

    // Mid-transaction: the inline ddl_command_end sync has run and every lock it
    // took is held until the worker commits.
    let modes = worker_lock_modes(imv_oid, pid);
    let during1 = read_with_lock_timeout("SELECT count(*) FROM la2_imv WHERE k = 1");
    let during2 = read_with_lock_timeout("SELECT count(*) FROM la2_imv WHERE k = 1");
    let during3 = read_with_lock_timeout("SELECT count(*) FROM la2_imv");

    worker_exec("COMMIT");
    let after = read_with_lock_timeout("SELECT count(*) FROM la2_imv WHERE k = 5");
    worker_exec(
        "DROP TABLE IF EXISTS la2_src CASCADE; DROP TABLE IF EXISTS la2_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'la2_imv'",
    );
    worker_disconnect();

    assert_eq!(baseline, 1000, "baseline reader could not read the IMV");
    assert_eq!(
        during1, 1000,
        "a reader of an UNRELATED partition blocked during the partition add"
    );
    assert_eq!(during2, 1000, "reader blocked during the partition add");
    assert_eq!(
        during3, 1000,
        "an unpruned reader of the whole IMV blocked during the partition add"
    );
    assert_root_lock_shape(&modes, "mirror depth 2");
    assert_eq!(after, 1200, "the new partition's data is wrong after commit");
}

// ---------------------------------------------------------------------------
// T2 — mirror depth 1: the case a detached-skeleton-only fix does NOT close,
// because the top-level child IS the leaf, so the reconcile's swap DETACHes it
// straight off the root.
// ---------------------------------------------------------------------------

#[pg_test]
fn attach_new_partition_never_locks_imv_root_depth1() {
    setup_dblink();
    worker_connect();
    worker_exec(
        "CREATE TABLE la1_src (k INT NOT NULL, v INT) PARTITION BY LIST (k); \
         CREATE TABLE la1_src_1 PARTITION OF la1_src FOR VALUES IN (1); \
         INSERT INTO la1_src SELECT 1, g FROM generate_series(1, 500) g; \
         DO $mk$ BEGIN PERFORM create_reflex_ivm('la1_imv', 'SELECT k, v FROM la1_src', \
             'k,v', NULL, NULL, NULL, ARRAY['k']); END $mk$",
    );
    worker_exec(
        "CREATE TABLE la1_src_5 (k INT NOT NULL, v INT); \
         INSERT INTO la1_src_5 SELECT 5, g FROM generate_series(1, 900) g",
    );
    let pid = worker_pid();
    let imv_oid = relation_oid("la1_imv");

    let baseline = read_with_lock_timeout("SELECT count(*) FROM la1_imv WHERE k = 1");

    worker_exec("BEGIN");
    worker_exec("ALTER TABLE la1_src ATTACH PARTITION la1_src_5 FOR VALUES IN (5)");
    let modes_after_sync = worker_lock_modes(imv_oid, pid);
    let during1 = read_with_lock_timeout("SELECT count(*) FROM la1_imv WHERE k = 1");

    // Drain the deferred COMMIT-time flush WITHOUT ending the transaction, so
    // the locks the reconcile takes are still observable.
    worker_exec("SET CONSTRAINTS ALL IMMEDIATE");
    let modes_after_reconcile = worker_lock_modes(imv_oid, pid);
    let during2 = read_with_lock_timeout("SELECT count(*) FROM la1_imv WHERE k = 1");
    let during3 = read_with_lock_timeout("SELECT count(*) FROM la1_imv WHERE k = 1");

    worker_exec("COMMIT");
    let after = read_with_lock_timeout("SELECT count(*) FROM la1_imv WHERE k = 5");
    worker_exec(
        "DROP TABLE IF EXISTS la1_src CASCADE; DROP TABLE IF EXISTS la1_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'la1_imv'",
    );
    worker_disconnect();

    assert_eq!(baseline, 500, "baseline reader could not read the IMV");
    assert_eq!(
        during1, 500,
        "a reader of an UNRELATED partition blocked after the inline sync"
    );
    assert_eq!(
        during2, 500,
        "a reader of an UNRELATED partition blocked after the COMMIT-time reconcile"
    );
    assert_eq!(during3, 500, "reader blocked after the reconcile");
    assert_root_lock_shape(&modes_after_sync, "mirror depth 1, after sync");
    assert_root_lock_shape(&modes_after_reconcile, "mirror depth 1, after reconcile");
    assert_eq!(after, 900, "the new partition's data is wrong after commit");
}

// ---------------------------------------------------------------------------
// T3 — the attached partition's data must be complete and correct
// ---------------------------------------------------------------------------

#[pg_test]
fn attach_new_partition_data_is_correct_depth2() {
    Spi::run("CREATE TABLE ac_src (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY LIST (k)")
        .expect("root");
    Spi::run("CREATE TABLE ac_src_1 PARTITION OF ac_src FOR VALUES IN (1) PARTITION BY RANGE (d)")
        .expect("branch 1");
    Spi::run(
        "CREATE TABLE ac_src_1_m1 PARTITION OF ac_src_1 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')",
    )
    .expect("m1");
    Spi::run("INSERT INTO ac_src VALUES (1, '2025-01-10', 7), (1, '2025-01-11', 8)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('ac_imv', 'SELECT k, d, v FROM ac_src', \
         'k,d,v', NULL, NULL, NULL, ARRAY['k','d'])",
    )
    .expect("create")
    .expect("create");

    Spi::run("CREATE TABLE ac_src_5 (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY RANGE (d)")
        .expect("new branch");
    Spi::run(
        "CREATE TABLE ac_src_5_m1 PARTITION OF ac_src_5 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')",
    )
    .expect("m1");
    Spi::run(
        "CREATE TABLE ac_src_5_m2 PARTITION OF ac_src_5 FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')",
    )
    .expect("m2 (stays empty)");
    Spi::run(
        "CREATE TABLE ac_src_5_m3 PARTITION OF ac_src_5 FOR VALUES FROM ('2025-03-01') TO ('2025-04-01')",
    )
    .expect("m3");
    Spi::run("INSERT INTO ac_src_5 VALUES (5, '2025-01-20', 11), (5, '2025-03-20', 33)")
        .expect("seed new branch");
    Spi::run("ALTER TABLE ac_src ATTACH PARTITION ac_src_5 FOR VALUES IN (5)").expect("attach");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("deferred flush");

    assert_imv_correct("ac_imv", "SELECT k, d, v FROM ac_src");

    let n5 = Spi::get_one::<i64>("SELECT count(*) FROM ac_imv WHERE k = 5")
        .unwrap()
        .unwrap();
    assert_eq!(n5, 2, "new branch rows must all be present exactly once");
    let n1 = Spi::get_one::<i64>("SELECT count(*) FROM ac_imv WHERE k = 1")
        .unwrap()
        .unwrap();
    assert_eq!(n1, 2, "pre-existing branch must be untouched");
    let mirrored_empty_month =
        Spi::get_one::<bool>("SELECT to_regclass('public.ac_imv_ac_src_5_m2') IS NOT NULL")
            .unwrap()
            .unwrap();
    assert!(
        mirrored_empty_month,
        "the empty month must still be mirrored as a partition"
    );
}

// ---------------------------------------------------------------------------
// T4 — the new node must be filled exactly once
// ---------------------------------------------------------------------------

#[pg_test]
fn attach_new_partition_is_filled_exactly_once() {
    Spi::run("CREATE TABLE af_src (k INT NOT NULL, v INT) PARTITION BY LIST (k)").expect("root");
    Spi::run("CREATE TABLE af_src_1 PARTITION OF af_src FOR VALUES IN (1)").expect("p1");
    Spi::run("INSERT INTO af_src VALUES (1, 1), (1, 2)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('af_imv', 'SELECT k, v FROM af_src', \
         'k,v', NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("create")
    .expect("create");

    Spi::run("CREATE TABLE af_src_5 (k INT NOT NULL, v INT)").expect("new");
    Spi::run("INSERT INTO af_src_5 VALUES (5, 10), (5, 20), (5, 30)").expect("seed new");
    Spi::run("ALTER TABLE af_src ATTACH PARTITION af_src_5 FOR VALUES IN (5)").expect("attach");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("deferred flush");

    // A node filled by BOTH the inline sync and the COMMIT-time reconcile shows
    // up as duplicated rows in a passthrough IMV.
    let n = Spi::get_one::<i64>("SELECT count(*) FROM af_imv WHERE k = 5")
        .unwrap()
        .unwrap();
    assert_eq!(n, 3, "new partition filled more than once (or not at all)");
    let dupes = Spi::get_one::<i64>(
        "SELECT count(*) FROM (SELECT k, v FROM af_imv GROUP BY k, v HAVING count(*) > 1) d",
    )
    .unwrap()
    .unwrap();
    assert_eq!(dupes, 0, "duplicate rows — the new node was filled twice");
    assert_imv_correct("af_imv", "SELECT k, v FROM af_src");
}

// ---------------------------------------------------------------------------
// T5 — a non-empty DEFAULT partition must not break the add
// ---------------------------------------------------------------------------

#[pg_test]
fn attach_new_partition_with_non_empty_default_stays_correct() {
    Spi::run("CREATE TABLE ad_src (k INT NOT NULL, v INT) PARTITION BY LIST (k)").expect("root");
    Spi::run("CREATE TABLE ad_src_1 PARTITION OF ad_src FOR VALUES IN (1)").expect("p1");
    Spi::run("CREATE TABLE ad_src_def PARTITION OF ad_src DEFAULT").expect("default");
    Spi::run("INSERT INTO ad_src VALUES (1, 1), (1, 2)").expect("seed p1");
    Spi::run("INSERT INTO ad_src SELECT 9, g FROM generate_series(1, 300) g").expect("seed default");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('ad_imv', 'SELECT k, v FROM ad_src', \
         'k,v', NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("create")
    .expect("create");

    let before = Spi::get_one::<i64>("SELECT count(*) FROM ad_imv")
        .unwrap()
        .unwrap();
    assert_eq!(before, 302, "IMV must mirror the default partition's rows");

    Spi::run("CREATE TABLE ad_src_5 (k INT NOT NULL, v INT)").expect("new");
    Spi::run("INSERT INTO ad_src_5 VALUES (5, 10), (5, 20)").expect("seed new");
    Spi::run("ALTER TABLE ad_src ATTACH PARTITION ad_src_5 FOR VALUES IN (5)").expect("attach");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("deferred flush");

    assert_imv_correct("ad_imv", "SELECT k, v FROM ad_src");
    let def_rows = Spi::get_one::<i64>("SELECT count(*) FROM ad_imv WHERE k = 9")
        .unwrap()
        .unwrap();
    assert_eq!(def_rows, 300, "default-resident rows must survive the add");
    let n5 = Spi::get_one::<i64>("SELECT count(*) FROM ad_imv WHERE k = 5")
        .unwrap()
        .unwrap();
    assert_eq!(n5, 2, "new partition must be filled exactly once");
}

// ---------------------------------------------------------------------------
// T5b — a DEFAULT partition already holding rows that BELONG to the incoming
// bound. Sync drains defaults, builds the new node, attaches it and refills;
// the refill routes those rows into the new node, so it is no longer empty and
// the reconcile must fall back to the full swap rather than fill on top.
// ---------------------------------------------------------------------------

#[pg_test]
fn attach_new_partition_absorbing_default_rows_stays_correct() {
    Spi::run("CREATE TABLE ae_src (k INT NOT NULL, v INT) PARTITION BY LIST (k)").expect("root");
    Spi::run("CREATE TABLE ae_src_1 PARTITION OF ae_src FOR VALUES IN (1)").expect("p1");
    Spi::run("CREATE TABLE ae_src_def PARTITION OF ae_src DEFAULT").expect("default");
    Spi::run("INSERT INTO ae_src VALUES (1, 1)").expect("seed p1");
    // k=5 rows live in the source DEFAULT, so the IMV's own default holds them too.
    Spi::run("INSERT INTO ae_src SELECT 5, g FROM generate_series(1, 50) g").expect("seed default");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('ae_imv', 'SELECT k, v FROM ae_src', \
         'k,v', NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("create")
    .expect("create");
    assert_imv_correct("ae_imv", "SELECT k, v FROM ae_src");

    // Move those rows out of the source default into a real k=5 partition.
    Spi::run("CREATE TABLE ae_src_5 (k INT NOT NULL, v INT)").expect("new");
    Spi::run("WITH d AS (DELETE FROM ae_src_def WHERE k = 5 RETURNING *) \
              INSERT INTO ae_src_5 SELECT * FROM d")
        .expect("relocate");
    Spi::run("ALTER TABLE ae_src ATTACH PARTITION ae_src_5 FOR VALUES IN (5)").expect("attach");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("deferred flush");

    assert_imv_correct("ae_imv", "SELECT k, v FROM ae_src");
    let n5 = Spi::get_one::<i64>("SELECT count(*) FROM ae_imv WHERE k = 5")
        .unwrap()
        .unwrap();
    assert_eq!(n5, 50, "absorbed default rows must appear exactly once");
}
