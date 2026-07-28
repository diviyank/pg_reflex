// Adding a source partition must never take AccessExclusive on the live IMV
// root: readers of the IMV — including readers pruning to a completely
// unrelated partition — must stay unblocked for the whole transaction (the
// inline `ddl_command_end` sync AND the COMMIT-time reconcile it precedes).
//
// A `#[pg_test]` body is one transaction that is rolled back, which cannot host
// this scenario: the IMV root would be created by the very transaction under
// test (so it already holds AccessExclusive on it from the CREATE, masking the
// lock under test), and nothing it does is visible to a second session. These
// tests therefore drive a REMOTE `dblink` session: it builds the fixture, opens
// a transaction, changes the source partition set, and commits — while this
// session observes `pg_locks` and reads the IMV with a `lock_timeout`.
//
// The remote fixture lives in its OWN database. It has to: it is committed, and
// these are the only tests in the suite that commit global state, so a fixture
// in the shared test database perturbs any test that takes a cluster-wide
// census of artifact relations (`drop_deferred_imv_wipes_every_nonmaintenance_table`
// does exactly that) whenever the two run concurrently.

fn sql_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn current_db_conninfo() -> String {
    Spi::get_one::<String>(
        "SELECT 'host=' || split_part(current_setting('unix_socket_directories'), ',', 1) \
              || ' port=' || current_setting('port') \
              || ' dbname=' || current_database() \
              || ' user=' || current_user",
    )
    .expect("admin conninfo")
    .expect("admin conninfo NULL")
}

fn conninfo_for(dbname: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT 'host=' || split_part(current_setting('unix_socket_directories'), ',', 1) \
              || ' port=' || current_setting('port') \
              || ' dbname=' || {} \
              || ' user=' || current_user",
        sql_lit(dbname)
    ))
    .expect("conninfo query")
    .expect("conninfo NULL")
}

fn setup_dblink() {
    Spi::run("CREATE EXTENSION IF NOT EXISTS dblink").expect("dblink extension");
}

/// Run a statement in a throwaway session on the CURRENT database — used for
/// `CREATE DATABASE` / `DROP DATABASE`, which cannot run in a transaction block.
fn admin_exec(sql: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_exec({}, {})",
        sql_lit(&current_db_conninfo()),
        sql_lit(sql)
    ))
    .unwrap_or_else(|e| panic!("admin_exec failed for <{}>: {}", sql, e));
}

/// Run a statement (or `;`-separated batch returning no rows) on the worker.
fn worker_exec(sql: &str) {
    Spi::get_one::<String>(&format!(
        "SELECT dblink_exec('reflex_lock_worker', {})",
        sql_lit(sql)
    ))
    .unwrap_or_else(|e| panic!("worker_exec failed for <{}>: {}", sql, e));
}

/// Create a private database for one lock test and open the worker session on
/// it. Idempotent: a leftover from an aborted run is dropped first.
fn probe_db_open(dbname: &str) {
    setup_dblink();
    admin_exec(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", dbname));
    admin_exec(&format!("CREATE DATABASE {}", dbname));
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('reflex_lock_worker', {})",
        sql_lit(&conninfo_for(dbname))
    ))
    .expect("worker connect")
    .expect("worker connect NULL");
    worker_exec("CREATE EXTENSION pg_reflex");
}

fn probe_db_close(dbname: &str) {
    let _ = Spi::get_one::<String>("SELECT dblink_disconnect('reflex_lock_worker')");
    admin_exec(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", dbname));
}

fn worker_scalar_i64(query: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT c FROM dblink('reflex_lock_worker', {}) AS t(c bigint)",
        sql_lit(query)
    ))
    .expect("worker scalar")
    .expect("worker scalar NULL")
}

fn worker_pid() -> i32 {
    Spi::get_one::<i32>(
        "SELECT p FROM dblink('reflex_lock_worker', 'SELECT pg_backend_pid()') AS t(p int)",
    )
    .expect("worker pid")
    .expect("worker pid NULL")
}

/// Resolve a relation OID inside the probe database — OIDs are per-database and
/// `pg_locks` reports them raw.
fn worker_relation_oid(rel: &str) -> i64 {
    worker_scalar_i64(&format!("SELECT to_regclass({})::oid::int8", sql_lit(rel)))
}

/// Lock modes the worker backend currently holds on the relation.
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

/// Read `query` from a THROWAWAY session on the probe database with
/// `lock_timeout = 2s`, returning -1 when the read blocked — the freeze under
/// test.
///
/// The reader must be neither this session nor the worker: a `SELECT` here
/// would hold `AccessShareLock` for the rest of the test transaction and the
/// worker's teardown would deadlock against it. `dblink` with an inline
/// conninfo opens and closes its connection per call, so it leaves no lock
/// behind. The plpgsql EXCEPTION block confines the lock-timeout error to a
/// subtransaction so the test reports it instead of aborting.
fn read_with_lock_timeout(dbname: &str, query: &str) -> i64 {
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
    let conn = format!("{} options=-c\\ lock_timeout=2000", conninfo_for(dbname));
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
    const DBNAME: &str = "reflex_lockprobe_la2";
    probe_db_open(DBNAME);
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
    let imv_oid = worker_relation_oid("la2_imv");

    let baseline = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la2_imv WHERE k = 1");

    worker_exec("BEGIN");
    worker_exec("ALTER TABLE la2_src ATTACH PARTITION la2_src_5 FOR VALUES IN (5)");

    // Mid-transaction: the inline ddl_command_end sync has run and every lock it
    // took is held until the worker commits.
    let modes = worker_lock_modes(imv_oid, pid);
    let during1 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la2_imv WHERE k = 1");
    let during2 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la2_imv WHERE k = 1");
    let during3 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la2_imv");

    worker_exec("COMMIT");
    let after = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la2_imv WHERE k = 5");
    worker_exec(
        "DROP TABLE IF EXISTS la2_src CASCADE; DROP TABLE IF EXISTS la2_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'la2_imv'",
    );
    probe_db_close(DBNAME);

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
    const DBNAME: &str = "reflex_lockprobe_la1";
    probe_db_open(DBNAME);
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
    let imv_oid = worker_relation_oid("la1_imv");

    let baseline = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la1_imv WHERE k = 1");

    worker_exec("BEGIN");
    worker_exec("ALTER TABLE la1_src ATTACH PARTITION la1_src_5 FOR VALUES IN (5)");
    let modes_after_sync = worker_lock_modes(imv_oid, pid);
    let during1 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la1_imv WHERE k = 1");

    // Drain the deferred COMMIT-time flush WITHOUT ending the transaction, so
    // the locks the reconcile takes are still observable.
    worker_exec("SET CONSTRAINTS ALL IMMEDIATE");
    let modes_after_reconcile = worker_lock_modes(imv_oid, pid);
    let during2 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la1_imv WHERE k = 1");
    let during3 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la1_imv WHERE k = 1");

    worker_exec("COMMIT");
    let after = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM la1_imv WHERE k = 5");
    worker_exec(
        "DROP TABLE IF EXISTS la1_src CASCADE; DROP TABLE IF EXISTS la1_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'la1_imv'",
    );
    probe_db_close(DBNAME);

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

// ---------------------------------------------------------------------------
// T7 — the canonical partition-rollover shape at mirror depth 1: create next
// period's source partition and LOAD it in the same transaction.
//
// The load's own IMV maintenance delta lands in the brand-new mirror child
// before the COMMIT-time partition reconcile reaches it, so an "is the child
// empty?" gate sees a non-empty child and refuses the in-place fill. The child
// is nonetheless brand new — nothing in it predates this transaction, and no
// reader could have been reading a partition that did not exist at transaction
// start — so the reconcile must still avoid DETACHing it off the live root.
// ---------------------------------------------------------------------------

#[pg_test]
fn create_and_load_partition_never_locks_imv_root_depth1() {
    const DBNAME: &str = "reflex_lockprobe_lz";
    probe_db_open(DBNAME);
    worker_exec(
        "CREATE TABLE lz_src (k INT NOT NULL, v INT) PARTITION BY LIST (k); \
         CREATE TABLE lz_src_1 PARTITION OF lz_src FOR VALUES IN (1); \
         INSERT INTO lz_src SELECT 1, g FROM generate_series(1, 500) g; \
         DO $mk$ BEGIN PERFORM create_reflex_ivm('lz_imv', 'SELECT k, v FROM lz_src', \
             'k,v', NULL, NULL, NULL, ARRAY['k']); END $mk$",
    );
    let pid = worker_pid();
    let imv_oid = worker_relation_oid("lz_imv");

    let baseline = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM lz_imv WHERE k = 1");

    worker_exec("BEGIN");
    worker_exec("CREATE TABLE lz_src_5 PARTITION OF lz_src FOR VALUES IN (5)");
    let modes_after_sync = worker_lock_modes(imv_oid, pid);

    worker_exec("INSERT INTO lz_src SELECT 5, g FROM generate_series(1, 900) g");
    let modes_after_load = worker_lock_modes(imv_oid, pid);
    let during1 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM lz_imv WHERE k = 1");

    worker_exec("SET CONSTRAINTS ALL IMMEDIATE");
    let modes_after_reconcile = worker_lock_modes(imv_oid, pid);
    let during2 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM lz_imv WHERE k = 1");
    let during3 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM lz_imv");

    worker_exec("COMMIT");
    let new_rows = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM lz_imv WHERE k = 5");
    let mismatches = read_with_lock_timeout(DBNAME, 
        "SELECT count(*) FROM ((SELECT * FROM lz_imv EXCEPT ALL SELECT k, v FROM lz_src) \
         UNION ALL (SELECT k, v FROM lz_src EXCEPT ALL SELECT * FROM lz_imv)) o",
    );
    worker_exec(
        "DROP TABLE IF EXISTS lz_src CASCADE; DROP TABLE IF EXISTS lz_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'lz_imv'",
    );
    probe_db_close(DBNAME);

    assert_eq!(baseline, 500, "baseline reader could not read the IMV");
    assert_eq!(
        during1, 500,
        "a reader of an UNRELATED partition blocked after the load"
    );
    assert_eq!(
        during2, 500,
        "a reader of an UNRELATED partition blocked after the COMMIT-time reconcile"
    );
    assert_eq!(during3, 500, "an unpruned reader of the whole IMV blocked");
    assert_root_lock_shape(&modes_after_sync, "rollover depth 1, after sync");
    assert_root_lock_shape(&modes_after_load, "rollover depth 1, after load");
    assert_root_lock_shape(&modes_after_reconcile, "rollover depth 1, after reconcile");
    assert_eq!(new_rows, 900, "the loaded rows are wrong after commit");
    assert_eq!(mismatches, 0, "EXCEPT ALL oracle: IMV diverges from source");
}

// ---------------------------------------------------------------------------
// T8 — ATTACH a pre-populated branch and then INSERT more into it in the same
// transaction, at mirror depth 2.
//
// WHAT THIS PINS, PRECISELY. Its lock assertions are a regression guard only:
// at mirror depth 2 the reconcile's swap DETACHes off the intermediate branch,
// never off the root, so the root stays clean here whether or not the fresh-
// child mechanism works — this test does NOT go RED when that mechanism is
// broken (mutation M5). What IS load-bearing here are the row-count and oracle
// assertions: they go RED under M6 (the target TRUNCATE removed), which the
// depth-1 test cannot catch because there the duplicate INSERT collides with
// the IMV's unique key and the reconcile's error is discarded upstream.
//
// The test that pins the fresh-child mechanism by lock shape is T7
// (`create_and_load_partition_never_locks_imv_root_depth1`).
// ---------------------------------------------------------------------------

#[pg_test]
fn attach_then_load_partition_depth2_row_counts_and_root_lock_guard() {
    const DBNAME: &str = "reflex_lockprobe_ly";
    probe_db_open(DBNAME);
    worker_exec(
        "CREATE TABLE ly_src (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY LIST (k); \
         CREATE TABLE ly_src_1 PARTITION OF ly_src FOR VALUES IN (1) PARTITION BY RANGE (d); \
         CREATE TABLE ly_src_1_m1 PARTITION OF ly_src_1 \
             FOR VALUES FROM ('2025-01-01') TO ('2025-02-01'); \
         INSERT INTO ly_src SELECT 1, '2025-01-15'::date, g FROM generate_series(1, 500) g; \
         DO $mk$ BEGIN PERFORM create_reflex_ivm('ly_imv', 'SELECT k, d, v FROM ly_src', \
             'k,d,v', NULL, NULL, NULL, ARRAY['k','d']); END $mk$",
    );
    worker_exec(
        "CREATE TABLE ly_src_5 (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY RANGE (d); \
         CREATE TABLE ly_src_5_m1 PARTITION OF ly_src_5 \
             FOR VALUES FROM ('2025-01-01') TO ('2025-02-01'); \
         CREATE TABLE ly_src_5_m2 PARTITION OF ly_src_5 \
             FOR VALUES FROM ('2025-02-01') TO ('2025-03-01'); \
         INSERT INTO ly_src_5 SELECT 5, '2025-01-10'::date, g FROM generate_series(1, 400) g",
    );
    let pid = worker_pid();
    let imv_oid = worker_relation_oid("ly_imv");

    let baseline = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM ly_imv WHERE k = 1");

    worker_exec("BEGIN");
    worker_exec("ALTER TABLE ly_src ATTACH PARTITION ly_src_5 FOR VALUES IN (5)");
    worker_exec("INSERT INTO ly_src SELECT 5, '2025-02-10'::date, g FROM generate_series(1, 300) g");
    let modes_after_load = worker_lock_modes(imv_oid, pid);
    let during1 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM ly_imv WHERE k = 1");

    worker_exec("SET CONSTRAINTS ALL IMMEDIATE");
    let modes_after_reconcile = worker_lock_modes(imv_oid, pid);
    let during2 = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM ly_imv WHERE k = 1");

    worker_exec("COMMIT");
    let new_rows = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM ly_imv WHERE k = 5");
    let mismatches = read_with_lock_timeout(DBNAME, 
        "SELECT count(*) FROM ((SELECT * FROM ly_imv EXCEPT ALL SELECT k, d, v FROM ly_src) \
         UNION ALL (SELECT k, d, v FROM ly_src EXCEPT ALL SELECT * FROM ly_imv)) o",
    );
    worker_exec(
        "DROP TABLE IF EXISTS ly_src CASCADE; DROP TABLE IF EXISTS ly_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'ly_imv'",
    );
    probe_db_close(DBNAME);

    assert_eq!(baseline, 500, "baseline reader could not read the IMV");
    assert_eq!(
        during1, 500,
        "a reader of an UNRELATED partition blocked after the load"
    );
    assert_eq!(
        during2, 500,
        "a reader of an UNRELATED partition blocked after the COMMIT-time reconcile"
    );
    assert_root_lock_shape(&modes_after_load, "attach+load depth 2, after load");
    assert_root_lock_shape(&modes_after_reconcile, "attach+load depth 2, after reconcile");
    assert_eq!(new_rows, 700, "the loaded rows are wrong after commit");
    assert_eq!(mismatches, 0, "EXCEPT ALL oracle: IMV diverges from source");
}

// ---------------------------------------------------------------------------
// T9 / T10 — AGGREGATE partition rollover.
//
// T1-T8 are all passthrough IMVs, where `end_query` is empty and the
// intermediate half of the fresh-child gate short-circuits to "qualified"
// without ever being exercised. Only an aggregate IMV has an intermediate
// table, so only an aggregate fixture reaches the intermediate TRUNCATE.
//
// The failure mode there is SILENT: without the TRUNCATE the intermediate child
// keeps the maintenance delta that the same transaction's load wrote into it
// and the authoritative refill is added on top, so the aggregate double-counts.
// No error is raised — only the bidirectional EXCEPT ALL oracle catches it,
// which is why both tests assert through `assert_imv_correct` and on the
// aggregate value itself rather than on lock shape.
// ---------------------------------------------------------------------------

#[pg_test]
fn aggregate_rollover_create_and_load_is_not_double_counted() {
    Spi::run("CREATE TABLE zg_src (k INT NOT NULL, v INT) PARTITION BY LIST (k)").expect("root");
    Spi::run("CREATE TABLE zg_src_1 PARTITION OF zg_src FOR VALUES IN (1)").expect("p1");
    Spi::run("INSERT INTO zg_src VALUES (1, 10), (1, 20)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('zg_imv', \
         'SELECT k, SUM(v) AS total FROM zg_src GROUP BY k', \
         NULL, NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("create")
    .expect("create");

    // Rollover: create next period's partition and load it in ONE transaction,
    // so the load's own maintenance delta lands in the brand-new mirror children.
    Spi::run("CREATE TABLE zg_src_5 PARTITION OF zg_src FOR VALUES IN (5)").expect("rollover");
    Spi::run("INSERT INTO zg_src VALUES (5, 100), (5, 200), (5, 300)").expect("load");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("deferred flush");

    assert_imv_correct("zg_imv", "SELECT k, SUM(v) AS total FROM zg_src GROUP BY k");

    let new_total = Spi::get_one::<i64>("SELECT total::int8 FROM zg_imv WHERE k = 5")
        .expect("q")
        .expect("no row for the new partition");
    assert_eq!(
        new_total, 600,
        "aggregate over the rolled-over partition is wrong — the intermediate \
         child kept its maintenance delta and the refill was added on top"
    );
    let old_total = Spi::get_one::<i64>("SELECT total::int8 FROM zg_imv WHERE k = 1")
        .expect("q")
        .expect("no row for the pre-existing partition");
    assert_eq!(old_total, 30, "pre-existing partition must be untouched");
}

#[pg_test]
fn aggregate_rollover_attach_then_load_is_not_double_counted() {
    Spi::run("CREATE TABLE zh_src (k INT NOT NULL, v INT) PARTITION BY LIST (k)").expect("root");
    Spi::run("CREATE TABLE zh_src_1 PARTITION OF zh_src FOR VALUES IN (1)").expect("p1");
    Spi::run("INSERT INTO zh_src VALUES (1, 10), (1, 20)").expect("seed");
    Spi::get_one::<String>(
        "SELECT create_reflex_ivm('zh_imv', \
         'SELECT k, SUM(v) AS total FROM zh_src GROUP BY k', \
         NULL, NULL, NULL, NULL, ARRAY['k'])",
    )
    .expect("create")
    .expect("create");

    // The other route: ATTACH a pre-populated partition, then load more into it
    // in the same transaction.
    Spi::run("CREATE TABLE zh_src_5 (k INT NOT NULL, v INT)").expect("branch");
    Spi::run("INSERT INTO zh_src_5 VALUES (5, 100), (5, 200)").expect("pre-populate");
    Spi::run("ALTER TABLE zh_src ATTACH PARTITION zh_src_5 FOR VALUES IN (5)").expect("attach");
    Spi::run("INSERT INTO zh_src VALUES (5, 300)").expect("load more");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("deferred flush");

    assert_imv_correct("zh_imv", "SELECT k, SUM(v) AS total FROM zh_src GROUP BY k");

    let new_total = Spi::get_one::<i64>("SELECT total::int8 FROM zh_imv WHERE k = 5")
        .expect("q")
        .expect("no row for the new partition");
    assert_eq!(
        new_total, 600,
        "aggregate over the attached-then-loaded partition is wrong"
    );
    let old_total = Spi::get_one::<i64>("SELECT total::int8 FROM zh_imv WHERE k = 1")
        .expect("q")
        .expect("no row for the pre-existing partition");
    assert_eq!(old_total, 30, "pre-existing partition must be untouched");
}

// ---------------------------------------------------------------------------
// A full `reflex_reconcile` of a depth-2 IMV must swap mirror LEAVES, so every
// DETACH/ATTACH lands on the leaf's immediate parent — a BRANCH — and never on
// the IMV root. Swapping top-level children instead (the shape that also
// silently flattened the mirror) DETACHes straight off the root, and PostgreSQL
// holds that AccessExclusiveLock to commit.
//
// This narrows the lock; it does not make the reconcile reader-free. Plan-time
// expansion locks the branches a query reaches, so a reader still blocks behind
// whichever branch is mid-swap. What changes is that the lock is no longer on
// the root, which every reader of the IMV must take regardless of pruning.
// ---------------------------------------------------------------------------

#[pg_test]
fn full_reconcile_never_locks_imv_root_depth2() {
    const DBNAME: &str = "reflex_lockprobe_rc2";
    probe_db_open(DBNAME);
    worker_exec(
        "CREATE TABLE rc2_src (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY LIST (k); \
         CREATE TABLE rc2_src_1 PARTITION OF rc2_src FOR VALUES IN (1) PARTITION BY RANGE (d); \
         CREATE TABLE rc2_src_1_m1 PARTITION OF rc2_src_1 \
             FOR VALUES FROM ('2025-01-01') TO ('2025-02-01'); \
         CREATE TABLE rc2_src_1_m2 PARTITION OF rc2_src_1 \
             FOR VALUES FROM ('2025-02-01') TO ('2025-03-01'); \
         CREATE TABLE rc2_src_2 PARTITION OF rc2_src FOR VALUES IN (2) PARTITION BY RANGE (d); \
         CREATE TABLE rc2_src_2_m1 PARTITION OF rc2_src_2 \
             FOR VALUES FROM ('2025-01-01') TO ('2025-02-01'); \
         INSERT INTO rc2_src SELECT 1, '2025-01-15'::date, g FROM generate_series(1, 500) g; \
         INSERT INTO rc2_src SELECT 1, '2025-02-15'::date, g FROM generate_series(1, 500) g; \
         INSERT INTO rc2_src SELECT 2, '2025-01-15'::date, g FROM generate_series(1, 300) g; \
         ANALYZE rc2_src; \
         DO $mk$ BEGIN PERFORM create_reflex_ivm('rc2_imv', 'SELECT k, d, v FROM rc2_src', \
             'k,d,v', NULL, NULL, NULL, ARRAY['k','d']); END $mk$",
    );
    let pid = worker_pid();
    let imv_oid = worker_relation_oid("rc2_imv");
    let branch_oid = worker_relation_oid("rc2_imv_rc2_src_2");

    let baseline = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM rc2_imv WHERE k = 2");

    worker_exec("BEGIN");
    worker_exec("DO $rc$ BEGIN PERFORM reflex_reconcile('rc2_imv'); END $rc$");

    let root_modes = worker_lock_modes(imv_oid, pid);
    let branch_modes = worker_lock_modes(branch_oid, pid);

    worker_exec("COMMIT");
    let after = read_with_lock_timeout(DBNAME, "SELECT count(*) FROM rc2_imv");
    let partitioned_children = worker_scalar_i64(
        "SELECT count(*)::int8 FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = 'rc2_imv'::regclass AND c.relkind = 'p'",
    );
    let grandchildren = worker_scalar_i64(
        "SELECT count(*)::int8 FROM pg_class c \
         JOIN pg_inherits i  ON i.inhrelid  = c.oid \
         JOIN pg_inherits i2 ON i2.inhrelid = i.inhparent \
         WHERE i2.inhparent = 'rc2_imv'::regclass",
    );
    worker_exec(
        "DROP TABLE IF EXISTS rc2_src CASCADE; DROP TABLE IF EXISTS rc2_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'rc2_imv'",
    );
    probe_db_close(DBNAME);

    assert_eq!(baseline, 300, "baseline reader could not read the IMV");
    assert!(
        !root_modes.iter().any(|m| m == "AccessExclusiveLock"),
        "a full reconcile took AccessExclusiveLock on the IMV ROOT and holds it to commit, \
         freezing every reader of the IMV (locks held on the root: {root_modes:?})"
    );
    assert!(
        branch_modes.iter().any(|m| m == "AccessExclusiveLock"),
        "the swap's AccessExclusiveLock is not on the swapped leaf's immediate parent \
         (locks held on the branch: {branch_modes:?}) — the reconcile did not swap leaves"
    );
    assert_eq!(after, 1300, "the IMV is wrong after the reconcile committed");
    assert_eq!(
        partitioned_children, 2,
        "the full reconcile flattened the depth-2 mirror"
    );
    assert_eq!(
        grandchildren, 3,
        "the full reconcile dropped part of the mirror's sub-partition subtree"
    );
}
