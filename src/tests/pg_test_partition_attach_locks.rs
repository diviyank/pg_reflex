// Attaching a NEW source partition must never take AccessExclusive on the live
// IMV root: readers of the IMV — including readers of an unrelated partition —
// must stay unblocked for the whole transaction (sync + COMMIT-time reconcile).
//
// The lock assertions need an IMV root that this transaction did NOT create
// (CREATE TABLE takes AccessExclusive on the new relation and holds it to
// commit, which would mask the very lock under test). The fixtures are
// therefore built through `dblink` in a separate, committed session, and the
// concurrent reader is a second `dblink` connection with `lock_timeout`.

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

fn sql_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Run DDL/DML in a separate session that COMMITS immediately.
fn remote_exec(sql: &str) {
    let conn = dblink_conninfo();
    Spi::get_one::<String>(&format!(
        "SELECT dblink_exec({}, {})",
        sql_lit(&conn),
        sql_lit(sql)
    ))
    .unwrap_or_else(|e| panic!("remote_exec failed for <{}>: {}", sql, e));
}

/// Read `query` from a separate session with `lock_timeout = 2s`.
/// Returns -1 when the reader blocked (lock timeout) — the freeze under test.
/// The plpgsql EXCEPTION block keeps the block from aborting our transaction,
/// so the assertions and the fixture cleanup below still run.
fn remote_read_count(query: &str) -> i64 {
    Spi::run(
        "CREATE OR REPLACE FUNCTION pg_temp.__reflex_probe_reader(conn text, q text) \
         RETURNS bigint LANGUAGE plpgsql AS $fn$ \
         DECLARE n bigint; \
         BEGIN \
           SELECT c INTO n FROM dblink(conn, q) AS t(c bigint); \
           RETURN n; \
         EXCEPTION WHEN OTHERS THEN RETURN -1; \
         END $fn$",
    )
    .expect("reader probe function");
    let conn = format!("{} options=-c\\ lock_timeout=2000", dblink_conninfo());
    Spi::get_one::<i64>(&format!(
        "SELECT pg_temp.__reflex_probe_reader({}, {})",
        sql_lit(&conn),
        sql_lit(query)
    ))
    .expect("reader probe call")
    .expect("reader probe NULL")
}

/// Lock modes this backend currently holds on `rel`.
fn held_lock_modes(rel: &str) -> Vec<String> {
    Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(mode, ',' ORDER BY mode), '') FROM pg_locks \
         WHERE pid = pg_backend_pid() AND locktype = 'relation' \
           AND relation = {}::regclass AND granted",
        sql_lit(rel)
    ))
    .expect("pg_locks query")
    .expect("pg_locks NULL")
    .split(',')
    .filter(|s| !s.is_empty())
    .map(|s| s.to_string())
    .collect()
}

fn setup_dblink() {
    Spi::run("CREATE EXTENSION IF NOT EXISTS dblink").expect("dblink extension");
}

// NOTE: the committed fixtures below are NOT dropped at the end of the test.
// They cannot be: the test transaction still holds locks on the source table it
// attached a partition to, so a DROP from another session would block until
// this transaction ends — i.e. forever, since we are inside it. Each fixture
// therefore drops its own leftovers up front and is fully idempotent.

// ---------------------------------------------------------------------------
// T1 — mirror depth 2
// ---------------------------------------------------------------------------

#[pg_test]
fn attach_new_partition_never_locks_imv_root_depth2() {
    setup_dblink();
    remote_exec(
        "DROP TABLE IF EXISTS la2_src CASCADE; \
         DROP TABLE IF EXISTS la2_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'la2_imv'; \
         CREATE TABLE la2_src (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY LIST (k); \
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

    // Baseline: the reader is fast when nothing is in flight.
    let baseline = remote_read_count("SELECT count(*) FROM la2_imv WHERE k = 1");

    // Attach a brand-new, pre-populated top-level branch with 3 monthly leaves.
    Spi::run(
        "CREATE TABLE la2_src_5 (k INT NOT NULL, d DATE NOT NULL, v INT) PARTITION BY RANGE (d)",
    )
    .expect("branch");
    for m in 1..=3 {
        Spi::run(&format!(
            "CREATE TABLE la2_src_5_m{m} PARTITION OF la2_src_5 \
             FOR VALUES FROM ('2025-0{m}-01') TO ('2025-0{}-01')",
            m + 1
        ))
        .expect("month");
        Spi::run(&format!(
            "INSERT INTO la2_src_5 SELECT 5, '2025-0{m}-10'::date, g FROM generate_series(1, 400) g"
        ))
        .expect("seed month");
    }
    Spi::run("ALTER TABLE la2_src ATTACH PARTITION la2_src_5 FOR VALUES IN (5)").expect("attach");

    // A reader of a DIFFERENT partition, after sync has run inline.
    let mid = remote_read_count("SELECT count(*) FROM la2_imv WHERE k = 1");

    // Drain the COMMIT-time deferred flush inside this transaction.
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("deferred flush");

    let after1 = remote_read_count("SELECT count(*) FROM la2_imv WHERE k = 1");
    let after2 = remote_read_count("SELECT count(*) FROM la2_imv WHERE k = 1");

    let modes = held_lock_modes("la2_imv");

    assert_eq!(baseline, 1000, "baseline reader could not read the IMV");
    assert_eq!(mid, 1000, "reader of an unrelated partition blocked after the inline sync");
    assert_eq!(after1, 1000, "reader blocked after the deferred reconcile");
    assert_eq!(after2, 1000, "reader blocked after the deferred reconcile");
    assert!(
        !modes.iter().any(|m| m == "AccessExclusiveLock"),
        "IMV root went AccessExclusiveLock (locks held: {:?})",
        modes
    );
    assert!(
        modes.iter().any(|m| m == "ShareUpdateExclusiveLock"),
        "IMV root shows no ShareUpdateExclusiveLock (locks held: {:?})",
        modes
    );
}

// ---------------------------------------------------------------------------
// T2 — mirror depth 1 (the case the detached-skeleton-only proposal misses)
// ---------------------------------------------------------------------------

#[pg_test]
fn attach_new_partition_never_locks_imv_root_depth1() {
    setup_dblink();
    remote_exec(
        "DROP TABLE IF EXISTS la1_src CASCADE; \
         DROP TABLE IF EXISTS la1_imv CASCADE; \
         DELETE FROM public.__reflex_ivm_reference WHERE name = 'la1_imv'; \
         CREATE TABLE la1_src (k INT NOT NULL, v INT) PARTITION BY LIST (k); \
         CREATE TABLE la1_src_1 PARTITION OF la1_src FOR VALUES IN (1); \
         INSERT INTO la1_src SELECT 1, g FROM generate_series(1, 500) g; \
         DO $mk$ BEGIN PERFORM create_reflex_ivm('la1_imv', 'SELECT k, v FROM la1_src', \
             'k,v', NULL, NULL, NULL, ARRAY['k']); END $mk$",
    );

    let baseline = remote_read_count("SELECT count(*) FROM la1_imv WHERE k = 1");

    Spi::run("CREATE TABLE la1_src_5 (k INT NOT NULL, v INT)").expect("branch");
    Spi::run("INSERT INTO la1_src_5 SELECT 5, g FROM generate_series(1, 900) g").expect("seed");
    Spi::run("ALTER TABLE la1_src ATTACH PARTITION la1_src_5 FOR VALUES IN (5)").expect("attach");

    let mid = remote_read_count("SELECT count(*) FROM la1_imv WHERE k = 1");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("deferred flush");
    let after1 = remote_read_count("SELECT count(*) FROM la1_imv WHERE k = 1");
    let after2 = remote_read_count("SELECT count(*) FROM la1_imv WHERE k = 1");

    let modes = held_lock_modes("la1_imv");

    assert_eq!(baseline, 500, "baseline reader could not read the IMV");
    assert_eq!(mid, 500, "reader of an unrelated partition blocked after the inline sync");
    assert_eq!(after1, 500, "reader blocked after the deferred reconcile");
    assert_eq!(after2, 500, "reader blocked after the deferred reconcile");
    assert!(
        !modes.iter().any(|m| m == "AccessExclusiveLock"),
        "IMV root went AccessExclusiveLock at mirror depth 1 (locks held: {:?})",
        modes
    );
    assert!(
        modes.iter().any(|m| m == "ShareUpdateExclusiveLock"),
        "IMV root shows no ShareUpdateExclusiveLock (locks held: {:?})",
        modes
    );
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
