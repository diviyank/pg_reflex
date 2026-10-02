// A reconcile (full rebuild) of a DEFERRED IMV inside a transaction that
// staged deltas for it: the rebuild already reflects every staged delta, so the
// COMMIT flush must not apply them again, and must still apply the ones staged
// after the rebuild. Reached explicitly (`reflex_reconcile` /
// `reflex_rebuild_imv`), and automatically by the refresh an upstream rebuild
// gives its IGNORING dependents (high-selectivity dispatch, partitioned
// cascade).
//
// `SET CONSTRAINTS ALL IMMEDIATE` fires the queued COMMIT flush inside the test
// transaction. The oracle is the bidirectional EXCEPT ALL against the base
// tables, plus known_stale false.

const DRC_UPSTREAM_SQL: &str = "SELECT g, SUM(w) AS sw, COUNT(*) AS n FROM {p}_us GROUP BY g";

/// `{p}_s` (the dependent's own source), `{p}_us` (the upstream's source), an
/// aggregate `{p}_u` over `{p}_us` in `upstream_mode` whose every flushed write is
/// delegated to a full reconcile (wipe_threshold 0; only a DEFERRED flush
/// dispatches, a statement trigger never rebuilds), and a DEFERRED aggregate
/// `{p}_d` reading `{p}_s` and `{p}_u` that IGNORES `{p}_u`, kept on the
/// incremental flush path.
fn drc_build(p: &str, upstream_mode: &str) -> String {
    Spi::run(&format!(
        "CREATE TABLE {p}_s (k INT PRIMARY KEY, g INT, v INT)"
    ))
    .expect("s");
    Spi::run(&format!(
        "INSERT INTO {p}_s SELECT i, i % 3, i FROM generate_series(1, 30) i"
    ))
    .expect("seed s");
    Spi::run(&format!(
        "CREATE TABLE {p}_us (id INT PRIMARY KEY, g INT, w INT)"
    ))
    .expect("us");
    Spi::run(&format!(
        "INSERT INTO {p}_us SELECT i, i % 3, i FROM generate_series(1, 12) i"
    ))
    .expect("seed us");
    assert_eq!(
        crate::create_reflex_ivm(
            &format!("{p}_u"),
            &DRC_UPSTREAM_SQL.replace("{p}", p),
            None,
            None,
            Some(upstream_mode),
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run(&format!(
        "UPDATE public.__reflex_ivm_reference SET wipe_threshold = 0 WHERE name = '{p}_u'"
    ))
    .expect("every write of the upstream is a full reconcile");
    let d_sql = format!(
        "SELECT s.g, COUNT(*) AS n, SUM(s.v) AS sv, SUM(u.sw) AS usw \
         FROM {p}_s s LEFT JOIN {p}_u u ON u.g = s.g GROUP BY s.g"
    );
    assert_eq!(
        crate::create_reflex_ivm(
            &format!("{p}_d"),
            &d_sql,
            None,
            None,
            Some("DEFERRED"),
            Some(&format!("!{p}_u"))
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    dmw_force_incremental(&format!("{p}_d"));
    d_sql.replace(
        &format!("{p}_u"),
        &format!("({})", DRC_UPSTREAM_SQL.replace("{p}", p)),
    )
}

fn drc_reconcile(view: &str) {
    let res = Spi::get_one::<&str>(&format!("SELECT reflex_reconcile('{view}')"))
        .expect("reconcile")
        .expect("reconcile result");
    assert_eq!(res, "RECONCILED");
}

/// The review's reproduction: a write to the dependent's own source is staged,
/// then a write to the upstream's source dispatches the upstream to a full
/// reconcile, which refreshes the ignoring dependent from the base tables.
#[pg_test]
fn pg_drc_dispatch_reconcile_refreshes_ignoring_dependent_once() {
    let fresh = drc_build("drc1", "DEFERRED");
    Spi::run("INSERT INTO drc1_s VALUES (100, 0, 1000)").expect("staged for the dependent");
    Spi::run("UPDATE drc1_us SET w = w + 1").expect("upstream's flush dispatches a reconcile");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    assert_imv_correct("drc1_u", &DRC_UPSTREAM_SQL.replace("{p}", "drc1"));
    dmw_assert_fresh("drc1_d", &fresh);

    Spi::run("INSERT INTO drc1_s VALUES (101, 1, 7)").expect("write after the flush");
    Spi::run("UPDATE drc1_s SET v = v + 1 WHERE k <= 4").expect("update after the flush");
    dmw_assert_fresh("drc1_d", &fresh);
}

/// Same, through an explicit `reflex_reconcile` of the upstream.
#[pg_test]
fn pg_drc_explicit_upstream_reconcile_refreshes_ignoring_dependent_once() {
    let fresh = drc_build("drc2", "IMMEDIATE");
    Spi::run("INSERT INTO drc2_s VALUES (100, 0, 1000)").expect("staged for the dependent");
    Spi::run("ALTER TABLE drc2_us DISABLE TRIGGER USER").expect("disable");
    Spi::run("UPDATE drc2_us SET w = w + 1").expect("upstream drifts");
    Spi::run("ALTER TABLE drc2_us ENABLE TRIGGER USER").expect("enable");
    drc_reconcile("drc2_u");
    Spi::run("INSERT INTO drc2_s VALUES (101, 1, 7)").expect("staged after the reconcile");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drc2_d", &fresh);
}

/// The ignoring dependent also reads the table whose write dispatched the
/// upstream to a reconcile: that statement's delta for the dependent is staged
/// only after the upstream's maintenance returns, i.e. after any refresh of the
/// dependent run from inside it, although that refresh already read the write.
/// Real COMMITs, in a private database through `dblink`.
#[pg_test]
fn pg_drc_dispatch_reconcile_dependent_reading_the_written_table() {
    const DBNAME: &str = "reflex_drc_written";
    let up_sql = "SELECT g, SUM(w) AS sw, COUNT(*) AS n FROM drc6_us GROUP BY g";
    let d_sql = "SELECT us.g, COUNT(*) AS n, SUM(us.w) AS sw, SUM(u.n) AS un \
                 FROM drc6_us us LEFT JOIN drc6_u u ON u.g = us.g GROUP BY us.g";
    let fresh = d_sql.replace("drc6_u u", &format!("({up_sql}) u"));
    probe_db_open(DBNAME);
    worker_exec("CREATE TABLE drc6_us (id INT PRIMARY KEY, g INT, w INT)");
    worker_exec("INSERT INTO drc6_us SELECT i, i % 3, i FROM generate_series(1, 12) i");
    let created = rbc_select(&format!(
        "create_reflex_ivm('drc6_u', {}, NULL, NULL, 'DEFERRED')",
        sql_lit(up_sql)
    ));
    assert!(!created.starts_with("ERROR"), "create drc6_u: {created}");
    worker_exec(
        "UPDATE public.__reflex_ivm_reference SET wipe_threshold = 0 WHERE name = 'drc6_u'",
    );
    let created = rbc_select(&format!(
        "create_reflex_ivm('drc6_d', {}, NULL, NULL, 'DEFERRED', '!drc6_u')",
        sql_lit(d_sql)
    ));
    assert!(!created.starts_with("ERROR"), "create drc6_d: {created}");
    worker_exec(
        "UPDATE public.__reflex_ivm_reference SET wipe_threshold = 1e9 WHERE name = 'drc6_d'",
    );

    worker_exec("BEGIN; UPDATE drc6_us SET g = 0, w = w + 100 WHERE id = 1; COMMIT");
    assert_eq!(rbc_mismatch("drc6_d", &fresh), 0, "one dispatched write");
    worker_exec(
        "BEGIN; \
         UPDATE drc6_us SET w = w + 1; \
         INSERT INTO drc6_us VALUES (100, 2, 1000); \
         UPDATE drc6_us SET g = 1 WHERE id = 2; \
         COMMIT",
    );
    assert_eq!(
        rbc_mismatch("drc6_d", &fresh),
        0,
        "dispatched writes and a plain write"
    );
    assert!(!rbc_known_stale("drc6_d"), "drc6_d left known_stale");
    probe_db_close(DBNAME);
}

/// The review's reproduction with a real COMMIT.
#[pg_test]
fn pg_drc_dispatch_reconcile_refreshes_ignoring_dependent_once_commit() {
    const DBNAME: &str = "reflex_drc_commit";
    let up_sql = DRC_UPSTREAM_SQL.replace("{p}", "drc7");
    let d_sql = "SELECT s.g, COUNT(*) AS n, SUM(s.v) AS sv, SUM(u.sw) AS usw \
                 FROM drc7_s s LEFT JOIN drc7_u u ON u.g = s.g GROUP BY s.g";
    let fresh = d_sql.replace("drc7_u u", &format!("({up_sql}) u"));
    probe_db_open(DBNAME);
    worker_exec("CREATE TABLE drc7_s (k INT PRIMARY KEY, g INT, v INT)");
    worker_exec("INSERT INTO drc7_s SELECT i, i % 3, i FROM generate_series(1, 30) i");
    worker_exec("CREATE TABLE drc7_us (id INT PRIMARY KEY, g INT, w INT)");
    worker_exec("INSERT INTO drc7_us SELECT i, i % 3, i FROM generate_series(1, 12) i");
    let created = rbc_select(&format!(
        "create_reflex_ivm('drc7_u', {}, NULL, NULL, 'DEFERRED')",
        sql_lit(&up_sql)
    ));
    assert!(!created.starts_with("ERROR"), "create drc7_u: {created}");
    worker_exec(
        "UPDATE public.__reflex_ivm_reference SET wipe_threshold = 0 WHERE name = 'drc7_u'",
    );
    let created = rbc_select(&format!(
        "create_reflex_ivm('drc7_d', {}, NULL, NULL, 'DEFERRED', '!drc7_u')",
        sql_lit(d_sql)
    ));
    assert!(!created.starts_with("ERROR"), "create drc7_d: {created}");
    worker_exec(
        "UPDATE public.__reflex_ivm_reference SET wipe_threshold = 1e9 WHERE name = 'drc7_d'",
    );

    worker_exec(
        "BEGIN; \
         INSERT INTO drc7_s VALUES (100, 0, 1000); \
         UPDATE drc7_us SET w = w + 1; \
         INSERT INTO drc7_s VALUES (101, 1, 7); \
         COMMIT",
    );
    assert_eq!(rbc_mismatch("drc7_d", &fresh), 0);
    assert!(!rbc_known_stale("drc7_d"), "drc7_d left known_stale");
    probe_db_close(DBNAME);
}

/// A partitioned upstream: its rebuild refreshes the ignoring dependent through
/// the partitioned cascade.
#[pg_test]
fn pg_drc_partitioned_cascade_refreshes_ignoring_dependent_once() {
    rda_build_partitioned("drc3");
    Spi::run("CREATE TABLE drc3_s (k INT PRIMARY KEY, g INT, v INT)").expect("s");
    Spi::run("INSERT INTO drc3_s SELECT i, 1 + i % 3, i FROM generate_series(1, 30) i")
        .expect("seed s");
    let d_sql = "SELECT s.g, COUNT(*) AS n, SUM(s.v) AS sv, SUM(u.qty) AS q \
                 FROM drc3_s s LEFT JOIN drc3_up u ON u.plan = s.g AND u.id = s.k GROUP BY s.g";
    assert_eq!(
        crate::create_reflex_ivm(
            "drc3_d",
            d_sql,
            None,
            None,
            Some("DEFERRED"),
            Some("!drc3_up")
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    dmw_force_incremental("drc3_d");
    let fresh = d_sql.replace("drc3_up", "drc3_src");

    Spi::run("INSERT INTO drc3_s VALUES (100, 1, 1000)").expect("staged for the dependent");
    Spi::run("ALTER TABLE drc3_up DISABLE TRIGGER USER").expect("disable");
    Spi::run("UPDATE drc3_up SET qty = qty + 1000 WHERE plan = 2 AND id = 2").expect("drift");
    Spi::run("ALTER TABLE drc3_up ENABLE TRIGGER USER").expect("enable");
    drc_reconcile("drc3_up");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drc3_d", &fresh);
}

/// The filed report's reproduction: an explicit reconcile of the DEFERRED IMV
/// itself, with a write staged before it and writes staged after it.
#[pg_test]
fn pg_drc_explicit_reconcile_of_deferred_imv_with_staged_deltas() {
    Spi::run("CREATE TABLE drc4_s (k INT PRIMARY KEY, g INT, v INT)").expect("s");
    Spi::run("INSERT INTO drc4_s SELECT i, i % 3, i FROM generate_series(1, 30) i").expect("seed");
    let sql = "SELECT g, COUNT(*) AS n, SUM(v) AS s FROM drc4_s GROUP BY g";
    dmw_create_deferred("drc4_v", sql, None);
    dmw_force_incremental("drc4_v");

    Spi::run("INSERT INTO drc4_s VALUES (100, 0, 1000)").expect("staged before");
    drc_reconcile("drc4_v");
    Spi::run("INSERT INTO drc4_s VALUES (101, 1, 7)").expect("staged after");
    Spi::run("UPDATE drc4_s SET v = v + 1 WHERE k <= 4").expect("update after");
    Spi::run("DELETE FROM drc4_s WHERE k = 30").expect("delete after");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drc4_v", sql);
}

/// Keyed passthrough: re-applying a staged insert the rebuild already holds
/// would hit the IMV's unique key and flag it stale.
#[pg_test]
fn pg_drc_explicit_reconcile_of_deferred_passthrough_with_staged_deltas() {
    Spi::run("CREATE TABLE drc5_s (k INT PRIMARY KEY, v INT)").expect("s");
    Spi::run("INSERT INTO drc5_s SELECT i, i FROM generate_series(1, 30) i").expect("seed");
    let sql = "SELECT k, v FROM drc5_s";
    dmw_create_deferred("drc5_v", sql, Some("k"));
    dmw_force_incremental("drc5_v");

    Spi::run("INSERT INTO drc5_s VALUES (100, 100)").expect("staged before");
    Spi::run("UPDATE drc5_s SET v = -v WHERE k <= 3").expect("staged before");
    Spi::run("SELECT reflex_rebuild_imv('drc5_v')").expect("rebuild");
    Spi::run("INSERT INTO drc5_s VALUES (101, 101)").expect("staged after");
    Spi::run("UPDATE drc5_s SET v = v + 1 WHERE k = 100").expect("update after");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drc5_v", sql);
}

/// Runs `sql` on the worker without raising; returns the error message, empty on success.
fn drc_worker_try(sql: &str) -> String {
    let _ = Spi::get_one::<String>(&format!(
        "SELECT dblink_exec('reflex_lock_worker', {}, false)",
        sql_lit(sql)
    ))
    .expect("dblink_exec");
    Spi::get_one::<String>("SELECT dblink_error_message('reflex_lock_worker')")
        .expect("dblink_error_message")
        .map(|m| if m == "OK" { String::new() } else { m })
        .unwrap_or_default()
}

/// A COMMIT-time rebuild postponed while an upstream DEFERRED IMV is pending
/// flags the IMV stale with a registry write held to COMMIT. Another session
/// flushing that IMV takes its advisory lock, a RowExclusiveLock on it, then
/// writes its registry row. Unless the postponement takes the advisory lock
/// first, the second session waits on the registry row while the first, when
/// the upstream settles, waits on the IMV to rebuild it: 40P01.
#[pg_test]
fn pg_drc_postponed_rebuild_flag_takes_imv_lock_first() {
    const DBNAME: &str = "reflex_drc_postpone";
    const FLUSHER: &str = "DO $f$ BEGIN \
         PERFORM pg_advisory_xact_lock(hashtext('dl_x'), hashtext(reverse('dl_x'))); \
         LOCK TABLE dl_x IN ROW EXCLUSIVE MODE; \
         UPDATE public.__reflex_ivm_reference SET last_update_date = now() WHERE name = 'dl_x'; \
       END $f$";
    let x_fresh = "SELECT t1.k, t1.v, t2.w, d.a FROM dl_t1 t1 JOIN dl_t2 t2 ON t2.k = t1.k \
                   LEFT JOIN dl_rel d ON d.k = t1.k";
    probe_db_open(DBNAME);
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('drc_flusher', {})",
        sql_lit(&conninfo_for(DBNAME))
    ))
    .expect("flusher connect")
    .expect("flusher connect NULL");
    worker_exec("CREATE TABLE dl_t1 (k INT PRIMARY KEY, v INT)");
    worker_exec("CREATE TABLE dl_t2 (k INT PRIMARY KEY, w INT)");
    worker_exec("CREATE TABLE dl_rel (k INT PRIMARY KEY, a BOOL)");
    worker_exec("INSERT INTO dl_t1 SELECT g, g FROM generate_series(1, 20) g");
    worker_exec("INSERT INTO dl_t2 SELECT g, g FROM generate_series(1, 20) g");
    worker_exec("INSERT INTO dl_rel SELECT g, g % 2 = 0 FROM generate_series(1, 20) g");
    rbc_create_deferred("dl_d", "SELECT k, a FROM dl_rel", "k");
    rbc_create_deferred("dl_x", &x_fresh.replace("dl_rel d", "dl_d d"), "k");

    worker_exec("BEGIN");
    worker_exec("UPDATE dl_t1 SET v = v + 1 WHERE k = 1");
    worker_exec("UPDATE dl_t2 SET w = w + 1 WHERE k = 2");
    worker_exec("UPDATE dl_rel SET a = NOT a WHERE k = 3");
    worker_exec("DO $f$ BEGIN PERFORM reflex_flush_deferred('dl_t1'); END $f$");
    assert_eq!(
        rbc_select(
            "(SELECT stale_reason FROM public.__reflex_ivm_reference WHERE name = 'dl_x') \
             LIKE '%postponed%'"
        ),
        "true",
        "precondition: dl_x's COMMIT-time rebuild was postponed"
    );
    Spi::run(&format!(
        "SELECT dblink_send_query('drc_flusher', {})",
        sql_lit(FLUSHER)
    ))
    .expect("async flusher");
    Spi::run(
        "CREATE FUNCTION pg_temp.drc_await_lock_wait(q text) RETURNS bool LANGUAGE plpgsql AS $fn$ \
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
    let flusher_waiting = Spi::get_one::<bool>(&format!(
        "SELECT pg_temp.drc_await_lock_wait({})",
        sql_lit(FLUSHER)
    ))
    .expect("poll")
    .unwrap_or(false);

    let rebuild_error = drc_worker_try(
        "DO $f$ BEGIN PERFORM reflex_flush_deferred('dl_rel'); \
         PERFORM reflex_flush_deferred('dl_t2'); END $f$",
    );
    let commit_error = if rebuild_error.is_empty() {
        drc_worker_try("COMMIT")
    } else {
        worker_exec("ROLLBACK");
        String::new()
    };
    let _ = Spi::get_one::<String>(
        "SELECT r FROM dblink_get_result('drc_flusher', false) AS t(r text)",
    );
    let flusher_error = Spi::get_one::<String>("SELECT dblink_error_message('drc_flusher')")
        .expect("dblink_error_message")
        .map(|m| if m == "OK" { String::new() } else { m })
        .unwrap_or_default();
    let _ = Spi::get_one::<String>("SELECT dblink_disconnect('drc_flusher')");

    assert!(
        flusher_waiting,
        "the flusher was never observed waiting on a lock"
    );
    assert_eq!(rebuild_error, "", "the rebuilding transaction failed");
    assert_eq!(
        commit_error, "",
        "the rebuilding transaction's COMMIT failed"
    );
    assert_eq!(flusher_error, "", "the flushing session failed");
    assert_eq!(rbc_mismatch("dl_x", x_fresh), 0, "dl_x incorrect");
    assert!(!rbc_known_stale("dl_x"), "dl_x left known_stale");
    probe_db_close(DBNAME);
}

/// The multi-source guard's COMMIT-time rebuild of a partitioned IMV must not
/// drop an orphan IMV partition (destructive DDL that `reflex_doctor` refuses
/// without `drop_orphans`), and must still converge with the orphan present.
#[pg_test]
fn pg_drc_guard_rebuild_keeps_orphan_partition() {
    Spi::run(
        "CREATE TABLE drco_src (id INT NOT NULL, region TEXT NOT NULL, amount INT) \
         PARTITION BY LIST (region)",
    )
    .expect("src");
    Spi::run("CREATE TABLE drco_src_n PARTITION OF drco_src FOR VALUES IN ('N')").expect("n");
    Spi::run("CREATE TABLE drco_src_s PARTITION OF drco_src FOR VALUES IN ('S')").expect("s");
    Spi::run(
        "INSERT INTO drco_src SELECT g, CASE WHEN g % 2 = 0 THEN 'N' ELSE 'S' END, g \
              FROM generate_series(1, 40) g",
    )
    .expect("seed src");
    Spi::run("CREATE TABLE drco_dim (id INT PRIMARY KEY, f INT)").expect("dim");
    Spi::run("INSERT INTO drco_dim SELECT g, 1 + g % 3 FROM generate_series(1, 40) g")
        .expect("seed dim");
    let sql = "SELECT s.region, SUM(s.amount * d.f) AS total, COUNT(*) AS n \
               FROM drco_src s JOIN drco_dim d ON d.id = s.id GROUP BY s.region";
    create_imv(
        "drco_v",
        &format!(
            "SELECT create_reflex_ivm('drco_v', '{sql}', NULL, NULL, 'DEFERRED', NULL, \
             ARRAY['region'])"
        ),
    );
    dmw_force_incremental("drco_v");
    // Detached with pg_reflex's event and constraint triggers silent (as under
    // replication), so no partition flush ever handles it: an orphan.
    Spi::run("SET LOCAL session_replication_role = replica").expect("replica");
    Spi::run("ALTER TABLE drco_src DETACH PARTITION drco_src_s").expect("detach");
    Spi::run("SET LOCAL session_replication_role = origin").expect("origin");
    let imv_partitions = || {
        Spi::get_one::<i64>(
            "SELECT count(*)::int8 FROM pg_inherits WHERE inhparent = 'drco_v'::regclass",
        )
        .expect("q")
        .expect("c")
    };
    assert_eq!(
        imv_partitions(),
        2,
        "precondition: the IMV's S partition is an orphan"
    );

    Spi::run("UPDATE drco_src SET amount = amount + 1 WHERE id <= 10").expect("source 1");
    Spi::run("UPDATE drco_dim SET f = f + 1 WHERE id <= 10").expect("source 2");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");

    assert!(
        dmw_marked("drco_v"),
        "precondition: the multi-source guard rebuilt drco_v"
    );
    assert_eq!(
        imv_partitions(),
        2,
        "the guard's rebuild dropped the orphan partition"
    );
    assert_imv_correct(
        "(SELECT * FROM drco_v WHERE region = 'N') v",
        &format!("SELECT * FROM ({sql}) f WHERE region = 'N'"),
    );
    let stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'drco_v'",
    )
    .expect("registry")
    .unwrap_or(false);
    assert!(!stale, "drco_v flagged stale");
}

/// `{p}_s` partitioned by LIST (plan) in plans 1..3, ids 1..20 each.
fn drc_build_partitioned_source(p: &str) {
    Spi::run(&format!(
        "CREATE TABLE {p}_s (plan INT NOT NULL, id INT NOT NULL, qty INT) PARTITION BY LIST (plan)"
    ))
    .expect("s");
    for plan in [1, 2, 3] {
        Spi::run(&format!(
            "CREATE TABLE {p}_s{plan} PARTITION OF {p}_s FOR VALUES IN ({plan})"
        ))
        .expect("s partition");
    }
    Spi::run(&format!(
        "INSERT INTO {p}_s SELECT v.plan, g, g FROM generate_series(1, 20) g, \
         (VALUES (1), (2), (3)) v(plan)"
    ))
    .expect("seed s");
}

fn drc_create_partitioned_deferred(view: &str, sql: &str, key: Option<&str>, ignore: &str) {
    let key = key.map_or("NULL".to_string(), |k| format!("'{k}'"));
    let ignore = if ignore.is_empty() {
        "NULL".to_string()
    } else {
        format!("'{ignore}'")
    };
    create_imv(
        view,
        &format!(
            "SELECT create_reflex_ivm('{view}', {}, {key}, NULL, 'DEFERRED', {ignore}, \
             ARRAY['plan'])",
            sql_lit(sql)
        ),
    );
    dmw_force_incremental(view);
}

fn drc_reconcile_partition(view: &str, keys: &str) {
    let res = Spi::get_one::<String>(&format!(
        "SELECT reflex_reconcile_partition('{view}', '{keys}')"
    ))
    .expect("reconcile_partition")
    .expect("reconcile_partition result");
    assert!(res.starts_with("RECONCILED"), "reconcile_partition: {res}");
}

/// Whether `pg_temp.<table>` exists and has a row named `imv`.
fn drc_temp_lists(table: &str, imv: &str) -> bool {
    let exists = Spi::get_one::<bool>(&format!(
        "SELECT to_regclass('pg_temp.{table}') IS NOT NULL"
    ))
    .expect("to_regclass")
    .unwrap_or(false);
    exists
        && Spi::get_one::<bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_temp.{table} WHERE name = '{imv}')"
        ))
        .expect("temp table")
        .unwrap_or(false)
}

/// Whether a partition- or key-scoped rebuild of `imv` was recorded with its
/// watermark (the flush then skips only the rebuilt slice's earlier deltas).
fn drc_scoped_recorded(imv: &str) -> bool {
    drc_temp_lists("__reflex_deferred_scoped_rebuilds", imv)
}

/// Whether `imv` is listed for the COMMIT-time full reconcile.
fn drc_listed_for_commit(imv: &str) -> bool {
    drc_temp_lists("__reflex_deferred_rebuild", imv)
}

/// The filed report's reproduction: a partition-scoped rebuild of a DEFERRED
/// IMV after a write staged for the rebuilt partition, plus writes staged for
/// another partition before it and for the rebuilt one after it.
#[pg_test]
fn pg_drc_partition_reconcile_of_deferred_imv_with_staged_deltas() {
    drc_build_partitioned_source("drp1");
    let sql = "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM drp1_s GROUP BY plan";
    drc_create_partitioned_deferred("drp1_v", sql, None, "");

    Spi::run("INSERT INTO drp1_s VALUES (2, 100, 1000)").expect("staged, rebuilt plan");
    Spi::run("INSERT INTO drp1_s VALUES (1, 100, 500)").expect("staged, other plan");
    Spi::run("UPDATE drp1_s SET qty = qty + 1 WHERE plan = 3 AND id <= 3")
        .expect("staged, other plan");
    drc_reconcile_partition("drp1_v", "2");
    assert!(
        drc_scoped_recorded("drp1_v"),
        "drp1_v's scoped rebuild not recorded"
    );
    assert!(
        !drc_listed_for_commit("drp1_v"),
        "drp1_v needlessly listed for a COMMIT-time full rebuild"
    );
    Spi::run("INSERT INTO drp1_s VALUES (2, 101, 7)").expect("staged after, rebuilt plan");
    Spi::run("DELETE FROM drp1_s WHERE plan = 2 AND id = 1").expect("staged after");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drp1_v", sql);
}

/// A row moved across partitions before the rebuild of its new partition: its
/// old image (outside the rebuilt partition) must still be applied, its new
/// image (inside it) must not. Aggregate and keyed passthrough.
#[pg_test]
fn pg_drc_partition_reconcile_after_cross_partition_update() {
    drc_build_partitioned_source("drp2");
    let agg = "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM drp2_s GROUP BY plan";
    let pass = "SELECT plan, id, qty FROM drp2_s";
    drc_create_partitioned_deferred("drp2_a", agg, None, "");
    drc_create_partitioned_deferred("drp2_p", pass, Some("plan, id"), "");

    Spi::run("UPDATE drp2_s SET plan = 2, id = id + 100 WHERE plan = 1 AND id <= 3")
        .expect("moved 1 -> 2");
    Spi::run("UPDATE drp2_s SET plan = 3, id = id + 200 WHERE plan = 2 AND id = 5")
        .expect("moved 2 -> 3");
    drc_reconcile_partition("drp2_a", "2");
    drc_reconcile_partition("drp2_p", "2");
    Spi::run("UPDATE drp2_s SET plan = 1, id = id - 100 WHERE plan = 2 AND id = 101")
        .expect("moved back after the rebuild");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drp2_a", agg);
    dmw_assert_fresh("drp2_p", pass);
}

/// The IMV's partition column comes from a joined table, not from the staged
/// source: a staged row's own `plan` does not say which partition it lands in.
/// Falls back to a COMMIT-time full rebuild.
#[pg_test]
fn pg_drc_partition_reconcile_partition_column_from_joined_table() {
    drc_build_partitioned_source("drp3");
    Spi::run("CREATE TABLE drp3_dim (id INT PRIMARY KEY, plan INT NOT NULL)").expect("dim");
    Spi::run("INSERT INTO drp3_dim SELECT g, 1 + g % 3 FROM generate_series(1, 300) g")
        .expect("seed dim");
    let sql = "SELECT d.plan, SUM(s.qty) AS q, COUNT(*) AS n \
               FROM drp3_s s JOIN drp3_dim d ON d.id = s.id GROUP BY d.plan";
    drc_create_partitioned_deferred("drp3_v", sql, None, "!drp3_dim");

    // id 1 joins dim plan 2, while the row itself is in source plan 1.
    Spi::run("INSERT INTO drp3_s VALUES (1, 1, 1000)").expect("staged, lands in plan 2");
    drc_reconcile_partition("drp3_v", "2");
    assert!(
        drc_listed_for_commit("drp3_v"),
        "drp3_v not listed for the COMMIT-time full rebuild"
    );
    Spi::run("INSERT INTO drp3_s VALUES (3, 4, 9)").expect("staged after");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drp3_v", sql);
}

/// A partition-scoped rebuild of a DEFERRED IMV reached from a trigger that
/// fires before the statement's own staging trigger reads a write whose delta
/// is staged after it.
#[pg_test]
fn pg_drc_partition_reconcile_inside_trigger_before_staging() {
    drc_build_partitioned_source("drp4");
    let sql = "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM drp4_s GROUP BY plan";
    drc_create_partitioned_deferred("drp4_v", sql, None, "");
    Spi::run(
        "CREATE FUNCTION drp4_rec() RETURNS trigger LANGUAGE plpgsql AS $f$ \
         BEGIN PERFORM reflex_reconcile_partition('drp4_v', '2'); RETURN NULL; END $f$",
    )
    .expect("fn");
    // Named to sort before pg_reflex's `__reflex_*` triggers, so it fires first.
    Spi::run(
        "CREATE TRIGGER \"A_drp4_rec\" AFTER INSERT ON drp4_s \
         FOR EACH STATEMENT EXECUTE FUNCTION drp4_rec()",
    )
    .expect("trigger");

    Spi::run("INSERT INTO drp4_s VALUES (2, 100, 1000)").expect("write + in-trigger rebuild");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drp4_v", sql);
}

/// `reflex_reconcile_partition` of an UNPARTITIONED DEFERRED IMV from inside a
/// trigger is refused as everywhere else, never queued for a COMMIT-time full
/// rebuild.
#[pg_test]
fn pg_drc_in_trigger_partition_reconcile_of_unpartitioned_imv_is_refused() {
    Spi::run("CREATE TABLE dru_s (k INT PRIMARY KEY, g INT, v INT)").expect("s");
    Spi::run("INSERT INTO dru_s SELECT i, i % 3, i FROM generate_series(1, 30) i").expect("seed");
    let sql = "SELECT g, COUNT(*) AS n, SUM(v) AS s FROM dru_s GROUP BY g";
    dmw_create_deferred("dru_v", sql, None);
    Spi::run("CREATE TABLE dru_log (result TEXT)").expect("log");
    Spi::run(
        "CREATE FUNCTION dru_rec() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN \
           INSERT INTO dru_log SELECT reflex_reconcile_partition('dru_v', '1'); \
           RETURN NULL; END $f$",
    )
    .expect("fn");
    Spi::run(
        "CREATE TRIGGER \"A_dru_rec\" AFTER INSERT ON dru_s \
         FOR EACH STATEMENT EXECUTE FUNCTION dru_rec()",
    )
    .expect("trigger");

    Spi::run("INSERT INTO dru_s VALUES (100, 1, 1000)").expect("write + in-trigger reconcile");
    let result = Spi::get_one::<String>("SELECT result FROM dru_log")
        .expect("log")
        .unwrap_or_default();
    assert!(
        result.starts_with("ERROR") && result.contains("not partitioned"),
        "in-trigger reconcile_partition of an unpartitioned IMV returned: {result}"
    );
    assert!(
        !drc_listed_for_commit("dru_v"),
        "dru_v listed for a COMMIT-time full rebuild"
    );
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("dru_v", sql);
}

/// A DEFERRED IMV reconciled from inside a trigger: an enabled one is queued for
/// the COMMIT-time pass (`RECONCILE QUEUED FOR COMMIT`); a disabled one is
/// refused like everywhere else, and never flagged stale at COMMIT.
#[pg_test]
fn pg_drc_in_trigger_reconcile_queues_enabled_and_skips_disabled_imv() {
    Spi::run("CREATE TABLE drn_s (k INT PRIMARY KEY, g INT, v INT)").expect("s");
    Spi::run("INSERT INTO drn_s SELECT i, i % 3, i FROM generate_series(1, 30) i").expect("seed");
    let sql = "SELECT g, COUNT(*) AS n, SUM(v) AS s FROM drn_s GROUP BY g";
    dmw_create_deferred("drn_on", sql, None);
    dmw_create_deferred("drn_off", sql, None);
    Spi::run("UPDATE public.__reflex_ivm_reference SET enabled = FALSE WHERE name = 'drn_off'")
        .expect("disable");
    Spi::run("CREATE TABLE drn_log (imv TEXT, result TEXT)").expect("log");
    Spi::run("CREATE TABLE drn_t (x INT)").expect("t");
    Spi::run(
        "CREATE FUNCTION drn_rec() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN \
           INSERT INTO drn_log SELECT i, reflex_reconcile(i) FROM unnest(ARRAY['drn_on', 'drn_off']) i; \
           RETURN NULL; END $f$",
    )
    .expect("fn");
    Spi::run(
        "CREATE TRIGGER drn_rec AFTER INSERT ON drn_t FOR EACH STATEMENT EXECUTE FUNCTION drn_rec()",
    )
    .expect("trigger");

    Spi::run("INSERT INTO drn_t VALUES (1)").expect("in-trigger reconciles");
    let result = |imv: &str| {
        Spi::get_one::<String>(&format!("SELECT result FROM drn_log WHERE imv = '{imv}'"))
            .expect("log")
            .unwrap_or_default()
    };
    assert_eq!(result("drn_on"), "RECONCILE QUEUED FOR COMMIT");
    assert!(
        result("drn_off").starts_with("ERROR"),
        "disabled IMV: {}",
        result("drn_off")
    );
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drn_on", sql);
    let off_stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'drn_off'",
    )
    .expect("registry")
    .unwrap_or(false);
    assert!(
        !off_stale,
        "disabled drn_off flagged stale by the COMMIT-time pass"
    );
}

/// A DEFERRED IMV queued for the COMMIT-time reconcile and disabled before
/// COMMIT is skipped by the pass, not flagged stale.
#[pg_test]
fn pg_drc_queued_reconcile_of_imv_disabled_before_commit_not_flagged() {
    Spi::run("CREATE TABLE drn2_s (k INT PRIMARY KEY, g INT, v INT)").expect("s");
    Spi::run("INSERT INTO drn2_s SELECT i, i % 3, i FROM generate_series(1, 30) i").expect("seed");
    let sql = "SELECT g, COUNT(*) AS n, SUM(v) AS s FROM drn2_s GROUP BY g";
    dmw_create_deferred("drn2_v", sql, None);
    Spi::run("CREATE TABLE drn2_t (x INT)").expect("t");
    Spi::run(
        "CREATE FUNCTION drn2_rec() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN \
           PERFORM reflex_reconcile('drn2_v'); RETURN NULL; END $f$",
    )
    .expect("fn");
    Spi::run(
        "CREATE TRIGGER drn2_rec AFTER INSERT ON drn2_t FOR EACH STATEMENT EXECUTE FUNCTION drn2_rec()",
    )
    .expect("trigger");

    Spi::run("INSERT INTO drn2_t VALUES (1)").expect("queued");
    Spi::run("UPDATE public.__reflex_ivm_reference SET enabled = FALSE WHERE name = 'drn2_v'")
        .expect("disable");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    let stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'drn2_v'",
    )
    .expect("registry")
    .unwrap_or(false);
    assert!(
        !stale,
        "disabled drn2_v flagged stale by the COMMIT-time pass"
    );
}

/// A per-IMV flush failure is handled after its subtransaction rolled back,
/// which released the IMV's advisory lock taken inside it; the handler's
/// registry write must hold it again (lock order: advisory lock, then row).
#[pg_test]
fn pg_drc_flush_failure_handler_holds_imv_lock() {
    Spi::run("CREATE TABLE drl_s (k INT PRIMARY KEY, v INT)").expect("s");
    Spi::run("INSERT INTO drl_s SELECT i, i FROM generate_series(1, 10) i").expect("seed");
    dmw_create_deferred("drl_v", "SELECT k, v FROM drl_s", Some("k"));
    dmw_force_incremental("drl_v");
    Spi::run("ALTER TABLE drl_v ADD CONSTRAINT drl_small CHECK (v < 1000)").expect("check");

    Spi::run("INSERT INTO drl_s VALUES (100, 5000)").expect("staged, violates the check");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    let stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'drl_v'",
    )
    .expect("registry")
    .unwrap_or(false);
    assert!(
        stale,
        "precondition: the flush of drl_v failed and was handled"
    );
    let locked = Spi::get_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' \
           AND pid = pg_backend_pid() AND granted AND objsubid = 2 \
           AND classid::bigint = (hashtext('drl_v')::bigint & 4294967295) \
           AND objid::bigint = (hashtext(reverse('drl_v'))::bigint & 4294967295))",
    )
    .expect("pg_locks")
    .unwrap_or(false);
    assert!(
        locked,
        "the failure handler wrote the registry without the IMV lock"
    );
}

/// The key-scoped cascade into a non-partitioned DEFERRED aggregate grouped by
/// the upstream's partition column, which observes the upstream: rows staged on
/// the upstream for the rebuilt keys before the cascade are in it. (Triggers are
/// silenced during the upstream's reconcile so its rebuild reaches the
/// dependent through the cascade rather than as a row diff.)
#[pg_test]
fn pg_drc_scoped_cascade_into_deferred_dependent_with_staged_deltas() {
    drc_build_partitioned_source("drp5");
    create_imv(
        "drp5_u",
        "SELECT create_reflex_ivm('drp5_u', 'SELECT plan, id, qty FROM drp5_s', 'plan, id', \
         NULL, 'IMMEDIATE', NULL, ARRAY['plan'])",
    );
    let sql = "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM drp5_u GROUP BY plan";
    create_imv(
        "drp5_d",
        &format!(
            "SELECT create_reflex_ivm('drp5_d', {}, NULL, NULL, 'DEFERRED', NULL, \
             ARRAY[]::text[])",
            sql_lit(sql)
        ),
    );
    dmw_force_incremental("drp5_d");
    let fresh = sql.replace("drp5_u", "drp5_s");

    Spi::run("INSERT INTO drp5_s VALUES (2, 100, 1000)").expect("staged on drp5_u, plan 2");
    Spi::run("INSERT INTO drp5_s VALUES (1, 100, 500)").expect("staged on drp5_u, plan 1");
    Spi::run("SET LOCAL session_replication_role = replica").expect("replica");
    drc_reconcile_partition("drp5_u", "2");
    Spi::run("SET LOCAL session_replication_role = origin").expect("origin");
    assert!(
        drc_scoped_recorded("drp5_d"),
        "drp5_d's key-scoped rebuild not recorded"
    );
    Spi::run("INSERT INTO drp5_s VALUES (2, 101, 7)").expect("staged after");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drp5_d", &fresh);
}

/// A key-scoped cascade whose scoped rebuild fails falls back to a full
/// reconcile: the rebuilt slice must not stay recorded, or a fallback that
/// rebuilds nothing would leave the flush skipping deltas no rebuild reflects.
/// (A row trigger on the dependent's intermediate, firing under the replica
/// role the upstream's reconcile runs in, makes the scoped DELETE fail; the
/// full reconcile empties it by TRUNCATE.)
#[pg_test]
fn pg_drc_failed_scoped_cascade_leaves_no_slice_record() {
    drc_build_partitioned_source("drp8");
    create_imv(
        "drp8_u",
        "SELECT create_reflex_ivm('drp8_u', 'SELECT plan, id, qty FROM drp8_s', 'plan, id', \
         NULL, 'IMMEDIATE', NULL, ARRAY['plan'])",
    );
    let sql = "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM drp8_u GROUP BY plan";
    create_imv(
        "drp8_d",
        &format!(
            "SELECT create_reflex_ivm('drp8_d', {}, NULL, NULL, 'DEFERRED', NULL, \
             ARRAY[]::text[])",
            sql_lit(sql)
        ),
    );
    dmw_force_incremental("drp8_d");
    let fresh = sql.replace("drp8_u", "drp8_s");
    Spi::run(
        "CREATE FUNCTION drp8_refuse_delete() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN RAISE EXCEPTION 'drp8: scoped delete refused'; END $$",
    )
    .expect("refusing trigger fn");
    Spi::run(
        "CREATE TRIGGER drp8_refuse BEFORE DELETE ON __reflex_intermediate_drp8_d \
         FOR EACH ROW EXECUTE FUNCTION drp8_refuse_delete()",
    )
    .expect("refusing trigger");
    Spi::run("ALTER TABLE __reflex_intermediate_drp8_d ENABLE ALWAYS TRIGGER drp8_refuse")
        .expect("fires under replica");

    Spi::run("INSERT INTO drp8_s VALUES (2, 100, 1000)").expect("staged on drp8_u, plan 2");
    Spi::run("INSERT INTO drp8_s VALUES (1, 100, 500)").expect("staged on drp8_u, plan 1");
    Spi::run("SET LOCAL session_replication_role = replica").expect("replica");
    drc_reconcile_partition("drp8_u", "2");
    Spi::run("SET LOCAL session_replication_role = origin").expect("origin");
    assert!(
        !drc_scoped_recorded("drp8_d"),
        "drp8_d's failed key-scoped rebuild left its slice recorded"
    );
    Spi::run("INSERT INTO drp8_s VALUES (2, 101, 7)").expect("staged after");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drp8_d", &fresh);
}

/// The flush of a DEFERRED partitioned IMV's own delta rebuilds a hot partition
/// (partition-aware dispatch) while that delta is still staged: the flush
/// consumes it, so the rebuild neither needs a watermark nor a COMMIT-time full
/// rebuild, even for an IMV that joins a second table.
#[pg_test]
fn pg_drc_hot_partition_dispatch_in_own_flush_stays_scoped() {
    drc_build_partitioned_source("drp6");
    Spi::run("CREATE TABLE drp6_dim (id INT PRIMARY KEY, f INT NOT NULL)").expect("dim");
    Spi::run("INSERT INTO drp6_dim SELECT g, 1 + g % 4 FROM generate_series(1, 300) g")
        .expect("seed dim");
    let sql = "SELECT s.plan, SUM(s.qty * d.f) AS q, COUNT(*) AS n \
               FROM drp6_s s JOIN drp6_dim d ON d.id = s.id GROUP BY s.plan";
    drc_create_partitioned_deferred("drp6_v", sql, None, "");
    Spi::run("UPDATE public.__reflex_ivm_reference SET wipe_threshold = 0 WHERE name = 'drp6_v'")
        .expect("every touched partition is hot");

    Spi::run("INSERT INTO drp6_s VALUES (2, 100, 1000)").expect("staged, plan 2");
    Spi::run("UPDATE drp6_s SET qty = qty + 1 WHERE plan = 2 AND id <= 4").expect("staged");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drp6_v", sql);
    assert!(
        !drc_temp_lists("__reflex_deferred_reconciled_batch", "drp6_v"),
        "drp6_v's own flush escalated to a COMMIT-time full rebuild"
    );
}

/// A source partition swapped in a transaction that then stages a write into
/// it: the COMMIT-time partition flush rebuilds that partition of the DEFERRED
/// IMV (reading the write) before the deferred flush; the write's staged delta
/// is skipped, the other partitions' deltas applied, and no full rebuild runs.
#[pg_test]
fn pg_drc_partition_flush_rebuild_skips_its_staged_deltas() {
    drc_build_partitioned_source("drp7");
    let sql = "SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM drp7_s GROUP BY plan";
    drc_create_partitioned_deferred("drp7_v", sql, None, "");

    Spi::run("CREATE TABLE drp7_s3_new (LIKE drp7_s)").expect("new partition");
    Spi::run("INSERT INTO drp7_s3_new SELECT 3, g, 2 * g FROM generate_series(1, 25) g")
        .expect("fill new partition");
    Spi::run("ALTER TABLE drp7_s DETACH PARTITION drp7_s3").expect("detach");
    Spi::run("ALTER TABLE drp7_s ATTACH PARTITION drp7_s3_new FOR VALUES IN (3)").expect("attach");
    Spi::run("INSERT INTO drp7_s VALUES (3, 100, 1000)").expect("staged into the swapped plan");
    Spi::run("INSERT INTO drp7_s VALUES (2, 100, 500)").expect("staged, other plan");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flushes");
    dmw_assert_fresh("drp7_v", sql);
    assert!(
        !drc_temp_lists("__reflex_deferred_reconciled_batch", "drp7_v"),
        "the partition flush escalated drp7_v to a COMMIT-time full rebuild"
    );
}
