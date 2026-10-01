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
/// IMMEDIATE aggregate `{p}_u` over `{p}_us` whose every write is delegated to a
/// full reconcile (wipe_threshold 0), and a DEFERRED aggregate `{p}_d` reading
/// `{p}_s` and `{p}_u` that IGNORES `{p}_u`, kept on the incremental flush path.
fn drc_build(p: &str) -> String {
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
            None,
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
    let fresh = drc_build("drc1");
    Spi::run("INSERT INTO drc1_s VALUES (100, 0, 1000)").expect("staged for the dependent");
    Spi::run("UPDATE drc1_us SET w = w + 1").expect("upstream dispatched to a reconcile");
    assert_imv_correct("drc1_u", &DRC_UPSTREAM_SQL.replace("{p}", "drc1"));
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("commit-time flush");
    dmw_assert_fresh("drc1_d", &fresh);

    Spi::run("INSERT INTO drc1_s VALUES (101, 1, 7)").expect("write after the flush");
    Spi::run("UPDATE drc1_s SET v = v + 1 WHERE k <= 4").expect("update after the flush");
    dmw_assert_fresh("drc1_d", &fresh);
}

/// Same, through an explicit `reflex_reconcile` of the upstream.
#[pg_test]
fn pg_drc_explicit_upstream_reconcile_refreshes_ignoring_dependent_once() {
    let fresh = drc_build("drc2");
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
    let created = rbc_select(&format!("create_reflex_ivm('drc6_u', {})", sql_lit(up_sql)));
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
        "create_reflex_ivm('drc7_u', {})",
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
