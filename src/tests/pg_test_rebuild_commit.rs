// Real-COMMIT tests: the deferred flush, and with it the multi-source guard's
// rebuild, runs at COMMIT only, so a remote `dblink` session commits the
// scenario in a private database (the probe pattern of
// `pg_test_partition_attach_locks.rs`, whose helpers these reuse).

const RBC_CAAV_SQL: &str = "SELECT product_id, location_id, is_active FROM rbc_rel \
     WHERE assortment_id = (SELECT assortment_id FROM rbc_cur)";
const RBC_SFV_SQL: &str = "SELECT a.id, a.product_id, a.location_id, a.qty, \
     COALESCE(c.is_active, FALSE) AS active FROM rbc_anchor a \
     LEFT JOIN rbc_caav c ON c.product_id = a.product_id AND c.location_id = a.location_id";
const RBC_SFV_FRESH: &str = "SELECT a.id, a.product_id, a.location_id, a.qty, \
     COALESCE(c.is_active, FALSE) FROM rbc_anchor a \
     LEFT JOIN rbc_rel c ON c.product_id = a.product_id AND c.location_id = a.location_id";

/// Runs a single-row SELECT on the worker and returns its value as text.
fn rbc_select(sql: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT v FROM dblink('reflex_lock_worker', {}) AS t(v text)",
        sql_lit(&format!("SELECT ({sql})::text"))
    ))
    .unwrap_or_else(|e| panic!("rbc_select failed for <{sql}>: {e}"))
    .unwrap_or_default()
}

fn rbc_create_deferred(name: &str, sql: &str, key: &str) {
    let created = rbc_select(&format!(
        "create_reflex_ivm({}, {}, {}, NULL, 'DEFERRED')",
        sql_lit(name),
        sql_lit(sql),
        sql_lit(key)
    ));
    assert!(!created.starts_with("ERROR"), "create {name}: {created}");
}

/// Prod chain: a caav-shaped DEFERRED passthrough over two sources and a
/// sop_forecast-shaped DEFERRED dependent LEFT JOINing it.
fn rbc_prod_chain_open(dbname: &str) {
    probe_db_open(dbname);
    worker_exec("CREATE TABLE rbc_cur (assortment_id INT)");
    worker_exec("INSERT INTO rbc_cur VALUES (1)");
    worker_exec(
        "CREATE TABLE rbc_rel (assortment_id INT, product_id INT NOT NULL, \
         location_id INT NOT NULL, is_active BOOL)",
    );
    worker_exec(
        "INSERT INTO rbc_rel SELECT 1, p, l, (p + l) % 2 = 0 \
         FROM generate_series(0,6) p, generate_series(0,4) l",
    );
    worker_exec(
        "CREATE TABLE rbc_anchor (id INT PRIMARY KEY, product_id INT NOT NULL, \
         location_id INT NOT NULL, qty INT)",
    );
    worker_exec("INSERT INTO rbc_anchor SELECT g, g % 7, g % 5, g FROM generate_series(1, 700) g");
    rbc_create_deferred("rbc_caav", RBC_CAAV_SQL, "product_id, location_id");
    rbc_create_deferred("rbc_sfv", RBC_SFV_SQL, "product_id, location_id, id");
    assert_eq!(
        worker_scalar_i64(
            "SELECT count(*)::int8 FROM public.__reflex_ivm_reference \
             WHERE name = 'rbc_caav' AND depends_on @> ARRAY['rbc_rel', 'rbc_cur']"
        ),
        1,
        "fixture: rbc_caav observes both rbc_rel and rbc_cur"
    );
}

fn rbc_last_rebuilt_epoch(imv: &str) -> f64 {
    Spi::get_one::<f64>(&format!(
        "SELECT v FROM dblink('reflex_lock_worker', {}) AS t(v float8)",
        sql_lit(&format!(
            "SELECT COALESCE(EXTRACT(EPOCH FROM last_update_date), 0)::float8 \
             FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
        ))
    ))
    .expect("last_update_date")
    .expect("last_update_date NULL")
}

fn rbc_mismatch(imv: &str, fresh: &str) -> i64 {
    worker_scalar_i64(&format!(
        "SELECT count(*)::int8 FROM ((SELECT * FROM {imv} EXCEPT ALL ({fresh})) \
         UNION ALL (({fresh}) EXCEPT ALL SELECT * FROM {imv})) x"
    ))
}

fn rbc_known_stale(imv: &str) -> bool {
    worker_scalar_i64(&format!(
        "SELECT COALESCE(known_stale, FALSE)::int::int8 FROM public.__reflex_ivm_reference \
         WHERE name = '{imv}'"
    )) == 1
}

/// Two of caav's sources change in one transaction, so the multi-source guard
/// rebuilds it at COMMIT. The dependent must end correct and receive only the
/// changed keys' rows.
#[pg_test]
fn pg_rbc_multi_source_guard_rebuild_reaches_dependent_as_diff() {
    const DBNAME: &str = "reflex_rbc_diff";
    rbc_prod_chain_open(DBNAME);
    let rebuilt_before = rbc_last_rebuilt_epoch("rbc_caav");

    worker_exec(
        "BEGIN; \
         UPDATE rbc_rel SET is_active = NOT is_active WHERE product_id = 0 AND location_id = 0; \
         UPDATE rbc_cur SET assortment_id = 1; \
         COMMIT",
    );

    assert!(
        rbc_last_rebuilt_epoch("rbc_caav") > rebuilt_before,
        "precondition: the multi-source guard rebuilt rbc_caav at COMMIT"
    );
    assert_eq!(
        rbc_mismatch("rbc_caav", RBC_CAAV_SQL),
        0,
        "rbc_caav incorrect"
    );
    assert_eq!(
        rbc_mismatch("rbc_sfv", RBC_SFV_FRESH),
        0,
        "dependent incorrect after the guard rebuild at COMMIT"
    );
    rbc_select("pg_stat_force_next_flush()");
    let touched = worker_scalar_i64(
        "SELECT (n_tup_ins + n_tup_upd + n_tup_del)::int8 FROM pg_stat_user_tables \
         WHERE relname = 'rbc_sfv'",
    );
    // 700 rows written by the create, plus the 20 rows of the changed key
    // (g % 35 = 0) maintained as a delete + an insert each: a full rewrite would
    // add 1400.
    let initial = 700;
    assert!(
        touched <= initial + 2 * 20,
        "dependent rewritten in full at COMMIT: {touched} tuple writes"
    );
    assert!(!rbc_known_stale("rbc_caav"), "rbc_caav left known_stale");
    assert!(!rbc_known_stale("rbc_sfv"), "rbc_sfv left known_stale");
    probe_db_close(DBNAME);
}

/// The guard's rebuild fails (a NOT VALID check the diff's UPDATE of a product-6
/// row violates): the COMMIT must still succeed and caav be flagged stale.
#[pg_test]
fn pg_rbc_guard_rebuild_failure_marks_stale_without_aborting_commit() {
    const DBNAME: &str = "reflex_rbc_fail";
    rbc_prod_chain_open(DBNAME);
    worker_exec("ALTER TABLE rbc_caav ADD CONSTRAINT rbc_never CHECK (product_id < 6) NOT VALID");
    let rebuilt_before = rbc_last_rebuilt_epoch("rbc_caav");

    // One implicit transaction (a multi-statement query string), committed when
    // the string ends. Not BEGIN ... COMMIT: rolling back a subtransaction while
    // an explicit block is in TBLOCK_END trips an assertion-only check in
    // `RollbackAndReleaseCurrentSubTransaction` on cassert builds — the same one
    // any PL/pgSQL EXCEPTION block hits there — which release builds compile out.
    let committed = Spi::get_one::<String>(&format!(
        "SELECT dblink_exec('reflex_lock_worker', {}, false)",
        sql_lit(
            "UPDATE rbc_rel SET is_active = NOT is_active WHERE product_id = 6 AND location_id = 0; \
             UPDATE rbc_cur SET assortment_id = 1"
        )
    ))
    .expect("dblink_exec")
    .unwrap_or_default();
    let error = Spi::get_one::<String>("SELECT dblink_error_message('reflex_lock_worker')")
        .expect("dblink_error_message")
        .unwrap_or_default();
    worker_exec("ROLLBACK");

    assert_eq!(
        worker_scalar_i64(
            "SELECT count(*)::int8 FROM rbc_rel \
             WHERE product_id = 6 AND location_id = 0 AND is_active = ((6 + 0) % 2 <> 0)"
        ),
        1,
        "the COMMIT was aborted ({committed}): {error}"
    );
    assert_eq!(
        rbc_last_rebuilt_epoch("rbc_caav"),
        rebuilt_before,
        "the failed rebuild left no trace"
    );
    assert!(
        rbc_known_stale("rbc_caav"),
        "rbc_caav not flagged known_stale"
    );
    probe_db_close(DBNAME);
}

/// The guard engages on X (two of its table sources change) while X's upstream
/// DEFERRED IMV U is changed later in the same transaction: X must be rebuilt
/// only once U has settled, so X ends correct and not stale.
#[pg_test]
fn pg_rbc_guard_rebuild_waits_for_upstream_deferred_imv() {
    const DBNAME: &str = "reflex_rbc_upstream";
    probe_db_open(DBNAME);
    worker_exec("CREATE TABLE rbu_t1 (k INT PRIMARY KEY, a INT)");
    worker_exec("CREATE TABLE rbu_t2 (k INT PRIMARY KEY, b INT)");
    worker_exec("CREATE TABLE rbu_s (k INT PRIMARY KEY, c INT)");
    worker_exec("INSERT INTO rbu_t1 SELECT g, g FROM generate_series(1, 50) g");
    worker_exec("INSERT INTO rbu_t2 SELECT g, 10 * g FROM generate_series(1, 50) g");
    worker_exec("INSERT INTO rbu_s SELECT g, 100 * g FROM generate_series(1, 50) g");
    rbc_create_deferred("rbu_u", "SELECT k, c FROM rbu_s", "k");
    let x_sql = "SELECT t1.k, t1.a, t2.b, u.c FROM rbu_t1 t1 \
                 JOIN rbu_t2 t2 ON t2.k = t1.k LEFT JOIN rbu_u u ON u.k = t1.k";
    let x_fresh = "SELECT t1.k, t1.a, t2.b, s.c FROM rbu_t1 t1 \
                   JOIN rbu_t2 t2 ON t2.k = t1.k LEFT JOIN rbu_s s ON s.k = t1.k";
    rbc_create_deferred("rbu_x", x_sql, "k");
    let rebuilt_before = rbc_last_rebuilt_epoch("rbu_x");
    let flush_count = "SELECT COALESCE(flush_count, 0)::int8 \
                       FROM public.__reflex_ivm_reference WHERE name = 'rbu_x'";
    let flushes_before = worker_scalar_i64(flush_count);

    worker_exec(
        "BEGIN; \
         UPDATE rbu_t1 SET a = a + 1 WHERE k = 1; \
         UPDATE rbu_t2 SET b = b + 1 WHERE k = 2; \
         UPDATE rbu_s SET c = c + 1 WHERE k = 3; \
         COMMIT",
    );

    assert!(
        rbc_last_rebuilt_epoch("rbu_x") > rebuilt_before,
        "precondition: the multi-source guard rebuilt rbu_x at COMMIT"
    );
    assert_eq!(
        rbc_mismatch("rbu_u", "SELECT k, c FROM rbu_s"),
        0,
        "rbu_u incorrect"
    );
    assert_eq!(
        rbc_mismatch("rbu_x", x_fresh),
        0,
        "rbu_x rebuilt before its upstream DEFERRED IMV settled"
    );
    assert!(!rbc_known_stale("rbu_x"), "rbu_x left known_stale");
    assert_eq!(
        worker_scalar_i64(flush_count),
        flushes_before,
        "a delta staged for rbu_x was applied while its guard rebuild was pending"
    );
    probe_db_close(DBNAME);
}

/// A flush nested in a COMMIT-time rebuild pass lists an IMV for the guard
/// rebuild; the nested pass returns at once, so the outer pass must pick it up.
/// Constraints IMMEDIATE: a TRUNCATE of `rbn_a` (nullable side of `rbn_x`) runs
/// a pass that rebuilds `rbn_x`; that write flushes `rbn_x` into `rbn_d`, whose
/// user trigger writes `rbn_b` once, so `rbn_b`'s flush, nested in the pass,
/// sees both of `rbn_d`'s sources pending and lists `rbn_d`.
#[pg_test]
fn pg_rbc_guard_listing_from_a_nested_flush_is_rebuilt() {
    Spi::run("CREATE TABLE rbn_c (k INT PRIMARY KEY)").expect("c");
    Spi::run("CREATE TABLE rbn_a (k INT PRIMARY KEY, v INT)").expect("a");
    Spi::run("CREATE TABLE rbn_b (id SERIAL PRIMARY KEY, k INT, w INT)").expect("b");
    Spi::run("INSERT INTO rbn_c SELECT g FROM generate_series(1, 5) g").expect("seed c");
    Spi::run("INSERT INTO rbn_a SELECT g, g FROM generate_series(1, 5) g").expect("seed a");
    Spi::run("INSERT INTO rbn_b (k, w) SELECT g, g FROM generate_series(1, 5) g").expect("seed b");
    let x_sql = "SELECT c.k, a.v FROM rbn_c c LEFT JOIN rbn_a a ON a.k = c.k";
    let d_sql = "SELECT x.k, x.v, b.id, b.w FROM rbn_x x JOIN rbn_b b ON b.k = x.k";
    let d_fresh = "SELECT x.k, x.v, b.id, b.w FROM (SELECT c.k, a.v FROM rbn_c c \
                   LEFT JOIN rbn_a a ON a.k = c.k) x JOIN rbn_b b ON b.k = x.k";
    crate::create_reflex_ivm("rbn_x", x_sql, Some("k"), None, Some("DEFERRED"), None);
    crate::create_reflex_ivm("rbn_d", d_sql, Some("k, id"), None, Some("DEFERRED"), None);
    Spi::run(
        "CREATE FUNCTION rbn_feed_b() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN \
           IF NOT EXISTS (SELECT 1 FROM rbn_b WHERE w = 100) THEN \
             INSERT INTO rbn_b (k, w) VALUES (1, 100); \
           END IF; \
           RETURN NULL; \
         END $$",
    )
    .expect("feed fn");
    Spi::run(
        "CREATE TRIGGER rbn_feed_b AFTER INSERT OR UPDATE OR DELETE ON rbn_d \
         FOR EACH STATEMENT EXECUTE FUNCTION rbn_feed_b()",
    )
    .expect("feed trigger");

    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("immediate");
    Spi::run("TRUNCATE rbn_a").expect("truncate");

    assert_eq!(
        Spi::get_one::<bool>(
            "SELECT reconcile FROM pg_temp.__reflex_deferred_rebuild WHERE name = 'rbn_d'"
        )
        .expect("rebuild list"),
        Some(true),
        "precondition: the guard listed rbn_d from the nested flush"
    );

    assert_imv_correct("rbn_x", x_sql);
    assert_imv_correct("rbn_d", d_fresh);
}

/// A source write committed while a rebuild of its IMV is in progress is never
/// lost: the rebuild's table lock serialises the writer behind it.
#[pg_test]
fn pg_rbc_concurrent_write_during_rebuild_not_lost() {
    const DBNAME: &str = "reflex_rbc_concurrent";
    const WRITE: &str = "INSERT INTO rcw_rel VALUES (999999, 1)";
    probe_db_open(DBNAME);
    Spi::get_one::<String>(&format!(
        "SELECT dblink_connect('rbc_writer', {})",
        sql_lit(&conninfo_for(DBNAME))
    ))
    .expect("writer connect")
    .expect("writer connect NULL");
    worker_exec("CREATE TABLE rcw_rel (k INT PRIMARY KEY, v INT)");
    worker_exec("INSERT INTO rcw_rel SELECT g, g FROM generate_series(1, 20000) g");
    rbc_select("create_reflex_ivm('rcw_up', 'SELECT k, v FROM rcw_rel', 'k')");
    rbc_select("create_reflex_ivm('rcw_dep', 'SELECT k, v FROM rcw_up', 'k')");

    worker_exec("BEGIN");
    rbc_select("reflex_reconcile('rcw_up')");
    Spi::run(&format!(
        "SELECT dblink_send_query('rbc_writer', {})",
        sql_lit(WRITE)
    ))
    .expect("async write");
    Spi::run(
        "CREATE FUNCTION pg_temp.rbc_await_lock_wait(q text) RETURNS bool LANGUAGE plpgsql AS $fn$ \
         BEGIN \
           FOR i IN 1..100 LOOP \
             PERFORM pg_stat_clear_snapshot(); \
             IF EXISTS (SELECT 1 FROM pg_stat_activity \
                        WHERE query = q AND wait_event_type = 'Lock') THEN RETURN TRUE; END IF; \
             PERFORM pg_sleep(0.05); \
           END LOOP; \
           RETURN FALSE; \
         END $fn$",
    )
    .expect("poll fn");
    let waiting = Spi::get_one::<bool>(&format!(
        "SELECT pg_temp.rbc_await_lock_wait({})",
        sql_lit(WRITE)
    ))
    .expect("poll")
    .unwrap_or(false);
    worker_exec("COMMIT");
    Spi::run("SELECT * FROM dblink_get_result('rbc_writer') AS t(r text)").expect("write result");

    assert_eq!(
        worker_scalar_i64("SELECT count(*)::int8 FROM rcw_up WHERE k = 999999"),
        1,
        "write committed during the rebuild was lost from the upstream IMV"
    );
    assert_eq!(
        worker_scalar_i64("SELECT count(*)::int8 FROM rcw_dep WHERE k = 999999"),
        1,
        "write committed during the rebuild was lost from the dependent IMV"
    );
    assert_eq!(rbc_mismatch("rcw_up", "SELECT k, v FROM rcw_rel"), 0);
    assert_eq!(rbc_mismatch("rcw_dep", "SELECT k, v FROM rcw_rel"), 0);
    assert!(
        waiting,
        "the writer was never observed blocked behind the rebuild"
    );
    let _ = Spi::get_one::<String>("SELECT dblink_disconnect('rbc_writer')");
    probe_db_close(DBNAME);
}
