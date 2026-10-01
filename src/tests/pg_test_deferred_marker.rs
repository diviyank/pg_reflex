// A DEFERRED IMV rebuilt mid-transaction (SET CONSTRAINTS ALL IMMEDIATE fires
// the queued flush, so the COMMIT-time rebuild of a TRUNCATE or of the
// multi-source guard runs before the transaction's last write) must still
// receive every delta staged after that rebuild, and never the ones staged
// before it (the rebuild already reflects them).
//
// Fixtures are real IMVs over real tables; the oracle is the bidirectional
// EXCEPT ALL against the base query, plus known_stale false.

fn dmw_assert_fresh(imv: &str, fresh: &str) {
    assert_imv_correct(imv, fresh);
    let stale = Spi::get_one::<bool>(&format!(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = '{imv}'"
    ))
    .expect("registry")
    .unwrap_or(false);
    assert!(!stale, "{imv} flagged stale");
}

fn dmw_marked(imv: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT to_regclass('pg_temp.__reflex_deferred_reconciled_batch') IS NOT NULL \
         AND EXISTS (SELECT 1 FROM pg_temp.__reflex_deferred_reconciled_batch WHERE name = '{imv}')"
    ))
    .expect("marker")
    .unwrap_or(false)
}

/// Keeps every flush of `imv` on the incremental path: in a test transaction the
/// freshly rebuilt intermediate has no statistics, so any delta looks
/// high-selectivity and would be delegated to an idempotent full reconcile.
fn dmw_force_incremental(imv: &str) {
    Spi::run(&format!(
        "UPDATE public.__reflex_ivm_reference SET wipe_threshold = 1e9 WHERE name = '{imv}'"
    ))
    .expect("wipe_threshold");
}

fn dmw_watermark(imv: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT watermark FROM pg_temp.__reflex_deferred_reconciled_batch WHERE name = '{imv}'"
    ))
    .expect("marker")
    .unwrap_or(-1)
}

fn dmw_create_deferred(name: &str, sql: &str, key: Option<&str>) {
    assert_eq!(
        crate::create_reflex_ivm(name, sql, key, None, Some("DEFERRED"), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
}

/// The bug report's reproduction (TRUNCATE path), plus a write in a
/// subtransaction after the rebuild.
#[pg_test]
fn pg_dmw_truncate_then_dml_after_set_constraints() {
    Spi::run("CREATE TABLE dmt_s (k INT PRIMARY KEY, v INT)").expect("s");
    Spi::run("INSERT INTO dmt_s SELECT g, g FROM generate_series(1, 100) g").expect("seed");
    dmw_create_deferred("dmt_v", "SELECT k, v FROM dmt_s", Some("k"));
    Spi::run("TRUNCATE dmt_s").expect("truncate");
    Spi::run("INSERT INTO dmt_s SELECT g, g FROM generate_series(1, 50) g").expect("refill");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire the queued flush");
    assert!(
        dmw_marked("dmt_v"),
        "precondition: dmt_v rebuilt mid-transaction"
    );

    Spi::run("INSERT INTO dmt_s VALUES (1000, 1000)").expect("write after the rebuild");
    dmw_assert_fresh("dmt_v", "SELECT k, v FROM dmt_s");

    Spi::run(
        "DO $$ BEGIN \
           BEGIN \
             UPDATE dmt_s SET v = -v WHERE k <= 5; \
             DELETE FROM dmt_s WHERE k = 50; \
           EXCEPTION WHEN OTHERS THEN RAISE; \
           END; \
         END $$",
    )
    .expect("writes in a subtransaction");
    dmw_assert_fresh("dmt_v", "SELECT k, v FROM dmt_s");
}

/// Aggregate IMV kept on the incremental path: an anchor delta
/// staged before the TRUNCATE rebuild applied on top of it would count those
/// rows twice; deltas staged after it must be counted once.
#[pg_test]
fn pg_dmw_truncate_aggregate_counts_each_row_once() {
    Spi::run("CREATE TABLE dma_a (k INT PRIMARY KEY, g INT, v INT)").expect("a");
    Spi::run("CREATE TABLE dma_t (k INT PRIMARY KEY)").expect("t");
    Spi::run("INSERT INTO dma_a SELECT i, i, i FROM generate_series(1, 100) i").expect("seed a");
    Spi::run("INSERT INTO dma_t SELECT i FROM generate_series(1, 100, 2) i").expect("seed t");
    let sql = "SELECT a.g, SUM(a.v) AS sv, COUNT(t.k) AS nt, COUNT(*) AS n \
               FROM dma_a a LEFT JOIN dma_t t ON t.k = a.k GROUP BY a.g";
    dmw_create_deferred("dma_v", sql, None);
    dmw_force_incremental("dma_v");
    Spi::run("INSERT INTO dma_a VALUES (1000, 1, 1000)").expect("anchor delta before");
    Spi::run("TRUNCATE dma_t").expect("truncate the nullable side");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire the queued flushes");
    assert!(
        dmw_marked("dma_v"),
        "precondition: dma_v rebuilt mid-transaction"
    );
    dmw_assert_fresh("dma_v", sql);

    Spi::run("INSERT INTO dma_a VALUES (1001, 2, 7)").expect("insert after");
    Spi::run("INSERT INTO dma_t VALUES (3), (1001)").expect("insert nullable side after");
    Spi::run("UPDATE dma_a SET v = v + 1 WHERE k <= 3").expect("update after");
    dmw_assert_fresh("dma_v", sql);
}

/// A second TRUNCATE after the mid-transaction rebuild must rebuild again.
#[pg_test]
fn pg_dmw_truncate_again_after_set_constraints() {
    Spi::run("CREATE TABLE dm2_s (k INT PRIMARY KEY, v INT)").expect("s");
    Spi::run("INSERT INTO dm2_s SELECT g, g FROM generate_series(1, 100) g").expect("seed");
    dmw_create_deferred("dm2_v", "SELECT k, v FROM dm2_s", Some("k"));
    Spi::run("TRUNCATE dm2_s").expect("truncate");
    Spi::run("INSERT INTO dm2_s SELECT g, g FROM generate_series(1, 50) g").expect("refill");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire the queued flush");
    assert!(
        dmw_marked("dm2_v"),
        "precondition: dm2_v rebuilt mid-transaction"
    );

    Spi::run("TRUNCATE dm2_s").expect("truncate again");
    Spi::run("INSERT INTO dm2_s SELECT g, 2 * g FROM generate_series(1, 10) g").expect("refill");
    dmw_assert_fresh("dm2_v", "SELECT k, v FROM dm2_s");
}

const DMG_SQL: &str = "SELECT a.k, a.v, b.w FROM dmg_a a JOIN dmg_b b ON b.k = a.k";
const DMG_AGG_SQL: &str = "SELECT b.grp, SUM(a.v) AS sv, COUNT(*) AS n \
     FROM dmg_a a JOIN dmg_b b ON b.k = a.k GROUP BY b.grp";

fn dmg_tables() {
    Spi::run("CREATE SCHEMA dmg").expect("schema of the IMVs");
    Spi::run("CREATE TABLE dmg_a (k INT PRIMARY KEY, v INT)").expect("a");
    Spi::run("CREATE TABLE dmg_b (k INT PRIMARY KEY, w INT, grp INT)").expect("b");
    Spi::run("INSERT INTO dmg_a SELECT g, g FROM generate_series(1, 50) g").expect("seed a");
    Spi::run("INSERT INTO dmg_b SELECT g, 10 * g, g FROM generate_series(1, 50) g")
        .expect("seed b");
}

fn dmg_guard_rebuild_mid_transaction(imv: &str) {
    // b first: its flush lists the IMV and the rebuild runs before a's flush,
    // so a's staged delta (which an aggregate over a.v would double) predates it.
    Spi::run("UPDATE dmg_b SET w = w + 1, grp = grp + 100 WHERE k BETWEEN 2 AND 6")
        .expect("change b");
    Spi::run("UPDATE dmg_a SET v = v + 1 WHERE k <= 3").expect("change a");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire the queued flushes");
    assert!(
        dmw_marked(imv),
        "precondition: the guard rebuilt {imv} mid-transaction"
    );
}

/// Cross-source guard path, schema-qualified IMV: two sources changed, the
/// guard rebuilt the IMV mid-transaction, then more DML on each source.
#[pg_test]
fn pg_dmw_guard_then_dml_after_set_constraints() {
    dmg_tables();
    dmw_create_deferred("dmg.x", DMG_SQL, Some("k"));
    dmg_guard_rebuild_mid_transaction("dmg.x");
    dmw_assert_fresh("dmg.x", DMG_SQL);

    Spi::run("INSERT INTO dmg_b VALUES (1000, 5, 1)").expect("b row");
    Spi::run("INSERT INTO dmg_a VALUES (1000, 7)").expect("a row joins it");
    Spi::run("UPDATE dmg_a SET v = 0 WHERE k = 1").expect("update a");
    Spi::run("DELETE FROM dmg_b WHERE k = 4").expect("delete b");
    dmw_assert_fresh("dmg.x", DMG_SQL);
}

/// Guard path, aggregate IMV kept on the incremental path: pre-rebuild deltas
/// must not be counted again.
#[pg_test]
fn pg_dmw_guard_aggregate_counts_each_row_once() {
    dmg_tables();
    dmw_create_deferred("dmg.xa", DMG_AGG_SQL, None);
    dmw_force_incremental("dmg.xa");
    dmg_guard_rebuild_mid_transaction("dmg.xa");
    dmw_assert_fresh("dmg.xa", DMG_AGG_SQL);

    Spi::run("UPDATE dmg_a SET v = v + 100 WHERE k BETWEEN 10 AND 12").expect("update a");
    Spi::run("INSERT INTO dmg_b VALUES (1000, 5, 1)").expect("insert b");
    dmw_assert_fresh("dmg.xa", DMG_AGG_SQL);
}

/// After the rebuild the constraints are deferred again and both sources change
/// before the next flush: applying each source's later delta on its own would
/// count the new a-row joined with the new b-row twice.
#[pg_test]
fn pg_dmw_guard_two_sources_change_again_after_rebuild() {
    dmg_tables();
    dmw_create_deferred("dmg.x2", DMG_AGG_SQL, None);
    dmw_force_incremental("dmg.x2");
    dmg_guard_rebuild_mid_transaction("dmg.x2");

    Spi::run("SET CONSTRAINTS ALL DEFERRED").expect("deferred again");
    Spi::run("INSERT INTO dmg_a VALUES (1000, 7)").expect("a row");
    Spi::run("INSERT INTO dmg_b VALUES (1000, 5, 1)").expect("b row joining it");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire the queued flushes");
    dmw_assert_fresh("dmg.x2", DMG_AGG_SQL);
}

/// Deltas before and after the rebuild net to nothing on the staged source
/// (w goes 20 -> 21 before, 21 -> 20 after), so the source-wide spurious
/// short-circuit would skip the flush; the IMV still holds 21.
#[pg_test]
fn pg_dmw_guard_change_reverted_after_rebuild() {
    dmg_tables();
    dmw_create_deferred("dmg.x3", DMG_SQL, Some("k"));
    Spi::run("UPDATE dmg_a SET v = v + 1 WHERE k = 1").expect("change a");
    Spi::run("UPDATE dmg_b SET w = 21 WHERE k = 2").expect("change b");
    Spi::run("SELECT reflex_flush_deferred('dmg_a')").expect("flush a: guard rebuild");
    assert!(
        dmw_marked("dmg.x3"),
        "precondition: the guard rebuilt dmg.x3 before b's flush"
    );
    Spi::run("UPDATE dmg_b SET w = 20 WHERE k = 2").expect("revert b");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire the queued flushes");
    dmw_assert_fresh("dmg.x3", DMG_SQL);
}

/// After the rebuild, a delta staged on one source while another source's
/// pre-rebuild delta is still pending: only the first is applied, and the
/// pending one is not mistaken for a second source changed since the rebuild.
#[pg_test]
fn pg_dmw_guard_delta_after_rebuild_while_other_source_pending() {
    dmg_tables();
    dmw_create_deferred("dmg.x4", DMG_AGG_SQL, None);
    dmw_force_incremental("dmg.x4");
    Spi::run("UPDATE dmg_a SET v = v + 1 WHERE k <= 3").expect("change a");
    Spi::run("UPDATE dmg_b SET grp = grp + 100 WHERE k BETWEEN 2 AND 6").expect("change b");
    Spi::run("SELECT reflex_flush_deferred('dmg_a')").expect("flush a: guard rebuild");
    assert!(
        dmw_marked("dmg.x4"),
        "precondition: the guard rebuilt dmg.x4 before b's flush"
    );
    Spi::run("UPDATE dmg_a SET v = v + 50 WHERE k = 10").expect("change a after the rebuild");
    Spi::run("SELECT reflex_flush_deferred('dmg_a')").expect("flush a");
    Spi::run("SELECT reflex_flush_deferred('dmg_b')").expect("flush b");
    dmw_assert_fresh("dmg.x4", DMG_AGG_SQL);
}

/// The report's reproduction and its aggregate variant at a real COMMIT.
#[pg_test]
fn pg_dmw_truncate_then_dml_after_set_constraints_commit() {
    const DBNAME: &str = "reflex_dmw_commit";
    probe_db_open(DBNAME);
    worker_exec("CREATE TABLE dmc_s (k INT PRIMARY KEY, g INT, v INT)");
    worker_exec("INSERT INTO dmc_s SELECT i, i % 5, i FROM generate_series(1, 100) i");
    let agg_sql = "SELECT g, SUM(v) AS sv, COUNT(*) AS n FROM dmc_s GROUP BY g";
    rbc_create_deferred("dmc_v", "SELECT k, g, v FROM dmc_s", "k");
    let created = rbc_select(&format!(
        "create_reflex_ivm('dmc_a', {}, NULL, NULL, 'DEFERRED')",
        sql_lit(agg_sql)
    ));
    assert!(!created.starts_with("ERROR"), "create dmc_a: {created}");

    worker_exec(
        "BEGIN; \
         TRUNCATE dmc_s; \
         INSERT INTO dmc_s SELECT i, i % 5, i FROM generate_series(1, 50) i; \
         SET CONSTRAINTS ALL IMMEDIATE; \
         INSERT INTO dmc_s VALUES (1000, 1, 1000); \
         UPDATE dmc_s SET v = v + 1 WHERE k <= 10; \
         COMMIT",
    );

    let rows = worker_scalar_i64("SELECT count(*)::int8 FROM dmc_v");
    let v_mismatch = rbc_mismatch("dmc_v", "SELECT k, g, v FROM dmc_s");
    let a_mismatch = rbc_mismatch("dmc_a", agg_sql);
    let v_stale = rbc_known_stale("dmc_v");
    let a_stale = rbc_known_stale("dmc_a");
    probe_db_close(DBNAME);

    assert_eq!(
        rows, 51,
        "the write after the mid-transaction rebuild was lost"
    );
    assert_eq!(v_mismatch, 0, "dmc_v wrong after COMMIT");
    assert_eq!(a_mismatch, 0, "dmc_a wrong after COMMIT");
    assert!(!v_stale, "dmc_v flagged stale");
    assert!(!a_stale, "dmc_a flagged stale");
}

/// Runs the queued flushes in a subtransaction that then aborts: the flush's
/// `DELETE FROM` the staging table is rolled back, so the staged rows stay
/// visible with an aborted `xmax` and a combo command id in `cmin`.
fn dmw_flush_in_aborted_subtransaction() {
    Spi::run(
        "DO $$ BEGIN \
           BEGIN \
             SET CONSTRAINTS ALL IMMEDIATE; \
             RAISE EXCEPTION 'abort the flushing subtransaction'; \
           EXCEPTION WHEN OTHERS THEN NULL; \
           END; \
         END $$",
    )
    .expect("flush in an aborted subtransaction");
}

/// Review repro: a write after the rebuild whose first flush aborts must still
/// be applied by the next one (its `cmin` is then a combo id, not its command).
#[pg_test]
fn pg_dmw_aborted_flush_after_rebuild_keeps_later_write() {
    Spi::run("CREATE TABLE dmr_s (k INT PRIMARY KEY, v INT)").expect("s");
    Spi::run("INSERT INTO dmr_s SELECT g, g FROM generate_series(1, 100) g").expect("seed");
    dmw_create_deferred("dmr_v", "SELECT k, v FROM dmr_s", Some("k"));
    Spi::run("TRUNCATE dmr_s").expect("truncate");
    Spi::run("INSERT INTO dmr_s SELECT g, g FROM generate_series(1, 50) g").expect("refill");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire the queued flush");
    assert!(
        dmw_marked("dmr_v"),
        "precondition: dmr_v rebuilt mid-transaction"
    );
    Spi::run("SET CONSTRAINTS ALL DEFERRED").expect("deferred");
    Spi::run("INSERT INTO dmr_s VALUES (1000, 1000)").expect("write after the rebuild");
    dmw_flush_in_aborted_subtransaction();
    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM __reflex_delta_dmr_s WHERE xmax <> '0'::xid")
            .expect("delta")
            .unwrap_or(0),
        1,
        "precondition: the staged row carries the aborted flush's xmax"
    );
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("final flush");
    dmw_assert_fresh("dmr_v", "SELECT k, v FROM dmr_s");
}

/// Aggregate variant: the write after the rebuild is counted exactly once.
#[pg_test]
fn pg_dmw_aborted_flush_after_rebuild_aggregate() {
    Spi::run("CREATE TABLE dmq_a (k INT PRIMARY KEY, g INT, v INT)").expect("a");
    Spi::run("CREATE TABLE dmq_t (k INT PRIMARY KEY)").expect("t");
    Spi::run("INSERT INTO dmq_a SELECT i, i, i FROM generate_series(1, 100) i").expect("seed a");
    Spi::run("INSERT INTO dmq_t SELECT i FROM generate_series(1, 100, 2) i").expect("seed t");
    let sql = "SELECT a.g, SUM(a.v) AS sv, COUNT(t.k) AS nt, COUNT(*) AS n \
               FROM dmq_a a LEFT JOIN dmq_t t ON t.k = a.k GROUP BY a.g";
    dmw_create_deferred("dmq_v", sql, None);
    dmw_force_incremental("dmq_v");
    Spi::run("INSERT INTO dmq_a VALUES (1000, 1, 1000)").expect("anchor delta before");
    Spi::run("TRUNCATE dmq_t").expect("truncate the nullable side");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire the queued flushes");
    assert!(
        dmw_marked("dmq_v"),
        "precondition: dmq_v rebuilt mid-transaction"
    );
    Spi::run("SET CONSTRAINTS ALL DEFERRED").expect("deferred");
    Spi::run("INSERT INTO dmq_a VALUES (1001, 2, 7)").expect("insert after");
    Spi::run("UPDATE dmq_a SET v = v + 1 WHERE k <= 3").expect("update after");
    dmw_flush_in_aborted_subtransaction();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("final flush");
    dmw_assert_fresh("dmq_v", sql);
}

/// A delta staged before the guard rebuild whose flush aborts after it: the
/// row is ambiguous (aborted deleter newer than the rebuild), so the IMV is
/// rebuilt again rather than given the row a second time.
#[pg_test]
fn pg_dmw_pre_rebuild_row_with_flush_aborted_after_rebuild() {
    dmg_tables();
    dmw_create_deferred("dmg.x5", DMG_AGG_SQL, None);
    dmw_force_incremental("dmg.x5");
    Spi::run("UPDATE dmg_b SET grp = grp + 100 WHERE k BETWEEN 2 AND 6").expect("change b");
    Spi::run("UPDATE dmg_a SET v = v + 1 WHERE k <= 3").expect("change a");
    Spi::run("SELECT reflex_flush_deferred('dmg_b')").expect("flush b: guard rebuild");
    assert!(
        dmw_marked("dmg.x5"),
        "precondition: the guard rebuilt dmg.x5 before a's flush"
    );
    let first_rebuild = dmw_watermark("dmg.x5");
    dmw_flush_in_aborted_subtransaction();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("final flush");
    dmw_assert_fresh("dmg.x5", DMG_AGG_SQL);
    assert!(
        dmw_watermark("dmg.x5") > first_rebuild,
        "the ambiguous row did not rebuild dmg.x5 again"
    );
}

/// A delta whose flush aborted before the rebuild is reflected by it and is
/// skipped afterwards.
#[pg_test]
fn pg_dmw_flush_aborted_before_rebuild() {
    dmg_tables();
    dmw_create_deferred("dmg.x6", DMG_AGG_SQL, None);
    dmw_force_incremental("dmg.x6");
    Spi::run("UPDATE dmg_b SET grp = grp + 100 WHERE k BETWEEN 2 AND 6").expect("change b");
    Spi::run("UPDATE dmg_a SET v = v + 1 WHERE k <= 3").expect("change a");
    dmw_flush_in_aborted_subtransaction();
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("flush: guard rebuild");
    assert!(dmw_marked("dmg.x6"), "precondition: dmg.x6 rebuilt");
    dmw_assert_fresh("dmg.x6", DMG_AGG_SQL);
}

/// Several aborted flushes in a row, each after more writes: no loop, and the
/// IMV ends correct.
#[pg_test]
fn pg_dmw_repeated_aborted_flushes_after_rebuild() {
    Spi::run("CREATE TABLE dmn_s (k INT PRIMARY KEY, v INT)").expect("s");
    Spi::run("INSERT INTO dmn_s SELECT g, g FROM generate_series(1, 100) g").expect("seed");
    dmw_create_deferred("dmn_v", "SELECT k, v FROM dmn_s", Some("k"));
    Spi::run("TRUNCATE dmn_s").expect("truncate");
    Spi::run("INSERT INTO dmn_s SELECT g, g FROM generate_series(1, 50) g").expect("refill");
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("fire the queued flush");
    assert!(dmw_marked("dmn_v"), "precondition: dmn_v rebuilt");
    Spi::run("SET CONSTRAINTS ALL DEFERRED").expect("deferred");
    for i in 0..4 {
        Spi::run(&format!("INSERT INTO dmn_s VALUES ({}, {})", 1000 + i, i)).expect("write");
        Spi::run(&format!("UPDATE dmn_s SET v = v + 1 WHERE k = {}", i + 1)).expect("update");
        dmw_flush_in_aborted_subtransaction();
    }
    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("final flush");
    dmw_assert_fresh("dmn_v", "SELECT k, v FROM dmn_s");
}

/// A flush nested in a COMMIT-time rebuild pass re-lists an IMV the pass had
/// already rebuilt: Y's rebuild fires a user trigger that stages writes on both
/// of X's sources before flushing them, so the nested flush finds rows staged
/// after X's rebuild on two sources and re-lists X. The pass must rebuild X again before
/// it returns: no later flush is coming.
#[pg_test]
fn pg_dmw_relist_nested_in_rebuild_pass_is_rebuilt() {
    Spi::run("CREATE TABLE dmh_s1 (k INT PRIMARY KEY, v INT)").expect("s1");
    Spi::run("CREATE TABLE dmh_s2 (k INT PRIMARY KEY, w INT)").expect("s2");
    Spi::run("CREATE TABLE dmh_t (k INT PRIMARY KEY)").expect("t");
    Spi::run("INSERT INTO dmh_s1 SELECT g, g FROM generate_series(1, 20) g").expect("seed s1");
    Spi::run("INSERT INTO dmh_s2 SELECT g, 10 * g FROM generate_series(1, 20) g").expect("seed s2");
    Spi::run("INSERT INTO dmh_t SELECT g FROM generate_series(1, 5) g").expect("seed t");
    let x_sql = "SELECT s1.k, s1.v, s2.w FROM dmh_s1 s1 JOIN dmh_s2 s2 ON s2.k = s1.k";
    dmw_create_deferred("dmh_a_x", x_sql, Some("k"));
    dmw_create_deferred("dmh_b_y", "SELECT k FROM dmh_t", Some("k"));
    Spi::run(
        "CREATE FUNCTION dmh_feed() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN \
           IF NOT EXISTS (SELECT 1 FROM dmh_s1 WHERE k = 1000) THEN \
             SET CONSTRAINTS ALL DEFERRED; \
             INSERT INTO dmh_s1 VALUES (1000, 1); \
             INSERT INTO dmh_s2 VALUES (1000, 2); \
             SET CONSTRAINTS ALL IMMEDIATE; \
           END IF; \
           RETURN NULL; \
         END $$",
    )
    .expect("feed fn");
    Spi::run(
        "CREATE TRIGGER dmh_feed AFTER INSERT OR UPDATE OR DELETE ON dmh_b_y \
         FOR EACH STATEMENT EXECUTE FUNCTION dmh_feed()",
    )
    .expect("feed trigger");

    Spi::run("SET CONSTRAINTS ALL IMMEDIATE").expect("immediate");
    Spi::run("TRUNCATE dmh_s1").expect("truncate s1");
    Spi::run("INSERT INTO dmh_s1 SELECT g, g FROM generate_series(1, 20) g").expect("refill s1");
    assert!(dmw_marked("dmh_a_x"), "precondition: dmh_a_x rebuilt");
    Spi::run("TRUNCATE dmh_t").expect("truncate t: rebuilds dmh_b_y");
    assert_eq!(
        Spi::get_one::<i64>("SELECT count(*) FROM dmh_s1 WHERE k = 1000")
            .expect("s1")
            .unwrap_or(0),
        1,
        "precondition: the rebuild of dmh_b_y fired the feed"
    );
    dmw_assert_fresh("dmh_a_x", x_sql);
    dmw_assert_fresh("dmh_b_y", "SELECT k FROM dmh_t");
}
