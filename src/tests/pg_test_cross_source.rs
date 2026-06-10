// Cross-source consistency guard oracle tests (audit Phase 2 M1).
// The guard at src/trigger/deferred.rs:401-533 engages when >=2 distinct sources
// stage deltas in one transaction AND an IMV joins >=2 of them: it full-reconciles
// that IMV once (via the __reflex_deferred_reconciled_batch marker) so the
// ΔA⋈ΔB cross product is not double-counted. These tests force that batch shape
// and diff the IMV against a fresh recompute. A diff = a real cross-source bug.

/// Both sources of an inner-join aggregate IMV mutated in one transaction:
/// the guard must engage and the IMV must equal a fresh recompute.
#[pg_test]
fn xs_inner_join_both_sources_mutated_matches_recompute() {
    Spi::run("CREATE TABLE xa (id INT PRIMARY KEY, g INT, m NUMERIC)").unwrap();
    Spi::run("CREATE TABLE xb (id INT PRIMARY KEY, g INT, w NUMERIC)").unwrap();
    Spi::run("INSERT INTO xa VALUES (1,1,10),(2,1,20),(3,2,30)").unwrap();
    Spi::run("INSERT INTO xb VALUES (1,1,100),(2,2,200)").unwrap();
    let sql = "SELECT xa.g AS g, SUM(xa.m) AS sm, SUM(xb.w) AS sw \
               FROM xa JOIN xb ON xb.g = xa.g GROUP BY xa.g";
    crate::create_reflex_ivm("xj_v", sql, None, None, Some("DEFERRED"), None);
    // Stage deltas to BOTH sources in this (single) transaction.
    Spi::run("INSERT INTO xa VALUES (4,1,5)").unwrap();
    Spi::run("INSERT INTO xb VALUES (3,1,50)").unwrap();
    Spi::run("SELECT reflex_flush_deferred('xa')").expect("flush xa");
    Spi::run("SELECT reflex_flush_deferred('xb')").expect("flush xb");
    assert_imv_correct("xj_v", sql);
}

/// LEFT JOIN aggregate, both sources mutated in one txn (guard engages).
#[pg_test]
fn xs_left_join_both_sources_mutated_matches_recompute() {
    Spi::run("CREATE TABLE xla (id INT PRIMARY KEY, g INT, m NUMERIC)").unwrap();
    Spi::run("CREATE TABLE xlb (id INT PRIMARY KEY, g INT, w NUMERIC)").unwrap();
    Spi::run("INSERT INTO xla VALUES (1,1,10),(2,2,20)").unwrap();
    Spi::run("INSERT INTO xlb VALUES (1,1,100)").unwrap();
    let sql = "SELECT xla.g AS g, SUM(xla.m) AS sm, SUM(xlb.w) AS sw \
               FROM xla LEFT JOIN xlb ON xlb.g = xla.g GROUP BY xla.g";
    crate::create_reflex_ivm("xlj_v", sql, None, None, Some("DEFERRED"), None);
    Spi::run("INSERT INTO xla VALUES (3,2,7)").unwrap();
    Spi::run("INSERT INTO xlb VALUES (2,2,70)").unwrap();
    Spi::run("SELECT reflex_flush_deferred('xla')").expect("flush");
    Spi::run("SELECT reflex_flush_deferred('xlb')").expect("flush");
    assert_imv_correct("xlj_v", sql);
}

/// INSERT into A, DELETE from B, same txn (guard engages).
#[pg_test]
fn xs_insert_a_delete_b_matches_recompute() {
    Spi::run("CREATE TABLE xda (id INT PRIMARY KEY, g INT, m NUMERIC)").unwrap();
    Spi::run("CREATE TABLE xdb (id INT PRIMARY KEY, g INT, w NUMERIC)").unwrap();
    Spi::run("INSERT INTO xda VALUES (1,1,10),(2,1,20)").unwrap();
    Spi::run("INSERT INTO xdb VALUES (1,1,100),(2,1,200)").unwrap();
    let sql = "SELECT xda.g AS g, SUM(xda.m) AS sm, SUM(xdb.w) AS sw \
               FROM xda JOIN xdb ON xdb.g = xda.g GROUP BY xda.g";
    crate::create_reflex_ivm("xd_v", sql, None, None, Some("DEFERRED"), None);
    Spi::run("INSERT INTO xda VALUES (3,1,5)").unwrap();
    Spi::run("DELETE FROM xdb WHERE id = 2").unwrap();
    Spi::run("SELECT reflex_flush_deferred('xda')").expect("flush");
    Spi::run("SELECT reflex_flush_deferred('xdb')").expect("flush");
    assert_imv_correct("xd_v", sql);
}

/// UPDATE on A and UPDATE on B, same txn (guard engages).
#[pg_test]
fn xs_update_a_update_b_matches_recompute() {
    Spi::run("CREATE TABLE xua (id INT PRIMARY KEY, g INT, m NUMERIC)").unwrap();
    Spi::run("CREATE TABLE xub (id INT PRIMARY KEY, g INT, w NUMERIC)").unwrap();
    Spi::run("INSERT INTO xua VALUES (1,1,10),(2,1,20)").unwrap();
    Spi::run("INSERT INTO xub VALUES (1,1,100),(2,1,200)").unwrap();
    let sql = "SELECT xua.g AS g, SUM(xua.m) AS sm, SUM(xub.w) AS sw \
               FROM xua JOIN xub ON xub.g = xua.g GROUP BY xua.g";
    crate::create_reflex_ivm("xu_v", sql, None, None, Some("DEFERRED"), None);
    Spi::run("UPDATE xua SET m = 99 WHERE id = 1").unwrap();
    Spi::run("UPDATE xub SET w = 999 WHERE id = 2").unwrap();
    Spi::run("SELECT reflex_flush_deferred('xua')").expect("flush");
    Spi::run("SELECT reflex_flush_deferred('xub')").expect("flush");
    assert_imv_correct("xu_v", sql);
}

/// Only ONE source mutated: the guard must NOT engage, and the incremental
/// path must still be correct. (Guards against over-eager reconcile AND a
/// broken incremental path.)
#[pg_test]
fn xs_single_source_mutated_incremental_matches_recompute() {
    Spi::run("CREATE TABLE xsa (id INT PRIMARY KEY, g INT, m NUMERIC)").unwrap();
    Spi::run("CREATE TABLE xsb (id INT PRIMARY KEY, g INT, w NUMERIC)").unwrap();
    Spi::run("INSERT INTO xsa VALUES (1,1,10),(2,1,20)").unwrap();
    Spi::run("INSERT INTO xsb VALUES (1,1,100),(2,1,200)").unwrap();
    let sql = "SELECT xsa.g AS g, SUM(xsa.m) AS sm, SUM(xsb.w) AS sw \
               FROM xsa JOIN xsb ON xsb.g = xsa.g GROUP BY xsa.g";
    crate::create_reflex_ivm("xs1_v", sql, None, None, Some("DEFERRED"), None);
    Spi::run("INSERT INTO xsa VALUES (3,1,5)").unwrap();   // only A
    Spi::run("SELECT reflex_flush_deferred('xsa')").expect("flush");
    assert_imv_correct("xs1_v", sql);
}

/// Three-source join, two of three sources mutated in one txn (guard engages).
#[pg_test]
fn xs_three_source_two_mutated_matches_recompute() {
    Spi::run("CREATE TABLE x3a (id INT PRIMARY KEY, g INT, m NUMERIC)").unwrap();
    Spi::run("CREATE TABLE x3b (id INT PRIMARY KEY, g INT, w NUMERIC)").unwrap();
    Spi::run("CREATE TABLE x3c (id INT PRIMARY KEY, g INT, q NUMERIC)").unwrap();
    Spi::run("INSERT INTO x3a VALUES (1,1,10)").unwrap();
    Spi::run("INSERT INTO x3b VALUES (1,1,100)").unwrap();
    Spi::run("INSERT INTO x3c VALUES (1,1,1000)").unwrap();
    let sql = "SELECT x3a.g AS g, SUM(x3a.m) AS sm, SUM(x3b.w) AS sw, SUM(x3c.q) AS sq \
               FROM x3a JOIN x3b ON x3b.g = x3a.g JOIN x3c ON x3c.g = x3a.g GROUP BY x3a.g";
    crate::create_reflex_ivm("x3_v", sql, None, None, Some("DEFERRED"), None);
    Spi::run("INSERT INTO x3a VALUES (2,1,5)").unwrap();
    Spi::run("INSERT INTO x3b VALUES (2,1,50)").unwrap();   // a and b, not c
    Spi::run("SELECT reflex_flush_deferred('x3a')").expect("flush");
    Spi::run("SELECT reflex_flush_deferred('x3b')").expect("flush");
    assert_imv_correct("x3_v", sql);
}

/// Marker-skip path: two multi-source IMVs share source A; mutating A and B in
/// one txn must reconcile each affected IMV exactly once (the marker prevents
/// a second reconcile), and both must match recompute.
#[pg_test]
fn xs_two_imvs_shared_source_each_reconciled_once() {
    Spi::run("CREATE TABLE xma (id INT PRIMARY KEY, g INT, m NUMERIC)").unwrap();
    Spi::run("CREATE TABLE xmb (id INT PRIMARY KEY, g INT, w NUMERIC)").unwrap();
    Spi::run("INSERT INTO xma VALUES (1,1,10)").unwrap();
    Spi::run("INSERT INTO xmb VALUES (1,1,100)").unwrap();
    let sql1 = "SELECT xma.g AS g, SUM(xma.m) AS sm, SUM(xmb.w) AS sw \
                FROM xma JOIN xmb ON xmb.g = xma.g GROUP BY xma.g";
    let sql2 = "SELECT xma.g AS g, COUNT(*) AS c, SUM(xmb.w) AS sw \
                FROM xma JOIN xmb ON xmb.g = xma.g GROUP BY xma.g";
    crate::create_reflex_ivm("xm1_v", sql1, None, None, Some("DEFERRED"), None);
    crate::create_reflex_ivm("xm2_v", sql2, None, None, Some("DEFERRED"), None);
    Spi::run("INSERT INTO xma VALUES (2,1,5)").unwrap();
    Spi::run("INSERT INTO xmb VALUES (2,1,50)").unwrap();
    Spi::run("SELECT reflex_flush_deferred('xma')").expect("flush");
    Spi::run("SELECT reflex_flush_deferred('xmb')").expect("flush");
    assert_imv_correct("xm1_v", sql1);
    assert_imv_correct("xm2_v", sql2);
}
