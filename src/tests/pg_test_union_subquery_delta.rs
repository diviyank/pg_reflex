// Regression suite for operand-scoped delta on aggregate IMVs whose FROM clause
// contains a set-op subquery (audit B2). A mutation to a table inside ONE
// UNION ALL operand must contribute ONLY that operand's delta — the sibling
// operands are unchanged and must not be re-scanned/re-aggregated. Before the
// fix, the delta rewriter swapped only the changed operand's table and left
// siblings full, then MERGE-added the result, double-counting every sibling row
// (silent wrong answers when a sibling contributes non-zero to a SUM) and
// scanning the full base (O(base)). See docs/audit/2026-06-ivm-audit.md §3.
//
// Oracle = assert_imv_correct (EXCEPT ALL vs a fresh recompute).

/// The minimal reproduction of the silent correctness bug: a sibling UNION ALL
/// operand (stb) contributes a NON-ZERO value to the SUM, and a primary INSERT
/// lands in a group that already has a stb row. A double-count corrupts the SUM.
#[pg_test]
fn union_subquery_insert_nonzero_sibling_no_double_count() {
    Spi::run("CREATE TABLE usd_pb (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("CREATE TABLE usd_stb (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("INSERT INTO usd_pb VALUES (1,10,3)").unwrap();
    Spi::run("INSERT INTO usd_stb VALUES (1,10,2)").unwrap(); // non-zero sibling in group 10
    let sql = "SELECT st.product_id, SUM(st.v) AS total \
               FROM ( \
                 SELECT product_id, qty AS v FROM usd_pb \
                 UNION ALL \
                 SELECT product_id, qty AS v FROM usd_stb \
               ) st GROUP BY st.product_id";
    crate::create_reflex_ivm("usd_iv", sql, None, None, Some("IMMEDIATE"), None);
    assert_imv_correct("usd_iv", sql);
    // INSERT into the same group as the existing stb row.
    Spi::run("INSERT INTO usd_pb VALUES (2,10,7)").unwrap();
    assert_imv_correct("usd_iv", sql); // correct total = 3+7+2 = 12, not 14
    // A second insert into a fresh group must not disturb group 10 either.
    Spi::run("INSERT INTO usd_pb VALUES (3,20,5)").unwrap();
    assert_imv_correct("usd_iv", sql);
}

/// The production-masked shape: sibling projects a constant 0, so the historical
/// double-count was invisible in the SUM. Still must be correct after the fix.
#[pg_test]
fn union_subquery_insert_zero_sibling_stays_correct() {
    Spi::run("CREATE TABLE usz_pb (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("CREATE TABLE usz_stb (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("INSERT INTO usz_pb VALUES (1,10,3)").unwrap();
    Spi::run("INSERT INTO usz_stb VALUES (1,10,99)").unwrap();
    let sql = "SELECT st.product_id, SUM(st.v) AS total \
               FROM ( \
                 SELECT product_id, qty AS v FROM usz_pb \
                 UNION ALL \
                 SELECT product_id, 0 AS v FROM usz_stb \
               ) st GROUP BY st.product_id";
    crate::create_reflex_ivm("usz_iv", sql, None, None, Some("IMMEDIATE"), None);
    Spi::run("INSERT INTO usz_pb VALUES (2,10,7)").unwrap();
    assert_imv_correct("usz_iv", sql);
}

/// DELETE of a primary-operand row in a group that also has a non-zero sibling.
#[pg_test]
fn union_subquery_delete_nonzero_sibling_correct() {
    Spi::run("CREATE TABLE usdel_pb (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("CREATE TABLE usdel_stb (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("INSERT INTO usdel_pb VALUES (1,10,3),(2,10,7)").unwrap();
    Spi::run("INSERT INTO usdel_stb VALUES (1,10,2)").unwrap();
    let sql = "SELECT st.product_id, SUM(st.v) AS total \
               FROM ( \
                 SELECT product_id, qty AS v FROM usdel_pb \
                 UNION ALL \
                 SELECT product_id, qty AS v FROM usdel_stb \
               ) st GROUP BY st.product_id";
    crate::create_reflex_ivm("usdel_iv", sql, None, None, Some("IMMEDIATE"), None);
    Spi::run("DELETE FROM usdel_pb WHERE id = 1").unwrap();
    assert_imv_correct("usdel_iv", sql); // correct total = 7+2 = 9
}

/// UPDATE of a primary-operand row in a group that also has a non-zero sibling.
#[pg_test]
fn union_subquery_update_nonzero_sibling_correct() {
    Spi::run("CREATE TABLE usup_pb (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("CREATE TABLE usup_stb (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("INSERT INTO usup_pb VALUES (1,10,3)").unwrap();
    Spi::run("INSERT INTO usup_stb VALUES (1,10,2)").unwrap();
    let sql = "SELECT st.product_id, SUM(st.v) AS total \
               FROM ( \
                 SELECT product_id, qty AS v FROM usup_pb \
                 UNION ALL \
                 SELECT product_id, qty AS v FROM usup_stb \
               ) st GROUP BY st.product_id";
    crate::create_reflex_ivm("usup_iv", sql, None, None, Some("IMMEDIATE"), None);
    Spi::run("UPDATE usup_pb SET qty = 11 WHERE id = 1").unwrap();
    assert_imv_correct("usup_iv", sql); // correct total = 11+2 = 13
}

/// Passthrough (no aggregation) over a UNION ALL subquery: an INSERT into one
/// operand must append only that operand's new rows, not re-insert the full
/// sibling operand. Same root cause as the aggregate double-count.
#[pg_test]
fn union_subquery_passthrough_insert_no_duplicate() {
    Spi::run("CREATE TABLE usp_a (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("CREATE TABLE usp_b (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("INSERT INTO usp_a VALUES (1,10,3)").unwrap();
    Spi::run("INSERT INTO usp_b VALUES (1,10,2)").unwrap();
    let sql = "SELECT g, v FROM ( \
                 SELECT g, v FROM usp_a UNION ALL SELECT g, v FROM usp_b \
               ) st";
    crate::create_reflex_ivm("usp_iv", sql, None, None, Some("IMMEDIATE"), None);
    assert_imv_correct("usp_iv", sql);
    Spi::run("INSERT INTO usp_a VALUES (2,10,7)").unwrap();
    assert_imv_correct("usp_iv", sql);
}

/// Passthrough over a UNION ALL subquery with a declared key: UPDATE + DELETE in
/// one operand must not disturb the sibling operand's rows.
#[pg_test]
fn union_subquery_passthrough_update_delete_keyed() {
    Spi::run("CREATE TABLE uspk_a (id INT PRIMARY KEY, v INT)").unwrap();
    Spi::run("CREATE TABLE uspk_b (id INT PRIMARY KEY, v INT)").unwrap();
    Spi::run("INSERT INTO uspk_a VALUES (1,3)").unwrap();
    Spi::run("INSERT INTO uspk_b VALUES (2,2)").unwrap();
    let sql = "SELECT id, v FROM ( \
                 SELECT id, v FROM uspk_a UNION ALL SELECT id, v FROM uspk_b \
               ) st";
    crate::create_reflex_ivm("uspk_iv", sql, Some("id"), None, Some("IMMEDIATE"), None);
    assert_imv_correct("uspk_iv", sql);
    Spi::run("UPDATE uspk_a SET v = 9 WHERE id = 1").unwrap();
    assert_imv_correct("uspk_iv", sql);
    Spi::run("DELETE FROM uspk_a WHERE id = 1").unwrap();
    assert_imv_correct("uspk_iv", sql);
    Spi::run("INSERT INTO uspk_a VALUES (3,5)").unwrap();
    assert_imv_correct("uspk_iv", sql);
}

/// Passthrough over a UNION ALL subquery with NO declared key and cross-operand
/// value collisions: maintenance must fall back to full refresh (no keyed
/// DELETE), so a DELETE in one operand cannot over-delete the sibling's
/// colliding row. (When a key IS declared, the enforced unique index instead
/// rejects the collision outright — so the keyed DELETE path never runs on
/// colliding data.)
#[pg_test]
fn union_subquery_passthrough_nokey_collision_correct() {
    Spi::run("CREATE TABLE usnc_a (id INT PRIMARY KEY, v INT)").unwrap();
    Spi::run("CREATE TABLE usnc_b (id INT PRIMARY KEY, v INT)").unwrap();
    Spi::run("INSERT INTO usnc_a VALUES (1,3)").unwrap();
    Spi::run("INSERT INTO usnc_b VALUES (1,99)").unwrap(); // collides on id=1
    let sql = "SELECT id, v FROM ( \
                 SELECT id, v FROM usnc_a UNION ALL SELECT id, v FROM usnc_b \
               ) st";
    crate::create_reflex_ivm("usnc_iv", sql, None, None, Some("IMMEDIATE"), None);
    assert_imv_correct("usnc_iv", sql);
    // Delete the usnc_a row; the colliding usnc_b row (id=1, v=99) must survive.
    Spi::run("DELETE FROM usnc_a WHERE id = 1").unwrap();
    assert_imv_correct("usnc_iv", sql);
}

/// A non-distributive set op (UNION, i.e. DISTINCT) referencing the mutated
/// source cannot be delta-maintained by operand scoping; the codegen must fall
/// back to a correct full recompute.
#[pg_test]
fn union_subquery_distinct_setop_recompute_correct() {
    Spi::run("CREATE TABLE usu_a (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("CREATE TABLE usu_b (id INT PRIMARY KEY, product_id INT, qty NUMERIC)").unwrap();
    Spi::run("INSERT INTO usu_a VALUES (1,10,5)").unwrap();
    Spi::run("INSERT INTO usu_b VALUES (1,10,5),(2,20,8)").unwrap();
    let sql = "SELECT st.product_id, SUM(st.v) AS total \
               FROM ( \
                 SELECT product_id, qty AS v FROM usu_a \
                 UNION \
                 SELECT product_id, qty AS v FROM usu_b \
               ) st GROUP BY st.product_id";
    crate::create_reflex_ivm("usu_iv", sql, None, None, Some("IMMEDIATE"), None);
    assert_imv_correct("usu_iv", sql);
    // Insert a row into usu_a that duplicates an existing (product_id, qty) from
    // usu_b — UNION distinct must collapse it; recompute keeps it correct.
    Spi::run("INSERT INTO usu_a VALUES (3,20,8)").unwrap();
    assert_imv_correct("usu_iv", sql);
    Spi::run("INSERT INTO usu_a VALUES (4,30,1)").unwrap();
    assert_imv_correct("usu_iv", sql);
}
