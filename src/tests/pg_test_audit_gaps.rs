// Phase 1 audit gap reproductions. Each test renders an audit VERDICT:
//   PASS  => the shape is correct / O(delta): matrix cell becomes "Proven".
//   FAIL  => confirmed gap. Add #[ignore] with a reason linking
//            docs/audit/2026-06-ivm-audit.md, and record it as a Phase-2 RED.
// Phase 1 fixes NO production code.
//
// PLAN-QUALITY PROBE protocol: `last_flush_ms` is only recorded on the DEFERRED
// flush path, so plan-scaling tests create the IMV with mode DEFERRED, apply an
// identical single-row delta against a small and a 25x-larger base, drain each
// with `reflex_flush_deferred('<source>')`, then compare the two flush times via
// `assert_sublinear`. Correctness tests (immediate mode) just diff against a
// fresh recompute with `assert_imv_correct`.

/// CALIBRATION: prove the plan-quality instrument before trusting its verdicts.
/// Two parts: (1) a REAL measurement of a known-O(delta) shape (keyed
/// passthrough) must be judged sublinear; (2) synthetic checks prove the
/// discriminator actually fires on an O(base) growth pattern and stays quiet on
/// a flat one. Together these rule out a vacuous "always passes" probe.
#[pg_test]
fn audit_probe_calibration_passthrough_is_sublinear() {
    // (1) Real O(delta) shape: keyed passthrough, identical single-row delta
    //     against a small (20k) and a 25x-larger (500k) base. DEFERRED so the
    //     flush time is recorded.
    Spi::run("CREATE TABLE cal_s (id INT PRIMARY KEY, g INT, v NUMERIC)").unwrap();
    Spi::run("INSERT INTO cal_s SELECT i, i % 100, i FROM generate_series(1,20000) i").unwrap();
    crate::create_reflex_ivm(
        "cal_s_v",
        "SELECT id, g, v FROM cal_s WHERE v > 0",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    Spi::run("INSERT INTO cal_s VALUES (900001, 7, 5)").unwrap();
    Spi::run("SELECT reflex_flush_deferred('cal_s')").expect("flush small");
    let small = last_flush_ms_of("cal_s_v");

    Spi::run("CREATE TABLE cal_b (id INT PRIMARY KEY, g INT, v NUMERIC)").unwrap();
    Spi::run("INSERT INTO cal_b SELECT i, i % 100, i FROM generate_series(1,500000) i").unwrap();
    crate::create_reflex_ivm(
        "cal_b_v",
        "SELECT id, g, v FROM cal_b WHERE v > 0",
        None,
        None,
        Some("DEFERRED"),
        None,
    );
    Spi::run("INSERT INTO cal_b VALUES (900001, 7, 5)").unwrap();
    Spi::run("SELECT reflex_flush_deferred('cal_b')").expect("flush big");
    let big = last_flush_ms_of("cal_b_v");

    // Keyed passthrough is O(delta): a 1-row insert touches one IMV row whether
    // the base holds 20k or 500k rows.
    assert_sublinear("passthrough-control", small, big, 25);

    // (2) The discriminator must FIRE on an O(base) growth pattern...
    assert!(
        flush_scales_with_base(2, 60, 25),
        "discriminator failed to flag 2ms->60ms at 25x (clear O(base) growth)"
    );
    // ...stay quiet on sublinear growth...
    assert!(
        !flush_scales_with_base(2, 9, 25),
        "discriminator wrongly flagged 2ms->9ms (sublinear)"
    );
    // ...and NOT mistake a heavy-but-FLAT shape (constant factor, no scaling)
    // for an O(base) one.
    assert!(
        !flush_scales_with_base(70, 95, 25),
        "discriminator wrongly flagged a heavy-but-flat shape (70ms->95ms)"
    );
}

/// PLAN-QUALITY: an aggregate IMV joining a secondary dimension table must
/// maintain a 1-row primary delta in O(delta), not re-aggregate the whole base.
#[pg_test]
fn audit_multisource_aggregate_secondary_join_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!(
            "CREATE TABLE ma_fact_{s} (id INT PRIMARY KEY, dim INT, amt NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "CREATE TABLE ma_dim_{s} (dim INT PRIMARY KEY, label TEXT)", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ma_dim_{s} SELECT d, 'L'||d FROM generate_series(1,100) d", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ma_fact_{s} SELECT i, i % 100 + 1, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        crate::create_reflex_ivm(
            &format!("ma_v_{s}", s = suf),
            &format!(
                "SELECT f.dim, d.label, SUM(f.amt) AS s FROM ma_fact_{s} f \
                 LEFT JOIN ma_dim_{s} d ON d.dim = f.dim GROUP BY f.dim, d.label", s = suf),
            None, None, Some("DEFERRED"), None);
        Spi::run(&format!(
            "INSERT INTO ma_fact_{s} VALUES (900001, 7, 5)", s = suf)).unwrap();
        Spi::run(&format!("SELECT reflex_flush_deferred('ma_fact_{s}')", s = suf)).unwrap();
    }
    let small = last_flush_ms_of("ma_v_s");
    let big = last_flush_ms_of("ma_v_b");
    eprintln!("AUDIT_T4 multisource-aggregate small={}ms big={}ms", small, big);
    assert_sublinear("multisource-aggregate-secondary-join", small, big, 25);
}

/// CORRECTNESS: updating one row's score must re-rank the whole partition in a
/// ROW_NUMBER window IMV. Oracle = fresh recompute via EXCEPT ALL.
#[pg_test]
fn audit_window_row_number_update_reranks_correctly() {
    Spi::run("CREATE TABLE awr (id INT PRIMARY KEY, grp INT, score INT)").unwrap();
    Spi::run("INSERT INTO awr VALUES (1,1,90),(2,1,80),(3,1,70),(4,2,50)").unwrap();
    let sql = "SELECT id, grp, score, \
               ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) AS rnk FROM awr";
    crate::create_reflex_ivm("awr_v", sql, None, None, None, None);
    Spi::run("UPDATE awr SET score = 999 WHERE id = 3").unwrap();
    assert_imv_correct("awr_v", sql);
}

/// CORRECTNESS: demoting the current DISTINCT ON winner must promote the
/// runner-up in the same group. Oracle = fresh recompute via EXCEPT ALL.
#[pg_test]
fn audit_distinct_on_winner_demotion_promotes_runner_up() {
    Spi::run("CREATE TABLE ado (id INT PRIMARY KEY, city TEXT, name TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ado VALUES \
        (1,'Paris','A',100),(2,'Paris','B',90),(3,'Lyon','C',50)").unwrap();
    let sql = "SELECT DISTINCT ON (city) city, name, val FROM ado ORDER BY city, val DESC";
    crate::create_reflex_ivm("ado_v", sql, None, None, None, None);
    Spi::run("UPDATE ado SET val = 1 WHERE id = 1").unwrap();
    assert_imv_correct("ado_v", sql);
}

/// CORRECTNESS: a passthrough IMV filtered by `IN (SELECT ...)` must skip an
/// update to a row OUTSIDE the filter, even when it collides on the unique key
/// with an in-filter row. Oracle = fresh recompute via EXCEPT ALL.
#[pg_test]
fn audit_in_subquery_filter_skips_out_of_filter_update() {
    Spi::run("CREATE TABLE ais (k INT, period INT, v NUMERIC)").unwrap();
    Spi::run("CREATE TABLE ais_active (period INT)").unwrap();
    Spi::run("INSERT INTO ais_active VALUES (1)").unwrap();
    // (k=1,p=1) is IN-filter; (k=1,p=2) is OUT-of-filter but shares the IMV
    // unique key k=1 — the exact collision shape of the 1.10.2 silent-delete bug.
    Spi::run("INSERT INTO ais VALUES (1,1,10),(1,2,999)").unwrap();
    let sql = "SELECT k, period, v FROM ais WHERE period IN (SELECT period FROM ais_active)";
    crate::create_reflex_ivm("ais_v", sql, Some("k"), None, None, None);
    // Update the OUT-of-filter row. A buggy keyed maintenance would DELETE the
    // in-filter row sharing k=1; the IMV must instead stay equal to the recompute.
    Spi::run("UPDATE ais SET v = 123 WHERE period = 2").unwrap();
    assert_imv_correct("ais_v", sql);
}
