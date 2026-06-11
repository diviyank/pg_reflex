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

/// PLAN-QUALITY: a single-source GROUP BY aggregate must maintain a 1-row delta
/// by recomputing only the affected group, not re-aggregating the whole base.
#[pg_test]
fn audit_single_source_aggregate_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!(
            "CREATE TABLE sa_{s} (id INT PRIMARY KEY, g INT, v NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO sa_{s} SELECT i, i % 1000, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        crate::create_reflex_ivm(
            &format!("sa_v_{s}", s = suf),
            &format!("SELECT g, SUM(v) AS s, COUNT(*) AS c FROM sa_{s} GROUP BY g", s = suf),
            None, None, Some("DEFERRED"), None);
        Spi::run(&format!("INSERT INTO sa_{s} VALUES (900001, 7, 5)", s = suf)).unwrap();
        Spi::run(&format!("SELECT reflex_flush_deferred('sa_{s}')", s = suf)).unwrap();
    }
    let small = last_flush_ms_of("sa_v_s");
    let big = last_flush_ms_of("sa_v_b");
    eprintln!("AUDIT_M2 single-source-aggregate small={}ms big={}ms", small, big);
    assert_sublinear("single-source-aggregate", small, big, 25);
}

/// PLAN-QUALITY: inner-join aggregate, 1-row primary delta stays O(delta).
#[pg_test]
fn audit_inner_join_aggregate_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!(
            "CREATE TABLE ija_fact_{s} (id INT PRIMARY KEY, dim INT, amt NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "CREATE TABLE ija_dim_{s} (dim INT PRIMARY KEY, label TEXT)", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ija_dim_{s} SELECT d, 'L'||d FROM generate_series(1,100) d", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ija_fact_{s} SELECT i, i % 100 + 1, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        crate::create_reflex_ivm(
            &format!("ija_v_{s}", s = suf),
            &format!(
                "SELECT f.dim, d.label, SUM(f.amt) AS s FROM ija_fact_{s} f \
                 JOIN ija_dim_{s} d ON d.dim = f.dim GROUP BY f.dim, d.label", s = suf),
            None, None, Some("DEFERRED"), None);
        Spi::run(&format!("INSERT INTO ija_fact_{s} VALUES (900001, 7, 5)", s = suf)).unwrap();
        Spi::run(&format!("SELECT reflex_flush_deferred('ija_fact_{s}')", s = suf)).unwrap();
    }
    let small = last_flush_ms_of("ija_v_s");
    let big = last_flush_ms_of("ija_v_b");
    eprintln!("AUDIT_M2 inner-join-aggregate small={}ms big={}ms", small, big);
    assert_sublinear("inner-join-aggregate", small, big, 25);
}

/// PLAN-QUALITY: CTE-decomposed aggregate, 1-row delta stays O(delta).
/// NOTE: CTE-decomposed views create sub-IMVs with generated names (__cte_agg).
/// We query the sub-IMV timing, not the main IMV, since that's where the work is recorded.
#[pg_test]
fn audit_cte_decomposed_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!(
            "CREATE TABLE cd_fact_{s} (id INT PRIMARY KEY, dim INT, amt NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO cd_fact_{s} SELECT i, i % 100 + 1, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        crate::create_reflex_ivm(
            &format!("cd_v_{s}", s = suf),
            &format!(
                "WITH agg AS (SELECT dim, SUM(amt) AS s FROM cd_fact_{s} GROUP BY dim) \
                 SELECT dim, s FROM agg", s = suf),
            None, None, Some("DEFERRED"), None);
        Spi::run(&format!("INSERT INTO cd_fact_{s} VALUES (900001, 7, 5)", s = suf)).unwrap();
        Spi::run(&format!("SELECT reflex_flush_deferred('cd_fact_{s}')", s = suf)).unwrap();
    }
    // For CTE-decomposed views, query the sub-IMV __cte_agg that records the flush
    let small = last_flush_ms_of("cd_v_s__cte_agg");
    let big = last_flush_ms_of("cd_v_b__cte_agg");
    eprintln!("AUDIT_M2 cte-decomposed small={}ms big={}ms", small, big);
    assert_sublinear("cte-decomposed", small, big, 25);
}

/// PLAN-QUALITY: UNION ALL set-op, 1-row delta into one operand stays O(delta).
/// NOTE: UNION decomposed views create sub-IMVs with generated names (__union_0, __union_1).
/// We query the first operand's sub-IMV since that's where we insert.
#[pg_test]
fn audit_union_all_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!(
            "CREATE TABLE ua_p_{s} (id INT PRIMARY KEY, g INT, v NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "CREATE TABLE ua_q_{s} (id INT PRIMARY KEY, g INT, v NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ua_p_{s} SELECT i, i % 100, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ua_q_{s} SELECT i, i % 100, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        crate::create_reflex_ivm(
            &format!("ua_v_{s}", s = suf),
            &format!(
                "SELECT id, g, v FROM ua_p_{s} UNION ALL SELECT id, g, v FROM ua_q_{s}", s = suf),
            None, None, Some("DEFERRED"), None);
        Spi::run(&format!("INSERT INTO ua_p_{s} VALUES (900001, 7, 5)", s = suf)).unwrap();
        Spi::run(&format!("SELECT reflex_flush_deferred('ua_p_{s}')", s = suf)).unwrap();
    }
    // For UNION decomposed views, query the sub-IMV __union_0 that corresponds to the first operand
    let small = last_flush_ms_of("ua_v_s__union_0");
    let big = last_flush_ms_of("ua_v_b__union_0");
    eprintln!("AUDIT_M2 union-all small={}ms big={}ms", small, big);
    assert_sublinear("union-all", small, big, 25);
}

/// CONFIRMED BUG (gate finding, Phase 2B M6): a DISTINCT ON IMV that declares its
/// natural output unique key (`d`) crashes at CREATE. pg_reflex classifies
/// DISTINCT ON as passthrough and applies the declared key to the pre-dedup
/// `__base` table — where `d` repeats — so `CREATE UNIQUE INDEX
/// __reflex_uk_<imv>__base` fails with "Key (d)=(g0) is duplicated". Aggregates
/// with the same GROUP BY key do NOT hit this (they are not passthrough, so they
/// skip resolve_unique_columns). Desired: create succeeds and the IMV equals a
/// fresh recompute. Root cause: `try_decompose_distinct_on` (decompose.rs) passed
/// the outer view's declared `unique_columns` to the pre-dedup `__base` sub-IMV;
/// `__base`'s natural key is the source PK, not the DISTINCT-ON output key. Fixed:
/// `__base` auto-infers its source key (empty unique_columns).
#[pg_test]
fn audit_distinct_on_declared_output_key_should_not_crash_create() {
    Spi::run("CREATE TABLE dok (id INT PRIMARY KEY, m NUMERIC, d TEXT)").unwrap();
    Spi::run("INSERT INTO dok SELECT i, i, 'g'||(i % 4) FROM generate_series(0,7) i").unwrap();
    let sql = "SELECT DISTINCT ON (d) d, id, m FROM dok ORDER BY d, id";
    let r = crate::create_reflex_ivm("dok_v", sql, Some("d"), None, None, None);
    assert_eq!(
        r, "CREATE REFLEX INCREMENTAL VIEW",
        "DISTINCT ON IMV with declared output key d should be created, got: {}",
        r
    );
    Spi::run("INSERT INTO dok VALUES (100, 5, 'g100')").unwrap();
    assert_imv_correct("dok_v", sql);
}
