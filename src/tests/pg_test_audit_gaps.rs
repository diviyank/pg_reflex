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
    let small = min_flush_ms_sampled("cal_s", "cal_s_v",
        |k| format!("INSERT INTO cal_s VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);

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
    let big = min_flush_ms_sampled("cal_b", "cal_b_v",
        |k| format!("INSERT INTO cal_b VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);

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
///
/// `dim = i` (PS-13/gap-2): group count scales 1:1 with the base, so the small
/// vs big comparison actually exercises O(base) vs O(delta), not two runs of
/// identical fixed-100-group work. Safe since PS-13 confirmed the target sync
/// is index/pruning-driven, not O(total groups); the dimension table scales
/// alongside the fact table so every generated `dim` still finds its label.
#[pg_test]
fn audit_multisource_aggregate_secondary_join_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!(
            "CREATE TABLE ma_fact_{s} (id INT PRIMARY KEY, dim INT, amt NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "CREATE TABLE ma_dim_{s} (dim INT PRIMARY KEY, label TEXT)", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ma_dim_{s} SELECT d, 'L'||d FROM generate_series(1,{n}) d", s = suf, n = n)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ma_fact_{s} SELECT i, i, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        crate::create_reflex_ivm(
            &format!("ma_v_{s}", s = suf),
            &format!(
                "SELECT f.dim, d.label, SUM(f.amt) AS s FROM ma_fact_{s} f \
                 LEFT JOIN ma_dim_{s} d ON d.dim = f.dim GROUP BY f.dim, d.label", s = suf),
            None, None, Some("DEFERRED"), None);
    }
    let small = min_flush_ms_sampled("ma_fact_s", "ma_v_s",
        |k| format!("INSERT INTO ma_fact_s VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
    let big = min_flush_ms_sampled("ma_fact_b", "ma_v_b",
        |k| format!("INSERT INTO ma_fact_b VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
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
///
/// `g = i` (PS-13/gap-2): group count scales 1:1 with the base — see the
/// multisource probe above for why a fixed group count made this a smoke test.
#[pg_test]
fn audit_single_source_aggregate_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!(
            "CREATE TABLE sa_{s} (id INT PRIMARY KEY, g INT, v NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO sa_{s} SELECT i, i, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        crate::create_reflex_ivm(
            &format!("sa_v_{s}", s = suf),
            &format!("SELECT g, SUM(v) AS s, COUNT(*) AS c FROM sa_{s} GROUP BY g", s = suf),
            None, None, Some("DEFERRED"), None);
    }
    let small = min_flush_ms_sampled("sa_s", "sa_v_s",
        |k| format!("INSERT INTO sa_s VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
    let big = min_flush_ms_sampled("sa_b", "sa_v_b",
        |k| format!("INSERT INTO sa_b VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
    eprintln!("AUDIT_M2 single-source-aggregate small={}ms big={}ms", small, big);
    assert_sublinear("single-source-aggregate", small, big, 25);
}

/// PLAN-QUALITY: inner-join aggregate, 1-row primary delta stays O(delta).
///
/// `dim = i` (PS-13/gap-2): group count scales 1:1 with the base — see the
/// multisource probe above. The dimension table scales alongside the fact
/// table so an INNER JOIN still matches every generated row.
#[pg_test]
fn audit_inner_join_aggregate_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!(
            "CREATE TABLE ija_fact_{s} (id INT PRIMARY KEY, dim INT, amt NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "CREATE TABLE ija_dim_{s} (dim INT PRIMARY KEY, label TEXT)", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ija_dim_{s} SELECT d, 'L'||d FROM generate_series(1,{n}) d", s = suf, n = n)).unwrap();
        Spi::run(&format!(
            "INSERT INTO ija_fact_{s} SELECT i, i, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        crate::create_reflex_ivm(
            &format!("ija_v_{s}", s = suf),
            &format!(
                "SELECT f.dim, d.label, SUM(f.amt) AS s FROM ija_fact_{s} f \
                 JOIN ija_dim_{s} d ON d.dim = f.dim GROUP BY f.dim, d.label", s = suf),
            None, None, Some("DEFERRED"), None);
    }
    let small = min_flush_ms_sampled("ija_fact_s", "ija_v_s",
        |k| format!("INSERT INTO ija_fact_s VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
    let big = min_flush_ms_sampled("ija_fact_b", "ija_v_b",
        |k| format!("INSERT INTO ija_fact_b VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
    eprintln!("AUDIT_M2 inner-join-aggregate small={}ms big={}ms", small, big);
    assert_sublinear("inner-join-aggregate", small, big, 25);
}

/// PLAN-QUALITY: CTE-decomposed aggregate, 1-row delta stays O(delta).
/// NOTE: CTE-decomposed views create sub-IMVs with generated names (__cte_agg).
/// We query the sub-IMV timing, not the main IMV, since that's where the work is recorded.
///
/// `dim = i` (PS-13/gap-2): group count scales 1:1 with the base — see the
/// multisource probe above for why a fixed group count made this a smoke test.
#[pg_test]
fn audit_cte_decomposed_is_sublinear() {
    for (suf, n) in [("s", 20000_i32), ("b", 500000_i32)] {
        Spi::run(&format!(
            "CREATE TABLE cd_fact_{s} (id INT PRIMARY KEY, dim INT, amt NUMERIC)", s = suf)).unwrap();
        Spi::run(&format!(
            "INSERT INTO cd_fact_{s} SELECT i, i, i FROM generate_series(1,{n}) i",
            s = suf, n = n)).unwrap();
        crate::create_reflex_ivm(
            &format!("cd_v_{s}", s = suf),
            &format!(
                "WITH agg AS (SELECT dim, SUM(amt) AS s FROM cd_fact_{s} GROUP BY dim) \
                 SELECT dim, s FROM agg", s = suf),
            None, None, Some("DEFERRED"), None);
    }
    // For CTE-decomposed views, query the sub-IMV __cte_agg that records the flush
    let small = min_flush_ms_sampled("cd_fact_s", "cd_v_s__cte_agg",
        |k| format!("INSERT INTO cd_fact_s VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
    let big = min_flush_ms_sampled("cd_fact_b", "cd_v_b__cte_agg",
        |k| format!("INSERT INTO cd_fact_b VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
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
    }
    // For UNION decomposed views, query the sub-IMV __union_0 that corresponds to the first operand
    let small = min_flush_ms_sampled("ua_p_s", "ua_v_s__union_0",
        |k| format!("INSERT INTO ua_p_s VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
    let big = min_flush_ms_sampled("ua_p_b", "ua_v_b__union_0",
        |k| format!("INSERT INTO ua_p_b VALUES ({}, 7, 5)", 900001 + k), PLAN_PROBE_SAMPLES);
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

// =============================================================================
// PS-5 — plan-quality gate for the sargable affected-groups match.
//
// This is the deterministic lock-in for PS-5 and the right instrument for it.
// The timing-based `assert_sublinear` probes cannot lock this in yet: after PS-5
// the intermediate MERGE's `ON t.k IS NOT DISTINCT FROM d.k` is the dominant
// O(total_groups) cost (measured: 48.7 ms at 200k groups / 1 affected row,
// 86,434 ms at 200k groups / 5k affected), so a flush-time ratio would be
// dominated by a defect PS-5 does not touch. Asserting on the PLAN of the
// statements PS-5 actually changes is immune to that.
// =============================================================================

/// Pull the flush statements the codegen emits for `imv`, straight from the
/// registry row, so the assertion is against real production codegen rather than
/// a hand-built plan.
fn flush_statements_for(imv: &str, source: &str, op: &str) -> Vec<String> {
    let sql = Spi::get_one::<String>(&format!(
        "SELECT reflex_build_delta_sql(name, '{src}', '{op}', base_query, end_query, \
                aggregations::text, base_query) \
         FROM public.__reflex_ivm_reference WHERE name = '{imv}'",
        src = source,
        op = op,
        imv = imv
    ))
    .expect("delta sql query failed")
    .expect("no registry row for IMV");
    sql.split("\n--<<REFLEX_SEP>>--\n")
        .map(|s| s.to_string())
        .collect()
}

/// PLAN-QUALITY (PS-5): with a NULLABLE group key and a NULL-free affected set,
/// the target DELETE and the target INSERT must both reach their relation by
/// INDEX SCAN, not by scanning the whole target/intermediate.
///
/// Before PS-5 both statements matched with `IS NOT DISTINCT FROM`, which is not
/// an operator-family member and therefore cannot serve an `Index Cond` nor a
/// hash/merge join key — the planner's only option was a nested loop with a
/// `Join Filter` over the entire relation. Measured on this shape at 200k groups:
/// target sync 52.11 ms -> 0.09 ms (579x); at 5k affected groups,
/// 95,376 ms -> 82 ms (1158x).
#[pg_test]
fn audit_ps5_nullable_group_key_target_sync_uses_index_scan() {
    // Nullable group key (no NOT NULL on `grp`) — the common case: expression
    // keys, LEFT/RIGHT-JOIN-reached columns, and every decomposed sub-IMV target.
    Spi::run("CREATE TABLE ps5t (id INT PRIMARY KEY, grp TEXT, amt NUMERIC)").unwrap();
    Spi::run("INSERT INTO ps5t SELECT i, 'g'||i, i FROM generate_series(1,20000) i").unwrap();
    crate::create_reflex_ivm(
        "ps5t_v",
        "SELECT grp, SUM(amt) AS s FROM ps5t GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    // Populate the affected-groups table with a single NON-NULL key, which is
    // what the gate keys off. ANALYZE so the planner costs it as one row.
    Spi::run("INSERT INTO ps5t VALUES (900001, 'g7', 5)").unwrap();
    Spi::run("TRUNCATE \"__reflex_affected_ps5t_v\"").unwrap();
    Spi::run("INSERT INTO \"__reflex_affected_ps5t_v\" VALUES ('g7')").unwrap();
    Spi::run("ANALYZE \"__reflex_affected_ps5t_v\"").unwrap();
    Spi::run("ANALYZE \"__reflex_intermediate_ps5t_v\"").unwrap();
    Spi::run("ANALYZE ps5t_v").unwrap();

    let stmts = flush_statements_for("ps5t_v", "ps5t", "INSERT");

    // The gate must be present at all — otherwise this test is vacuous.
    assert!(
        stmts.iter().any(|s| s.contains("AS __ng WHERE")),
        "PS-5 gate absent from generated flush; codegen under test is not gated: {:#?}",
        stmts
    );

    // Exactly one sargable and one NULL-safe variant of each target statement.
    let target_deletes: Vec<&String> = stmts
        .iter()
        .filter(|s| s.trim_start().starts_with("DELETE FROM \"ps5t_v\""))
        .collect();
    let target_inserts: Vec<&String> = stmts
        .iter()
        .filter(|s| s.trim_start().starts_with("INSERT INTO \"ps5t_v\""))
        .collect();
    assert_eq!(
        target_deletes.len(),
        2,
        "expected a gated target-DELETE pair: {:#?}",
        target_deletes
    );
    assert_eq!(
        target_inserts.len(),
        2,
        "expected a gated target-INSERT pair: {:#?}",
        target_inserts
    );

    // The SARGABLE variant of each (the one gated on NOT EXISTS(null)) is the one
    // that actually runs for a NULL-free affected set. Its plan must use an index.
    // (relation the Index Cond must be ON, label, statement)
    for (rel, label, stmt) in [
        (
            "ps5t_v",
            "target DELETE",
            *target_deletes.iter().find(|s| s.contains("AND NOT EXISTS")).expect("sargable DELETE variant"),
        ),
        (
            "__reflex_intermediate_ps5t_v",
            "target INSERT",
            *target_inserts.iter().find(|s| s.contains("AND NOT EXISTS")).expect("sargable INSERT variant"),
        ),
    ] {
        let plan = Spi::connect(|client| {
            let rows = client
                .select(&format!("EXPLAIN (COSTS OFF) {}", stmt), None, &[])
                .unwrap();
            rows.filter_map(|r| r.get_datum_by_ordinal(1).ok().and_then(|d| d.value::<String>().ok().flatten()))
                .collect::<Vec<String>>()
                .join("\n")
        });
        // Pin BOTH the access method and the relation it applies to: a bare
        // `contains("Index Scan")` would be satisfied by an index scan on the
        // tiny affected table while the big relation still got seq-scanned.
        let scanned_by_index = plan.lines().any(|l| {
            (l.contains("Index Scan") || l.contains("Index Only Scan") || l.contains("Bitmap Index Scan"))
                && l.contains(rel)
        });
        assert!(
            scanned_by_index,
            "PS-5 PLAN-QUALITY GAP [{}]: `{}` must be reached by index scan, got:\n{}\nstatement: {}",
            label,
            rel,
            plan,
            stmt
        );
        // And the probe must be a real index condition, not a post-scan filter.
        assert!(
            plan.contains("Index Cond:"),
            "PS-5 [{}]: the group-key probe must be an Index Cond, not a Filter:\n{}",
            label,
            plan
        );
    }
}

/// PLAN-QUALITY + SCOPE-TIGHTNESS (PS-11): the top-K MIN/MAX scalar-refresh
/// (`build_topk_scalar_refresh_sql`) scopes the refresh to the affected groups.
/// Its scoping used a NULL-unsafe `(cols) IN (SELECT ...)` that silently DROPPED
/// the NULL group — a reproduced silent-wrong-result after an unshrunk NULL-group
/// UPDATE. The fix makes it NULL-safe via a gated pair, but the common (NULL-free
/// affected set) path must STILL reach the intermediate by INDEX SCAN — a plain
/// always-NULL-safe `IS NOT DISTINCT FROM` seq-scans the whole intermediate on
/// every top-K Sub. This pins both: a NULL-safe variant exists AND the sargable
/// variant is index-driven. Without the guard, scoping could silently collapse to
/// a full-intermediate scan (correct results, so contents-only tests miss it).
#[pg_test]
fn audit_ps11_topk_scalar_refresh_null_safe_and_index_scoped() {
    Spi::run("CREATE TABLE ps11t (id INT PRIMARY KEY, grp TEXT, v INT NOT NULL)").unwrap();
    Spi::run("INSERT INTO ps11t SELECT i, 'g'||i, i FROM generate_series(1,20000) i").unwrap();
    // Nullable group key (no NOT NULL on `grp`); top-K is on by default.
    crate::create_reflex_ivm(
        "ps11t_v",
        "SELECT grp, MIN(v) AS lo, MAX(v) AS hi FROM ps11t GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    // Affected set holds a single NON-NULL key — what the sargable branch keys off.
    Spi::run("TRUNCATE \"__reflex_affected_ps11t_v\"").unwrap();
    Spi::run("INSERT INTO \"__reflex_affected_ps11t_v\" VALUES ('g7')").unwrap();
    Spi::run("ANALYZE \"__reflex_affected_ps11t_v\"").unwrap();
    Spi::run("ANALYZE \"__reflex_intermediate_ps11t_v\"").unwrap();

    let stmts = flush_statements_for("ps11t_v", "ps11t", "UPDATE");

    // The scalar-refresh statements: they refresh the scalar from the top-K array
    // (`... = "..._topk"[1]`) gated by the heap `cardinality` predicate.
    let refreshes: Vec<&String> = stmts
        .iter()
        .filter(|s| {
            s.contains("__reflex_intermediate_ps11t_v")
                && s.contains("[1]")
                && s.contains("cardinality")
        })
        .collect();
    assert!(
        !refreshes.is_empty(),
        "no top-K scalar-refresh statement found in UPDATE flush: {:#?}",
        stmts
    );
    // NULL-safe: there must be a variant using `IS NOT DISTINCT FROM` (else a NULL
    // group is silently dropped — the reproduced bug).
    assert!(
        refreshes.iter().any(|s| s.contains("IS NOT DISTINCT FROM")),
        "top-K scalar-refresh has no NULL-safe variant (NULL group would be dropped): {:#?}",
        refreshes
    );

    // The sargable variant (runs for a NULL-free affected set) is the one gated on
    // `AND NOT EXISTS(<null-key>)` with a `=` scope. Its plan must hit the
    // intermediate's unique group-key index, not seq-scan the whole intermediate.
    let sargable = refreshes
        .iter()
        .find(|s| s.contains("AND NOT EXISTS") && !s.contains("IS NOT DISTINCT FROM"))
        .expect("sargable top-K refresh variant (`=` scope, gated on NOT EXISTS)");
    let plan = Spi::connect(|client| {
        let rows = client
            .select(&format!("EXPLAIN (COSTS OFF) {}", sargable), None, &[])
            .unwrap();
        rows.filter_map(|r| {
            r.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<String>().ok().flatten())
        })
        .collect::<Vec<String>>()
        .join("\n")
    });
    let scanned_by_index = plan.lines().any(|l| {
        (l.contains("Index Scan") || l.contains("Index Only Scan") || l.contains("Bitmap Index Scan"))
            && l.contains("__reflex_intermediate_ps11t_v")
    });
    assert!(
        scanned_by_index,
        "PS-11 SCOPE-TIGHTNESS: top-K scalar-refresh must reach the intermediate by index scan, \
         not a full scan, got:\n{}\nstatement: {}",
        plan, sargable
    );
    assert!(
        plan.contains("Index Cond:"),
        "PS-11: the top-K refresh group-key probe must be an Index Cond, not a Filter:\n{}",
        plan
    );
}

/// PLAN-QUALITY (PS-5, 2026-07-25): the keyed outer-join-secondary passthrough
/// must not nested-loop the FULL base relation against the transition delta.
///
/// `build_null_safe_membership_predicate` chose between the sargable `IN` form
/// and `EXISTS (... IS NOT DISTINCT FROM ...)` STATICALLY, from
/// `plan.not_null_columns`. On this shape the target columns come off the
/// NULLABLE side of the outer join, so `provably_not_null_key_columns` can never
/// prove them — it excludes LEFT-join target tables outright and returns an EMPTY
/// set for any RIGHT/FULL join — which made the expensive form the ROUTINE case
/// for every IMV of this shape, not a rare-NULL-data edge case. `IS NOT DISTINCT
/// FROM` is in no operator family, so it can serve neither a hash nor a merge
/// join key and the planner's only option is a nested loop over the whole base.
/// Measured on PG 18.4 at 500k base rows x 2k changed keys: 23,698 ms with
/// `Rows Removed by Join Filter: 489,995,000`, versus 40.9 ms for `IN` (579x).
///
/// The fix emits a runtime-gated PAIR. This pins that (a) the pair exists, (b)
/// the sargable variant that runs for a NULL-free delta is planned as a
/// hash/merge join rather than a nested loop, and (c) the NULL-safe variant is
/// still there — so the test cannot pass by simply dropping NULL safety.
#[pg_test]
fn audit_ps5_keyed_outer_join_secondary_avoids_nested_loop_over_base() {
    // `k` is deliberately NULLABLE on both sides: that is the shape
    // `provably_not_null_key_columns` cannot prove, i.e. the common case.
    Spi::run("CREATE TABLE ps5lj_prim (k INT, payload TEXT)").unwrap();
    Spi::run("CREATE UNIQUE INDEX ON ps5lj_prim (k)").unwrap();
    Spi::run("CREATE TABLE ps5lj_sec (k INT, extra TEXT)").unwrap();
    Spi::run("CREATE UNIQUE INDEX ON ps5lj_sec (k)").unwrap();
    Spi::run("INSERT INTO ps5lj_prim SELECT i, 'p'||i FROM generate_series(1,20000) i").unwrap();
    Spi::run("INSERT INTO ps5lj_sec SELECT i, 's'||i FROM generate_series(1,20000) i").unwrap();

    let base = "SELECT p.k, p.payload, s.extra FROM ps5lj_prim p LEFT JOIN ps5lj_sec s ON s.k = p.k";
    let res = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('ps5lj_v', '{}', 'k')",
        base.replace('\'', "''")
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    // The statements reference the trigger's transition tables, which do not
    // exist outside trigger execution. Stand-ins with the same names and column
    // types let the planner produce a real plan for them.
    for side in ["old", "new"] {
        Spi::run(&format!(
            "CREATE TABLE \"__reflex_{}_ps5lj_sec\" (k INT, extra TEXT)",
            side
        ))
        .unwrap();
    }
    // A NULL-free delta of 200 changed keys — the case the fast variant serves.
    Spi::run("INSERT INTO \"__reflex_new_ps5lj_sec\" SELECT i, 's'||i FROM generate_series(1,200) i")
        .unwrap();
    Spi::run("ANALYZE ps5lj_prim").unwrap();
    Spi::run("ANALYZE ps5lj_sec").unwrap();
    Spi::run("ANALYZE \"__reflex_old_ps5lj_sec\"").unwrap();
    Spi::run("ANALYZE \"__reflex_new_ps5lj_sec\"").unwrap();

    let stmts = flush_statements_for("ps5lj_v", "ps5lj_sec", "UPDATE");

    // Guard against a vacuous test: this fixture must actually be on the keyed
    // secondary path, not the full-rebuild fallback.
    assert!(
        stmts.iter().any(|s| s.contains("__ck")),
        "fixture is not on the keyed outer-join-secondary path: {:#?}",
        stmts
    );
    let target_inserts: Vec<&String> = stmts
        .iter()
        .filter(|s| s.trim_start().starts_with("INSERT INTO \"ps5lj_v\""))
        .collect();
    assert_eq!(
        target_inserts.len(),
        2,
        "expected a gated target-INSERT pair: {:#?}",
        target_inserts
    );
    assert!(
        target_inserts
            .iter()
            .any(|s| s.contains("IS NOT DISTINCT FROM")),
        "the NULL-safe variant must survive — dropping it would be silent data loss: {:#?}",
        target_inserts
    );

    let explain = |stmt: &str| -> String {
        Spi::connect(|client| {
            let rows = client
                .select(&format!("EXPLAIN (COSTS OFF) {}", stmt), None, &[])
                .unwrap();
            rows.filter_map(|r| {
                r.get_datum_by_ordinal(1)
                    .ok()
                    .and_then(|d| d.value::<String>().ok().flatten())
            })
            .collect::<Vec<String>>()
            .join("\n")
        })
    };

    let sargable = target_inserts
        .iter()
        .find(|s| s.contains("AND NOT EXISTS") && !s.contains("IS NOT DISTINCT FROM"))
        .expect("sargable INSERT variant (`IN` scope, gated on NOT EXISTS)");
    // Assert on the MEMBERSHIP join specifically. A bare `Nested Loop` check
    // would be wrong here: the base query has its own LEFT JOIN to the secondary,
    // which the planner legitimately serves with a nested loop + index scan. The
    // cliff has a distinct signature — a `Nested Loop Semi Join` whose
    // `Join Filter` is the `IS DISTINCT FROM` negation.
    let membership_join_cond = |p: &str| {
        p.lines().any(|l: &str| {
            (l.contains("Hash Cond:") || l.contains("Merge Cond:"))
                && l.contains("__reflex_old_ps5lj_sec")
        })
    };
    let plan = explain(sargable);
    assert!(
        membership_join_cond(&plan),
        "PS-5 PLAN-QUALITY GAP: the sargable variant must match the base against \
         the changed keys by a hash/merge join CONDITION, got:\n{}\nstatement: {}",
        plan,
        sargable
    );
    assert!(
        !plan.contains("Nested Loop Semi Join") && !plan.contains("IS DISTINCT FROM"),
        "PS-5 PLAN-QUALITY GAP: the sargable variant must carry no non-sargable \
         semi join over the base relation — that is the O(base x delta) cliff, \
         got:\n{}\nstatement: {}",
        plan,
        sargable
    );

    // The contrast that makes the assertions above meaningful: the NULL-safe
    // variant, which the pre-fix codegen emitted UNCONDITIONALLY, genuinely does
    // get the nested-loop semi join. If this ever stops being true the fast/slow
    // distinction has evaporated and the assertions above prove nothing.
    let null_safe = target_inserts
        .iter()
        .find(|s| s.contains("IS NOT DISTINCT FROM"))
        .expect("NULL-safe INSERT variant");
    let slow_plan = explain(null_safe);
    assert!(
        slow_plan.contains("Nested Loop Semi Join") && slow_plan.contains("IS DISTINCT FROM"),
        "PS-5 test is vacuous: the NULL-safe form was expected to nested-loop the \
         base relation (that is the cliff being avoided), got:\n{}",
        slow_plan
    );
    assert!(
        !membership_join_cond(&slow_plan),
        "PS-5 test is vacuous: the NULL-safe form must NOT get a hash/merge \
         membership condition, or the two variants plan identically:\n{}",
        slow_plan
    );
}
