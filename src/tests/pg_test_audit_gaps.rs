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
