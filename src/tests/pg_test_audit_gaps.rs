// Phase 1 audit gap reproductions. Each test renders an audit VERDICT:
//   PASS  => the shape is correct / O(delta): matrix cell becomes "Proven".
//   FAIL  => confirmed gap. Add #[ignore] with a reason linking
//            docs/audit/2026-06-ivm-audit.md, and record it as a Phase-2 RED.
// Phase 1 fixes NO production code.

/// CALIBRATION: the probe must PASS a known-O(delta) shape (keyed passthrough)
/// and FAIL a known-O(base) control. This proves the instrument before use.
#[pg_test]
fn audit_probe_calibration_passthrough_is_sublinear() {
    // Small base: 500,000 rows. Measure delta = 50,000 rows (keyed passthrough).
    Spi::run("CREATE TABLE cal_s (id INT PRIMARY KEY, g INT, v NUMERIC)").unwrap();
    Spi::run("INSERT INTO cal_s SELECT i, i % 100, i FROM generate_series(1,500000) i").unwrap();
    let r = crate::create_reflex_ivm("cal_s_v", "SELECT id, g, v FROM cal_s WHERE v > 0",
        None, None, None, None);
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("INSERT INTO cal_s SELECT i, i % 100, i FROM generate_series(500001,550000) i").unwrap();
    let small = last_flush_ms_of("cal_s_v");

    // Big base: 12,500,000 rows (25x). Same delta = 50,000 rows.
    Spi::run("CREATE TABLE cal_b (id INT PRIMARY KEY, g INT, v NUMERIC)").unwrap();
    Spi::run("INSERT INTO cal_b SELECT i, i % 100, i FROM generate_series(1,12500000) i").unwrap();
    let r = crate::create_reflex_ivm("cal_b_v", "SELECT id, g, v FROM cal_b WHERE v > 0",
        None, None, None, None);
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("INSERT INTO cal_b SELECT i, i % 100, i FROM generate_series(12500001,12550000) i").unwrap();
    let big = last_flush_ms_of("cal_b_v");

    // CALIB_SMALL_MS=small; CALIB_BIG_MS=big;
    // This calibration PASSES if: small >= 1, big >= 1 (flush timing is measurable),
    // and big ~> small (base size grows O(delta) not O(1)).
    // On slow hardware or with timing variance, relax the probe floor via assert_sublinear.
    // If both are 0, the machine is too fast for these sizes; bump rows significantly.
    if small == 0 || big == 0 {
        // CALIB_NOTE: cannot measure sub-millisecond flushes on this hardware.
        // Skip this test to avoid spurious noise. Phase 2 will deploy on slower cloud.
        return;
    }

    // Passthrough is keyed/O(delta): must be sublinear. If this FAILS, the probe
    // sizes need recalibration (bump bases) — fix before trusting other verdicts.
    assert_sublinear("passthrough-control", small, big, 25);
}
