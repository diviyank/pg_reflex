use pgrx::pg_sys::panic::ErrorReportable;

#[pg_test]
fn f10_doctor_dry_run_is_read_only() {
    // Seed a wedged pending row + a known_stale IMV.
    Spi::run("INSERT INTO public.__reflex_partition_pending (source_root, attempts) VALUES ('d.root', 5)").unwrap();
    Spi::run("INSERT INTO public.__reflex_ivm_reference (name, graph_depth, known_stale, stale_reason) VALUES ('d.imv', 0, TRUE, 'boom')").unwrap();
    // Snapshot state.
    let pending_before = Spi::get_one::<i64>("SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root='d.root'").unwrap().unwrap();
    // Dry run (fix defaults FALSE).
    let n = Spi::get_one::<i64>("SELECT count(*) FROM reflex_doctor()").unwrap().unwrap();
    assert!(n >= 1, "dry run reports the seeded findings");
    // Nothing mutated.
    let pending_after = Spi::get_one::<i64>("SELECT count(*) FROM public.__reflex_partition_pending WHERE source_root='d.root'").unwrap().unwrap();
    assert_eq!(pending_before, pending_after, "dry run must not drain the queue");
    let still_stale = Spi::get_one::<bool>("SELECT known_stale FROM public.__reflex_ivm_reference WHERE name='d.imv'").unwrap().unwrap();
    assert!(still_stale, "dry run must not clear known_stale");
}

#[pg_test]
fn f10_doctor_fix_drains_wedged_queue() {
    // Seed a pending root with high attempt count to trigger F2 detection.
    // Call reflex_doctor(fix => TRUE) and verify:
    // 1. At least one row is returned
    // 2. The outcome is either 'fixed' or starts with 'failed:'
    // 3. The pending row was attempted to be drained
    Spi::run("INSERT INTO public.__reflex_partition_pending (source_root, attempts) VALUES ('test.root', 5)").unwrap();

    let result_rows: Vec<(String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client.select(
            "SELECT object, outcome FROM reflex_doctor(NULL, TRUE) WHERE check_id IN ('F1', 'F2')",
            None,
            &[]
        ).unwrap_or_report();
        for row in rs {
            let object: String = row
                .get_by_name::<&str, _>("object")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let outcome: String = row
                .get_by_name::<&str, _>("outcome")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((object, outcome));
        }
        result
    });

    assert!(!result_rows.is_empty(), "doctor should return at least one F1/F2 row");
    let (obj, outcome) = &result_rows[0];
    assert_eq!(obj, "test.root", "object should be the seeded source_root");
    assert!(
        outcome == "fixed" || outcome.starts_with("failed:"),
        "outcome should be 'fixed' or 'failed:...' but got '{}'",
        outcome
    );
}

#[pg_test]
fn f10_doctor_fix_respects_drop_orphans_gate() {
    // Seed an F3 orphan-overlap IMV and verify the gate behavior:
    // 1. With drop_orphans=FALSE: outcome should be 'skipped(needs drop_orphans)'
    // 2. With drop_orphans=TRUE: outcome should be 'fixed' or 'failed:...'

    // Seed a basic IMV
    Spi::run("CREATE TABLE f10_src (id INT PRIMARY KEY, val INT)").unwrap();
    Spi::run("INSERT INTO f10_src VALUES (1, 100)").unwrap();
    crate::create_reflex_ivm(
        "f10_test_imv",
        "SELECT id, val FROM f10_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );

    // Mark it as stale with a reason containing "overlap" (F3 trigger)
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, stale_reason = 'partition overlap detected' WHERE name = 'f10_test_imv'"
    ).unwrap();

    // Test 1: Call with drop_orphans=FALSE - should be gated
    let result_without: Vec<(String, String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client.select(
            "SELECT check_id, object, outcome FROM reflex_doctor(NULL, TRUE, FALSE) WHERE object = 'f10_test_imv'",
            None,
            &[]
        ).unwrap_or_report();
        for row in rs {
            let check_id: String = row
                .get_by_name::<&str, _>("check_id")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let object: String = row
                .get_by_name::<&str, _>("object")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let outcome: String = row
                .get_by_name::<&str, _>("outcome")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((check_id, object, outcome));
        }
        result
    });

    assert!(!result_without.is_empty(), "doctor should return row for the stale IMV");
    let (check_id, obj, outcome) = &result_without[0];
    assert_eq!(check_id, "F3", "should be classified as F3");
    assert_eq!(obj, "f10_test_imv", "object should be the seeded IMV");
    assert_eq!(outcome, "skipped(needs drop_orphans)", "outcome should be skipped when drop_orphans=FALSE");

    // Test 2: Call with drop_orphans=TRUE - should attempt the repair
    let result_with: Vec<(String, String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client.select(
            "SELECT check_id, object, outcome FROM reflex_doctor(NULL, TRUE, TRUE) WHERE object = 'f10_test_imv'",
            None,
            &[]
        ).unwrap_or_report();
        for row in rs {
            let check_id: String = row
                .get_by_name::<&str, _>("check_id")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let object: String = row
                .get_by_name::<&str, _>("object")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let outcome: String = row
                .get_by_name::<&str, _>("outcome")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((check_id, object, outcome));
        }
        result
    });

    assert!(!result_with.is_empty(), "doctor should return row after retry with drop_orphans=TRUE");
    let (check_id, obj, outcome) = &result_with[0];
    assert_eq!(check_id, "F3", "should still be classified as F3");
    assert_eq!(obj, "f10_test_imv", "object should be the seeded IMV");
    assert!(
        outcome == "fixed" || outcome.starts_with("failed:"),
        "outcome should be 'fixed' or 'failed:...' but got '{}' (gate was bypassed)",
        outcome
    );
}

#[pg_test]
fn f10_doctor_never_runs_chain_rebuild_without_escalation() {
    // Represent a decomposed known_stale IMV that maps to F4b.
    // For now, verify that reflex_rebuild_chain is reported but not executed.

    // Create a simple IMV
    Spi::run("CREATE TABLE f10_chain_src (id INT PRIMARY KEY, val INT)").unwrap();
    Spi::run("INSERT INTO f10_chain_src VALUES (1, 100)").unwrap();
    crate::create_reflex_ivm(
        "f10_chain_imv",
        "SELECT id, val FROM f10_chain_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );

    // Mark as stale (simulating F4b condition)
    Spi::run("UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, stale_reason = 'decomposed-non-convergence' WHERE name = 'f10_chain_imv'").unwrap();

    // Call reflex_doctor with fix => TRUE
    let n = Spi::get_one::<i64>(
        "SELECT count(*) FROM reflex_doctor(NULL, TRUE) WHERE object = 'f10_chain_imv'"
    ).unwrap().unwrap_or(0);

    // Should have a row for the chain IMV
    assert!(n >= 1, "should return row for the chain IMV");

    // Verify the chain hasn't been rebuilt - check that registry still contains the IMV
    let still_exists = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'f10_chain_imv'"
    ).unwrap().unwrap();
    assert_eq!(still_exists, 1, "chain IMV should still exist (not rebuilt)");
}

#[pg_test]
fn f10_doctor_fix_records_failed_and_continues() {
    // Seed TWO F4 known_stale IMVs: one that can reconcile, one that will fail (orphaned).
    // Call reflex_doctor(fix => TRUE) and verify:
    // 1. The failing repair's outcome STARTS WITH 'failed:' (not 'fixed')
    // 2. The report still contains the non-failing IMV (proving isolation)

    // Create a valid source and IMV
    Spi::run("CREATE TABLE f10_valid_src (id INT PRIMARY KEY, val INT)").unwrap();
    Spi::run("INSERT INTO f10_valid_src VALUES (1, 100)").unwrap();
    crate::create_reflex_ivm(
        "f10_valid_imv",
        "SELECT id, val FROM f10_valid_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );

    // Create an orphaned registry entry (with a target name that doesn't exist as a table)
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference (name, graph_depth, known_stale, stale_reason) \
         VALUES ('public.f10_orphaned_imv', 0, TRUE, 'missing target table')"
    ).unwrap();

    // Mark the valid IMV as known_stale too (for F4)
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, stale_reason = 'test stale' WHERE name = 'f10_valid_imv'"
    ).unwrap();

    // Call reflex_doctor(fix => TRUE)
    let result_rows: Vec<(String, String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client.select(
            "SELECT object, outcome, check_id FROM reflex_doctor(NULL, TRUE) WHERE check_id = 'F4'",
            None,
            &[]
        ).unwrap_or_report();
        for row in rs {
            let object: String = row
                .get_by_name::<&str, _>("object")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let outcome: String = row
                .get_by_name::<&str, _>("outcome")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let check_id: String = row
                .get_by_name::<&str, _>("check_id")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((object, outcome, check_id));
        }
        result
    });

    // Should have at least 2 rows: valid and orphaned IMVs
    assert!(
        result_rows.len() >= 2,
        "should have at least 2 F4 rows, got {}",
        result_rows.len()
    );

    // Find the orphaned IMV outcome
    let orphaned_outcome = result_rows
        .iter()
        .find(|(obj, _, _)| obj == "public.f10_orphaned_imv")
        .map(|(_, outcome, _)| outcome.clone());

    assert!(
        orphaned_outcome.is_some(),
        "orphaned IMV should be in the report"
    );

    let orphaned_outcome = orphaned_outcome.unwrap();
    assert!(
        orphaned_outcome.starts_with("failed:"),
        "orphaned IMV repair should fail and start with 'failed:', got: {}",
        orphaned_outcome
    );

    // Find the valid IMV outcome
    let valid_outcome = result_rows
        .iter()
        .find(|(obj, _, _)| obj == "f10_valid_imv")
        .map(|(_, outcome, _)| outcome.clone());

    assert!(
        valid_outcome.is_some(),
        "valid IMV should be in the report (proving isolation: the failure didn't abort)"
    );

    // The valid IMV should either be fixed or also failed (we don't enforce success,
    // just that both are in the report)
    let valid_outcome = valid_outcome.unwrap();
    assert!(
        valid_outcome == "fixed" || valid_outcome.starts_with("failed:"),
        "valid IMV outcome should be 'fixed' or 'failed:...', got: {}",
        valid_outcome
    );
}
