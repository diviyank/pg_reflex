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

    // Mark it as stale with a realistic overlap error (F3 trigger)
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, stale_reason = 'ERROR:  partition \"x\" would overlap partition \"y\"' WHERE name = 'f10_test_imv'"
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
    // Verify that reflex_rebuild_chain is reported but not executed.
    // A decomposed chain is detected structurally: if there exist registered IMVs
    // whose names begin with <this_imv's_bare_name>__ (the sub-IMV convention).

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

    // Register a sub-IMV to simulate a decomposed chain (e.g., from CTE decomposition)
    // The naming convention is <root_bare>__<something>, e.g. f10_chain_imv__cte_x
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference (name, graph_depth, known_stale, stale_reason) \
         VALUES ('f10_chain_imv__cte_x', 1, FALSE, NULL)"
    ).unwrap();

    // Mark the root IMV as stale with a realistic reason
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, stale_reason = 'missing intermediate bound for child x' WHERE name = 'f10_chain_imv'"
    ).unwrap();

    // Call reflex_doctor with fix => TRUE
    let result_rows: Vec<(String, String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client.select(
            "SELECT check_id, object, outcome FROM reflex_doctor(NULL, TRUE) WHERE object = 'f10_chain_imv'",
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

    assert!(!result_rows.is_empty(), "should return row for the chain IMV");
    let (check_id, obj, outcome) = &result_rows[0];
    assert_eq!(check_id, "F4b", "should be classified as F4b (structural decomposed chain detection)");
    assert_eq!(obj, "f10_chain_imv", "object should be the root IMV");
    assert_eq!(outcome, "reported", "F4b outcome should be 'reported' (never auto-performed)");

    // Verify the chain hasn't been rebuilt - check that both root and sub-IMV still exist
    let root_exists = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'f10_chain_imv'"
    ).unwrap().unwrap();
    assert_eq!(root_exists, 1, "root chain IMV should still exist (not rebuilt)");

    let sub_exists = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'f10_chain_imv__cte_x'"
    ).unwrap().unwrap();
    assert_eq!(sub_exists, 1, "sub-IMV should still exist");
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

#[pg_test]
fn f10_decomposed_chain_like_escape_fix() {
    // F10 regression test: verify that LIKE pattern for decomposed-chain detection
    // correctly escapes metacharacters (especially underscore) so that unrelated IMVs
    // with similar names are not misclassified as decomposed chains.
    //
    // Before fix: "f10_base_v" matches "f10_base_v__%", and an unrelated IMV
    //            "f10_base_xy" would also match because underscore is a wildcard.
    // After fix: only "f10_base_v__<suffix>" pattern matches literally.

    // Create a known_stale aggregate IMV (not a decomposed chain on its own)
    Spi::run("CREATE TABLE f10_base_src (id INT PRIMARY KEY, val INT)").unwrap();
    Spi::run("INSERT INTO f10_base_src VALUES (1, 100)").unwrap();
    crate::create_reflex_ivm(
        "f10_base_v",
        "SELECT COUNT(*) AS cnt FROM f10_base_src",
        None,  // no unique key - aggregate
        None,
        Some("IMMEDIATE"),
        None,
    );

    // Create an unrelated IMV whose name would collide under buggy wildcard logic:
    // "f10_base_xy" would match "f10_base_v__%"  if '_' is treated as a wildcard.
    Spi::run("CREATE TABLE f10_unrelated_src (id INT PRIMARY KEY, val INT)").unwrap();
    Spi::run("INSERT INTO f10_unrelated_src VALUES (1, 200)").unwrap();
    crate::create_reflex_ivm(
        "f10_base_xy",
        "SELECT COUNT(*) AS cnt FROM f10_unrelated_src",
        None,  // aggregate
        None,
        Some("IMMEDIATE"),
        None,
    );

    // Mark both as stale with a generic reason (not overlap, not archive)
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, stale_reason = 'test stale' WHERE name = 'f10_base_v'"
    ).unwrap();
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, stale_reason = 'test stale' WHERE name = 'f10_base_xy'"
    ).unwrap();

    // Call reflex_doctor and check classifications
    let result_rows: Vec<(String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client.select(
            "SELECT object, check_id FROM reflex_doctor(NULL, FALSE) WHERE object IN ('f10_base_v', 'f10_base_xy')",
            None,
            &[]
        ).unwrap_or_report();
        for row in rs {
            let object: String = row
                .get_by_name::<&str, _>("object")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let check_id: String = row
                .get_by_name::<&str, _>("check_id")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((object, check_id));
        }
        result
    });

    // Should have 2 results
    assert_eq!(result_rows.len(), 2, "should have exactly 2 F4 results (no decomposed chains)");

    // Both should be F4, not F4b, because neither has actual sub-IMVs registered
    for (obj, check_id) in &result_rows {
        assert_eq!(
            check_id, "F4",
            "IMV '{}' should be classified as F4 (not F4b), because neither has registered sub-IMVs",
            obj
        );
    }

    // Now register an actual sub-IMV to make f10_base_v a real decomposed chain
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference (name, graph_depth, known_stale, stale_reason) \
         VALUES ('f10_base_v__sub', 1, FALSE, NULL)"
    ).unwrap();

    // Call reflex_doctor again
    let result_rows_after: Vec<(String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client.select(
            "SELECT object, check_id FROM reflex_doctor(NULL, FALSE) WHERE object IN ('f10_base_v', 'f10_base_xy')",
            None,
            &[]
        ).unwrap_or_report();
        for row in rs {
            let object: String = row
                .get_by_name::<&str, _>("object")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let check_id: String = row
                .get_by_name::<&str, _>("check_id")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((object, check_id));
        }
        result
    });

    // Now f10_base_v should be F4b (has sub-IMV), but f10_base_xy should still be F4
    let f10_base_v_check = result_rows_after
        .iter()
        .find(|(obj, _)| obj == "f10_base_v")
        .map(|(_, check_id)| check_id.clone());
    let f10_base_xy_check = result_rows_after
        .iter()
        .find(|(obj, _)| obj == "f10_base_xy")
        .map(|(_, check_id)| check_id.clone());

    assert_eq!(
        f10_base_v_check, Some("F4b".to_string()),
        "f10_base_v should be classified as F4b (has registered sub-IMV f10_base_v__sub)"
    );
    assert_eq!(
        f10_base_xy_check, Some("F4".to_string()),
        "f10_base_xy should still be F4 (no sub-IMVs, escaped LIKE pattern doesn't match)"
    );
}

#[pg_test]
fn pg_test_doctor_advisory_residue_not_executed() {
    // Problem A: advisory residue findings (prose, not runnable SQL) should be
    // reported without executing them, even in fix mode.
    // Create a partitioned IMV with empty target partitions and corrupt the
    // base_query so the residue definition probe fails on ALL partitions,
    // triggering advisory "Investigate ..." findings instead of confirmed residue.

    // Create partitioned source with multiple partitions
    Spi::run(
        "CREATE TABLE advisory_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create partitioned source");
    Spi::run("CREATE TABLE advisory_src_us PARTITION OF advisory_src FOR VALUES IN ('us')")
        .expect("create us partition");
    Spi::run("CREATE TABLE advisory_src_eu PARTITION OF advisory_src FOR VALUES IN ('eu')")
        .expect("create eu partition");
    Spi::run("CREATE TABLE advisory_src_asia PARTITION OF advisory_src FOR VALUES IN ('asia')")
        .expect("create asia partition");

    // Insert data to multiple source partitions
    Spi::run(
        "INSERT INTO advisory_src VALUES (1, 'us', 100), (2, 'eu', 200), (3, 'asia', 300)",
    )
    .expect("seed source");

    // Create partitioned IMV
    Spi::run(
        "SELECT create_reflex_ivm( \
            'advisory_imv', \
            'SELECT region, SUM(amount) AS total FROM advisory_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create IMV");

    // Mark source as ignored (simulate archive scenario)
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET ignored_sources = ARRAY['advisory_src'] \
         WHERE name = 'advisory_imv'",
    )
    .expect("mark ignored");

    // Empty ALL target partitions to simulate complete archive
    let tgt_children: Vec<String> = Spi::get_one::<Vec<String>>(
        "SELECT array_agg(c.relname::text ORDER BY c.relname) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = to_regclass('advisory_imv')::oid",
    )
    .expect("ok")
    .unwrap_or_default();

    for child in &tgt_children {
        let delete_cmd = format!("DELETE FROM \"{}\"", child);
        Spi::run(&delete_cmd).expect("empty target partition");
    }

    // BEFORE running audit: corrupt base_query to reference a missing relation so
    // the residue definition probe errors on ALL partitions (safe_count -> None),
    // yielding advisory "Investigate ..." findings, not confirmed residue.
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
         SET base_query = 'SELECT region, SUM(amount) AS total FROM __reflex_no_such_advisory GROUP BY region' \
         WHERE name = 'advisory_imv'",
    )
    .expect("corrupt base_query");

    // Run audit to generate advisory findings
    let _audit_out: String = Spi::get_one("SELECT reflex_audit('advisory_imv')")
        .expect("ok")
        .expect("non-null");

    // Now run doctor in fix mode. All residue findings should be advisory
    // (prose suggested_fix like "Investigate source table..."), NOT runnable SQL.
    // They must be reported with outcome "reported", NOT executed.
    let doctor_rows: Vec<(String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client
            .select(
                "SELECT outcome, action FROM reflex_doctor('advisory_imv', TRUE) \
                 WHERE check_id = 'F5/F6'",
                None,
                &[],
            )
            .unwrap_or_report();
        for row in rs {
            let outcome: String = row
                .get_by_name::<&str, _>("outcome")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let action: String = row
                .get_by_name::<&str, _>("action")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((outcome, action));
        }
        result
    });

    // All findings should be advisory (action contains "Investigate")
    assert!(
        !doctor_rows.is_empty(),
        "expected at least one F5/F6 finding in doctor output"
    );

    for (outcome, action) in doctor_rows {
        // Advisory findings have prose action containing "Investigate"
        if action.to_lowercase().contains("investigate") {
            // Advisory findings must be reported, never fixed or failed
            assert_eq!(
                outcome, "reported",
                "advisory residue finding should have outcome 'reported', got '{}'. \
                 This means prose was incorrectly executed as SQL. Action was: {}",
                outcome, action
            );
        }
    }
}

#[pg_test]
fn pg_test_doctor_collapse_many_residual_partitions() {
    // Problem B: when an IMV has >3 confirmed-residue partitions (threshold=3),
    // collapse them into a single reflex_reconcile(imv) action instead of per-partition ones.

    // Create partitioned source with 5 partitions (ensures >3 threshold)
    // Using LIST with explicit partitions plus DEFAULT to ensure all data lands somewhere
    Spi::run(
        "CREATE TABLE collapse_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create partitioned source");
    Spi::run("CREATE TABLE collapse_src_us PARTITION OF collapse_src FOR VALUES IN ('us')")
        .expect("create us partition");
    Spi::run("CREATE TABLE collapse_src_eu PARTITION OF collapse_src FOR VALUES IN ('eu')")
        .expect("create eu partition");
    Spi::run("CREATE TABLE collapse_src_asia PARTITION OF collapse_src FOR VALUES IN ('asia')")
        .expect("create asia partition");
    Spi::run("CREATE TABLE collapse_src_br PARTITION OF collapse_src FOR VALUES IN ('br')")
        .expect("create br partition");
    Spi::run("CREATE TABLE collapse_src_default PARTITION OF collapse_src DEFAULT")
        .expect("create default partition");

    // Insert data to ALL source partitions (one row each is enough to trigger residue)
    Spi::run(
        "INSERT INTO collapse_src VALUES \
         (1, 'us', 100), (2, 'eu', 200), (3, 'asia', 300), (4, 'br', 400), (5, 'ca', 500)",
    )
    .expect("seed source");

    // Create partitioned IMV
    Spi::run(
        "SELECT create_reflex_ivm( \
            'collapse_imv', \
            'SELECT region, SUM(amount) AS total FROM collapse_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create IMV");

    // Mark source as ignored (simulate archive scenario)
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET ignored_sources = ARRAY['collapse_src'] \
         WHERE name = 'collapse_imv'",
    )
    .expect("mark ignored");

    // Empty ALL target partitions to trigger confirmed residue on all of them
    let tgt_children: Vec<String> = Spi::get_one::<Vec<String>>(
        "SELECT array_agg(c.relname::text ORDER BY c.relname) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = to_regclass('collapse_imv')::oid",
    )
    .expect("ok")
    .unwrap_or_default();

    assert!(
        tgt_children.len() > 3,
        "expected >3 target partitions to test collapse threshold, got {}",
        tgt_children.len()
    );

    for child in &tgt_children {
        let delete_cmd = format!("DELETE FROM \"{}\"", child);
        Spi::run(&delete_cmd).expect("empty target partition");
    }

    // Dry-run: should show exactly ONE F5/F6 row with collapsed reflex_reconcile(imv)
    let dry_rows: Vec<(String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client
            .select(
                "SELECT action, finding FROM reflex_doctor('collapse_imv', FALSE) \
                 WHERE check_id = 'F5/F6'",
                None,
                &[],
            )
            .unwrap_or_report();
        for row in rs {
            let action: String = row
                .get_by_name::<&str, _>("action")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let finding: String = row
                .get_by_name::<&str, _>("finding")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((action, finding));
        }
        result
    });

    // With >3 partitions, should be collapsed into single reflex_reconcile(imv) row
    let reconcile_rows: Vec<_> = dry_rows
        .iter()
        .filter(|(action, _)| action.contains("reflex_reconcile('collapse_imv')"))
        .collect();

    assert!(
        !reconcile_rows.is_empty(),
        "expected collapsed reflex_reconcile(imv) row when >3 partitions have residue, got: {:?}",
        dry_rows
    );

    // Verify there is EXACTLY ONE collapsed row
    assert_eq!(
        reconcile_rows.len(),
        1,
        "expected exactly ONE collapsed reflex_reconcile(imv) row for {} residual partitions, got {}",
        tgt_children.len(),
        reconcile_rows.len()
    );

    // Verify the finding mentions the partition count
    let (_, finding) = &reconcile_rows[0];
    assert!(
        finding.contains(&format!("{} partitions", tgt_children.len())),
        "collapsed finding should mention partition count {}, got: {}",
        tgt_children.len(),
        finding
    );

    // Fix-run and verify the repair succeeds and residue is gone
    let fix_rows: Vec<(String, String)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client
            .select(
                "SELECT outcome, action FROM reflex_doctor('collapse_imv', TRUE) \
                 WHERE check_id = 'F5/F6'",
                None,
                &[],
            )
            .unwrap_or_report();
        for row in rs {
            let outcome: String = row
                .get_by_name::<&str, _>("outcome")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let action: String = row
                .get_by_name::<&str, _>("action")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((outcome, action));
        }
        result
    });

    let reconcile_fix_rows: Vec<_> = fix_rows
        .iter()
        .filter(|(_, action)| action.contains("reflex_reconcile('collapse_imv')"))
        .collect();

    assert!(
        !reconcile_fix_rows.is_empty(),
        "expected reconcile row in fix mode"
    );

    let (outcome, _) = &reconcile_fix_rows[0];
    assert_eq!(
        outcome, "fixed",
        "collapsed reflex_reconcile should return 'fixed', got '{}'",
        outcome
    );

    // Verify the residue is actually gone: re-run audit should not find archive_residue
    let reaudit: String = Spi::get_one("SELECT reflex_audit('collapse_imv')")
        .expect("ok")
        .expect("non-null");

    assert!(
        !reaudit.contains("archive_residue"),
        "after reconciliation, re-audit should not find archive_residue"
    );
}

#[pg_test]
fn pg_test_doctor_below_threshold_keeps_per_partition_action() {
    // Problem B (threshold boundary): when an IMV has exactly 1 partition with
    // confirmed residue (below the collapse threshold of 4), the action should
    // be per-partition reflex_reconcile_partition, not collapsed reflex_reconcile.

    // Create partitioned source with just 1 partition
    Spi::run(
        "CREATE TABLE threshold_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create partitioned source");
    Spi::run("CREATE TABLE threshold_src_us PARTITION OF threshold_src FOR VALUES IN ('us')")
        .expect("create us partition");

    // Insert data
    Spi::run("INSERT INTO threshold_src VALUES (1, 'us', 100)").expect("seed source");

    // Create partitioned IMV
    Spi::run(
        "SELECT create_reflex_ivm( \
            'threshold_imv', \
            'SELECT region, SUM(amount) AS total FROM threshold_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create IMV");

    // Mark source as ignored
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET ignored_sources = ARRAY['threshold_src'] \
         WHERE name = 'threshold_imv'",
    )
    .expect("mark ignored");

    // Empty the target partition
    let tgt_children: Vec<String> = Spi::get_one::<Vec<String>>(
        "SELECT array_agg(c.relname::text ORDER BY c.relname) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = to_regclass('threshold_imv')::oid",
    )
    .expect("ok")
    .unwrap_or_default();

    if !tgt_children.is_empty() {
        let delete_cmd = format!("DELETE FROM \"{}\"", tgt_children[0]);
        Spi::run(&delete_cmd).expect("empty target partition");
    }

    // Dry-run: should show per-partition reflex_reconcile_partition action
    let dry_rows: Vec<(String,)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client
            .select(
                "SELECT action FROM reflex_doctor('threshold_imv', FALSE) \
                 WHERE check_id = 'F5/F6'",
                None,
                &[],
            )
            .unwrap_or_report();
        for row in rs {
            let action: String = row
                .get_by_name::<&str, _>("action")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            result.push((action,));
        }
        result
    });

    assert!(
        !dry_rows.is_empty(),
        "expected at least one F5/F6 row in dry-run"
    );

    let (action,) = &dry_rows[0];
    assert!(
        action.contains("reflex_reconcile_partition"),
        "below-threshold IMV should use per-partition action, got: {}",
        action
    );
    assert!(
        !action.contains("reflex_reconcile('threshold_imv')"),
        "below-threshold IMV should NOT use collapsed reflex_reconcile(imv), got: {}",
        action
    );
}

#[pg_test]
fn f9_f11_doctor_surfaces_orphan_and_duplicate_findings() {
    // Orphan aux tables carrying reflex prefixes but with no owning enabled IMV.
    Spi::run("CREATE TABLE __reflex_scratch_orphan_x (g TEXT, n BIGINT)").expect("orphan scratch");
    Spi::run("CREATE TABLE __reflex_intermediate_orphan_y (g TEXT, n BIGINT)")
        .expect("orphan intermediate");

    // F9: orphan-scratch is surfaced with its DROP command and the object named.
    let scratch_action: String = Spi::get_one(
        "SELECT action FROM reflex_doctor() \
         WHERE check_id = 'F9' AND object LIKE '%__reflex_scratch_orphan_x%' LIMIT 1",
    )
    .expect("q")
    .expect("expected an F9 orphan-scratch row");
    assert!(
        scratch_action.contains("DROP TABLE"),
        "orphan action should be the DROP command, got: {}",
        scratch_action
    );

    // F9: orphan-intermediate surfaced too.
    let inter_rows: i64 = Spi::get_one(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE check_id = 'F9' AND object LIKE '%__reflex_intermediate_orphan_y%'",
    )
    .expect("q")
    .unwrap_or(0);
    assert!(inter_rows >= 1, "expected an F9 orphan-intermediate row");

    // Orphan findings are report-only (destructive DROP is never auto-run).
    let outcome: String = Spi::get_one(
        "SELECT outcome FROM reflex_doctor(NULL, TRUE) \
         WHERE check_id = 'F9' AND object LIKE '%__reflex_scratch_orphan_x%' LIMIT 1",
    )
    .expect("q")
    .expect("expected an F9 row in fix mode");
    assert_eq!(
        outcome, "reported",
        "orphan drops must not auto-execute in fix mode"
    );
}

/// A multi-source filtered IMV whose partition is correctly empty because the
/// filter excludes its rows must not be flagged as residue. The definition
/// probe evaluates the IMV's own base_query (filter included), so the empty
/// partition is definitively cleared.
#[pg_test]
fn pg_doctor_filtered_multi_source_imv_is_not_residue() {
    Spi::run("CREATE TABLE fr_src (k TEXT NOT NULL, d DATE, v INT) PARTITION BY LIST (k)")
        .expect("src");
    Spi::run("CREATE TABLE fr_src_a PARTITION OF fr_src FOR VALUES IN ('a')").expect("a");
    Spi::run("CREATE TABLE fr_src_b PARTITION OF fr_src FOR VALUES IN ('b')").expect("b");
    Spi::run("CREATE TABLE fr_cutoff (d DATE)").expect("cutoff");
    Spi::run("INSERT INTO fr_cutoff VALUES ('2026-06-01')").expect("seed cutoff");
    Spi::run("INSERT INTO fr_src VALUES ('a', '2026-07-01', 1)").expect("seed a");
    // Every 'b' row is older than the cutoff, so partition b is correctly empty.
    Spi::run("INSERT INTO fr_src VALUES ('b', '2026-01-01', 2)").expect("seed b");

    let sql = "SELECT k, sum(v) AS s FROM fr_src \
               WHERE d >= (SELECT d FROM fr_cutoff) GROUP BY k";
    let res = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('fr_imv', '{}', 'k', NULL, NULL, NULL, ARRAY['k'])",
        sql.replace('\'', "''")
    ))
    .expect("create call")
    .expect("create result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");

    // reflex_audit() returns a formatted text report (see other tests in this
    // file / pg_test_audit.rs), not a queryable table.
    let report: String = Spi::get_one("SELECT reflex_audit('fr_imv')")
        .expect("audit query")
        .expect("non-null report");
    assert!(
        !report.contains("archive_residue"),
        "a correctly-filtered empty partition must not be flagged as residue:\n{}",
        report
    );
}

/// An UNFILTERED multi-source join with a legitimately-empty partition (a key
/// present in the partitioned source but with no join match) is definitively
/// verified by the IMV definition probe — the join yields no rows for that
/// partition — and must not be flagged as residue.
#[pg_test]
fn pg_doctor_unfiltered_multi_source_join_legit_empty_not_residue() {
    Spi::run("CREATE TABLE fu_a (k TEXT NOT NULL, v INT) PARTITION BY LIST (k)").expect("a");
    Spi::run("CREATE TABLE fu_a_x PARTITION OF fu_a FOR VALUES IN ('x')").expect("ax");
    Spi::run("CREATE TABLE fu_a_y PARTITION OF fu_a FOR VALUES IN ('y')").expect("ay");
    Spi::run("CREATE TABLE fu_b (k TEXT, w INT)").expect("b");
    Spi::run("INSERT INTO fu_a VALUES ('x', 1)").expect("seed a");
    // No matching fu_b row for 'y', so the IMV's 'y' partition is legitimately
    // empty though fu_a_y is non-empty — with NO where clause anywhere.
    Spi::run("INSERT INTO fu_a VALUES ('y', 2)").expect("seed ay");
    Spi::run("INSERT INTO fu_b VALUES ('x', 100)").expect("seed b");

    let sql = "SELECT a.k, sum(b.w) AS s FROM fu_a a JOIN fu_b b ON b.k = a.k GROUP BY a.k";
    let res = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('fu_imv', '{}', 'k', NULL, NULL, NULL, ARRAY['k'])",
        sql.replace('\'', "''")
    ))
    .expect("create")
    .expect("result");
    assert!(!res.starts_with("ERROR"), "create returned: {res}");

    let report: String = Spi::get_one("SELECT reflex_audit('fu_imv')")
        .expect("audit")
        .expect("report");
    assert!(
        !report.contains("archive_residue"),
        "an unfiltered multi-source join must not be flagged as residue:\n{}",
        report
    );
}

// ---------------------------------------------------------------------------
// PS-4: reflex_doctor truthfulness
// ---------------------------------------------------------------------------

#[pg_test]
fn ps4_pending_queue_has_last_attempt_at_column() {
    let present: bool = Spi::get_one(
        "SELECT EXISTS(SELECT 1 FROM pg_attribute \
         WHERE attrelid = 'public.__reflex_partition_pending'::regclass \
           AND attname = 'last_attempt_at' AND NOT attisdropped)",
    )
    .expect("q")
    .unwrap_or(false);
    assert!(
        present,
        "__reflex_partition_pending must carry last_attempt_at"
    );
}

#[pg_test]
fn ps4_drain_stamps_last_attempt_at_on_failure() {
    // A drain that fails must leave the surviving pending row dated. Neither
    // `enqueued_at` (reset by every re-enqueue) nor `attempts` (an enqueue
    // counter) can date a failure, so without this stamp a wedged root reports
    // an arbitrarily old error next to a freshly reset age.
    Spi::run("CREATE TABLE ps4_stamp_src (region TEXT NOT NULL, v INT) PARTITION BY LIST (region)")
        .expect("src");
    Spi::run("CREATE TABLE ps4_stamp_src_a PARTITION OF ps4_stamp_src FOR VALUES IN ('a')")
        .expect("a");
    // A registry row whose relations do not exist: the drain reaches it and fails.
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference \
            (name, graph_depth, depends_on, partition_columns, partition_strategy, enabled) \
         VALUES ('ps4_stamp_ghost', 0, ARRAY['public.ps4_stamp_src'], ARRAY['region'], 'LIST', TRUE)",
    )
    .expect("ghost");
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending (source_root) \
         VALUES ('public.ps4_stamp_src')",
    )
    .expect("seed");

    let _ = Spi::get_one::<String>("SELECT reflex_flush_partitions()");

    let stamped: bool = Spi::get_one(
        "SELECT last_attempt_at IS NOT NULL FROM public.__reflex_partition_pending \
         WHERE source_root = 'public.ps4_stamp_src'",
    )
    .expect("the failed drain must leave the pending row in place")
    .unwrap_or(false);
    assert!(
        stamped,
        "the drain must stamp last_attempt_at on a root it attempted"
    );

    let failures: i32 = Spi::get_one(
        "SELECT failures FROM public.__reflex_partition_pending \
         WHERE source_root = 'public.ps4_stamp_src'",
    )
    .expect("q")
    .unwrap_or(0);
    assert_eq!(failures, 1, "the failed drain must have counted a failure");
}

#[pg_test]
fn ps4_enqueue_counter_is_not_a_failure_counter() {
    // `attempts` counts partition ATTACHes, not drain attempts. A busy but
    // healthy root crosses any retry threshold within a day and must not be
    // reported as wedged.
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending \
             (source_root, attempts, failures, enqueued_at) \
         VALUES ('ps4_busy.root', 9, 0, now())",
    )
    .expect("seed");
    let n: i64 = Spi::get_one(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE object = 'ps4_busy.root' AND check_id IN ('F1','F2')",
    )
    .expect("q")
    .unwrap_or(-1);
    assert_eq!(n, 0, "9 enqueues with 0 drain failures is not a finding");
}

#[pg_test]
fn ps4_drain_failures_classify_as_f2() {
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending \
             (source_root, attempts, failures, enqueued_at) \
         VALUES ('ps4_wedged.root', 0, 5, now())",
    )
    .expect("seed");
    let n: i64 = Spi::get_one(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE object = 'ps4_wedged.root' AND check_id = 'F2'",
    )
    .expect("q")
    .unwrap_or(-1);
    assert_eq!(n, 1, "5 drain failures with 0 enqueues is F2");
}

#[pg_test]
fn ps4_capped_root_says_auto_retry_suppressed() {
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending \
             (source_root, failures, enqueued_at) VALUES ('ps4_capped.root', 5, now())",
    )
    .expect("seed");
    let n: i64 = Spi::get_one(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE object = 'ps4_capped.root' AND finding LIKE '%auto-retry suppressed%'",
    )
    .expect("q")
    .unwrap_or(-1);
    assert_eq!(
        n, 1,
        "at the failure cap the finding must say auto-retry has stopped"
    );
}

#[pg_test]
fn ps4_finding_age_comes_from_last_attempt_not_enqueue() {
    // enqueued_at fresh (a re-enqueue bumped it), last drain attempt 3 days old.
    // An enqueued_at-derived age would report ~0s over a 3-day-old failure.
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending \
             (source_root, attempts, failures, enqueued_at, last_attempt_at) \
         VALUES ('ps4_dated.root', 40, 3, now(), now() - interval '3 days')",
    )
    .expect("seed");
    let n: i64 = Spi::get_one(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE object = 'ps4_dated.root' AND check_id = 'F2' \
           AND finding ~ 'last drain attempt 2[0-9]{5}s ago'",
    )
    .expect("q")
    .unwrap_or(-1);
    assert_eq!(n, 1, "the reported age must be derived from last_attempt_at");
}

#[pg_test]
fn ps4_never_attempted_row_is_f1() {
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending \
             (source_root, failures, enqueued_at) \
         VALUES ('ps4_rearm.root', 0, now() - interval '3 days')",
    )
    .expect("seed");
    let n: i64 = Spi::get_one(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE object = 'ps4_rearm.root' AND check_id = 'F1' \
           AND finding LIKE '%never attempted%'",
    )
    .expect("q")
    .unwrap_or(-1);
    assert_eq!(
        n, 1,
        "an old row that no drain ever touched is F1 / never attempted"
    );
}

#[pg_test]
fn ps4_recently_retried_row_is_not_reported() {
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending \
             (source_root, failures, enqueued_at, last_attempt_at) \
         VALUES ('ps4_retried.root', 1, now() - interval '3 days', now())",
    )
    .expect("seed");
    let n: i64 = Spi::get_one(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE object = 'ps4_retried.root' AND check_id IN ('F1','F2')",
    )
    .expect("q")
    .unwrap_or(-1);
    assert_eq!(
        n, 0,
        "a root retried a moment ago is not a stuck-queue finding"
    );
}

#[pg_test]
fn ps4_finding_dates_the_wedge_from_stale_since() {
    Spi::run(
        "INSERT INTO public.__reflex_partition_pending \
             (source_root, failures, enqueued_at) VALUES ('ps4_join.root', 5, now())",
    )
    .expect("seed");
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference \
             (name, graph_depth, depends_on, enabled, known_stale, stale_reason, stale_since) \
         VALUES ('ps4_join_imv', 0, ARRAY['ps4_join.root'], TRUE, TRUE, 'boom', \
                 '2026-07-21 16:20:00+00')",
    )
    .expect("seed imv");
    let n: i64 = Spi::get_one(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE object = 'ps4_join.root' \
           AND finding LIKE '%dependent IMVs stale since 2026-07-21%'",
    )
    .expect("q")
    .unwrap_or(-1);
    assert_eq!(n, 1, "the finding must date the wedge from stale_since");
}
