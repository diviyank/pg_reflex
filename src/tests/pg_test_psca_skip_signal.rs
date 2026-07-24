// PS-CA — demonstration tests for the "skip-unchanged reconcile" investigation.
//
// These tests do NOT test a skip feature. They PROVE, on real IMVs over real
// sources with the EXCEPT ALL oracle, that every cheap package-controlled
// change-signal is BLIND to at least one input change that leaves an IMV stale.
// They are the evidence for the investigation's conclusion: no sound, cheap,
// testable skip-unchanged signal exists at the package level for the general
// case (and specifically for the current_assortment matview-scalar-subquery
// shape). A skip built on any of these signals would silently serve stale data.
//
// Each test is written so it goes GREEN by DEMONSTRATING the blindness (the
// signal did NOT move / relfilenode did NOT change) while the source genuinely
// changed. If a future PostgreSQL/pgrx behavior change made a signal actually
// track the input, the corresponding assert flips and the test goes RED — a
// signpost to re-open the skip design.

/// Helper: read flush_count from the registry.
fn psca_flush_count(imv: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT COALESCE(flush_count, 0) FROM public.__reflex_ivm_reference WHERE name = '{}'",
        imv
    ))
    .expect("flush_count query failed")
    .expect("flush_count NULL")
}

/// Helper: read last_update_date as text (NULL -> empty string).
fn psca_last_update(imv: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT COALESCE(last_update_date::text, '') FROM public.__reflex_ivm_reference WHERE name = '{}'",
        imv
    ))
    .expect("last_update_date query failed")
    .unwrap_or_default()
}

/// Helper: relfilenode of a relation (0 if none / partitioned parent).
fn psca_relfilenode(qualified: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT relfilenode::bigint FROM pg_class WHERE oid = '{}'::regclass",
        qualified
    ))
    .expect("relfilenode query failed")
    .unwrap_or(0)
}

/// Count of oracle mismatches between IMV and a fresh evaluation of its query.
fn psca_oracle_mismatches(imv: &str, fresh_sql: &str) -> i64 {
    let check = format!(
        "SELECT COUNT(*) FROM (\
            (SELECT * FROM {imv} EXCEPT ALL SELECT * FROM ({q}) AS __f1) \
            UNION ALL \
            (SELECT * FROM ({q}) AS __f2 EXCEPT ALL SELECT * FROM {imv}) \
         ) __oracle",
        imv = imv,
        q = fresh_sql
    );
    Spi::get_one::<i64>(&check)
        .expect("oracle query failed")
        .expect("oracle NULL")
}

// T1 — In IMMEDIATE mode (the default, and current_assortment's mode) flush_count
// is NEVER written. A real source change that the IMV correctly maintains leaves
// flush_count frozen. => "skip if flush_count did not advance" classifies every
// immediate IMV as permanently unchanged: a catastrophic blanket wrong-skip.
#[pg_test]
fn psca_t1_flush_count_frozen_in_immediate_mode() {
    Spi::run("CREATE TABLE psca_t1 (id INT PRIMARY KEY, v INT, active BOOLEAN)").expect("t");
    Spi::run("INSERT INTO psca_t1 VALUES (1, 100, true), (2, 200, false)").expect("seed");
    assert_eq!(
        crate::create_reflex_ivm(
            "psca_v1",
            "SELECT id, v FROM psca_t1 WHERE active = true",
            None,
            None,
            None, // IMMEDIATE
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );

    let fc_before = psca_flush_count("psca_v1");

    // A real, IMV-affecting source change (new matching row).
    Spi::run("INSERT INTO psca_t1 VALUES (3, 300, true)").expect("insert");

    // The IMV is correctly maintained by the immediate trigger ...
    assert_imv_correct("psca_v1", "SELECT id, v FROM psca_t1 WHERE active = true");
    let n = Spi::get_one::<i64>("SELECT COUNT(*) FROM psca_v1").unwrap().unwrap();
    assert_eq!(n, 2, "source genuinely changed (Alice+Carol now match)");

    // ... yet flush_count did NOT move. The signal is dead in immediate mode.
    let fc_after = psca_flush_count("psca_v1");
    assert_eq!(
        fc_after, fc_before,
        "DEMONSTRATION: flush_count is never written in IMMEDIATE mode ({} -> {}); \
         a flush_count-based skip signal would treat this just-changed IMV as unchanged",
        fc_before, fc_after
    );
}

// T2 — last_update_date is stamped by the immediate flush ONLY if it was not
// already stamped within the last second (a deliberate hot-path throttle). A
// reconcile stamps it; a source change in the throttle window then leaves it
// unmoved. => "skip if last_update_date == value-at-last-reconcile" wrongly skips
// a stale IMV. (In the pgrx harness NOW() is frozen for the whole test txn, so
// ANY post-reconcile change is inside the throttle window — the production hole
// is the real ~1s window right after a reconcile.)
#[pg_test]
fn psca_t2_last_update_date_throttle_is_blind() {
    Spi::run("CREATE TABLE psca_t2 (id INT PRIMARY KEY, v INT, active BOOLEAN)").expect("t");
    Spi::run("INSERT INTO psca_t2 VALUES (1, 100, true)").expect("seed");
    assert_eq!(
        crate::create_reflex_ivm(
            "psca_v2",
            "SELECT id, v FROM psca_t2 WHERE active = true",
            None,
            None,
            None, // IMMEDIATE
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );

    // Reconcile stamps last_update_date = clock_timestamp().
    assert_eq!(crate::reflex_reconcile("psca_v2"), "RECONCILED");
    let lud_before = psca_last_update("psca_v2");

    // A real source change (trigger maintains the IMV) inside the throttle window.
    Spi::run("INSERT INTO psca_t2 VALUES (2, 200, true)").expect("insert");
    assert_imv_correct("psca_v2", "SELECT id, v FROM psca_t2 WHERE active = true");
    let n = Spi::get_one::<i64>("SELECT COUNT(*) FROM psca_t2").unwrap().unwrap();
    assert_eq!(n, 2, "source genuinely changed");

    let lud_after = psca_last_update("psca_v2");
    assert_eq!(
        lud_after, lud_before,
        "DEMONSTRATION: the 1s throttle skipped the last_update_date stamp \
         ('{}' -> '{}'); a last_update_date-based skip signal would skip a stale IMV",
        lud_before, lud_after
    );
}

// T3 — relfilenode (pg_class) is lag-free and transactionally visible, and it
// changes on a heap rewrite (TRUNCATE, VACUUM FULL, non-concurrent REFRESH). But
// it does NOT change on in-place INSERT/UPDATE/DELETE. => a relfilenode-only
// signal is blind to ordinary DML and fails the never-skip-a-stale requirement
// for a plain UPDATE on the anchor source.
#[pg_test]
fn psca_t3_relfilenode_blind_to_in_place_dml() {
    Spi::run("CREATE TABLE psca_t3 (id INT PRIMARY KEY, v INT)").expect("t");
    Spi::run("INSERT INTO psca_t3 VALUES (1, 100), (2, 200)").expect("seed");

    let rfn_before = psca_relfilenode("psca_t3");
    Spi::run("UPDATE psca_t3 SET v = v + 1").expect("update");
    Spi::run("INSERT INTO psca_t3 VALUES (3, 300)").expect("insert");
    Spi::run("DELETE FROM psca_t3 WHERE id = 1").expect("delete");
    let rfn_after = psca_relfilenode("psca_t3");

    assert_eq!(
        rfn_after, rfn_before,
        "DEMONSTRATION: relfilenode unmoved by in-place UPDATE/INSERT/DELETE \
         ({} -> {}); a relfilenode-only skip signal misses ordinary DML",
        rfn_before, rfn_after
    );

    // Pitfall worth recording: relfilenode change on TRUNCATE is CONTEXT
    // DEPENDENT. For a table created in the CURRENT transaction (exactly the
    // pgrx test-harness situation, and any create-then-load txn), TRUNCATE
    // truncates the existing file in place and does NOT assign a new
    // relfilenode — so relfilenode is not even a reliable rewrite signal here.
    Spi::run("TRUNCATE psca_t3").expect("truncate");
    let rfn_trunc = psca_relfilenode("psca_t3");
    assert_eq!(
        rfn_trunc, rfn_before,
        "DEMONSTRATION: TRUNCATE of a same-transaction table did NOT change \
         relfilenode ({} -> {}) — relfilenode is a context-dependent partial signal",
        rfn_before, rfn_trunc
    );
}

// T4 — CROWN JEWEL. The current_assortment shape: a passthrough IMV whose WHERE
// filter comes from an uncorrelated scalar subquery over a MATERIALIZED VIEW.
// Flipping which value the matview selects (with NO change to the anchor table)
// makes the IMV stale. This staleness is invisible to EVERY package-controlled
// signal: flush_count (frozen in immediate mode), last_update_date (no IMV-source
// trigger fires — the matview isn't a DML source), and the matview's relfilenode
// (unchanged under REFRESH ... CONCURRENTLY). Only a from-scratch reconcile
// restores correctness. => the motivating 2.5h IMV cannot be safely skipped.
#[pg_test]
fn psca_t4_matview_subquery_flip_invisible_to_all_signals() {
    // Anchor table: two assortments, mirrors assortment_activity_relation.
    Spi::run("CREATE TABLE psca_aar (product_id INT, assortment_id INT, is_active BOOLEAN)")
        .expect("aar");
    Spi::run(
        "INSERT INTO psca_aar VALUES \
         (1, 10, true), (2, 10, false), (3, 20, true), (4, 20, true)",
    )
    .expect("seed aar");

    // Control table + matview selecting the "current" assortment (mirrors sop_current_view).
    Spi::run("CREATE TABLE psca_ctl (cur INT)").expect("ctl");
    Spi::run("INSERT INTO psca_ctl VALUES (10)").expect("seed ctl");
    Spi::run("CREATE MATERIALIZED VIEW psca_cur AS SELECT cur AS assortment_id FROM psca_ctl")
        .expect("mv");
    Spi::run("CREATE UNIQUE INDEX psca_cur_uq ON psca_cur(assortment_id)").expect("uq");

    let base_query = "SELECT product_id, is_active FROM psca_aar \
                      WHERE assortment_id = (SELECT assortment_id FROM psca_cur)";
    assert_eq!(
        crate::create_reflex_ivm("psca_ca", base_query, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );

    // Initially correct: holds assortment 10's rows (products 1, 2).
    assert_imv_correct("psca_ca", base_query);
    let rows0 = Spi::get_one::<i64>("SELECT COUNT(*) FROM psca_ca").unwrap().unwrap();
    assert_eq!(rows0, 2, "assortment 10 has 2 rows");

    // Capture every candidate signal.
    let fc0 = psca_flush_count("psca_ca");
    let lud0 = psca_last_update("psca_ca");
    let rfn0 = psca_relfilenode("psca_cur");

    // FLIP the matview's selected assortment WITHOUT touching psca_aar.
    Spi::run("UPDATE psca_ctl SET cur = 20").expect("flip ctl");
    Spi::run("REFRESH MATERIALIZED VIEW CONCURRENTLY psca_cur").expect("refresh concurrently");

    // The IMV is now STALE: fresh query yields assortment 20's rows (products 3,4),
    // but the IMV still holds assortment 10's rows.
    let mismatches = psca_oracle_mismatches("psca_ca", base_query);
    assert!(
        mismatches > 0,
        "IMV must be STALE after the matview flip (oracle mismatches = {}); \
         if this is 0 the fixture failed to induce staleness",
        mismatches
    );

    // Yet NO package-controlled signal moved:
    let fc1 = psca_flush_count("psca_ca");
    let lud1 = psca_last_update("psca_ca");
    let rfn1 = psca_relfilenode("psca_cur");
    assert_eq!(fc1, fc0, "flush_count blind to matview flip ({} -> {})", fc0, fc1);
    assert_eq!(
        lud1, lud0,
        "last_update_date blind to matview flip ('{}' -> '{}') — no IMV-source trigger fires",
        lud0, lud1
    );
    assert_eq!(
        rfn1, rfn0,
        "matview relfilenode blind to REFRESH CONCURRENTLY ({} -> {})",
        rfn0, rfn1
    );

    // Only a full reconcile restores correctness — proving the reconcile is
    // genuinely necessary here and must NOT be skipped.
    assert_eq!(crate::reflex_reconcile("psca_ca"), "RECONCILED");
    assert_imv_correct("psca_ca", base_query);
    let rows1 = Spi::get_one::<i64>("SELECT COUNT(*) FROM psca_ca").unwrap().unwrap();
    assert_eq!(rows1, 2, "assortment 20 also has 2 rows, now correctly reflected");
}

// T4b — same shape, but NON-concurrent REFRESH. Here the matview's relfilenode
// DOES change, so a relfilenode-inclusive signal could catch THIS refresh — but
// T4 shows the concurrent variant it cannot catch, and flush_count/last_update
// remain blind to both. Documents that relfilenode covers only the non-concurrent
// refresh, leaving a permanent hole for concurrent refresh.
#[pg_test]
fn psca_t4b_nonconcurrent_refresh_changes_relfilenode_only() {
    Spi::run("CREATE TABLE psca_ctlb (cur INT)").expect("ctl");
    Spi::run("INSERT INTO psca_ctlb VALUES (10)").expect("seed");
    Spi::run("CREATE MATERIALIZED VIEW psca_curb AS SELECT cur AS assortment_id FROM psca_ctlb")
        .expect("mv");

    let rfn0 = psca_relfilenode("psca_curb");
    Spi::run("UPDATE psca_ctlb SET cur = 20").expect("flip");
    Spi::run("REFRESH MATERIALIZED VIEW psca_curb").expect("refresh non-concurrent");
    let rfn1 = psca_relfilenode("psca_curb");

    assert_ne!(
        rfn1, rfn0,
        "non-concurrent REFRESH rewrites the matview heap => relfilenode changes ({} -> {})",
        rfn0, rfn1
    );
}
