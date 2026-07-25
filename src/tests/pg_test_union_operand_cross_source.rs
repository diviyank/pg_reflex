// 2026-07-25 bug — the DEFERRED cross-source guard (`trigger/deferred.rs`)
// reconciled a GENERATED UNION-ALL operand sub-IMV through the public
// `reflex_reconcile`, which inside a trigger skips straight to `reconcile_one`
// with the operand's own mirror trigger LIVE. The rebuild's INSERT re-fires
// that mirror into the materialised wrapper a second time — nothing removes
// the wrapper's stale slice (`TRUNCATE` doesn't fire the mirror) — silently
// doubling the wrapper and the parent aggregate that reads it.
//
// The fix (`reconcile_generated_child_for_cross_source_guard`, `reconcile.rs`)
// rebuilds the operand with propagation suppressed, then resyncs the
// wrapper's `__reflex_src_idx` slice for it explicitly.

/// Main repro: a CTE-over-UNION-ALL feeding an aggregate, DEFERRED mode. An
/// ordinary transaction — insert a parent row and its child row together —
/// mutates BOTH of one UNION-ALL operand's own sources, tripping the guard.
/// The parent aggregate must match a fresh recompute.
#[pg_test]
fn xsu_union_operand_cross_source_matches_recompute() {
    Spi::run("CREATE TABLE xsu1_ua (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("CREATE TABLE xsu1_ub (id INT PRIMARY KEY, ua_id INT, w INT)").unwrap();
    Spi::run("CREATE TABLE xsu1_uc (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("INSERT INTO xsu1_ua VALUES (1,10,0), (2,20,0)").unwrap();
    Spi::run("INSERT INTO xsu1_ub VALUES (100,1,7), (101,2,8)").unwrap();
    Spi::run("INSERT INTO xsu1_uc VALUES (200,10,3)").unwrap();

    let sql = "WITH u AS ( \
                 SELECT xsu1_ua.g AS g, xsu1_ub.w AS v \
                   FROM xsu1_ua JOIN xsu1_ub ON xsu1_ub.ua_id = xsu1_ua.id \
                 UNION ALL \
                 SELECT g, v FROM xsu1_uc \
               ) \
               SELECT g, SUM(v) AS s FROM u GROUP BY g";
    crate::create_reflex_ivm("xsu1_v", sql, None, None, Some("DEFERRED"), None);
    assert_imv_correct("xsu1_v", sql);

    // Ordinary txn: a parent row (xsu1_ua) and its child row (xsu1_ub)
    // inserted together — both of union_0's own sources mutated in one txn.
    Spi::run("INSERT INTO xsu1_ua VALUES (3,30,0)").unwrap();
    Spi::run("INSERT INTO xsu1_ub VALUES (102,3,9)").unwrap();
    Spi::run("SELECT reflex_flush_deferred('xsu1_ua')").expect("flush ua");
    Spi::run("SELECT reflex_flush_deferred('xsu1_ub')").expect("flush ub");
    // The wrapper resync's DELETE+INSERT stages a fresh delta for the parent
    // (xsu1_v depends on the wrapper as a source too); a real COMMIT's
    // deferred constraint trigger re-fires for that newly queued row
    // automatically, but these tests call `reflex_flush_deferred` directly
    // rather than actually committing, so drain it explicitly — the same
    // idiom `test_deferred_cascade_*` (`pg_test_deferred.rs`) uses for any
    // multi-level DEFERRED chain.
    drain_deferred_pending();

    assert_imv_correct("xsu1_v", sql);
}

/// Same shape, but pins the wrapper's stored `__reflex_src_idx = 0` slice
/// directly — the row count the parent's SUM could coincidentally cancel out
/// even if the wrapper itself were doubled. Pre-fix this holds 5 rows (the 2
/// stale + the 3 freshly reinserted); fixed it holds exactly 3.
#[pg_test]
fn xsu_union_operand_cross_source_wrapper_slice_not_doubled() {
    Spi::run("CREATE TABLE xsu2_ua (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("CREATE TABLE xsu2_ub (id INT PRIMARY KEY, ua_id INT, w INT)").unwrap();
    Spi::run("CREATE TABLE xsu2_uc (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    Spi::run("INSERT INTO xsu2_ua VALUES (1,10,0), (2,20,0)").unwrap();
    Spi::run("INSERT INTO xsu2_ub VALUES (100,1,7), (101,2,8)").unwrap();
    Spi::run("INSERT INTO xsu2_uc VALUES (200,10,3)").unwrap();

    let sql = "WITH u AS ( \
                 SELECT xsu2_ua.g AS g, xsu2_ub.w AS v \
                   FROM xsu2_ua JOIN xsu2_ub ON xsu2_ub.ua_id = xsu2_ua.id \
                 UNION ALL \
                 SELECT g, v FROM xsu2_uc \
               ) \
               SELECT g, SUM(v) AS s FROM u GROUP BY g";
    crate::create_reflex_ivm("xsu2_v", sql, None, None, Some("DEFERRED"), None);

    let wrapper = "xsu2_v__cte_u";
    let before = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM {wrapper} WHERE __reflex_src_idx = 0"
    ))
    .expect("q")
    .expect("v");
    assert_eq!(
        before, 2,
        "fixture precondition: union_0 (ua join ub) seeds 2 rows into the wrapper"
    );

    Spi::run("INSERT INTO xsu2_ua VALUES (3,30,0)").unwrap();
    Spi::run("INSERT INTO xsu2_ub VALUES (102,3,9)").unwrap();
    Spi::run("SELECT reflex_flush_deferred('xsu2_ua')").expect("flush ua");
    Spi::run("SELECT reflex_flush_deferred('xsu2_ub')").expect("flush ub");
    drain_deferred_pending();

    let after = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM {wrapper} WHERE __reflex_src_idx = 0"
    ))
    .expect("q")
    .expect("v");
    assert_eq!(
        after, 3,
        "the wrapper's union_0 slice must hold exactly the 3 correct rows, not \
         5 (2 stale + 3 re-appended)"
    );

    // And the parent, fed from the (now correctly resynced) wrapper, matches.
    assert_imv_correct("xsu2_v", sql);
}

/// Facet (b): the guard must not silently discard a staged delta whose only
/// consumer — the reconcile it forces — failed. Forces a REAL reconcile
/// failure (a partitioned join IMV whose target partition was DETACHed, so
/// `reconcile_one`'s per-partition swap returns "ERROR: partition reconcile
/// failed" as a string, never a raise) inside the cross-source guard, and
/// asserts the IMV ends up flagged (`known_stale`) rather than the delta
/// vanishing with no trace.
///
/// Deliberately a PLAIN (non-generated) partitioned join IMV: the return-value
/// check applies uniformly to both the generated and non-generated branches of
/// the guard, and a plain partitioned join is the simplest real way to force a
/// soft `ERROR:` string out of `reconcile_one`.
#[pg_test]
fn xsu_guard_reconcile_failure_flags_known_stale() {
    Spi::run(
        "CREATE TABLE xpb_s (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .unwrap();
    Spi::run("CREATE TABLE xpb_s_p1 PARTITION OF xpb_s FOR VALUES IN ('a')").unwrap();
    Spi::run("CREATE TABLE xpb_s_p2 PARTITION OF xpb_s FOR VALUES IN ('b')").unwrap();
    Spi::run("INSERT INTO xpb_s VALUES (1,'a',10),(2,'b',20)").unwrap();
    Spi::run("CREATE TABLE xpb_d (region TEXT PRIMARY KEY, label TEXT)").unwrap();
    Spi::run("INSERT INTO xpb_d VALUES ('a','A'),('b','B')").unwrap();

    Spi::run(
        "SELECT create_reflex_ivm('xpb_v', \
           'SELECT s.region, s.id, s.amount, d.label FROM xpb_s s \
            JOIN xpb_d d ON d.region = s.region', \
           'region,id', 'UNLOGGED', 'DEFERRED', NULL, ARRAY['region'])",
    )
    .expect("create partitioned join");

    // Break one target partition child by DETACHing it — a real,
    // physically-inducible corruption (a stuck prior swap, an operator's
    // manual DETACH) — so `execute_partition_swap_for_child` cannot find its
    // bound and `reconcile_one` returns a soft error string.
    let broken_child = Spi::get_one::<String>(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = 'xpb_v'::regclass ORDER BY c.relname LIMIT 1",
    )
    .expect("q")
    .expect("v");
    Spi::run(&format!("ALTER TABLE xpb_v DETACH PARTITION {broken_child}")).expect("detach");

    // Cross-source guard shape: mutate BOTH of xpb_v's own sources in one txn.
    Spi::run("INSERT INTO xpb_s VALUES (3,'a',5)").unwrap();
    Spi::run("UPDATE xpb_d SET label = 'A2' WHERE region = 'a'").unwrap();
    Spi::run("SELECT reflex_flush_deferred('xpb_s')").expect("flush s");
    Spi::run("SELECT reflex_flush_deferred('xpb_d')").expect("flush d");

    let known_stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'xpb_v'",
    )
    .expect("q")
    .expect("v");
    assert!(
        known_stale,
        "a guard-forced reconcile that fails must flag known_stale, not silently \
         discard the staged delta"
    );
    let stale_reason = Spi::get_one::<String>(
        "SELECT stale_reason FROM public.__reflex_ivm_reference WHERE name = 'xpb_v'",
    )
    .expect("q")
    .expect("v");
    assert!(
        stale_reason.contains("partition reconcile failed"),
        "stale_reason must name the underlying failure: {stale_reason}"
    );
}
