// PS-16 — nightly source-partition swap leaves an orphan mirror child that
// collides with the freshly-attached replacement (`untreated_bugs/
// 2026-07-25_nightly_swap_target_overlap_restale.md`).
//
// `__reflex_on_ddl_command_end` (src/lib.rs) auto-syncs every partitioned IMV
// synchronously on each ALTER TABLE / CREATE TABLE ... PARTITION OF event,
// always with `drop_orphans => FALSE` ("orphan deletion is never automatic").
// A source-side nightly repartition that DETACHes an old leaf and ATTACHes a
// freshly-named replacement covering the SAME bound (rather than reusing the
// old leaf's name) fires two such events in one transaction:
//
//   1. DETACH old_child  -> sync(FALSE): the mirror child for old_child is
//      now an orphan (its source leaf is gone) but is PRESERVED, not dropped.
//   2. CREATE new_child ... FOR VALUES <same bound> -> sync(FALSE): tries to
//      attach a mirror child for new_child at the identical bound, but the
//      still-live orphan from step 1 collides -> Postgres raises "would
//      overlap partition" -> caught -> known_stale = TRUE, forever (a manual
//      reconcile never touches this hardcoded FALSE, so it recurs on every
//      subsequent nightly swap).
//
// These tests never call reconcile/sync manually — they must pass purely
// from the automatic DDL-event sync, matching the field failure exactly.

/// Passthrough RANGE IMV: the target-table collision (report examples 1 & 2).
#[pg_test]
fn ps16_ddl_sync_heals_target_bound_collision() {
    Spi::run(
        "CREATE TABLE ps16_src (id BIGINT, d DATE NOT NULL) PARTITION BY RANGE (d)",
    )
    .expect("create partitioned source");
    Spi::run(
        "CREATE TABLE ps16_src_jan PARTITION OF ps16_src \
         FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
    )
    .expect("create jan child");
    Spi::run("INSERT INTO ps16_src (id, d) VALUES (1, '2026-01-15')").expect("seed");

    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'ps16_v', 'SELECT id, d FROM ps16_src', 'id,d', NULL, NULL, NULL, \
            ARRAY['d'] \
         )",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        !create_result.starts_with("ERROR"),
        "create failed: {}",
        create_result
    );

    let before = Spi::get_one::<i64>("SELECT COUNT(*) FROM ps16_v")
        .expect("q")
        .expect("c");
    assert_eq!(before, 1, "IMV should mirror the seeded row");

    // Nightly repartition swap: DETACH the old leaf, ATTACH a freshly-named
    // replacement covering the identical bound. Two separate DDL commands,
    // same transaction — exactly what the pipeline does.
    Spi::run("ALTER TABLE ps16_src DETACH PARTITION ps16_src_jan").expect("detach jan");
    Spi::run(
        "CREATE TABLE ps16_src_jan_v2 PARTITION OF ps16_src \
         FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
    )
    .expect("attach replacement jan");
    Spi::run("INSERT INTO ps16_src (id, d) VALUES (2, '2026-01-20')").expect("seed replacement");

    let known_stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'ps16_v'",
    )
    .expect("q")
    .expect("row");
    let stale_reason = Spi::get_one::<String>(
        "SELECT COALESCE(stale_reason, '') FROM public.__reflex_ivm_reference WHERE name = 'ps16_v'",
    )
    .expect("q")
    .unwrap_or_default();
    assert!(
        !known_stale,
        "IMV must not go known_stale on a same-bound source repartition (reason: {})",
        stale_reason
    );
    assert!(
        !stale_reason.to_lowercase().contains("overlap"),
        "must not carry an overlap stale_reason (got: {})",
        stale_reason
    );

    // DETACH removes the old leaf from the live source entirely (it becomes a
    // free-standing table pg_reflex no longer reads), so the truthful
    // post-swap count reflects ONLY the replacement leaf's row (id=1's row
    // left with the detached table), not old+new.
    let after = Spi::get_one::<i64>("SELECT COUNT(*) FROM ps16_v")
        .expect("q")
        .expect("c");
    assert_eq!(
        after, 1,
        "IMV must mirror only the replacement leaf's row post-detach"
    );
    let surviving_id = Spi::get_one::<i64>("SELECT id FROM ps16_v")
        .expect("q")
        .expect("id");
    assert_eq!(surviving_id, 2, "the surviving row must be the replacement leaf's");
}

/// Aggregate IMV (has an intermediate table): the intermediate-table
/// collision (report example 3, `__reflex_intermediate_...`).
#[pg_test]
fn ps16_ddl_sync_heals_intermediate_bound_collision() {
    Spi::run(
        "CREATE TABLE ps16_agg_src (id BIGINT, plan_id INT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (plan_id)",
    )
    .expect("create partitioned source");
    Spi::run(
        "CREATE TABLE ps16_agg_src_p1 PARTITION OF ps16_agg_src FOR VALUES IN (1)",
    )
    .expect("create p1 child");
    Spi::run("INSERT INTO ps16_agg_src (id, plan_id, amount) VALUES (1, 1, 100)").expect("seed");

    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'ps16_agg_v', \
            'SELECT plan_id, SUM(amount) AS total FROM ps16_agg_src GROUP BY plan_id', \
            NULL, NULL, NULL, NULL, \
            ARRAY['plan_id'] \
         )",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        !create_result.starts_with("ERROR"),
        "create failed: {}",
        create_result
    );

    let before = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM ps16_agg_v WHERE plan_id = 1",
    )
    .expect("q")
    .expect("row");
    assert_eq!(before.to_string(), "100");

    // Nightly repartition swap on the source: DETACH old leaf, ATTACH a
    // freshly-named replacement at the identical LIST bound.
    Spi::run("ALTER TABLE ps16_agg_src DETACH PARTITION ps16_agg_src_p1").expect("detach p1");
    Spi::run(
        "CREATE TABLE ps16_agg_src_p1_v2 PARTITION OF ps16_agg_src FOR VALUES IN (1)",
    )
    .expect("attach replacement p1");
    Spi::run("INSERT INTO ps16_agg_src (id, plan_id, amount) VALUES (2, 1, 50)")
        .expect("seed replacement");

    let known_stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'ps16_agg_v'",
    )
    .expect("q")
    .expect("row");
    let stale_reason = Spi::get_one::<String>(
        "SELECT COALESCE(stale_reason, '') FROM public.__reflex_ivm_reference WHERE name = 'ps16_agg_v'",
    )
    .expect("q")
    .unwrap_or_default();
    assert!(
        !known_stale,
        "aggregate IMV must not go known_stale on a same-bound source repartition (reason: {})",
        stale_reason
    );
    assert!(
        !stale_reason.to_lowercase().contains("overlap"),
        "must not carry an overlap stale_reason (got: {})",
        stale_reason
    );

    // As above: DETACH removes the old leaf's row (id=1, amount=100) from the
    // live source entirely, so the truthful post-swap aggregate is just the
    // replacement leaf's own row (id=2, amount=50), not old+new.
    let after = Spi::get_one::<pgrx::AnyNumeric>(
        "SELECT total FROM ps16_agg_v WHERE plan_id = 1",
    )
    .expect("q")
    .expect("row");
    assert_eq!(
        after.to_string(),
        "50",
        "aggregate must reflect only the replacement leaf's fresh data post-detach"
    );
}
