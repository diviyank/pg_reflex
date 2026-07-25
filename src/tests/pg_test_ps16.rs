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

/// Adversarial-review regression: the bound-collision heal must be scoped to
/// DIRECT children of the exact immediate parent receiving the incoming
/// CREATE, never the whole multi-level subtree. A flat whole-tree scan would
/// treat unrelated leaves under DIFFERENT top-level branches that happen to
/// share a repeated sub-partition bound literal (a common shape: LIST(region)
/// -> LIST(quarter), with 'Q1'..'Q4' repeated under every region) as
/// colliding siblings — silently CASCADE-dropping live, unrelated data with
/// no error and no known_stale flag, which is strictly worse than the
/// original bug. Retiring one branch (`east`) must never touch a sibling
/// branch's (`west`) same-bound sub-partition.
#[pg_test]
fn ps16_bound_collision_heal_is_scoped_to_exact_parent_not_whole_subtree() {
    Spi::run(
        "CREATE TABLE ps16m_src (id BIGINT, region TEXT NOT NULL, quarter TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create root");
    Spi::run(
        "CREATE TABLE ps16m_src_east PARTITION OF ps16m_src FOR VALUES IN ('east') \
         PARTITION BY LIST (quarter)",
    )
    .expect("create east branch");
    Spi::run("CREATE TABLE ps16m_src_east_q1 PARTITION OF ps16m_src_east FOR VALUES IN ('Q1')")
        .expect("create east/Q1 leaf");
    Spi::run(
        "CREATE TABLE ps16m_src_west PARTITION OF ps16m_src FOR VALUES IN ('west') \
         PARTITION BY LIST (quarter)",
    )
    .expect("create west branch");
    Spi::run("CREATE TABLE ps16m_src_west_q1 PARTITION OF ps16m_src_west FOR VALUES IN ('Q1')")
        .expect("create west/Q1 leaf");
    Spi::run("INSERT INTO ps16m_src (id, region, quarter, amount) VALUES (1, 'east', 'Q1', 111)")
        .expect("seed east");
    Spi::run("INSERT INTO ps16m_src (id, region, quarter, amount) VALUES (2, 'west', 'Q1', 222)")
        .expect("seed west");

    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'ps16m_v', 'SELECT id, region, quarter, amount FROM ps16m_src', 'id,region,quarter', \
            NULL, NULL, NULL, ARRAY['region','quarter'] \
         )",
    )
    .expect("create call")
    .expect("create result");
    assert!(
        !create_result.starts_with("ERROR"),
        "create failed: {}",
        create_result
    );

    let west_before = Spi::get_one::<i64>("SELECT COUNT(*) FROM ps16m_v WHERE region = 'west'")
        .expect("q")
        .expect("c");
    assert_eq!(west_before, 1, "west/Q1 should mirror the seeded row");

    // The defect actually manifests HERE, during ordinary tree construction:
    // creating the 'west' branch's mirror is itself a `CREATE TABLE ...
    // PARTITION OF` event, which re-enters the synchronous auto-sync
    // (`__reflex_on_ddl_command_end` fires on its own DDL). A whole-subtree
    // scoped heal sees 'east_q1' (bound 'Q1', already built with real data)
    // as a false-positive collision with the about-to-be-verified 'west_q1'
    // node and silently CASCADE-drops it — before any DETACH ever happens.
    let east_q1_mirror_intact_after_create = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM ps16m_v WHERE region = 'east'",
    )
    .expect("q")
    .expect("c");
    assert_eq!(
        east_q1_mirror_intact_after_create, 1,
        "creating the unrelated 'west/Q1' mirror must never drop 'east/Q1' \
         merely because they share the same sub-partition bound ('Q1')"
    );

    // Ordinary, legitimate branch retirement — no repartition trick. 'east_q1'
    // is now a genuine orphan (its source is gone); drop_orphans=false must
    // PRESERVE it, not touch it, exactly as it does for a single-level tree.
    Spi::run("ALTER TABLE ps16m_src DETACH PARTITION ps16m_src_east").expect("detach east");
    Spi::run("DROP TABLE ps16m_src_east CASCADE").expect("drop east");

    let east_q1_mirror_preserved = Spi::get_one::<bool>(
        "SELECT to_regclass('public.ps16m_v_ps16m_src_east_q1') IS NOT NULL",
    )
    .expect("q")
    .expect("v");
    assert!(
        east_q1_mirror_preserved,
        "ps16m_v_ps16m_src_east_q1 must be PRESERVED as an orphan (drop_orphans=false), \
         not CASCADE-dropped as a false-positive bound collision with 'west_q1'"
    );

    let west_after = Spi::get_one::<i64>("SELECT COUNT(*) FROM ps16m_v WHERE region = 'west'")
        .expect("q")
        .expect("c");
    assert_eq!(
        west_after, 1,
        "the unrelated 'west/Q1' mirror partition must be untouched throughout"
    );
}
