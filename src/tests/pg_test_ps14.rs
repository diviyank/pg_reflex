// PS-14 — make futile reflex_rebuild_imv retries observable, and warn (not
// refuse) when rebuild targets an IMV it structurally cannot converge.
//
// Field evidence: reflex_rebuild_imv('yse.sop_last_forecast_view') retried 1020x
// externally because a partitioned IMV whose stale data arrives via an
// ignore_sources / matview source cannot be refilled by an anchor-scoped rebuild,
// and nothing made the futile repetition visible. Two gaps:
//   (A1) invisibility — add rebuild_count + last_rebuild_at, bumped only by the
//        targeted-recovery entry points, surfaced in reflex_ivm_status.
//   (A2) wrong primitive silently — an actionable advisory naming the primitive
//        that CAN converge (reflex_reconcile_partition / reflex_rebuild_chain /
//        refresh_imv_depending_on). Additive WARN, never a refusal.

fn ps14_rebuild_count(name: &str) -> i64 {
    Spi::get_one::<i64>(&format!(
        "SELECT COALESCE(rebuild_count, 0) FROM public.__reflex_ivm_reference WHERE name = '{}'",
        name
    ))
    .expect("rebuild_count query failed")
    .expect("no registry row")
}

fn ps14_last_rebuild_is_set(name: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT last_rebuild_at IS NOT NULL FROM public.__reflex_ivm_reference WHERE name = '{}'",
        name
    ))
    .expect("last_rebuild_at query failed")
    .expect("no registry row")
}

/// (1) A targeted reflex_rebuild_imv bumps rebuild_count each call and stamps
/// last_rebuild_at. The count is the repeat-call signal the field loop lacked.
#[pg_test]
fn ps14_targeted_rebuild_increments_count_and_stamps() {
    Spi::run("CREATE TABLE ps14a_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps14a_src VALUES (1,'a',10),(2,'b',20)").unwrap();
    crate::create_reflex_ivm(
        "ps14a_imv",
        "SELECT grp, SUM(val) AS total FROM ps14a_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    assert_eq!(ps14_rebuild_count("ps14a_imv"), 0, "fresh IMV starts at 0");
    assert!(!ps14_last_rebuild_is_set("ps14a_imv"), "never rebuilt yet");

    let r = Spi::get_one::<String>("SELECT reflex_rebuild_imv('ps14a_imv')")
        .unwrap()
        .unwrap();
    assert_eq!(r, "RECONCILED", "rebuild still returns its normal result");
    assert_eq!(ps14_rebuild_count("ps14a_imv"), 1, "one targeted rebuild -> 1");
    assert!(
        ps14_last_rebuild_is_set("ps14a_imv"),
        "last_rebuild_at must be stamped"
    );

    Spi::run("SELECT reflex_rebuild_imv('ps14a_imv')").unwrap();
    assert_eq!(
        ps14_rebuild_count("ps14a_imv"),
        2,
        "a second targeted rebuild -> 2 (this is the repeat-call visibility)"
    );
}

/// (2) reflex_reconcile (the other targeted-recovery entry point) also counts.
#[pg_test]
fn ps14_reflex_reconcile_increments_count() {
    Spi::run("CREATE TABLE ps14b_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps14b_src VALUES (1,'a',10)").unwrap();
    crate::create_reflex_ivm(
        "ps14b_imv",
        "SELECT grp, SUM(val) AS total FROM ps14b_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    Spi::run("SELECT reflex_reconcile('ps14b_imv')").unwrap();
    assert_eq!(ps14_rebuild_count("ps14b_imv"), 1);
    // The two-arg overload (used by reflex_doctor) is also a targeted recovery.
    Spi::run("SELECT reflex_reconcile('ps14b_imv', TRUE)").unwrap();
    assert_eq!(ps14_rebuild_count("ps14b_imv"), 2);
}

/// (3) The internal cascade descent must NOT inflate the count, or it is
/// meaningless. Rebuilding a decomposed parent rebuilds its generated CTE child
/// via reconcile_one (not the entry point), so the child's count stays 0 while
/// the parent's is 1.
#[pg_test]
fn ps14_internal_cascade_does_not_inflate_child_count() {
    Spi::run("CREATE TABLE ps14c_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps14c_src VALUES (1,'a',10),(2,'b',20)").unwrap();
    crate::create_reflex_ivm(
        "ps14c_imv",
        "WITH base AS (SELECT id, grp, val FROM ps14c_src) \
         SELECT grp, SUM(val) AS total FROM base GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Sanity: the generated child exists.
    let child = "ps14c_imv__cte_base";
    let child_exists = Spi::get_one::<i64>(&format!(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = '{}'",
        child
    ))
    .unwrap()
    .unwrap();
    assert_eq!(child_exists, 1, "generated CTE child must exist");

    Spi::run("SELECT reflex_rebuild_imv('ps14c_imv')").unwrap();

    assert_eq!(
        ps14_rebuild_count("ps14c_imv"),
        1,
        "the named parent is a targeted rebuild -> 1"
    );
    assert_eq!(
        ps14_rebuild_count(child),
        0,
        "the generated child was rebuilt via the internal cascade descent, NOT a \
         targeted entry point — its count must stay 0"
    );
}

/// (4) reflex_ivm_status surfaces both new columns.
#[pg_test]
fn ps14_status_surfaces_rebuild_columns() {
    Spi::run("CREATE TABLE ps14d_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps14d_src VALUES (1,'a',10)").unwrap();
    crate::create_reflex_ivm(
        "ps14d_imv",
        "SELECT grp, SUM(val) AS total FROM ps14d_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    Spi::run("SELECT reflex_rebuild_imv('ps14d_imv')").unwrap();

    let count = Spi::get_one::<i64>(
        "SELECT rebuild_count FROM reflex_ivm_status() WHERE name = 'ps14d_imv'",
    )
    .expect("status rebuild_count query failed")
    .expect("status row missing");
    assert_eq!(count, 1, "reflex_ivm_status must surface rebuild_count");

    let stamped = Spi::get_one::<bool>(
        "SELECT last_rebuild_at IS NOT NULL FROM reflex_ivm_status() WHERE name = 'ps14d_imv'",
    )
    .expect("status last_rebuild_at query failed")
    .expect("status row missing");
    assert!(stamped, "reflex_ivm_status must surface last_rebuild_at");
}

/// (5) A matview-source IMV (PS-3 shape, requires_explicit_refresh=TRUE) yields
/// an advisory naming the convergent primitive for that shape
/// (refresh_imv_depending_on). Rebuild still succeeds (additive WARN, not error).
#[pg_test]
fn ps14_matview_source_imv_advisory_names_convergent_primitive() {
    Spi::run("CREATE TABLE ps14e_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps14e_src VALUES (1,'a',10),(2,'b',20)").unwrap();
    Spi::run("CREATE MATERIALIZED VIEW ps14e_mv AS SELECT id, grp, val FROM ps14e_src").unwrap();
    crate::create_reflex_ivm(
        "ps14e_imv",
        "SELECT grp, SUM(val) AS total FROM ps14e_mv GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    let advisory = crate::reconcile::rebuild_convergence_advisory("ps14e_imv")
        .expect("a matview-only IMV must produce a convergence advisory");
    assert!(
        advisory.contains("refresh_imv_depending_on"),
        "advisory must name refresh_imv_depending_on for a matview-fed IMV, got: {advisory}"
    );

    // Additive: the rebuild still succeeds and the count still moves.
    let r = Spi::get_one::<String>("SELECT reflex_rebuild_imv('ps14e_imv')")
        .unwrap()
        .unwrap();
    assert_eq!(r, "RECONCILED", "the WARN is additive — rebuild still succeeds");
    assert_eq!(ps14_rebuild_count("ps14e_imv"), 1);
}

/// (6) The field shape: a partitioned IMV declaring ignore_sources yields an
/// advisory naming reflex_reconcile_partition and reflex_rebuild_chain — the
/// primitives that CAN refill an anchor-empty partition. Rebuild still succeeds.
#[pg_test]
fn ps14_partitioned_ignore_sources_imv_advisory() {
    Spi::run(
        "CREATE TABLE ps14f_anchor (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .unwrap();
    Spi::run("CREATE TABLE ps14f_anchor_n PARTITION OF ps14f_anchor FOR VALUES IN ('N')").unwrap();
    Spi::run("CREATE TABLE ps14f_anchor_s PARTITION OF ps14f_anchor FOR VALUES IN ('S')").unwrap();
    Spi::run("INSERT INTO ps14f_anchor VALUES (1,'N',100),(2,'S',50)").unwrap();
    Spi::run("CREATE TABLE ps14f_auth (region TEXT, note TEXT)").unwrap();
    Spi::run("INSERT INTO ps14f_auth VALUES ('N','x'),('S','y')").unwrap();

    let create = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'ps14f_imv', \
            'SELECT a.id, a.region, a.amount, x.note FROM ps14f_anchor a \
                LEFT JOIN ps14f_auth x ON a.region = x.region', \
            'id,region', NULL, NULL, 'ps14f_auth', ARRAY['region'] \
         )",
    )
    .unwrap()
    .unwrap();
    assert!(!create.starts_with("ERROR"), "create returned: {create}");

    let advisory = crate::reconcile::rebuild_convergence_advisory("ps14f_imv")
        .expect("a partitioned ignore_sources IMV must produce a convergence advisory");
    assert!(
        advisory.contains("reflex_reconcile_partition"),
        "advisory must name reflex_reconcile_partition, got: {advisory}"
    );
    assert!(
        advisory.contains("reflex_rebuild_chain"),
        "advisory must name reflex_rebuild_chain, got: {advisory}"
    );

    let r = Spi::get_one::<String>("SELECT reflex_rebuild_imv('ps14f_imv')")
        .unwrap()
        .unwrap();
    assert!(!r.starts_with("ERROR"), "the WARN is additive — rebuild still succeeds, got: {r}");
    assert_eq!(ps14_rebuild_count("ps14f_imv"), 1);
}

/// (7) An ordinary table-backed IMV produces NO advisory (no false warning) and
/// still reconciles normally — the anti-regression pin.
#[pg_test]
fn ps14_ordinary_imv_no_advisory_and_still_reconciles() {
    Spi::run("CREATE TABLE ps14g_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps14g_src VALUES (1,'a',10)").unwrap();
    crate::create_reflex_ivm(
        "ps14g_imv",
        "SELECT grp, SUM(val) AS total FROM ps14g_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    assert!(
        crate::reconcile::rebuild_convergence_advisory("ps14g_imv").is_none(),
        "an ordinary maintainable IMV must NOT produce a convergence advisory"
    );
    let r = Spi::get_one::<String>("SELECT reflex_rebuild_imv('ps14g_imv')")
        .unwrap()
        .unwrap();
    assert_eq!(r, "RECONCILED");
}

/// (8) Anti-false-positive: a partitioned IMV WITHOUT ignore_sources converges
/// via a plain anchor-scoped rebuild (every partition has an anchor), so it must
/// NOT warn.
#[pg_test]
fn ps14_partitioned_without_ignore_sources_no_advisory() {
    Spi::run(
        "CREATE TABLE ps14h_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .unwrap();
    Spi::run("CREATE TABLE ps14h_src_n PARTITION OF ps14h_src FOR VALUES IN ('N')").unwrap();
    Spi::run("CREATE TABLE ps14h_src_s PARTITION OF ps14h_src FOR VALUES IN ('S')").unwrap();
    Spi::run("INSERT INTO ps14h_src VALUES (1,'N',100),(2,'S',50)").unwrap();

    let create = Spi::get_one::<String>(
        "SELECT create_reflex_ivm( \
            'ps14h_imv', \
            'SELECT region, SUM(amount) AS total FROM ps14h_src GROUP BY region', \
            NULL, NULL, NULL, NULL, ARRAY['region'] \
         )",
    )
    .unwrap()
    .unwrap();
    assert!(!create.starts_with("ERROR"), "create returned: {create}");

    assert!(
        crate::reconcile::rebuild_convergence_advisory("ps14h_imv").is_none(),
        "a partitioned IMV without ignore_sources refills every partition from its \
         anchor and must NOT warn"
    );
}
