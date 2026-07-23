// PS-3 — unmaintainable-source visibility.
//
// An IMV whose only real sources are materialized views cannot self-maintain
// (PG fires no trigger on a matview): it is a snapshot frozen at create time.
// `requires_explicit_refresh` records that structurally and durably, so
// `reflex_ivm_status` and `reflex_doctor` can see it. The flag is PERMANENT and
// kept distinct from `known_stale` (PS-4's `verify_stale_cleared` authority),
// so a by-design-unmaintainable node never becomes a permanent `failed:` alarm.

fn ps3_requires_explicit_refresh(name: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT requires_explicit_refresh FROM public.__reflex_ivm_reference WHERE name = '{}'",
        name
    ))
    .expect("registry query failed")
    .expect("no registry row / NULL flag")
}

/// (1) A matview-only IMV is flagged at create time, BEFORE any mutation, and
/// the flag is surfaced by reflex_ivm_status. known_stale stays FALSE — the
/// node is fresh at create, not a failed flush.
#[pg_test]
fn ps3_matview_only_imv_flagged_at_create_time() {
    Spi::run("CREATE TABLE ps3a_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps3a_src VALUES (1,'a',10),(2,'a',20),(3,'b',30)").unwrap();
    Spi::run("CREATE MATERIALIZED VIEW ps3a_mv AS SELECT id, grp, val FROM ps3a_src").unwrap();

    let r = crate::create_reflex_ivm(
        "ps3a_imv",
        "SELECT grp, SUM(val) AS total FROM ps3a_mv GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    assert!(
        ps3_requires_explicit_refresh("ps3a_imv"),
        "a matview-only IMV must be flagged at create time"
    );

    let via_status = Spi::get_one::<bool>(
        "SELECT requires_explicit_refresh FROM reflex_ivm_status() WHERE name = 'ps3a_imv'",
    )
    .expect("status query failed")
    .expect("status row missing");
    assert!(
        via_status,
        "reflex_ivm_status must surface requires_explicit_refresh"
    );

    let known_stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'ps3a_imv'",
    )
    .unwrap()
    .unwrap();
    assert!(
        !known_stale,
        "known_stale must stay FALSE — a fresh snapshot is not a failed flush"
    );
}

/// (2) The incident shape: a generated CTE child whose source is a matview is
/// flagged; the parent (which reads the child, a triggerable sub-IMV table) is
/// NOT flagged. Classification is by the graph, not names.
#[pg_test]
fn ps3_generated_child_reading_matview_is_flagged() {
    Spi::run("CREATE TABLE ps3b_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps3b_src VALUES (1,'a',10),(2,'a',20),(3,'b',30)").unwrap();
    Spi::run("CREATE MATERIALIZED VIEW ps3b_mv AS SELECT id, grp, val FROM ps3b_src").unwrap();

    let r = crate::create_reflex_ivm(
        "ps3b_agg",
        "WITH base AS (SELECT id, grp, val FROM ps3b_mv) \
         SELECT grp, SUM(val) AS total FROM base GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    assert!(
        ps3_requires_explicit_refresh("ps3b_agg__cte_base"),
        "the generated child reading a matview must be flagged"
    );
    assert!(
        !ps3_requires_explicit_refresh("ps3b_agg"),
        "the parent reads a triggerable sub-IMV table, so it must NOT be flagged"
    );
}

/// (3) A mixed-source IMV (one matview + one real table) is maintainable via the
/// table, so it must NOT be flagged.
#[pg_test]
fn ps3_mixed_source_imv_not_flagged() {
    Spi::run("CREATE TABLE ps3c_tbl (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps3c_tbl VALUES (1,'a',10),(2,'b',20)").unwrap();
    Spi::run("CREATE TABLE ps3c_src2 (id INT PRIMARY KEY, note TEXT)").unwrap();
    Spi::run("INSERT INTO ps3c_src2 VALUES (1,'x'),(2,'y')").unwrap();
    Spi::run("CREATE MATERIALIZED VIEW ps3c_mv AS SELECT id, note FROM ps3c_src2").unwrap();

    let r = crate::create_reflex_ivm(
        "ps3c_imv",
        "SELECT t.id, t.grp, t.val, m.note FROM ps3c_tbl t JOIN ps3c_mv m ON t.id = m.id",
        Some("id"),
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    assert!(
        !ps3_requires_explicit_refresh("ps3c_imv"),
        "a mixed-source IMV is maintainable via its real table and must NOT be flagged"
    );
}

/// (4) The flag survives reflex_doctor(fix=>TRUE): the F12 finding is not a
/// failure, the flag stays TRUE, known_stale is never set, and the node stays
/// visible on a subsequent run. This is the anti-false-alarm property.
#[pg_test]
fn ps3_flag_survives_doctor_fix_without_false_alarm() {
    Spi::run("CREATE TABLE ps3d_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps3d_src VALUES (1,'a',10),(2,'b',20)").unwrap();
    Spi::run("CREATE MATERIALIZED VIEW ps3d_mv AS SELECT id, grp, val FROM ps3d_src").unwrap();
    crate::create_reflex_ivm(
        "ps3d_imv",
        "SELECT grp, SUM(val) AS total FROM ps3d_mv GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert!(ps3_requires_explicit_refresh("ps3d_imv"));

    let (check_id, outcome): (String, String) = Spi::connect(|client| {
        let mut out = (String::new(), String::new());
        let rs = client
            .select(
                "SELECT check_id, outcome FROM reflex_doctor(NULL, TRUE) \
                 WHERE check_id = 'F12' AND object = 'ps3d_imv'",
                None,
                &[],
            )
            .unwrap_or_report();
        for row in rs {
            out.0 = row
                .get_by_name::<&str, _>("check_id")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            out.1 = row
                .get_by_name::<&str, _>("outcome")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
        }
        out
    });
    assert_eq!(check_id, "F12", "matview-only node must surface as an F12 finding");
    assert!(
        !outcome.starts_with("failed"),
        "F12 fix outcome must not be a failure (got '{}')",
        outcome
    );

    assert!(
        ps3_requires_explicit_refresh("ps3d_imv"),
        "requires_explicit_refresh must survive reflex_doctor(fix=>TRUE)"
    );
    let known_stale = Spi::get_one::<bool>(
        "SELECT known_stale FROM public.__reflex_ivm_reference WHERE name = 'ps3d_imv'",
    )
    .unwrap()
    .unwrap();
    assert!(
        !known_stale,
        "the F12 path must never set/leave known_stale (no verify_stale_cleared collision)"
    );

    let still_seen = Spi::get_one::<i64>(
        "SELECT count(*) FROM reflex_doctor() WHERE check_id = 'F12' AND object = 'ps3d_imv'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        still_seen, 1,
        "the node stays visible to reflex_doctor after a fix run"
    );
}

/// (5) The remedy refreshes the frozen snapshot to match the current matview,
/// while the structural flag PERSISTS (re-marked, not cleared) — clearing it
/// would recreate the invisibility bug on the next matview refresh.
#[pg_test]
fn ps3_remedy_refreshes_data_and_flag_persists() {
    Spi::run("CREATE TABLE ps3e_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps3e_src VALUES (1,'a',10),(2,'a',20)").unwrap();
    Spi::run("CREATE MATERIALIZED VIEW ps3e_mv AS SELECT id, grp, val FROM ps3e_src").unwrap();
    crate::create_reflex_ivm(
        "ps3e_imv",
        "SELECT grp, SUM(val) AS total FROM ps3e_mv GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    let total_at_create =
        Spi::get_one::<i64>("SELECT total FROM ps3e_imv WHERE grp = 'a'").unwrap().unwrap();
    assert_eq!(total_at_create, 30);

    // Mutate the base and refresh the matview: the IMV cannot see it (frozen).
    Spi::run("INSERT INTO ps3e_src VALUES (3,'a',100)").unwrap();
    Spi::run("REFRESH MATERIALIZED VIEW ps3e_mv").unwrap();
    let stale_total =
        Spi::get_one::<i64>("SELECT total FROM ps3e_imv WHERE grp = 'a'").unwrap().unwrap();
    assert_eq!(
        stale_total, 30,
        "the IMV is a frozen snapshot: unchanged after the matview refresh"
    );

    // Run the prescribed remedy.
    let res = Spi::get_one::<String>("SELECT refresh_imv_depending_on('ps3e_mv')")
        .unwrap()
        .unwrap();
    assert!(res.starts_with("REFRESHED"), "the remedy must run (got '{}')", res);

    let fresh_total =
        Spi::get_one::<i64>("SELECT total FROM ps3e_imv WHERE grp = 'a'").unwrap().unwrap();
    assert_eq!(
        fresh_total, 130,
        "the remedy refreshed the snapshot to match the matview"
    );

    assert!(
        ps3_requires_explicit_refresh("ps3e_imv"),
        "requires_explicit_refresh must persist through the remedy (structural, permanent)"
    );
}

/// (6) Regression: no normally-maintainable shape becomes flagged — a plain
/// table-backed aggregate and a CTE-decomposed chain over a table (parent +
/// generated child) all stay unflagged.
#[pg_test]
fn ps3_normal_imv_shapes_are_not_flagged() {
    Spi::run("CREATE TABLE ps3f_src (id INT PRIMARY KEY, grp TEXT, val INT)").unwrap();
    Spi::run("INSERT INTO ps3f_src VALUES (1,'a',10),(2,'b',20)").unwrap();

    crate::create_reflex_ivm(
        "ps3f_agg",
        "SELECT grp, SUM(val) AS total FROM ps3f_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    crate::create_reflex_ivm(
        "ps3f_cte",
        "WITH base AS (SELECT id, grp, val FROM ps3f_src) \
         SELECT grp, SUM(val) AS total FROM base GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    let flagged = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE requires_explicit_refresh = TRUE",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        flagged, 0,
        "no table-backed IMV (including generated children) may be flagged"
    );
}
