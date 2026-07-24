// PS-9 — F3 `partition-mirror` phantom intermediate-child drift.
//
// A partitioned PASSTHROUGH IMV has no intermediate table at all
// (`intermediate_column_spec` returns None, so `__reflex_intermediate_<view>` is
// never created). `reflex_sync_partitions` knows that and gates every
// intermediate-child DDL on a `to_regclass` existence probe. The
// `partition-mirror` audit check did not: it diffed the anchor's expected
// intermediate-child set against the empty child list of an absent parent and
// reported every one of them missing, prescribing a sync that structurally
// cannot create them. Field symptom: 42 IMVs across 5 tenants stuck on
// `Intermediate is missing child partitions: … (17 tables)` with the printed
// remedy returning `sync: +0 intermediate, +675 target` forever.
//
// The check is NARROWED, not disabled. The intermediate half now runs only when an
// intermediate is both EXPECTED (`end_query` non-empty) and PRESENT:
//
// - expected + present  → unchanged; a genuinely missing intermediate child on an
//   aggregate IMV is still reported, still with the sync remedy that repairs it.
// - not expected        → silent, whether or not a stray relation of that name
//   exists; sync would only materialise dead partition children.
// - expected + absent   → silent HERE, because no primitive in the extension
//   recreates a dropped intermediate (verified by probe: both `reflex_reconcile`
//   and its alias `reflex_rebuild_imv` return `ERROR: partition reconcile
//   failed`), so any remedy this check printed would be unclearable — the very
//   anti-pattern being deleted. `internal-tables-exist` already reports the
//   absence as an Error, and the target half of this check keeps running.

/// The `Suggested fix:` block of the first `reflex_audit` finding whose header
/// line names `category`. Panics when there is none, so a test can never pass by
/// silently finding nothing.
fn ps9_suggested_fix(report: &str, category: &str) -> String {
    for block in report.split("\n\n") {
        let header = block.lines().next().unwrap_or("");
        if !(header.starts_with('[') && header.contains(category)) {
            continue;
        }
        let mut fix = String::new();
        let mut in_fix = false;
        for line in block.lines() {
            if line.trim() == "Suggested fix:" {
                in_fix = true;
                continue;
            }
            if in_fix && !line.trim().is_empty() {
                fix.push_str(line.trim());
                fix.push(' ');
            }
        }
        let fix = fix.trim().to_string();
        if !fix.is_empty() {
            return fix;
        }
    }
    panic!(
        "no '{}' finding with a suggested fix in report:\n{}",
        category, report
    );
}

fn ps9_count(sql: &str) -> i64 {
    Spi::get_one::<i64>(sql)
        .expect("count query failed")
        .expect("NULL count")
}

fn ps9_audit(target: &str) -> String {
    Spi::get_one::<String>(&format!("SELECT reflex_audit('{}')", target))
        .expect("reflex_audit errored")
        .expect("NULL audit report")
}

/// Build the PS-9 passthrough fixture: a LIST-partitioned source with two
/// children and a real partitioned passthrough IMV over it. Asserts the
/// preconditions that make this the phantom-finding shape — the registry row is
/// passthrough (`end_query = ''`) and no intermediate relation exists — so the
/// test can never be green because the fixture silently drifted into an
/// aggregate shape.
fn ps9_make_passthrough_fixture(src: &str, view: &str) {
    Spi::run(&format!(
        "CREATE TABLE {src} (dem_plan_id BIGINT NOT NULL, product_id BIGINT, qty NUMERIC) \
         PARTITION BY LIST (dem_plan_id)"
    ))
    .expect("create partitioned source");
    Spi::run(&format!(
        "CREATE TABLE {src}_p1 PARTITION OF {src} FOR VALUES IN (1)"
    ))
    .expect("p1");
    Spi::run(&format!(
        "CREATE TABLE {src}_p2 PARTITION OF {src} FOR VALUES IN (2)"
    ))
    .expect("p2");
    Spi::run(&format!(
        "INSERT INTO {src} VALUES (1, 10, 5), (1, 11, 6), (2, 20, 7)"
    ))
    .expect("seed");

    Spi::run(&format!(
        "SELECT create_reflex_ivm( \
            '{view}', \
            'SELECT dem_plan_id, product_id, qty FROM {src}', \
            'dem_plan_id,product_id', NULL, NULL, NULL, \
            ARRAY['dem_plan_id'] \
         )"
    ))
    .expect("create partitioned passthrough IMV");

    let bare_view = view.rsplit('.').next().unwrap();
    let passthrough = Spi::get_one::<bool>(&format!(
        "SELECT end_query = '' AND (aggregations->>'is_passthrough')::bool \
         FROM public.__reflex_ivm_reference WHERE name = '{view}'"
    ))
    .expect("registry query failed")
    .expect("no registry row");
    assert!(
        passthrough,
        "fixture precondition: {view} must be a passthrough IMV \
         (empty end_query AND aggregations.is_passthrough)"
    );

    let int_relations = ps9_count(&format!(
        "SELECT count(*) FROM pg_class WHERE relname LIKE '__reflex_intermediate_{bare_view}%'"
    ));
    assert_eq!(
        int_relations, 0,
        "fixture precondition: a passthrough IMV must have no intermediate relation"
    );

    let tgt_children = ps9_count(&format!(
        "SELECT count(*) FROM pg_inherits WHERE inhparent = '{view}'::regclass"
    ));
    assert_eq!(
        tgt_children, 2,
        "fixture precondition: the target must mirror both source partitions"
    );
}

/// Build a partitioned AGGREGATE fixture — the shape that really does own an
/// intermediate table, so the narrowed check must keep reporting its drift.
fn ps9_make_aggregate_fixture(src: &str, view: &str) {
    Spi::run(&format!(
        "CREATE TABLE {src} (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)"
    ))
    .expect("create partitioned source");
    Spi::run(&format!(
        "CREATE TABLE {src}_us PARTITION OF {src} FOR VALUES IN ('us')"
    ))
    .expect("us");
    Spi::run(&format!(
        "CREATE TABLE {src}_eu PARTITION OF {src} FOR VALUES IN ('eu')"
    ))
    .expect("eu");
    Spi::run(&format!(
        "INSERT INTO {src} VALUES (1, 'us', 100), (2, 'eu', 200)"
    ))
    .expect("seed");
    Spi::run(&format!(
        "SELECT create_reflex_ivm( \
            '{view}', \
            'SELECT region, SUM(amount) AS total FROM {src} GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )"
    ))
    .expect("create partitioned aggregate IMV");

    let bare_view = view.rsplit('.').next().unwrap();
    let int_children = ps9_count(&format!(
        "SELECT count(*) FROM pg_inherits \
         WHERE inhparent = '__reflex_intermediate_{bare_view}'::regclass"
    ));
    assert_eq!(
        int_children, 2,
        "fixture precondition: the aggregate IMV's intermediate must mirror both \
         source partitions"
    );
}

/// (1) THE BUG. A freshly created, cleanly synced partitioned passthrough IMV
/// must produce ZERO `partition-mirror` findings. Before the fix the check
/// reported every expected intermediate child as missing.
#[pg_test]
fn ps9_passthrough_partitioned_imv_reports_no_phantom_intermediate_drift() {
    ps9_make_passthrough_fixture("ps9_pt_src", "ps9_pt_view");

    // The remedy the check would print is already a clean no-op here: there is
    // nothing for it to repair, which is precisely why the finding was
    // unclearable in the field.
    let sync = Spi::get_one::<String>("SELECT reflex_sync_partitions('ps9_pt_view')")
        .expect("sync errored")
        .expect("NULL sync result");
    assert_eq!(
        sync, "sync: +0 intermediate, +0 target",
        "fixture precondition: sync must have nothing to do, got: {sync}"
    );

    let report = ps9_audit("ps9_pt_view");
    assert!(
        !report.contains("Intermediate is missing child partitions"),
        "phantom intermediate drift reported for a passthrough IMV that has no \
         intermediate table:\n{report}"
    );
    assert!(
        !report.contains("partition-mirror"),
        "the target side is clean, so partition-mirror must return zero findings — \
         not an empty-bodied one:\n{report}"
    );
}

/// (1b) Same fixture, seen through `reflex_doctor` — the tool the field report
/// used. No F3 row may name this IMV.
#[pg_test]
fn ps9_passthrough_partitioned_imv_has_no_f3_doctor_row() {
    ps9_make_passthrough_fixture("ps9_dr_src", "ps9_dr_view");

    let f3_rows = ps9_count(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE check_id = 'F3' AND object = 'ps9_dr_view'",
    );
    assert_eq!(
        f3_rows, 0,
        "reflex_doctor must report no F3 for a clean partitioned passthrough IMV"
    );

    let phantom = ps9_count(
        "SELECT count(*) FROM reflex_doctor() \
         WHERE finding LIKE '%Intermediate is missing child partitions%'",
    );
    assert_eq!(phantom, 0, "no phantom intermediate finding may reach F3");
}

/// (2) ANTI-REGRESSION. An aggregate IMV whose intermediate genuinely lost a
/// child partition is still reported, still with the sync remedy. The check was
/// narrowed, not disabled.
#[pg_test]
fn ps9_aggregate_imv_missing_intermediate_child_is_still_reported() {
    ps9_make_aggregate_fixture("ps9_ag_src", "ps9_ag_view");

    let victim = Spi::get_one::<String>(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = '__reflex_intermediate_ps9_ag_view'::regclass \
         ORDER BY c.relname LIMIT 1",
    )
    .expect("child lookup failed")
    .expect("no intermediate child");
    Spi::run(&format!("DROP TABLE {victim}")).expect("drop one intermediate child");

    let report = ps9_audit("ps9_ag_view");
    assert!(
        report.contains("Intermediate is missing child partitions")
            && report.contains(&victim),
        "a genuinely missing intermediate child must still be reported by name:\n{report}"
    );
    let fix = ps9_suggested_fix(&report, "partition-mirror");
    assert!(
        fix.contains("reflex_sync_partitions"),
        "missing intermediate CHILDREN are repairable by sync; got fix: {fix}"
    );
}

/// (3) An aggregate IMV whose intermediate PARENT is gone entirely. Mirror drift
/// is not the right vocabulary for a relation that does not exist, and — verified
/// by probe — no primitive in the extension recreates one: `reflex_reconcile` on a
/// partitioned IMV takes the swap path and returns `ERROR: partition reconcile
/// failed`, and `reflex_rebuild_imv` is a literal alias for it (`src/lib.rs:823`).
/// So `partition-mirror` must NOT invent a remedy it cannot honour; absence stays
/// the business of `internal-tables-exist`, which reports it as an Error — the
/// division of labour `IntermediateShape` documents ("internal-tables-exist covers
/// absence"). Asserting BOTH halves is what keeps this from being a silencing
/// test: the misleading text must go AND the true Error must remain.
#[pg_test]
fn ps9_aggregate_imv_missing_intermediate_parent_is_left_to_internal_tables_exist() {
    ps9_make_aggregate_fixture("ps9_mp_src", "ps9_mp_view");

    Spi::run("DROP TABLE __reflex_intermediate_ps9_mp_view CASCADE")
        .expect("drop the whole intermediate tree");

    let report = ps9_audit("ps9_mp_view");
    assert!(
        !report.contains("Intermediate is missing child partitions"),
        "an absent intermediate parent must not be described as missing children:\n{report}"
    );
    assert!(
        !report.contains("partition-mirror"),
        "partition-mirror must stay silent on absence — it has no remedy that \
         works, and inventing one recreates the unclearable-finding bug this \
         branch exists to delete:\n{report}"
    );
    assert!(
        report.contains("internal-tables-exist")
            && report.contains("__reflex_intermediate_ps9_mp_view"),
        "the absence itself must remain visible via internal-tables-exist, naming \
         the relation:\n{report}"
    );
    let fix = ps9_suggested_fix(&report, "internal-tables-exist");
    assert!(
        fix.contains("reflex_rebuild_imv"),
        "internal-tables-exist owns the remedy for absence; got fix: {fix}"
    );
}

/// (3b) The target half must keep working when the intermediate parent is absent.
/// Deleting the early return that the old Case-2 finding needed is only safe if
/// real target drift is still reported in that state — otherwise silencing the
/// intermediate half would have blinded the check to the target too.
#[pg_test]
fn ps9_absent_intermediate_does_not_blind_the_target_half() {
    ps9_make_aggregate_fixture("ps9_bt_src", "ps9_bt_view");

    Spi::run("DROP TABLE __reflex_intermediate_ps9_bt_view CASCADE")
        .expect("drop the whole intermediate tree");
    let victim = Spi::get_one::<String>(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = 'ps9_bt_view'::regclass ORDER BY c.relname LIMIT 1",
    )
    .expect("target child lookup failed")
    .expect("no target child");
    Spi::run(&format!("DROP TABLE {victim}")).expect("drop one target child");

    let report = ps9_audit("ps9_bt_view");
    assert!(
        report.contains("Target is missing child partitions") && report.contains(&victim),
        "target drift must still be reported while the intermediate parent is \
         absent:\n{report}"
    );
    assert!(
        !report.contains("Intermediate is missing child partitions"),
        "the intermediate half must stay silent on absence:\n{report}"
    );
    let fix = ps9_suggested_fix(&report, "partition-mirror");
    assert!(
        fix.contains("reflex_sync_partitions"),
        "target-only drift is repairable by sync; got fix: {fix}"
    );
}

/// (4) REMEDY CONVERGENCE — the property B9 is really about. On the passthrough
/// fixture, drift the TARGET tree, confirm the check reports it, run the exact
/// `suggested_fix` string it printed, and confirm the finding is gone.
#[pg_test]
fn ps9_partition_mirror_suggested_fix_clears_its_own_finding() {
    ps9_make_passthrough_fixture("ps9_rc_src", "ps9_rc_view");

    let victim = Spi::get_one::<String>(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = 'ps9_rc_view'::regclass ORDER BY c.relname LIMIT 1",
    )
    .expect("target child lookup failed")
    .expect("no target child");
    Spi::run(&format!("DROP TABLE {victim}")).expect("drop one target child");

    let report = ps9_audit("ps9_rc_view");
    assert!(
        report.contains("Target is missing child partitions") && report.contains(&victim),
        "target drift must be reported by name:\n{report}"
    );
    assert!(
        !report.contains("Intermediate is missing child partitions"),
        "the intermediate half must stay silent for a passthrough IMV even when \
         the target half has real drift:\n{report}"
    );

    let fix = ps9_suggested_fix(&report, "partition-mirror");
    Spi::run(&fix).unwrap_or_else(|e| panic!("printed remedy `{fix}` failed: {e}"));

    let after = ps9_audit("ps9_rc_view");
    assert!(
        !after.contains("partition-mirror"),
        "the printed remedy `{fix}` did not clear its own finding:\n{after}"
    );
}

/// (5) SCHEMA-QUALIFIED, NARROW `search_path`. The 2026-07-05 doctor-residue bug
/// was a search-path bug in this exact area, so the passthrough guard must hold
/// for an IMV whose schema is not on the path.
#[pg_test]
fn ps9_passthrough_partitioned_imv_in_custom_schema_under_narrow_search_path() {
    Spi::run("CREATE SCHEMA ps9np").expect("schema");
    ps9_make_passthrough_fixture("ps9np.src", "ps9np.pview");

    Spi::run("SET search_path = pg_catalog").expect("narrow search_path");
    let report = Spi::get_one::<String>("SELECT public.reflex_audit('ps9np.pview')")
        .expect("audit must not error under a narrow search_path")
        .expect("NULL audit report");
    Spi::run("SET search_path = public").expect("reset search_path");

    assert!(
        !report.contains("Intermediate is missing child partitions"),
        "phantom intermediate drift reported for a schema-qualified passthrough \
         IMV under a narrow search_path:\n{report}"
    );
    assert!(
        !report.contains("partition-mirror"),
        "schema-qualified passthrough IMV must yield zero partition-mirror \
         findings:\n{report}"
    );
}

/// (6) HARDENING — intermediate NOT expected but a relation with that name
/// nevertheless PRESENT (residue of an earlier definition of the same IMV name).
/// Gating on presence alone re-opens the phantom by a second route: the stray
/// parent has no children, so every anchor child is "missing" again, and the
/// prescribed sync would materialise partition children on a table no maintenance
/// path reads or writes.
///
/// The stray relation itself is reported by NO check: `OrphanIntermediate` builds
/// its `expected` set from `intermediate_table_name` for every enabled IMV
/// including passthroughs, so the stray counts as owned. That is accepted — the
/// relation is inert (no maintenance path reads or writes it, and it holds no
/// rows), and the alternative would arm a `DROP TABLE … CASCADE` remedy keyed on a
/// passthrough classification inside the check the field just caught
/// false-positiving. Recorded, not fixed.
#[pg_test]
fn ps9_stray_intermediate_on_passthrough_imv_is_not_reported_as_missing_children() {
    ps9_make_passthrough_fixture("ps9_st_src", "ps9_st_view");

    // A real, childless, partitioned relation occupying the intermediate name.
    Spi::run(
        "CREATE TABLE __reflex_intermediate_ps9_st_view \
           (dem_plan_id BIGINT NOT NULL, product_id BIGINT, qty NUMERIC) \
         PARTITION BY LIST (dem_plan_id)",
    )
    .expect("create stray intermediate");

    let report = ps9_audit("ps9_st_view");
    assert!(
        !report.contains("Intermediate is missing child partitions"),
        "a stray intermediate on a passthrough IMV must not be reported as missing \
         child partitions — nothing reads or writes it:\n{report}"
    );
    assert!(
        !report.contains("partition-mirror"),
        "the target side is clean, so partition-mirror must stay silent:\n{report}"
    );
}
