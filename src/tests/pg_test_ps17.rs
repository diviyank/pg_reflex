// PS-17 — repair primitive + audit check for a materialised UNION-ALL
// wrapper's per-operand `__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>`
// triggers. PS-10 made `trigger-attached` skip decomposed-wrapper rows
// entirely (both VIEW and materialised wrappers), which removed a false
// positive but also left the materialised wrapper's real trigger set
// unchecked and unrepairable — see
// untreated_bugs/2026-07-24_union_mirror_triggers_unchecked.md.

fn ps17_count(sql: &str) -> i64 {
    Spi::get_one::<i64>(sql)
        .expect("count query failed")
        .expect("NULL count")
}

fn ps17_audit(target: &str) -> String {
    Spi::get_one::<String>(&format!("SELECT reflex_audit('{}')", target))
        .expect("reflex_audit errored")
        .expect("NULL audit report")
}

fn ps17_findings(report: &str, category: &str) -> Vec<String> {
    report
        .split("\n\n")
        .filter(|block| {
            let header = block.lines().next().unwrap_or("");
            header.starts_with('[') && header.contains(category)
        })
        .map(|s| s.to_string())
        .collect()
}

/// A materialised UNION-ALL wrapper (CTE body consumed by an aggregate, so the
/// wrapper is a TABLE, not a VIEW): two operand tables, 3 mirror triggers each.
fn ps17_make_materialized_wrapper_fixture() {
    Spi::run("CREATE TABLE ps17_mw_a (id BIGINT, v NUMERIC)").expect("a");
    Spi::run("CREATE TABLE ps17_mw_b (id BIGINT, v NUMERIC)").expect("b");
    Spi::run("INSERT INTO ps17_mw_a VALUES (1, 10)").expect("seed a");
    Spi::run("INSERT INTO ps17_mw_b VALUES (2, 20)").expect("seed b");
    Spi::run(
        "SELECT create_reflex_ivm('ps17_mw_top', \
           'WITH u AS (SELECT id, v FROM ps17_mw_a UNION ALL SELECT id, v FROM ps17_mw_b) \
            SELECT id, SUM(v) AS total FROM u GROUP BY id', 'id')",
    )
    .expect("create CTE-over-UNION-ALL IMV");

    let is_table = ps17_count(
        "SELECT count(*) FROM pg_class WHERE relname = 'ps17_mw_top__cte_u' AND relkind = 'r'",
    );
    assert_eq!(
        is_table, 1,
        "fixture precondition: the consumed wrapper must be materialised as a TABLE"
    );
    let mirror_triggers = ps17_count(
        "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal \
         AND tgname LIKE '\\_\\_reflex\\_union\\_mirror\\_%ps17\\_mw\\_top\\_\\_cte\\_u%'",
    );
    assert_eq!(
        mirror_triggers, 6,
        "fixture precondition: 3 DML kinds x 2 operands"
    );
}

/// (1) Functional: dropping a mirror trigger silently breaks propagation from
/// that operand; `reflex_rebuild_union_mirror` restores it — proven by an
/// actual INSERT reaching the wrapper, not just by trigger existence.
#[pg_test]
fn ps17_rebuild_union_mirror_reinstalls_dropped_trigger() {
    ps17_make_materialized_wrapper_fixture();

    // The mirror trigger lives on the operand's own SUB-IMV target table
    // (`ps17_mw_top__cte_u__union_0`, itself an IMV over ps17_mw_a) — not on
    // the raw base table. A base-table INSERT first flows through that
    // sub-IMV's own incremental maintenance, which is what actually fires
    // the mirror trigger.
    Spi::run(
        "DROP TRIGGER __reflex_union_mirror_ins_ps17_mw_top__cte_u_0 \
         ON ps17_mw_top__cte_u__union_0",
    )
    .expect("drop mirror ins trigger");

    Spi::run("INSERT INTO ps17_mw_a VALUES (3, 30)").expect("insert while trigger missing");
    let wrapper_rows_before =
        ps17_count("SELECT count(*) FROM ps17_mw_top__cte_u WHERE __reflex_src_idx = 0");
    assert_eq!(
        wrapper_rows_before, 1,
        "precondition: with the mirror trigger dropped, the new operand-0 row \
         must NOT have propagated to the wrapper"
    );

    let result = Spi::get_one::<String>("SELECT reflex_rebuild_union_mirror('ps17_mw_top__cte_u')")
        .expect("reflex_rebuild_union_mirror errored")
        .expect("NULL result");
    assert!(
        !result.starts_with("ERROR"),
        "repair on a genuinely broken materialised wrapper must succeed: {result}"
    );

    let restored = ps17_count(
        "SELECT count(*) FROM pg_trigger \
         WHERE tgname = '__reflex_union_mirror_ins_ps17_mw_top__cte_u_0' \
           AND NOT tgisinternal",
    );
    assert_eq!(restored, 1, "the ins mirror trigger must be reinstalled");

    Spi::run("INSERT INTO ps17_mw_a VALUES (4, 40)").expect("insert after repair");
    let wrapper_rows_after =
        ps17_count("SELECT count(*) FROM ps17_mw_top__cte_u WHERE __reflex_src_idx = 0");
    assert_eq!(
        wrapper_rows_after, 2,
        "after repair, a NEW operand-0 insert must propagate to the wrapper"
    );
}

/// (2) `trigger-attached` must now catch a missing mirror trigger instead of
/// silently skipping every decomposed-wrapper row.
#[pg_test]
fn ps17_trigger_attached_flags_missing_mirror_trigger() {
    ps17_make_materialized_wrapper_fixture();

    let clean_report = ps17_audit("ps17_mw_top__cte_u");
    assert!(
        ps17_findings(&clean_report, "trigger-attached").is_empty(),
        "a healthy materialised wrapper must audit clean:\n{clean_report}"
    );

    Spi::run(
        "DROP TRIGGER __reflex_union_mirror_del_ps17_mw_top__cte_u_1 \
         ON ps17_mw_top__cte_u__union_1",
    )
    .expect("drop mirror del trigger on operand 1");

    let report = ps17_audit("ps17_mw_top__cte_u");
    let findings = ps17_findings(&report, "trigger-attached");
    assert_eq!(
        findings.len(),
        1,
        "the dropped mirror trigger on operand 1 must be reported:\n{report}"
    );
    assert!(
        findings[0].contains("__reflex_union_mirror_del_ps17_mw_top__cte_u_1"),
        "the finding must name the missing trigger:\n{}",
        findings[0]
    );
    assert!(
        findings[0].contains("reflex_rebuild_union_mirror('ps17_mw_top__cte_u')"),
        "the fix must name the new repair primitive, not reflex_rebuild_triggers:\n{}",
        findings[0]
    );

    // Round-trip: the printed remedy must actually clear the finding.
    Spi::run("SELECT reflex_rebuild_union_mirror('ps17_mw_top__cte_u')").expect("repair");
    let healed_report = ps17_audit("ps17_mw_top__cte_u");
    assert!(
        ps17_findings(&healed_report, "trigger-attached").is_empty(),
        "the printed remedy must converge:\n{healed_report}"
    );
}

/// (3) A VIEW wrapper (top-level UNION ALL, no aggregate consumer) has no
/// operand triggers by design; the check must stay silent and the repair
/// primitive must refuse cleanly rather than raise.
#[pg_test]
fn ps17_view_wrapper_unaffected() {
    Spi::run("CREATE TABLE ps17_vw_a (id BIGINT, v NUMERIC)").expect("a");
    Spi::run("CREATE TABLE ps17_vw_b (id BIGINT, v NUMERIC)").expect("b");
    Spi::run("INSERT INTO ps17_vw_a VALUES (1, 10)").expect("seed a");
    Spi::run("INSERT INTO ps17_vw_b VALUES (2, 20)").expect("seed b");
    Spi::run(
        "SELECT create_reflex_ivm('ps17_vw_top', \
           'SELECT id, v FROM ps17_vw_a UNION ALL SELECT id, v FROM ps17_vw_b', 'id')",
    )
    .expect("create UNION ALL IMV");
    let is_view =
        ps17_count("SELECT count(*) FROM pg_class WHERE relname = 'ps17_vw_top' AND relkind = 'v'");
    assert_eq!(is_view, 1, "fixture precondition: top-level wrapper is a VIEW");

    let report = ps17_audit("ps17_vw_top");
    assert!(
        ps17_findings(&report, "trigger-attached").is_empty(),
        "a VIEW wrapper must still audit clean:\n{report}"
    );

    let result = Spi::get_one::<String>("SELECT reflex_rebuild_union_mirror('ps17_vw_top')")
        .expect("call errored")
        .expect("NULL result");
    assert!(
        result.starts_with("ERROR"),
        "must refuse cleanly on a VIEW wrapper, not attempt repair: {result}"
    );
}

/// (4) A normal (non-wrapper) IMV must be refused cleanly, not raise or
/// silently do nothing useful.
#[pg_test]
fn ps17_rebuild_union_mirror_rejects_non_wrapper() {
    Spi::run("CREATE TABLE ps17_plain_src (id BIGINT, v NUMERIC)").expect("src");
    Spi::run("SELECT create_reflex_ivm('ps17_plain_view', 'SELECT id, v FROM ps17_plain_src', 'id')")
        .expect("create plain IMV");

    let result = Spi::get_one::<String>("SELECT reflex_rebuild_union_mirror('ps17_plain_view')")
        .expect("call errored")
        .expect("NULL result");
    assert!(
        result.starts_with("ERROR"),
        "must refuse cleanly on a non-wrapper IMV: {result}"
    );
}

/// (5) Unknown name: clean error, not a panic/raise.
#[pg_test]
fn ps17_rebuild_union_mirror_rejects_unknown_name() {
    let result = Spi::get_one::<String>("SELECT reflex_rebuild_union_mirror('does_not_exist')")
        .expect("call errored")
        .expect("NULL result");
    assert!(
        result.starts_with("ERROR"),
        "must refuse cleanly on an unknown name: {result}"
    );
}
