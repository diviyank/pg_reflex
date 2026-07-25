// PS-10 — the two `internal-tables-exist` bugs filed under PS-9.
//
// Bug 2: `InternalTablesExist` classified a DECOMPOSED WRAPPER row (set-op,
// DISTINCT ON, window) as an aggregate, because `RegistryRow::decomposed` writes
// `aggregations = '{}'` and `ImvRow::is_passthrough()` `unwrap_or(false)`s on the
// absent key. A wrapper owns NO internal relations at all — probed: a top-level
// UNION-ALL wrapper is a VIEW over its `__union_N` sub-IMVs, each of which owns
// its own passthrough scratch pair, and the wrapper owns neither an intermediate
// nor a scratch pair. So neither existing branch fits it; it gets a third one
// with an empty requirement set. (Flipping the branch to `end_query.is_empty()`,
// as the report suggested, would have swapped the false positive for a new one
// naming `__reflex_pt_new_<wrapper>_<subimv>`.)
//
// Bug 1: no primitive recreated a dropped `__reflex_intermediate_<view>`, yet
// `internal-tables-exist` prescribed `reflex_rebuild_imv` (a literal alias for
// `reflex_reconcile`, `src/lib.rs:823`) at Error severity. `reflex_reconcile` now
// heals the aggregate aux relations before the partition-swap walk, so the
// printed remedy converges.

fn ps10_count(sql: &str) -> i64 {
    Spi::get_one::<i64>(sql)
        .expect("count query failed")
        .expect("NULL count")
}

fn ps10_audit(target: &str) -> String {
    Spi::get_one::<String>(&format!("SELECT reflex_audit('{}')", target))
        .expect("reflex_audit errored")
        .expect("NULL audit report")
}

/// Every finding block in `report` whose header line names `category`. Asserting
/// on the category (not on relation-name substrings) is what makes "zero
/// findings" mean "the check is silent" rather than "the wording changed".
fn ps10_findings(report: &str, category: &str) -> Vec<String> {
    report
        .split("\n\n")
        .filter(|block| {
            let header = block.lines().next().unwrap_or("");
            header.starts_with('[') && header.contains(category)
        })
        .map(|s| s.to_string())
        .collect()
}

/// Rows present in `imv` but not in `query`, plus rows present in `query` but not
/// in `imv`. Zero means the IMV matches its defining query exactly, multiset-wise.
///
/// Every operand is parenthesised: `EXCEPT ALL` and `UNION ALL` share precedence
/// and associate left, so a bare `imv EXCEPT ALL a UNION ALL b` would silently
/// mean `(imv EXCEPT ALL a) UNION ALL b` and report a nonzero diff for a correct
/// IMV.
fn ps10_diff_count(imv_select: &str, query: &str) -> i64 {
    ps10_count(&format!(
        "SELECT count(*) FROM ( \
           (({imv_select}) EXCEPT ALL ({query})) \
           UNION ALL \
           (({query}) EXCEPT ALL ({imv_select})) \
         ) AS d"
    ))
}

/// The exact aux-relation names an IMV named `view` could ever own, per the two
/// `internal-tables-exist` branches. Used to prove a decomposed wrapper owns
/// none of them — a substring scan would not do, because a sub-IMV's scratch
/// pair (`__reflex_pt_new_<view>__union_0_<src>`) contains the wrapper's name.
fn ps10_own_aux_relation_count(view: &str, sources: &[&str]) -> i64 {
    let mut names: Vec<String> = vec![
        format!("__reflex_intermediate_{view}"),
        format!("__reflex_affected_{view}"),
        format!("__reflex_scratch_{view}"),
        format!("__reflex_shrunk_{view}"),
    ];
    for src in sources {
        names.push(format!("__reflex_pt_new_{view}_{src}"));
        names.push(format!("__reflex_pt_old_{view}_{src}"));
    }
    let list = names
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(", ");
    ps10_count(&format!(
        "SELECT count(*) FROM pg_class WHERE relname IN ({list})"
    ))
}

// --- Bug 2 fixtures ------------------------------------------------------

/// A real top-level UNION-ALL IMV over two real tables, with the preconditions
/// that make it the false-positive shape asserted, so this test can never pass
/// because the fixture silently drifted into some other shape.
fn ps10_make_union_all_fixture(a: &str, b: &str, view: &str) {
    Spi::run(&format!("CREATE TABLE {a} (id BIGINT, v NUMERIC)")).expect("a");
    Spi::run(&format!("CREATE TABLE {b} (id BIGINT, v NUMERIC)")).expect("b");
    Spi::run(&format!("INSERT INTO {a} VALUES (1, 10), (2, 20)")).expect("seed a");
    Spi::run(&format!("INSERT INTO {b} VALUES (3, 30)")).expect("seed b");
    Spi::run(&format!(
        "SELECT create_reflex_ivm('{view}', \
           'SELECT id, v FROM {a} UNION ALL SELECT id, v FROM {b}', 'id')"
    ))
    .expect("create UNION ALL IMV");

    let wrapper_shape = Spi::get_one::<bool>(&format!(
        "SELECT end_query = '' \
                AND (SELECT count(*) FROM jsonb_object_keys(aggregations::jsonb)) = 0 \
         FROM public.__reflex_ivm_reference WHERE name = '{view}'"
    ))
    .expect("registry query failed")
    .expect("no registry row");
    assert!(
        wrapper_shape,
        "fixture precondition: {view} must be a DECOMPOSED WRAPPER row \
         (end_query = '' AND aggregations = '{{}}')"
    );

    let subs = ps10_count(&format!(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name LIKE '{view}\\_\\_union\\_%'"
    ));
    assert_eq!(subs, 2, "fixture precondition: two operand sub-IMVs");

    let owned = ps10_own_aux_relation_count(view, &[a, b]);
    assert_eq!(
        owned, 0,
        "fixture precondition: a UNION-ALL wrapper owns no aux relation of \
         EITHER branch — no intermediate/affected, and no per-source scratch pair"
    );

    let diff = ps10_diff_count(
        &format!("SELECT id, v FROM {view}"),
        &format!("SELECT id, v FROM {a} UNION ALL SELECT id, v FROM {b}"),
    );
    assert_eq!(diff, 0, "fixture precondition: the wrapper IMV is correct");
}

/// (1) THE BUG. A clean, freshly created UNION-ALL IMV must yield ZERO
/// `internal-tables-exist` findings.
#[pg_test]
fn ps10_union_all_wrapper_reports_no_internal_tables_finding() {
    ps10_make_union_all_fixture("ps10_ua_a", "ps10_ua_b", "ps10_ua_view");

    let report = ps10_audit("ps10_ua_view");
    let findings = ps10_findings(&report, "internal-tables-exist");
    assert!(
        findings.is_empty(),
        "a healthy UNION-ALL wrapper owns no internal tables, so \
         internal-tables-exist must be silent; got:\n{report}"
    );
}

/// (1b) The same shape reached through DISTINCT ON and window decomposition —
/// both also register a `RegistryRow::decomposed` wrapper (probed: `relkind = 'v'`,
/// `aggregations = '{}'`, zero aux relations), so the third branch must cover
/// them and not just set-ops.
#[pg_test]
fn ps10_distinct_on_and_window_wrappers_report_no_internal_tables_finding() {
    Spi::run("CREATE TABLE ps10_dw_src (grp BIGINT, id BIGINT, v NUMERIC)").expect("src");
    Spi::run("INSERT INTO ps10_dw_src VALUES (1,1,10),(1,2,20),(2,3,30)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_dw_dview', \
           'SELECT DISTINCT ON (grp) grp, id, v FROM ps10_dw_src ORDER BY grp, v DESC', 'grp')",
    )
    .expect("create DISTINCT ON IMV");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_dw_wview', \
           'SELECT grp, id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn \
            FROM ps10_dw_src', 'grp,id')",
    )
    .expect("create window IMV");

    for view in ["ps10_dw_dview", "ps10_dw_wview"] {
        assert_eq!(
            ps10_own_aux_relation_count(view, &["ps10_dw_src"]),
            0,
            "fixture precondition: {view} wrapper owns no aux relation"
        );
        let report = ps10_audit(view);
        assert!(
            ps10_findings(&report, "internal-tables-exist").is_empty(),
            "internal-tables-exist must be silent on the {view} wrapper:\n{report}"
        );
    }
}

/// (2) ANTI-REGRESSION for the narrowing: a real aggregate IMV whose
/// `__reflex_intermediate_<view>` is genuinely gone still produces the
/// `internal-tables-exist` Error, naming the relation.
#[pg_test]
fn ps10_aggregate_missing_intermediate_is_still_reported() {
    Spi::run("CREATE TABLE ps10_mi_src (region TEXT, amount NUMERIC)").expect("src");
    Spi::run("INSERT INTO ps10_mi_src VALUES ('us', 100), ('eu', 200)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_mi_view', \
           'SELECT region, SUM(amount) AS total FROM ps10_mi_src GROUP BY region')",
    )
    .expect("create aggregate IMV");
    Spi::run("DROP TABLE __reflex_intermediate_ps10_mi_view CASCADE").expect("drop intermediate");

    let report = ps10_audit("ps10_mi_view");
    let findings = ps10_findings(&report, "internal-tables-exist");
    assert_eq!(
        findings.len(),
        1,
        "a genuinely missing intermediate must still be reported:\n{report}"
    );
    assert!(
        findings[0].contains("ERROR") && findings[0].contains("__reflex_intermediate_ps10_mi_view"),
        "the finding must stay an Error naming the relation:\n{report}"
    );
}

/// (3) ANTI-REGRESSION for the passthrough branch: a real (non-decomposed)
/// passthrough IMV with a genuinely dropped per-source scratch table still
/// reports, still with the two-half `reflex_rebuild_triggers` + `reflex_reconcile`
/// remedy PS-6 established.
#[pg_test]
fn ps10_passthrough_missing_scratch_is_still_reported() {
    Spi::run("CREATE TABLE ps10_ps_src (id BIGINT, v NUMERIC)").expect("src");
    Spi::run("INSERT INTO ps10_ps_src VALUES (1, 10)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_ps_view', 'SELECT id, v FROM ps10_ps_src', 'id')",
    )
    .expect("create passthrough IMV");
    let is_pt = Spi::get_one::<bool>(
        "SELECT end_query = '' AND (aggregations->>'is_passthrough')::bool \
         FROM public.__reflex_ivm_reference WHERE name = 'ps10_ps_view'",
    )
    .expect("registry query failed")
    .expect("no registry row");
    assert!(
        is_pt,
        "fixture precondition: a real passthrough row (empty end_query AND \
         aggregations.is_passthrough), not a decomposed wrapper"
    );

    Spi::run("DROP TABLE __reflex_pt_new_ps10_ps_view_ps10_ps_src CASCADE")
        .expect("drop scratch");

    let report = ps10_audit("ps10_ps_view");
    let findings = ps10_findings(&report, "internal-tables-exist");
    assert_eq!(
        findings.len(),
        1,
        "a genuinely missing passthrough scratch table must still be reported:\n{report}"
    );
    let fix = ps9_suggested_fix(&report, "internal-tables-exist");
    assert!(
        fix.contains("reflex_rebuild_triggers('ps10_ps_src')")
            && fix.contains("reflex_reconcile('ps10_ps_view')"),
        "the scratch remedy must keep both halves; got fix: {fix}"
    );
}

/// (3b) The divergence the wrapper discriminator is chosen for. A row with a NULL
/// `aggregations` and an empty `end_query` is not a wrapper — it is a row whose
/// shape is UNKNOWN (the column is nullable, and legacy rows predate it), and
/// silencing it would hide a genuinely missing scratch table AND a genuinely
/// missing source trigger. `!is_passthrough()` cannot tell the two apart, because
/// it answers `false` for absent JSON exactly as it does for an aggregate; the key
/// count answers "unknown, not zero" and the checks keep reporting.
///
/// The fixture is a REAL passthrough IMV over a real source with real damage; only
/// the one column that no create path can leave NULL is set to NULL, to reach the
/// legacy state instead of fabricating a row.
#[pg_test]
fn ps10_null_aggregations_row_is_not_treated_as_a_wrapper() {
    Spi::run("CREATE TABLE ps10_na_src (id BIGINT, v NUMERIC)").expect("src");
    Spi::run("INSERT INTO ps10_na_src VALUES (1, 10)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('ps10_na_view', 'SELECT id, v FROM ps10_na_src', 'id')")
        .expect("create passthrough IMV");
    Spi::run("DROP TABLE __reflex_pt_new_ps10_na_view_ps10_na_src CASCADE")
        .expect("drop scratch");
    Spi::run("DROP TRIGGER __reflex_trigger_ins_on_ps10_na_src ON ps10_na_src")
        .expect("drop trigger");
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET aggregations = NULL \
         WHERE name = 'ps10_na_view'",
    )
    .expect("blank the plan");
    let unknown_shape = Spi::get_one::<bool>(
        "SELECT aggregations IS NULL AND end_query = '' \
         FROM public.__reflex_ivm_reference WHERE name = 'ps10_na_view'",
    )
    .expect("registry query failed")
    .expect("no registry row");
    assert!(
        unknown_shape,
        "fixture precondition: NULL aggregations with an empty end_query"
    );

    let report = ps10_audit("ps10_na_view");
    assert!(
        !ps10_findings(&report, "internal-tables-exist").is_empty(),
        "a row of unknown shape must not be silenced — the missing scratch table is \
         still real:\n{report}"
    );
    assert!(
        !ps10_findings(&report, "trigger-attached").is_empty(),
        "nor may the missing source trigger be silenced:\n{report}"
    );
}

/// (4) `trigger-attached` on the same wrapper fixture. Probed: a VIEW wrapper has
/// no triggers anywhere by design (its sub-IMVs maintain themselves and the view
/// reads them), and a materialised UNION-ALL wrapper is maintained by
/// `__reflex_union_mirror_*` triggers — never by the consolidated
/// `__reflex_trigger_*_on_<source>` set this check looks for. Same
/// decomposed-wrapper root cause, so the check skips those rows.
#[pg_test]
fn ps10_decomposed_wrapper_reports_no_trigger_attached_finding() {
    ps10_make_union_all_fixture("ps10_ta_a", "ps10_ta_b", "ps10_ta_view");

    let sub_triggers = ps10_count(
        "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE NOT t.tgisinternal AND c.relname LIKE 'ps10\\_ta\\_view\\_\\_union\\_%'",
    );
    assert_eq!(
        sub_triggers, 0,
        "fixture precondition: a VIEW wrapper's operands carry no triggers at all"
    );

    let report = ps10_audit("ps10_ta_view");
    assert!(
        ps10_findings(&report, "trigger-attached").is_empty(),
        "trigger-attached must be silent on a decomposed wrapper:\n{report}"
    );
}

/// (4b) ANTI-REGRESSION for that skip: a real source whose consolidated trigger
/// was dropped still reports `trigger-attached`.
#[pg_test]
fn ps10_real_source_missing_trigger_is_still_reported() {
    Spi::run("CREATE TABLE ps10_tr_src (region TEXT, amount NUMERIC)").expect("src");
    Spi::run("INSERT INTO ps10_tr_src VALUES ('us', 100)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_tr_view', \
           'SELECT region, SUM(amount) AS total FROM ps10_tr_src GROUP BY region')",
    )
    .expect("create aggregate IMV");
    Spi::run("DROP TRIGGER __reflex_trigger_ins_on_ps10_tr_src ON ps10_tr_src")
        .expect("drop trigger");

    let report = ps10_audit("ps10_tr_view");
    let findings = ps10_findings(&report, "trigger-attached");
    assert_eq!(
        findings.len(),
        1,
        "a genuinely missing source trigger must still be reported:\n{report}"
    );
    assert!(
        findings[0].contains("__reflex_trigger_ins_on_ps10_tr_src"),
        "the finding must name the missing trigger:\n{report}"
    );
}

/// (4c) The materialised UNION-ALL wrapper (CTE body consumed by an aggregate,
/// so the wrapper must be a TABLE). It owns no aux relation either, and its
/// operands carry mirror triggers rather than consolidated ones.
#[pg_test]
fn ps10_materialized_union_wrapper_reports_neither_finding() {
    Spi::run("CREATE TABLE ps10_mw_a (id BIGINT, v NUMERIC)").expect("a");
    Spi::run("CREATE TABLE ps10_mw_b (id BIGINT, v NUMERIC)").expect("b");
    Spi::run("INSERT INTO ps10_mw_a VALUES (1, 10)").expect("seed a");
    Spi::run("INSERT INTO ps10_mw_b VALUES (2, 20)").expect("seed b");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_mw_top', \
           'WITH u AS (SELECT id, v FROM ps10_mw_a UNION ALL SELECT id, v FROM ps10_mw_b) \
            SELECT id, SUM(v) AS total FROM u GROUP BY id', 'id')",
    )
    .expect("create CTE-over-UNION-ALL IMV");

    let wrapper = "ps10_mw_top__cte_u";
    let is_table = ps10_count(&format!(
        "SELECT count(*) FROM pg_class WHERE relname = '{wrapper}' AND relkind = 'r'"
    ));
    assert_eq!(
        is_table, 1,
        "fixture precondition: the consumed wrapper must be materialised as a TABLE"
    );
    let mirror_triggers = ps10_count(
        "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal \
         AND tgname LIKE '\\_\\_reflex\\_union\\_mirror\\_%'",
    );
    assert_eq!(
        mirror_triggers, 6,
        "fixture precondition: the wrapper is maintained by per-operand mirror \
         triggers (3 DML kinds x 2 operands)"
    );
    assert_eq!(
        ps10_own_aux_relation_count(wrapper, &["ps10_mw_top__cte_u__union_0"]),
        0,
        "fixture precondition: a materialised wrapper owns no aux relation either"
    );

    let report = ps10_audit(wrapper);
    assert!(
        ps10_findings(&report, "internal-tables-exist").is_empty()
            && ps10_findings(&report, "trigger-attached").is_empty(),
        "both checks must be silent on a materialised UNION-ALL wrapper:\n{report}"
    );
}

// --- Bug 1: the printed remedy must converge ------------------------------

fn ps10_make_unpartitioned_aggregate(src: &str, view: &str) {
    Spi::run(&format!(
        "CREATE TABLE {src} (id BIGINT, region TEXT NOT NULL, amount NUMERIC)"
    ))
    .expect("create source");
    Spi::run(&format!(
        "INSERT INTO {src} VALUES (1,'us',100),(2,'eu',200),(3,'us',50)"
    ))
    .expect("seed");
    Spi::run(&format!(
        "SELECT create_reflex_ivm('{view}', \
           'SELECT region, SUM(amount) AS total FROM {src} GROUP BY region')"
    ))
    .expect("create aggregate IMV");
}

fn ps10_make_partitioned_aggregate(src: &str, view: &str) {
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
        "INSERT INTO {src} VALUES (1,'us',100),(2,'eu',200),(3,'us',50)"
    ))
    .expect("seed");
    Spi::run(&format!(
        "SELECT create_reflex_ivm('{view}', \
           'SELECT region, SUM(amount) AS total FROM {src} GROUP BY region', \
           NULL, NULL, NULL, NULL, ARRAY['region'])"
    ))
    .expect("create partitioned aggregate IMV");
    let int_children = ps10_count(&format!(
        "SELECT count(*) FROM pg_inherits \
         WHERE inhparent = '__reflex_intermediate_{view}'::regclass"
    ));
    assert_eq!(
        int_children, 2,
        "fixture precondition: the intermediate mirrors both source partitions"
    );
}

/// Run the audit's own printed remedy for `internal-tables-exist`, then assert
/// the finding is gone and the IMV matches its defining query. Returns the fix
/// that was run so the caller can assert on it.
fn ps10_run_printed_remedy(view: &str) -> String {
    let report = ps10_audit(view);
    assert_eq!(
        ps10_findings(&report, "internal-tables-exist").len(),
        1,
        "the missing intermediate must be reported before repair:\n{report}"
    );
    let fix = ps9_suggested_fix(&report, "internal-tables-exist");
    Spi::run(&fix).unwrap_or_else(|e| panic!("the PRINTED remedy `{fix}` failed: {e}"));
    fix
}

/// (5) Bug 1, unpartitioned aggregate: `DROP TABLE __reflex_intermediate_<view>
/// CASCADE` (plus the affected-groups table, which the audit reports in the same
/// finding), then run the exact string the check printed.
#[pg_test]
fn ps10_dropped_intermediate_printed_remedy_converges_unpartitioned() {
    ps10_make_unpartitioned_aggregate("ps10_c1_src", "ps10_c1_view");
    Spi::run("DROP TABLE __reflex_intermediate_ps10_c1_view CASCADE").expect("drop intermediate");
    Spi::run("DROP TABLE __reflex_affected_ps10_c1_view CASCADE").expect("drop affected");

    let fix = ps10_run_printed_remedy("ps10_c1_view");
    assert!(
        fix.contains("reflex_rebuild_imv") || fix.contains("reflex_reconcile"),
        "unexpected remedy for a missing intermediate: {fix}"
    );

    let report = ps10_audit("ps10_c1_view");
    assert!(
        ps10_findings(&report, "internal-tables-exist").is_empty(),
        "the printed remedy must CLEAR its own finding:\n{report}"
    );
    assert_eq!(
        ps10_diff_count(
            "SELECT region, total FROM ps10_c1_view",
            "SELECT region, SUM(amount) FROM ps10_c1_src GROUP BY region"
        ),
        0,
        "the repaired IMV must match its defining query"
    );
}

/// (5b) Same for a PARTITIONED aggregate — the report shows the two shapes fail
/// on different code paths (swap-ATTACH `missing intermediate bound` vs
/// `TRUNCATE` 42P01), so both need the convergence assertion.
#[pg_test]
fn ps10_dropped_intermediate_printed_remedy_converges_partitioned() {
    ps10_make_partitioned_aggregate("ps10_c2_src", "ps10_c2_view");
    Spi::run("DROP TABLE __reflex_intermediate_ps10_c2_view CASCADE").expect("drop intermediate");

    ps10_run_printed_remedy("ps10_c2_view");

    let report = ps10_audit("ps10_c2_view");
    assert!(
        ps10_findings(&report, "internal-tables-exist").is_empty(),
        "the printed remedy must CLEAR its own finding:\n{report}"
    );
    let int_children = ps10_count(
        "SELECT count(*) FROM pg_inherits \
         WHERE inhparent = '__reflex_intermediate_ps10_c2_view'::regclass",
    );
    assert_eq!(
        int_children, 2,
        "the healed intermediate must mirror the source partition tree again"
    );
    // The partitioned branch of `reconcile_one` returns after the swap walk and
    // never calls `build_indexes_ddl`, so the heal is the ONLY thing that can put
    // the MERGE-lookup index back. Without it every later maintenance MERGE
    // seq-scans the intermediate.
    let int_indexes = ps10_count(
        "SELECT count(*) FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid \
         WHERE ix.indrelid = '__reflex_intermediate_ps10_c2_view'::regclass \
           AND i.relname = 'idx__reflex_int_ps10_c2_view'",
    );
    assert_eq!(
        int_indexes, 1,
        "the healed intermediate must carry its reflex-managed lookup index"
    );
    assert_eq!(
        ps10_diff_count(
            "SELECT region, total FROM ps10_c2_view",
            "SELECT region, SUM(amount) FROM ps10_c2_src GROUP BY region"
        ),
        0,
        "the repaired partitioned IMV must match its defining query"
    );
}

/// (5c) Schema-qualified fixture: source and IMV in a non-`public` schema.
#[pg_test]
fn ps10_dropped_intermediate_printed_remedy_converges_non_public_schema() {
    Spi::run("CREATE SCHEMA ps10_s").expect("schema");
    ps10_make_unpartitioned_aggregate("ps10_s.c3_src", "ps10_s.c3_view");
    let int_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = '__reflex_intermediate_c3_view'",
    )
    .expect("q")
    .expect("no intermediate");
    assert_eq!(
        int_schema, "ps10_s",
        "fixture precondition: aux tables are co-located with the IMV"
    );
    Spi::run("DROP TABLE ps10_s.__reflex_intermediate_c3_view CASCADE").expect("drop intermediate");

    ps10_run_printed_remedy("ps10_s.c3_view");

    let report = ps10_audit("ps10_s.c3_view");
    assert!(
        ps10_findings(&report, "internal-tables-exist").is_empty(),
        "the printed remedy must clear the finding for a non-public IMV:\n{report}"
    );
    let healed_schema = Spi::get_one::<String>(
        "SELECT n.nspname::TEXT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = '__reflex_intermediate_c3_view'",
    )
    .expect("q")
    .expect("intermediate not recreated");
    assert_eq!(
        healed_schema, "ps10_s",
        "the healed intermediate must land in the IMV's schema, not public"
    );
    assert_eq!(
        ps10_diff_count(
            "SELECT region, total FROM ps10_s.c3_view",
            "SELECT region, SUM(amount) FROM ps10_s.c3_src GROUP BY region"
        ),
        0,
        "the repaired non-public IMV must match its defining query"
    );
}

/// (5d) An EXPRESSION group key. This is what pins the `base_query` half of the
/// `column_types` reconstruction: `date_trunc('day', ts)` exists in no catalog, so
/// the sources' `pg_attribute` rows cannot type it and `resolve_column_type` would
/// fall through to its NUMERIC default. Only the temp view over `base_query` —
/// whose output column names ARE the intermediate's column names — resolves it.
/// Every other fixture here groups on a bare source column and stays green without
/// that line.
#[pg_test]
fn ps10_dropped_intermediate_remedy_converges_with_expression_group_key() {
    Spi::run("CREATE TABLE ps10_c6_src (id BIGINT, ts TIMESTAMPTZ NOT NULL, amount NUMERIC)")
        .expect("src");
    Spi::run(
        "INSERT INTO ps10_c6_src VALUES (1,'2026-01-01 04:00+00',100), \
         (2,'2026-01-01 20:00+00',200), (3,'2026-02-03 09:00+00',50)",
    )
    .expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_c6_view', \
           'SELECT date_trunc(''day'', ts) AS d, SUM(amount) AS total \
            FROM ps10_c6_src GROUP BY date_trunc(''day'', ts)')",
    )
    .expect("create expression-group-key IMV");

    let key_type_sql = "SELECT format_type(a.atttypid, a.atttypmod) \
         FROM pg_attribute a \
         WHERE a.attrelid = '__reflex_intermediate_ps10_c6_view'::regclass \
           AND a.attnum > 0 AND NOT a.attisdropped \
           AND a.attname NOT LIKE '\\_\\_%' \
         ORDER BY a.attnum LIMIT 1";
    let created_key_type = Spi::get_one::<String>(key_type_sql)
        .expect("q")
        .expect("no intermediate key column");
    assert_eq!(
        created_key_type, "timestamp with time zone",
        "fixture precondition: create time types the expression key from the query, \
         not from the catalog"
    );

    Spi::run("DROP TABLE __reflex_intermediate_ps10_c6_view CASCADE").expect("drop intermediate");
    ps10_run_printed_remedy("ps10_c6_view");

    let healed_key_type = Spi::get_one::<String>(key_type_sql)
        .expect("q")
        .expect("intermediate not recreated");
    assert_eq!(
        healed_key_type, created_key_type,
        "the healed intermediate must type the expression key the same way create \
         time did — a NUMERIC fallback here means the reconstruction lost the \
         base_query half"
    );

    let report = ps10_audit("ps10_c6_view");
    assert!(
        ps10_findings(&report, "internal-tables-exist").is_empty(),
        "the printed remedy must clear its own finding:\n{report}"
    );
    assert_eq!(
        ps10_diff_count(
            "SELECT d, total FROM ps10_c6_view",
            "SELECT date_trunc('day', ts), SUM(amount) FROM ps10_c6_src \
             GROUP BY date_trunc('day', ts)"
        ),
        0,
        "the repaired IMV must match its defining query"
    );

    Spi::run("INSERT INTO ps10_c6_src VALUES (4,'2026-02-03 23:00+00',7)").expect("insert");
    Spi::run("DELETE FROM ps10_c6_src WHERE id = 1").expect("delete");
    assert_eq!(
        ps10_diff_count(
            "SELECT d, total FROM ps10_c6_view",
            "SELECT date_trunc('day', ts), SUM(amount) FROM ps10_c6_src \
             GROUP BY date_trunc('day', ts)"
        ),
        0,
        "the healed expression-key IMV must still maintain incrementally"
    );
}

/// (9) The scheduled sweep must survive a database that contains a set-op IMV.
///
/// `reflex_reconcile` cannot operate on a decomposed wrapper at all — a VIEW
/// wrapper RAISES `"<view>" is not a table` from the passthrough branch's TRUNCATE,
/// and the materialised UNION-ALL wrapper would be column-shifted (its
/// `base_query` yields N values for N+1 columns). The raise propagates out of
/// `reflex_scheduled_reconcile`, so ONE set-op IMV killed the whole sweep and every
/// other IMV in the batch went unreconciled. A wrapper row is not born stale — its
/// `last_update_date` is stamped at create like any other — but nothing ever
/// advances it, because the only writer is the tail of `reconcile_one`, which a
/// wrapper never reaches. So it becomes a candidate `max_age_minutes` after creation
/// and stays one forever. The candidate CTE now applies the same
/// `aggregations <> '{}'` filter the `covered` CTE two lines below it already used.
#[pg_test]
fn ps10_scheduled_reconcile_survives_a_set_op_imv() {
    Spi::run("CREATE TABLE ps10_sr_src (region TEXT, amount NUMERIC)").expect("agg src");
    Spi::run("INSERT INTO ps10_sr_src VALUES ('us', 100), ('eu', 200)").expect("seed agg");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_sr_agg', \
           'SELECT region, SUM(amount) AS total FROM ps10_sr_src GROUP BY region')",
    )
    .expect("create aggregate IMV");
    ps10_make_union_all_fixture("ps10_sr_a", "ps10_sr_b", "ps10_sr_union");

    // The candidate CTE's own age predicate, evaluated for the wrapper at the
    // `max_age_minutes` this test passes. If this is false the test proves nothing,
    // because the sweep would never have looked at the wrapper.
    let wrapper_is_candidate = Spi::get_one::<bool>(
        "SELECT last_update_date IS NULL \
                OR last_update_date < (CURRENT_TIMESTAMP - make_interval(mins => -1)) \
         FROM public.__reflex_ivm_reference WHERE name = 'ps10_sr_union'",
    )
    .expect("registry query failed")
    .expect("no registry row");
    assert!(
        wrapper_is_candidate,
        "fixture precondition: the wrapper must be old enough to be a sweep candidate"
    );

    // -1 opens the age gate for every row. This is the call that used to raise.
    Spi::run("CREATE TEMP TABLE ps10_sr_out AS SELECT * FROM reflex_scheduled_reconcile(-1)")
        .expect("the scheduled sweep must not raise on a database holding a set-op IMV");

    assert_eq!(
        ps10_count(
            "SELECT count(*) FROM ps10_sr_out \
             WHERE name = 'ps10_sr_agg' AND status = 'RECONCILED'"
        ),
        1,
        "the aggregate IMV must still be swept and reconciled"
    );
    assert_eq!(
        ps10_count("SELECT count(*) FROM ps10_sr_out WHERE name = 'ps10_sr_union'"),
        0,
        "the wrapper must not be attempted — reconcile cannot operate on it"
    );
    assert_eq!(
        ps10_diff_count(
            "SELECT region, total FROM ps10_sr_agg",
            "SELECT region, SUM(amount) FROM ps10_sr_src GROUP BY region"
        ),
        0,
        "the swept aggregate IMV must match its defining query"
    );
    assert_eq!(
        ps10_diff_count(
            "SELECT id, v FROM ps10_sr_union",
            "SELECT id, v FROM ps10_sr_a UNION ALL SELECT id, v FROM ps10_sr_b"
        ),
        0,
        "and the skipped set-op IMV must still be correct"
    );
}

/// (6) A healed IMV must keep maintaining INCREMENTALLY. A recreated but
/// unregistered / unindexed intermediate that silently stopped being maintained
/// would be worse than the bug.
#[pg_test]
fn ps10_healed_intermediate_maintains_incrementally() {
    ps10_make_unpartitioned_aggregate("ps10_c4_src", "ps10_c4_view");
    Spi::run("DROP TABLE __reflex_intermediate_ps10_c4_view CASCADE").expect("drop intermediate");
    ps10_run_printed_remedy("ps10_c4_view");

    Spi::run("INSERT INTO ps10_c4_src VALUES (4,'ap',7)").expect("insert");
    Spi::run("UPDATE ps10_c4_src SET amount = 999 WHERE id = 1").expect("update");
    Spi::run("DELETE FROM ps10_c4_src WHERE id = 2").expect("delete");

    assert_eq!(
        ps10_diff_count(
            "SELECT region, total FROM ps10_c4_view",
            "SELECT region, SUM(amount) FROM ps10_c4_src GROUP BY region"
        ),
        0,
        "the healed IMV must still maintain incrementally after INSERT/UPDATE/DELETE"
    );
}

/// (7) NO COLLATERAL HEAL — the mirror of the B9 phantom. A partitioned
/// PASSTHROUGH IMV run through reconcile must still own ZERO intermediate
/// relations: the heal is gated on the same `owns_intermediate()` predicate the
/// audit classifies with.
#[pg_test]
fn ps10_reconcile_does_not_heal_passthrough_partitioned() {
    ps9_make_passthrough_fixture("ps10_np_src", "ps10_np_view");

    let res = Spi::get_one::<String>("SELECT reflex_reconcile('ps10_np_view')")
        .expect("reconcile errored")
        .expect("NULL reconcile result");
    assert_eq!(res, "RECONCILED", "reconcile of a passthrough IMV must succeed");

    let int_relations = ps10_count(
        "SELECT count(*) FROM pg_class \
         WHERE relname LIKE '\\_\\_reflex\\_intermediate\\_ps10\\_np\\_view%'",
    );
    assert_eq!(
        int_relations, 0,
        "reconcile must not build an intermediate for a passthrough IMV"
    );
    let aff_relations = ps10_count(
        "SELECT count(*) FROM pg_class \
         WHERE relname LIKE '\\_\\_reflex\\_affected\\_ps10\\_np\\_view%'",
    );
    assert_eq!(
        aff_relations, 0,
        "reconcile must not build an affected-groups table for a passthrough IMV"
    );
}

/// (7b) The unpartitioned passthrough shape — the other `end_query = ''` row —
/// must likewise gain nothing from a reconcile.
///
/// The third `end_query = ''` shape, a decomposed WRAPPER, is asserted
/// elsewhere: PS-12 gave `reconcile_one` a backstop that refuses one with a
/// returned error STRING instead of the raise that used to abort the caller's
/// transaction (`src/tests/pg_test_ps12.rs`), and its operands are covered by
/// `src/tests/pg_test_union_operand_direct_reconcile.rs`.
#[pg_test]
fn ps10_reconcile_does_not_heal_passthrough_unpartitioned() {
    Spi::run("CREATE TABLE ps10_nw_src (id BIGINT, v NUMERIC)").expect("src");
    Spi::run("INSERT INTO ps10_nw_src VALUES (1, 10), (2, 20)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('ps10_nw_view', 'SELECT id, v FROM ps10_nw_src', 'id')")
        .expect("create passthrough IMV");

    let res = Spi::get_one::<String>("SELECT reflex_reconcile('ps10_nw_view')")
        .expect("reconcile errored")
        .expect("NULL reconcile result");
    assert_eq!(
        res, "RECONCILED",
        "reconcile of a passthrough IMV must succeed"
    );
    assert_eq!(
        ps10_own_aux_relation_count("ps10_nw_view", &[]),
        0,
        "reconcile must not build an intermediate or affected-groups table for a \
         passthrough IMV"
    );
    assert_eq!(
        ps10_diff_count(
            "SELECT id, v FROM ps10_nw_view",
            "SELECT id, v FROM ps10_nw_src"
        ),
        0,
        "the reconciled passthrough IMV must match its defining query"
    );
}

/// (7c) The shape that makes the heal's gate load-bearing: `COUNT(DISTINCT v)`
/// with no GROUP BY is `is_passthrough` (so `end_query = ''` and create time never
/// emitted intermediate DDL) YET has non-empty `distinct_columns`, so
/// `intermediate_column_spec` — and hence `build_intermediate_table_ddl` — WOULD
/// happily produce a table for it. Every other `end_query = ''` shape is protected
/// twice over, by the gate and by that builder returning None; this one is
/// protected only by the gate. Without it, reconcile materialises a phantom
/// intermediate that no maintenance path reads or writes: the exact mirror of the
/// PS-9 phantom this branch exists to stop reintroducing.
#[pg_test]
fn ps10_reconcile_does_not_heal_count_distinct_passthrough() {
    Spi::run("CREATE TABLE ps10_cd_src (id BIGINT, v NUMERIC)").expect("src");
    Spi::run("INSERT INTO ps10_cd_src VALUES (1,10),(2,10),(3,20)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_cd_view', \
           'SELECT COUNT(DISTINCT v) AS n FROM ps10_cd_src')",
    )
    .expect("create COUNT(DISTINCT) IMV");

    let shape = Spi::get_one::<bool>(
        "SELECT end_query = '' \
                AND (aggregations->>'is_passthrough')::bool \
                AND jsonb_array_length((aggregations->'distinct_columns')::jsonb) > 0 \
         FROM public.__reflex_ivm_reference WHERE name = 'ps10_cd_view'",
    )
    .expect("registry query failed")
    .expect("no registry row");
    assert!(
        shape,
        "fixture precondition: passthrough (empty end_query) WITH non-empty \
         distinct_columns — the shape where only the gate stops the heal"
    );
    assert_eq!(
        ps10_own_aux_relation_count("ps10_cd_view", &[]),
        0,
        "fixture precondition: create time built no intermediate for it"
    );

    let res = Spi::get_one::<String>("SELECT reflex_reconcile('ps10_cd_view')")
        .expect("reconcile errored")
        .expect("NULL reconcile result");
    assert_eq!(res, "RECONCILED", "reconcile must succeed");

    assert_eq!(
        ps10_own_aux_relation_count("ps10_cd_view", &[]),
        0,
        "reconcile must not materialise a phantom intermediate for a passthrough \
         IMV whose plan happens to carry distinct_columns"
    );
    let report = ps10_audit("ps10_cd_view");
    assert!(
        ps10_findings(&report, "internal-tables-exist").is_empty(),
        "the IMV was healthy before and after; the check must stay silent:\n{report}"
    );
    assert_eq!(
        ps10_count("SELECT n FROM ps10_cd_view"),
        2,
        "the reconciled IMV must match its defining query"
    );
}

/// (8) The healed intermediate must be structurally identical to the one create
/// time would have built — column names AND types AND persistence AND storage
/// options. A heal that reconstructs a plausible-but-different shape would pass
/// every content assertion above and then drift under maintenance.
#[pg_test]
fn ps10_healed_intermediate_matches_a_fresh_twin() {
    Spi::run(
        "CREATE TABLE ps10_c5_src (id BIGINT, region TEXT NOT NULL, ts TIMESTAMPTZ, \
         amount NUMERIC, flag BOOLEAN)",
    )
    .expect("src");
    Spi::run(
        "INSERT INTO ps10_c5_src VALUES (1,'us','2026-01-01',100,TRUE), \
         (2,'eu','2026-02-01',200,FALSE)",
    )
    .expect("seed");
    let body = "SELECT region, SUM(amount) AS total, COUNT(*) AS n, MIN(ts) AS first_ts, \
                MAX(ts) AS last_ts, BOOL_OR(flag) AS any_flag \
                FROM ps10_c5_src GROUP BY region";
    Spi::run(&format!(
        "SELECT create_reflex_ivm('ps10_c5_victim', '{body}')"
    ))
    .expect("victim");
    Spi::run(&format!(
        "SELECT create_reflex_ivm('ps10_c5_twin', '{body}')"
    ))
    .expect("twin");

    Spi::run("DROP TABLE __reflex_intermediate_ps10_c5_victim CASCADE").expect("drop intermediate");
    ps10_run_printed_remedy("ps10_c5_victim");

    let shape_sql = |rel: &str| {
        format!(
            "SELECT COALESCE(string_agg(format('%s:%s', a.attname, \
                 format_type(a.atttypid, a.atttypmod)), ',' ORDER BY a.attname), '<none>') \
             FROM pg_attribute a WHERE a.attrelid = '{rel}'::regclass \
               AND a.attnum > 0 AND NOT a.attisdropped"
        )
    };
    let victim_shape =
        Spi::get_one::<String>(&shape_sql("__reflex_intermediate_ps10_c5_victim"))
            .expect("q")
            .expect("healed intermediate missing");
    let twin_shape = Spi::get_one::<String>(&shape_sql("__reflex_intermediate_ps10_c5_twin"))
        .expect("q")
        .expect("twin intermediate missing");
    assert_eq!(
        victim_shape, twin_shape,
        "the healed intermediate's columns/types must match a fresh twin's"
    );

    let storage_sql = |rel: &str| {
        format!(
            "SELECT format('%s|%s', relpersistence, COALESCE(array_to_string(reloptions, ','), '')) \
             FROM pg_class WHERE oid = '{rel}'::regclass"
        )
    };
    let victim_storage =
        Spi::get_one::<String>(&storage_sql("__reflex_intermediate_ps10_c5_victim"))
            .expect("q")
            .expect("v");
    let twin_storage = Spi::get_one::<String>(&storage_sql("__reflex_intermediate_ps10_c5_twin"))
        .expect("q")
        .expect("t");
    assert_eq!(
        victim_storage, twin_storage,
        "persistence and storage options must match a fresh twin's"
    );

    let index_sql = |view: &str| {
        format!(
            "SELECT COALESCE(string_agg(regexp_replace(pg_get_indexdef(ix.indexrelid), \
                 '{view}', 'X', 'g'), E'\\n' ORDER BY 1), '<none>') \
             FROM pg_index ix WHERE ix.indrelid = '__reflex_intermediate_{view}'::regclass"
        )
    };
    let victim_idx = Spi::get_one::<String>(&index_sql("ps10_c5_victim"))
        .expect("q")
        .expect("v");
    let twin_idx = Spi::get_one::<String>(&index_sql("ps10_c5_twin"))
        .expect("q")
        .expect("t");
    assert_eq!(
        victim_idx, twin_idx,
        "the healed intermediate must carry the same indexes as a fresh twin"
    );

    assert_eq!(
        ps10_diff_count(
            "SELECT region, total, n, first_ts, last_ts, any_flag FROM ps10_c5_victim",
            "SELECT region, total, n, first_ts, last_ts, any_flag FROM ps10_c5_twin"
        ),
        0,
        "healed and twin IMVs must hold identical contents"
    );

    Spi::run("INSERT INTO ps10_c5_src VALUES (3,'us','2026-03-01',7,TRUE)").expect("insert");
    Spi::run("DELETE FROM ps10_c5_src WHERE id = 2").expect("delete");
    assert_eq!(
        ps10_diff_count(
            "SELECT region, total, n, first_ts, last_ts, any_flag FROM ps10_c5_victim",
            "SELECT region, total, n, first_ts, last_ts, any_flag FROM ps10_c5_twin"
        ),
        0,
        "healed and twin IMVs must stay identical under maintenance"
    );
}

// --- Bug 1b: a companion dropped ALONE ------------------------------------
//
// `__reflex_affected_<view>` / `__reflex_shrunk_<view>` are CTAS'd
// `... AS SELECT <group cols> FROM <intermediate> WHERE FALSE`, and a CTAS
// records NO catalog dependency on the table it selected from. So dropping a
// companion leaves the intermediate untouched and vice versa — the two failure
// shapes are independent, and the heal must probe them independently. Before
// this, the heal returned early whenever the INTERMEDIATE was present, so the
// `internal-tables-exist` finding for a lone missing companion could never be
// cleared by its own printed remedy.

/// (9) Bug 1b: only `__reflex_affected_<view>` is dropped. The audit reports it
/// and its printed remedy must recreate it and clear the finding.
#[pg_test]
fn ps10_dropped_affected_alone_printed_remedy_converges() {
    ps10_make_unpartitioned_aggregate("ps10_c8_src", "ps10_c8_view");
    Spi::run("DROP TABLE __reflex_affected_ps10_c8_view").expect("drop affected");
    assert_eq!(
        ps10_count(
            "SELECT count(*) FROM pg_class WHERE relname = '__reflex_intermediate_ps10_c8_view'"
        ),
        1,
        "fixture precondition: dropping the affected table must leave the intermediate"
    );

    ps10_run_printed_remedy("ps10_c8_view");

    assert_eq!(
        ps10_count("SELECT count(*) FROM pg_class WHERE relname = '__reflex_affected_ps10_c8_view'"),
        1,
        "the printed remedy must recreate the lone missing group-capture table"
    );
    let report = ps10_audit("ps10_c8_view");
    assert!(
        ps10_findings(&report, "internal-tables-exist").is_empty(),
        "the printed remedy must CLEAR its own finding:\n{report}"
    );

    Spi::run("INSERT INTO ps10_c8_src VALUES (4,'ap',7)").expect("insert");
    Spi::run("UPDATE ps10_c8_src SET amount = 999 WHERE id = 1").expect("update");
    Spi::run("DELETE FROM ps10_c8_src WHERE id = 2").expect("delete");
    assert_eq!(
        ps10_diff_count(
            "SELECT region, total FROM ps10_c8_view",
            "SELECT region, SUM(amount) FROM ps10_c8_src GROUP BY region"
        ),
        0,
        "the repaired IMV must maintain incrementally after INSERT/UPDATE/DELETE"
    );
}

/// (10) Bug 1b, top-K twin: only `__reflex_shrunk_<view>` is dropped. It is the
/// companion the UPDATE path scopes its forced MIN/MAX recompute to, so a
/// silently absent one wedges maintenance of exactly the plan that needs it.
#[pg_test]
fn ps10_dropped_shrunk_alone_is_healed_by_reconcile() {
    Spi::run("CREATE TABLE ps10_c7_src (id BIGINT, grp TEXT NOT NULL, v INT)").expect("src");
    Spi::run(
        "INSERT INTO ps10_c7_src SELECT i, 'g' || (i % 4), i FROM generate_series(1, 40) i",
    )
    .expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm('ps10_c7_view', \
           'SELECT grp, MIN(v) AS mn, MAX(v) AS mx FROM ps10_c7_src GROUP BY grp')",
    )
    .expect("create top-K IMV");
    assert_eq!(
        ps10_count("SELECT count(*) FROM pg_class WHERE relname = '__reflex_shrunk_ps10_c7_view'"),
        1,
        "fixture precondition: a MIN/MAX plan owns a shrunk-groups table"
    );

    Spi::run("DROP TABLE __reflex_shrunk_ps10_c7_view").expect("drop shrunk");
    assert_eq!(
        ps10_count(
            "SELECT count(*) FROM pg_class WHERE relname = '__reflex_intermediate_ps10_c7_view'"
        ),
        1,
        "fixture precondition: dropping the shrunk table must leave the intermediate"
    );

    let res = Spi::get_one::<String>("SELECT reflex_reconcile('ps10_c7_view')")
        .expect("reconcile errored")
        .expect("NULL reconcile result");
    assert_eq!(res, "RECONCILED", "reconcile must succeed");
    assert_eq!(
        ps10_count("SELECT count(*) FROM pg_class WHERE relname = '__reflex_shrunk_ps10_c7_view'"),
        1,
        "reconcile must recreate the lone missing shrunk-groups table"
    );

    Spi::run("UPDATE ps10_c7_src SET v = v + 1000 WHERE id IN (1,2,3)").expect("update");
    Spi::run("DELETE FROM ps10_c7_src WHERE id BETWEEN 5 AND 9").expect("delete");
    Spi::run("INSERT INTO ps10_c7_src VALUES (99,'g1',-5)").expect("insert");
    assert_eq!(
        ps10_diff_count(
            "SELECT grp, mn, mx FROM ps10_c7_view",
            "SELECT grp, MIN(v), MAX(v) FROM ps10_c7_src GROUP BY grp"
        ),
        0,
        "the repaired top-K IMV must maintain incrementally"
    );
}
