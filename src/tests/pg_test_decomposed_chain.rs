/// PS-1 (N1) — the decomposer writes the parent's source double-quoted, so
/// `resolve_existing_imv_deps` must canonicalise before probing the registry or
/// the parent → generated-child edge is never recorded.
#[pg_test]
fn pg_decomposed_cte_records_generated_child_edge() {
    Spi::run("CREATE TABLE dc1_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dc1_src VALUES (1,'a',10),(2,'a',20),(3,'b',30)").expect("seed");

    let result = crate::create_reflex_ivm(
        "dc1_agg",
        "WITH base AS (SELECT id, grp, val FROM dc1_src) \
         SELECT grp, SUM(val) AS total FROM base GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let deps = Spi::get_one::<Vec<String>>(
        "SELECT depends_on_imv FROM public.__reflex_ivm_reference WHERE name = 'dc1_agg'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(
        deps,
        vec!["dc1_agg__cte_base".to_string()],
        "parent's depends_on_imv must name the generated child"
    );

    let kids = Spi::get_one::<Vec<String>>(
        "SELECT graph_child FROM public.__reflex_ivm_reference \
         WHERE name = 'dc1_agg__cte_base'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(
        kids,
        vec!["dc1_agg".to_string()],
        "generated child's graph_child must name the parent"
    );

    let child_is_generated = Spi::get_one::<bool>(
        "SELECT is_generated_sub_imv FROM public.__reflex_ivm_reference \
         WHERE name = 'dc1_agg__cte_base'",
    )
    .expect("query")
    .expect("value");
    assert!(child_is_generated, "generated child must be flagged");

    let parent_is_generated = Spi::get_one::<bool>(
        "SELECT is_generated_sub_imv FROM public.__reflex_ivm_reference \
         WHERE name = 'dc1_agg'",
    )
    .expect("query")
    .expect("value");
    assert!(
        !parent_is_generated,
        "the user-declared parent must NOT be flagged"
    );

    let child_depth = Spi::get_one::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference \
         WHERE name = 'dc1_agg__cte_base'",
    )
    .expect("query")
    .expect("value");
    let parent_depth = Spi::get_one::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'dc1_agg'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(child_depth, 1, "generated child sits at depth 1");
    assert_eq!(parent_depth, 2, "parent reads the child, so depth 2");
}

/// PS-1 (N1 consequence 2) — a user IMV stacked on a decomposed parent must get
/// depth 3, not the collapsed 2 the exact-string match produced.
#[pg_test]
fn pg_decomposed_chain_depths_are_not_collapsed() {
    Spi::run("CREATE TABLE dc2_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dc2_src VALUES (1,'a',10),(2,'b',20)").expect("seed");

    crate::create_reflex_ivm(
        "dc2_agg",
        "WITH base AS (SELECT id, grp, val FROM dc2_src) \
         SELECT grp, SUM(val) AS total FROM base GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    crate::create_reflex_ivm(
        "dc2_top",
        "SELECT grp, SUM(total) AS grand FROM dc2_agg GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    for (name, want) in [("dc2_agg__cte_base", 1), ("dc2_agg", 2), ("dc2_top", 3)] {
        let got = Spi::get_one::<i32>(&format!(
            "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = '{}'",
            name
        ))
        .expect("query")
        .expect("value");
        assert_eq!(got, want, "graph_depth of {}", name);
    }
}

/// PS-1 (B1) — the field bug, IMMEDIATE mode. A matview cannot carry triggers,
/// so the generated child is a frozen snapshot; `reflex_rebuild_imv` on the
/// parent must reconcile the child first instead of re-aggregating stale rows.
#[pg_test]
fn pg_rebuild_imv_recurses_into_generated_child_immediate() {
    Spi::run("CREATE TABLE dc3_src (id INT, d DATE)").expect("create src");
    Spi::run("INSERT INTO dc3_src SELECT g, '2026-01-01'::date + g FROM generate_series(1,50) g")
        .expect("seed");
    Spi::run("CREATE MATERIALIZED VIEW dc3_mv AS SELECT * FROM dc3_src").expect("create mv");

    crate::create_reflex_ivm(
        "dc3_agg",
        "WITH base AS (SELECT id, EXTRACT('year' FROM d)::int AS y, d FROM dc3_mv) \
         SELECT id, y, min(d) AS mn FROM base GROUP BY id, y",
        Some("id,y"),
        None,
        None,
        None,
    );

    Spi::run("INSERT INTO dc3_src VALUES (888, '2029-01-01')").expect("late insert");
    Spi::run("REFRESH MATERIALIZED VIEW dc3_mv").expect("refresh mv");

    let status = Spi::get_one::<String>("SELECT reflex_rebuild_imv('dc3_agg')")
        .expect("query")
        .expect("value");
    assert_eq!(status, "RECONCILED");

    let child_rows = Spi::get_one::<i64>("SELECT COUNT(*) FROM dc3_agg__cte_base WHERE id = 888")
        .expect("query")
        .expect("value");
    assert_eq!(child_rows, 1, "generated child must have been reconciled");

    let parent_rows = Spi::get_one::<i64>("SELECT COUNT(*) FROM dc3_agg WHERE id = 888")
        .expect("query")
        .expect("value");
    assert_eq!(
        parent_rows, 1,
        "parent must show the row REFRESHed into the matview"
    );
}

/// PS-1 (B1) — same as above in DEFERRED mode, the mode every affected prod IMV
/// uses. This is also the shape spec §1c would double-count without trigger
/// suppression.
#[pg_test]
fn pg_rebuild_imv_recurses_into_generated_child_deferred() {
    Spi::run("CREATE TABLE dc4_src (id INT, d DATE)").expect("create src");
    Spi::run("INSERT INTO dc4_src SELECT g, '2026-01-01'::date + g FROM generate_series(1,50) g")
        .expect("seed");
    Spi::run("CREATE MATERIALIZED VIEW dc4_mv AS SELECT * FROM dc4_src").expect("create mv");

    crate::create_reflex_ivm(
        "dc4_agg",
        "WITH base AS (SELECT id, EXTRACT('year' FROM d)::int AS y, d FROM dc4_mv) \
         SELECT id, y, min(d) AS mn FROM base GROUP BY id, y",
        Some("id,y"),
        Some("UNLOGGED"),
        Some("DEFERRED"),
        None,
    );

    Spi::run("INSERT INTO dc4_src VALUES (888, '2029-01-01')").expect("late insert");
    Spi::run("REFRESH MATERIALIZED VIEW dc4_mv").expect("refresh mv");

    let status = Spi::get_one::<String>("SELECT reflex_rebuild_imv('dc4_agg')")
        .expect("query")
        .expect("value");
    assert_eq!(status, "RECONCILED");

    let parent_rows = Spi::get_one::<i64>("SELECT COUNT(*) FROM dc4_agg WHERE id = 888")
        .expect("query")
        .expect("value");
    assert_eq!(parent_rows, 1, "parent must show the refreshed row");
}

/// PS-1 (spec §1c / D7-revised) — reconciling the generated child must not leave
/// a staged deferred delta that the COMMIT flush re-applies on top of the
/// already-rebuilt parent. Oracle-compared, so a doubled SUM fails.
#[pg_test]
fn pg_reconcile_generated_child_is_not_double_counted_deferred() {
    Spi::run("CREATE TABLE dc5_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dc5_src SELECT g, 'g' || (g % 5), 10 FROM generate_series(1,200) g")
        .expect("seed");

    crate::create_reflex_ivm(
        "dc5_agg",
        "WITH base AS (SELECT id, grp, val FROM dc5_src) \
         SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM base GROUP BY grp",
        Some("grp"),
        Some("UNLOGGED"),
        Some("DEFERRED"),
        None,
    );

    let status = Spi::get_one::<String>("SELECT reflex_rebuild_imv('dc5_agg')")
        .expect("query")
        .expect("value");
    assert_eq!(status, "RECONCILED");

    assert_imv_correct(
        "dc5_agg",
        "SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM dc5_src GROUP BY grp",
    );
}

/// PS-1 (D18) — `reflex_reconcile` is invoked from inside pg_reflex's own
/// generated trigger bodies on the high-selectivity "wipe" branch. Bulk DML that
/// trips that branch on a decomposed chain must not recurse into DDL on the
/// relation whose statement trigger is running. Observable proxy: it stays
/// oracle-correct and does not error.
#[pg_test]
fn pg_bulk_dml_on_decomposed_chain_stays_correct() {
    Spi::run("CREATE TABLE dc6_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dc6_src SELECT g, 'g' || (g % 4), 1 FROM generate_series(1,100) g")
        .expect("seed");

    crate::create_reflex_ivm(
        "dc6_agg",
        "WITH base AS (SELECT id, grp, val FROM dc6_src) \
         SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM base GROUP BY grp",
        Some("grp"),
        None,
        None,
        None,
    );

    // 400 new rows against a 100-row base: |affected| / reltuples is far above
    // any wipe_threshold, so the trigger takes the reflex_reconcile branch.
    Spi::run("INSERT INTO dc6_src SELECT g, 'g' || (g % 4), 2 FROM generate_series(101,500) g")
        .expect("bulk insert");

    assert_imv_correct(
        "dc6_agg",
        "SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM dc6_src GROUP BY grp",
    );
}

/// PS-1 — recursion is for pg_reflex's own generated nodes only. Reconciling an
/// IMV that reads a *user's* IMV must leave that IMV alone.
#[pg_test]
fn pg_reconcile_does_not_recurse_into_user_imv_dependency() {
    Spi::run("CREATE TABLE dc7_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dc7_src VALUES (1,'a',10),(2,'b',20)").expect("seed");

    crate::create_reflex_ivm(
        "dc7_lower",
        "SELECT grp, SUM(val) AS total FROM dc7_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    crate::create_reflex_ivm(
        "dc7_upper",
        "SELECT grp, SUM(total) AS grand FROM dc7_lower GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
         SET last_update_date = TIMESTAMP '2001-01-01 00:00:00' WHERE name = 'dc7_lower'",
    )
    .expect("backdate");

    Spi::get_one::<String>("SELECT reflex_reconcile('dc7_upper')")
        .expect("query")
        .expect("value");

    let untouched = Spi::get_one::<bool>(
        "SELECT last_update_date = TIMESTAMP '2001-01-01 00:00:00' \
         FROM public.__reflex_ivm_reference WHERE name = 'dc7_lower'",
    )
    .expect("query")
    .expect("value");
    assert!(
        untouched,
        "reconciling dc7_upper must not reconcile the user-declared dc7_lower"
    );
}

/// PS-1 (N1 consequence 3, the latent data-destroyer) — with the parent finally
/// registered as a dependent, rebuild_chain on a generated child must refuse
/// instead of dropping the child's table (and the parent's triggers with it) and
/// recreating only the child.
#[pg_test]
fn pg_rebuild_chain_refuses_on_generated_child_without_cascade() {
    Spi::run("CREATE TABLE dc8_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dc8_src VALUES (1,'a',10)").expect("seed");

    crate::create_reflex_ivm(
        "dc8_agg",
        "WITH base AS (SELECT id, grp, val FROM dc8_src) \
         SELECT grp, SUM(val) AS total FROM base GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    let out = crate::reflex_rebuild_chain("dc8_agg__cte_base", false);
    assert!(
        out.starts_with("ERROR"),
        "rebuild_chain on a generated child must refuse without cascade, got: {}",
        out
    );
    assert!(
        out.contains("dc8_agg"),
        "the refusal must name the dependent parent, got: {}",
        out
    );
}

/// PS-1 (migration backfill) — legacy rows carry the broken shape. The repair
/// primitive must derive all four columns from `depends_on`, and be idempotent.
#[pg_test]
fn pg_repair_dependency_graph_backfills_broken_rows() {
    // The exact broken shape observed on 1.10.11: quoted sub-IMV source in
    // depends_on, empty depends_on_imv / graph_child, collapsed graph_depth.
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference (name, graph_depth, depends_on) VALUES \
           ('rp_agg__cte_base', 1, ARRAY['rp_mv']), \
           ('rp_agg',          1, ARRAY['\"rp_agg__cte_base\"']), \
           ('rp_top',          2, ARRAY['rp_agg'])",
    )
    .expect("seed registry");

    let summary = Spi::get_one::<String>("SELECT reflex_repair_dependency_graph()")
        .expect("query")
        .expect("value");
    assert!(
        summary.starts_with("REPAIRED"),
        "unexpected summary: {}",
        summary
    );

    let flagged = Spi::get_one::<bool>(
        "SELECT is_generated_sub_imv FROM public.__reflex_ivm_reference \
         WHERE name = 'rp_agg__cte_base'",
    )
    .expect("query")
    .expect("value");
    assert!(flagged, "generated child must be flagged by the backfill");

    let deps = Spi::get_one::<Vec<String>>(
        "SELECT depends_on_imv FROM public.__reflex_ivm_reference WHERE name = 'rp_agg'",
    )
    .expect("query")
    .expect("value");
    assert!(
        deps.contains(&"rp_agg__cte_base".to_string()),
        "depends_on_imv not backfilled: {:?}",
        deps
    );

    let kids = Spi::get_one::<Vec<String>>(
        "SELECT graph_child FROM public.__reflex_ivm_reference \
         WHERE name = 'rp_agg__cte_base'",
    )
    .expect("query")
    .expect("value");
    assert!(
        kids.contains(&"rp_agg".to_string()),
        "graph_child not backfilled: {:?}",
        kids
    );

    for (name, want) in [("rp_agg__cte_base", 1), ("rp_agg", 2), ("rp_top", 3)] {
        let got = Spi::get_one::<i32>(&format!(
            "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = '{}'",
            name
        ))
        .expect("query")
        .expect("value");
        assert_eq!(got, want, "graph_depth of {} after repair", name);
    }

    // Idempotent: a second pass must change nothing.
    let before = Spi::get_one::<String>(
        "SELECT string_agg(name || ':' || graph_depth || ':' || \
                COALESCE(array_to_string(depends_on_imv, ','), '') || ':' || \
                COALESCE(array_to_string(graph_child, ','), ''), '|' ORDER BY name) \
         FROM public.__reflex_ivm_reference",
    )
    .expect("query")
    .expect("value");
    Spi::get_one::<String>("SELECT reflex_repair_dependency_graph()")
        .expect("query")
        .expect("value");
    let after = Spi::get_one::<String>(
        "SELECT string_agg(name || ':' || graph_depth || ':' || \
                COALESCE(array_to_string(depends_on_imv, ','), '') || ':' || \
                COALESCE(array_to_string(graph_child, ','), ''), '|' ORDER BY name) \
         FROM public.__reflex_ivm_reference",
    )
    .expect("query")
    .expect("value");
    assert_eq!(before, after, "repair must be idempotent");
}

/// PS-1 (D11) — the backfill's prefix heuristic must not misclassify a plain
/// user-declared chain, and must not disturb its already-correct depths.
#[pg_test]
fn pg_repair_dependency_graph_leaves_user_chain_alone() {
    Spi::run("CREATE TABLE dc9_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dc9_src VALUES (1,'a',10)").expect("seed");

    crate::create_reflex_ivm(
        "dc9_lower",
        "SELECT grp, SUM(val) AS total FROM dc9_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    crate::create_reflex_ivm(
        "dc9_upper",
        "SELECT grp, SUM(total) AS grand FROM dc9_lower GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    Spi::get_one::<String>("SELECT reflex_repair_dependency_graph()")
        .expect("query")
        .expect("value");

    let any_flagged = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference \
         WHERE is_generated_sub_imv AND name IN ('dc9_lower','dc9_upper')",
    )
    .expect("query")
    .expect("value");
    assert_eq!(any_flagged, 0, "user IMVs must not be flagged as generated");

    let lower = Spi::get_one::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'dc9_lower'",
    )
    .expect("query")
    .expect("value");
    let upper = Spi::get_one::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'dc9_upper'",
    )
    .expect("query")
    .expect("value");
    assert_eq!((lower, upper), (1, 2), "user chain depths must be unchanged");
}

/// PS-1 (D9) — the set-op path used `operand_count + 1` as graph_depth, so a
/// 3-operand UNION ALL claimed depth 4. Depth must describe position in the
/// graph, not operand arity, or create-time and the repair primitive disagree.
#[pg_test]
fn pg_decomposed_set_op_depth_reflects_operand_depth() {
    Spi::run("CREATE TABLE dca_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dca_src VALUES (1,'a',10),(2,'b',20),(3,'c',30)").expect("seed");

    let result = crate::create_reflex_ivm(
        "dca_u",
        "SELECT grp, val FROM dca_src WHERE grp = 'a' \
         UNION ALL SELECT grp, val FROM dca_src WHERE grp = 'b' \
         UNION ALL SELECT grp, val FROM dca_src WHERE grp = 'c'",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    let depth = Spi::get_one::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'dca_u'",
    )
    .expect("query")
    .expect("value");
    assert_eq!(
        depth, 2,
        "a UNION ALL wrapper sits one level above its depth-1 operands"
    );
}
