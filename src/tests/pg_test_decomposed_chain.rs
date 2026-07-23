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

    // The load-bearing assertion: suppression must have left the child with NO
    // staged deferred delta. A pgrx test runs in one never-committed transaction,
    // so the COMMIT-time flush never fires on its own — without this the oracle
    // below cannot see a double-count and the test passes even on `main`. Assert
    // the child staged nothing, then drive the flush by hand and oracle-check.
    let pending_for_child = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_deferred_pending \
          WHERE source_table LIKE '%dc5_agg__cte_base%'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        pending_for_child, 0,
        "reconciling the generated child must leave no staged deferred delta"
    );

    drain_deferred_pending();
    assert_imv_correct(
        "dc5_agg",
        "SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM dc5_src GROUP BY grp",
    );
}

/// PS-1 (D18) — `reflex_reconcile` is invoked from inside pg_reflex's own
/// generated trigger bodies on the high-selectivity "wipe" branch
/// (`trigger/dispatch.rs`). Without the `pg_trigger_depth()` gate, that inner
/// call on a decomposed parent would try to `DISABLE TRIGGER` / `TRUNCATE` the
/// generated child while the child's own INSERT statement trigger is live — a
/// hard error. So on this fixture, correctness is the discriminator: the gate is
/// exactly what turns an error into a correct single-node maintenance.
///
/// The premise (the wipe branch actually fires) is FORCED, not hoped for: reflex
/// takes it when `|affected| / reltuples >= wipe_threshold`, so the threshold is
/// pinned to 0.01 and the base is ANALYZEd to give reltuples a real value before a
/// bulk insert an order of magnitude larger. Without forcing it, an INSERT could
/// take the incremental branch and the test would pass on `main` too.
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

    // Force the wipe branch: low threshold on both the parent and its generated
    // child (the child carries the source triggers), plus real reltuples.
    Spi::run("SELECT reflex_set_wipe_threshold('dc6_agg', 0.01::numeric)").expect("thr parent");
    Spi::run("SELECT reflex_set_wipe_threshold('dc6_agg__cte_base', 0.01::numeric)")
        .expect("thr child");
    Spi::run("ANALYZE dc6_agg__cte_base").expect("analyze child intermediate source");

    // 400 rows onto a ~100-row base -> ratio ~4.0, far above 0.01, so the trigger
    // takes the reflex_reconcile ("wipe") branch. Under the D18 gate this stays
    // correct; without it, it errors trying to TRUNCATE the child under its own
    // trigger.
    Spi::run("INSERT INTO dc6_src SELECT g, 'g' || (g % 4), 2 FROM generate_series(101,500) g")
        .expect("bulk insert must not error under the trigger-depth gate");

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

/// PS-1 (D6, revised after measurement) — `reflex_scheduled_reconcile` walks every
/// registry row in `graph_depth` order, so on a decomposed chain each parent used
/// to re-rebuild the generated children the scan had already visited: 1+2+…+d
/// rebuilds for a depth-d chain instead of d. Measured at 15 rebuilds for a
/// 4-generated-child chain (3x, and quadratic in depth), not the 2x D6 assumed.
///
/// A generated node whose consumer is also a candidate must therefore be dropped
/// from the candidate list — the consumer's own recursion covers it. Observable
/// as the number of rows the function returns (one per *attempted* IMV), while
/// every node still ends up rebuilt and correct.
#[pg_test]
fn pg_scheduled_reconcile_does_not_revisit_covered_generated_children() {
    Spi::run("CREATE TABLE dcb_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dcb_src SELECT g, 'g' || (g % 5), 1 FROM generate_series(1,100) g")
        .expect("seed");

    crate::create_reflex_ivm(
        "dcb_top",
        "WITH a AS (SELECT id, grp, val FROM dcb_src), \
              b AS (SELECT id, grp, val FROM a), \
              c AS (SELECT id, grp, val FROM b) \
         SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM c GROUP BY grp",
        Some("grp"),
        None,
        None,
        None,
    );

    let generated = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference \
         WHERE is_generated_sub_imv AND name LIKE 'dcb_top%'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(generated, 3, "fixture must produce 3 generated children");

    // Inside one transaction CURRENT_TIMESTAMP is the transaction start, so a row
    // touched in this transaction never looks stale. Backdate to make all 4 rows
    // candidates.
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET last_update_date = TIMESTAMP '2001-01-01 00:00:00' \
          WHERE name LIKE 'dcb_top%'",
    )
    .expect("backdate");

    let attempted = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM reflex_scheduled_reconcile(1) WHERE name LIKE 'dcb_top%'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        attempted, 1,
        "only the top IMV should be attempted; its recursion covers the 3 generated children"
    );

    // Every node must nevertheless have been rebuilt — the recursion did the work
    // the scan no longer duplicates.
    let still_backdated = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference \
          WHERE name LIKE 'dcb_top%' \
            AND last_update_date = TIMESTAMP '2001-01-01 00:00:00'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        still_backdated, 0,
        "every node in the chain must have been reconciled, not just the top"
    );

    assert_imv_correct(
        "dcb_top",
        "SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM dcb_src GROUP BY grp",
    );
}

/// PS-1 REVIEW BLOCKING 1 (S1, silent data corruption) — a UNION-ALL CTE is
/// materialised by `install_union_all_intermediate_wrapper` as
/// `(__reflex_src_idx SMALLINT NOT NULL, <payload…>)` but registered with
/// `base_query = 'SELECT * FROM op0 UNION ALL SELECT * FROM op1'` and an empty
/// `end_query`. Handing that to `reconcile_one` takes the passthrough branch,
/// which runs `INSERT INTO <wrapper> SELECT * FROM …` — N payload values into an
/// N+1-column table. PostgreSQL left-shifts rather than rejecting when the types
/// line up, and raises a hard `ereport` when they do not; either way the node the
/// parent is then rebuilt from is wrong, and the call still says RECONCILED.
///
/// `RegistryRow::decomposed` nodes are not `reconcile_one`-rebuildable. The
/// recursion must skip them. Rebuilding them correctly is a separate pre-spec.
#[pg_test]
fn pg_reconcile_skips_decomposed_union_all_wrapper_node() {
    Spi::run(
        "CREATE TABLE dcu_us(id INT PRIMARY KEY, country TEXT, amount NUMERIC); \
         CREATE TABLE dcu_eu(id INT PRIMARY KEY, country TEXT, amount NUMERIC); \
         INSERT INTO dcu_us VALUES (1,'US',100),(2,'US',50); \
         INSERT INTO dcu_eu VALUES (1,'FR',200),(2,'DE',25);",
    )
    .expect("seed");

    let res = crate::create_reflex_ivm(
        "dcu_imv",
        "WITH all_ord AS ( \
             SELECT id, country, amount FROM dcu_us \
             UNION ALL \
             SELECT id, country, amount FROM dcu_eu \
         ) \
         SELECT country, SUM(amount) AS total FROM all_ord GROUP BY country",
        None,
        Some("UNLOGGED"),
        None,
        None,
    );
    assert_eq!(res, "CREATE REFLEX INCREMENTAL VIEW");

    // The wrapper is a decomposed node: `aggregations` is the literal '{}' that
    // `RegistryRow::decomposed` writes. Pin that, because the skip predicate
    // depends on it distinguishing wrappers from passthrough sub-IMVs.
    let wrapper_aggs = Spi::get_one::<String>(
        "SELECT aggregations::text FROM public.__reflex_ivm_reference \
          WHERE name = 'dcu_imv__cte_all_ord'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        wrapper_aggs, "{}",
        "wrapper must be recognisable as a decomposed node"
    );

    let src_idx_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM dcu_imv__cte_all_ord WHERE __reflex_src_idx IN (0,1)",
    )
    .expect("q")
    .expect("v");
    assert_eq!(src_idx_before, 4, "wrapper should hold 4 tagged payload rows");

    // Pre-fix this either left-shifts the wrapper's columns or aborts the
    // transaction on a type mismatch.
    let status = Spi::get_one::<String>("SELECT reflex_rebuild_imv('dcu_imv')")
        .expect("q")
        .expect("v");
    assert_eq!(status, "RECONCILED");

    let src_idx_after = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM dcu_imv__cte_all_ord WHERE __reflex_src_idx IN (0,1)",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        src_idx_after, 4,
        "reconcile must not disturb the wrapper's __reflex_src_idx tagging"
    );

    assert_imv_correct(
        "dcu_imv",
        "SELECT country, SUM(amount) AS total FROM ( \
             SELECT id, country, amount FROM dcu_us \
             UNION ALL SELECT id, country, amount FROM dcu_eu) u GROUP BY country",
    );
}

/// PS-1 REVIEW BLOCKING 2 (S2) — the `relkind` probe passed an UNQUOTED name to
/// `to_regclass`, which down-cases, so for any IMV whose name needs quoting the
/// probe returned NULL, `triggerable` came out false, and the child was rebuilt
/// with its triggers LIVE. In DEFERRED mode that reinstates exactly the
/// COMMIT-time double-count the suppression exists to prevent — silently, still
/// returning RECONCILED. The codebase persists sub-IMV sources double-quoted
/// precisely to preserve identifier case (`query_decomposer.rs:18-23`).
#[pg_test]
fn pg_reconcile_suppresses_triggers_for_mixed_case_child_deferred() {
    Spi::run("CREATE TABLE dcm_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dcm_src SELECT g, 'g' || (g % 5), 10 FROM generate_series(1,200) g")
        .expect("seed");

    crate::create_reflex_ivm(
        "MixedCase_Agg",
        "WITH base AS (SELECT id, grp, val FROM dcm_src) \
         SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM base GROUP BY grp",
        Some("grp"),
        Some("UNLOGGED"),
        Some("DEFERRED"),
        None,
    );

    // The bug in one line: the unquoted probe cannot see this relation.
    let unquoted_probe_blind = Spi::get_one::<bool>(
        "SELECT to_regclass('MixedCase_Agg__cte_base') IS NULL",
    )
    .expect("q")
    .expect("v");
    assert!(
        unquoted_probe_blind,
        "fixture must exercise a name that an unquoted to_regclass cannot resolve"
    );

    let status = Spi::get_one::<String>("SELECT reflex_rebuild_imv('MixedCase_Agg')")
        .expect("q")
        .expect("v");
    assert_eq!(status, "RECONCILED");

    // Suppression must have left no staged delta for the child. A pgrx test never
    // commits, so the COMMIT-time flush must be driven by hand or the
    // double-count is unobservable.
    let pending_for_child = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_deferred_pending \
          WHERE source_table LIKE '%MixedCase_Agg__cte_base%'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        pending_for_child, 0,
        "suppressed child must stage no deferred delta"
    );

    // Cheap regression guard: the DISABLE must not leak past the rebuild.
    let disabled_triggers = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
          WHERE c.relname = 'MixedCase_Agg__cte_base' \
            AND NOT t.tgisinternal AND t.tgenabled = 'D'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(disabled_triggers, 0, "trigger suppression leaked past reconcile");

    drain_deferred_pending();
    assert_imv_correct(
        "\"MixedCase_Agg\"",
        "SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM dcm_src GROUP BY grp",
    );
}

/// PS-1 REVIEW BLOCKING 3 — warn-and-continue plus a `"RECONCILED"` return makes
/// a failed generated-child rebuild invisible. Merged with PS-4, whose
/// `verify_stale_cleared` checks only `known_stale` on the NAMED IMV (which the
/// parent's own rebuild just cleared), `reflex_doctor(fix => TRUE)` would report a
/// *verified* repair over a parent re-derived from a stale child. The child was
/// never `known_stale` — that is B2's whole finding — so nothing else catches it.
/// `reflex_reconcile` must therefore surface the failure in its return value.
#[pg_test]
fn pg_reconcile_reports_error_when_generated_child_fails() {
    Spi::run("CREATE TABLE dcf_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dcf_src VALUES (1,'a',10)").expect("seed");

    crate::create_reflex_ivm(
        "dcf_top",
        "WITH base AS (SELECT id, grp, val FROM dcf_src) \
         SELECT grp, SUM(val) AS total FROM base GROUP BY grp",
        Some("grp"),
        None,
        None,
        None,
    );

    // A generated child whose name `validate_view_name` rejects, so
    // `reconcile_one` returns its soft "ERROR: …" string rather than raising.
    // Non-empty `aggregations` so the BLOCKING-1 decomposed-node skip keeps it in
    // the recursion set.
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference \
             (name, graph_depth, depends_on, aggregations, enabled, is_generated_sub_imv) \
         VALUES ('dcf_top__cte_bad-name', 1, ARRAY['dcf_src'], \
                 '{\"is_passthrough\": true}'::jsonb, TRUE, TRUE)",
    )
    .expect("insert synthetic child");
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET depends_on_imv = ARRAY['dcf_top__cte_bad-name'] WHERE name = 'dcf_top'",
    )
    .expect("link child");

    let status = Spi::get_one::<String>("SELECT reflex_reconcile('dcf_top')")
        .expect("q")
        .expect("v");
    assert!(
        status.starts_with("ERROR"),
        "a failed generated-child rebuild must not be reported as success, got: {status}"
    );
}

/// PS-1 REVIEW — `generated_dependencies_shallowest_first` relies on `UNION` to
/// dedupe a generated child shared by two consumers, but every other fixture here
/// is a linear chain. The codebase has a real diamond
/// (`<root>__cte_date_limits`, `drop_ivm.rs:69-72`).
#[pg_test]
fn pg_reconcile_diamond_shares_one_generated_child() {
    Spi::run("CREATE TABLE dcd_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dcd_src SELECT g, 'g' || (g % 4), 1 FROM generate_series(1,80) g")
        .expect("seed");

    let res = crate::create_reflex_ivm(
        "dcd_top",
        "WITH shared AS (SELECT id, grp, val FROM dcd_src), \
              left_leg  AS (SELECT id, grp, val FROM shared), \
              right_leg AS (SELECT id, grp, val FROM shared) \
         SELECT l.grp, COUNT(*) AS cnt \
           FROM left_leg l JOIN right_leg r ON l.id = r.id GROUP BY l.grp",
        Some("grp"),
        None,
        None,
        None,
    );
    if !res.starts_with("CREATE REFLEX") {
        assert!(
            res.contains(crate::REFLEX_UNSUPPORTED_TAG),
            "unexpected create failure: {res}"
        );
        return;
    }

    // `shared` is read by both legs, so it must appear once in the reconcile set.
    let shared_consumers = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference \
          WHERE 'dcd_top__cte_shared' = ANY(depends_on_imv)",
    )
    .expect("q")
    .expect("v");
    assert_eq!(shared_consumers, 2, "fixture must be a real diamond");

    let status = Spi::get_one::<String>("SELECT reflex_rebuild_imv('dcd_top')")
        .expect("q")
        .expect("v");
    assert_eq!(status, "RECONCILED");

    assert_imv_correct(
        "dcd_top",
        "SELECT l.grp, COUNT(*) AS cnt FROM dcd_src l JOIN dcd_src r ON l.id = r.id \
         GROUP BY l.grp",
    );
}

/// PS-1 (D9) — the DISTINCT-ON / window half of the hard-coded-depth fix. The
/// set-op half is covered by `pg_decomposed_set_op_depth_reflects_operand_depth`;
/// these two paths used a literal `2` (`decompose.rs`), which is wrong as soon as
/// the `__base` sub-IMV is itself decomposed.
#[pg_test]
fn pg_decomposed_window_depth_reflects_base_depth() {
    Spi::run("CREATE TABLE dcw_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dcw_src VALUES (1,'a',10),(2,'a',20),(3,'b',30)").expect("seed");

    // The window base is itself CTE-decomposed, so `__base` lands at depth 2 and
    // the window VIEW above it must be 3 — not the old literal 2.
    let res = crate::create_reflex_ivm(
        "dcw_top",
        "WITH base AS (SELECT id, grp, val FROM dcw_src) \
         SELECT grp, SUM(val) AS total, ROW_NUMBER() OVER (ORDER BY grp) AS rn \
           FROM base GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    if !res.starts_with("CREATE REFLEX") {
        assert!(
            res.contains(crate::REFLEX_UNSUPPORTED_TAG),
            "unexpected create failure: {res}"
        );
        return;
    }

    let top_depth = Spi::get_one::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'dcw_top'",
    )
    .expect("q")
    .expect("v");
    let base_depth = Spi::get_one::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'dcw_top__base'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        top_depth,
        base_depth + 1,
        "window wrapper must sit exactly one level above its __base ({base_depth})"
    );
    assert!(
        base_depth >= 2,
        "the __base is itself decomposed, so it should be deeper than 1, got {base_depth}"
    );
}

/// PS-1 (D6-revised) — the "covered by a candidate" filter must NOT drop a
/// generated child whose consumer is fresh enough to be absent from the batch,
/// or that child would never be reconciled at all. This is the hole D6 cited as
/// the reason not to filter; the filter is keyed on coverage precisely to close it.
#[pg_test]
fn pg_scheduled_reconcile_keeps_generated_child_when_parent_is_fresh() {
    Spi::run("CREATE TABLE dcp_src (id INT, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO dcp_src SELECT g, 'g' || (g % 3), 1 FROM generate_series(1,60) g")
        .expect("seed");

    crate::create_reflex_ivm(
        "dcp_top",
        "WITH base AS (SELECT id, grp, val FROM dcp_src) \
         SELECT grp, COUNT(*) AS cnt FROM base GROUP BY grp",
        Some("grp"),
        None,
        None,
        None,
    );

    // Child stale, parent fresh: the parent is not a candidate, so nothing would
    // cover the child.
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET last_update_date = TIMESTAMP '2001-01-01 00:00:00' \
          WHERE name = 'dcp_top__cte_base'",
    )
    .expect("backdate child");
    Spi::run(
        "UPDATE public.__reflex_ivm_reference \
            SET last_update_date = CURRENT_TIMESTAMP WHERE name = 'dcp_top'",
    )
    .expect("freshen parent");

    let attempted = Spi::get_one::<String>(
        "SELECT string_agg(name, ',' ORDER BY name) FROM reflex_scheduled_reconcile(1) \
          WHERE name LIKE 'dcp_top%'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        attempted, "dcp_top__cte_base",
        "an uncovered stale generated child must still be reconciled"
    );
}

/// PS-1 REVIEW — `recompute_graph_depth` cannot converge on a dependency cycle,
/// and each run inflated `graph_depth` by `MAX_DEPTH_PASSES` (20 -> 40 -> …) while
/// still reporting `REPAIRED`. Acyclic idempotence is covered by
/// `pg_repair_dependency_graph_backfills_broken_rows`; this pins the cyclic case,
/// where the function must refuse to claim success.
#[pg_test]
fn pg_repair_dependency_graph_reports_non_convergence_on_a_cycle() {
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference (name, graph_depth, depends_on_imv) VALUES \
           ('rpc_a', 1, ARRAY['rpc_b']), \
           ('rpc_b', 1, ARRAY['rpc_a'])",
    )
    .expect("seed cycle");

    let summary = Spi::get_one::<String>("SELECT reflex_repair_dependency_graph()")
        .expect("q")
        .expect("v");
    assert!(
        summary.starts_with("WARNING"),
        "a non-converging graph must not be reported as repaired, got: {summary}"
    );
    assert!(
        summary.contains("cycle"),
        "the summary must name the cause, got: {summary}"
    );
}
