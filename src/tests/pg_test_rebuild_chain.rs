/// Bug 3: reflex_rebuild_chain CASCADE-drops dependents and recreates only the
/// named IMV. Without an explicit cascade it must refuse rather than destroy.
#[pg_test]
fn pg_rebuild_chain_refuses_with_dependents() {
    Spi::run("CREATE TABLE rc_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rc_src VALUES ('a', 1), ('b', 2)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rc_base', 'SELECT k, sum(v) AS s FROM rc_src GROUP BY k', 'k')")
        .expect("base");
    Spi::run("SELECT create_reflex_ivm('rc_dep', 'SELECT count(*) AS c FROM rc_base', 'c')")
        .expect("dep");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rc_base')")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(out.starts_with("ERROR"), "must refuse, got: {out}");
    assert!(out.contains("rc_dep"), "error must name the dependent: {out}");

    let dep_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rc_dep'",
    ).expect("dep query").unwrap_or(0);
    assert_eq!(dep_alive, 1, "the dependent must still exist after the refusal");
}

/// No dependents: behaviour is unchanged.
#[pg_test]
fn pg_rebuild_chain_succeeds_without_dependents() {
    Spi::run("CREATE TABLE rs_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rs_src VALUES ('a', 1)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rs_base', 'SELECT k, sum(v) AS s FROM rs_src GROUP BY k', 'k')")
        .expect("base");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rs_base')")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(!out.starts_with("ERROR"), "rebuild returned: {out}");

    let rows: i64 = Spi::get_one("SELECT count(*) FROM rs_base")
        .expect("count").unwrap_or(-1);
    assert_eq!(rows, 1, "rebuilt IMV must hold its rows");
}

/// With cascade => TRUE, dependents are recreated from their stored create_args.
#[pg_test]
fn pg_rebuild_chain_cascade_restores_dependents() {
    Spi::run("CREATE TABLE rcc_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rcc_src VALUES ('a', 1), ('b', 2)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rcc_base', 'SELECT k, sum(v) AS s FROM rcc_src GROUP BY k', 'k')")
        .expect("base");
    Spi::run("SELECT create_reflex_ivm('rcc_dep', 'SELECT count(*) AS c FROM rcc_base', 'c')")
        .expect("dep");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rcc_base', cascade => TRUE)")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(!out.starts_with("ERROR"), "cascade rebuild returned: {out}");

    let dep_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rcc_dep'",
    ).expect("dep query").unwrap_or(0);
    assert_eq!(dep_alive, 1, "dependent must be restored in the registry");

    let dep_rows: i64 = Spi::get_one("SELECT c::bigint FROM rcc_dep")
        .expect("dep rows").unwrap_or(-1);
    assert_eq!(dep_rows, 2, "restored dependent must hold correct data");
}

/// CASCADE drop recurses over the whole dependent tree, so cascade recreate must
/// restore TRANSITIVE dependents too, not just direct ones. A depth+2 dependent
/// dropped-but-not-recreated is silent data loss reported as success — exactly
/// the design's own 4-level motivating chain.
#[pg_test]
fn pg_rebuild_chain_cascade_restores_transitive_dependents() {
    Spi::run("CREATE TABLE rct_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rct_src VALUES ('a', 1), ('b', 2)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rct_base', 'SELECT k, sum(v) AS s FROM rct_src GROUP BY k', 'k')")
        .expect("base");
    // rct_mid depends on rct_base (depth+1); rct_leaf depends on rct_mid (depth+2).
    Spi::run("SELECT create_reflex_ivm('rct_mid', 'SELECT k, s FROM rct_base', 'k')")
        .expect("mid");
    Spi::run("SELECT create_reflex_ivm('rct_leaf', 'SELECT count(*) AS c FROM rct_mid', 'c')")
        .expect("leaf");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rct_base', cascade => TRUE)")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(!out.starts_with("ERROR"), "cascade rebuild returned: {out}");

    let mid_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rct_mid'",
    ).expect("mid query").unwrap_or(0);
    assert_eq!(mid_alive, 1, "direct dependent must be restored");

    let leaf_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rct_leaf'",
    ).expect("leaf query").unwrap_or(0);
    assert_eq!(leaf_alive, 1, "transitive (depth+2) dependent must be restored, not silently lost");

    let leaf_rows: i64 = Spi::get_one("SELECT c::bigint FROM rct_leaf")
        .expect("leaf rows").unwrap_or(-1);
    assert_eq!(leaf_rows, 2, "restored transitive dependent must hold correct data");
}

/// A dependent predating create_args (1.10.8) cannot be faithfully recreated.
/// Recreating it from `{}` would silently reset storage/refresh/partitioning,
/// which is the same data-loss shape as the bug. It must refuse.
#[pg_test]
fn pg_rebuild_chain_cascade_refuses_null_create_args() {
    Spi::run("CREATE TABLE rcn_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rcn_src VALUES ('a', 1)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rcn_base', 'SELECT k, sum(v) AS s FROM rcn_src GROUP BY k', 'k')")
        .expect("base");
    Spi::run("SELECT create_reflex_ivm('rcn_dep', 'SELECT count(*) AS c FROM rcn_base', 'c')")
        .expect("dep");
    Spi::run("UPDATE public.__reflex_ivm_reference SET create_args = NULL WHERE name = 'rcn_dep'")
        .expect("simulate pre-1.10.8 dependent");

    let out = Spi::get_one::<String>("SELECT reflex_rebuild_chain('rcn_base', cascade => TRUE)")
        .expect("rebuild call")
        .expect("rebuild result");
    assert!(out.starts_with("ERROR"), "must refuse, got: {out}");
    assert!(out.contains("rcn_dep"), "error must name the dependent: {out}");
}

/// PS-2 Part 1 — the NAMED IMV is protected by the same fail-closed check as its
/// dependents. A row predating create_args (1.10.8) has no faithful spec, so
/// recreating it would silently reset storage/refresh/partitioning — the very
/// downgrade the dependents check warns about. It must refuse UP FRONT, before
/// any drop, leaving the IMV intact.
#[pg_test]
fn pg_rebuild_chain_refuses_named_null_create_args() {
    Spi::run("CREATE TABLE rnn_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rnn_src VALUES ('a', 1), ('b', 2)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rnn_agg', 'SELECT k, sum(v) AS s FROM rnn_src GROUP BY k', 'k')")
        .expect("agg");
    Spi::run("UPDATE public.__reflex_ivm_reference SET create_args = NULL WHERE name = 'rnn_agg'")
        .expect("simulate pre-1.10.8 named row");

    let out = crate::reflex_rebuild_chain("rnn_agg", false);
    assert!(out.starts_with("ERROR"), "named IMV with no create_args must refuse, got: {out}");
    assert!(out.contains("rnn_agg"), "error must name the IMV: {out}");

    // Nothing may be dropped: the target table and registry row survive intact.
    let alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rnn_agg'",
    )
    .expect("q")
    .unwrap_or(0);
    assert_eq!(alive, 1, "the refused IMV's registry row must survive");
    let rows: i64 = Spi::get_one("SELECT count(*) FROM rnn_agg").expect("q").unwrap_or(-1);
    assert_eq!(rows, 2, "the refused IMV's target table must be untouched");
}

/// PS-2 Part 2 (D22) — reflex_rebuild_chain hard-errored on every CTE-decomposed
/// parent: the parent's stored sql_query is the REWRITTEN body naming the
/// generated `<root>__cte_<alias>` child, so drop-CASCADE removes that child and
/// the recreate then references a relation that no longer exists ("relation
/// ...cte_base does not exist"), aborting the transaction. The primitive must
/// instead REFUSE cleanly before any drop, pointing at reflex_reconcile (the
/// recursive, correct recovery since 1.11.0) and leaving the chain intact.
#[pg_test]
fn pg_rebuild_chain_refuses_on_cte_decomposed_parent() {
    Spi::run("CREATE TABLE rcd_src (id INT, grp TEXT, val NUMERIC)").expect("src");
    Spi::run("INSERT INTO rcd_src VALUES (1,'a',10),(2,'a',20),(3,'b',30)").expect("seed");

    let created = crate::create_reflex_ivm(
        "rcd_agg",
        "WITH base AS (SELECT id, grp, val FROM rcd_src) \
         SELECT grp, SUM(val) AS total FROM base GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");

    // Sanity: the parent really is CTE-decomposed (generated child present).
    let child_present: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rcd_agg__cte_base'",
    )
    .expect("q")
    .unwrap_or(0);
    assert_eq!(child_present, 1, "fixture must be CTE-decomposed");

    // Pre-fix this drops the parent + child then aborts on the vanished relation.
    let out = crate::reflex_rebuild_chain("rcd_agg", false);
    assert!(
        out.starts_with("ERROR"),
        "rebuild_chain on a decomposed parent must refuse cleanly, got: {out}"
    );
    assert!(
        out.contains("reflex_reconcile"),
        "the refusal must point at reflex_reconcile as the recovery, got: {out}"
    );

    // Nothing dropped: parent, generated child and the parent's data all survive.
    let parent_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rcd_agg'",
    )
    .expect("q")
    .unwrap_or(0);
    assert_eq!(parent_alive, 1, "the decomposed parent must not be dropped");
    let child_alive: i64 = Spi::get_one(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name = 'rcd_agg__cte_base'",
    )
    .expect("q")
    .unwrap_or(0);
    assert_eq!(child_alive, 1, "the generated child must not be dropped");
    let data: i64 = Spi::get_one("SELECT count(*) FROM rcd_agg").expect("q").unwrap_or(-1);
    assert_eq!(data, 2, "the parent's data must be untouched");
}

/// PS-2 Part 2b (backfill) — a legacy row (create_args NULL) carrying DEFERRED /
/// partitioning in its dedicated registry columns must, after the 1.11.0
/// backfill, expose a create_args that reconstructs those fields and is marked
/// `"backfilled": true`, so reflex_rebuild_chain preserves DEFERRED rather than
/// silently resetting it to IMMEDIATE. This mirrors the migration's backfill
/// UPDATE (the .sql delta is not executed by the pgrx test harness).
#[pg_test]
fn pg_rebuild_chain_backfill_reconstructs_deferred_from_columns() {
    Spi::run("CREATE TABLE rbf_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rbf_src VALUES ('a', 1), ('b', 2)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rbf_agg', 'SELECT k, sum(v) AS s FROM rbf_src GROUP BY k', 'k', 'UNLOGGED', 'DEFERRED')")
        .expect("agg");
    // Simulate a pre-1.10.8 row: create_args absent, but the dedicated columns
    // (unique_columns/index_columns, storage_mode, refresh_mode) still describe it.
    Spi::run("UPDATE public.__reflex_ivm_reference SET create_args = NULL WHERE name = 'rbf_agg'")
        .expect("null create_args");

    // The backfill UPDATE shipped in the PS-2 migration section.
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET create_args = json_build_object( \
             'unique_columns_str', array_to_string( \
                 CASE WHEN unique_columns IS NOT NULL AND cardinality(unique_columns) > 0 \
                      THEN unique_columns ELSE index_columns END, ','), \
             'storage_mode', COALESCE(storage_mode, 'UNLOGGED'), \
             'refresh_mode', COALESCE(refresh_mode, 'IMMEDIATE'), \
             'ignore_sources', to_json(COALESCE(ignored_sources, ARRAY[]::TEXT[])), \
             'partition_by', to_json(COALESCE(partition_columns, ARRAY[]::TEXT[])), \
             'backfilled', TRUE)::text \
           WHERE create_args IS NULL \
             AND COALESCE(is_generated_sub_imv, FALSE) = FALSE \
             AND COALESCE(aggregations::text, '{}') <> '{}'",
    )
    .expect("backfill");

    let refresh = Spi::get_one::<String>(
        "SELECT (create_args::jsonb)->>'refresh_mode' FROM public.__reflex_ivm_reference WHERE name = 'rbf_agg'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(refresh, "DEFERRED", "backfill must recover the DEFERRED refresh mode");

    let marked = Spi::get_one::<bool>(
        "SELECT (create_args::jsonb)->>'backfilled' = 'true' FROM public.__reflex_ivm_reference WHERE name = 'rbf_agg'",
    )
    .expect("q")
    .expect("v");
    assert!(marked, "backfilled rows must be honestly marked");

    let uniq = Spi::get_one::<String>(
        "SELECT (create_args::jsonb)->>'unique_columns_str' FROM public.__reflex_ivm_reference WHERE name = 'rbf_agg'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(uniq, "k", "backfill must recover the declared unique key");

    // And a rebuild from the backfilled row must preserve DEFERRED, not reset it.
    let out = crate::reflex_rebuild_chain("rbf_agg", false);
    assert!(!out.starts_with("ERROR"), "rebuild of a backfilled row must succeed, got: {out}");
    let mode_after = Spi::get_one::<String>(
        "SELECT refresh_mode FROM public.__reflex_ivm_reference WHERE name = 'rbf_agg'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(mode_after, "DEFERRED", "rebuild must preserve DEFERRED from the backfilled spec");
}

/// Finding 1 (BLOCKER): the migration backfill cannot reconstruct `topk_k` or
/// `explicit_unpartitioned` from any registry column, so a rebuild from a
/// backfilled row silently resets them to their create-time defaults (no top-K
/// bound; auto-partitioning) — a result-content / layout change on the recovery
/// path. The `"backfilled": true` marker must therefore be load-bearing:
/// reflex_rebuild_chain must surface a WARNING when it rebuilds from such a row,
/// and must NOT warn when create_args was genuinely populated at create time.
#[pg_test]
fn pg_rebuild_chain_backfilled_row_warns_lossy_fields() {
    // A genuinely-populated create_args (normal create) must NOT warn.
    Spi::run("CREATE TABLE rbw_clean_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rbw_clean_src VALUES ('a', 1)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rbw_clean', 'SELECT k, sum(v) AS s FROM rbw_clean_src GROUP BY k', 'k')")
        .expect("clean");
    let clean_out = crate::reflex_rebuild_chain("rbw_clean", false);
    assert!(!clean_out.starts_with("ERROR"), "clean rebuild returned: {clean_out}");
    assert!(
        !clean_out.contains("WARNING"),
        "a genuinely-populated create_args must not warn, got: {clean_out}"
    );

    // A backfilled row MUST warn that topk_k / explicit_unpartitioned were defaulted.
    Spi::run("CREATE TABLE rbw_bf_src (k TEXT, v INT)").expect("src");
    Spi::run("INSERT INTO rbw_bf_src VALUES ('a', 1), ('b', 2)").expect("seed");
    Spi::run("SELECT create_reflex_ivm('rbw_bf', 'SELECT k, sum(v) AS s FROM rbw_bf_src GROUP BY k', 'k')")
        .expect("bf");
    Spi::run("UPDATE public.__reflex_ivm_reference SET create_args = NULL WHERE name = 'rbw_bf'")
        .expect("null create_args");
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET create_args = json_build_object( \
             'unique_columns_str', array_to_string( \
                 CASE WHEN unique_columns IS NOT NULL AND cardinality(unique_columns) > 0 \
                      THEN unique_columns ELSE index_columns END, ','), \
             'storage_mode', COALESCE(storage_mode, 'UNLOGGED'), \
             'refresh_mode', COALESCE(refresh_mode, 'IMMEDIATE'), \
             'ignore_sources', to_json(COALESCE(ignored_sources, ARRAY[]::TEXT[])), \
             'partition_by', to_json(COALESCE(partition_columns, ARRAY[]::TEXT[])), \
             'backfilled', TRUE)::text \
           WHERE create_args IS NULL \
             AND COALESCE(is_generated_sub_imv, FALSE) = FALSE \
             AND COALESCE(aggregations::text, '{}') <> '{}'",
    )
    .expect("backfill");

    let bf_out = crate::reflex_rebuild_chain("rbw_bf", false);
    assert!(!bf_out.starts_with("ERROR"), "backfilled rebuild returned: {bf_out}");
    assert!(
        bf_out.contains("WARNING"),
        "rebuild from a backfilled row must warn, got: {bf_out}"
    );
    assert!(
        bf_out.contains("topk_k") && bf_out.contains("explicit_unpartitioned"),
        "the warning must name the two unreconstructible fields, got: {bf_out}"
    );
}

/// Finding 4: the backfill round-trip must also preserve partitioning, not only
/// DEFERRED + the unique key. A partitioned aggregate IMV whose create_args is
/// nulled (pre-1.10.8 shape) then backfilled from `partition_columns` must,
/// after reflex_rebuild_chain, come back as a partitioned target with the same
/// partition key.
#[pg_test]
fn pg_rebuild_chain_backfill_reconstructs_partitioning_from_columns() {
    Spi::run(
        "CREATE TABLE rbp_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region)",
    )
    .expect("partitioned source");
    Spi::run("CREATE TABLE rbp_src_north PARTITION OF rbp_src FOR VALUES IN ('NORTH')")
        .expect("child north");
    Spi::run("CREATE TABLE rbp_src_south PARTITION OF rbp_src FOR VALUES IN ('SOUTH')")
        .expect("child south");
    Spi::run("INSERT INTO rbp_src (id, region, amount) VALUES (1,'NORTH',100),(2,'NORTH',200),(3,'SOUTH',50)")
        .expect("seed");
    let create_result = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('rbp_view', \
             'SELECT region, SUM(amount) AS total FROM rbp_src GROUP BY region', \
             NULL, NULL, NULL, NULL, ARRAY['region'])",
    )
    .expect("create call")
    .expect("create result");
    assert!(!create_result.starts_with("ERROR"), "create returned: {create_result}");

    // Simulate a pre-1.10.8 row and apply the migration backfill.
    Spi::run("UPDATE public.__reflex_ivm_reference SET create_args = NULL WHERE name = 'rbp_view'")
        .expect("null create_args");
    Spi::run(
        "UPDATE public.__reflex_ivm_reference SET create_args = json_build_object( \
             'unique_columns_str', array_to_string( \
                 CASE WHEN unique_columns IS NOT NULL AND cardinality(unique_columns) > 0 \
                      THEN unique_columns ELSE index_columns END, ','), \
             'storage_mode', COALESCE(storage_mode, 'UNLOGGED'), \
             'refresh_mode', COALESCE(refresh_mode, 'IMMEDIATE'), \
             'ignore_sources', to_json(COALESCE(ignored_sources, ARRAY[]::TEXT[])), \
             'partition_by', to_json(COALESCE(partition_columns, ARRAY[]::TEXT[])), \
             'backfilled', TRUE)::text \
           WHERE create_args IS NULL \
             AND COALESCE(is_generated_sub_imv, FALSE) = FALSE \
             AND COALESCE(aggregations::text, '{}') <> '{}'",
    )
    .expect("backfill");

    let part_by = Spi::get_one::<String>(
        "SELECT (create_args::jsonb)->'partition_by'->>0 FROM public.__reflex_ivm_reference WHERE name = 'rbp_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(part_by, "region", "backfill must recover the partition key");

    let out = crate::reflex_rebuild_chain("rbp_view", false);
    assert!(!out.starts_with("ERROR"), "rebuild of a partitioned backfilled row must succeed, got: {out}");

    let strategy = Spi::get_one::<String>(
        "SELECT pt.partstrat::text FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'rbp_view'",
    )
    .expect("strategy query")
    .expect("strategy");
    assert_eq!(strategy, "l", "rebuilt target must stay LIST partitioned");

    let part_cols: Vec<String> = Spi::get_one::<Vec<String>>(
        "SELECT partition_columns FROM public.__reflex_ivm_reference WHERE name = 'rbp_view'",
    )
    .expect("catalog query")
    .expect("part_cols");
    assert_eq!(part_cols, vec!["region".to_string()], "rebuild must preserve the partition key");
}
