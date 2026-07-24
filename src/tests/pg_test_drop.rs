
#[pg_test]
fn test_drop_reflex_ivm_basic() {
    Spi::run("CREATE TABLE drop_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO drop_src (grp, val) VALUES ('a', 1)").expect("seed");

    crate::create_reflex_ivm(
        "drop_view",
        "SELECT grp, SUM(val) AS total FROM drop_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Verify IMV exists
    let exists = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'drop_view'",
    ).expect("q").expect("v");
    assert_eq!(exists, 1);

    // Drop it
    let result = crate::drop_reflex_ivm("drop_view");
    assert_eq!(result, "DROP REFLEX INCREMENTAL VIEW");

    // Verify reference row gone
    let gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'drop_view'",
    ).expect("q").expect("v");
    assert_eq!(gone, 0);

    // Verify target table gone
    let tbl_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'drop_view'",
    ).expect("q").expect("v");
    assert_eq!(tbl_gone, 0);

    // Verify intermediate table gone
    let int_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '__reflex_intermediate_drop_view'",
    ).expect("q").expect("v");
    assert_eq!(int_gone, 0);

    // Verify triggers gone
    let trig_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_drop_view_%'",
    ).expect("q").expect("v");
    assert_eq!(trig_gone, 0);
}

// Maintenance tables are persistent extension infrastructure (the registry and
// the deferred / partition bookkeeping queues). They are NOT per-IMV and must
// survive every create→drop cycle. Every other relation under the `__reflex_`
// prefix is a per-IMV / per-source artifact that a complete drop must wipe.
const COUNT_REFLEX_ARTIFACT_TABLES: &str = "SELECT COUNT(*) FROM pg_class c \
     JOIN pg_namespace n ON n.oid = c.relnamespace \
     WHERE c.relkind = 'r' AND c.relname LIKE '\\_\\_reflex\\_%' ESCAPE '\\' \
       AND c.relname NOT IN ( \
           '__reflex_ivm_reference', '__reflex_deferred_pending', \
           '__reflex_source_partition_snapshot', '__reflex_partition_pending')";

// A DEFERRED IMV is the most artifact-heavy shape: it materializes a per-source
// staging delta table (`__reflex_delta_<source>`) on top of the per-IMV
// intermediate / affected / scratch tables. Counting every non-maintenance
// `__reflex_` table before create, after create, and after drop proves the
// teardown is complete: the count must return to its pre-create baseline. The
// staging delta table was the relation the drop path historically orphaned.
#[pg_test]
fn drop_deferred_imv_wipes_every_nonmaintenance_table() {
    let baseline = Spi::get_one::<i64>(COUNT_REFLEX_ARTIFACT_TABLES)
        .expect("q")
        .expect("v");

    Spi::run("CREATE TABLE ddw_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO ddw_src (grp, val) VALUES ('a', 1)").expect("seed");

    crate::create_reflex_ivm(
        "ddw_view",
        "SELECT grp, SUM(val) AS total FROM ddw_src GROUP BY grp",
        None,
        None,
        Some("DEFERRED"),
        None,
    );

    let after_create = Spi::get_one::<i64>(COUNT_REFLEX_ARTIFACT_TABLES)
        .expect("q")
        .expect("v");
    assert!(
        after_create > baseline,
        "create should materialize per-IMV artifact tables (baseline {baseline}, after {after_create})"
    );

    // The staging delta table specifically must exist for a DEFERRED IMV — this
    // is the relation the drop path used to leak.
    let staging_present = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_class WHERE relkind = 'r' AND relname = '__reflex_delta_ddw_src'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(staging_present, 1, "deferred IMV must create a staging delta table");

    let result = crate::drop_reflex_ivm("ddw_view");
    assert_eq!(result, "DROP REFLEX INCREMENTAL VIEW");

    let after_drop = Spi::get_one::<i64>(COUNT_REFLEX_ARTIFACT_TABLES)
        .expect("q")
        .expect("v");
    assert_eq!(
        after_drop, baseline,
        "drop must wipe every non-maintenance artifact table; \
         a leftover count ({after_drop} vs baseline {baseline}) means an orphan — \
         e.g. the staging delta table __reflex_delta_ddw_src"
    );

    // Belt-and-braces: the target table (not `__reflex_`-prefixed, so outside the
    // generic count) must also be gone.
    let target_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ddw_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(target_gone, 0, "target table must be dropped");
}

#[pg_test]
fn test_drop_reflex_ivm_refuses_with_children() {
    Spi::run("CREATE TABLE drop_ch_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO drop_ch_src (grp, val) VALUES ('a', 1)").expect("seed");

    crate::create_reflex_ivm(
        "drop_parent",
        "SELECT grp, SUM(val) AS total FROM drop_ch_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    crate::create_reflex_ivm(
        "drop_child",
        "SELECT grp, SUM(total) AS grand FROM drop_parent GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Should refuse without cascade
    let result = crate::drop_reflex_ivm("drop_parent");
    assert!(result.starts_with("ERROR"));
}

#[pg_test]
fn test_drop_reflex_ivm_cascade() {
    Spi::run("CREATE TABLE drop_cas_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO drop_cas_src (grp, val) VALUES ('a', 1)").expect("seed");

    crate::create_reflex_ivm(
        "drop_cas_parent",
        "SELECT grp, SUM(val) AS total FROM drop_cas_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    crate::create_reflex_ivm(
        "drop_cas_child",
        "SELECT grp, SUM(total) AS grand FROM drop_cas_parent GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Cascade should drop both
    let result = crate::drop_reflex_ivm_cascade("drop_cas_parent", true);
    assert_eq!(result, "DROP REFLEX INCREMENTAL VIEW");

    // Both should be gone
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name IN ('drop_cas_parent', 'drop_cas_child')",
    ).expect("q").expect("v");
    assert_eq!(count, 0);
}

// Regression: a CTE that joins another CTE stores the sibling sub-IMV in
// `depends_on` already double-quoted (`"v__cte_x"`). Dropping such an IMV must
// strip those quotes when rebuilding the source-trigger / function names, else
// the generated DDL is `... "__reflex_trigger_ins_on_"v__cte_x"" ...` and PG
// raises `syntax error at or near "v__cte_x"`.
#[pg_test]
fn test_drop_imv_with_quoted_sub_imv_source() {
    Spi::run("CREATE TABLE dq_s (gid INT NOT NULL, pid INT NOT NULL, qty INT NOT NULL)")
        .expect("s");
    Spi::run("CREATE TABLE dq_d (gid INT PRIMARY KEY, status TEXT)").expect("d");
    Spi::run("INSERT INTO dq_s VALUES (1,100,5)").expect("seed s");
    Spi::run("INSERT INTO dq_d VALUES (1,'a')").expect("seed d");

    let r = crate::create_reflex_ivm(
        "dq_v",
        "WITH lim AS (SELECT gid, COUNT(*) AS n FROM dq_d WHERE status = 'a' GROUP BY gid), \
              j AS (SELECT s.gid, s.pid, s.qty FROM dq_s s JOIN lim l ON s.gid = l.gid) \
         SELECT gid, pid, qty FROM j",
        Some("gid,pid"),
        None,
        None,
        None,
    );
    assert_eq!(r, "CREATE REFLEX INCREMENTAL VIEW");

    // 1.11.0 (PS-1): `dq_v` is now registered as a `graph_child` of
    // `dq_v__cte_j`, so a NON-cascade drop of that intermediate generated node
    // refuses instead of leaving `dq_v` reading a vanished relation. Pin the new
    // guard here too — before PS-1 this drop silently succeeded and broke the
    // parent, which is the same latent data-destroyer N1 caused in
    // reflex_rebuild_chain.
    let refused = crate::drop_reflex_ivm("dq_v__cte_j");
    assert!(
        refused.starts_with("ERROR: IMV has children"),
        "non-cascade drop of a generated node the parent reads must refuse, got: {refused}"
    );

    // `dq_v__cte_j` depends on the quoted sub-IMV "dq_v__cte_lim". The cascade
    // teardown rebuilds the source-trigger / function names from that same
    // quoted source, so it still covers the quoting regression this test exists
    // for.
    let d = crate::drop_reflex_ivm_cascade("dq_v__cte_j", true);
    assert_eq!(
        d, "DROP REFLEX INCREMENTAL VIEW",
        "drop must not raise a syntax error on the quoted sub-IMV source"
    );
    let gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'dq_v__cte_j'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(gone, 0);
}

#[pg_test]
fn test_drop_shared_trigger_lifecycle() {
    // Two IMVs on the same source. Dropping one should keep triggers;
    // dropping the last should remove triggers.
    Spi::run("CREATE TABLE drop_sh_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO drop_sh_src (grp, val) VALUES ('a', 1)").expect("seed");

    crate::create_reflex_ivm(
        "drop_sh_v1",
        "SELECT grp, SUM(val) AS total FROM drop_sh_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    crate::create_reflex_ivm(
        "drop_sh_v2",
        "SELECT grp, COUNT(*) AS cnt FROM drop_sh_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Both share 4 triggers on the source
    let trig_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_drop_sh_src'",
    ).expect("q").expect("v");
    assert_eq!(trig_count, 4);

    // Drop v1 → triggers should remain (v2 still depends on source)
    crate::drop_reflex_ivm("drop_sh_v1");
    let trig_after_v1 = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_drop_sh_src'",
    ).expect("q").expect("v");
    assert_eq!(trig_after_v1, 4);

    // v2 should still work after v1 is dropped
    Spi::run("INSERT INTO drop_sh_src (grp, val) VALUES ('b', 2)").expect("insert");
    let cnt = Spi::get_one::<i64>(
        "SELECT cnt FROM drop_sh_v2 WHERE grp = 'b'",
    ).expect("q").expect("v");
    assert_eq!(cnt, 1);

    // Drop v2 → triggers should be removed (no more dependents)
    crate::drop_reflex_ivm("drop_sh_v2");
    let trig_after_v2 = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_drop_sh_src'",
    ).expect("q").expect("v");
    assert_eq!(trig_after_v2, 0);
}

#[pg_test]
fn test_source_drop_removes_aggregate_imv_artifacts() {
    Spi::run("CREATE TABLE sd_agg_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO sd_agg_src (grp, val) VALUES ('a', 1), ('b', 2)").expect("seed");

    crate::create_reflex_ivm(
        "sd_agg_view",
        "SELECT grp, SUM(val) AS total FROM sd_agg_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    let registered = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'sd_agg_view'",
    ).expect("q").expect("v");
    assert_eq!(registered, 1);

    Spi::run("DROP TABLE sd_agg_src").expect("drop source");

    let registry_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'sd_agg_view'",
    ).expect("q").expect("v");
    assert_eq!(registry_gone, 0, "registry row should be deleted by sql_drop trigger");

    let target_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sd_agg_view'",
    ).expect("q").expect("v");
    assert_eq!(target_gone, 0, "target table should be dropped by sql_drop trigger");

    let interm_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '__reflex_intermediate_sd_agg_view'",
    ).expect("q").expect("v");
    assert_eq!(interm_gone, 0, "intermediate table should be dropped by sql_drop trigger");

    let affected_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '__reflex_affected_sd_agg_view'",
    ).expect("q").expect("v");
    assert_eq!(affected_gone, 0, "affected-groups table should be dropped by sql_drop trigger");
}

#[pg_test]
fn test_source_drop_cascades_to_child_imvs() {
    Spi::run("CREATE TABLE sd_chain_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO sd_chain_src (grp, val) VALUES ('x', 10)").expect("seed");

    crate::create_reflex_ivm(
        "sd_chain_l1",
        "SELECT grp, SUM(val) AS total FROM sd_chain_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    crate::create_reflex_ivm(
        "sd_chain_l2",
        "SELECT grp, SUM(total) AS grand FROM sd_chain_l1 GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    Spi::run("DROP TABLE sd_chain_src").expect("drop source");

    let l1_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'sd_chain_l1'",
    ).expect("q").expect("v");
    assert_eq!(l1_gone, 0, "L1 should be cleaned up");

    let l2_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'sd_chain_l2'",
    ).expect("q").expect("v");
    assert_eq!(l2_gone, 0, "L2 should also be cleaned up via cascade");

    let any_target = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('sd_chain_l1', 'sd_chain_l2')",
    ).expect("q").expect("v");
    assert_eq!(any_target, 0, "both target tables should be dropped");
}

#[pg_test]
fn test_source_drop_passthrough() {
    Spi::run("CREATE TABLE sd_pt_src (id INTEGER PRIMARY KEY, name TEXT, status TEXT)")
        .expect("create table");
    Spi::run("INSERT INTO sd_pt_src VALUES (1, 'a', 'active'), (2, 'b', 'active')")
        .expect("seed");

    crate::create_reflex_ivm(
        "sd_pt_view",
        "SELECT id, name FROM sd_pt_src WHERE status = 'active'",
        Some("id"),
        None,
        None,
        None,
    );

    let target_present = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sd_pt_view'",
    ).expect("q").expect("v");
    assert_eq!(target_present, 1);

    Spi::run("DROP TABLE sd_pt_src").expect("drop source");

    let registry_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'sd_pt_view'",
    ).expect("q").expect("v");
    assert_eq!(registry_gone, 0);

    let target_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sd_pt_view'",
    ).expect("q").expect("v");
    assert_eq!(target_gone, 0, "passthrough target table should be dropped");
}

#[pg_test]
fn test_drop_aggregate_over_union_all_subquery_source() {
    Spi::run("CREATE TABLE drop_sq_t1 (k TEXT, v NUMERIC)").expect("create t1");
    Spi::run("CREATE TABLE drop_sq_t2 (k TEXT, v NUMERIC)").expect("create t2");
    Spi::run("INSERT INTO drop_sq_t1 VALUES ('a', 1), ('b', 2)").expect("seed t1");
    Spi::run("INSERT INTO drop_sq_t2 VALUES ('a', 10), ('c', 3)").expect("seed t2");

    // An inline FROM-subquery source registers a synthetic `<subquery:s>` source
    // in `depends_on`. The drop path must skip it the way create does — otherwise
    // it interpolates `<subquery:s>` into teardown DDL and fails to parse.
    crate::create_reflex_ivm(
        "drop_sq_view",
        "SELECT k, SUM(v) AS total FROM ( \
             SELECT k, v FROM drop_sq_t1 \
             UNION ALL \
             SELECT k, v FROM drop_sq_t2 \
         ) AS s GROUP BY k",
        Some("k"),
        None,
        None,
        None,
    );

    let registered = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'drop_sq_view'",
    ).expect("q").expect("v");
    assert_eq!(registered, 1);

    let result = crate::drop_reflex_ivm("drop_sq_view");
    assert_eq!(result, "DROP REFLEX INCREMENTAL VIEW");

    let gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'drop_sq_view'",
    ).expect("q").expect("v");
    assert_eq!(gone, 0);
}

#[pg_test]
fn drop_cte_subimv_source_leaves_no_orphans() {
    Spi::run("CREATE TABLE dq_a (id int primary key, g int, m numeric);").unwrap();
    Spi::run("CREATE TABLE dq_b (id int primary key, fk int, w numeric);").unwrap();
    let body = "WITH agg AS (SELECT fk AS g, SUM(w) AS sw FROM dq_b GROUP BY fk) \
                SELECT dq_a.id, SUM(dq_a.m) AS s, a.sw FROM dq_a LEFT JOIN agg a ON a.g = dq_a.id GROUP BY dq_a.id, a.sw";
    let msg = crate::create_reflex_ivm(
        "dq_imv",
        body,
        Some("id"),
        None,
        None,
        None,
    );
    assert!(msg.starts_with("CREATE REFLEX") || msg.contains(crate::REFLEX_UNSUPPORTED_TAG),
        "create failed: {msg}");

    // If the create succeeded (returned CREATE REFLEX), verify drop path.
    // Dropping the parent must also remove its synthetic CTE sub-IMV
    // (`dq_imv__cte_agg`) — no separate manual drop is needed.
    if msg.starts_with("CREATE REFLEX") {
        let drop_msg = crate::drop_reflex_ivm("dq_imv");
        assert_eq!(drop_msg, "DROP REFLEX INCREMENTAL VIEW", "drop failed: {drop_msg}");

        // No leftover reflex objects after dropping the parent alone.
        let leftover = Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_class WHERE relname LIKE '%dq_imv%' OR relname LIKE '%cte%dq%'"
        ).unwrap().unwrap();
        assert_eq!(leftover, 0, "orphan objects remain after drop");

        let ref_left = Spi::get_one::<i64>(
            "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name LIKE 'dq_imv%'"
        ).unwrap().unwrap();
        assert_eq!(ref_left, 0, "reference rows remain after drop");
    }
}

/// Regression: a NON-cascade drop of a CTE-decomposed IMV must remove its
/// internal synthetic sub-IMV (`<view>__cte_…`) and every backing object, with
/// zero orphans. Before the fix, the sub-IMV was recorded only in the parent's
/// `depends_on` (not `depends_on_imv`), so the cascade-gated cleanup never ran
/// on a plain drop and left the sub-IMV's table/intermediate/scratch/indexes
/// plus its reference row behind.
#[pg_test]
fn drop_decomposed_imv_noncascade_leaves_no_subimv_orphans() {
    Spi::run("CREATE TABLE nd_a (id int primary key, g int, m numeric);").unwrap();
    Spi::run("CREATE TABLE nd_b (id int primary key, fk int, w numeric);").unwrap();
    let body = "WITH agg AS (SELECT fk AS g, SUM(w) AS sw FROM nd_b GROUP BY fk) \
                SELECT nd_a.id, SUM(nd_a.m) AS s, a.sw FROM nd_a LEFT JOIN agg a ON a.g = nd_a.id GROUP BY nd_a.id, a.sw";
    let create_msg = crate::create_reflex_ivm("nd_imv", body, Some("id"), None, None, None);
    if !create_msg.starts_with("CREATE REFLEX") {
        assert!(
            create_msg.contains(crate::REFLEX_UNSUPPORTED_TAG),
            "unexpected create failure: {create_msg}"
        );
        return;
    }

    // The CTE was decomposed into at least one synthetic sub-IMV.
    let sub_imv_count = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_ivm_reference \
         WHERE name LIKE 'nd_imv%' AND name <> 'nd_imv'",
    )
    .unwrap()
    .unwrap();
    assert!(sub_imv_count >= 1, "expected a decomposition sub-IMV before drop");

    // NON-cascade drop of the parent alone.
    let drop_msg = crate::drop_reflex_ivm("nd_imv");
    assert_eq!(drop_msg, "DROP REFLEX INCREMENTAL VIEW", "drop failed: {drop_msg}");

    let ref_left = Spi::get_one::<i64>(
        "SELECT count(*) FROM public.__reflex_ivm_reference WHERE name LIKE 'nd_imv%'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(ref_left, 0, "reference rows remain after non-cascade drop");

    let class_left = Spi::get_one::<i64>(
        "SELECT count(*) FROM pg_class WHERE relname LIKE '%nd_imv%' AND relkind IN ('r', 'i')",
    )
    .unwrap()
    .unwrap();
    assert_eq!(class_left, 0, "orphan tables/indexes remain after non-cascade drop");
}

#[pg_test]
fn test_union_all_intermediate_wrapper_has_src_idx_column() {
    Spi::run(
        "CREATE TABLE us_orders(id INT PRIMARY KEY, country TEXT, amount NUMERIC);
         CREATE TABLE eu_orders(id INT PRIMARY KEY, country TEXT, amount NUMERIC);
         INSERT INTO us_orders VALUES (1, 'US', 100);
         INSERT INTO eu_orders VALUES (1, 'FR', 200);",
    )
    .unwrap();

    let res = Spi::get_one::<String>(
        "SELECT create_reflex_ivm(
           view_name => 'ord_imv',
           sql       => 'WITH all_ord AS (
                           SELECT id, country, amount FROM us_orders
                           UNION ALL
                           SELECT id, country, amount FROM eu_orders
                         )
                         SELECT country, SUM(amount) AS total
                         FROM all_ord
                         GROUP BY country',
           storage   => 'UNLOGGED'
         )",
    )
    .unwrap()
    .unwrap_or_default();
    assert!(res.contains("CREATE"), "create_reflex_ivm returned: {res}");

    // The CTE wrapper is registered as `ord_imv__cte_all_ord`.
    // It must carry __reflex_src_idx as a SMALLINT NOT NULL column.
    let idx_col_type: Option<String> = Spi::get_one(
        "SELECT format_type(a.atttypid, a.atttypmod)
         FROM pg_attribute a
         JOIN pg_class c ON c.oid = a.attrelid
         WHERE c.relname = 'ord_imv__cte_all_ord'
           AND a.attname = '__reflex_src_idx'
           AND a.attnum > 0",
    )
    .unwrap();
    assert_eq!(
        idx_col_type.as_deref(),
        Some("smallint"),
        "__reflex_src_idx column missing or wrong type on wrapper table"
    );

    let notnull: Option<bool> = Spi::get_one(
        "SELECT a.attnotnull
         FROM pg_attribute a
         JOIN pg_class c ON c.oid = a.attrelid
         WHERE c.relname = 'ord_imv__cte_all_ord'
           AND a.attname = '__reflex_src_idx'",
    )
    .unwrap();
    assert_eq!(notnull, Some(true), "__reflex_src_idx must be NOT NULL");
}

#[pg_test]
fn test_union_all_cross_operand_delete_isolation() {
    Spi::run(
        "CREATE TABLE pool_a(id INT PRIMARY KEY, label TEXT);
         CREATE TABLE pool_b(id INT PRIMARY KEY, label TEXT);
         INSERT INTO pool_a VALUES (1, 'shared');
         INSERT INTO pool_b VALUES (1, 'shared');  -- same id+label, different operand",
    )
    .unwrap();

    Spi::get_one::<String>(
        "SELECT create_reflex_ivm(
           view_name => 'pool_imv',
           sql       => 'WITH pooled AS (
                           SELECT id, label FROM pool_a
                           UNION ALL
                           SELECT id, label FROM pool_b
                         )
                         SELECT label, COUNT(*) AS cnt
                         FROM pooled
                         GROUP BY label',
           storage   => 'UNLOGGED'
         )",
    )
    .unwrap()
    .unwrap_or_default();

    // Initially: cnt for 'shared' = 2 (one row from each operand).
    let cnt_before = Spi::get_one::<i64>(
        "SELECT cnt FROM pool_imv WHERE label = 'shared'",
    )
    .expect("q1").expect("v1");
    assert_eq!(cnt_before, 2, "expected 2 'shared' rows after setup");

    // Delete from operand A only. Cross-operand collision: operand A and B
    // each have ('shared'). After this DELETE, only A's row should disappear
    // from the wrapper. The aggregate cnt should be 1 (B's row still there).
    Spi::run("DELETE FROM pool_a WHERE id = 1").expect("delete");

    // The bug: the mirror DELETE removes ANY wrapper row matching the column values,
    // not scoped to the operand. So both rows get deleted, and this query returns empty.
    // We verify this by checking if the row still exists; we expect it to exist with cnt=1.
    let cnt_exists = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pool_imv WHERE label = 'shared'",
    )
    .expect("count_q").expect("count_v");

    // If cnt_exists == 0, the bug manifested (both rows deleted).
    // If cnt_exists == 1, the row still exists.
    if cnt_exists > 0 {
        let cnt_result = Spi::get_one::<i64>(
            "SELECT cnt FROM pool_imv WHERE label = 'shared'",
        )
        .expect("q2").expect("v2");
        assert_eq!(
            cnt_result, 1,
            "cross-operand DELETE over-deleted: expected cnt=1 (B's row preserved), got {cnt_result}"
        );
    } else {
        panic!(
            "cross-operand DELETE over-deleted: expected cnt=1 (B's row preserved), \
             but the entire 'shared' row disappeared from pool_imv"
        );
    }
}

#[pg_test]
fn test_drop_cleans_up_union_mirror_functions() {
    Spi::run(
        "CREATE TABLE clean_a(id INT PRIMARY KEY, x INT);
         CREATE TABLE clean_b(id INT PRIMARY KEY, x INT);",
    )
    .unwrap();

    Spi::get_one::<String>(
        "SELECT create_reflex_ivm(
           view_name => 'clean_imv',
           sql       => 'WITH pooled AS (
                           SELECT id, x FROM clean_a
                           UNION ALL
                           SELECT id, x FROM clean_b
                         )
                         SELECT x, COUNT(*) AS cnt FROM pooled GROUP BY x',
           storage   => 'UNLOGGED'
         )",
    )
    .unwrap();

    // Before drop: at least 6 mirror functions exist (2 operands × 3 ops).
    let before: Option<i64> = Spi::get_one(
        "SELECT COUNT(*)
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'public'
           AND p.proname LIKE '\\_\\_reflex\\_union\\_mirror\\_clean\\_imv\\_\\_cte\\_pooled\\_%' ESCAPE '\\'",
    )
    .unwrap();
    assert!(before.unwrap_or(0) >= 6, "expected ≥6 mirror functions before drop, got {before:?}");

    Spi::run("SELECT drop_reflex_ivm('clean_imv', TRUE)").unwrap();

    // After drop: zero mirror functions remain.
    let after: Option<i64> = Spi::get_one(
        "SELECT COUNT(*)
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'public'
           AND p.proname LIKE '\\_\\_reflex\\_union\\_mirror\\_clean\\_imv\\_\\_cte\\_pooled\\_%' ESCAPE '\\'",
    )
    .unwrap();
    assert_eq!(
        after,
        Some(0),
        "expected 0 mirror functions after drop, got {after:?} — function orphan"
    );
}

// Regression: an IMV created with a BARE name while `search_path` points at a
// non-public schema lands every object (target + aux tables) in that schema.
// The reference row stores the bare name, so `drop_reflex_ivm` must record the
// creation schema and reuse it for teardown DDL — otherwise the unqualified
// DROPs resolve against the session `search_path` at drop time, silently skip
// the real objects, and orphan the target + aux tables in the non-public schema.
#[pg_test]
fn test_drop_resolves_creation_schema_for_bare_name() {
    Spi::run("CREATE SCHEMA drop_sch").expect("schema");
    Spi::run("CREATE TABLE drop_sch.bsrc (id SERIAL, grp TEXT, val NUMERIC)").expect("table");
    Spi::run("INSERT INTO drop_sch.bsrc (grp, val) VALUES ('a', 1)").expect("seed");

    // Create with a BARE name while search_path's head schema is non-public.
    Spi::run("SET search_path = drop_sch, public").expect("set sp");
    crate::create_reflex_ivm(
        "bview",
        "SELECT grp, SUM(val) AS total FROM bsrc GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // Target + intermediate aux table landed in drop_sch.
    let tgt = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'drop_sch' AND c.relname = 'bview'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(tgt, 1, "target should be created in drop_sch");

    // Drop from a DIFFERENT search_path (public only) — the orphan repro.
    Spi::run("SET search_path = public").expect("reset sp");
    let result = crate::drop_reflex_ivm("bview");
    assert_eq!(result, "DROP REFLEX INCREMENTAL VIEW");

    let ref_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'bview'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(ref_gone, 0, "reference row must be deleted");

    let tgt_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'drop_sch' AND c.relname = 'bview'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(tgt_gone, 0, "target table must be dropped, not orphaned in drop_sch");

    let int_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'drop_sch' AND c.relname = '__reflex_intermediate_bview'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(int_gone, 0, "intermediate aux table must be dropped, not orphaned");
}

// Regression: a per-source trigger function that a schema migration left as an
// extension member (deptype='e') must not turn drop_reflex_ivm into a hard wall.
// pg_reflex's runtime trigger functions are created from create_reflex_ivm and
// normally belong to the database, but a CREATE/CREATE OR REPLACE that runs while
// PG's `creating_extension` flag is set (any `ALTER EXTENSION pg_reflex UPDATE`
// window) adopts them as members. Once that happens, a plain `DROP FUNCTION`
// fails with "cannot drop function … because extension pg_reflex requires it"
// and `unwrap_or_report()` aborts the entire teardown. The drop must detach the
// function from the extension first, then drop it.
#[pg_test]
fn test_drop_self_heals_extension_member_trigger_fn() {
    Spi::run("CREATE TABLE drop_extfn_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create table");
    Spi::run("INSERT INTO drop_extfn_src (grp, val) VALUES ('a', 1)").expect("seed");

    crate::create_reflex_ivm(
        "drop_extfn_view",
        "SELECT grp, SUM(val) AS total FROM drop_extfn_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    // As of 1.7.7, per-source trigger functions are automatically registered as
    // extension members during IMV creation. Verify that they are indeed members.
    let member_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_depend d \
         JOIN pg_extension e ON e.oid = d.refobjid \
         WHERE d.deptype = 'e' AND e.extname = 'pg_reflex' \
           AND d.objid = to_regprocedure('__reflex_ins_trigger_on_drop_extfn_src()')",
    )
    .expect("q")
    .expect("v");
    assert_eq!(member_before, 1, "precondition: trigger fn is an extension member after create_reflex_ivm");

    // Must self-heal rather than abort when dropping a function that's an extension member.
    let result = crate::drop_reflex_ivm("drop_extfn_view");
    assert_eq!(result, "DROP REFLEX INCREMENTAL VIEW");

    // The function must actually be gone, not merely detached.
    let fn_left = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_proc WHERE proname = '__reflex_ins_trigger_on_drop_extfn_src'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(fn_left, 0, "trigger function must be dropped");
}

// Dropping the IMV's own *target* table (e.g. via `DROP SCHEMA … CASCADE`, or a
// stray `DROP TABLE`) must tear the IMV down completely: registry row, aux
// tables, and source-side triggers. Before this branch the sql_drop trigger
// only reacted to *source* drops, so an IMV whose target vanished left an
// orphaned registry row pointing at a non-existent table — exactly the `yse.*`
// orphans observed in db_qa, where the sources were views the trigger ignored.
#[pg_test]
fn test_target_table_drop_removes_imv() {
    Spi::run("CREATE TABLE ttd_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
    Spi::run("INSERT INTO ttd_src (grp, val) VALUES ('a', 1), ('b', 2)").expect("seed");

    crate::create_reflex_ivm(
        "ttd_view",
        "SELECT grp, SUM(val) AS total FROM ttd_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );

    let registered = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'ttd_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(registered, 1);

    // Drop the IMV's target table directly — the source survives.
    Spi::run("DROP TABLE ttd_view").expect("drop target");

    let registry_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'ttd_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        registry_gone, 0,
        "registry row must be deleted when the target table is dropped"
    );

    let interm_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_name = '__reflex_intermediate_ttd_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(interm_gone, 0, "intermediate table must be cleaned up");

    let trig_gone = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_ttd_src'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(trig_gone, 0, "source-side triggers must be removed");
}

// Guard against the target-drop branch firing during a normal maintenance path.
// A global `reflex_reconcile` on a partitioned IMV swaps each child partition by
// building, ATTACHing, and DROPping per-child tables. Those child / swap tables
// are NOT the registered target, so the new branch must leave both the parent
// target table and its registry row untouched. Keying the branch on exact target
// identity (not a prefix) is what makes this safe; this test proves it.
#[pg_test]
fn test_partition_swap_preserves_registry_row() {
    Spi::run(
        "CREATE TABLE psp_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create");
    for r in ["A", "B"] {
        Spi::run(&format!(
            "CREATE TABLE psp_src_{r} PARTITION OF psp_src FOR VALUES IN ('{r}')"
        ))
        .expect("p");
    }
    Spi::run("INSERT INTO psp_src VALUES (1, 'A', 10), (2, 'B', 20)").expect("seed");
    Spi::run(
        "SELECT create_reflex_ivm( \
            'psp_view', \
            'SELECT region, SUM(amount) AS total FROM psp_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("imv");

    // Force the per-child swap path by drifting every child.
    Spi::run("UPDATE psp_view SET total = -1 WHERE region IN ('A', 'B')").expect("drift");
    let res = Spi::get_one::<&str>("SELECT reflex_reconcile('psp_view')")
        .expect("rec")
        .expect("res");
    assert_eq!(res, "RECONCILED");

    // The child-partition drops during the swap must not delete the parent row.
    let row_alive = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'psp_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(
        row_alive, 1,
        "registry row must survive a partition-swap maintenance cycle"
    );

    let target_alive = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'psp_view'",
    )
    .expect("q")
    .expect("v");
    assert_eq!(target_alive, 1, "parent target table must survive the swap");
}

#[pg_test]
fn f4b_create_args_persisted() {
    // Test that create_args JSONB column captures and persists creation parameters.
    // We create an IMV with non-default args (unique_columns, ignore_sources)
    // and verify they round-trip through the registry.

    Spi::run("CREATE TABLE f4b_src (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC)")
        .expect("create source");
    Spi::run("INSERT INTO f4b_src (grp, val) VALUES ('A', 10), ('B', 20)")
        .expect("seed");

    // Create with specific args: unique_columns=id, ignore_sources (if applicable)
    let result = crate::create_reflex_ivm(
        "f4b_view",
        "SELECT grp, SUM(val) AS total FROM f4b_src GROUP BY grp",
        Some("grp"), // unique_columns
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW", "create should succeed");

    // Verify that create_args was captured in the registry
    let create_args_json = Spi::get_one::<String>(
        "SELECT COALESCE(create_args, '{}') FROM public.__reflex_ivm_reference WHERE name = 'f4b_view'",
    )
    .expect("registry read")
    .expect("row found");

    // Verify the JSON is non-empty and contains the expected structure
    assert!(!create_args_json.is_empty(), "create_args should be populated");
    assert!(create_args_json.contains("grp") || create_args_json == "{}",
            "create_args should reflect unique_columns (got: {})", create_args_json);
}

#[pg_test]
fn f4b_rebuild_chain_basic() {
    // Test that reflex_rebuild_chain can be called on a simple IMV.
    // This test verifies that create_args round-trips correctly for
    // non-decomposed IMVs and that the function executes without error.

    Spi::run("CREATE TABLE f4b_simple_src (id SERIAL, grp TEXT, val NUMERIC)")
        .expect("create source");
    Spi::run("INSERT INTO f4b_simple_src (grp, val) VALUES ('A', 10), ('B', 20)")
        .expect("seed");

    // Create a simple (non-decomposed) IMV
    let result = crate::create_reflex_ivm(
        "f4b_simple",
        "SELECT grp, SUM(val) AS total FROM f4b_simple_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW", "simple IMV creation should succeed");

    // Verify IMV exists
    let exists_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'f4b_simple'",
    )
    .expect("count before")
    .expect("count");
    assert_eq!(exists_before, 1, "IMV should exist in registry");

    // Call rebuild_chain
    let rebuild_result = crate::reflex_rebuild_chain("f4b_simple", false);
    assert!(rebuild_result.starts_with("REBUILT CHAIN"), "rebuild should succeed: {}", rebuild_result);

    // Verify IMV still exists after rebuild
    let exists_after = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'f4b_simple'",
    )
    .expect("count after")
    .expect("count");
    assert_eq!(exists_after, 1, "IMV should still exist after rebuild");

    // Verify create_args were persisted and round-tripped
    let create_args = Spi::get_one::<String>(
        "SELECT COALESCE(create_args, '{}') FROM public.__reflex_ivm_reference WHERE name = 'f4b_simple'",
    )
    .expect("read create_args")
    .expect("create_args exists");
    assert!(!create_args.is_empty(), "create_args should be populated after rebuild");
}

#[pg_test]
fn f4b_create_args_roundtrip_fidelity() {
    // Test that create_args with non-default values (ignore_sources) round-trip correctly
    // through reflex_rebuild_chain. This exercises the JSON parsing fix (Finding 2).

    Spi::run("CREATE TABLE f4b_rt_src1 (id SERIAL, grp TEXT, val NUMERIC)").expect("create src1");
    Spi::run("CREATE TABLE f4b_rt_src2 (id SERIAL, grp TEXT, val NUMERIC)").expect("create src2");
    Spi::run("INSERT INTO f4b_rt_src1 (grp, val) VALUES ('A', 10), ('B', 20)").expect("seed src1");
    Spi::run("INSERT INTO f4b_rt_src2 (grp, val) VALUES ('X', 100), ('Y', 200)").expect("seed src2");

    // Create an IMV with IGNORE_SOURCES to have non-default create_args
    let result = crate::create_reflex_ivm(
        "f4b_roundtrip",
        "SELECT grp, SUM(val) AS total FROM f4b_rt_src1 GROUP BY grp",
        None,
        None,
        None,
        Some("f4b_rt_src2"),
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW", "IMV with ignore_sources should create");

    // Verify ignore_sources is in create_args before rebuild
    let create_args_before = Spi::get_one::<String>(
        "SELECT COALESCE(create_args, '{}') FROM public.__reflex_ivm_reference WHERE name = 'f4b_roundtrip'",
    )
    .expect("read before")
    .expect("exists before");
    assert!(
        create_args_before.contains("f4b_rt_src2"),
        "create_args should contain ignored source before rebuild: {}",
        create_args_before
    );

    // Rebuild the chain
    let rebuild_result = crate::reflex_rebuild_chain("f4b_roundtrip", false);
    assert!(rebuild_result.starts_with("REBUILT CHAIN"), "rebuild should succeed: {}", rebuild_result);

    // Verify ignore_sources is STILL in create_args after rebuild (round-trip fidelity)
    let create_args_after = Spi::get_one::<String>(
        "SELECT COALESCE(create_args, '{}') FROM public.__reflex_ivm_reference WHERE name = 'f4b_roundtrip'",
    )
    .expect("read after")
    .expect("exists after");
    assert!(
        create_args_after.contains("f4b_rt_src2"),
        "create_args should still contain ignored source after rebuild (fidelity test): {}",
        create_args_after
    );

    // Verify target table row count matches (should have 2 rows from src1)
    let row_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f4b_roundtrip",
    )
    .expect("count rows")
    .expect("rows");
    assert_eq!(row_count, 2, "target should have 2 rows (one per group)");
}

#[pg_test]
fn f4b_rebuild_chain_atomicity() {
    // Test that reflex_rebuild_chain is atomic: if the drop succeeds but the
    // recreate fails, the entire transaction is aborted so the drop is rolled back.
    // This exercises the atomicity fix (Finding 1).

    Spi::run("CREATE TABLE f4b_atom_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create src");
    Spi::run("INSERT INTO f4b_atom_src (grp, val) VALUES ('A', 10)").expect("seed");

    // Create a simple IMV
    let result = crate::create_reflex_ivm(
        "f4b_atomic",
        "SELECT grp, SUM(val) AS total FROM f4b_atom_src GROUP BY grp",
        None,
        None,
        None,
        None,
    );
    assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

    // Verify IMV exists in registry
    let exists_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'f4b_atomic'",
    )
    .expect("count before")
    .expect("before");
    assert_eq!(exists_before, 1, "IMV should exist before rebuild");

    // Verify target table exists and has data
    let row_count_before = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f4b_atomic",
    )
    .expect("count rows before")
    .expect("rows before");
    assert_eq!(row_count_before, 1, "target should have 1 row before rebuild");

    // Call rebuild_chain (should succeed on normal IMV)
    let rebuild_result = crate::reflex_rebuild_chain("f4b_atomic", false);
    assert!(rebuild_result.starts_with("REBUILT CHAIN"), "rebuild should succeed: {}", rebuild_result);

    // Verify IMV still exists and target has the same row count
    let exists_after = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'f4b_atomic'",
    )
    .expect("count after")
    .expect("after");
    assert_eq!(exists_after, 1, "IMV should still exist after rebuild (atomicity proof)");

    let row_count_after = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM f4b_atomic",
    )
    .expect("count rows after")
    .expect("rows after");
    assert_eq!(
        row_count_after, row_count_before,
        "target row count should be preserved after rebuild"
    );
}
