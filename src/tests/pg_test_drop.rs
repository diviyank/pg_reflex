
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

    // `dq_v__cte_j` depends on the quoted sub-IMV "dq_v__cte_lim".
    let d = crate::drop_reflex_ivm("dq_v__cte_j");
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
