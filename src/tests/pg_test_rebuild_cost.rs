// Cost estimate used by the volume-triggered rebuild dispatch.

fn rco_two_level_source(prefix: &str, rows: i32) {
    Spi::run(&format!(
        "CREATE TABLE {prefix} (plan INT NOT NULL, m INT NOT NULL, id INT NOT NULL, v INT, k INT) PARTITION BY LIST (plan)"
    ))
    .expect("root");
    Spi::run(&format!(
        "CREATE TABLE {prefix}_p1 PARTITION OF {prefix} FOR VALUES IN (1) PARTITION BY RANGE (m)"
    ))
    .expect("plan level");
    Spi::run(&format!(
        "CREATE TABLE {prefix}_p1_a PARTITION OF {prefix}_p1 FOR VALUES FROM (0) TO (6)"
    ))
    .expect("leaf a");
    Spi::run(&format!(
        "CREATE TABLE {prefix}_p1_b PARTITION OF {prefix}_p1 FOR VALUES FROM (6) TO (12)"
    ))
    .expect("leaf b");
    Spi::run(&format!(
        "INSERT INTO {prefix} SELECT 1, g % 12, g, g, g % 10 FROM generate_series(1, {rows}) g"
    ))
    .expect("seed");
}

/// A hot-partition rebuild swaps the child tables, so the leaf OIDs of the target change.
fn rco_target_leaf_oids(imv: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT string_agg(relid::int8::text, ',' ORDER BY relid) \
         FROM pg_partition_tree('{imv}'::regclass) WHERE isleaf"
    ))
    .expect("leaf oids")
    .unwrap_or_default()
}

/// Passthrough partitioned IMV over a two-level source, optionally with a dependent.
fn rco_build_passthrough(prefix: &str, with_dependent: bool) {
    rco_two_level_source(&format!("{prefix}_src"), 5000);
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{prefix}_v', 'SELECT plan, m, id, v FROM {prefix}_src', \
         'plan, m, id', NULL, NULL, NULL, ARRAY['plan', 'm'])"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    if with_dependent {
        assert_eq!(
            crate::create_reflex_ivm(
                &format!("{prefix}_d"),
                &format!("SELECT plan, id FROM {prefix}_v"),
                Some("plan, id"),
                None,
                None,
                None
            ),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
    }
}

/// A two-level child (plan -> months) is sized by its leaves, not reltuples -1.
#[pg_test]
fn pg_rco_two_level_child_sized_by_leaves() {
    Spi::run(
        "CREATE TABLE rco1 (plan INT NOT NULL, m INT NOT NULL, v INT) PARTITION BY LIST (plan)",
    )
    .expect("root");
    Spi::run("CREATE TABLE rco1_p1 PARTITION OF rco1 FOR VALUES IN (1) PARTITION BY RANGE (m)")
        .expect("plan");
    Spi::run("CREATE TABLE rco1_p1_a PARTITION OF rco1_p1 FOR VALUES FROM (0) TO (6)")
        .expect("leaf a");
    Spi::run("CREATE TABLE rco1_p1_b PARTITION OF rco1_p1 FOR VALUES FROM (6) TO (12)")
        .expect("leaf b");
    Spi::run("INSERT INTO rco1 SELECT 1, g % 12, g FROM generate_series(1, 5000) g").expect("seed");
    Spi::run("ANALYZE rco1").expect("analyze");
    let rows =
        Spi::get_one::<f64>("SELECT __reflex_rebuild_cost_rows('rco1', 'rco1_p1'::regclass)")
            .unwrap()
            .unwrap();
    assert!(
        (4000.0..6000.0).contains(&rows),
        "two-level child sized {rows}"
    );
}

/// Never-analyzed leaves are sized from their file size, not 0 / -1.
#[pg_test]
fn pg_rco_unanalyzed_leaf_sized_from_file() {
    Spi::run("CREATE TABLE rco2 (plan INT NOT NULL, v TEXT) PARTITION BY LIST (plan)")
        .expect("root");
    Spi::run("CREATE TABLE rco2_p1 PARTITION OF rco2 FOR VALUES IN (1)").expect("leaf");
    Spi::run("INSERT INTO rco2 SELECT 1, repeat('x', 50) FROM generate_series(1, 20000)")
        .expect("seed");
    let rows =
        Spi::get_one::<f64>("SELECT __reflex_rebuild_cost_rows('rco2', 'rco2_p1'::regclass)")
            .unwrap()
            .unwrap();
    assert!(rows >= 5000.0, "unanalyzed leaf sized {rows}");
}

/// With dependents, the default threshold is the higher one.
#[pg_test]
fn pg_rco_target_propagates_reflects_dependents() {
    Spi::run("CREATE TABLE rco3 (k INT PRIMARY KEY, v INT)").expect("t");
    assert_eq!(
        crate::create_reflex_ivm(
            "rco3_v",
            "SELECT k, v FROM rco3",
            Some("k"),
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert!(
        !Spi::get_one::<bool>("SELECT __reflex_target_propagates('rco3_v')")
            .unwrap()
            .unwrap()
    );
    assert_eq!(
        crate::create_reflex_ivm(
            "rco3_d",
            "SELECT k FROM rco3_v",
            Some("k"),
            None,
            None,
            None
        ),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    assert!(
        Spi::get_one::<bool>("SELECT __reflex_target_propagates('rco3_v')")
            .unwrap()
            .unwrap()
    );
}

/// Aggregate IMV: the target's child is sized by the anchor SOURCE's matching child.
#[pg_test]
fn pg_rco_aggregate_child_maps_to_source_child() {
    rco_two_level_source("rco5_src", 6000);
    Spi::run("ANALYZE rco5_src").expect("analyze");
    let created = Spi::get_one::<String>(
        "SELECT create_reflex_ivm('rco5_v', \
         'SELECT plan, m, k, SUM(v) AS s FROM rco5_src GROUP BY plan, m, k')",
    )
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    // 120 groups in the target, 6000 rows in the source: sizing must see the source.
    let rows = Spi::get_one::<f64>(
        "SELECT __reflex_rebuild_cost_rows('rco5_v', 'rco5_v_rco5_src_p1'::regclass)",
    )
    .unwrap()
    .unwrap();
    assert!(
        (5000.0..7000.0).contains(&rows),
        "aggregate child sized {rows}"
    );
}

/// 20% of a plan is deleted: with a dependent the (leaf-summed) ratio is far below
/// the threshold, so the plan stays on the incremental path.
#[pg_test]
fn pg_rco_small_delete_of_two_level_plan_stays_cold() {
    rco_build_passthrough("rco4", true);
    // Autovacuum never analyzes partitioned parents: their reltuples stays -1.
    Spi::run("UPDATE pg_class SET reltuples = -1 WHERE relkind = 'p' AND relname LIKE 'rco4_v%'")
        .expect("unanalyzed plan-level child");
    let before = rco_target_leaf_oids("rco4_v");
    Spi::run("DELETE FROM rco4_src WHERE id <= 1000").expect("delete");
    assert_eq!(
        rco_target_leaf_oids("rco4_v"),
        before,
        "20% delete rebuilt the plan"
    );
    assert_imv_correct("rco4_v", "SELECT plan, m, id, v FROM rco4_src");
    assert_imv_correct("rco4_d", "SELECT plan, id FROM rco4_src");
}

/// 80% of a plan is deleted: stays incremental while the IMV has a dependent
/// (threshold 0.9) ...
#[pg_test]
fn pg_rco_large_delete_stays_cold_with_dependent() {
    rco_build_passthrough("rco6", true);
    let before = rco_target_leaf_oids("rco6_v");
    Spi::run("DELETE FROM rco6_src WHERE id <= 4000").expect("delete");
    assert_eq!(
        rco_target_leaf_oids("rco6_v"),
        before,
        "rebuilt despite dependent"
    );
    assert_imv_correct("rco6_v", "SELECT plan, m, id, v FROM rco6_src");
}

/// ... and goes hot (rebuild) without one (threshold 0.5).
#[pg_test]
fn pg_rco_large_delete_goes_hot_without_dependent() {
    rco_build_passthrough("rco7", false);
    let before = rco_target_leaf_oids("rco7_v");
    Spi::run("DELETE FROM rco7_src WHERE id <= 4000").expect("delete");
    assert_ne!(
        rco_target_leaf_oids("rco7_v"),
        before,
        "plan was not rebuilt"
    );
    assert_imv_correct("rco7_v", "SELECT plan, m, id, v FROM rco7_src");
}

/// A hot-partition / trip-cap rebuild that returns an ERROR string is raised, not discarded.
#[pg_test]
fn pg_rco_dispatch_sql_raises_on_error_result() {
    let cold_del = ["DELETE FROM \"public\".\"v\" WHERE id IN (SELECT id FROM o)".to_string()];
    let passthrough = crate::trigger::build_passthrough_partition_dispatch_sql(
        "v",
        "\"public\".\"v\"",
        "SELECT 1 AS pkey",
        "region",
        "\"public\".\"v\".\"region\"",
        "LIST",
        &cold_del,
        "",
    );
    let aggregate = crate::trigger::build_partition_aware_dispatch_sql_strategy(
        "v",
        "i",
        "i_parent",
        "a",
        "region",
        "LIST",
        "MERGE",
        &[],
        &[],
        &[],
    );
    for (name, sql) in [("passthrough", passthrough), ("aggregate", aggregate)] {
        assert!(
            !sql.contains("PERFORM public.reflex_reconcile"),
            "{name}: result discarded:\n{sql}"
        );
        assert_eq!(
            sql.matches("RAISE EXCEPTION").count(),
            2,
            "{name}: hot + trip-cap must raise"
        );
        assert!(
            sql.contains("__reflex_rebuild_cost_rows"),
            "{name}: cost helper unused"
        );
        assert!(
            sql.contains("__reflex_target_propagates"),
            "{name}: dependents threshold unused"
        );
    }
}

/// Intended behaviour: a large UPDATE confined to one plan of an IMV without
/// dependents rebuilds that plan. Today the passthrough UPDATE dispatch
/// collapses `affected` to distinct partition values, so it never goes hot.
#[pg_test]
#[ignore = "untreated_bugs/2026-10-01_passthrough_update_dispatch_union_collapses_dirty.md"]
fn pg_rco_large_update_goes_hot_without_dependent_passthrough() {
    rco_build_passthrough("rco8", false);
    let before = rco_target_leaf_oids("rco8_v");
    Spi::run("UPDATE rco8_src SET v = v + 1 WHERE id <= 4000").expect("update");
    assert_ne!(
        rco_target_leaf_oids("rco8_v"),
        before,
        "plan was not rebuilt"
    );
    assert_imv_correct("rco8_v", "SELECT plan, m, id, v FROM rco8_src");
}
