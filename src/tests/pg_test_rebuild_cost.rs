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
        "SELECT 1 AS pkey, TRUE AS is_old",
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

/// A large UPDATE confined to one plan of an IMV without dependents rebuilds
/// that plan (the dispatch counts changed rows, not distinct partition values).
#[pg_test]
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

const RCU_ROWS_PER_PLAN: i32 = 2000;
const RCU_PLANS: i32 = 5;

/// Plans 1..=5 (LIST) each split into two month leaves (RANGE), 2000 rows per
/// plan, with a primary key so upserts can target it; mirrors the
/// `alp.sop_forecast_view` layout. Ids are unique across plans.
fn rcu_source(src: &str) {
    Spi::run(&format!(
        "CREATE TABLE {src} (plan INT NOT NULL, m INT NOT NULL, id INT NOT NULL, v INT, \
         PRIMARY KEY (plan, m, id)) PARTITION BY LIST (plan)"
    ))
    .expect("root");
    for p in 1..=RCU_PLANS {
        Spi::run(&format!(
            "CREATE TABLE {src}_p{p} PARTITION OF {src} FOR VALUES IN ({p}) PARTITION BY RANGE (m)"
        ))
        .expect("plan level");
        Spi::run(&format!(
            "CREATE TABLE {src}_p{p}_a PARTITION OF {src}_p{p} FOR VALUES FROM (0) TO (6)"
        ))
        .expect("leaf a");
        Spi::run(&format!(
            "CREATE TABLE {src}_p{p}_b PARTITION OF {src}_p{p} FOR VALUES FROM (6) TO (12)"
        ))
        .expect("leaf b");
    }
    Spi::run(&format!(
        "INSERT INTO {src} SELECT p, g % 12, (p - 1) * {RCU_ROWS_PER_PLAN} + g, g \
         FROM generate_series(1, {RCU_PLANS}) p, generate_series(1, {RCU_ROWS_PER_PLAN}) g"
    ))
    .expect("seed");
}

fn rcu_view_sql(prefix: &str) -> String {
    format!("SELECT plan, m, id, v FROM {prefix}_src")
}

/// Passthrough IMV `{prefix}_v` partitioned plan -> month over `rcu_source`,
/// optionally observed by the dependent `{prefix}_d`. Analyzed, so each plan is
/// sized at exactly 2000 rows and the ratios below are exact.
fn rcu_build(prefix: &str, with_dependent: bool) {
    rcu_source(&format!("{prefix}_src"));
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{prefix}_v', '{}', 'plan, m, id', NULL, NULL, NULL, \
         ARRAY['plan', 'm'])",
        rcu_view_sql(prefix)
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    if with_dependent {
        assert_eq!(
            crate::create_reflex_ivm(
                &format!("{prefix}_d"),
                &format!("SELECT plan, id, v FROM {prefix}_v"),
                Some("plan, id"),
                None,
                None,
                None
            ),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
    }
    let depth = Spi::get_one::<i32>(&format!(
        "SELECT max(level) FROM pg_partition_tree('{prefix}_v'::regclass)"
    ))
    .expect("depth")
    .expect("depth value");
    assert_eq!(depth, 2, "target is not partitioned plan -> month");
    Spi::run(&format!("ANALYZE {prefix}_v")).expect("analyze target");
}

/// Leaf OIDs of the target child holding `plan`: a swap rebuild changes them.
fn rcu_plan_leaf_oids(imv: &str, plan: i32) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT string_agg(relid::int8::text, ',' ORDER BY relid) \
         FROM pg_partition_tree(public.__reflex_partition_child_for_key('{imv}'::regclass, 'plan', '{plan}')) \
         WHERE isleaf"
    ))
    .expect("plan leaf oids")
    .unwrap_or_default()
}

fn rcu_all_plan_leaf_oids(imv: &str) -> Vec<String> {
    (1..=RCU_PLANS)
        .map(|p| rcu_plan_leaf_oids(imv, p))
        .collect()
}

/// Rows (inserted, updated, deleted) in the target's leaves this transaction. The
/// cold path maintains a passthrough UPDATE as keyed DELETE + INSERT; only the hot
/// leaf diff (the rebuild taken when the target has dependents) UPDATEs rows.
fn rcu_target_xact_counts(imv: &str) -> (i64, i64, i64) {
    Spi::connect(|client| {
        let row = client
            .select(
                &format!(
                    "SELECT COALESCE(sum(pg_stat_get_xact_tuples_inserted(relid)), 0)::int8, \
                            COALESCE(sum(pg_stat_get_xact_tuples_updated(relid)), 0)::int8, \
                            COALESCE(sum(pg_stat_get_xact_tuples_deleted(relid)), 0)::int8 \
                     FROM pg_partition_tree('{imv}'::regclass) WHERE isleaf"
                ),
                Some(1),
                &[],
            )
            .expect("xact stats")
            .first();
        (
            row.get::<i64>(1).expect("ins").unwrap_or(0),
            row.get::<i64>(2).expect("upd").unwrap_or(0),
            row.get::<i64>(3).expect("del").unwrap_or(0),
        )
    })
}

/// `rows` rows of plan 1 get `v = v + 1`.
fn rcu_update_plan1(prefix: &str, rows: i32) {
    Spi::run(&format!(
        "UPDATE {prefix}_src SET v = v + 1 WHERE plan = 1 AND id <= {rows}"
    ))
    .expect("update");
}

fn rcu_assert_only_plans_rebuilt(prefix: &str, before: &[String], rebuilt: &[i32]) {
    let after = rcu_all_plan_leaf_oids(&format!("{prefix}_v"));
    for p in 1..=RCU_PLANS {
        let i = (p - 1) as usize;
        if rebuilt.contains(&p) {
            assert_ne!(after[i], before[i], "plan {p} was not rebuilt");
        } else {
            assert_eq!(after[i], before[i], "plan {p} was rebuilt");
        }
    }
}

/// 80% of a plan updated, with an observing dependent (threshold 0.9): cold, i.e.
/// keyed DELETE + INSERT of the 1600 rows, nothing updated in place.
#[pg_test]
fn pg_rcu_large_update_with_dependent_below_090_stays_cold() {
    rcu_build("rcu1", true);
    let before = rcu_all_plan_leaf_oids("rcu1_v");
    let (ins, upd, del) = rcu_target_xact_counts("rcu1_v");
    rcu_update_plan1("rcu1", 1600);
    let (ins2, upd2, del2) = rcu_target_xact_counts("rcu1_v");
    assert_eq!(
        (ins2 - ins, upd2 - upd, del2 - del),
        (1600, 0, 1600),
        "80% update was rebuilt despite the dependent's 0.9 threshold"
    );
    rcu_assert_only_plans_rebuilt("rcu1", &before, &[]);
    assert_imv_correct("rcu1_v", &rcu_view_sql("rcu1"));
    assert_imv_correct("rcu1_d", "SELECT plan, id, v FROM rcu1_src");
}

/// 95% of a plan updated, with an observing dependent: hot. The plan is rebuilt
/// through the leaf diff — the 1900 changed rows updated in place, nothing
/// deleted or reinserted, leaves kept — so the dependent receives exactly that
/// diff, not a rebuild.
#[pg_test]
fn pg_rcu_large_update_with_dependent_above_090_goes_hot_by_diff() {
    rcu_build("rcu2", true);
    let before = rcu_all_plan_leaf_oids("rcu2_v");
    let (ins, upd, del) = rcu_target_xact_counts("rcu2_v");
    rcu_update_plan1("rcu2", 1900);
    let (ins2, upd2, del2) = rcu_target_xact_counts("rcu2_v");
    assert_eq!(
        (ins2 - ins, upd2 - upd, del2 - del),
        (0, 1900, 0),
        "95% update did not reach the target as the hot leaf diff"
    );
    rcu_assert_only_plans_rebuilt("rcu2", &before, &[]);
    assert_imv_correct("rcu2_v", &rcu_view_sql("rcu2"));
    assert_imv_correct("rcu2_d", "SELECT plan, id, v FROM rcu2_src");
}

/// A small UPDATE stays on the incremental path: no plan is rebuilt.
#[pg_test]
fn pg_rcu_small_update_stays_cold() {
    rcu_build("rcu3", false);
    let before = rcu_all_plan_leaf_oids("rcu3_v");
    rcu_update_plan1("rcu3", 100);
    rcu_assert_only_plans_rebuilt("rcu3", &before, &[]);
    assert_imv_correct("rcu3_v", &rcu_view_sql("rcu3"));
}

/// An UPDATE of N rows of a partition counts N, not 2N (old + new image): 40%
/// of a plan at the 0.5 threshold stays cold.
#[pg_test]
fn pg_rcu_update_counts_each_row_once() {
    rcu_build("rcu4", false);
    let before = rcu_all_plan_leaf_oids("rcu4_v");
    rcu_update_plan1("rcu4", 800);
    rcu_assert_only_plans_rebuilt("rcu4", &before, &[]);
    assert_imv_correct("rcu4_v", &rcu_view_sql("rcu4"));
}

/// 60% of plan 1 moves to plan 2: each plan is dirtied by the 1200 moved rows,
/// so both go hot; the other plans are untouched.
#[pg_test]
fn pg_rcu_partition_key_update_dirties_both_plans_by_moved_rows() {
    rcu_build("rcu5", false);
    let before = rcu_all_plan_leaf_oids("rcu5_v");
    Spi::run("UPDATE rcu5_src SET plan = 2 WHERE plan = 1 AND id <= 1200").expect("move");
    rcu_assert_only_plans_rebuilt("rcu5", &before, &[1, 2]);
    assert_imv_correct("rcu5_v", &rcu_view_sql("rcu5"));
}

/// 30% of plan 1 moves to plan 3: 0.3 on each side, both stay cold.
#[pg_test]
fn pg_rcu_small_partition_key_update_stays_cold() {
    rcu_build("rcu6", false);
    let before = rcu_all_plan_leaf_oids("rcu6_v");
    Spi::run("UPDATE rcu6_src SET plan = 3 WHERE plan = 1 AND id <= 600").expect("move");
    rcu_assert_only_plans_rebuilt("rcu6", &before, &[]);
    assert_imv_correct("rcu6_v", &rcu_view_sql("rcu6"));
}

/// A large upsert whose rows all conflict reaches the IMV as an UPDATE and goes hot.
#[pg_test]
fn pg_rcu_large_upsert_goes_hot() {
    rcu_build("rcu7", false);
    let before = rcu_all_plan_leaf_oids("rcu7_v");
    Spi::run(
        "INSERT INTO rcu7_src SELECT plan, m, id, v + 1 FROM rcu7_src WHERE plan = 1 AND id <= 1600 \
         ON CONFLICT (plan, m, id) DO UPDATE SET v = EXCLUDED.v",
    )
    .expect("upsert");
    rcu_assert_only_plans_rebuilt("rcu7", &before, &[1]);
    assert_imv_correct("rcu7_v", &rcu_view_sql("rcu7"));
}

/// Many small updates spread over every plan stay cold: the trip-cap (more than
/// half the plans hot -> full rebuild) must not fire.
#[pg_test]
fn pg_rcu_small_updates_across_all_plans_do_not_trip_cap() {
    rcu_build("rcu8", false);
    let before = rcu_all_plan_leaf_oids("rcu8_v");
    Spi::run("UPDATE rcu8_src SET v = v + 1 WHERE id % 10 = 0").expect("update");
    rcu_assert_only_plans_rebuilt("rcu8", &before, &[]);
    assert_imv_correct("rcu8_v", &rcu_view_sql("rcu8"));
}

/// Large updates of three of five plans trip the cap: one full rebuild, which
/// replaces every plan's leaves.
#[pg_test]
fn pg_rcu_large_updates_of_most_plans_trip_cap() {
    rcu_build("rcu9", false);
    let before = rcu_all_plan_leaf_oids("rcu9_v");
    Spi::run("UPDATE rcu9_src SET v = v + 1 WHERE plan IN (1, 2, 3)").expect("update");
    rcu_assert_only_plans_rebuilt("rcu9", &before, &[1, 2, 3, 4, 5]);
    assert_imv_correct("rcu9_v", &rcu_view_sql("rcu9"));
}

/// Probe: a passthrough IMV LEFT JOINing a secondary table (the
/// `sop_forecast_view` shape) maintains an UPDATE and an upsert of its anchor
/// through the same partition dispatch, so a large one goes hot.
#[pg_test]
fn pg_rcu_join_passthrough_anchor_update_and_upsert_go_hot() {
    rcu_source("rcu10_src");
    Spi::run("CREATE TABLE rcu10_caav (id INT PRIMARY KEY, active BOOL)").expect("caav");
    Spi::run("INSERT INTO rcu10_caav SELECT g, g % 2 = 0 FROM generate_series(1, 10000, 3) g")
        .expect("seed caav");
    let view_sql = "SELECT s.plan, s.m, s.id, s.v, c.active \
                    FROM rcu10_src s LEFT JOIN rcu10_caav c ON c.id = s.id";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('rcu10_v', '{view_sql}', 'plan, m, id', NULL, NULL, NULL, \
         ARRAY['plan', 'm'])"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("ANALYZE rcu10_v").expect("analyze");

    let before = rcu_all_plan_leaf_oids("rcu10_v");
    Spi::run("UPDATE rcu10_src SET v = v + 1 WHERE plan = 1 AND id <= 1600").expect("update");
    rcu_assert_only_plans_rebuilt("rcu10", &before, &[1]);
    assert_imv_correct("rcu10_v", view_sql);

    let before = rcu_all_plan_leaf_oids("rcu10_v");
    Spi::run(
        "INSERT INTO rcu10_src SELECT plan, m, id, v + 1 FROM rcu10_src \
         WHERE plan = 2 AND id <= 3600 \
         ON CONFLICT (plan, m, id) DO UPDATE SET v = EXCLUDED.v",
    )
    .expect("upsert");
    rcu_assert_only_plans_rebuilt("rcu10", &before, &[2]);
    assert_imv_correct("rcu10_v", view_sql);
}

const RCE_VIEW_SQL: &str = "SELECT region, COUNT(DISTINCT cust) AS n FROM {p}_src GROUP BY region";

/// A partitioned COUNT(DISTINCT) IMV, whose UPDATE dispatch is the unpartitioned
/// high-selectivity block, with an unrelated table holding the name of the mirror
/// child for source partition `0`: the delegated `reflex_reconcile` then returns
/// `ERROR: partition reconcile failed` before it reaches leaf `B`.
fn rce_failing_reconcile_fixture(prefix: &str, mode: &str, exec: impl Fn(&str)) {
    let view_sql = RCE_VIEW_SQL.replace("{p}", prefix);
    for sql in [
        format!(
            "CREATE TABLE {prefix}_src (region TEXT NOT NULL, id INT, cust INT) PARTITION BY LIST (region)"
        ),
        format!("CREATE TABLE {prefix}_src_a PARTITION OF {prefix}_src FOR VALUES IN ('A')"),
        format!("CREATE TABLE {prefix}_src_b PARTITION OF {prefix}_src FOR VALUES IN ('B')"),
        format!(
            "INSERT INTO {prefix}_src SELECT CASE WHEN g % 2 = 0 THEN 'A' ELSE 'B' END, g, g % 50 \
             FROM generate_series(1, 2000) g"
        ),
        format!(
            "DO $c$ BEGIN IF create_reflex_ivm('{prefix}_v', '{view_sql}', 'region', NULL, '{mode}', \
             NULL, ARRAY['region']) <> 'CREATE REFLEX INCREMENTAL VIEW' THEN \
             RAISE EXCEPTION 'create {prefix}_v failed'; END IF; END $c$"
        ),
        format!("CREATE TABLE {prefix}_v_{prefix}_src_0 (x INT)"),
        format!("CREATE TABLE {prefix}_src_0 PARTITION OF {prefix}_src FOR VALUES IN ('0')"),
        format!("ANALYZE {prefix}_src"),
        format!("ANALYZE __reflex_intermediate_{prefix}_v"),
        format!(
            "DO $p$ BEGIN IF NOT EXISTS (SELECT 1 FROM public.__reflex_ivm_reference r \
             WHERE r.name = '{prefix}_v' AND strpos(reflex_build_delta_sql(r.name, '{prefix}_src', \
             'UPDATE', r.base_query, r.end_query, r.aggregations::text, r.base_query), \
             'pg_reflex wipe: ratio') > 0) \
             THEN RAISE EXCEPTION 'fixture: UPDATE does not take the unpartitioned dispatch'; END IF; END $p$"
        ),
    ] {
        exec(&sql);
    }
}

/// The UPDATE below touches 5% of the source (under the trigger's pre-scratch
/// ratio) but every group of `B`, so it reaches the dispatch's rebuild branch.
const RCE_UPDATE: &str = "UPDATE {p}_src SET cust = id + 1000 WHERE region = 'B' AND id <= 200";

/// The unpartitioned high-selectivity dispatch delegates to `reflex_reconcile`;
/// when that returns an `ERROR` string, the statement must fail rather than
/// commit a source change the IMV never received.
#[pg_test]
fn pg_rco_high_selectivity_reconcile_error_fails_the_statement() {
    rce_failing_reconcile_fixture("rce1", "IMMEDIATE", |sql| {
        Spi::run(sql).unwrap_or_else(|e| panic!("<{sql}>: {e}"))
    });
    let outcome = Spi::get_one::<String>(&format!(
        "DO $d$ BEGIN {}; \
           PERFORM set_config('rce1.outcome', 'NO ERROR', true); \
         EXCEPTION WHEN OTHERS THEN PERFORM set_config('rce1.outcome', SQLERRM, true); END $d$; \
         SELECT current_setting('rce1.outcome')",
        RCE_UPDATE.replace("{p}", "rce1")
    ))
    .expect("outcome")
    .expect("outcome value");
    assert_imv_correct("rce1_v", &RCE_VIEW_SQL.replace("{p}", "rce1"));
    assert!(
        outcome.contains("partition reconcile failed"),
        "the failed rebuild must fail the statement, got: {outcome}"
    );
}

/// DEFERRED: the same failure at COMMIT must leave the IMV flagged stale, not
/// silently behind its committed source.
#[pg_test]
fn pg_rco_high_selectivity_reconcile_error_marks_deferred_stale() {
    const DBNAME: &str = "reflex_rce_deferred";
    probe_db_open(DBNAME);
    rce_failing_reconcile_fixture("rce2", "DEFERRED", worker_exec);
    worker_exec(&RCE_UPDATE.replace("{p}", "rce2"));
    assert_eq!(
        worker_scalar_i64("SELECT count(DISTINCT cust)::int8 FROM rce2_src WHERE region = 'B'"),
        125,
        "the source change committed"
    );
    let mismatch = rbc_mismatch("rce2_v", &RCE_VIEW_SQL.replace("{p}", "rce2"));
    assert!(
        rbc_known_stale("rce2_v"),
        "IMV silently diverged from its committed source ({mismatch} mismatched rows) without known_stale"
    );
    probe_db_close(DBNAME);
}
