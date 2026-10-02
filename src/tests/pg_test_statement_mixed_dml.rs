// One statement that both changes and inserts (or deletes) source rows — an
// upsert (`INSERT … ON CONFLICT DO UPDATE`: AFTER UPDATE then AFTER INSERT
// statement triggers), a MERGE, a writable CTE — fires several statement
// triggers, and each sees the source as the WHOLE statement left it. A rebuild
// run by an earlier trigger therefore already holds the later triggers' rows,
// which they then apply a second time. Statement triggers never rebuild; the
// DEFERRED flush, which nets the transaction into one delta, still may.

const SMD_AGG_SQL: &str =
    "SELECT plan, m, id, SUM(v) AS s, COUNT(*) AS n FROM {p}_src GROUP BY plan, m, id";

/// `conflicts` rows of `conflict_plan` updated through their conflict, and 400
/// new rows inserted into `new_plan`, in one upsert.
fn smd_mixed_upsert(prefix: &str, conflicts: i32, conflict_plan: i32, new_plan: i32) {
    Spi::run(&format!(
        "INSERT INTO {prefix}_src \
         SELECT plan, m, id, v + 1 FROM {prefix}_src \
          WHERE plan = {conflict_plan} AND id <= ({conflict_plan} - 1) * {RCU_ROWS_PER_PLAN} + {conflicts} \
         UNION ALL \
         SELECT {new_plan}, g % 12, 100000 * {new_plan} + g, g FROM generate_series(1, 400) g \
         ON CONFLICT (plan, m, id) DO UPDATE SET v = EXCLUDED.v"
    ))
    .expect("mixed upsert");
}

/// MERGE of plan 1: 1600 matched rows (every tenth deleted, the rest updated)
/// and 400 new rows inserted.
fn smd_merge(prefix: &str) {
    Spi::run(&format!(
        "MERGE INTO {prefix}_src t \
         USING (SELECT plan, m, id, v FROM {prefix}_src WHERE plan = 1 AND id <= 1600 \
                UNION ALL \
                SELECT 1, g % 12, 100000 + g, g FROM generate_series(1, 400) g) s \
            ON t.plan = s.plan AND t.m = s.m AND t.id = s.id \
         WHEN MATCHED AND t.id % 10 = 0 THEN DELETE \
         WHEN MATCHED THEN UPDATE SET v = t.v + 1 \
         WHEN NOT MATCHED THEN INSERT VALUES (s.plan, s.m, s.id, s.v)"
    ))
    .expect("merge");
}

/// Partitioned aggregate over `rcu_source`, one group per source row so a plan's
/// dirty groups match its source rows and a large change can go hot.
fn smd_build_aggregate(prefix: &str, mode: &str) {
    rcu_source(&format!("{prefix}_src"));
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{prefix}_v', '{}', NULL, NULL, '{mode}')",
        SMD_AGG_SQL.replace("{p}", prefix)
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run(&format!("ANALYZE {prefix}_src")).expect("analyze source");
}

fn smd_agg_sql(prefix: &str) -> String {
    SMD_AGG_SQL.replace("{p}", prefix)
}

/// Passthrough, IMMEDIATE, upsert of 1600 conflicting + 400 new rows of one plan.
#[pg_test]
fn pg_smd_immediate_passthrough_mixed_upsert_same_plan() {
    rcu_build("smd1", false, RC_IMMEDIATE);
    let before = rcu_all_plan_leaf_oids("smd1_v");
    smd_mixed_upsert("smd1", 1600, 1, 1);
    rcu_assert_only_plans_rebuilt("smd1", &before, &[]);
    assert_imv_correct("smd1_v", &rcu_view_sql("smd1"));
}

/// 1900 conflicting rows (95%, above the 0.9 threshold of an IMV with an
/// observing dependent, whose hot path is the leaf diff) + 400 new rows.
#[pg_test]
fn pg_smd_immediate_passthrough_mixed_upsert_with_dependent() {
    rcu_build("smd2", true, RC_IMMEDIATE);
    smd_mixed_upsert("smd2", 1900, 1, 1);
    assert_imv_correct("smd2_v", &rcu_view_sql("smd2"));
    assert_imv_correct("smd2_d", "SELECT plan, id, v FROM smd2_src");
}

/// Conflicts in plan 1 and new rows in plan 1 and plan 3, in one statement.
#[pg_test]
fn pg_smd_immediate_passthrough_mixed_upsert_across_plans() {
    rcu_build("smd3", false, RC_IMMEDIATE);
    Spi::run(&format!(
        "INSERT INTO smd3_src \
         SELECT plan, m, id, v + 1 FROM smd3_src WHERE plan IN (1, 2) AND id % {RCU_ROWS_PER_PLAN} <= 1600 \
         UNION ALL SELECT 1, g % 12, 100000 + g, g FROM generate_series(1, 400) g \
         UNION ALL SELECT 3, g % 12, 300000 + g, g FROM generate_series(1, 400) g \
         ON CONFLICT (plan, m, id) DO UPDATE SET v = EXCLUDED.v"
    ))
    .expect("upsert");
    assert_imv_correct("smd3_v", &rcu_view_sql("smd3"));
}

/// DEFERRED: the flush nets the upsert into one delta (plan 1: 1600 old images,
/// 2000 new) and rebuilds the plan, correctly.
#[pg_test]
fn pg_smd_deferred_passthrough_mixed_upsert_goes_hot() {
    rcu_build("smd4", false, RC_DEFERRED);
    let before = rcu_all_plan_leaf_oids("smd4_v");
    smd_mixed_upsert("smd4", 1600, 1, 1);
    rc_flush();
    rcu_assert_only_plans_rebuilt("smd4", &before, &[1]);
    assert_imv_correct("smd4_v", &rcu_view_sql("smd4"));
}

/// DEFERRED with an observing dependent: the plan goes hot through the leaf diff.
#[pg_test]
fn pg_smd_deferred_passthrough_mixed_upsert_with_dependent() {
    rcu_build("smd5", true, RC_DEFERRED);
    smd_mixed_upsert("smd5", 1900, 1, 1);
    rc_flush();
    assert_imv_correct("smd5_v", &rcu_view_sql("smd5"));
    assert_imv_correct("smd5_d", "SELECT plan, id, v FROM smd5_src");
}

/// Passthrough, IMMEDIATE, MERGE with matched UPDATE / DELETE and NOT MATCHED INSERT.
#[pg_test]
fn pg_smd_immediate_passthrough_merge() {
    rcu_build("smd6", false, RC_IMMEDIATE);
    let before = rcu_all_plan_leaf_oids("smd6_v");
    smd_merge("smd6");
    rcu_assert_only_plans_rebuilt("smd6", &before, &[]);
    assert_imv_correct("smd6_v", &rcu_view_sql("smd6"));
}

#[pg_test]
fn pg_smd_deferred_passthrough_merge_goes_hot() {
    rcu_build("smd7", false, RC_DEFERRED);
    let before = rcu_all_plan_leaf_oids("smd7_v");
    smd_merge("smd7");
    rc_flush();
    rcu_assert_only_plans_rebuilt("smd7", &before, &[1]);
    assert_imv_correct("smd7_v", &rcu_view_sql("smd7"));
}

/// The 1600 conflicting rows leave the IMV's filter, so the UPDATE trigger maps
/// to a DELETE (`DELETE_PROMOTED`), whose dispatch could rebuild the plan before
/// the INSERT trigger adds the 400 new rows.
#[pg_test]
fn pg_smd_immediate_filtered_passthrough_upsert_leaving_filter() {
    rcu_source("smd8_src");
    let view_sql = "SELECT plan, m, id, v FROM smd8_src WHERE v < 1000000";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('smd8_v', '{view_sql}', 'plan, m, id', NULL, 'IMMEDIATE', NULL, \
         ARRAY['plan', 'm'])"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("ANALYZE smd8_v").expect("analyze");
    Spi::run(
        "INSERT INTO smd8_src \
         SELECT plan, m, id, 2000000 FROM smd8_src WHERE plan = 1 AND id <= 1600 \
         UNION ALL SELECT 1, g % 12, 100000 + g, g FROM generate_series(1, 400) g \
         ON CONFLICT (plan, m, id) DO UPDATE SET v = EXCLUDED.v",
    )
    .expect("upsert");
    assert_imv_correct("smd8_v", view_sql);
}

/// Writable CTE: the INSERT reads the DELETE's rows, so the DELETE trigger fires first.
#[pg_test]
fn pg_smd_immediate_passthrough_writable_cte_delete_then_insert() {
    rcu_build("smd9", false, RC_IMMEDIATE);
    Spi::run(
        "WITH d AS (DELETE FROM smd9_src WHERE plan = 1 AND id <= 1600 RETURNING *) \
         INSERT INTO smd9_src SELECT plan, m, id + 100000, v FROM d WHERE id <= 400",
    )
    .expect("writable cte");
    assert_imv_correct("smd9_v", &rcu_view_sql("smd9"));
}

/// DEFERRED: the flush routes every operation through the netted dispatch, so a
/// bulk INSERT (1600 new rows into a 2000-row plan) goes hot too.
#[pg_test]
fn pg_smd_deferred_passthrough_bulk_insert_goes_hot() {
    rcu_build("smd10", false, RC_DEFERRED);
    let before = rcu_all_plan_leaf_oids("smd10_v");
    Spi::run(
        "INSERT INTO smd10_src SELECT 1, g % 12, 100000 + g, g FROM generate_series(1, 1600) g",
    )
    .expect("bulk insert");
    rc_flush();
    rcu_assert_only_plans_rebuilt("smd10", &before, &[1]);
    assert_imv_correct("smd10_v", &rcu_view_sql("smd10"));
}

/// The reviewer's reproduction: partitioned aggregate, IMMEDIATE, mixed upsert.
/// Was 800 mismatches (the 400 new rows counted twice).
#[pg_test]
fn pg_smd_immediate_aggregate_mixed_upsert() {
    smd_build_aggregate("smd11", RC_IMMEDIATE);
    smd_mixed_upsert("smd11", 1600, 1, 1);
    assert_imv_correct("smd11_v", &smd_agg_sql("smd11"));
}

#[pg_test]
fn pg_smd_immediate_aggregate_merge() {
    smd_build_aggregate("smd12", RC_IMMEDIATE);
    smd_merge("smd12");
    assert_imv_correct("smd12_v", &smd_agg_sql("smd12"));
}

/// DEFERRED: the netted flush may rebuild the plan, and stays correct.
#[pg_test]
fn pg_smd_deferred_aggregate_mixed_upsert_goes_hot() {
    smd_build_aggregate("smd13", RC_DEFERRED);
    let before = rcu_all_plan_leaf_oids("smd13_v");
    smd_mixed_upsert("smd13", 1600, 1, 1);
    rc_flush();
    rcu_assert_only_plans_rebuilt("smd13", &before, &[1]);
    assert_imv_correct("smd13_v", &smd_agg_sql("smd13"));
}

#[pg_test]
fn pg_smd_deferred_aggregate_merge() {
    smd_build_aggregate("smd14", RC_DEFERRED);
    smd_merge("smd14");
    rc_flush();
    assert_imv_correct("smd14_v", &smd_agg_sql("smd14"));
}

/// Unpartitioned source `{p}` (2000 rows), optionally analyzed so the statement
/// triggers knew its size (the removed Path B compared the statement's rows to it).
fn smd_plain_source(p: &str, analyzed: bool) {
    Spi::run(&format!(
        "CREATE TABLE {p} (id INT PRIMARY KEY, g INT, v INT)"
    ))
    .expect("source");
    Spi::run(&format!(
        "INSERT INTO {p} SELECT g, g % 10, g FROM generate_series(1, 2000) g"
    ))
    .expect("seed");
    if analyzed {
        Spi::run(&format!("ANALYZE {p}")).expect("analyze");
    }
}

/// 1600 conflicting rows (ids 401..2000) and 400 new ones (2001..2400).
fn smd_plain_mixed_upsert(p: &str) {
    Spi::run(&format!(
        "INSERT INTO {p} SELECT g, g % 10, g + 1 FROM generate_series(401, 2400) g \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v"
    ))
    .expect("mixed upsert");
}

/// Unpartitioned aggregate, analyzed source: the UPDATE trigger used to rebuild
/// the IMV (80% of the source changed) before the INSERT trigger added its rows.
#[pg_test]
fn pg_smd_immediate_unpartitioned_aggregate_mixed_upsert() {
    smd_plain_source("smd15", true);
    let sql = "SELECT g, SUM(v) AS s, COUNT(*) AS n FROM smd15 GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("smd15_v", sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    smd_plain_mixed_upsert("smd15");
    assert_imv_correct("smd15_v", sql);
}

/// Same for a keyed passthrough (was a 23505 on the IMV's key).
#[pg_test]
fn pg_smd_immediate_unpartitioned_passthrough_mixed_upsert() {
    smd_plain_source("smd16", true);
    let sql = "SELECT id, g, v FROM smd16";
    assert_eq!(
        crate::create_reflex_ivm("smd16_v", sql, Some("id"), None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    smd_plain_mixed_upsert("smd16");
    assert_imv_correct("smd16_v", sql);
}

/// Unanalyzed source: the statement-level size check is skipped and the
/// aggregate UPDATE reaches the post-scratch high-selectivity dispatch, which
/// rebuilds when every group is affected.
#[pg_test]
fn pg_smd_immediate_unpartitioned_aggregate_high_selectivity_mixed_upsert() {
    smd_plain_source("smd17", false);
    let sql = "SELECT g, SUM(v) AS s, COUNT(*) AS n FROM smd17 GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("smd17_v", sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    smd_plain_mixed_upsert("smd17");
    assert_imv_correct("smd17_v", sql);
}

/// MERGE on an analyzed unpartitioned source: INSERT, then UPDATE (80% of the
/// source), then DELETE triggers; a rebuild by the UPDATE trigger was followed by
/// the DELETE trigger subtracting its rows again.
#[pg_test]
fn pg_smd_immediate_unpartitioned_aggregate_merge() {
    smd_plain_source("smd18", true);
    let sql = "SELECT g, SUM(v) AS s, COUNT(*) AS n FROM smd18 GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("smd18_v", sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    Spi::run(
        "MERGE INTO smd18 t USING generate_series(401, 2400) s(i) ON t.id = s.i \
         WHEN MATCHED AND t.id % 10 = 0 THEN DELETE \
         WHEN MATCHED THEN UPDATE SET v = t.v + 1 \
         WHEN NOT MATCHED THEN INSERT VALUES (s.i, s.i % 10, s.i)",
    )
    .expect("merge");
    assert_imv_correct("smd18_v", sql);
}

/// The 1.11.5 migration cuts the rebuild out of each installed 1.11.4
/// statement-trigger body, leaving exactly what 1.11.5 renders, and the source's
/// mixed upsert is then maintained correctly.
#[pg_test]
fn pg_smd_migration_removes_rebuild_from_statement_trigger_bodies() {
    smd_plain_source("smd19", true);
    let sql = "SELECT g, SUM(v) AS s, COUNT(*) AS n FROM smd19 GROUP BY g";
    assert_eq!(
        crate::create_reflex_ivm("smd19_v", sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    let function_of = |ddl: &str| -> String {
        ddl.split(";\nCREATE OR REPLACE TRIGGER")
            .next()
            .expect("function ddl")
            .to_string()
    };
    let old_ddls = crate::schema_builder::build_trigger_ddls_from_body(
        "smd19",
        include_str!("fixtures/trigger_body_1.11.4.plpgsql.in"),
    );
    for ddl in &old_ddls[..3] {
        Spi::run(&function_of(ddl)).expect("install a 1.11.4 body");
    }
    let rebuilding = || {
        Spi::get_one::<i64>(
            "SELECT count(*)::int8 FROM pg_proc WHERE proname LIKE '\\_\\_reflex\\_%\\_trigger\\_on\\_smd19' \
             AND strpos(prosrc, 'reflex_reconcile') > 0",
        )
        .expect("count")
        .unwrap_or(0)
    };
    assert_eq!(rebuilding(), 3, "precondition: 1.11.4 bodies rebuild");

    let migration = include_str!("../../sql/pg_reflex--1.11.4--1.11.5.sql");
    let start = migration
        .find("DO $stmt_triggers$")
        .expect("migration step 5");
    let end = migration[start..]
        .find("$stmt_triggers$;")
        .expect("step 5 terminates")
        + start
        + "$stmt_triggers$;".len();
    Spi::run(&migration[start..end]).expect("migration step 5");

    assert_eq!(rebuilding(), 0, "a statement-trigger body still rebuilds");
    for ddl in &crate::schema_builder::build_trigger_ddls("smd19")[..3] {
        let function = function_of(ddl);
        let name = function
            .split("FUNCTION public.")
            .nth(1)
            .and_then(|r| r.split("()").next())
            .expect("function name");
        let body = function.split("$fn$").nth(1).expect("function body");
        let installed = Spi::get_one::<String>(&format!(
            "SELECT prosrc FROM pg_proc WHERE oid = 'public.{name}()'::regprocedure"
        ))
        .expect("prosrc")
        .expect("prosrc value");
        assert_eq!(installed, body, "{name} differs from the 1.11.5 rendering");
    }
    smd_plain_mixed_upsert("smd19");
    assert_imv_correct("smd19_v", sql);
}
