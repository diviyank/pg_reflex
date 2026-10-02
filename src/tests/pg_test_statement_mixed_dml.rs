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
/// Not on PG15: its MERGE into a multi-level partitioned table hands the AFTER
/// UPDATE statement trigger a NEW transition table holding INSERTed rows (see
/// untreated_bugs/2026-10-02_pg15_merge_partitioned_transition_tables.md).
#[cfg(not(feature = "pg15"))]
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

/// Keyless passthrough (no source key reaches the IMV): `{p}` holds 2000 rows
/// with many identical (g, v, t) projections, NULLs and empty strings.
fn smd_keyless_source(p: &str) {
    Spi::run(&format!(
        "CREATE TABLE {p} (id INT PRIMARY KEY, g INT, v INT, t TEXT)"
    ))
    .expect("source");
    Spi::run(&format!(
        "INSERT INTO {p} SELECT i, i % 3, CASE WHEN i % 4 = 0 THEN NULL ELSE i % 2 END, \
         CASE i % 5 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'x' END \
         FROM generate_series(1, 2000) i"
    ))
    .expect("seed");
}

fn smd_keyless_build(p: &str, mode: &str) -> String {
    smd_keyless_source(&format!("{p}_src"));
    let sql = format!("SELECT g, v, t FROM {p}_src");
    assert_eq!(
        crate::create_reflex_ivm(&format!("{p}_v"), &sql, None, None, Some(mode), None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    sql
}

/// Keyless passthrough, IMMEDIATE, upsert of 100 conflicting + 100 new rows:
/// the UPDATE trigger used to refresh the whole target from the source (which
/// already held the 100 new rows), then the INSERT trigger appended them again.
#[pg_test]
fn pg_smd_immediate_keyless_passthrough_mixed_upsert() {
    let sql = smd_keyless_build("smd20", RC_IMMEDIATE);
    Spi::run(
        "INSERT INTO smd20_src SELECT i, i % 7, i, 'y' FROM generate_series(1901, 2100) i \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, t = EXCLUDED.t",
    )
    .expect("upsert");
    assert_imv_correct("smd20_v", &sql);
}

/// Writable CTE: DELETE 100 rows, INSERT 100 new ones (DELETE trigger first).
#[pg_test]
fn pg_smd_immediate_keyless_passthrough_writable_cte() {
    let sql = smd_keyless_build("smd21", RC_IMMEDIATE);
    Spi::run(
        "WITH d AS (DELETE FROM smd21_src WHERE id <= 100 RETURNING *) \
         INSERT INTO smd21_src SELECT id + 10000, g, v, t FROM d",
    )
    .expect("writable cte");
    assert_imv_correct("smd21_v", &sql);
}

/// Duplicates, NULLs and empty strings: each UPDATE / DELETE must remove exactly
/// one target row per changed source row, whichever identical copy it is.
#[pg_test]
fn pg_smd_immediate_keyless_passthrough_duplicates_and_nulls() {
    let sql = smd_keyless_build("smd22", RC_IMMEDIATE);
    for stmt in [
        "UPDATE smd22_src SET v = NULL WHERE id % 9 = 0",
        "UPDATE smd22_src SET t = '' WHERE t IS NULL AND id % 2 = 0",
        "UPDATE smd22_src SET t = NULL WHERE t = '' AND id % 3 = 0",
        "DELETE FROM smd22_src WHERE id % 11 = 0",
        "DELETE FROM smd22_src WHERE v IS NULL AND id % 13 = 0",
        "UPDATE smd22_src SET g = g + 1 WHERE id % 17 = 0",
        "INSERT INTO smd22_src SELECT i, 1, NULL, NULL FROM generate_series(1990, 2050) i \
         ON CONFLICT (id) DO UPDATE SET v = NULL, t = NULL",
    ] {
        Spi::run(stmt).unwrap_or_else(|e| panic!("<{stmt}>: {e}"));
        assert_imv_correct("smd22_v", &sql);
    }
}

/// Keyless passthrough over a join: both sources reach the IMV without a key.
#[pg_test]
fn pg_smd_immediate_keyless_join_passthrough() {
    smd_keyless_source("smd23_src");
    Spi::run("CREATE TABLE smd23_dim (g INT, name TEXT)").expect("dim");
    Spi::run("INSERT INTO smd23_dim VALUES (0, 'a'), (1, 'b'), (1, 'b'), (2, NULL)")
        .expect("seed dim");
    let sql = "SELECT s.g, s.v, d.name FROM smd23_src s JOIN smd23_dim d ON d.g = s.g";
    assert_eq!(
        crate::create_reflex_ivm("smd23_v", sql, None, None, None, None),
        "CREATE REFLEX INCREMENTAL VIEW"
    );
    for stmt in [
        "INSERT INTO smd23_src SELECT i, i % 7, i, 'y' FROM generate_series(1901, 2100) i \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        "DELETE FROM smd23_src WHERE id % 10 = 0",
        "UPDATE smd23_dim SET name = 'c' WHERE g = 1 AND ctid = (SELECT min(ctid) FROM smd23_dim WHERE g = 1)",
        "DELETE FROM smd23_dim WHERE g = 2",
    ] {
        Spi::run(stmt).unwrap_or_else(|e| panic!("<{stmt}>: {e}"));
        assert_imv_correct("smd23_v", sql);
    }
}

/// DEFERRED keyless passthrough: the flush nets the upsert and stays correct.
#[pg_test]
fn pg_smd_deferred_keyless_passthrough_mixed_upsert() {
    let sql = smd_keyless_build("smd24", RC_DEFERRED);
    Spi::run(
        "INSERT INTO smd24_src SELECT i, i % 7, i, 'y' FROM generate_series(1901, 2100) i \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, t = EXCLUDED.t",
    )
    .expect("upsert");
    rc_flush();
    assert_imv_correct("smd24_v", &sql);
}

/// Partition flush of an unpartitioned IMV: three children attached in one
/// flush (100, 3000 and 100 rows over an analyzed 2000-row root). The 3000-row
/// child is large enough to reconcile the IMV, which reads every attached child;
/// the next child's delta must not then be applied on top.
#[pg_test]
fn pg_smd_partition_flush_reconcile_then_delta_not_double_applied() {
    Spi::run("CREATE TABLE smd25_src (k INT NOT NULL, g INT, v INT) PARTITION BY LIST (k)")
        .expect("root");
    Spi::run("CREATE TABLE smd25_src_0 PARTITION OF smd25_src FOR VALUES IN (0)").expect("p0");
    Spi::run("INSERT INTO smd25_src SELECT 0, i % 4, i FROM generate_series(1, 2000) i")
        .expect("seed");
    let sql = "SELECT g, SUM(v) AS s, COUNT(*) AS n FROM smd25_src GROUP BY g";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('smd25_v', '{sql}', NULL, NULL, NULL, NULL, ARRAY[]::text[])"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("ANALYZE smd25_src").expect("analyze");
    for (k, rows) in [(1, 100), (2, 3000), (3, 100)] {
        Spi::run(&format!(
            "CREATE TABLE smd25_src_{k} (k INT NOT NULL, g INT, v INT)"
        ))
        .expect("child");
        Spi::run(&format!(
            "INSERT INTO smd25_src_{k} SELECT {k}, i % 4, i FROM generate_series(1, {rows}) i"
        ))
        .expect("child rows");
        Spi::run(&format!(
            "ALTER TABLE smd25_src ATTACH PARTITION smd25_src_{k} FOR VALUES IN ({k})"
        ))
        .expect("attach");
    }
    Spi::run("SELECT reflex_flush_partitions()").expect("flush");
    assert_imv_correct("smd25_v", sql);
}

/// A statement trigger's partitioned passthrough maintenance is the plain keyed
/// cold body (pruned to the touched LIST values): no volume-dispatch helpers are
/// called, so it neither pays for them nor needs them during an upgrade. The
/// DEFERRED flush's netted UPDATE still dispatches.
#[pg_test]
fn pg_smd_immediate_partitioned_passthrough_sql_has_no_dispatch() {
    rcu_build("smd26", false, RC_IMMEDIATE);
    let sql_for = |op: &str| {
        Spi::get_one::<String>(&format!(
            "SELECT reflex_build_delta_sql(r.name, 'smd26_src', '{op}', r.base_query, r.end_query, \
             r.aggregations::text, r.base_query) FROM public.__reflex_ivm_reference r WHERE r.name = 'smd26_v'"
        ))
        .expect("delta sql")
        .expect("delta sql value")
    };
    for op in ["UPDATE", "DELETE"] {
        let sql = sql_for(op);
        assert!(
            !sql.contains("__reflex_rebuild_cost_rows")
                && !sql.contains("__reflex_target_propagates")
                && !sql.contains("reflex_reconcile"),
            "{op}: statement trigger SQL still dispatches:\n{sql}"
        );
        assert!(
            sql.contains("= ANY($1::text[]::"),
            "{op}: LIST pruning lost:\n{sql}"
        );
    }
    let netted = sql_for("UPDATE_NETTED");
    assert!(
        netted.contains("__reflex_rebuild_cost_rows"),
        "the flush no longer dispatches:\n{netted}"
    );
}

/// Touched rows with a NULL partition value (DEFAULT partition) are maintained
/// without the value restriction, which could not match them.
#[pg_test]
fn pg_smd_immediate_partitioned_passthrough_null_partition_value() {
    Spi::run(
        "CREATE TABLE smd27_src (plan INT, id INT NOT NULL, v INT, UNIQUE (plan, id)) PARTITION BY LIST (plan)",
    )
    .expect("root");
    Spi::run("CREATE TABLE smd27_src_1 PARTITION OF smd27_src FOR VALUES IN (1)").expect("p1");
    Spi::run("CREATE TABLE smd27_src_d PARTITION OF smd27_src DEFAULT").expect("default");
    Spi::run(
        "INSERT INTO smd27_src SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE 1 END, i, i FROM generate_series(1, 300) i",
    )
    .expect("seed");
    let sql = "SELECT plan, id, v FROM smd27_src";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('smd27_v', '{sql}', 'plan, id', NULL, 'IMMEDIATE', NULL, ARRAY['plan'])"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    for stmt in [
        "UPDATE smd27_src SET v = v + 1 WHERE id % 2 = 0",
        "DELETE FROM smd27_src WHERE id % 5 = 0",
    ] {
        Spi::run(stmt).unwrap_or_else(|e| panic!("<{stmt}>: {e}"));
        assert_imv_correct("smd27_v", sql);
    }
}

/// The DEFERRED flush's dispatch restricted its cold body to one representative
/// value per cold child and skipped NULL values, so rows of a DEFAULT partition
/// (NULL partition value) were never maintained.
#[pg_test]
fn pg_smd_deferred_partitioned_passthrough_null_partition_value() {
    Spi::run(
        "CREATE TABLE smd28_src (plan INT, id INT NOT NULL, v INT, UNIQUE (plan, id)) PARTITION BY LIST (plan)",
    )
    .expect("root");
    Spi::run("CREATE TABLE smd28_src_1 PARTITION OF smd28_src FOR VALUES IN (1)").expect("p1");
    Spi::run("CREATE TABLE smd28_src_d PARTITION OF smd28_src DEFAULT").expect("default");
    Spi::run(
        "INSERT INTO smd28_src SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE 1 END, i, i FROM generate_series(1, 300) i",
    )
    .expect("seed");
    let sql = "SELECT plan, id, v FROM smd28_src";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('smd28_v', '{sql}', 'plan, id', NULL, 'DEFERRED', NULL, ARRAY['plan'])"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    for stmt in [
        "UPDATE smd28_src SET v = v + 1 WHERE id % 2 = 0",
        "DELETE FROM smd28_src WHERE id % 5 = 0",
    ] {
        Spi::run(stmt).unwrap_or_else(|e| panic!("<{stmt}>: {e}"));
        rc_flush();
        assert_imv_correct("smd28_v", sql);
    }
}

/// A LIST child holding several values (`IN (1, 2)`): the cold body must cover
/// every touched value of the child, not one representative.
fn smd_multivalue_list(p: &str, mode: &str) {
    Spi::run(&format!(
        "CREATE TABLE {p}_src (plan INT NOT NULL, id INT NOT NULL, v INT, UNIQUE (plan, id)) PARTITION BY LIST (plan)"
    ))
    .expect("root");
    Spi::run(&format!(
        "CREATE TABLE {p}_src_12 PARTITION OF {p}_src FOR VALUES IN (1, 2)"
    ))
    .expect("p12");
    Spi::run(&format!(
        "CREATE TABLE {p}_src_3 PARTITION OF {p}_src FOR VALUES IN (3)"
    ))
    .expect("p3");
    Spi::run(&format!(
        "INSERT INTO {p}_src SELECT 1 + i % 3, i, i FROM generate_series(1, 3000) i"
    ))
    .expect("seed");
    let sql = format!("SELECT plan, id, v FROM {p}_src");
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{p}_v', '{sql}', 'plan, id', NULL, '{mode}', NULL, ARRAY['plan'])"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    for stmt in [
        format!("UPDATE {p}_src SET v = v + 1 WHERE id % 50 = 0"),
        format!("DELETE FROM {p}_src WHERE id % 70 = 0"),
    ] {
        Spi::run(&stmt).unwrap_or_else(|e| panic!("<{stmt}>: {e}"));
        rc_flush();
        assert_imv_correct(&format!("{p}_v"), &sql);
    }
}

#[pg_test]
fn pg_smd_deferred_partitioned_passthrough_multivalue_list_child() {
    smd_multivalue_list("smd29", RC_DEFERRED);
}

#[pg_test]
fn pg_smd_immediate_partitioned_passthrough_multivalue_list_child() {
    smd_multivalue_list("smd30", RC_IMMEDIATE);
}

/// A multi-value LIST child going hot: the cold body must exclude every touched
/// value of the rebuilt child, not only its representative one. Here 80% of
/// plan 1 moves to plan 2 (same child): the child is rebuilt, and the cold
/// insert of the moved rows must not run on it again.
#[pg_test]
fn pg_smd_deferred_passthrough_hot_multivalue_list_child() {
    smd_multivalue_list("smd31", RC_DEFERRED);
    Spi::run("CREATE TABLE smd31_src_4 PARTITION OF smd31_src FOR VALUES IN (4)").expect("p4");
    Spi::run("UPDATE smd31_src SET plan = 2 WHERE plan = 1 AND id % 5 <> 0").expect("move");
    rc_flush();
    assert_imv_correct("smd31_v", "SELECT plan, id, v FROM smd31_src");
}

/// Same for a partitioned aggregate (one group per row): a hot multi-value LIST
/// child is rebuilt, and its other value's groups must not also be merged cold.
#[pg_test]
fn pg_smd_deferred_aggregate_hot_multivalue_list_child() {
    Spi::run(
        "CREATE TABLE smd32_src (plan INT NOT NULL, id INT NOT NULL, v INT) PARTITION BY LIST (plan)",
    )
    .expect("root");
    Spi::run("CREATE TABLE smd32_src_12 PARTITION OF smd32_src FOR VALUES IN (1, 2)").expect("p12");
    Spi::run("CREATE TABLE smd32_src_3 PARTITION OF smd32_src FOR VALUES IN (3)").expect("p3");
    Spi::run("CREATE TABLE smd32_src_4 PARTITION OF smd32_src FOR VALUES IN (4)").expect("p4");
    Spi::run("INSERT INTO smd32_src SELECT 1 + i % 4, i, i FROM generate_series(1, 4000) i")
        .expect("seed");
    let sql = "SELECT plan, id, SUM(v) AS s, COUNT(*) AS n FROM smd32_src GROUP BY plan, id";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('smd32_v', '{sql}', NULL, NULL, 'DEFERRED')"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("ANALYZE smd32_src").expect("analyze");
    Spi::run("UPDATE smd32_src SET v = v + 1 WHERE plan IN (1, 2)").expect("update");
    rc_flush();
    assert_imv_correct("smd32_v", sql);
}

/// A NULL partition value lives in the DEFAULT child. The flush dispatch resolved
/// it to no child, so it stayed cold even when its DEFAULT child went hot: the
/// child's rebuild already held the NULL groups' new values, and the cold MERGE
/// added their delta again.
fn smd_null_in_hot_default(p: &str, strategy: &str) {
    Spi::run(&format!(
        "CREATE TABLE {p}_src (plan INT, id INT NOT NULL, v INT) PARTITION BY {strategy} (plan)"
    ))
    .expect("root");
    let (b1, b2) = if strategy == "LIST" {
        ("IN (1)", "IN (2)")
    } else {
        ("FROM (1) TO (2)", "FROM (2) TO (3)")
    };
    Spi::run(&format!(
        "CREATE TABLE {p}_src_1 PARTITION OF {p}_src FOR VALUES {b1}"
    ))
    .expect("p1");
    Spi::run(&format!(
        "CREATE TABLE {p}_src_2 PARTITION OF {p}_src FOR VALUES {b2}"
    ))
    .expect("p2");
    Spi::run(&format!(
        "CREATE TABLE {p}_src_d PARTITION OF {p}_src DEFAULT"
    ))
    .expect("default");
    Spi::run(&format!(
        "INSERT INTO {p}_src SELECT CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN 9 ELSE i % 4 - 1 END, i, i \
         FROM generate_series(1, 8000) i"
    ))
    .expect("seed");
    let sql = format!("SELECT plan, id, SUM(v) AS s, COUNT(*) AS n FROM {p}_src GROUP BY plan, id");
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('{p}_v', '{sql}', NULL, NULL, 'DEFERRED')"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run(&format!("ANALYZE {p}_src")).expect("analyze");
    Spi::run("SET LOCAL reflex.wipe_threshold = 0.2").expect("threshold");
    Spi::run(&format!(
        "UPDATE {p}_src SET v = v + 1 WHERE plan IS NULL OR plan = 9"
    ))
    .expect("update");
    rc_flush();
    assert_imv_correct(&format!("{p}_v"), &sql);
}

#[pg_test]
fn pg_smd_deferred_aggregate_null_in_hot_list_default() {
    smd_null_in_hot_default("smd33", "LIST");
}

#[pg_test]
fn pg_smd_deferred_aggregate_null_in_hot_range_default() {
    smd_null_in_hot_default("smd34", "RANGE");
}

/// Two-level source (LIST plan -> RANGE m) with a sub-partitioned DEFAULT plan
/// child: NULL and unlisted plans resolve to the DEFAULT child, which goes hot
/// as a whole, while plan 1 stays cold.
#[pg_test]
fn pg_smd_deferred_two_level_null_in_hot_default() {
    Spi::run("CREATE TABLE smd35_src (plan INT, m INT NOT NULL, id INT NOT NULL, v INT) PARTITION BY LIST (plan)")
        .expect("root");
    for (child, bound) in [
        ("1", "FOR VALUES IN (1)"),
        ("2", "FOR VALUES IN (2)"),
        ("d", "DEFAULT"),
    ] {
        Spi::run(&format!(
            "CREATE TABLE smd35_src_{child} PARTITION OF smd35_src {bound} PARTITION BY RANGE (m)"
        ))
        .expect("plan child");
        Spi::run(&format!(
            "CREATE TABLE smd35_src_{child}_a PARTITION OF smd35_src_{child} FOR VALUES FROM (0) TO (6)"
        ))
        .expect("leaf a");
        Spi::run(&format!(
            "CREATE TABLE smd35_src_{child}_b PARTITION OF smd35_src_{child} FOR VALUES FROM (6) TO (12)"
        ))
        .expect("leaf b");
    }
    Spi::run(
        "INSERT INTO smd35_src SELECT CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN 9 ELSE i % 4 - 1 END, i % 12, i, i \
         FROM generate_series(1, 8000) i",
    )
    .expect("seed");
    let sql = "SELECT plan, m, id, SUM(v) AS s, COUNT(*) AS n FROM smd35_src GROUP BY plan, m, id";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('smd35_v', '{sql}', NULL, NULL, 'DEFERRED')"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("ANALYZE smd35_src").expect("analyze");
    Spi::run("SET LOCAL reflex.wipe_threshold = 0.2").expect("threshold");
    Spi::run("UPDATE smd35_src SET v = v + 1 WHERE plan IS NULL OR plan = 9 OR (plan = 1 AND id % 50 = 0)")
        .expect("update");
    rc_flush();
    assert_imv_correct("smd35_v", sql);
}

/// A passthrough over the same two-level shape: NULL and unlisted plans in the
/// hot DEFAULT child are rebuilt once, not also re-inserted cold.
#[pg_test]
fn pg_smd_deferred_passthrough_null_in_hot_default() {
    Spi::run("CREATE TABLE smd36_src (plan INT, id INT NOT NULL, v INT, UNIQUE (plan, id)) PARTITION BY LIST (plan)")
        .expect("root");
    Spi::run("CREATE TABLE smd36_src_1 PARTITION OF smd36_src FOR VALUES IN (1)").expect("p1");
    Spi::run("CREATE TABLE smd36_src_d PARTITION OF smd36_src DEFAULT").expect("default");
    Spi::run(
        "INSERT INTO smd36_src SELECT CASE i % 3 WHEN 0 THEN NULL WHEN 1 THEN 9 ELSE 1 END, i, i \
         FROM generate_series(1, 6000) i",
    )
    .expect("seed");
    let sql = "SELECT plan, id, v FROM smd36_src";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('smd36_v', '{sql}', 'plan, id', NULL, 'DEFERRED', NULL, ARRAY['plan'])"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("ANALYZE smd36_src").expect("analyze");
    Spi::run("SET LOCAL reflex.wipe_threshold = 0.2").expect("threshold");
    Spi::run("UPDATE smd36_src SET v = v + 1 WHERE plan IS NULL OR plan = 9").expect("update");
    rc_flush();
    assert_imv_correct("smd36_v", sql);
}

/// A LIST child listing NULL, touched only by NULL rows: it has no text
/// representative for `reflex_reconcile_partition`, so it must stay cold
/// (maintained by the delta) rather than be excluded from the cold body
/// without being rebuilt.
#[pg_test]
fn pg_smd_deferred_aggregate_null_only_hot_child_stays_cold() {
    Spi::run("CREATE TABLE smd37_src (plan INT, id INT NOT NULL, v INT) PARTITION BY LIST (plan)")
        .expect("root");
    Spi::run("CREATE TABLE smd37_src_1 PARTITION OF smd37_src FOR VALUES IN (1)").expect("p1");
    Spi::run("CREATE TABLE smd37_src_n PARTITION OF smd37_src FOR VALUES IN (NULL, 2)")
        .expect("pnull");
    Spi::run(
        "INSERT INTO smd37_src SELECT CASE i % 3 WHEN 0 THEN NULL WHEN 1 THEN 1 ELSE 2 END, i, i \
         FROM generate_series(1, 6000) i",
    )
    .expect("seed");
    let sql = "SELECT plan, id, SUM(v) AS s, COUNT(*) AS n FROM smd37_src GROUP BY plan, id";
    let created = Spi::get_one::<String>(&format!(
        "SELECT create_reflex_ivm('smd37_v', '{sql}', NULL, NULL, 'DEFERRED')"
    ))
    .expect("create call")
    .expect("create result");
    assert_eq!(created, "CREATE REFLEX INCREMENTAL VIEW");
    Spi::run("ANALYZE smd37_src").expect("analyze");
    Spi::run("SET LOCAL reflex.wipe_threshold = 0.2").expect("threshold");
    Spi::run("UPDATE smd37_src SET v = v + 1 WHERE plan IS NULL").expect("update");
    rc_flush();
    assert_imv_correct("smd37_v", sql);
}

/// The keyless-IMMEDIATE WARNING is decided from the plan create_reflex_ivm
/// stores: a passthrough without the source PK warns, one with it does not.
#[pg_test]
fn pg_smd_keyless_immediate_passthrough_warning_follows_stored_plan() {
    Spi::run("CREATE TABLE smd38_src (id INT PRIMARY KEY, g INT, v INT)").expect("source");
    for (view, sql) in [
        ("smd38_keyless", "SELECT g, v FROM smd38_src"),
        ("smd38_keyed", "SELECT id, g, v FROM smd38_src"),
    ] {
        assert_eq!(
            crate::create_reflex_ivm(view, sql, None, None, None, None),
            "CREATE REFLEX INCREMENTAL VIEW"
        );
    }
    let warning_for = |view: &str| {
        let (plan, depends_on) = Spi::get_two::<String, Vec<String>>(&format!(
            "SELECT aggregations::text, depends_on FROM public.__reflex_ivm_reference WHERE name = '{view}'"
        ))
        .expect("registry row");
        let plan: crate::aggregation::AggregationPlan =
            serde_json::from_str(&plan.expect("plan")).expect("plan json");
        crate::create_ivm::keyless_immediate_warning(
            view,
            "IMMEDIATE",
            &plan,
            &depends_on.expect("depends_on"),
            &[],
        )
    };
    let msg = warning_for("smd38_keyless").expect("keyless warns");
    assert!(msg.contains("smd38_src"), "{msg}");
    assert_eq!(warning_for("smd38_keyed"), None);
}
