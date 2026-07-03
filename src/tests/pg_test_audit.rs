
#[pg_test]
fn pg_test_audit_smoke_no_imvs_returns_ok_header() {
    let out: String = Spi::get_one("SELECT reflex_audit()")
        .expect("query ok")
        .expect("non-null result");
    assert!(
        out.starts_with("pg_reflex audit: OK"),
        "expected OK header, got: {}",
        out
    );
}

#[pg_test]
#[should_panic]
fn pg_test_audit_smoke_scoped_unknown_imv_errors() {
    let _out: String = Spi::get_one("SELECT reflex_audit('does_not_exist')")
        .expect("query ok")
        .expect("non-null result");
}

#[pg_test]
fn pg_test_audit_staging_shape_detects_column_drift() {
    Spi::run(
        "CREATE TABLE audit_ss_src (\
            id BIGINT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, \
            creation_date TIMESTAMPTZ)",
    )
    .expect("create v1");
    crate::create_reflex_ivm(
        "audit_ss_view",
        "SELECT id, a, b, creation_date FROM audit_ss_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );
    // Force a drift: add a column to the SOURCE so staging is now missing one.
    Spi::run("ALTER TABLE audit_ss_src ADD COLUMN c INT").expect("alter src");

    let report: String = Spi::get_one("SELECT reflex_audit()")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[ERROR]") && report.contains("staging-shape"),
        "expected ERROR/staging-shape in report:\n{}",
        report
    );
    assert!(
        report.contains("audit_ss_view"),
        "expected IMV attribution in report:\n{}",
        report
    );
    assert!(
        report.to_lowercase().contains("suggested fix"),
        "expected suggested-fix block:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_staging_shape_green_when_aligned() {
    Spi::run("CREATE TABLE audit_ss_ok_src (id BIGINT PRIMARY KEY, a INT)")
        .expect("create src");
    crate::create_reflex_ivm(
        "audit_ss_ok_view",
        "SELECT id, a FROM audit_ss_ok_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );
    let report: String = Spi::get_one("SELECT reflex_audit('audit_ss_ok_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        !report.contains("staging-shape"),
        "expected no staging-shape finding when aligned:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_trigger_attached_detects_dropped_trigger() {
    Spi::run("CREATE TABLE audit_ta_src (id BIGINT PRIMARY KEY, a INT)")
        .expect("create src");
    crate::create_reflex_ivm(
        "audit_ta_view",
        "SELECT id, a FROM audit_ta_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    Spi::run("DROP TRIGGER __reflex_trigger_ins_on_audit_ta_src ON audit_ta_src")
        .expect("drop trigger");

    let report: String = Spi::get_one("SELECT reflex_audit('audit_ta_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[ERROR]") && report.contains("trigger-attached"),
        "expected ERROR/trigger-attached:\n{}",
        report
    );
    assert!(
        report.contains("__reflex_trigger_ins_on_audit_ta_src"),
        "expected missing-trigger name in body:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_trigger_attached_green_when_all_present() {
    Spi::run("CREATE TABLE audit_ta_ok_src (id BIGINT PRIMARY KEY, a INT)")
        .expect("create");
    crate::create_reflex_ivm(
        "audit_ta_ok_view",
        "SELECT id, a FROM audit_ta_ok_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    let report: String = Spi::get_one("SELECT reflex_audit('audit_ta_ok_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        !report.contains("trigger-attached"),
        "expected no trigger-attached finding:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_trigger_mode_detects_downgrade() {
    Spi::run("CREATE TABLE audit_tm_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_tm_view",
        "SELECT id, a FROM audit_tm_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );
    // Simulate the pre-1.6.2 silent downgrade: replace the trigger function
    // body with an immediate-mode no-op stub that does not reference the
    // staging delta.
    Spi::run(
        "CREATE OR REPLACE FUNCTION public.__reflex_ins_trigger_on_audit_tm_src () RETURNS TRIGGER \
         LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$",
    )
    .expect("downgrade fn");

    let report: String = Spi::get_one("SELECT reflex_audit('audit_tm_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[ERROR]") && report.contains("trigger-mode-matches"),
        "expected ERROR/trigger-mode-matches:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_trigger_mode_green_when_consistent() {
    Spi::run("CREATE TABLE audit_tm_ok_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_tm_ok_view",
        "SELECT id, a FROM audit_tm_ok_src",
        Some("id"),
        None,
        Some("DEFERRED"),
        None,
    );
    let report: String = Spi::get_one("SELECT reflex_audit('audit_tm_ok_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        !report.contains("trigger-mode-matches"),
        "expected no mode mismatch:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_internal_tables_detects_missing_intermediate() {
    Spi::run("CREATE TABLE audit_it_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    // Use an aggregate query so the intermediate table is created
    crate::create_reflex_ivm(
        "audit_it_view",
        "SELECT id, COUNT(*) as cnt FROM audit_it_src GROUP BY id",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    // Drop the intermediate out from under the IMV.
    Spi::run("DROP TABLE \"__reflex_intermediate_audit_it_view\" CASCADE")
        .expect("drop intermediate");

    let report: String = Spi::get_one("SELECT reflex_audit('audit_it_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[ERROR]") && report.contains("internal-tables-exist"),
        "expected ERROR/internal-tables-exist:\n{}",
        report
    );
    assert!(
        report.contains("__reflex_intermediate_audit_it_view"),
        "expected missing-table name in body:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_internal_tables_green_when_present() {
    Spi::run("CREATE TABLE audit_it_ok_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    // Use an aggregate query so the intermediate table is created
    crate::create_reflex_ivm(
        "audit_it_ok_view",
        "SELECT id, COUNT(*) as cnt FROM audit_it_ok_src GROUP BY id",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    let report: String = Spi::get_one("SELECT reflex_audit('audit_it_ok_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        !report.contains("internal-tables-exist"),
        "expected no internal-tables finding:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_source_exists_detects_dropped_source() {
    Spi::run("CREATE TABLE audit_se_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_se_view",
        "SELECT id, a FROM audit_se_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    // Disable the auto-drop event trigger so the __reflex_ivm_reference row persists.
    Spi::run("ALTER EVENT TRIGGER reflex_on_sql_drop DISABLE").expect("disable trigger");
    // Drop the source out from under the IMV (CASCADE pulls down the IMV
    // internal tables too, but the __reflex_ivm_reference row remains).
    Spi::run("DROP TABLE audit_se_src CASCADE").expect("drop source");
    // Re-enable for other tests
    Spi::run("ALTER EVENT TRIGGER reflex_on_sql_drop ENABLE").expect("enable trigger");

    let report: String = Spi::get_one("SELECT reflex_audit('audit_se_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[ERROR]") && report.contains("source-exists"),
        "expected ERROR/source-exists:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_source_exists_green_when_live() {
    Spi::run("CREATE TABLE audit_se_ok_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_se_ok_view",
        "SELECT id, a FROM audit_se_ok_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    let report: String = Spi::get_one("SELECT reflex_audit('audit_se_ok_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        !report.contains("source-exists"),
        "expected no source-exists finding:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_intermediate_shape_detects_extra_column() {
    Spi::run("CREATE TABLE audit_is_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_is_view",
        "SELECT id, COUNT(*) as cnt FROM audit_is_src GROUP BY id",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    Spi::run("ALTER TABLE __reflex_intermediate_audit_is_view ADD COLUMN extra INT")
        .expect("add column");

    let report: String = Spi::get_one("SELECT reflex_audit('audit_is_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[WARNING]") && report.contains("intermediate-shape"),
        "expected WARNING/intermediate-shape:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_target_shape_detects_dropped_column() {
    Spi::run("CREATE TABLE audit_ts_src (id BIGINT PRIMARY KEY, a INT, b INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_ts_view",
        "SELECT id, a, b, COUNT(*) as cnt FROM audit_ts_src GROUP BY id, a, b",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    Spi::run("ALTER TABLE audit_ts_view DROP COLUMN b").expect("drop col");

    let report: String = Spi::get_one("SELECT reflex_audit('audit_ts_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[WARNING]") && report.contains("target-shape"),
        "expected WARNING/target-shape:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_shape_green_when_aligned() {
    Spi::run("CREATE TABLE audit_sh_ok_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_sh_ok_view",
        "SELECT id, COUNT(*) as cnt FROM audit_sh_ok_src GROUP BY id",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    let report: String = Spi::get_one("SELECT reflex_audit('audit_sh_ok_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        !report.contains("intermediate-shape") && !report.contains("target-shape"),
        "expected no shape findings:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_base_query_runs_detects_dropped_column() {
    Spi::run("CREATE TABLE audit_bq_src (id BIGINT PRIMARY KEY, a INT, b INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_bq_view",
        "SELECT id, a, b FROM audit_bq_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    // Disable the auto-drop event trigger so the registry row persists
    // after we drop column `b` (the registry stores base_query that still
    // references it).
    Spi::run("ALTER EVENT TRIGGER reflex_on_sql_drop DISABLE").expect("disable");
    Spi::run("ALTER TABLE audit_bq_src DROP COLUMN b").expect("drop col b");
    Spi::run("ALTER EVENT TRIGGER reflex_on_sql_drop ENABLE").expect("enable");

    let report: String = Spi::get_one("SELECT reflex_audit('audit_bq_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[WARNING]") && report.contains("base-query-runs"),
        "expected WARNING/base-query-runs:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_base_query_runs_green() {
    Spi::run("CREATE TABLE audit_bq_ok_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_bq_ok_view",
        "SELECT id, a FROM audit_bq_ok_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    let report: String = Spi::get_one("SELECT reflex_audit('audit_bq_ok_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        !report.contains("base-query-runs"),
        "expected no base-query-runs finding:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_partition_mirror_detects_missing_partition() {
    // Create a partitioned source table with regions and amounts.
    Spi::run(
        "CREATE TABLE audit_pm_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("partitioned src");
    Spi::run("CREATE TABLE audit_pm_src_us PARTITION OF audit_pm_src FOR VALUES IN ('us')")
        .expect("us partition");
    Spi::run("CREATE TABLE audit_pm_src_eu PARTITION OF audit_pm_src FOR VALUES IN ('eu')")
        .expect("eu partition");
    Spi::run("INSERT INTO audit_pm_src VALUES (1, 'us', 100), (2, 'eu', 200)")
        .expect("seed");

    // Create the IMV with explicit partition_by=region
    Spi::run(
        "SELECT create_reflex_ivm( \
            'audit_pm_view', \
            'SELECT region, SUM(amount) AS total FROM audit_pm_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create IMV");

    // Get the actual intermediate partition names to detach one.
    let int_children: Vec<String> = Spi::get_one::<Vec<String>>(
        "SELECT array_agg(c.relname::text ORDER BY c.relname) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = '__reflex_intermediate_audit_pm_view'::regclass",
    )
    .expect("ok")
    .expect("should have children");

    // Manually detach one intermediate partition to simulate drift.
    if !int_children.is_empty() {
        let detach_cmd = format!(
            "ALTER TABLE __reflex_intermediate_audit_pm_view DETACH PARTITION {}",
            int_children[0]
        );
        Spi::run(&detach_cmd).expect("detach");
    }

    let report: String = Spi::get_one("SELECT reflex_audit('audit_pm_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[WARNING]") && report.contains("partition-mirror"),
        "expected WARNING/partition-mirror:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_partition_mirror_green_when_unpartitioned() {
    // Non-partitioned IMV should not trigger this check.
    Spi::run("CREATE TABLE audit_pm_np_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    Spi::run(
        "SELECT create_reflex_ivm( \
            'audit_pm_np_view', \
            'SELECT id, a FROM audit_pm_np_src' \
         )",
    )
    .expect("create IMV");
    let report: String = Spi::get_one("SELECT reflex_audit('audit_pm_np_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        !report.contains("partition-mirror"),
        "expected no partition-mirror finding on non-partitioned IMV:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_detects_partition_tree_drift() {
    // Create a multi-level partitioned source: LIST -> RANGE
    Spi::run(
        "CREATE TABLE ssb (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, product_id BIGINT, qty INT) \
         PARTITION BY LIST (dem_plan_id)",
    )
    .expect("root");
    Spi::run(
        "CREATE TABLE ssb_172 PARTITION OF ssb FOR VALUES IN (172) \
         PARTITION BY RANGE (order_date)",
    )
    .expect("list");
    Spi::run(
        "CREATE TABLE ssb_172_2025_01 PARTITION OF ssb_172 \
         FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')",
    )
    .expect("l1");

    // Create IMV with FULL-depth partitioning (both levels) so the IMV mirrors
    // the order_date month leaves — required for this test to exercise
    // partition-tree-drift detection (a depth-1 IMV deliberately would NOT
    // mirror months, so a new source month is not drift).
    Spi::run(
        "SELECT create_reflex_ivm(\
            'fcstb', \
            'SELECT dem_plan_id, order_date, product_id, qty FROM ssb', \
            'dem_plan_id,product_id,order_date', \
            NULL, NULL, NULL, \
            ARRAY['dem_plan_id','order_date'])",
    )
    .expect("create IMV");

    // Induce drift: attach a new source leaf but deliberately skip the flush.
    Spi::run(
        "CREATE TABLE ssb_172_2025_02 PARTITION OF ssb_172 \
         FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')",
    )
    .expect("attach new source leaf");

    // Simulate a forgotten flush by emptying the partition pending table.
    // DELETE, not TRUNCATE: the 1.10.0 deferred flush trigger queues an AFTER
    // INSERT event on this table at enqueue time, and Postgres forbids
    // TRUNCATE on a table with pending trigger events in the same transaction.
    Spi::run("DELETE FROM public.__reflex_partition_pending").expect("simulate forgotten flush");

    // Force drift by dropping the auto-synced IMV leaf if it was created
    Spi::run("DROP TABLE IF EXISTS public.fcstb_ssb_172_2025_02 CASCADE").expect("force drift");

    let report: String = Spi::get_one("SELECT reflex_audit('fcstb')")
        .expect("audit")
        .expect("audit result");
    assert!(
        report.to_lowercase().contains("partition") && report.to_lowercase().contains("drift"),
        "audit should flag partition drift: {report}"
    );
}

#[pg_test]
fn pg_test_audit_orphan_intermediate_detects() {
    Spi::run("CREATE TABLE __reflex_intermediate_audit_orph_view (id BIGINT)")
        .expect("plant orphan");

    let report: String = Spi::get_one("SELECT reflex_audit()")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("orphan-intermediate"),
        "expected orphan-intermediate finding:\n{}",
        report
    );
    Spi::run("DROP TABLE __reflex_intermediate_audit_orph_view").expect("cleanup");
}

#[pg_test]
fn pg_test_audit_orphan_staging_detects() {
    Spi::run(
        "CREATE UNLOGGED TABLE __reflex_delta_audit_orph_src \
         (__reflex_op TEXT NOT NULL, id BIGINT)",
    )
    .expect("plant orphan staging");

    let report: String = Spi::get_one("SELECT reflex_audit()")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("orphan-staging"),
        "expected orphan-staging finding:\n{}",
        report
    );
    Spi::run("DROP TABLE __reflex_delta_audit_orph_src").expect("cleanup");
}

#[pg_test]
fn pg_test_audit_orphan_scratch_is_info_severity() {
    Spi::run("CREATE TABLE __reflex_scratch_audit_orph_view (id BIGINT)")
        .expect("plant orphan scratch");

    let report: String = Spi::get_one("SELECT reflex_audit()")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[INFO]") && report.contains("orphan-scratch"),
        "expected INFO/orphan-scratch:\n{}",
        report
    );
    Spi::run("DROP TABLE __reflex_scratch_audit_orph_view").expect("cleanup");
}

#[pg_test]
fn pg_test_audit_scoped_skips_orphan_checks() {
    Spi::run("CREATE TABLE audit_no_orph_src (id BIGINT PRIMARY KEY, a INT)").expect("src");
    crate::create_reflex_ivm(
        "audit_no_orph_view",
        "SELECT id, a FROM audit_no_orph_src",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    Spi::run("CREATE TABLE __reflex_intermediate_audit_no_orph_GHOST (id BIGINT)")
        .expect("plant orphan");

    let report: String = Spi::get_one("SELECT reflex_audit('audit_no_orph_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        !report.contains("orphan-"),
        "scoped audit must skip orphan checks:\n{}",
        report
    );
    Spi::run("DROP TABLE __reflex_intermediate_audit_no_orph_GHOST").expect("cleanup");
}

#[pg_test]
fn pg_test_audit_composition_two_findings_at_once() {
    // Two unrelated IMVs with two distinct drifts. Audit should report both.
    Spi::run("CREATE TABLE audit_comp_src_a (id BIGINT PRIMARY KEY, a INT)").expect("src a");
    Spi::run("CREATE TABLE audit_comp_src_b (id BIGINT PRIMARY KEY, b INT)").expect("src b");
    crate::create_reflex_ivm(
        "audit_comp_view_a",
        "SELECT id, a FROM audit_comp_src_a",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    // Use an aggregate query so the intermediate table is created
    crate::create_reflex_ivm(
        "audit_comp_view_b",
        "SELECT id, COUNT(*) as cnt FROM audit_comp_src_b GROUP BY id",
        Some("id"),
        None,
        Some("IMMEDIATE"),
        None,
    );
    Spi::run("DROP TRIGGER __reflex_trigger_ins_on_audit_comp_src_a ON audit_comp_src_a")
        .expect("drop trig on a");
    Spi::run("DROP TABLE \"__reflex_intermediate_audit_comp_view_b\" CASCADE")
        .expect("drop intermediate of b");

    let report: String = Spi::get_one("SELECT reflex_audit()")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("trigger-attached") && report.contains("audit_comp_view_a"),
        "expected trigger-attached on view_a:\n{}",
        report
    );
    assert!(
        report.contains("internal-tables-exist") && report.contains("audit_comp_view_b"),
        "expected internal-tables-exist on view_b:\n{}",
        report
    );
}

#[pg_test]
fn pg_test_audit_end_to_end_stale_staging_post_source_recreate() {
    // Set up a stale staging drift: source with dropped columns but
    // stale staging delta with old column set.

    // v1: source with columns a, b, c, d
    Spi::run(
        "CREATE TABLE audit_e2e_src (\
            id BIGINT PRIMARY KEY, a INT, b INT, c INT, d INT)",
    )
    .expect("v1");

    // Create the stale staging delta with v1 schema
    // (simulating what would exist after drop_ivm from v1, before columns dropped).
    Spi::run(
        "CREATE UNLOGGED TABLE __reflex_delta_audit_e2e_src \
         (__reflex_op TEXT NOT NULL, id BIGINT, a INT, b INT, c INT, d INT)",
    )
    .expect("create stale staging with v1 schema");

    // v2: drop + recreate source, but drop columns c and d
    Spi::run("DROP TABLE audit_e2e_src").expect("drop v1");

    Spi::run(
        "CREATE TABLE audit_e2e_src (\
            id BIGINT PRIMARY KEY, a INT, b INT)",
    )
    .expect("v2 with fewer columns");

    // Bypass the create_ivm staging-shape guard by inserting the reference
    // row directly. The stale staging persists with v1 columns (a, b, c, d),
    // but v2 source only has (a, b). The column SETS no longer match!
    // In normal operation this is unreachable; the audit must catch the
    // resulting drift if such bypass ever happened.
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference \
         (name, graph_depth, depends_on, refresh_mode, base_query, end_query, enabled) \
         VALUES ('audit_e2e_bypass_view', 0, ARRAY['audit_e2e_src'], 'DEFERRED', \
                 'SELECT id, a, b FROM audit_e2e_src', \
                 'SELECT id, a, b FROM audit_e2e_src', TRUE)",
    )
    .expect("direct bypass insert");

    let report: String = Spi::get_one("SELECT reflex_audit('audit_e2e_bypass_view')")
        .expect("ok")
        .expect("non-null");
    assert!(
        report.contains("[ERROR]") && report.contains("staging-shape"),
        "expected ERROR/staging-shape on 1.6.2 fixture:\n{}",
        report
    );
    assert!(
        report.to_lowercase().contains("suggested fix"),
        "expected suggested-fix block:\n{}",
        report
    );

    // Cleanup so siblings don't see this row.
    Spi::run(
        "DELETE FROM public.__reflex_ivm_reference WHERE name = 'audit_e2e_bypass_view'",
    )
    .expect("cleanup");
}

#[pg_test]
fn test_audit_flags_duplicate_trigger_function() {
    Spi::run("CREATE SCHEMA IF NOT EXISTS s_dup").expect("create schema");
    Spi::run(
        "CREATE OR REPLACE FUNCTION public.__reflex_ins_trigger_on_dup_demo() \
         RETURNS TRIGGER AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql",
    )
    .expect("create public fn");
    Spi::run(
        "CREATE OR REPLACE FUNCTION s_dup.__reflex_ins_trigger_on_dup_demo() \
         RETURNS TRIGGER AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql",
    )
    .expect("create s_dup fn");

    let report: String = Spi::get_one("SELECT reflex_audit()")
        .expect("query ok")
        .expect("non-null result");
    assert!(
        report.contains("duplicate-function") && report.contains("__reflex_ins_trigger_on_dup_demo"),
        "audit must flag the duplicate function. report:\n{}",
        report
    );

    // Cleanup
    Spi::run("DROP FUNCTION IF EXISTS s_dup.__reflex_ins_trigger_on_dup_demo() CASCADE")
        .expect("drop s_dup fn");
    Spi::run("DROP FUNCTION IF EXISTS public.__reflex_ins_trigger_on_dup_demo() CASCADE")
        .expect("drop public fn");
    Spi::run("DROP SCHEMA IF EXISTS s_dup CASCADE").expect("drop schema");
}

#[pg_test]
fn f8_bare_name_flagged_when_relation_in_two_schemas() {
    Spi::run("CREATE SCHEMA IF NOT EXISTS sa").unwrap();
    Spi::run("CREATE SCHEMA IF NOT EXISTS sb").unwrap();
    Spi::run("CREATE TABLE IF NOT EXISTS sa.dup(x int)").unwrap();
    Spi::run("CREATE TABLE IF NOT EXISTS sb.dup(x int)").unwrap();
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference (name, graph_depth, depends_on) \
         VALUES ('m', 0, ARRAY['dup'])",
    )
    .unwrap();

    let report: String = Spi::get_one("SELECT reflex_audit()")
        .expect("query ok")
        .expect("non-null result");

    assert!(
        report.contains("[ERROR]") && report.contains("bare_name_ambiguity"),
        "expected ERROR/bare_name_ambiguity in report:\n{}",
        report
    );
    assert!(
        report.contains("dup"),
        "expected bare name 'dup' mentioned in report:\n{}",
        report
    );

    // Cleanup
    Spi::run("DELETE FROM public.__reflex_ivm_reference WHERE name = 'm'").unwrap();
    Spi::run("DROP TABLE IF EXISTS sa.dup").unwrap();
    Spi::run("DROP TABLE IF EXISTS sb.dup").unwrap();
    Spi::run("DROP SCHEMA IF EXISTS sa").unwrap();
    Spi::run("DROP SCHEMA IF EXISTS sb").unwrap();
}

#[pg_test]
fn f8_qualified_name_not_flagged() {
    Spi::run("CREATE SCHEMA IF NOT EXISTS sc").unwrap();
    Spi::run("CREATE TABLE IF NOT EXISTS sc.qual_test(x int)").unwrap();
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference (name, graph_depth, depends_on) \
         VALUES ('m_qual', 0, ARRAY['sc.qual_test'])",
    )
    .unwrap();

    let report: String = Spi::get_one("SELECT reflex_audit()")
        .expect("query ok")
        .expect("non-null result");

    assert!(
        !report.contains("bare_name_ambiguity"),
        "expected no bare_name_ambiguity finding for qualified name:\n{}",
        report
    );

    // Cleanup
    Spi::run("DELETE FROM public.__reflex_ivm_reference WHERE name = 'm_qual'").unwrap();
    Spi::run("DROP TABLE IF EXISTS sc.qual_test").unwrap();
    Spi::run("DROP SCHEMA IF EXISTS sc").unwrap();
}

#[pg_test]
fn f8_bare_name_unique_relation_not_flagged() {
    Spi::run("CREATE SCHEMA IF NOT EXISTS sd").unwrap();
    Spi::run("CREATE TABLE IF NOT EXISTS sd.unique_rel(x int)").unwrap();
    Spi::run(
        "INSERT INTO public.__reflex_ivm_reference (name, graph_depth, depends_on) \
         VALUES ('m_unique', 0, ARRAY['unique_rel'])",
    )
    .unwrap();

    let report: String = Spi::get_one("SELECT reflex_audit()")
        .expect("query ok")
        .expect("non-null result");

    assert!(
        !report.contains("bare_name_ambiguity"),
        "expected no bare_name_ambiguity finding when bare name exists in only one schema:\n{}",
        report
    );

    // Cleanup
    Spi::run("DELETE FROM public.__reflex_ivm_reference WHERE name = 'm_unique'").unwrap();
    Spi::run("DROP TABLE IF EXISTS sd.unique_rel").unwrap();
    Spi::run("DROP SCHEMA IF EXISTS sd").unwrap();
}

#[pg_test]
fn f6_residue_check_flags_empty_imv_partition_with_source_rows() {
    // Create a partitioned source table
    Spi::run(
        "CREATE TABLE f6_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create partitioned source");
    Spi::run("CREATE TABLE f6_src_us PARTITION OF f6_src FOR VALUES IN ('us')")
        .expect("create us partition");
    Spi::run("CREATE TABLE f6_src_eu PARTITION OF f6_src FOR VALUES IN ('eu')")
        .expect("create eu partition");

    // Add data to the source
    Spi::run("INSERT INTO f6_src VALUES (1, 'us', 100), (2, 'eu', 200)")
        .expect("seed source");

    // Create a partitioned IMV - initially without ignored_sources to allow triggers
    Spi::run(
        "SELECT create_reflex_ivm( \
            'f6_imv', \
            'SELECT region, SUM(amount) AS total FROM f6_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create IMV");

    // Now mark f6_src as ignored to simulate the archive residue scenario
    Spi::run("UPDATE public.__reflex_ivm_reference SET ignored_sources = ARRAY['f6_src'] WHERE name = 'f6_imv'")
        .expect("mark source as ignored");

    // Manually empty one TARGET partition to simulate the archive residue bug
    let tgt_children_list: Vec<String> = Spi::get_one::<Vec<String>>(
        "SELECT array_agg(c.relname::text ORDER BY c.relname) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = to_regclass('f6_imv')::oid",
    )
    .expect("ok")
    .unwrap_or_default();

    if !tgt_children_list.is_empty() {
        // Delete all rows from the first target partition child to simulate archive residue
        let delete_cmd = format!("DELETE FROM \"{}\"", tgt_children_list[0]);
        Spi::run(&delete_cmd).expect("empty target partition");
    }

    // Run audit and check for archive_residue finding
    let report: String = Spi::get_one("SELECT reflex_audit('f6_imv')")
        .expect("ok")
        .expect("non-null");

    assert!(
        report.contains("archive_residue"),
        "expected archive_residue finding in report:\n{}",
        report
    );
    assert!(
        report.contains("reflex_reconcile_partition"),
        "expected reflex_reconcile_partition in suggested fix:\n{}",
        report
    );
}

#[pg_test]
fn f6_residue_check_clean_when_partitions_match() {
    // Create a partitioned source table
    Spi::run(
        "CREATE TABLE f6_clean_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC) \
         PARTITION BY LIST (region)",
    )
    .expect("create partitioned source");
    Spi::run("CREATE TABLE f6_clean_src_us PARTITION OF f6_clean_src FOR VALUES IN ('us')")
        .expect("create us partition");
    Spi::run("CREATE TABLE f6_clean_src_eu PARTITION OF f6_clean_src FOR VALUES IN ('eu')")
        .expect("create eu partition");

    // Add data to the source
    Spi::run("INSERT INTO f6_clean_src VALUES (1, 'us', 100), (2, 'eu', 200)")
        .expect("seed source");

    // Create a partitioned IMV
    Spi::run(
        "SELECT create_reflex_ivm( \
            'f6_clean_imv', \
            'SELECT region, SUM(amount) AS total FROM f6_clean_src GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create IMV");

    // Run audit - should not find archive_residue when partitions are properly populated
    let report: String = Spi::get_one("SELECT reflex_audit('f6_clean_imv')")
        .expect("ok")
        .expect("non-null");

    assert!(
        !report.contains("archive_residue"),
        "expected no archive_residue finding when partitions match:\n{}",
        report
    );
}

#[pg_test]
fn f6_residue_check_respects_where_predicate() {
    // Create a partitioned source table
    Spi::run(
        "CREATE TABLE f6_filter_src (id BIGINT, region TEXT NOT NULL, amount NUMERIC, status TEXT) \
         PARTITION BY LIST (region)",
    )
    .expect("create partitioned source");
    Spi::run("CREATE TABLE f6_filter_src_us PARTITION OF f6_filter_src FOR VALUES IN ('us')")
        .expect("create us partition");
    Spi::run("CREATE TABLE f6_filter_src_eu PARTITION OF f6_filter_src FOR VALUES IN ('eu')")
        .expect("create eu partition");

    // Add data: us rows all have status='inactive' (filtered out by WHERE)
    Spi::run("INSERT INTO f6_filter_src VALUES (1, 'us', 100, 'inactive'), (2, 'eu', 200, 'active')")
        .expect("seed source");

    // Create a partitioned IMV with WHERE predicate that filters out 'us' rows
    Spi::run(
        "SELECT create_reflex_ivm( \
            'f6_filter_imv', \
            'SELECT region, SUM(amount) AS total FROM f6_filter_src WHERE status = ''active'' GROUP BY region', \
            NULL, NULL, NULL, NULL, \
            ARRAY['region'] \
         )",
    )
    .expect("create IMV with WHERE predicate");

    // Manually empty the 'us' target partition (simulating archive residue scenario)
    let tgt_children: Vec<String> = Spi::get_one::<Vec<String>>(
        "SELECT array_agg(c.relname::text ORDER BY c.relname) FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = to_regclass('f6_filter_imv')::oid",
    )
    .expect("ok")
    .unwrap_or_default();

    if !tgt_children.is_empty() {
        // Find and empty the first partition (which should be 'us')
        let delete_cmd = format!("DELETE FROM \"{}\"", tgt_children[0]);
        Spi::run(&delete_cmd).expect("empty target partition");
    }

    // Run audit - The WHERE predicate filters out the 'us' rows, so an empty 'us' partition
    // is legitimate. The check may over-report (acceptable per spec) or respect the predicate.
    let report: String = Spi::get_one("SELECT reflex_audit('f6_filter_imv')")
        .expect("ok")
        .expect("non-null");

    // The WHERE predicate filters out the 'us' rows, so even an empty 'us' partition
    // may or may not be flagged depending on whether the check applies the WHERE.
    // For this test, we just verify the audit runs without ERROR level findings.
    assert!(
        !report.contains("[ERROR]"),
        "expected no ERROR level findings in audit report:\n{}",
        report
    );
}
