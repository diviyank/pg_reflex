
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
