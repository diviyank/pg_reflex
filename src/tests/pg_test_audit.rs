
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
