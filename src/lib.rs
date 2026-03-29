use pgrx::prelude::*;

mod aggregation;
mod create_ivm;
mod drop_ivm;
mod query_decomposer;
mod reconcile;
mod schema_builder;
mod sql_analyzer;
mod trigger;
mod window;

::pgrx::pg_module_magic!(name, version);

// This SQL will be executed exactly once when 'CREATE EXTENSION' is run.
// Collate "C" for faster lookups
extension_sql!(
    r#"
    CREATE TABLE IF NOT EXISTS public.__reflex_ivm_reference (
        name TEXT PRIMARY KEY COLLATE "C",
        graph_depth INT NOT NULL,
        depends_on TEXT[],
        depends_on_imv TEXT[],
        unlogged_tables TEXT[],
        graph_child TEXT[],
        sql_query TEXT,
        base_query TEXT,
        end_query TEXT,
        parsed_sql_query JSON,
        aggregations JSON,
        index_columns TEXT[],
        unique_columns TEXT[],
        enabled BOOLEAN DEFAULT TRUE,
        last_update_date TIMESTAMP,
        storage_mode TEXT DEFAULT 'UNLOGGED',
        refresh_mode TEXT DEFAULT 'IMMEDIATE',
        where_predicate TEXT
    );

    -- Index on name for fast lookups
    CREATE INDEX IF NOT EXISTS idx__reflex_ivm_name ON public.__reflex_ivm_reference(name);
    "#,
    name = "pg_reflex_init",
);

/// Validates that a view name contains only safe characters.
/// Allows: ASCII letters, digits, underscore, period (for schema qualification).
/// Rejects everything else (quotes, semicolons, whitespace, etc.).
fn validate_view_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("ERROR: Invalid view name: name is empty");
    }
    if name.starts_with('.') || name.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return Err("ERROR: Invalid view name: must start with a letter or underscore");
    }
    if name.contains("..") || name.ends_with('.') {
        return Err("ERROR: Invalid view name: invalid period placement");
    }
    for ch in name.chars() {
        if !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.') {
            return Err(
                "ERROR: Invalid view name: only alphanumeric, underscore, and period allowed",
            );
        }
    }
    Ok(())
}

#[pg_extern]
fn create_reflex_ivm(
    view_name: &str,
    sql: &str,
    unique_columns: default!(Option<&str>, "NULL"),
    storage: default!(Option<&str>, "'UNLOGGED'"),
    mode: default!(Option<&str>, "'IMMEDIATE'"),
) -> &'static str {
    create_ivm::create_reflex_ivm_impl(view_name, sql, unique_columns.unwrap_or(""), false, storage.unwrap_or("UNLOGGED"), mode.unwrap_or("IMMEDIATE"))
}

#[pg_extern]
fn create_reflex_ivm_if_not_exists(
    view_name: &str,
    sql: &str,
    unique_columns: default!(Option<&str>, "NULL"),
    storage: default!(Option<&str>, "'UNLOGGED'"),
    mode: default!(Option<&str>, "'IMMEDIATE'"),
) -> &'static str {
    create_ivm::create_reflex_ivm_impl(view_name, sql, unique_columns.unwrap_or(""), true, storage.unwrap_or("UNLOGGED"), mode.unwrap_or("IMMEDIATE"))
}

/// Drop a reflex IMV and all its artifacts (triggers, tables, reference row).
/// Refuses to drop if the IMV has children unless cascade is true.
#[pg_extern]
fn drop_reflex_ivm(view_name: &str) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
    drop_ivm::drop_reflex_ivm_impl(view_name, false)
}

#[pg_extern(name = "drop_reflex_ivm")]
fn drop_reflex_ivm_cascade(view_name: &str, cascade: bool) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
    drop_ivm::drop_reflex_ivm_impl(view_name, cascade)
}

/// Reconcile an IMV by rebuilding intermediate + target from scratch.
/// Use this as a safety net (manually or via pg_cron) to fix drift.
#[pg_extern]
fn reflex_reconcile(view_name: &str) -> &'static str {
    reconcile::reflex_reconcile(view_name)
}

/// Refresh a single IMV by rebuilding from source. Alias for reflex_reconcile.
/// Use after REFRESH MATERIALIZED VIEW on a source that feeds this IMV.
#[pg_extern]
fn refresh_reflex_imv(view_name: &str) -> &'static str {
    reconcile::reflex_reconcile(view_name)
}

/// Refresh ALL IMVs that depend on a given source table or materialized view.
/// Processes IMVs in graph_depth order (L1 before L2).
#[pg_extern]
fn refresh_imv_depending_on(source: &str) -> &'static str {
    reconcile::refresh_imv_depending_on(source)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_extern]
    fn hello_pg_reflex() -> &'static str {
        "Hello, pg_reflex"
    }

    /// Verify IMV matches a fresh computation using EXCEPT ALL oracle.
    fn assert_imv_correct(imv: &str, fresh_sql: &str) {
        let check = format!(
            "SELECT COUNT(*) FROM (\
                (SELECT * FROM {} EXCEPT ALL SELECT * FROM ({}) AS __fresh1) \
                UNION ALL \
                (SELECT * FROM ({}) AS __fresh2 EXCEPT ALL SELECT * FROM {}) \
             ) __oracle",
            imv, fresh_sql, fresh_sql, imv
        );
        let mismatches = Spi::get_one::<i64>(&check)
            .expect("oracle query failed")
            .expect("oracle returned NULL");
        assert_eq!(mismatches, 0,
            "EXCEPT ALL oracle failed for '{}': {} mismatches between IMV and fresh query",
            imv, mismatches);
    }

    include!("tests/pg_test_basic.rs");
    include!("tests/pg_test_trigger.rs");
    include!("tests/pg_test_passthrough.rs");
    include!("tests/pg_test_cte.rs");
    include!("tests/pg_test_set_ops.rs");
    include!("tests/pg_test_window.rs");
    include!("tests/pg_test_drop.rs");
    include!("tests/pg_test_reconcile.rs");
    include!("tests/pg_test_deferred.rs");
    include!("tests/pg_test_error.rs");
    include!("tests/pg_test_e2e.rs");
    include!("tests/pg_test_correctness.rs");
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}

#[cfg(test)]
#[path = "tests/unit_proptest.rs"]
mod proptest_tests;
