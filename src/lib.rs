use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

/// This SQL will be executed exactly once when 'CREATE EXTENSION' is run.
/// Collate "C" for faster lookups
extension_sql!(
    r#"
    CREATE TABLE IF NOT EXISTS public.__reflex_ivm_reference (
        name TEXT PRIMARY KEY COLLATE "C",
        graph_level INT NOT NULL,
        depends_on TEXT[],
        depends_on_imv TEXT[],
        sql_query TEXT,
        parsed_sql_query JSON,
        indexes TEXT[],
    );
    
    -- You can also add indexes here
    CREATE INDEX IF NOT EXISTS idx__reflex_ivm_name ON public.__reflex_ivm_reference(name);
    "#,
    name = "pg_reflex_init", // Unique name for this SQL block
);

#[pg_extern]
fn hello_pg_reflex() -> &'static str {
    "Hello, pg_reflex"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pg_reflex() {
        assert_eq!("Hello, pg_reflex", crate::hello_pg_reflex());
    }
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
