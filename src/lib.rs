use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::{self, PostgreSqlDialect};
use sqlparser::parser::Parser;
mod sql_analyzer;

use sql_analyzer::{analyze, SqlAnalysisError};
::pgrx::pg_module_magic!(name, version);

/// This SQL will be executed exactly once when 'CREATE EXTENSION' is run.
/// Collate "C" for faster lookups
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
        parsed_sql_query JSON,
        aggregations JSON,
        index_columns TEXT[],
        enabled BOOLEAN DEFAULT TRUE
        last_update_date TIMESTAMP
    );
    
    -- You can also add indexes here
    CREATE INDEX IF NOT EXISTS idx__reflex_ivm_name ON public.__reflex_ivm_reference(name);
    "#,
    name = "pg_reflex_init",
);

#[pg_extern]
fn create_reflex_ivm(view_name: &str, sql: &str) -> &'static str {
    let dialect = PostgreSqlDialect {};
    let parsed_sql = Parser::parse_sql(&dialect, sql).unwrap();
    let froms: Vec<String> = match analyze(&parsed_sql) {
        Err(SqlAnalysisError::MultipleQueries(n)) => {
            return "ERROR: Expected 1 query, got multiple";
        }
        Err(SqlAnalysisError::NotASelectQuery) => {
            return "ERROR: Query is not a SELECT";
        }
        Ok(analysis) => {
            if analysis.has_unsupported_features() {
                return "ERROR: Query has one or multiple of the unsupported features (CTE, LIMIT, ORDER BY, WINDOW)";
            }

            println!("\nSources found:");
            for src in &analysis.sources {
                println!("  - {}", src);
            }
            analysis.sources
        }
    };

    Spi::connect(|mut client| {
        let args = [unsafe { DatumWithOid::new(froms, PgBuiltInOids::TEXTARRAYOID.oid().value()) }];

        let matching_froms = client
            .select(
                "SELECT name, graph_depth from public.__reflex_ivm_reference where name = ANY($1)",
                None,
                &args,
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

        let mut results: Vec<&str> = Vec::new();

        for row in matching_froms {
            if let Some(name) = row.get_by_name("name").unwrap_or(None) {
                results.push(name);
            }
        }

        // TODO: CREATE Intermediate table(s)

        // TODO: CREATE target table

        // TODO: CREATE indexes

        // TODO: CREATE triggers if not exist except on target tables

        client.update(
            "INSERT INTO public.__reflex_ivm_reference
                 (name,
                 graph_level,
                 depends_on,
                 depends_on_imv,
                 graph_child,
                 sql_query,
                 parsed_sql_query,
                 index_columns,
                 enabled,
                 last_update_date)",
            None,
            None,
        );
    });

    "CREATE REFLEX INCREMENTAL VIEW"
}

/// Run the given trigger
#[pg_extern]
fn run_reflex_trigger(view_name: &str, new_data: &str) -> &str {
    // TODO: GET all info from reflex_reference
    // TODO: Build dependency graph
    // TODO: FOR TOPOLOGICAL levels:

    // TODO: Run query Up until group by (base-query - if there is) and pull datas for given topological level

    // TODO: Run base-aggregations for every

    // TODO: Compute deltas

    // TODO: Go up a topological level: with deltas!

    // TODO: Update all-deltas

    &format!("UPDATED all views from {}", view_name)
}

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
