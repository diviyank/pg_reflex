use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
mod sql_analyzer;

use sql_analyzer::{analyze, SqlAnalysis, SqlAnalysisError};
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
        enabled BOOLEAN DEFAULT TRUE,
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
    let analysis: SqlAnalysis = match analyze(&parsed_sql) {
        Err(SqlAnalysisError::MultipleQueries(_n)) => {
            return "ERROR: Expected 1 query, got multiple";
        }
        Err(SqlAnalysisError::NotASelectQuery) => {
            return "ERROR: Query is not a SELECT";
        }
        Ok(a) => {
            if a.has_unsupported_features() {
                return "ERROR: Query has one or multiple of the unsupported features (CTE, LIMIT, ORDER BY, WINDOW)";
            }
            a
        }
    };

    let froms = &analysis.sources;

    Spi::connect_mut(|client| {
        let args = [unsafe { DatumWithOid::new(froms.clone(), PgBuiltInOids::TEXTARRAYOID.oid().value()) }];

        let matching_froms = client
            .select(
                "SELECT name, graph_depth from public.__reflex_ivm_reference where name = ANY($1)",
                None,
                &args,
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

        let ivm_froms: Vec<&str> = matching_froms
            .iter()
            .filter_map(|row| row.get_by_name("name").unwrap_or(None))
            .collect();

        // Getting depth
        let depth = matching_froms
            .iter()
            .filter_map(|row| row.get_by_name::<i32, _>("graph_depth").unwrap_or(None))
            .max()
            .unwrap_or(0);

        // CREATE intermediate (delta) tables
        let mut unlogged_tables: Vec<String> = Vec::new();
        for source in froms {
            if !ivm_froms.contains(&source.as_str()) {
                let delta_name = format!("__reflex_delta_{}_{}", view_name, source);
                let create_delta_sql = format!(
                    "CREATE UNLOGGED TABLE IF NOT EXISTS {} (LIKE {} INCLUDING ALL, __reflex_op CHAR(1))",
                    delta_name, source
                );
                client.update(&create_delta_sql, None, &[]).unwrap_or_report();
                unlogged_tables.push(delta_name);
            }
        }

        // CREATE target table
        let create_target_sql = format!(
            "CREATE UNLOGGED TABLE {} AS {}",
            view_name, sql
        );
        client.update(&create_target_sql, None, &[]).unwrap_or_report();

        // CREATE indexes on GROUP BY columns
        if !analysis.group_by_columns.is_empty() {
            let cols_csv = analysis.group_by_columns.join(", ");
            let create_index_sql = format!(
                "CREATE INDEX IF NOT EXISTS idx_{}_group_by ON {} ({})",
                view_name, view_name, cols_csv
            );
            client.update(&create_index_sql, None, &[]).unwrap_or_report();
        }

        // CREATE triggers
        // Stub trigger function
        client.update(
            "CREATE OR REPLACE FUNCTION __reflex_trigger_stub() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql",
            None,
            &[],
        ).unwrap_or_report();

        // Per source table (not IVM views, not the target)
        for source in froms {
            if !ivm_froms.contains(&source.as_str()) && source != view_name {
                let create_trigger_sql = format!(
                    "CREATE OR REPLACE TRIGGER __reflex_trigger_{}_{} AFTER INSERT OR UPDATE OR DELETE ON {} FOR EACH STATEMENT EXECUTE FUNCTION __reflex_trigger_stub()",
                    view_name, source, source
                );
                client.update(&create_trigger_sql, None, &[]).unwrap_or_report();
            }
        }

        // INSERT into reference table
        let depends_on = froms.clone();
        let ivm_froms_owned: Vec<String> = ivm_froms.iter().map(|s| s.to_string()).collect();
        let group_by_cols = analysis.group_by_columns.clone();

        let insert_args = [
            unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
            unsafe { DatumWithOid::new(depth + 1, PgBuiltInOids::INT4OID.oid().value()) },
            unsafe { DatumWithOid::new(depends_on, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
            unsafe { DatumWithOid::new(ivm_froms_owned, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
            unsafe { DatumWithOid::new(unlogged_tables, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
            unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
            unsafe { DatumWithOid::new(group_by_cols, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
        ];

        client.update(
            "INSERT INTO public.__reflex_ivm_reference
                 (name, graph_depth, depends_on, depends_on_imv, unlogged_tables, sql_query, index_columns, enabled, last_update_date)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, NOW())",
            None,
            &insert_args,
        ).unwrap_or_report();
    });

    "CREATE REFLEX INCREMENTAL VIEW"
}

/// Run the given trigger
// #[pg_extern]
// fn run_reflex_trigger<'a>(view_name: &'a str, new_data: &'a str) -> &'a str {
//     // TODO: GET all info from reflex_reference
//     // TODO: Build dependency graph
//     // TODO: FOR TOPOLOGICAL levels:
//
//     // TODO: Run query Up until group by (base-query - if there is) and pull datas for given topological level
//
//     // TODO: Run base-aggregations for every
//
//     // TODO: Compute deltas
//
//     // TODO: Go up a topological level: with deltas!
//
//     // TODO: Update all-deltas
//
//     &format!("UPDATED all views from {}", view_name)
// }

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

    #[pg_test]
    fn test_create_ivm_simple() {
        // Setup: create source table
        Spi::run("CREATE TABLE test_orders (id INT, status TEXT, amount NUMERIC)").unwrap();

        let result = crate::create_reflex_ivm(
            "mv_orders",
            "SELECT status, SUM(amount) FROM test_orders GROUP BY status",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify target table was created
        let exists: bool = Spi::get_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'mv_orders')",
        )
        .unwrap()
        .unwrap();
        assert!(exists, "target table mv_orders should exist");

        // Verify delta table was created
        let delta_exists: bool = Spi::get_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '__reflex_delta_mv_orders_test_orders')",
        )
        .unwrap()
        .unwrap();
        assert!(delta_exists, "delta table should exist");

        // Verify reference row was inserted
        let ref_name: Option<String> = Spi::get_one(
            "SELECT name FROM public.__reflex_ivm_reference WHERE name = 'mv_orders'",
        )
        .unwrap();
        assert_eq!(ref_name, Some("mv_orders".to_string()));

        // Verify graph_depth = 1 (no IVM dependencies)
        let depth: Option<i32> = Spi::get_one(
            "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'mv_orders'",
        )
        .unwrap();
        assert_eq!(depth, Some(1));
    }

    #[pg_test]
    fn test_create_ivm_index_on_group_by() {
        Spi::run("CREATE TABLE test_sales (id INT, region TEXT, category TEXT, amount NUMERIC)").unwrap();

        crate::create_reflex_ivm(
            "mv_sales",
            "SELECT region, category, SUM(amount) FROM test_sales GROUP BY region, category",
        );

        // Verify index was created on group by columns
        let idx_exists: bool = Spi::get_one(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_mv_sales_group_by')",
        )
        .unwrap()
        .unwrap();
        assert!(idx_exists, "group by index should exist");
    }

    #[pg_test]
    fn test_create_ivm_no_index_without_group_by() {
        Spi::run("CREATE TABLE test_items (id INT, name TEXT)").unwrap();

        crate::create_reflex_ivm(
            "mv_items",
            "SELECT * FROM test_items",
        );

        let idx_exists: bool = Spi::get_one(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_mv_items_group_by')",
        )
        .unwrap()
        .unwrap();
        assert!(!idx_exists, "no group by index should exist without GROUP BY");
    }

    #[pg_test]
    fn test_create_ivm_trigger_created() {
        Spi::run("CREATE TABLE test_products (id INT, price NUMERIC)").unwrap();

        crate::create_reflex_ivm(
            "mv_products",
            "SELECT * FROM test_products",
        );

        // Verify trigger exists on source table
        let trigger_exists: bool = Spi::get_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.triggers WHERE trigger_name = '__reflex_trigger_mv_products_test_products')",
        )
        .unwrap()
        .unwrap();
        assert!(trigger_exists, "trigger should exist on source table");
    }

    #[pg_test]
    fn test_create_ivm_stub_function_exists() {
        Spi::run("CREATE TABLE test_tbl (id INT)").unwrap();

        crate::create_reflex_ivm("mv_tbl", "SELECT * FROM test_tbl");

        let func_exists: bool = Spi::get_one(
            "SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = '__reflex_trigger_stub')",
        )
        .unwrap()
        .unwrap();
        assert!(func_exists, "stub trigger function should exist");
    }

    #[pg_test]
    fn test_create_ivm_multiple_sources() {
        Spi::run("CREATE TABLE test_a (id INT, val TEXT)").unwrap();
        Spi::run("CREATE TABLE test_b (bid INT, ref_id INT, data TEXT)").unwrap();

        crate::create_reflex_ivm(
            "mv_joined",
            "SELECT test_a.id, test_a.val, test_b.data FROM test_a JOIN test_b ON test_a.id = test_b.ref_id",
        );

        // Verify both delta tables were created
        let delta_a: bool = Spi::get_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '__reflex_delta_mv_joined_test_a')",
        )
        .unwrap()
        .unwrap();
        let delta_b: bool = Spi::get_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '__reflex_delta_mv_joined_test_b')",
        )
        .unwrap()
        .unwrap();
        assert!(delta_a, "delta table for test_a should exist");
        assert!(delta_b, "delta table for test_b should exist");

        // Verify depends_on has both sources
        let depends: Option<Vec<String>> = Spi::get_one(
            "SELECT depends_on FROM public.__reflex_ivm_reference WHERE name = 'mv_joined'",
        )
        .unwrap();
        let deps = depends.unwrap();
        assert!(deps.contains(&"test_a".to_string()));
        assert!(deps.contains(&"test_b".to_string()));
    }

    #[pg_test]
    fn test_create_ivm_delta_has_reflex_op_column() {
        Spi::run("CREATE TABLE test_delta_src (id INT, name TEXT)").unwrap();

        crate::create_reflex_ivm("mv_delta_test", "SELECT * FROM test_delta_src");

        // Verify __reflex_op column exists on the delta table
        let col_exists: bool = Spi::get_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '__reflex_delta_mv_delta_test_test_delta_src' AND column_name = '__reflex_op')",
        )
        .unwrap()
        .unwrap();
        assert!(col_exists, "delta table should have __reflex_op column");
    }

    #[pg_test]
    fn test_create_ivm_target_is_unlogged() {
        Spi::run("CREATE TABLE test_unlog_src (id INT)").unwrap();

        crate::create_reflex_ivm("mv_unlog", "SELECT * FROM test_unlog_src");

        let persistence: Option<String> = Spi::get_one(
            "SELECT relpersistence::text FROM pg_class WHERE relname = 'mv_unlog'",
        )
        .unwrap();
        assert_eq!(persistence, Some("u".to_string()), "target table should be unlogged");
    }

    #[pg_test]
    fn test_create_ivm_rejects_multiple_queries() {
        // The parser itself will handle this — but create_reflex_ivm should return error
        // sqlparser treats "SELECT 1; SELECT 2" as two statements
        let result = crate::create_reflex_ivm("mv_bad", "SELECT 1; SELECT 2");
        assert!(result.starts_with("ERROR"), "should reject multiple queries");
    }

    #[pg_test]
    fn test_create_ivm_rejects_cte() {
        Spi::run("CREATE TABLE test_cte_src (id INT)").unwrap();
        let result = crate::create_reflex_ivm(
            "mv_cte",
            "WITH cte AS (SELECT * FROM test_cte_src) SELECT * FROM cte",
        );
        assert!(result.starts_with("ERROR"), "should reject CTE");
    }

    #[pg_test]
    fn test_create_ivm_rejects_order_by() {
        Spi::run("CREATE TABLE test_ord_src (id INT)").unwrap();
        let result = crate::create_reflex_ivm(
            "mv_ord",
            "SELECT * FROM test_ord_src ORDER BY id",
        );
        assert!(result.starts_with("ERROR"), "should reject ORDER BY");
    }

    #[pg_test]
    fn test_create_ivm_rejects_limit() {
        Spi::run("CREATE TABLE test_lim_src (id INT)").unwrap();
        let result = crate::create_reflex_ivm(
            "mv_lim",
            "SELECT * FROM test_lim_src LIMIT 10",
        );
        assert!(result.starts_with("ERROR"), "should reject LIMIT");
    }

    #[pg_test]
    fn test_create_ivm_reference_stores_sql() {
        Spi::run("CREATE TABLE test_sql_src (id INT, val TEXT)").unwrap();
        let query = "SELECT * FROM test_sql_src";

        crate::create_reflex_ivm("mv_sql_check", query);

        let stored_sql: Option<String> = Spi::get_one(
            "SELECT sql_query FROM public.__reflex_ivm_reference WHERE name = 'mv_sql_check'",
        )
        .unwrap();
        assert_eq!(stored_sql, Some(query.to_string()));
    }

    #[pg_test]
    fn test_create_ivm_reference_stores_index_columns() {
        Spi::run("CREATE TABLE test_idx_src (id INT, cat TEXT, amt NUMERIC)").unwrap();

        crate::create_reflex_ivm(
            "mv_idx_check",
            "SELECT cat, SUM(amt) FROM test_idx_src GROUP BY cat",
        );

        let idx_cols: Option<Vec<String>> = Spi::get_one(
            "SELECT index_columns FROM public.__reflex_ivm_reference WHERE name = 'mv_idx_check'",
        )
        .unwrap();
        assert_eq!(idx_cols, Some(vec!["cat".to_string()]));
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
