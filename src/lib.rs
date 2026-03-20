use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

mod aggregation;
mod query_decomposer;
mod schema_builder;
mod sql_analyzer;
mod trigger;

use aggregation::plan_aggregation;
use query_decomposer::{
    bare_column_name, generate_aggregations_json, generate_base_query, generate_end_query,
    intermediate_table_name, replace_identifier,
};
use schema_builder::{
    build_indexes_ddl, build_intermediate_table_ddl, build_target_table_ddl, build_trigger_ddls,
};
use sql_analyzer::{analyze, SqlAnalysisError};

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
        enabled BOOLEAN DEFAULT TRUE,
        last_update_date TIMESTAMP
    );

    -- Index on name for fast lookups
    CREATE INDEX IF NOT EXISTS idx__reflex_ivm_name ON public.__reflex_ivm_reference(name);
    "#,
    name = "pg_reflex_init",
);

#[pg_extern]
fn create_reflex_ivm(view_name: &str, sql: &str) -> &'static str {
    let dialect = PostgreSqlDialect {};
    let parsed_sql = Parser::parse_sql(&dialect, sql).unwrap();
    let analysis = match analyze(&parsed_sql) {
        Err(SqlAnalysisError::MultipleQueries(_)) => {
            return "ERROR: Expected 1 query, got multiple";
        }
        Err(SqlAnalysisError::NotASelectQuery) => {
            return "ERROR: Query is not a SELECT";
        }
        Ok(a) => {
            if a.has_unsupported_features() {
                return "ERROR: Query has unsupported features (RECURSIVE CTE, LIMIT, ORDER BY, WINDOW)";
            }
            a
        }
    };

    // --- CTE decomposition: each CTE becomes its own sub-IMV ---
    if !analysis.ctes.is_empty() {
        let mut cte_name_map: Vec<(String, String)> = Vec::new();

        for cte in &analysis.ctes {
            // Rewrite references to earlier CTEs in this CTE's query
            let mut cte_query = cte.query_sql.clone();
            for (earlier_alias, earlier_imv) in &cte_name_map {
                cte_query = replace_identifier(&cte_query, earlier_alias, earlier_imv);
            }

            let cte_view_name = format!("{}__cte_{}", view_name, cte.alias);
            let result = create_reflex_ivm(&cte_view_name, &cte_query);
            if result.starts_with("ERROR") {
                return result;
            }
            cte_name_map.push((cte.alias.clone(), cte_view_name));
        }

        // Rewrite main query body: serialize without WITH, replace CTE names
        let body_sql = if let sqlparser::ast::Statement::Query(ref query) = parsed_sql[0] {
            let mut body = query.body.to_string();
            // Append ORDER BY / LIMIT if present (shouldn't be for valid IMV queries)
            if let Some(ref ob) = query.order_by {
                body = format!("{} {}", body, ob);
            }
            for (cte_alias, cte_imv_name) in &cte_name_map {
                body = replace_identifier(&body, cte_alias, cte_imv_name);
            }
            body
        } else {
            return "ERROR: Query is not a SELECT";
        };

        // Check if the main body is passthrough (no aggregation).
        // If so, all its sources are CTE sub-IMVs which don't get triggers,
        // so we create a VIEW (reads live from sub-IMV targets, zero overhead).
        let body_parsed = Parser::parse_sql(&dialect, &body_sql).unwrap();
        let body_analysis = match analyze(&body_parsed) {
            Ok(a) => a,
            Err(_) => return "ERROR: Failed to analyze rewritten CTE body",
        };

        let body_plan = plan_aggregation(&body_analysis);
        if body_plan.is_passthrough {
            // All sources are CTE sub-IMVs → VIEW reads live, always up-to-date
            Spi::connect_mut(|client| {
                client
                    .update(
                        &format!("CREATE OR REPLACE VIEW \"{}\" AS {}", view_name, body_sql),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            });
            return "CREATE REFLEX INCREMENTAL VIEW";
        }

        // Main body has aggregation → create as a normal IMV
        return create_reflex_ivm(view_name, &body_sql);
    }
    // --- End CTE decomposition ---

    let froms = analysis.sources.clone();

    // Build aggregation plan from the analysis
    let plan = plan_aggregation(&analysis);

    Spi::connect_mut(|client| {
        // Lookup existing IMVs among the source tables
        let args = [unsafe {
            DatumWithOid::new(froms.clone(), PgBuiltInOids::TEXTARRAYOID.oid().value())
        }];

        let matching_froms = client
            .select(
                "SELECT name, graph_depth FROM public.__reflex_ivm_reference WHERE name = ANY($1)",
                None,
                &args,
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

        let ivm_froms: Vec<String> = matching_froms
            .iter()
            .filter_map(|row| row.get_by_name::<&str, _>("name").unwrap_or(None))
            .map(|s| s.to_string())
            .collect();

        // Calculate graph depth
        let depth = matching_froms
            .iter()
            .filter_map(|row| row.get_by_name::<i32, _>("graph_depth").unwrap_or(None))
            .max()
            .unwrap_or(0)
            + 1;

        let mut unlogged_tables: Vec<String> = Vec::new();

        if plan.is_passthrough {
            // Passthrough: CREATE TABLE AS — Postgres infers columns + types, populates data
            client
                .update(
                    &format!("CREATE TABLE \"{}\" AS {}", view_name, sql),
                    None,
                    &[],
                )
                .unwrap_or_report();
            // ANALYZE so the query planner has statistics for the new table
            client
                .update(&format!("ANALYZE \"{}\"", view_name), None, &[])
                .unwrap_or_report();
        } else {
            // Aggregate: build intermediate + target tables from the plan
            let column_types = query_column_types_from_catalog(client, &froms);

            if let Some(ddl) = build_intermediate_table_ddl(view_name, &plan, &column_types) {
                let tbl = intermediate_table_name(view_name);
                client.update(&ddl, None, &[]).unwrap_or_report();
                unlogged_tables.push(tbl);
            }

            let target_ddl = build_target_table_ddl(view_name, &plan, &column_types);
            client.update(&target_ddl, None, &[]).unwrap_or_report();

            for index_ddl in build_indexes_ddl(view_name, &plan) {
                client.update(&index_ddl, None, &[]).unwrap_or_report();
            }
        }

        // CREATE consolidated triggers on source tables (one set per source, shared by all IMVs).
        // Skip if triggers already exist on this source (another IMV already created them).
        for source in &froms {
            if source.starts_with('<') {
                continue;
            }
            let safe_source = source.replace('.', "_");
            let trig_exists = client
                .select(
                    &format!(
                        "SELECT 1 FROM pg_trigger WHERE tgname = '__reflex_trigger_ins_on_{}'",
                        safe_source
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report()
                .len() > 0;

            if !trig_exists {
                for ddl in build_trigger_ddls(source) {
                    client.update(&ddl, None, &[]).unwrap_or_report();
                }
            }
        }

        // Issue 4: Add index on source GROUP BY columns for MIN/MAX recompute performance
        let has_min_max = plan.intermediate_columns.iter()
            .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX");
        if has_min_max && !plan.group_by_columns.is_empty() {
            for source in &froms {
                if source.starts_with('<') || ivm_froms.contains(source) {
                    continue;
                }
                let idx_cols: Vec<String> = plan.group_by_columns.iter()
                    .map(|c| format!("\"{}\"", bare_column_name(c)))
                    .collect();
                let safe_src = source.replace('.', "_");
                let idx_name = format!("__reflex_idx_{}_{}", view_name, safe_src);
                let ddl = format!(
                    "CREATE INDEX IF NOT EXISTS \"{}\" ON {} ({})",
                    idx_name, source, idx_cols.join(", ")
                );
                client.update(&ddl, None, &[]).unwrap_or_report();
            }
        }

        // Generate decomposed queries and metadata
        let base_query = if plan.is_passthrough {
            sql.to_string() // Passthrough: base_query = original SQL verbatim
        } else {
            generate_base_query(&analysis, &plan)
        };
        let end_query = if plan.is_passthrough {
            String::new() // Passthrough: no intermediate → target stage
        } else {
            generate_end_query(view_name, &plan)
        };
        let aggregations_json = generate_aggregations_json(&plan);
        let index_columns: Vec<String> = plan
            .group_by_columns
            .iter()
            .chain(plan.distinct_columns.iter())
            .map(|c| bare_column_name(c).to_string())
            .collect();

        // INSERT into reference table
        let depends_on: Vec<String> = froms.clone();
        let depends_on_imv: Vec<String> = ivm_froms.clone();
        let graph_child: Vec<String> = Vec::new();

        client.update(
            "INSERT INTO public.__reflex_ivm_reference
             (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
              graph_child, sql_query, base_query, end_query,
              aggregations, index_columns, enabled, last_update_date)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::json, $11, TRUE, NOW())",
            None,
            &[
                unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(depth, PgBuiltInOids::INT4OID.oid().value()) },
                unsafe { DatumWithOid::new(depends_on, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                unsafe { DatumWithOid::new(depends_on_imv, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                unsafe { DatumWithOid::new(unlogged_tables.clone(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                unsafe { DatumWithOid::new(graph_child, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(base_query, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(end_query.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(aggregations_json, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(index_columns, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
            ],
        ).unwrap_or_report();

        // Update source IMVs with the new child in their graph_child field
        for imv_name in &ivm_froms {
            client.update(
                "UPDATE public.__reflex_ivm_reference
                 SET graph_child = array_append(COALESCE(graph_child, ARRAY[]::TEXT[]), $1)
                 WHERE name = $2",
                None,
                &[
                    unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(imv_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            ).unwrap_or_report();
        }

        // Initial materialization (skip for passthrough — CREATE TABLE AS already populated)
        if !plan.is_passthrough {
            let intermediate_tbl = intermediate_table_name(view_name);
            let base_q = generate_base_query(&analysis, &plan);
            let initial_insert = format!("INSERT INTO {} {}", intermediate_tbl, base_q);
            client
                .update(&initial_insert, None, &[])
                .unwrap_or_report();

            let target_insert = format!("INSERT INTO \"{}\" {}", view_name, end_query);
            client
                .update(&target_insert, None, &[])
                .unwrap_or_report();
        }

    });

    "CREATE REFLEX INCREMENTAL VIEW"
}

/// Query the PostgreSQL catalog for column types of the given source tables.
fn query_column_types_from_catalog(
    client: &pgrx::spi::SpiClient<'_>,
    table_names: &[String],
) -> HashMap<String, String> {
    let mut types = HashMap::new();
    for table in table_names {
        // Skip non-real tables (subqueries, functions)
        if table.starts_with('<') {
            continue;
        }
        // Handle schema-qualified names
        let (schema, tbl) = if table.contains('.') {
            let parts: Vec<&str> = table.splitn(2, '.').collect();
            (parts[0], parts[1])
        } else {
            ("public", table.as_str())
        };
        let query = format!(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = '{}' AND table_name = '{}'",
            schema, tbl
        );
        let rows = client
            .select(&query, None, &[])
            .unwrap_or_report()
            .collect::<Vec<_>>();
        for row in &rows {
            if let (Some(col_name), Some(data_type)) = (
                row.get_by_name::<&str, _>("column_name").unwrap_or(None),
                row.get_by_name::<&str, _>("data_type").unwrap_or(None),
            ) {
                let pg_type = map_information_schema_type(data_type);
                types.insert(format!("{}.{}", tbl, col_name), pg_type.clone());
                // Also insert bare column name for simpler lookups
                types.entry(col_name.to_string()).or_insert(pg_type);
            }
        }
    }
    types
}

/// Map information_schema data_type strings to PostgreSQL type names usable in DDL.
fn map_information_schema_type(data_type: &str) -> String {
    match data_type {
        "integer" => "INTEGER".to_string(),
        "bigint" => "BIGINT".to_string(),
        "smallint" => "SMALLINT".to_string(),
        "numeric" => "NUMERIC".to_string(),
        "real" => "REAL".to_string(),
        "double precision" => "DOUBLE PRECISION".to_string(),
        "boolean" => "BOOLEAN".to_string(),
        "date" => "DATE".to_string(),
        "timestamp without time zone" => "TIMESTAMP".to_string(),
        "timestamp with time zone" => "TIMESTAMPTZ".to_string(),
        "character varying" => "TEXT".to_string(),
        "character" => "TEXT".to_string(),
        "text" => "TEXT".to_string(),
        "uuid" => "UUID".to_string(),
        "json" => "JSON".to_string(),
        "jsonb" => "JSONB".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Drop a reflex IMV and all its artifacts (triggers, tables, reference row).
/// Refuses to drop if the IMV has children unless cascade is true.
#[pg_extern]
fn drop_reflex_ivm(view_name: &str) -> &'static str {
    drop_reflex_ivm_impl(view_name, false)
}

#[pg_extern(name = "drop_reflex_ivm")]
fn drop_reflex_ivm_cascade(view_name: &str, cascade: bool) -> &'static str {
    drop_reflex_ivm_impl(view_name, cascade)
}

fn drop_reflex_ivm_impl(view_name: &str, cascade: bool) -> &'static str {
    Spi::connect_mut(|client| {
        // 1. Check if view exists
        let exists = client
            .select(
                "SELECT name, graph_child, depends_on, depends_on_imv \
                 FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

        if exists.is_empty() {
            return "ERROR: IMV not found";
        }

        let row = &exists[0];
        let children: Vec<String> = row
            .get_by_name::<Vec<String>, _>("graph_child")
            .unwrap_or(None)
            .unwrap_or_default();
        let depends_on: Vec<String> = row
            .get_by_name::<Vec<String>, _>("depends_on")
            .unwrap_or(None)
            .unwrap_or_default();
        let depends_on_imv: Vec<String> = row
            .get_by_name::<Vec<String>, _>("depends_on_imv")
            .unwrap_or(None)
            .unwrap_or_default();

        // 2. Check children
        if !children.is_empty() && !cascade {
            return "ERROR: IMV has children. Use drop_reflex_ivm(name, true) to cascade.";
        }

        // 3. Cascade: drop children first
        if cascade {
            for child in &children {
                let result = drop_reflex_ivm_impl(child, true);
                if result.starts_with("ERROR") {
                    return result;
                }
            }
        }

        // 4. Drop consolidated triggers only if no OTHER IMV depends on this source.
        //    The trigger function is shared; it discovers IMVs via the reference table.
        //    We must delete the reference row FIRST (step 8) so the trigger stops
        //    finding this IMV. But we need the reference row for step 7. So we check
        //    here and drop triggers AFTER deleting the row (moved below step 8).
        //    For now, just collect sources that need trigger cleanup.
        let mut sources_to_cleanup: Vec<(String, String)> = Vec::new(); // (source, safe_source)
        for source in &depends_on {
            let safe_source = source.replace('.', "_");
            let other_count = client
                .select(
                    &format!(
                        "SELECT COUNT(*) AS cnt FROM public.__reflex_ivm_reference \
                         WHERE '{}' = ANY(depends_on) AND name != '{}'",
                        source, view_name
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<i64, _>("cnt")
                .unwrap_or(None)
                .unwrap_or(0);

            if other_count == 0 {
                sources_to_cleanup.push((source.clone(), safe_source));
            }
        }

        // 5. Drop target table
        client
            .update(
                &format!("DROP TABLE IF EXISTS \"{}\"", view_name),
                None,
                &[],
            )
            .unwrap_or_report();

        // 6. Drop intermediate table
        let intermediate = intermediate_table_name(view_name);
        client
            .update(
                &format!("DROP TABLE IF EXISTS {}", intermediate),
                None,
                &[],
            )
            .unwrap_or_report();

        // 7. Update parent IMVs: remove this view from their graph_child
        for parent in &depends_on_imv {
            client
                .update(
                    "UPDATE public.__reflex_ivm_reference \
                     SET graph_child = array_remove(graph_child, $1) \
                     WHERE name = $2",
                    None,
                    &[
                        unsafe {
                            DatumWithOid::new(
                                view_name.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                        unsafe {
                            DatumWithOid::new(
                                parent.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                    ],
                )
                .unwrap_or_report();
        }

        // 8. Delete from reference table
        client
            .update(
                "DELETE FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report();

        // 9. Drop consolidated triggers on sources where no other IMV depends
        for (source, safe_source) in &sources_to_cleanup {
            for op in &["ins", "del", "upd", "trunc"] {
                let trig_name = format!("__reflex_trigger_{}_on_{}", op, safe_source);
                client
                    .update(
                        &format!("DROP TRIGGER IF EXISTS \"{}\" ON {}", trig_name, source),
                        None,
                        &[],
                    )
                    .unwrap_or_report();

                let fn_name = format!("__reflex_{}_trigger_on_{}", op, safe_source);
                client
                    .update(
                        &format!("DROP FUNCTION IF EXISTS {}()", fn_name),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            }
        }

        "DROP REFLEX INCREMENTAL VIEW"
    })
}

/// Reconcile an IMV by rebuilding intermediate + target from scratch.
/// Use this as a safety net (manually or via pg_cron) to fix drift.
#[pg_extern]
fn reflex_reconcile(view_name: &str) -> &'static str {
    Spi::connect_mut(|client| {
        let rows = client
            .select(
                "SELECT base_query, end_query, aggregations \
                 FROM public.__reflex_ivm_reference WHERE name = $1 AND enabled = TRUE",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

        if rows.is_empty() {
            return "ERROR: IMV not found or disabled";
        }

        let row = &rows[0];
        let base_query: String = row
            .get_by_name::<&str, _>("base_query")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string();
        let end_query: String = row
            .get_by_name::<&str, _>("end_query")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string();
        let agg_json: String = row
            .get_by_name::<&str, _>("aggregations")
            .unwrap_or(None)
            .unwrap_or("{}")
            .to_string();

        let is_passthrough = if let Ok(plan) =
            serde_json::from_str::<aggregation::AggregationPlan>(&agg_json)
        {
            plan.is_passthrough
        } else {
            false
        };

        if is_passthrough || end_query.is_empty() {
            // Passthrough or no end_query: full refresh from base_query
            client
                .update(&format!("DELETE FROM \"{}\"", view_name), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!("INSERT INTO \"{}\" {}", view_name, base_query),
                    None,
                    &[],
                )
                .unwrap_or_report();
        } else {
            // Aggregate: rebuild intermediate + target
            let intermediate = intermediate_table_name(view_name);
            client
                .update(&format!("TRUNCATE {}", intermediate), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!("INSERT INTO {} {}", intermediate, base_query),
                    None,
                    &[],
                )
                .unwrap_or_report();
            client
                .update(&format!("DELETE FROM \"{}\"", view_name), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!("INSERT INTO \"{}\" {}", view_name, end_query),
                    None,
                    &[],
                )
                .unwrap_or_report();
        }

        // Update last_update_date
        client
            .update(
                "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report();

        "RECONCILED"
    })
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

    #[pg_test]
    fn test_create_simple_sum_imv() {
        Spi::run("CREATE TABLE test_orders (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run(
            "INSERT INTO test_orders (city, amount) VALUES
             ('Paris', 100), ('Paris', 200), ('London', 300)",
        )
        .expect("insert data");

        let result = crate::create_reflex_ivm(
            "test_city_totals",
            "SELECT city, SUM(amount) AS total FROM test_orders GROUP BY city",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify intermediate table exists and has correct data
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM __reflex_intermediate_test_city_totals",
        )
        .expect("query")
        .expect("count");
        assert_eq!(count, 2); // Paris, London

        // Verify target table has correct data
        let paris_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM test_city_totals WHERE city = 'Paris'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(paris_total.to_string(), "300");

        let london_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM test_city_totals WHERE city = 'London'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(london_total.to_string(), "300");
    }

    #[pg_test]
    fn test_create_avg_imv() {
        Spi::run("CREATE TABLE test_emp (id SERIAL, dept TEXT, salary NUMERIC)")
            .expect("create table");
        Spi::run(
            "INSERT INTO test_emp (dept, salary) VALUES
             ('eng', 100), ('eng', 200), ('sales', 150)",
        )
        .expect("insert data");

        let result = crate::create_reflex_ivm(
            "test_dept_avg",
            "SELECT dept, AVG(salary) AS avg_sal FROM test_emp GROUP BY dept",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify intermediate table has SUM and COUNT columns
        let eng_sum = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT \"__sum_salary\" FROM __reflex_intermediate_test_dept_avg WHERE dept = 'eng'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(eng_sum.to_string(), "300");

        let eng_count = Spi::get_one::<i64>(
            "SELECT \"__count_salary\" FROM __reflex_intermediate_test_dept_avg WHERE dept = 'eng'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(eng_count, 2);

        // Verify target table has correct AVG (150 = 300/2)
        let eng_avg = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT ROUND(avg_sal::numeric, 2) FROM test_dept_avg WHERE dept = 'eng'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(eng_avg.to_string(), "150.00");
    }

    #[pg_test]
    fn test_create_distinct_imv() {
        Spi::run("CREATE TABLE test_visits (id SERIAL, country TEXT)").expect("create table");
        Spi::run(
            "INSERT INTO test_visits (country) VALUES ('US'), ('US'), ('FR'), ('FR'), ('FR')",
        )
        .expect("insert data");

        let result = crate::create_reflex_ivm(
            "test_distinct_countries",
            "SELECT DISTINCT country FROM test_visits",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify target table has only distinct countries
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_distinct_countries",
        )
        .expect("query")
        .expect("count");
        assert_eq!(count, 2); // US, FR
    }

    #[pg_test]
    fn test_create_count_star_imv() {
        Spi::run("CREATE TABLE test_items (id SERIAL, category TEXT)").expect("create table");
        Spi::run(
            "INSERT INTO test_items (category) VALUES ('A'), ('A'), ('A'), ('B'), ('B')",
        )
        .expect("insert data");

        let result = crate::create_reflex_ivm(
            "test_cat_counts",
            "SELECT category, COUNT(*) AS cnt FROM test_items GROUP BY category",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let a_count = Spi::get_one::<i64>(
            "SELECT cnt FROM test_cat_counts WHERE category = 'A'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(a_count, 3);
    }

    #[pg_test]
    fn test_create_min_max_imv() {
        Spi::run("CREATE TABLE test_scores (id SERIAL, subject TEXT, score NUMERIC)")
            .expect("create table");
        Spi::run(
            "INSERT INTO test_scores (subject, score) VALUES
             ('math', 85), ('math', 92), ('math', 78),
             ('science', 88), ('science', 95)",
        )
        .expect("insert data");

        let result = crate::create_reflex_ivm(
            "test_score_range",
            "SELECT subject, MIN(score) AS lo, MAX(score) AS hi FROM test_scores GROUP BY subject",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let math_lo = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT lo FROM test_score_range WHERE subject = 'math'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(math_lo.to_string(), "78");

        let math_hi = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT hi FROM test_score_range WHERE subject = 'math'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(math_hi.to_string(), "92");
    }

    #[pg_test]
    fn test_create_multi_aggregate_imv() {
        Spi::run("CREATE TABLE test_sales (id SERIAL, region TEXT, revenue NUMERIC)")
            .expect("create table");
        Spi::run(
            "INSERT INTO test_sales (region, revenue) VALUES
             ('US', 1000), ('US', 2000), ('EU', 1500)",
        )
        .expect("insert data");

        let result = crate::create_reflex_ivm(
            "test_region_stats",
            "SELECT region, SUM(revenue) AS total, COUNT(*) AS cnt, AVG(revenue) AS avg_rev FROM test_sales GROUP BY region",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let us_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM test_region_stats WHERE region = 'US'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(us_total.to_string(), "3000");

        let us_cnt = Spi::get_one::<i64>(
            "SELECT cnt FROM test_region_stats WHERE region = 'US'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(us_cnt, 2);
    }

    #[pg_test]
    fn test_chained_imv_depth() {
        Spi::run("CREATE TABLE test_base (id SERIAL, val TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run(
            "INSERT INTO test_base (val, amount) VALUES ('a', 10), ('a', 20), ('b', 30)",
        )
        .expect("insert data");

        // First IMV at depth 1
        crate::create_reflex_ivm(
            "test_imv_1",
            "SELECT val, SUM(amount) AS total FROM test_base GROUP BY val",
        );

        // Second IMV depends on test_imv_1, should be at depth 2
        crate::create_reflex_ivm(
            "test_imv_2",
            "SELECT val, SUM(total) AS grand_total FROM test_imv_1 GROUP BY val",
        );

        let depth1 = Spi::get_one::<i32>(
            "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'test_imv_1'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(depth1, 1);

        let depth2 = Spi::get_one::<i32>(
            "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = 'test_imv_2'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(depth2, 2);

        // Verify graph_child of imv_1 includes imv_2
        let children = Spi::get_one::<Vec<String>>(
            "SELECT graph_child FROM public.__reflex_ivm_reference WHERE name = 'test_imv_1'",
        )
        .expect("query")
        .expect("value");
        assert!(children.contains(&"test_imv_2".to_string()));
    }

    #[pg_test]
    fn test_recursive_cte_rejected() {
        Spi::run("CREATE TABLE test_t1 (id INT)").expect("create table");
        let result = crate::create_reflex_ivm(
            "bad_view",
            "WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM nums WHERE n < 10) SELECT n, COUNT(*) AS cnt FROM nums GROUP BY n",
        );
        assert!(result.starts_with("ERROR"));
        assert!(result.contains("RECURSIVE"));
    }

    #[pg_test]
    fn test_unsupported_limit_rejected() {
        Spi::run("CREATE TABLE test_t2 (id INT)").expect("create table");
        let result =
            crate::create_reflex_ivm("bad_view2", "SELECT id, COUNT(*) AS cnt FROM test_t2 GROUP BY id LIMIT 10");
        assert!(result.starts_with("ERROR"));
    }

    #[pg_test]
    fn test_unsupported_window_rejected() {
        Spi::run("CREATE TABLE test_t3 (id INT, amount INT)").expect("create table");
        let result = crate::create_reflex_ivm(
            "bad_view3",
            "SELECT id, SUM(amount) OVER (PARTITION BY id) FROM test_t3",
        );
        assert!(result.starts_with("ERROR"));
    }

    #[pg_test]
    fn test_reference_table_populated() {
        Spi::run("CREATE TABLE test_ref_src (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO test_ref_src (city, amount) VALUES ('X', 1)").expect("insert");

        crate::create_reflex_ivm(
            "test_ref_view",
            "SELECT city, SUM(amount) AS total FROM test_ref_src GROUP BY city",
        );

        // Verify all key fields are populated
        let row = Spi::get_one::<bool>(
            "SELECT
                name IS NOT NULL
                AND graph_depth IS NOT NULL
                AND depends_on IS NOT NULL
                AND sql_query IS NOT NULL
                AND base_query IS NOT NULL
                AND end_query IS NOT NULL
                AND aggregations IS NOT NULL
                AND index_columns IS NOT NULL
                AND enabled = TRUE
             FROM public.__reflex_ivm_reference WHERE name = 'test_ref_view'",
        )
        .expect("query")
        .expect("value");
        assert!(row);
    }

    #[pg_test]
    fn test_triggers_created() {
        Spi::run("CREATE TABLE test_trig_src (id SERIAL, grp TEXT, val NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO test_trig_src (grp, val) VALUES ('a', 1)").expect("insert");

        crate::create_reflex_ivm(
            "test_trig_view",
            "SELECT grp, SUM(val) AS total FROM test_trig_src GROUP BY grp",
        );

        // Check all 4 consolidated triggers exist (INSERT, DELETE, UPDATE, TRUNCATE)
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pg_trigger
             WHERE tgname LIKE '__reflex_trigger_%_on_test_trig_src'",
        )
        .expect("query")
        .expect("count");
        assert_eq!(count, 4);
    }

    // ---- Trigger behavior tests ----

    #[pg_test]
    fn test_trigger_insert_updates_view() {
        Spi::run("CREATE TABLE trig_ins_src (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO trig_ins_src (city, amount) VALUES ('Paris', 100), ('London', 200)")
            .expect("seed");

        crate::create_reflex_ivm(
            "trig_ins_view",
            "SELECT city, SUM(amount) AS total FROM trig_ins_src GROUP BY city",
        );

        // Insert more rows AFTER IMV creation — triggers should fire
        Spi::run("INSERT INTO trig_ins_src (city, amount) VALUES ('Paris', 50), ('Berlin', 300)")
            .expect("insert");

        let paris = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM trig_ins_view WHERE city = 'Paris'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(paris.to_string(), "150"); // 100 + 50

        let berlin = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM trig_ins_view WHERE city = 'Berlin'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(berlin.to_string(), "300");
    }

    #[pg_test]
    fn test_trigger_delete_updates_view() {
        Spi::run("CREATE TABLE trig_del_src (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run(
            "INSERT INTO trig_del_src (city, amount) VALUES
             ('Paris', 100), ('Paris', 200), ('London', 300)",
        )
        .expect("seed");

        crate::create_reflex_ivm(
            "trig_del_view",
            "SELECT city, SUM(amount) AS total FROM trig_del_src GROUP BY city",
        );

        // Delete one Paris row
        Spi::run("DELETE FROM trig_del_src WHERE amount = 100").expect("delete");

        let paris = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM trig_del_view WHERE city = 'Paris'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(paris.to_string(), "200"); // 300 - 100
    }

    #[pg_test]
    fn test_trigger_delete_all_removes_group() {
        Spi::run("CREATE TABLE trig_delall_src (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO trig_delall_src (city, amount) VALUES ('X', 10), ('Y', 20)")
            .expect("seed");

        crate::create_reflex_ivm(
            "trig_delall_view",
            "SELECT city, SUM(amount) AS total FROM trig_delall_src GROUP BY city",
        );

        // Delete all rows for city 'X'
        Spi::run("DELETE FROM trig_delall_src WHERE city = 'X'").expect("delete");

        // 'X' should no longer appear in the target (ivm_count = 0)
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM trig_delall_view WHERE city = 'X'",
        )
        .expect("query")
        .expect("count");
        assert_eq!(count, 0);

        // 'Y' should still be there
        let y_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM trig_delall_view WHERE city = 'Y'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(y_total.to_string(), "20");
    }

    #[pg_test]
    fn test_trigger_update_correctness() {
        Spi::run("CREATE TABLE trig_upd_src (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO trig_upd_src (city, amount) VALUES ('A', 100), ('B', 200)")
            .expect("seed");

        crate::create_reflex_ivm(
            "trig_upd_view",
            "SELECT city, SUM(amount) AS total FROM trig_upd_src GROUP BY city",
        );

        // Update: change amount for city 'A'
        Spi::run("UPDATE trig_upd_src SET amount = 150 WHERE city = 'A'").expect("update");

        let a_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM trig_upd_view WHERE city = 'A'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(a_total.to_string(), "150");
    }

    #[pg_test]
    fn test_trigger_avg_insert_delete() {
        Spi::run("CREATE TABLE trig_avg_src (id SERIAL, dept TEXT, salary NUMERIC)")
            .expect("create table");
        Spi::run(
            "INSERT INTO trig_avg_src (dept, salary) VALUES ('eng', 100), ('eng', 200)",
        )
        .expect("seed");

        crate::create_reflex_ivm(
            "trig_avg_view",
            "SELECT dept, AVG(salary) AS avg_sal FROM trig_avg_src GROUP BY dept",
        );

        // Insert another row
        Spi::run("INSERT INTO trig_avg_src (dept, salary) VALUES ('eng', 300)")
            .expect("insert");

        // AVG should be (100+200+300)/3 = 200
        let avg = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT ROUND(avg_sal::numeric, 0) FROM trig_avg_view WHERE dept = 'eng'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(avg.to_string(), "200");

        // Delete the 100 row
        Spi::run("DELETE FROM trig_avg_src WHERE salary = 100").expect("delete");

        // AVG should be (200+300)/2 = 250
        let avg2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT ROUND(avg_sal::numeric, 0) FROM trig_avg_view WHERE dept = 'eng'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(avg2.to_string(), "250");
    }

    #[pg_test]
    fn test_trigger_distinct_ref_counting() {
        Spi::run("CREATE TABLE trig_dist_src (id SERIAL, country TEXT)")
            .expect("create table");
        Spi::run("INSERT INTO trig_dist_src (country) VALUES ('US'), ('US'), ('FR')")
            .expect("seed");

        crate::create_reflex_ivm(
            "trig_dist_view",
            "SELECT DISTINCT country FROM trig_dist_src",
        );

        // Delete one 'US' — should still appear (ref count > 0)
        Spi::run("DELETE FROM trig_dist_src WHERE id = 1").expect("delete one US");
        let us_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM trig_dist_view WHERE country = 'US'",
        )
        .expect("query")
        .expect("count");
        assert_eq!(us_count, 1); // Still visible

        // Delete last 'US' — should disappear
        Spi::run("DELETE FROM trig_dist_src WHERE country = 'US'").expect("delete last US");
        let us_gone = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM trig_dist_view WHERE country = 'US'",
        )
        .expect("query")
        .expect("count");
        assert_eq!(us_gone, 0);

        // FR should still be there
        let fr = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM trig_dist_view WHERE country = 'FR'",
        )
        .expect("query")
        .expect("count");
        assert_eq!(fr, 1);
    }

    #[pg_test]
    fn test_trigger_bulk_insert() {
        Spi::run("CREATE TABLE trig_bulk_src (id SERIAL, grp TEXT, val NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO trig_bulk_src (grp, val) VALUES ('X', 1)").expect("seed");

        crate::create_reflex_ivm(
            "trig_bulk_view",
            "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM trig_bulk_src GROUP BY grp",
        );

        // Bulk insert 100 rows for group 'X'
        Spi::run(
            "INSERT INTO trig_bulk_src (grp, val)
             SELECT 'X', generate_series(1, 100)",
        )
        .expect("bulk insert");

        let cnt = Spi::get_one::<i64>(
            "SELECT cnt FROM trig_bulk_view WHERE grp = 'X'",
        )
        .expect("query")
        .expect("count");
        assert_eq!(cnt, 101); // 1 seed + 100 bulk
    }

    #[pg_test]
    fn test_join_query_imv() {
        Spi::run("CREATE TABLE test_j_emp (id SERIAL PRIMARY KEY, name TEXT, dept_id INT)")
            .expect("create emp table");
        Spi::run("CREATE TABLE test_j_dept (id SERIAL PRIMARY KEY, dept_name TEXT)")
            .expect("create dept table");
        Spi::run("INSERT INTO test_j_dept (id, dept_name) VALUES (1, 'Engineering'), (2, 'Sales')")
            .expect("insert depts");
        Spi::run("INSERT INTO test_j_emp (name, dept_id) VALUES ('Alice', 1), ('Bob', 1), ('Carol', 2)")
            .expect("insert emps");

        let result = crate::create_reflex_ivm(
            "test_dept_counts",
            "SELECT d.dept_name, COUNT(*) AS emp_count FROM test_j_emp e JOIN test_j_dept d ON e.dept_id = d.id GROUP BY d.dept_name",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let eng_count = Spi::get_one::<i64>(
            "SELECT emp_count FROM test_dept_counts WHERE dept_name = 'Engineering'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(eng_count, 2);

        // Verify both source tables are tracked in depends_on
        let depends = Spi::get_one::<Vec<String>>(
            "SELECT depends_on FROM public.__reflex_ivm_reference WHERE name = 'test_dept_counts'",
        )
        .expect("query")
        .expect("value");
        assert_eq!(depends.len(), 2);
    }

    #[pg_test]
    fn test_passthrough_simple() {
        Spi::run("CREATE TABLE pt_src (id SERIAL, name TEXT, active BOOLEAN)")
            .expect("create table");
        Spi::run("INSERT INTO pt_src (name, active) VALUES ('Alice', true), ('Bob', false)")
            .expect("seed");

        let result = crate::create_reflex_ivm(
            "pt_view",
            "SELECT id, name FROM pt_src WHERE active = true",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify initial data
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_view")
            .expect("q").expect("v");
        assert_eq!(count, 1); // Only Alice (active=true)

        // INSERT a matching row → appears in target
        Spi::run("INSERT INTO pt_src (name, active) VALUES ('Carol', true)").expect("insert");
        let count2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_view")
            .expect("q").expect("v");
        assert_eq!(count2, 2);

        // INSERT a non-matching row → does not appear
        Spi::run("INSERT INTO pt_src (name, active) VALUES ('Dave', false)").expect("insert");
        let count3 = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_view")
            .expect("q").expect("v");
        assert_eq!(count3, 2); // Still 2
    }

    #[pg_test]
    fn test_passthrough_join() {
        Spi::run("CREATE TABLE pt_orders (id SERIAL, product_id INT, amount NUMERIC)")
            .expect("create orders");
        Spi::run("CREATE TABLE pt_products (id SERIAL PRIMARY KEY, name TEXT)")
            .expect("create products");
        Spi::run("INSERT INTO pt_products (id, name) VALUES (1, 'Widget'), (2, 'Gadget')")
            .expect("seed products");
        Spi::run("INSERT INTO pt_orders (product_id, amount) VALUES (1, 100), (2, 200)")
            .expect("seed orders");

        let result = crate::create_reflex_ivm(
            "pt_join_view",
            "SELECT o.id, p.name, o.amount FROM pt_orders o JOIN pt_products p ON o.product_id = p.id",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_join_view")
            .expect("q").expect("v");
        assert_eq!(count, 2);

        // INSERT into orders → trigger fires, new row appears
        Spi::run("INSERT INTO pt_orders (product_id, amount) VALUES (1, 300)")
            .expect("insert");
        let count2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_join_view")
            .expect("q").expect("v");
        assert_eq!(count2, 3);
    }

    #[pg_test]
    fn test_passthrough_delete_refreshes() {
        Spi::run("CREATE TABLE pt_del (id SERIAL, val TEXT)").expect("create");
        Spi::run("INSERT INTO pt_del (val) VALUES ('a'), ('b'), ('c')").expect("seed");

        crate::create_reflex_ivm("pt_del_view", "SELECT id, val FROM pt_del");

        // DELETE → full refresh
        Spi::run("DELETE FROM pt_del WHERE val = 'b'").expect("delete");
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_del_view")
            .expect("q").expect("v");
        assert_eq!(count, 2);
    }

    // ---- CTE tests ----

    #[pg_test]
    fn test_cte_simple_aggregate() {
        Spi::run("CREATE TABLE cte_src1 (id SERIAL, region TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO cte_src1 (region, amount) VALUES ('US', 100), ('US', 200), ('EU', 300)")
            .expect("seed");

        let result = crate::create_reflex_ivm(
            "cte_simple",
            "WITH regional AS (SELECT region, SUM(amount) AS total FROM cte_src1 GROUP BY region) SELECT region, total FROM regional",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Sub-IMV should exist with correct data
        let us = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cte_simple__cte_regional WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us.to_string(), "300");

        // The main view should be a VIEW reading from the sub-IMV
        let us_view = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cte_simple WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us_view.to_string(), "300");
    }

    #[pg_test]
    fn test_cte_trigger_propagation() {
        Spi::run("CREATE TABLE cte_src2 (id SERIAL, region TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO cte_src2 (region, amount) VALUES ('A', 10), ('B', 20)")
            .expect("seed");

        crate::create_reflex_ivm(
            "cte_prop",
            "WITH totals AS (SELECT region, SUM(amount) AS total FROM cte_src2 GROUP BY region) SELECT region, total FROM totals",
        );

        // INSERT into source → sub-IMV updates → VIEW reflects changes
        Spi::run("INSERT INTO cte_src2 (region, amount) VALUES ('A', 40)")
            .expect("insert");

        let a = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cte_prop WHERE region = 'A'",
        ).expect("q").expect("v");
        assert_eq!(a.to_string(), "50"); // 10 + 40

        // DELETE → propagates
        Spi::run("DELETE FROM cte_src2 WHERE amount = 10").expect("delete");
        let a2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cte_prop WHERE region = 'A'",
        ).expect("q").expect("v");
        assert_eq!(a2.to_string(), "40");
    }

    #[pg_test]
    fn test_cte_with_where_filter() {
        Spi::run("CREATE TABLE cte_src3 (id SERIAL, region TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO cte_src3 (region, amount) VALUES ('X', 50), ('Y', 200)")
            .expect("seed");

        crate::create_reflex_ivm(
            "cte_filtered",
            "WITH totals AS (SELECT region, SUM(amount) AS total FROM cte_src3 GROUP BY region) SELECT region, total FROM totals WHERE total > 100",
        );

        // Only Y (200) should appear, not X (50)
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cte_filtered",
        ).expect("q").expect("v");
        assert_eq!(count, 1);

        // INSERT that pushes X over threshold
        Spi::run("INSERT INTO cte_src3 (region, amount) VALUES ('X', 100)")
            .expect("insert");
        let count2 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cte_filtered",
        ).expect("q").expect("v");
        assert_eq!(count2, 2); // Both X (150) and Y (200) now > 100
    }

    #[pg_test]
    fn test_cte_multiple_chained() {
        Spi::run("CREATE TABLE cte_src4 (id SERIAL, region TEXT, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run(
            "INSERT INTO cte_src4 (region, city, amount) VALUES \
             ('US', 'NYC', 100), ('US', 'LA', 200), ('EU', 'London', 300)",
        )
        .expect("seed");

        let result = crate::create_reflex_ivm(
            "cte_chain",
            "WITH by_city AS (\
                SELECT region, city, SUM(amount) AS city_total FROM cte_src4 GROUP BY region, city\
             ), by_region AS (\
                SELECT region, SUM(city_total) AS total FROM by_city GROUP BY region\
             ) SELECT region, total FROM by_region",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify both sub-IMVs exist
        let city_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cte_chain__cte_by_city",
        ).expect("q").expect("v");
        assert_eq!(city_count, 3);

        // Verify final VIEW
        let us = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cte_chain WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us.to_string(), "300"); // 100 + 200
    }

    #[pg_test]
    fn test_cte_main_body_with_aggregation() {
        Spi::run("CREATE TABLE cte_src5 (id SERIAL, region TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO cte_src5 (region, amount) VALUES ('A', 10), ('B', 20), ('C', 30)")
            .expect("seed");

        // Main body has COUNT(*) → should create an IMV, not a VIEW
        let result = crate::create_reflex_ivm(
            "cte_agg_main",
            "WITH totals AS (SELECT region, SUM(amount) AS total FROM cte_src5 GROUP BY region) SELECT COUNT(*) AS num_regions FROM totals",
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let cnt = Spi::get_one::<i64>(
            "SELECT num_regions FROM cte_agg_main",
        ).expect("q").expect("v");
        assert_eq!(cnt, 3);
    }

    // ---- End-to-end tests ----

    #[pg_test]
    fn test_e2e_full_lifecycle() {
        Spi::run("CREATE TABLE e2e_src (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");

        crate::create_reflex_ivm(
            "e2e_view",
            "SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM e2e_src GROUP BY city",
        );

        // 1. INSERT initial rows
        Spi::run("INSERT INTO e2e_src (city, amount) VALUES ('A', 100), ('A', 200), ('B', 50)")
            .expect("insert");
        let a_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM e2e_view WHERE city = 'A'",
        ).expect("q").expect("v");
        assert_eq!(a_total.to_string(), "300");
        let b_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM e2e_view WHERE city = 'B'",
        ).expect("q").expect("v");
        assert_eq!(b_total.to_string(), "50");

        // 2. INSERT more rows
        Spi::run("INSERT INTO e2e_src (city, amount) VALUES ('A', 50), ('C', 400)")
            .expect("insert more");
        let a2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM e2e_view WHERE city = 'A'",
        ).expect("q").expect("v");
        assert_eq!(a2.to_string(), "350");
        let c = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM e2e_view WHERE city = 'C'",
        ).expect("q").expect("v");
        assert_eq!(c.to_string(), "400");

        // 3. UPDATE a row's value
        Spi::run("UPDATE e2e_src SET amount = 500 WHERE city = 'C'").expect("update");
        let c2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM e2e_view WHERE city = 'C'",
        ).expect("q").expect("v");
        assert_eq!(c2.to_string(), "500");

        // 4. DELETE one row from group A
        Spi::run("DELETE FROM e2e_src WHERE city = 'A' AND amount = 100").expect("delete one");
        let a3 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM e2e_view WHERE city = 'A'",
        ).expect("q").expect("v");
        assert_eq!(a3.to_string(), "250"); // 200 + 50

        // 5. DELETE all rows for group B → group disappears
        Spi::run("DELETE FROM e2e_src WHERE city = 'B'").expect("delete all B");
        let b_gone = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM e2e_view WHERE city = 'B'",
        ).expect("q").expect("v");
        assert_eq!(b_gone, 0);

        // 6. INSERT back into empty group B → reappears
        Spi::run("INSERT INTO e2e_src (city, amount) VALUES ('B', 999)").expect("reinsert B");
        let b_back = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM e2e_view WHERE city = 'B'",
        ).expect("q").expect("v");
        assert_eq!(b_back.to_string(), "999");
    }

    #[pg_test]
    fn test_e2e_cascading_propagation() {
        // Tests 2-level trigger propagation: source → L1 → L2
        // Triggers on L1's target table fire L2's delta processing automatically.
        Spi::run("CREATE TABLE cascade_src (id SERIAL, category TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO cascade_src (category, amount) VALUES ('X', 10), ('X', 20), ('Y', 30)")
            .expect("seed");

        // L1: SUM by category
        crate::create_reflex_ivm(
            "cascade_l1",
            "SELECT category, SUM(amount) AS total FROM cascade_src GROUP BY category",
        );

        // L2: SUM of L1 totals (grand total across all categories)
        crate::create_reflex_ivm(
            "cascade_l2",
            "SELECT SUM(total) AS grand_total FROM cascade_l1",
        );

        // Verify initial state
        let x1 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cascade_l1 WHERE category = 'X'",
        ).expect("q").expect("v");
        assert_eq!(x1.to_string(), "30"); // 10+20

        let gt = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM cascade_l2",
        ).expect("q").expect("v");
        assert_eq!(gt.to_string(), "60"); // 30+30

        // INSERT into base → L1 updates → L2 updates via cascade
        Spi::run("INSERT INTO cascade_src (category, amount) VALUES ('X', 70)")
            .expect("insert");
        let x1_after = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cascade_l1 WHERE category = 'X'",
        ).expect("q").expect("v");
        assert_eq!(x1_after.to_string(), "100"); // 10+20+70

        let gt_after = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM cascade_l2",
        ).expect("q").expect("v");
        assert_eq!(gt_after.to_string(), "130"); // 100+30

        // DELETE from base → both levels update
        Spi::run("DELETE FROM cascade_src WHERE amount = 10").expect("delete");
        let x1_del = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cascade_l1 WHERE category = 'X'",
        ).expect("q").expect("v");
        assert_eq!(x1_del.to_string(), "90"); // 20+70

        let gt_del = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM cascade_l2",
        ).expect("q").expect("v");
        assert_eq!(gt_del.to_string(), "120"); // 90+30
    }

    #[pg_test]
    fn test_e2e_join_trigger_insert_delete() {
        Spi::run("CREATE TABLE e2e_emp (id SERIAL PRIMARY KEY, name TEXT, dept_id INT)")
            .expect("create emp");
        Spi::run("CREATE TABLE e2e_dept (id SERIAL PRIMARY KEY, dept_name TEXT)")
            .expect("create dept");
        Spi::run("INSERT INTO e2e_dept (id, dept_name) VALUES (1, 'Eng'), (2, 'Sales')")
            .expect("seed depts");
        Spi::run("INSERT INTO e2e_emp (name, dept_id) VALUES ('Alice', 1), ('Bob', 1), ('Carol', 2)")
            .expect("seed emps");

        crate::create_reflex_ivm(
            "e2e_dept_counts",
            "SELECT d.dept_name, COUNT(*) AS emp_count FROM e2e_emp e JOIN e2e_dept d ON e.dept_id = d.id GROUP BY d.dept_name",
        );

        // Verify initial
        let eng = Spi::get_one::<i64>(
            "SELECT emp_count FROM e2e_dept_counts WHERE dept_name = 'Eng'",
        ).expect("q").expect("v");
        assert_eq!(eng, 2);

        // INSERT new employee → trigger fires, IMV updates
        Spi::run("INSERT INTO e2e_emp (name, dept_id) VALUES ('Dave', 1)")
            .expect("insert emp");
        let eng2 = Spi::get_one::<i64>(
            "SELECT emp_count FROM e2e_dept_counts WHERE dept_name = 'Eng'",
        ).expect("q").expect("v");
        assert_eq!(eng2, 3);

        // DELETE employee → Sales group disappears (ivm_count = 0)
        Spi::run("DELETE FROM e2e_emp WHERE name = 'Carol'").expect("delete emp");
        let sales_gone = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM e2e_dept_counts WHERE dept_name = 'Sales'",
        ).expect("q").expect("v");
        assert_eq!(sales_gone, 0); // Row removed from target
    }

    // ---- drop_reflex_ivm tests ----

    #[pg_test]
    fn test_drop_reflex_ivm_basic() {
        Spi::run("CREATE TABLE drop_src (id SERIAL, grp TEXT, val NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO drop_src (grp, val) VALUES ('a', 1)").expect("seed");

        crate::create_reflex_ivm(
            "drop_view",
            "SELECT grp, SUM(val) AS total FROM drop_src GROUP BY grp",
        );

        // Verify IMV exists
        let exists = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'drop_view'",
        ).expect("q").expect("v");
        assert_eq!(exists, 1);

        // Drop it
        let result = crate::drop_reflex_ivm("drop_view");
        assert_eq!(result, "DROP REFLEX INCREMENTAL VIEW");

        // Verify reference row gone
        let gone = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name = 'drop_view'",
        ).expect("q").expect("v");
        assert_eq!(gone, 0);

        // Verify target table gone
        let tbl_gone = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'drop_view'",
        ).expect("q").expect("v");
        assert_eq!(tbl_gone, 0);

        // Verify intermediate table gone
        let int_gone = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '__reflex_intermediate_drop_view'",
        ).expect("q").expect("v");
        assert_eq!(int_gone, 0);

        // Verify triggers gone
        let trig_gone = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_drop_view_%'",
        ).expect("q").expect("v");
        assert_eq!(trig_gone, 0);
    }

    #[pg_test]
    fn test_drop_reflex_ivm_refuses_with_children() {
        Spi::run("CREATE TABLE drop_ch_src (id SERIAL, grp TEXT, val NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO drop_ch_src (grp, val) VALUES ('a', 1)").expect("seed");

        crate::create_reflex_ivm(
            "drop_parent",
            "SELECT grp, SUM(val) AS total FROM drop_ch_src GROUP BY grp",
        );
        crate::create_reflex_ivm(
            "drop_child",
            "SELECT grp, SUM(total) AS grand FROM drop_parent GROUP BY grp",
        );

        // Should refuse without cascade
        let result = crate::drop_reflex_ivm("drop_parent");
        assert!(result.starts_with("ERROR"));
    }

    #[pg_test]
    fn test_drop_reflex_ivm_cascade() {
        Spi::run("CREATE TABLE drop_cas_src (id SERIAL, grp TEXT, val NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO drop_cas_src (grp, val) VALUES ('a', 1)").expect("seed");

        crate::create_reflex_ivm(
            "drop_cas_parent",
            "SELECT grp, SUM(val) AS total FROM drop_cas_src GROUP BY grp",
        );
        crate::create_reflex_ivm(
            "drop_cas_child",
            "SELECT grp, SUM(total) AS grand FROM drop_cas_parent GROUP BY grp",
        );

        // Cascade should drop both
        let result = crate::drop_reflex_ivm_cascade("drop_cas_parent", true);
        assert_eq!(result, "DROP REFLEX INCREMENTAL VIEW");

        // Both should be gone
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM public.__reflex_ivm_reference WHERE name IN ('drop_cas_parent', 'drop_cas_child')",
        ).expect("q").expect("v");
        assert_eq!(count, 0);
    }

    #[pg_test]
    fn test_drop_shared_trigger_lifecycle() {
        // Two IMVs on the same source. Dropping one should keep triggers;
        // dropping the last should remove triggers.
        Spi::run("CREATE TABLE drop_sh_src (id SERIAL, grp TEXT, val NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO drop_sh_src (grp, val) VALUES ('a', 1)").expect("seed");

        crate::create_reflex_ivm(
            "drop_sh_v1",
            "SELECT grp, SUM(val) AS total FROM drop_sh_src GROUP BY grp",
        );
        crate::create_reflex_ivm(
            "drop_sh_v2",
            "SELECT grp, COUNT(*) AS cnt FROM drop_sh_src GROUP BY grp",
        );

        // Both share 4 triggers on the source
        let trig_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_drop_sh_src'",
        ).expect("q").expect("v");
        assert_eq!(trig_count, 4);

        // Drop v1 → triggers should remain (v2 still depends on source)
        crate::drop_reflex_ivm("drop_sh_v1");
        let trig_after_v1 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_drop_sh_src'",
        ).expect("q").expect("v");
        assert_eq!(trig_after_v1, 4);

        // v2 should still work after v1 is dropped
        Spi::run("INSERT INTO drop_sh_src (grp, val) VALUES ('b', 2)").expect("insert");
        let cnt = Spi::get_one::<i64>(
            "SELECT cnt FROM drop_sh_v2 WHERE grp = 'b'",
        ).expect("q").expect("v");
        assert_eq!(cnt, 1);

        // Drop v2 → triggers should be removed (no more dependents)
        crate::drop_reflex_ivm("drop_sh_v2");
        let trig_after_v2 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_drop_sh_src'",
        ).expect("q").expect("v");
        assert_eq!(trig_after_v2, 0);
    }

    // ---- TRUNCATE tests ----

    #[pg_test]
    fn test_truncate_clears_imv() {
        Spi::run("CREATE TABLE trunc_src (id SERIAL, grp TEXT, val NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO trunc_src (grp, val) VALUES ('a', 10), ('b', 20)")
            .expect("seed");

        crate::create_reflex_ivm(
            "trunc_view",
            "SELECT grp, SUM(val) AS total FROM trunc_src GROUP BY grp",
        );

        // Verify data exists
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM trunc_view")
            .expect("q").expect("v");
        assert_eq!(count, 2);

        // TRUNCATE source → IMV should be empty
        Spi::run("TRUNCATE trunc_src").expect("truncate");

        let count_after = Spi::get_one::<i64>("SELECT COUNT(*) FROM trunc_view")
            .expect("q").expect("v");
        assert_eq!(count_after, 0);

        // Intermediate should also be empty
        let int_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM __reflex_intermediate_trunc_view",
        ).expect("q").expect("v");
        assert_eq!(int_count, 0);
    }

    #[pg_test]
    fn test_truncate_then_reinsert() {
        Spi::run("CREATE TABLE trunc2_src (id SERIAL, grp TEXT, val NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO trunc2_src (grp, val) VALUES ('x', 100)")
            .expect("seed");

        crate::create_reflex_ivm(
            "trunc2_view",
            "SELECT grp, SUM(val) AS total FROM trunc2_src GROUP BY grp",
        );

        Spi::run("TRUNCATE trunc2_src").expect("truncate");
        Spi::run("INSERT INTO trunc2_src (grp, val) VALUES ('y', 500)")
            .expect("reinsert");

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM trunc2_view")
            .expect("q").expect("v");
        assert_eq!(count, 1);

        let y_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM trunc2_view WHERE grp = 'y'",
        ).expect("q").expect("v");
        assert_eq!(y_total.to_string(), "500");
    }

    // ---- reflex_reconcile tests ----

    #[pg_test]
    fn test_reconcile_fixes_drift() {
        Spi::run("CREATE TABLE recon_src (id SERIAL, grp TEXT, val NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO recon_src (grp, val) VALUES ('a', 10), ('b', 20)")
            .expect("seed");

        crate::create_reflex_ivm(
            "recon_view",
            "SELECT grp, SUM(val) AS total FROM recon_src GROUP BY grp",
        );

        // Corrupt the intermediate table by zeroing out a value
        Spi::run("UPDATE __reflex_intermediate_recon_view SET \"__sum_val\" = 0 WHERE grp = 'a'")
            .expect("corrupt");

        // Target is now stale — verify corruption propagated
        // (target reflects intermediate, not source)

        // Reconcile should fix it
        let result = crate::reflex_reconcile("recon_view");
        assert_eq!(result, "RECONCILED");

        // Verify data matches expected
        let a = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM recon_view WHERE grp = 'a'",
        ).expect("q").expect("v");
        assert_eq!(a.to_string(), "10");

        let b = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM recon_view WHERE grp = 'b'",
        ).expect("q").expect("v");
        assert_eq!(b.to_string(), "20");
    }

    #[pg_test]
    fn test_reconcile_passthrough() {
        Spi::run("CREATE TABLE recon_pt_src (id SERIAL, name TEXT)")
            .expect("create table");
        Spi::run("INSERT INTO recon_pt_src (name) VALUES ('Alice'), ('Bob')")
            .expect("seed");

        crate::create_reflex_ivm("recon_pt_view", "SELECT id, name FROM recon_pt_src");

        // Manually delete a row from target (corrupt)
        Spi::run("DELETE FROM recon_pt_view WHERE name = 'Alice'").expect("corrupt");
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM recon_pt_view")
            .expect("q").expect("v");
        assert_eq!(count, 1);

        // Reconcile should restore
        let result = crate::reflex_reconcile("recon_pt_view");
        assert_eq!(result, "RECONCILED");

        let count_after = Spi::get_one::<i64>("SELECT COUNT(*) FROM recon_pt_view")
            .expect("q").expect("v");
        assert_eq!(count_after, 2);
    }

    // ---- Fan-out / fan-in topology tests ----

    #[pg_test]
    fn test_one_source_multiple_imvs() {
        // One source table with 3 independent IMVs depending on it.
        // INSERT/DELETE/UPDATE on the source should update all 3 correctly.
        Spi::run("CREATE TABLE multi_src (id SERIAL, city TEXT, amount NUMERIC, qty INTEGER)")
            .expect("create table");
        Spi::run(
            "INSERT INTO multi_src (city, amount, qty) VALUES \
             ('Paris', 100, 2), ('Paris', 200, 3), ('London', 300, 1)",
        ).expect("seed");

        // IMV 1: SUM of amount by city
        crate::create_reflex_ivm(
            "multi_v1",
            "SELECT city, SUM(amount) AS total FROM multi_src GROUP BY city",
        );
        // IMV 2: COUNT by city
        crate::create_reflex_ivm(
            "multi_v2",
            "SELECT city, COUNT(*) AS cnt FROM multi_src GROUP BY city",
        );
        // IMV 3: SUM of qty (no group by — global aggregate)
        crate::create_reflex_ivm(
            "multi_v3",
            "SELECT SUM(qty) AS total_qty FROM multi_src",
        );

        // Verify initial state
        let p_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM multi_v1 WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(p_total.to_string(), "300");

        let p_cnt = Spi::get_one::<i64>(
            "SELECT cnt FROM multi_v2 WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(p_cnt, 2);

        let tq = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total_qty FROM multi_v3",
        ).expect("q").expect("v");
        assert_eq!(tq.to_string(), "6"); // 2+3+1

        // INSERT → all 3 IMVs update
        Spi::run("INSERT INTO multi_src (city, amount, qty) VALUES ('Paris', 50, 5)")
            .expect("insert");

        let p_total2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM multi_v1 WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(p_total2.to_string(), "350");

        let p_cnt2 = Spi::get_one::<i64>(
            "SELECT cnt FROM multi_v2 WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(p_cnt2, 3);

        let tq2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total_qty FROM multi_v3",
        ).expect("q").expect("v");
        assert_eq!(tq2.to_string(), "11"); // 6+5

        // DELETE → all 3 update
        Spi::run("DELETE FROM multi_src WHERE amount = 100").expect("delete");

        let p_total3 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM multi_v1 WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(p_total3.to_string(), "250"); // 200+50

        let p_cnt3 = Spi::get_one::<i64>(
            "SELECT cnt FROM multi_v2 WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(p_cnt3, 2);

        let tq3 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total_qty FROM multi_v3",
        ).expect("q").expect("v");
        assert_eq!(tq3.to_string(), "9"); // 11-2

        // UPDATE → all 3 update
        Spi::run("UPDATE multi_src SET amount = 999, qty = 10 WHERE city = 'London'")
            .expect("update");

        let l_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM multi_v1 WHERE city = 'London'",
        ).expect("q").expect("v");
        assert_eq!(l_total.to_string(), "999");

        let tq4 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total_qty FROM multi_v3",
        ).expect("q").expect("v");
        assert_eq!(tq4.to_string(), "18"); // 9 - 1 + 10

        // Verify consolidated triggers: 3 IMVs on same source → only 4 triggers (not 12)
        let trig_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_multi_src'",
        ).expect("q").expect("v");
        assert_eq!(trig_count, 4); // ins, del, upd, trunc — shared by all 3 IMVs
    }

    #[pg_test]
    fn test_4_level_cascade_chain() {
        // 4-level chain: base → L1 → L2 → L3 → L4
        // Each level aggregates differently to exercise the cascade.
        Spi::run(
            "CREATE TABLE chain4_src (id SERIAL, region TEXT, city TEXT, amount NUMERIC)",
        ).expect("create table");
        Spi::run(
            "INSERT INTO chain4_src (region, city, amount) VALUES \
             ('US', 'NYC', 100), ('US', 'LA', 200), \
             ('EU', 'London', 300), ('EU', 'Paris', 400)",
        ).expect("seed");

        // L1: SUM by region+city (keeps full granularity)
        crate::create_reflex_ivm(
            "chain4_l1",
            "SELECT region, city, SUM(amount) AS city_total FROM chain4_src GROUP BY region, city",
        );

        // L2: SUM by region (rolls up cities)
        crate::create_reflex_ivm(
            "chain4_l2",
            "SELECT region, SUM(city_total) AS region_total FROM chain4_l1 GROUP BY region",
        );

        // L3: COUNT of regions (how many regions have data)
        crate::create_reflex_ivm(
            "chain4_l3",
            "SELECT COUNT(*) AS num_regions FROM chain4_l2",
        );

        // L4: passthrough of L3 (tests cascading through passthrough)
        crate::create_reflex_ivm(
            "chain4_l4",
            "SELECT num_regions FROM chain4_l3",
        );

        // Verify initial state across all levels
        let nyc = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT city_total FROM chain4_l1 WHERE city = 'NYC'",
        ).expect("q").expect("v");
        assert_eq!(nyc.to_string(), "100");

        let us = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT region_total FROM chain4_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us.to_string(), "300"); // 100+200

        let nr = Spi::get_one::<i64>(
            "SELECT num_regions FROM chain4_l3",
        ).expect("q").expect("v");
        assert_eq!(nr, 2); // US, EU

        let nr4 = Spi::get_one::<i64>(
            "SELECT num_regions FROM chain4_l4",
        ).expect("q").expect("v");
        assert_eq!(nr4, 2);

        // INSERT into base → all 4 levels update
        Spi::run(
            "INSERT INTO chain4_src (region, city, amount) VALUES ('US', 'NYC', 50)",
        ).expect("insert");

        let nyc2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT city_total FROM chain4_l1 WHERE city = 'NYC'",
        ).expect("q").expect("v");
        assert_eq!(nyc2.to_string(), "150"); // 100+50

        let us2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT region_total FROM chain4_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us2.to_string(), "350"); // 150+200

        // Still 2 regions
        let nr2 = Spi::get_one::<i64>(
            "SELECT num_regions FROM chain4_l4",
        ).expect("q").expect("v");
        assert_eq!(nr2, 2);

        // INSERT a new region → L3/L4 should show 3
        Spi::run(
            "INSERT INTO chain4_src (region, city, amount) VALUES ('ASIA', 'Tokyo', 500)",
        ).expect("insert new region");

        let nr3 = Spi::get_one::<i64>(
            "SELECT num_regions FROM chain4_l3",
        ).expect("q").expect("v");
        assert_eq!(nr3, 3);

        let nr3_l4 = Spi::get_one::<i64>(
            "SELECT num_regions FROM chain4_l4",
        ).expect("q").expect("v");
        assert_eq!(nr3_l4, 3);

        // DELETE all rows for ASIA → region disappears, back to 2
        Spi::run("DELETE FROM chain4_src WHERE region = 'ASIA'").expect("delete region");

        let nr_del = Spi::get_one::<i64>(
            "SELECT num_regions FROM chain4_l4",
        ).expect("q").expect("v");
        assert_eq!(nr_del, 2);

        // Verify EU region total unchanged through all operations
        let eu = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT region_total FROM chain4_l2 WHERE region = 'EU'",
        ).expect("q").expect("v");
        assert_eq!(eu.to_string(), "700"); // 300+400
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
