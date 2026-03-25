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
mod window;

use aggregation::plan_aggregation;
use query_decomposer::{
    bare_column_name, generate_aggregations_json, generate_base_query, generate_end_query, normalized_column_name,
    intermediate_table_name, quote_identifier, replace_identifier, split_qualified_name,
};
use schema_builder::{
    build_indexes_ddl, build_intermediate_table_ddl, build_target_table_ddl, build_trigger_ddls,
    build_deferred_trigger_ddls, build_deferred_flush_ddl, build_staging_table_ddl,
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
        unique_columns TEXT[],
        enabled BOOLEAN DEFAULT TRUE,
        last_update_date TIMESTAMP,
        storage_mode TEXT DEFAULT 'UNLOGGED',
        refresh_mode TEXT DEFAULT 'IMMEDIATE'
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
    create_reflex_ivm_impl(view_name, sql, unique_columns.unwrap_or(""), false, storage.unwrap_or("UNLOGGED"), mode.unwrap_or("IMMEDIATE"))
}

#[pg_extern]
fn create_reflex_ivm_if_not_exists(
    view_name: &str,
    sql: &str,
    unique_columns: default!(Option<&str>, "NULL"),
    storage: default!(Option<&str>, "'UNLOGGED'"),
    mode: default!(Option<&str>, "'IMMEDIATE'"),
) -> &'static str {
    create_reflex_ivm_impl(view_name, sql, unique_columns.unwrap_or(""), true, storage.unwrap_or("UNLOGGED"), mode.unwrap_or("IMMEDIATE"))
}

fn create_reflex_ivm_impl(view_name: &str, sql: &str, unique_columns_str: &str, if_not_exists: bool, storage_mode: &str, refresh_mode: &str) -> &'static str {
    let storage_upper = storage_mode.to_uppercase();
    if storage_upper != "LOGGED" && storage_upper != "UNLOGGED" {
        return "ERROR: storage must be 'LOGGED' or 'UNLOGGED'";
    }
    let logged = storage_upper == "LOGGED";
    let mode_upper = refresh_mode.to_uppercase();
    if mode_upper != "IMMEDIATE" && mode_upper != "DEFERRED" {
        return "ERROR: mode must be 'IMMEDIATE' or 'DEFERRED'";
    }
    let deferred = mode_upper == "DEFERRED";
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
    let dialect = PostgreSqlDialect {};
    let parsed_sql = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(e) => {
            warning!("pg_reflex: failed to parse SQL for '{}': {}", view_name, e);
            return Box::leak(
                format!("ERROR: Failed to parse SQL: {}", e).into_boxed_str(),
            );
        }
    };
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

    // --- Set operation decomposition: UNION ALL / UNION ---
    if let Some(ref set_op) = analysis.set_operation {
        match set_op.op {
            sqlparser::ast::SetOperator::Union => {}
            _ => {
                return "ERROR: Only UNION and UNION ALL are currently supported. \
                        INTERSECT and EXCEPT support is planned.";
            }
        }

        // Each operand becomes its own sub-IMV
        let mut sub_imv_names: Vec<String> = Vec::new();
        for (i, operand_sql) in set_op.operand_sqls.iter().enumerate() {
            let sub_name = format!("{}__union_{}", view_name, i);
            let result = create_reflex_ivm_impl(
                &sub_name, operand_sql, "", false, storage_mode, refresh_mode,
            );
            if result.starts_with("ERROR") {
                return result;
            }
            sub_imv_names.push(sub_name);
        }

        // Build the union query over sub-IMV targets
        let union_selects: Vec<String> = sub_imv_names
            .iter()
            .map(|name| format!("SELECT * FROM {}", quote_identifier(name)))
            .collect();

        if set_op.is_all {
            // UNION ALL: create a VIEW (zero overhead, always up-to-date)
            let view_sql = union_selects.join(" UNION ALL ");
            Spi::connect_mut(|client| {
                client
                    .update(
                        &format!(
                            "CREATE OR REPLACE VIEW {} AS {}",
                            quote_identifier(view_name),
                            view_sql
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            });

            // Register in reference table so drop_reflex_ivm can clean up.
            // depends_on = sub-IMV names (the VIEW reads from them, not from real sources)
            Spi::connect_mut(|client| {
                let depends_on: Vec<String> = sub_imv_names.clone();
                let depends_on_imv: Vec<String> = sub_imv_names.clone();
                let graph_child: Vec<String> = Vec::new();
                let depth = sub_imv_names.len() as i32 + 1;
                client.update(
                    "INSERT INTO public.__reflex_ivm_reference
                     (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
                      graph_child, sql_query, base_query, end_query,
                      aggregations, index_columns, unique_columns, enabled, last_update_date,
                      storage_mode, refresh_mode)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::json, $11, $12, TRUE, NOW(), $13, $14)",
                    None,
                    &[
                        unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(depth, PgBuiltInOids::INT4OID.oid().value()) },
                        unsafe { DatumWithOid::new(depends_on, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(depends_on_imv.clone(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(graph_child, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(view_sql.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::new(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new("{}".to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(storage_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(mode_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                ).unwrap_or_report();

                // Update sub-IMVs graph_child
                for imv_name in &depends_on_imv {
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
            });
        } else {
            // UNION (dedup): create a VIEW with UNION over sub-IMVs.
            // The sub-IMVs maintain data incrementally; PostgreSQL's UNION
            // handles deduplication at query time. This is correct and simple —
            // the expensive part (scanning source tables) is already incremental.
            let view_sql = union_selects.join(" UNION ");
            Spi::connect_mut(|client| {
                client
                    .update(
                        &format!(
                            "CREATE OR REPLACE VIEW {} AS {}",
                            quote_identifier(view_name),
                            view_sql
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            });

            // Register in reference table
            Spi::connect_mut(|client| {
                let depends_on: Vec<String> = sub_imv_names.clone();
                let depends_on_imv: Vec<String> = sub_imv_names.clone();
                let graph_child: Vec<String> = Vec::new();
                let depth = sub_imv_names.len() as i32 + 1;
                client.update(
                    "INSERT INTO public.__reflex_ivm_reference
                     (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
                      graph_child, sql_query, base_query, end_query,
                      aggregations, index_columns, unique_columns, enabled, last_update_date,
                      storage_mode, refresh_mode)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::json, $11, $12, TRUE, NOW(), $13, $14)",
                    None,
                    &[
                        unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(depth, PgBuiltInOids::INT4OID.oid().value()) },
                        unsafe { DatumWithOid::new(depends_on, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(depends_on_imv.clone(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(graph_child, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(view_sql.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::new(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new("{}".to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                        unsafe { DatumWithOid::new(storage_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(mode_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                ).unwrap_or_report();

                for imv_name in &depends_on_imv {
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
            });
        }

        return "CREATE REFLEX INCREMENTAL VIEW";
    }
    // --- End set operation decomposition ---

    // --- Window function decomposition: base sub-IMV + VIEW wrapper ---
    // Window functions can't be incrementally maintained (ROW_NUMBER, RANK, LAG
    // depend on the full result set). Instead, we decompose into:
    //   1. A sub-IMV for the base query (aggregate or passthrough) — incrementally maintained
    //   2. A VIEW that applies window functions at read time over the sub-IMV result
    // For GROUP BY + WINDOW, the sub-IMV result is small (one row per group),
    // so the window computation at read time is fast.
    if analysis.has_window_function {
        let decomp = window::decompose_window_query(&analysis);

        // Create a sub-IMV for the base query (aggregate or passthrough, no windows)
        let base_name = format!("{}__base", view_name);
        let result = create_reflex_ivm_impl(
            &base_name, &decomp.base_query, unique_columns_str, false, storage_mode, refresh_mode,
        );
        if result.starts_with("ERROR") {
            return result;
        }

        // Create a VIEW that applies window functions to the base sub-IMV
        let view_sql = format!(
            "SELECT {} FROM {}",
            decomp.view_select,
            quote_identifier(&base_name)
        );
        Spi::connect_mut(|client| {
            client
                .update(
                    &format!(
                        "CREATE OR REPLACE VIEW {} AS {}",
                        quote_identifier(view_name),
                        view_sql
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // Register in reference table for cleanup
            let depends_on = vec![base_name.clone()];
            let depends_on_imv = vec![base_name.clone()];
            client.update(
                "INSERT INTO public.__reflex_ivm_reference
                 (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
                  graph_child, sql_query, base_query, end_query,
                  aggregations, index_columns, unique_columns, enabled, last_update_date,
                  storage_mode, refresh_mode)
                 VALUES ($1, 2, $2, $3, $4, $5, $6, $7, $8, $9::json, $10, $11, TRUE, NOW(), $12, $13)",
                None,
                &[
                    unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(depends_on, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe { DatumWithOid::new(depends_on_imv.clone(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(view_sql.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::new(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new("{}".to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe { DatumWithOid::new(Vec::<String>::new(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe { DatumWithOid::new(storage_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(mode_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            ).unwrap_or_report();

            // Update base IMV's graph_child
            for name in &depends_on_imv {
                client.update(
                    "UPDATE public.__reflex_ivm_reference
                     SET graph_child = array_append(COALESCE(graph_child, ARRAY[]::TEXT[]), $1)
                     WHERE name = $2",
                    None,
                    &[
                        unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                ).unwrap_or_report();
            }
        });

        return "CREATE REFLEX INCREMENTAL VIEW";
    }
    // --- End window function decomposition ---

    // Reject subqueries with aggregation in FROM — the trigger replaces the inner table
    // with the transition table, so inner aggregations would only see delta rows.
    let has_subquery_with_agg = analysis.sources.iter().any(|s| s.starts_with("<subquery:"))
        && analysis
            .from_clause_sql
            .to_uppercase()
            .contains("GROUP BY");
    if has_subquery_with_agg {
        return "ERROR: Subqueries with aggregation in FROM are not supported. \
                Use a CTE (WITH clause) instead — pg_reflex decomposes CTEs into sub-IMVs automatically.";
    }

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
            let result = create_reflex_ivm_impl(&cte_view_name, &cte_query, "", false, storage_mode, refresh_mode);
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
        let body_parsed = match Parser::parse_sql(&dialect, &body_sql) {
            Ok(stmts) => stmts,
            Err(e) => {
                warning!("pg_reflex: failed to parse rewritten CTE body for '{}': {}", view_name, e);
                return Box::leak(
                    format!("ERROR: Failed to parse rewritten CTE body: {}", e).into_boxed_str(),
                );
            }
        };
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
                        &format!("CREATE OR REPLACE VIEW {} AS {}", quote_identifier(view_name), body_sql),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            });
            return "CREATE REFLEX INCREMENTAL VIEW";
        }

        // Main body has aggregation → create as a normal IMV
        return create_reflex_ivm_impl(view_name, &body_sql, "", false, storage_mode, refresh_mode);
    }
    // --- End CTE decomposition ---

    let froms = analysis.sources.clone();

    // Build aggregation plan from the analysis
    let mut plan = plan_aggregation(&analysis);

    // Resolve unique key columns for passthrough IMVs (enables targeted DELETE/UPDATE)
    let mut resolved_unique_columns: Vec<String> = Vec::new();
    let real_sources: Vec<&String> = froms.iter().filter(|s| !s.starts_with('<')).collect();
    let is_join_query = real_sources.len() > 1;

    if plan.is_passthrough {
        if !unique_columns_str.is_empty() {
            // Explicit unique columns from 3rd parameter
            resolved_unique_columns = unique_columns_str
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect();
            plan.passthrough_columns = resolved_unique_columns.clone();
            info!("pg_reflex: using explicit unique key ({}) for '{}'",
                resolved_unique_columns.join(", "), view_name);

            // Build per-source-table column mappings
            build_passthrough_key_mappings(
                &mut plan,
                &resolved_unique_columns,
                &real_sources,
                &analysis,
            );
        } else if !is_join_query {
            // Auto-detect: only for single-source queries (JOINs need explicit key)
            let select_bare_names: std::collections::HashSet<String> = analysis
                .select_columns
                .iter()
                .map(|c| {
                    let name = c.alias.as_deref().unwrap_or(&c.expr_sql);
                    bare_column_name(name).to_lowercase()
                })
                .collect();

            for source in &real_sources {
                let (src_schema, src_name) = split_qualified_name(source);
                let src_schema_str = src_schema.unwrap_or("public");

                let pk_cols: Vec<String> = Spi::connect(|client| {
                    client
                        .select(
                            "SELECT array_agg(a.attname ORDER BY k.n) as cols \
                             FROM pg_index ix \
                             JOIN pg_class t ON t.oid = ix.indrelid \
                             JOIN pg_namespace n ON n.oid = t.relnamespace \
                             JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, n) ON true \
                             JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.col \
                             WHERE n.nspname = $1 AND t.relname = $2 AND ix.indisunique AND ix.indisprimary \
                             GROUP BY ix.indexrelid \
                             ORDER BY count(*) \
                             LIMIT 1",
                            None,
                            &[
                                unsafe { DatumWithOid::new(src_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                                unsafe { DatumWithOid::new(src_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                            ],
                        )
                        .unwrap_or_report()
                        .filter_map(|row| {
                            row.get_by_name::<Vec<String>, _>("cols")
                                .unwrap_or(None)
                        })
                        .next()
                        .unwrap_or_default()
                });

                if !pk_cols.is_empty() {
                    let pk_lower: Vec<String> = pk_cols.iter().map(|c| c.to_lowercase()).collect();
                    let all_in_select = pk_lower.iter().all(|c| select_bare_names.contains(c));
                    if all_in_select {
                        resolved_unique_columns = pk_lower;
                        plan.passthrough_columns = resolved_unique_columns.clone();
                        // Single source: 1:1 mapping (target col == source col)
                        plan.passthrough_key_mappings.insert(
                            source.to_string(),
                            resolved_unique_columns.iter().map(|c| (c.clone(), c.clone())).collect(),
                        );
                        info!("pg_reflex: auto-detected PK ({}) from '{}' for '{}'",
                            resolved_unique_columns.join(", "), source, view_name);
                        break;
                    }
                }
            }
        } else {
            // JOIN query without explicit key: fall back to full refresh on DELETE/UPDATE
            info!(
                "pg_reflex: JOIN passthrough '{}' has no unique key. \
                 Provide 3rd argument to create_reflex_ivm for incremental DELETE/UPDATE. \
                 Example: SELECT create_reflex_ivm('{}', '...', 'col1,col2')",
                view_name, view_name
            );
        }
    }

    // Warn about select columns that are neither GROUP BY nor recognized aggregates.
    // These are silently dropped (e.g., bool_or(), string_agg(), unsupported functions).
    if !plan.is_passthrough {
        let group_by_set: std::collections::HashSet<&str> = analysis
            .group_by_columns
            .iter()
            .map(|s| s.as_str())
            .collect();
        for col in &analysis.select_columns {
            if !col.is_passthrough && col.aggregate.is_none() {
                warning!(
                    "pg_reflex: unsupported expression '{}' in SELECT — column will be missing from IMV '{}'",
                    col.alias.as_deref().unwrap_or(&col.expr_sql),
                    view_name
                );
            } else if col.is_passthrough
                && !group_by_set.contains(col.expr_sql.as_str())
                && !analysis.has_distinct
            {
                // Passthrough column not in GROUP BY — likely an unrecognized aggregate or expression
                let name = col.alias.as_deref().unwrap_or(&col.expr_sql);
                let bare = bare_column_name(name);
                let in_gb = group_by_set.iter().any(|gb| bare_column_name(gb) == bare);
                if !in_gb {
                    warning!(
                        "pg_reflex: expression '{}' not in GROUP BY and not a recognized aggregate — column will be missing from IMV '{}'",
                        col.expr_sql,
                        view_name
                    );
                }
            }
        }
    }

    // Check for duplicate view name
    let already_exists = Spi::connect(|client| {
        !client
            .select(
                "SELECT 1 FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>()
            .is_empty()
    });
    if already_exists {
        if if_not_exists {
            return "REFLEX INCREMENTAL VIEW ALREADY EXISTS (skipped)";
        }
        return "ERROR: IMV with this name already exists";
    }

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
            let create_kw = if logged { "CREATE TABLE" } else { "CREATE UNLOGGED TABLE" };
            client
                .update(
                    &format!("{} {} AS {}", create_kw, quote_identifier(view_name), sql),
                    None,
                    &[],
                )
                .unwrap_or_report();
            // ANALYZE so the query planner has statistics for the new table
            client
                .update(&format!("ANALYZE {}", quote_identifier(view_name)), None, &[])
                .unwrap_or_report();

            // Create unique index on target for resolved unique key columns
            if !resolved_unique_columns.is_empty() {
                let bare_view = split_qualified_name(view_name).1;
                let uk_cols: Vec<String> = resolved_unique_columns.iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect();
                client.update(
                    &format!(
                        "CREATE UNIQUE INDEX IF NOT EXISTS \"__reflex_uk_{}\" ON {} ({})",
                        bare_view, quote_identifier(view_name), uk_cols.join(", ")
                    ),
                    None, &[],
                ).unwrap_or_report();
            }
        } else {
            // Aggregate: build intermediate + target tables from the plan
            let column_types = query_column_types_from_catalog(client, &froms);

            if let Some(ddl) = build_intermediate_table_ddl(view_name, &plan, &column_types, logged) {
                let tbl = intermediate_table_name(view_name);
                client.update(&ddl, None, &[]).unwrap_or_report();
                unlogged_tables.push(tbl);
            }

            let target_ddl = build_target_table_ddl(view_name, &plan, &column_types, logged);
            client.update(&target_ddl, None, &[]).unwrap_or_report();
            // Note: indexes are created AFTER bulk insert for performance
        }

        // CREATE consolidated triggers on source tables (one set per source, shared by all IMVs).
        // Skip if triggers already exist on this source (another IMV already created them).
        for source in &froms {
            if source.starts_with("<subquery:") || source.starts_with("<function:") {
                warning!(
                    "pg_reflex: source '{}' for '{}' is a subquery — \
                     triggers are created on the underlying tables inside the subquery, \
                     but the subquery itself is re-executed on each delta",
                    source, view_name
                );
                continue;
            }
            if source.starts_with('<') {
                continue;
            }

            // Check if source is a materialized view (can't have triggers)
            let (src_schema, src_name) = split_qualified_name(source);
            let src_schema_str = src_schema.unwrap_or("public").to_string();
            let is_matview = client
                .select(
                    "SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
                     WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'm'",
                    None,
                    &[
                        unsafe {
                            DatumWithOid::new(
                                src_schema_str.clone(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                        unsafe {
                            DatumWithOid::new(
                                src_name.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                    ],
                )
                .unwrap_or_report()
                .next()
                .is_some();

            if is_matview {
                warning!(
                    "pg_reflex: source '{}' is a materialized view — triggers skipped. \
                     Use SELECT refresh_imv_depending_on('{}') after REFRESH MATERIALIZED VIEW.",
                    source, source
                );
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
                .next()
                .is_some();

            if deferred {
                // Deferred mode: create staging table if not exists
                let staging_ddl = build_staging_table_ddl(source);
                client.update(&staging_ddl, None, &[]).unwrap_or_report();
            }

            if !trig_exists {
                // Choose trigger type: if ANY deferred IMV exists on this source,
                // use deferred triggers (they handle both IMMEDIATE and DEFERRED IMVs).
                let has_any_deferred = deferred || {
                    let check = client
                        .select(
                            &format!(
                                "SELECT 1 FROM public.__reflex_ivm_reference \
                                 WHERE '{}' = ANY(depends_on) AND refresh_mode = 'DEFERRED' AND enabled = TRUE",
                                source.replace("'", "''")
                            ),
                            None,
                            &[],
                        )
                        .unwrap_or_report()
                        .next()
                        .is_some();
                    check
                };

                if has_any_deferred {
                    for ddl in build_deferred_trigger_ddls(source) {
                        client.update(&ddl, None, &[]).unwrap_or_report();
                    }
                } else {
                    for ddl in build_trigger_ddls(source) {
                        client.update(&ddl, None, &[]).unwrap_or_report();
                    }
                }
            } else if deferred {
                // Triggers already exist — upgrade to deferred triggers
                // (which handle both IMMEDIATE and DEFERRED IMVs)
                for ddl in build_deferred_trigger_ddls(source) {
                    client.update(&ddl, None, &[]).unwrap_or_report();
                }
            }
        }

        // Create deferred flush infrastructure if this IMV uses deferred mode
        if deferred {
            for ddl in build_deferred_flush_ddl() {
                client.update(&ddl, None, &[]).unwrap_or_report();
            }
        }

        // Issue 4: Add index on source GROUP BY columns for MIN/MAX recompute performance
        let has_min_max = plan.intermediate_columns.iter()
            .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX" || ic.source_aggregate == "BOOL_OR");
        if has_min_max && !plan.group_by_columns.is_empty() {
            for source in &froms {
                if source.starts_with('<') || ivm_froms.contains(source) {
                    continue;
                }
                // Only index columns that actually exist on this source table
                let (src_schema, src_name) = split_qualified_name(source);
                let src_schema_str = src_schema.unwrap_or("public");
                let source_cols: Vec<String> = client
                    .select(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
                        None,
                        &[
                            unsafe { DatumWithOid::new(src_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                            unsafe { DatumWithOid::new(src_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        ],
                    )
                    .unwrap_or_report()
                    .filter_map(|row| row.get_by_name::<&str, _>("column_name").unwrap_or(None).map(|s| s.to_lowercase()))
                    .collect();

                let idx_cols: Vec<String> = plan.group_by_columns.iter()
                    .map(|c| normalized_column_name(c))
                    .filter(|c| source_cols.contains(c))
                    .map(|c| format!("\"{}\"", c))
                    .collect();

                if idx_cols.is_empty() {
                    continue;
                }
                let safe_src = source.replace('.', "_");
                let bare_view = split_qualified_name(view_name).1;
                let idx_name = format!("__reflex_idx_{}_{}", bare_view, safe_src);
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
            .map(|c| normalized_column_name(c))
            .collect();

        // INSERT into reference table
        let depends_on: Vec<String> = froms.clone();
        let depends_on_imv: Vec<String> = ivm_froms.clone();
        let graph_child: Vec<String> = Vec::new();

        client.update(
            "INSERT INTO public.__reflex_ivm_reference
             (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
              graph_child, sql_query, base_query, end_query,
              aggregations, index_columns, unique_columns, enabled, last_update_date, storage_mode, refresh_mode)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::json, $11, $12, TRUE, NOW(), $13, $14)",
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
                unsafe { DatumWithOid::new(resolved_unique_columns.clone(), PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                unsafe { DatumWithOid::new(storage_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(mode_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
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

            let target_insert = format!("INSERT INTO {} {}", quote_identifier(view_name), end_query);
            client
                .update(&target_insert, None, &[])
                .unwrap_or_report();

            // Create indexes AFTER bulk insert (much faster than indexing during insert)
            for index_ddl in build_indexes_ddl(view_name, &plan) {
                client.update(&index_ddl, None, &[]).unwrap_or_report();
            }

            // ANALYZE so the query planner has accurate statistics
            client
                .update(&format!("ANALYZE {}", intermediate_tbl), None, &[])
                .unwrap_or_report();
            client
                .update(&format!("ANALYZE {}", quote_identifier(view_name)), None, &[])
                .unwrap_or_report();
        }

    });

    info!("pg_reflex: created IMV '{}'", view_name);
    "CREATE REFLEX INCREMENTAL VIEW"
}

/// Build per-source-table column mappings for passthrough DELETE/UPDATE.
///
/// For the "key owner" table (whose columns directly match the key), mapping is 1:1.
/// For secondary (joined) tables, the mapping is derived from JOIN conditions:
/// e.g., `ON s.product_id = p.id` maps target "product_id" → source "id" for the products table.
fn build_passthrough_key_mappings(
    plan: &mut crate::aggregation::AggregationPlan,
    key_columns: &[String],
    sources: &[&String],
    analysis: &crate::sql_analyzer::SqlAnalysis,
) {
    use std::collections::HashMap;

    // Build reverse alias map: real table name → alias
    let reverse_aliases: HashMap<&str, &str> = analysis
        .table_aliases
        .iter()
        .map(|(alias, table)| (table.as_str(), alias.as_str()))
        .collect();

    // Build a map from target column name → expr_sql (e.g., "product_id" → "s.product_id")
    let mut target_col_to_expr: HashMap<String, String> = HashMap::new();
    for col in &analysis.select_columns {
        let target_name = col
            .alias
            .as_deref()
            .unwrap_or(&col.expr_sql);
        let target_name = bare_column_name(target_name).to_lowercase();
        target_col_to_expr.insert(target_name, col.expr_sql.to_lowercase());
    }

    // For each source table, determine if it's the key owner or a secondary table
    for source in sources {
        let source_str = source.as_str();
        let alias = reverse_aliases.get(source_str).copied();

        // Check if this source owns all key columns directly
        // (i.e., for each key column, the SELECT expr references this table)
        let mut is_key_owner = true;
        for kc in key_columns {
            if let Some(expr) = target_col_to_expr.get(kc.as_str()) {
                // expr is like "s.product_id" — check if the table qualifier matches this source
                if let Some(dot_pos) = expr.rfind('.') {
                    let qualifier = &expr[..dot_pos];
                    let matches_alias = alias.is_some_and(|a| a.to_lowercase() == qualifier);
                    let matches_table = bare_column_name(source_str).to_lowercase() == qualifier;
                    if !matches_alias && !matches_table {
                        is_key_owner = false;
                        break;
                    }
                }
                // No qualifier (e.g., single table) — assume it belongs to this source if single source
            } else {
                is_key_owner = false;
                break;
            }
        }

        if is_key_owner {
            // Key owner: target_col == source_col (columns exist directly in this table)
            let mappings: Vec<(String, String)> = key_columns
                .iter()
                .map(|kc| {
                    // Extract the bare source column name from the expression
                    let source_col = target_col_to_expr
                        .get(kc.as_str())
                        .map(|expr| bare_column_name(expr).to_string())
                        .unwrap_or_else(|| kc.clone());
                    (kc.clone(), source_col)
                })
                .collect();
            plan.passthrough_key_mappings
                .insert(source_str.to_string(), mappings);
        } else {
            // Secondary table: derive mapping from JOIN conditions
            let mut mappings: Vec<(String, String)> = Vec::new();
            for join in &analysis.joins {
                if let Some(ref cond) = join.condition_sql {
                    let join_mappings = parse_join_condition_mappings(
                        cond,
                        source_str,
                        &analysis.table_aliases,
                        key_columns,
                        &target_col_to_expr,
                    );
                    mappings.extend(join_mappings);
                }
            }
            if !mappings.is_empty() {
                plan.passthrough_key_mappings
                    .insert(source_str.to_string(), mappings);
            }
            // If no mappings found, this source has no entry → triggers fall back to full refresh
        }
    }
}

/// Parse a JOIN condition to extract column mappings between the key-owner table and a secondary table.
///
/// For `s.product_id = p.id AND s.version = p.version`:
/// - Splits by AND
/// - For each equality, identifies which side belongs to the secondary table
/// - Maps the key-owner side's target column name to the secondary side's source column name
fn parse_join_condition_mappings(
    condition: &str,
    secondary_table: &str,
    table_aliases: &std::collections::HashMap<String, String>,
    key_columns: &[String],
    target_col_to_expr: &std::collections::HashMap<String, String>,
) -> Vec<(String, String)> {
    let mut mappings = Vec::new();

    // Build a set for fast lookup: which aliases/names refer to the secondary table?
    let secondary_lower = secondary_table.to_lowercase();
    let secondary_bare = bare_column_name(secondary_table).to_lowercase();
    let secondary_aliases: Vec<String> = table_aliases
        .iter()
        .filter(|(_, table)| table.to_lowercase() == secondary_lower)
        .map(|(alias, _)| alias.to_lowercase())
        .collect();

    // Build reverse: for each key column, which expr_sql does it correspond to?
    // e.g., "product_id" → "s.product_id"

    // Split condition by AND (case insensitive)
    for part in condition.split(" AND ").chain(condition.split(" and ")) {
        let part = part.trim();
        let sides: Vec<&str> = part.splitn(2, '=').collect();
        if sides.len() != 2 {
            continue;
        }
        let left = sides[0].trim().to_lowercase();
        let right = sides[1].trim().to_lowercase();

        // Determine which side belongs to the secondary table
        let (secondary_side, other_side) =
            if is_from_table(&left, &secondary_bare, &secondary_aliases) {
                (left, right)
            } else if is_from_table(&right, &secondary_bare, &secondary_aliases) {
                (right, left)
            } else {
                continue;
            };

        let secondary_col = bare_column_name(&secondary_side).to_string();
        let other_col = bare_column_name(&other_side).to_string();

        // Find which key column the other side maps to
        // The other side's bare column might be a key column directly,
        // or the other side's full expression might match a key column's expr_sql
        for kc in key_columns {
            if *kc == other_col {
                // Direct match: key column "product_id" and other side bare name is "product_id"
                mappings.push((kc.clone(), secondary_col.clone()));
                break;
            }
            // Check via expr_sql: key column "product_id" has expr "s.product_id",
            // and other_side is "s.product_id"
            if let Some(expr) = target_col_to_expr.get(kc.as_str()) {
                if *expr == other_side {
                    mappings.push((kc.clone(), secondary_col.clone()));
                    break;
                }
            }
        }
    }

    mappings
}

/// Check if a qualified column reference (e.g., "p.id") belongs to a given table.
fn is_from_table(qualified_col: &str, table_bare_name: &str, table_aliases: &[String]) -> bool {
    if let Some(dot_pos) = qualified_col.rfind('.') {
        let qualifier = &qualified_col[..dot_pos];
        qualifier == table_bare_name || table_aliases.iter().any(|a| a == qualifier)
    } else {
        false
    }
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
        let rows = client
            .select(
                "SELECT column_name::text AS col_name, data_type::text AS data_type \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2",
                None,
                &[
                    unsafe {
                        DatumWithOid::new(
                            schema.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe {
                        DatumWithOid::new(
                            tbl.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                ],
            )
            .unwrap_or_report();
        for row in rows {
            if let (Some(col_name), Some(data_type)) = (
                row.get_by_name::<String, _>("col_name").unwrap_or(None),
                row.get_by_name::<String, _>("data_type").unwrap_or(None),
            ) {
                let pg_type = map_information_schema_type(&data_type);
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
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
    drop_reflex_ivm_impl(view_name, false)
}

#[pg_extern(name = "drop_reflex_ivm")]
fn drop_reflex_ivm_cascade(view_name: &str, cascade: bool) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
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
            warning!("pg_reflex: drop failed — IMV '{}' not found", view_name);
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
                    "SELECT COUNT(*) AS cnt FROM public.__reflex_ivm_reference \
                     WHERE $1 = ANY(depends_on) AND name != $2",
                    None,
                    &[
                        unsafe {
                            DatumWithOid::new(
                                source.clone(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                        unsafe {
                            DatumWithOid::new(
                                view_name.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                    ],
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
                &format!("DROP TABLE IF EXISTS {}", quote_identifier(view_name)),
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

        info!("pg_reflex: dropped IMV '{}'", view_name);
        "DROP REFLEX INCREMENTAL VIEW"
    })
}

/// Reconcile an IMV by rebuilding intermediate + target from scratch.
/// Use this as a safety net (manually or via pg_cron) to fix drift.
#[pg_extern]
fn reflex_reconcile(view_name: &str) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
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
            warning!("pg_reflex: reconcile failed — IMV '{}' not found or disabled", view_name);
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
            // Passthrough: optimized refresh — drop indexes, TRUNCATE, INSERT, recreate, ANALYZE
            let (tgt_schema, tgt_name) = split_qualified_name(view_name);
            let tgt_schema_str = tgt_schema.unwrap_or("public");

            // Save and drop all indexes on target
            let saved_indexes: Vec<(String, String)> = client
                .select(
                    "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
                    None,
                    &[
                        unsafe { DatumWithOid::new(tgt_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(tgt_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    let name = row.get_by_name::<&str, _>("indexname").unwrap_or(None)?.to_string();
                    let def = row.get_by_name::<&str, _>("indexdef").unwrap_or(None)?.to_string();
                    Some((name, def))
                })
                .collect();

            for (idx_name, _) in &saved_indexes {
                client.update(&format!("DROP INDEX IF EXISTS \"{}\".\"{}\"", tgt_schema_str, idx_name), None, &[]).unwrap_or_report();
            }

            // Bulk refresh without indexes
            client
                .update(&format!("TRUNCATE {}", quote_identifier(view_name)), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!("INSERT INTO {} {}", quote_identifier(view_name), base_query),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // Recreate all indexes
            for (_, idx_def) in &saved_indexes {
                client.update(idx_def, None, &[]).unwrap_or_report();
            }

            // ANALYZE
            client.update(&format!("ANALYZE {}", quote_identifier(view_name)), None, &[]).unwrap_or_report();
        } else {
            // Aggregate: rebuild intermediate + target
            // Drop pg_reflex-managed indexes first for faster bulk insert
            let plan: aggregation::AggregationPlan =
                serde_json::from_str(&agg_json).unwrap_or_else(|_| {
                    aggregation::AggregationPlan {
                        group_by_columns: vec![],
                        intermediate_columns: vec![],
                        end_query_mappings: vec![],
                        has_distinct: false,
                        needs_ivm_count: false,
                        distinct_columns: vec![],
                        is_passthrough: false,
                        passthrough_columns: vec![],
                        passthrough_key_mappings: std::collections::HashMap::new(),
                        having_clause: None,
                    }
                });

            let intermediate = intermediate_table_name(view_name);
            let (_, bare_view) = split_qualified_name(view_name);
            let int_unquoted = intermediate.replace('"', "");
            let (int_schema, _) = split_qualified_name(&int_unquoted);
            let int_schema_str = int_schema.unwrap_or("public");

            // Collect and drop reflex-managed indexes on intermediate table
            let int_indexes: Vec<String> = client
                .select(
                    "SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
                    None,
                    &[
                        unsafe { DatumWithOid::new(int_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(format!("__reflex_intermediate_{}", bare_view), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                )
                .unwrap_or_report()
                .filter_map(|row| row.get_by_name::<&str, _>("indexname").unwrap_or(None).map(|s| s.to_string()))
                .collect();

            for idx in &int_indexes {
                client.update(&format!("DROP INDEX IF EXISTS \"{}\".\"{}\"", int_schema_str, idx), None, &[]).unwrap_or_report();
            }

            // Collect ALL indexes on target table (save DDL for user-created ones)
            let (tgt_schema, tgt_name) = split_qualified_name(view_name);
            let tgt_schema_str = tgt_schema.unwrap_or("public");
            let tgt_saved_indexes: Vec<(String, String)> = client
                .select(
                    "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
                    None,
                    &[
                        unsafe { DatumWithOid::new(tgt_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(tgt_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    let name = row.get_by_name::<&str, _>("indexname").unwrap_or(None)?.to_string();
                    let def = row.get_by_name::<&str, _>("indexdef").unwrap_or(None)?.to_string();
                    Some((name, def))
                })
                .collect();

            for (idx_name, _) in &tgt_saved_indexes {
                client.update(&format!("DROP INDEX IF EXISTS \"{}\".\"{}\"", tgt_schema_str, idx_name), None, &[]).unwrap_or_report();
            }

            // Bulk insert without indexes
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
                .update(&format!("TRUNCATE {}", quote_identifier(view_name)), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!("INSERT INTO {} {}", quote_identifier(view_name), end_query),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // Recreate reflex-managed indexes (hash index on intermediate + target indexes)
            for index_ddl in build_indexes_ddl(view_name, &plan) {
                client.update(&index_ddl, None, &[]).unwrap_or_report();
            }

            // Recreate user-created indexes on target (skip reflex-managed ones already recreated above)
            for (idx_name, idx_def) in &tgt_saved_indexes {
                if idx_name.starts_with("idx__reflex_") || idx_name.starts_with("__reflex_") {
                    continue; // Already handled by build_indexes_ddl
                }
                client.update(idx_def, None, &[]).unwrap_or_report();
            }

            // ANALYZE for query planner
            client.update(&format!("ANALYZE {}", intermediate), None, &[]).unwrap_or_report();
            client.update(&format!("ANALYZE {}", quote_identifier(view_name)), None, &[]).unwrap_or_report();
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

        info!("pg_reflex: reconciled IMV '{}'", view_name);
        "RECONCILED"
    })
}

/// Refresh a single IMV by rebuilding from source. Alias for reflex_reconcile.
/// Use after REFRESH MATERIALIZED VIEW on a source that feeds this IMV.
#[pg_extern]
fn refresh_reflex_imv(view_name: &str) -> &'static str {
    reflex_reconcile(view_name)
}

/// Refresh ALL IMVs that depend on a given source table or materialized view.
/// Processes IMVs in graph_depth order (L1 before L2).
#[pg_extern]
fn refresh_imv_depending_on(source: &str) -> &'static str {
    // Collect IMV names in a separate SPI connection (closed before reconcile calls)
    let names: Vec<String> = Spi::connect(|client| {
        client
            .select(
                "SELECT name FROM public.__reflex_ivm_reference \
                 WHERE $1 = ANY(depends_on) AND enabled = TRUE \
                 ORDER BY graph_depth",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        source.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<&str, _>("name")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
            .collect()
    });

    if names.is_empty() {
        warning!("pg_reflex: no IMVs depend on '{}'", source);
        return "REFRESHED 0 IMVs";
    }

    let count = names.len();
    for name in &names {
        let result = reflex_reconcile(name);
        if result.starts_with("ERROR") {
            warning!("pg_reflex: failed to refresh '{}': {}", name, result);
        }
    }

    info!(
        "pg_reflex: refreshed {} IMV(s) depending on '{}'",
        count, source
    );
    Box::leak(format!("REFRESHED {} IMVs", count).into_boxed_str())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_extern]
    fn hello_pg_reflex() -> &'static str {
        "Hello, pg_reflex"
    }

    #[pg_test]
    fn test_hello_pg_reflex() {
        assert_eq!("Hello, pg_reflex", hello_pg_reflex());
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
        );

        // Second IMV depends on test_imv_1, should be at depth 2
        crate::create_reflex_ivm(
            "test_imv_2",
            "SELECT val, SUM(total) AS grand_total FROM test_imv_1 GROUP BY val",
            None,
            None,
            None,
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
            None,
            None,
            None,
        );
        assert!(result.starts_with("ERROR"));
        assert!(result.contains("RECURSIVE"));
    }

    #[pg_test]
    fn test_unsupported_limit_rejected() {
        Spi::run("CREATE TABLE test_t2 (id INT)").expect("create table");
        let result =
            crate::create_reflex_ivm("bad_view2", "SELECT id, COUNT(*) AS cnt FROM test_t2 GROUP BY id LIMIT 10", None, None, None);
        assert!(result.starts_with("ERROR"));
    }

    #[pg_test]
    fn test_window_function_accepted() {
        Spi::run("CREATE TABLE test_t3 (id INT, amount INT)").expect("create table");
        Spi::run("INSERT INTO test_t3 VALUES (1, 10), (1, 20), (2, 30)").expect("seed");
        let result = crate::create_reflex_ivm(
            "bad_view3",
            "SELECT id, amount, SUM(amount) OVER (PARTITION BY id) AS part_sum FROM test_t3",
            None,
            None,
            None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
    }

    #[pg_test]
    fn test_reference_table_populated() {
        Spi::run("CREATE TABLE test_ref_src (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO test_ref_src (city, amount) VALUES ('X', 1)").expect("insert");

        crate::create_reflex_ivm(
            "test_ref_view",
            "SELECT city, SUM(amount) AS total FROM test_ref_src GROUP BY city",
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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

        crate::create_reflex_ivm("pt_del_view", "SELECT id, val FROM pt_del", None, None, None);

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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
        );

        // L2: SUM of L1 totals (grand total across all categories)
        crate::create_reflex_ivm(
            "cascade_l2",
            "SELECT SUM(total) AS grand_total FROM cascade_l1",
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
        );
        crate::create_reflex_ivm(
            "drop_child",
            "SELECT grp, SUM(total) AS grand FROM drop_parent GROUP BY grp",
            None,
            None,
            None,
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
            None,
            None,
            None,
        );
        crate::create_reflex_ivm(
            "drop_cas_child",
            "SELECT grp, SUM(total) AS grand FROM drop_cas_parent GROUP BY grp",
            None,
            None,
            None,
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
            None,
            None,
            None,
        );
        crate::create_reflex_ivm(
            "drop_sh_v2",
            "SELECT grp, COUNT(*) AS cnt FROM drop_sh_src GROUP BY grp",
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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
            None,
            None,
            None,
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

        crate::create_reflex_ivm("recon_pt_view", "SELECT id, name FROM recon_pt_src", None, None, None);

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
            None,
            None,
            None,
        );
        // IMV 2: COUNT by city
        crate::create_reflex_ivm(
            "multi_v2",
            "SELECT city, COUNT(*) AS cnt FROM multi_src GROUP BY city",
            None,
            None,
            None,
        );
        // IMV 3: SUM of qty (no group by — global aggregate)
        crate::create_reflex_ivm(
            "multi_v3",
            "SELECT SUM(qty) AS total_qty FROM multi_src",
            None,
            None,
            None,
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
            None,
            None,
            None,
        );

        // L2: SUM by region (rolls up cities)
        crate::create_reflex_ivm(
            "chain4_l2",
            "SELECT region, SUM(city_total) AS region_total FROM chain4_l1 GROUP BY region",
            None,
            None,
            None,
        );

        // L3: COUNT of regions (how many regions have data)
        crate::create_reflex_ivm(
            "chain4_l3",
            "SELECT COUNT(*) AS num_regions FROM chain4_l2",
            None,
            None,
            None,
        );

        // L4: passthrough of L3 (tests cascading through passthrough)
        crate::create_reflex_ivm(
            "chain4_l4",
            "SELECT num_regions FROM chain4_l3",
            None,
            None,
            None,
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

    // ================================================================
    // Targeted refresh correctness tests
    // ================================================================

    /// Test that INSERT creates new groups and updates existing groups correctly.
    #[pg_test]
    fn pg_test_targeted_refresh_insert_correctness() {
        // Setup: 100 rows across 10 groups (group_id 0..9, 10 rows each)
        Spi::run("CREATE TABLE tr_src (id SERIAL, group_id INT NOT NULL, amount NUMERIC NOT NULL)").expect("create");
        Spi::run(
            "INSERT INTO tr_src (group_id, amount) \
             SELECT i % 10, (i * 7 % 100)::numeric FROM generate_series(1, 100) i"
        ).expect("seed");

        Spi::run(
            "SELECT create_reflex_ivm('tr_insert_test', \
             'SELECT group_id, SUM(amount) AS total, COUNT(*) AS cnt FROM tr_src GROUP BY group_id')"
        ).expect("create imv");

        // Verify 10 groups
        let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_insert_test").expect("q").expect("v");
        assert_eq!(cnt, 10);

        // INSERT 20 rows: 15 into existing groups (0..4), 5 into NEW groups (10..14)
        Spi::run(
            "INSERT INTO tr_src (group_id, amount) \
             SELECT CASE WHEN i <= 15 THEN (i - 1) % 5 ELSE i - 16 + 10 END, 100.0 \
             FROM generate_series(1, 20) i"
        ).expect("insert");

        // Now should have 15 groups (10 original + 5 new)
        let cnt2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_insert_test").expect("q").expect("v");
        assert_eq!(cnt2, 15);

        // Verify correctness against direct query
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM ( \
                SELECT r.group_id::text FROM tr_insert_test r \
                FULL OUTER JOIN (SELECT group_id, SUM(amount) AS total, COUNT(*) AS cnt FROM tr_src GROUP BY group_id) d \
                    ON r.group_id::text = d.group_id::text \
                WHERE r.total IS DISTINCT FROM d.total OR r.cnt IS DISTINCT FROM d.cnt \
            ) x"
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "IMV should match direct query after INSERT");
    }

    /// Test that DELETE removes groups when all their rows are deleted.
    #[pg_test]
    fn pg_test_targeted_refresh_delete_group_elimination() {
        Spi::run("CREATE TABLE tr_del_src (id SERIAL, region TEXT NOT NULL, amount NUMERIC NOT NULL)").expect("create");
        Spi::run("INSERT INTO tr_del_src (region, amount) VALUES ('A', 10), ('A', 20), ('A', 30)").expect("ins A");
        Spi::run("INSERT INTO tr_del_src (region, amount) VALUES ('B', 40), ('B', 50)").expect("ins B");
        Spi::run("INSERT INTO tr_del_src (region, amount) VALUES ('C', 60)").expect("ins C");

        Spi::run(
            "SELECT create_reflex_ivm('tr_del_test', \
             'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM tr_del_src GROUP BY region')"
        ).expect("create imv");

        // 3 groups: A(60, 3), B(90, 2), C(60, 1)
        let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_del_test").expect("q").expect("v");
        assert_eq!(cnt, 3);

        // Delete ALL rows from group B
        Spi::run("DELETE FROM tr_del_src WHERE region = 'B'").expect("delete B");

        // Group B should be gone
        let cnt2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_del_test").expect("q").expect("v");
        assert_eq!(cnt2, 2);

        let has_b = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM tr_del_test WHERE region = 'B'"
        ).expect("q").expect("v");
        assert_eq!(has_b, 0, "Group B should be eliminated");

        // A and C should be unchanged
        let a_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM tr_del_test WHERE region = 'A'"
        ).expect("q").expect("v");
        assert_eq!(a_total.to_string(), "60");
    }

    /// Test that UPDATE correctly handles rows changing groups.
    #[pg_test]
    fn pg_test_targeted_refresh_update_group_change() {
        Spi::run("CREATE TABLE tr_upd_src (id SERIAL, region TEXT NOT NULL, amount NUMERIC NOT NULL)").expect("create");
        Spi::run("INSERT INTO tr_upd_src (region, amount) VALUES \
                  ('East', 100), ('East', 200), ('West', 300), ('West', 400)").expect("seed");

        Spi::run(
            "SELECT create_reflex_ivm('tr_upd_test', \
             'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM tr_upd_src GROUP BY region')"
        ).expect("create imv");

        // East=300(2), West=700(2)
        let east = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM tr_upd_test WHERE region = 'East'"
        ).expect("q").expect("v");
        assert_eq!(east.to_string(), "300");

        // Move one East row to a NEW group "North"
        Spi::run("UPDATE tr_upd_src SET region = 'North' WHERE id = 1").expect("update");

        // East should lose 100 (now 200, cnt=1), North should appear (100, cnt=1), West unchanged
        let east2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM tr_upd_test WHERE region = 'East'"
        ).expect("q").expect("v");
        assert_eq!(east2.to_string(), "200");

        let north = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM tr_upd_test WHERE region = 'North'"
        ).expect("q").expect("v");
        assert_eq!(north.to_string(), "100");

        let west = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM tr_upd_test WHERE region = 'West'"
        ).expect("q").expect("v");
        assert_eq!(west.to_string(), "700");

        let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_upd_test").expect("q").expect("v");
        assert_eq!(cnt, 3);
    }

    /// Test targeted refresh with multi-column GROUP BY.
    #[pg_test]
    fn pg_test_targeted_refresh_multi_column_group() {
        Spi::run("CREATE TABLE tr_mc_src (id SERIAL, region TEXT NOT NULL, category TEXT NOT NULL, amount NUMERIC NOT NULL)").expect("create");
        Spi::run("INSERT INTO tr_mc_src (region, category, amount) VALUES \
                  ('US', 'A', 10), ('US', 'B', 20), ('EU', 'A', 30), ('EU', 'B', 40)").expect("seed");

        Spi::run(
            "SELECT create_reflex_ivm('tr_mc_test', \
             'SELECT region, category, SUM(amount) AS total FROM tr_mc_src GROUP BY region, category')"
        ).expect("create imv");

        // 4 groups: US-A(10), US-B(20), EU-A(30), EU-B(40)
        let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_mc_test").expect("q").expect("v");
        assert_eq!(cnt, 4);

        // INSERT into existing group US-A and new group US-C
        Spi::run("INSERT INTO tr_mc_src (region, category, amount) VALUES ('US', 'A', 5), ('US', 'C', 50)").expect("insert");

        let cnt2 = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_mc_test").expect("q").expect("v");
        assert_eq!(cnt2, 5, "Should have 5 groups after insert (4 + US-C)");

        let us_a = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM tr_mc_test WHERE region = 'US' AND category = 'A'"
        ).expect("q").expect("v");
        assert_eq!(us_a.to_string(), "15"); // 10 + 5

        // DELETE all EU rows
        Spi::run("DELETE FROM tr_mc_src WHERE region = 'EU'").expect("delete");

        let cnt3 = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_mc_test").expect("q").expect("v");
        assert_eq!(cnt3, 3, "Should have 3 groups after deleting EU");
    }

    /// Test that INTEGER GROUP BY columns are preserved (not cast to TEXT).
    #[pg_test]
    fn pg_test_integer_group_by_type_preservation() {
        Spi::run("CREATE TABLE tr_type_src (id SERIAL, bucket_id INTEGER NOT NULL, val NUMERIC NOT NULL)").expect("create");
        Spi::run("INSERT INTO tr_type_src (bucket_id, val) SELECT i % 5, i::numeric FROM generate_series(1, 50) i").expect("seed");

        Spi::run(
            "SELECT create_reflex_ivm('tr_type_test', \
             'SELECT bucket_id, SUM(val) AS total, COUNT(*) AS cnt FROM tr_type_src GROUP BY bucket_id')"
        ).expect("create imv");

        // Check the column type in the target table — should preserve INTEGER
        let col_type = Spi::get_one::<String>(
            "SELECT data_type::text FROM information_schema.columns \
             WHERE table_name = 'tr_type_test' AND column_name = 'bucket_id'"
        ).expect("q").expect("v");
        assert_eq!(col_type, "integer", "bucket_id should be INTEGER, not TEXT");

        // Regardless of type, correctness should hold
        Spi::run("INSERT INTO tr_type_src (bucket_id, val) VALUES (0, 999), (5, 111)").expect("insert");

        let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_type_test").expect("q").expect("v");
        assert_eq!(cnt, 6, "Should have 6 groups (0-4 original + 5 new)");

        // Full correctness check using text cast to handle both cases
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM ( \
                SELECT r.bucket_id FROM tr_type_test r \
                FULL OUTER JOIN (SELECT bucket_id, SUM(val) AS total FROM tr_type_src GROUP BY bucket_id) d \
                    ON r.bucket_id::text = d.bucket_id::text \
                WHERE r.total IS DISTINCT FROM d.total \
            ) x"
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "IMV should match direct query");
    }

    /// Test correctness with higher cardinality (10K rows, 1K groups).
    #[pg_test]
    fn pg_test_high_cardinality_correctness() {
        Spi::run("CREATE TABLE tr_hc_src (id SERIAL, grp INT NOT NULL, val NUMERIC NOT NULL)").expect("create");
        Spi::run(
            "INSERT INTO tr_hc_src (grp, val) \
             SELECT i % 1000, ROUND((random() * 100)::numeric, 2) FROM generate_series(1, 10000) i"
        ).expect("seed");

        Spi::run(
            "SELECT create_reflex_ivm('tr_hc_test', \
             'SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM tr_hc_src GROUP BY grp')"
        ).expect("create imv");

        let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_hc_test").expect("q").expect("v");
        assert_eq!(cnt, 1000);

        // INSERT 500 rows (some new groups 1000..1049, some existing)
        Spi::run(
            "INSERT INTO tr_hc_src (grp, val) \
             SELECT CASE WHEN i <= 450 THEN i % 500 ELSE 999 + i - 449 END, 10.0 \
             FROM generate_series(1, 500) i"
        ).expect("insert");

        // DELETE 200 rows from known ids
        Spi::run("DELETE FROM tr_hc_src WHERE id <= 200").expect("delete");

        // UPDATE 100 rows (change amounts)
        Spi::run("UPDATE tr_hc_src SET val = val + 1 WHERE id > 200 AND id <= 300").expect("update");

        // Full correctness verification
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM ( \
                SELECT r.grp FROM tr_hc_test r \
                FULL OUTER JOIN (SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM tr_hc_src GROUP BY grp) d \
                    ON r.grp::text = d.grp::text \
                WHERE r.total IS DISTINCT FROM d.total OR r.cnt IS DISTINCT FROM d.cnt \
            ) x"
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "IMV should match direct query after INSERT+DELETE+UPDATE");

        // Verify group count makes sense
        let final_cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM tr_hc_test").expect("q").expect("v");
        let expected_cnt = Spi::get_one::<i64>(
            "SELECT COUNT(DISTINCT grp) FROM tr_hc_src"
        ).expect("q").expect("v");
        assert_eq!(final_cnt, expected_cnt, "Group count should match source distinct count");
    }

    // ---- Phase 1 & 2: Error handling and validation tests ----

    #[pg_test]
    fn test_malformed_sql_returns_error() {
        let result = crate::create_reflex_ivm("bad_sql_view", "SELEC broken garbage !!!", None, None, None);
        assert!(
            result.starts_with("ERROR"),
            "Malformed SQL should return error, got: {}",
            result
        );
        assert!(result.contains("parse"), "Error should mention parse failure");
    }

    #[pg_test]
    fn test_special_chars_view_name_rejected() {
        Spi::run("CREATE TABLE vn_src (id SERIAL, val INT)").expect("create table");
        let r1 = crate::create_reflex_ivm("bad'name", "SELECT val FROM vn_src", None, None, None);
        assert!(r1.starts_with("ERROR"), "Single quote should be rejected");
        let r2 = crate::create_reflex_ivm("bad;name", "SELECT val FROM vn_src", None, None, None);
        assert!(r2.starts_with("ERROR"), "Semicolon should be rejected");
        let r3 = crate::create_reflex_ivm("bad--name", "SELECT val FROM vn_src", None, None, None);
        assert!(r3.starts_with("ERROR"), "SQL comment should be rejected");
        let r4 = crate::create_reflex_ivm("bad name", "SELECT val FROM vn_src", None, None, None);
        assert!(r4.starts_with("ERROR"), "Whitespace should be rejected");
        let r5 = crate::create_reflex_ivm("", "SELECT val FROM vn_src", None, None, None);
        assert!(r5.starts_with("ERROR"), "Empty name should be rejected");
    }

    #[pg_test]
    fn test_drop_nonexistent_imv() {
        let result = crate::drop_reflex_ivm("nonexistent_view_xyz");
        assert!(result.starts_with("ERROR"), "Should error on non-existent IMV");
    }

    #[pg_test]
    fn test_validate_view_name_unit() {
        // Valid names
        assert!(crate::validate_view_name("my_view").is_ok());
        assert!(crate::validate_view_name("schema1.my_view").is_ok());
        assert!(crate::validate_view_name("_private").is_ok());
        assert!(crate::validate_view_name("View123").is_ok());
        // Invalid names
        assert!(crate::validate_view_name("").is_err());
        assert!(crate::validate_view_name("bad'name").is_err());
        assert!(crate::validate_view_name("bad\"name").is_err());
        assert!(crate::validate_view_name("bad;name").is_err());
        assert!(crate::validate_view_name("bad name").is_err());
        assert!(crate::validate_view_name("bad\\name").is_err());
        assert!(crate::validate_view_name("1starts_with_digit").is_err());
        assert!(crate::validate_view_name(".starts_with_dot").is_err());
        assert!(crate::validate_view_name("bad..double").is_err());
        assert!(crate::validate_view_name("ends_with_dot.").is_err());
    }

    // ---- Phase 4: Edge case tests ----

    #[pg_test]
    fn test_duplicate_view_name() {
        Spi::run("CREATE TABLE dup_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
        Spi::run("INSERT INTO dup_src (grp, val) VALUES ('a', 1)").expect("seed");
        let r1 = crate::create_reflex_ivm(
            "dup_view",
            "SELECT grp, SUM(val) AS total FROM dup_src GROUP BY grp",
            None,
            None,
            None,
        );
        assert_eq!(r1, "CREATE REFLEX INCREMENTAL VIEW");
        let r2 = crate::create_reflex_ivm(
            "dup_view",
            "SELECT grp, SUM(val) AS total FROM dup_src GROUP BY grp",
            None,
            None,
            None,
        );
        assert!(
            r2.starts_with("ERROR"),
            "Duplicate view name should return error, got: {}",
            r2
        );
    }

    #[pg_test]
    fn test_empty_source_table() {
        Spi::run("CREATE TABLE empty_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
        let result = crate::create_reflex_ivm(
            "empty_view",
            "SELECT grp, SUM(val) AS total FROM empty_src GROUP BY grp",
            None,
            None,
            None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM empty_view").expect("q").expect("v");
        assert_eq!(count, 0, "Empty source should produce empty view");
        // Now insert and verify trigger works
        Spi::run("INSERT INTO empty_src (grp, val) VALUES ('x', 42)").expect("insert");
        let total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM empty_view WHERE grp = 'x'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(total.to_string(), "42");
    }

    #[pg_test]
    fn test_update_group_by_column() {
        Spi::run(
            "CREATE TABLE grpmove_src (id SERIAL, grp TEXT, val NUMERIC)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO grpmove_src (grp, val) VALUES ('A', 10), ('A', 20), ('B', 30)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "grpmove_view",
            "SELECT grp, SUM(val) AS total FROM grpmove_src GROUP BY grp",
            None,
            None,
            None,
        );
        // Move a row from group A to group B
        Spi::run("UPDATE grpmove_src SET grp = 'B' WHERE val = 10").expect("update");
        let a = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM grpmove_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(a.to_string(), "20", "Group A should have lost 10");
        let b = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM grpmove_view WHERE grp = 'B'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(b.to_string(), "40", "Group B should have gained 10");
    }

    #[pg_test]
    fn test_min_max_delete_recompute() {
        Spi::run("CREATE TABLE mmr_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
        Spi::run("INSERT INTO mmr_src (grp, val) VALUES ('X', 10), ('X', 20), ('X', 30)")
            .expect("seed");
        crate::create_reflex_ivm(
            "mmr_view",
            "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mmr_src GROUP BY grp",
            None,
            None,
            None,
        );
        let lo =
            Spi::get_one::<pgrx::AnyNumeric>("SELECT lo FROM mmr_view WHERE grp = 'X'")
                .expect("q")
                .expect("v");
        assert_eq!(lo.to_string(), "10", "Initial MIN should be 10");
        // Delete the MIN row — should trigger recompute
        Spi::run("DELETE FROM mmr_src WHERE val = 10").expect("delete min");
        let lo2 =
            Spi::get_one::<pgrx::AnyNumeric>("SELECT lo FROM mmr_view WHERE grp = 'X'")
                .expect("q")
                .expect("v");
        assert_eq!(lo2.to_string(), "20", "After deleting 10, MIN should be 20");
    }

    #[pg_test]
    fn test_delete_all_rows_from_source() {
        Spi::run("CREATE TABLE delall_src (id SERIAL, grp TEXT, val NUMERIC)").expect("create table");
        Spi::run("INSERT INTO delall_src (grp, val) VALUES ('A', 10), ('B', 20)").expect("seed");
        crate::create_reflex_ivm(
            "delall_view",
            "SELECT grp, SUM(val) AS total FROM delall_src GROUP BY grp",
            None,
            None,
            None,
        );
        Spi::run("DELETE FROM delall_src").expect("delete all");
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM delall_view").expect("q").expect("v");
        assert_eq!(count, 0, "View should be empty after deleting all source rows");
    }

    #[pg_test]
    fn test_reconcile_aggregate() {
        Spi::run(
            "CREATE TABLE recon_agg_src (id SERIAL, grp TEXT, val NUMERIC)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO recon_agg_src (grp, val) VALUES ('A', 10), ('A', 20), ('B', 30)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "recon_agg_view",
            "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM recon_agg_src GROUP BY grp",
            None,
            None,
            None,
        );
        // Corrupt intermediate table
        Spi::run(
            "UPDATE __reflex_intermediate_recon_agg_view SET \"__sum_val\" = 999 WHERE \"grp\" = 'A'",
        )
        .expect("corrupt intermediate");
        // Reconcile should fix it
        let result = crate::reflex_reconcile("recon_agg_view");
        assert_eq!(result, "RECONCILED");
        let a = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM recon_agg_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(a.to_string(), "30", "After reconcile, SUM should be 10+20=30");
    }

    // ---- Round 2: Boundary condition tests ----

    #[pg_test]
    fn test_null_in_aggregate_expression() {
        Spi::run(
            "CREATE TABLE null_agg_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO null_agg_src (grp, val) VALUES ('A', 10), ('A', NULL), ('A', 30)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "null_agg_view",
            "SELECT grp, SUM(val) AS total, COUNT(val) AS cnt FROM null_agg_src GROUP BY grp",
            None,
            None,
            None,
        );
        // SUM should ignore NULL: 10 + 30 = 40
        let total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM null_agg_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(total.to_string(), "40", "SUM should ignore NULLs");
        // COUNT(val) should skip NULL: 2
        let cnt = Spi::get_one::<i64>(
            "SELECT cnt FROM null_agg_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(cnt, 2, "COUNT(col) should skip NULLs");
    }

    #[pg_test]
    fn test_count_col_vs_count_star() {
        Spi::run(
            "CREATE TABLE ccvs_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO ccvs_src (grp, val) VALUES ('X', 1), ('X', NULL), ('X', 3), ('X', NULL)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "ccvs_view",
            "SELECT grp, COUNT(*) AS cnt_star, COUNT(val) AS cnt_val FROM ccvs_src GROUP BY grp",
            None,
            None,
            None,
        );
        let cnt_star = Spi::get_one::<i64>(
            "SELECT cnt_star FROM ccvs_view WHERE grp = 'X'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(cnt_star, 4, "COUNT(*) should count all rows including NULLs");
        let cnt_val = Spi::get_one::<i64>(
            "SELECT cnt_val FROM ccvs_view WHERE grp = 'X'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(cnt_val, 2, "COUNT(col) should skip NULLs");
    }

    #[pg_test]
    fn test_distinct_with_group_by() {
        Spi::run(
            "CREATE TABLE dg_src (id SERIAL, grp TEXT NOT NULL, val TEXT NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO dg_src (grp, val) VALUES \
             ('A', 'x'), ('A', 'x'), ('A', 'y'), ('B', 'x'), ('B', 'x')",
        )
        .expect("seed");
        let result = crate::create_reflex_ivm(
            "dg_view",
            "SELECT DISTINCT grp, val FROM dg_src",
            None,
            None,
            None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM dg_view").expect("q").expect("v");
        // DISTINCT (A,x), (A,y), (B,x) = 3 unique pairs
        assert_eq!(count, 3, "DISTINCT should eliminate duplicate (grp, val) pairs");
    }

    #[pg_test]
    fn test_schema_qualified_source() {
        Spi::run(
            "CREATE TABLE sq_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
        )
        .expect("create table");
        Spi::run("INSERT INTO sq_src (grp, val) VALUES ('A', 10), ('B', 20)").expect("seed");
        let result = crate::create_reflex_ivm(
            "sq_view",
            "SELECT grp, SUM(val) AS total FROM public.sq_src GROUP BY grp",
            None,
            None,
            None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");
        let total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM sq_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(total.to_string(), "10");
        // Trigger should work with schema-qualified source
        Spi::run("INSERT INTO sq_src (grp, val) VALUES ('A', 5)").expect("insert");
        let total2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM sq_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(total2.to_string(), "15");
    }

    #[pg_test]
    fn test_insert_zero_rows() {
        Spi::run(
            "CREATE TABLE zr_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
        )
        .expect("create table");
        Spi::run("INSERT INTO zr_src (grp, val) VALUES ('A', 10)").expect("seed");
        crate::create_reflex_ivm(
            "zr_view",
            "SELECT grp, SUM(val) AS total FROM zr_src GROUP BY grp",
            None,
            None,
            None,
        );
        // Insert zero rows (WHERE false) — trigger fires but no delta
        Spi::run("INSERT INTO zr_src (grp, val) SELECT 'B', 99 WHERE false").expect("empty insert");
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM zr_view").expect("q").expect("v");
        assert_eq!(count, 1, "Zero-row insert should not change view");
    }

    #[pg_test]
    fn test_update_value_only() {
        Spi::run(
            "CREATE TABLE uvo_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO uvo_src (grp, val) VALUES ('A', 10), ('A', 20)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "uvo_view",
            "SELECT grp, SUM(val) AS total FROM uvo_src GROUP BY grp",
            None,
            None,
            None,
        );
        // Update value, not group column
        Spi::run("UPDATE uvo_src SET val = 50 WHERE val = 10").expect("update");
        let total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM uvo_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(total.to_string(), "70", "SUM should be 50 + 20 = 70");
    }

    #[pg_test]
    fn test_multiple_deletes_same_group() {
        Spi::run(
            "CREATE TABLE md_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO md_src (grp, val) VALUES ('A', 10), ('A', 20), ('A', 30), ('A', 40)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "md_view",
            "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM md_src GROUP BY grp",
            None,
            None,
            None,
        );
        // Delete two rows separately
        Spi::run("DELETE FROM md_src WHERE val = 10").expect("delete 1");
        Spi::run("DELETE FROM md_src WHERE val = 30").expect("delete 2");
        let total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM md_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(total.to_string(), "60", "SUM should be 20 + 40 = 60");
        let cnt = Spi::get_one::<i64>(
            "SELECT cnt FROM md_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(cnt, 2, "COUNT should be 2 after deleting 2 of 4 rows");
    }

    #[pg_test]
    fn test_large_batch_correctness() {
        Spi::run(
            "CREATE TABLE lb_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
        )
        .expect("create table");
        // 10K rows across 100 groups
        Spi::run(
            "INSERT INTO lb_src (grp, val) \
             SELECT 'g' || (i % 100), i FROM generate_series(1, 10000) i",
        )
        .expect("seed 10K rows");
        crate::create_reflex_ivm(
            "lb_view",
            "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM lb_src GROUP BY grp",
            None,
            None,
            None,
        );
        // Compare IMV against direct query
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM ( \
                SELECT grp, total, cnt FROM lb_view \
                EXCEPT \
                SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM lb_src GROUP BY grp \
            ) x",
        )
        .expect("q")
        .expect("v");
        assert_eq!(mismatches, 0, "IMV should match direct query for 10K rows");
        // Insert another batch and re-verify
        Spi::run(
            "INSERT INTO lb_src (grp, val) \
             SELECT 'g' || (i % 100), i FROM generate_series(10001, 15000) i",
        )
        .expect("insert 5K more");
        let mismatches2 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM ( \
                SELECT grp, total, cnt FROM lb_view \
                EXCEPT \
                SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM lb_src GROUP BY grp \
            ) x",
        )
        .expect("q")
        .expect("v");
        assert_eq!(mismatches2, 0, "IMV should match after additional batch insert");
    }

    #[pg_test]
    fn test_where_clause_imv() {
        Spi::run(
            "CREATE TABLE wc_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL, active BOOLEAN NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO wc_src (grp, val, active) VALUES \
             ('A', 10, true), ('A', 20, false), ('B', 30, true), ('B', 40, true)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "wc_view",
            "SELECT grp, SUM(val) AS total FROM wc_src WHERE active = true GROUP BY grp",
            None,
            None,
            None,
        );
        let a = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM wc_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(a.to_string(), "10", "WHERE should filter out inactive row (val=20)");
        let b = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM wc_view WHERE grp = 'B'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(b.to_string(), "70", "Both B rows are active: 30 + 40 = 70");
    }

    #[pg_test]
    fn test_avg_with_all_same_values() {
        Spi::run(
            "CREATE TABLE avg_same_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO avg_same_src (grp, val) VALUES ('X', 42), ('X', 42), ('X', 42)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "avg_same_view",
            "SELECT grp, AVG(val) AS avg_val FROM avg_same_src GROUP BY grp",
            None,
            None,
            None,
        );
        let avg = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT avg_val FROM avg_same_view WHERE grp = 'X'",
        )
        .expect("q")
        .expect("v");
        // AVG of identical values should be that value (no precision loss)
        let avg_f: f64 = avg.to_string().parse().expect("parse avg");
        assert!(
            (avg_f - 42.0).abs() < 0.0001,
            "AVG of identical values should be exact, got {}",
            avg_f
        );
    }

    // ---- Schema support tests ----

    #[pg_test]
    fn test_schema_qualified_view_name() {
        Spi::run("CREATE SCHEMA IF NOT EXISTS test_schema").expect("create schema");
        Spi::run(
            "CREATE TABLE test_schema.sq_src2 (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
        )
        .expect("create table in schema");
        Spi::run("INSERT INTO test_schema.sq_src2 (grp, val) VALUES ('A', 10), ('B', 20)")
            .expect("seed");

        let result = crate::create_reflex_ivm(
            "test_schema.sq_view2",
            "SELECT grp, SUM(val) AS total FROM test_schema.sq_src2 GROUP BY grp",
            None,
            None,
            None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify table exists in test_schema and has correct data
        let total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM test_schema.sq_view2 WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(total.to_string(), "10");

        // Verify trigger fires for source table INSERTs
        Spi::run("INSERT INTO test_schema.sq_src2 (grp, val) VALUES ('A', 5)").expect("insert");
        let total2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM test_schema.sq_view2 WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(total2.to_string(), "15");

        // Verify drop works
        let drop_result = crate::drop_reflex_ivm("test_schema.sq_view2");
        assert_eq!(drop_result, "DROP REFLEX INCREMENTAL VIEW");
    }

    // ---- Multi-level cascade test ----

    #[pg_test]
    fn test_multi_level_cascade_propagation() {
        // Base table
        Spi::run(
            "CREATE TABLE mlc_src (id SERIAL, region TEXT NOT NULL, amount NUMERIC NOT NULL)",
        )
        .expect("create base table");
        Spi::run(
            "INSERT INTO mlc_src (region, amount) VALUES ('US', 100), ('US', 200), ('EU', 150)",
        )
        .expect("seed");

        // L1: aggregates base by region
        let r1 = crate::create_reflex_ivm(
            "mlc_l1",
            "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM mlc_src GROUP BY region",
            None,
            None,
            None,
        );
        assert_eq!(r1, "CREATE REFLEX INCREMENTAL VIEW");

        // L2: aggregates L1 (re-aggregates totals — tests cascade)
        let r2 = crate::create_reflex_ivm(
            "mlc_l2",
            "SELECT region, SUM(total) AS grand_total FROM mlc_l1 GROUP BY region",
            None,
            None,
            None,
        );
        assert_eq!(r2, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify initial state
        let l1_us = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM mlc_l1 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l1_us.to_string(), "300", "L1 US = 100 + 200");

        let l2_us = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM mlc_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l2_us.to_string(), "300", "L2 US = L1 US = 300");

        // INSERT into base → both L1 and L2 should update
        Spi::run("INSERT INTO mlc_src (region, amount) VALUES ('US', 50)").expect("insert");

        let l1_after_ins = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM mlc_l1 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l1_after_ins.to_string(), "350", "L1 US after insert = 350");

        let l2_after_ins = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM mlc_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l2_after_ins.to_string(), "350", "L2 US should cascade from L1");

        // UPDATE base (change amount) → both levels should reflect
        Spi::run("UPDATE mlc_src SET amount = 500 WHERE amount = 200").expect("update");

        let l1_after_upd = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM mlc_l1 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l1_after_upd.to_string(), "650", "L1 US after update = 100 + 500 + 50");

        let l2_after_upd = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM mlc_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l2_after_upd.to_string(), "650", "L2 US should cascade update");

        // DELETE from base → both levels update
        Spi::run("DELETE FROM mlc_src WHERE amount = 100").expect("delete");

        let l1_after_del = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM mlc_l1 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l1_after_del.to_string(), "550", "L1 US after delete = 500 + 50");

        let l2_after_del = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM mlc_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l2_after_del.to_string(), "550", "L2 US should cascade delete");

        // DELETE all US rows → US group should disappear from both levels
        Spi::run("DELETE FROM mlc_src WHERE region = 'US'").expect("delete all US");

        let l1_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM mlc_l1 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l1_count, 0, "L1 US group should be gone");

        let l2_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM mlc_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(l2_count, 0, "L2 US group should cascade-disappear");

        // EU should still be intact at both levels
        let l2_eu = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM mlc_l2 WHERE region = 'EU'",
        ).expect("q").expect("v");
        assert_eq!(l2_eu.to_string(), "150", "L2 EU untouched");
    }

    // ---- Incremental passthrough DELETE/UPDATE tests ----

    #[pg_test]
    fn test_passthrough_incremental_delete() {
        Spi::run(
            "CREATE TABLE pt_del_src (id SERIAL PRIMARY KEY, region TEXT NOT NULL, val INT NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO pt_del_src (region, val) VALUES ('A', 1), ('A', 2), ('B', 3), ('B', 4), ('C', 5)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "pt_del_view",
            "SELECT id, region, val FROM pt_del_src",
            None,
            None,
            None,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_del_view").expect("q").expect("v"),
            5,
            "Initial view should have 5 rows"
        );

        // Delete 2 specific rows
        Spi::run("DELETE FROM pt_del_src WHERE id IN (2, 4)").expect("delete");

        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_del_view").expect("q").expect("v");
        assert_eq!(count, 3, "View should have 3 rows after deleting 2");

        // Verify exact content matches source
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, region, val FROM pt_del_view
                EXCEPT
                SELECT id, region, val FROM pt_del_src
            ) x",
        )
        .expect("q")
        .expect("v");
        assert_eq!(mismatches, 0, "View should exactly match source after delete");
    }

    #[pg_test]
    fn test_passthrough_incremental_update() {
        Spi::run(
            "CREATE TABLE pt_upd_src (id SERIAL PRIMARY KEY, region TEXT NOT NULL, val INT NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO pt_upd_src (region, val) VALUES ('A', 10), ('B', 20), ('C', 30)",
        )
        .expect("seed");
        crate::create_reflex_ivm(
            "pt_upd_view",
            "SELECT id, region, val FROM pt_upd_src",
            None,
            None,
            None,
        );

        // Update a value
        Spi::run("UPDATE pt_upd_src SET val = 99 WHERE region = 'B'").expect("update");

        let val = Spi::get_one::<i32>(
            "SELECT val FROM pt_upd_view WHERE region = 'B'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(val, 99, "Updated value should propagate to view");

        // Update region (changes a different column)
        Spi::run("UPDATE pt_upd_src SET region = 'D' WHERE val = 99").expect("update region");

        let count_b =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_upd_view WHERE region = 'B'")
                .expect("q")
                .expect("v");
        assert_eq!(count_b, 0, "Old region B should be gone from view");

        let count_d =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pt_upd_view WHERE region = 'D'")
                .expect("q")
                .expect("v");
        assert_eq!(count_d, 1, "New region D should appear in view");

        // Full content check
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, region, val FROM pt_upd_view
                EXCEPT
                SELECT id, region, val FROM pt_upd_src
            ) x",
        )
        .expect("q")
        .expect("v");
        assert_eq!(mismatches, 0, "View should exactly match source after updates");
    }

    #[pg_test]
    fn test_passthrough_join_delete_secondary_table() {
        // Setup: two source tables with a JOIN
        Spi::run(
            "CREATE TABLE ptj_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
        )
        .expect("create products");
        Spi::run(
            "CREATE TABLE ptj_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount NUMERIC NOT NULL)",
        )
        .expect("create sales");
        Spi::run(
            "INSERT INTO ptj_products (id, name) VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey')",
        )
        .expect("seed products");
        Spi::run(
            "INSERT INTO ptj_sales (product_id, amount) VALUES (1, 100), (1, 200), (2, 300), (3, 50)",
        )
        .expect("seed sales");

        // Create passthrough JOIN IMV with explicit unique key (id comes from ptj_sales)
        let result = crate::create_reflex_ivm(
            "ptj_view",
            "SELECT s.id, s.product_id, s.amount, p.name FROM ptj_sales s JOIN ptj_products p ON s.product_id = p.id",
            Some("id"),
            None,
            None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM ptj_view")
            .expect("q").expect("v");
        assert_eq!(count, 4, "Initial view should have 4 rows");

        // DELETE from the SECONDARY table (products) — this is the critical test
        // Deleting product 2 should remove all sales rows referencing it
        Spi::run("DELETE FROM ptj_products WHERE id = 2").expect("delete product");

        let count_after = Spi::get_one::<i64>("SELECT COUNT(*) FROM ptj_view")
            .expect("q").expect("v");
        assert_eq!(count_after, 3, "View should have 3 rows after deleting product 2");

        // Verify no rows reference the deleted product
        let orphans = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM ptj_view WHERE product_id = 2",
        )
        .expect("q").expect("v");
        assert_eq!(orphans, 0, "No rows should reference deleted product");

        // Verify remaining data is correct
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, product_id, amount, name FROM ptj_view
                EXCEPT
                SELECT s.id, s.product_id, s.amount, p.name
                FROM ptj_sales s JOIN ptj_products p ON s.product_id = p.id
            ) x",
        )
        .expect("q").expect("v");
        assert_eq!(mismatches, 0, "View should exactly match source after delete");
    }

    #[pg_test]
    fn test_passthrough_join_update_secondary_table() {
        Spi::run(
            "CREATE TABLE ptju_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
        )
        .expect("create products");
        Spi::run(
            "CREATE TABLE ptju_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, qty INT NOT NULL)",
        )
        .expect("create sales");
        Spi::run("INSERT INTO ptju_products VALUES (1, 'Alpha'), (2, 'Beta')").expect("seed products");
        Spi::run("INSERT INTO ptju_sales (product_id, qty) VALUES (1, 10), (2, 20)").expect("seed sales");

        crate::create_reflex_ivm(
            "ptju_view",
            "SELECT s.id, s.qty, p.name FROM ptju_sales s JOIN ptju_products p ON s.product_id = p.id",
            Some("id"),
            None,
            None,
        );

        // UPDATE the secondary table (product name change)
        Spi::run("UPDATE ptju_products SET name = 'Alpha-v2' WHERE id = 1").expect("update product");

        // The view should reflect the updated product name
        let name = Spi::get_one::<String>(
            "SELECT name FROM ptju_view WHERE id = 1",
        )
        .expect("q").expect("v");
        assert_eq!(name, "Alpha-v2", "View should reflect updated product name");
    }

    /// JOIN passthrough with no explicit key: DELETE on secondary table should fall back
    /// to full refresh and still produce correct results.
    #[pg_test]
    fn test_passthrough_join_no_key_delete_secondary() {
        Spi::run("CREATE TABLE ptjnk_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create products");
        Spi::run("CREATE TABLE ptjnk_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount INT NOT NULL)")
            .expect("create sales");
        Spi::run("INSERT INTO ptjnk_products VALUES (1, 'A'), (2, 'B'), (3, 'C')").expect("seed products");
        Spi::run("INSERT INTO ptjnk_sales (product_id, amount) VALUES (1, 10), (2, 20), (3, 30)").expect("seed sales");

        // No explicit key → JOIN triggers fall back to full refresh
        crate::create_reflex_ivm(
            "ptjnk_view",
            "SELECT s.id, s.amount, p.name FROM ptjnk_sales s JOIN ptjnk_products p ON s.product_id = p.id",
            None,
            None,
            None,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjnk_view").expect("q").expect("v"),
            3
        );

        // DELETE from secondary table → full refresh should still be correct
        Spi::run("DELETE FROM ptjnk_products WHERE id = 2").expect("delete product");
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjnk_view").expect("q").expect("v");
        assert_eq!(count, 2, "Full refresh should remove orphaned rows");

        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, amount, name FROM ptjnk_view
                EXCEPT
                SELECT s.id, s.amount, p.name FROM ptjnk_sales s JOIN ptjnk_products p ON s.product_id = p.id
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "View should exactly match source");
    }

    /// JOIN passthrough with explicit key: DELETE on the key-owner table should use
    /// direct key extraction (fast path, no JOINs).
    #[pg_test]
    fn test_passthrough_join_delete_key_owner_table() {
        Spi::run("CREATE TABLE ptjko_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create products");
        Spi::run("CREATE TABLE ptjko_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount INT NOT NULL)")
            .expect("create sales");
        Spi::run("INSERT INTO ptjko_products VALUES (1, 'A'), (2, 'B')").expect("seed products");
        Spi::run("INSERT INTO ptjko_sales (product_id, amount) VALUES (1, 10), (1, 20), (2, 30)")
            .expect("seed sales");

        crate::create_reflex_ivm(
            "ptjko_view",
            "SELECT s.id, s.product_id, s.amount, p.name FROM ptjko_sales s JOIN ptjko_products p ON s.product_id = p.id",
            Some("id"),
            None,
            None,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjko_view").expect("q").expect("v"),
            3
        );

        // DELETE from key-owner table (sales) → direct key extraction
        Spi::run("DELETE FROM ptjko_sales WHERE id = 2").expect("delete sale");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjko_view").expect("q").expect("v"),
            2,
            "Should remove exactly 1 row"
        );

        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, product_id, amount, name FROM ptjko_view
                EXCEPT
                SELECT s.id, s.product_id, s.amount, p.name FROM ptjko_sales s JOIN ptjko_products p ON s.product_id = p.id
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0);
    }

    /// 3-table JOIN passthrough: verify DELETE on each table produces correct results.
    #[pg_test]
    fn test_passthrough_three_table_join() {
        Spi::run("CREATE TABLE pt3_regions (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create regions");
        Spi::run("CREATE TABLE pt3_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create products");
        Spi::run("CREATE TABLE pt3_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, region_id INT NOT NULL, qty INT NOT NULL)")
            .expect("create sales");
        Spi::run("INSERT INTO pt3_regions VALUES (1, 'North'), (2, 'South')").expect("seed regions");
        Spi::run("INSERT INTO pt3_products VALUES (1, 'Widget'), (2, 'Gadget')").expect("seed products");
        Spi::run("INSERT INTO pt3_sales (product_id, region_id, qty) VALUES (1,1,10), (1,2,20), (2,1,30), (2,2,40)")
            .expect("seed sales");

        crate::create_reflex_ivm(
            "pt3_view",
            "SELECT s.id, s.qty, p.name AS product_name, r.name AS region_name \
             FROM pt3_sales s \
             JOIN pt3_products p ON s.product_id = p.id \
             JOIN pt3_regions r ON s.region_id = r.id",
            Some("id"),
            None,
            None,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pt3_view").expect("q").expect("v"),
            4
        );

        // DELETE from 2nd secondary table (regions)
        Spi::run("DELETE FROM pt3_regions WHERE id = 2").expect("delete region");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pt3_view").expect("q").expect("v"),
            2,
            "Should remove 2 rows (both sales in South region)"
        );

        // DELETE from 1st secondary table (products)
        Spi::run("DELETE FROM pt3_products WHERE id = 1").expect("delete product");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pt3_view").expect("q").expect("v"),
            1,
            "Should remove 1 more row (Widget in North)"
        );

        // Verify exact match with source
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, qty, product_name, region_name FROM pt3_view
                EXCEPT
                SELECT s.id, s.qty, p.name, r.name FROM pt3_sales s
                    JOIN pt3_products p ON s.product_id = p.id
                    JOIN pt3_regions r ON s.region_id = r.id
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "View should exactly match 3-table JOIN");
    }

    /// JOIN passthrough with composite key: multiple key columns from the key-owner table.
    #[pg_test]
    fn test_passthrough_join_composite_key() {
        Spi::run("CREATE TABLE ptck_dims (id SERIAL PRIMARY KEY, label TEXT NOT NULL)")
            .expect("create dims");
        Spi::run(
            "CREATE TABLE ptck_facts (product_id INT NOT NULL, region_id INT NOT NULL, dim_id INT NOT NULL, val INT NOT NULL, \
             PRIMARY KEY (product_id, region_id))",
        ).expect("create facts");
        Spi::run("INSERT INTO ptck_dims VALUES (1, 'X'), (2, 'Y')").expect("seed dims");
        Spi::run(
            "INSERT INTO ptck_facts VALUES (1,1,1,10), (1,2,1,20), (2,1,2,30), (2,2,2,40)",
        ).expect("seed facts");

        crate::create_reflex_ivm(
            "ptck_view",
            "SELECT f.product_id, f.region_id, f.val, d.label \
             FROM ptck_facts f JOIN ptck_dims d ON f.dim_id = d.id",
            Some("product_id, region_id"),
            None,
            None,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptck_view").expect("q").expect("v"),
            4
        );

        // DELETE from key-owner table using composite key
        Spi::run("DELETE FROM ptck_facts WHERE product_id = 1 AND region_id = 2").expect("delete");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptck_view").expect("q").expect("v"),
            3
        );

        // DELETE from secondary table
        Spi::run("DELETE FROM ptck_dims WHERE id = 2").expect("delete dim");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptck_view").expect("q").expect("v"),
            1,
            "Should remove both rows referencing dim 2"
        );

        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT product_id, region_id, val, label FROM ptck_view
                EXCEPT
                SELECT f.product_id, f.region_id, f.val, d.label
                FROM ptck_facts f JOIN ptck_dims d ON f.dim_id = d.id
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0);
    }

    /// JOIN passthrough with aliased key column: target uses alias, source uses original name.
    #[pg_test]
    fn test_passthrough_join_aliased_key() {
        Spi::run("CREATE TABLE ptak_cats (id SERIAL PRIMARY KEY, cat_name TEXT NOT NULL)")
            .expect("create cats");
        Spi::run(
            "CREATE TABLE ptak_items (item_id SERIAL PRIMARY KEY, cat_id INT NOT NULL, price INT NOT NULL)",
        ).expect("create items");
        Spi::run("INSERT INTO ptak_cats VALUES (1, 'Electronics'), (2, 'Books')").expect("seed cats");
        Spi::run("INSERT INTO ptak_items (cat_id, price) VALUES (1, 100), (1, 200), (2, 50)")
            .expect("seed items");

        crate::create_reflex_ivm(
            "ptak_view",
            "SELECT i.item_id AS id, i.price, c.cat_name AS category \
             FROM ptak_items i JOIN ptak_cats c ON i.cat_id = c.id",
            Some("id"),
            None,
            None,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptak_view").expect("q").expect("v"),
            3
        );

        // DELETE from secondary table (cats) — mapping should resolve cat_id→id
        Spi::run("DELETE FROM ptak_cats WHERE id = 1").expect("delete cat");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptak_view").expect("q").expect("v"),
            1,
            "Should remove 2 electronics items"
        );

        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, price, category FROM ptak_view
                EXCEPT
                SELECT i.item_id, i.price, c.cat_name FROM ptak_items i JOIN ptak_cats c ON i.cat_id = c.id
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0);
    }

    /// INSERT on secondary table in a JOIN passthrough should add rows correctly.
    #[pg_test]
    fn test_passthrough_join_insert_secondary() {
        Spi::run("CREATE TABLE ptjis_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create products");
        Spi::run("CREATE TABLE ptjis_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount INT NOT NULL)")
            .expect("create sales");
        Spi::run("INSERT INTO ptjis_products VALUES (1, 'Alpha')").expect("seed products");
        Spi::run("INSERT INTO ptjis_sales (product_id, amount) VALUES (1, 100)").expect("seed sales");

        crate::create_reflex_ivm(
            "ptjis_view",
            "SELECT s.id, s.amount, p.name FROM ptjis_sales s JOIN ptjis_products p ON s.product_id = p.id",
            Some("id"),
            None,
            None,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjis_view").expect("q").expect("v"),
            1
        );

        // INSERT a new product — no new sales reference it, so view should not change
        Spi::run("INSERT INTO ptjis_products VALUES (2, 'Beta')").expect("insert product");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjis_view").expect("q").expect("v"),
            1,
            "New product with no sales should not affect view"
        );

        // Now add a sale referencing the new product
        Spi::run("INSERT INTO ptjis_sales (product_id, amount) VALUES (2, 200)").expect("insert sale");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ptjis_view").expect("q").expect("v"),
            2,
            "New sale should appear in view"
        );

        let name = Spi::get_one::<String>("SELECT name FROM ptjis_view WHERE amount = 200")
            .expect("q").expect("v");
        assert_eq!(name, "Beta");
    }

    // =====================================================================
    // Chained IMV tests with passthrough layers
    // =====================================================================

    /// Chain: source → passthrough L1 → aggregate L2
    /// Tests that DML on the source propagates through a passthrough layer
    /// into an aggregate layer, with full correctness checks.
    #[pg_test]
    fn test_chain_passthrough_then_aggregate() {
        Spi::run(
            "CREATE TABLE cpta_src (id SERIAL PRIMARY KEY, region TEXT NOT NULL, amount INT NOT NULL, active BOOLEAN NOT NULL)",
        ).expect("create table");
        Spi::run(
            "INSERT INTO cpta_src (region, amount, active) VALUES \
             ('US', 100, true), ('US', 200, true), ('US', 50, false), \
             ('EU', 300, true), ('EU', 150, false)",
        ).expect("seed");

        // L1: passthrough with WHERE filter (only active rows)
        crate::create_reflex_ivm(
            "cpta_l1",
            "SELECT id, region, amount FROM cpta_src WHERE active = true",
            None,
            None,
            None,
        );

        // L2: aggregate on L1 (SUM by region)
        crate::create_reflex_ivm(
            "cpta_l2",
            "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM cpta_l1 GROUP BY region",
            None,
            None,
            None,
        );

        // Verify initial state
        let us_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpta_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us_total.to_string(), "300"); // 100+200 (active only)

        let eu_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpta_l2 WHERE region = 'EU'",
        ).expect("q").expect("v");
        assert_eq!(eu_total.to_string(), "300"); // 300 (active only)

        // INSERT active row → propagates through L1 passthrough → L2 aggregate updates
        Spi::run("INSERT INTO cpta_src (region, amount, active) VALUES ('US', 400, true)")
            .expect("insert active");
        let us_after_ins = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpta_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us_after_ins.to_string(), "700"); // 100+200+400

        // INSERT inactive row → appears in source but NOT in L1 or L2
        Spi::run("INSERT INTO cpta_src (region, amount, active) VALUES ('US', 999, false)")
            .expect("insert inactive");
        let us_after_inactive = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpta_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us_after_inactive.to_string(), "700"); // unchanged

        // DELETE an active row → cascades through both levels
        Spi::run("DELETE FROM cpta_src WHERE amount = 100").expect("delete");
        let us_after_del = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpta_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us_after_del.to_string(), "600"); // 200+400

        // UPDATE a row to change region → moves between groups at L2
        Spi::run("UPDATE cpta_src SET region = 'EU' WHERE amount = 200").expect("update");
        let us_after_upd = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpta_l2 WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us_after_upd.to_string(), "400"); // only 400 left in US

        let eu_after_upd = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpta_l2 WHERE region = 'EU'",
        ).expect("q").expect("v");
        assert_eq!(eu_after_upd.to_string(), "500"); // 300+200

        // Verify L1 matches source exactly
        let l1_mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, region, amount FROM cpta_l1
                EXCEPT
                SELECT id, region, amount FROM cpta_src WHERE active = true
            ) x",
        ).expect("q").expect("v");
        assert_eq!(l1_mismatches, 0, "L1 should exactly match filtered source");

        // Verify L2 matches aggregate of filtered source
        let l2_mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT region, total, cnt FROM cpta_l2
                EXCEPT
                SELECT region, SUM(amount), COUNT(*)
                FROM cpta_src WHERE active = true GROUP BY region
            ) x",
        ).expect("q").expect("v");
        assert_eq!(l2_mismatches, 0, "L2 should exactly match aggregate of filtered source");
    }

    /// Chain: source → aggregate L1 → passthrough L2
    /// Tests that an aggregate feeds into a passthrough that tracks all its rows.
    #[pg_test]
    fn test_chain_aggregate_then_passthrough() {
        Spi::run(
            "CREATE TABLE catp_src (id SERIAL, city TEXT NOT NULL, revenue INT NOT NULL)",
        ).expect("create table");
        Spi::run(
            "INSERT INTO catp_src (city, revenue) VALUES \
             ('Paris', 100), ('Paris', 200), ('London', 300), ('Berlin', 50)",
        ).expect("seed");

        // L1: aggregate (SUM by city)
        crate::create_reflex_ivm(
            "catp_l1",
            "SELECT city, SUM(revenue) AS total, COUNT(*) AS cnt FROM catp_src GROUP BY city",
            None,
            None,
            None,
        );

        // L2: passthrough of L1 (reads all rows from the aggregate target)
        crate::create_reflex_ivm(
            "catp_l2",
            "SELECT city, total, cnt FROM catp_l1",
            None,
            None,
            None,
        );

        // Verify initial state
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM catp_l2").expect("q").expect("v"),
            3, // Paris, London, Berlin
        );
        let paris = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM catp_l2 WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(paris.to_string(), "300");

        // INSERT → L1 updates → L2 passthrough picks up change
        Spi::run("INSERT INTO catp_src (city, revenue) VALUES ('Paris', 50)")
            .expect("insert");
        let paris_ins = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM catp_l2 WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(paris_ins.to_string(), "350");

        // INSERT new city → new group appears in L1 and L2
        Spi::run("INSERT INTO catp_src (city, revenue) VALUES ('Tokyo', 500)")
            .expect("insert new city");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM catp_l2").expect("q").expect("v"),
            4,
        );

        // DELETE all rows for a city → group disappears from L1 and L2
        Spi::run("DELETE FROM catp_src WHERE city = 'Berlin'").expect("delete group");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM catp_l2").expect("q").expect("v"),
            3, // Berlin gone
        );
        let berlin_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM catp_l2 WHERE city = 'Berlin'",
        ).expect("q").expect("v");
        assert_eq!(berlin_count, 0, "Berlin should not exist in L2");

        // UPDATE → value change propagates through both levels
        Spi::run("UPDATE catp_src SET revenue = 999 WHERE city = 'London'").expect("update");
        let london = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM catp_l2 WHERE city = 'London'",
        ).expect("q").expect("v");
        assert_eq!(london.to_string(), "999");

        // Verify L1 matches source
        let l1_mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT city, total, cnt FROM catp_l1
                EXCEPT
                SELECT city, SUM(revenue), COUNT(*) FROM catp_src GROUP BY city
            ) x",
        ).expect("q").expect("v");
        assert_eq!(l1_mismatches, 0, "L1 should exactly match aggregate of source");

        // Verify L2 matches L1 exactly
        let l2_mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT city, total, cnt FROM catp_l2
                EXCEPT
                SELECT city, total, cnt FROM catp_l1
            ) x",
        ).expect("q").expect("v");
        assert_eq!(l2_mismatches, 0, "L2 should exactly mirror L1");
    }

    /// Chain: source → passthrough L1 (JOIN) → aggregate L2
    /// The passthrough layer is a JOIN, so this tests cascade through a JOIN passthrough.
    #[pg_test]
    fn test_chain_passthrough_join_then_aggregate() {
        Spi::run("CREATE TABLE cpja_products (id SERIAL PRIMARY KEY, category TEXT NOT NULL)")
            .expect("create products");
        Spi::run("CREATE TABLE cpja_sales (id SERIAL PRIMARY KEY, product_id INT NOT NULL, amount INT NOT NULL)")
            .expect("create sales");
        Spi::run("INSERT INTO cpja_products VALUES (1, 'Electronics'), (2, 'Books'), (3, 'Food')")
            .expect("seed products");
        Spi::run(
            "INSERT INTO cpja_sales (product_id, amount) VALUES \
             (1, 100), (1, 200), (2, 50), (2, 150), (3, 75)",
        ).expect("seed sales");

        // L1: passthrough JOIN (denormalize sales with product category)
        crate::create_reflex_ivm(
            "cpja_l1",
            "SELECT s.id, s.amount, p.category \
             FROM cpja_sales s JOIN cpja_products p ON s.product_id = p.id",
            Some("id"),
            None,
            None,
        );

        // L2: aggregate on L1 (SUM by category)
        crate::create_reflex_ivm(
            "cpja_l2",
            "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM cpja_l1 GROUP BY category",
            None,
            None,
            None,
        );

        // Verify initial
        let elec = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpja_l2 WHERE category = 'Electronics'",
        ).expect("q").expect("v");
        assert_eq!(elec.to_string(), "300"); // 100+200

        // INSERT into sales → propagates through L1 JOIN → L2 aggregate
        Spi::run("INSERT INTO cpja_sales (product_id, amount) VALUES (2, 100)")
            .expect("insert sale");
        let books = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpja_l2 WHERE category = 'Books'",
        ).expect("q").expect("v");
        assert_eq!(books.to_string(), "300"); // 50+150+100

        // DELETE a product from secondary table → L1 removes rows → L2 group shrinks
        Spi::run("DELETE FROM cpja_products WHERE id = 3").expect("delete product");
        let food_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cpja_l2 WHERE category = 'Food'",
        ).expect("q").expect("v");
        assert_eq!(food_count, 0, "Food category should disappear from L2");

        // DELETE from sales (key-owner) → direct key extraction at L1 → cascades to L2
        Spi::run("DELETE FROM cpja_sales WHERE amount = 100 AND product_id = 1").expect("delete sale");
        let elec_after = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpja_l2 WHERE category = 'Electronics'",
        ).expect("q").expect("v");
        assert_eq!(elec_after.to_string(), "200"); // only 200 left

        // UPDATE product category → L1 updates → L2 groups shift
        Spi::run("UPDATE cpja_products SET category = 'Electronics' WHERE id = 2")
            .expect("update product category");
        let elec_final = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cpja_l2 WHERE category = 'Electronics'",
        ).expect("q").expect("v");
        assert_eq!(elec_final.to_string(), "500"); // 200 + 50+150+100

        // Verify L1 exactly matches direct JOIN
        let l1_mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, amount, category FROM cpja_l1
                EXCEPT
                SELECT s.id, s.amount, p.category
                FROM cpja_sales s JOIN cpja_products p ON s.product_id = p.id
            ) x",
        ).expect("q").expect("v");
        assert_eq!(l1_mismatches, 0, "L1 should exactly match the JOIN");

        // Verify L2 matches aggregate of L1
        let l2_mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT category, total, cnt FROM cpja_l2
                EXCEPT
                SELECT category, SUM(amount), COUNT(*) FROM cpja_l1 GROUP BY category
            ) x",
        ).expect("q").expect("v");
        assert_eq!(l2_mismatches, 0, "L2 should exactly match aggregate of L1");
    }

    // =====================================================================
    // Multiple IMVs on same source — diverse types
    // =====================================================================

    /// Multiple IMVs of different types (aggregate, passthrough, distinct) on the same
    /// source table. All must stay correct through INSERT, DELETE, and UPDATE.
    #[pg_test]
    fn test_multiple_mixed_imvs_on_same_source() {
        Spi::run(
            "CREATE TABLE mmis_src (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL, active BOOLEAN NOT NULL)",
        ).expect("create table");
        Spi::run(
            "INSERT INTO mmis_src (dept, salary, active) VALUES \
             ('Eng', 100, true), ('Eng', 200, true), ('Eng', 50, false), \
             ('Sales', 300, true), ('Sales', 150, false), \
             ('HR', 80, true)",
        ).expect("seed");

        // IMV1: aggregate — SUM salary by dept
        crate::create_reflex_ivm(
            "mmis_agg",
            "SELECT dept, SUM(salary) AS total, COUNT(*) AS cnt FROM mmis_src GROUP BY dept",
            None,
            None,
            None,
        );

        // IMV2: passthrough — all active employees
        crate::create_reflex_ivm(
            "mmis_active",
            "SELECT id, dept, salary FROM mmis_src WHERE active = true",
            None,
            None,
            None,
        );

        // IMV3: distinct — unique departments
        crate::create_reflex_ivm(
            "mmis_depts",
            "SELECT DISTINCT dept FROM mmis_src",
            None,
            None,
            None,
        );

        // IMV4: aggregate — AVG salary globally
        crate::create_reflex_ivm(
            "mmis_avg",
            "SELECT AVG(salary) AS avg_sal FROM mmis_src",
            None,
            None,
            None,
        );

        // Verify initial state
        let eng_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM mmis_agg WHERE dept = 'Eng'",
        ).expect("q").expect("v");
        assert_eq!(eng_total.to_string(), "350"); // 100+200+50

        let active_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM mmis_active",
        ).expect("q").expect("v");
        assert_eq!(active_count, 4); // 100,200,300,80

        let dept_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM mmis_depts",
        ).expect("q").expect("v");
        assert_eq!(dept_count, 3);

        // INSERT → all 4 IMVs must update correctly
        Spi::run("INSERT INTO mmis_src (dept, salary, active) VALUES ('Eng', 400, true)")
            .expect("insert");

        assert_eq!(
            Spi::get_one::<pgrx::AnyNumeric>("SELECT total FROM mmis_agg WHERE dept = 'Eng'")
                .expect("q").expect("v").to_string(),
            "750", // 350+400
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_active").expect("q").expect("v"),
            5,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_depts").expect("q").expect("v"),
            3, // no new dept
        );

        // INSERT new dept → distinct count changes
        Spi::run("INSERT INTO mmis_src (dept, salary, active) VALUES ('Legal', 250, true)")
            .expect("insert new dept");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_depts").expect("q").expect("v"),
            4,
        );

        // DELETE → all 4 IMVs update
        Spi::run("DELETE FROM mmis_src WHERE salary = 50").expect("delete inactive eng");
        assert_eq!(
            Spi::get_one::<pgrx::AnyNumeric>("SELECT total FROM mmis_agg WHERE dept = 'Eng'")
                .expect("q").expect("v").to_string(),
            "700", // 100+200+400
        );
        // Active count shouldn't change (deleted row was inactive)
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_active").expect("q").expect("v"),
            6,
        );

        // DELETE all rows in a dept → group disappears from aggregate and distinct
        Spi::run("DELETE FROM mmis_src WHERE dept = 'HR'").expect("delete HR");
        let hr_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM mmis_agg WHERE dept = 'HR'",
        ).expect("q").expect("v");
        assert_eq!(hr_count, 0, "HR should disappear from aggregate");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM mmis_depts").expect("q").expect("v"),
            3, // HR gone, Legal still there
        );

        // UPDATE → value change propagates to aggregate and avg
        Spi::run("UPDATE mmis_src SET salary = 1000 WHERE dept = 'Legal'").expect("update");
        assert_eq!(
            Spi::get_one::<pgrx::AnyNumeric>("SELECT total FROM mmis_agg WHERE dept = 'Legal'")
                .expect("q").expect("v").to_string(),
            "1000",
        );

        // Verify passthrough matches source exactly
        let pt_mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, dept, salary FROM mmis_active
                EXCEPT
                SELECT id, dept, salary FROM mmis_src WHERE active = true
            ) x",
        ).expect("q").expect("v");
        assert_eq!(pt_mismatches, 0, "Passthrough IMV should exactly match filtered source");

        // Verify aggregate matches source
        let agg_mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT dept, total, cnt FROM mmis_agg
                EXCEPT
                SELECT dept, SUM(salary), COUNT(*) FROM mmis_src GROUP BY dept
            ) x",
        ).expect("q").expect("v");
        assert_eq!(agg_mismatches, 0, "Aggregate IMV should match source");

        // Verify distinct matches source
        let dist_mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT dept FROM mmis_depts
                EXCEPT
                SELECT DISTINCT dept FROM mmis_src
            ) x",
        ).expect("q").expect("v");
        assert_eq!(dist_mismatches, 0, "Distinct IMV should match source");
    }

    #[pg_test]
    fn test_cte_passthrough_sub_imv() {
        Spi::run(
            "CREATE TABLE cte_pt_src (id SERIAL, region TEXT NOT NULL, val INT NOT NULL, active BOOLEAN NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO cte_pt_src (region, val, active) VALUES \
             ('A', 10, true), ('A', 20, false), ('B', 30, true)",
        )
        .expect("seed");

        // CTE is passthrough (no aggregation) — should become a passthrough sub-IMV
        let result = crate::create_reflex_ivm(
            "cte_pt_view",
            "WITH active_orders AS (
                SELECT id, region, val FROM cte_pt_src WHERE active = true
            )
            SELECT region, SUM(val) AS total FROM active_orders GROUP BY region",
            None,
            None,
            None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify initial state
        let a = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cte_pt_view WHERE region = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(a.to_string(), "10", "Only active A rows: 10");

        // Insert active row → should propagate through CTE sub-IMV
        Spi::run("INSERT INTO cte_pt_src (region, val, active) VALUES ('A', 5, true)")
            .expect("insert");

        let a2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cte_pt_view WHERE region = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(a2.to_string(), "15", "After insert active A: 10 + 5 = 15");

        // Insert inactive row → should NOT affect view
        Spi::run("INSERT INTO cte_pt_src (region, val, active) VALUES ('A', 100, false)")
            .expect("insert inactive");

        let a3 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM cte_pt_view WHERE region = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(a3.to_string(), "15", "Inactive row should not affect view");
    }

    // ---- HAVING clause tests ----

    #[pg_test]
    fn test_having_filters_groups() {
        Spi::run(
            "CREATE TABLE hv_src (id SERIAL, region TEXT NOT NULL, amount NUMERIC NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO hv_src (region, amount) VALUES \
             ('US', 500), ('US', 600), ('EU', 100), ('EU', 50), ('JP', 2000)",
        )
        .expect("seed");

        // Only regions with SUM > 200 should appear
        crate::create_reflex_ivm(
            "hv_view",
            "SELECT region, SUM(amount) AS total FROM hv_src GROUP BY region HAVING SUM(amount) > 200",
            None,
            None,
            None,
        );

        // US = 1100, JP = 2000 → both > 200. EU = 150 → excluded.
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM hv_view").expect("q").expect("v");
        assert_eq!(count, 2, "Only US and JP should pass HAVING SUM > 200");

        let eu = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM hv_view WHERE region = 'EU'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(eu, 0, "EU (150) should be excluded by HAVING");
    }

    #[pg_test]
    fn test_having_dynamic_threshold() {
        Spi::run(
            "CREATE TABLE hvd_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO hvd_src (grp, val) VALUES ('A', 80), ('B', 40)",
        )
        .expect("seed");

        crate::create_reflex_ivm(
            "hvd_view",
            "SELECT grp, SUM(val) AS total FROM hvd_src GROUP BY grp HAVING SUM(val) > 50",
            None,
            None,
            None,
        );

        // Initially: A=80 passes, B=40 fails
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM hvd_view").expect("q").expect("v");
        assert_eq!(count, 1, "Only A should pass HAVING > 50");

        // Push B over threshold
        Spi::run("INSERT INTO hvd_src (grp, val) VALUES ('B', 20)").expect("insert B");
        let b_count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM hvd_view WHERE grp = 'B'")
                .expect("q")
                .expect("v");
        assert_eq!(b_count, 1, "B (60) should now appear after crossing threshold");

        // Pull A below threshold by deleting
        Spi::run("DELETE FROM hvd_src WHERE grp = 'A' AND val = 80").expect("delete A");
        let a_count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM hvd_view WHERE grp = 'A'")
                .expect("q")
                .expect("v");
        assert_eq!(a_count, 0, "A (0) should disappear after falling below threshold");
    }

    #[pg_test]
    fn test_having_with_aggregate_not_in_select() {
        Spi::run(
            "CREATE TABLE hvn_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)",
        )
        .expect("create table");
        Spi::run(
            "INSERT INTO hvn_src (grp, val) VALUES \
             ('A', 10), ('A', 20), ('A', 30), ('B', 100), ('B', 200)",
        )
        .expect("seed");

        // SELECT has SUM but HAVING uses COUNT(*) — not in SELECT
        crate::create_reflex_ivm(
            "hvn_view",
            "SELECT grp, SUM(val) AS total FROM hvn_src GROUP BY grp HAVING COUNT(*) > 2",
            None,
            None,
            None,
        );

        // A has 3 rows → passes. B has 2 rows → fails.
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM hvn_view").expect("q").expect("v");
        assert_eq!(count, 1, "Only A (3 rows) should pass HAVING COUNT(*) > 2");

        let total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM hvn_view WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(total.to_string(), "60", "A total should be 10+20+30=60");
    }

    // ---- Materialized view + refresh tests ----

    #[pg_test]
    fn test_matview_source_skip_triggers() {
        // Create a base table and a materialized view on it
        Spi::run("CREATE TABLE mv_base (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)")
            .expect("create base");
        Spi::run("INSERT INTO mv_base (grp, val) VALUES ('A', 10), ('B', 20)")
            .expect("seed base");
        Spi::run("CREATE MATERIALIZED VIEW mv_src AS SELECT grp, SUM(val) AS total FROM mv_base GROUP BY grp")
            .expect("create matview");

        // Create an IMV that reads from the materialized view — should succeed
        // (triggers skipped for matview, warning emitted)
        let result = crate::create_reflex_ivm(
            "mv_imv",
            "SELECT grp, SUM(total) AS grand_total FROM mv_src GROUP BY grp",
            None,
            None,
            None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify initial data is correct
        let a = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM mv_imv WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(a.to_string(), "10");

        // Insert into base table and refresh the matview
        Spi::run("INSERT INTO mv_base (grp, val) VALUES ('A', 5)").expect("insert");
        Spi::run("REFRESH MATERIALIZED VIEW mv_src").expect("refresh matview");

        // IMV is stale (no triggers on matview) — still shows old value
        let a_stale = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM mv_imv WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(a_stale.to_string(), "10", "IMV should be stale before refresh");

        // Refresh the IMV — should pick up new matview data
        let refresh = crate::refresh_reflex_imv("mv_imv");
        assert_eq!(refresh, "RECONCILED");

        let a_fresh = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT grand_total FROM mv_imv WHERE grp = 'A'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(a_fresh.to_string(), "15", "After refresh, IMV should have 10+5=15");
    }

    #[pg_test]
    fn test_refresh_imv_depending_on() {
        Spi::run("CREATE TABLE rdep_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL)")
            .expect("create table");
        Spi::run("INSERT INTO rdep_src (grp, val) VALUES ('X', 10), ('Y', 20)")
            .expect("seed");

        // Create two IMVs on the same source
        crate::create_reflex_ivm(
            "rdep_v1",
            "SELECT grp, SUM(val) AS total FROM rdep_src GROUP BY grp",
            None,
            None,
            None,
        );
        crate::create_reflex_ivm(
            "rdep_v2",
            "SELECT grp, COUNT(*) AS cnt FROM rdep_src GROUP BY grp",
            None,
            None,
            None,
        );

        // Corrupt both by directly modifying intermediate tables
        Spi::run("UPDATE __reflex_intermediate_rdep_v1 SET \"__sum_val\" = 999 WHERE \"grp\" = 'X'")
            .expect("corrupt v1");
        Spi::run("UPDATE __reflex_intermediate_rdep_v2 SET \"__count_star\" = 999 WHERE \"grp\" = 'X'")
            .expect("corrupt v2");

        // Refresh all IMVs depending on rdep_src
        let result = crate::refresh_imv_depending_on("rdep_src");
        assert!(result.contains("2"), "Should refresh 2 IMVs, got: {}", result);

        // Verify both are fixed
        let v1 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM rdep_v1 WHERE grp = 'X'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(v1.to_string(), "10", "v1 should be fixed after refresh");

        let v2 = Spi::get_one::<i64>(
            "SELECT cnt FROM rdep_v2 WHERE grp = 'X'",
        )
        .expect("q")
        .expect("v");
        assert_eq!(v2, 1, "v2 should be fixed after refresh");
    }

    // =====================================================================
    // Integration tests for untested query features
    // =====================================================================

    /// BOOL_OR aggregate: INSERT, DELETE (recompute), UPDATE
    #[pg_test]
    fn test_bool_or_insert_delete_update() {
        Spi::run(
            "CREATE TABLE bo_src (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, flag BOOLEAN NOT NULL)",
        ).expect("create");
        Spi::run(
            "INSERT INTO bo_src (grp, flag) VALUES ('A', false), ('A', false), ('B', true)",
        ).expect("seed");

        crate::create_reflex_ivm(
            "bo_view",
            "SELECT grp, BOOL_OR(flag) AS any_flag FROM bo_src GROUP BY grp",
            None,
            None,
            None,
        );

        // Initial: A=false (both false), B=true
        // BOOL_OR returns NULL-safe boolean, read via i64 count to verify
        let a_val = Spi::get_one::<bool>("SELECT any_flag FROM bo_view WHERE grp = 'A'")
            .expect("initial A query failed")
            .expect("initial A returned no rows");
        assert!(!a_val, "A should be false initially");

        let b_val = Spi::get_one::<bool>("SELECT any_flag FROM bo_view WHERE grp = 'B'")
            .expect("initial B query failed")
            .expect("initial B returned no rows");
        assert!(b_val, "B should be true initially");

        // INSERT true into A → should become true (OR logic)
        Spi::run("INSERT INTO bo_src (grp, flag) VALUES ('A', true)").expect("insert true");
        let a_ins = Spi::get_one::<bool>("SELECT any_flag FROM bo_view WHERE grp = 'A'")
            .expect("after insert query failed")
            .expect("after insert returned no rows");
        assert!(a_ins, "A should be true after inserting true");

        // DELETE the only true row → recompute from source, should become false
        Spi::run("DELETE FROM bo_src WHERE grp = 'A' AND flag = true").expect("delete true");
        let a_after_del = Spi::get_one::<bool>("SELECT any_flag FROM bo_view WHERE grp = 'A'")
            .expect("after delete query failed")
            .expect("after delete no rows");
        assert!(!a_after_del, "A should revert to false after deleting only true row");

        // UPDATE false→true
        Spi::run("UPDATE bo_src SET flag = true WHERE id = (SELECT MIN(id) FROM bo_src WHERE grp = 'A')")
            .expect("update");

        // Final EXCEPT correctness check (no reconcile needed)
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT grp, any_flag FROM bo_view
                EXCEPT
                SELECT grp, BOOL_OR(flag) FROM bo_src GROUP BY grp
            ) x",
        ).expect("final except query failed").expect("final except no rows");
        assert_eq!(mismatches, 0, "View should exactly match source");
    }

    /// LEFT JOIN aggregate: group by a non-nullable column to avoid NULL group key limitation.
    /// Tests that LEFT JOIN preserves unmatched rows via the left table.
    #[pg_test]
    fn test_left_join_aggregate() {
        Spi::run("CREATE TABLE lj_orders (id SERIAL PRIMARY KEY, product_id INT, region TEXT NOT NULL, amount INT NOT NULL)")
            .expect("create orders");
        Spi::run("CREATE TABLE lj_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create products");
        Spi::run("INSERT INTO lj_products VALUES (1, 'Widget'), (2, 'Gadget')").expect("seed products");
        Spi::run(
            "INSERT INTO lj_orders (product_id, region, amount) VALUES \
             (1, 'US', 100), (1, 'EU', 200), (2, 'US', 50), (999, 'US', 75)",
        ).expect("seed orders");

        // GROUP BY region (non-nullable) to avoid NULL group key issue
        crate::create_reflex_ivm(
            "lj_view",
            "SELECT o.region, SUM(o.amount) AS total, COUNT(*) AS cnt \
             FROM lj_orders o LEFT JOIN lj_products p ON o.product_id = p.id \
             GROUP BY o.region",
            None,
            None,
            None,
        );

        // Verify initial: US=100+50+75=225, EU=200
        let us = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM lj_view WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us.to_string(), "225");

        // INSERT unmatched order (no product) → still counted in LEFT JOIN
        Spi::run("INSERT INTO lj_orders (product_id, region, amount) VALUES (NULL, 'EU', 30)")
            .expect("insert unmatched");
        let eu = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM lj_view WHERE region = 'EU'",
        ).expect("q").expect("v");
        assert_eq!(eu.to_string(), "230"); // 200+30

        // DELETE
        Spi::run("DELETE FROM lj_orders WHERE amount = 100").expect("delete");
        let us_del = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM lj_view WHERE region = 'US'",
        ).expect("q").expect("v");
        assert_eq!(us_del.to_string(), "125"); // 50+75

        // EXCEPT correctness
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT region, total, cnt FROM lj_view
                EXCEPT
                SELECT o.region, SUM(o.amount), COUNT(*)
                FROM lj_orders o LEFT JOIN lj_products p ON o.product_id = p.id
                GROUP BY o.region
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "LEFT JOIN view should match source");
    }

    /// RIGHT JOIN passthrough: rows from right table even when left has no match
    #[pg_test]
    fn test_right_join_passthrough() {
        Spi::run("CREATE TABLE rj_items (id SERIAL PRIMARY KEY, cat_id INT, val INT NOT NULL)")
            .expect("create items");
        Spi::run("CREATE TABLE rj_cats (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create cats");
        Spi::run("INSERT INTO rj_cats VALUES (1, 'A'), (2, 'B'), (3, 'C')").expect("seed cats");
        Spi::run("INSERT INTO rj_items (cat_id, val) VALUES (1, 10), (1, 20)").expect("seed items");

        // RIGHT JOIN: all categories appear, even those with no items
        crate::create_reflex_ivm(
            "rj_view",
            "SELECT i.id AS item_id, i.val, c.name AS cat_name \
             FROM rj_items i RIGHT JOIN rj_cats c ON i.cat_id = c.id",
            None,
            None,
            None,
        );

        // All 3 cats should appear (A has 2 items, B and C have NULL items)
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM rj_view")
            .expect("q").expect("v");
        assert_eq!(count, 4); // 2 items + 2 NULL rows for B and C

        // INSERT item for cat B
        Spi::run("INSERT INTO rj_items (cat_id, val) VALUES (2, 30)").expect("insert");
        let b_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM rj_view WHERE cat_name = 'B' AND item_id IS NOT NULL",
        ).expect("q").expect("v");
        assert_eq!(b_count, 1);

        // DELETE item from cat A
        Spi::run("DELETE FROM rj_items WHERE val = 10").expect("delete");

        // EXCEPT correctness
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT item_id, val, cat_name FROM rj_view
                EXCEPT
                SELECT i.id, i.val, c.name
                FROM rj_items i RIGHT JOIN rj_cats c ON i.cat_id = c.id
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "RIGHT JOIN view should match source");
    }

    /// Cast propagation: SUM(x)::BIGINT should produce correct values and BIGINT column type
    #[pg_test]
    fn test_cast_propagation_sum_bigint() {
        Spi::run("CREATE TABLE cast_src (id SERIAL, grp TEXT NOT NULL, val INT NOT NULL)")
            .expect("create");
        Spi::run("INSERT INTO cast_src (grp, val) VALUES ('X', 100), ('X', 200), ('Y', 50)")
            .expect("seed");

        crate::create_reflex_ivm(
            "cast_view",
            "SELECT grp, SUM(val)::BIGINT AS total FROM cast_src GROUP BY grp",
            None,
            None,
            None,
        );

        // Verify initial values
        let x = Spi::get_one::<i64>("SELECT total FROM cast_view WHERE grp = 'X'")
            .expect("q").expect("v");
        assert_eq!(x, 300);

        // Verify column type is BIGINT (int8)
        let col_type = Spi::get_one::<String>(
            "SELECT data_type FROM information_schema.columns \
             WHERE table_name = 'cast_view' AND column_name = 'total'",
        ).expect("q").expect("v");
        assert_eq!(col_type, "bigint", "Column should be BIGINT due to cast");

        // INSERT
        Spi::run("INSERT INTO cast_src (grp, val) VALUES ('X', 400)").expect("insert");
        let x_after = Spi::get_one::<i64>("SELECT total FROM cast_view WHERE grp = 'X'")
            .expect("q").expect("v");
        assert_eq!(x_after, 700);

        // DELETE
        Spi::run("DELETE FROM cast_src WHERE val = 200").expect("delete");
        let x_del = Spi::get_one::<i64>("SELECT total FROM cast_view WHERE grp = 'X'")
            .expect("q").expect("v");
        assert_eq!(x_del, 500);

        // UPDATE
        Spi::run("UPDATE cast_src SET val = 999 WHERE grp = 'Y'").expect("update");
        let y = Spi::get_one::<i64>("SELECT total FROM cast_view WHERE grp = 'Y'")
            .expect("q").expect("v");
        assert_eq!(y, 999);

        // EXCEPT correctness
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT grp, total FROM cast_view
                EXCEPT
                SELECT grp, SUM(val)::BIGINT FROM cast_src GROUP BY grp
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "Cast view should match source");
    }

    /// Subquery in FROM with a simple filter: the inner table still gets triggers.
    /// Note: subqueries with aggregation inside don't work incrementally because
    /// the trigger replaces the inner table with the transition table, but the
    /// aggregation in the subquery then only sees the delta rows, not the full table.
    /// This test uses a simple WHERE filter instead.
    #[pg_test]
    fn test_subquery_in_from_with_trigger() {
        Spi::run("CREATE TABLE sq_src (id SERIAL PRIMARY KEY, val INT NOT NULL, active BOOLEAN NOT NULL)")
            .expect("create");
        Spi::run("INSERT INTO sq_src (val, active) VALUES (10, true), (20, true), (30, false)")
            .expect("seed");

        // Subquery with simple WHERE filter (no aggregation inside)
        crate::create_reflex_ivm(
            "sq_view",
            "SELECT id, val FROM (SELECT id, val FROM sq_src WHERE active = true) AS sub",
            None,
            None,
            None,
        );

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM sq_view")
            .expect("q").expect("v");
        assert_eq!(count, 2); // only active rows

        // INSERT active row → appears via trigger on sq_src
        Spi::run("INSERT INTO sq_src (val, active) VALUES (40, true)").expect("insert active");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM sq_view").expect("q").expect("v"),
            3,
        );

        // INSERT inactive row → does not appear
        Spi::run("INSERT INTO sq_src (val, active) VALUES (50, false)").expect("insert inactive");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM sq_view").expect("q").expect("v"),
            3,
        );

        // DELETE active row
        Spi::run("DELETE FROM sq_src WHERE val = 10").expect("delete");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM sq_view").expect("q").expect("v"),
            2,
        );

        // EXCEPT correctness
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, val FROM sq_view
                EXCEPT
                SELECT id, val FROM sq_src WHERE active = true
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "Subquery view should match filtered source");
    }

    /// Subquery with aggregation in FROM should be rejected with a clear error.
    #[pg_test]
    fn test_subquery_with_aggregation_rejected() {
        Spi::run("CREATE TABLE sqr_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create");
        Spi::run("CREATE TABLE sqr_orders (id SERIAL, product_id INT NOT NULL, qty INT NOT NULL)")
            .expect("create");

        let result = crate::create_reflex_ivm(
            "sqr_view",
            "SELECT p.name, sub.total_qty \
             FROM sqr_products p \
             JOIN (SELECT product_id, SUM(qty) AS total_qty FROM sqr_orders GROUP BY product_id) AS sub \
             ON p.id = sub.product_id",
            None,
            None,
            None,
        );
        assert!(
            result.starts_with("ERROR:"),
            "Subquery with aggregation should be rejected, got: {}",
            result
        );
        assert!(
            result.contains("CTE"),
            "Error should suggest using CTE, got: {}",
            result
        );
    }

    /// Subquery as only source: the inner table still gets triggers via visitor recursion.
    #[pg_test]
    fn test_subquery_only_source() {
        Spi::run("CREATE TABLE sqo_src (id SERIAL PRIMARY KEY, val INT NOT NULL)")
            .expect("create");
        Spi::run("INSERT INTO sqo_src (val) VALUES (10), (20), (-5)").expect("seed");

        crate::create_reflex_ivm(
            "sqo_view",
            "SELECT id, val FROM (SELECT id, val FROM sqo_src WHERE val > 0) AS sub",
            None,
            None,
            None,
        );

        // Initial: only positive values
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM sqo_view")
            .expect("q").expect("v");
        assert_eq!(count, 2);

        // INSERT positive → appears
        Spi::run("INSERT INTO sqo_src (val) VALUES (30)").expect("insert positive");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM sqo_view").expect("q").expect("v"),
            3,
        );

        // INSERT negative → does not appear (filtered by subquery WHERE)
        Spi::run("INSERT INTO sqo_src (val) VALUES (-10)").expect("insert negative");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM sqo_view").expect("q").expect("v"),
            3,
        );

        // DELETE positive row
        Spi::run("DELETE FROM sqo_src WHERE val = 20").expect("delete");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM sqo_view").expect("q").expect("v"),
            2,
        );

        // EXCEPT correctness
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT id, val FROM sqo_view
                EXCEPT
                SELECT id, val FROM sqo_src WHERE val > 0
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "Subquery view should match filtered source");
    }

    // ---- Storage mode tests ----

    #[pg_test]
    fn test_create_logged_imv() {
        Spi::run("CREATE TABLE log_orders (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO log_orders (city, amount) VALUES ('Paris', 100), ('London', 200)")
            .expect("insert data");

        let result = crate::create_reflex_ivm(
            "log_city_totals",
            "SELECT city, SUM(amount) AS total FROM log_orders GROUP BY city",
            None,
            Some("LOGGED"),
            None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify both tables are LOGGED (relpersistence = 'p')
        let target_persist = Spi::get_one::<String>(
            "SELECT relpersistence::text FROM pg_class WHERE relname = 'log_city_totals'",
        ).expect("query").expect("value");
        assert_eq!(target_persist, "p", "Target table should be permanent (logged)");

        let intermediate_persist = Spi::get_one::<String>(
            "SELECT relpersistence::text FROM pg_class WHERE relname = '__reflex_intermediate_log_city_totals'",
        ).expect("query").expect("value");
        assert_eq!(intermediate_persist, "p", "Intermediate table should be permanent (logged)");

        // Verify data is correct
        let paris_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM log_city_totals WHERE city = 'Paris'",
        ).expect("query").expect("value");
        assert_eq!(paris_total.to_string(), "100");

        // Verify storage_mode in reference table
        let mode = Spi::get_one::<String>(
            "SELECT storage_mode FROM public.__reflex_ivm_reference WHERE name = 'log_city_totals'",
        ).expect("query").expect("value");
        assert_eq!(mode, "LOGGED");
    }

    #[pg_test]
    fn test_create_logged_passthrough() {
        Spi::run("CREATE TABLE log_pt_src (id SERIAL PRIMARY KEY, val TEXT NOT NULL)")
            .expect("create table");
        Spi::run("INSERT INTO log_pt_src (val) VALUES ('a'), ('b')").expect("insert");

        crate::create_reflex_ivm(
            "log_pt_view",
            "SELECT id, val FROM log_pt_src",
            None,
            Some("LOGGED"),
            None,
        );

        // Verify target table is LOGGED
        let persist = Spi::get_one::<String>(
            "SELECT relpersistence::text FROM pg_class WHERE relname = 'log_pt_view'",
        ).expect("query").expect("value");
        assert_eq!(persist, "p", "Passthrough target should be permanent (logged)");
    }

    #[pg_test]
    fn test_logged_trigger_works() {
        Spi::run("CREATE TABLE log_trg (id SERIAL, region TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO log_trg (region, amount) VALUES ('A', 10), ('B', 20)")
            .expect("insert");

        crate::create_reflex_ivm(
            "log_trg_view",
            "SELECT region, SUM(amount) AS total FROM log_trg GROUP BY region",
            None,
            Some("LOGGED"),
            None,
        );

        // INSERT trigger
        Spi::run("INSERT INTO log_trg (region, amount) VALUES ('A', 5)").expect("insert");
        let a_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM log_trg_view WHERE region = 'A'",
        ).expect("query").expect("value");
        assert_eq!(a_total.to_string(), "15");

        // DELETE trigger
        Spi::run("DELETE FROM log_trg WHERE amount = 20").expect("delete");
        let b_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM log_trg_view WHERE region = 'B'",
        ).expect("query").expect("value");
        assert_eq!(b_count, 0, "Region B should be gone after deleting its only row");
    }

    #[pg_test]
    fn test_default_is_unlogged() {
        Spi::run("CREATE TABLE def_orders (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO def_orders (city, amount) VALUES ('Paris', 100)")
            .expect("insert");

        crate::create_reflex_ivm(
            "def_city_totals",
            "SELECT city, SUM(amount) AS total FROM def_orders GROUP BY city",
            None,
            None,
            None,
        );

        // Verify tables are UNLOGGED (relpersistence = 'u')
        let target_persist = Spi::get_one::<String>(
            "SELECT relpersistence::text FROM pg_class WHERE relname = 'def_city_totals'",
        ).expect("query").expect("value");
        assert_eq!(target_persist, "u", "Default target should be unlogged");

        let intermediate_persist = Spi::get_one::<String>(
            "SELECT relpersistence::text FROM pg_class WHERE relname = '__reflex_intermediate_def_city_totals'",
        ).expect("query").expect("value");
        assert_eq!(intermediate_persist, "u", "Default intermediate should be unlogged");
    }

    #[pg_test]
    fn test_invalid_storage_mode() {
        Spi::run("CREATE TABLE inv_stor (id SERIAL, val INT)").expect("create table");
        let result = crate::create_reflex_ivm(
            "inv_stor_view",
            "SELECT val, COUNT(*) AS cnt FROM inv_stor GROUP BY val",
            None,
            Some("INVALID"),
            None,
        );
        assert!(result.starts_with("ERROR:"), "Invalid storage should return error, got: {}", result);
    }

    // ---- Deferred mode tests ----

    #[pg_test]
    fn test_deferred_basic_insert() {
        Spi::run("CREATE TABLE def_src (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO def_src (city, amount) VALUES ('Paris', 100), ('London', 200)")
            .expect("insert seed");

        let result = crate::create_reflex_ivm(
            "def_view",
            "SELECT city, SUM(amount) AS total FROM def_src GROUP BY city",
            None,
            None,
            Some("DEFERRED"),
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify refresh_mode in reference table
        let mode = Spi::get_one::<String>(
            "SELECT refresh_mode FROM public.__reflex_ivm_reference WHERE name = 'def_view'",
        ).expect("query").expect("value");
        assert_eq!(mode, "DEFERRED");

        // Verify staging table exists
        let staging_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = '__reflex_delta_def_src')",
        ).expect("query").expect("value");
        assert!(staging_exists, "Staging table should exist");

        // Verify deferred pending table exists
        let pending_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = '__reflex_deferred_pending')",
        ).expect("query").expect("value");
        assert!(pending_exists, "Deferred pending table should exist");

        // Verify initial data is correct (created during initial materialization)
        let paris_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM def_view WHERE city = 'Paris'",
        ).expect("query").expect("value");
        assert_eq!(paris_total.to_string(), "100");
    }

    #[pg_test]
    fn test_immediate_mode_explicit() {
        Spi::run("CREATE TABLE imm_src (id SERIAL, city TEXT, amount NUMERIC)")
            .expect("create table");
        Spi::run("INSERT INTO imm_src (city, amount) VALUES ('Paris', 100)")
            .expect("insert");

        let result = crate::create_reflex_ivm(
            "imm_view",
            "SELECT city, SUM(amount) AS total FROM imm_src GROUP BY city",
            None,
            None,
            Some("IMMEDIATE"),
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Verify it works like normal: INSERT should update immediately
        Spi::run("INSERT INTO imm_src (city, amount) VALUES ('Paris', 50)")
            .expect("insert");
        let total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM imm_view WHERE city = 'Paris'",
        ).expect("query").expect("value");
        assert_eq!(total.to_string(), "150");
    }

    #[pg_test]
    fn test_invalid_mode() {
        Spi::run("CREATE TABLE inv_mode (id SERIAL, val INT)").expect("create table");
        let result = crate::create_reflex_ivm(
            "inv_mode_view",
            "SELECT val, COUNT(*) AS cnt FROM inv_mode GROUP BY val",
            None,
            None,
            Some("INVALID"),
        );
        assert!(result.starts_with("ERROR:"), "Invalid mode should return error, got: {}", result);
    }

    // ========================================================================
    // UNION ALL tests
    // ========================================================================

    /// Basic UNION ALL of two tables — initial materialization
    #[pg_test]
    fn test_union_all_basic() {
        Spi::run("CREATE TABLE ua_eu (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("CREATE TABLE ua_us (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO ua_eu (city, amount) VALUES ('Paris', 100), ('Berlin', 200)").expect("seed");
        Spi::run("INSERT INTO ua_us (city, amount) VALUES ('NYC', 300), ('LA', 400)").expect("seed");

        let result = crate::create_reflex_ivm(
            "ua_basic",
            "SELECT city, amount FROM ua_eu UNION ALL SELECT city, amount FROM ua_us",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM ua_basic")
            .expect("q").expect("v");
        assert_eq!(count, 4);

        // Verify all rows present
        let paris = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM ua_basic WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(paris, 1);

        let nyc = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM ua_basic WHERE city = 'NYC'",
        ).expect("q").expect("v");
        assert_eq!(nyc, 1);
    }

    /// UNION ALL: INSERT into first source propagates to target
    #[pg_test]
    fn test_union_all_insert_source_a() {
        Spi::run("CREATE TABLE uaia_eu (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("CREATE TABLE uaia_us (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO uaia_eu (city, amount) VALUES ('Paris', 100)").expect("seed");
        Spi::run("INSERT INTO uaia_us (city, amount) VALUES ('NYC', 200)").expect("seed");

        crate::create_reflex_ivm(
            "uaia_view",
            "SELECT city, amount FROM uaia_eu UNION ALL SELECT city, amount FROM uaia_us",
            None, None, None,
        );

        // INSERT into EU source → should appear in target
        Spi::run("INSERT INTO uaia_eu (city, amount) VALUES ('Berlin', 300)").expect("insert");
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM uaia_view")
            .expect("q").expect("v");
        assert_eq!(count, 3);

        let berlin = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT amount FROM uaia_view WHERE city = 'Berlin'",
        ).expect("q").expect("v");
        assert_eq!(berlin.to_string(), "300");
    }

    /// UNION ALL: INSERT into second source propagates to target
    #[pg_test]
    fn test_union_all_insert_source_b() {
        Spi::run("CREATE TABLE uaib_eu (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("CREATE TABLE uaib_us (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO uaib_eu (city, amount) VALUES ('Paris', 100)").expect("seed");
        Spi::run("INSERT INTO uaib_us (city, amount) VALUES ('NYC', 200)").expect("seed");

        crate::create_reflex_ivm(
            "uaib_view",
            "SELECT city, amount FROM uaib_eu UNION ALL SELECT city, amount FROM uaib_us",
            None, None, None,
        );

        // INSERT into US source
        Spi::run("INSERT INTO uaib_us (city, amount) VALUES ('LA', 400), ('Chicago', 500)").expect("insert");
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM uaib_view")
            .expect("q").expect("v");
        assert_eq!(count, 4);
    }

    /// UNION ALL: DELETE from one source removes only those rows
    #[pg_test]
    fn test_union_all_delete() {
        Spi::run("CREATE TABLE uad_a (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
        Spi::run("CREATE TABLE uad_b (id SERIAL PRIMARY KEY, val TEXT)").expect("create");
        Spi::run("INSERT INTO uad_a (val) VALUES ('x'), ('y')").expect("seed");
        Spi::run("INSERT INTO uad_b (val) VALUES ('z')").expect("seed");

        crate::create_reflex_ivm(
            "uad_view",
            "SELECT id, val FROM uad_a UNION ALL SELECT id, val FROM uad_b",
            None, None, None,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uad_view").expect("q").expect("v"),
            3
        );

        // DELETE from source A
        Spi::run("DELETE FROM uad_a WHERE val = 'x'").expect("delete");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uad_view").expect("q").expect("v"),
            2
        );

        // Source B rows untouched
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uad_view WHERE val = 'z'").expect("q").expect("v"),
            1
        );
    }

    /// UNION ALL: UPDATE in one source reflected in target
    #[pg_test]
    fn test_union_all_update() {
        Spi::run("CREATE TABLE uau_a (id SERIAL PRIMARY KEY, val INT)").expect("create");
        Spi::run("CREATE TABLE uau_b (id SERIAL PRIMARY KEY, val INT)").expect("create");
        Spi::run("INSERT INTO uau_a (val) VALUES (10), (20)").expect("seed");
        Spi::run("INSERT INTO uau_b (val) VALUES (30)").expect("seed");

        crate::create_reflex_ivm(
            "uau_view",
            "SELECT id, val FROM uau_a UNION ALL SELECT id, val FROM uau_b",
            None, None, None,
        );

        Spi::run("UPDATE uau_a SET val = 99 WHERE val = 10").expect("update");
        let updated = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM uau_view WHERE val = 99",
        ).expect("q").expect("v");
        assert_eq!(updated, 1);

        // Old value gone
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uau_view WHERE val = 10").expect("q").expect("v"),
            0
        );
    }

    /// UNION ALL with 3 operands
    #[pg_test]
    fn test_union_all_three_operands() {
        Spi::run("CREATE TABLE ua3_a (id SERIAL, val TEXT)").expect("create");
        Spi::run("CREATE TABLE ua3_b (id SERIAL, val TEXT)").expect("create");
        Spi::run("CREATE TABLE ua3_c (id SERIAL, val TEXT)").expect("create");
        Spi::run("INSERT INTO ua3_a (val) VALUES ('a1'), ('a2')").expect("seed");
        Spi::run("INSERT INTO ua3_b (val) VALUES ('b1')").expect("seed");
        Spi::run("INSERT INTO ua3_c (val) VALUES ('c1'), ('c2'), ('c3')").expect("seed");

        let result = crate::create_reflex_ivm(
            "ua3_view",
            "SELECT val FROM ua3_a UNION ALL SELECT val FROM ua3_b UNION ALL SELECT val FROM ua3_c",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ua3_view").expect("q").expect("v"),
            6
        );

        // INSERT into middle source
        Spi::run("INSERT INTO ua3_b (val) VALUES ('b2')").expect("insert");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ua3_view").expect("q").expect("v"),
            7
        );
    }

    /// UNION ALL with aggregation in sub-queries
    #[pg_test]
    fn test_union_all_with_aggregates() {
        Spi::run("CREATE TABLE uaag_eu (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("CREATE TABLE uaag_us (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO uaag_eu (city, amount) VALUES ('Paris', 100), ('Paris', 200), ('Berlin', 50)").expect("seed");
        Spi::run("INSERT INTO uaag_us (city, amount) VALUES ('NYC', 300), ('NYC', 100)").expect("seed");

        let result = crate::create_reflex_ivm(
            "uaag_view",
            "SELECT city, SUM(amount) AS total FROM uaag_eu GROUP BY city \
             UNION ALL \
             SELECT city, SUM(amount) AS total FROM uaag_us GROUP BY city",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Should have 3 rows: Paris(300), Berlin(50), NYC(400)
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uaag_view").expect("q").expect("v"),
            3
        );

        let paris = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM uaag_view WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(paris.to_string(), "300");

        // INSERT into EU → Paris aggregate updates
        Spi::run("INSERT INTO uaag_eu (city, amount) VALUES ('Paris', 50)").expect("insert");
        let paris2 = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM uaag_view WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(paris2.to_string(), "350");
    }

    /// UNION ALL with different WHERE clauses on the same table
    #[pg_test]
    fn test_union_all_same_table_different_filters() {
        Spi::run("CREATE TABLE uaf_src (id SERIAL, category TEXT, val INT)").expect("create");
        Spi::run("INSERT INTO uaf_src (category, val) VALUES \
            ('A', 10), ('A', 20), ('B', 30), ('B', 40), ('C', 50)").expect("seed");

        let result = crate::create_reflex_ivm(
            "uaf_view",
            "SELECT category, SUM(val) AS total FROM uaf_src WHERE category = 'A' GROUP BY category \
             UNION ALL \
             SELECT category, SUM(val) AS total FROM uaf_src WHERE category = 'B' GROUP BY category",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Only A and B, not C
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uaf_view").expect("q").expect("v"),
            2
        );

        // Insert a new A row
        Spi::run("INSERT INTO uaf_src (category, val) VALUES ('A', 5)").expect("insert");
        let a_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM uaf_view WHERE category = 'A'",
        ).expect("q").expect("v");
        assert_eq!(a_total.to_string(), "35"); // 10+20+5

        // Insert a C row — should NOT appear (filtered out by both operands)
        Spi::run("INSERT INTO uaf_src (category, val) VALUES ('C', 100)").expect("insert");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uaf_view").expect("q").expect("v"),
            2
        );
    }

    /// UNION ALL: TRUNCATE one source clears its rows but not the other
    #[pg_test]
    fn test_union_all_truncate() {
        Spi::run("CREATE TABLE uat_a (id SERIAL, val TEXT)").expect("create");
        Spi::run("CREATE TABLE uat_b (id SERIAL, val TEXT)").expect("create");
        Spi::run("INSERT INTO uat_a (val) VALUES ('a1'), ('a2')").expect("seed");
        Spi::run("INSERT INTO uat_b (val) VALUES ('b1')").expect("seed");

        crate::create_reflex_ivm(
            "uat_view",
            "SELECT val FROM uat_a UNION ALL SELECT val FROM uat_b",
            None, None, None,
        );

        Spi::run("TRUNCATE uat_a").expect("truncate");
        // Only b1 remains
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uat_view").expect("q").expect("v"),
            1
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uat_view WHERE val = 'b1'").expect("q").expect("v"),
            1
        );
    }

    // ========================================================================
    // UNION (dedup) tests
    // ========================================================================

    /// Basic UNION dedup — duplicate rows across sources appear once
    #[pg_test]
    fn test_union_dedup_basic() {
        Spi::run("CREATE TABLE ud_a (id SERIAL, city TEXT)").expect("create");
        Spi::run("CREATE TABLE ud_b (id SERIAL, city TEXT)").expect("create");
        Spi::run("INSERT INTO ud_a (city) VALUES ('Paris'), ('Berlin')").expect("seed");
        Spi::run("INSERT INTO ud_b (city) VALUES ('Paris'), ('NYC')").expect("seed");

        let result = crate::create_reflex_ivm(
            "ud_basic",
            "SELECT city FROM ud_a UNION SELECT city FROM ud_b",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Paris appears in both, but UNION deduplicates → 3 distinct cities
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM ud_basic").expect("q").expect("v"),
            3
        );
    }

    /// UNION dedup: delete from one source — row still visible from other source
    #[pg_test]
    fn test_union_dedup_delete_one_source() {
        Spi::run("CREATE TABLE udd_a (id SERIAL PRIMARY KEY, city TEXT)").expect("create");
        Spi::run("CREATE TABLE udd_b (id SERIAL PRIMARY KEY, city TEXT)").expect("create");
        Spi::run("INSERT INTO udd_a (city) VALUES ('Paris'), ('Berlin')").expect("seed");
        Spi::run("INSERT INTO udd_b (city) VALUES ('Paris'), ('NYC')").expect("seed");

        crate::create_reflex_ivm(
            "udd_view",
            "SELECT city FROM udd_a UNION SELECT city FROM udd_b",
            None, None, None,
        );

        // Delete Paris from source A — still visible via source B
        Spi::run("DELETE FROM udd_a WHERE city = 'Paris'").expect("delete");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM udd_view WHERE city = 'Paris'").expect("q").expect("v"),
            1,
            "Paris should still be visible via source B"
        );

        // Total: Berlin, Paris, NYC = 3
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM udd_view").expect("q").expect("v"),
            3
        );
    }

    /// UNION dedup: delete from both sources — row disappears
    #[pg_test]
    fn test_union_dedup_delete_both_sources() {
        Spi::run("CREATE TABLE uddb_a (id SERIAL PRIMARY KEY, city TEXT)").expect("create");
        Spi::run("CREATE TABLE uddb_b (id SERIAL PRIMARY KEY, city TEXT)").expect("create");
        Spi::run("INSERT INTO uddb_a (city) VALUES ('Paris')").expect("seed");
        Spi::run("INSERT INTO uddb_b (city) VALUES ('Paris'), ('NYC')").expect("seed");

        crate::create_reflex_ivm(
            "uddb_view",
            "SELECT city FROM uddb_a UNION SELECT city FROM uddb_b",
            None, None, None,
        );

        // Delete Paris from A
        Spi::run("DELETE FROM uddb_a WHERE city = 'Paris'").expect("delete");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uddb_view WHERE city = 'Paris'").expect("q").expect("v"),
            1, "Still visible from B"
        );

        // Delete Paris from B too
        Spi::run("DELETE FROM uddb_b WHERE city = 'Paris'").expect("delete");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uddb_view WHERE city = 'Paris'").expect("q").expect("v"),
            0, "Gone from both sources"
        );

        // Only NYC remains
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uddb_view").expect("q").expect("v"),
            1
        );
    }

    /// UNION dedup: INSERT a duplicate — count stays the same
    #[pg_test]
    fn test_union_dedup_insert_duplicate() {
        Spi::run("CREATE TABLE udi_a (id SERIAL, city TEXT)").expect("create");
        Spi::run("CREATE TABLE udi_b (id SERIAL, city TEXT)").expect("create");
        Spi::run("INSERT INTO udi_a (city) VALUES ('Paris')").expect("seed");
        Spi::run("INSERT INTO udi_b (city) VALUES ('NYC')").expect("seed");

        crate::create_reflex_ivm(
            "udi_view",
            "SELECT city FROM udi_a UNION SELECT city FROM udi_b",
            None, None, None,
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM udi_view").expect("q").expect("v"),
            2
        );

        // Insert Paris into B — already exists via A, total stays 2
        Spi::run("INSERT INTO udi_b (city) VALUES ('Paris')").expect("insert dup");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM udi_view").expect("q").expect("v"),
            2, "UNION dedup: duplicate across sources should not add a row"
        );
    }

    /// UNION with aggregates in sub-queries
    #[pg_test]
    fn test_union_dedup_with_aggregates() {
        Spi::run("CREATE TABLE uda_eu (id SERIAL, region TEXT, amount NUMERIC)").expect("create");
        Spi::run("CREATE TABLE uda_us (id SERIAL, region TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO uda_eu (region, amount) VALUES ('West', 100), ('West', 200), ('East', 50)").expect("seed");
        Spi::run("INSERT INTO uda_us (region, amount) VALUES ('West', 300), ('South', 75)").expect("seed");

        let result = crate::create_reflex_ivm(
            "uda_view",
            "SELECT region, SUM(amount) AS total FROM uda_eu GROUP BY region \
             UNION \
             SELECT region, SUM(amount) AS total FROM uda_us GROUP BY region",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // West appears in both with DIFFERENT totals (300 from EU, 300 from US)
        // UNION dedup on (region, total): EU-West=300, US-West=300 are same row → deduplicated
        // Result: (West, 300), (East, 50), (South, 75) = 3 rows
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM uda_view").expect("q").expect("v"),
            3
        );
    }

    // ========================================================================
    // WINDOW function tests
    // ========================================================================

    /// Simple ROW_NUMBER() over entire result (no PARTITION BY)
    #[pg_test]
    fn test_window_row_number_no_partition() {
        Spi::run("CREATE TABLE wrn_src (id SERIAL, name TEXT, score INT)").expect("create");
        Spi::run("INSERT INTO wrn_src (name, score) VALUES \
            ('Alice', 90), ('Bob', 80), ('Charlie', 70)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wrn_view",
            "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM wrn_src",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Alice=1, Bob=2, Charlie=3
        let alice_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wrn_view WHERE name = 'Alice'",
        ).expect("q").expect("v");
        assert_eq!(alice_rank, 1);

        let charlie_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wrn_view WHERE name = 'Charlie'",
        ).expect("q").expect("v");
        assert_eq!(charlie_rank, 3);
    }

    /// ROW_NUMBER: INSERT changes rankings
    #[pg_test]
    fn test_window_row_number_insert_reranks() {
        Spi::run("CREATE TABLE wrni_src (id SERIAL, name TEXT, score INT)").expect("create");
        Spi::run("INSERT INTO wrni_src (name, score) VALUES \
            ('Alice', 90), ('Bob', 80)").expect("seed");

        crate::create_reflex_ivm(
            "wrni_view",
            "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM wrni_src",
            None, None, None,
        );

        // Insert someone who beats Alice
        Spi::run("INSERT INTO wrni_src (name, score) VALUES ('Zara', 95)").expect("insert");

        let zara_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wrni_view WHERE name = 'Zara'",
        ).expect("q").expect("v");
        assert_eq!(zara_rank, 1, "Zara should be rank 1");

        let alice_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wrni_view WHERE name = 'Alice'",
        ).expect("q").expect("v");
        assert_eq!(alice_rank, 2, "Alice should drop to rank 2");

        let bob_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wrni_view WHERE name = 'Bob'",
        ).expect("q").expect("v");
        assert_eq!(bob_rank, 3, "Bob should drop to rank 3");
    }

    /// ROW_NUMBER: DELETE changes rankings
    #[pg_test]
    fn test_window_row_number_delete_reranks() {
        Spi::run("CREATE TABLE wrnd_src (id SERIAL PRIMARY KEY, name TEXT, score INT)").expect("create");
        Spi::run("INSERT INTO wrnd_src (name, score) VALUES \
            ('Alice', 90), ('Bob', 80), ('Charlie', 70)").expect("seed");

        crate::create_reflex_ivm(
            "wrnd_view",
            "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM wrnd_src",
            None, None, None,
        );

        // Delete Alice (rank 1)
        Spi::run("DELETE FROM wrnd_src WHERE name = 'Alice'").expect("delete");

        // Bob promoted to rank 1
        let bob_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wrnd_view WHERE name = 'Bob'",
        ).expect("q").expect("v");
        assert_eq!(bob_rank, 1);

        // Charlie promoted to rank 2
        let charlie_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wrnd_view WHERE name = 'Charlie'",
        ).expect("q").expect("v");
        assert_eq!(charlie_rank, 2);

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM wrnd_view").expect("q").expect("v"),
            2
        );
    }

    /// ROW_NUMBER with PARTITION BY — partitioned ranking
    #[pg_test]
    fn test_window_row_number_partition_by() {
        Spi::run("CREATE TABLE wrnp_src (id SERIAL, dept TEXT, name TEXT, score INT)").expect("create");
        Spi::run("INSERT INTO wrnp_src (dept, name, score) VALUES \
            ('eng', 'Alice', 90), ('eng', 'Bob', 80), ('eng', 'Charlie', 70), \
            ('sales', 'Dave', 95), ('sales', 'Eve', 85)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wrnp_view",
            "SELECT dept, name, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM wrnp_src",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // eng: Alice=1, Bob=2, Charlie=3
        let alice_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wrnp_view WHERE name = 'Alice'",
        ).expect("q").expect("v");
        assert_eq!(alice_rank, 1);

        // sales: Dave=1, Eve=2
        let dave_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wrnp_view WHERE name = 'Dave'",
        ).expect("q").expect("v");
        assert_eq!(dave_rank, 1);

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM wrnp_view").expect("q").expect("v"),
            5
        );
    }

    /// PARTITION BY: INSERT affects only the partition, not other partitions
    #[pg_test]
    fn test_window_partition_insert_isolation() {
        Spi::run("CREATE TABLE wpi_src (id SERIAL, dept TEXT, name TEXT, score INT)").expect("create");
        Spi::run("INSERT INTO wpi_src (dept, name, score) VALUES \
            ('eng', 'Alice', 90), ('eng', 'Bob', 80), \
            ('sales', 'Dave', 95)").expect("seed");

        crate::create_reflex_ivm(
            "wpi_view",
            "SELECT dept, name, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM wpi_src",
            None, None, None,
        );

        // Insert into eng partition — sales should be unaffected
        Spi::run("INSERT INTO wpi_src (dept, name, score) VALUES ('eng', 'Zara', 95)").expect("insert");

        // eng: Zara=1, Alice=2, Bob=3
        let zara = Spi::get_one::<i64>(
            "SELECT rnk FROM wpi_view WHERE name = 'Zara'",
        ).expect("q").expect("v");
        assert_eq!(zara, 1);

        let alice = Spi::get_one::<i64>(
            "SELECT rnk FROM wpi_view WHERE name = 'Alice'",
        ).expect("q").expect("v");
        assert_eq!(alice, 2);

        // sales: Dave still rank 1 (unaffected)
        let dave = Spi::get_one::<i64>(
            "SELECT rnk FROM wpi_view WHERE name = 'Dave'",
        ).expect("q").expect("v");
        assert_eq!(dave, 1);
    }

    /// RANK() with ties
    #[pg_test]
    fn test_window_rank_with_ties() {
        Spi::run("CREATE TABLE wr_src (id SERIAL, name TEXT, score INT)").expect("create");
        Spi::run("INSERT INTO wr_src (name, score) VALUES \
            ('Alice', 90), ('Bob', 90), ('Charlie', 80)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wr_view",
            "SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rnk FROM wr_src",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Alice and Bob tied at rank 1, Charlie at rank 3 (not 2)
        let alice = Spi::get_one::<i64>(
            "SELECT rnk FROM wr_view WHERE name = 'Alice'",
        ).expect("q").expect("v");
        assert_eq!(alice, 1);

        let bob = Spi::get_one::<i64>(
            "SELECT rnk FROM wr_view WHERE name = 'Bob'",
        ).expect("q").expect("v");
        assert_eq!(bob, 1);

        let charlie = Spi::get_one::<i64>(
            "SELECT rnk FROM wr_view WHERE name = 'Charlie'",
        ).expect("q").expect("v");
        assert_eq!(charlie, 3); // RANK skips 2
    }

    /// DENSE_RANK() — no gaps after ties
    #[pg_test]
    fn test_window_dense_rank() {
        Spi::run("CREATE TABLE wdr_src (id SERIAL, name TEXT, score INT)").expect("create");
        Spi::run("INSERT INTO wdr_src (name, score) VALUES \
            ('Alice', 90), ('Bob', 90), ('Charlie', 80)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wdr_view",
            "SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS rnk FROM wdr_src",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let charlie = Spi::get_one::<i64>(
            "SELECT rnk FROM wdr_view WHERE name = 'Charlie'",
        ).expect("q").expect("v");
        assert_eq!(charlie, 2); // DENSE_RANK: 1,1,2 not 1,1,3
    }

    /// SUM() OVER (PARTITION BY) — running/partition aggregate via window
    #[pg_test]
    fn test_window_sum_partition() {
        Spi::run("CREATE TABLE wsp_src (id SERIAL, dept TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO wsp_src (dept, amount) VALUES \
            ('eng', 100), ('eng', 200), ('sales', 50), ('sales', 75)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wsp_view",
            "SELECT dept, amount, SUM(amount) OVER (PARTITION BY dept) AS dept_total FROM wsp_src",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // All eng rows should show dept_total = 300
        let eng_totals: Vec<String> = Spi::connect(|client| {
            client
                .select("SELECT dept_total::text FROM wsp_view WHERE dept = 'eng' ORDER BY amount", None, &[])
                .unwrap()
                .map(|row| row.get_by_name::<&str, _>("dept_total").unwrap().unwrap().to_string())
                .collect()
        });
        assert_eq!(eng_totals, vec!["300", "300"]);

        // Insert into eng → dept_total should update for ALL eng rows
        Spi::run("INSERT INTO wsp_src (dept, amount) VALUES ('eng', 50)").expect("insert");
        let eng_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT DISTINCT dept_total FROM wsp_view WHERE dept = 'eng'",
        ).expect("q").expect("v");
        assert_eq!(eng_total.to_string(), "350");
    }

    /// LAG() — access previous row in window
    #[pg_test]
    fn test_window_lag() {
        Spi::run("CREATE TABLE wl_src (id SERIAL, ts INT, val INT)").expect("create");
        Spi::run("INSERT INTO wl_src (ts, val) VALUES (1, 10), (2, 20), (3, 30)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wl_view",
            "SELECT ts, val, LAG(val) OVER (ORDER BY ts) AS prev_val FROM wl_src",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // ts=1 → prev_val = NULL; ts=2 → prev_val = 10; ts=3 → prev_val = 20
        let prev_at_2 = Spi::get_one::<i32>(
            "SELECT prev_val FROM wl_view WHERE ts = 2",
        ).expect("q").expect("v");
        assert_eq!(prev_at_2, 10);

        let prev_at_3 = Spi::get_one::<i32>(
            "SELECT prev_val FROM wl_view WHERE ts = 3",
        ).expect("q").expect("v");
        assert_eq!(prev_at_3, 20);

        // Insert ts=0 → shifts everything
        Spi::run("INSERT INTO wl_src (ts, val) VALUES (0, 5)").expect("insert");
        let prev_at_1 = Spi::get_one::<i32>(
            "SELECT prev_val FROM wl_view WHERE ts = 1",
        ).expect("q").expect("v");
        assert_eq!(prev_at_1, 5, "ts=1 should now lag ts=0 (val=5)");
    }

    /// LEAD() — access next row in window
    #[pg_test]
    fn test_window_lead() {
        Spi::run("CREATE TABLE wld_src (id SERIAL, ts INT, val INT)").expect("create");
        Spi::run("INSERT INTO wld_src (ts, val) VALUES (1, 10), (2, 20), (3, 30)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wld_view",
            "SELECT ts, val, LEAD(val) OVER (ORDER BY ts) AS next_val FROM wld_src",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        let next_at_1 = Spi::get_one::<i32>(
            "SELECT next_val FROM wld_view WHERE ts = 1",
        ).expect("q").expect("v");
        assert_eq!(next_at_1, 20);

        // ts=3 has no next → NULL
        let next_at_3_is_null = Spi::get_one::<bool>(
            "SELECT next_val IS NULL FROM wld_view WHERE ts = 3",
        ).expect("q").expect("v");
        assert!(next_at_3_is_null, "Last row should have NULL next_val");
    }

    /// GROUP BY + WINDOW: Aggregates maintained incrementally,
    /// window recomputed over intermediate result
    #[pg_test]
    fn test_window_group_by_plus_window() {
        Spi::run("CREATE TABLE wgw_src (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO wgw_src (city, amount) VALUES \
            ('Paris', 100), ('Paris', 200), ('London', 50), ('Berlin', 300)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wgw_view",
            "SELECT city, SUM(amount) AS total, \
                    RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk \
             FROM wgw_src GROUP BY city",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Berlin=300 rank 1, Paris=300 rank 1, London=50 rank 3
        let berlin_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wgw_view WHERE city = 'Berlin'",
        ).expect("q").expect("v");
        assert_eq!(berlin_rank, 1);

        let paris_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wgw_view WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(paris_rank, 1); // Tied with Berlin

        let london_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wgw_view WHERE city = 'London'",
        ).expect("q").expect("v");
        assert_eq!(london_rank, 3);
    }

    /// GROUP BY + WINDOW: INSERT changes aggregate and re-ranks
    #[pg_test]
    fn test_window_group_by_insert_reranks() {
        Spi::run("CREATE TABLE wgwi_src (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO wgwi_src (city, amount) VALUES \
            ('Paris', 100), ('London', 200), ('Berlin', 150)").expect("seed");

        crate::create_reflex_ivm(
            "wgwi_view",
            "SELECT city, SUM(amount) AS total, \
                    RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk \
             FROM wgwi_src GROUP BY city",
            None, None, None,
        );

        // Initial: London=200(1), Berlin=150(2), Paris=100(3)
        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wgwi_view WHERE city = 'London'").expect("q").expect("v"),
            1
        );

        // Insert enough to make Paris rank 1
        Spi::run("INSERT INTO wgwi_src (city, amount) VALUES ('Paris', 250)").expect("insert");

        // Now: Paris=350(1), London=200(2), Berlin=150(3)
        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wgwi_view WHERE city = 'Paris'").expect("q").expect("v"),
            1, "Paris should be rank 1 after insert"
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wgwi_view WHERE city = 'London'").expect("q").expect("v"),
            2, "London should drop to rank 2"
        );

        // Verify aggregate is correct
        let paris_total = Spi::get_one::<pgrx::AnyNumeric>(
            "SELECT total FROM wgwi_view WHERE city = 'Paris'",
        ).expect("q").expect("v");
        assert_eq!(paris_total.to_string(), "350");
    }

    /// GROUP BY + WINDOW with PARTITION BY — partitioned ranking over aggregates
    #[pg_test]
    fn test_window_group_by_partition_by() {
        Spi::run("CREATE TABLE wgwp_src (id SERIAL, region TEXT, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO wgwp_src (region, city, amount) VALUES \
            ('EU', 'Paris', 100), ('EU', 'Paris', 200), ('EU', 'Berlin', 300), \
            ('US', 'NYC', 400), ('US', 'LA', 150), ('US', 'Chicago', 250)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wgwp_view",
            "SELECT region, city, SUM(amount) AS total, \
                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) AS rnk \
             FROM wgwp_src GROUP BY region, city",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // EU: Berlin=300(1), Paris=300(1 or 2 depending on tie-breaking)
        // US: NYC=400(1), Chicago=250(2), LA=150(3)
        let nyc_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wgwp_view WHERE city = 'NYC'",
        ).expect("q").expect("v");
        assert_eq!(nyc_rank, 1);

        let la_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wgwp_view WHERE city = 'LA'",
        ).expect("q").expect("v");
        assert_eq!(la_rank, 3);

        // INSERT into EU → only EU partition should re-rank
        Spi::run("INSERT INTO wgwp_src (region, city, amount) VALUES ('EU', 'Madrid', 500)").expect("insert");
        let madrid_rank = Spi::get_one::<i64>(
            "SELECT rnk FROM wgwp_view WHERE city = 'Madrid'",
        ).expect("q").expect("v");
        assert_eq!(madrid_rank, 1, "Madrid (500) should be rank 1 in EU");

        // US ranks unchanged
        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wgwp_view WHERE city = 'NYC'").expect("q").expect("v"),
            1, "NYC rank should be unchanged (US partition untouched)"
        );
    }

    /// GROUP BY + WINDOW: DELETE removes a group and re-ranks
    #[pg_test]
    fn test_window_group_by_delete_reranks() {
        Spi::run("CREATE TABLE wgwd_src (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO wgwd_src (city, amount) VALUES \
            ('A', 100), ('B', 200), ('C', 300)").expect("seed");

        crate::create_reflex_ivm(
            "wgwd_view",
            "SELECT city, SUM(amount) AS total, \
                    ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) AS rnk \
             FROM wgwd_src GROUP BY city",
            None, None, None,
        );

        // Delete the top city
        Spi::run("DELETE FROM wgwd_src WHERE city = 'C'").expect("delete");

        // B should become rank 1, A rank 2
        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wgwd_view WHERE city = 'B'").expect("q").expect("v"),
            1
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wgwd_view WHERE city = 'A'").expect("q").expect("v"),
            2
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM wgwd_view").expect("q").expect("v"),
            2
        );
    }

    /// Multiple window functions in the same query
    #[pg_test]
    fn test_window_multiple_functions() {
        Spi::run("CREATE TABLE wmf_src (id SERIAL, name TEXT, score INT)").expect("create");
        Spi::run("INSERT INTO wmf_src (name, score) VALUES \
            ('Alice', 90), ('Bob', 80), ('Charlie', 70), ('Dave', 90)").expect("seed");

        let result = crate::create_reflex_ivm(
            "wmf_view",
            "SELECT name, score, \
                    ROW_NUMBER() OVER (ORDER BY score DESC, name) AS row_num, \
                    RANK() OVER (ORDER BY score DESC) AS rnk, \
                    DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rnk \
             FROM wmf_src",
            None, None, None,
        );
        assert_eq!(result, "CREATE REFLEX INCREMENTAL VIEW");

        // Alice: row_num=1, rank=1, dense_rank=1
        let alice_rn = Spi::get_one::<i64>("SELECT row_num FROM wmf_view WHERE name = 'Alice'").expect("q").expect("v");
        let alice_r = Spi::get_one::<i64>("SELECT rnk FROM wmf_view WHERE name = 'Alice'").expect("q").expect("v");
        let alice_dr = Spi::get_one::<i64>("SELECT dense_rnk FROM wmf_view WHERE name = 'Alice'").expect("q").expect("v");
        assert_eq!(alice_rn, 1);
        assert_eq!(alice_r, 1);
        assert_eq!(alice_dr, 1);

        // Charlie: row_num=4, rank=3, dense_rank=2 (because RANK skips, DENSE_RANK doesn't)
        // Wait: Alice=90, Dave=90, Bob=80, Charlie=70
        // ROW_NUMBER by (score DESC, name): Alice(1), Dave(2), Bob(3), Charlie(4)
        // RANK by score DESC: 1,1,3,4
        // DENSE_RANK by score DESC: 1,1,2,3
        let charlie_rn = Spi::get_one::<i64>("SELECT row_num FROM wmf_view WHERE name = 'Charlie'").expect("q").expect("v");
        let charlie_r = Spi::get_one::<i64>("SELECT rnk FROM wmf_view WHERE name = 'Charlie'").expect("q").expect("v");
        let charlie_dr = Spi::get_one::<i64>("SELECT dense_rnk FROM wmf_view WHERE name = 'Charlie'").expect("q").expect("v");
        assert_eq!(charlie_rn, 4);
        assert_eq!(charlie_r, 4);
        assert_eq!(charlie_dr, 3);
    }

    /// Window function with UPDATE — value change triggers re-ranking
    #[pg_test]
    fn test_window_update_reranks() {
        Spi::run("CREATE TABLE wu_src (id SERIAL PRIMARY KEY, name TEXT, score INT)").expect("create");
        Spi::run("INSERT INTO wu_src (name, score) VALUES \
            ('Alice', 90), ('Bob', 80), ('Charlie', 70)").expect("seed");

        crate::create_reflex_ivm(
            "wu_view",
            "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM wu_src",
            None, None, None,
        );

        // Update Charlie to top score
        Spi::run("UPDATE wu_src SET score = 100 WHERE name = 'Charlie'").expect("update");

        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wu_view WHERE name = 'Charlie'").expect("q").expect("v"),
            1, "Charlie should be rank 1 after score update"
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wu_view WHERE name = 'Alice'").expect("q").expect("v"),
            2
        );
    }

    /// EXCEPT correctness check for GROUP BY + WINDOW
    #[pg_test]
    fn test_window_group_by_except_correctness() {
        Spi::run("CREATE TABLE wge_src (id SERIAL, city TEXT, amount NUMERIC)").expect("create");
        Spi::run("INSERT INTO wge_src (city, amount) VALUES \
            ('Paris', 100), ('Paris', 200), ('London', 50), ('Berlin', 300), ('Berlin', 50)").expect("seed");

        crate::create_reflex_ivm(
            "wge_view",
            "SELECT city, SUM(amount) AS total, \
                    RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk \
             FROM wge_src GROUP BY city",
            None, None, None,
        );

        // Verify via EXCEPT against fresh computation
        let mismatches = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT city, total, rnk FROM wge_view
                EXCEPT
                SELECT city, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
                FROM wge_src GROUP BY city
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches, 0, "IMV should match fresh computation");

        // Mutate and re-check
        Spi::run("INSERT INTO wge_src (city, amount) VALUES ('London', 500)").expect("insert");
        Spi::run("DELETE FROM wge_src WHERE city = 'Berlin' AND amount = 50").expect("delete");

        let mismatches2 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM (
                SELECT city, total, rnk FROM wge_view
                EXCEPT
                SELECT city, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
                FROM wge_src GROUP BY city
            ) x",
        ).expect("q").expect("v");
        assert_eq!(mismatches2, 0, "IMV should match fresh computation after mutations");
    }

    /// Window with TRUNCATE — should clear and recompute correctly on re-insert
    #[pg_test]
    fn test_window_truncate_and_reinsert() {
        Spi::run("CREATE TABLE wtr_src (id SERIAL, val INT)").expect("create");
        Spi::run("INSERT INTO wtr_src (val) VALUES (10), (20), (30)").expect("seed");

        crate::create_reflex_ivm(
            "wtr_view",
            "SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS rnk FROM wtr_src",
            None, None, None,
        );

        Spi::run("TRUNCATE wtr_src").expect("truncate");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM wtr_view").expect("q").expect("v"),
            0
        );

        // Re-insert different data
        Spi::run("INSERT INTO wtr_src (val) VALUES (99), (1)").expect("reinsert");
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM wtr_view").expect("q").expect("v"),
            2
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wtr_view WHERE val = 1").expect("q").expect("v"),
            1
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT rnk FROM wtr_view WHERE val = 99").expect("q").expect("v"),
            2
        );
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

#[cfg(test)]
mod proptest_tests {
    use proptest::prelude::*;

    proptest! {
        /// Any string with characters outside [a-zA-Z0-9_.] should be rejected
        #[test]
        fn validate_rejects_unsafe_chars(s in "[a-zA-Z_][a-zA-Z0-9_.]{0,20}[^a-zA-Z0-9_.]+") {
            assert!(crate::validate_view_name(&s).is_err());
        }

        /// Any valid identifier (letter/underscore start, alphanumeric/underscore/period body,
        /// no consecutive dots, no trailing dot) should be accepted
        #[test]
        fn validate_accepts_safe_names(s in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
            assert!(crate::validate_view_name(&s).is_ok());
        }

        /// Schema-qualified names (one dot, valid parts) should be accepted
        #[test]
        fn validate_accepts_schema_qualified(
            schema in "[a-zA-Z_][a-zA-Z0-9_]{0,10}",
            name in "[a-zA-Z_][a-zA-Z0-9_]{0,10}",
        ) {
            let qualified = format!("{}.{}", schema, name);
            assert!(crate::validate_view_name(&qualified).is_ok());
        }

        /// Empty string should always be rejected
        #[test]
        fn validate_rejects_empty(s in "[ \t\n]*") {
            if s.is_empty() {
                assert!(crate::validate_view_name(&s).is_err());
            }
        }
    }
}
