use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

use crate::aggregation::plan_aggregation;
use crate::query_decomposer::{
    bare_column_name, generate_aggregations_json, generate_base_query, generate_end_query, normalized_column_name,
    intermediate_table_name, quote_identifier, replace_identifier, split_qualified_name,
};
use crate::schema_builder::{
    build_indexes_ddl, build_intermediate_table_ddl, build_target_table_ddl, build_trigger_ddls,
    build_deferred_trigger_ddls, build_deferred_flush_ddl, build_staging_table_ddl,
};
use crate::sql_analyzer::{analyze, SqlAnalysisError};
use crate::window;
use crate::validate_view_name;

pub(crate) fn create_reflex_ivm_impl(view_name: &str, sql: &str, unique_columns_str: &str, if_not_exists: bool, storage_mode: &str, refresh_mode: &str) -> &'static str {
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
            if let Some(reason) = a.unsupported_reason() {
                return Box::leak(
                    format!("ERROR: {}", reason).into_boxed_str(),
                );
            }
            // Reject SUM(DISTINCT), AVG(DISTINCT), etc. — DISTINCT modifier is only
            // supported on COUNT. Check the original SQL for the pattern.
            let sql_upper = sql.to_uppercase();
            let has_distinct_agg = sql_upper.contains("SUM(DISTINCT") || sql_upper.contains("SUM (DISTINCT")
                || sql_upper.contains("AVG(DISTINCT") || sql_upper.contains("AVG (DISTINCT")
                || sql_upper.contains("MIN(DISTINCT") || sql_upper.contains("MIN (DISTINCT")
                || sql_upper.contains("MAX(DISTINCT") || sql_upper.contains("MAX (DISTINCT")
                || sql_upper.contains("BOOL_OR(DISTINCT") || sql_upper.contains("BOOL_OR (DISTINCT");
            if has_distinct_agg {
                return "ERROR: DISTINCT modifier on SUM/AVG/MIN/MAX/BOOL_OR is not supported. \
                        Only COUNT(DISTINCT col) is supported. Use a CTE with SELECT DISTINCT \
                        to pre-deduplicate: WITH d AS (SELECT DISTINCT grp, val FROM t) SELECT grp, SUM(val) FROM d GROUP BY grp";
            }
            a
        }
    };

    // --- Set operation decomposition: UNION / INTERSECT / EXCEPT ---
    if let Some(ref set_op) = analysis.set_operation {
        match set_op.op {
            sqlparser::ast::SetOperator::Union
            | sqlparser::ast::SetOperator::Intersect
            | sqlparser::ast::SetOperator::Except => {}
            _ => {
                return "ERROR: Unsupported set operation. Supported: UNION, INTERSECT, EXCEPT.";
            }
        }

        // Each operand becomes its own sub-IMV.
        // Propagate unique_columns so passthrough sub-IMVs can use targeted DELETE/UPDATE
        // instead of falling back to full refresh.
        let mut sub_imv_names: Vec<String> = Vec::new();
        for (i, operand_sql) in set_op.operand_sqls.iter().enumerate() {
            let sub_name = format!("{}__union_{}", view_name, i);
            let result = create_reflex_ivm_impl(
                &sub_name, operand_sql, unique_columns_str, false, storage_mode, refresh_mode,
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
            // UNION / INTERSECT / EXCEPT (without ALL): create a VIEW.
            // The sub-IMVs maintain data incrementally; PostgreSQL handles
            // the set operation semantics at query time.
            let set_keyword = match set_op.op {
                sqlparser::ast::SetOperator::Union => "UNION",
                sqlparser::ast::SetOperator::Intersect => "INTERSECT",
                sqlparser::ast::SetOperator::Except => "EXCEPT",
                _ => "UNION",
            };
            let view_sql = union_selects.join(&format!(" {} ", set_keyword));
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

    // --- DISTINCT ON decomposition: passthrough sub-IMV + ROW_NUMBER VIEW ---
    // DISTINCT ON (cols) ORDER BY ... selects one row per group. We decompose into:
    //   1. A sub-IMV for the base data (passthrough) — incrementally maintained
    //   2. A VIEW with ROW_NUMBER() OVER (PARTITION BY <cols> ORDER BY <order>) WHERE rn = 1
    if analysis.has_distinct_on && !analysis.distinct_on_columns.is_empty() {
        // Build base SQL: original SELECT without DISTINCT ON and ORDER BY
        let select_items: Vec<String> = analysis.select_columns.iter().map(|c| {
            if let Some(ref alias) = c.alias {
                format!("{} AS {}", c.expr_sql, alias)
            } else {
                c.expr_sql.clone()
            }
        }).collect();
        let mut base_sql = format!("SELECT {} FROM {}", select_items.join(", "), analysis.from_clause_sql);
        if let Some(ref wc) = analysis.where_clause {
            base_sql.push_str(&format!(" WHERE {}", wc));
        }

        // Create sub-IMV for the base data
        let base_name = format!("{}__base", view_name);
        let result = create_reflex_ivm_impl(
            &base_name, &base_sql, unique_columns_str, false, storage_mode, refresh_mode,
        );
        if result.starts_with("ERROR") {
            return result;
        }

        // Build the VIEW: SELECT <cols> FROM (SELECT *, ROW_NUMBER() OVER (...) AS __reflex_rn FROM base) WHERE __reflex_rn = 1
        // Strip table qualifiers — the VIEW reads from the base sub-IMV which has bare column names
        let partition_cols: Vec<String> = analysis.distinct_on_columns.iter()
            .map(|c| format!("\"{}\"", bare_column_name(c)))
            .collect();
        let partition_by = partition_cols.join(", ");

        // For ORDER BY, strip table qualifiers but preserve ASC/DESC/NULLS modifiers
        let order_parts: Vec<String> = analysis.order_by_exprs.iter().map(|expr| {
            // Split on first space to separate column from modifiers (e.g., "j2.val DESC")
            let parts: Vec<&str> = expr.splitn(2, ' ').collect();
            let col = format!("\"{}\"", bare_column_name(parts[0]));
            if parts.len() > 1 {
                format!("{} {}", col, parts[1])
            } else {
                col
            }
        }).collect();
        let order_by = order_parts.join(", ");

        // Output column list (just names/aliases, no expressions)
        let output_cols: Vec<String> = analysis.select_columns.iter().map(|c| {
            if let Some(ref alias) = c.alias {
                format!("\"{}\"", alias)
            } else {
                format!("\"{}\"", bare_column_name(&c.expr_sql))
            }
        }).collect();

        let view_sql = format!(
            "SELECT {} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}) AS __reflex_rn FROM {}) __sub WHERE __reflex_rn = 1",
            output_cols.join(", "),
            partition_by,
            order_by,
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
    // --- End DISTINCT ON decomposition ---

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

    // Reject mixed queries: COUNT(DISTINCT) + other aggregates (SUM, AVG, MIN, MAX, BOOL_OR).
    // COUNT(DISTINCT) uses a compound intermediate key (grp, val) which is incompatible
    // with regular aggregates that use (grp) as the key.
    let has_cd = analysis.select_columns.iter()
        .any(|c| matches!(c.aggregate, Some(crate::sql_analyzer::AggregateKind::CountDistinct)));
    let has_other_agg = analysis.select_columns.iter()
        .any(|c| matches!(c.aggregate, Some(ref k) if !matches!(k,
            crate::sql_analyzer::AggregateKind::CountDistinct)));
    if has_cd && has_other_agg {
        return "ERROR: COUNT(DISTINCT col) cannot be mixed with other aggregates in the same query. \
                Use a CTE to separate them: WITH cd AS (SELECT grp, COUNT(DISTINCT col) ...) SELECT ...";
    }

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

        // Store the WHERE predicate for predicate-filtered trigger skip
        let where_predicate: String = analysis.where_clause.clone().unwrap_or_default();

        client.update(
            "INSERT INTO public.__reflex_ivm_reference
             (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
              graph_child, sql_query, base_query, end_query,
              aggregations, index_columns, unique_columns, enabled, last_update_date,
              storage_mode, refresh_mode, where_predicate)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::json, $11, $12, TRUE, NOW(), $13, $14, NULLIF($15, ''))",
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
                unsafe { DatumWithOid::new(where_predicate, PgBuiltInOids::TEXTOID.oid().value()) },
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

            // Create persistent affected-groups table (avoids DROP+CREATE per trigger fire).
            // Uses UNLOGGED for speed; lost on crash but rebuilt by reflex_reconcile.
            if !plan.group_by_columns.is_empty() || !plan.distinct_columns.is_empty() {
                let bare_view = split_qualified_name(view_name).1;
                let affected_name = format!("__reflex_affected_{}", bare_view);
                client
                    .update(
                        &format!(
                            "CREATE UNLOGGED TABLE IF NOT EXISTS \"{}\" AS SELECT {} FROM {} WHERE FALSE",
                            affected_name,
                            plan.group_by_columns.iter()
                                .chain(plan.distinct_columns.iter())
                                .map(|c| format!("\"{}\"", normalized_column_name(c)))
                                .collect::<Vec<_>>()
                                .join(", "),
                            intermediate_tbl
                        ),
                        None, &[],
                    )
                    .unwrap_or_report();
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
