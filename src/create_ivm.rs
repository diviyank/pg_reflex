use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

use crate::aggregation::{plan_aggregation, plan_aggregation_with_topk};
use crate::query_decomposer::{
    affected_groups_table_name, bare_column_name, format_pg_text_array_literal,
    generate_aggregations_json, generate_base_query, generate_end_query, intermediate_table_name,
    normalized_column_name, quote_identifier, replace_identifier, safe_identifier,
    shrunk_groups_table_name, split_qualified_name,
};
use crate::schema_builder::{
    build_deferred_flush_ddl, build_deferred_trigger_ddls, build_delta_scratch_table_ddl,
    build_indexes_ddl, build_intermediate_table_ddl, build_passthrough_scratch_ddls,
    build_staging_table_ddl, build_target_table_ddl, build_trigger_ddls, resolve_column_type,
};
use crate::sql_analyzer::{analyze, SqlAnalysisError};
use crate::validate_view_name;
use crate::window;

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_reflex_ivm_impl(
    view_name: &str,
    sql: &str,
    unique_columns_str: &str,
    if_not_exists: bool,
    storage_mode: &str,
    refresh_mode: &str,
    topk_k: Option<usize>,
    ignore_sources: &[String],
) -> &'static str {
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
            return Box::leak(format!("ERROR: Failed to parse SQL: {}", e).into_boxed_str());
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
                return Box::leak(format!("ERROR: {}", reason).into_boxed_str());
            }
            // Reject SUM(DISTINCT), AVG(DISTINCT), etc. — DISTINCT modifier is only
            // supported on COUNT. Check the original SQL for the pattern.
            let sql_upper = sql.to_uppercase();
            let has_distinct_agg = sql_upper.contains("SUM(DISTINCT")
                || sql_upper.contains("SUM (DISTINCT")
                || sql_upper.contains("AVG(DISTINCT")
                || sql_upper.contains("AVG (DISTINCT")
                || sql_upper.contains("MIN(DISTINCT")
                || sql_upper.contains("MIN (DISTINCT")
                || sql_upper.contains("MAX(DISTINCT")
                || sql_upper.contains("MAX (DISTINCT")
                || sql_upper.contains("BOOL_OR(DISTINCT")
                || sql_upper.contains("BOOL_OR (DISTINCT");
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
            let sub_name = safe_identifier(&format!("{}__union_{}", view_name, i));
            let result = create_reflex_ivm_impl(
                &sub_name,
                operand_sql,
                unique_columns_str,
                false,
                storage_mode,
                refresh_mode,
                topk_k,
                ignore_sources,
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
                     VALUES ($1, $2, $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7, $8, $9, $10::json, $11::TEXT[], $12::TEXT[], TRUE, NOW(), $13, $14)",
                    None,
                    &[
                        unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(depth, PgBuiltInOids::INT4OID.oid().value()) },
                        unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on_imv), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(format_pg_text_array_literal(&graph_child), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(view_sql.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::new(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new("{}".to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(storage_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(mode_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                ).unwrap_or_report();

                // Update sub-IMVs graph_child
                for imv_name in &depends_on_imv {
                    client
                        .update(
                            "UPDATE public.__reflex_ivm_reference
                         SET graph_child = array_append(COALESCE(graph_child, ARRAY[]::TEXT[]), $1)
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
                                        imv_name.to_string(),
                                        PgBuiltInOids::TEXTOID.oid().value(),
                                    )
                                },
                            ],
                        )
                        .unwrap_or_report();
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
                     VALUES ($1, $2, $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7, $8, $9, $10::json, $11::TEXT[], $12::TEXT[], TRUE, NOW(), $13, $14)",
                    None,
                    &[
                        unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(depth, PgBuiltInOids::INT4OID.oid().value()) },
                        unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on_imv), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(format_pg_text_array_literal(&graph_child), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(view_sql.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::new(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new("{}".to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(storage_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(mode_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                ).unwrap_or_report();

                for imv_name in &depends_on_imv {
                    client
                        .update(
                            "UPDATE public.__reflex_ivm_reference
                         SET graph_child = array_append(COALESCE(graph_child, ARRAY[]::TEXT[]), $1)
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
                                        imv_name.to_string(),
                                        PgBuiltInOids::TEXTOID.oid().value(),
                                    )
                                },
                            ],
                        )
                        .unwrap_or_report();
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
        let select_items: Vec<String> = analysis
            .select_columns
            .iter()
            .map(|c| {
                if let Some(ref alias) = c.alias {
                    format!("{} AS {}", c.expr_sql, alias)
                } else {
                    c.expr_sql.clone()
                }
            })
            .collect();
        let mut base_sql = format!(
            "SELECT {} FROM {}",
            select_items.join(", "),
            analysis.from_clause_sql
        );
        if let Some(ref wc) = analysis.where_clause {
            base_sql.push_str(&format!(" WHERE {}", wc));
        }

        // Create sub-IMV for the base data
        let base_name = format!("{}__base", view_name);
        let result = create_reflex_ivm_impl(
            &base_name,
            &base_sql,
            unique_columns_str,
            false,
            storage_mode,
            refresh_mode,
            topk_k,
            ignore_sources,
        );
        if result.starts_with("ERROR") {
            return result;
        }

        // Build the VIEW: SELECT <cols> FROM (SELECT *, ROW_NUMBER() OVER (...) AS __reflex_rn FROM base) WHERE __reflex_rn = 1
        // Strip table qualifiers — the VIEW reads from the base sub-IMV which has bare column names
        let partition_cols: Vec<String> = analysis
            .distinct_on_columns
            .iter()
            .map(|c| format!("\"{}\"", bare_column_name(c)))
            .collect();
        let partition_by = partition_cols.join(", ");

        // For ORDER BY, strip table qualifiers but preserve ASC/DESC/NULLS modifiers
        let order_parts: Vec<String> = analysis
            .order_by_exprs
            .iter()
            .map(|expr| {
                // Split on first space to separate column from modifiers (e.g., "j2.val DESC")
                let parts: Vec<&str> = expr.splitn(2, ' ').collect();
                let col = format!("\"{}\"", bare_column_name(parts[0]));
                if parts.len() > 1 {
                    format!("{} {}", col, parts[1])
                } else {
                    col
                }
            })
            .collect();
        let order_by = order_parts.join(", ");

        // Output column list (just names/aliases, no expressions)
        let output_cols: Vec<String> = analysis
            .select_columns
            .iter()
            .map(|c| {
                if let Some(ref alias) = c.alias {
                    format!("\"{}\"", alias)
                } else {
                    format!("\"{}\"", bare_column_name(&c.expr_sql))
                }
            })
            .collect();

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
                 VALUES ($1, 2, $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6, $7, $8, $9::json, $10::TEXT[], $11::TEXT[], TRUE, NOW(), $12, $13)",
                None,
                &[
                    unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on_imv), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(view_sql.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::new(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new("{}".to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(storage_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(mode_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            ).unwrap_or_report();

            // Update base IMV's graph_child
            for name in &depends_on_imv {
                client
                    .update(
                        "UPDATE public.__reflex_ivm_reference
                     SET graph_child = array_append(COALESCE(graph_child, ARRAY[]::TEXT[]), $1)
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
                                    name.to_string(),
                                    PgBuiltInOids::TEXTOID.oid().value(),
                                )
                            },
                        ],
                    )
                    .unwrap_or_report();
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
            &base_name,
            &decomp.base_query,
            unique_columns_str,
            false,
            storage_mode,
            refresh_mode,
            topk_k,
            ignore_sources,
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
                 VALUES ($1, 2, $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6, $7, $8, $9::json, $10::TEXT[], $11::TEXT[], TRUE, NOW(), $12, $13)",
                None,
                &[
                    unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on_imv), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(view_sql.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::new(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new("{}".to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(String::from("{}"), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(storage_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(mode_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            ).unwrap_or_report();

            // Update base IMV's graph_child
            for name in &depends_on_imv {
                client
                    .update(
                        "UPDATE public.__reflex_ivm_reference
                     SET graph_child = array_append(COALESCE(graph_child, ARRAY[]::TEXT[]), $1)
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
                                    name.to_string(),
                                    PgBuiltInOids::TEXTOID.oid().value(),
                                )
                            },
                        ],
                    )
                    .unwrap_or_report();
            }
        });

        return "CREATE REFLEX INCREMENTAL VIEW";
    }
    // --- End window function decomposition ---

    // Reject subqueries with aggregation in FROM — the trigger replaces the inner table
    // with the transition table, so inner aggregations would only see delta rows.
    let has_subquery_with_agg = analysis.sources.iter().any(|s| s.starts_with("<subquery:"))
        && analysis.from_clause_sql.to_uppercase().contains("GROUP BY");
    if has_subquery_with_agg {
        return "ERROR: Subqueries with aggregation in FROM are not supported. \
                Use a CTE (WITH clause) instead — pg_reflex decomposes CTEs into sub-IMVs automatically.";
    }

    // --- CTE decomposition: each CTE becomes its own sub-IMV ---
    if !analysis.ctes.is_empty() {
        let mut cte_name_map: Vec<(String, String)> = Vec::new();

        for cte in &analysis.ctes {
            let alias_lower = cte.alias.to_lowercase();
            if alias_lower.starts_with("__reflex_new_")
                || alias_lower.starts_with("__reflex_old_")
                || alias_lower.starts_with("__reflex_delta_")
            {
                return "ERROR: CTE alias conflicts with pg_reflex reserved prefix (__reflex_new_/old_/delta_)";
            }

            // Rewrite references to earlier CTEs in this CTE's query
            let mut cte_query = cte.query_sql.clone();
            for (earlier_alias, earlier_imv) in &cte_name_map {
                let quoted = quote_identifier(earlier_imv);
                cte_query = replace_identifier(&cte_query, earlier_alias, &quoted);
            }

            let cte_view_name = safe_identifier(&format!("{}__cte_{}", view_name, cte.alias));
            let result = create_reflex_ivm_impl(
                &cte_view_name,
                &cte_query,
                "",
                false,
                storage_mode,
                refresh_mode,
                topk_k,
                ignore_sources,
            );
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
                let quoted = quote_identifier(cte_imv_name);
                body = replace_identifier(&body, cte_alias, &quoted);
            }
            body
        } else {
            return "ERROR: Query is not a SELECT";
        };

        // Check if the main body is passthrough (no aggregation).
        // If so, all its sources are CTE sub-IMVs which don't get triggers,
        // CTE body (passthrough or aggregate) → create as a normal IMV
        return create_reflex_ivm_impl(
            view_name,
            &body_sql,
            "",
            false,
            storage_mode,
            refresh_mode,
            topk_k,
            ignore_sources,
        );
    }
    // --- End CTE decomposition ---

    let froms = analysis.sources.clone();

    // Build aggregation plan from the analysis. When the caller asked for top-K
    // MIN/MAX, propagate it here so MIN/MAX intermediate columns gain a
    // companion top-K array column (`__min_x_topk` / `__max_x_topk`).
    let mut plan = if topk_k.is_some() {
        plan_aggregation_with_topk(&analysis, topk_k)
    } else {
        plan_aggregation(&analysis)
    };

    // Reject mixed queries: COUNT(DISTINCT) + other aggregates (SUM, AVG, MIN, MAX, BOOL_OR).
    // COUNT(DISTINCT) uses a compound intermediate key (grp, val) which is incompatible
    // with regular aggregates that use (grp) as the key.
    let has_cd = analysis.select_columns.iter().any(|c| {
        matches!(
            c.aggregate,
            Some(crate::sql_analyzer::AggregateKind::CountDistinct)
        )
    });
    let has_other_agg = analysis.select_columns.iter().any(|c| {
        matches!(c.aggregate, Some(ref k) if !matches!(k,
            crate::sql_analyzer::AggregateKind::CountDistinct))
    });
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
                .map(|s| normalized_column_name(s.trim()))
                .filter(|s| !s.is_empty())
                .collect();
            plan.passthrough_columns = resolved_unique_columns.clone();
            info!(
                "pg_reflex: using explicit unique key ({}) for '{}'",
                resolved_unique_columns.join(", "),
                view_name
            );

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
                            resolved_unique_columns
                                .iter()
                                .map(|c| (c.clone(), c.clone()))
                                .collect(),
                        );
                        info!(
                            "pg_reflex: auto-detected PK ({}) from '{}' for '{}'",
                            resolved_unique_columns.join(", "),
                            source,
                            view_name
                        );
                        break;
                    } else {
                        info!(
                            "pg_reflex: source '{}' has PK ({}) but the SELECT list does not include all PK columns — \
                             passthrough '{}' will fall back to row-matching for DELETE/UPDATE. \
                             Add the PK columns to the SELECT list, or pass them as the 3rd argument to create_reflex_ivm.",
                            source,
                            pk_lower.join(", "),
                            view_name
                        );
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

    // 1.4.6 — populate `source_join_keys` for aggregate plans. Identifies
    // sources where Item α's directional promotion can short-circuit to
    // bulk-INSERT (OUT→IN) or bulk-DELETE (IN→OUT) without per-row MERGE
    // probing. See `build_source_join_keys` for the safety gates.
    if !plan.is_passthrough && is_join_query {
        build_source_join_keys(&mut plan, &real_sources, &analysis);
    }

    // Warn about select columns that are neither GROUP BY nor recognized aggregates.
    // Note: passthrough columns not explicitly in GROUP BY are auto-added by plan_aggregation,
    // so we use the plan's group_by_columns (which includes auto-added ones) for validation.
    if !plan.is_passthrough {
        let group_by_set: std::collections::HashSet<&str> =
            plan.group_by_columns.iter().map(|s| s.as_str()).collect();
        for col in &analysis.select_columns {
            if !col.is_passthrough && col.aggregate.is_none() && !col.is_aggregate_derived {
                warning!(
                    "pg_reflex: unsupported expression '{}' in SELECT — column will be missing from IMV '{}'",
                    col.alias.as_deref().unwrap_or(&col.expr_sql),
                    view_name
                );
            } else if col.is_passthrough
                && !group_by_set.contains(col.expr_sql.as_str())
                && !analysis.has_distinct
            {
                // Passthrough column not in GROUP BY — likely an unrecognized aggregate or expression.
                // Match on the underlying expression, not the output alias: `src.col AS renamed`
                // is still a valid grouped passthrough if `col` (bare) is in GROUP BY.
                let bare = bare_column_name(&col.expr_sql);
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
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
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

    // Bug #10: reject creation if it would form a cycle in the dependency DAG.
    // Traverse depends_on edges reachable from froms; if view_name appears, it's a cycle.
    // UNION (not UNION ALL) prevents infinite loops when the existing graph already has cycles.
    //
    // We deliberately avoid `.first().get_one::<bool>()` here: in pgrx 0.18 that path
    // calls `PgMemoryContexts::CurrentMemoryContext.parent()`, which segfaults when
    // CurrentMemoryContext is NULL in this call context (observed on PG 17.7). Instead
    // we project the match rows directly and check whether the result set is non-empty,
    // mirroring the duplicate-name probe above (line 776).
    let cycle_detected = if froms.is_empty() {
        false
    } else {
        Spi::connect(|client| {
            let args = [
                unsafe {
                    DatumWithOid::new(
                        format_pg_text_array_literal(&froms),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                },
                unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
            ];
            !client
                .select(
                    "WITH RECURSIVE dep_graph(dep) AS (\
                        SELECT unnest(depends_on) \
                        FROM public.__reflex_ivm_reference \
                        WHERE name = ANY($1::TEXT[]) \
                        UNION \
                        SELECT unnest(r.depends_on) \
                        FROM dep_graph dg \
                        JOIN public.__reflex_ivm_reference r ON r.name = dg.dep \
                    ) \
                    SELECT 1 FROM dep_graph WHERE dep = $2 LIMIT 1",
                    Some(1),
                    &args,
                )
                .unwrap_or_report()
                .collect::<Vec<_>>()
                .is_empty()
        })
    };
    if cycle_detected {
        return "ERROR: circular dependency detected — this IMV would form a cycle in the dependency graph";
    }

    Spi::connect_mut(|client| {
        // Lookup existing IMVs among the source tables
        let args = [unsafe {
            DatumWithOid::new(
                format_pg_text_array_literal(&froms),
                PgBuiltInOids::TEXTOID.oid().value(),
            )
        }];

        let matching_froms = client
            .select(
                "SELECT name, graph_depth FROM public.__reflex_ivm_reference WHERE name = ANY($1::TEXT[])",
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

        // 1.5.1 — Filter `imv_relevant_columns` down to columns that
        // actually exist per source. The analyzer over-attributes bare
        // identifiers in multi-source queries to every real source as a
        // safe-correctness move; without this filter the persisted JSON
        // would name columns that don't exist on the source's transition
        // table (e.g. `sales_simulation.dem_plan_id` getting wrongly
        // attributed to `demand_planning` too in a join IMV), and the
        // skip SQL would error at trigger fire time with
        // `column "X" does not exist`.
        //
        // The filter must run for BOTH passthrough and aggregate IMVs.
        // (Pre-1.5.1 this only ran in the aggregate branch — passthrough
        // IMVs with bare projections crashed at the first UPDATE.) The
        // aggregate branch below re-fetches the catalog info (it needs
        // column_types + not_null_cols too), which is a tiny duplicate
        // SPI per IMV — acceptable for the simplicity of a single fix
        // site that covers both paths.
        {
            let (_t, _nn, per_source_cols_for_filter) =
                query_column_types_from_catalog_with_per_source(client, &froms);
            for (source, cols) in plan.imv_relevant_columns.iter_mut() {
                if let Some(actual) = per_source_cols_for_filter.get(source) {
                    cols.retain(|c| actual.contains(c.as_str()));
                } else if source.starts_with('<') {
                    cols.clear();
                }
            }
            plan.imv_relevant_columns.retain(|_, v| !v.is_empty());
        }

        if plan.is_passthrough {
            // Passthrough: CREATE TABLE AS — Postgres infers columns + types, populates data
            let create_kw = if logged {
                "CREATE TABLE"
            } else {
                "CREATE UNLOGGED TABLE"
            };
            client
                .update(
                    &format!("{} {} AS {}", create_kw, quote_identifier(view_name), sql),
                    None,
                    &[],
                )
                .unwrap_or_report();
            // ANALYZE so the query planner has statistics for the new table
            client
                .update(
                    &format!("ANALYZE {}", quote_identifier(view_name)),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // Create unique index on target for resolved unique key columns
            if !resolved_unique_columns.is_empty() {
                let bare_view = split_qualified_name(view_name).1;
                let uk_cols: Vec<String> = resolved_unique_columns
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect();
                client
                    .update(
                        &format!(
                            "CREATE UNIQUE INDEX IF NOT EXISTS \"__reflex_uk_{}\" ON {} ({})",
                            bare_view,
                            quote_identifier(view_name),
                            uk_cols.join(", ")
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            }

            // Passthrough scratch tables: one new-side + one old-side per real source.
            // Populated at trigger time from the transition tables so downstream DML
            // (DELETE ... WHERE IN (SELECT FROM transition), INSERT ... SELECT FROM transition)
            // reads from a plain table, not a transition table — avoids the nested-trigger
            // transition-table-in-EXECUTE assertion.
            for source in &froms {
                if source.starts_with('<') {
                    continue;
                }
                for ddl in build_passthrough_scratch_ddls(view_name, source) {
                    client.update(&ddl, None, &[]).unwrap_or_report();
                }
            }
        } else {
            // Aggregate: build intermediate + target tables from the plan
            let (mut column_types, not_null_cols, per_source_cols) =
                query_column_types_from_catalog_with_per_source(client, &froms);
            plan.optimize_not_null_sums(&not_null_cols);
            // 1.4.5 — filter `imv_relevant_columns` down to columns that
            // actually exist per source. The analyzer over-attributes bare
            // identifiers in multi-source queries to every real source as a
            // safe-correctness move; without this filter the persisted JSON
            // would name columns that don't exist on the source's transition
            // table, and the skip SQL would error at trigger fire time.
            for (source, cols) in plan.imv_relevant_columns.iter_mut() {
                if let Some(actual) = per_source_cols.get(source) {
                    cols.retain(|c| actual.contains(c.as_str()));
                } else if source.starts_with('<') {
                    cols.clear();
                }
            }
            plan.imv_relevant_columns.retain(|_, v| !v.is_empty());

            // Discover actual types for computed expressions by introspecting query output.
            // 1. Base query types: resolves GROUP BY expressions (DATE_TRUNC, EXTRACT, etc.)
            let base_q_for_types = generate_base_query(&analysis, &plan);
            augment_column_types_from_query(&base_q_for_types, &mut column_types);
            // 2. Original SQL types: resolves aggregate output types (SUM(int)→BIGINT, etc.)
            augment_column_types_from_query(sql, &mut column_types);

            // Fix intermediate SUM column types: use DOUBLE PRECISION instead of NUMERIC
            // when the base_query produces DOUBLE PRECISION (preserves float arithmetic path).
            for ic in &mut plan.intermediate_columns {
                if ic.source_aggregate == "SUM" {
                    let base_type = resolve_column_type(&ic.name, &column_types, "").to_uppercase();
                    if base_type == "DOUBLE PRECISION" {
                        ic.pg_type = "DOUBLE PRECISION".to_string();
                    }
                }
            }

            // Fix intermediate MIN/MAX column types from the resolved source-column
            // catalog type. Without this, `pg_type` stays at the planner default
            // ("NUMERIC") and trigger codegen emits `'{}'::NUMERIC[]` for the
            // top-K array even when the actual column is TEXT[] / DATE[] /
            // TIMESTAMP[]. The schema builder already special-cases this for
            // DDL, but the trigger MERGE statements read `pg_type` directly.
            for ic in &mut plan.intermediate_columns {
                if (ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX")
                    && ic.pg_type.eq_ignore_ascii_case("NUMERIC")
                {
                    let resolved = resolve_column_type(&ic.source_arg, &column_types, "");
                    if !resolved.is_empty() && !resolved.eq_ignore_ascii_case("NUMERIC") {
                        ic.pg_type = resolved;
                    }
                }
            }

            // Set cast_type on end_query_mappings so the end_query casts intermediate
            // to the correct target type (e.g., BIGINT for SUM(int)).
            for mapping in &mut plan.end_query_mappings {
                if mapping.cast_type.is_none() {
                    let discovered = resolve_column_type(&mapping.output_alias, &column_types, "");
                    if !discovered.is_empty() {
                        let default_type = match mapping.aggregate_type.as_str() {
                            "SUM" | "AVG" | "DERIVED" => "NUMERIC",
                            "COUNT" => "BIGINT",
                            "BOOL_OR" => "BOOLEAN",
                            _ => "",
                        };
                        // Only set cast if discovered type differs from the intermediate default
                        if !default_type.is_empty() && discovered.to_uppercase() != default_type {
                            mapping.cast_type = Some(discovered);
                        }
                    }
                }
            }

            if let Some(ddl) = build_intermediate_table_ddl(view_name, &plan, &column_types, logged)
            {
                let tbl = intermediate_table_name(view_name);
                client.update(&ddl, None, &[]).unwrap_or_report();
                unlogged_tables.push(tbl);
                // Delta scratch table: materialized intermediate for MERGE (avoids
                // transition-table-in-EXECUTE SIGABRT). Always UNLOGGED, no indexes.
                if let Some(scratch_ddl) =
                    build_delta_scratch_table_ddl(view_name, &plan, &column_types)
                {
                    client.update(&scratch_ddl, None, &[]).unwrap_or_report();
                }
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
                    source,
                    view_name
                );
                continue;
            }
            if source.starts_with('<') {
                continue;
            }

            // 1.4.5: ignore_sources — operator-requested exclusion of trigger
            // installation on listed sources. Matches both schema-qualified
            // ('alp.product') and bare ('product') forms against the IMV's
            // depends_on entry.
            let (_, source_bare) = split_qualified_name(source);
            if ignore_sources
                .iter()
                .any(|s| s == source || s == source_bare)
            {
                info!(
                    "pg_reflex: skipping trigger install on source '{}' for IMV '{}' (ignored)",
                    source, view_name
                );
                continue;
            }

            // Check if source is a materialized view (can't have triggers).
            // Use to_regclass() which respects the current search_path.
            let is_matview = client
                .select(
                    "SELECT 1 FROM pg_class WHERE oid = to_regclass($1) AND relkind = 'm'",
                    None,
                    &[unsafe {
                        DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .unwrap_or_report()
                .next()
                .is_some();

            if is_matview {
                warning!(
                    "pg_reflex: source '{}' is a materialized view — triggers skipped. \
                     Use SELECT refresh_imv_depending_on('{}') after REFRESH MATERIALIZED VIEW.",
                    source,
                    source
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
                let has_any_deferred = deferred
                    || {
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
        let has_min_max = plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX");
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

                let idx_cols: Vec<String> = plan
                    .group_by_columns
                    .iter()
                    .map(|c| normalized_column_name(c))
                    .filter(|c| source_cols.contains(c))
                    .map(|c| format!("\"{}\"", c))
                    .collect();

                if idx_cols.is_empty() {
                    continue;
                }
                let safe_src = source.replace('.', "_");
                let bare_view = split_qualified_name(view_name).1;
                let idx_name = safe_identifier(&format!("__reflex_idx_{}_{}", bare_view, safe_src));
                let ddl = format!(
                    "CREATE INDEX IF NOT EXISTS \"{}\" ON {} ({})",
                    idx_name,
                    source,
                    idx_cols.join(", ")
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
        // 1.4.5 — `imv_relevant_columns` carries a possibly-over-inclusive
        // set of columns per source (the analyzer conservatively attributes
        // bare identifiers in multi-source queries to every real source).
        // The trigger codegen INTERSECTS this with the source's actual
        // columns at fire time via pg_attribute, so a column listed here
        // that doesn't exist on the source is simply ignored — no need to
        // filter at IMV-create time, which would have to compete with
        // catalog snapshot visibility for the just-created source table.
        let aggregations_json = generate_aggregations_json(&plan);
        let index_columns: Vec<String> = plan
            .group_by_columns
            .iter()
            .chain(plan.distinct_columns.iter())
            .map(|c| {
                if let Some(alias) = plan.group_by_aliases.get(c) {
                    normalized_column_name(alias)
                } else {
                    normalized_column_name(c)
                }
            })
            .collect();

        // INSERT into reference table
        let depends_on: Vec<String> = froms.clone();
        let depends_on_imv: Vec<String> = ivm_froms.clone();
        let graph_child: Vec<String> = Vec::new();

        // Store the WHERE predicate for predicate-filtered trigger skip.
        // Only safe for single-source queries: multi-table WHERE clauses may reference
        // joined tables whose columns are not available in the trigger transition table.
        let where_predicate: String = if real_sources.len() <= 1 {
            analysis.where_clause.clone().unwrap_or_default()
        } else {
            String::new()
        };

        let ignored_sources_vec: Vec<String> = ignore_sources.to_vec();
        client.update(
            "INSERT INTO public.__reflex_ivm_reference
             (name, graph_depth, depends_on, depends_on_imv, unlogged_tables,
              graph_child, sql_query, base_query, end_query,
              aggregations, index_columns, unique_columns, enabled, last_update_date,
              storage_mode, refresh_mode, where_predicate, ignored_sources)
             VALUES ($1, $2, $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[], $7, $8, $9, $10::jsonb, $11::TEXT[], $12::TEXT[], TRUE, NOW(), $13, $14, NULLIF($15, ''), $16::TEXT[])",
            None,
            &[
                unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(depth, PgBuiltInOids::INT4OID.oid().value()) },
                unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(format_pg_text_array_literal(&depends_on_imv), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(format_pg_text_array_literal(&unlogged_tables), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(format_pg_text_array_literal(&graph_child), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(sql.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(base_query, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(end_query.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(aggregations_json, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(format_pg_text_array_literal(&index_columns), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(format_pg_text_array_literal(&resolved_unique_columns), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(storage_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(mode_upper.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(where_predicate, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(format_pg_text_array_literal(&ignored_sources_vec), PgBuiltInOids::TEXTOID.oid().value()) },
            ],
        ).unwrap_or_report();

        // Update source IMVs with the new child in their graph_child field
        for imv_name in &ivm_froms {
            client
                .update(
                    "UPDATE public.__reflex_ivm_reference
                 SET graph_child = array_append(COALESCE(graph_child, ARRAY[]::TEXT[]), $1)
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
                                imv_name.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                    ],
                )
                .unwrap_or_report();
        }

        // Initial materialization (skip for passthrough — CREATE TABLE AS already populated)
        if !plan.is_passthrough {
            let intermediate_tbl = intermediate_table_name(view_name);
            let base_q = generate_base_query(&analysis, &plan);
            let initial_insert = format!("INSERT INTO {} {}", intermediate_tbl, base_q);
            client.update(&initial_insert, None, &[]).unwrap_or_report();

            let target_insert =
                format!("INSERT INTO {} {}", quote_identifier(view_name), end_query);
            client.update(&target_insert, None, &[]).unwrap_or_report();

            // Create indexes AFTER bulk insert (much faster than indexing during insert)
            for index_ddl in build_indexes_ddl(view_name, &plan) {
                client.update(&index_ddl, None, &[]).unwrap_or_report();
            }

            // Create persistent affected-groups table (avoids DROP+CREATE per trigger fire).
            // Uses UNLOGGED for speed; lost on crash but rebuilt by reflex_reconcile.
            // Co-located in the IMV's schema (1.4.1) so SQL works under any `search_path`.
            if !plan.group_by_columns.is_empty() || !plan.distinct_columns.is_empty() {
                let group_cols_csv = plan
                    .group_by_columns
                    .iter()
                    .chain(plan.distinct_columns.iter())
                    .map(|c| format!("\"{}\"", normalized_column_name(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let affected_ref = affected_groups_table_name(view_name);
                client
                    .update(
                        &format!(
                            "CREATE UNLOGGED TABLE IF NOT EXISTS {} AS SELECT {} FROM {} WHERE FALSE",
                            affected_ref,
                            group_cols_csv,
                            intermediate_tbl
                        ),
                        None, &[],
                    )
                    .unwrap_or_report();

                // N1: per-IMV "shrunk groups" capture table — populated post-Sub
                // on UPDATE for top-K MIN/MAX IMVs to scope the forced recompute
                // to groups whose heap actually shrank below K. Provisioned only
                // when the plan has any top-K column; non-top-K IMVs leave it
                // unallocated.
                let has_topk = plan.intermediate_columns.iter().any(|ic| ic.has_topk());
                if has_topk {
                    let shrunk_ref = shrunk_groups_table_name(view_name);
                    client
                        .update(
                            &format!(
                                "CREATE UNLOGGED TABLE IF NOT EXISTS {} AS SELECT {} FROM {} WHERE FALSE",
                                shrunk_ref,
                                group_cols_csv,
                                intermediate_tbl
                            ),
                            None, &[],
                        )
                        .unwrap_or_report();
                }
            }

            // ANALYZE so the query planner has accurate statistics
            client
                .update(&format!("ANALYZE {}", intermediate_tbl), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!("ANALYZE {}", quote_identifier(view_name)),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // 1.4.5: data-probe pass. Scan the intermediate for group-by /
            // distinct columns whose actual data is NULL-free, and add them
            // to not_null_columns. Catches the case where a catalog-NULLable
            // column is effectively NOT NULL because the IMV's INNER JOIN
            // or filter semantics exclude NULLs. Without this, the trigger's
            // MERGE codegen would emit `IS NOT DISTINCT FROM` on the
            // composite-index leading column, defeating the index — the
            // 405 s yse.ivm_sop_forecast_view regression in 1.4.4.
            let probed_nn = probe_not_null_columns_from_data(client, &intermediate_tbl, &plan);
            let new_cols: Vec<String> = probed_nn
                .into_iter()
                .filter(|c| !plan.not_null_columns.contains(c))
                .collect();
            if !new_cols.is_empty() {
                for c in &new_cols {
                    plan.not_null_columns.insert(c.clone());
                }
                persist_probed_not_null_columns(client, view_name, &new_cols);
                info!(
                    "pg_reflex: data-probe added {} effectively-NOT-NULL column(s) to '{}': {:?}",
                    new_cols.len(),
                    view_name,
                    new_cols
                );
            }
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
        let target_name = col.alias.as_deref().unwrap_or(&col.expr_sql);
        let target_name = normalized_column_name(target_name);
        target_col_to_expr.insert(target_name, col.expr_sql.clone());
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
                        .map(|expr| normalized_column_name(expr))
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

/// 1.4.6 — Build the per-source JOIN-key mapping for aggregate plans.
///
/// For each source in the IMV, find JOIN equalities where the source's
/// column equals another table's column AND the other column projects to
/// a GROUP BY column of the intermediate. Then, gate on:
///   * **All** equalities involving the source map to a GROUP BY column
///     (any partial mapping → unsafe, falls back to MERGE).
///   * The mapped source columns cover a UNIQUE key of the source table
///     (PK or unique index). Without a unique key, multiple source rows
///     could share the JOIN-col values and collide in bulk-INSERT.
///
/// Sets `plan.source_join_keys[source] = Vec<(intermediate_col, source_col)>`
/// only when both gates pass. Empty / missing entry → standard MERGE path.
///
/// Skipped for passthrough plans (they use `passthrough_key_mappings`).
pub(crate) fn build_source_join_keys(
    plan: &mut crate::aggregation::AggregationPlan,
    sources: &[&String],
    analysis: &crate::sql_analyzer::SqlAnalysis,
) {
    use std::collections::{HashMap, HashSet};

    if plan.is_passthrough {
        return;
    }

    // 1) Build the lowered set of GROUP BY column names and a target→expr
    //    map so `parse_join_condition_mappings` can recognize "other side"
    //    references whether they're bare (`dem_plan_id`) or qualified
    //    (`sales_simulation.dem_plan_id`).
    let group_by_lowercased: Vec<String> = plan
        .group_by_columns
        .iter()
        .map(|c| normalized_column_name(c).to_lowercase())
        .collect();
    let group_by_set: HashSet<String> = group_by_lowercased.iter().cloned().collect();

    let mut target_col_to_expr: HashMap<String, String> = HashMap::new();
    for col in &analysis.select_columns {
        let target_name = col.alias.as_deref().unwrap_or(&col.expr_sql);
        let target_name = bare_column_name(target_name).to_lowercase();
        if group_by_set.contains(&target_name) {
            target_col_to_expr.insert(target_name, col.expr_sql.to_lowercase());
        }
    }

    // 2) For each source, look only at the JOIN that *introduces* this
    //    source into the FROM clause — i.e. `analysis.joins[i]` with
    //    target_table matching `source`. Other JOINs may reference this
    //    source's columns incidentally (e.g. `pricing` joining on
    //    `demand_planning.assortment_id`), but those say nothing about
    //    THIS source's identity. Counting them inflates the
    //    "all-equalities-mapped" denominator and spuriously refuses the
    //    bulk path.
    let source_bare_lc: std::collections::HashMap<&String, String> = sources
        .iter()
        .map(|s| (*s, bare_column_name(s).to_lowercase()))
        .collect();
    for source in sources {
        let source_str = source.as_str();
        let source_lc = source_str.to_lowercase();
        let source_bare = source_bare_lc.get(source).cloned().unwrap_or_default();

        let mut all_mappings: Vec<(String, String)> = Vec::new();
        let mut all_equalities_count: usize = 0;
        for join in &analysis.joins {
            let target_lc = join.target_table.to_lowercase();
            let target_bare = bare_column_name(&join.target_table).to_lowercase();
            let target_matches = target_lc == source_lc
                || target_bare == source_bare
                || target_lc == source_bare
                || target_bare == source_lc;
            if !target_matches {
                continue;
            }
            if let Some(ref cond) = join.condition_sql {
                let n_eq =
                    count_equalities_involving_source(cond, source_str, &analysis.table_aliases);
                if n_eq == 0 {
                    continue;
                }
                all_equalities_count += n_eq;
                let join_mappings = parse_join_condition_mappings(
                    cond,
                    source_str,
                    &analysis.table_aliases,
                    &group_by_lowercased,
                    &target_col_to_expr,
                );
                all_mappings.extend(join_mappings);
            }
        }

        if all_mappings.is_empty() || all_equalities_count == 0 {
            continue;
        }

        // Dedup mappings before counting coverage.
        all_mappings.sort();
        all_mappings.dedup();

        // Gate A: every JOIN equality involving the source must map to a
        // GROUP BY column. Partial mappings (some equality components map,
        // others don't) leave un-pinned dimensions on the source side, so
        // a single source row's identity doesn't fully determine its
        // intermediate group keys → bulk path unsafe.
        if all_mappings.len() < all_equalities_count {
            continue;
        }

        // Gate B: the source-side columns of the mapping must cover a
        // UNIQUE key on the source table. Otherwise multiple source rows
        // can produce the same mapped values, breaking the "transition row
        // uniquely identifies a slice of intermediate" assumption.
        let source_cols: Vec<String> = all_mappings.iter().map(|(_, sc)| sc.clone()).collect();
        if !source_cols_cover_unique_key(source_str, &source_cols) {
            continue;
        }

        plan.source_join_keys
            .insert(source_str.to_string(), all_mappings);
    }
}

/// Count how many "x = y" equalities in a JOIN ON clause reference
/// `source_table` on one side. Conservative — only counts top-level
/// AND-joined equality predicates (the same shape
/// `parse_join_condition_mappings` understands). Stand-alone helper so
/// the safety gate ("did all equalities map?") can compare against a
/// concrete denominator.
pub(crate) fn count_equalities_involving_source(
    condition: &str,
    source_table: &str,
    table_aliases: &std::collections::HashMap<String, String>,
) -> usize {
    let source_lower = source_table.to_lowercase();
    let source_bare = bare_column_name(source_table).to_lowercase();
    let source_aliases: Vec<String> = table_aliases
        .iter()
        .filter(|(_, table)| table.to_lowercase() == source_lower)
        .map(|(alias, _)| alias.to_lowercase())
        .collect();

    let mut count = 0;
    // Case-insensitive AND split: lowercase first, split once. Avoids the
    // double-iteration footgun that existed in the legacy
    // `parse_join_condition_mappings` (now fixed).
    let condition_lower = condition.to_lowercase();
    for part in condition_lower.split(" and ") {
        let part = part.trim();
        let sides: Vec<&str> = part.splitn(2, '=').collect();
        if sides.len() != 2 {
            continue;
        }
        let left = sides[0].trim();
        let right = sides[1].trim();
        if is_from_table(left, &source_bare, &source_aliases)
            || is_from_table(right, &source_bare, &source_aliases)
        {
            count += 1;
        }
    }
    count
}

/// Query pg_catalog to confirm `cols` cover at least one UNIQUE/PK index
/// on `source`. Returns `true` if any unique index has all of its columns
/// contained in `cols` (case-insensitive). Falls back to `false` on any
/// catalog access error — safe to refuse the bulk path on doubt.
fn source_cols_cover_unique_key(source: &str, cols: &[String]) -> bool {
    let (schema, name) = split_qualified_name(source);
    let schema_str = schema.unwrap_or("public");
    let cols_lower: std::collections::HashSet<String> =
        cols.iter().map(|c| c.to_lowercase()).collect();

    let unique_indexes: Vec<Vec<String>> = Spi::connect(|client| {
        client
            .select(
                "SELECT array_agg(a.attname::TEXT ORDER BY k.n) AS cols \
                 FROM pg_index ix \
                 JOIN pg_class t ON t.oid = ix.indrelid \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, n) ON true \
                 JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.col \
                 WHERE n.nspname = $1 AND t.relname = $2 AND ix.indisunique \
                 GROUP BY ix.indexrelid",
                None,
                &[
                    unsafe {
                        DatumWithOid::new(
                            schema_str.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe {
                        DatumWithOid::new(name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<Vec<String>, _>("cols").unwrap_or(None))
            .collect()
    });

    unique_indexes.iter().any(|idx_cols| {
        idx_cols
            .iter()
            .all(|c| cols_lower.contains(&c.to_lowercase()))
    })
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

    // Case-insensitive split on AND: lowercase the condition first, then split
    // only on the lowercase form. The previous chain-of-two-splits formulation
    // produced spurious whole-condition iterations when only one of "AND" /
    // "and" was present in the source string (caught by
    // pg_test_source_join_keys_skipped_when_composite_join_partial_map).
    let condition_lower = condition.to_lowercase();
    for part in condition_lower.split(" and ") {
        let part = part.trim();
        let sides: Vec<&str> = part.splitn(2, '=').collect();
        if sides.len() != 2 {
            continue;
        }
        let left = sides[0].trim().to_string();
        let right = sides[1].trim().to_string();

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

/// Return type of [`query_column_types_from_catalog_with_per_source`]:
/// `(types, not_null_cols, per_source_columns)`.
type CatalogColumnInfo = (
    HashMap<String, String>,
    std::collections::HashSet<String>,
    HashMap<String, std::collections::HashSet<String>>,
);

/// Per-source-aware column-type catalog lookup. Returns the cross-source
/// type map and not-null set used elsewhere, plus a per-source map
/// `source-table-name → set of catalog-known column names`.
///
/// Used by the 1.4.5 filter-aware spurious-skip metadata to ground the
/// analyzer's over-inclusive `imv_relevant_columns` (which attributes
/// bare idents to every real source) in the actual catalog shape, so the
/// persisted JSON only ever names columns that exist on the named source.
fn query_column_types_from_catalog_with_per_source(
    client: &pgrx::spi::SpiClient<'_>,
    table_names: &[String],
) -> CatalogColumnInfo {
    let mut types = HashMap::new();
    let mut not_null_cols = std::collections::HashSet::new();
    let mut per_source: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
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
                "SELECT column_name::text AS col_name, data_type::text AS data_type, \
                        is_nullable::text AS is_nullable \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2",
                None,
                &[
                    unsafe {
                        DatumWithOid::new(schema.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    },
                    unsafe {
                        DatumWithOid::new(tbl.to_string(), PgBuiltInOids::TEXTOID.oid().value())
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

                // Track NOT NULL columns for SUM optimization
                let is_nullable = row
                    .get_by_name::<String, _>("is_nullable")
                    .unwrap_or(None)
                    .unwrap_or_default();
                if is_nullable == "NO" {
                    not_null_cols.insert(col_name.to_string());
                }
                per_source
                    .entry(table.clone())
                    .or_default()
                    .insert(col_name.clone());
            }
        }
    }
    (types, not_null_cols, per_source)
}

/// 1.4.5: data-probe. Scan the populated intermediate for group-by and
/// distinct columns whose actual data contains zero NULLs. Returns the set
/// of normalized column names found to be NULL-free.
///
/// Closes the gap left by the pure catalog heuristic
/// `query_column_types_from_catalog`: a column declared NULLable in
/// `information_schema.columns` may still be effectively NOT NULL on the
/// intermediate if the base_query's INNER JOIN keys or filter predicates
/// exclude NULLs. The probe runs *after* the bulk INSERT into intermediate
/// so it sees the real data.
///
/// Customer-reported regression (yse.ivm_sop_forecast_view, 1.4.4): catalog
/// declared `sales_simulation.dem_plan_id / product_id / location_id`
/// NULLable; the IMV's INNER JOIN on `dem_plan_id = demand_planning.id`
/// makes those columns non-NULL on the join output. Without the probe, the
/// MERGE codegen emits `IS NOT DISTINCT FROM` on the composite-index
/// leading column, defeating the index. Symptom: 405 s UPDATE on a 1-row
/// source change.
///
/// Trade-off: one EXISTS scan per group-by column at create time, each
/// short-circuiting on the first matching NULL. On a NULL-free 867 K
/// composite key column the scan touches the index leading-column once
/// per page (~50 ms total for 8 columns). Trivial relative to the
/// alternative (the 405 s blowup on first UPDATE).
fn probe_not_null_columns_from_data(
    client: &mut pgrx::spi::SpiClient<'_>,
    intermediate_tbl: &str,
    plan: &crate::aggregation::AggregationPlan,
) -> std::collections::HashSet<String> {
    let mut probed = std::collections::HashSet::new();
    for col_expr in plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
    {
        let norm = normalized_column_name(col_expr);
        if plan.not_null_columns.contains(&norm) {
            continue;
        }
        // Quote the column name with " escaping (defensive — normalized
        // names are bare ASCII in practice, but the input came from user SQL).
        let quoted = norm.replace('"', "\"\"");
        let sql = format!(
            "SELECT NOT EXISTS (SELECT 1 FROM {} WHERE \"{}\" IS NULL) AS null_free",
            intermediate_tbl, quoted
        );
        let null_free: Option<bool> = match client.select(&sql, Some(1), &[]) {
            Ok(mut t) => t
                .next()
                .and_then(|r| r.get_by_name::<bool, _>("null_free").ok().flatten()),
            Err(_) => None,
        };
        if null_free == Some(true) {
            probed.insert(norm);
        }
    }
    probed
}

/// Persist a delta of newly-discovered NOT NULL columns to
/// `__reflex_ivm_reference.aggregations`. Performs a JSON-level merge with
/// the existing array (no overwrite of catalog-derived entries).
fn persist_probed_not_null_columns(
    client: &mut pgrx::spi::SpiClient<'_>,
    view_name: &str,
    new_cols: &[String],
) {
    if new_cols.is_empty() {
        return;
    }
    let arr_literal = format_pg_text_array_literal(new_cols);
    client
        .update(
            "UPDATE public.__reflex_ivm_reference \
             SET aggregations = jsonb_set( \
                 aggregations::jsonb, \
                 '{not_null_columns}', \
                 ( \
                     SELECT jsonb_agg(DISTINCT col) \
                     FROM ( \
                         SELECT jsonb_array_elements_text( \
                             COALESCE((aggregations::jsonb)->'not_null_columns', '[]'::jsonb) \
                         ) AS col \
                         UNION \
                         SELECT unnest($1::text[]) AS col \
                     ) s \
                 ) \
             )::json \
             WHERE name = $2",
            None,
            &[
                unsafe { DatumWithOid::new(arr_literal, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
            ],
        )
        .unwrap_or_report();
}

/// SQL-callable wrapper: re-probe an existing IMV from data and update its
/// stored aggregations. Used by the 1.4.4→1.4.5 migration to backfill
/// effectively-NOT-NULL columns the catalog missed on existing IMVs, and by
/// operators after a data shape change. Returns a human-readable status.
pub(crate) fn reflex_probe_not_null_columns_impl(view_name: &str) -> String {
    if let Err(msg) = validate_view_name(view_name) {
        return msg.to_string();
    }
    let result: Result<Vec<String>, String> = Spi::connect_mut(|client| {
        let rows = client
            .select(
                "SELECT aggregations::text AS aggregations \
                 FROM public.__reflex_ivm_reference \
                 WHERE name = $1 AND enabled = TRUE",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("lookup failed: {}", e))?
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Err(format!("IMV '{}' not found or disabled", view_name));
        }
        let agg_json: String = rows[0]
            .get_by_name::<&str, _>("aggregations")
            .map_err(|e| format!("read aggregations: {}", e))?
            .unwrap_or("{}")
            .to_string();
        let plan: crate::aggregation::AggregationPlan = serde_json::from_str(&agg_json)
            .map_err(|e| format!("aggregations JSON parse: {}", e))?;
        if plan.is_passthrough {
            return Ok(Vec::new());
        }
        let intermediate_tbl = intermediate_table_name(view_name);
        let probed = probe_not_null_columns_from_data(client, &intermediate_tbl, &plan);
        let new_cols: Vec<String> = probed
            .into_iter()
            .filter(|c| !plan.not_null_columns.contains(c))
            .collect();
        persist_probed_not_null_columns(client, view_name, &new_cols);
        Ok(new_cols)
    });
    match result {
        Ok(cols) if cols.is_empty() => {
            format!(
                "pg_reflex: probe found no additional NOT NULL columns on '{}'",
                view_name
            )
        }
        Ok(cols) => format!(
            "pg_reflex: probe added {} NOT NULL column(s) on '{}': {:?}",
            cols.len(),
            view_name,
            cols
        ),
        Err(e) => format!("ERROR: {}", e),
    }
}

/// 1.4.5 — `reflex_compact_imv(view_name TEXT) RETURNS TEXT`.
///
/// VACUUM FULL both the intermediate and target tables of an IMV. The
/// 1.4.3→1.4.4 migration set fillfactor=70 on these tables but did not
/// rewrite existing pages — HOT updates can only fire after pages have
/// been written with the new fillfactor. Running VACUUM FULL once per
/// table materializes the fillfactor immediately, restoring HOT-eligibility
/// on legacy-populated IMVs.
///
/// VACUUM FULL takes ACCESS EXCLUSIVE on the rewritten table; for
/// multi-gigabyte IMVs this is a maintenance-window operation. Caller is
/// responsible for scheduling.
///
/// Returns a status string with per-table elapsed times.
/// Plan the SQL commands `reflex_compact_imv_impl` will issue for a
/// given IMV name. Pure function: no SPI, no side effects. Extracted so
/// the planning logic (identifier resolution, command shaping) is
/// unit-testable independent of `VACUUM`, which cannot run inside a
/// transaction (the pgrx test framework wraps every test in one).
///
/// Returns the ordered list of statements; the caller issues each via
/// SPI in a top-level statement.
pub(crate) fn plan_compact_imv(view_name: &str) -> Result<Vec<String>, String> {
    if let Err(msg) = validate_view_name(view_name) {
        return Err(msg.to_string());
    }
    let intermediate_tbl = intermediate_table_name(view_name);
    let target_tbl = quote_identifier(view_name);
    Ok(vec![
        format!("VACUUM (FULL) {}", intermediate_tbl),
        format!("VACUUM (FULL) {}", target_tbl),
    ])
}

/// Format the post-execution summary of a compact run. Pure function:
/// no SPI. Extracted so the message-shaping logic is unit-testable
/// (the executor path inside `reflex_compact_imv_impl` itself can't run
/// in a pgrx-test transaction because `VACUUM` is forbidden there).
pub(crate) fn format_compact_imv_summary(view_name: &str, per_stmt: &[(String, u128)]) -> String {
    let parts: Vec<String> = per_stmt
        .iter()
        .map(|(stmt, ms)| format!("{}: {} ms", stmt, ms))
        .collect();
    format!(
        "pg_reflex: compacted '{}' — {}",
        view_name,
        parts.join(", ")
    )
}

pub(crate) fn reflex_compact_imv_impl(view_name: &str) -> String {
    let stmts = match plan_compact_imv(view_name) {
        Ok(s) => s,
        Err(msg) => return msg,
    };
    let mut per_stmt: Vec<(String, u128)> = Vec::new();
    let result: Result<(), String> = Spi::connect_mut(|client| {
        for stmt in &stmts {
            let t0 = std::time::Instant::now();
            client
                .update(stmt, None, &[])
                .map_err(|e| format!("{} failed: {}", stmt, e))?;
            per_stmt.push((stmt.clone(), t0.elapsed().as_millis()));
        }
        Ok(())
    });
    match result {
        Ok(()) => format_compact_imv_summary(view_name, &per_stmt),
        Err(e) => format!("ERROR: {}", e),
    }
}

/// 1.4.5 — `reflex_compact_all_imv() RETURNS TEXT`.
///
/// VACUUM FULL every enabled IMV's intermediate and target tables. Iterates
/// the catalog in `(graph_depth, name)` order and dispatches to
/// `reflex_compact_imv_impl` for each row. Errors on one IMV are recorded
/// in the result but do not abort processing of the remaining IMVs — this
/// matches the semantics of a maintenance-window operator who wants
/// "compact everything you can".
/// Pure summary builder for `reflex_compact_all_imv_impl`. Given the
/// list of IMV names and per-IMV result messages, return the final
/// summary string. Extracted so the summary-shaping logic is
/// unit-testable independent of `VACUUM`.
pub(crate) fn build_compact_all_summary(
    names: &[String],
    results: &[(String, String)],
    total_ms: u128,
) -> String {
    let successes = results
        .iter()
        .filter(|(_, msg)| !msg.starts_with("ERROR"))
        .count();
    let failures: Vec<String> = results
        .iter()
        .filter(|(_, msg)| msg.starts_with("ERROR"))
        .map(|(name, msg)| format!("{}: {}", name, msg))
        .collect();
    let details: Vec<String> = results.iter().map(|(_, msg)| msg.clone()).collect();
    let mut summary = format!(
        "pg_reflex: compacted {}/{} IMV(s) in {} ms",
        successes,
        names.len(),
        total_ms
    );
    if !failures.is_empty() {
        summary.push_str(" — failures: ");
        summary.push_str(&failures.join("; "));
    }
    summary.push('\n');
    summary.push_str(&details.join("\n"));
    summary
}

pub(crate) fn reflex_compact_all_imv_impl() -> String {
    let names: Vec<String> = Spi::connect(|client| {
        let mut out: Vec<String> = Vec::new();
        if let Ok(table) = client.select(
            "SELECT name FROM public.__reflex_ivm_reference \
             WHERE enabled = TRUE ORDER BY graph_depth, name",
            None,
            &[],
        ) {
            for row in table {
                if let Ok(Some(name)) = row.get::<&str>(1) {
                    out.push(name.to_string());
                }
            }
        }
        out
    });
    if names.is_empty() {
        return "pg_reflex: no enabled IMVs to compact".to_string();
    }
    let t0 = std::time::Instant::now();
    let results: Vec<(String, String)> = names
        .iter()
        .map(|name| (name.clone(), reflex_compact_imv_impl(name)))
        .collect();
    build_compact_all_summary(&names, &results, t0.elapsed().as_millis())
}

/// 1.4.5 — `reflex_rebuild_imv_metadata(view_name TEXT) RETURNS TEXT`.
///
/// Re-analyzes an existing IMV's stored `base_query` and merges the newly
/// computed `imv_relevant_columns` / `imv_relevant_where` maps into its
/// `aggregations` JSON. Idempotent: existing fields (group_by_columns,
/// intermediate_columns, end_query_mappings, not_null_columns, …) are NOT
/// overwritten beyond the two new metadata maps.
///
/// Used by the 1.4.4→1.4.5 migration to backfill IMVs created before the
/// filter-aware spurious-skip metadata was introduced. Also useful after a
/// future analyzer extension shifts what falls into either map.
///
/// Returns a status string for telemetry.
pub(crate) fn reflex_rebuild_imv_metadata_impl(view_name: &str) -> String {
    if let Err(msg) = validate_view_name(view_name) {
        return msg.to_string();
    }
    let result: Result<(usize, usize, usize), String> = Spi::connect_mut(|client| {
        let rows = client
            .select(
                "SELECT base_query FROM public.__reflex_ivm_reference \
                 WHERE name = $1 AND enabled = TRUE",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("lookup failed: {}", e))?
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Err(format!("IMV '{}' not found or disabled", view_name));
        }
        let base_query: String = rows[0]
            .get_by_name::<&str, _>("base_query")
            .map_err(|e| format!("read base_query: {}", e))?
            .unwrap_or("")
            .to_string();

        let parsed = Parser::parse_sql(&PostgreSqlDialect {}, &base_query)
            .map_err(|e| format!("parse base_query: {}", e))?;
        let analysis =
            analyze(&parsed).map_err(|e: SqlAnalysisError| format!("analyze base_query: {}", e))?;

        // Ground analyzer output against the actual catalog shape. The
        // analyzer attributes bare identifiers to every real source as a
        // safe-correctness move; if we serialize that raw, the trigger's
        // filter-aware-skip block SELECTs columns that don't exist on the
        // source's transition table and errors at fire time. Mirror the
        // 1.4.5 filter applied at create-time in the create_reflex_ivm
        // aggregate path (search "1.4.5 — filter `imv_relevant_columns`").
        let froms_list: Vec<String> = analysis.sources.to_vec();
        let (_types, _nn, per_source_cols) =
            query_column_types_from_catalog_with_per_source(client, &froms_list);
        let mut relevant_cols: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for (source, set) in analysis.imv_relevant_columns.iter() {
            if source.starts_with('<') {
                continue;
            }
            let filtered: Vec<String> = if let Some(actual) = per_source_cols.get(source) {
                set.iter()
                    .filter(|c| actual.contains(&c.to_lowercase()))
                    .cloned()
                    .collect()
            } else {
                // Source not found in catalog — preserve raw (conservative)
                set.iter().cloned().collect()
            };
            if !filtered.is_empty() {
                relevant_cols.insert(source.clone(), filtered);
            }
        }

        let cols_json = serde_json::to_string(&relevant_cols)
            .map_err(|e| format!("serialize relevant cols: {}", e))?;
        let where_json = serde_json::to_string(&analysis.imv_relevant_where)
            .map_err(|e| format!("serialize relevant where: {}", e))?;

        let n_cols_sources = relevant_cols.len();
        let n_where_sources = analysis.imv_relevant_where.len();

        // 1.4.6 — also backfill source_join_keys for the bulk-INSERT /
        // bulk-DELETE / Path B paths. Rebuild a plan from base_query so we
        // can run the same build_source_join_keys logic the create path
        // uses. Single-source IMVs skip the call (no JOINs to mine).
        let mut tmp_plan = crate::aggregation::plan_aggregation(&analysis);
        let real_sources: Vec<&String> = analysis
            .sources
            .iter()
            .filter(|s| !s.starts_with('<'))
            .collect();
        let is_join_query = real_sources.len() > 1;
        if !tmp_plan.is_passthrough && is_join_query {
            build_source_join_keys(&mut tmp_plan, &real_sources, &analysis);
        }
        let sjk_json = serde_json::to_string(&tmp_plan.source_join_keys)
            .map_err(|e| format!("serialize source_join_keys: {}", e))?;
        let n_sjk_sources = tmp_plan.source_join_keys.len();

        client
            .update(
                "UPDATE public.__reflex_ivm_reference \
                 SET aggregations = jsonb_set( \
                       jsonb_set( \
                         jsonb_set( \
                           aggregations::jsonb, \
                           '{imv_relevant_columns}', \
                           $1::jsonb, \
                           TRUE \
                         ), \
                         '{imv_relevant_where}', \
                         $2::jsonb, \
                         TRUE \
                       ), \
                       '{source_join_keys}', \
                       $4::jsonb, \
                       TRUE \
                     )::json \
                 WHERE name = $3",
                None,
                &[
                    unsafe { DatumWithOid::new(cols_json, PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(where_json, PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe {
                        DatumWithOid::new(
                            view_name.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe { DatumWithOid::new(sjk_json, PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            )
            .map_err(|e| format!("update aggregations: {}", e))?;
        Ok((n_cols_sources, n_where_sources, n_sjk_sources))
    });
    match result {
        Ok((n_cols, n_where, n_sjk)) => format!(
            "pg_reflex: rebuilt metadata for '{}' ({} relevant_columns sources, {} relevant_where sources, {} source_join_keys sources)",
            view_name, n_cols, n_where, n_sjk
        ),
        Err(e) => format!("ERROR: {}", e),
    }
}

/// 1.4.5 — `reflex_rebuild_triggers(source_table TEXT) RETURNS TEXT`.
///
/// Re-emits the four CREATE OR REPLACE FUNCTION + CREATE OR REPLACE TRIGGER
/// statements for INSERT/DELETE/UPDATE/TRUNCATE on `source_table`, picking
/// up the latest codegen (e.g. the 1.4.5 filter-aware spurious-skip block).
///
/// Idempotent — `CREATE OR REPLACE` overwrites the existing function body
/// without changing trigger identity.
///
/// 1.4.6 — when `source_table` arrives unqualified (`'demand_planning'` from
/// a legacy `depends_on` array), resolve the actual schema via `pg_class`
/// instead of letting the DDL fall through to the caller's `search_path`.
/// Without this, the 1.4.6 migration silently rebuilt triggers on
/// `public.demand_planning` (which doesn't exist) when the real source was
/// `alp.demand_planning`, then deferred-flush failures aborted every
/// UPDATE in the session. If the bare name resolves to multiple schemas,
/// return an error rather than guess.
///
/// Returns a status string.
pub(crate) fn reflex_rebuild_triggers_impl(source_table: &str) -> String {
    // Already schema-qualified: trust the caller.
    let resolved = if source_table.contains('.') {
        source_table.to_string()
    } else {
        // Bare name — resolve via pg_class. We restrict to ordinary tables
        // and materialized views (the two source kinds pg_reflex supports).
        let lookup = Spi::connect(|client| {
            client
                .select(
                    "SELECT n.nspname::TEXT AS schema_name \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.relname = $1 \
                       AND c.relkind IN ('r', 'm', 'p', 'f') \
                       AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
                     ORDER BY n.nspname",
                    None,
                    &[unsafe {
                        DatumWithOid::new(
                            source_table.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    }],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    row.get_by_name::<&str, _>("schema_name")
                        .unwrap_or(None)
                        .map(|s| s.to_string())
                })
                .collect::<Vec<_>>()
        });
        match lookup.len() {
            0 => {
                return format!(
                    "ERROR: source table '{}' not found in any user schema — \
                     update __reflex_ivm_reference.depends_on with the qualified name",
                    source_table
                );
            }
            1 => format!("{}.{}", lookup[0], source_table),
            _ => {
                return format!(
                    "ERROR: source name '{}' is ambiguous (found in schemas: {}); \
                     update __reflex_ivm_reference.depends_on with the qualified name",
                    source_table,
                    lookup.join(", ")
                );
            }
        }
    };

    let result: Result<usize, String> = Spi::connect_mut(|client| {
        let ddls = crate::schema_builder::build_trigger_ddls(&resolved);
        for ddl in &ddls {
            client
                .update(ddl, None, &[])
                .map_err(|e| format!("CREATE TRIGGER failed: {}", e))?;
        }
        Ok(ddls.len())
    });
    match result {
        Ok(n) => format!("pg_reflex: rebuilt {} trigger DDL(s) for '{}'", n, resolved),
        Err(e) => format!("ERROR: {}", e),
    }
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

/// Augment the column_types map with actual types from the base_query output.
/// This handles computed expressions (DATE_TRUNC, EXTRACT, COALESCE, etc.)
/// whose types cannot be resolved from the source table catalog alone.
///
/// Creates a temporary view from the base_query, reads column types from
/// pg_attribute, then drops the view.
fn augment_column_types_from_query(base_query: &str, column_types: &mut HashMap<String, String>) {
    Spi::connect_mut(|client| {
        let tmp = "__reflex_typecheck_view";
        let create = format!("CREATE TEMP VIEW {} AS {}", tmp, base_query);
        if client.update(&create, None, &[]).is_err() {
            return;
        }
        let rows = client
            .select(
                "SELECT a.attname::text AS col_name, \
                        format_type(a.atttypid, a.atttypmod)::text AS col_type \
                 FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 WHERE c.relname = $1 AND a.attnum > 0 AND NOT a.attisdropped",
                None,
                &[unsafe {
                    DatumWithOid::new(tmp.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report();
        for row in rows {
            if let (Some(col_name), Some(col_type)) = (
                row.get_by_name::<String, _>("col_name").unwrap_or(None),
                row.get_by_name::<String, _>("col_type").unwrap_or(None),
            ) {
                let pg_type = map_information_schema_type(&col_type);
                column_types
                    .entry(col_name)
                    .or_insert_with(|| pg_type.to_string());
            }
        }
        let _ = client.update(&format!("DROP VIEW IF EXISTS {}", tmp), None, &[]);
    });
}

#[cfg(test)]
#[path = "tests/unit_create_ivm.rs"]
mod tests;
