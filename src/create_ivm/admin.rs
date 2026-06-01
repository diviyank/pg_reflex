use super::*;
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

use crate::query_decomposer::{intermediate_table_name, quote_identifier};
use crate::sql_analyzer::{analyze, SqlAnalysisError};
use crate::validate_view_name;

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
        // 1.6.2: pick the correct trigger flavour by inspecting the live
        // dependency graph.  If any enabled IMV depending on this source is
        // DEFERRED, the source's trigger function must use the deferred
        // body (which stages into `__reflex_delta_<src>`); otherwise the
        // immediate body suffices.  Pre-1.6.2 this always emitted the
        // immediate body, which silently broke deferred IMVs whose trigger
        // had been rebuilt via this entry point.
        let has_deferred = client
            .select(
                "SELECT 1 FROM public.__reflex_ivm_reference \
                 WHERE $1 = ANY(depends_on) \
                   AND refresh_mode = 'DEFERRED' \
                   AND enabled = TRUE \
                 LIMIT 1",
                None,
                &[unsafe {
                    DatumWithOid::new(resolved.clone(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("dependency lookup failed: {}", e))?
            .next()
            .is_some();
        let ddls = if has_deferred {
            let cols = fetch_source_columns(client, &resolved);
            crate::schema_builder::build_deferred_trigger_ddls(&resolved, &cols)
        } else {
            crate::schema_builder::build_trigger_ddls(&resolved)
        };
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
pub(crate) fn map_information_schema_type(data_type: &str) -> String {
    // `format_type` appends type modifiers (`numeric(10,2)`, `character
    // varying(255)`); strip them so the base type matches the arms below.
    // information_schema spellings have no parens, so this is a no-op for them.
    let data_type = data_type.split('(').next().unwrap_or(data_type).trim();
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
pub(crate) fn augment_column_types_from_query(
    base_query: &str,
    column_types: &mut HashMap<String, String>,
) {
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
