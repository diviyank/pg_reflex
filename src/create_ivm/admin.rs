use super::*;
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

use crate::drop_ivm::drop_reflex_ivm_impl;
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

        // PS-6 heal — a passthrough IMV references a per-(IMV, source) scratch
        // pair (`__reflex_pt_new/old_<imv>_<source>`) that `passthrough_op_stmts`
        // emits TRUNCATE/INSERT against on every flush. If that pair is missing
        // (an older create loop that didn't cover this source, a partial create,
        // or a manual drop), every flush fails fast with 42P01, is swallowed as a
        // WARNING, and the IMV goes silently stale forever. Recreate the pair
        // idempotently (`build_passthrough_scratch_ddls` is CREATE IF NOT EXISTS)
        // for every enabled passthrough IMV that depends on this source, using
        // the IMV's own `depends_on` entry as the source string so the recreated
        // name matches exactly what the trigger references. This is the
        // source-scoped, no-drop recovery the 1.4.5+ migrations already invoke.
        let bare_source = split_qualified_name(&resolved).1.to_string();
        let passthrough_deps: Vec<(String, Vec<String>)> = client
            .select(
                "SELECT name, depends_on \
                 FROM public.__reflex_ivm_reference \
                 WHERE enabled = TRUE \
                   AND COALESCE((aggregations->>'is_passthrough')::bool, FALSE) \
                   AND ($1 = ANY(depends_on) OR $2 = ANY(depends_on))",
                None,
                &[
                    unsafe {
                        DatumWithOid::new(resolved.clone(), PgBuiltInOids::TEXTOID.oid().value())
                    },
                    unsafe {
                        DatumWithOid::new(bare_source.clone(), PgBuiltInOids::TEXTOID.oid().value())
                    },
                ],
            )
            .map_err(|e| format!("passthrough-dependent lookup failed: {}", e))?
            .filter_map(|row| {
                let name = row
                    .get_by_name::<&str, _>("name")
                    .ok()
                    .flatten()?
                    .to_string();
                let deps = row
                    .get_by_name::<Vec<String>, _>("depends_on")
                    .ok()
                    .flatten()
                    .unwrap_or_default();
                Some((name, deps))
            })
            .collect();

        for (imv_name, deps) in &passthrough_deps {
            for dep in deps {
                if dep == &resolved
                    || dep == source_table
                    || split_qualified_name(dep).1 == bare_source
                {
                    for ddl in crate::schema_builder::build_passthrough_scratch_ddls(imv_name, dep)
                    {
                        client
                            .update(&ddl, None, &[])
                            .map_err(|e| format!("passthrough scratch recreate failed: {}", e))?;
                    }
                }
            }
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

/// Rebuild a decomposed (CTE/set-op) IMV chain by cascading-dropping the
/// top IMV and all its sub-IMVs, then re-creating from the stored registry spec.
///
/// This is the in-extension recovery for corrupted decomposed chains where
/// bottom-up per-sub reconciliation does not converge. The drop + recreate
/// happens in a single SPI transaction so atomicity is guaranteed: if the
/// recreate fails, the drop is rolled back.
///
/// Requires `create_args` in the registry row (1.11.0+). Returns an ERROR string
/// on any failure (IMV not found, missing create_args, drop/create failure).
///
/// Without `cascade`, refuses when other IMVs depend on `view_name` — CASCADE
/// would drop them too, but only `view_name` gets recreated, silently
/// destroying the dependents. With `cascade => TRUE`, each dependent's
/// creation spec is captured before the drop and recreated afterwards, in
/// shallowest-first order. A dependent with no stored `create_args` (created
/// before 1.10.8) cannot be faithfully recreated and causes an upfront
/// refusal, before anything is dropped.
pub(crate) fn reflex_rebuild_chain_impl(view_name: &str, cascade: bool) -> String {
    if let Err(msg) = validate_view_name(view_name) {
        return format!("ERROR: {}", msg);
    }

    let result: Result<String, String> = Spi::connect_mut(|client| {
        // 1. Read registry entry (with create_args)
        let (sql_query, create_args_json) =
            crate::sql_writer::registry::read_imv_for_rebuild(client, view_name)
                .ok_or_else(|| format!("IMV '{}' not found in registry", view_name))?;

        // D22 — a CTE-decomposed parent stores the REWRITTEN body as sql_query (it
        // names the generated <root>__cte_<alias> child), so a CASCADE drop removes
        // that child and the recreate then references a vanished relation, aborting
        // the transaction. The original user SQL is not recoverable from the row, so
        // refuse BEFORE any drop and point at the recursive reflex_reconcile (the
        // correct recovery since 1.11.0) or a full drop-and-recreate.
        if is_decomposed_parent(client, view_name) {
            return Err(format!(
                "IMV '{v}' was decomposed by pg_reflex into generated sub-IMV(s) it \
                 depends on; its stored query references a generated sibling that a \
                 rebuild would drop first, so drop-and-recreate cannot restore it. \
                 To refresh it, run: SELECT reflex_reconcile('{v}'); (recursive over \
                 the whole chain since 1.11.0). To rebuild its structure, drop and \
                 recreate from the original spec: SELECT drop_reflex_ivm('{v}', true); \
                 then re-run the original create_reflex_ivm.",
                v = view_name
            ));
        }

        // Part 1 — the NAMED IMV gets the same fail-closed guard as its dependents
        // (below): a row with no faithful create_args (legacy pre-1.10.8, surfaced
        // as "" or "{}") cannot be recreated without silently resetting storage
        // mode, refresh mode and partitioning. Refuse up front, before the drop.
        if create_args_unusable(&create_args_json) {
            return Err(format!(
                "IMV '{v}' has no usable create_args. Recreating it would silently \
                 reset its storage mode, refresh mode and partitioning. To refresh it \
                 without a structural rebuild, run: SELECT reflex_reconcile('{v}'); \
                 to rebuild its structure, drop and recreate from the original spec: \
                 SELECT drop_reflex_ivm('{v}', true); then re-run the original \
                 create_reflex_ivm. The 1.11.0 migration backfills create_args for \
                 legacy aggregate/join IMVs, but not for set-op / DISTINCT-ON / window \
                 wrappers (aggregations = '{{}}'): for a decomposed parent still in the \
                 pre-repair window this refusal self-resolves once \
                 SELECT reflex_repair_dependency_graph(); has run (it is then caught by \
                 the more specific decomposed-parent guard).",
                v = view_name
            ));
        }

        let dependents = read_dependents_shallowest_first(client, view_name);
        if !dependents.is_empty() && !cascade {
            let names: Vec<&str> = dependents.iter().map(|(n, _)| n.as_str()).collect();
            return Err(format!(
                "IMV '{}' has {} dependent IMV(s) that CASCADE would destroy: {}. \
                 To rebuild the whole chain, drop and recreate it from its original spec: \
                 SELECT drop_reflex_ivm('{}', true); then re-run the original create_reflex_ivm. \
                 (cascade => TRUE is unsafe on a decomposed chain — it recreates from a stored \
                 query that references a sibling the cascade has already dropped.)",
                view_name,
                dependents.len(),
                names.join(", "),
                view_name
            ));
        }

        let unrecreatable: Vec<&str> = dependents
            .iter()
            .filter(|(_, args)| create_args_unusable(args.as_deref().unwrap_or("")))
            .map(|(n, _)| n.as_str())
            .collect();
        if cascade && !unrecreatable.is_empty() {
            return Err(format!(
                "IMV '{}' has dependent IMV(s) with no stored create_args (created before 1.10.8): {}. \
                 Recreating them would silently reset storage mode, refresh mode and partitioning. \
                 Rebuild them individually with reflex_rebuild_imv first.",
                view_name,
                unrecreatable.join(", ")
            ));
        }

        // 2. Parse create_args JSON to extract parameters using PostgreSQL's native JSON
        // parsing to handle escaped quotes and array elements correctly.
        let unique_columns_str =
            extract_string_field_via_sql(client, &create_args_json, "unique_columns_str")
                .unwrap_or_default();
        let storage_mode = extract_string_field_via_sql(client, &create_args_json, "storage_mode")
            .unwrap_or_else(|| "UNLOGGED".to_string());
        let refresh_mode = extract_string_field_via_sql(client, &create_args_json, "refresh_mode")
            .unwrap_or_else(|| "IMMEDIATE".to_string());
        let topk_k = extract_number_field_via_sql(client, &create_args_json, "topk_k");
        let ignore_sources =
            extract_array_field_via_sql(client, &create_args_json, "ignore_sources");
        let partition_by = extract_array_field_via_sql(client, &create_args_json, "partition_by");
        let explicit_unpartitioned =
            extract_bool_field_via_sql(client, &create_args_json, "explicit_unpartitioned")
                .unwrap_or(false);
        // The 1.11.0 migration backfills legacy create_args from dedicated columns
        // but cannot reconstruct topk_k or explicit_unpartitioned (no column holds
        // them), so a rebuild from such a row defaults both — silently dropping a
        // top-K bound or auto-partitioning a deliberately-unpartitioned IMV. The
        // marker makes that loss load-bearing: it is surfaced at rebuild time.
        let named_backfilled =
            extract_bool_field_via_sql(client, &create_args_json, "backfilled").unwrap_or(false);

        // 2b. Capture each dependent's spec before the drop — after the drop
        // the registry rows are gone, so nothing may be read lazily.
        let captured_dependents: Vec<CapturedDependent> = dependents
            .iter()
            .filter_map(|(dep_name, _)| {
                let (dep_sql_query, dep_create_args_json) =
                    crate::sql_writer::registry::read_imv_for_rebuild(client, dep_name)?;
                Some(CapturedDependent {
                    name: dep_name.clone(),
                    sql_query: dep_sql_query,
                    unique_columns_str: extract_string_field_via_sql(
                        client,
                        &dep_create_args_json,
                        "unique_columns_str",
                    )
                    .unwrap_or_default(),
                    storage_mode: extract_string_field_via_sql(
                        client,
                        &dep_create_args_json,
                        "storage_mode",
                    )
                    .unwrap_or_else(|| "UNLOGGED".to_string()),
                    refresh_mode: extract_string_field_via_sql(
                        client,
                        &dep_create_args_json,
                        "refresh_mode",
                    )
                    .unwrap_or_else(|| "IMMEDIATE".to_string()),
                    topk_k: extract_number_field_via_sql(client, &dep_create_args_json, "topk_k"),
                    ignore_sources: extract_array_field_via_sql(
                        client,
                        &dep_create_args_json,
                        "ignore_sources",
                    ),
                    partition_by: extract_array_field_via_sql(
                        client,
                        &dep_create_args_json,
                        "partition_by",
                    ),
                    explicit_unpartitioned: extract_bool_field_via_sql(
                        client,
                        &dep_create_args_json,
                        "explicit_unpartitioned",
                    )
                    .unwrap_or(false),
                    backfilled: extract_bool_field_via_sql(
                        client,
                        &dep_create_args_json,
                        "backfilled",
                    )
                    .unwrap_or(false),
                })
            })
            .collect();

        // 3. Drop the IMV with CASCADE (removes all sub-IMVs)
        let drop_result = drop_reflex_ivm_impl(view_name, true);
        if drop_result.starts_with("ERROR") {
            return Err(drop_result.to_string());
        }

        // 4. Re-create with stored parameters
        // Note: if_not_exists=false because we just dropped it
        let create_result = create_reflex_ivm_impl(
            view_name,
            &sql_query,
            &unique_columns_str,
            false, // if_not_exists=false (we just dropped it)
            &storage_mode,
            &refresh_mode,
            topk_k,
            &ignore_sources,
            &partition_by,
            explicit_unpartitioned,
        );

        if create_result.starts_with("ERROR") {
            // Post-drop failure: use pgrx::error! to abort transaction so drop rolls back
            pgrx::error!(
                "reflex_rebuild_chain: failed to recreate IMV '{}': {}",
                view_name,
                create_result
            );
        }

        // 5. Re-create dependents, shallowest first, so each recreate sees its
        // own upstream dependency already restored.
        for dep in &captured_dependents {
            let dep_create_result = create_reflex_ivm_impl(
                &dep.name,
                &dep.sql_query,
                &dep.unique_columns_str,
                false,
                &dep.storage_mode,
                &dep.refresh_mode,
                dep.topk_k,
                &dep.ignore_sources,
                &dep.partition_by,
                dep.explicit_unpartitioned,
            );
            if dep_create_result.starts_with("ERROR") {
                pgrx::error!(
                    "reflex_rebuild_chain: failed to recreate dependent IMV '{}': {}",
                    dep.name,
                    dep_create_result
                );
            }
        }

        // Surface the honest-partial backfill loss on the recovery path. Emitted
        // only after every recreate succeeded, so it never fires on a rolled-back
        // rebuild. A live WARNING reaches the operator's session at rebuild time
        // (months after the one-time migration NOTICE), and the same caveat is
        // appended to the returned status so programmatic callers can see it too.
        let mut backfilled_names: Vec<&str> = Vec::new();
        if named_backfilled {
            backfilled_names.push(view_name);
        }
        backfilled_names.extend(
            captured_dependents
                .iter()
                .filter(|d| d.backfilled)
                .map(|d| d.name.as_str()),
        );

        let base = format!(
            "REBUILT CHAIN ({} dependent(s) restored)",
            captured_dependents.len()
        );
        if backfilled_names.is_empty() {
            return Ok(base);
        }

        let caveat = format!(
            "rebuilt from a backfilled create_args ({}) — topk_k and \
             explicit_unpartitioned are not reconstructible by the 1.11.0 migration \
             and were reset to create-time defaults (no top-K bound; auto-partitioning). \
             If any of these IMVs was created with a top-K bound or explicitly \
             unpartitioned, re-create it from its original create_reflex_ivm call to \
             restore those settings.",
            backfilled_names.join(", ")
        );
        pgrx::warning!("pg_reflex: {}", caveat);
        Ok(format!("{} [WARNING: {}]", base, caveat))
    });

    match result {
        Ok(msg) => msg,
        Err(e) => format!("ERROR: {}", e),
    }
}

/// A dependent IMV's creation spec, captured before the CASCADE-drop so it can
/// be recreated afterwards — the registry row backing it is gone once the
/// drop runs, so every field needed for `create_reflex_ivm_impl` must be an
/// owned value read up front.
struct CapturedDependent {
    name: String,
    sql_query: String,
    unique_columns_str: String,
    storage_mode: String,
    refresh_mode: String,
    topk_k: Option<usize>,
    ignore_sources: Vec<String>,
    partition_by: Vec<String>,
    explicit_unpartitioned: bool,
    backfilled: bool,
}

/// A stored `create_args` cannot faithfully recreate an IMV when it is absent —
/// legacy pre-1.10.8 rows carry NULL, surfaced by `read_imv_for_rebuild` and the
/// registry read as an empty string or the placeholder `"{}"`. Recreating from it
/// would silently reset storage mode, refresh mode and partitioning, so the named
/// IMV and every dependent are refused on this one shared predicate — the two
/// paths cannot drift.
fn create_args_unusable(create_args: &str) -> bool {
    let trimmed = create_args.trim();
    trimmed.is_empty() || trimmed == "{}"
}

/// True when `view_name` is a parent pg_reflex decomposed into generated
/// sub-IMV(s) it now depends on (a CTE / set-op / DISTINCT-ON / window split).
/// reflex_rebuild_chain cannot rebuild such a parent: its stored query names a
/// generated sibling that the CASCADE drop removes first, so the recreate
/// references a vanished relation (D22).
///
/// Detected two ways so the guard holds for new and legacy rows alike:
///   * structurally — a `is_generated_sub_imv` child sits in the parent's
///     `depends_on_imv` (precise; correct for new rows and after
///     reflex_repair_dependency_graph backfills legacy rows);
///   * via the uncorrupted `depends_on`, which always carries the generated child
///     double-quoted as `"<bare_root>__…"`. That spelling survives even when the
///     structural columns do not (legacy rows predating `is_generated_sub_imv`,
///     including schema-qualified names the old name-prefix heuristic could not
///     match). Anchoring the match on the opening quote plus the view's OWN bare
///     name keeps it from firing on an unrelated source.
fn is_decomposed_parent(client: &SpiClient<'_>, view_name: &str) -> bool {
    let (_, bare) = crate::query_decomposer::canonical_source(view_name);
    let escaped_bare = bare
        .replace('\\', "\\\\")
        .replace('_', "\\_")
        .replace('%', "\\%");
    let dep_pattern = format!("%\"{}\\_\\_%", escaped_bare);
    client
        .select(
            "SELECT (EXISTS( \
                 SELECT 1 FROM public.__reflex_ivm_reference child \
                  WHERE COALESCE(child.is_generated_sub_imv, FALSE) \
                    AND child.name = ANY(SELECT unnest(COALESCE(p.depends_on_imv, ARRAY[]::TEXT[])) \
                                           FROM public.__reflex_ivm_reference p WHERE p.name = $1)) \
               OR EXISTS( \
                 SELECT 1 FROM public.__reflex_ivm_reference p, \
                        unnest(COALESCE(p.depends_on, ARRAY[]::TEXT[])) AS dep \
                  WHERE p.name = $1 AND dep LIKE $2 ESCAPE '\\')) AS decomposed",
            Some(1),
            &[
                unsafe {
                    DatumWithOid::new(
                        view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                },
                unsafe {
                    DatumWithOid::new(dep_pattern, PgBuiltInOids::TEXTOID.oid().value())
                },
            ],
        )
        .ok()
        .and_then(|mut it| {
            it.next()
                .and_then(|r| r.get_by_name::<bool, _>("decomposed").ok().flatten())
        })
        // Fail CLOSED: an SPI error or an unexpected shape means we could not prove
        // the parent is NOT decomposed, so treat it as decomposed and refuse rather
        // than proceed to a CASCADE drop that a decomposed parent cannot survive.
        // The EXISTS query always yields one non-NULL bool, so None here is a real
        // fault, not an empty result — refusing on it is correctness-first.
        .unwrap_or(true)
}

/// Dependents of `view_name` registered in `__reflex_ivm_reference`, shallowest
/// first, so recreation order matches dependency order.
///
/// The walk follows `graph_child` transitively — the SAME forward edge the
/// CASCADE drop recurses over (`src/drop_ivm.rs`) — so the capture set is
/// exactly the set the drop destroys. A direct-only enumeration (matching
/// `depends_on`) would miss depth+2-and-deeper dependents, which CASCADE still
/// drops, silently losing them: the very failure this guard exists to prevent.
/// Ordering by ascending `graph_depth` yields a valid recreate order (a
/// dependent is always recreated after everything it reads from), including for
/// diamond dependencies, which the recursive UNION deduplicates.
fn read_dependents_shallowest_first(
    client: &SpiClient<'_>,
    view_name: &str,
) -> Vec<(String, Option<String>)> {
    client
        .select(
            "WITH RECURSIVE chain AS (
                 SELECT unnest(graph_child) AS dep_name
                   FROM public.__reflex_ivm_reference
                  WHERE name = $1 OR name = split_part($1, '.', 2)
               UNION
                 SELECT unnest(r.graph_child)
                   FROM public.__reflex_ivm_reference r
                   JOIN chain c ON r.name = c.dep_name
             )
             SELECT ref.name, ref.create_args
               FROM chain
               JOIN public.__reflex_ivm_reference ref ON ref.name = chain.dep_name
              ORDER BY ref.graph_depth",
            None,
            &[unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .ok()
        .map(|it| {
            it.filter_map(|r| {
                let name = r.get_by_name::<&str, _>("name").ok().flatten()?.to_string();
                let args = r
                    .get_by_name::<&str, _>("create_args")
                    .ok()
                    .flatten()
                    .map(|s| s.to_string());
                Some((name, args))
            })
            .collect()
        })
        .unwrap_or_default()
}

/// Helper: extract a string field from JSON using PostgreSQL's native JSON parsing.
/// Uses SQL to correctly handle escaped quotes and special characters.
/// Returns None if field not found or empty.
fn extract_string_field_via_sql(
    client: &pgrx::spi::SpiClient<'_>,
    json: &str,
    field_name: &str,
) -> Option<String> {
    let sql = format!(
        "SELECT ($1::jsonb)->>'{}' AS val",
        field_name.replace('\'', "''")
    );
    let rows =
        client
            .select(
                &sql,
                Some(1),
                &[unsafe {
                    DatumWithOid::new(json.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

    rows.first()
        .and_then(|row| row.get_by_name::<&str, _>("val").ok().flatten())
        .map(|s| s.to_string())
}

/// Helper: extract a number field from JSON using PostgreSQL's native JSON parsing.
/// Returns None if field not found or not a valid number.
fn extract_number_field_via_sql(
    client: &pgrx::spi::SpiClient<'_>,
    json: &str,
    field_name: &str,
) -> Option<usize> {
    let sql = format!(
        "SELECT (($1::jsonb)->>'{}')::bigint AS val",
        field_name.replace('\'', "''")
    );
    let rows =
        client
            .select(
                &sql,
                Some(1),
                &[unsafe {
                    DatumWithOid::new(json.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

    rows.first()
        .and_then(|row| row.get_by_name::<i64, _>("val").ok().flatten())
        .and_then(|n| usize::try_from(n).ok())
}

/// Helper: extract a boolean field from JSON using PostgreSQL's native JSON parsing.
/// Returns None if field not found or not a valid boolean.
fn extract_bool_field_via_sql(
    client: &pgrx::spi::SpiClient<'_>,
    json: &str,
    field_name: &str,
) -> Option<bool> {
    let sql = format!(
        "SELECT (($1::jsonb)->>'{}')::boolean AS val",
        field_name.replace('\'', "''")
    );
    let rows =
        client
            .select(
                &sql,
                Some(1),
                &[unsafe {
                    DatumWithOid::new(json.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

    rows.first()
        .and_then(|row| row.get_by_name::<bool, _>("val").ok().flatten())
}

/// Helper: extract an array field from JSON using PostgreSQL's native JSON parsing.
/// Returns empty vec if not found or empty.
fn extract_array_field_via_sql(
    client: &pgrx::spi::SpiClient<'_>,
    json: &str,
    field_name: &str,
) -> Vec<String> {
    let sql = format!(
        "SELECT COALESCE(array_agg(x), ARRAY[]::text[]) AS result FROM jsonb_array_elements_text(($1::jsonb)->'{}') x",
        field_name.replace('\'', "''")
    );
    let rows =
        client
            .select(
                &sql,
                Some(1),
                &[unsafe {
                    DatumWithOid::new(json.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

    rows.first()
        .and_then(|row| row.get_by_name::<Vec<String>, _>("result").ok().flatten())
        .unwrap_or_default()
}
