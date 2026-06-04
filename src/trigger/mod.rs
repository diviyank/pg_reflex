use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::Spi;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, OnceLock};

use crate::aggregation::AggregationPlan;
use crate::query_decomposer::{
    affected_groups_table_name, delta_scratch_table_name, intermediate_table_name,
    quote_identifier, shrunk_groups_table_name, split_qualified_name, transition_new_table_name,
    transition_old_table_name,
};
use crate::sql_writer::identifier::replace_source_with_transition;

/// Per-backend cache of built delta SQL keyed by a hash of all inputs.
/// Entries are content-addressable: identical inputs always produce identical
/// SQL, so a registry rebuild that changes base_query/aggregations naturally
/// produces a different cache key (no explicit invalidation needed).
const DELTA_SQL_CACHE_MAX: usize = 256;

fn delta_sql_cache() -> &'static Mutex<HashMap<u64, String>> {
    static CACHE: OnceLock<Mutex<HashMap<u64, String>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::with_capacity(DELTA_SQL_CACHE_MAX)))
}

fn delta_sql_cache_key(
    view_name: &str,
    source_table: &str,
    operation: &str,
    base_query: &str,
    end_query: &str,
    aggregations_json: Option<&str>,
    orig_base_query: &str,
) -> u64 {
    let mut h = DefaultHasher::new();
    view_name.hash(&mut h);
    source_table.hash(&mut h);
    operation.hash(&mut h);
    base_query.hash(&mut h);
    end_query.hash(&mut h);
    aggregations_json.unwrap_or("").hash(&mut h);
    orig_base_query.hash(&mut h);
    h.finish()
}

#[cfg(any(test, feature = "pg_test"))]
pub fn reset_delta_sql_cache() {
    if let Ok(mut guard) = delta_sql_cache().lock() {
        guard.clear();
    }
}

/// Whether a delta adds or subtracts from the intermediate table.
#[derive(Clone, Copy)]
pub enum DeltaOp {
    Add,
    Subtract,
}

/// Generates the SQL statements to apply a delta to an IMV.
///
/// Called from plpgsql trigger wrappers. Returns a delimiter-separated string
/// of SQL statements for the plpgsql function to EXECUTE.
#[pg_extern(parallel_safe)]
pub fn reflex_build_delta_sql(
    view_name: &str,
    source_table: &str,
    operation: &str,
    base_query: &str,
    end_query: &str,
    aggregations_json: Option<&str>,
    orig_base_query: &str,
) -> String {
    let cache_key = delta_sql_cache_key(
        view_name,
        source_table,
        operation,
        base_query,
        end_query,
        aggregations_json,
        orig_base_query,
    );
    if let Ok(guard) = delta_sql_cache().lock() {
        if let Some(cached) = guard.get(&cache_key) {
            return cached.clone();
        }
    }

    // aggregations_json is written by pg_reflex itself via generate_aggregations_json
    // (which is now infallible — see query_decomposer.rs:751-754). A malformed
    // value would mean catalog corruption, not user error; failing loudly
    // beats silently emitting empty SQL.
    let json = aggregations_json.unwrap_or("{}");
    let plan: AggregationPlan = serde_json::from_str(json).unwrap_or_else(|e| {
        panic!(
            "pg_reflex: __reflex_ivm_reference.aggregations for '{}' must be valid JSON (catalog invariant violated: {})",
            view_name, e
        )
    });

    let intermediate_tbl = intermediate_table_name(view_name);
    // Use the transition table names directly (no temp table copy needed).
    // Transition tables are visible in plpgsql EXECUTE context.
    let new_tbl = transition_new_table_name(source_table);
    let old_tbl = transition_old_table_name(source_table);

    let mut stmts: Vec<String> = Vec::new();

    let mut pending_dispatch: Option<PendingDispatch> = None;

    // Pre-compute group columns and affected-groups table name (used by multiple paths).
    // Affected / shrunk / scratch live in the IMV's schema (1.4.1) so the generated
    // SQL works under any session `search_path`.
    let grp_cols = group_columns(&plan);
    let affected_tbl = affected_groups_table_name(view_name);
    let shrunk_tbl = shrunk_groups_table_name(view_name);
    let scratch_tbl = delta_scratch_table_name(view_name);

    // Detect cases where standard incremental delta is incorrect:
    // 1. Self-join: source_table appears multiple times in base_query
    // 2. LEFT/RIGHT JOIN secondary table DELETE/UPDATE: NULL semantics can't be captured by MERGE subtract
    let bare_source = split_qualified_name(source_table).1;
    // Detect self-join and outer-join-secondary for BOTH aggregate and passthrough queries.
    let occurrences = base_query
        .split_whitespace()
        .filter(|w| {
            let trimmed = w.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
            trimmed == source_table || trimmed == bare_source
        })
        .count();
    let is_self_join = occurrences > 1;

    let bq_upper = base_query.to_uppercase();
    let is_full_outer = bq_upper.contains("FULL JOIN") || bq_upper.contains("FULL OUTER");
    // Check if source_table is the secondary table in a LEFT/RIGHT/FULL JOIN.
    // The source is secondary if it appears as the table being outer-joined,
    // i.e. directly after "LEFT JOIN", "RIGHT JOIN", or "FULL JOIN".
    // Do NOT match if source_table only appears in ON conditions (that's the primary table).
    // Strip surrounding quotes: a CTE sub-IMV / schema-qualified secondary is
    // registered (and passed here) quoted, e.g. `"v__cte_agg"`, but the
    // JOIN-keyword scan compares against the unquoted token in base_query.
    let src_upper = source_table.trim_matches('"').to_uppercase();
    let bare_upper = bare_source.trim_matches('"').to_uppercase();
    let is_outer_join_secondary_table = !is_self_join
        && (bq_upper.contains("LEFT JOIN")
            || bq_upper.contains("RIGHT JOIN")
            || bq_upper.contains("LEFT OUTER")
            || bq_upper.contains("RIGHT OUTER")
            || is_full_outer)
        && {
            // Check if source_table appears directly after an outer JOIN keyword
            let patterns = [
                "LEFT JOIN ",
                "LEFT OUTER JOIN ",
                "RIGHT JOIN ",
                "RIGHT OUTER JOIN ",
                "FULL JOIN ",
                "FULL OUTER JOIN ",
            ];
            patterns.iter().any(|pat| {
                let mut search_from = 0;
                while let Some(pos) = bq_upper[search_from..].find(pat) {
                    let after = &bq_upper[search_from + pos + pat.len()..];
                    // Strip surrounding double-quotes: a CTE sub-IMV or schema-
                    // qualified secondary is emitted quoted (`LEFT JOIN "v__cte_x" a`),
                    // and the source_table registry name is unquoted — without
                    // this the match (and thus the whole outer-join-secondary
                    // handling) silently misses every quoted secondary.
                    let next_token = after
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .trim_matches('"');
                    if next_token == src_upper || next_token == bare_upper {
                        return true;
                    }
                    search_from += pos + pat.len();
                }
                false
            })
        };
    // For LEFT/RIGHT JOIN: EVERY operation on the secondary table needs special
    // handling. The plain `L LEFT JOIN Δsecondary` delta re-emits all left rows
    // NULL-extended, which double-counts non-matching left rows on INSERT (breaks
    // COUNT(*) and any secondary-derived group key) and can't represent NULL
    // semantics on DELETE/UPDATE. For FULL OUTER JOIN: ALL operations on BOTH
    // tables need targeted reconcile, because the FULL JOIN delta always includes
    // unmatched rows from the other side.
    let is_outer_join_secondary = (is_outer_join_secondary_table
        && matches!(
            operation,
            "DELETE" | "DELETE_PROMOTED" | "UPDATE" | "INSERT" | "INSERT_PROMOTED"
        ))
        || (is_full_outer && !is_self_join);

    if is_self_join {
        self_join_full_refresh_stmts(
            view_name,
            base_query,
            end_query,
            &intermediate_tbl,
            &plan,
            &mut stmts,
        );
    } else if is_outer_join_secondary {
        outer_join_secondary_stmts(
            view_name,
            source_table,
            operation,
            base_query,
            end_query,
            &plan,
            &grp_cols,
            &intermediate_tbl,
            &affected_tbl,
            &old_tbl,
            &new_tbl,
            &mut stmts,
        );
    } else if plan.is_passthrough {
        passthrough_op_stmts(
            view_name,
            source_table,
            operation,
            base_query,
            &plan,
            &new_tbl,
            &old_tbl,
            &mut stmts,
        );
    } else {
        let has_min_max = plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX");

        match operation {
            "INSERT" | "INSERT_PROMOTED" => {
                aggregate_insert_stmts(
                    operation,
                    &plan,
                    base_query,
                    source_table,
                    &grp_cols,
                    &intermediate_tbl,
                    &affected_tbl,
                    &scratch_tbl,
                    &new_tbl,
                    &mut stmts,
                );
            }
            "DELETE" | "DELETE_PROMOTED" => {
                let took_bulk_early_return = aggregate_delete_stmts(
                    &plan,
                    view_name,
                    source_table,
                    base_query,
                    end_query,
                    orig_base_query,
                    has_min_max,
                    &grp_cols,
                    &intermediate_tbl,
                    &affected_tbl,
                    &scratch_tbl,
                    &old_tbl,
                    &mut stmts,
                );
                if took_bulk_early_return {
                    // Cache + return — no target sync needed (bulk-DELETE
                    // already removed target rows).
                    let result = stmts.join("\n--<<REFLEX_SEP>>--\n");
                    if let Ok(mut guard) = delta_sql_cache().lock() {
                        if guard.len() >= DELTA_SQL_CACHE_MAX {
                            guard.clear();
                        }
                        guard.insert(cache_key, result.clone());
                    }
                    return result;
                }
            }
            "UPDATE" => {
                if let Some(merge_sql) = aggregate_update_stmts(
                    &plan,
                    source_table,
                    base_query,
                    orig_base_query,
                    has_min_max,
                    &grp_cols,
                    &intermediate_tbl,
                    &affected_tbl,
                    &shrunk_tbl,
                    &scratch_tbl,
                    &old_tbl,
                    &new_tbl,
                    &mut stmts,
                ) {
                    pending_dispatch = Some(PendingDispatch { merge_sql });
                }
            }
            _ => {}
        }

        aggregate_epilogue_stmts(
            view_name,
            source_table,
            operation,
            end_query,
            &plan,
            &grp_cols,
            &intermediate_tbl,
            &affected_tbl,
            &scratch_tbl,
            pending_dispatch.take(),
            &mut stmts,
        );
    }

    // Historical note (2026-04-24): an earlier version of this function
    // guarded against *any* transition-table reference outside a sanctioned
    // scratch-populate INSERT. That guard existed under the hypothesis that
    // `EXECUTE '…__reflex_new_*…'` inside a trigger body was the root cause
    // of the backend SIGSEGV/SIGABRT we were seeing. The real root cause
    // turned out to be in build.rs — weak stub definitions of
    // `CurrentMemoryContext` etc. were leaking into the installed cdylib,
    // shadowing postgres's real globals and causing NULL derefs in pgrx's
    // SPI path. With that fixed, transition-table references in EXECUTE
    // are safe again and the guard was over-rejecting legitimate full-
    // refresh SQL (e.g. the LEFT JOIN secondary-table fallback that does
    // `DELETE FROM target; INSERT INTO target <end_query>` where end_query
    // can legitimately read from a transition table in some code paths).

    let result = stmts.join("\n--<<REFLEX_SEP>>--\n");

    if let Ok(mut guard) = delta_sql_cache().lock() {
        if guard.len() >= DELTA_SQL_CACHE_MAX {
            guard.clear();
        }
        guard.insert(cache_key, result.clone());
    }

    result
}

/// Returns the rewritten scratch-fill SQL for a Item α `INSERT_PROMOTED`
/// bulk-INSERT: `base_query` with `source_table` rewritten to its
/// `__reflex_new_*` transition table, identical to what the bulk-INSERT
/// codegen runs. The trigger function body wraps this in
/// `EXPLAIN (FORMAT JSON)` to read the planner's row estimate without
/// executing the JOIN, then compares against the IMV's wipe threshold.
///
/// The PL/pgSQL trigger calls EXPLAIN itself rather than delegating it here
/// because nested SPI contexts cannot see the transition tables created in
/// the outer trigger's scope.
///
/// Returns the empty string if the IMV has no row in
/// `__reflex_ivm_reference` (e.g., dropped between scan and call).
#[pg_extern(parallel_safe)]
pub fn reflex_build_path_c_explain_sql(view_name: &str, source_table: &str) -> String {
    // Path C bulk-INSERT skips the conflict-aware MERGE on the assumption
    // that the affected slice of intermediate group keys cannot already be
    // populated. That assumption only holds when the source's identity
    // uniquely determines its slice of keys — i.e., when the analyser
    // captured a `source_join_keys` entry for this source. For single-source
    // aggregates one source row can feed many group keys, and other rows
    // (filter-passed before this UPDATE) may already be contributing to
    // those groups; bulk-INSERT then duplicates rows in the intermediate /
    // target and silently wrong-answers. Match the Rust-side
    // `aggregate_insert_stmts` gate (`plan.source_join_keys.contains_key`)
    // by returning the empty string here, which the trigger body's
    // `IF _pc_sql <> ''` check turns into a Path C skip → falls into the
    // standard MERGE path.
    let escaped_view = view_name.replace('\'', "''");
    let lookup_sql = format!(
        "SELECT base_query, aggregations::text AS agg \
         FROM public.__reflex_ivm_reference WHERE name = '{}' AND enabled = TRUE",
        escaped_view
    );
    let (base_query, agg_json): (String, String) = match Spi::connect(|client| {
        client
            .select(&lookup_sql, None, &[])
            .ok()
            .and_then(|mut it| it.next())
            .and_then(|row| {
                let bq = row
                    .get_by_name::<&str, _>("base_query")
                    .ok()
                    .flatten()
                    .map(|s| s.to_string())?;
                let agg = row
                    .get_by_name::<&str, _>("agg")
                    .ok()
                    .flatten()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "{}".to_string());
                Some((bq, agg))
            })
    }) {
        Some(t) => t,
        None => return String::new(),
    };
    match serde_json::from_str::<AggregationPlan>(&agg_json) {
        Ok(plan) if plan.source_join_keys.contains_key(source_table) => {}
        _ => return String::new(),
    }
    let transition = transition_new_table_name(source_table);
    replace_source_with_transition(&base_query, source_table, &transition)
}

/// Resolve an IMV name to its already-quoted intermediate-table reference.
///
/// Thin SQL wrapper over [`intermediate_table_name`]: the canonical builder
/// handles schema-qualified vs bare names uniformly and routes long names
/// through `safe_identifier` so the returned string always matches the
/// real relation pg_reflex created. Path C in the UPDATE trigger body uses
/// this instead of constructing `'"' || split_part(name, '.', 1) || '"."__reflex_intermediate_' || split_part(name, '.', 2) || '"'`
/// — that ad-hoc form silently breaks on bare IMV names (the second
/// `split_part` returns empty) and on long view names that need the
/// `safe()` truncation+hash suffix.
#[pg_extern(parallel_safe, immutable)]
pub fn reflex_intermediate_table_name(view_name: &str) -> String {
    intermediate_table_name(view_name)
}

/// Resolve an IMV name to its already-quoted delta-scratch-table reference.
/// Same reason as [`reflex_intermediate_table_name`].
#[pg_extern(parallel_safe, immutable)]
pub fn reflex_delta_scratch_table_name(view_name: &str) -> String {
    delta_scratch_table_name(view_name)
}

/// Quote a (possibly schema-qualified) identifier the same way every other
/// pg_reflex name-builder does. Path C uses this for the IMV's target-table
/// reference inside the plpgsql trigger body, where it would otherwise
/// re-do the dot-split + double-quote concat by hand and get the bare-name
/// case wrong.
#[pg_extern(parallel_safe, immutable)]
pub fn reflex_quote_identifier(name: &str) -> String {
    quote_identifier(name)
}

/// Generates SQL statements to handle a TRUNCATE on a source table.
/// TRUNCATE has no transition tables, so we clear intermediate + target entirely.
#[pg_extern(parallel_safe)]
pub fn reflex_build_truncate_sql(view_name: &str) -> String {
    let intermediate_tbl = intermediate_table_name(view_name);

    // Check if this is a passthrough IMV by reading aggregations from the reference table
    let agg_json: String = Spi::get_one::<&str>(&format!(
        "SELECT aggregations::text FROM public.__reflex_ivm_reference WHERE name = '{}'",
        view_name.replace("'", "''")
    ))
    .unwrap_or(None)
    .unwrap_or("{}")
    .to_string();

    let is_passthrough = if let Ok(plan) = serde_json::from_str::<AggregationPlan>(&agg_json) {
        plan.is_passthrough
    } else {
        false
    };

    let mut stmts: Vec<String> = Vec::new();

    if is_passthrough {
        // Passthrough: just clear the target, then re-insert from source (which is now empty)
        stmts.push(format!("DELETE FROM {}", quote_identifier(view_name)));
    } else {
        // Aggregate: clear intermediate and target
        stmts.push(format!("TRUNCATE {}", intermediate_tbl));
        stmts.push(format!("DELETE FROM {}", quote_identifier(view_name)));
    }

    // Update last_update_date (lazy: skip if updated within the last second)
    stmts.push(format!(
        "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() \
         WHERE name = '{}' AND (last_update_date IS NULL OR last_update_date < NOW() - INTERVAL '1 second')",
        view_name.replace("'", "''")
    ));

    stmts.join("\n--<<REFLEX_SEP>>--\n")
}

/// Theme 5.3: execute a `\n--<<REFLEX_SEP>>--\n`-separated SQL string, running
/// each non-empty statement in order. Replaces the `string_to_array + FOREACH`
/// pattern in generated trigger bodies with a single Rust-side call — smaller
/// trigger DDL, no intermediate array allocation.
#[pg_extern]
pub fn reflex_execute_separated(sql: &str) {
    for stmt in sql.split("\n--<<REFLEX_SEP>>--\n") {
        let trimmed = stmt.trim();
        if !trimmed.is_empty() {
            Spi::run(trimmed).unwrap_or_report();
        }
    }
}

mod deferred;
mod dispatch;
mod merge;
mod ops;
mod scope;

#[cfg(test)]
pub(crate) use deferred::build_netted_view_sql;
pub(crate) use dispatch::*;
pub(crate) use merge::*;
pub(crate) use ops::*;
pub(crate) use scope::*;

#[cfg(test)]
#[path = "../tests/unit_trigger.rs"]
mod tests;
