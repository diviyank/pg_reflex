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
    quote_identifier, shrunk_groups_table_name, transition_new_table_name,
    transition_old_table_name,
};
use crate::sql_writer::identifier::{
    replace_source_with_transition, source_rewrite_breaks_quoted_identifier, token_names_source,
};
use outer_join::{outer_joins, OuterJoins};

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

fn cached_delta_sql(cache_key: u64) -> Option<String> {
    delta_sql_cache().lock().ok()?.get(&cache_key).cloned()
}

fn remember_delta_sql(cache_key: u64, sql: &str) {
    if let Ok(mut guard) = delta_sql_cache().lock() {
        if guard.len() >= DELTA_SQL_CACHE_MAX {
            guard.clear();
        }
        guard.insert(cache_key, sql.to_string());
    }
}

#[cfg(any(test, feature = "pg_test"))]
pub fn delta_sql_cache_len() -> usize {
    delta_sql_cache().lock().map_or(0, |guard| guard.len())
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

/// Operation the DEFERRED flush passes to `reflex_build_delta_sql`: an UPDATE
/// whose transition views hold the transaction's NETTED delta (inserts, deletes
/// and updates of every statement folded into one old / new pair). Generated as
/// an `"UPDATE"` except where a statement-level UPDATE must be treated
/// differently (see `passthrough_op_stmts`); distinct so it is cached apart.
pub(crate) const NETTED_UPDATE_OP: &str = "UPDATE_NETTED";

/// Number of whitespace-delimited tokens of `base_query` that name
/// `source_table`. More than one means the source is joined to itself.
pub(crate) fn source_reference_count(base_query: &str, source_table: &str) -> usize {
    let is_word_char = |c: char| c.is_alphanumeric() || c == '_';
    base_query
        .split_whitespace()
        .filter(|w| {
            let bare_word = w.trim_matches(|c: char| !is_word_char(c));
            let quoted_word = w.trim_matches(|c: char| !is_word_char(c) && c != '"');
            token_names_source(bare_word, source_table)
                || token_names_source(quoted_word, source_table)
        })
        .count()
}

/// `sql` upper-cased with every run of whitespace collapsed to one space.
#[cfg(test)]
pub(crate) fn upper_with_single_spaces(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase()
}

/// Whether `source_table` is the table named directly after a LEFT / RIGHT /
/// FULL (OUTER) JOIN keyword of the upper-cased query. A source that only
/// appears in an ON condition does not count.
#[cfg(test)]
pub(crate) fn follows_outer_join_keyword(bq_upper: &str, source_table: &str) -> bool {
    let source_upper = source_table.to_uppercase();
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
            let next_token = after.split_whitespace().next().unwrap_or("");
            if token_names_source(next_token, &source_upper) {
                return true;
            }
            search_from += pos + pat.len();
        }
        false
    })
}

/// Whether a write to `source_table` has no per-source delta, so the IMV is
/// recomputed from `base_query`: the source is read more than once, the query
/// holds a RIGHT JOIN, the source is read inside a derived table or a
/// parenthesised join that a LEFT JOIN null-extends, the source is read by a
/// subquery used as an expression, or the transition-table rewrite would break
/// a quoted identifier of another relation.
///
/// A RIGHT JOIN is never scoped. The table before the keyword is the
/// null-extended one, and the per-source key mappings of the plan assume the
/// null-extended table is the joined one: a group or passthrough key read from
/// the table before the keyword is NULL on the unmatched rows of the table
/// after it.
pub(crate) fn has_no_scoped_delta(base_query: &str, source_table: &str) -> bool {
    outer_joins_leave_no_scoped_delta(&outer_joins(base_query, source_table))
        || source_reference_count(base_query, source_table) > 1
        || expression_subquery::base_query_reads_source_by_expression_subquery(
            base_query,
            source_table,
        )
        || source_rewrite_breaks_quoted_identifier(base_query, source_table)
}

fn outer_joins_leave_no_scoped_delta(joins: &OuterJoins) -> bool {
    joins.source_reads > 1
        || joins.holds_right_join
        || joins.source_inside_left_joined_subtree
        || joins.source_name_stands_as_a_value
}

pub(crate) use expression_subquery::end_query_reads_source;

/// [`has_no_scoped_delta`] over the whole stored query of an IMV: a source
/// read by a subquery of `end_query` (HAVING, an expression over an aggregate)
/// has no scoped delta either.
pub(crate) fn imv_has_no_scoped_delta(
    base_query: &str,
    end_query: &str,
    source_table: &str,
) -> bool {
    has_no_scoped_delta(base_query, source_table) || end_query_reads_source(end_query, source_table)
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
    let netted_delta = operation == NETTED_UPDATE_OP;
    let operation = if netted_delta { "UPDATE" } else { operation };

    // aggregations_json is written by pg_reflex itself via generate_aggregations_json
    // (which is infallible). A malformed
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
    // Affected / shrunk / scratch live in the IMV's schema so the generated
    // SQL works under any session `search_path`.
    let grp_cols = group_columns(&plan);
    let affected_tbl = affected_groups_table_name(view_name);
    let shrunk_tbl = shrunk_groups_table_name(view_name);
    let scratch_tbl = delta_scratch_table_name(view_name);

    // Detect cases where standard incremental delta is incorrect:
    // 1. Self-join: source_table appears multiple times in base_query
    // 2. LEFT/RIGHT JOIN secondary table DELETE/UPDATE: NULL semantics can't be captured by MERGE subtract
    // Detect self-join and outer-join-secondary for BOTH aggregate and passthrough queries.
    let joins = outer_joins(base_query, source_table);
    let recompute_in_full = outer_joins_leave_no_scoped_delta(&joins)
        || source_reference_count(base_query, source_table) > 1
        || source_rewrite_breaks_quoted_identifier(base_query, source_table)
        || expression_subquery::imv_reads_source_by_expression_subquery(
            base_query,
            end_query,
            source_table,
        );
    // For LEFT JOIN: EVERY operation on the secondary table needs special
    // handling. The plain `L LEFT JOIN Δsecondary` delta re-emits all left rows
    // NULL-extended, which double-counts non-matching left rows on INSERT (breaks
    // COUNT(*) and any secondary-derived group key) and can't represent NULL
    // semantics on DELETE/UPDATE. For FULL OUTER JOIN: ALL operations on BOTH
    // tables need targeted reconcile, because the FULL JOIN delta always includes
    // unmatched rows from the other side.
    let is_outer_join_secondary = joins.holds_full_join
        || (joins.source_is_null_extended_table
            && matches!(
                operation,
                "DELETE" | "DELETE_PROMOTED" | "UPDATE" | "INSERT" | "INSERT_PROMOTED"
            ));

    if recompute_in_full {
        full_refresh_stmts(
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
    } else if union_delta::source_requires_recompute(base_query, source_table) {
        // The mutated source sits inside a non-distributive set-op
        // (UNION/INTERSECT/EXCEPT) subquery: no incremental delta is valid, so
        // recompute the affected IMV in full. Covers both passthrough and
        // aggregate shapes (full_refresh_stmts branches on plan.is_passthrough).
        full_refresh_stmts(
            view_name,
            base_query,
            end_query,
            &intermediate_tbl,
            &plan,
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
            netted_delta,
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
                    view_name,
                    netted_delta,
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
                    if netted_delta {
                        pending_dispatch = Some(PendingDispatch { merge_sql });
                    } else {
                        // A statement trigger never rebuilds (see `passthrough_op_stmts`):
                        // run the MERGE inline and let the epilogue sync the target.
                        stmts.push(merge_sql);
                        stmts.push(format!("ANALYZE {}", intermediate_tbl));
                    }
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

    // Transition-table references inside EXECUTE are safe in trigger bodies, so
    // full-refresh SQL (e.g. the LEFT JOIN secondary-table fallback `DELETE FROM
    // target; INSERT INTO target <end_query>`, whose end_query may read a
    // transition table) is not guarded against them.

    let result = stmts.join("\n--<<REFLEX_SEP>>--\n");

    if let Ok(mut guard) = delta_sql_cache().lock() {
        if guard.len() >= DELTA_SQL_CACHE_MAX {
            guard.clear();
        }
        guard.insert(cache_key, result.clone());
    }

    result
}

/// Returns the rewritten scratch-fill SQL for an `INSERT_PROMOTED`
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
    // those groups; bulk-INSERT would then duplicate rows in the intermediate /
    // target and silently give wrong answers. Match the Rust-side
    // `aggregate_insert_stmts` gate (`plan.source_join_keys.contains_key`)
    // by returning the empty string here, which the trigger body's
    // `IF _pc_sql <> ''` check turns into a Path C skip → falls into the
    // standard MERGE path.
    let escaped_view = view_name.replace('\'', "''");
    let lookup_sql = format!(
        "SELECT base_query, COALESCE(end_query, '') AS end_query, aggregations::text AS agg \
         FROM public.__reflex_ivm_reference WHERE name = '{}' AND enabled = TRUE",
        escaped_view
    );
    let (base_query, end_query, agg_json): (String, String, String) = match Spi::connect(|client| {
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
                let end_query = row
                    .get_by_name::<&str, _>("end_query")
                    .ok()
                    .flatten()
                    .map(|s| s.to_string())?;
                let agg = row
                    .get_by_name::<&str, _>("agg")
                    .ok()
                    .flatten()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "{}".to_string());
                Some((bq, end_query, agg))
            })
    }) {
        Some(t) => t,
        None => return String::new(),
    };
    let cache_key = delta_sql_cache_key(
        view_name,
        source_table,
        PATH_C_EXPLAIN_OP,
        &base_query,
        "",
        Some(&agg_json),
        "",
    );
    if let Some(cached) = cached_delta_sql(cache_key) {
        return cached;
    }
    let explain_sql = path_c_explain_sql(source_table, &base_query, &end_query, &agg_json);
    remember_delta_sql(cache_key, &explain_sql);
    explain_sql
}

/// Cache-key operation of the Path C statement, distinct from every DML operation.
const PATH_C_EXPLAIN_OP: &str = "PATH_C_EXPLAIN";

fn path_c_explain_sql(
    source_table: &str,
    base_query: &str,
    end_query: &str,
    agg_json: &str,
) -> String {
    match serde_json::from_str::<AggregationPlan>(agg_json) {
        Ok(plan) if plan.source_join_keys.contains_key(source_table) => {}
        _ => return String::new(),
    }
    // The swap keeps the unmatched rows of a FULL JOIN's other table, whose
    // groups the intermediate table can already hold.
    if imv_has_no_scoped_delta(base_query, end_query, source_table)
        || outer_join::holds_full_join(base_query)
    {
        return String::new();
    }
    let transition = transition_new_table_name(source_table);
    replace_source_with_transition(base_query, source_table, &transition)
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
///
/// TRUNCATE has no transition tables, so the IMV is rebuilt from its base
/// query. Clearing it instead is only right when the truncated source empties
/// the result (the anchor, an inner join); on the nullable side of an outer
/// join, under NOT EXISTS, or for an ungrouped aggregate the result keeps rows,
/// and clearing would delete them outright — in DEFERRED mode outside the
/// flush's savepoint, so a failed flush would make the loss permanent. Where
/// clearing is right, the rebuild reads an empty join and costs about the same.
///
/// IMMEDIATE: rebuilt now, reaching its dependents as a row diff.
/// DEFERRED: untouched now. The IMV is listed in `__reflex_deferred_rebuild` and
/// a pending row is enqueued on a source it observes, so the COMMIT-time flush
/// rebuilds it and skips every delta staged for it before that rebuild (see
/// `deferred::rebuild_truncated_imvs`). A TRUNCATE after that rebuild re-arms it.
#[pg_extern(parallel_safe)]
pub fn reflex_build_truncate_sql(view_name: &str) -> String {
    let escaped_name = view_name.replace('\'', "''");
    let mut stmts = match truncate_rebuild(view_name) {
        TruncateRebuild::Stmts(stmts) => stmts,
        TruncateRebuild::Wrapper => return String::new(),
        TruncateRebuild::Unreadable => return mark_stale_after_truncate_sql(view_name),
    };

    if registry_value(view_name, "COALESCE(refresh_mode, 'IMMEDIATE')") == "DEFERRED" {
        let Some(enqueue) = enqueue_truncate_flush_sql(view_name) else {
            return mark_stale_after_truncate_sql(view_name);
        };
        // The request row lives until its own event deletes it (flush_fn). A
        // trigger body that deleted it early would still be harmless: the queued
        // event still fires (AfterTriggerExecute fetches with SnapshotAny).
        stmts = vec![
            DEFERRED_REBUILD_TABLE_DDL.to_string(),
            format!(
                "INSERT INTO __reflex_deferred_rebuild VALUES ('{escaped_name}') ON CONFLICT DO NOTHING"
            ),
            deferred::rearm_rebuild_sql(view_name),
            enqueue,
        ];
    }

    // Update last_update_date (lazy: skip if updated within the last second)
    stmts.push(format!(
        "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() \
         WHERE name = '{escaped_name}' AND (last_update_date IS NULL OR last_update_date < NOW() - INTERVAL '1 second')"
    ));

    stmts.join("\n--<<REFLEX_SEP>>--\n")
}

/// Transaction-local list of DEFERRED IMVs to rebuild at COMMIT — their source
/// was truncated, or the cross-source guard engaged on them (`reconcile`: rebuilt
/// through `reflex_reconcile`) — with the number of times their rebuild was
/// postponed.
pub(crate) const DEFERRED_REBUILD_TABLE_DDL: &str =
    "CREATE TEMP TABLE IF NOT EXISTS __reflex_deferred_rebuild \
     (name TEXT PRIMARY KEY, attempts INT NOT NULL DEFAULT 0, \
      reconcile BOOLEAN NOT NULL DEFAULT FALSE) ON COMMIT DROP";

/// The first table `view_name` reads and does not ignore: a pending row on it makes
/// a flush that selects `view_name` run at COMMIT. `None` when it ignores every
/// source. The placeholder of a derived table (`<subquery:x>`) is no table: it has
/// no staging table to flush.
pub(crate) fn observed_source(view_name: &str) -> Option<String> {
    let source = registry_value(
        view_name,
        "(SELECT d FROM unnest(depends_on) WITH ORDINALITY AS u(d, i) \
          WHERE NOT (COALESCE(ignored_sources, ARRAY[]::TEXT[]) \
                     && ARRAY[d, regexp_replace(d, '^.*\\.', '')]) \
            AND d NOT LIKE '<%' \
          ORDER BY i LIMIT 1)",
    );
    (!source.is_empty()).then_some(source)
}

/// Requests a COMMIT-time flush that selects `view_name` (see [`observed_source`]).
pub(crate) fn enqueue_truncate_flush_sql(view_name: &str) -> Option<String> {
    observed_source(view_name).map(|source| {
        format!(
            "INSERT INTO public.__reflex_deferred_pending (source_table, operation) \
             VALUES ('{}', 'TRUNCATE')",
            source.replace('\'', "''")
        )
    })
}

fn registry_value(view_name: &str, expr: &str) -> String {
    Spi::get_one::<&str>(&format!(
        "SELECT ({expr})::text FROM public.__reflex_ivm_reference WHERE name = '{}'",
        view_name.replace('\'', "''")
    ))
    .unwrap_or(None)
    .unwrap_or("")
    .to_string()
}

pub(crate) enum TruncateRebuild {
    /// Intermediate refilled, target through the dependent-safe diff.
    Stmts(Vec<String>),
    /// A decomposed wrapper, refused like `reconcile_one` refuses it: nothing to
    /// do, its operands are the truncated source's dependents and maintain it.
    Wrapper,
    Unreadable,
}

/// How `view_name` is rebuilt from its base query after a source TRUNCATE.
pub(crate) fn truncate_rebuild(view_name: &str) -> TruncateRebuild {
    if crate::reconcile::is_decomposed_wrapper_row(view_name) {
        return TruncateRebuild::Wrapper;
    }
    let base_query = registry_value(view_name, "base_query");
    let end_query = registry_value(view_name, "end_query");
    let plan =
        match serde_json::from_str::<AggregationPlan>(&registry_value(view_name, "aggregations")) {
            Ok(plan) if !base_query.is_empty() => plan,
            _ => return TruncateRebuild::Unreadable,
        };
    let mut stmts: Vec<String> = Vec::new();
    full_refresh_stmts(
        view_name,
        &base_query,
        &end_query,
        &intermediate_table_name(view_name),
        &plan,
        &mut stmts,
    );
    TruncateRebuild::Stmts(stmts)
}

/// Marks `view_name` stale when a source TRUNCATE cannot rebuild it; never clears it.
pub(crate) fn mark_stale_after_truncate_sql(view_name: &str) -> String {
    warning!(
        "pg_reflex: cannot rebuild '{}' after a source TRUNCATE — registry row unreadable \
         or observes no source; marking it stale instead of clearing it",
        view_name
    );
    let escaped_name = view_name.replace('\'', "''");
    format!(
        "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, \
         stale_since = COALESCE(stale_since, now()), \
         stale_reason = 'source truncated and the IMV could not be rebuilt; \
         run reflex_reconcile(''{escaped_name}'')' \
         WHERE name = '{escaped_name}'"
    )
}

/// Execute a `\n--<<REFLEX_SEP>>--\n`-separated SQL string, running
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

pub(crate) mod deferred;
mod dispatch;
mod expression_subquery;
mod merge;
mod minmax_bulk_replace;
mod ops;
mod outer_join;
mod scope;
mod union_delta;

#[cfg(test)]
pub(crate) use deferred::build_netted_view_sql;
pub(crate) use dispatch::*;
pub(crate) use merge::*;
pub(crate) use minmax_bulk_replace::*;
pub(crate) use ops::*;
pub(crate) use scope::*;

#[cfg(test)]
#[path = "../tests/unit_trigger.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/unit_trigger_source_guards.rs"]
mod source_guard_tests;
