use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::Spi;
use pgrx::PgBuiltInOids;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, OnceLock};

use crate::aggregation::AggregationPlan;
use crate::query_decomposer::{
    affected_groups_table_name, delta_scratch_table_name, intermediate_table_name,
    normalized_column_name, passthrough_scratch_new_table_name, passthrough_scratch_old_table_name,
    quote_identifier, shrunk_groups_table_name, split_qualified_name, staging_delta_table_name,
    transition_new_table_name, transition_old_table_name,
};

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

/// Build a MERGE statement that merges a delta query into the intermediate table.
/// Used directly by unit tests; production code goes through `push_materialized_merge`.
/// MERGE is 3-4x faster than INSERT...ON CONFLICT because it uses a hash join
/// strategy instead of per-row index probes for conflict resolution.
#[cfg_attr(not(test), allow(dead_code))]
pub fn build_merge_sql(
    intermediate_tbl: &str,
    delta_query: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) -> String {
    build_merge_using(intermediate_tbl, &format!("({})", delta_query), plan, op)
}

/// Like `build_merge_sql` but reads the delta from a pre-materialized table rather
/// than an inline subquery.  Use this when the delta may reference a transition
/// table — PostgreSQL's MERGE rejects transition-table references inside a USING
/// subquery executed via EXECUTE (triggers a PG cassert / SIGABRT on cassert builds).
fn build_merge_from_table_sql(
    intermediate_tbl: &str,
    scratch_tbl: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) -> String {
    // `scratch_tbl` is pre-qualified (`"schema"."local"` or bare local) by
    // `delta_scratch_table_name`, so no extra quoting here.
    build_merge_using(intermediate_tbl, scratch_tbl, plan, op)
}

fn build_merge_using(
    intermediate_tbl: &str,
    using_clause: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) -> String {
    // Join columns = group_by + distinct (normalized lowercase names)
    let mut join_cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| format!("\"{}\"", normalized_column_name(c)))
        .collect();

    // For aggregates without GROUP BY: use sentinel column
    if join_cols.is_empty() && !plan.intermediate_columns.is_empty() {
        join_cols.push("__reflex_group".to_string());
    }

    let operator = match op {
        DeltaOp::Add => "+",
        DeltaOp::Subtract => "-",
    };

    // ON clause: prefer `=` for NOT NULL columns so a composite btree index on
    // the group columns is usable by the planner. `IS NOT DISTINCT FROM` is
    // semantically nicer (treats NULL = NULL as true) but is NOT index-usable
    // on NULLable columns via plain btree, even with a `UNIQUE NULLS NOT
    // DISTINCT` constraint. For columns known NOT NULL at IMV-create time,
    // `IS NOT DISTINCT FROM` reduces to `=` semantically; we emit the
    // index-friendly `=` form. NULLable columns keep `IS NOT DISTINCT FROM`
    // so groups with NULL group-keys still match.
    //
    // The NOT NULL set lives directly on the AggregationPlan (populated at
    // IMV-create time by `query_column_types_from_catalog` and persisted in
    // the aggregations JSON). No SPI lookup needed at MERGE-codegen time.
    let on_clause = join_cols
        .iter()
        .map(|c| {
            // `c` is `"col"` (already quoted). Unquote for the lookup.
            let unquoted = c.trim_matches('"');
            if plan.not_null_columns.contains(unquoted) {
                format!("t.{} = d.{}", c, c)
            } else {
                format!("t.{} IS NOT DISTINCT FROM d.{}", c, c)
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    // WHEN MATCHED THEN UPDATE SET clauses
    let mut set_clauses: Vec<String> = Vec::new();
    for ic in &plan.intermediate_columns {
        match (ic.source_aggregate.as_str(), op) {
            ("MIN", DeltaOp::Add) => {
                set_clauses.push(format!(
                    "\"{}\" = LEAST(t.\"{}\", d.\"{}\")",
                    ic.name, ic.name, ic.name
                ));
                // Top-K MIN: merge intermediate top-K with delta top-K, keep K smallest.
                if let Some(k) = ic.topk_k {
                    let topk = ic.topk_column_name();
                    set_clauses.push(format!(
                        "\"{topk}\" = (\
                         SELECT array_agg(v ORDER BY v ASC) FROM \
                         (SELECT v FROM unnest(t.\"{topk}\" || COALESCE(d.\"{topk}\", '{{}}'::{ty}[])) v ORDER BY v ASC LIMIT {k}) s)",
                        topk = topk,
                        ty = ic.pg_type,
                        k = k,
                    ));
                }
            }
            ("MAX", DeltaOp::Add) => {
                set_clauses.push(format!(
                    "\"{}\" = GREATEST(t.\"{}\", d.\"{}\")",
                    ic.name, ic.name, ic.name
                ));
                if let Some(k) = ic.topk_k {
                    let topk = ic.topk_column_name();
                    set_clauses.push(format!(
                        "\"{topk}\" = (\
                         SELECT array_agg(v ORDER BY v DESC) FROM \
                         (SELECT v FROM unnest(t.\"{topk}\" || COALESCE(d.\"{topk}\", '{{}}'::{ty}[])) v ORDER BY v DESC LIMIT {k}) s)",
                        topk = topk,
                        ty = ic.pg_type,
                        k = k,
                    ));
                }
            }
            ("MIN", DeltaOp::Subtract) | ("MAX", DeltaOp::Subtract) => {
                if ic.topk_k.is_some() {
                    // Top-K retraction: subtract retracted values from the heap
                    // via the multiset helper, ONCE per row. The scalar
                    // `__min_x` / `__max_x` is set NULL here; a post-MERGE
                    // UPDATE emitted by `build_topk_scalar_refresh_sql` reads
                    // `__min_x = __min_x_topk[1]` for groups whose heap
                    // survived. Calling the helper twice in a single SET
                    // clause doubled the per-row cost.
                    let topk = ic.topk_column_name();
                    set_clauses.push(format!(
                        "\"{topk}\" = public.__reflex_array_subtract_multiset(t.\"{topk}\", d.\"{topk}\")",
                        topk = topk,
                    ));
                    set_clauses.push(format!("\"{}\" = NULL", ic.name));
                } else {
                    set_clauses.push(format!("\"{}\" = NULL", ic.name));
                }
            }
            _ => {
                // COALESCE handles NULL in delta (e.g., SUM(NULL)=NULL but we
                // need 0). Intermediate columns are always BIGINT or NUMERIC
                // (SUM / COUNT / __nonnull_count / topk-array elements) — the
                // builder never emits BOOLEAN intermediates. Use "0" directly.
                set_clauses.push(format!(
                    "\"{}\" = COALESCE(t.\"{}\", 0) {} COALESCE(d.\"{}\", 0)",
                    ic.name, ic.name, operator, ic.name
                ));
            }
        }
    }
    if plan.needs_ivm_count {
        set_clauses.push(format!(
            "__ivm_count = COALESCE(t.__ivm_count, 0) {} COALESCE(d.__ivm_count, 0)",
            operator
        ));
    }

    // WHEN NOT MATCHED THEN INSERT: all columns with values from d
    let mut insert_cols: Vec<String> = join_cols.clone();
    for ic in &plan.intermediate_columns {
        insert_cols.push(format!("\"{}\"", ic.name));
        if ic.has_topk() {
            insert_cols.push(format!("\"{}\"", ic.topk_column_name()));
        }
    }
    if plan.needs_ivm_count {
        insert_cols.push("__ivm_count".to_string());
    }

    // Determine default values for INSERT COALESCE based on column types.
    // MIN/MAX columns and top-K array columns should NOT be coalesced —
    // NULL/empty is valid (means "no value"). Only SUM/COUNT need COALESCE
    // to 0 (NULL + 0 = 0, not NULL).
    let insert_vals: Vec<String> = insert_cols
        .iter()
        .map(|c| {
            if c.starts_with("\"__") || c == "__ivm_count" {
                // Check if this is a MIN/MAX column or a top-K array column — don't coalesce
                let is_min_max_or_topk = plan.intermediate_columns.iter().any(|ic| {
                    let is_main = format!("\"{}\"", ic.name) == *c
                        && (ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX");
                    let is_topk = ic.has_topk() && format!("\"{}\"", ic.topk_column_name()) == *c;
                    is_main || is_topk
                });
                if is_min_max_or_topk {
                    format!("d.{}", c) // No COALESCE for MIN/MAX or top-K array
                } else {
                    let is_bool = plan
                        .intermediate_columns
                        .iter()
                        .any(|ic| format!("\"{}\"", ic.name) == *c && ic.pg_type == "BOOLEAN");
                    let default_val = if is_bool { "FALSE" } else { "0" };
                    format!("COALESCE(d.{}, {})", c, default_val)
                }
            } else {
                format!("d.{}", c)
            }
        })
        .collect();

    // For Subtract: omit WHEN NOT MATCHED (can't subtract from non-existent group)
    let not_matched = match op {
        DeltaOp::Add => format!(
            " WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
            insert_cols.join(", "),
            insert_vals.join(", ")
        ),
        DeltaOp::Subtract => String::new(),
    };

    format!(
        "MERGE INTO {} AS t USING {} AS d ON {} WHEN MATCHED THEN UPDATE SET {}{}",
        intermediate_tbl,
        using_clause,
        on_clause,
        set_clauses.join(", "),
        not_matched
    )
}

/// Build a net-delta query for UPDATE: combines old (negated) and new transition tables
/// into a single aggregated delta. Halves the MERGE count for SUM/COUNT aggregates.
///
/// Produces: SELECT group_cols, SUM(CASE WHEN __op='N' THEN val ELSE -val END) AS __sum_val, ...
///           FROM (SELECT 'N', * FROM new_tbl UNION ALL SELECT 'O', * FROM old_tbl) GROUP BY ...
fn build_net_delta_query(delta_old: &str, delta_new: &str, plan: &AggregationPlan) -> String {
    // Extract the GROUP BY columns and aggregate expressions from the base query pattern.
    // The delta queries look like: SELECT group_col, SUM(amount) AS __sum_amount, COUNT(*) AS __ivm_count FROM transition_table GROUP BY group_col
    // We need to rewrite them into a net-delta form.
    //
    // Approach: UNION ALL the new (positive) and old (negated) delta queries, then re-aggregate.
    // The outer SELECT uses the same GROUP BY and sums the results — since old values are
    // negated in the subtract query, the net effect is (new - old) per group.
    //
    // For SUM: SUM(val_from_new) + SUM(-val_from_old) = net delta
    // For COUNT: COUNT(new) - COUNT(old) = net ivm_count delta
    //
    // We achieve this by treating the Add delta as positive and using the Subtract delta
    // which already produces negative aggregates via the MERGE subtract path.
    // But actually, both delta queries produce POSITIVE aggregates — the negation
    // happens in the MERGE SET clause (t.col - d.col for subtract).
    //
    // Simplest correct approach: just wrap both in a UNION ALL and re-aggregate.
    // The new delta contributes positively, the old delta contributes negatively.

    // Build group column list
    let mut grp_cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| format!("\"{}\"", normalized_column_name(c)))
        .collect();

    // For aggregates without GROUP BY: use sentinel column
    let needs_sentinel = grp_cols.is_empty() && !plan.intermediate_columns.is_empty();
    if needs_sentinel {
        grp_cols.push("__reflex_group".to_string());
    }

    let grp_select = if grp_cols.is_empty() {
        String::new()
    } else {
        format!("{}, ", grp_cols.join(", "))
    };

    let grp_by = if grp_cols.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", grp_cols.join(", "))
    };

    // Build aggregate expressions: for each intermediate column, compute net delta
    let mut agg_exprs: Vec<String> = Vec::new();
    for ic in &plan.intermediate_columns {
        // SUM/COUNT: net = positive from new + negative from old. COALESCE for NULL safety.
        agg_exprs.push(format!(
            "SUM(CASE WHEN __reflex_sign = 1 THEN COALESCE(\"{}\", 0) ELSE -COALESCE(\"{}\", 0) END) AS \"{}\"",
            ic.name, ic.name, ic.name
        ));
    }
    if plan.needs_ivm_count {
        agg_exprs.push(
            "SUM(CASE WHEN __reflex_sign = 1 THEN COALESCE(__ivm_count, 0) ELSE -COALESCE(__ivm_count, 0) END) AS __ivm_count".to_string()
        );
    }

    let agg_select = agg_exprs.join(", ");

    // The inner UNION ALL: new delta (sign=+1) UNION ALL old delta (sign=-1)
    let sentinel_col = if needs_sentinel {
        ", 0 AS __reflex_group"
    } else {
        ""
    };
    format!(
        "SELECT {grp_select}{agg_select} FROM (\
            SELECT 1 AS __reflex_sign, __d.*{sentinel_col} FROM ({delta_new}) AS __d \
            UNION ALL \
            SELECT -1 AS __reflex_sign, __d.*{sentinel_col} FROM ({delta_old}) AS __d\
         ) AS __net{grp_by}"
    )
}

/// Build a SQL UPDATE that recomputes MIN/MAX columns from the original
/// (un-delta-substituted) base_query for groups whose value was set to NULL
/// by a subtract operation. Returns None if the plan has no MIN/MAX columns.
///
/// The recompute source is `orig_base_query` as a subquery — this preserves any
/// JOINs and aliases referenced by the aggregated expression. A scalar subquery
/// `SELECT AGG(expr) FROM source_table WHERE …` would fail for such expressions
/// because `source_table` alone doesn't expose the JOINs.
///
/// When `affected_tbl` is `Some(name)` and the plan has group columns, the
/// `orig_base_query` is wrapped in a filter that restricts its output to groups
/// present in the affected-groups table. Without this filter, every MIN/MAX
/// retraction re-aggregates the full source — the cliff that makes stock_chart
/// IMVs unusable in practice. The wrapper is `SELECT * FROM (<orig>) AS __all
/// WHERE (<gb_cols>) IN (SELECT DISTINCT <gb_cols> FROM "<affected_tbl>")`, which
/// pushes the group-key filter down through the aggregation boundary.
/// Build a UPDATE that refreshes the scalar `__min_x` / `__max_x` from the
/// companion `__min_x_topk[1]` for groups whose heap is non-empty after a
/// top-K subtract. Returns `None` when the plan has no top-K MIN/MAX columns.
///
/// The MERGE codegen sets `__min_x = NULL` on subtract; this UPDATE reads the
/// surviving heap top into the scalar. Groups whose heap underflowed (now
/// NULL/empty) keep `__min_x = NULL` — they're picked up by
/// `build_min_max_recompute_sql` which scans the source for them.
pub fn build_topk_scalar_refresh_sql(
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    affected_tbl: Option<&str>,
) -> Option<String> {
    let topk_cols: Vec<&crate::aggregation::IntermediateColumn> = plan
        .intermediate_columns
        .iter()
        .filter(|ic| ic.has_topk())
        .collect();

    if topk_cols.is_empty() {
        return None;
    }

    let group_cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| normalized_column_name(c))
        .collect();

    let mut set_parts: Vec<String> = Vec::new();
    for ic in &topk_cols {
        set_parts.push(format!(
            "\"{name}\" = \"{topk}\"[1]",
            name = ic.name,
            topk = ic.topk_column_name(),
        ));
    }

    // Predicate: heap is non-empty for at least one of the topk columns.
    let heap_predicates: Vec<String> = topk_cols
        .iter()
        .map(|ic| {
            let topk = ic.topk_column_name();
            format!(
                "\"{topk}\" IS NOT NULL AND cardinality(\"{topk}\") > 0",
                topk = topk
            )
        })
        .collect();
    let heap_pred = heap_predicates.join(" OR ");

    // Scope to affected groups when possible.
    // `at` is a fully-formed identifier ref (qualified+quoted or bare local).
    let scope_filter = match (affected_tbl, !group_cols.is_empty()) {
        (Some(at), true) => {
            let cols_csv = group_cols
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                " AND ({cols}) IN (SELECT {cols} FROM {at})",
                cols = cols_csv,
                at = at,
            )
        }
        _ => String::new(),
    };

    Some(format!(
        "UPDATE {tbl} SET {sets} WHERE ({heap}){scope}",
        tbl = intermediate_tbl,
        sets = set_parts.join(", "),
        heap = heap_pred,
        scope = scope_filter,
    ))
}

pub fn build_min_max_recompute_sql(
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    orig_base_query: &str,
    affected_tbl: Option<&str>,
) -> Option<String> {
    build_min_max_recompute_sql_inner(intermediate_tbl, plan, orig_base_query, affected_tbl, false)
}

/// N1 — Capture the subset of affected groups whose top-K heap shrank below
/// K during the preceding Sub merge. Only those groups need a source-scan
/// recompute: groups whose heap stayed at K had no heap-eligible row removed,
/// so the algebraic Sub+Add merge alone is correct.
///
/// Emits two statements: TRUNCATE the per-IMV shrunk table, then INSERT
/// DISTINCT group keys from `intermediate ⨝ affected` filtered by
/// `OR-of-(topk_col IS NULL OR cardinality(topk_col) < K)` across every
/// top-K column in the plan. Designed to run between the Sub MERGE and
/// `build_topk_scalar_refresh_sql` so the cardinality reflects post-Sub
/// state.
///
/// Returns `false` (and pushes nothing) when the plan has no top-K columns
/// or no group columns — in those cases the caller should not invoke the
/// gated recompute path.
pub fn push_topk_shrunk_groups_capture(
    stmts: &mut Vec<String>,
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    affected_tbl: &str,
    shrunk_tbl: &str,
) -> bool {
    let topk_cols: Vec<&crate::aggregation::IntermediateColumn> = plan
        .intermediate_columns
        .iter()
        .filter(|ic| ic.has_topk())
        .collect();
    if topk_cols.is_empty() {
        return false;
    }

    let group_cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| normalized_column_name(c))
        .collect();
    if group_cols.is_empty() {
        return false;
    }

    let proj = group_cols
        .iter()
        .map(|c| format!("i.\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");
    let join_cond = group_cols
        .iter()
        .map(|gc| format!("i.\"{gc}\" IS NOT DISTINCT FROM a.\"{gc}\"", gc = gc))
        .collect::<Vec<_>>()
        .join(" AND ");

    let predicates: Vec<String> = topk_cols
        .iter()
        .map(|ic| {
            let topk = ic.topk_column_name();
            let k = ic.topk_k.expect("has_topk() => topk_k is Some");
            format!(
                "(i.\"{topk}\" IS NULL OR cardinality(i.\"{topk}\") < {k})",
                topk = topk,
                k = k,
            )
        })
        .collect();
    let where_clause = predicates.join(" OR ");

    stmts.push(format!("TRUNCATE {}", shrunk_tbl));
    stmts.push(format!(
        "INSERT INTO {shrunk_tbl} SELECT DISTINCT {proj} \
         FROM {intermediate_tbl} i JOIN {affected_tbl} a ON {join_cond} \
         WHERE {where_clause}",
        shrunk_tbl = shrunk_tbl,
        proj = proj,
        intermediate_tbl = intermediate_tbl,
        affected_tbl = affected_tbl,
        join_cond = join_cond,
        where_clause = where_clause,
    ));
    true
}

/// UPDATE-flavoured variant: when any MIN/MAX column has top-K enabled, the
/// algebraic Sub+Add merge can leave the heap with K elements but the *wrong*
/// K — for groups whose source has unchanged rows that should be in heap and
/// aren't (because the heap pre-update never held them and the Add step only
/// merges the delta_new top-K into what survived Sub). Force a recompute for
/// every affected top-K column so heap+scalar reflect the post-UPDATE source
/// truthfully. Non-top-K MIN/MAX columns keep the legacy `scalar IS NULL`
/// gate. INSERT/DELETE flows are unaffected.
pub fn build_min_max_recompute_sql_force_topk(
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    orig_base_query: &str,
    affected_tbl: Option<&str>,
) -> Option<String> {
    build_min_max_recompute_sql_inner(intermediate_tbl, plan, orig_base_query, affected_tbl, true)
}

fn build_min_max_recompute_sql_inner(
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    orig_base_query: &str,
    affected_tbl: Option<&str>,
    force_topk: bool,
) -> Option<String> {
    let min_max_cols: Vec<&crate::aggregation::IntermediateColumn> = plan
        .intermediate_columns
        .iter()
        .filter(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX")
        .collect();

    if min_max_cols.is_empty() {
        return None;
    }

    let group_cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| normalized_column_name(c))
        .collect();

    // For top-K-enabled MIN/MAX columns, also rebuild the companion array.
    // The `orig_base_query` already projects `__min_x_topk` because
    // `generate_base_query` emits the slice when `topk_k.is_some()`.
    let mut set_parts: Vec<String> = Vec::new();
    for ic in &min_max_cols {
        set_parts.push(format!("\"{}\" = __src.\"{}\"", ic.name, ic.name));
        if ic.has_topk() {
            let topk = ic.topk_column_name();
            set_parts.push(format!("\"{}\" = __src.\"{}\"", topk, topk));
        }
    }

    // Trigger recompute when the scalar slot is NULL (legacy path) OR — for
    // top-K columns — when the companion array is empty/NULL (heap underflow
    // after a multi-set subtract). When `force_topk` is set (UPDATE flow on
    // top-K IMVs) the heap-staleness check becomes an unconditional `TRUE`
    // for top-K columns so every affected group gets re-derived from source —
    // the algebraic merge can leave heap with K elements but the wrong K.
    let mut null_check: Vec<String> = Vec::new();
    for ic in &min_max_cols {
        if force_topk && ic.has_topk() {
            null_check.push("TRUE".to_string());
            continue;
        }
        null_check.push(format!("{}.\"{}\" IS NULL", intermediate_tbl, ic.name));
        if ic.has_topk() {
            let topk = ic.topk_column_name();
            null_check.push(format!(
                "({tbl}.\"{topk}\" IS NULL OR cardinality({tbl}.\"{topk}\") = 0)",
                tbl = intermediate_tbl,
                topk = topk,
            ));
        }
    }

    // Sentinel-only (no GROUP BY) case: single row, no join keys to match.
    // The WHERE reduces to the NULL filter only. Affected-groups filter is
    // meaningless without group columns — skip the wrap.
    if group_cols.is_empty() {
        return Some(format!(
            "UPDATE {} SET {} FROM ({}) AS __src WHERE {}",
            intermediate_tbl,
            set_parts.join(", "),
            orig_base_query,
            null_check.join(" OR ")
        ));
    }

    // Scope the recompute to affected groups when a table name is available.
    // The filter must be injected BEFORE the GROUP BY so it scopes the scan
    // of the source, not the output of the aggregation. Wrapping the query
    // in `SELECT * FROM (<orig>) WHERE grp IN (...)` post-aggregation was
    // insufficient: Postgres' planner does not reliably push a filter through
    // GROUP BY, leaving the full source scan intact (verified with EXPLAIN).
    //
    // The injected WHERE references the raw GROUP BY column expressions on
    // the LHS (so it applies to pre-aggregation rows) and the normalized
    // column names in the affected-groups table on the RHS. Postgres matches
    // by value, not by alias, so a raw/normalized pair works.
    let scoped_source = match affected_tbl {
        Some(at) => {
            let raw_csv = plan.group_by_columns.join(", ");
            let norm_csv: Vec<String> = group_cols.iter().map(|c| format!("\"{}\"", c)).collect();
            let norm_csv = norm_csv.join(", ");
            let filter = format!(
                " AND ({}) IN (SELECT DISTINCT {} FROM {})",
                raw_csv, norm_csv, at
            );
            match splice_before_group_by(orig_base_query, &filter) {
                Some(spliced) => spliced,
                None => orig_base_query.to_string(),
            }
        }
        None => orig_base_query.to_string(),
    };

    let join_cond: Vec<String> = group_cols
        .iter()
        .map(|gc| {
            format!(
                "{}.\"{}\" IS NOT DISTINCT FROM __src.\"{}\"",
                intermediate_tbl, gc, gc
            )
        })
        .collect();

    let update_sql = format!(
        "UPDATE {} SET {} FROM ({}) AS __src WHERE {} AND ({})",
        intermediate_tbl,
        set_parts.join(", "),
        scoped_source,
        join_cond.join(" AND "),
        null_check.join(" OR ")
    );

    // 1.3.0: gate the recompute on `EXISTS (intermediate row with NULL slot
    // in an affected group)`. The post-MERGE topk-scalar refresh sets the
    // scalar from `topk[1]` for groups whose heap survived; the recompute
    // only needs to fire for groups that genuinely underflowed. An always-
    // executing UPDATE used to trigger the source aggregation even when no
    // group needed it, which dominated the bench.
    if let Some(at) = affected_tbl {
        let aff_join_cond: Vec<String> = group_cols
            .iter()
            .map(|gc| {
                format!(
                    "{}.\"{}\" IS NOT DISTINCT FROM __aff.\"{}\"",
                    intermediate_tbl, gc, gc
                )
            })
            .collect();
        let exists_check = format!(
            "EXISTS (SELECT 1 FROM {tbl} JOIN {at} __aff ON {join} WHERE {nullc})",
            tbl = intermediate_tbl,
            at = at,
            join = aff_join_cond.join(" AND "),
            nullc = null_check.join(" OR "),
        );
        return Some(format!(
            "DO $_reflex_recompute$ BEGIN IF {check} THEN {upd}; END IF; END $_reflex_recompute$",
            check = exists_check,
            upd = update_sql,
        ));
    }

    Some(update_sql)
}

/// Build a match condition for affected groups, used as the WHERE filter on
/// the target/intermediate during the end_query refresh phase.
///
/// 1.4.4: per-column `=` for NOT NULL group cols, `IS NOT DISTINCT FROM` for
/// NULLable ones. Same motivation as `build_merge_using`'s rewrite — `=` is
/// btree-index-usable, `IS NOT DISTINCT FROM` is not. The composite index on
/// the target/intermediate group cols becomes a usable EXISTS-probe target
/// for the NOT NULL prefix, so the DELETE/INSERT during target refresh
/// avoids the seq-scan-with-IS-NOT-DISTINCT-FROM fallback that, on a 900K
/// intermediate and 47K affected rows, costs several seconds per UPDATE.
///
/// `outer_qualifier` is the unambiguous reference to the outer table or
/// subquery the EXISTS predicate is being attached to (e.g.
/// `"rb"."fcast"` for a DELETE-from-target, or `"rb"."__reflex_intermediate_v"`
/// for an INSERT-into-target whose SELECT reads from the intermediate, or
/// `"__full"` for a base_query-derived subquery alias). The outer column
/// reference is emitted as `<outer_qualifier>.<outer_col>` so the bind is to
/// the outer scope, NOT the inner `__a.<col>` scope. Without this qualifier
/// Postgres's name-resolution picks the inner scope whenever the outer and
/// `__a` share column names (always true in practice — the affected table is
/// populated from intermediate-named cols), and the predicate degenerates to
/// `__a.<col> = __a.<col>` (a one-time TRUE filter that wipes the whole
/// outer relation on DELETE / re-inserts the whole intermediate on INSERT).
/// See `journal/2026-05-13_null_safe_in_bug.md`.
///
/// `outer_cols` and `affected_cols` are parallel quoted column lists. Their
/// names may differ when the IMV's SELECT aliases a GROUP BY column (e.g.,
/// `SELECT dp.id AS dem_plan_id ... GROUP BY dp.id` ⇒ target has
/// `dem_plan_id`, intermediate and affected keep `id`).
///
/// `not_null_columns` is keyed on the affected column's bare name; NOT NULL
/// cols use `=` (sargable / index-usable), NULLable cols use
/// `IS NOT DISTINCT FROM` (NULL-safe but not sargable).
fn null_safe_in(
    affected_tbl: &str,
    outer_qualifier: &str,
    outer_cols: &[String],
    affected_cols: &[String],
    not_null_columns: &std::collections::HashSet<String>,
) -> String {
    debug_assert_eq!(
        outer_cols.len(),
        affected_cols.len(),
        "null_safe_in: outer_cols and affected_cols must have the same length"
    );
    let conditions: Vec<String> = outer_cols
        .iter()
        .zip(affected_cols.iter())
        .map(|(outer_col, aff_col)| {
            let bare = aff_col.trim_matches('"');
            let op = if not_null_columns.contains(bare) {
                "="
            } else {
                "IS NOT DISTINCT FROM"
            };
            format!("{}.{} {} __a.{}", outer_qualifier, outer_col, op, aff_col)
        })
        .collect();
    // `affected_tbl` is already a fully-formed identifier ref (qualified+quoted
    // when the IMV has a schema, bare local otherwise) — see
    // `affected_groups_table_name`.
    format!(
        "EXISTS (SELECT 1 FROM {} AS __a WHERE {})",
        affected_tbl,
        conditions.join(" AND ")
    )
}

/// Splice a SQL fragment (already formatted as ` AND (...)` or similar) into a
/// query immediately before its `GROUP BY` clause. If the query has no
/// existing `WHERE` clause between `FROM` and `GROUP BY`, the leading `AND`
/// is rewritten to `WHERE`. Returns `None` if no `GROUP BY` is found.
///
/// Used by `build_min_max_recompute_sql` to push an affected-groups filter
/// through the base-query aggregation boundary so the source scan is scoped.
fn splice_before_group_by(query: &str, and_fragment: &str) -> Option<String> {
    let upper = query.to_uppercase();
    let gb_marker = " GROUP BY ";
    let gb_pos = upper.rfind(gb_marker)?;

    // Determine whether a WHERE exists between the last FROM/JOIN and GROUP BY.
    let pre_gb_upper = &upper[..gb_pos];
    let has_where = pre_gb_upper.contains(" WHERE ");

    let fragment = if has_where {
        and_fragment.to_string()
    } else {
        // Rewrite leading " AND" to " WHERE"
        let trimmed = and_fragment.trim_start();
        if let Some(rest) = trimmed.strip_prefix("AND ") {
            format!(" WHERE {}", rest)
        } else {
            // Fallback: just prepend WHERE
            format!(" WHERE {}", trimmed)
        }
    };

    let mut out = String::with_capacity(query.len() + fragment.len());
    out.push_str(&query[..gb_pos]);
    out.push_str(&fragment);
    out.push_str(&query[gb_pos..]);
    Some(out)
}

/// Splice an affected-groups filter into `end_query` immediately before its `GROUP BY` clause.
///
/// `output_gb_cols` must be pre-quoted column names matching `plan.group_by_columns`.
/// `outer_qualifier` is the table/subquery the EXISTS predicate references in
/// `end_query`'s scope — typically the intermediate table identifier, since
/// `end_query`'s FROM is the intermediate. See `null_safe_in` doc for why
/// qualifying the outer is mandatory.
/// Returns `None` if `end_query` contains no ` GROUP BY ` marker (defensive fallback).
fn inject_affected_filter_before_group_by(
    end_query: &str,
    output_gb_cols: &[String],
    affected_tbl: &str,
    outer_qualifier: &str,
    not_null_columns: &std::collections::HashSet<String>,
) -> Option<String> {
    let upper = end_query.to_uppercase();
    let gb_marker = " GROUP BY ";
    let pos = upper.rfind(gb_marker)?;
    let filter = null_safe_in(
        affected_tbl,
        outer_qualifier,
        output_gb_cols,
        output_gb_cols,
        not_null_columns,
    );
    Some(format!(
        "{} AND {}{}",
        &end_query[..pos],
        filter,
        &end_query[pos..]
    ))
}

/// Build the group column list for targeted refresh.
/// Returns quoted column names from group_by + distinct columns (bare names).
/// These are the *intermediate* (and affected) column names — the affected
/// table is populated from intermediate naming. Returns None if there are no
/// group columns (sentinel-only case).
fn group_columns(plan: &AggregationPlan) -> Option<Vec<String>> {
    let cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| format!("\"{}\"", normalized_column_name(c)))
        .collect();
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}

/// Build the parallel column list as the columns appear in the *target*
/// table. When the user aliases a GROUP BY column (`SELECT dp.id AS
/// dem_plan_id`), the target table is created with the alias (`dem_plan_id`)
/// while the intermediate / affected tables keep the source-bare name (`id`).
/// The returned list is positionally aligned with `group_columns(plan)`
/// followed by `distinct_columns` — i.e. `target_group_columns(plan)[i]` is
/// the target-side name of the same column whose intermediate-side name is
/// `group_columns(plan).unwrap()[i]`.
fn target_group_columns(plan: &AggregationPlan) -> Vec<String> {
    // Mirrors `group_columns` (group_by_columns then distinct_columns) but
    // applies `group_by_aliases` to the GROUP BY side so the returned names
    // match what's actually stored in the target table. Used when the user
    // aliases a GROUP BY column (`SELECT col AS alias`); for the
    // SELECT-DISTINCT path the distinct columns are projected to the target
    // unaliased (caller is responsible for restricting to distinct_columns
    // by length when the target doesn't carry them — see end_query_has_group_by
    // branch, where target = GROUP BY only).
    plan.group_by_columns
        .iter()
        .map(|gb| {
            let output = plan
                .group_by_aliases
                .get(gb)
                .map(String::as_str)
                .unwrap_or(gb);
            format!("\"{}\"", normalized_column_name(output))
        })
        .chain(
            plan.distinct_columns
                .iter()
                .map(|c| format!("\"{}\"", normalized_column_name(c))),
        )
        .collect()
}

/// Build SELECT DISTINCT clause for affected group columns.
fn affected_groups_select(cols: &[String]) -> String {
    cols.join(", ")
}

/// Build a row-value expression for WHERE ... IN clauses.
/// Single column: "col"   Multi-column: ("col1", "col2")
fn row_expr(cols: &[String]) -> String {
    if cols.len() == 1 {
        cols[0].clone()
    } else {
        format!("({})", cols.join(", "))
    }
}

// Phase 1 (plans/1_6_1_refacto.md) — `replace_source_with_transition` is now
// a re-export of the canonical implementation in
// [`crate::sql_writer::identifier::replace_source_with_transition`]. Existing
// call sites in this file (and `unit_trigger.rs` tests) compile unchanged.
use crate::sql_writer::identifier::replace_source_with_transition;

/// Push MERGE + affected-groups population.
/// PG17+: single CTE with MERGE RETURNING (captures affected groups in one statement).
///   When `include_cleanup` is true, prepends a DELETE FROM affected CTE (replaces TRUNCATE).
/// PG15/16: separate MERGE + SELECT DISTINCT from delta query (MERGE RETURNING unsupported).
fn push_materialized_merge(
    stmts: &mut Vec<String>,
    scratch_tbl: &str,
    delta_query: &str,
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) {
    stmts.push(format!("TRUNCATE {}", scratch_tbl));
    stmts.push(format!("INSERT INTO {} {}", scratch_tbl, delta_query));
    stmts.push(build_merge_from_table_sql(
        intermediate_tbl,
        scratch_tbl,
        plan,
        op,
    ));
}

/// 2026-05-15 — high-selectivity dispatch threshold. When the number of
/// affected groups exceeds this fraction of the intermediate's row count, the
/// trigger switches from MERGE-based incremental to TRUNCATE+rebuild (full
/// IMV-body refresh via `reflex_reconcile`).
///
/// History: 1.4.5 introduced this at 0.3 because reconcile (~14 s on
/// SOP-forecast shape at the time) was faster than the broken incremental
/// path (~17 s at 100K-affected dead-cleanup pathology, exposed in 2026-05-15).
///
/// 1.4.6 (Item α + ANALYZE fix) makes incremental fast at ALL reachable
/// selectivities on the SOP-forecast shape (1 M source × 50 dem_plan, ~200 K
/// intermediate, 20 K rows per plan = 10 % per flip):
///
/// | Selectivity | Incremental | reconcile (1.4.5) |
/// |---:|---:|---:|
/// | 11 % (20K) | ~620 ms  | ~17 s |
/// | 33 % (60K) | ~1.3 s   | ~17 s |
/// | 50 % (100K)| ~1.9 s   | ~17 s |
/// | 78 % (140K)| ~2.9 s   | ~17 s |
///
/// On that synthetic shape reconcile never wins. **But real db_clone shapes
/// invert the proportions** (76 M source × 28 dem_plan, 7.7 M intermediate,
/// 0.9–8.9 M rows per plan = 12–115 % per flip). On those shapes the
/// per-row MERGE + target double-rewrite hits O(|affected| · log
/// |intermediate|) and loses 2–8× to reconcile at high selectivity.
///
/// 1.4.6 lowers the default to 0.50, a compromise that:
///   * Keeps small-mutation workloads (< 50 % of intermediate) on the
///     incremental path — preserves the 60×-faster pure-data UPDATE case.
///   * Dispatches catastrophic bulk flips (>= 50 % of intermediate) to
///     reconcile, which on db_clone alp is ~2× faster than the bulk
///     incremental path.
///
/// Per-IMV override via the `wipe_threshold` column in
/// `__reflex_ivm_reference` (write via `reflex_set_wipe_threshold(name,
/// value)`) is the right tool when this global default doesn't fit a
/// specific IMV's shape — e.g. yse-style 419 K intermediate where the
/// crossover is closer to 0.15.
const WIPE_THRESHOLD_DEFAULT: f64 = 0.5;

/// 1.4.5 — emit a DO block that dispatches between MERGE-incremental and
/// TRUNCATE-rebuild based on runtime selectivity.
///
/// Replaces these statements in the standard flow:
///   1. MERGE intermediate USING scratch
///   2. DELETE intermediate WHERE __ivm_count<=0 AND in_affected (optional)
///   3. DELETE target WHERE in_affected
///   4. INSERT target SELECT end_query WHERE in_affected
///
/// At high selectivity, instead:
///   1. TRUNCATE intermediate
///   2. INSERT INTO intermediate <base_query> -- full re-aggregation
///   3. TRUNCATE target
///   4. INSERT INTO target <end_query>
///
/// Scratch and affected are populated BEFORE this block (steps 1-3 of the
/// standard flow remain unchanged). The DO block reads the affected table's
/// row count and compares to pg_class.reltuples on the intermediate.
///
/// Threshold: `current_setting('reflex.wipe_threshold', true)::numeric` if
/// set, else `WIPE_THRESHOLD_DEFAULT`. Operators can `SET LOCAL` per-session
/// or per-statement.
#[allow(clippy::too_many_arguments)]
fn build_high_selectivity_dispatch_sql(
    view_name: &str,
    intermediate_tbl: &str,
    affected_tbl: &str,
    merge_sql: &str,
    dead_cleanup_sql: Option<&str>,
    target_delete_sql: &str,
    target_insert_sql: &str,
) -> String {
    let dead_cleanup = match dead_cleanup_sql {
        Some(s) => format!(
            "        EXECUTE $reflex_inner${cleanup}$reflex_inner$;\n",
            cleanup = s.replace("$reflex_inner$", "$reflex_inner_alt$")
        ),
        None => String::new(),
    };
    let safe_merge = merge_sql.replace("$reflex_inner$", "$reflex_inner_alt$");
    let safe_tdel = target_delete_sql.replace("$reflex_inner$", "$reflex_inner_alt$");
    let safe_tins = target_insert_sql.replace("$reflex_inner$", "$reflex_inner_alt$");
    let safe_view = view_name.replace('\'', "''");

    format!(
        "DO $reflex_dispatch$\n\
         DECLARE\n\
             _aff BIGINT;\n\
             _imm NUMERIC;\n\
             _thr NUMERIC;\n\
             _per_imv NUMERIC;\n\
             _ratio NUMERIC;\n\
         BEGIN\n\
             SELECT count(*) INTO _aff FROM {affected};\n\
             SELECT GREATEST(reltuples::NUMERIC, 1.0) INTO _imm\n\
                 FROM pg_class WHERE oid = '{intermediate}'::regclass;\n\
             -- 1.4.6 precedence: per-IMV wipe_threshold column (operator\n\
             -- override) → session GUC reflex.wipe_threshold → compiled\n\
             -- default. Per-IMV is the right granularity when one extension\n\
             -- instance serves IMVs with shape-divergent crossovers.\n\
             SELECT wipe_threshold INTO _per_imv\n\
                 FROM public.__reflex_ivm_reference WHERE name = '{view}';\n\
             _thr := COALESCE(_per_imv, current_setting('reflex.wipe_threshold', true)::NUMERIC, {default_thr});\n\
             _ratio := _aff::NUMERIC / _imm;\n\
             IF _ratio >= _thr THEN\n\
                 -- High-selectivity path — delegate to reflex_reconcile,\n\
                 -- which implements the drop-index/bulk-INSERT/recreate-\n\
                 -- index pattern. At >= threshold selectivity the cost of\n\
                 -- the standard MERGE + target double-rewrite exceeds the\n\
                 -- cost of a full IMV rebuild.\n\
                 RAISE DEBUG 'pg_reflex wipe: ratio=% thr=% — reconcile', _ratio, _thr;\n\
                 PERFORM public.reflex_reconcile('{view}');\n\
             ELSE\n\
                 RAISE DEBUG 'pg_reflex wipe: ratio=% thr=% — incremental', _ratio, _thr;\n\
                 EXECUTE $reflex_inner${merge}$reflex_inner$;\n\
                 -- ANALYZE intermediate after MERGE so the planner has\n\
                 -- accurate stats for downstream dead-cleanup, target_delete,\n\
                 -- and target_insert. The MERGE modified ~|scratch| rows in\n\
                 -- the intermediate (UPDATE/INSERT), and the algebraic count\n\
                 -- column (__ivm_count) just shifted distribution; without\n\
                 -- fresh stats the planner can pick pathological NestedLoop+\n\
                 -- SeqScan plans (12+ min on 100K groups). ~150 ms cost on\n\
                 -- the SOP-forecast shape.\n\
                 EXECUTE 'ANALYZE {intermediate}';\n\
{dead_cleanup}\
                 EXECUTE $reflex_inner${tdel}$reflex_inner$;\n\
                 EXECUTE $reflex_inner${tins}$reflex_inner$;\n\
             END IF;\n\
         END\n\
         $reflex_dispatch$",
        affected = affected_tbl,
        intermediate = intermediate_tbl,
        view = safe_view,
        default_thr = WIPE_THRESHOLD_DEFAULT,
        merge = safe_merge,
        dead_cleanup = dead_cleanup,
        tdel = safe_tdel,
        tins = safe_tins,
    )
}

/// Compiled default for the per-partition denominator floor.  A partition
/// with `reltuples = 0` (brand-new or never-ANALYZE'd) would otherwise
/// trip the dispatch on any non-zero |dirty|; the floor caps that to a
/// meaningful "small partition" size.
const WIPE_FLOOR_ROWS_DEFAULT: i64 = 1000;

/// 1.5.3 (plans/partitioning_3.md §3) — partition-aware sibling of
/// `build_high_selectivity_dispatch_sql`.
///
/// Emitted only when the plan is partitioned AND the firing source is the
/// anchor (Tier 1).  The DO block:
///   1. GROUPs the populated `affected_tbl` by the partition column to get
///      per-partition dirty counts.
///   2. For each partition, looks up the actual child via the
///      `__reflex_partition_child_for_key` SQL helper and reads its
///      `reltuples` to compute a per-partition ratio
///      `dirty / GREATEST(reltuples, wipe_floor_rows)`.
///   3. Classifies partitions as hot / cold using the IMV's
///      `wipe_threshold` (per-IMV override → GUC → compiled default).
///   4. Trip-cap: if `hot_count > total_partitions / 2`, falls back to
///      `reflex_reconcile(view)` (global) since DETACHing many partitions
///      sequentially is worse than one rebuild.
///   5. Hot → `PERFORM reflex_reconcile_partition(view, csv_of_hot_keys)`
///      (uses the atomic swap from Phase A).
///   6. Cold → runs the standard MERGE / dead-cleanup / target DELETE /
///      target INSERT with a `<partition_col> <> ALL($1::TEXT[])` filter
///      added.  `$1` is `_hot_keys` passed via EXECUTE USING.
///
/// The cold-path SQL strings MUST already contain the filter splice — the
/// caller is responsible for wrapping the scratch / WHERE clauses with
/// the `$1::TEXT[]` parameter binding before passing in.
#[allow(clippy::too_many_arguments)]
fn build_partition_aware_dispatch_sql(
    view_name: &str,
    intermediate_tbl: &str,
    intermediate_parent_qual: &str,
    affected_tbl: &str,
    partition_col: &str,
    merge_sql_with_filter: &str,
    dead_cleanup_sql: Option<&str>,
    target_delete_sql_with_filter: &str,
    target_insert_sql_with_filter: &str,
) -> String {
    let dead_cleanup = match dead_cleanup_sql {
        Some(s) => format!(
            "                EXECUTE $reflex_inner${cleanup}$reflex_inner$ USING _hot_keys;\n",
            cleanup = s.replace("$reflex_inner$", "$reflex_inner_alt$")
        ),
        None => String::new(),
    };
    let safe_merge = merge_sql_with_filter.replace("$reflex_inner$", "$reflex_inner_alt$");
    let safe_tdel = target_delete_sql_with_filter.replace("$reflex_inner$", "$reflex_inner_alt$");
    let safe_tins = target_insert_sql_with_filter.replace("$reflex_inner$", "$reflex_inner_alt$");
    let safe_view = view_name.replace('\'', "''");
    let safe_part_col = partition_col.replace('"', "");
    let safe_part_col_lit = safe_part_col.replace('\'', "''");

    format!(
        "DO $reflex_dispatch$\n\
         DECLARE\n\
             _thr NUMERIC;\n\
             _per_imv NUMERIC;\n\
             _floor BIGINT;\n\
             _per_imv_floor BIGINT;\n\
             _hot_keys TEXT[] := ARRAY[]::TEXT[];\n\
             _hot_count INT;\n\
             _partition_total INT;\n\
         BEGIN\n\
             SELECT wipe_threshold, wipe_floor_rows INTO _per_imv, _per_imv_floor\n\
                 FROM public.__reflex_ivm_reference WHERE name = '{view}';\n\
             _thr   := COALESCE(_per_imv, current_setting('reflex.wipe_threshold', true)::NUMERIC, {default_thr});\n\
             _floor := COALESCE(_per_imv_floor, NULLIF(current_setting('reflex.wipe_floor_rows', true), '')::BIGINT, {default_floor});\n\
             -- Per-partition dirty counts from the already-populated\n\
             -- affected table (GROUP BY partition_col).\n\
             SELECT count(*) INTO _partition_total\n\
                 FROM pg_inherits WHERE inhparent = '{int_parent}'::regclass;\n\
             IF _partition_total IS NULL OR _partition_total = 0 THEN\n\
                 RAISE DEBUG 'pg_reflex partition dispatch: % has no children — fallback to global', '{view}';\n\
                 EXECUTE $reflex_inner${merge}$reflex_inner$ USING _hot_keys;\n\
                 EXECUTE 'ANALYZE {intermediate}';\n\
{dead_cleanup}\
                 EXECUTE $reflex_inner${tdel}$reflex_inner$ USING _hot_keys;\n\
                 EXECUTE $reflex_inner${tins}$reflex_inner$ USING _hot_keys;\n\
                 RETURN;\n\
             END IF;\n\
             SELECT COALESCE(array_agg(pp.pkey::text), ARRAY[]::TEXT[])\n\
                 INTO _hot_keys\n\
                 FROM (\n\
                     SELECT \"{part_col}\"::text AS pkey, count(*) AS dirty\n\
                     FROM {affected}\n\
                     GROUP BY \"{part_col}\"\n\
                 ) pp\n\
                 JOIN LATERAL (\n\
                     SELECT c.reltuples::NUMERIC AS rt\n\
                     FROM pg_class c\n\
                     WHERE c.oid = public.__reflex_partition_child_for_key(\n\
                                       '{int_parent}'::regclass, '{part_col_lit}', pp.pkey)\n\
                 ) c ON TRUE\n\
                 WHERE pp.dirty::NUMERIC\n\
                       / GREATEST(c.rt, _floor::NUMERIC)\n\
                       >= _thr;\n\
             _hot_count := COALESCE(array_length(_hot_keys, 1), 0);\n\
             RAISE DEBUG 'pg_reflex partition dispatch: hot=% total=% thr=% floor=%', _hot_count, _partition_total, _thr, _floor;\n\
             -- Trip-cap: sequential DETACH/ATTACH on > half of partitions\n\
             -- is worse than one full reconcile.\n\
             IF _hot_count > _partition_total / 2 THEN\n\
                 RAISE DEBUG 'pg_reflex partition dispatch: % hot of % partitions for %, fallback global', _hot_count, _partition_total, '{view}';\n\
                 PERFORM public.reflex_reconcile('{view}');\n\
                 RETURN;\n\
             END IF;\n\
             -- Hot partitions: atomic-swap reconcile.\n\
             IF _hot_count > 0 THEN\n\
                 RAISE DEBUG 'pg_reflex partition dispatch: % hot partitions for % → reflex_reconcile_partition', _hot_count, '{view}';\n\
                 PERFORM public.reflex_reconcile_partition('{view}', array_to_string(_hot_keys, ','));\n\
             END IF;\n\
             -- Cold partitions: standard MERGE + target sync, restricted\n\
             -- by partition filter ($1 bound to _hot_keys).\n\
             EXECUTE $reflex_inner${merge}$reflex_inner$ USING _hot_keys;\n\
             EXECUTE 'ANALYZE {intermediate}';\n\
{dead_cleanup}\
             EXECUTE $reflex_inner${tdel}$reflex_inner$ USING _hot_keys;\n\
             EXECUTE $reflex_inner${tins}$reflex_inner$ USING _hot_keys;\n\
         END\n\
         $reflex_dispatch$",
        view = safe_view,
        int_parent = intermediate_parent_qual.replace('"', "").replace('\'', "''"),
        intermediate = intermediate_tbl,
        affected = affected_tbl,
        part_col = safe_part_col,
        part_col_lit = safe_part_col_lit,
        default_thr = WIPE_THRESHOLD_DEFAULT,
        default_floor = WIPE_FLOOR_ROWS_DEFAULT,
        merge = safe_merge,
        dead_cleanup = dead_cleanup,
        tdel = safe_tdel,
        tins = safe_tins,
    )
}

#[allow(clippy::too_many_arguments)]
fn push_materialized_merge_and_affected(
    stmts: &mut Vec<String>,
    scratch_tbl: &str,
    delta_query: &str,
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
    affected_tbl: &str,
    select_expr: &str,
    include_cleanup: bool,
) {
    if include_cleanup {
        stmts.push(format!("TRUNCATE {}", affected_tbl));
    }
    stmts.push(format!("TRUNCATE {}", scratch_tbl));
    stmts.push(format!("INSERT INTO {} {}", scratch_tbl, delta_query));
    stmts.push(build_merge_from_table_sql(
        intermediate_tbl,
        scratch_tbl,
        plan,
        op,
    ));
    // 2026-05-15 (Item α surfaced this): the MERGE just modified
    // ~|scratch| rows in the intermediate. `pg_class.reltuples` is stale
    // (last set at IMV creation = 0). Downstream statements that JOIN on
    // the intermediate via the composite UNIQUE index (target sync's INSERT
    // and dead-cleanup DELETE) need accurate stats so the planner picks
    // Index Scan via the composite key instead of a pathological SeqScan
    // or NestedLoop. A full ANALYZE on a 180K-row intermediate is ~150 ms.
    stmts.push(format!("ANALYZE {}", intermediate_tbl));
    // Scratch is the result of GROUP BY in build_merge_from_table_sql's delta,
    // so it already contains one row per group key. DISTINCT here would add a
    // redundant hash/sort pass for the same output.
    stmts.push(format!(
        "INSERT INTO {} SELECT {} FROM {} AS __d",
        affected_tbl, select_expr, scratch_tbl
    ));
    // ANALYZE affected after INSERT — TRUNCATE clobbers reltuples; without
    // fresh stats the planner estimates 1 row and picks NL+SeqScan for the
    // downstream dead-cleanup DELETE and target sync EXISTS lookups
    // (pathological at high affected counts — 12+ minutes on 100K groups).
    // ~50 ms cost.
    stmts.push(format!("ANALYZE {}", affected_tbl));
}

// 1.4.6 — placeholder for a future revisit of dispatch on the INSERT and
// DELETE codegen paths (currently UPDATE-only). The blocker is that any
// dispatch decision needs |affected|, which requires scratch + affected
// fill, which on the bulk-flip shapes that motivate dispatch is the
// dominant cost being paid (50–100 s on alp 8.9 M rows). A future
// implementation must estimate |affected| from cheaper signals (pg_stats
// most_common_freqs on the JOIN key, or per-IMV calibrated cost) so the
// decision can fire before scratch is built. See
// journal/2026-05-15_dispatch_wiring_revert.md for the decision log.
//
// Signature sketch:
//     fn push_scratch_and_affected_for_dispatch(
//         stmts: &mut Vec<String>, scratch_tbl: &str, delta_query: &str,
//         intermediate_tbl: &str, plan: &AggregationPlan, op: DeltaOp,
//         affected_tbl: &str, select_expr: &str,
//     ) -> String { /* returns merge SQL; pushes scratch+affected fill */ }

/// 1.4.6 — bulk-DELETE path for Item α IN→OUT (and regular DELETE) on
/// sources that have passed the `plan.source_join_keys` safety gate.
///
/// Pre-conditions (enforced by the caller):
///   * Source has a mapping in `plan.source_join_keys` (every JOIN
///     equality maps to a GROUP BY col AND the source side covers a
///     UNIQUE key on the source). This guarantees the transition row's
///     identity uniquely identifies a slice of intermediate.
///   * end_query has no further GROUP BY — target rows are 1:1 with
///     intermediate after filtering `__ivm_count > 0`. Without this,
///     bulk DELETE on intermediate could leave target rows that should
///     be recomputed instead of removed.
///   * Source is not an outer-join secondary / not self-join (handled
///     earlier in the function).
///
/// Emits two indexed DELETEs (intermediate + target) that scan only the
/// transition rows against the leading column of the intermediate's
/// composite unique index. Bypasses the scratch fill — which is the
/// dominant cost (50-100 s) on bulk dim-flips like db_clone alp A4b.
fn push_bulk_delete_via_transition(
    stmts: &mut Vec<String>,
    intermediate_tbl: &str,
    target_qv: &str,
    transition_tbl: &str,
    mappings: &[(String, String)],
    plan: &AggregationPlan,
) {
    let int_cols: Vec<String> = mappings
        .iter()
        .map(|(ic, _)| format!("\"{}\"", ic))
        .collect();
    let src_cols: Vec<String> = mappings
        .iter()
        .map(|(_, sc)| format!("\"{}\"", sc))
        .collect();
    let src_select = src_cols.join(", ");
    let int_tuple = if int_cols.len() == 1 {
        int_cols[0].clone()
    } else {
        format!("({})", int_cols.join(", "))
    };

    // 1) DELETE from intermediate — uses the leading-col index because the
    // mapping always covers a unique key on the source which corresponds to
    // a prefix of the intermediate's composite UNIQUE index.
    stmts.push(format!(
        "DELETE FROM {} WHERE {} IN (SELECT {} FROM \"{}\")",
        intermediate_tbl, int_tuple, src_select, transition_tbl
    ));

    // 2) DELETE from target — translate intermediate cols to target cols via
    // plan.group_by_columns position (target_group_columns mirrors that
    // ordering with alias resolution applied).
    let target_cols_all = target_group_columns(plan);
    let mut target_tuple_cols: Vec<String> = Vec::with_capacity(mappings.len());
    for (ic, _) in mappings {
        if let Some(idx) = plan
            .group_by_columns
            .iter()
            .position(|gb| normalized_column_name(gb) == *ic)
        {
            if let Some(tc) = target_cols_all.get(idx) {
                target_tuple_cols.push(tc.clone());
            }
        }
    }
    if target_tuple_cols.len() == mappings.len() {
        let tgt_tuple = if target_tuple_cols.len() == 1 {
            target_tuple_cols[0].clone()
        } else {
            format!("({})", target_tuple_cols.join(", "))
        };
        stmts.push(format!(
            "DELETE FROM {} WHERE {} IN (SELECT {} FROM \"{}\")",
            target_qv, tgt_tuple, src_select, transition_tbl
        ));
    }

    // 3) ANALYZE intermediate — like the MERGE path. Future trigger fires
    // on this IMV want fresh stats on the row count.
    stmts.push(format!("ANALYZE {}", intermediate_tbl));
}

/// 1.4.6 — bulk-INSERT path for Item α OUT→IN flips on sources that have
/// passed the `plan.source_join_keys` safety gate.
///
/// Pre-condition (enforced by the caller via `plan.source_join_keys.contains`):
/// the source is JOIN-secondary with all JOIN equalities mapping to GROUP
/// BY columns AND those source columns cover a UNIQUE key on the source.
/// Together with Item α's OUT→IN guarantee (OLD post-filter empty), this
/// means the intermediate has zero rows for the group keys the scratch
/// will produce — MERGE's per-row probe is wasted, a plain INSERT is
/// correct.
///
/// Companion change in the target-sync block: the target DELETE on the
/// affected keys is dropped (target had zero rows for those keys by the
/// same reasoning).
fn push_bulk_insert_and_affected(
    stmts: &mut Vec<String>,
    scratch_tbl: &str,
    delta_query: &str,
    intermediate_tbl: &str,
    affected_tbl: &str,
    select_expr: &str,
) {
    stmts.push(format!("TRUNCATE {}", affected_tbl));
    stmts.push(format!("TRUNCATE {}", scratch_tbl));
    stmts.push(format!("INSERT INTO {} {}", scratch_tbl, delta_query));
    stmts.push(format!(
        "INSERT INTO {} SELECT * FROM {}",
        intermediate_tbl, scratch_tbl
    ));
    // Same reasoning as push_materialized_merge_and_affected: the
    // intermediate just grew by ~|scratch| rows. The downstream target-sync
    // INSERT joins on the composite UNIQUE index and needs accurate stats.
    stmts.push(format!("ANALYZE {}", intermediate_tbl));
    stmts.push(format!(
        "INSERT INTO {} SELECT {} FROM {} AS __d",
        affected_tbl, select_expr, scratch_tbl
    ));
    stmts.push(format!("ANALYZE {}", affected_tbl));
}

/// Aggregate-IMV `INSERT` / `INSERT_PROMOTED` arm of [`reflex_build_delta_sql`].
///
/// Pushes the materialized scratch fill + MERGE statements onto `stmts`.
/// Bulk-INSERT (skipping MERGE) only fires when the operation is an Item α
/// `INSERT_PROMOTED` *and* the source has a `source_join_keys` entry — see
/// the in-body comment for why regular `INSERT` always takes the MERGE path.
#[allow(clippy::too_many_arguments)]
fn aggregate_insert_stmts(
    operation: &str,
    plan: &AggregationPlan,
    base_query: &str,
    source_table: &str,
    grp_cols: &Option<Vec<String>>,
    intermediate_tbl: &str,
    affected_tbl: &str,
    scratch_tbl: &str,
    new_tbl: &str,
    stmts: &mut Vec<String>,
) {
    let delta_q = replace_source_with_transition(base_query, source_table, new_tbl);

    // Bulk-INSERT eligibility:
    //   * Item α promoted OUT→IN (op = INSERT_PROMOTED), AND
    //   * plan.source_join_keys has an entry for the trigger
    //     source → the source's identity uniquely determines
    //     its slice of intermediate group keys; the OLD-side
    //     was filter-rejected so those keys do not exist in
    //     intermediate; plain INSERT is correct.
    //
    // Regular INSERT (op = INSERT) NEVER takes the bulk path —
    // new fact rows can legitimately aggregate into existing
    // groups, so MERGE is required.
    let bulk_insert_eligible =
        operation == "INSERT_PROMOTED" && plan.source_join_keys.contains_key(source_table);

    if let Some(ref cols) = grp_cols {
        let select_expr = affected_groups_select(cols);
        if bulk_insert_eligible {
            push_bulk_insert_and_affected(
                stmts,
                scratch_tbl,
                &delta_q,
                intermediate_tbl,
                affected_tbl,
                &select_expr,
            );
        } else {
            // 1.4.6 attempt + revert (earlier in this same
            // session): an earlier draft wired the dispatch
            // DO block into the grouped INSERT path so Item α's
            // promoted bulk flips could re-route to reconcile.
            // Reverted because scratch-fill cost dominates;
            // see journal/2026-05-15_dispatch_wiring_revert.md.
            push_materialized_merge_and_affected(
                stmts,
                scratch_tbl,
                &delta_q,
                intermediate_tbl,
                plan,
                DeltaOp::Add,
                affected_tbl,
                &select_expr,
                true,
            );
        }
    } else {
        // Scalar IMV (no grouping). INSERT_PROMOTED degenerates
        // here — a one-row scalar intermediate is incompatible
        // with the "no overlap" guarantee. Stay on MERGE.
        push_materialized_merge(
            stmts,
            scratch_tbl,
            &delta_q,
            intermediate_tbl,
            plan,
            DeltaOp::Add,
        );
    }
}

/// Aggregate-IMV `DELETE` / `DELETE_PROMOTED` arm of [`reflex_build_delta_sql`].
///
/// Returns `true` when the bulk-eligible early-return path was taken — the
/// caller MUST then cache the result and `return` immediately, skipping the
/// target-sync / cleanup epilogue (the bulk path already removed target rows
/// via the JOIN mapping). Returns `false` to signal the standard scratch +
/// MERGE shape; the caller continues into the epilogue.
#[allow(clippy::too_many_arguments)]
fn aggregate_delete_stmts(
    plan: &AggregationPlan,
    view_name: &str,
    source_table: &str,
    base_query: &str,
    end_query: &str,
    orig_base_query: &str,
    has_min_max: bool,
    grp_cols: &Option<Vec<String>>,
    intermediate_tbl: &str,
    affected_tbl: &str,
    scratch_tbl: &str,
    old_tbl: &str,
    stmts: &mut Vec<String>,
) -> bool {
    let delta_q = replace_source_with_transition(base_query, source_table, old_tbl);

    // Bulk-DELETE eligibility:
    //   * Item α IN→OUT promotion (op = DELETE_PROMOTED) OR a
    //     regular DELETE on a source that has the safety mapping.
    //   * plan.source_join_keys has an entry for the trigger
    //     source (JOIN-secondary, mapping covers a unique key
    //     on the source).
    //   * end_query has no further GROUP BY — target rows are
    //     1:1 with intermediate. Without this, bulk-DELETE on
    //     intermediate could leave target rows that should be
    //     recomputed from surviving intermediate rows.
    let end_q_has_gb = end_query.to_uppercase().contains("GROUP BY");
    let bulk_delete_eligible =
        grp_cols.is_some() && plan.source_join_keys.contains_key(source_table) && !end_q_has_gb;

    if bulk_delete_eligible {
        // Skip scratch fill entirely. Two indexed DELETEs
        // (intermediate + target) using the JOIN mapping
        // against the OLD transition table.
        let mappings = plan
            .source_join_keys
            .get(source_table)
            .expect("eligibility checked above");
        push_bulk_delete_via_transition(
            stmts,
            intermediate_tbl,
            &quote_identifier(view_name),
            old_tbl,
            mappings,
            plan,
        );
        stmts.push(format!(
            "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() \
             WHERE name = '{}' AND (last_update_date IS NULL OR last_update_date < NOW() - INTERVAL '1 second')",
            view_name.replace("'", "''")
        ));
        return true;
    }

    let recompute_scope: Option<&str> = if let Some(ref cols) = grp_cols {
        let select_expr = affected_groups_select(cols);
        // 1.4.6 attempt + revert: same wasted-scratch problem as
        // the INSERT branch above. Bulk DELETE-shape (Item α
        // IN→OUT promotion) pays ~50–100 s for scratch fill on
        // alp; the dispatch win when reconcile is cheaper is
        // smaller than the scratch overhead. Stay on the inline
        // MERGE + dead-cleanup path.
        push_materialized_merge_and_affected(
            stmts,
            scratch_tbl,
            &delta_q,
            intermediate_tbl,
            plan,
            DeltaOp::Subtract,
            affected_tbl,
            &select_expr,
            true,
        );
        Some(affected_tbl)
    } else {
        push_materialized_merge(
            stmts,
            scratch_tbl,
            &delta_q,
            intermediate_tbl,
            plan,
            DeltaOp::Subtract,
        );
        None
    };
    if has_min_max {
        // Top-K (1.3.0): refresh scalar __min_x / __max_x from
        // topk[1] for groups whose heap survived the subtract,
        // BEFORE the source-scan recompute. The recompute then
        // only fires for actually-underflowed groups.
        if let Some(refresh) =
            build_topk_scalar_refresh_sql(intermediate_tbl, plan, recompute_scope)
        {
            stmts.push(refresh);
        }
        if let Some(recompute) =
            build_min_max_recompute_sql(intermediate_tbl, plan, orig_base_query, recompute_scope)
        {
            stmts.push(recompute);
        }
    }
    false
}

/// Aggregate-IMV `UPDATE` arm of [`reflex_build_delta_sql`].
///
/// Returns `Some(merge_sql)` when the non-MIN/MAX grouped path stashed the
/// MERGE SQL for the target-sync dispatch (the epilogue then routes it
/// through `build_high_selectivity_dispatch_sql` or the partition-aware
/// equivalent). Returns `None` for every shape that emitted the MERGE
/// inline.
#[allow(clippy::too_many_arguments)]
fn aggregate_update_stmts(
    plan: &AggregationPlan,
    source_table: &str,
    base_query: &str,
    orig_base_query: &str,
    has_min_max: bool,
    grp_cols: &Option<Vec<String>>,
    intermediate_tbl: &str,
    affected_tbl: &str,
    shrunk_tbl: &str,
    scratch_tbl: &str,
    old_tbl: &str,
    new_tbl: &str,
    stmts: &mut Vec<String>,
) -> Option<String> {
    let delta_old = replace_source_with_transition(base_query, source_table, old_tbl);
    let delta_new = replace_source_with_transition(base_query, source_table, new_tbl);

    let has_topk = plan.intermediate_columns.iter().any(|ic| ic.has_topk());

    if has_min_max {
        // Two orderings, picked by whether top-K is in play:
        //
        // - Non-top-K (legacy): Sub → recompute(if scalar NULL) → Add.
        //   The recompute MUST run BEFORE Add because Sub leaves
        //   `scalar = NULL` and Add would otherwise compute
        //   `LEAST(NULL, d.scalar) = d.scalar`, swallowing any unchanged
        //   source row that should be the new MIN/MAX.
        //
        // - Top-K: Sub → topk_refresh → Add → forced recompute. The
        //   algebraic merge can land heap on K elements that aren't the
        //   true top-K of the post-UPDATE source — heap pre-update never
        //   held the unchanged rows that should fill it now. Forcing
        //   recompute after Add re-derives heap+scalar from the
        //   (post-UPDATE) source for every affected top-K column.
        if let Some(ref cols) = grp_cols {
            let select_expr = affected_groups_select(cols);
            push_materialized_merge_and_affected(
                stmts,
                scratch_tbl,
                &delta_old,
                intermediate_tbl,
                plan,
                DeltaOp::Subtract,
                affected_tbl,
                &select_expr,
                true,
            );
            // N1: capture groups whose heap shrank below K post-Sub.
            // Must run BEFORE Add (which would re-fill the heap and
            // hide the shrinkage signal) and BEFORE topk_refresh
            // (which doesn't move the cardinality but ordering kept
            // contiguous with the Sub for clarity).
            let recompute_scope = if has_topk
                && push_topk_shrunk_groups_capture(
                    stmts,
                    intermediate_tbl,
                    plan,
                    affected_tbl,
                    shrunk_tbl,
                ) {
                shrunk_tbl
            } else {
                affected_tbl
            };
            if let Some(refresh) =
                build_topk_scalar_refresh_sql(intermediate_tbl, plan, Some(affected_tbl))
            {
                stmts.push(refresh);
            }
            if !has_topk {
                // Non-top-K: recompute BEFORE Add to avoid LEAST(NULL, d).
                if let Some(recompute) = build_min_max_recompute_sql(
                    intermediate_tbl,
                    plan,
                    orig_base_query,
                    Some(affected_tbl),
                ) {
                    stmts.push(recompute);
                }
            }
            push_materialized_merge_and_affected(
                stmts,
                scratch_tbl,
                &delta_new,
                intermediate_tbl,
                plan,
                DeltaOp::Add,
                affected_tbl,
                &select_expr,
                false,
            );
            if has_topk {
                // Top-K: forced recompute AFTER Add to overwrite any
                // stale heap content the algebraic merge left behind.
                // Scoped to `__reflex_shrunk_*` (groups whose heap
                // genuinely shrank) instead of `__reflex_affected_*` —
                // groups whose post-Sub heap stayed at K had no
                // heap-eligible row removed and Sub+Add alone is
                // correct.
                if let Some(recompute) = build_min_max_recompute_sql_force_topk(
                    intermediate_tbl,
                    plan,
                    orig_base_query,
                    Some(recompute_scope),
                ) {
                    stmts.push(recompute);
                }
            }
        } else {
            push_materialized_merge(
                stmts,
                scratch_tbl,
                &delta_old,
                intermediate_tbl,
                plan,
                DeltaOp::Subtract,
            );
            if let Some(refresh) = build_topk_scalar_refresh_sql(intermediate_tbl, plan, None) {
                stmts.push(refresh);
            }
            if !has_topk {
                if let Some(recompute) =
                    build_min_max_recompute_sql(intermediate_tbl, plan, orig_base_query, None)
                {
                    stmts.push(recompute);
                }
            }
            push_materialized_merge(
                stmts,
                scratch_tbl,
                &delta_new,
                intermediate_tbl,
                plan,
                DeltaOp::Add,
            );
            if has_topk {
                if let Some(recompute) = build_min_max_recompute_sql_force_topk(
                    intermediate_tbl,
                    plan,
                    orig_base_query,
                    None,
                ) {
                    stmts.push(recompute);
                }
            }
        }
        None
    } else if grp_cols.is_some() {
        let cols = grp_cols.as_ref().expect("grp_cols is Some — checked above");
        let net_delta = build_net_delta_query(&delta_old, &delta_new, plan);
        let select_expr = affected_groups_select(cols);
        // 1.4.5: emit scratch + affected EARLY so the dispatch
        // block below can read |affected| for the selectivity
        // check before deciding MERGE-vs-rebuild. The MERGE
        // statement itself moves into the dispatch DO block at
        // the end of this function.
        stmts.push(format!("TRUNCATE {}", affected_tbl));
        stmts.push(format!("TRUNCATE {}", scratch_tbl));
        stmts.push(format!("INSERT INTO {} {}", scratch_tbl, net_delta));
        // Scratch is pre-grouped by build_net_delta_query (SUM ... GROUP BY keys),
        // so it is already one row per group key — DISTINCT is redundant.
        stmts.push(format!(
            "INSERT INTO {} SELECT {} FROM {} AS __d",
            affected_tbl, select_expr, scratch_tbl
        ));
        // ANALYZE the freshly populated affected so downstream
        // statements (dead-cleanup DELETE, target DELETE/INSERT,
        // dispatch DO block) plan against current row counts. See
        // the comment in push_materialized_merge_and_affected for
        // the failure mode this avoids.
        stmts.push(format!("ANALYZE {}", affected_tbl));
        // Capture the MERGE SQL — the dispatch block emits it
        // (instead of running it unconditionally).
        let merge_sql_for_dispatch =
            build_merge_from_table_sql(intermediate_tbl, scratch_tbl, plan, DeltaOp::Add);
        Some(merge_sql_for_dispatch)
    } else {
        push_materialized_merge(
            stmts,
            scratch_tbl,
            &delta_old,
            intermediate_tbl,
            plan,
            DeltaOp::Subtract,
        );
        push_materialized_merge(
            stmts,
            scratch_tbl,
            &delta_new,
            intermediate_tbl,
            plan,
            DeltaOp::Add,
        );
        None
    }
}

/// 1.4.5: when set, the cleanup/target-sync block at the end emits the
/// high-selectivity dispatch DO block (TRUNCATE+rebuild OR MERGE+cleanup)
/// instead of the standard MERGE-then-cleanup+target-sync sequence.
struct PendingDispatch {
    merge_sql: String,
}

/// Self-join full refresh: source_table appears multiple times in base_query, so
/// the standard delta is wrong (every alias gets replaced with the same transition).
/// Both passthrough and aggregate paths rebuild from base_query.
fn self_join_full_refresh_stmts(
    view_name: &str,
    base_query: &str,
    end_query: &str,
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    stmts: &mut Vec<String>,
) {
    let qv = quote_identifier(view_name);
    if plan.is_passthrough {
        stmts.push(format!("DELETE FROM {}", qv));
        stmts.push(format!("INSERT INTO {} {}", qv, base_query));
    } else {
        stmts.push(format!("TRUNCATE {}", intermediate_tbl));
        stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
        if end_query.is_empty() {
            stmts.push(format!("TRUNCATE {}", qv));
            stmts.push(format!("INSERT INTO {} {}", qv, base_query));
        } else {
            stmts.push(format!("TRUNCATE {}", qv));
            stmts.push(format!("INSERT INTO {} {}", qv, end_query));
        }
    }
}

/// Identifiers (lowercased) that refer to `source_table` in `base_query`: its
/// bare name plus any alias bound immediately after the JOIN keyword
/// (`JOIN src a` or `JOIN src AS a`). Used to decide which GROUP BY columns are
/// sourced from the (mutated) secondary side of an outer join — those are the
/// ones whose value can migrate when the secondary changes.
fn secondary_ref_identifiers(base_query: &str, source_table: &str) -> Vec<String> {
    // `source_table` may be quoted (`"v__cte_agg"`); compare on the unquoted form.
    let src_unquoted = source_table.trim_matches('"');
    let bare = split_qualified_name(src_unquoted)
        .1
        .trim_matches('"')
        .to_lowercase();
    let src_l = src_unquoted.to_lowercase();
    let toks: Vec<String> = base_query
        .split(|c: char| c.is_whitespace())
        .filter(|t| !t.is_empty())
        .map(|t| t.trim_matches('"').to_string())
        .collect();
    const KW: [&str; 17] = [
        "on",
        "group",
        "where",
        "left",
        "right",
        "inner",
        "join",
        "full",
        "outer",
        "order",
        "having",
        "limit",
        "union",
        "except",
        "intersect",
        "cross",
        "natural",
    ];
    let mut ids = vec![bare.clone()];
    for (i, tok) in toks.iter().enumerate() {
        let t = tok.to_lowercase();
        if t == src_l || t == bare {
            if let Some(next) = toks.get(i + 1) {
                let mut nx = next.trim_matches(',').to_lowercase();
                if nx == "as" {
                    if let Some(n2) = toks.get(i + 2) {
                        nx = n2.trim_matches(',').to_lowercase();
                    }
                }
                if !nx.is_empty()
                    && nx.chars().all(|c| c.is_alphanumeric() || c == '_')
                    && !KW.contains(&nx.as_str())
                {
                    ids.push(nx);
                }
            }
        }
    }
    ids
}

/// Outer-join-secondary handling: when source_table is the secondary side of a
/// LEFT/RIGHT JOIN (or any side of FULL OUTER), the MERGE subtract can't represent
/// the NULL semantics. Passthrough → full refresh. Aggregate → targeted group
/// reconcile via the affected_tbl, scoped to the *stable* (non-secondary) group
/// columns so a secondary-derived group key that migrates is fully rebuilt for
/// the affected join keys. With no stable group column, falls back to a full
/// intermediate + target refresh.
#[allow(clippy::too_many_arguments)]
fn outer_join_secondary_stmts(
    view_name: &str,
    source_table: &str,
    operation: &str,
    base_query: &str,
    end_query: &str,
    plan: &AggregationPlan,
    grp_cols: &Option<Vec<String>>,
    intermediate_tbl: &str,
    affected_tbl: &str,
    old_tbl: &str,
    new_tbl: &str,
    stmts: &mut Vec<String>,
) {
    let qv = quote_identifier(view_name);

    if plan.is_passthrough {
        stmts.push(format!("DELETE FROM {}", qv));
        stmts.push(format!("INSERT INTO {} {}", qv, base_query));
        return;
    }

    if let Some(ref cols) = grp_cols {
        // Scope the recompute to the STABLE group columns — those NOT sourced
        // from the mutated secondary table. A secondary-derived group column
        // (e.g. `a.sx` in `GROUP BY t.g, a.sx`) can MIGRATE when the secondary
        // changes (NULL<->value, value<->value); matching the recompute on the
        // full group key would miss the OLD value's row (it isn't in the delta,
        // which only sees the new state), leaving a stale/phantom group. Matching
        // on the stable cols only deletes every row sharing the affected join
        // keys, so the migrated groups are fully rebuilt from the live query.
        // (For the common case where every group key is from the primary side,
        // the stable set equals the full set — behaviour is unchanged.)
        let sec_ids = secondary_ref_identifiers(base_query, source_table);
        let target_cols_all = target_group_columns(plan);
        let mut scope_cols: Vec<String> = Vec::new();
        let mut scope_target_cols: Vec<String> = Vec::new();
        for (i, gb) in plan.group_by_columns.iter().enumerate() {
            let qualifier = gb
                .split_once('.')
                .map(|(q, _)| q.trim().trim_matches('"').to_lowercase());
            // Stable only when confidently qualified by a non-secondary table.
            // Unqualified columns are excluded (treated as possibly migrating),
            // which only ever broadens the recompute — never narrows it.
            let stable = qualifier.is_some_and(|q| !sec_ids.contains(&q));
            if stable {
                if let Some(c) = cols.get(i) {
                    scope_cols.push(c.clone());
                }
                if let Some(tc) = target_cols_all.get(i) {
                    scope_target_cols.push(tc.clone());
                }
            }
        }
        // DISTINCT keys (DISTINCT-without-GROUP-BY) sit after group_by_columns in
        // `cols`/`target_cols_all` and are always projection keys → stable.
        for (c, tc) in cols
            .iter()
            .zip(target_cols_all.iter())
            .skip(plan.group_by_columns.len())
        {
            scope_cols.push(c.clone());
            scope_target_cols.push(tc.clone());
        }

        if scope_cols.is_empty() {
            // Every group key derives from the mutated secondary — no stable key
            // to scope by. Fall back to a full intermediate + target refresh.
            stmts.push(format!("TRUNCATE {}", intermediate_tbl));
            stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
            stmts.push(format!("DELETE FROM {}", qv));
            stmts.push(format!("INSERT INTO {} {}", qv, end_query));
            return;
        }

        let select_expr = affected_groups_select(cols);
        let transition = if operation == "DELETE" {
            old_tbl
        } else {
            new_tbl
        };
        let delta_q = replace_source_with_transition(base_query, source_table, transition);

        stmts.push(format!("TRUNCATE {}", affected_tbl));
        stmts.push(format!(
            "INSERT INTO {} SELECT DISTINCT {} FROM ({}) AS __d",
            affected_tbl, select_expr, delta_q
        ));

        let ns_in_int = null_safe_in(
            affected_tbl,
            intermediate_tbl,
            &scope_cols,
            &scope_cols,
            &plan.not_null_columns,
        );
        stmts.push(format!(
            "DELETE FROM {} WHERE {}",
            intermediate_tbl, ns_in_int
        ));

        let ns_in_full = null_safe_in(
            affected_tbl,
            "__full",
            &scope_cols,
            &scope_cols,
            &plan.not_null_columns,
        );
        stmts.push(format!(
            "INSERT INTO {} SELECT * FROM ({}) AS __full WHERE {}",
            intermediate_tbl, base_query, ns_in_full
        ));

        let ns_in_tgt_delete = null_safe_in(
            affected_tbl,
            &qv,
            &scope_target_cols,
            &scope_cols,
            &plan.not_null_columns,
        );
        stmts.push(format!("DELETE FROM {} WHERE {}", qv, ns_in_tgt_delete));

        let ns_in_tgt_insert = null_safe_in(
            affected_tbl,
            intermediate_tbl,
            &scope_cols,
            &scope_cols,
            &plan.not_null_columns,
        );
        stmts.push(format!(
            "INSERT INTO {} {} AND {}",
            qv, end_query, ns_in_tgt_insert
        ));
    } else {
        stmts.push(format!("TRUNCATE {}", intermediate_tbl));
        stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
        stmts.push(format!("TRUNCATE {}", qv));
        if end_query.is_empty() {
            stmts.push(format!("INSERT INTO {} {}", qv, base_query));
        } else {
            stmts.push(format!("INSERT INTO {} {}", qv, end_query));
        }
    }
}

/// Passthrough delta: route through per-(IMV, source) UNLOGGED scratch tables to
/// avoid the transition-table-in-EXECUTE assertion, then run the per-operation
/// targeted DML (mapping-driven DELETE/UPDATE; INSERT splices the scratch into
/// base_query).
#[allow(clippy::too_many_arguments)]
fn passthrough_op_stmts(
    view_name: &str,
    source_table: &str,
    operation: &str,
    base_query: &str,
    plan: &AggregationPlan,
    new_tbl: &str,
    old_tbl: &str,
    stmts: &mut Vec<String>,
) {
    let qv = quote_identifier(view_name);
    let pt_new = passthrough_scratch_new_table_name(view_name, source_table);
    let pt_old = passthrough_scratch_old_table_name(view_name, source_table);
    let mappings = plan.passthrough_key_mappings.get(source_table);

    let needs_new = matches!(operation, "INSERT" | "INSERT_PROMOTED" | "UPDATE");
    let needs_old = matches!(operation, "DELETE" | "DELETE_PROMOTED" | "UPDATE");
    if needs_new {
        stmts.push(format!("TRUNCATE {}", pt_new));
        stmts.push(format!(
            "INSERT INTO {} SELECT * FROM \"{}\"",
            pt_new, new_tbl
        ));
    }
    if needs_old {
        stmts.push(format!("TRUNCATE {}", pt_old));
        stmts.push(format!(
            "INSERT INTO {} SELECT * FROM \"{}\"",
            pt_old, old_tbl
        ));
    }

    match operation {
        "INSERT" | "INSERT_PROMOTED" => {
            let delta_q = replace_source_with_transition(base_query, source_table, &pt_new);
            stmts.push(format!("INSERT INTO {} {}", qv, delta_q));
            if operation == "INSERT_PROMOTED" {
                stmts.push(format!("ANALYZE {}", qv));
            }
        }
        "DELETE" | "DELETE_PROMOTED" => {
            if let Some(mappings) = mappings {
                let target_cols: Vec<String> =
                    mappings.iter().map(|(t, _)| format!("\"{}\"", t)).collect();
                let source_cols: Vec<String> =
                    mappings.iter().map(|(_, s)| format!("\"{}\"", s)).collect();
                let row = row_expr(&target_cols);
                stmts.push(format!(
                    "DELETE FROM {} WHERE {} IN (SELECT {} FROM {})",
                    qv,
                    row,
                    source_cols.join(", "),
                    pt_old
                ));
                if operation == "DELETE_PROMOTED" {
                    stmts.push(format!("ANALYZE {}", qv));
                }
            } else {
                stmts.push(format!("DELETE FROM {}", qv));
                stmts.push(format!("INSERT INTO {} {}", qv, base_query));
            }
        }
        "UPDATE" => {
            if let Some(mappings) = mappings {
                let target_cols: Vec<String> =
                    mappings.iter().map(|(t, _)| format!("\"{}\"", t)).collect();
                let source_cols: Vec<String> =
                    mappings.iter().map(|(_, s)| format!("\"{}\"", s)).collect();
                let row = row_expr(&target_cols);
                stmts.push(format!(
                    "DELETE FROM {} WHERE {} IN (SELECT {} FROM {})",
                    qv,
                    row,
                    source_cols.join(", "),
                    pt_old
                ));
                let delta_new = replace_source_with_transition(base_query, source_table, &pt_new);
                stmts.push(format!("INSERT INTO {} {}", qv, delta_new));
            } else {
                stmts.push(format!("DELETE FROM {}", qv));
                stmts.push(format!("INSERT INTO {} {}", qv, base_query));
            }
        }
        _ => {}
    }
}

/// Aggregate-branch epilogue: refresh target from intermediate, clean up dead groups,
/// stamp metadata. Takes `pending_dispatch` by value because the UPDATE arm may have
/// produced a deferred MERGE that the epilogue must splice into either the
/// high-selectivity dispatch DO block or the partition-aware dispatch.
#[allow(clippy::too_many_arguments)]
fn aggregate_epilogue_stmts(
    view_name: &str,
    source_table: &str,
    operation: &str,
    end_query: &str,
    plan: &AggregationPlan,
    grp_cols: &Option<Vec<String>>,
    intermediate_tbl: &str,
    affected_tbl: &str,
    scratch_tbl: &str,
    pending_dispatch: Option<PendingDispatch>,
    stmts: &mut Vec<String>,
) {
    let end_query_has_group_by = end_query.to_uppercase().contains("GROUP BY");
    let include_dead_cleanup = plan.needs_ivm_count
        && grp_cols.is_some()
        && matches!(operation, "DELETE" | "DELETE_PROMOTED" | "UPDATE");
    let skip_target_delete = operation == "INSERT_PROMOTED"
        && grp_cols.is_some()
        && plan.source_join_keys.contains_key(source_table);
    let metadata_sql = format!(
        "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() \
         WHERE name = '{}' AND (last_update_date IS NULL OR last_update_date < NOW() - INTERVAL '1 second')",
        view_name.replace("'", "''")
    );

    let mut pending_dispatch = pending_dispatch;

    if end_query_has_group_by {
        let qv = quote_identifier(view_name);
        if plan.group_by_columns.is_empty() {
            let tdel = format!("DELETE FROM {}", qv);
            let tins = format!("INSERT INTO {} {}", qv, end_query);
            if let Some(pd) = pending_dispatch.take() {
                stmts.push(build_high_selectivity_dispatch_sql(
                    view_name,
                    intermediate_tbl,
                    affected_tbl,
                    &pd.merge_sql,
                    None,
                    &tdel,
                    &tins,
                ));
            } else {
                if !skip_target_delete {
                    stmts.push(tdel);
                }
                stmts.push(tins);
            }
        } else {
            let output_cols: Vec<String> = plan
                .group_by_columns
                .iter()
                .map(|c| format!("\"{}\"", normalized_column_name(c)))
                .collect();
            let target_cols: Vec<String> = target_group_columns(plan)
                .into_iter()
                .take(plan.group_by_columns.len())
                .collect();
            match inject_affected_filter_before_group_by(
                end_query,
                &output_cols,
                affected_tbl,
                intermediate_tbl,
                &plan.not_null_columns,
            ) {
                Some(spliced_end_q) => {
                    let ns_in_target = null_safe_in(
                        affected_tbl,
                        &qv,
                        &target_cols,
                        &output_cols,
                        &plan.not_null_columns,
                    );
                    let tdel = format!("DELETE FROM {} WHERE {}", qv, ns_in_target);
                    let tins = format!("INSERT INTO {} {}", qv, spliced_end_q);
                    if let Some(pd) = pending_dispatch.take() {
                        stmts.push(build_high_selectivity_dispatch_sql(
                            view_name,
                            intermediate_tbl,
                            affected_tbl,
                            &pd.merge_sql,
                            None,
                            &tdel,
                            &tins,
                        ));
                    } else {
                        if !skip_target_delete {
                            stmts.push(tdel);
                        }
                        stmts.push(tins);
                    }
                }
                None => {
                    let tdel = format!("DELETE FROM {}", qv);
                    let tins = format!("INSERT INTO {} {}", qv, end_query);
                    if let Some(pd) = pending_dispatch.take() {
                        stmts.push(build_high_selectivity_dispatch_sql(
                            view_name,
                            intermediate_tbl,
                            affected_tbl,
                            &pd.merge_sql,
                            None,
                            &tdel,
                            &tins,
                        ));
                    } else {
                        if !skip_target_delete {
                            stmts.push(tdel);
                        }
                        stmts.push(tins);
                    }
                }
            }
        }
        stmts.push(metadata_sql);
    } else if let Some(ref cols) = grp_cols {
        let qv = quote_identifier(view_name);
        let target_cols = target_group_columns(plan);
        let ns_in_intermediate = null_safe_in(
            affected_tbl,
            intermediate_tbl,
            cols,
            cols,
            &plan.not_null_columns,
        );
        let ns_in_target_delete = null_safe_in(
            affected_tbl,
            &qv,
            &target_cols,
            cols,
            &plan.not_null_columns,
        );
        let dead_cleanup_sql = if include_dead_cleanup {
            Some(format!(
                "DELETE FROM {} WHERE __ivm_count <= 0 AND {}",
                intermediate_tbl, ns_in_intermediate
            ))
        } else {
            None
        };
        let target_delete_sql = format!("DELETE FROM {} WHERE {}", qv, ns_in_target_delete);
        let target_insert_sql = format!(
            "INSERT INTO {} {} AND {}",
            qv, end_query, ns_in_intermediate
        );

        if let Some(pd) = pending_dispatch.take() {
            let use_partition_dispatch = !plan.partition_columns.is_empty()
                && plan.partition_strategy.eq_ignore_ascii_case("LIST");
            if use_partition_dispatch {
                let part_col = &plan.partition_columns[0];
                let part_col_q = format!("\"{}\"", part_col);
                let filtered_scratch = format!(
                    "(SELECT * FROM {} WHERE {}::text <> ALL($1::TEXT[]))",
                    scratch_tbl, part_col_q
                );
                let merge_filtered = build_merge_from_table_sql(
                    intermediate_tbl,
                    &filtered_scratch,
                    plan,
                    DeltaOp::Add,
                );
                let dead_cleanup_filtered = dead_cleanup_sql.as_ref().map(|s| {
                    format!(
                        "{} AND EXISTS (SELECT 1 FROM {} __ap \
                          WHERE __ap.{} = {}.{} AND __ap.{}::text <> ALL($1::TEXT[]))",
                        s, affected_tbl, part_col_q, intermediate_tbl, part_col_q, part_col_q
                    )
                });
                let tdel_filtered = format!(
                    "{} AND {}.{}::text <> ALL($1::TEXT[])",
                    target_delete_sql, qv, part_col_q
                );
                let tins_filtered = format!(
                    "{} AND {}.{}::text <> ALL($1::TEXT[])",
                    target_insert_sql, intermediate_tbl, part_col_q
                );
                stmts.push(build_partition_aware_dispatch_sql(
                    view_name,
                    intermediate_tbl,
                    intermediate_tbl,
                    affected_tbl,
                    part_col,
                    &merge_filtered,
                    dead_cleanup_filtered.as_deref(),
                    &tdel_filtered,
                    &tins_filtered,
                ));
            } else {
                stmts.push(build_high_selectivity_dispatch_sql(
                    view_name,
                    intermediate_tbl,
                    affected_tbl,
                    &pd.merge_sql,
                    dead_cleanup_sql.as_deref(),
                    &target_delete_sql,
                    &target_insert_sql,
                ));
            }
        } else {
            if let Some(s) = dead_cleanup_sql {
                stmts.push(s);
            }
            if !skip_target_delete {
                stmts.push(target_delete_sql);
            }
            stmts.push(target_insert_sql);
        }
        stmts.push(metadata_sql);
    } else {
        let qv = quote_identifier(view_name);
        stmts.push(format!("TRUNCATE {}", qv));
        stmts.push(format!("INSERT INTO {} {}", qv, end_query));
        stmts.push(metadata_sql);
    }
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
    let escaped_view = view_name.replace('\'', "''");
    let lookup_sql = format!(
        "SELECT base_query FROM public.__reflex_ivm_reference WHERE name = '{}' AND enabled = TRUE",
        escaped_view
    );
    let base_query: String = match Spi::get_one::<&str>(&lookup_sql) {
        Ok(Some(s)) => s.to_string(),
        _ => return String::new(),
    };
    let transition = transition_new_table_name(source_table);
    replace_source_with_transition(&base_query, source_table, &transition)
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

/// Flushes all accumulated deferred deltas for a given source table.
///
/// Called by the deferred constraint trigger at COMMIT time.
/// Reads from the staging table (__reflex_delta_<source>), applies deltas
/// to each DEFERRED IMV, then cleans up staging and pending rows.
#[pg_extern]
pub fn reflex_flush_deferred(source_table: &str) -> String {
    let delta_tbl = staging_delta_table_name(source_table);

    // Read all DEFERRED IMVs that depend on this source
    let imvs: Vec<(String, String, String, String, Option<String>)> = Spi::connect(|client| {
        let args = [unsafe {
            DatumWithOid::new(
                source_table.to_string(),
                PgBuiltInOids::TEXTOID.oid().value(),
            )
        }];
        client
            .select(
                "SELECT name, base_query, end_query, aggregations::text AS aggregations, \
                        where_predicate \
                 FROM public.__reflex_ivm_reference \
                 WHERE $1 = ANY(depends_on) AND enabled = TRUE \
                   AND COALESCE(refresh_mode, 'IMMEDIATE') = 'DEFERRED' \
                 ORDER BY graph_depth, name",
                None,
                &args,
            )
            .unwrap_or_report()
            .map(|row| {
                (
                    row.get_by_name::<&str, _>("name")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    row.get_by_name::<&str, _>("base_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    row.get_by_name::<&str, _>("end_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    row.get_by_name::<&str, _>("aggregations")
                        .unwrap_or(None)
                        .unwrap_or("{}")
                        .to_string(),
                    row.get_by_name::<&str, _>("where_predicate")
                        .unwrap_or(None)
                        .map(|s: &str| s.to_string()),
                )
            })
            .collect()
    });

    if imvs.is_empty() {
        return "NO DEFERRED IMVS".to_string();
    }

    let mut total_processed = 0usize;

    Spi::connect_mut(|client| {
        // 1.4.3 — Serialize flushes on the same source.
        //
        // ANALYZE (ShareUpdateExclusiveLock) + TRUNCATE (AccessExclusiveLock)
        // on the same staging-delta table inside the same transaction is a
        // classic deadlock antipattern when two sessions flush concurrently:
        // ShareUpdateExclusive is self-conflicting, so the second session
        // queues behind the first's ANALYZE. When the first then tries to
        // upgrade to AccessExclusive for the end-of-flush TRUNCATE, the lock
        // manager queues that request *behind* the second's pending
        // ShareUpdate request, and a cycle forms. Reproduced as a real
        // 42P40 deadlock under customer concurrency.
        //
        // The advisory lock is acquired before any table-level lock on the
        // staging delta, so the second session blocks here and the locks
        // inside execute in single-session order on each turn.
        let lock_key = format!("reflex_flush:{}", source_table).replace('\'', "''");
        client
            .update(
                &format!("SELECT pg_advisory_xact_lock(hashtext('{}'))", lock_key),
                None,
                &[],
            )
            .unwrap_or_report();

        // Check if staging table has any rows
        let has_rows = client
            .select(
                &format!("SELECT EXISTS(SELECT 1 FROM {} LIMIT 1) AS has", delta_tbl),
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .map(|row| {
                row.get_by_name::<bool, _>("has")
                    .unwrap_or(None)
                    .unwrap_or(false)
            })
            .unwrap_or(false);

        if !has_rows {
            // No deltas to process — clean up pending rows
            client
                .update(
                    &format!(
                        "DELETE FROM public.__reflex_deferred_pending WHERE source_table = '{}'",
                        source_table.replace("'", "''")
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
            return;
        }

        // Refresh planner stats on the staging delta so queries over it get correct
        // row estimates (TRUNCATE resets stats to zero; without ANALYZE the planner
        // assumes an empty table and may pick a bad plan).
        client
            .update(&format!("ANALYZE {}", delta_tbl), None, &[])
            .unwrap_or_report();

        // Passthrough INSERT/DELETE/UPDATE branches in reflex_build_delta_sql
        // reference the NEW/OLD transition tables literally — either directly
        // (pre-Phase-E paths) or via the Phase E per-(IMV, source) scratch
        // populate `INSERT INTO __reflex_pt_*_<v>_<s> SELECT * FROM __reflex_(new|old)_<s>`.
        // Those transition tables only exist inside an IMMEDIATE trigger's
        // REFERENCING scope; here we're at COMMIT, so stand both sides up as
        // temp views over the staging delta. The views must project the source
        // columns only (no `__reflex_op` metadata column) so downstream DML —
        // including `INSERT INTO pt_scratch SELECT * FROM view` where pt_scratch
        // is shaped `LIKE source` — sees the same column list as a real
        // transition table.
        let (src_schema, src_name_only) = split_qualified_name(source_table);
        let src_schema_lit = src_schema.unwrap_or("public").replace("'", "''");
        let src_name_lit = src_name_only.replace("'", "''");
        // Fetch raw column NAME + TYPE-NAME together. The type name is
        // needed to cast `json` / `xml` to `text` in EXCEPT ALL projections
        // — those types lack an equality operator and crash the comparison
        // otherwise. The raw column projection (for the TEMP VIEW that
        // downstream incremental codegen reads) stays unchanged.
        let src_cols_with_types: Vec<(String, String)> = client
            .select(
                "SELECT a.attname::text AS rn, t.typname::text AS tn \
                 FROM pg_attribute a \
                 JOIN pg_type t ON t.oid = a.atttypid \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 \
                   AND a.attnum > 0 AND NOT a.attisdropped \
                 ORDER BY a.attnum",
                None,
                &[
                    unsafe {
                        DatumWithOid::new(
                            src_schema_lit.clone(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe {
                        DatumWithOid::new(
                            src_name_lit.clone(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                let name = row
                    .get_by_name::<&str, _>("rn")
                    .unwrap_or(None)
                    .map(|s| s.to_string());
                let tn = row
                    .get_by_name::<&str, _>("tn")
                    .unwrap_or(None)
                    .map(|s| s.to_string());
                match (name, tn) {
                    (Some(n), Some(t)) => Some((n, t)),
                    _ => None,
                }
            })
            .collect();
        let quote_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let needs_text_cast = |t: &str| t == "json" || t == "xml";
        let src_cols: Vec<String> = src_cols_with_types
            .iter()
            .map(|(n, _)| quote_ident(n))
            .collect();
        // EXCEPT ALL comparison projection: cast types that lack an
        // equality operator to text. `json` and `xml` are the two stock
        // PG types in this category that real schemas commonly use.
        let cmp_cols: Vec<String> = src_cols_with_types
            .iter()
            .map(|(n, t)| {
                let q = quote_ident(n);
                if needs_text_cast(t) {
                    format!("{}::text", q)
                } else {
                    q
                }
            })
            .collect();
        let col_type_map: std::collections::HashMap<String, String> = src_cols_with_types
            .iter()
            .map(|(n, t)| (n.clone(), t.clone()))
            .collect();
        let projection = src_cols.join(", ");
        let new_view = transition_new_table_name(source_table);
        let old_view = transition_old_table_name(source_table);
        // 1.4.3 — `CREATE OR REPLACE TEMP VIEW` replaces the previous
        // DROP-IF-EXISTS + CREATE pair. Eliminates the noisy
        // `NOTICE: view "..." does not exist, skipping` from the first
        // flush of each session and keeps the views reusable across flushes
        // in the same backend (subsequent CREATE OR REPLACE rewrites them).
        client
            .update(
                &format!(
                    "CREATE OR REPLACE TEMP VIEW {} AS SELECT {} FROM {} WHERE __reflex_op IN ('I', 'U_NEW')",
                    new_view, projection, delta_tbl
                ),
                None,
                &[],
            )
            .unwrap_or_report();
        client
            .update(
                &format!(
                    "CREATE OR REPLACE TEMP VIEW {} AS SELECT {} FROM {} WHERE __reflex_op IN ('D', 'U_OLD')",
                    old_view, projection, delta_tbl
                ),
                None,
                &[],
            )
            .unwrap_or_report();

        // 1.4.3 — Spurious-UPDATE short-circuit.
        //
        // If the staging delta contains only paired U_OLD/U_NEW rows whose
        // projections to the source columns are identical multisets (i.e.
        // every UPDATE was a no-op at the column level — e.g. `SET
        // status='validated'` on a row whose status is already 'validated'),
        // no IMV can observe a change. Skip every IMV body, clean up, return.
        //
        // EXCEPT ALL is multiset subtraction; if both directions are empty
        // and there are no INSERT/DELETE rows, U_OLD ≡ U_NEW.
        //
        // `cmp_cols` is non-empty for every real PG table (tables have at
        // least one column), so the run-the-EXCEPT branch always executes.
        let cols_csv = cmp_cols.join(", ");
        let is_spurious = {
            let sql = format!(
                "WITH \
                   has_id AS (SELECT 1 FROM {delta} WHERE __reflex_op IN ('I', 'D') LIMIT 1), \
                   only_old AS ( \
                     SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_OLD' \
                     EXCEPT ALL \
                     SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_NEW' \
                   ), \
                   only_new AS ( \
                     SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_NEW' \
                     EXCEPT ALL \
                     SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_OLD' \
                   ) \
                 SELECT NOT EXISTS(SELECT 1 FROM has_id) \
                    AND NOT EXISTS(SELECT 1 FROM only_old) \
                    AND NOT EXISTS(SELECT 1 FROM only_new) AS sp",
                delta = delta_tbl,
                cols = cols_csv,
            );
            client
                .select(&sql, None, &[])
                .unwrap_or_report()
                .next()
                .map(|row| {
                    row.get_by_name::<bool, _>("sp")
                        .unwrap_or(None)
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        };

        if is_spurious {
            // No IMV processing. Clean up the staging delta and pending rows.
            // DELETE (not TRUNCATE) — see end-of-function comment.
            client
                .update(&format!("DELETE FROM {}", delta_tbl), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!(
                        "DELETE FROM public.__reflex_deferred_pending WHERE source_table = '{}'",
                        source_table.replace("'", "''")
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
            return;
        }

        // Cross-source consistency gate (deferred mode). When 2+ distinct
        // sources staged deltas in the same transaction, an IMV that joins two
        // of them would double-count the ΔA⋈ΔB cross product if each source's
        // net delta were applied independently: every per-source delta joins
        // against the OTHER sources' already-committed NEW state, so the cross
        // product is added once per mutated source instead of once total.
        // IMMEDIATE mode is immune (per-statement triggers apply deltas
        // sequentially, each seeing the correct intermediate state); the
        // hazard is unique to the commit-time batch flush. Detect the batch
        // shape once here — the per-IMV loop full-reconciles any affected IMV
        // exactly once via the transaction-local marker below.
        let batch_has_multiple_sources = client
            .select(
                "SELECT count(DISTINCT source_table) >= 2 AS m FROM public.__reflex_deferred_pending",
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .map(|row| row.get_by_name::<bool, _>("m").unwrap_or(None).unwrap_or(false))
            .unwrap_or(false);
        // The marker (created below) survives across the batch's per-source
        // flush calls. A later flush sees a shrunken pending set (each flush
        // deletes its own pending rows), so `batch_has_multiple_sources` may
        // already read false by then — but if the marker exists, a
        // multi-source reconcile happened earlier in this batch and this flush
        // MUST still skip the reconciled IMVs. `pg_my_temp_schema()` scopes the
        // lookup to this session's temp schema (0 ⇒ no temp schema yet).
        let marker_exists = client
            .select(
                "SELECT EXISTS(SELECT 1 FROM pg_class \
                   WHERE relname = '__reflex_deferred_reconciled_batch' \
                     AND relnamespace = pg_my_temp_schema()) AS e",
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .map(|row| {
                row.get_by_name::<bool, _>("e")
                    .unwrap_or(None)
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        let engage_cross_source_guard = batch_has_multiple_sources || marker_exists;
        if engage_cross_source_guard {
            // ON COMMIT DROP: one marker per transaction, shared across the
            // per-source flush calls (the constraint trigger flushes each
            // mutated source separately), auto-removed at commit. Records the
            // IMVs already full-reconciled in this batch so a later source's
            // flush skips them — its net delta would otherwise corrupt the
            // just-reconciled state.
            client
                .update(
                    "CREATE TEMP TABLE IF NOT EXISTS __reflex_deferred_reconciled_batch \
                     (name TEXT PRIMARY KEY) ON COMMIT DROP",
                    None,
                    &[],
                )
                .unwrap_or_report();
        }

        for (imv_name, base_query, end_query, agg_json, where_pred) in &imvs {
            if engage_cross_source_guard {
                let imv_esc = imv_name.replace('\'', "''");
                let already_reconciled = client
                    .select(
                        &format!(
                            "SELECT EXISTS(SELECT 1 FROM __reflex_deferred_reconciled_batch \
                             WHERE name = '{}') AS e",
                            imv_esc
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report()
                    .next()
                    .map(|row| {
                        row.get_by_name::<bool, _>("e")
                            .unwrap_or(None)
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if already_reconciled {
                    continue;
                }
                // Count this IMV's own sources that are pending in the batch.
                // The first of the IMV's sources to be flushed still sees all
                // of them pending (per-source cleanup only runs for sources
                // already processed, none of which are this IMV's), so it
                // reliably detects the multi-source shape and reconciles.
                let imv_has_multiple_sources = client
                    .select(
                        &format!(
                            "SELECT count(DISTINCT p.source_table) >= 2 AS m \
                             FROM public.__reflex_deferred_pending p \
                             JOIN public.__reflex_ivm_reference r ON r.name = '{}' \
                             WHERE p.source_table = ANY(r.depends_on)",
                            imv_esc
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report()
                    .next()
                    .map(|row| {
                        row.get_by_name::<bool, _>("m")
                            .unwrap_or(None)
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if imv_has_multiple_sources {
                    client
                        .update(
                            &format!(
                                "INSERT INTO __reflex_deferred_reconciled_batch (name) \
                                 VALUES ('{}') ON CONFLICT DO NOTHING",
                                imv_esc
                            ),
                            None,
                            &[],
                        )
                        .unwrap_or_report();
                    client
                        .update(
                            &format!("SELECT public.reflex_reconcile('{}')", imv_esc),
                            None,
                            &[],
                        )
                        .unwrap_or_report();
                    total_processed += 1;
                    continue;
                }
            }
            // 1.4.5 — Skip this IMV iff NO staged row matches the
            // predicate, on either side of the delta. The 1.4.4 check
            // looked only at NEW-state rows (`I` + `U_NEW`); that silently
            // dropped row-leaves-filter UPDATEs — when a row transitions
            // out of the IMV's WHERE the IMV must DELETE its
            // contribution, which requires the trigger body to run. The OR
            // below extends the gate to OLD-state passing rows so we no
            // longer mistake "no new row to add" for "no work at all".
            if let Some(pred) = where_pred {
                let pred_sql = format!(
                    "SELECT EXISTS( \
                        SELECT 1 FROM {delta} WHERE __reflex_op IN ('I', 'U_NEW') AND ({pred}) \
                     ) OR EXISTS( \
                        SELECT 1 FROM {delta} WHERE __reflex_op IN ('D', 'U_OLD') AND ({pred}) \
                     ) AS m",
                    delta = delta_tbl,
                    pred = pred,
                );
                let matched = client
                    .select(&pred_sql, None, &[])
                    .unwrap_or_report()
                    .next()
                    .map(|row| {
                        row.get_by_name::<bool, _>("m")
                            .unwrap_or(None)
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if !matched {
                    continue;
                }
            }

            // 1.4.5 — Per-IMV filter-aware spurious-skip in DEFERRED mode.
            //
            // The 1.4.3 byte-identical multiset check above is a *source-wide*
            // gate — it fires only when EVERY column is byte-equal on every
            // U_OLD/U_NEW pair AND there are no INSERT/DELETE rows. That's
            // strong but rare. The check below is per-IMV and runs even when
            // the source-wide gate didn't fire:
            //
            //   * Read the IMV's `imv_relevant_columns[source_table]` — the
            //     columns the IMV actually projects / joins on / groups by.
            //     Filter-only columns (in WHERE only) are absent.
            //   * Read the source-restricted `imv_relevant_where[source]`
            //     — alias-stripped conjuncts that evaluate against the flat
            //     staging delta.
            //   * Compare multisets of (relevant_cols)-projected rows from
            //     old-state (U_OLD ∪ D) vs new-state (U_NEW ∪ I), each
            //     filtered by the per-source predicate.
            //
            // If multisets match in both directions, the IMV's output cannot
            // change for any group touched by this delta — skip it.
            //
            // Absent metadata (CTE IMVs, SELECT *, or IMVs created before
            // 1.4.5 metadata backfill) falls through to the existing path.
            let agg_jsonb: Result<serde_json::Value, _> = serde_json::from_str(agg_json);
            if let Ok(jv) = agg_jsonb {
                let cols_arr = jv
                    .get("imv_relevant_columns")
                    .and_then(|m| m.get(source_table))
                    .and_then(|a| a.as_array());
                if let Some(cols) = cols_arr {
                    // No runtime catalog filter — the analyzer (1.4.5) only
                    // attributes a column to a source when the reference is
                    // unambiguously resolvable, so every column listed here
                    // is guaranteed to exist on the source's transition /
                    // delta table.
                    // Cast json/xml to text (no equality operator). Drop
                    // columns the analyzer wrongly attributed to this
                    // source: pre-1.5.1 `create_ivm` only filtered the
                    // attribution catalog for aggregate IMVs, so
                    // passthrough IMVs created before that fix may have
                    // `imv_relevant_columns[source]` entries that don't
                    // exist on the source table. Selecting one would
                    // crash the EXCEPT ALL with `column "X" does not
                    // exist`. The `col_type_map` is populated from the
                    // source's catalog above; absence ⇒ not on the
                    // source ⇒ drop.
                    let cols_csv = cols
                        .iter()
                        .filter_map(|v| v.as_str())
                        .filter_map(|c| {
                            let q = format!("\"{}\"", c.replace('"', "\"\""));
                            match col_type_map.get(c) {
                                Some(t) if needs_text_cast(t) => Some(format!("{}::text", q)),
                                Some(_) => Some(q),
                                None => None,
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    if !cols_csv.is_empty() {
                        let pred = jv
                            .get("imv_relevant_where")
                            .and_then(|m| m.get(source_table))
                            .and_then(|s| s.as_str())
                            .filter(|s| !s.is_empty());
                        let old_filter = match pred {
                            Some(p) => format!("__reflex_op IN ('U_OLD', 'D') AND ({})", p),
                            None => "__reflex_op IN ('U_OLD', 'D')".to_string(),
                        };
                        let new_filter = match pred {
                            Some(p) => format!("__reflex_op IN ('U_NEW', 'I') AND ({})", p),
                            None => "__reflex_op IN ('U_NEW', 'I')".to_string(),
                        };
                        let sql = format!(
                            "WITH \
                               diff_o AS ( \
                                 SELECT {cols} FROM {delta} WHERE {of} \
                                 EXCEPT ALL \
                                 SELECT {cols} FROM {delta} WHERE {nf} \
                               ), \
                               diff_n AS ( \
                                 SELECT {cols} FROM {delta} WHERE {nf} \
                                 EXCEPT ALL \
                                 SELECT {cols} FROM {delta} WHERE {of} \
                               ) \
                             SELECT NOT EXISTS(SELECT 1 FROM diff_o) \
                                AND NOT EXISTS(SELECT 1 FROM diff_n) AS sp",
                            cols = cols_csv,
                            delta = delta_tbl,
                            of = old_filter,
                            nf = new_filter,
                        );
                        let filter_skip = client
                            .select(&sql, None, &[])
                            .unwrap_or_report()
                            .next()
                            .map(|row| {
                                row.get_by_name::<bool, _>("sp")
                                    .unwrap_or(None)
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false);
                        if filter_skip {
                            continue;
                        }
                    }
                }
            }

            // Collect every per-IMV statement into an ordered list; we emit them
            // inside a single PL/pgSQL DO block with EXCEPTION so one bad IMV
            // rolls back only its own subtransaction and lets the cascade continue.
            let mut imv_stmts: Vec<String> = Vec::new();

            imv_stmts.push(format!(
                "PERFORM pg_advisory_xact_lock(hashtext('{}'), hashtext(reverse('{}')))",
                imv_name.replace("'", "''"),
                imv_name.replace("'", "''")
            ));

            // 1.4.3 — Single op="UPDATE" call replaces the previous 4-way
            // dispatch (INSERT / DELETE / U_OLD-as-DELETE / U_NEW-as-INSERT).
            // The TEMP VIEWs created above (`__reflex_new_<src>` =
            // I + U_NEW, `__reflex_old_<src>` = D + U_OLD) act exactly like
            // the IMMEDIATE-mode transition tables, so `reflex_build_delta_sql`
            // routes through the normal UPDATE path and `build_net_delta_query`
            // fuses both halves into a single JOIN-scan instead of running
            // sub + add as two independent scans. Cuts per-flush JOIN cost ~2×
            // for real updates and exercises a single, well-tested code path.
            let upd_sql = reflex_build_delta_sql(
                imv_name,
                source_table,
                "UPDATE",
                base_query,
                end_query,
                Some(agg_json.as_str()),
                base_query,
            );
            let mut had_stmts = false;
            if !upd_sql.is_empty() {
                for stmt in upd_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        imv_stmts.push(stmt.to_string());
                        had_stmts = true;
                    }
                }
            }

            // Phase 3.4 — wrap per-IMV statements in a PL/pgSQL DO block. The
            // BEGIN…EXCEPTION…END creates an internal subtransaction: a single
            // bad IMV only rolls back its own work and logs a WARNING instead of
            // aborting the entire flush cascade.
            //
            // Theme 4 (observability): inside the same savepoint, record flush
            // timing + staged row count + clear last_error on success; on
            // failure the EXCEPTION branch captures SQLERRM into last_error.
            let body = imv_stmts
                .into_iter()
                .map(|s| format!("{};", s))
                .collect::<Vec<_>>()
                .join("\n");
            // 1.3.0 observability:
            //   * `flush_ms_history` ring buffer (size 64) collects recent flush
            //     wall times. `reflex_ivm_histogram(name)` reads it.
            //   * `application_name` is set to `reflex_flush:<view>` for the
            //     duration of this IMV's body so `pg_stat_statements` /
            //     `log_line_prefix` can correlate query rows back to the IMV.
            let do_block = format!(
                "DO $_reflex_imv_sp$ \
                 DECLARE _t0 TIMESTAMP := clock_timestamp(); \
                         _rows BIGINT; \
                         _ms BIGINT; \
                         _prev_app TEXT := current_setting('application_name', true); \
                 BEGIN \
                   PERFORM set_config('application_name', 'reflex_flush:{imv_name_esc}', true); \
                   SELECT COUNT(*) INTO _rows FROM {delta_tbl}; \
                   \n{body}\n \
                   _ms := (EXTRACT(EPOCH FROM (clock_timestamp() - _t0)) * 1000)::BIGINT; \
                   UPDATE public.__reflex_ivm_reference \
                     SET last_flush_ms = _ms, \
                         last_flush_rows = _rows, \
                         flush_count = COALESCE(flush_count, 0) + 1, \
                         last_error = NULL, \
                         flush_ms_history = (\
                             COALESCE(flush_ms_history, ARRAY[]::BIGINT[]) || _ms\
                         )[GREATEST(1, COALESCE(cardinality(flush_ms_history), 0) + 1 - 63):] \
                     WHERE name = '{imv_name_esc}'; \
                   PERFORM set_config('application_name', COALESCE(_prev_app, ''), true); \
                 EXCEPTION WHEN OTHERS THEN \
                   PERFORM set_config('application_name', COALESCE(_prev_app, ''), true); \
                   RAISE WARNING 'pg_reflex: IMV % flush failed at cascade: % (SQLSTATE %)', \
                     '{imv_name_esc}', SQLERRM, SQLSTATE; \
                   UPDATE public.__reflex_ivm_reference \
                     SET last_error = LEFT(SQLERRM || ' (SQLSTATE ' || SQLSTATE || ')', 500), \
                         flush_count = COALESCE(flush_count, 0) + 1 \
                     WHERE name = '{imv_name_esc}'; \
                 END $_reflex_imv_sp$",
                delta_tbl = delta_tbl,
                body = body,
                imv_name_esc = imv_name.replace("'", "''"),
            );
            client.update(&do_block, None, &[]).unwrap_or_report();

            if had_stmts {
                total_processed += 1;
            }
        }

        // 1.4.3 — DELETE (not TRUNCATE) for staging cleanup.
        //
        // TRUNCATE requires AccessExclusiveLock on the staging delta and
        // deadlocks against any concurrent session that holds a RowExclusive
        // on the same staging table from its earlier statement-level INSERT
        // and is now blocked at the COMMIT-time advisory lock. DELETE only
        // takes RowExclusive (no conflict at the table level), and MVCC
        // ensures we only remove rows visible to this transaction — i.e.
        // exactly the staged rows this flush just processed. Other
        // sessions' uncommitted staged rows remain for their own flush.
        //
        // The terminal DROP VIEW IF EXISTS calls are gone: the temp views
        // were redefined with CREATE OR REPLACE TEMP VIEW above and are
        // safe to leave for the next flush in the same session.
        client
            .update(&format!("DELETE FROM {}", delta_tbl), None, &[])
            .unwrap_or_report();
        client
            .update(
                &format!(
                    "DELETE FROM public.__reflex_deferred_pending WHERE source_table = '{}'",
                    source_table.replace("'", "''")
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    });

    format!("FLUSHED {} DEFERRED OPERATIONS", total_processed)
}

#[cfg(test)]
#[path = "tests/unit_trigger.rs"]
mod tests;
