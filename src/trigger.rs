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
    quote_identifier, replace_identifier, shrunk_groups_table_name, split_qualified_name,
    staging_delta_table_name, strip_redundant_bare_alias, transition_new_table_name,
    transition_old_table_name,
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
                // COALESCE handles NULL in delta (e.g., SUM(NULL)=NULL but we need 0).
                // Use type-appropriate default: 0 for numeric, FALSE for boolean.
                let default_val = if ic.pg_type == "BOOLEAN" {
                    "FALSE"
                } else {
                    "0"
                };
                set_clauses.push(format!(
                    "\"{}\" = COALESCE(t.\"{}\", {}) {} COALESCE(d.\"{}\", {})",
                    ic.name, ic.name, default_val, operator, ic.name, default_val
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

/// Replace a source table reference in a base_query with a transition table name.
/// Handles both schema-qualified names (e.g., `alp.sales_simulation` in FROM)
/// and bare table names used as column qualifiers (e.g., `sales_simulation.product_id`).
fn replace_source_with_transition(
    base_query: &str,
    source_table: &str,
    transition_tbl: &str,
) -> String {
    // `transition_tbl` is either a bare safe-identifier (real transition table
    // alias like `__reflex_new_<src>`) or an already-quoted/qualified ref like
    // `"schema"."__reflex_pt_new_v_s"`. Quote bare names; pass refs through.
    let quoted_tbl = if transition_tbl.contains('"') {
        transition_tbl.to_string()
    } else {
        format!("\"{}\"", transition_tbl)
    };
    // Pre-pass: strip a redundant `AS <bare>` alias when the user aliased the
    // schema-qualified source with its bare name. Without this, step 2 below
    // would rewrite `<bare>.col` qualifiers to `<transition_tbl>.col` — but PG
    // treats the explicit alias as hiding the underlying table's own name, so
    // those rewritten qualifiers would fail to resolve.
    let stripped = strip_redundant_bare_alias(base_query, source_table);
    // Use word-boundary-aware replacement to avoid corrupting column names
    // that contain the source table name as a substring (e.g., __bool_or_flag
    // contains "bo" when the source table is "bo").
    let replaced = replace_identifier(&stripped, source_table, &quoted_tbl);
    // Also replace unqualified table name in column qualifiers
    let (_, bare_source) = split_qualified_name(source_table);
    if bare_source != source_table {
        // Only needed when source_table was schema-qualified
        replace_identifier(&replaced, bare_source, &quoted_tbl)
    } else {
        replaced
    }
}

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

/// 1.4.5 — high-selectivity dispatch threshold. When the number of affected
/// groups exceeds this fraction of the intermediate's row count, the trigger
/// switches from MERGE-based incremental to TRUNCATE+rebuild ("full refresh
/// of the IMV body").
///
/// Rationale: at high selectivity, the per-row MERGE probe cost + the target
/// DELETE/INSERT round-trip exceed the cost of a bulk INSERT into a freshly
/// truncated table. The customer's 64 %-selectivity UPDATE on
/// yse.ivm_sop_forecast_view spent 5.5 s on MERGE + 4.1 s on target ops vs
/// 2-3 s for REFRESH MATERIALIZED VIEW on the same shape.
///
/// 0.3 is a conservative default (rebuilding at 30 % affected is breakeven
/// or slightly worse than MERGE on small-target IMVs; clearly better on
/// large ones). Operators can override per-IMV via the
/// `reflex.wipe_threshold` GUC at session scope.
const WIPE_THRESHOLD_DEFAULT: f64 = 0.3;

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
            "        EXECUTE $reflex_inner${}$reflex_inner$;\n",
            s.replace("$reflex_inner$", "$reflex_inner_alt$")
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
             _ratio NUMERIC;\n\
         BEGIN\n\
             SELECT count(*) INTO _aff FROM {affected};\n\
             SELECT GREATEST(reltuples::NUMERIC, 1.0) INTO _imm\n\
                 FROM pg_class WHERE oid = '{intermediate}'::regclass;\n\
             _thr := COALESCE(current_setting('reflex.wipe_threshold', true)::NUMERIC, {default_thr});\n\
             _ratio := _aff::NUMERIC / _imm;\n\
             IF _ratio >= _thr THEN\n\
                 -- 1.4.5: high-selectivity path — delegate to reflex_reconcile,\n\
                 -- which implements the optimized drop-index/bulk-INSERT/recreate-\n\
                 -- index pattern. At >= {default_thr} selectivity the cost of the\n\
                 -- standard MERGE + target double-rewrite exceeds the cost of a\n\
                 -- full IMV rebuild; reconcile is REFRESH-MATERIALIZED-VIEW-shape\n\
                 -- and minimizes per-row WAL/index overhead.\n\
                 RAISE DEBUG 'pg_reflex wipe: ratio=% thr=% — reconcile', _ratio, _thr;\n\
                 PERFORM public.reflex_reconcile('{view}');\n\
             ELSE\n\
                 RAISE DEBUG 'pg_reflex wipe: ratio=% thr=% — incremental', _ratio, _thr;\n\
                 EXECUTE $reflex_inner${merge}$reflex_inner$;\n\
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
    // Scratch is the result of GROUP BY in build_merge_from_table_sql's delta,
    // so it already contains one row per group key. DISTINCT here would add a
    // redundant hash/sort pass for the same output.
    stmts.push(format!(
        "INSERT INTO {} SELECT {} FROM {} AS __d",
        affected_tbl, select_expr, scratch_tbl
    ));
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

    let json = aggregations_json.unwrap_or("{}");
    let plan: AggregationPlan = match serde_json::from_str(json) {
        Ok(p) => p,
        Err(_) => {
            pgrx::warning!("pg_reflex: invalid aggregations JSON for '{}'", view_name);
            return String::new();
        }
    };

    let intermediate_tbl = intermediate_table_name(view_name);
    // Use the transition table names directly (no temp table copy needed).
    // Transition tables are visible in plpgsql EXECUTE context.
    let new_tbl = transition_new_table_name(source_table);
    let old_tbl = transition_old_table_name(source_table);

    let mut stmts: Vec<String> = Vec::new();

    // 1.4.5: when set, the cleanup/target-sync block at the end emits the
    // high-selectivity dispatch DO block (TRUNCATE+rebuild OR MERGE+cleanup)
    // instead of the standard MERGE-then-cleanup+target-sync sequence.
    struct PendingDispatch {
        merge_sql: String,
    }
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
    let src_upper = source_table.to_uppercase();
    let bare_upper = bare_source.to_uppercase();
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
                    let next_token = after.split_whitespace().next().unwrap_or("");
                    if next_token == src_upper || next_token == bare_upper {
                        return true;
                    }
                    search_from += pos + pat.len();
                }
                false
            })
        };
    // For LEFT/RIGHT JOIN: only the secondary table's DELETE/UPDATE needs special handling.
    // For FULL OUTER JOIN: ALL operations on BOTH tables need targeted reconcile,
    // because the FULL JOIN delta always includes unmatched rows from the other side.
    let is_outer_join_secondary = (is_outer_join_secondary_table
        && (operation == "DELETE" || operation == "UPDATE"))
        || (is_full_outer && !is_self_join);

    if is_self_join {
        // Self-join: full refresh (delta itself is wrong — both aliases get replaced).
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
    } else if is_outer_join_secondary && plan.is_passthrough {
        // Passthrough outer-join secondary: full refresh from source
        let qv = quote_identifier(view_name);
        stmts.push(format!("DELETE FROM {}", qv));
        stmts.push(format!("INSERT INTO {} {}", qv, base_query));
    } else if is_outer_join_secondary && !plan.is_passthrough {
        // LEFT/RIGHT JOIN secondary table DELETE/UPDATE: targeted group reconcile.
        // The delta correctly identifies WHICH groups changed (affected groups),
        // but the MERGE subtract produces wrong values (can't represent NULL from LEFT JOIN).
        // Fix: extract affected groups from delta, delete them from intermediate,
        // re-insert ONLY those groups from the full base_query.
        if let Some(ref cols) = grp_cols {
            let select_expr = affected_groups_select(cols);
            let qv = quote_identifier(view_name);

            // Determine transition table for affected group extraction
            let transition = if operation == "DELETE" {
                &old_tbl
            } else {
                &new_tbl
            };
            // Build a delta query to extract group keys from transition table
            let delta_q = replace_source_with_transition(base_query, source_table, transition);

            // Create affected groups table
            stmts.push(format!("TRUNCATE {}", affected_tbl));

            // Extract affected groups from delta
            stmts.push(format!(
                "INSERT INTO {} SELECT DISTINCT {} FROM ({}) AS __d",
                affected_tbl, select_expr, delta_q
            ));

            // Delete affected groups from intermediate. Outer is the
            // intermediate (its column names match `cols`).
            let ns_in_int = null_safe_in(
                &affected_tbl,
                &intermediate_tbl,
                cols,
                cols,
                &plan.not_null_columns,
            );
            stmts.push(format!(
                "DELETE FROM {} WHERE {}",
                intermediate_tbl, ns_in_int
            ));

            // Re-insert ONLY affected groups from the FULL base_query (reads
            // real source). Outer is the `__full` subquery alias — its
            // projection columns inherit the intermediate naming (because
            // `base_query`'s SELECT aliases the GROUP BY cols to the
            // intermediate-side names).
            let ns_in_full =
                null_safe_in(&affected_tbl, "__full", cols, cols, &plan.not_null_columns);
            stmts.push(format!(
                "INSERT INTO {} SELECT * FROM ({}) AS __full WHERE {}",
                intermediate_tbl, base_query, ns_in_full
            ));

            // Targeted refresh of target. Outer for the DELETE is the target
            // table — its column names may differ from `cols` (the
            // intermediate / affected naming) when the user aliases GROUP BY
            // cols in their SELECT (e.g., `dp.id AS dem_plan_id`).
            let target_cols = target_group_columns(&plan);
            let ns_in_tgt_delete = null_safe_in(
                &affected_tbl,
                &qv,
                &target_cols,
                cols,
                &plan.not_null_columns,
            );
            stmts.push(format!("DELETE FROM {} WHERE {}", qv, ns_in_tgt_delete));
            // For the INSERT INTO target via `end_query`, the appended WHERE
            // executes inside `end_query`'s scope. `end_query`'s FROM is the
            // intermediate, so outer is intermediate-named (cols match cols).
            let ns_in_tgt_insert = null_safe_in(
                &affected_tbl,
                &intermediate_tbl,
                cols,
                cols,
                &plan.not_null_columns,
            );
            stmts.push(format!(
                "INSERT INTO {} {} AND {}",
                qv, end_query, ns_in_tgt_insert
            ));
        } else {
            // No group columns: full refresh
            stmts.push(format!("TRUNCATE {}", intermediate_tbl));
            stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
            stmts.push(format!("TRUNCATE {}", quote_identifier(view_name)));
            if end_query.is_empty() {
                stmts.push(format!(
                    "INSERT INTO {} {}",
                    quote_identifier(view_name),
                    base_query
                ));
            } else {
                stmts.push(format!(
                    "INSERT INTO {} {}",
                    quote_identifier(view_name),
                    end_query
                ));
            }
        }
    } else if plan.is_passthrough {
        let qv = quote_identifier(view_name);
        let pt_new = passthrough_scratch_new_table_name(view_name, source_table);
        let pt_old = passthrough_scratch_old_table_name(view_name, source_table);
        // Look up per-source column mappings for targeted DELETE/UPDATE
        let mappings = plan.passthrough_key_mappings.get(source_table);

        // Materialize the transition tables into per-(IMV, source) UNLOGGED scratch
        // tables BEFORE any downstream DML references them. This is the key fix for
        // the nested-trigger SIGABRT: subquery reads of transition tables inside
        // EXECUTE'd DML (DELETE … WHERE IN (SELECT … FROM transition), INSERT …
        // SELECT … FROM transition) trip a PG assertion when fired from a
        // downstream trigger. Plain `INSERT INTO scratch SELECT * FROM transition`
        // is the one pattern that stays safe — so we confine every transition
        // reference to that pattern and route subsequent statements through the
        // scratch tables.
        let needs_new = matches!(operation, "INSERT" | "UPDATE");
        let needs_old = matches!(operation, "DELETE" | "UPDATE");
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
            "INSERT" => {
                let delta_q = replace_source_with_transition(base_query, source_table, &pt_new);
                stmts.push(format!("INSERT INTO {} {}", qv, delta_q));
            }
            "DELETE" => {
                if let Some(mappings) = mappings {
                    // Targeted delete using per-source column mapping
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
                } else {
                    // No mapping for this source: full refresh
                    stmts.push(format!("DELETE FROM {}", qv));
                    stmts.push(format!("INSERT INTO {} {}", qv, base_query));
                }
            }
            "UPDATE" => {
                if let Some(mappings) = mappings {
                    // Phase 1: delete old rows using per-source column mapping
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
                    // Phase 2: insert new rows (base_query with source→pt_new scratch)
                    let delta_new =
                        replace_source_with_transition(base_query, source_table, &pt_new);
                    stmts.push(format!("INSERT INTO {} {}", qv, delta_new));
                } else {
                    // No mapping for this source: full refresh
                    stmts.push(format!("DELETE FROM {}", qv));
                    stmts.push(format!("INSERT INTO {} {}", qv, base_query));
                }
            }
            _ => {}
        }
    } else {
        let has_min_max = plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX");

        match operation {
            "INSERT" => {
                let delta_q = replace_source_with_transition(base_query, source_table, &new_tbl);

                if let Some(ref cols) = grp_cols {
                    let select_expr = affected_groups_select(cols);
                    push_materialized_merge_and_affected(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_q,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Add,
                        &affected_tbl,
                        &select_expr,
                        true,
                    );
                } else {
                    push_materialized_merge(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_q,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Add,
                    );
                }
            }
            "DELETE" => {
                let delta_q = replace_source_with_transition(base_query, source_table, &old_tbl);

                let recompute_scope: Option<&str> = if let Some(ref cols) = grp_cols {
                    let select_expr = affected_groups_select(cols);
                    push_materialized_merge_and_affected(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_q,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Subtract,
                        &affected_tbl,
                        &select_expr,
                        true,
                    );
                    Some(affected_tbl.as_str())
                } else {
                    push_materialized_merge(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_q,
                        &intermediate_tbl,
                        &plan,
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
                        build_topk_scalar_refresh_sql(&intermediate_tbl, &plan, recompute_scope)
                    {
                        stmts.push(refresh);
                    }
                    if let Some(recompute) = build_min_max_recompute_sql(
                        &intermediate_tbl,
                        &plan,
                        orig_base_query,
                        recompute_scope,
                    ) {
                        stmts.push(recompute);
                    }
                }
            }
            "UPDATE" => {
                let delta_old = replace_source_with_transition(base_query, source_table, &old_tbl);
                let delta_new = replace_source_with_transition(base_query, source_table, &new_tbl);

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
                            &mut stmts,
                            &scratch_tbl,
                            &delta_old,
                            &intermediate_tbl,
                            &plan,
                            DeltaOp::Subtract,
                            &affected_tbl,
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
                                &mut stmts,
                                &intermediate_tbl,
                                &plan,
                                &affected_tbl,
                                &shrunk_tbl,
                            ) {
                            shrunk_tbl.as_str()
                        } else {
                            affected_tbl.as_str()
                        };
                        if let Some(refresh) = build_topk_scalar_refresh_sql(
                            &intermediate_tbl,
                            &plan,
                            Some(affected_tbl.as_str()),
                        ) {
                            stmts.push(refresh);
                        }
                        if !has_topk {
                            // Non-top-K: recompute BEFORE Add to avoid LEAST(NULL, d).
                            if let Some(recompute) = build_min_max_recompute_sql(
                                &intermediate_tbl,
                                &plan,
                                orig_base_query,
                                Some(affected_tbl.as_str()),
                            ) {
                                stmts.push(recompute);
                            }
                        }
                        push_materialized_merge_and_affected(
                            &mut stmts,
                            &scratch_tbl,
                            &delta_new,
                            &intermediate_tbl,
                            &plan,
                            DeltaOp::Add,
                            &affected_tbl,
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
                                &intermediate_tbl,
                                &plan,
                                orig_base_query,
                                Some(recompute_scope),
                            ) {
                                stmts.push(recompute);
                            }
                        }
                    } else {
                        push_materialized_merge(
                            &mut stmts,
                            &scratch_tbl,
                            &delta_old,
                            &intermediate_tbl,
                            &plan,
                            DeltaOp::Subtract,
                        );
                        if let Some(refresh) =
                            build_topk_scalar_refresh_sql(&intermediate_tbl, &plan, None)
                        {
                            stmts.push(refresh);
                        }
                        if !has_topk {
                            if let Some(recompute) = build_min_max_recompute_sql(
                                &intermediate_tbl,
                                &plan,
                                orig_base_query,
                                None,
                            ) {
                                stmts.push(recompute);
                            }
                        }
                        push_materialized_merge(
                            &mut stmts,
                            &scratch_tbl,
                            &delta_new,
                            &intermediate_tbl,
                            &plan,
                            DeltaOp::Add,
                        );
                        if has_topk {
                            if let Some(recompute) = build_min_max_recompute_sql_force_topk(
                                &intermediate_tbl,
                                &plan,
                                orig_base_query,
                                None,
                            ) {
                                stmts.push(recompute);
                            }
                        }
                    }
                } else if grp_cols.is_some() {
                    let cols = grp_cols.as_ref().expect("grp_cols is Some — checked above");
                    let net_delta = build_net_delta_query(&delta_old, &delta_new, &plan);
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
                    // Capture the MERGE SQL — the dispatch block emits it
                    // (instead of running it unconditionally).
                    let merge_sql_for_dispatch = build_merge_from_table_sql(
                        &intermediate_tbl,
                        &scratch_tbl,
                        &plan,
                        DeltaOp::Add,
                    );
                    pending_dispatch = Some(PendingDispatch {
                        merge_sql: merge_sql_for_dispatch,
                    });
                } else {
                    push_materialized_merge(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_old,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Subtract,
                    );
                    push_materialized_merge(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_new,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Add,
                    );
                }
            }
            _ => {}
        }

        // Refresh target from intermediate, clean up dead groups, and update metadata.
        //
        // Emitted as separate statements (not a single CTE chain): sibling CTEs
        // in Postgres share a snapshot, so an `INSERT` sibling cannot observe
        // a `DELETE` sibling — when the target has a unique index on the group
        // key, re-inserting the refreshed row hits a duplicate-key error.
        let end_query_has_group_by = end_query.to_uppercase().contains("GROUP BY");
        let include_dead_cleanup = plan.needs_ivm_count
            && grp_cols.is_some()
            && (operation == "DELETE" || operation == "UPDATE");
        let metadata_sql = format!(
            "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() \
             WHERE name = '{}' AND (last_update_date IS NULL OR last_update_date < NOW() - INTERVAL '1 second')",
            view_name.replace("'", "''")
        );

        if end_query_has_group_by {
            let qv = quote_identifier(view_name);
            if plan.group_by_columns.is_empty() {
                // Global COUNT(DISTINCT) with no output GROUP BY — single output row, full rebuild.
                let tdel = format!("DELETE FROM {}", qv);
                let tins = format!("INSERT INTO {} {}", qv, end_query);
                if let Some(pd) = pending_dispatch.take() {
                    stmts.push(build_high_selectivity_dispatch_sql(
                        view_name,
                        &intermediate_tbl,
                        &affected_tbl,
                        &pd.merge_sql,
                        None,
                        &tdel,
                        &tins,
                    ));
                } else {
                    stmts.push(tdel);
                    stmts.push(tins);
                }
            } else {
                // Intermediate-side column names (== affected column names).
                let output_cols: Vec<String> = plan
                    .group_by_columns
                    .iter()
                    .map(|c| format!("\"{}\"", normalized_column_name(c)))
                    .collect();
                let target_cols: Vec<String> = target_group_columns(&plan)
                    .into_iter()
                    .take(plan.group_by_columns.len())
                    .collect();
                match inject_affected_filter_before_group_by(
                    end_query,
                    &output_cols,
                    &affected_tbl,
                    &intermediate_tbl,
                    &plan.not_null_columns,
                ) {
                    Some(spliced_end_q) => {
                        let ns_in_target = null_safe_in(
                            &affected_tbl,
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
                                &intermediate_tbl,
                                &affected_tbl,
                                &pd.merge_sql,
                                None,
                                &tdel,
                                &tins,
                            ));
                        } else {
                            stmts.push(tdel);
                            stmts.push(tins);
                        }
                    }
                    None => {
                        // No GROUP BY found — defensive fallback to full rebuild.
                        let tdel = format!("DELETE FROM {}", qv);
                        let tins = format!("INSERT INTO {} {}", qv, end_query);
                        if let Some(pd) = pending_dispatch.take() {
                            stmts.push(build_high_selectivity_dispatch_sql(
                                view_name,
                                &intermediate_tbl,
                                &affected_tbl,
                                &pd.merge_sql,
                                None,
                                &tdel,
                                &tins,
                            ));
                        } else {
                            stmts.push(tdel);
                            stmts.push(tins);
                        }
                    }
                }
            }
            stmts.push(metadata_sql);
        } else if let Some(ref cols) = grp_cols {
            let qv = quote_identifier(view_name);
            let target_cols = target_group_columns(&plan);
            let ns_in_intermediate = null_safe_in(
                &affected_tbl,
                &intermediate_tbl,
                cols,
                cols,
                &plan.not_null_columns,
            );
            let ns_in_target_delete = null_safe_in(
                &affected_tbl,
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
                // 1.4.5: emit the high-selectivity dispatch DO block — at
                // runtime, decide between MERGE-incremental and
                // TRUNCATE+rebuild based on |affected| / |intermediate|.
                // The TRUNCATE+rebuild branch rebuilds intermediate from
                // base_query and target from end_query (full refresh of the
                // IMV body), bypassing the per-row MERGE probe cost and the
                // target double-rewrite that dominate at high selectivity.
                stmts.push(build_high_selectivity_dispatch_sql(
                    view_name,
                    &intermediate_tbl,
                    &affected_tbl,
                    &pd.merge_sql,
                    dead_cleanup_sql.as_deref(),
                    &target_delete_sql,
                    &target_insert_sql,
                ));
            } else {
                // Standard incremental path (no dispatch): MERGE already
                // pushed by a non-dispatch branch (e.g. outer-join-secondary
                // or top-K), now push the cleanup + target sync.
                if let Some(s) = dead_cleanup_sql {
                    stmts.push(s);
                }
                stmts.push(target_delete_sql);
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
        let src_cols: Vec<String> = client
            .select(
                "SELECT quote_ident(column_name) AS qc \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                 ORDER BY ordinal_position",
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
                row.get_by_name::<&str, _>("qc")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
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
        let cols_csv = src_cols.join(", ");
        let is_spurious = if cols_csv.is_empty() {
            false
        } else {
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

        for (imv_name, base_query, end_query, agg_json, where_pred) in &imvs {
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
                    let cols_csv = cols
                        .iter()
                        .filter_map(|v| v.as_str())
                        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
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
