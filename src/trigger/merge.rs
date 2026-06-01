use super::*;
use crate::aggregation::AggregationPlan;
use crate::query_decomposer::normalized_column_name;

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
pub(crate) fn build_merge_from_table_sql(
    intermediate_tbl: &str,
    scratch_tbl: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) -> String {
    // `scratch_tbl` is pre-qualified (`"schema"."local"` or bare local) by
    // `delta_scratch_table_name`, so no extra quoting here.
    build_merge_using(intermediate_tbl, scratch_tbl, plan, op)
}

pub(crate) fn build_merge_using(
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
pub(crate) fn build_net_delta_query(
    delta_old: &str,
    delta_new: &str,
    plan: &AggregationPlan,
) -> String {
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

pub(crate) fn build_min_max_recompute_sql_inner(
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
pub(crate) fn null_safe_in(
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
pub(crate) fn splice_before_group_by(query: &str, and_fragment: &str) -> Option<String> {
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
pub(crate) fn inject_affected_filter_before_group_by(
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
pub(crate) fn group_columns(plan: &AggregationPlan) -> Option<Vec<String>> {
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
pub(crate) fn target_group_columns(plan: &AggregationPlan) -> Vec<String> {
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
pub(crate) fn affected_groups_select(cols: &[String]) -> String {
    cols.join(", ")
}

/// Build a row-value expression for WHERE ... IN clauses.
/// Single column: "col"   Multi-column: ("col1", "col2")
pub(crate) fn row_expr(cols: &[String]) -> String {
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

/// Push MERGE + affected-groups population.
/// PG17+: single CTE with MERGE RETURNING (captures affected groups in one statement).
///   When `include_cleanup` is true, prepends a DELETE FROM affected CTE (replaces TRUNCATE).
/// PG15/16: separate MERGE + SELECT DISTINCT from delta query (MERGE RETURNING unsupported).
pub(crate) fn push_materialized_merge(
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
