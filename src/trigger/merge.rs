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

    // PS-5 — the join columns that are actually NULLABLE (the sentinel
    // `__reflex_group` is a constant, never NULL, so it is excluded and its
    // single-row MERGE is left byte-for-byte unchanged). When any exist, the
    // `ON` above is non-sargable (`IS NOT DISTINCT FROM`), which forces the
    // MERGE into a nested loop over the whole intermediate — 20.7 ms at 200k
    // groups / 1 delta row, and the dominant cost of the whole flush at scale.
    let nullable_join_cols: Vec<&String> = join_cols
        .iter()
        .filter(|c| {
            let unquoted = c.trim_matches('"');
            unquoted != "__reflex_group" && !plan.not_null_columns.contains(unquoted)
        })
        .collect();

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

    let one_merge = |using: &str, on: &str| -> String {
        format!(
            "MERGE INTO {} AS t USING {} AS d ON {} WHEN MATCHED THEN UPDATE SET {}{}",
            intermediate_tbl,
            using,
            on,
            set_clauses.join(", "),
            not_matched
        )
    };

    if nullable_join_cols.is_empty() {
        // All-NOT-NULL (or sentinel-only) key: `on_clause` is already sargable.
        return one_merge(using_clause, &on_clause);
    }

    // PS-5 — gate the MERGE into a sargable/NULL-safe pair. MERGE is NOT
    // idempotent, so the target-sync trick of emitting the statement twice would
    // double-apply the delta. Instead the gate lives in the USING SOURCE: the
    // untaken variant's source yields zero rows, so neither WHEN MATCHED nor WHEN
    // NOT MATCHED can fire. Verified for real (not just EXPLAIN): NULL-free
    // scratch → `MERGE 1`/`MERGE 0`, NULL-keyed scratch → `MERGE 0`/`MERGE 1`,
    // each applying the delta exactly once. See the ATOMICITY INVARIANT on
    // `AffectedMatch` — the flush's per-source scratch is rebuilt (TRUNCATE +
    // INSERT) under lock within the same txn, so no writer can change its
    // NULL-ness between the two MERGEs.
    //
    // The gate probes the SOURCE `d` (the scratch/delta), not `__reflex_affected`:
    // for a source row with a non-NULL key, `t.k IS NOT DISTINCT FROM d.k` and
    // `t.k = d.k` select the same target rows, so `=` is valid exactly when the
    // source has no NULL key.
    let gate_disjunction = nullable_join_cols
        .iter()
        .map(|c| format!("__ng.{} IS NULL", c))
        .collect::<Vec<_>>()
        .join(" OR ");
    let gate = format!(
        "EXISTS (SELECT 1 FROM {} AS __ng WHERE {})",
        using_clause, gate_disjunction
    );
    let fast_on = join_cols
        .iter()
        .map(|c| format!("t.{} = d.{}", c, c))
        .collect::<Vec<_>>()
        .join(" AND ");
    let fast_using = format!(
        "(SELECT __m.* FROM {} AS __m WHERE NOT {})",
        using_clause, gate
    );
    let safe_using = format!("(SELECT __m.* FROM {} AS __m WHERE {})", using_clause, gate);

    format!(
        "{}\n--<<REFLEX_SEP>>--\n{}",
        one_merge(&fast_using, &fast_on),
        one_merge(&safe_using, &on_clause)
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
/// IMVs unusable in practice. The wrapper splices, before the GROUP BY,
/// `AND EXISTS (SELECT 1 FROM (SELECT <gb_cols> AS __gN FROM "<affected_tbl>") __ng
/// WHERE <raw gb_col> IS NOT DISTINCT FROM __ng.__gN AND …)` — a NULL-safe
/// membership test (a NULL-unsafe `IN` dropped NULL group keys), which pushes the
/// group-key filter down through the aggregation boundary.
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

    let update_with_scope = |scope: &str| -> String {
        format!(
            "UPDATE {tbl} SET {sets} WHERE ({heap}){scope}",
            tbl = intermediate_tbl,
            sets = set_parts.join(", "),
            heap = heap_pred,
            scope = scope,
        )
    };

    // Scope to affected groups. `at` is a fully-formed identifier ref
    // (qualified+quoted or bare local). The scope must be NULL-safe: a plain
    // `(cols) IN (SELECT ...)` never matches a NULL group key — `(NULL) IN (...)`
    // is NULL, not TRUE — so the NULL group was skipped and its scalar left NULL
    // by the preceding Sub was never refreshed from `topk[1]`. On the top-K UPDATE
    // path the subsequent Add then computes LEAST/GREATEST(NULL, delta) = delta
    // (wrong) and the forced recompute only covers groups whose heap shrank, so an
    // unshrunk NULL group stayed wrong (same NULL-group family as the recompute).
    //
    // Correctness needs `IS NOT DISTINCT FROM`, but that is non-sargable: it cannot
    // use the intermediate's unique group-key index, so it seq-scans the whole
    // intermediate on every top-K Sub (verified with EXPLAIN). This UPDATE fires on
    // every top-K Sub, so — unlike the rare recompute — the common path must stay
    // sargable. Reuse PS-5's `affected_null_key_gate`: when the affected set holds
    // no NULL group key, `=` (index scan) is exact; only when it actually holds a
    // NULL key do we pay the NULL-safe scan, and just for that batch.
    let scope_exists = |at: &str, eq: bool| -> String {
        let conds = group_cols
            .iter()
            .map(|c| {
                if eq {
                    format!(
                        "{tbl}.\"{c}\" = __ng.\"{c}\"",
                        tbl = intermediate_tbl,
                        c = c
                    )
                } else {
                    format!(
                        "{tbl}.\"{c}\" IS NOT DISTINCT FROM __ng.\"{c}\"",
                        tbl = intermediate_tbl,
                        c = c
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        format!(" AND EXISTS (SELECT 1 FROM {at} __ng WHERE {conds})")
    };

    match (affected_tbl, !group_cols.is_empty()) {
        (Some(at), true) => {
            let quoted: Vec<String> = group_cols.iter().map(|c| format!("\"{}\"", c)).collect();
            match affected_null_key_gate(at, &quoted, &plan.not_null_columns) {
                None => Some(update_with_scope(&scope_exists(at, true))),
                Some(gate) => {
                    let fast = format!(
                        "DO $_reflex_topk_refresh$ BEGIN IF NOT {gate} THEN {upd}; END IF; \
                         END $_reflex_topk_refresh$",
                        gate = gate,
                        upd = update_with_scope(&scope_exists(at, true)),
                    );
                    let safe = format!(
                        "DO $_reflex_topk_refresh$ BEGIN IF {gate} THEN {upd}; END IF; \
                         END $_reflex_topk_refresh$",
                        gate = gate,
                        upd = update_with_scope(&scope_exists(at, false)),
                    );
                    Some(format!("{}\n--<<REFLEX_SEP>>--\n{}", fast, safe))
                }
            }
        }
        _ => Some(update_with_scope("")),
    }
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

    // PS-5 — the `i JOIN affected a ON i.gc IS NOT DISTINCT FROM a.gc` join is the
    // same non-sargable nested-loop-over-the-whole-intermediate pattern as the
    // target sync. Gate it into a sargable/NULL-safe pair on whether the affected
    // set contains a NULL key (the group cols here are the affected table's own
    // column names).
    let quoted_group_cols: Vec<String> = group_cols.iter().map(|c| format!("\"{}\"", c)).collect();
    let join_on = |eq: bool| -> String {
        group_cols
            .iter()
            .map(|gc| {
                if eq {
                    format!("i.\"{gc}\" = a.\"{gc}\"", gc = gc)
                } else {
                    format!("i.\"{gc}\" IS NOT DISTINCT FROM a.\"{gc}\"", gc = gc)
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let insert_with = |join_cond: &str, extra_gate: &str| -> String {
        format!(
            "INSERT INTO {shrunk_tbl} SELECT DISTINCT {proj} \
             FROM {intermediate_tbl} i JOIN {affected_tbl} a ON {join_cond} \
             WHERE ({where_clause}){extra_gate}",
            shrunk_tbl = shrunk_tbl,
            proj = proj,
            intermediate_tbl = intermediate_tbl,
            affected_tbl = affected_tbl,
            join_cond = join_cond,
            where_clause = where_clause,
            extra_gate = extra_gate,
        )
    };

    stmts.push(format!("TRUNCATE {}", shrunk_tbl));
    match affected_null_key_gate(affected_tbl, &quoted_group_cols, &plan.not_null_columns) {
        None => {
            // All group keys NOT NULL: `=` is sargable and semantically exact.
            stmts.push(insert_with(&join_on(true), ""));
        }
        Some(gate) => {
            stmts.push(insert_with(&join_on(true), &format!(" AND NOT {}", gate)));
            stmts.push(insert_with(&join_on(false), &format!(" AND {}", gate)));
        }
    }
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
            // NULL-safe affected-group scoping. `(cols) IN (SELECT ...)` never
            // matches a NULL group key — `(NULL) IN (...)` is NULL, not TRUE — so a
            // NULL group was dropped from the scoped source and a MIN/MAX left NULL
            // by a retraction was never re-derived (silent wrong result). Use a
            // correlated EXISTS with per-column `IS NOT DISTINCT FROM`, pairing each
            // raw GROUP BY expression (LHS, evaluated in the outer source scope) to
            // its normalized column in the affected table (RHS) — the same
            // raw/normalized pairing the IN form used.
            //
            // The affected columns are aliased through a derived table (`__g0`,
            // `__g1`, …) so the correlated subquery exposes NO column named like a
            // raw source column: an unqualified raw key (e.g. bare `grp`) then
            // resolves outward to the source row, not inward to the affected table.
            // Without the alias, a bare raw key equal to an affected column name
            // would bind to the affected column, the predicate would be trivially
            // TRUE, and the scoping would silently collapse to a full source scan.
            let pairs: Vec<(&String, &String)> = plan
                .group_by_columns
                .iter()
                .zip(group_cols.iter())
                .collect();
            let projection = pairs
                .iter()
                .enumerate()
                .map(|(i, (_, norm))| format!("\"{}\" AS __g{}", norm, i))
                .collect::<Vec<_>>()
                .join(", ");
            let conds = pairs
                .iter()
                .enumerate()
                .map(|(i, (raw, _))| format!("({}) IS NOT DISTINCT FROM __ng.__g{}", raw, i))
                .collect::<Vec<_>>()
                .join(" AND ");
            let filter = format!(
                " AND EXISTS (SELECT 1 FROM (SELECT {} FROM {}) __ng WHERE {})",
                projection, at, conds
            );
            match splice_before_group_by(orig_base_query, &filter) {
                Some(spliced) => spliced,
                None => orig_base_query.to_string(),
            }
        }
        None => orig_base_query.to_string(),
    };

    // PS-5 — both the recompute UPDATE's join to `__src` and (below) the EXISTS
    // firing gate's join to `__aff` matched the group key with `IS NOT DISTINCT
    // FROM`, non-sargable, forcing a nested loop over the whole intermediate. The
    // firing gate runs on EVERY UPDATE flush of EVERY MIN/MAX IMV. Gate both into
    // a sargable/NULL-safe pair, keyed on whether the AFFECTED set holds a NULL
    // group key. `__aff` is the probe for both joins: a NULL key is in `__aff`
    // iff it is in `__src` (both are scoped to the affected groups), and `__aff`
    // is a cheap indexed table whereas `__src` is a re-aggregation subquery that
    // must not be evaluated twice.
    let update_join = |eq: bool| -> String {
        group_cols
            .iter()
            .map(|gc| {
                if eq {
                    format!("{}.\"{}\" = __src.\"{}\"", intermediate_tbl, gc, gc)
                } else {
                    format!(
                        "{}.\"{}\" IS NOT DISTINCT FROM __src.\"{}\"",
                        intermediate_tbl, gc, gc
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let build_update = |eq: bool| -> String {
        format!(
            "UPDATE {} SET {} FROM ({}) AS __src WHERE {} AND ({})",
            intermediate_tbl,
            set_parts.join(", "),
            scoped_source,
            update_join(eq),
            null_check.join(" OR ")
        )
    };

    let Some(at) = affected_tbl else {
        // Unscoped full recompute (no affected table to gate on). Use `=` when
        // the key is entirely NOT NULL, else the NULL-safe form.
        let all_not_null = group_cols
            .iter()
            .all(|gc| plan.not_null_columns.contains(gc.as_str()));
        return Some(build_update(all_not_null));
    };

    // 1.3.0: gate the recompute on `EXISTS (intermediate row with NULL slot
    // in an affected group)`. The post-MERGE topk-scalar refresh sets the
    // scalar from `topk[1]` for groups whose heap survived; the recompute
    // only needs to fire for groups that genuinely underflowed. An always-
    // executing UPDATE used to trigger the source aggregation even when no
    // group needed it, which dominated the bench.
    let exists_join = |eq: bool| -> String {
        group_cols
            .iter()
            .map(|gc| {
                if eq {
                    format!("{}.\"{}\" = __aff.\"{}\"", intermediate_tbl, gc, gc)
                } else {
                    format!(
                        "{}.\"{}\" IS NOT DISTINCT FROM __aff.\"{}\"",
                        intermediate_tbl, gc, gc
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let exists_check = |eq: bool| -> String {
        format!(
            "EXISTS (SELECT 1 FROM {tbl} JOIN {at} __aff ON {join} WHERE {nullc})",
            tbl = intermediate_tbl,
            at = at,
            join = exists_join(eq),
            nullc = null_check.join(" OR "),
        )
    };
    let do_block = |check: &str, upd: &str| -> String {
        format!(
            "DO $_reflex_recompute$ BEGIN IF {check} THEN {upd}; END IF; END $_reflex_recompute$",
            check = check,
            upd = upd,
        )
    };

    let quoted_group_cols: Vec<String> = group_cols.iter().map(|c| format!("\"{}\"", c)).collect();
    match affected_null_key_gate(at, &quoted_group_cols, &plan.not_null_columns) {
        None => {
            // All group keys NOT NULL: `=` is sargable and semantically exact.
            Some(do_block(&exists_check(true), &build_update(true)))
        }
        Some(gate) => {
            let fast = do_block(
                &format!("{} AND NOT {}", exists_check(true), gate),
                &build_update(true),
            );
            let safe = do_block(
                &format!("{} AND {}", exists_check(false), gate),
                &build_update(false),
            );
            Some(format!("{}\n--<<REFLEX_SEP>>--\n{}", fast, safe))
        }
    }
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

/// PS-5 — the runtime NULL-freeness probe on the affected-groups table.
///
/// Returns `EXISTS (SELECT 1 FROM <affected> AS __ng WHERE <col> IS NULL OR ...)`
/// over the NULLABLE key columns only, or `None` when every key column is known
/// NOT NULL (in which case `null_safe_in` already emits a fully sargable `=`
/// match and no specialisation is needed).
///
/// The probe is deliberately **uncorrelated** — it references only `__ng`, never
/// the outer relation or the `__a` scope. That is what makes PostgreSQL evaluate
/// it once as an `InitPlan`, mark the qual `pseudoconstant`, and hoist it into a
/// gating `Result / One-Time Filter` above the whole join, so the untaken
/// variant's subtree is reported `(never executed)`. A correlated probe would be
/// re-evaluated per outer row and buy nothing.
fn affected_null_key_gate(
    affected_tbl: &str,
    affected_cols: &[String],
    not_null_columns: &std::collections::HashSet<String>,
) -> Option<String> {
    let nullable: Vec<&String> = affected_cols
        .iter()
        .filter(|c| !not_null_columns.contains(c.trim_matches('"')))
        .collect();
    if nullable.is_empty() {
        return None;
    }
    let disjunction = nullable
        .iter()
        .map(|c| format!("__ng.{} IS NULL", c))
        .collect::<Vec<_>>()
        .join(" OR ");
    Some(format!(
        "EXISTS (SELECT 1 FROM {} AS __ng WHERE {})",
        affected_tbl, disjunction
    ))
}

/// PS-5 — mutually exclusive alternatives for the same logical affected-groups
/// match, so the common case gets an index-usable plan.
///
/// `IS NOT DISTINCT FROM` is not a member of any operator family, so it can
/// serve neither an `Index Cond` nor a hash/merge join key: the planner's only
/// option is a nested loop with a `Join Filter` over the entire target, making
/// the target sync `O(total_groups × affected_groups)`. Measured on a
/// 200 000-group nullable-key fixture (PG 17.7): 52 ms for a 1-row delta, and
/// **681 576 ms** with 50 000 affected groups (`Rows Removed by Join Filter:
/// 8 749 975 000`) against **57 ms** for the same statement with `=`.
///
/// Nullable group keys are the norm, not the exception: expression keys, any
/// column reached through a LEFT/RIGHT JOIN, and every decomposed sub-IMV target
/// (the decomposer creates those with no NOT NULL constraints at all, so a
/// decomposed parent is permanently on this path).
///
/// The specialisation is sound because, for any affected row whose key is
/// non-NULL, `t.k IS NOT DISTINCT FROM a.k` and `t.k = a.k` select exactly the
/// same rows: with `t.k` non-NULL both reduce to value equality, and with
/// `t.k IS NULL` the former is false while the latter is NULL — neither is
/// *true*, so both exclude the row. Hence if the affected table holds no NULL
/// key the two predicates are interchangeable; the gate decides that at runtime,
/// from the data itself.
///
/// This reads data only to choose between two forms that agree *on that data* —
/// it never records a conclusion that could later become false, which is how it
/// differs from the unsound NOT-NULL *inference* that
/// `project_differential_fuzz_harness_2026_05_22` caught.
///
/// An ungated `=` would be wrong: with target `{1, 2, NULL}` and affected
/// `{NULL}`, `IS NOT DISTINCT FROM` selects the NULL group and `=` selects
/// nothing, so the NULL group's stale target row would survive the DELETE and
/// never be re-inserted. Pinned by
/// `pg_test_correctness.rs::test_correctness_null_group_key_gate_branch_boundary`.
///
/// # ATOMICITY INVARIANT — do not weaken the affected table's TRUNCATE
///
/// The two gates are logical complements, but that alone is **not** sufficient:
/// each statement takes its own snapshot, so complementarity *across the pair* is
/// not a property the SQL guarantees. If the affected table's NULL-ness could
/// change between the two statements, the FAST one could see "a NULL key exists"
/// and skip, while the SAFE one sees "none exists" and also skips — **neither
/// runs, and the target is left silently stale.**
///
/// That interleaving is unreachable only because every flush path `TRUNCATE`s the
/// affected table before populating it (`ops.rs`'s `TRUNCATE {affected_tbl}` and
/// the dispatch builders' equivalents), and `TRUNCATE` holds an
/// `AccessExclusiveLock` until transaction end — on top of the per-IMV advisory
/// lock the flush already takes. No concurrent writer can touch the affected
/// table between the pair's two statements.
///
/// Refactoring that `TRUNCATE` to `DELETE FROM` (only `RowExclusiveLock`) would
/// silently void this invariant and reintroduce the stale-target window. If you
/// change how the affected table is cleared, re-derive this argument first.
pub(crate) struct AffectedMatch {
    /// Sargable `=` form, self-gated on the affected set containing no NULL key.
    /// When no key column is nullable this is ungated and the only variant.
    pub fast: String,
    /// NULL-safe form, self-gated on the affected set *having* a NULL key.
    /// `None` when no key column is nullable.
    pub safe: Option<String>,
}

impl AffectedMatch {
    /// Expand a statement template into one statement per live variant.
    ///
    /// Callers emit every returned statement unconditionally; the gates make
    /// exactly one of them do work, and PostgreSQL skips the other's plan via a
    /// `One-Time Filter` (0.013–0.029 ms measured). The gate travels inside the
    /// SQL string, so this works identically for a statement pushed directly, one
    /// run via `EXECUTE`, and one run via `EXECUTE ... USING $1` — a `DO`-block
    /// branch could not serve the last of those, since a `DO` block takes no
    /// parameters.
    pub fn stmts(&self, build: impl Fn(&str) -> String) -> Vec<String> {
        let mut out = vec![build(&self.fast)];
        if let Some(safe) = &self.safe {
            out.push(build(safe));
        }
        out
    }
}

/// PS-5 — `null_safe_in`, specialised into a gated fast/safe pair. See
/// `AffectedMatch` for why, and `null_safe_in` for the argument contract (the
/// outer-qualification invariant from `journal/2026-05-13_null_safe_in_bug.md`
/// applies unchanged — the safe variant *is* `null_safe_in`).
pub(crate) fn null_safe_in_gated(
    affected_tbl: &str,
    outer_qualifier: &str,
    outer_cols: &[String],
    affected_cols: &[String],
    not_null_columns: &std::collections::HashSet<String>,
) -> AffectedMatch {
    let safe_match = null_safe_in(
        affected_tbl,
        outer_qualifier,
        outer_cols,
        affected_cols,
        not_null_columns,
    );
    let Some(gate) = affected_null_key_gate(affected_tbl, affected_cols, not_null_columns) else {
        // Every key column is NOT NULL: `null_safe_in` already emitted `=`
        // throughout, so it is the sargable form and no gate is needed. Keeping
        // this byte-for-byte identical means existing NOT NULL IMVs see no SQL
        // change at all.
        return AffectedMatch {
            fast: safe_match,
            safe: None,
        };
    };

    // The fast variant forces `=` on EVERY key column, which is only valid under
    // the gate. Build it by treating all affected columns as NOT NULL.
    let all_not_null: std::collections::HashSet<String> = affected_cols
        .iter()
        .map(|c| c.trim_matches('"').to_string())
        .collect();
    let fast_match = null_safe_in(
        affected_tbl,
        outer_qualifier,
        outer_cols,
        affected_cols,
        &all_not_null,
    );

    AffectedMatch {
        fast: format!("{} AND NOT {}", fast_match, gate),
        safe: Some(format!("{} AND {}", safe_match, gate)),
    }
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
///
/// PS-5: production codegen now goes through
/// `inject_affected_filter_before_group_by_gated`, which returns one spliced
/// query per gated variant. This ungated form is retained because its tests pin
/// the splice contract (marker choice, `AND` placement, outer qualification)
/// that the gated version builds on.
#[cfg_attr(not(test), allow(dead_code))]
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

/// PS-5 — gated sibling of `inject_affected_filter_before_group_by`: returns one
/// spliced `end_query` per live variant of the affected-groups match.
///
/// The gate lands in the **pre-`GROUP BY`** WHERE alongside the match, which is
/// where it has to be for the filter to scope the intermediate scan rather than
/// the aggregation output. Verified that PostgreSQL still hoists it: the
/// `One-Time Filter` appears inside the `GroupAggregate` and the untaken
/// variant's `Nested Loop Semi Join` is `(never executed)`.
///
/// Returns `None` if `end_query` contains no ` GROUP BY ` marker (same defensive
/// fallback as the ungated form).
pub(crate) fn inject_affected_filter_before_group_by_gated(
    end_query: &str,
    output_gb_cols: &[String],
    affected_tbl: &str,
    outer_qualifier: &str,
    not_null_columns: &std::collections::HashSet<String>,
) -> Option<Vec<String>> {
    let upper = end_query.to_uppercase();
    let gb_marker = " GROUP BY ";
    let pos = upper.rfind(gb_marker)?;
    let m = null_safe_in_gated(
        affected_tbl,
        outer_qualifier,
        output_gb_cols,
        output_gb_cols,
        not_null_columns,
    );
    Some(m.stmts(|filter| format!("{} AND {}{}", &end_query[..pos], filter, &end_query[pos..])))
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
