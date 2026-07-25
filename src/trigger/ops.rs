use super::union_delta::scoped_delta_query;
use super::*;
use crate::aggregation::AggregationPlan;
use crate::query_decomposer::{
    normalized_column_name, passthrough_scratch_new_table_name, passthrough_scratch_old_table_name,
    quote_identifier, split_qualified_name,
};
use crate::sql_writer::identifier::replace_source_with_transition;

/// Aggregate-IMV `INSERT` / `INSERT_PROMOTED` arm of [`reflex_build_delta_sql`].
///
/// Pushes the materialized scratch fill + MERGE statements onto `stmts`.
/// Bulk-INSERT (skipping MERGE) only fires when the operation is an Item α
/// `INSERT_PROMOTED` *and* the source has a `source_join_keys` entry — see
/// the in-body comment for why regular `INSERT` always takes the MERGE path.
#[allow(clippy::too_many_arguments)]
pub(crate) fn aggregate_insert_stmts(
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
    let delta_q = scoped_delta_query(base_query, source_table, new_tbl);

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
pub(crate) fn aggregate_delete_stmts(
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
    let delta_q = scoped_delta_query(base_query, source_table, old_tbl);

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
pub(crate) fn aggregate_update_stmts(
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
    let delta_old = scoped_delta_query(base_query, source_table, old_tbl);
    let delta_new = scoped_delta_query(base_query, source_table, new_tbl);

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
pub(crate) struct PendingDispatch {
    pub(crate) merge_sql: String,
}

/// Full refresh: rebuild the IMV from `base_query`/`end_query` when no valid
/// incremental delta exists. Used for self-joins (the source appears multiple
/// times, so a single transition swap is wrong) and for a source inside a
/// non-distributive set-op (UNION/INTERSECT/EXCEPT) subquery. Both passthrough
/// and aggregate shapes rebuild from `base_query`.
pub(crate) fn full_refresh_stmts(
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
pub(crate) fn secondary_ref_identifiers(base_query: &str, source_table: &str) -> Vec<String> {
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

/// Whether every GROUP BY column that `source_join_keys` maps for this
/// secondary is STABLE — i.e. not itself derived from the secondary being
/// mutated. `parse_join_condition_mappings` matches JOIN-equality sides by
/// bare column name only, so `fa LEFT JOIN fb ON fa.k = fb.k GROUP BY fb.k`
/// gets mapped as if the group column were `fa.k` (the primary's copy, which
/// can't migrate) when it's actually `fb.k` (the secondary's copy, which
/// migrates NULL<->value as fb changes). The join-key-scoped fast path below
/// only rebuilds the changed-key groups, never the NULL group a migrating
/// row leaves or joins, so it must not be used when a mapped column can
/// migrate. Mirrors the qualifier check the STABLE-column fallback (below)
/// uses to decide the same thing; an unmatched or unqualified column is
/// treated as migrating (fail toward the safe fallback).
///
/// A FULL OUTER JOIN base query never reaches this function at all — its
/// caller (`outer_join_secondary_stmts`) bails out to an unconditional full
/// rebuild for that join type before `source_join_keys` is even consulted,
/// for the same reason this predicate would otherwise have to refuse it: the
/// "qualifier != mutated table => stable" premise is false for either side of
/// a FULL JOIN, since either can carry unmatched rows whose join-key column
/// is NULL on one side of the row and populated on the other.
fn join_key_scope_is_sound(
    plan: &AggregationPlan,
    join_keys: &[(String, String)],
    base_query: &str,
    source_table: &str,
) -> bool {
    let sec_ids = secondary_ref_identifiers(base_query, source_table);
    join_keys.iter().all(|(interm_col, _)| {
        plan.group_by_columns
            .iter()
            .find(|gb| normalized_column_name(gb).to_lowercase() == *interm_col)
            .is_some_and(|gb| {
                gb.split_once('.')
                    .map(|(q, _)| q.trim().trim_matches('"').to_lowercase())
                    .is_some_and(|q| !sec_ids.contains(&q))
            })
    })
}

/// Outer-join-secondary handling: when source_table is the secondary side of a
/// LEFT/RIGHT JOIN (or any side of FULL OUTER), the MERGE subtract can't represent
/// the NULL semantics. Passthrough → full refresh. Aggregate → targeted group
/// reconcile via the affected_tbl, scoped to the *stable* (non-secondary) group
/// columns so a secondary-derived group key that migrates is fully rebuilt for
/// the affected join keys. With no stable group column, falls back to a full
/// intermediate + target refresh.
#[allow(clippy::too_many_arguments)]
pub(crate) fn outer_join_secondary_stmts(
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
        match plan.passthrough_key_mappings.get(source_table) {
            Some(mappings) if !mappings.is_empty() => {
                let bq_upper = base_query.to_uppercase();
                if bq_upper.contains("FULL JOIN") || bq_upper.contains("FULL OUTER") {
                    // A FULL JOIN delta surfaces unmatched rows from the OTHER
                    // side that keyed scoping (on this secondary's join keys)
                    // cannot capture — keep the safe full rebuild.
                    stmts.push(format!("DELETE FROM {}", qv));
                    stmts.push(format!("INSERT INTO {} {}", qv, base_query));
                    return;
                }
                let target_cols: Vec<String> =
                    mappings.iter().map(|(t, _)| format!("\"{}\"", t)).collect();
                // Changed secondary join keys, drawn only from the transition
                // tables that EXIST for this operation's trigger:
                //   INSERT(_PROMOTED) → NEW only, DELETE(_PROMOTED) → OLD only,
                //   UPDATE → both. The DELETE trigger declares only OLD TABLE and
                //   the INSERT trigger only NEW TABLE, so referencing the absent
                //   side raises `relation "__reflex_new_*" does not exist`. (The
                //   *_PROMOTED ops fire from the UPDATE trigger where both tables
                //   exist, but one side is empty post-filter, so a single side is
                //   both safe and sufficient.) Alias each source column to its
                //   target name so the membership subquery's projection matches the
                //   target columns being filtered (the secondary's join column may
                //   differ from the IMV output name, e.g. b.a_id projected as "id").
                let alias_pairs: Vec<String> = mappings
                    .iter()
                    .map(|(t, s)| format!("\"{}\" AS \"{}\"", s, t))
                    .collect();
                let ap = alias_pairs.join(", ");
                let needs_new = matches!(operation, "INSERT" | "INSERT_PROMOTED" | "UPDATE");
                let needs_old = matches!(operation, "DELETE" | "DELETE_PROMOTED" | "UPDATE");
                let mut sides: Vec<String> = Vec::new();
                if needs_old {
                    sides.push(format!(
                        "SELECT {ap} FROM \"{old}\"",
                        ap = ap,
                        old = old_tbl
                    ));
                }
                if needs_new {
                    sides.push(format!(
                        "SELECT {ap} FROM \"{new}\"",
                        ap = ap,
                        new = new_tbl
                    ));
                }
                let changed_keys = sides.join(" UNION ");
                // The membership source is a derived table (a UNION over the
                // transition tables), so it MUST carry an alias — an unaliased
                // subquery in FROM is rejected by every Postgres version with
                // "subquery in FROM must have an alias". The predicate is built
                // separately per statement (not reused verbatim) because the
                // NULL-safe form needs an explicit outer qualifier, and that
                // qualifier differs between the DELETE (qv itself) and the
                // INSERT (the `__bq` alias of base_query).
                let ck_relation = format!("({}) __ck", changed_keys);
                let del_pred = build_null_safe_membership_predicate(
                    &qv,
                    &target_cols,
                    &target_cols,
                    &ck_relation,
                    &plan.not_null_columns,
                );
                stmts.push(format!("DELETE FROM {} WHERE {}", qv, del_pred));
                let ins_pred = build_null_safe_membership_predicate(
                    "__bq",
                    &target_cols,
                    &target_cols,
                    &ck_relation,
                    &plan.not_null_columns,
                );
                stmts.push(format!(
                    "INSERT INTO {} SELECT * FROM ({}) __bq WHERE {}",
                    qv, base_query, ins_pred
                ));
                return;
            }
            _ => {
                // No derivable mapping → safe full-rebuild fallback.
                stmts.push(format!("DELETE FROM {}", qv));
                stmts.push(format!("INSERT INTO {} {}", qv, base_query));
                return;
            }
        }
    }

    // A FULL OUTER JOIN breaks BOTH of the aggregate scoping strategies below,
    // not just the join-key-scoped fast path: both rest on "a group-by column
    // qualified by a table OTHER than the one just mutated is stable" (see
    // `join_key_scope_is_sound`'s doc comment), and a FULL JOIN's unmatched-row
    // semantics make that false for either side — a write to one side can hand
    // the OTHER side's join column a new (or newly-vanished) NULL group value
    // without that other table's own row having changed at all. Bail out to
    // the same unconditional full intermediate+target refresh the passthrough
    // branch above already uses for this join type, mirroring its own
    // FULL-JOIN check.
    let bq_upper = base_query.to_uppercase();
    if bq_upper.contains("FULL JOIN") || bq_upper.contains("FULL OUTER") {
        stmts.push(format!("TRUNCATE {}", intermediate_tbl));
        stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
        stmts.push(format!("TRUNCATE {}", qv));
        if end_query.is_empty() {
            stmts.push(format!("INSERT INTO {} {}", qv, base_query));
        } else {
            stmts.push(format!("INSERT INTO {} {}", qv, end_query));
        }
        return;
    }

    // Scoped recompute for an aggregate outer-join secondary that carries a
    // join-key→group-column mapping (`source_join_keys`). The broad path below
    // builds its "affected groups" by re-aggregating the ENTIRE base with the
    // secondary swapped for its transition table — but the LEFT JOIN preserves
    // every primary row, so the affected set is ALL groups and a tiny secondary
    // delta costs a full rebuild (18 min on db_dev sop_incoming_stock_baseline).
    //
    // `source_join_keys` is populated only when every join equality maps to a
    // GROUP BY column AND the source columns cover a unique key of the secondary
    // (see build_source_join_keys). That is exactly the condition under which we
    // can scope by `(group_cols) IN (changed source keys from OLD∪NEW)`: a
    // predicate on GROUP BY columns the planner pushes below the aggregation
    // into the indexed base scan. OLD∪NEW because a secondary key change moves a
    // group (old key loses its contribution, new key gains one); a non-key
    // column change keeps the same join key, so both the old and new (possibly
    // migrated) group rows share it and are recomputed together.
    if let Some(join_keys) = plan.source_join_keys.get(source_table) {
        if !join_keys.is_empty()
            && join_key_scope_is_sound(plan, join_keys, base_query, source_table)
        {
            let interm_cols: Vec<String> = join_keys
                .iter()
                .map(|(interm, _)| format!("\"{}\"", interm))
                .collect();
            let alias_pairs = join_keys
                .iter()
                .map(|(interm, src)| format!("\"{}\" AS \"{}\"", src, interm))
                .collect::<Vec<_>>()
                .join(", ");

            let needs_new = matches!(operation, "INSERT" | "INSERT_PROMOTED" | "UPDATE");
            let needs_old = matches!(operation, "DELETE" | "DELETE_PROMOTED" | "UPDATE");
            let mut sides: Vec<String> = Vec::new();
            if needs_old {
                sides.push(format!("SELECT {alias_pairs} FROM \"{old_tbl}\""));
            }
            if needs_new {
                sides.push(format!("SELECT {alias_pairs} FROM \"{new_tbl}\""));
            }
            let changed_keys = format!("({}) __ck", sides.join(" UNION "));
            let pred = build_membership_predicate(&interm_cols, &interm_cols, &changed_keys);

            // Recompute exactly the affected groups into the intermediate, then
            // resync the target — every statement scoped by the same membership
            // predicate so the base scan prunes to the changed join keys.
            stmts.push(format!("DELETE FROM {} WHERE {}", intermediate_tbl, pred));
            stmts.push(format!(
                "INSERT INTO {} SELECT * FROM ({}) AS __full WHERE {}",
                intermediate_tbl, base_query, pred
            ));
            stmts.push(format!("DELETE FROM {} WHERE {}", qv, pred));
            if end_query.is_empty() {
                stmts.push(format!(
                    "INSERT INTO {} SELECT * FROM ({}) AS __full WHERE {}",
                    qv, base_query, pred
                ));
            } else {
                stmts.push(format!("INSERT INTO {} {} AND {}", qv, end_query, pred));
            }
            return;
        }
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

        // PS-5 — each affected-groups match expands into a self-gated
        // sargable/NULL-safe pair when any scope column is nullable, so every
        // slot below emits one statement per live variant.
        let ns_in_int = null_safe_in_gated(
            affected_tbl,
            intermediate_tbl,
            &scope_cols,
            &scope_cols,
            &plan.not_null_columns,
        );
        stmts.extend(
            ns_in_int.stmts(|pred| format!("DELETE FROM {} WHERE {}", intermediate_tbl, pred)),
        );

        let ns_in_full = null_safe_in_gated(
            affected_tbl,
            "__full",
            &scope_cols,
            &scope_cols,
            &plan.not_null_columns,
        );
        stmts.extend(ns_in_full.stmts(|pred| {
            format!(
                "INSERT INTO {} SELECT * FROM ({}) AS __full WHERE {}",
                intermediate_tbl, base_query, pred
            )
        }));

        let ns_in_tgt_delete = null_safe_in_gated(
            affected_tbl,
            &qv,
            &scope_target_cols,
            &scope_cols,
            &plan.not_null_columns,
        );
        stmts.extend(ns_in_tgt_delete.stmts(|pred| format!("DELETE FROM {} WHERE {}", qv, pred)));

        let ns_in_tgt_insert = null_safe_in_gated(
            affected_tbl,
            intermediate_tbl,
            &scope_cols,
            &scope_cols,
            &plan.not_null_columns,
        );
        stmts.extend(
            ns_in_tgt_insert.stmts(|pred| format!("INSERT INTO {} {} AND {}", qv, end_query, pred)),
        );
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
/// For a partition-dispatchable passthrough, resolve the partition column's
/// quoted forms and the strategy: the target (IMV output) name used to filter
/// `view`/the delta projection, the source name used to read the touched
/// partition values from the scratch tables (the two differ when the partition
/// column is aliased in the projection), and the partition strategy ("LIST" |
/// Resolve the non-key columns of the target view at codegen time via SPI.
/// Returns a Vec of quoted column names (e.g., "\"qty\"", "\"price\""), excluding
/// the key columns. If the target cannot be resolved or has no non-key columns,
/// returns an empty Vec (which signals fallback to standard DELETE+INSERT).
fn resolve_inplace_non_key_cols(view_name: &str, key_target_cols: &[String]) -> Vec<String> {
    use pgrx::datum::DatumWithOid;
    use pgrx::spi::Spi;
    use pgrx::PgBuiltInOids;

    // Codegen also runs in plain #[test] unit tests with no backend, where Spi
    // would panic on the weak SPI_connect stub. CurrentMemoryContext is NULL only
    // in that stub context; in a real backend (create/flush, #[pg_test]) it is set.
    // No backend → return empty so the caller falls back to DELETE+INSERT.
    if unsafe { pgrx::pg_sys::CurrentMemoryContext.is_null() } {
        return Vec::new();
    }

    // Build a normalized set of key column names for filtering.
    let key_names: std::collections::HashSet<String> = key_target_cols
        .iter()
        .map(|c| {
            // Remove quotes and normalize for comparison
            let unquoted = c.trim_matches('"');
            unquoted.to_lowercase()
        })
        .collect();

    let result = Spi::connect(|client| {
        // Query the target's columns in attnum order, excluding dropped columns.
        // Return quoted column names.
        let rows = client
            .select(
                "SELECT attname FROM pg_attribute \
                 WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped \
                 ORDER BY attnum",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report();

        rows.filter_map(|row| {
            row.get_by_name::<String, _>("attname")
                .unwrap_or(None)
                .and_then(|name| {
                    let norm = name.to_lowercase();
                    // Exclude key columns (use lowercase comparison).
                    if !key_names.contains(&norm) {
                        Some(format!("\"{}\"", name))
                    } else {
                        None
                    }
                })
        })
        .collect::<Vec<String>>()
    });

    result
}

/// "RANGE"). Returns `None` when the plan is not LIST/RANGE-partitioned, so the
/// caller emits the plain keyed delete/insert.
fn passthrough_partition_dispatch_cols(
    plan: &AggregationPlan,
    mappings: &[(String, String)],
) -> Option<(String, String, String, String)> {
    let is_list = plan.partition_strategy.eq_ignore_ascii_case("LIST");
    let is_range = plan.partition_strategy.eq_ignore_ascii_case("RANGE");
    if plan.partition_columns.is_empty() || !(is_list || is_range) {
        return None;
    }
    let part_col = plan.partition_columns[0].clone();
    let part_col_q = format!("\"{}\"", part_col.replace('"', ""));
    let part_col_norm = normalized_column_name(&part_col);
    let part_src = mappings
        .iter()
        .find(|(t, _)| normalized_column_name(t) == part_col_norm)
        .map(|(_, s)| s.clone())
        .unwrap_or_else(|| part_col.clone());
    let part_src_q = format!("\"{}\"", part_src.replace('"', ""));
    Some((
        part_col,
        part_col_q,
        part_src_q,
        plan.partition_strategy.clone(),
    ))
}

/// Re-key `plan.not_null_columns` (keyed on *target* column bare names) onto
/// the *source* column bare names of a passthrough key-owner mapping, for
/// `trigger::merge::null_safe_in`, whose `not_null_columns` argument is
/// looked up against the "affected" (here: source/scratch) side of the match.
fn source_not_null_cols(
    mappings: &[(String, String)],
    not_null_columns: &std::collections::HashSet<String>,
) -> std::collections::HashSet<String> {
    mappings
        .iter()
        .filter(|(t, _)| not_null_columns.contains(t.as_str()))
        .map(|(_, s)| s.clone())
        .collect()
}

/// The passthrough keyed DELETE/UPDATE predicate: `(target_cols) IN (SELECT
/// source_cols FROM pt_old)`, made NULL-safe when a key column is not proven
/// NOT NULL (2026-07-25 untreated_bugs: `(NULL) IN (...)` is never TRUE, so a
/// NULL-key row's target counterpart is never matched — a phantom row on
/// DELETE, a unique-index abort on UPDATE). Unlike `null_safe_in` (which
/// always emits the correlated-EXISTS shape, only varying the per-column
/// operator), this collapses to the plain `IN`-list — byte-identical to the
/// pre-fix output — when every mapped column is in `plan.not_null_columns`,
/// which is the case that must see no plan change.
fn passthrough_keyed_delete_predicate(
    pt_old: &str,
    qv: &str,
    mappings: &[(String, String)],
    target_cols: &[String],
    source_cols: &[String],
    not_null_columns: &std::collections::HashSet<String>,
) -> String {
    let all_not_null = mappings
        .iter()
        .all(|(t, _)| not_null_columns.contains(t.as_str()));
    if all_not_null {
        return build_membership_predicate(target_cols, source_cols, pt_old);
    }
    null_safe_in(
        pt_old,
        qv,
        target_cols,
        source_cols,
        &source_not_null_cols(mappings, not_null_columns),
    )
}

/// Strategy-specific cold-exclusion predicate for the passthrough dispatch.
/// LIST excludes hot partition VALUES (`$1::TEXT[]`); RANGE excludes rows of hot
/// CHILDREN by resolving the value to its child of `view_parent` and comparing
/// the child NAME (`$2::text[]`) — a value filter is wrong (many values per range
/// child) and an OID filter is wrong (the swap changes the child OID).
fn passthrough_cold_pred(
    strategy_is_range: bool,
    view_parent_lit: &str,
    part_col_lit: &str,
    qualified_col: &str,
) -> String {
    if strategy_is_range {
        format!(
            "public.__reflex_partition_child_for_key('{parent}'::regclass, '{col}', {qc}::text)::text <> ALL($2::text[])",
            parent = view_parent_lit,
            col = part_col_lit,
            qc = qualified_col
        )
    } else {
        format!("{qc}::text <> ALL($1::TEXT[])", qc = qualified_col)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn passthrough_op_stmts(
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
            let delta_q = scoped_delta_query(base_query, source_table, &pt_new);
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
                let base_del = format!(
                    "DELETE FROM {} WHERE {}",
                    qv,
                    passthrough_keyed_delete_predicate(
                        &pt_old,
                        &qv,
                        mappings,
                        &target_cols,
                        &source_cols,
                        &plan.not_null_columns,
                    )
                );

                if let Some((part_col, part_col_q, part_src_q, strategy)) =
                    passthrough_partition_dispatch_cols(plan, mappings)
                {
                    // Hybrid partition dispatch (audit #2): DELETE-only, so no
                    // cold INSERT body. Hot leaves are swapped (rebuilt from the
                    // post-delete source state); cold leaves get the keyed delete.
                    let strategy_is_range = strategy.eq_ignore_ascii_case("RANGE");
                    let parent_lit = qv.replace('"', "").replace('\'', "''");
                    let part_col_lit = part_col.replace('\'', "''");
                    let del_cold = format!(
                        "{} AND {}",
                        base_del,
                        passthrough_cold_pred(
                            strategy_is_range,
                            &parent_lit,
                            &part_col_lit,
                            &format!("{}.{}", qv, part_col_q)
                        )
                    );
                    let aff = format!("SELECT {}::text AS pkey FROM {}", part_src_q, pt_old);
                    stmts.push(build_passthrough_partition_dispatch_sql(
                        view_name,
                        &qv,
                        &aff,
                        &part_col,
                        &format!("{}.{}", qv, part_col_q),
                        &strategy,
                        &del_cold,
                        "",
                        None,
                    ));
                } else {
                    stmts.push(base_del);
                }
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
                let base_del = format!(
                    "DELETE FROM {} WHERE {}",
                    qv,
                    passthrough_keyed_delete_predicate(
                        &pt_old,
                        &qv,
                        mappings,
                        &target_cols,
                        &source_cols,
                        &plan.not_null_columns,
                    )
                );
                let delta_new = scoped_delta_query(base_query, source_table, &pt_new);
                let base_ins = format!("INSERT INTO {} {}", qv, delta_new);

                if let Some((part_col, part_col_q, part_src_q, strategy)) =
                    passthrough_partition_dispatch_cols(plan, mappings)
                {
                    // Hybrid partition dispatch (audit #2): hot leaves swapped,
                    // cold leaves keyed-maintained. The cold DELETE extends the
                    // keyed predicate with the hot-exclusion filter; the cold
                    // INSERT re-runs the delta projection (which reads pt_new) for
                    // cold partitions only.
                    let strategy_is_range = strategy.eq_ignore_ascii_case("RANGE");
                    let parent_lit = qv.replace('"', "").replace('\'', "''");
                    let part_col_lit = part_col.replace('\'', "''");
                    let del_cold = format!(
                        "{} AND {}",
                        base_del,
                        passthrough_cold_pred(
                            strategy_is_range,
                            &parent_lit,
                            &part_col_lit,
                            &format!("{}.{}", qv, part_col_q)
                        )
                    );
                    let ins_cold = format!(
                        "INSERT INTO {} SELECT * FROM ({}) __pt WHERE {}",
                        qv,
                        delta_new,
                        passthrough_cold_pred(
                            strategy_is_range,
                            &parent_lit,
                            &part_col_lit,
                            &format!("__pt.{}", part_col_q)
                        )
                    );
                    let aff = format!(
                        "SELECT {sc}::text AS pkey FROM {old} UNION SELECT {sc}::text AS pkey FROM {new}",
                        sc = part_src_q,
                        old = pt_old,
                        new = pt_new
                    );
                    debug_assert!(
                        operation == "UPDATE" && !mappings.is_empty(),
                        "in-place cold path only valid for keyed passthrough UPDATE"
                    );

                    // Resolve non-key columns at codegen time via SPI.
                    // This avoids runtime column list construction and ensures the upsert
                    // can cover all target columns (keys ∪ non-keys) correctly.
                    let non_key_cols = resolve_inplace_non_key_cols(view_name, &target_cols);

                    if !non_key_cols.is_empty() {
                        // In-place optimization is viable: we have both key and non-key columns.
                        let spec = crate::trigger::dispatch::InplaceSpec {
                            delta_new: &delta_new,
                            key_target_cols: &target_cols,
                            key_source_cols: &source_cols,
                            non_key_cols: &non_key_cols,
                            pt_old: &pt_old,
                            not_null_columns: &plan.not_null_columns,
                        };
                        stmts.push(build_passthrough_partition_dispatch_sql(
                            view_name,
                            &qv,
                            &aff,
                            &part_col,
                            &format!("{}.{}", qv, part_col_q),
                            &strategy,
                            &del_cold,
                            &ins_cold,
                            Some(&spec),
                        ));
                    } else {
                        // Fallback: non-key resolution failed or target cannot be resolved.
                        // Use standard DELETE+INSERT path (passes None for inplace).
                        stmts.push(build_passthrough_partition_dispatch_sql(
                            view_name,
                            &qv,
                            &aff,
                            &part_col,
                            &format!("{}.{}", qv, part_col_q),
                            &strategy,
                            &del_cold,
                            &ins_cold,
                            None,
                        ));
                    }
                } else {
                    stmts.push(base_del);
                    stmts.push(base_ins);
                }
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
pub(crate) fn aggregate_epilogue_stmts(
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
                    &[],
                    std::slice::from_ref(&tdel),
                    std::slice::from_ref(&tins),
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
            match inject_affected_filter_before_group_by_gated(
                end_query,
                &output_cols,
                affected_tbl,
                intermediate_tbl,
                &plan.not_null_columns,
            ) {
                Some(spliced_end_qs) => {
                    // PS-5 — the spliced INSERT and the target DELETE each expand
                    // into one statement per live variant of the gated match.
                    let ns_in_target = null_safe_in_gated(
                        affected_tbl,
                        &qv,
                        &target_cols,
                        &output_cols,
                        &plan.not_null_columns,
                    );
                    let tdel =
                        ns_in_target.stmts(|pred| format!("DELETE FROM {} WHERE {}", qv, pred));
                    let tins: Vec<String> = spliced_end_qs
                        .iter()
                        .map(|q| format!("INSERT INTO {} {}", qv, q))
                        .collect();
                    if let Some(pd) = pending_dispatch.take() {
                        stmts.push(build_high_selectivity_dispatch_sql(
                            view_name,
                            intermediate_tbl,
                            affected_tbl,
                            &pd.merge_sql,
                            &[],
                            &tdel,
                            &tins,
                        ));
                    } else {
                        if !skip_target_delete {
                            stmts.extend(tdel);
                        }
                        stmts.extend(tins);
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
                            &[],
                            std::slice::from_ref(&tdel),
                            std::slice::from_ref(&tins),
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
        // PS-5 — a nullable group key expands each affected-groups match into a
        // self-gated sargable/NULL-safe pair, so every slot below is a LIST of
        // statements. Exactly one member of each pair does work at run time; the
        // other's plan is skipped by a `One-Time Filter`.
        let ns_in_intermediate = null_safe_in_gated(
            affected_tbl,
            intermediate_tbl,
            cols,
            cols,
            &plan.not_null_columns,
        );
        let ns_in_target_delete = null_safe_in_gated(
            affected_tbl,
            &qv,
            &target_cols,
            cols,
            &plan.not_null_columns,
        );
        let dead_cleanup_sql: Vec<String> = if include_dead_cleanup {
            ns_in_intermediate.stmts(|pred| {
                format!(
                    "DELETE FROM {} WHERE __ivm_count <= 0 AND {}",
                    intermediate_tbl, pred
                )
            })
        } else {
            Vec::new()
        };
        let target_delete_sql =
            ns_in_target_delete.stmts(|pred| format!("DELETE FROM {} WHERE {}", qv, pred));
        let target_insert_sql = ns_in_intermediate
            .stmts(|pred| format!("INSERT INTO {} {} AND {}", qv, end_query, pred));

        if let Some(pd) = pending_dispatch.take() {
            let strategy_is_range = plan.partition_strategy.eq_ignore_ascii_case("RANGE");
            let use_partition_dispatch = !plan.partition_columns.is_empty()
                && (plan.partition_strategy.eq_ignore_ascii_case("LIST") || strategy_is_range);
            if use_partition_dispatch {
                let part_col = &plan.partition_columns[0];
                let part_col_q = format!("\"{}\"", part_col);
                let parent_lit = intermediate_tbl.replace('"', "").replace('\'', "''");
                let part_col_lit = part_col.replace('\'', "''");
                // Cold-exclusion predicate, strategy-specific:
                //   LIST  → exclude hot partition VALUES   ($1::TEXT[])
                //   RANGE → exclude rows of hot CHILDs      ($2::text[]), resolving
                //           each value to its child of the (partitioned) intermediate
                //           and comparing the child NAME — a value-array filter is
                //           wrong because many values map to one range child, and a
                //           child-OID filter is wrong because the hot swap changes
                //           the child OID (the name survives the swap's RENAME).
                let cold_pred = |qualified_col: &str| -> String {
                    if strategy_is_range {
                        format!(
                            "public.__reflex_partition_child_for_key('{parent}'::regclass, '{col}', {qc}::text)::text <> ALL($2::text[])",
                            parent = parent_lit,
                            col = part_col_lit,
                            qc = qualified_col
                        )
                    } else {
                        format!("{qc}::text <> ALL($1::TEXT[])", qc = qualified_col)
                    }
                };
                let filtered_scratch = format!(
                    "(SELECT * FROM {} WHERE {})",
                    scratch_tbl,
                    cold_pred(&part_col_q)
                );
                let merge_filtered = build_merge_from_table_sql(
                    intermediate_tbl,
                    &filtered_scratch,
                    plan,
                    DeltaOp::Add,
                );
                // The cold-partition filter is appended to EVERY gated variant —
                // both carry the same `$1`/`$2` placeholders, which is exactly why
                // the gate lives inside the statement rather than in a wrapping
                // `DO` block (a `DO` block cannot take parameters).
                let dead_cleanup_filtered: Vec<String> = dead_cleanup_sql
                    .iter()
                    .map(|s| {
                        format!(
                            "{} AND EXISTS (SELECT 1 FROM {} __ap \
                              WHERE __ap.{} = {}.{} AND {})",
                            s,
                            affected_tbl,
                            part_col_q,
                            intermediate_tbl,
                            part_col_q,
                            cold_pred(&format!("__ap.{}", part_col_q))
                        )
                    })
                    .collect();
                let tdel_filtered: Vec<String> = target_delete_sql
                    .iter()
                    .map(|s| format!("{} AND {}", s, cold_pred(&format!("{}.{}", qv, part_col_q))))
                    .collect();
                let tins_filtered: Vec<String> = target_insert_sql
                    .iter()
                    .map(|s| {
                        format!(
                            "{} AND {}",
                            s,
                            cold_pred(&format!("{}.{}", intermediate_tbl, part_col_q))
                        )
                    })
                    .collect();
                stmts.push(build_partition_aware_dispatch_sql_strategy(
                    view_name,
                    intermediate_tbl,
                    intermediate_tbl,
                    affected_tbl,
                    part_col,
                    &plan.partition_strategy,
                    &merge_filtered,
                    &dead_cleanup_filtered,
                    &tdel_filtered,
                    &tins_filtered,
                ));
            } else {
                stmts.push(build_high_selectivity_dispatch_sql(
                    view_name,
                    intermediate_tbl,
                    affected_tbl,
                    &pd.merge_sql,
                    &dead_cleanup_sql,
                    &target_delete_sql,
                    &target_insert_sql,
                ));
            }
        } else {
            stmts.extend(dead_cleanup_sql);
            if !skip_target_delete {
                stmts.extend(target_delete_sql);
            }
            stmts.extend(target_insert_sql);
        }
        stmts.push(metadata_sql);
    } else {
        let qv = quote_identifier(view_name);
        stmts.push(format!("TRUNCATE {}", qv));
        stmts.push(format!("INSERT INTO {} {}", qv, end_query));
        stmts.push(metadata_sql);
    }
}
