//! Registry dependency-graph repair (1.11.0, PS-1).
//!
//! `depends_on` is the one registry column no bug has corrupted: every create
//! path writes it from the analyzer's source list. `depends_on_imv`,
//! `graph_child`, `graph_depth` and `is_generated_sub_imv` are all derivable
//! from it, so this module re-derives them rather than trusting them.
//!
//! Called once after the 1.10.11 → 1.11.0 upgrade to repair rows created before
//! `resolve_existing_imv_deps` canonicalised its probe, and exposed as a
//! re-runnable operator primitive. Idempotent on an ACYCLIC graph — a second run
//! changes nothing. A registry holding a dependency cycle cannot converge, and
//! `graph_depth` there grows by `MAX_DEPTH_PASSES` per run; that case is detected
//! and reported rather than silently claimed as repaired.
//!
//! Every step de-quotes a stored source name with `replace(s, '"', '')`. A
//! CTE-decomposed sub-IMV source is persisted double-quoted
//! (`"schema"."view__cte_x"`) to preserve identifier case while the registry
//! `name` is stored bare, so stripping every `"` maps the former onto the
//! latter. An identifier containing a literal embedded double-quote would be
//! mangled; pg_reflex never generates one.

use pgrx::prelude::*;

/// Fixpoint iteration cap for `graph_depth`. The deepest chain observed in
/// production is 4; the cap exists so a cycle — which create-time rejects, but a
/// hand-edited registry could hold — terminates instead of spinning.
const MAX_DEPTH_PASSES: i32 = 20;

/// Rebuild `is_generated_sub_imv`, `depends_on_imv`, `graph_child` and
/// `graph_depth` for every registry row from `depends_on`.
///
/// The `depends_on_imv` and `graph_child` steps are additive — an edge is only
/// ever added. Removal is the only operation that could make the graph worse
/// than it already is, and every spurious extra edge fails in the safe
/// direction: `reflex_rebuild_chain` and a non-cascade `drop_reflex_ivm` refuse
/// rather than destroying or orphaning a dependent.
///
/// Returns `REPAIRED: …` on convergence, or `WARNING: …` when `graph_depth` did
/// not reach a fixpoint — which means the registry holds a dependency cycle.
#[pg_extern]
pub fn reflex_repair_dependency_graph() -> String {
    let generated = mark_legacy_generated_sub_imvs();
    let imv_edges = backfill_depends_on_imv();
    let child_edges = backfill_graph_child();
    let (depth_rows, passes, converged) = recompute_graph_depth();

    if !converged {
        warning!(
            "pg_reflex: graph_depth did not converge in {} passes — the registry's \
             depends_on_imv graph contains a cycle. Edge repair was applied, but \
             graph_depth is NOT trustworthy and re-running this function will keep \
             inflating it. Find the cycle with: WITH RECURSIVE w(a,b) AS (SELECT name, \
             unnest(depends_on_imv) FROM public.__reflex_ivm_reference UNION SELECT \
             w.a, unnest(r.depends_on_imv) FROM w JOIN public.__reflex_ivm_reference r \
             ON r.name = w.b) SELECT * FROM w WHERE a = b;",
            MAX_DEPTH_PASSES
        );
        return format!(
            "WARNING: is_generated_sub_imv={}, depends_on_imv={}, graph_child={} repaired, \
             but graph_depth did NOT converge in {} passes (dependency cycle)",
            generated, imv_edges, child_edges, MAX_DEPTH_PASSES
        );
    }

    format!(
        "REPAIRED: is_generated_sub_imv={}, depends_on_imv={}, graph_child={}, \
         graph_depth={} ({} passes)",
        generated, imv_edges, child_edges, depth_rows, passes
    )
}

/// Flag rows that pg_reflex generated before the column existed.
///
/// Two conditions must both hold: the name carries a decomposition suffix, and
/// some *other* registry row actually depends on the name. Using the name suffix
/// is acceptable here and nowhere else — this is a one-shot repair of rows
/// created before the flag was recorded, where no better evidence survives,
/// whereas the reconcile recursion evaluates its predicate forever and so reads
/// the column. A false positive needs a user to have named an IMV
/// `foo__cte_bar` *and* have `foo` read from it. The cost of that is bounded: the
/// node gets rebuilt on `foo`'s reconcile, which never silently changes another
/// IMV's values, but does suppress propagation to any OTHER consumer of it for
/// the duration of that rebuild, leaving those consumers to the
/// `reflex_on_ddl_command_end` staleness alarm rather than an incremental delta.
fn mark_legacy_generated_sub_imvs() -> i64 {
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE public.__reflex_ivm_reference t \
                    SET is_generated_sub_imv = TRUE \
                  WHERE NOT COALESCE(t.is_generated_sub_imv, FALSE) \
                    AND ( t.name LIKE '%\\_\\_cte\\_%' \
                       OR t.name ~ '__union_[0-9]+$' \
                       OR t.name LIKE '%\\_\\_base' ) \
                    AND EXISTS ( \
                          SELECT 1 \
                            FROM public.__reflex_ivm_reference p, \
                                 unnest(COALESCE(p.depends_on, ARRAY[]::TEXT[])) AS s \
                           WHERE p.name <> t.name AND replace(s, '\"', '') = t.name )",
                None,
                &[],
            )
            .map(|rows| rows.len() as i64)
            .unwrap_or(0)
    })
}

/// Union into each row's `depends_on_imv` every registry `name` reachable by
/// de-quoting one of its `depends_on` entries — the edge
/// `resolve_existing_imv_deps` failed to record for a decomposed parent.
fn backfill_depends_on_imv() -> i64 {
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE public.__reflex_ivm_reference t \
                    SET depends_on_imv = d.merged \
                   FROM ( \
                         SELECT p.name, \
                                ARRAY( SELECT DISTINCT x \
                                         FROM unnest( \
                                                COALESCE(p.depends_on_imv, ARRAY[]::TEXT[]) || \
                                                ARRAY( SELECT ref.name \
                                                         FROM unnest(COALESCE(p.depends_on, ARRAY[]::TEXT[])) AS s \
                                                         JOIN public.__reflex_ivm_reference ref \
                                                           ON ref.name = replace(s, '\"', '') \
                                                        WHERE ref.name <> p.name ) \
                                              ) AS x \
                                        ORDER BY x ) AS merged \
                           FROM public.__reflex_ivm_reference p \
                        ) d \
                  WHERE t.name = d.name \
                    AND COALESCE(t.depends_on_imv, ARRAY[]::TEXT[]) IS DISTINCT FROM d.merged",
                None,
                &[],
            )
            .map(|rows| rows.len() as i64)
            .unwrap_or(0)
    })
}

/// Union into each row's `graph_child` the inverse of `depends_on_imv`. Must run
/// after [`backfill_depends_on_imv`], which it reads.
fn backfill_graph_child() -> i64 {
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE public.__reflex_ivm_reference t \
                    SET graph_child = d.merged \
                   FROM ( \
                         SELECT c.name, \
                                ARRAY( SELECT DISTINCT x \
                                         FROM unnest( \
                                                COALESCE(c.graph_child, ARRAY[]::TEXT[]) || \
                                                ARRAY( SELECT p.name \
                                                         FROM public.__reflex_ivm_reference p \
                                                        WHERE c.name = ANY(COALESCE(p.depends_on_imv, ARRAY[]::TEXT[])) ) \
                                              ) AS x \
                                        ORDER BY x ) AS merged \
                           FROM public.__reflex_ivm_reference c \
                        ) d \
                  WHERE t.name = d.name \
                    AND COALESCE(t.graph_child, ARRAY[]::TEXT[]) IS DISTINCT FROM d.merged",
                None,
                &[],
            )
            .map(|rows| rows.len() as i64)
            .unwrap_or(0)
    })
}

/// Iterate `depth(v) = max(depth(d) for d in depends_on_imv(v)) + 1` to a
/// fixpoint — the same expression `resolve_existing_imv_deps` computes at create
/// time, so a row's depth means the same thing regardless of its age.
///
/// Returns `(rows changed in total, passes run, converged)`. `converged = false`
/// means the loop hit `MAX_DEPTH_PASSES` while still changing rows, i.e. the graph
/// has a cycle; depths are left at their last value and the caller must not report
/// success.
fn recompute_graph_depth() -> (i64, i32, bool) {
    let mut changed_total = 0i64;
    let mut passes = 0i32;
    let mut last_changed = 0i64;
    while passes < MAX_DEPTH_PASSES {
        let changed = Spi::connect_mut(|client| {
            client
                .update(
                    "UPDATE public.__reflex_ivm_reference t \
                        SET graph_depth = d.want \
                       FROM ( \
                             SELECT p.name, COALESCE(MAX(c.graph_depth), 0) + 1 AS want \
                               FROM public.__reflex_ivm_reference p \
                               LEFT JOIN unnest(COALESCE(p.depends_on_imv, ARRAY[]::TEXT[])) AS u(nm) \
                                      ON TRUE \
                               LEFT JOIN public.__reflex_ivm_reference c ON c.name = u.nm \
                              GROUP BY p.name \
                            ) d \
                      WHERE t.name = d.name AND t.graph_depth IS DISTINCT FROM d.want",
                    None,
                    &[],
                )
                .map(|rows| rows.len() as i64)
                .unwrap_or(0)
        });
        passes += 1;
        changed_total += changed;
        last_changed = changed;
        if changed == 0 {
            break;
        }
    }
    (changed_total, passes, last_changed == 0)
}
