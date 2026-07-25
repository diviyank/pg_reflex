//! Shared string builders for scoping passthrough DML to the rows a delta
//! actually touched. Pure functions (no DB), reused by the keyed secondary path
//! (defect #3). See docs/superpowers/specs/2026-06-04-passthrough-partition-keyed-maintenance-design.md.

use super::row_expr;

/// `(target_cols) IN (SELECT source_cols FROM <src_relation>)`.
/// `target_cols` and `source_cols` are already quoted identifiers and must be
/// the same length. Single-column collapses to `col IN (...)` via `row_expr`.
pub(crate) fn build_membership_predicate(
    target_cols: &[String],
    source_cols: &[String],
    src_relation: &str,
) -> String {
    debug_assert_eq!(target_cols.len(), source_cols.len());
    format!(
        "{} IN (SELECT {} FROM {})",
        row_expr(target_cols),
        source_cols.join(", "),
        src_relation
    )
}

/// NULL-safe sibling of `build_membership_predicate` for the passthrough
/// keyed-secondary path (2026-07-25 untreated_bugs: `(NULL) IN (...)` is never
/// TRUE, so a nullable key column silently drops out of the plain `IN` form,
/// leaving a phantom target row on DELETE and a unique-index abort on
/// UPDATE). Emits byte-identical output to `build_membership_predicate` when
/// every `target_cols` entry is in `not_null_columns` — the regression-free
/// common case — and a correlated `EXISTS` with per-column `=`/`IS NOT
/// DISTINCT FROM` otherwise, mirroring `trigger::merge::null_safe_in`'s op
/// choice.
///
/// Unlike `build_membership_predicate`, this needs an explicit
/// `outer_qualifier`: inside the `EXISTS` subquery an unqualified column
/// reference binds to `src_relation`'s own projection (the innermost scope),
/// not the enclosing statement's row — and the caller always aliases
/// `src_relation`'s columns to the same names as `target_cols`, so an
/// unqualified reference on the left would self-reference instead of
/// correlating outward.
pub(crate) fn build_null_safe_membership_predicate(
    outer_qualifier: &str,
    target_cols: &[String],
    source_cols: &[String],
    src_relation: &str,
    not_null_columns: &std::collections::HashSet<String>,
) -> String {
    debug_assert_eq!(target_cols.len(), source_cols.len());
    let all_not_null = target_cols
        .iter()
        .all(|t| not_null_columns.contains(t.trim_matches('"')));
    if all_not_null {
        return build_membership_predicate(target_cols, source_cols, src_relation);
    }
    let conditions: Vec<String> = target_cols
        .iter()
        .zip(source_cols.iter())
        .map(|(t, s)| {
            let bare = t.trim_matches('"');
            let op = if not_null_columns.contains(bare) {
                "="
            } else {
                "IS NOT DISTINCT FROM"
            };
            format!("{outer_qualifier}.{t} {op} {s}")
        })
        .collect();
    format!(
        "EXISTS (SELECT 1 FROM {} WHERE {})",
        src_relation,
        conditions.join(" AND ")
    )
}

/// PS-5 (2026-07-25) — `build_null_safe_membership_predicate` specialised into a
/// runtime-gated fast/safe pair, for the same reason
/// `trigger::merge::null_safe_in_gated` exists. Read `merge::AffectedMatch`'s doc
/// first: the soundness argument, the "exactly one variant does work" contract
/// and the `stmts()` expansion protocol are all identical here.
///
/// The static-only choice this replaces was the *worse* half of that bug. The
/// keyed passthrough secondary (`ops::outer_join_secondary_stmts`) matches the
/// FULL base relation against the tiny `(OLD ∪ NEW)` transition delta, and its
/// target columns come from the NULLABLE side of an outer join — so
/// `provably_not_null_key_columns` can never prove them (it excludes LEFT-join
/// target tables outright and returns an EMPTY set for any RIGHT/FULL join).
/// Every such IMV therefore took the `IS NOT DISTINCT FROM` form *always*, not
/// occasionally: `IS NOT DISTINCT FROM` is in no operator family, so the semi
/// join degrades to a nested loop over the whole base. Measured on PG 18.4,
/// 500 000 base rows x 2 000 changed keys: **23 698 ms** with
/// `Rows Removed by Join Filter: 489 995 000`, versus **40.9 ms** for the
/// sargable `IN` (579x). Gated, the untaken variant costs 6 ms
/// (`Nested Loop Semi Join (never executed)` under a `One-Time Filter`).
///
/// # Why the pair is atomic here
///
/// `AffectedMatch`'s atomicity invariant is carried by a different mechanism on
/// this path. There is no affected table to `TRUNCATE`; the gate and both
/// variants read the statement's transition tables (or the `TRUNCATE`d
/// per-source scratch that mirrors them), which are immutable for the whole
/// duration of the AFTER-STATEMENT trigger that emits these statements. No
/// interleaving can make the gate disagree between the two, so "exactly one
/// variant does work" holds.
///
/// `src_relation_body` is the membership relation **without** an alias: this
/// function attaches `__ck` for the match and `__ng` for the gate, so the two
/// scans of the same body never collide. Callers must not pre-alias it.
pub(crate) fn build_null_safe_membership_predicate_gated(
    outer_qualifier: &str,
    target_cols: &[String],
    source_cols: &[String],
    src_relation_body: &str,
    not_null_columns: &std::collections::HashSet<String>,
) -> super::merge::AffectedMatch {
    debug_assert_eq!(target_cols.len(), source_cols.len());
    let match_relation = format!("{src_relation_body} __ck");
    let safe = build_null_safe_membership_predicate(
        outer_qualifier,
        target_cols,
        source_cols,
        &match_relation,
        not_null_columns,
    );

    // The gate probes the MEMBERSHIP side for NULLs, so it names SOURCE columns —
    // but nullability is only known for the TARGET names, and the two lists can
    // differ. Pair them positionally to carry the target's nullability onto the
    // source column the gate has to reference.
    let unproven_source_cols: Vec<String> = target_cols
        .iter()
        .zip(source_cols.iter())
        .filter(|(t, _)| !not_null_columns.contains(t.trim_matches('"')))
        .map(|(_, s)| s.clone())
        .collect();

    // Every column proven NOT NULL: `build_null_safe_membership_predicate`
    // already returned the plain sargable `IN`, so there is nothing to gate and
    // the emitted SQL stays byte-identical to the pre-fix output.
    let Some(gate) = super::merge::affected_null_key_gate(
        src_relation_body,
        &unproven_source_cols,
        &std::collections::HashSet::new(),
    ) else {
        return super::merge::AffectedMatch {
            fast: safe,
            safe: None,
        };
    };

    let fast = build_membership_predicate(target_cols, source_cols, &match_relation);
    super::merge::AffectedMatch {
        fast: format!("{fast} AND NOT {gate}"),
        safe: Some(format!("{safe} AND {gate}")),
    }
}
