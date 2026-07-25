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
