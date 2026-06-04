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
