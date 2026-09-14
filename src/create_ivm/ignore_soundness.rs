//! A1: is it sound to ignore this source?
//!
//! An ignored source whose columns determine the IMV's contents can silently
//! invalidate it — a change the IMV never sees alters what the IMV should hold.
//! That is the shape behind the 2026-09 silent-wipe incident: an IMV declared
//! `ignore_sources: [demand_planning]` while filtering on
//! `demand_planning.status`. The status changed during a supply-plan run,
//! nothing refreshed the IMV, and a later partition-scoped rebuild from the
//! base query wrote the slice to zero rows — successfully and permanently.
//!
//! This deliberately does NOT reuse `collect_imv_relevant_columns`: that helper
//! excludes the WHERE clause by design and returns an empty map for CTE and
//! wildcard queries, so a check built on it would silently exempt exactly the
//! queries most likely to be unsound.
//!
//! Unresolvable input is reported as UNSOUND, never as sound. Guessing "sound"
//! reintroduces the class this check exists to block.

use sqlparser::ast::{
    Expr, JoinConstraint, JoinOperator, Select, SelectItem, SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Compare a catalog name against an ignored entry, matching both the qualified
/// and the bare form. Deliberately BROADER than the runtime skip: it is also
/// case-insensitive and matches across differing schema qualifiers, so
/// `alp.demand_planning`, `other.Demand_Planning` and `demand_planning` all
/// agree. Every widening here is in the fail-toward-refuse direction.
fn same_source(candidate: &str, ignored: &str) -> bool {
    let bare = |s: &str| s.rsplit('.').next().unwrap_or(s).to_ascii_lowercase();
    candidate.eq_ignore_ascii_case(ignored) || bare(candidate) == bare(ignored)
}

/// Record `(ignored_name, reason)` the first time an ignored source is
/// implicated. First reason wins, so the clause walked first (WHERE) supplies
/// the most legible explanation.
fn flag(
    candidate: &str,
    reason: String,
    ignored_clean: &[String],
    out: &mut Vec<(String, String)>,
) {
    if let Some(ig) = ignored_clean.iter().find(|ig| same_source(candidate, ig)) {
        if !out.iter().any(|(s, _)| s == ig) {
            out.push((ig.clone(), reason));
        }
    }
}

/// Every ignored entry, with one reason. Used when nothing at all about the
/// statement can be enumerated, so no entry may be narrowed away.
fn flag_all(ignored_clean: &[String], reason: &str) -> Vec<(String, String)> {
    ignored_clean
        .iter()
        .map(|s| (s.clone(), reason.to_string()))
        .collect()
}

/// Returns `(source, reason)` for every ignored source that can determine the
/// IMV's contents. Empty means sound.
///
/// Parses and analyzes `sql` itself, for callers that hold only the query text
/// (the audit). The create path already has both and calls
/// [`unsound_ignored_sources_parsed`] instead — one parse, one analysis, and no
/// second place where the dialect could diverge.
/// The text-only entry point has no in-crate caller yet: the create path holds
/// a parsed query and uses `unsound_ignored_sources_parsed`. It is the surface
/// the planned audit finding consumes, and the unit tests exercise it.
#[allow(dead_code)]
pub fn unsound_ignored_sources(sql: &str, ignored_clean: &[String]) -> Vec<(String, String)> {
    if ignored_clean.is_empty() {
        return Vec::new();
    }
    let stmts = match Parser::parse_sql(&PostgreSqlDialect {}, sql) {
        Ok(s) => s,
        Err(e) => return flag_all(ignored_clean, &format!("query could not be parsed ({e})")),
    };
    match crate::sql_analyzer::analyze(&stmts) {
        Ok(analysis) => unsound_ignored_sources_parsed(&stmts, &analysis, ignored_clean),
        Err(e) => flag_all(ignored_clean, &format!("query could not be analyzed ({e})")),
    }
}

/// [`unsound_ignored_sources`] over an already-parsed, already-analyzed query.
pub(crate) fn unsound_ignored_sources_parsed(
    stmts: &[Statement],
    analysis: &crate::sql_analyzer::SqlAnalysis,
    ignored_clean: &[String],
) -> Vec<(String, String)> {
    if ignored_clean.is_empty() {
        return Vec::new();
    }

    // Every relation the analyzer can see anywhere in the statement — CTE
    // bodies and subqueries included. Two uses, both fail-toward-refuse:
    //
    //   * an ignored name that appears nowhere in it is not a source of this
    //     IMV at all, so ignoring it is a no-op and stays sound even when the
    //     query as a whole cannot be attributed;
    //   * when a column reference cannot be attributed, it may belong to ANY
    //     of these relations — including one visible only inside a subquery —
    //     so every ignored one among them is implicated.
    let all_sources: &[String] = &analysis.sources;

    let unresolvable = |reason: &str| -> Vec<(String, String)> {
        ignored_clean
            .iter()
            .filter(|ig| all_sources.iter().any(|s| same_source(s, ig)))
            .map(|s| (s.clone(), reason.to_string()))
            .collect()
    };
    // A reference the walk cannot attribute implicates every ignored relation
    // of the statement, not just the top-level ones: `ColumnRefCollector`
    // descends into subqueries, so the owning relation may not be top-level.
    let flag_unattributed = |reason: &str, out: &mut Vec<(String, String)>| {
        for src in all_sources {
            flag(src, reason.to_string(), ignored_clean, out);
        }
    };

    let select: &Select = match stmts.first() {
        Some(Statement::Query(q)) => {
            // 2a. CTEs make attribution unsafe: a source referenced only inside
            // a CTE body is invisible to the top-level walk below.
            if q.with.is_some() {
                return unresolvable("query could not be attributed (CTE)");
            }
            match q.body.as_ref() {
                SetExpr::Select(s) => s,
                _ => return unresolvable("query could not be attributed (set operation)"),
            }
        }
        _ => return unresolvable("query is not a single SELECT"),
    };

    // 2b. A wildcard projection hides which columns are exposed.
    if select.projection.iter().any(|i| {
        matches!(
            i,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    }) {
        return unresolvable("query could not be attributed (wildcard projection)");
    }

    let aliases = crate::sql_analyzer::alias_map(select);
    let top_level = crate::sql_analyzer::top_level_sources(select);
    let mut out: Vec<(String, String)> = Vec::new();

    // 3. Walk every content-determining clause. Each collected reference is
    //    resolved to a top-level source through the alias map; an unqualified
    //    reference cannot be attributed and is treated as unsound for every
    //    ignored source that is a top-level source of this query.
    let mut clauses: Vec<(&str, &Expr)> = Vec::new();
    if let Some(w) = &select.selection {
        clauses.push(("WHERE", w));
    }
    if let Some(h) = &select.having {
        clauses.push(("HAVING", h));
    }
    for twj in &select.from {
        for join in &twj.joins {
            // Exhaustive on purpose: a future sqlparser variant must fail to
            // compile here, not fall through as silently sound.
            match crate::sql_analyzer::join_constraint(&join.join_operator) {
                None => {}
                Some(JoinConstraint::On(e)) => clauses.push(("JOIN ON", e)),
                Some(JoinConstraint::Using(cols)) => {
                    // A USING column names a column in both relations at
                    // once and cannot be attributed to either one — treated
                    // like any other unattributable reference.
                    let col_list = cols
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    flag_unattributed(
                        &format!(
                            "JOIN USING ({col_list}) references a column in both \
                             relations and cannot be attributed to one"
                        ),
                        &mut out,
                    );
                }
                Some(JoinConstraint::Natural) => {
                    // NATURAL equates every common column implicitly, so no
                    // single column or relation can be named.
                    flag_unattributed(
                        "NATURAL JOIN implicitly equates every common column and \
                         cannot be attributed to one relation",
                        &mut out,
                    );
                }
                Some(JoinConstraint::None) => {}
            }
        }
    }

    for (clause, expr) in clauses {
        for parts in crate::sql_analyzer::collect_column_refs(expr) {
            let (qualifier, col) = match parts.as_slice() {
                [c] => (None, c.clone()),
                [q, c, ..] => (Some(q.clone()), c.clone()),
                [] => continue,
            };
            match qualifier {
                // A qualifier that resolves to no top-level relation belongs to
                // a scope this walk cannot see: a subquery or derived table
                // (ColumnRefCollector descends into nested queries, `aliases`
                // is deliberately top-level only), or an alias whose quoted
                // spelling does not match the collector's unquoted one.
                // Resolving it to itself and moving on would drop the reference
                // as sound — a silent pass in the one branch where attribution
                // actually failed. Fail toward refuse instead, exactly as the
                // unqualified-column arm below does.
                Some(q) => match aliases.get(&q) {
                    Some(resolved) => flag(
                        resolved,
                        format!("column {col} referenced in {clause}"),
                        ignored_clean,
                        &mut out,
                    ),
                    None if top_level.iter().any(|s| same_source(s, &q)) => flag(
                        &q,
                        format!("column {col} referenced in {clause}"),
                        ignored_clean,
                        &mut out,
                    ),
                    None => flag_unattributed(
                        &format!(
                            "qualifier {q} in {clause} could not be attributed \
                             to a top-level source"
                        ),
                        &mut out,
                    ),
                },
                None => {
                    // Unqualified: we cannot say which source owns it — and the
                    // collector descends into subqueries, so it may not even be
                    // a top-level one.
                    flag_unattributed(
                        &format!(
                            "unqualified column {col} in {clause} could not be \
                             attributed to a source"
                        ),
                        &mut out,
                    );
                }
            }
        }
    }

    // 4. Inner / cross join participation alone is content-determining: if the
    //    ignored source loses a row, the IMV must lose the rows that joined to
    //    it. `JoinOperator::Join` is what a bare `JOIN ... ON` parses to,
    //    `Inner` is the explicit `INNER JOIN` spelling, and a CROSS JOIN
    //    multiplies by the ignored side's row count.
    for twj in &select.from {
        for join in &twj.joins {
            if matches!(
                join.join_operator,
                JoinOperator::Join(_) | JoinOperator::Inner(_) | JoinOperator::CrossJoin(_)
            ) {
                if let TableFactor::Table { name, .. } = &join.relation {
                    flag(
                        &name.to_string(),
                        "participates in an INNER JOIN (row removal changes the IMV)".to_string(),
                        ignored_clean,
                        &mut out,
                    );
                }
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::unsound_ignored_sources;

    fn ignored(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn no_ignored_sources_is_always_sound() {
        assert!(unsound_ignored_sources("this is not sql at all", &[]).is_empty());
    }

    #[test]
    fn where_reference_is_unsound_and_names_the_column() {
        let out = unsound_ignored_sources(
            "SELECT s.a FROM ss s JOIN dp ON dp.id = s.a WHERE dp.status = 'v'",
            &ignored(&["dp"]),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, "dp");
        assert!(out[0].1.contains("status"), "reason: {}", out[0].1);
    }

    #[test]
    fn plain_join_on_is_flagged() {
        let out = unsound_ignored_sources(
            "SELECT s.a FROM ss s JOIN dp ON dp.id = s.a",
            &ignored(&["dp"]),
        );
        assert_eq!(out.len(), 1, "a bare JOIN ... ON must be flagged: {out:?}");
    }

    #[test]
    fn unreferenced_source_is_sound() {
        let out = unsound_ignored_sources("SELECT a FROM ss", &ignored(&["other"]));
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn unparseable_query_is_unsound() {
        let out = unsound_ignored_sources("SELECT a FROM ss WHERE ((", &ignored(&["dp"]));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].1.contains("parsed"), "reason: {}", out[0].1);
    }

    #[test]
    fn cte_query_is_unsound() {
        let out = unsound_ignored_sources(
            "WITH v AS (SELECT id FROM dp) SELECT s.a FROM ss s JOIN v ON v.id = s.a",
            &ignored(&["dp"]),
        );
        assert_eq!(out.len(), 1);
        assert!(out[0].1.contains("CTE"), "reason: {}", out[0].1);
    }

    #[test]
    fn an_unattributable_query_still_spares_a_name_that_is_not_a_source() {
        let out = unsound_ignored_sources(
            "WITH v AS (SELECT id FROM dp) SELECT s.a FROM ss s JOIN v ON v.id = s.a",
            &ignored(&["not_a_table_here"]),
        );
        assert!(
            out.is_empty(),
            "ignoring a name absent from the query is a no-op, not an unsound ignore: {out:?}"
        );
    }

    /// C1(a) — the ignored source lives only in a WHERE subquery. The
    /// qualifier `d` belongs to a scope the top-level alias map cannot see;
    /// resolving it to itself would drop the reference as sound.
    #[test]
    fn subquery_scoped_qualifier_is_unsound() {
        let out = unsound_ignored_sources(
            "SELECT ss.a, ss.qty FROM ss \
             WHERE ss.id IN (SELECT d.id FROM demand_planning d WHERE d.status = 'validated')",
            &ignored(&["demand_planning"]),
        );
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].0, "demand_planning");
    }

    /// C1(b) — the ignored source is wrapped in a derived table. `t` is a
    /// TableFactor::Derived, so it never enters the alias map.
    #[test]
    fn derived_table_qualifier_is_unsound() {
        let out = unsound_ignored_sources(
            "SELECT ss.a, t.status FROM ss \
             JOIN (SELECT id, status FROM demand_planning) t ON t.id = ss.id \
             WHERE t.status = 'validated'",
            &ignored(&["demand_planning"]),
        );
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].0, "demand_planning");
    }

    /// C1(c) — a quoted alias. `Ident::to_string()` re-adds the quotes for the
    /// alias map key while `Ident.value` does not for the collected qualifier,
    /// so the lookup misses and the fallback must catch it.
    #[test]
    fn quoted_alias_qualifier_is_unsound() {
        let out = unsound_ignored_sources(
            "SELECT \"DP\".id, ss.qty FROM demand_planning \"DP\", ss \
             WHERE \"DP\".status = 'validated'",
            &ignored(&["demand_planning"]),
        );
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].0, "demand_planning");
    }

    /// The C1 fallback must not fire for a resolvable qualifier: a plain alias
    /// over a top-level table still resolves, and an unrelated ignored name
    /// stays sound.
    #[test]
    fn resolvable_qualifier_does_not_trip_the_fallback() {
        let out = unsound_ignored_sources(
            "SELECT ss.a FROM ss JOIN dp d ON d.id = ss.a WHERE d.status = 'v'",
            &ignored(&["elsewhere"]),
        );
        assert!(out.is_empty(), "{out:?}");
    }

    /// M7 — the accepted false negative, pinned as a test rather than prose: a
    /// source contributing only projected / grouped columns via an outer join,
    /// with no WHERE, HAVING or ON reference, is ALLOWED. Widening this to a
    /// refusal would force '!' on effectively every real use of ignore_sources.
    #[test]
    fn projection_only_reference_is_allowed() {
        let out = unsound_ignored_sources(
            "SELECT ss.a, dp.label FROM ss LEFT JOIN dp ON TRUE",
            &ignored(&["dp"]),
        );
        assert!(
            out.is_empty(),
            "a projection-only outer-joined source stays allowed: {out:?}"
        );
    }

    #[test]
    fn cross_joined_source_is_unsound() {
        let out = unsound_ignored_sources(
            "SELECT ss.a, dp.label FROM ss CROSS JOIN dp",
            &ignored(&["dp"]),
        );
        assert_eq!(
            out.len(),
            1,
            "a cross join multiplies by the ignored side: {out:?}"
        );
    }

    #[test]
    fn set_operation_is_unsound() {
        let out = unsound_ignored_sources(
            "SELECT a FROM ss UNION ALL SELECT id FROM dp",
            &ignored(&["dp"]),
        );
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn wildcard_projection_is_unsound() {
        let out = unsound_ignored_sources("SELECT * FROM ss", &ignored(&["ss"]));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn unqualified_column_is_unsound_only_for_sources_of_the_query() {
        let out = unsound_ignored_sources(
            "SELECT a FROM ss WHERE flag",
            &ignored(&["ss", "elsewhere"]),
        );
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].0, "ss");
    }

    #[test]
    fn schema_qualified_and_bare_names_agree() {
        let out = unsound_ignored_sources(
            "SELECT s.a FROM ss s JOIN alp.dp dp ON dp.id = s.a WHERE dp.status = 'v'",
            &ignored(&["dp"]),
        );
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn left_join_on_reference_is_flagged() {
        let out = unsound_ignored_sources(
            "SELECT s.a FROM ss s LEFT JOIN dp ON dp.id = s.a",
            &ignored(&["dp"]),
        );
        assert_eq!(out.len(), 1, "{out:?}");
    }

    #[test]
    fn join_using_on_ignored_source_is_unsound() {
        let out = unsound_ignored_sources(
            "SELECT a.x, dp.status FROM a LEFT JOIN dp USING (id)",
            &ignored(&["dp"]),
        );
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].0, "dp");
    }

    #[test]
    fn natural_join_on_ignored_source_is_unsound() {
        let out =
            unsound_ignored_sources("SELECT a.x FROM a NATURAL LEFT JOIN dp", &ignored(&["dp"]));
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].0, "dp");
    }

    #[test]
    fn join_using_does_not_flag_a_non_source() {
        let out = unsound_ignored_sources(
            "SELECT a.x, dp.status FROM a LEFT JOIN dp USING (id)",
            &ignored(&["elsewhere"]),
        );
        assert!(out.is_empty(), "{out:?}");
    }
}
