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
/// and the bare form — the same rule the runtime skip uses, so
/// `alp.demand_planning` and `demand_planning` agree.
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

/// Returns `(source, reason)` for every ignored source that can determine the
/// IMV's contents. Empty means sound.
pub fn unsound_ignored_sources(sql: &str, ignored_clean: &[String]) -> Vec<(String, String)> {
    if ignored_clean.is_empty() {
        return Vec::new();
    }

    let flag_all = |reason: &str| -> Vec<(String, String)> {
        ignored_clean
            .iter()
            .map(|s| (s.clone(), reason.to_string()))
            .collect()
    };

    // 1. Parse failure is unresolvable, and nothing can be enumerated to narrow
    //    it -> every ignored source is unsound.
    let stmts = match Parser::parse_sql(&PostgreSqlDialect {}, sql) {
        Ok(s) => s,
        Err(e) => return flag_all(&format!("query could not be parsed ({e})")),
    };

    // Every relation the analyzer can see anywhere in the statement — CTE
    // bodies and subqueries included. An ignored name that appears nowhere in
    // it is not a source of this IMV at all, so ignoring it is a no-op and
    // stays sound even when the query as a whole cannot be attributed. `None`
    // means the statement could not be analyzed, so nothing may be narrowed.
    let all_sources: Option<Vec<String>> =
        crate::sql_analyzer::analyze(&stmts).ok().map(|a| a.sources);
    let unresolvable = |reason: &str| -> Vec<(String, String)> {
        match &all_sources {
            None => flag_all(reason),
            Some(sources) => ignored_clean
                .iter()
                .filter(|ig| sources.iter().any(|s| same_source(s, ig)))
                .map(|s| (s.clone(), reason.to_string()))
                .collect(),
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
    let sources = crate::sql_analyzer::top_level_sources(select);
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
            if let Some(JoinConstraint::On(e)) =
                crate::sql_analyzer::join_constraint(&join.join_operator)
            {
                clauses.push(("JOIN ON", e));
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
                Some(q) => {
                    let resolved = aliases.get(&q).cloned().unwrap_or(q);
                    flag(
                        &resolved,
                        format!("column {col} referenced in {clause}"),
                        ignored_clean,
                        &mut out,
                    );
                }
                None => {
                    // Unqualified: we cannot say which source owns it. Fail
                    // toward unsound for every ignored source of this query.
                    for src in &sources {
                        flag(
                            src,
                            format!(
                                "unqualified column {col} in {clause} \
                                 could not be attributed to a source"
                            ),
                            ignored_clean,
                            &mut out,
                        );
                    }
                }
            }
        }
    }

    // 4. INNER JOIN participation alone is content-determining: if the ignored
    //    source loses a row, the IMV must lose the rows that joined to it.
    //    `JoinOperator::Join` is what a bare `JOIN ... ON` parses to; `Inner`
    //    is the explicit `INNER JOIN` spelling. Both are inner joins.
    for twj in &select.from {
        for join in &twj.joins {
            if matches!(
                join.join_operator,
                JoinOperator::Join(_) | JoinOperator::Inner(_)
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
}
