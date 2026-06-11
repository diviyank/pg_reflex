//! Operand-scoped delta queries for aggregate IMVs whose FROM clause contains a
//! `UNION ALL` subquery.
//!
//! The standard trigger-time rewriter [`replace_source_with_transition`] swaps
//! the mutated source for its transition table across the WHOLE `base_query`.
//! For a `UNION ALL` subquery that is wrong: only the operand containing the
//! source changed, but the swap leaves the sibling operands referencing their
//! full base tables. Aggregate maintenance then MERGE-adds the result as if it
//! were a delta — double-counting every sibling-operand row (silent wrong
//! answers when a sibling contributes non-zero to a SUM) and scanning the full
//! base (O(base)).
//!
//! `UNION ALL` is a multiset sum, so SUM/COUNT distribute over it: the delta of
//! the aggregate equals the aggregate of the changed operand's delta alone.
//! [`scoped_delta_query`] therefore prunes the subquery to only the operand(s)
//! that reference the mutated source before applying the transition swap.
//!
//! Non-`ALL` set operations (`UNION`/`INTERSECT`/`EXCEPT`) are NOT distributive,
//! so they cannot be pruned; [`aggregate_source_requires_recompute`] flags them
//! so the caller falls back to a correct full recompute.

use crate::sql_writer::identifier::replace_source_with_transition;
use sqlparser::ast::{
    Select, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, Visit, Visitor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

/// Bare, unquoted, lower-cased final component of a possibly schema-qualified,
/// possibly quoted identifier. `"schema"."Tbl"` -> `tbl`, `schema.tbl` -> `tbl`.
fn bare_lower(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('"')
        .to_lowercase()
}

/// Visitor that records whether any table relation matching `target_bare` is
/// referenced anywhere in the visited subtree — including nested subqueries, so
/// a scalar-subquery filter `WHERE x = (SELECT … FROM source)` still counts the
/// operand as depending on `source`.
struct RelationRefFinder {
    target_bare: String,
    found: bool,
}

impl Visitor for RelationRefFinder {
    type Break = ();
    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, .. } = factor {
            if bare_lower(&name.to_string()) == self.target_bare {
                self.found = true;
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

fn references_source(node: &SetExpr, target_bare: &str) -> bool {
    let mut finder = RelationRefFinder {
        target_bare: target_bare.to_string(),
        found: false,
    };
    let _ = node.visit(&mut finder);
    finder.found
}

/// Collect the operands of a left-associative `UNION ALL` chain. A mixed
/// operator/quantifier subtree is left intact as a single operand.
fn flatten_union_all(body: &SetExpr, out: &mut Vec<SetExpr>) {
    if let SetExpr::SetOperation {
        op: SetOperator::Union,
        set_quantifier: SetQuantifier::All,
        left,
        right,
    } = body
    {
        flatten_union_all(left, out);
        flatten_union_all(right, out);
    } else {
        out.push(body.clone());
    }
}

/// Fold operands back into a left-associative `UNION ALL` chain.
fn rebuild_union_all(operands: Vec<SetExpr>) -> SetExpr {
    let mut it = operands.into_iter();
    let mut acc = it.next().expect("rebuild_union_all: at least one operand");
    for operand in it {
        acc = SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            left: Box::new(acc),
            right: Box::new(operand),
        };
    }
    acc
}

#[derive(PartialEq)]
enum Outcome {
    /// The subtree was pruned (a `UNION ALL` lost a sibling that does not
    /// reference the source).
    Pruned,
    /// A non-distributive set-op references the source — caller must recompute.
    NeedsRecompute,
    /// Nothing to do for the source in this subtree.
    NoChange,
}

/// Walk a `SetExpr` in place, pruning every `UNION ALL` subquery that references
/// the source down to only its source-referencing operands.
fn scope_setexpr(node: &mut SetExpr, target_bare: &str) -> Outcome {
    match node {
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            ..
        } => {
            if !references_source(node, target_bare) {
                // Source is in none of the operands: this UNION ALL is a JOIN
                // partner of the changed side (like a dimension table) and must
                // stay whole for the join delta to be correct.
                return Outcome::NoChange;
            }
            let mut operands = Vec::new();
            flatten_union_all(node, &mut operands);
            let total = operands.len();
            let mut kept: Vec<SetExpr> = Vec::with_capacity(total);
            for mut operand in operands {
                if references_source(&operand, target_bare) {
                    if scope_setexpr(&mut operand, target_bare) == Outcome::NeedsRecompute {
                        return Outcome::NeedsRecompute;
                    }
                    kept.push(operand);
                }
            }
            let pruned = kept.len() < total;
            *node = rebuild_union_all(kept);
            if pruned {
                Outcome::Pruned
            } else {
                Outcome::NoChange
            }
        }
        SetExpr::SetOperation { .. } => {
            // UNION / INTERSECT / EXCEPT (non-ALL): not distributive.
            if references_source(node, target_bare) {
                Outcome::NeedsRecompute
            } else {
                Outcome::NoChange
            }
        }
        SetExpr::Select(select) => scope_select(select, target_bare),
        _ => Outcome::NoChange,
    }
}

/// Walk a `SELECT`'s FROM clause, descending into derived subqueries.
fn scope_select(select: &mut Select, target_bare: &str) -> Outcome {
    let mut outcome = Outcome::NoChange;
    for twj in select.from.iter_mut() {
        match scope_table_factor(&mut twj.relation, target_bare) {
            Outcome::NeedsRecompute => return Outcome::NeedsRecompute,
            Outcome::Pruned => outcome = Outcome::Pruned,
            Outcome::NoChange => {}
        }
        for join in twj.joins.iter_mut() {
            match scope_table_factor(&mut join.relation, target_bare) {
                Outcome::NeedsRecompute => return Outcome::NeedsRecompute,
                Outcome::Pruned => outcome = Outcome::Pruned,
                Outcome::NoChange => {}
            }
        }
    }
    outcome
}

fn scope_table_factor(factor: &mut TableFactor, target_bare: &str) -> Outcome {
    if let TableFactor::Derived { subquery, .. } = factor {
        scope_setexpr(&mut subquery.body, target_bare)
    } else {
        Outcome::NoChange
    }
}

/// Parse `base_query` and prune every `UNION ALL` FROM-subquery to the operands
/// that reference `source_table`. Returns the rewritten SQL plus a flag for
/// whether a non-distributive set-op forces a full recompute.
fn analyze(base_query: &str, source_table: &str) -> Option<(Outcome, String)> {
    let target_bare = bare_lower(source_table);
    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, base_query).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let Statement::Query(query) = &mut stmts[0] else {
        return None;
    };
    // The aggregate-IMV base_query is a grouped SELECT; a top-level set op is
    // handled by decomposition, not here.
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    let outcome = scope_select(select, &target_bare);
    let sql = match outcome {
        Outcome::Pruned => stmts[0].to_string(),
        _ => base_query.to_string(),
    };
    Some((outcome, sql))
}

/// True when the mutated source sits inside a non-distributive set-op
/// (`UNION`/`INTERSECT`/`EXCEPT`) subquery, where no incremental delta is valid
/// and the caller must fall back to a full recompute. Applies to both aggregate
/// and passthrough IMVs.
pub(crate) fn source_requires_recompute(base_query: &str, source_table: &str) -> bool {
    matches!(
        analyze(base_query, source_table),
        Some((Outcome::NeedsRecompute, _))
    )
}

/// Build the trigger-time delta query for `source_table`, scoping any
/// `UNION ALL` FROM-subquery to the changed operand before applying the
/// transition-table swap. Falls back to the whole-query swap when there is no
/// prunable set-op subquery (the correct delta for plain JOINs / single-source
/// aggregates).
pub(crate) fn scoped_delta_query(
    base_query: &str,
    source_table: &str,
    transition_tbl: &str,
) -> String {
    match analyze(base_query, source_table) {
        Some((Outcome::Pruned, pruned_sql)) => {
            replace_source_with_transition(&pruned_sql, source_table, transition_tbl)
        }
        _ => replace_source_with_transition(base_query, source_table, transition_tbl),
    }
}
