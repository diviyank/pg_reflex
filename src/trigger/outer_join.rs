//! Outer-join placements of a written source that the keyword guard of
//! `reflex_build_delta_sql` cannot see.

use super::union_delta::{bare_lower, references_source};
use sqlparser::ast::{
    Expr, Join, JoinConstraint, JoinOperator, Query, SetExpr, Statement, TableFactor,
    TableWithJoins, Visit, Visitor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

pub(super) fn single_query(base_query: &str) -> Option<Box<Query>> {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, base_query).ok()?;
    if statements.len() != 1 {
        return None;
    }
    match statements.pop()? {
        Statement::Query(query) => Some(query),
        _ => None,
    }
}

/// Whether `source_table` is read inside a derived table or a parenthesised
/// join that a `LEFT JOIN` null-extends, at any nesting depth. The transition
/// swap then empties that whole operand, null-extending every preserved row
/// instead of only the rows the write touches. A query holding a `LEFT JOIN`
/// that does not parse counts as inside.
///
/// Only `LEFT JOIN` is inspected: a query holding a `RIGHT JOIN` or a
/// `FULL JOIN` is recomputed on a write to any of its sources.
pub(crate) fn source_inside_left_joined_subtree(
    whitespace_normalized_upper_query: &str,
    base_query: &str,
    source_table: &str,
) -> bool {
    let holds_left_join = whitespace_normalized_upper_query.contains("LEFT JOIN")
        || whitespace_normalized_upper_query.contains("LEFT OUTER JOIN");
    if !holds_left_join {
        return false;
    }
    let Some(query) = single_query(base_query) else {
        return true;
    };
    let mut finder = LeftJoinedSubtreeFinder {
        source: bare_lower(source_table),
        found: false,
    };
    let _ = query.visit(&mut finder);
    finder.found
}

struct LeftJoinedSubtreeFinder {
    source: String,
    found: bool,
}

impl LeftJoinedSubtreeFinder {
    fn inspect(&mut self, from_item: &TableWithJoins) -> ControlFlow<()> {
        let null_extends_source = from_item.joins.iter().any(|join| {
            matches!(
                join.join_operator,
                JoinOperator::Left(_) | JoinOperator::LeftOuter(_)
            ) && !matches!(join.relation, TableFactor::Table { .. })
                && references_source(&join.relation, &self.source)
        });
        if null_extends_source {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn inspect_selects(&mut self, body: &SetExpr) -> ControlFlow<()> {
        match body {
            SetExpr::Select(select) => {
                for from_item in &select.from {
                    self.inspect(from_item)?;
                }
                ControlFlow::Continue(())
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.inspect_selects(left)?;
                self.inspect_selects(right)
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

impl Visitor for LeftJoinedSubtreeFinder {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        self.inspect_selects(&query.body)
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        match factor {
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.inspect(table_with_joins),
            _ => ControlFlow::Continue(()),
        }
    }
}

/// Whether the query can drop a joined row because of the row of
/// `source_table` it holds: its WHERE, or the ON condition of a join that is
/// not a LEFT JOIN, reads a column of the source. `source_table` is a table a
/// top-level LEFT JOIN introduces.
///
/// The query with the source swapped for a transition table then lists only
/// the groups of the changed rows that pass, and misses the group a row leaves
/// by failing the condition, or enters by no longer matching a failing row. An
/// unqualified column counts as read from the source; so does anything this
/// cannot resolve.
pub(crate) fn left_joined_source_is_filtered(base_query: &str, source_table: &str) -> bool {
    let Some(query) = single_query(base_query) else {
        return true;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return true;
    };
    let source = bare_lower(source_table);
    let introduces_source = |join: &&Join| match &join.relation {
        TableFactor::Table { name, .. } => bare_lower(&name.to_string()) == source,
        _ => false,
    };
    let joins = || select.from.iter().flat_map(|from_item| &from_item.joins);
    let Some(source_join) = joins().find(introduces_source) else {
        return true;
    };
    let qualifier = match &source_join.relation {
        TableFactor::Table {
            alias: Some(alias), ..
        } => alias.name.value.to_lowercase(),
        _ => source.clone(),
    };
    let reads_source = |expr: &Expr| {
        let mut finder = QualifiedColumnFinder {
            qualifier: &qualifier,
        };
        expr.visit(&mut finder).is_break()
    };
    let filtering_join_reads_source = joins()
        .filter(|join| !std::ptr::eq(*join, source_join))
        .any(|join| match &join.join_operator {
            JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => false,
            JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => match constraint {
                JoinConstraint::On(condition) => reads_source(condition),
                JoinConstraint::None => false,
                _ => true,
            },
            JoinOperator::CrossJoin(_) => false,
            _ => true,
        });
    filtering_join_reads_source || select.selection.as_ref().is_some_and(reads_source)
}

/// Breaks on a column reference that is unqualified or qualified by
/// `qualifier`, subqueries included.
struct QualifiedColumnFinder<'a> {
    qualifier: &'a str,
}

impl Visitor for QualifiedColumnFinder<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(_) => ControlFlow::Break(()),
            Expr::CompoundIdentifier(ids)
                if ids[..ids.len() - 1]
                    .iter()
                    .any(|part| part.value.to_lowercase() == self.qualifier) =>
            {
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit_trigger_outer_join.rs"]
mod tests;
