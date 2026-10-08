//! The outer joins of a stored query, as they bear on a write to one source.

use super::union_delta::{bare_lower, references_source};
use super::{follows_outer_join_keyword, upper_with_single_spaces};
use crate::sql_writer::identifier::token_names_source;
use sqlparser::ast::{
    JoinOperator, Query, SetExpr, Statement, TableFactor, TableWithJoins, Visit, Visitor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

fn single_query(base_query: &str) -> Option<Box<Query>> {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, base_query).ok()?;
    if statements.len() != 1 {
        return None;
    }
    match statements.pop()? {
        Statement::Query(query) => Some(query),
        _ => None,
    }
}

/// What the joins of a query mean for a write to one of its sources, read from
/// the parsed query: a comment, a newline or a string literal never changes it.
#[derive(Default)]
pub(crate) struct OuterJoins {
    pub holds_right_join: bool,
    pub holds_full_join: bool,
    /// The source is the table directly joined by a LEFT or FULL JOIN.
    pub source_is_null_extended_table: bool,
    /// The source is read inside a derived table or a parenthesised join that a
    /// LEFT JOIN null-extends. The transition swap then empties that whole
    /// operand, null-extending every preserved row instead of only the rows
    /// the write touches.
    pub source_inside_left_joined_subtree: bool,
    /// Table references naming the source, at any depth.
    pub source_reads: usize,
}

/// Joins at any depth count: derived tables, parenthesised joins, CTE bodies,
/// set-operation operands and subqueries of expressions.
///
/// The stored query of an IMV always parses, since it was parsed at create.
/// Text that does not parse is read by keyword instead, which is exact for the
/// single-spaced, comment-free text the aggregate path generates.
pub(crate) fn outer_joins(base_query: &str, source_table: &str) -> OuterJoins {
    let Some(query) = single_query(base_query) else {
        return outer_joins_by_keyword(base_query, source_table);
    };
    let mut finder = OuterJoinFinder {
        source_lower: source_table.to_lowercase(),
        source_bare: bare_lower(source_table),
        found: OuterJoins::default(),
    };
    let _ = query.visit(&mut finder);
    finder.found
}

pub(crate) fn holds_full_join(base_query: &str) -> bool {
    outer_joins(base_query, "").holds_full_join
}

fn outer_joins_by_keyword(base_query: &str, source_table: &str) -> OuterJoins {
    let upper = upper_with_single_spaces(base_query);
    let holds_left_join = upper.contains("LEFT JOIN") || upper.contains("LEFT OUTER");
    let holds_full_join = upper.contains("FULL JOIN") || upper.contains("FULL OUTER");
    OuterJoins {
        holds_right_join: upper.contains("RIGHT JOIN") || upper.contains("RIGHT OUTER"),
        holds_full_join,
        source_is_null_extended_table: (holds_left_join || holds_full_join)
            && follows_outer_join_keyword(&upper, source_table),
        source_inside_left_joined_subtree: holds_left_join,
        source_reads: 0,
    }
}

#[cfg(test)]
pub(crate) fn source_inside_left_joined_subtree(
    _whitespace_normalized_upper_query: &str,
    base_query: &str,
    source_table: &str,
) -> bool {
    outer_joins(base_query, source_table).source_inside_left_joined_subtree
}

struct OuterJoinFinder {
    source_lower: String,
    source_bare: String,
    found: OuterJoins,
}

impl OuterJoinFinder {
    fn names_source(&self, factor: &TableFactor) -> bool {
        matches!(factor, TableFactor::Table { name, .. }
            if token_names_source(&name.to_string().to_lowercase(), &self.source_lower))
    }

    fn inspect(&mut self, from_item: &TableWithJoins) {
        for join in &from_item.joins {
            let is_left = matches!(
                join.join_operator,
                JoinOperator::Left(_) | JoinOperator::LeftOuter(_)
            );
            let is_full = matches!(join.join_operator, JoinOperator::FullOuter(_));
            self.found.holds_full_join |= is_full;
            self.found.holds_right_join |= matches!(
                join.join_operator,
                JoinOperator::Right(_) | JoinOperator::RightOuter(_)
            );
            let joins_plain_table = matches!(join.relation, TableFactor::Table { .. });
            self.found.source_is_null_extended_table |=
                (is_left || is_full) && self.names_source(&join.relation);
            self.found.source_inside_left_joined_subtree |= is_left
                && !joins_plain_table
                && references_source(&join.relation, &self.source_bare);
        }
    }

    fn inspect_selects(&mut self, body: &SetExpr) {
        match body {
            SetExpr::Select(select) => {
                for from_item in &select.from {
                    self.inspect(from_item);
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.inspect_selects(left);
                self.inspect_selects(right);
            }
            _ => {}
        }
    }
}

impl Visitor for OuterJoinFinder {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        self.inspect_selects(&query.body);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = factor
        {
            self.inspect(table_with_joins);
        }
        if self.names_source(factor) {
            self.found.source_reads += 1;
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
#[path = "../tests/unit_trigger_outer_join.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/unit_trigger_outer_join_kinds.rs"]
mod kind_tests;
