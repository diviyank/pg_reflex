//! Outer-join placements of a written source that the keyword guard of
//! `reflex_build_delta_sql` cannot see.

use super::union_delta::{bare_lower, references_source};
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

#[cfg(test)]
#[path = "../tests/unit_trigger_outer_join.rs"]
mod tests;
