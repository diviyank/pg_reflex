//! Sources a query reads through a subquery used as an expression.

use super::outer_join::single_query;
use super::union_delta::{bare_lower, references_source};
use sqlparser::ast::{Expr, Query, Visit, Visitor};
use std::ops::ControlFlow;

/// Whether `source_table` is read by a subquery that stands where a value is
/// expected: `IN (SELECT …)`, `EXISTS (…)`, a quantified or scalar comparison,
/// a scalar subquery of the select list, at any nesting depth. A row of such a
/// table yields no row of the result; it decides which rows of the other tables
/// pass, or what value they carry, so the transition swap computes the result
/// over the changed rows of the wrong table. A query holding a nested `SELECT`
/// that does not parse counts as reading the source this way.
pub(crate) fn source_read_by_expression_subquery(
    whitespace_normalized_upper_query: &str,
    base_query: &str,
    source_table: &str,
) -> bool {
    let may_hold_subquery = whitespace_normalized_upper_query.matches("SELECT").count() > 1
        || whitespace_normalized_upper_query.contains("TABLE ");
    if !may_hold_subquery {
        return false;
    }
    let Some(query) = single_query(base_query) else {
        return true;
    };
    let mut finder = ExpressionSubqueryFinder {
        source: bare_lower(source_table),
        enclosing_expressions: 0,
        found: false,
    };
    let _ = query.visit(&mut finder);
    finder.found
}

struct ExpressionSubqueryFinder {
    source: String,
    enclosing_expressions: usize,
    found: bool,
}

impl Visitor for ExpressionSubqueryFinder {
    type Break = ();

    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<()> {
        self.enclosing_expressions += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<()> {
        self.enclosing_expressions -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if self.enclosing_expressions > 0 && references_source(query, &self.source) {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
#[path = "../tests/unit_trigger_expression_subquery.rs"]
mod tests;
