//! The outer joins of a stored query, as they bear on a write to one source.

use super::union_delta::{bare_lower, references_source};
use crate::sql_writer::identifier::token_names_source;
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Join, JoinConstraint,
    JoinOperator, Query, SetExpr, Statement, TableFactor, TableWithJoins, Visit, Visitor,
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
    /// A bare word of an expression, or a `name.*` argument, is spelled like
    /// the source or like its alias: a column of that name, or the table read
    /// as a whole row. The transition swap cannot tell the first from a table
    /// reference, and the changed rows do not carry the row type of the source.
    pub source_name_stands_as_a_value: bool,
}

/// Joins at any depth count: derived tables, parenthesised joins, CTE bodies,
/// set-operation operands and subqueries of expressions.
///
/// The stored query of an IMV always parses, since it was parsed at create.
/// Text that does not parse gets the answer that recomputes in full on every
/// write: nothing read from it can be trusted to scope a delta.
pub(crate) fn outer_joins(base_query: &str, source_table: &str) -> OuterJoins {
    let Some(query) = single_query(base_query) else {
        return OuterJoins {
            holds_right_join: true,
            holds_full_join: true,
            ..OuterJoins::default()
        };
    };
    let mut finder = OuterJoinFinder {
        source_lower: source_table.to_lowercase(),
        source_bare: bare_lower(source_table),
        source_aliases: Vec::new(),
        words_standing_as_values: Vec::new(),
        found: OuterJoins::default(),
    };
    let _ = query.visit(&mut finder);
    finder.found.source_name_stands_as_a_value |= finder
        .words_standing_as_values
        .iter()
        .any(|word| finder.source_aliases.contains(word));
    finder.found
}

/// The relations a function call receives as `name.*`.
fn whole_row_arguments(function: &Function) -> Vec<String> {
    let FunctionArguments::List(list) = &function.args else {
        return Vec::new();
    };
    list.args
        .iter()
        .filter_map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name))
            | FunctionArg::Named {
                arg: FunctionArgExpr::QualifiedWildcard(name),
                ..
            }
            | FunctionArg::ExprNamed {
                arg: FunctionArgExpr::QualifiedWildcard(name),
                ..
            } => Some(name.to_string()),
            _ => None,
        })
        .collect()
}

pub(crate) fn holds_full_join(base_query: &str) -> bool {
    outer_joins(base_query, "").holds_full_join
}

#[cfg(test)]
pub(crate) fn source_inside_left_joined_subtree(
    whitespace_normalized_upper_query: &str,
    base_query: &str,
    source_table: &str,
) -> bool {
    match single_query(base_query) {
        Some(_) => outer_joins(base_query, source_table).source_inside_left_joined_subtree,
        None => {
            whitespace_normalized_upper_query.contains("LEFT JOIN")
                || whitespace_normalized_upper_query.contains("LEFT OUTER")
        }
    }
}

struct OuterJoinFinder {
    source_lower: String,
    source_bare: String,
    /// Lower-cased aliases of the table references naming the source.
    source_aliases: Vec<String>,
    /// Lower-cased bare words of expressions, and names of `name.*` arguments.
    words_standing_as_values: Vec<String>,
    found: OuterJoins,
}

impl OuterJoinFinder {
    fn note_word_standing_as_a_value(&mut self, written: &str) {
        let word = written.to_lowercase();
        self.found.source_name_stands_as_a_value |= token_names_source(&word, &self.source_lower);
        self.words_standing_as_values.push(bare_lower(&word));
    }

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
            if let TableFactor::Table {
                alias: Some(alias), ..
            } = factor
            {
                self.source_aliases.push(alias.name.value.to_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(word) => self.note_word_standing_as_a_value(&word.to_string()),
            Expr::QualifiedWildcard(name, _) => {
                self.note_word_standing_as_a_value(&name.to_string())
            }
            Expr::Function(function) => {
                for name in whole_row_arguments(function) {
                    self.note_word_standing_as_a_value(&name);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Whether the query can drop a joined row because of the row of
/// `source_table` it holds. `source_table` is a table a top-level LEFT JOIN
/// introduces. A table LEFT JOINed on a column of the source carries, in each
/// joined row, the row the source's row selects, so it depends on the source
/// too, and so on down the chain. The joined row can be dropped when the
/// WHERE, or the ON condition of a join that is not a LEFT JOIN, reads a column
/// of the source or of a table depending on it.
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
    let Some(source_qualifier) = qualifier_of(&source_join.relation) else {
        return true;
    };
    let other_joins = || joins().filter(|join| !std::ptr::eq(*join, source_join));
    let is_left_join = |join: &Join| {
        matches!(
            join.join_operator,
            JoinOperator::Left(_) | JoinOperator::LeftOuter(_)
        )
    };

    let mut depending_on_source = vec![source_qualifier];
    loop {
        let mut grew = false;
        for join in other_joins().filter(|join| is_left_join(join)) {
            if !join_condition_reads(join, &depending_on_source) {
                continue;
            }
            let Some(qualifier) = qualifier_of(&join.relation) else {
                return true;
            };
            if !depending_on_source.contains(&qualifier) {
                depending_on_source.push(qualifier);
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }

    let filtering_join_reads_source = other_joins()
        .filter(|join| !is_left_join(join))
        .any(|join| join_condition_reads(join, &depending_on_source));
    filtering_join_reads_source
        || select
            .selection
            .as_ref()
            .is_some_and(|condition| reads_column_of(condition, &depending_on_source))
}

/// The lower-cased name a relation's columns are qualified by: its alias, or
/// the bare name of an unaliased table.
fn qualifier_of(relation: &TableFactor) -> Option<String> {
    match relation {
        TableFactor::Table {
            alias: Some(alias), ..
        }
        | TableFactor::Derived {
            alias: Some(alias), ..
        }
        | TableFactor::NestedJoin {
            alias: Some(alias), ..
        } => Some(alias.name.value.to_lowercase()),
        TableFactor::Table { name, .. } => Some(bare_lower(&name.to_string())),
        _ => None,
    }
}

/// Whether the condition of `join` reads a column under one of `qualifiers`.
/// A cross join reads nothing; `USING`, `NATURAL` and any other join form
/// count as reading.
fn join_condition_reads(join: &Join, qualifiers: &[String]) -> bool {
    match &join.join_operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint) => match constraint {
            JoinConstraint::On(condition) => reads_column_of(condition, qualifiers),
            JoinConstraint::None => false,
            _ => true,
        },
        JoinOperator::CrossJoin(_) => false,
        _ => true,
    }
}

fn reads_column_of(expr: &Expr, qualifiers: &[String]) -> bool {
    let mut finder = QualifiedColumnFinder { qualifiers };
    expr.visit(&mut finder).is_break()
}

/// Breaks on a column reference that is unqualified or qualified by one of
/// `qualifiers`, subqueries included.
struct QualifiedColumnFinder<'a> {
    qualifiers: &'a [String],
}

impl Visitor for QualifiedColumnFinder<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(_) => ControlFlow::Break(()),
            Expr::CompoundIdentifier(ids)
                if ids[..ids.len() - 1]
                    .iter()
                    .any(|part| self.qualifiers.contains(&part.value.to_lowercase())) =>
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

#[cfg(test)]
#[path = "../tests/unit_trigger_outer_join_kinds.rs"]
mod kind_tests;
