//! Expressions the sqlparser printer writes back as text PostgreSQL reads
//! differently. pg_reflex builds its maintenance SQL from the printed form of every
//! clause but the select list and the FROM clause of a plain SELECT, which
//! PostgreSQL runs as the caller wrote them.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, SetExpr, Statement, UnaryOperator, Value, ValueWithSpan, Visit, Visitor,
};

use super::identifier_quotes::byte_offset;

/// The characters PostgreSQL reads as one operator when they follow each other.
const OPERATOR_CHARACTERS: &str = "+-*/<>=~!@#%^&|`?";

fn is_sign(character: char) -> bool {
    character == '+' || character == '-'
}

/// A prefix operator printed directly before an operand that starts with an operator
/// character. PostgreSQL reads the two as a comment (`--`) or as one operator
/// (`@-`), except two signs other than `--`, which it reads one by one.
fn prefix_operator_joined_to_its_operand(
    operator: &UnaryOperator,
    operand: &Expr,
    printed: &str,
) -> Option<String> {
    if *operator == UnaryOperator::PGPostfixFactorial {
        return None;
    }
    let operator = operator.to_string();
    let joined = printed
        .strip_prefix(&operator)?
        .chars()
        .next()
        .filter(|character| OPERATOR_CHARACTERS.contains(*character))?;
    let two_signs = operator.chars().all(is_sign) && is_sign(joined);
    if two_signs && !(operator == "-" && joined == '-') {
        return None;
    }
    let (read_as, plain_value) = match operand {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: negated_twice,
        } if operator == "-" => (
            "the start of a comment".to_string(),
            format!(", or {negated_twice} alone"),
        ),
        _ if operator == "-" && joined == '-' => {
            ("the start of a comment".to_string(), String::new())
        }
        _ => (format!("the operator {operator}{joined}"), String::new()),
    };
    Some(format!(
        "pg_reflex writes the expression '{operator} {operand}' back as '{printed}', where \
         PostgreSQL reads '{operator}{joined}' as {read_as}. Fix: put the operand in \
         parentheses, {operator}({operand}){plain_value}."
    ))
}

/// An integer written `0x1F`, which the parser holds as the string of its digits and
/// prints as the bit string `X'1F'`. A bit string written `X'1F'` is printed as is.
fn hexadecimal_integer(literal: &ValueWithSpan, sql: &str) -> Option<String> {
    let Value::HexStringLiteral(digits) = &literal.value else {
        return None;
    };
    let start = literal.span.start;
    let written_as_a_bit_string = byte_offset(sql, start.line, start.column)
        .and_then(|offset| sql.get(offset..))
        .is_some_and(|text| text.starts_with(['x', 'X']));
    if written_as_a_bit_string {
        return None;
    }
    let in_decimal = match u128::from_str_radix(digits, 16) {
        Ok(value) => format!(": {value}"),
        Err(_) => String::new(),
    };
    Some(format!(
        "pg_reflex writes the hexadecimal integer 0x{digits} back as the bit string \
         X'{digits}', which is not a number. Fix: write the integer in decimal{in_decimal}."
    ))
}

struct ReprintedDifferently<'a> {
    sql: &'a str,
    first: Option<String>,
}

impl Visitor for ReprintedDifferently<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        self.first = match expr {
            Expr::UnaryOp { op, expr: operand } => {
                prefix_operator_joined_to_its_operand(op, operand, &expr.to_string())
            }
            Expr::Value(literal) => hexadecimal_integer(literal, self.sql),
            _ => None,
        };
        match self.first {
            Some(_) => ControlFlow::Break(()),
            None => ControlFlow::Continue(()),
        }
    }
}

/// Which clauses of the query PostgreSQL runs from the caller's text.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum RunAsWritten {
    Nothing,
    SelectListAndFrom,
}

/// Why the first expression of `statements`, the parse of `sql`, that pg_reflex
/// would write back with another meaning cannot be maintained, with its rewrite.
pub(super) fn first_expression_reprinted_differently(
    statements: &[Statement],
    sql: &str,
    run_as_written: RunAsWritten,
) -> Option<String> {
    let mut reprinted = ReprintedDifferently { sql, first: None };
    for statement in statements {
        let mut rewritten_clauses = statement.clone();
        if run_as_written == RunAsWritten::SelectListAndFrom {
            if let Statement::Query(query) = &mut rewritten_clauses {
                if let SetExpr::Select(select) = query.body.as_mut() {
                    select.projection.clear();
                    select.from.clear();
                }
            }
        }
        if rewritten_clauses.visit(&mut reprinted).is_break() {
            break;
        }
    }
    reprinted.first
}
