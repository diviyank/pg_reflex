//! Expressions the sqlparser printer writes back as text PostgreSQL reads
//! differently, and integers the parser reads differently from PostgreSQL. pg_reflex
//! builds its maintenance SQL from the printed form of every clause, except in a
//! plain SELECT, stored as the caller's text: there only the WHERE conjuncts the
//! analysis keeps per source are printed.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, SetExpr, Spanned, Statement, UnaryOperator, Value, ValueWithSpan, Visit, Visitor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, Tokenizer};

use super::identifier_quotes::byte_offset;
use crate::sql_analyzer::split_top_level_and;

/// The characters PostgreSQL reads as one operator when they follow each other.
const OPERATOR_CHARACTERS: &str = "+-*/<>=~!@#%^&|`?";

fn is_sign(character: char) -> bool {
    character == '+' || character == '-'
}

/// The first character of `operand_text` when PostgreSQL reads it together with
/// `operator` written directly before it: as a comment (`--`) or as one operator
/// (`@-`). Two signs other than `--` are read one by one.
fn joined_character(operator: &str, operand_text: &str) -> Option<char> {
    let joined = operand_text
        .chars()
        .next()
        .filter(|character| OPERATOR_CHARACTERS.contains(*character))?;
    let two_signs = operator.chars().all(is_sign) && is_sign(joined);
    (!two_signs || (operator == "-" && joined == '-')).then_some(joined)
}

/// The operator and the operand of a prefix operator the printer writes directly
/// before its operand.
fn printed_without_a_space(expr: &Expr) -> Option<(String, &Expr)> {
    let Expr::UnaryOp { op, expr: operand } = expr else {
        return None;
    };
    let operator = op.to_string();
    (*op != UnaryOperator::PGPostfixFactorial && expr.to_string() == format!("{operator}{operand}"))
        .then_some((operator, operand.as_ref()))
}

/// `expr` with a space between its leading prefix operators, as a caller writes them.
fn with_operators_apart(expr: &Expr) -> String {
    match printed_without_a_space(expr) {
        Some((operator, operand)) if printed_without_a_space(operand).is_some() => {
            format!("{operator} {}", with_operators_apart(operand))
        }
        _ => expr.to_string(),
    }
}

/// `expr` with the operand of each leading prefix operator in parentheses where
/// PostgreSQL would read the operator together with it.
fn with_operands_in_parentheses(expr: &Expr) -> String {
    match printed_without_a_space(expr) {
        Some((operator, operand)) => {
            let operand = with_operands_in_parentheses(operand);
            match joined_character(&operator, &operand) {
                Some(_) => format!("{operator}({operand})"),
                None => format!("{operator}{operand}"),
            }
        }
        None => expr.to_string(),
    }
}

/// Whether `text` is an expression pg_reflex writes back with its meaning.
fn is_written_back_unchanged(text: &str) -> bool {
    let parsed = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(text)
        .and_then(|mut parser| parser.parse_expr());
    let Ok(expr) = parsed else {
        return false;
    };
    let mut reprinted = ReprintedDifferently {
        sql: text,
        first: None,
    };
    let _ = expr.visit(&mut reprinted);
    reprinted.first.is_none()
}

/// A prefix operator printed directly before an operand that starts with an operator
/// character.
fn prefix_operator_joined_to_its_operand(expr: &Expr) -> Option<String> {
    let (operator, operand) = printed_without_a_space(expr)?;
    let joined = joined_character(&operator, &operand.to_string())?;
    let read_as = if operator == "-" && joined == '-' {
        "the start of a comment"
    } else {
        "one operator"
    };
    let rewritten = with_operands_in_parentheses(expr);
    let remedy = if is_written_back_unchanged(&rewritten) {
        let plain_value = match operand {
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: negated_twice,
            } if operator == "-" => Some(with_operands_in_parentheses(negated_twice))
                .filter(|value| is_written_back_unchanged(value))
                .map(|value| format!(", or write {value} alone"))
                .unwrap_or_default(),
            _ => String::new(),
        };
        format!(
            "put the operand in parentheses, {rewritten} in place of {}{plain_value}",
            with_operators_apart(expr)
        )
    } else {
        format!(
            "put the operand of each prefix operator that directly follows another operator \
             in parentheses, as in {operator}({joined}v)"
        )
    };
    Some(format!(
        "pg_reflex writes two operators in a row back as '{operator}{joined}', which \
         PostgreSQL reads as {read_as}. Fix: {remedy}."
    ))
}

fn in_decimal(digits: &str, radix: u32) -> String {
    match u128::from_str_radix(&digits.replace('_', ""), radix) {
        Ok(value) => format!(": {value}"),
        Err(_) => String::new(),
    }
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
    Some(format!(
        "pg_reflex writes the hexadecimal integer 0x{digits} back as the bit string \
         X'{digits}', which is not a number. Fix: write the integer in decimal{}.",
        in_decimal(digits, 16)
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
            Expr::UnaryOp { .. } => prefix_operator_joined_to_its_operand(expr),
            Expr::Value(literal) => hexadecimal_integer(literal, self.sql),
            _ => None,
        };
        match self.first {
            Some(_) => ControlFlow::Break(()),
            None => ControlFlow::Continue(()),
        }
    }
}

/// An integer written `0X1F`, `0o17` or `0b101` at or after `from`, which the parser
/// reads as the number 0 followed by a name: the alias of a select item that ends
/// with it. PostgreSQL reads one integer.
fn integer_read_as_zero_and_a_name(sql: &str, from: Option<Location>) -> Option<String> {
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    tokens.windows(2).find_map(|pair| {
        let (Token::Number(number, _), Token::Word(name)) = (&pair[0].token, &pair[1].token) else {
            return None;
        };
        let radix = match name.value.chars().next()? {
            'x' | 'X' => 16,
            'o' | 'O' => 8,
            'b' | 'B' => 2,
            _ => return None,
        };
        let start_of = |location: Location| byte_offset(sql, location.line, location.column);
        let directly_follows =
            start_of(pair[0].span.start)? + number.len() == start_of(pair[1].span.start)?;
        let rewritten = from.is_none_or(|from| pair[0].span.start >= from);
        (number == "0" && name.quote_style.is_none() && directly_follows && rewritten).then(|| {
            format!(
                "pg_reflex reads the integer {number}{name} as the number 0 followed by the \
                 name {name}, where PostgreSQL reads one integer. Fix: write the integer in \
                 decimal{}.",
                in_decimal(&name.value[1..], radix),
                name = name.value
            )
        })
    })
}

/// Which clauses of the query PostgreSQL runs from the caller's text.
#[derive(Clone, Copy)]
pub(super) enum RunAsWritten<'a> {
    Nothing,
    /// The select list, the FROM clause and the top-level WHERE conjuncts other than
    /// those at `printed_where_conjuncts`.
    PlainSelect {
        printed_where_conjuncts: &'a [usize],
    },
}

/// Why the first expression of `statements`, the parse of `sql`, that pg_reflex
/// would write back with another meaning, or reads with another meaning than
/// PostgreSQL, cannot be maintained, with its rewrite.
pub(super) fn first_expression_reprinted_differently(
    statements: &[Statement],
    sql: &str,
    run_as_written: RunAsWritten,
) -> Option<String> {
    let mut reprinted = ReprintedDifferently { sql, first: None };
    let mut rewritten_from: Option<Location> = None;
    for statement in statements {
        let mut rewritten_clauses = statement.clone();
        let mut printed_conjuncts: Vec<Expr> = Vec::new();
        if let RunAsWritten::PlainSelect {
            printed_where_conjuncts,
        } = run_as_written
        {
            if let Statement::Query(query) = &mut rewritten_clauses {
                if let SetExpr::Select(select) = query.body.as_mut() {
                    select.projection.clear();
                    select.from.clear();
                    let mut conjuncts: Vec<Expr> = Vec::new();
                    if let Some(selection) = &select.selection {
                        split_top_level_and(selection, &mut conjuncts);
                        rewritten_from = Some(selection.span().start);
                    } else {
                        rewritten_from = Some(Location::new(u64::MAX, u64::MAX));
                    }
                    if printed_where_conjuncts
                        .iter()
                        .all(|position| *position < conjuncts.len())
                    {
                        select.selection = None;
                        printed_conjuncts = printed_where_conjuncts
                            .iter()
                            .map(|position| conjuncts[*position].clone())
                            .collect();
                    }
                }
            }
        }
        let found = rewritten_clauses.visit(&mut reprinted).is_break()
            || printed_conjuncts
                .iter()
                .any(|conjunct| conjunct.visit(&mut reprinted).is_break());
        if found {
            return reprinted.first;
        }
    }
    integer_read_as_zero_and_a_name(sql, rewritten_from)
}
