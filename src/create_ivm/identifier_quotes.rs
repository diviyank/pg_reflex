//! One spelling per identifier in the query handed to `create_reflex_ivm`.
//!
//! Sources are matched by text everywhere: `depends_on`, the trigger bodies, the
//! join analysis and the transition-table rewrite. `"t"` and `t` name one
//! relation, so the quotes PostgreSQL itself would not write are removed from
//! the query text before anything reads it.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Statement, Visit, VisitMut, Visitor, VisitorMut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::keywords::ALL_KEYWORDS;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};
use std::ops::ControlFlow;

fn tokens_of(sql: &str) -> Option<Vec<TokenWithSpan>> {
    Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()
        .ok()
}

fn is_lower_case_word(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_parser_keyword(name: &str) -> bool {
    ALL_KEYWORDS
        .binary_search(&name.to_ascii_uppercase().as_str())
        .is_ok()
}

fn byte_offset(sql: &str, line: u64, column: u64) -> Option<usize> {
    let line_start: usize = sql
        .split_inclusive('\n')
        .take(line.checked_sub(1)? as usize)
        .map(str::len)
        .sum();
    let in_line = sql.get(line_start..)?;
    let skipped = column.checked_sub(1)? as usize;
    match in_line.char_indices().nth(skipped) {
        Some((offset, _)) => Some(line_start + offset),
        None => (in_line.chars().count() == skipped).then_some(sql.len()),
    }
}

fn glues_to_a_bare_word(neighbour: Option<char>) -> bool {
    neighbour.is_some_and(|c| c.is_alphanumeric() || matches!(c, '_' | '$' | '"' | '\'' | '&'))
}

/// `(byte offset, name)` of every double-quoted identifier that is a plain
/// lower-case word and stands apart from its neighbours.
fn quoted_lower_case_words(sql: &str) -> Vec<(usize, String)> {
    let Some(tokens) = tokens_of(sql) else {
        return Vec::new();
    };
    tokens
        .iter()
        .filter_map(|token| {
            let Token::Word(word) = &token.token else {
                return None;
            };
            if word.quote_style != Some('"') || !is_lower_case_word(&word.value) {
                return None;
            }
            let start = byte_offset(sql, token.span.start.line, token.span.start.column)?;
            let end = start + word.value.len() + 2;
            let written = sql.get(start..end)?;
            let stands_apart = !glues_to_a_bare_word(sql[..start].chars().next_back())
                && !glues_to_a_bare_word(sql[end..].chars().next());
            (written == format!("\"{}\"", word.value) && stands_apart)
                .then(|| (start, word.value.clone()))
        })
        .collect()
}

fn significant_tokens(sql: &str) -> Option<Vec<Token>> {
    Some(
        tokens_of(sql)?
            .into_iter()
            .map(|token| token.token)
            .filter(|token| !matches!(token, Token::Whitespace(_)))
            .collect(),
    )
}

fn same_tokens_apart_from_quotes(written: &str, unquoted: &str) -> bool {
    let (Some(before), Some(after)) = (significant_tokens(written), significant_tokens(unquoted))
    else {
        return false;
    };
    before.len() == after.len()
        && before.iter().zip(&after).all(|pair| match pair {
            (Token::Word(a), Token::Word(b)) => {
                a.value == b.value && (a.quote_style == b.quote_style || b.quote_style.is_none())
            }
            (a, b) => a == b,
        })
}

fn single_statement_text(sql: &str) -> Option<String> {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    (statements.len() == 1).then(|| statements.remove(0).to_string())
}

fn with_quotes_removed(sql: &str, loses_its_quotes: &dyn Fn(&str) -> bool) -> String {
    let mut unquoted = String::with_capacity(sql.len());
    let mut copied_up_to = 0;
    for (start, name) in quoted_lower_case_words(sql) {
        if !loses_its_quotes(&name) {
            continue;
        }
        unquoted.push_str(&sql[copied_up_to..start]);
        unquoted.push_str(&name);
        copied_up_to = start + name.len() + 2;
    }
    unquoted.push_str(&sql[copied_up_to..]);
    unquoted
}

/// Whether the parser reads `unquoted` as `written`: same tokens, and the same
/// statement once both are written back out. A name the parser treats as a
/// keyword when bare fails here.
fn parser_reads_them_the_same(
    written: &str,
    unquoted: &str,
    loses_its_quotes: &dyn Fn(&str) -> bool,
) -> bool {
    same_tokens_apart_from_quotes(written, unquoted)
        && match (
            single_statement_text(written),
            single_statement_text(unquoted),
        ) {
            (Some(before), Some(after)) => {
                with_quotes_removed(&before, loses_its_quotes)
                    == with_quotes_removed(&after, loses_its_quotes)
            }
            _ => false,
        }
}

/// `sql` with the quotes removed from every quoted identifier `reads_the_same_bare`
/// accepts, or `None` when nothing changes. Only the quote characters differ
/// from `sql`: literals, comments and spacing are kept. When a bare name would
/// change what the parser reads, the names it knows as keywords keep their quotes.
pub(crate) fn without_needless_quotes(
    sql: &str,
    reads_the_same_bare: &dyn Fn(&str) -> bool,
) -> Option<String> {
    let attempt = |loses_its_quotes: &dyn Fn(&str) -> bool| {
        let unquoted = with_quotes_removed(sql, loses_its_quotes);
        (unquoted != sql && parser_reads_them_the_same(sql, &unquoted, loses_its_quotes))
            .then_some(unquoted)
    };
    attempt(reads_the_same_bare)
        .or_else(|| attempt(&|name| reads_the_same_bare(name) && !is_parser_keyword(name)))
}

/// The names among `candidates` that PostgreSQL reads the same with and without
/// quotes in every position: `quote_ident` leaves them bare and they are no
/// keyword of any class (an unreserved keyword still changes how a bare column
/// label parses).
fn names_postgres_reads_the_same_bare(candidates: &[String]) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                "SELECT w::text AS w FROM unnest($1::text[]) AS w \
                 WHERE quote_ident(w) = w \
                   AND NOT EXISTS (SELECT 1 FROM pg_get_keywords() k WHERE k.word = w)",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        crate::sql_writer::identifier::format_pg_text_array(candidates),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .map(|rows| {
                rows.filter_map(|row| {
                    row.get_by_name::<&str, _>("w")
                        .ok()
                        .flatten()
                        .map(str::to_string)
                })
                .collect()
            })
            .unwrap_or_default()
    })
}

/// The query with one spelling per identifier, or `None` when it already has.
pub(crate) fn with_one_spelling_per_identifier(sql: &str) -> Option<String> {
    let mut candidates: Vec<String> = quoted_lower_case_words(sql)
        .into_iter()
        .map(|(_, name)| name)
        .collect();
    candidates.sort();
    candidates.dedup();
    if candidates.is_empty() {
        return None;
    }
    let bare = names_postgres_reads_the_same_bare(&candidates);
    without_needless_quotes(sql, &|name| bare.iter().any(|b| b == name))
}

/// The quoted last part of a relation name written unqualified or under
/// `public`. A relation of another schema is registered from the catalog,
/// whatever the query wrote.
fn quoted_name_of_a_public_relation(relation: &ObjectName) -> Option<&Ident> {
    let parts: Vec<&Ident> = relation
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(name) => Some(name),
            _ => None,
        })
        .collect::<Option<_>>()?;
    let name = match parts.as_slice() {
        [name] => name,
        [schema, name] if schema.value == "public" => name,
        _ => return None,
    };
    (name.quote_style == Some('"')).then_some(*name)
}

/// The double-quoted plain lower-case names the statements read as a relation,
/// apart from the CTEs they define.
fn quoted_relation_names(statements: &[Statement]) -> Vec<String> {
    #[derive(Default)]
    struct Names {
        relations: Vec<String>,
        ctes: Vec<String>,
    }
    impl Visitor for Names {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            if let Some(with) = &query.with {
                self.ctes.extend(
                    with.cte_tables
                        .iter()
                        .map(|cte| cte.alias.name.value.clone()),
                );
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
            if let Some(name) = quoted_name_of_a_public_relation(relation) {
                if is_lower_case_word(&name.value) {
                    self.relations.push(name.value.clone());
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut names = Names::default();
    for statement in statements {
        let _ = statement.visit(&mut names);
    }
    names.relations.retain(|name| !names.ctes.contains(name));
    names.relations.sort();
    names.relations.dedup();
    names.relations
}

/// The keywords among `candidates` PostgreSQL accepts without quotes where a
/// relation is named: the unreserved ones and those that cannot name a function
/// or a type. A reserved word is only ever written quoted, and a word that is
/// no keyword has lost its quotes already.
fn keywords_postgres_reads_bare_as_a_relation(candidates: &[String]) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                "SELECT w::text AS w FROM unnest($1::text[]) AS w \
                 WHERE EXISTS (SELECT 1 FROM pg_get_keywords() k \
                               WHERE k.word = w AND k.catcode IN ('U', 'C'))",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        crate::sql_writer::identifier::format_pg_text_array(candidates),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .map(|rows| {
                rows.filter_map(|row| {
                    row.get_by_name::<&str, _>("w")
                        .ok()
                        .flatten()
                        .map(str::to_string)
                })
                .collect()
            })
            .unwrap_or_default()
    })
}

struct RelationNameUnquoter<'a> {
    read_bare: &'a [String],
}

impl VisitorMut for RelationNameUnquoter<'_> {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<()> {
        let names_a_keyword = quoted_name_of_a_public_relation(relation)
            .is_some_and(|name| self.read_bare.contains(&name.value));
        if let (true, Some(ObjectNamePart::Identifier(name))) =
            (names_a_keyword, relation.0.last_mut())
        {
            name.quote_style = None;
        }
        ControlFlow::Continue(())
    }
}

fn rendered(statements: &[Statement]) -> Vec<String> {
    statements.iter().map(|s| s.to_string()).collect()
}

/// Removes the quotes of `"zone"` where it names a relation. The token-level
/// step above keeps the quotes of a keyword, because a bare keyword is not
/// accepted in every position; where a relation is named PostgreSQL accepts
/// the bare word, so `zone` and `"zone"` register one source there. The
/// statements are left as written when the parser does not read the bare
/// names back the same.
pub(crate) fn unquote_relation_names(statements: &mut Vec<Statement>) {
    let candidates = quoted_relation_names(statements);
    if candidates.is_empty() {
        return;
    }
    let read_bare = keywords_postgres_reads_bare_as_a_relation(&candidates);
    if read_bare.is_empty() {
        return;
    }
    let mut unquoted = statements.clone();
    for statement in &mut unquoted {
        let _ = statement.visit(&mut RelationNameUnquoter {
            read_bare: &read_bare,
        });
    }
    let text = rendered(&unquoted);
    let parser_reads_it_back = Parser::parse_sql(&PostgreSqlDialect {}, &text.join("; "))
        .is_ok_and(|reparsed| rendered(&reparsed) == text);
    if parser_reads_it_back {
        *statements = unquoted;
    }
}

#[cfg(test)]
#[path = "../tests/unit_identifier_quotes.rs"]
mod tests;
