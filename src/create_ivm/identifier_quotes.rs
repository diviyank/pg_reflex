//! One spelling per identifier in the query handed to `create_reflex_ivm`.
//!
//! Sources are matched by text everywhere: `depends_on`, the trigger bodies, the
//! join analysis and the transition-table rewrite. `"t"` and `t` name one
//! relation, so the quotes PostgreSQL itself would not write are removed from
//! the query text before anything reads it.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    Query, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor, Visit,
    VisitMut, Visitor, VisitorMut,
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

/// A neighbour a space separates from the bare word: the last letter of a keyword
/// or of a name, as in `FROM"t"WHERE`.
fn is_letter(neighbour: Option<char>) -> bool {
    neighbour.is_some_and(|c| c.is_alphabetic() || c == '_')
}

/// A neighbour the quoted name is left beside as written: another quoted name, a
/// string, the `&` of `U&"..."`, a digit or a `$`.
fn keeps_the_quotes_beside(neighbour: Option<char>) -> bool {
    neighbour.is_some_and(|c| c.is_numeric() || matches!(c, '"' | '\'' | '&' | '$'))
}

/// A double-quoted identifier that is a plain lower-case word, and whether the
/// bare word would run into the text before or after it.
struct QuotedWord {
    start: usize,
    name: String,
    glued_before: bool,
    glued_after: bool,
}

impl QuotedWord {
    fn end(&self) -> usize {
        self.start + self.name.len() + 2
    }

    fn bare(&self) -> String {
        format!(
            "{}{}{}",
            if self.glued_before { " " } else { "" },
            self.name,
            if self.glued_after { " " } else { "" }
        )
    }
}

fn quoted_word_at(sql: &str, start: usize, name: &str) -> Option<QuotedWord> {
    let end = start + name.len() + 2;
    let before = sql.get(..start)?.chars().next_back();
    let after = sql.get(end..)?.chars().next();
    let written_as_expected = sql.get(start..end)? == format!("\"{name}\"");
    let stands_apart = !keeps_the_quotes_beside(before) && !keeps_the_quotes_beside(after);
    (written_as_expected && stands_apart).then(|| QuotedWord {
        start,
        name: name.to_string(),
        glued_before: is_letter(before),
        glued_after: is_letter(after),
    })
}

/// Every double-quoted identifier that is a plain lower-case word and is a token
/// of its own.
fn quoted_lower_case_words(sql: &str) -> Vec<QuotedWord> {
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
            quoted_word_at(sql, start, &word.value)
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
    for word in quoted_lower_case_words(sql) {
        if !loses_its_quotes(&word.name) {
            continue;
        }
        unquoted.push_str(&sql[copied_up_to..word.start]);
        unquoted.push_str(&word.bare());
        copied_up_to = word.end();
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
/// from `sql`, a space taking their place where the bare word would run into its
/// neighbour: literals, comments and spacing are kept. When a bare name would
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
        .map(|word| word.name)
        .collect();
    candidates.sort();
    candidates.dedup();
    if candidates.is_empty() {
        return None;
    }
    let bare = names_postgres_reads_the_same_bare(&candidates);
    without_needless_quotes(sql, &|name| bare.iter().any(|b| b == name))
}

/// An identifier written `U&"..."`, which the parser reads as `u & "..."`.
struct UnicodeEscapedIdentifier {
    start: usize,
    written: String,
    /// No escape sequence and no `UESCAPE` clause: the name is the text between
    /// the quotes, as for a plain quoted identifier.
    holds_no_escape: bool,
}

fn unicode_escaped_identifiers(sql: &str) -> Vec<UnicodeEscapedIdentifier> {
    let Some(tokens) = tokens_of(sql) else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for (position, window) in tokens.windows(3).enumerate() {
        let (Token::Word(prefix), Token::Ampersand, Token::Word(name)) =
            (&window[0].token, &window[1].token, &window[2].token)
        else {
            continue;
        };
        if prefix.quote_style.is_some()
            || !prefix.value.eq_ignore_ascii_case("u")
            || name.quote_style != Some('"')
        {
            continue;
        }
        let Some(start) = byte_offset(sql, window[0].span.start.line, window[0].span.start.column)
        else {
            continue;
        };
        if !sql
            .get(start..start + 3)
            .is_some_and(|written| written.eq_ignore_ascii_case("u&\""))
        {
            continue;
        }
        let followed_by_uescape = tokens[position + 3..]
            .iter()
            .map(|token| &token.token)
            .find(|token| !matches!(token, Token::Whitespace(_)))
            .is_some_and(
                |token| matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("uescape")),
            );
        found.push(UnicodeEscapedIdentifier {
            start,
            written: format!("{}&\"{}\"", prefix.value, name.value),
            holds_no_escape: !name.value.contains('\\') && !followed_by_uescape,
        });
    }
    found
}

/// `sql` with `U&"name"` written `"name"` wherever the name holds no escape
/// sequence, or `None` when nothing changes.
pub(crate) fn without_idle_unicode_escape_prefixes(sql: &str) -> Option<String> {
    let mut plain = String::with_capacity(sql.len());
    let mut copied_up_to = 0;
    for identifier in unicode_escaped_identifiers(sql) {
        if !identifier.holds_no_escape {
            continue;
        }
        plain.push_str(&sql[copied_up_to..identifier.start]);
        copied_up_to = identifier.start + 2;
    }
    plain.push_str(&sql[copied_up_to..]);
    (plain != sql).then_some(plain)
}

/// The first identifier of `sql` written with Unicode escapes, as written.
pub(crate) fn unicode_escaped_identifier(sql: &str) -> Option<String> {
    unicode_escaped_identifiers(sql)
        .into_iter()
        .next()
        .map(|identifier| identifier.written)
}

/// `sql` with every unquoted word written like one of `spellings` in lower case.
/// Only the case of those letters differs from `sql`.
pub(crate) fn with_words_in_lower_case(sql: &str, spellings: &[String]) -> Option<String> {
    let mut lower_cased = sql.to_string();
    for token in tokens_of(sql)? {
        let Token::Word(word) = &token.token else {
            continue;
        };
        if word.quote_style.is_some() || !spellings.contains(&word.value) {
            continue;
        }
        let start = byte_offset(sql, token.span.start.line, token.span.start.column)?;
        let end = start + word.value.len();
        if sql.get(start..end)? != word.value {
            return None;
        }
        lower_cased.replace_range(start..end, &word.value.to_ascii_lowercase());
    }
    Some(lower_cased)
}

/// The table name of a relation written unqualified or under a schema.
fn table_name_of(relation: &ObjectName) -> Option<&Ident> {
    let parts: Vec<&Ident> = relation
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(name) => Some(name),
            _ => None,
        })
        .collect::<Option<_>>()?;
    match parts.as_slice() {
        [name] | [_, name] => Some(name),
        _ => None,
    }
}

fn is_spelled_as_a_plain_word(name: &Ident) -> bool {
    matches!(name.quote_style, None | Some('"')) && is_lower_case_word(&name.value)
}

/// How PostgreSQL prints a keyword that names a relation: an unreserved one
/// bare, any other one quoted.
#[derive(Clone, Copy, PartialEq)]
enum PrintedAs {
    Bare,
    Quoted,
}

/// The names among `candidates` that are keywords, each with the way
/// `quote_ident` prints it.
fn keywords_as_postgres_prints_them(candidates: &[String]) -> Vec<(String, PrintedAs)> {
    Spi::connect(|client| {
        client
            .select(
                "SELECT k.word::text AS w, (k.catcode = 'U') AS bare \
                 FROM pg_get_keywords() k WHERE k.word = ANY($1::text[])",
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
                    let word = row.get_by_name::<&str, _>("w").ok().flatten()?.to_string();
                    let bare = row.get_by_name::<bool, _>("bare").ok().flatten()?;
                    Some((
                        word,
                        if bare {
                            PrintedAs::Bare
                        } else {
                            PrintedAs::Quoted
                        },
                    ))
                })
                .collect()
            })
            .unwrap_or_default()
    })
}

/// The spelling `name` must take to read as PostgreSQL prints it, when it is not
/// written so already. A quoted keyword PostgreSQL does not accept bare where a
/// relation is named (a reserved one, or one that names a function or a type) is
/// only ever written quoted.
fn respelling(name: &Ident, keywords: &[(String, PrintedAs)]) -> Option<PrintedAs> {
    let printed_as = keywords
        .iter()
        .find(|(word, _)| *word == name.value)
        .map(|(_, printed_as)| *printed_as)?;
    match (name.quote_style, printed_as) {
        (Some(_), PrintedAs::Bare) | (None, PrintedAs::Quoted) => Some(printed_as),
        _ => None,
    }
}

/// The relation names written as a plain lower-case word, quoted or not, the CTEs
/// the statements define and the aliases they give their tables.
#[derive(Default)]
struct PlainWordRelationNames {
    relations: Vec<String>,
    ctes: Vec<String>,
    aliases: Vec<String>,
}

impl Visitor for PlainWordRelationNames {
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
        if let Some(name) = table_name_of(relation) {
            if is_spelled_as_a_plain_word(name) {
                self.relations.push(name.value.clone());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        let alias = match factor {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. }
            | TableFactor::NestedJoin { alias, .. } => alias.as_ref(),
            _ => None,
        };
        if let Some(alias) = alias {
            self.aliases.push(alias.name.value.clone());
        }
        ControlFlow::Continue(())
    }
}

/// Respells the keyword-named relations, and quotes with them every reference to
/// one that gains quotes (`t.c`, `s.t.c`, `t.*`, `s.t.*`), recording each name it
/// changes as it was written. A name that is also a table alias of the statements
/// is respelled where it names the relation only: a qualifier written like it may
/// be the alias. A quoted qualifier of a relation that loses its quotes stays as
/// written: the analysis reads `"zone".c` as a column of `zone`.
struct RelationNameRespeller<'a> {
    keywords: &'a [(String, PrintedAs)],
    relations: &'a [String],
    aliases: &'a [String],
    respelled: Vec<(Ident, PrintedAs)>,
}

impl RelationNameRespeller<'_> {
    fn respell(&mut self, name: &mut Ident, is_a_qualifier: bool) {
        if !self.relations.contains(&name.value) || !is_spelled_as_a_plain_word(name) {
            return;
        }
        let respelling = respelling(name, self.keywords)
            .filter(|printed_as| !is_a_qualifier || *printed_as == PrintedAs::Quoted);
        if let Some(printed_as) = respelling {
            self.respelled.push((name.clone(), printed_as));
            name.quote_style = match printed_as {
                PrintedAs::Bare => None,
                PrintedAs::Quoted => Some('"'),
            };
        }
    }

    fn respell_qualifiers(&mut self, qualifiers: &mut [Ident]) {
        for qualifier in qualifiers {
            if !self.aliases.contains(&qualifier.value) {
                self.respell(qualifier, true);
            }
        }
    }

    fn respell_wildcard_qualifiers(&mut self, name: &mut ObjectName) {
        for part in &mut name.0 {
            if let ObjectNamePart::Identifier(qualifier) = part {
                self.respell_qualifiers(std::slice::from_mut(qualifier));
            }
        }
    }

    fn respell_select_level_wildcards(&mut self, body: &mut SetExpr) {
        match body {
            SetExpr::Select(select) => {
                for item in &mut select.projection {
                    if let SelectItem::QualifiedWildcard(
                        SelectItemQualifiedWildcardKind::ObjectName(name),
                        _,
                    ) = item
                    {
                        self.respell_wildcard_qualifiers(name);
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.respell_select_level_wildcards(left);
                self.respell_select_level_wildcards(right);
            }
            _ => {}
        }
    }
}

impl VisitorMut for RelationNameRespeller<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        self.respell_select_level_wildcards(&mut query.body);
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<()> {
        let is_a_table_name = table_name_of(relation).is_some();
        if let (true, Some(ObjectNamePart::Identifier(name))) =
            (is_a_table_name, relation.0.last_mut())
        {
            self.respell(name, false);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::CompoundIdentifier(parts) => {
                if let Some((_column, qualifiers)) = parts.split_last_mut() {
                    self.respell_qualifiers(qualifiers);
                }
            }
            Expr::QualifiedWildcard(name, _) => self.respell_wildcard_qualifiers(name),
            Expr::Function(function) => {
                if let FunctionArguments::List(list) = &mut function.args {
                    for argument in &mut list.args {
                        let (FunctionArg::Unnamed(value)
                        | FunctionArg::Named { arg: value, .. }
                        | FunctionArg::ExprNamed { arg: value, .. }) = argument;
                        if let FunctionArgExpr::QualifiedWildcard(name) = value {
                            self.respell_wildcard_qualifiers(name);
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn rendered(statements: &[Statement]) -> Vec<String> {
    statements.iter().map(|s| s.to_string()).collect()
}

/// `sql` with the name at `name`'s position spelled as `printed_as`, or `None`
/// when the text there is not the name as the parser read it.
fn respelled_in_place(sql: &mut String, name: &Ident, printed_as: PrintedAs) -> Option<()> {
    let start = byte_offset(sql, name.span.start.line, name.span.start.column)?;
    match printed_as {
        PrintedAs::Bare => {
            let word = quoted_word_at(sql, start, &name.value)?;
            sql.replace_range(start..word.end(), &word.bare());
        }
        PrintedAs::Quoted => {
            let end = start + name.value.len();
            if sql.get(start..end)? != name.value {
                return None;
            }
            sql.replace_range(start..end, &format!("\"{}\"", name.value));
        }
    }
    Some(())
}

/// Spells a keyword that names a relation the way PostgreSQL prints it, which is
/// how a dropped table is reported and how a table of another schema is registered:
/// `"zone"` becomes `zone` (an unreserved keyword) and `position` becomes
/// `"position"`, in the relation name and in every reference qualified by it. The
/// token-level step above leaves keywords alone, because a bare keyword is not
/// accepted in every position; where a relation or a qualifier is named these
/// spellings name one relation. Returns the text with only those quote characters
/// changed, or `None`, leaving `statements` as written, when nothing changes or
/// when the parser does not read the new text as the respelled statements.
pub(crate) fn with_keyword_relations_as_postgres_prints_them(
    sql: &str,
    statements: &mut Vec<Statement>,
) -> Option<String> {
    let mut names = PlainWordRelationNames::default();
    for statement in statements.iter() {
        let _ = statement.visit(&mut names);
    }
    names.relations.retain(|name| !names.ctes.contains(name));
    names.relations.sort();
    names.relations.dedup();
    if names.relations.is_empty() {
        return None;
    }
    let keywords = keywords_as_postgres_prints_them(&names.relations);
    if keywords.is_empty() {
        return None;
    }
    let mut respeller = RelationNameRespeller {
        keywords: &keywords,
        relations: &names.relations,
        aliases: &names.aliases,
        respelled: Vec::new(),
    };
    let mut respelled = statements.clone();
    for statement in &mut respelled {
        let _ = statement.visit(&mut respeller);
    }
    let mut edits = respeller.respelled;
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|(name, _)| (name.span.start.line, name.span.start.column));
    edits.dedup_by_key(|(name, _)| (name.span.start.line, name.span.start.column));
    let mut respelled_sql = sql.to_string();
    for (name, printed_as) in edits.iter().rev() {
        respelled_in_place(&mut respelled_sql, name, *printed_as)?;
    }
    let reparsed = Parser::parse_sql(&PostgreSqlDialect {}, &respelled_sql).ok()?;
    if rendered(&reparsed) != rendered(&respelled) {
        return None;
    }
    *statements = reparsed;
    Some(respelled_sql)
}

#[cfg(test)]
#[path = "../tests/unit_identifier_quotes.rs"]
mod tests;
