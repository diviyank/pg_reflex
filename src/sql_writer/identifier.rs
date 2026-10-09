//! Canonical identifier handling for pg_reflex.
//!
//! Centralises the quoting, NAMEDATALEN truncation, schema split, and
//! word-boundary identifier rewriting so that callers share one implementation
//! instead of subtly different copies.
//!
//! Every helper here is deliberately small and pure so callers can compose
//! them. The rewriters:
//! - `replace_identifier` — case-sensitive word-boundary substitution that
//!   reads string literals as code. No query is rewritten with it: it is the
//!   reference the tests of the literal-aware rewriters compare against.
//! - [`strip_redundant_bare_alias`] — strips a redundant `AS <bare>` clause
//!   that the user wrote after a schema-qualified source ref.
//! - [`substitute_identifier_ci`] — case-insensitive word-level substitution
//!   used by partition-constraint expression rewrites.
//! - [`replace_source_with_transition`] — the composition used by the
//!   trigger-time delta rewriter (strip alias → replace qualified → replace
//!   bare).
//!
//! Naming utilities ([`quote`], [`safe`], [`split_qualified`], [`format_pg_text_array`])
//! are bundled here so callers do not have to remember which module they live in.

use std::ops::Range;

use super::keyword_scan::string_literal_spans;

/// Forward-only cursor over ascending, non-overlapping byte ranges that a
/// rewrite must copy through untouched.
struct UntouchedSpans<'a> {
    remaining: &'a [Range<usize>],
}

impl UntouchedSpans<'_> {
    fn end_of_span_containing(&mut self, position: usize) -> Option<usize> {
        while let Some((first, rest)) = self.remaining.split_first() {
            if first.end > position {
                break;
            }
            self.remaining = rest;
        }
        self.remaining
            .first()
            .filter(|span| span.start <= position)
            .map(|span| span.end)
    }
}

/// Split a potentially schema-qualified name into (Option<schema>, name).
///
/// `"my_view"` -> `(None, "my_view")`
/// `"myschema.my_view"` -> `(Some("myschema"), "my_view")`
pub fn split_qualified(name: &str) -> (Option<&str>, &str) {
    match name.find('.') {
        Some(pos) => (Some(&name[..pos]), &name[pos + 1..]),
        None => (None, name),
    }
}

/// Quote a potentially schema-qualified name for use in SQL DDL/DML.
///
/// `"my_view"` -> `"\"my_view\""`
/// `"myschema.my_view"` -> `"\"myschema\".\"my_view\""`
pub fn quote(name: &str) -> String {
    let (schema, tbl) = split_qualified(name);
    match schema {
        Some(s) => format!(
            "\"{}\".\"{}\"",
            s.replace('"', "\"\""),
            tbl.replace('"', "\"\"")
        ),
        None => format!("\"{}\"", tbl.replace('"', "\"\"")),
    }
}

/// Ensure an identifier fits within PostgreSQL's NAMEDATALEN limit (63 bytes).
///
/// If the raw name exceeds 63 bytes, truncate on a character boundary and
/// append a hash suffix so that distinct input names remain distinct after
/// truncation.
pub fn safe(raw: &str) -> String {
    const MAX_IDENT: usize = 63;
    if raw.len() <= MAX_IDENT {
        return raw.to_string();
    }
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    raw.hash(&mut hasher);
    let hash = hasher.finish();
    // 9 chars for suffix: _ + 8 hex digits
    let mut kept_len = MAX_IDENT - 9;
    while !raw.is_char_boundary(kept_len) {
        kept_len -= 1;
    }
    format!("{}_{:08x}", &raw[..kept_len], hash as u32)
}

/// Format a slice of strings as a PostgreSQL array literal in TEXT form
/// (e.g. `{"a","b","c"}`). Use with a `::TEXT[]` cast on the SQL side.
///
/// Motivation: pgrx 0.16/0.18 on PG 17.7 cassert builds trips a
/// `MemoryContextIsValid(context)` assertion when `DatumWithOid::new(Vec<String>,
/// TEXTARRAYOID)` is passed as an SPI parameter — `initArrayResult` fires with a
/// CurrentMemoryContext that PG considers invalid. Avoiding the ArrayResult code
/// path entirely by shipping the array as a TEXT scalar and letting the server
/// parse it sidesteps the crash.
///
/// Empty slice returns `{}`. Each element is double-quoted; embedded `"` and `\`
/// are backslash-escaped per PG's array input syntax.
pub fn format_pg_text_array(values: &[String]) -> String {
    let mut out = String::with_capacity(values.iter().map(|v| v.len() + 3).sum::<usize>() + 2);
    out.push('{');
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        for ch in v.chars() {
            match ch {
                '\\' | '"' => {
                    out.push('\\');
                    out.push(ch);
                }
                _ => out.push(ch),
            }
        }
        out.push('"');
    }
    out.push('}');
    out
}

/// PostgreSQL reads every byte of a multi-byte character as part of an
/// unquoted identifier, so such a byte next to a name extends that name.
fn continues_identifier(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80
}

/// Replace a SQL identifier with another, respecting word boundaries.
///
/// Only replaces when the match is NOT part of a longer identifier
/// (i.e., the character before/after is not alphanumeric, `_` or multi-byte).
///
/// **Contract:** `new_name` must itself be a simple identifier (possibly
/// schema-qualified and quoted). A match followed by `.col` produces invalid
/// SQL if `new_name` is a sub-query expression.
#[cfg(test)]
pub fn replace_identifier(sql: &str, old_name: &str, new_name: &str) -> String {
    replace_identifier_outside(sql, old_name, new_name, &[])
}

fn replace_identifier_outside_literals(sql: &str, old_name: &str, new_name: &str) -> String {
    replace_identifier_outside(sql, old_name, new_name, &string_literal_spans(sql))
}

fn replace_identifier_outside(
    sql: &str,
    old_name: &str,
    new_name: &str,
    untouched: &[Range<usize>],
) -> String {
    if old_name.is_empty() {
        return sql.to_string();
    }
    let mut untouched = UntouchedSpans {
        remaining: untouched,
    };
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let old_bytes = old_name.as_bytes();
    let mut unmatched_start = 0;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(span_end) = untouched.end_of_span_containing(i) {
            i = span_end;
            continue;
        }
        if i + old_bytes.len() <= bytes.len() && &bytes[i..i + old_bytes.len()] == old_bytes {
            let before_ok = i == 0 || !(continues_identifier(bytes[i - 1]) || bytes[i - 1] == b'.');
            // Don't replace identifiers that follow "AS " (output aliases, not table refs)
            let after_as = i >= 3
                && (bytes[i - 1] == b' ')
                && (bytes[i - 2] == b'S' || bytes[i - 2] == b's')
                && (bytes[i - 3] == b'A' || bytes[i - 3] == b'a')
                && (i < 4 || !bytes[i - 4].is_ascii_alphanumeric());
            let after_pos = i + old_bytes.len();
            let after_ok = after_pos >= bytes.len() || !continues_identifier(bytes[after_pos]);
            if before_ok && after_ok && !after_as {
                result.push_str(&sql[unmatched_start..i]);
                result.push_str(new_name);
                i += old_bytes.len();
                unmatched_start = i;
                continue;
            }
        }
        i += 1;
    }
    result.push_str(&sql[unmatched_start..]);
    result
}

/// When `source_table` is schema-qualified (`schema.bare`) and the user has
/// aliased the source with the bare name (`FROM schema.bare AS bare` or
/// `FROM schema.bare bare`), strip that redundant alias clause.
///
/// Motivation: callers that go on to wholesale-replace bare `<bare>.col`
/// qualifiers (e.g. [`replace_source_with_transition`]'s step 2) otherwise
/// produce SQL like `FROM "__reflex_new_<src>" AS <bare> WHERE "__reflex_new_<src>".col`,
/// which is rejected by PG because the alias `<bare>` hides the table's own
/// name. Stripping the alias before the rewrite collapses both sides to the
/// unaliased table.
pub fn strip_redundant_bare_alias(sql: &str, source_table: &str) -> String {
    let (schema, bare) = split_qualified(source_table);
    if schema.is_none() || bare == source_table {
        return sql.to_string();
    }
    let literals = string_literal_spans(sql);
    let mut untouched = UntouchedSpans {
        remaining: &literals,
    };
    let bytes = sql.as_bytes();
    let pat = source_table.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut unmatched_start = 0;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(span_end) = untouched.end_of_span_containing(i) {
            i = span_end;
            continue;
        }
        if i + pat.len() <= bytes.len() && &bytes[i..i + pat.len()] == pat {
            let before_ok = i == 0 || !(continues_identifier(bytes[i - 1]) || bytes[i - 1] == b'.');
            let after_pos = i + pat.len();
            let after_ok = after_pos >= bytes.len() || !continues_identifier(bytes[after_pos]);
            if before_ok && after_ok {
                let (user_alias, consumed_to) = consume_table_alias(bytes, after_pos);
                if matches!(&user_alias, Some(a) if a == bare) {
                    out.push_str(&sql[unmatched_start..i]);
                    out.push_str(source_table);
                    i = consumed_to;
                    unmatched_start = i;
                    continue;
                }
            }
        }
        i += 1;
    }
    out.push_str(&sql[unmatched_start..]);
    out
}

/// Case-INSENSITIVE word-level substitution. Walks `expr` word by word and
/// replaces each bare identifier whose lower-cased text equals
/// `ident.to_lowercase()` with `replacement`.
///
/// Used by partition-constraint expression rewrites (`pg_get_partition_constraintdef`
/// returns lower-cased identifiers regardless of the user's quoting, so the
/// constraint expression must be matched case-insensitively).
///
/// A partition bound is data: string literals are copied through unchanged.
pub fn substitute_identifier_ci(expr: &str, ident: &str, replacement: &str) -> String {
    substitute_word_ci_outside(expr, ident, replacement, &string_literal_spans(expr))
}

/// [`substitute_identifier_ci`] for a column name: the type name of a cast
/// (`'2024-01-01'::date`, `::timestamp with time zone`) is not a column, even
/// when a column is called `date`, `text` or `zone`.
pub fn substitute_column_ci(expr: &str, column: &str, replacement: &str) -> String {
    let literals = string_literal_spans(expr);
    let mut untouched = cast_type_spans(expr, &literals);
    untouched.extend(literals);
    untouched.sort_by_key(|span| span.start);
    substitute_word_ci_outside(expr, column, replacement, &untouched)
}

/// Words that continue a type name after its first word.
const TYPE_NAME_CONTINUATIONS: &[&str] = &[
    " with time zone",
    " without time zone",
    " precision",
    " varying",
];

/// The spans of `expr`, outside `literals`, that hold the type name of a `::` cast.
fn cast_type_spans(expr: &str, literals: &[Range<usize>]) -> Vec<Range<usize>> {
    let bytes = expr.as_bytes();
    let mut in_literal = UntouchedSpans {
        remaining: literals,
    };
    let mut spans = Vec::new();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if let Some(span_end) = in_literal.end_of_span_containing(i) {
            i = span_end;
            continue;
        }
        if &bytes[i..i + 2] != b"::" {
            i += 1;
            continue;
        }
        let type_start = i + 2;
        let mut end = type_start;
        while end < bytes.len() && bytes[end] == b' ' {
            end += 1;
        }
        loop {
            if end < bytes.len() && bytes[end] == b'"' {
                end += 1;
                while end < bytes.len() && bytes[end] != b'"' {
                    end += 1;
                }
                end = (end + 1).min(bytes.len());
            } else {
                while end < bytes.len() && continues_identifier(bytes[end]) {
                    end += 1;
                }
            }
            if end < bytes.len() && bytes[end] == b'.' {
                end += 1;
            } else {
                break;
            }
        }
        if end < bytes.len() && bytes[end] == b'(' {
            while end < bytes.len() && bytes[end] != b')' {
                end += 1;
            }
            end = (end + 1).min(bytes.len());
        }
        let rest = expr[end..].to_lowercase();
        if let Some(continuation) = TYPE_NAME_CONTINUATIONS
            .iter()
            .find(|continuation| rest.starts_with(**continuation))
        {
            end += continuation.len();
        }
        spans.push(type_start..end);
        i = end.max(type_start);
    }
    spans
}

fn substitute_word_ci_outside(
    expr: &str,
    ident: &str,
    replacement: &str,
    untouched_spans: &[Range<usize>],
) -> String {
    let bytes = expr.as_bytes();
    let lower_ident = ident.to_lowercase();
    let mut untouched = UntouchedSpans {
        remaining: untouched_spans,
    };
    let mut out = String::with_capacity(expr.len());
    let mut unmatched_start = 0;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(span_end) = untouched.end_of_span_containing(i) {
            i = span_end;
            continue;
        }
        if !continues_identifier(bytes[i]) || bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        let word_start = i;
        while i < bytes.len() && continues_identifier(bytes[i]) {
            i += 1;
        }
        if expr[word_start..i].to_lowercase() == lower_ident {
            out.push_str(&expr[unmatched_start..word_start]);
            out.push_str(replacement);
            unmatched_start = i;
        }
    }
    out.push_str(&expr[unmatched_start..]);
    out
}

/// Every spelling under which a query may reference `source_table`, in the
/// order the rewrite must replace them.
pub(super) fn rewritten_source_spellings(source_table: &str) -> Vec<String> {
    // A stored source can be *referenced* in `base_query` in any quoting
    // spelling, independent of how it was registered: a CTE sub-IMV source is
    // registered bare (`schema.bare`) but emitted **quoted**
    // (`"schema"."bare"`) in the decomposed parent's `base_query`, and a user
    // may quote only one of the two parts. The quoted
    // spellings MUST be rewritten before the bare-token pass, otherwise that
    // pass matches `bare` *inside* the existing quotes (`"bare"`) and injects
    // the already-quoted transition name, producing `"schema".""__reflex_…""`
    // — a zero-length delimited identifier the scanner rejects (42601).
    let (schema, bare_source) = split_qualified(source_table);
    let bare_unq = bare_source.trim_matches('"');
    let mut spellings = Vec::new();
    if let Some(s) = schema {
        let schema_unq = s.trim_matches('"');
        spellings.push(format!("\"{}\".\"{}\"", schema_unq, bare_unq));
        spellings.push(format!("\"{}\".{}", schema_unq, bare_unq));
        spellings.push(format!("{}.\"{}\"", schema_unq, bare_unq));
    }
    spellings.push(format!("\"{}\"", bare_unq));
    spellings.push(source_table.to_string());
    if bare_source != source_table {
        // Bare column qualifiers (`bare.col`). Quoted spellings are already
        // consumed above, so every remaining match here is genuinely unquoted.
        spellings.push(bare_unq.to_string());
    }
    spellings
}

/// Whether `token`, one whitespace-delimited word of a query, names
/// `source_table`. Recognises every spelling the rewrite replaces, and any
/// other placement of double quotes around the same name, so a guard built on
/// it can never miss a reference the rewrite would swap for a transition table.
/// An unqualified source is registered that way only when it lives in `public`
/// (or does not resolve), so `public.<source>` names it too.
/// Case-sensitive: callers fold `token` and `source_table` the same way first.
pub fn token_names_source(token: &str, source_table: &str) -> bool {
    let unquoted_token = token.replace('"', "");
    let unquoted_source = source_table.replace('"', "");
    let unquoted_bare = split_qualified(source_table).1.replace('"', "");
    let names_unqualified_source_in_public = !unquoted_source.contains('.')
        && unquoted_token
            .strip_prefix("public.")
            .is_some_and(|bare| bare == unquoted_source);
    unquoted_token == unquoted_source
        || unquoted_token == unquoted_bare
        || names_unqualified_source_in_public
        || rewritten_source_spellings(source_table)
            .iter()
            .any(|spelling| spelling == token)
}

/// Trigger-time delta-table rewriter: takes a stored `base_query` and rewrites
/// every reference to the source table to use the transition table instead.
///
/// `transition_tbl` is either a bare safe-identifier (real transition table
/// alias like `__reflex_new_<src>`) or an already-quoted/qualified ref like
/// `"schema"."__reflex_pt_new_v_s"`. Quote bare names; pass refs through.
///
/// Two passes:
///   1. Strip a redundant `AS <bare>` alias the user may have written after a
///      schema-qualified source ref (without this, step 2 would rewrite
///      `<bare>.col` qualifiers and PG would then reject the result because
///      the alias hides the table's own name).
///   2. Replace `<source_table>` with the transition table reference. When
///      the source was schema-qualified, also replace the bare form so column
///      qualifiers like `<bare>.col` are picked up.
///
/// String literals are data, never a reference to the source: both passes
/// copy single-quoted and dollar-quoted strings through unchanged.
pub fn replace_source_with_transition(
    base_query: &str,
    source_table: &str,
    transition_tbl: &str,
) -> String {
    let quoted_tbl = if transition_tbl.contains('"') {
        transition_tbl.to_string()
    } else {
        format!("\"{}\"", transition_tbl)
    };
    let stripped = strip_redundant_bare_alias(base_query, source_table);

    let mut out = stripped;
    for spelling in rewritten_source_spellings(source_table) {
        out = replace_identifier_outside_literals(&out, &spelling, &quoted_tbl);
    }
    out
}

/// Whether [`replace_source_with_transition`] would replace text inside a
/// double-quoted identifier that is not the source: `other."dim"` for the bare
/// name `dim`. The result holds a zero-length delimited identifier, so such a
/// query has no usable transition-table rewrite for this source.
pub fn source_rewrite_breaks_quoted_identifier(base_query: &str, source_table: &str) -> bool {
    const PROBE: &str = "\"__reflex_rewrite_probe\"";
    let rewritten = replace_source_with_transition(base_query, source_table, PROBE);
    rewritten.contains(&format!("\"{PROBE}")) || rewritten.contains(&format!("{PROBE}\""))
}

/// Scan forward from `start` looking for an optional table alias. Returns
/// `(Some(alias), new_index)` if an alias (with or without `AS`) was consumed,
/// else `(None, start)` so the caller can fall back to its default.
///
/// Reject bare identifiers that match SQL keywords which can follow a table
/// reference (JOIN, WHERE, ON, …) — those are NOT aliases.
pub(super) fn consume_table_alias(bytes: &[u8], start: usize) -> (Option<String>, usize) {
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() {
        return (None, start);
    }

    // `AS <ident>` form — AS is unambiguous, but the identifier after it
    // must not be a reserved keyword (SELECT, WHERE, JOIN, …). Blindly
    // consuming `AS SELECT` pushes a mis-parse downstream with a confusing
    // error — reject it here so the original SQL is preserved.
    let has_as = i + 2 <= bytes.len()
        && (bytes[i] == b'A' || bytes[i] == b'a')
        && (bytes[i + 1] == b'S' || bytes[i + 1] == b's')
        && (i + 2 == bytes.len() || bytes[i + 2].is_ascii_whitespace());
    if has_as {
        let mut j = i + 2;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        let name_start = j;
        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
            j += 1;
        }
        if j > name_start {
            let name_bytes = &bytes[name_start..j];
            if is_follow_keyword(name_bytes) {
                return (None, start);
            }
            let name = std::str::from_utf8(name_bytes).unwrap_or("").to_string();
            return (Some(name), j);
        }
        return (None, start);
    }

    // Bare identifier — must not be a SQL keyword that can follow a table.
    let name_start = i;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    if i == name_start {
        return (None, start);
    }
    let name_bytes = &bytes[name_start..i];
    if is_follow_keyword(name_bytes) {
        return (None, start);
    }
    let name = std::str::from_utf8(name_bytes).unwrap_or("").to_string();
    (Some(name), i)
}

/// SQL keywords that can follow a table reference in FROM/JOIN clauses,
/// plus a handful of fully-reserved words that are never valid as an alias
/// (SELECT, FROM, AS, DISTINCT). A bare identifier matching any of these
/// is not a table alias; under the `AS <ident>` form it marks a mis-parse.
fn is_follow_keyword(ident: &[u8]) -> bool {
    const KEYWORDS: &[&[u8]] = &[
        b"ON",
        b"JOIN",
        b"LEFT",
        b"RIGHT",
        b"INNER",
        b"OUTER",
        b"FULL",
        b"CROSS",
        b"NATURAL",
        b"LATERAL",
        b"USING",
        b"WHERE",
        b"GROUP",
        b"ORDER",
        b"HAVING",
        b"LIMIT",
        b"OFFSET",
        b"UNION",
        b"INTERSECT",
        b"EXCEPT",
        b"WITH",
        b"TABLESAMPLE",
        b"AND",
        b"OR",
        b"FETCH",
        b"WINDOW",
        b"FOR",
        b"RETURNING",
        b"SELECT",
        b"FROM",
        b"AS",
        b"DISTINCT",
    ];
    KEYWORDS.iter().any(|kw| {
        kw.len() == ident.len()
            && kw
                .iter()
                .zip(ident.iter())
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
    })
}
