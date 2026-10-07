pub(crate) struct KeywordHit {
    pub pos: usize,
    pub depth: usize,
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80
}

fn opens_escape_string(bytes: &[u8], quote: usize) -> bool {
    quote >= 1
        && (bytes[quote - 1] == b'E' || bytes[quote - 1] == b'e')
        && (quote < 2 || !is_identifier_byte(bytes[quote - 2]))
}

fn end_of_single_quoted(bytes: &[u8], quote: usize) -> usize {
    let backslash_escapes = opens_escape_string(bytes, quote);
    let mut i = quote + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if backslash_escapes => i += 2,
            b'\'' if bytes.get(i + 1) == Some(&b'\'') => i += 2,
            b'\'' => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

fn end_of_double_quoted(bytes: &[u8], quote: usize) -> usize {
    let mut i = quote + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' if bytes.get(i + 1) == Some(&b'"') => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

fn end_of_dollar_quoted(bytes: &[u8], dollar: usize) -> Option<usize> {
    if dollar > 0 && is_identifier_byte(bytes[dollar - 1]) {
        return None;
    }
    let mut tag_end = dollar + 1;
    while tag_end < bytes.len() && is_identifier_byte(bytes[tag_end]) {
        tag_end += 1;
    }
    if tag_end >= bytes.len() || bytes[tag_end] != b'$' {
        return None;
    }
    if tag_end > dollar + 1 && bytes[dollar + 1].is_ascii_digit() {
        return None;
    }
    let tag = &bytes[dollar..=tag_end];
    let body_start = tag_end + 1;
    let close = bytes[body_start..]
        .windows(tag.len())
        .position(|window| window == tag);
    Some(close.map_or(bytes.len(), |offset| body_start + offset + tag.len()))
}

fn end_of_block_comment(bytes: &[u8], open: usize) -> usize {
    let mut nesting = 1;
    let mut i = open + 2;
    while i + 1 < bytes.len() {
        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
            nesting += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            nesting -= 1;
            i += 2;
            if nesting == 0 {
                return i;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

fn end_of_line_comment(bytes: &[u8], open: usize) -> usize {
    bytes[open..]
        .iter()
        .position(|&byte| byte == b'\n')
        .map_or(bytes.len(), |offset| open + offset)
}

/// Byte ranges of the single-quoted and dollar-quoted string literals of
/// `sql`, in ascending order. A quote character inside a double-quoted
/// identifier or a comment does not open a literal.
pub(crate) fn string_literal_spans(sql: &str) -> Vec<std::ops::Range<usize>> {
    let bytes = sql.as_bytes();
    let mut spans = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let literal_end = match bytes[i] {
            b'\'' => Some(end_of_single_quoted(bytes, i)),
            b'$' => end_of_dollar_quoted(bytes, i),
            _ => None,
        };
        if let Some(end) = literal_end {
            spans.push(i..end);
            i = end;
            continue;
        }
        i = match bytes[i] {
            b'"' => end_of_double_quoted(bytes, i),
            b'-' if bytes.get(i + 1) == Some(&b'-') => end_of_line_comment(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => end_of_block_comment(bytes, i),
            _ => i + 1,
        };
    }
    spans
}

/// Byte ranges of the double-quoted identifiers of `sql`, quotes included, in
/// ascending order. A quote character inside a string literal or a comment
/// does not open an identifier.
pub(crate) fn quoted_identifier_spans(sql: &str) -> Vec<std::ops::Range<usize>> {
    let bytes = sql.as_bytes();
    let mut spans = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        i = match bytes[i] {
            b'"' => {
                let end = end_of_double_quoted(bytes, i);
                spans.push(i..end);
                end
            }
            b'\'' => end_of_single_quoted(bytes, i),
            b'$' => end_of_dollar_quoted(bytes, i).unwrap_or(i + 1),
            b'-' if bytes.get(i + 1) == Some(&b'-') => end_of_line_comment(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => end_of_block_comment(bytes, i),
            _ => i + 1,
        };
    }
    spans
}

pub(crate) fn keyword_hits(sql: &str, keyword: &str) -> Vec<KeywordHit> {
    let bytes = sql.as_bytes();
    let wanted = keyword.as_bytes();
    let mut hits = Vec::new();
    if wanted.is_empty() {
        return hits;
    }
    let mut depth = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        let skip_to = match bytes[i] {
            b'\'' => Some(end_of_single_quoted(bytes, i)),
            b'"' => Some(end_of_double_quoted(bytes, i)),
            b'$' => end_of_dollar_quoted(bytes, i),
            b'-' if bytes.get(i + 1) == Some(&b'-') => Some(
                bytes[i..]
                    .iter()
                    .position(|&byte| byte == b'\n')
                    .map_or(bytes.len(), |offset| i + offset),
            ),
            b'/' if bytes.get(i + 1) == Some(&b'*') => Some(end_of_block_comment(bytes, i)),
            _ => None,
        };
        if let Some(end) = skip_to {
            i = end;
            continue;
        }
        if i + wanted.len() <= bytes.len()
            && bytes[i..i + wanted.len()].eq_ignore_ascii_case(wanted)
        {
            hits.push(KeywordHit { pos: i, depth });
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        i += 1;
    }
    hits
}

pub(crate) fn last_keyword(sql: &str, keyword: &str) -> Option<usize> {
    keyword_hits(sql, keyword).last().map(|hit| hit.pos)
}

pub(crate) fn has_keyword(sql: &str, keyword: &str) -> bool {
    !keyword_hits(sql, keyword).is_empty()
}

pub(crate) fn has_top_level_keyword(sql: &str, keyword: &str) -> bool {
    keyword_hits(sql, keyword).iter().any(|hit| hit.depth == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn positions(sql: &str, kw: &str) -> Vec<usize> {
        keyword_hits(sql, kw).into_iter().map(|h| h.pos).collect()
    }

    fn legacy_last(sql: &str, kw: &str) -> Option<usize> {
        sql.to_uppercase().rfind(&kw.to_uppercase())
    }

    #[test]
    fn finds_keyword_in_plain_sql_case_insensitively() {
        let sql = "SELECT g FROM t WHERE x group by g";
        assert_eq!(last_keyword(sql, " GROUP BY "), sql.find(" group by "));
        assert!(has_keyword(sql, " WHERE "));
    }

    #[test]
    fn skips_single_quoted_literals() {
        let sql = "SELECT g FROM t WHERE c <> 'a group by b' GROUP BY g HAVING x <> ' group by '";
        assert_eq!(
            positions(sql, " GROUP BY "),
            vec![sql.find(" GROUP BY g").unwrap()]
        );
    }

    #[test]
    fn skips_doubled_quote_and_escape_string() {
        let sql = "SELECT 'it''s where it is' , E'a\\' where b' FROM t GROUP BY g";
        assert!(!has_keyword(sql, " WHERE "));
        assert!(has_keyword(sql, " GROUP BY "));
    }

    #[test]
    fn skips_quoted_identifiers_dollar_quotes_and_comments() {
        let sql = "SELECT \"a group by b\", $$ x group by y $$, $tag$ group by $tag$ \
                   FROM t /* group by */ -- group by \n GROUP BY g";
        assert_eq!(
            positions(sql, " GROUP BY "),
            vec![sql.rfind(" GROUP BY g").unwrap()]
        );
    }

    #[test]
    fn positional_parameter_is_not_a_dollar_quote() {
        let sql = "SELECT g FROM t WHERE a = $1 AND b = $2 GROUP BY g";
        assert!(has_keyword(sql, " WHERE "));
        assert!(has_keyword(sql, " GROUP BY "));
    }

    #[test]
    fn reports_parenthesis_depth() {
        let sql = "SELECT g FROM (SELECT * FROM t WHERE x) q WHERE y GROUP BY g";
        let depths: Vec<usize> = keyword_hits(sql, " WHERE ")
            .into_iter()
            .map(|h| h.depth)
            .collect();
        assert_eq!(depths, vec![1, 0]);
    }

    #[test]
    fn positions_index_the_original_string_for_multibyte_text() {
        let sql = "SELECT g FROM t WHERE c <> 'ıııı' GROUP BY g";
        let pos = last_keyword(sql, " GROUP BY ").unwrap();
        assert_eq!(&sql[pos..pos + 10], " GROUP BY ");
    }

    #[test]
    fn unterminated_literal_hides_the_rest() {
        assert!(!has_keyword("SELECT 'abc GROUP BY g", " GROUP BY "));
    }

    #[test]
    fn letter_e_ending_an_identifier_does_not_open_an_escape_string() {
        assert!(has_keyword(
            "SELECT date'a\\' FROM t GROUP BY g",
            " GROUP BY "
        ));
    }

    #[test]
    fn dollar_inside_an_identifier_is_not_a_dollar_quote() {
        let sql = "SELECT a$b$ FROM t GROUP BY c$b$";
        assert_eq!(
            positions(sql, " GROUP BY "),
            vec![sql.find(" GROUP BY ").unwrap()]
        );
    }

    #[test]
    fn digit_led_tag_is_not_a_dollar_quote() {
        assert!(has_keyword("SELECT $1$ FROM t GROUP BY x$1$", " GROUP BY "));
    }

    #[test]
    fn nested_block_comment_ends_at_its_outer_close() {
        let sql = "SELECT g /* a /* b */ GROUP BY c */ FROM t GROUP BY g";
        assert_eq!(
            positions(sql, " GROUP BY "),
            vec![sql.rfind(" GROUP BY ").unwrap()]
        );
    }

    #[test]
    fn top_level_keyword_ignores_nested_and_quoted_occurrences() {
        assert!(has_top_level_keyword(
            "SELECT g FROM i WHERE c > 0 GROUP BY g",
            "GROUP BY"
        ));
        assert!(!has_top_level_keyword(
            "SELECT g FROM i WHERE c > (SELECT 1 FROM (SELECT k FROM o GROUP BY k) z)",
            "GROUP BY"
        ));
        assert!(!has_top_level_keyword(
            "SELECT g AS \"x GROUP BY y\" FROM i WHERE c <> ' GROUP BY '",
            "GROUP BY"
        ));
        assert!(has_top_level_keyword(
            "SELECT g FROM i WHERE c > (SELECT 1 FROM o GROUP BY k) GROUP BY g",
            "GROUP BY"
        ));
    }

    #[test]
    fn literal_spans_cover_single_and_dollar_quoted_strings() {
        let sql = "SELECT 'a''b', E'c\\'d', $$e$$, $t$f$t$ FROM t WHERE x = $1";
        let literals: Vec<&str> = string_literal_spans(sql)
            .into_iter()
            .map(|span| &sql[span])
            .collect();
        assert_eq!(literals, vec!["'a''b'", "'c\\'d'", "$$e$$", "$t$f$t$"]);
    }

    #[test]
    fn literal_spans_ignore_quotes_in_identifiers_and_comments() {
        let sql = "SELECT \"it's\" /* don't */ FROM t -- can't\nWHERE x = 'y'";
        let literals: Vec<&str> = string_literal_spans(sql)
            .into_iter()
            .map(|span| &sql[span])
            .collect();
        assert_eq!(literals, vec!["'y'"]);
    }

    #[test]
    fn unterminated_literal_span_runs_to_the_end() {
        let sql = "SELECT 'abc FROM t";
        assert_eq!(string_literal_spans(sql), vec![7..sql.len()]);
    }

    proptest! {
        #[test]
        fn agrees_with_legacy_search_when_nothing_is_quoted(
            sql in "[A-Za-z0-9_ ,.=<>()*+]{0,80}( (GROUP BY|group by|WHERE|where) [A-Za-z0-9_ ,.=<>()*+]{0,40}){0,3}"
        ) {
            for kw in [" GROUP BY ", " WHERE ", "GROUP BY"] {
                prop_assert_eq!(last_keyword(&sql, kw), legacy_last(&sql, kw));
                prop_assert_eq!(has_keyword(&sql, kw), legacy_last(&sql, kw).is_some());
            }
        }
    }

    #[test]
    fn quoted_identifiers_are_whole_tokens_outside_literals_and_comments() {
        let spans = |sql: &str| -> Vec<String> {
            quoted_identifier_spans(sql)
                .into_iter()
                .map(|span| sql[span].to_string())
                .collect()
        };
        assert_eq!(spans("\"a\" + \"b\""), vec!["\"a\"", "\"b\""]);
        assert_eq!(spans("\"a\"\"b\" / 2"), vec!["\"a\"\"b\""]);
        assert_eq!(spans("'\"a\"' || \"b\""), vec!["\"b\""]);
        assert_eq!(
            spans("'it''s \"a\"' || $$\"c\"$$ || $t$\"d\"$t$"),
            Vec::<String>::new()
        );
        assert_eq!(
            spans("\"a\" -- \"b\"\n/* \"c\" */ \"d\""),
            vec!["\"a\"", "\"d\""]
        );
        assert_eq!(spans("$1 + \"a\""), vec!["\"a\""]);
        assert_eq!(spans("\"it's\" || 'x'"), vec!["\"it's\""]);
    }
}
