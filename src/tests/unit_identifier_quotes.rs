use super::*;

fn unquote_all(sql: &str) -> Option<String> {
    without_needless_quotes(sql, &|_| true)
}

#[test]
fn every_placement_of_a_lower_case_name_loses_its_quotes() {
    for (written, expected) in [
        ("SELECT a FROM \"t\"", "SELECT a FROM t"),
        ("SELECT \"t\".a FROM \"public\".\"t\"", "SELECT t.a FROM public.t"),
        ("SELECT x.\"a\" AS \"b\" FROM t AS \"x\"", "SELECT x.a AS b FROM t AS x"),
        (
            "SELECT 1 FROM \"t\" JOIN \"u\" USING (\"k\")",
            "SELECT 1 FROM t JOIN u USING (k)",
        ),
        (
            "WITH \"c\" AS (SELECT a FROM \"t\")\nSELECT a\tFROM \"c\"\nUNION ALL SELECT a FROM \"t\"",
            "WITH c AS (SELECT a FROM t)\nSELECT a\tFROM c\nUNION ALL SELECT a FROM t",
        ),
        ("SELECT 'é', a FROM \"t\"", "SELECT 'é', a FROM t"),
    ] {
        assert_eq!(unquote_all(written).as_deref(), Some(expected), "{written}");
    }
}

#[test]
fn names_that_need_their_quotes_keep_them() {
    for written in [
        "SELECT a FROM \"T\"",
        "SELECT a FROM \"my table\"",
        "SELECT a FROM \"t-1\"",
        "SELECT a FROM \"1t\"",
        "SELECT \"from\" FROM t",
        "SELECT a FROM \"a\"\"b\"",
        "SELECT a FROM t",
    ] {
        assert_eq!(unquote_all(written), None, "{written}");
    }
}

#[test]
fn only_identifiers_are_touched() {
    for written in [
        "SELECT '\"t\"' FROM t",
        "SELECT 'it''s \"t\"' FROM t",
        "SELECT $$\"t\"$$ FROM t",
        "SELECT a /* \"t\" */ FROM t -- \"t\"",
    ] {
        assert_eq!(unquote_all(written), None, "{written}");
    }
    assert_eq!(
        unquote_all("SELECT 'it''s \"t\"', a /* \"t\" */ FROM \"t\" -- \"t\"").as_deref(),
        Some("SELECT 'it''s \"t\"', a /* \"t\" */ FROM t -- \"t\"")
    );
}

#[test]
fn a_quoted_name_glued_to_its_neighbour_keeps_its_quotes() {
    assert_eq!(unquote_all("SELECT 1\"t\" FROM x"), None);
}

#[test]
fn the_caller_decides_which_names_read_the_same_bare() {
    assert_eq!(
        without_needless_quotes("SELECT \"year\" FROM \"t\"", &|name| name != "year").as_deref(),
        Some("SELECT \"year\" FROM t")
    );
    assert_eq!(
        without_needless_quotes("SELECT \"year\" FROM \"t\"", &|_| false),
        None
    );
}

#[test]
fn a_name_the_parser_reads_as_a_keyword_keeps_its_quotes_and_the_others_lose_theirs() {
    assert_eq!(
        unquote_all("SELECT \"from\", \"a\" FROM \"t\"").as_deref(),
        Some("SELECT \"from\", a FROM t")
    );
}

#[test]
fn text_that_does_not_tokenize_or_parse_is_left_alone() {
    assert_eq!(unquote_all("SELECT 'open FROM \"t\""), None);
    assert_eq!(unquote_all("SELECT a FROM \"t\" WHERE ("), None);
    assert_eq!(unquote_all("SELECT a FROM \"t\"; SELECT 1"), None);
}
