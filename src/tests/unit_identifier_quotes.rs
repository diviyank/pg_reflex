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

#[test]
fn a_quoted_name_glued_to_a_letter_is_set_apart_by_a_space() {
    for (written, expected) in [
        ("SELECT\"id\",\"v\"FROM\"t\"", "SELECT id,v FROM t"),
        (
            "SELECT a FROM\"t\"WHERE a > 0",
            "SELECT a FROM t WHERE a > 0",
        ),
        ("SELECT a FROM \"t\"AS\"x\"", "SELECT a FROM t AS x"),
    ] {
        assert_eq!(unquote_all(written).as_deref(), Some(expected), "{written}");
    }
}

#[test]
fn a_quoted_name_beside_a_string_another_quoted_name_or_an_ampersand_keeps_its_quotes() {
    for written in [
        "SELECT 'a'\"t\" FROM x",
        "SELECT u&\"t\" FROM x",
        "SELECT a$\"t\" FROM x",
    ] {
        assert_eq!(unquote_all(written), None, "{written}");
    }
}

#[test]
fn a_unicode_escape_prefix_before_a_name_without_escapes_is_dropped() {
    for (written, expected) in [
        ("SELECT u&\"v\" FROM t", "SELECT \"v\" FROM t"),
        (
            "SELECT U&\"v\", t.U&\"My Col\" FROM t",
            "SELECT \"v\", t.\"My Col\" FROM t",
        ),
        ("SELECT id,U&\"v\"FROM t", "SELECT id,\"v\"FROM t"),
    ] {
        assert_eq!(
            without_idle_unicode_escape_prefixes(written).as_deref(),
            Some(expected),
            "{written}"
        );
        assert_eq!(unicode_escaped_identifier(expected), None, "{expected}");
    }
}

#[test]
fn a_name_with_an_escape_sequence_or_a_uescape_clause_keeps_its_prefix() {
    for (written, identifier) in [
        ("SELECT u&\"\\0076\" FROM t", "u&\"\\0076\""),
        ("SELECT U&\"d!0061t\" UESCAPE '!' FROM t", "U&\"d!0061t\""),
        ("SELECT U&\"v\"\n  uescape '!' FROM t", "U&\"v\""),
    ] {
        assert_eq!(
            without_idle_unicode_escape_prefixes(written),
            None,
            "{written}"
        );
        assert_eq!(
            unicode_escaped_identifier(written).as_deref(),
            Some(identifier),
            "{written}"
        );
    }
}

#[test]
fn what_is_not_a_unicode_escaped_identifier_is_left_alone() {
    for written in [
        "SELECT u & \"v\" FROM t",
        "SELECT u &\"v\" FROM t",
        "SELECT xu&\"v\" FROM t",
        "SELECT \"u\"&\"v\" FROM t",
        "SELECT U&'\\0041' FROM t",
        "SELECT 'u&\"v\"', $$U&\"v\"$$ FROM t -- U&\"v\"",
    ] {
        assert_eq!(
            without_idle_unicode_escape_prefixes(written),
            None,
            "{written}"
        );
        assert_eq!(unicode_escaped_identifier(written), None, "{written}");
    }
}

#[test]
fn only_the_listed_unquoted_words_change_case() {
    let spellings = vec!["Id".to_string(), "T1".to_string(), "ÉA".to_string()];
    for (written, expected) in [
        (
            "SELECT Id, ID, \"Id\", 'Id', v ISNULL FROM T1 -- Id\n WHERE Id > 0x1F /* T1 */",
            "SELECT id, ID, \"Id\", 'Id', v ISNULL FROM t1 -- Id\n WHERE id > 0x1F /* T1 */",
        ),
        (
            "SELECT $q$Id$q$, - -Id, u&'Id' FROM T1",
            "SELECT $q$Id$q$, - -id, u&'Id' FROM t1",
        ),
        ("SELECT é, ÉA\r\n\tFROM T1", "SELECT é, Éa\r\n\tFROM t1"),
        ("SELECT a FROM t", "SELECT a FROM t"),
    ] {
        assert_eq!(
            with_words_in_lower_case(written, &spellings).as_deref(),
            Some(expected),
            "{written}"
        );
    }
    assert_eq!(
        with_words_in_lower_case("SELECT 'open FROM Id", &spellings),
        None
    );
}
