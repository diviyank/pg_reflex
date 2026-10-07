use super::*;
use crate::trigger::upper_with_single_spaces;

fn inside_left_joined_subtree(query: &str, source: &str) -> bool {
    source_inside_left_joined_subtree(&upper_with_single_spaces(query), query, source)
}

#[test]
fn source_inside_a_left_joined_derived_table_or_nested_join_is_found() {
    for query in [
        "SELECT 1 FROM a LEFT JOIN (SELECT k FROM b WHERE k > 0) d ON d.k = a.k",
        "SELECT 1 FROM a LEFT OUTER JOIN (SELECT k FROM b) d ON d.k = a.k",
        "SELECT 1 FROM a left\n join (SELECT k FROM s.b) d ON d.k = a.k",
        "SELECT 1 FROM a LEFT JOIN (b JOIN c ON c.k = b.k) ON b.k = a.k",
        "SELECT 1 FROM a LEFT JOIN (SELECT x.k FROM (SELECT k FROM b) x) d ON d.k = a.k",
        "SELECT 1 FROM a LEFT JOIN (SELECT k FROM c WHERE k IN (SELECT k FROM b)) d ON d.k = a.k",
        "SELECT 1 FROM a LEFT JOIN LATERAL (SELECT k FROM b WHERE b.k = a.k) d ON TRUE",
        "SELECT 1 FROM c JOIN (SELECT a.k FROM a LEFT JOIN (SELECT k FROM b) d ON d.k = a.k) x ON x.k = c.k",
        "SELECT 1 FROM c JOIN (a LEFT JOIN (SELECT k FROM b) d ON d.k = a.k) ON a.k = c.k",
        "SELECT 1 FROM c WHERE EXISTS (SELECT 1 FROM a LEFT JOIN (SELECT k FROM b) d ON d.k = a.k)",
        "SELECT 1 FROM a JOIN c ON c.k = a.k LEFT JOIN (SELECT k FROM b) d ON d.k = a.k JOIN e ON e.k = a.k",
        "SELECT 1 FROM a LEFT JOIN (SELECT k FROM b) d ON d.k = a.k WHERE (",
    ] {
        assert!(inside_left_joined_subtree(query, "b"), "{query}");
    }
}

#[test]
fn source_outside_every_left_joined_subtree_is_not_found() {
    for (query, source) in [
        (
            "SELECT 1 FROM a LEFT JOIN (SELECT k FROM b) d ON d.k = a.k",
            "a",
        ),
        (
            "SELECT 1 FROM a LEFT JOIN (SELECT k FROM b) d ON d.k = a.k JOIN c ON c.k = a.k",
            "c",
        ),
        ("SELECT 1 FROM a LEFT JOIN b ON b.k = a.k", "b"),
        ("SELECT 1 FROM a LEFT JOIN b ON b.k = a.k", "a"),
        ("SELECT 1 FROM a JOIN (SELECT k FROM b) d ON d.k = a.k", "b"),
        (
            "SELECT 1 FROM (SELECT k FROM b) d LEFT JOIN a ON d.k = a.k",
            "b",
        ),
        (
            "SELECT 1 FROM a JOIN (SELECT k FROM b) d ON d.k = a.k LEFT JOIN c ON c.k = a.k",
            "b",
        ),
        (
            "SELECT 1 FROM a JOIN (SELECT k FROM b) d ON d.k = a.k WHERE (",
            "b",
        ),
        (
            "SELECT 1 FROM a LEFT JOIN (SELECT k FROM b_backup) d ON d.k = a.k",
            "b",
        ),
    ] {
        assert!(
            !inside_left_joined_subtree(query, source),
            "{query} / {source}"
        );
    }
}

#[test]
fn query_without_left_join_is_never_parsed() {
    assert!(!inside_left_joined_subtree(
        "SELECT 1 FROM a JOIN (SELECT k FROM b) d ON d.k = a.k WHERE (",
        "b"
    ));
    assert!(!inside_left_joined_subtree(
        "SELECT 1 FROM a RIGHT JOIN (SELECT k FROM b) d ON d.k = a.k",
        "b"
    ));
}
