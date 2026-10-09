use super::*;
use crate::trigger::{has_no_scoped_delta, upper_with_single_spaces};

fn read_by_expression_subquery(query: &str, source: &str) -> bool {
    source_read_by_expression_subquery(&upper_with_single_spaces(query), query, source)
}

#[test]
fn source_read_by_a_subquery_used_as_an_expression_is_found() {
    for query in [
        "SELECT g, SUM(v) FROM a WHERE g IN (SELECT g FROM b) GROUP BY g",
        "SELECT g, SUM(v) FROM a WHERE g NOT IN (SELECT g FROM s.b) GROUP BY g",
        "SELECT g FROM a WHERE g in\n (select g\n from\n \"b\")",
        "SELECT g FROM a WHERE g IN (SELECT g FROM b) OR w IS NULL",
        "SELECT a.g FROM a WHERE EXISTS (SELECT 1 FROM b x WHERE x.g = a.g)",
        "SELECT a.g FROM a WHERE NOT EXISTS (SELECT 1 FROM b x WHERE x.g = a.g)",
        "SELECT g FROM a WHERE v >= (SELECT max(v) FROM b)",
        "SELECT g FROM a WHERE g = ANY (SELECT g FROM b)",
        "SELECT g FROM a WHERE v > ALL (SELECT v FROM b)",
        "SELECT g, (SELECT max(v) FROM b) AS top FROM a",
        "SELECT g, SUM(v + (SELECT max(v) FROM b)) FROM a GROUP BY g",
        "SELECT g, SUM(v) FROM a GROUP BY g HAVING SUM(v) > (SELECT max(v) FROM b)",
        "SELECT a.g FROM a JOIN c ON c.g = a.g AND c.v > (SELECT max(v) FROM b)",
        "SELECT g FROM a WHERE g IN (SELECT g FROM c WHERE v IN (SELECT v FROM b))",
        "SELECT g FROM a WHERE g IN (SELECT x.g FROM (SELECT g FROM b) x)",
        "SELECT x.g FROM (SELECT g FROM a WHERE g IN (SELECT g FROM b)) x",
        "SELECT g FROM a WHERE g IN (SELECT g FROM c UNION ALL SELECT g FROM b)",
        "SELECT g FROM a WHERE CASE WHEN w IS NULL THEN TRUE ELSE g IN (SELECT g FROM b) END",
        "SELECT g FROM a WHERE g IN (SELECT g FROM b) AND (",
    ] {
        assert!(read_by_expression_subquery(query, "b"), "{query}");
        assert!(has_no_scoped_delta(query, "b"), "{query}");
    }
}

#[test]
fn source_read_only_in_a_from_clause_is_not_found() {
    for (query, source) in [
        (
            "SELECT g, SUM(v) FROM a WHERE g IN (SELECT g FROM b) GROUP BY g",
            "a",
        ),
        (
            "SELECT a.g FROM a WHERE EXISTS (SELECT 1 FROM b x WHERE x.g = a.g)",
            "a",
        ),
        ("SELECT g, (SELECT max(v) FROM b) AS top FROM a", "a"),
        ("SELECT a.g FROM a JOIN b ON b.g = a.g WHERE a.v > 0", "b"),
        (
            "SELECT a.g FROM a JOIN b ON b.g = a.g WHERE a.g IN (SELECT g FROM c)",
            "b",
        ),
        (
            "SELECT x.g FROM (SELECT g FROM b WHERE v > 0) x JOIN a ON a.g = x.g",
            "b",
        ),
        (
            "SELECT x.g FROM (SELECT g FROM b UNION ALL SELECT g FROM a) x",
            "b",
        ),
        (
            "SELECT g, COUNT(*) FROM b WHERE v IN (1, 2) AND g = ANY (ARRAY['a']) GROUP BY g",
            "b",
        ),
        ("SELECT g FROM b WHERE (", "b"),
    ] {
        assert!(
            !read_by_expression_subquery(query, source),
            "{query} / {source}"
        );
    }
}

#[test]
fn a_query_with_one_select_keyword_is_not_parsed() {
    let unparsable = "SELECT g FROM b WHERE ((( unbalanced";
    assert!(!read_by_expression_subquery(unparsable, "b"));
    assert!(read_by_expression_subquery(
        "SELECT g FROM a WHERE g IN (SELECT g FROM b WHERE (((",
        "b"
    ));
}

#[test]
fn source_read_only_by_a_subquery_of_the_end_query_is_found() {
    let base =
        "SELECT g AS \"g\", SUM(v) AS \"__sum_v\", COUNT(*) AS __ivm_count FROM a GROUP BY g";
    for end_query in [
        "SELECT \"g\", \"__sum_v\" AS \"sv\" FROM \"__reflex_intermediate_x\" WHERE __ivm_count > 0 AND \"__sum_v\" > (SELECT max(v) FROM b)",
        "SELECT \"g\", \"__sum_v\" + (SELECT max(v) FROM s.b) AS \"x\" FROM \"__reflex_intermediate_x\" WHERE __ivm_count > 0",
        "SELECT \"g\", CASE WHEN \"__sum_v\" > (SELECT max(v) FROM \"b\") THEN 1 ELSE 0 END AS \"hi\" FROM \"__reflex_intermediate_x\" WHERE __ivm_count > 0",
        "SELECT \"g\" FROM \"__reflex_intermediate_x\" WHERE __ivm_count > 0 AND \"g\" IN (SELECT g FROM b) AND (",
    ] {
        assert!(!has_no_scoped_delta(base, "b"), "{end_query}");
        assert!(imv_reads_source_by_expression_subquery(base, end_query, "b"), "{end_query}");
        assert!(crate::trigger::imv_has_no_scoped_delta(base, end_query, "b"), "{end_query}");
        assert!(!crate::trigger::imv_has_no_scoped_delta(base, end_query, "a"), "{end_query}");
    }
    let own_table = "SELECT \"g\", \"__sum_v\" AS \"sv\" FROM \"__reflex_intermediate_x\" WHERE __ivm_count > 0 AND \"__sum_v\" >= (SELECT max(v) FROM a)";
    assert!(crate::trigger::imv_has_no_scoped_delta(
        base, own_table, "a"
    ));
}

#[test]
fn end_query_that_does_not_read_the_source_keeps_the_scoped_delta() {
    let base = "SELECT g AS \"g\", SUM(v) AS \"__sum_v\", COUNT(*) AS __ivm_count FROM a JOIN b ON b.g = a.g GROUP BY g";
    for end_query in [
        "",
        "SELECT \"g\", \"__sum_v\" AS \"b\" FROM \"__reflex_intermediate_x\" WHERE __ivm_count > 0",
        "SELECT \"g\", \"__sum_v\" AS \"sv\" FROM \"__reflex_intermediate_x\" WHERE __ivm_count > 0 AND \"__sum_v\" > (SELECT max(v) FROM c)",
        "SELECT \"g\", \"__sum_v\" AS \"sv\" FROM \"__reflex_intermediate_b_x\" WHERE __ivm_count > 0 AND \"b_total\" > 0",
    ] {
        for source in ["a", "b"] {
            assert!(
                !crate::trigger::imv_has_no_scoped_delta(base, end_query, source),
                "{source} / {end_query}"
            );
        }
    }
}
