use super::*;

#[test]
fn join_kind_is_read_through_comments_newlines_and_case() {
    for query in [
        "SELECT 1 FROM a RIGHT /* c */ JOIN b ON a.k = b.k",
        "SELECT 1 FROM a RIGHT -- c\n JOIN b ON a.k = b.k",
        "SELECT 1 FROM a right /* c */ outer /* c */ join b ON a.k = b.k",
        "SELECT 1 FROM a NATURAL RIGHT JOIN b",
        "SELECT 1 FROM c JOIN (SELECT a.k FROM a RIGHT/**/JOIN b USING (k)) x ON x.k = c.k",
        "WITH x AS (SELECT a.k FROM a RIGHT /* c */ JOIN b ON a.k = b.k) SELECT 1 FROM x",
        "SELECT 1 FROM c UNION ALL SELECT 1 FROM a RIGHT /* c */ JOIN b ON a.k = b.k",
        "SELECT 1 FROM c WHERE EXISTS (SELECT 1 FROM a RIGHT /* c */ JOIN b ON a.k = b.k)",
        "SELECT 1 FROM c JOIN (a RIGHT /* c */ JOIN b ON a.k = b.k) ON a.k = c.k",
    ] {
        let joins = outer_joins(query, "a");
        assert!(joins.holds_right_join, "{query}");
        assert!(!joins.holds_full_join, "{query}");
    }
    for query in [
        "SELECT 1 FROM a FULL /* c */ JOIN b ON a.k = b.k",
        "SELECT 1 FROM a FULL -- c\n OUTER JOIN b ON a.k = b.k",
        "SELECT 1 FROM a NATURAL FULL JOIN b",
    ] {
        assert!(holds_full_join(query), "{query}");
        assert!(!outer_joins(query, "a").holds_right_join, "{query}");
    }
}

#[test]
fn text_naming_a_join_kind_is_not_a_join() {
    for query in [
        "SELECT 1 FROM a JOIN b ON a.k = b.k WHERE b.l <> 'right join' AND b.m <> 'FULL JOIN'",
        "SELECT 1 FROM a JOIN b ON a.k = b.k /* LEFT JOIN b */ -- RIGHT JOIN b",
        "SELECT a.\"right join\" FROM a JOIN b ON a.k = b.k",
    ] {
        let joins = outer_joins(query, "b");
        assert!(!joins.holds_right_join, "{query}");
        assert!(!joins.holds_full_join, "{query}");
        assert!(!joins.source_is_null_extended_table, "{query}");
        assert!(!joins.source_inside_left_joined_subtree, "{query}");
    }
}

#[test]
fn table_joined_by_a_left_or_full_join_is_the_null_extended_one() {
    for query in [
        "SELECT 1 FROM a LEFT JOIN b ON a.k = b.k",
        "SELECT 1 FROM a LEFT /* c */ JOIN b x ON a.k = x.k",
        "SELECT 1 FROM a LEFT -- c\n OUTER JOIN /* c */ b AS x ON a.k = x.k",
        "SELECT 1 FROM a NATURAL LEFT JOIN b",
        "SELECT 1 FROM a LEFT JOIN \"b\" USING (k)",
        "SELECT 1 FROM a FULL /* c */ JOIN b ON a.k = b.k",
        "SELECT 1 FROM c JOIN (SELECT a.k FROM a LEFT /* c */ JOIN b ON a.k = b.k) x ON x.k = c.k",
        "SELECT 1 FROM c JOIN (a LEFT /* c */ JOIN b ON a.k = b.k) ON a.k = c.k",
    ] {
        assert!(
            outer_joins(query, "b").source_is_null_extended_table,
            "{query}"
        );
        assert!(
            !outer_joins(query, "a").source_is_null_extended_table,
            "{query}"
        );
    }
    for query in [
        "SELECT 1 FROM a JOIN b ON a.k = b.k LEFT JOIN c ON c.k = b.k",
        "SELECT 1 FROM b LEFT JOIN a ON a.k = b.k",
        "SELECT 1 FROM a LEFT JOIN b_backup ON a.k = b_backup.k JOIN b ON b.k = a.k",
    ] {
        assert!(
            !outer_joins(query, "b").source_is_null_extended_table,
            "{query}"
        );
    }
}

#[test]
fn left_joined_subtree_is_found_through_a_comment_in_the_keyword() {
    for query in [
        "SELECT 1 FROM a LEFT /* c */ JOIN (SELECT k FROM b) d ON d.k = a.k",
        "SELECT 1 FROM a LEFT -- c\n JOIN (b JOIN c ON c.k = b.k) ON b.k = a.k",
        "SELECT 1 FROM a NATURAL LEFT JOIN (SELECT k FROM b) d",
    ] {
        assert!(
            outer_joins(query, "b").source_inside_left_joined_subtree,
            "{query}"
        );
        assert!(
            !outer_joins(query, "a").source_inside_left_joined_subtree,
            "{query}"
        );
    }
}

#[test]
fn every_table_reference_naming_the_source_is_counted() {
    assert_eq!(
        outer_joins("SELECT 1 FROM t/*c*/a JOIN \"t\" b ON a.k = b.k", "t").source_reads,
        2
    );
    assert_eq!(
        outer_joins("SELECT 1 FROM t WHERE k IN (SELECT k FROM public.t)", "t").source_reads,
        2
    );
    assert_eq!(
        outer_joins("SELECT t.k FROM s.t AS t WHERE t.k > 0", "s.t").source_reads,
        1
    );
    assert_eq!(
        outer_joins("SELECT 1 FROM t JOIN t_backup b ON TRUE", "t").source_reads,
        1
    );
}

#[test]
fn unparseable_text_is_read_by_keyword() {
    let joins = outer_joins("SELECT 1 FROM a RIGHT JOIN b ON a.k = b.k WHERE (", "a");
    assert!(joins.holds_right_join);
    let joins = outer_joins("SELECT 1 FROM a LEFT JOIN b ON a.k = b.k WHERE (", "b");
    assert!(joins.source_is_null_extended_table);
    assert!(joins.source_inside_left_joined_subtree);
    assert!(holds_full_join(
        "SELECT 1 FROM a FULL OUTER JOIN b ON a.k = b.k WHERE ("
    ));
    let joins = outer_joins("SELECT 1 FROM a JOIN b ON a.k = b.k WHERE (", "b");
    assert!(!joins.holds_right_join && !joins.holds_full_join);
    assert!(!joins.source_is_null_extended_table && !joins.source_inside_left_joined_subtree);
}
