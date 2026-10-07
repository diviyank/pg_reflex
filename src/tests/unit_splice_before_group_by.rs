use super::splice_before_group_by;

const FILTER: &str = " AND EXISTS (SELECT 1 FROM aff)";

fn spliced(query: &str) -> String {
    splice_before_group_by(query, FILTER).expect("query has a GROUP BY")
}

#[test]
fn top_level_or_predicate_is_parenthesised() {
    assert_eq!(
        spliced("SELECT g FROM t WHERE a OR b GROUP BY g"),
        "SELECT g FROM t WHERE (a OR b) AND EXISTS (SELECT 1 FROM aff) GROUP BY g"
    );
}

#[test]
fn chained_or_predicate_is_parenthesised_once() {
    assert_eq!(
        spliced("SELECT g FROM t WHERE a OR b OR c GROUP BY g"),
        "SELECT g FROM t WHERE (a OR b OR c) AND EXISTS (SELECT 1 FROM aff) GROUP BY g"
    );
}

#[test]
fn lower_case_or_mixed_with_and_is_parenthesised() {
    assert_eq!(
        spliced("select g from t where a and b or c group by g"),
        "select g from t where (a and b or c) AND EXISTS (SELECT 1 FROM aff) group by g"
    );
}

#[test]
fn already_parenthesised_or_is_left_as_written() {
    assert_eq!(
        spliced("SELECT g FROM t WHERE (a OR b) AND c GROUP BY g"),
        "SELECT g FROM t WHERE (a OR b) AND c AND EXISTS (SELECT 1 FROM aff) GROUP BY g"
    );
}

#[test]
fn and_only_predicate_is_left_as_written() {
    assert_eq!(
        spliced("SELECT g FROM t WHERE a AND b GROUP BY g"),
        "SELECT g FROM t WHERE a AND b AND EXISTS (SELECT 1 FROM aff) GROUP BY g"
    );
}

#[test]
fn or_inside_a_literal_is_left_as_written() {
    assert_eq!(
        spliced("SELECT g FROM t WHERE city <> 'x or y' GROUP BY g"),
        "SELECT g FROM t WHERE city <> 'x or y' AND EXISTS (SELECT 1 FROM aff) GROUP BY g"
    );
}

#[test]
fn or_inside_a_subquery_is_left_as_written() {
    assert_eq!(
        spliced("SELECT g FROM t WHERE a IN (SELECT x FROM u WHERE p OR q) GROUP BY g"),
        "SELECT g FROM t WHERE a IN (SELECT x FROM u WHERE p OR q) AND EXISTS (SELECT 1 FROM aff) GROUP BY g"
    );
}

#[test]
fn or_in_a_join_condition_without_where_is_left_as_written() {
    assert_eq!(
        spliced("SELECT g FROM t JOIN u ON t.k = u.k OR t.j = u.j GROUP BY g"),
        "SELECT g FROM t JOIN u ON t.k = u.k OR t.j = u.j WHERE EXISTS (SELECT 1 FROM aff) GROUP BY g"
    );
}

#[test]
fn or_in_a_join_condition_before_where_is_left_as_written() {
    assert_eq!(
        spliced("SELECT g FROM t JOIN u ON t.k = u.k OR t.j = u.j WHERE a GROUP BY g"),
        "SELECT g FROM t JOIN u ON t.k = u.k OR t.j = u.j WHERE a AND EXISTS (SELECT 1 FROM aff) GROUP BY g"
    );
}

fn text_search_splice(query: &str, and_fragment: &str) -> Option<String> {
    let upper = query.to_uppercase();
    let gb_pos = upper.rfind(" GROUP BY ")?;
    let fragment = if upper[..gb_pos].contains(" WHERE ") {
        and_fragment.to_string()
    } else {
        format!(
            " WHERE {}",
            and_fragment.trim_start().trim_start_matches("AND ")
        )
    };
    Some(format!(
        "{}{}{}",
        &query[..gb_pos],
        fragment,
        &query[gb_pos..]
    ))
}

proptest::proptest! {
    #[test]
    fn agrees_with_text_search_without_quotes_parentheses_or_alternatives(
        query in "[A-NP-Za-np-z0-9_ ,.=<>*+]{0,60}( (GROUP BY|group by|WHERE|where|AND|JOIN) [A-NP-Za-np-z0-9_ ,.=<>*+]{0,30}){0,4}"
    ) {
        proptest::prop_assert_eq!(
            splice_before_group_by(&query, FILTER),
            text_search_splice(&query, FILTER)
        );
    }
}
