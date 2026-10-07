use super::*;
use crate::query_decomposer::split_qualified_name;

fn legacy_source_reference_count(base_query: &str, source_table: &str) -> usize {
    let bare_source = split_qualified_name(source_table).1;
    base_query
        .split_whitespace()
        .filter(|w| {
            let trimmed = w.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
            trimmed == source_table || trimmed == bare_source
        })
        .count()
}

fn legacy_follows_outer_join_keyword(bq_upper: &str, source_table: &str) -> bool {
    let bare_source = split_qualified_name(source_table).1;
    let src_upper = source_table.trim_matches('"').to_uppercase();
    let bare_upper = bare_source.trim_matches('"').to_uppercase();
    let patterns = [
        "LEFT JOIN ",
        "LEFT OUTER JOIN ",
        "RIGHT JOIN ",
        "RIGHT OUTER JOIN ",
        "FULL JOIN ",
        "FULL OUTER JOIN ",
    ];
    patterns.iter().any(|pat| {
        let mut search_from = 0;
        while let Some(pos) = bq_upper[search_from..].find(pat) {
            let after = &bq_upper[search_from + pos + pat.len()..];
            let next_token = after
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"');
            if next_token == src_upper || next_token == bare_upper {
                return true;
            }
            search_from += pos + pat.len();
        }
        false
    })
}

fn legacy_secondary_ref_identifiers(base_query: &str, source_table: &str) -> Vec<String> {
    let src_unquoted = source_table.trim_matches('"');
    let bare = split_qualified_name(src_unquoted)
        .1
        .trim_matches('"')
        .to_lowercase();
    let src_l = src_unquoted.to_lowercase();
    let toks: Vec<String> = base_query
        .split(|c: char| c.is_whitespace())
        .filter(|t| !t.is_empty())
        .map(|t| t.trim_matches('"').to_string())
        .collect();
    const KW: [&str; 17] = [
        "on",
        "group",
        "where",
        "left",
        "right",
        "inner",
        "join",
        "full",
        "outer",
        "order",
        "having",
        "limit",
        "union",
        "except",
        "intersect",
        "cross",
        "natural",
    ];
    let mut ids = vec![bare.clone()];
    for (i, tok) in toks.iter().enumerate() {
        let t = tok.to_lowercase();
        if t == src_l || t == bare {
            if let Some(next) = toks.get(i + 1) {
                let mut nx = next.trim_matches(',').to_lowercase();
                if nx == "as" {
                    if let Some(n2) = toks.get(i + 2) {
                        nx = n2.trim_matches(',').to_lowercase();
                    }
                }
                if !nx.is_empty()
                    && nx.chars().all(|c| c.is_alphanumeric() || c == '_')
                    && !KW.contains(&nx.as_str())
                {
                    ids.push(nx);
                }
            }
        }
    }
    ids
}

const QUALIFIED_SPELLINGS: [&str; 4] = ["alp.dim", "\"alp\".\"dim\"", "\"alp\".dim", "alp.\"dim\""];
const UNQUALIFIED_SPELLINGS: [&str; 2] = ["dim", "\"dim\""];
const OUTER_JOIN_KEYWORDS: [&str; 6] = [
    "LEFT JOIN",
    "LEFT OUTER JOIN",
    "RIGHT JOIN",
    "RIGHT OUTER JOIN",
    "FULL JOIN",
    "full outer join",
];

fn registered_and_referenced_spellings() -> Vec<(&'static str, &'static str)> {
    let mut pairs = Vec::new();
    for family in [&QUALIFIED_SPELLINGS[..], &UNQUALIFIED_SPELLINGS[..]] {
        for registered in family {
            for referenced in family {
                pairs.push((*registered, *referenced));
            }
        }
    }
    pairs
}

#[test]
fn self_join_is_counted_in_every_spelling_of_the_source() {
    for (registered, referenced) in registered_and_referenced_spellings() {
        let query = format!(
            "SELECT a.k, COUNT(*) AS c FROM {referenced} a JOIN {referenced} b ON a.k = b.k GROUP BY a.k"
        );
        assert_eq!(
            source_reference_count(&query, registered),
            2,
            "registered {registered}, query {query}"
        );
    }
}

#[test]
fn self_join_count_sees_references_next_to_punctuation() {
    assert_eq!(
        source_reference_count(
            "SELECT 1 FROM (\"alp\".dim a CROSS JOIN alp.\"dim\") , \"alp\".\"dim\", alp.dim",
            "alp.dim"
        ),
        4
    );
}

#[test]
fn self_join_count_ignores_other_tables() {
    for query in [
        "SELECT 1 FROM alp.dim_backup a JOIN \"alp\".\"dimension\" b ON a.k = b.k",
        "SELECT 1 FROM other.dim a JOIN \"other\".dim b ON a.k = b.k",
        "SELECT 1 FROM \"other\".\"dim\" a JOIN other.\"dim\" b ON a.k = b.k",
        "SELECT alp.dim.k FROM fact WHERE fact.k = alp.dim.k",
    ] {
        assert_eq!(source_reference_count(query, "alp.dim"), 0, "query {query}");
    }
}

#[test]
fn outer_join_secondary_is_recognised_in_every_spelling_of_the_source() {
    for (registered, referenced) in registered_and_referenced_spellings() {
        for keyword in OUTER_JOIN_KEYWORDS {
            let query = format!("SELECT d.label FROM fact f {keyword} {referenced} d ON d.k = f.k");
            assert!(
                follows_outer_join_keyword(&query.to_uppercase(), registered),
                "registered {registered}, query {query}"
            );
        }
    }
}

#[test]
fn outer_join_primary_is_not_a_secondary_in_any_spelling() {
    for (registered, referenced) in registered_and_referenced_spellings() {
        for keyword in OUTER_JOIN_KEYWORDS {
            let query = format!(
                "SELECT d.label FROM {referenced} d {keyword} fact f ON d.k = f.k JOIN {referenced} e ON e.k = f.k"
            );
            assert!(
                !follows_outer_join_keyword(&query.to_uppercase(), registered),
                "registered {registered}, query {query}"
            );
        }
    }
    assert!(!follows_outer_join_keyword(
        "SELECT 1 FROM FACT F LEFT JOIN \"OTHER\".DIM D ON D.K = F.K",
        "alp.dim"
    ));
}

#[test]
fn secondary_alias_is_collected_in_every_spelling_of_the_source() {
    for (registered, referenced) in registered_and_referenced_spellings() {
        for alias_clause in ["d", "AS d"] {
            let query = format!(
                "SELECT d.label FROM fact f LEFT JOIN {referenced} {alias_clause} ON d.k = f.k GROUP BY d.label"
            );
            assert_eq!(
                secondary_ref_identifiers(&query, registered),
                vec!["dim".to_string(), "d".to_string()],
                "registered {registered}, query {query}"
            );
        }
    }
}

#[test]
fn guards_match_legacy_when_the_source_is_referenced_unquoted() {
    const FRAGMENTS: &[&str] = &[
        "alp.dim",
        "dim",
        " alp.dim ",
        " dim ",
        "Dim",
        "ALP.DIM",
        "alp.dim.k",
        "alp.dim_backup",
        "other.dim",
        "fact",
        "alp.fact",
        " LEFT JOIN ",
        " left join ",
        " LEFT OUTER JOIN ",
        " RIGHT JOIN ",
        " RIGHT OUTER JOIN ",
        " FULL JOIN ",
        " FULL OUTER JOIN ",
        " JOIN ",
        " f ",
        " d ",
        " AS ",
        " as d",
        " ON ",
        "d.k = f.k",
        ",",
        "(",
        ")",
        " ",
        "\n",
        "SELECT ",
        " FROM ",
        " GROUP BY ",
        " WHERE ",
        "d.label",
        "'dim'",
        "'alp.dim'",
        "_",
        ".",
    ];
    let mut state = 0x9a2d_u64;
    let mut below = |bound: usize| {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 33) as usize) % bound
    };
    let mut self_joins = 0;
    let mut secondaries = 0;
    let mut aliased = 0;
    for _ in 0..8000 {
        let count = 1 + below(16);
        let query: String = (0..count)
            .map(|_| FRAGMENTS[below(FRAGMENTS.len())])
            .collect();
        for source in ["alp.dim", "dim"] {
            let occurrences = legacy_source_reference_count(&query, source);
            assert_eq!(
                source_reference_count(&query, source),
                occurrences,
                "source {source}, query {query:?}"
            );
            let upper = query.to_uppercase();
            let secondary = legacy_follows_outer_join_keyword(&upper, source);
            assert_eq!(
                follows_outer_join_keyword(&upper, source),
                secondary,
                "source {source}, query {query:?}"
            );
            let identifiers = legacy_secondary_ref_identifiers(&query, source);
            assert_eq!(
                secondary_ref_identifiers(&query, source),
                identifiers,
                "source {source}, query {query:?}"
            );
            self_joins += usize::from(occurrences > 1);
            secondaries += usize::from(secondary);
            aliased += usize::from(identifiers.len() > 1);
        }
    }
    assert!(self_joins > 200, "self-joins: {self_joins}");
    assert!(secondaries > 30, "secondaries: {secondaries}");
    assert!(aliased > 100, "aliased: {aliased}");
}
