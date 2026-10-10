//! Unit tests for create_ivm helpers.
//!
//! Pure-Rust coverage of `count_equalities_involving_source` and the
//! parsing logic feeding `build_source_join_keys`. The full
//! `build_source_join_keys` (which queries pg_catalog) is exercised by
//! the `#[pg_test]` integration tests in `pg_test_directional_dispatch.rs`.

use super::*;
use std::collections::HashMap;

fn aliases(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(a, t)| (a.to_string(), t.to_string()))
        .collect()
}

#[test]
fn test_count_equalities_single_join_one_eq() {
    // demand_planning.id = sales_simulation.dem_plan_id
    let cond = "demand_planning.id = sales_simulation.dem_plan_id";
    let n = count_equalities_involving_source(cond, "demand_planning", &aliases(&[]));
    assert_eq!(n, 1, "expected single equality involving demand_planning");
    // Same condition viewed from the other source — still 1
    let n2 = count_equalities_involving_source(cond, "sales_simulation", &aliases(&[]));
    assert_eq!(n2, 1);
}

#[test]
fn test_count_equalities_composite_join_two_eq() {
    // pricing JOIN with composite key: assortment_id + product_id
    let cond = "demand_planning.assortment_id = pricing.assortment_id \
                AND sales_simulation.product_id = pricing.product_id";
    let n = count_equalities_involving_source(cond, "pricing", &aliases(&[]));
    assert_eq!(n, 2, "expected 2 equalities involving pricing");
    // Each "other side" source sees only its own equality.
    let n_dp = count_equalities_involving_source(cond, "demand_planning", &aliases(&[]));
    assert_eq!(n_dp, 1);
    let n_ss = count_equalities_involving_source(cond, "sales_simulation", &aliases(&[]));
    assert_eq!(n_ss, 1);
}

#[test]
fn test_count_equalities_resolves_table_aliases() {
    // Aliased: `dp` alias for demand_planning
    let cond = "dp.id = ss.dem_plan_id";
    let map = aliases(&[("dp", "demand_planning"), ("ss", "sales_simulation")]);
    let n = count_equalities_involving_source(cond, "demand_planning", &map);
    assert_eq!(
        n, 1,
        "alias `dp` must resolve to demand_planning for equality counting"
    );
    let n_ss = count_equalities_involving_source(cond, "sales_simulation", &map);
    assert_eq!(n_ss, 1);
    // An unrelated source must not match.
    let n_other = count_equalities_involving_source(cond, "pricing", &map);
    assert_eq!(n_other, 0);
}

#[test]
fn test_count_equalities_ignores_non_equality_predicates() {
    let cond = "dp.id = ss.dem_plan_id AND ss.amount > 0";
    let map = aliases(&[("dp", "demand_planning"), ("ss", "sales_simulation")]);
    let n = count_equalities_involving_source(cond, "demand_planning", &map);
    assert_eq!(n, 1, "non-equality predicates must not be counted");
}

#[test]
fn test_count_equalities_no_match_returns_zero() {
    let cond = "x.a = y.b";
    let n = count_equalities_involving_source(cond, "demand_planning", &aliases(&[]));
    assert_eq!(n, 0);
}

#[test]
fn test_count_equalities_case_insensitive() {
    // Mixed-case input — the function lowercases both sides.
    let cond = "Demand_Planning.id = Sales_Simulation.Dem_Plan_Id";
    let n = count_equalities_involving_source(cond, "demand_planning", &aliases(&[]));
    assert_eq!(n, 1);
}

// ---- 1.5.1 coverage: plan_compact_imv ----

#[test]
fn cov_plan_compact_imv_happy() {
    let stmts = plan_compact_imv("my_view").expect("happy");
    assert_eq!(stmts.len(), 2);
    assert!(stmts[0].starts_with("VACUUM (FULL)"));
    assert!(stmts[0].contains("__reflex_intermediate_my_view"));
    assert!(stmts[1].starts_with("VACUUM (FULL)"));
    assert!(stmts[1].contains("my_view"));
}

#[test]
fn cov_plan_compact_imv_schema_qualified() {
    let stmts = plan_compact_imv("mysch.myview").expect("schema-qual");
    assert_eq!(stmts.len(), 2);
    assert!(stmts[0].contains("mysch"));
    assert!(stmts[1].contains("mysch"));
    assert!(stmts[1].contains("myview"));
}

#[test]
fn cov_plan_compact_imv_invalid_name_returns_error() {
    let result = plan_compact_imv("1bad-name");
    assert!(result.is_err());
    let msg = result.unwrap_err();
    assert!(msg.starts_with("ERROR"));
}

#[test]
fn cov_plan_compact_imv_empty_name_returns_error() {
    let result = plan_compact_imv("");
    assert!(result.is_err());
}

#[test]
fn cov_plan_compact_imv_double_dot_returns_error() {
    let result = plan_compact_imv("foo..bar");
    assert!(result.is_err());
}

// ---- build_compact_all_summary ----

#[test]
fn cov_build_compact_all_summary_all_success() {
    let names = vec!["a".to_string(), "b".to_string()];
    let results = vec![
        (
            "a".to_string(),
            "pg_reflex: compacted 'a' — ...".to_string(),
        ),
        (
            "b".to_string(),
            "pg_reflex: compacted 'b' — ...".to_string(),
        ),
    ];
    let summary = build_compact_all_summary(&names, &results, 123);
    assert!(summary.contains("compacted 2/2"));
    assert!(summary.contains("123 ms"));
    assert!(!summary.contains("failures"));
}

#[test]
fn cov_build_compact_all_summary_with_failures() {
    let names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let results = vec![
        ("a".to_string(), "pg_reflex: compacted 'a'".to_string()),
        ("b".to_string(), "ERROR: boom".to_string()),
        ("c".to_string(), "pg_reflex: compacted 'c'".to_string()),
    ];
    let summary = build_compact_all_summary(&names, &results, 456);
    assert!(summary.contains("compacted 2/3"));
    assert!(summary.contains("failures:"));
    assert!(summary.contains("b: ERROR: boom"));
}

#[test]
fn cov_build_compact_all_summary_all_failures() {
    let names = vec!["a".to_string()];
    let results = vec![("a".to_string(), "ERROR: bad".to_string())];
    let summary = build_compact_all_summary(&names, &results, 0);
    assert!(summary.contains("compacted 0/1"));
    assert!(summary.contains("a: ERROR: bad"));
}

// ---- format_compact_imv_summary ----

#[test]
fn cov_format_compact_imv_summary_two_stmts() {
    let per_stmt = vec![
        ("VACUUM (FULL) __reflex_intermediate_v".to_string(), 100u128),
        ("VACUUM (FULL) v".to_string(), 200u128),
    ];
    let summary = format_compact_imv_summary("v", &per_stmt);
    assert!(summary.contains("compacted 'v'"));
    assert!(summary.contains("100 ms"));
    assert!(summary.contains("200 ms"));
    assert!(summary.contains(", "));
}

#[test]
fn cov_format_compact_imv_summary_empty() {
    let summary = format_compact_imv_summary("v", &[]);
    assert!(summary.contains("compacted 'v'"));
    assert!(summary.ends_with("— "));
}

#[test]
fn cov_format_compact_imv_summary_single() {
    let per_stmt = vec![("VACUUM x".to_string(), 50u128)];
    let summary = format_compact_imv_summary("x", &per_stmt);
    assert!(summary.contains("50 ms"));
}

// ---- split_top_level_and / split_top_level_eq: string-literal tokenizing ----
// These guard the NOT-NULL equi-join inference (inner_join_equi_non_null_refs):
// a misparsed string literal must never let an in-literal ` AND ` / `=` be
// mistaken for a top-level token. SQL escaped quotes are adjacent pairs (`''`),
// so toggling in_str on every quote is correct — these tests freeze that.

#[test]
fn split_and_keeps_plain_literal_with_and_intact() {
    let parts = split_top_level_and("a.x = b.y AND c.note = 'foo AND bar'");
    assert_eq!(parts, vec!["a.x = b.y", "c.note = 'foo AND bar'"]);
}

#[test]
fn split_and_keeps_escaped_quote_literal_with_and_intact() {
    // 'it''s AND fine' is ONE literal (escaped quote); its inner AND must not split.
    let parts = split_top_level_and("a.x = b.y AND c.note = 'it''s AND fine'");
    assert_eq!(parts, vec!["a.x = b.y", "c.note = 'it''s AND fine'"]);
}

#[test]
fn split_eq_ignores_equals_inside_literal() {
    // The only top-level `=` is after `c.note`; the `=` inside 'a=b' must be skipped.
    let (l, r) = split_top_level_eq("c.note = 'a=b'").expect("top-level eq present");
    assert_eq!(l.trim(), "c.note");
    assert_eq!(r.trim(), "'a=b'");
}

#[test]
fn split_eq_none_when_only_equals_is_inside_literal() {
    assert!(split_top_level_eq("'a=b'").is_none());
}

#[test]
fn reflex_reject_carries_tag_and_error_prefix() {
    let msg = crate::reflex_reject("storage must be 'LOGGED' or 'UNLOGGED'");
    assert!(msg.starts_with("ERROR: "), "must keep ERROR: prefix: {msg}");
    assert!(
        msg.contains(crate::REFLEX_UNSUPPORTED_TAG),
        "must carry the unsupported tag: {msg}"
    );
}

fn plan_from_sql(sql: &str) -> crate::aggregation::AggregationPlan {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    let parsed = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    let analysis = crate::sql_analyzer::analyze(&parsed).unwrap();
    crate::aggregation::plan_aggregation(&analysis)
}

fn sources(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_keyless_immediate_passthrough_warns_with_source_and_remedy() {
    let plan = plan_from_sql("SELECT g, v FROM public.kl");
    let msg = keyless_immediate_warning("kl_v", "IMMEDIATE", &plan, &sources(&["public.kl"]), &[])
        .expect("keyless IMMEDIATE passthrough warns");
    assert!(msg.contains("'kl_v'"), "{msg}");
    assert!(msg.contains("public.kl"), "{msg}");
    assert!(msg.contains("fully refreshes the IMV"), "{msg}");
    assert!(
        msg.contains("unique key") && msg.contains("DEFERRED"),
        "{msg}"
    );
}

#[test]
fn test_keyless_warning_silent_when_keyed_deferred_aggregate_or_ignored() {
    let mut keyed = plan_from_sql("SELECT id, v FROM public.kl");
    keyed.passthrough_key_mappings.insert(
        "public.kl".to_string(),
        vec![("id".to_string(), "id".to_string())],
    );
    let src = sources(&["public.kl"]);
    assert_eq!(
        keyless_immediate_warning("v", "IMMEDIATE", &keyed, &src, &[]),
        None
    );

    let keyless = plan_from_sql("SELECT g, v FROM public.kl");
    assert_eq!(
        keyless_immediate_warning("v", "DEFERRED", &keyless, &src, &[]),
        None
    );
    assert_eq!(
        keyless_immediate_warning("v", "IMMEDIATE", &keyless, &src, &sources(&["kl"])),
        None
    );

    let aggregate = plan_from_sql("SELECT g, SUM(v) FROM public.kl GROUP BY g");
    assert_eq!(
        keyless_immediate_warning("v", "IMMEDIATE", &aggregate, &src, &[]),
        None
    );
}

#[test]
fn test_keyless_warning_lists_only_unmapped_join_sources() {
    let mut plan =
        plan_from_sql("SELECT a.id, b.v FROM public.a a LEFT JOIN public.b b ON b.k = a.id");
    plan.passthrough_key_mappings.insert(
        "public.a".to_string(),
        vec![("id".to_string(), "id".to_string())],
    );
    let msg = keyless_immediate_warning(
        "j",
        "IMMEDIATE",
        &plan,
        &sources(&["public.a", "public.b"]),
        &[],
    )
    .expect("unmapped secondary warns");
    assert!(
        msg.contains("public.b") && !msg.contains("public.a"),
        "{msg}"
    );
}

// ---- split_top_level_and: offsets stay valid next to non-ASCII text ----

fn legacy_split_top_level_and(cond: &str) -> Vec<String> {
    let bytes = cond.as_bytes();
    let upper = cond.to_uppercase();
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            if c == '\'' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        match c {
            '\'' => in_str = true,
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {
                if depth == 0 && upper[i..].starts_with(" AND ") {
                    parts.push(cond[start..i].to_string());
                    i += 5;
                    start = i;
                    continue;
                }
            }
        }
        i += 1;
    }
    parts.push(cond[start..].to_string());
    parts
}

#[test]
fn split_and_after_literal_that_grows_when_uppercased() {
    assert_eq!(
        split_top_level_and("s.tag <> 'ŉ' AND xb.k = s.xk"),
        vec!["s.tag <> 'ŉ'", "xb.k = s.xk"]
    );
}

#[test]
fn split_and_after_literal_that_shrinks_when_uppercased() {
    assert_eq!(
        split_top_level_and("s.tag <> 'Kış' AND xb.k = s.xk"),
        vec!["s.tag <> 'Kış'", "xb.k = s.xk"]
    );
}

#[test]
fn split_and_with_multibyte_identifier_outside_literal() {
    assert_eq!(
        split_top_level_and("d.\"clé\" = s.kind and d.été = s.ŉ AND d.x = s.y"),
        vec!["d.\"clé\" = s.kind", "d.été = s.ŉ", "d.x = s.y"]
    );
}

#[test]
fn split_and_matches_legacy_on_ascii_conditions() {
    const FRAGMENTS: &[&str] = &[
        " AND ", " and ", " And ", " OR ", "a.x", "b.y", " = ", " <> ", "'", "''", "' AND '", "(",
        ")", " ", "AND", "1", "\"q\"", "sand ", " andy",
    ];
    let mut state = 0x5eed_u64;
    let mut below = |bound: usize| {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 33) as usize) % bound
    };
    let mut split_cases = 0;
    for _ in 0..5000 {
        let count = 1 + below(12);
        let cond: String = (0..count)
            .map(|_| FRAGMENTS[below(FRAGMENTS.len())])
            .collect();
        let expected = legacy_split_top_level_and(&cond);
        assert_eq!(split_top_level_and(&cond), expected, "cond: {cond:?}");
        split_cases += usize::from(expected.len() > 1);
    }
    assert!(split_cases > 500, "split only {split_cases}");
}

#[test]
fn timezone_warning_names_view_and_functions_and_pins_remedy() {
    let parsed = Parser::parse_sql(
        &PostgreSqlDialect {},
        "SELECT date_trunc('day', ts) AS d, to_char(ts, 'YYYY') AS y, SUM(v) AS s FROM t GROUP BY 1, 2",
    )
    .unwrap();
    let analysis = analyze(&parsed).unwrap();
    let msg = timezone_sensitivity_warning("my_view", &analysis).expect("warns");
    assert!(msg.contains("'my_view'"), "{msg}");
    assert!(msg.contains("date_trunc, to_char"), "{msg}");
    assert!(msg.contains("TimeZone"), "{msg}");
    assert!(msg.contains("date_trunc('day', ts, 'UTC')"), "{msg}");
}

#[test]
fn timezone_warning_is_silent_for_zone_independent_functions() {
    for sql in [
        "SELECT id, length(g) AS l, concat(g, 'x') AS c, upper(g) AS u FROM t",
        "SELECT date_trunc('day', ts, 'UTC') AS d, SUM(v) AS s FROM t GROUP BY 1",
        "SELECT g, SUM(v) AS s FROM t GROUP BY g",
    ] {
        let parsed = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let analysis = analyze(&parsed).unwrap();
        assert_eq!(
            timezone_sensitivity_warning("my_view", &analysis),
            None,
            "{sql}"
        );
    }
}

#[test]
fn timezone_warning_promises_silence_only_for_three_argument_date_trunc() {
    let parsed = Parser::parse_sql(
        &PostgreSqlDialect {},
        "SELECT date_trunc('day', ts) AS d, SUM(v) AS s FROM t GROUP BY 1",
    )
    .unwrap();
    let analysis = analyze(&parsed).unwrap();
    let msg = timezone_sensitivity_warning("my_view", &analysis).expect("warns");
    assert!(
        msg.contains("date_trunc('day', ts, 'UTC') clears this warning"),
        "{msg}"
    );
    assert!(msg.contains("by name and remains"), "{msg}");
    assert!(!msg.contains("or ts AT TIME ZONE"), "{msg}");
    assert!(!msg.contains('\n'), "{msg}");
}

// ---------- CTE references pointed at their sub-IMVs ----------

fn sub_imvs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(cte, sub_imv)| (cte.to_string(), sub_imv.to_string()))
        .collect()
}

/// A query whose every CTE reference carries its own alias, and that spells
/// the CTE nowhere else, is rewritten exactly as the textual replacement of
/// the CTE name by the quoted sub-IMV name does.
#[test]
fn aliased_cte_references_are_rewritten_as_the_textual_replacement_does() {
    let map = sub_imvs(&[("base", "v__cte_base"), ("agg", "sch.v__cte_agg")]);
    for query in [
        "SELECT b.id, b.v FROM base AS b WHERE b.v > 1",
        "SELECT b.g, SUM(b.v) AS total FROM base AS b GROUP BY b.g",
        "SELECT b.g, a.total FROM base AS b JOIN agg AS a ON a.g = b.g",
        "SELECT b.g FROM base AS b LEFT JOIN agg AS a ON a.g = b.g WHERE a.total IS NULL",
        "SELECT x.g FROM (SELECT b.g FROM base AS b) AS x",
        "SELECT o.id FROM other AS o WHERE EXISTS (SELECT 1 FROM agg AS a WHERE a.g = o.g)",
        "SELECT o.id, o.v FROM other AS o WHERE o.v > 0",
        "SELECT b.g FROM base AS b UNION ALL SELECT a.g FROM agg AS a",
    ] {
        let textual = map.iter().fold(query.to_string(), |sql, (cte, sub_imv)| {
            crate::sql_writer::identifier::replace_identifier(
                &sql,
                cte,
                &crate::query_decomposer::quote_identifier(sub_imv),
            )
        });
        assert_eq!(cte_query_reading_sub_imvs(query, &map), textual, "{query}");
    }
}

#[test]
fn unaliased_cte_reference_keeps_the_cte_name_as_alias() {
    let map = sub_imvs(&[("e", "v__cte_e")]);
    assert_eq!(
        cte_query_reading_sub_imvs("SELECT e.season, s FROM e WHERE e.s > 1", &map),
        "SELECT e.season, s FROM \"v__cte_e\" AS e WHERE e.s > 1"
    );
    assert_eq!(
        cte_query_reading_sub_imvs("SELECT season FROM E", &map),
        "SELECT season FROM \"v__cte_e\" AS E"
    );
}

#[test]
fn only_relation_references_to_a_cte_are_rewritten() {
    let map = sub_imvs(&[("e", "v__cte_e")]);
    for (query, rewritten) in [
        (
            "SELECT season, 'e' AS tag FROM e WHERE season <> 'e' AND note <> 'a-e-b'",
            "SELECT season, 'e' AS tag FROM \"v__cte_e\" AS e WHERE season <> 'e' AND note <> 'a-e-b'",
        ),
        (
            "SELECT season, e FROM e WHERE e > 2 GROUP BY season, e",
            "SELECT season, e FROM \"v__cte_e\" AS e WHERE e > 2 GROUP BY season, e",
        ),
        (
            "SELECT o.e, e(o.v) FROM public.e AS o JOIN other AS x ON x.e = o.e",
            "SELECT o.e, e(o.v) FROM public.e AS o JOIN other AS x ON x.e = o.e",
        ),
    ] {
        assert_eq!(cte_query_reading_sub_imvs(query, &map), rewritten);
    }
}

#[test]
fn query_of_the_first_cte_is_kept_as_written() {
    let query = "SELECT  season,sum(v) as s FROM t GROUP BY season";
    assert_eq!(cte_query_reading_sub_imvs(query, &[]), query);
}

fn aggregate_join_key_mappings(
    condition: &str,
    secondary: &str,
    aliases: &[(&str, &str)],
    group_column: &str,
    group_expression: &str,
) -> Vec<(String, String)> {
    let aliases: HashMap<String, String> = aliases
        .iter()
        .map(|(alias, table)| (alias.to_string(), table.to_string()))
        .collect();
    let expressions: HashMap<String, String> =
        HashMap::from([(group_column.to_string(), group_expression.to_string())]);
    soundness::parse_join_condition_mappings(
        condition,
        secondary,
        &aliases,
        &[group_column.to_string()],
        &expressions,
        soundness::KeyColumnMatch::EitherSideOfTheEquality,
    )
}

#[test]
fn a_group_column_maps_to_a_join_column_only_when_it_is_a_side_of_the_equality() {
    let aliases = [("f", "fact"), ("d", "dim")];
    let mapped = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(target, source)| (target.to_string(), source.to_string()))
            .collect()
    };
    for (condition, secondary, group_expression, expected) in [
        ("d.id = f.k", "dim", "d.k", mapped(&[])),
        ("d.id = f.k", "fact", "d.k", mapped(&[])),
        ("f.k = d.k", "dim", "f.k", mapped(&[("k", "k")])),
        ("f.k = d.k", "dim", "d.k", mapped(&[("k", "k")])),
        ("f.k = d.k", "fact", "d.k", mapped(&[("k", "k")])),
        ("f.k = d.k", "dim", "public.fact.k", mapped(&[])),
        (
            "fact.k = dim.k",
            "dim",
            "public.fact.k",
            mapped(&[("k", "k")]),
        ),
        (
            "fact.k = dim.k",
            "dim",
            "public.dim.k",
            mapped(&[("k", "k")]),
        ),
        ("fact.k = dim.id", "dim", "public.dim.k", mapped(&[])),
        ("f.k = d.k", "dim", "k", mapped(&[("k", "k")])),
        ("d.kk = f.k", "dim", "f.k", mapped(&[("k", "kk")])),
    ] {
        assert_eq!(
            aggregate_join_key_mappings(condition, secondary, &aliases, "k", group_expression),
            expected,
            "{condition} / {secondary} / {group_expression}"
        );
    }
}
