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
        ("a".to_string(), "pg_reflex: compacted 'a' — ...".to_string()),
        ("b".to_string(), "pg_reflex: compacted 'b' — ...".to_string()),
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
