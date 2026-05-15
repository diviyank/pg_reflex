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
