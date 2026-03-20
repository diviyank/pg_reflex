use pgrx::prelude::*;

use crate::aggregation::AggregationPlan;
use crate::query_decomposer::{bare_column_name, delta_table_new, delta_table_old, intermediate_table_name};

/// Whether a delta adds or subtracts from the intermediate table.
#[derive(Clone, Copy)]
pub enum DeltaOp {
    Add,
    Subtract,
}

/// Build an INSERT ... ON CONFLICT DO UPDATE SET statement
/// that merges a delta query into the intermediate table.
pub fn build_upsert_sql(
    intermediate_tbl: &str,
    delta_query: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) -> String {
    // Conflict columns = group_by + distinct (bare names)
    let mut conflict_cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| format!("\"{}\"", bare_column_name(c)))
        .collect();

    // For aggregates without GROUP BY: use sentinel column
    if conflict_cols.is_empty() && !plan.intermediate_columns.is_empty() {
        conflict_cols.push("__reflex_group".to_string());
    }

    let operator = match op {
        DeltaOp::Add => "+",
        DeltaOp::Subtract => "-",
    };

    let mut set_clauses: Vec<String> = Vec::new();

    for ic in &plan.intermediate_columns {
        match (ic.source_aggregate.as_str(), op) {
            ("MIN", DeltaOp::Add) => {
                set_clauses.push(format!(
                    "\"{}\" = LEAST({}.\"{}\", EXCLUDED.\"{}\")",
                    ic.name, intermediate_tbl, ic.name, ic.name
                ));
            }
            ("MAX", DeltaOp::Add) => {
                set_clauses.push(format!(
                    "\"{}\" = GREATEST({}.\"{}\", EXCLUDED.\"{}\")",
                    ic.name, intermediate_tbl, ic.name, ic.name
                ));
            }
            ("MIN", DeltaOp::Subtract) | ("MAX", DeltaOp::Subtract) => {
                // MIN/MAX can't be decremented algebraically.
                // Set to NULL; will be recomputed by build_min_max_recompute_sql.
                set_clauses.push(format!("\"{}\" = NULL", ic.name));
            }
            _ => {
                // SUM, COUNT: additive with +/-
                set_clauses.push(format!(
                    "\"{}\" = {}.\"{}\" {} EXCLUDED.\"{}\"",
                    ic.name, intermediate_tbl, ic.name, operator, ic.name
                ));
            }
        }
    }

    // __ivm_count always uses +/-
    if plan.needs_ivm_count {
        set_clauses.push(format!(
            "__ivm_count = {}.__ivm_count {} EXCLUDED.__ivm_count",
            intermediate_tbl, operator
        ));
    }

    format!(
        "INSERT INTO {} {} ON CONFLICT ({}) DO UPDATE SET {}",
        intermediate_tbl,
        delta_query,
        conflict_cols.join(", "),
        set_clauses.join(", ")
    )
}

/// Build a SQL UPDATE that recomputes MIN/MAX columns from the source table
/// for groups whose MIN/MAX was set to NULL by a subtract operation.
/// Returns None if the plan has no MIN/MAX columns.
pub fn build_min_max_recompute_sql(
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    source_table: &str,
) -> Option<String> {
    let min_max_cols: Vec<&crate::aggregation::IntermediateColumn> = plan
        .intermediate_columns
        .iter()
        .filter(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX")
        .collect();

    if min_max_cols.is_empty() {
        return None;
    }

    let group_cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| bare_column_name(c).to_string())
        .collect();

    let mut set_parts: Vec<String> = Vec::new();
    for ic in &min_max_cols {
        let join_cond: Vec<String> = group_cols
            .iter()
            .map(|gc| format!("{}.\"{}\" = {}.\"{}\"", source_table, gc, intermediate_tbl, gc))
            .collect();

        set_parts.push(format!(
            "\"{}\" = (SELECT {}({}) FROM {} WHERE {})",
            ic.name,
            ic.source_aggregate,
            ic.source_arg,
            source_table,
            join_cond.join(" AND ")
        ));
    }

    let null_check: Vec<String> = min_max_cols
        .iter()
        .map(|ic| format!("{}.\"{}\" IS NULL", intermediate_tbl, ic.name))
        .collect();

    Some(format!(
        "UPDATE {} SET {} WHERE {}",
        intermediate_tbl,
        set_parts.join(", "),
        null_check.join(" OR ")
    ))
}


/// Generates the SQL statements to apply a delta to an IMV.
///
/// Called from plpgsql trigger wrappers. Returns a delimiter-separated string
/// of SQL statements for the plpgsql function to EXECUTE.
#[pg_extern]
pub fn reflex_build_delta_sql(
    view_name: &str,
    source_table: &str,
    operation: &str,
    base_query: &str,
    end_query: &str,
    aggregations_json: &str,
) -> String {
    let plan: AggregationPlan = match serde_json::from_str(aggregations_json) {
        Ok(p) => p,
        Err(_) => return String::new(),
    };

    let intermediate_tbl = intermediate_table_name(view_name);
    let new_tbl = delta_table_new(view_name);
    let old_tbl = delta_table_old(view_name);

    let mut stmts: Vec<String> = Vec::new();

    if plan.is_passthrough {
        match operation {
            "INSERT" => {
                let delta_q = base_query.replace(source_table, &format!("\"{}\"", new_tbl));
                stmts.push(format!("INSERT INTO \"{}\" {}", view_name, delta_q));
            }
            "DELETE" | "UPDATE" => {
                stmts.push(format!("DELETE FROM \"{}\"", view_name));
                stmts.push(format!("INSERT INTO \"{}\" {}", view_name, base_query));
            }
            _ => {}
        }
    } else {
        let has_min_max = plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX");

        match operation {
            "INSERT" => {
                let delta_q = base_query.replace(source_table, &format!("\"{}\"", new_tbl));
                stmts.push(build_upsert_sql(&intermediate_tbl, &delta_q, &plan, DeltaOp::Add));
            }
            "DELETE" => {
                let delta_q = base_query.replace(source_table, &format!("\"{}\"", old_tbl));
                stmts.push(build_upsert_sql(&intermediate_tbl, &delta_q, &plan, DeltaOp::Subtract));
                if has_min_max {
                    if let Some(recompute) = build_min_max_recompute_sql(&intermediate_tbl, &plan, source_table) {
                        stmts.push(recompute);
                    }
                }
            }
            "UPDATE" => {
                let delta_old = base_query.replace(source_table, &format!("\"{}\"", old_tbl));
                stmts.push(build_upsert_sql(&intermediate_tbl, &delta_old, &plan, DeltaOp::Subtract));
                if has_min_max {
                    if let Some(recompute) = build_min_max_recompute_sql(&intermediate_tbl, &plan, source_table) {
                        stmts.push(recompute);
                    }
                }
                let delta_new = base_query.replace(source_table, &format!("\"{}\"", new_tbl));
                stmts.push(build_upsert_sql(&intermediate_tbl, &delta_new, &plan, DeltaOp::Add));
            }
            _ => {}
        }

        // Refresh target from intermediate
        stmts.push(format!("DELETE FROM \"{}\"", view_name));
        stmts.push(format!("INSERT INTO \"{}\" {}", view_name, end_query));
    }

    // Update last_update_date
    stmts.push(format!(
        "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = '{}'",
        view_name
    ));

    stmts.join("\n--<<REFLEX_SEP>>--\n")
}

/// Generates SQL statements to handle a TRUNCATE on a source table.
/// TRUNCATE has no transition tables, so we clear intermediate + target entirely.
#[pg_extern]
pub fn reflex_build_truncate_sql(view_name: &str) -> String {
    let intermediate_tbl = intermediate_table_name(view_name);

    // Check if this is a passthrough IMV by reading aggregations from the reference table
    let agg_json: String = Spi::get_one::<&str>(&format!(
        "SELECT aggregations::text FROM public.__reflex_ivm_reference WHERE name = '{}'",
        view_name
    ))
    .unwrap_or(None)
    .unwrap_or("{}")
    .to_string();

    let is_passthrough = if let Ok(plan) =
        serde_json::from_str::<AggregationPlan>(&agg_json)
    {
        plan.is_passthrough
    } else {
        false
    };

    let mut stmts: Vec<String> = Vec::new();

    if is_passthrough {
        // Passthrough: just clear the target, then re-insert from source (which is now empty)
        stmts.push(format!("DELETE FROM \"{}\"", view_name));
    } else {
        // Aggregate: clear intermediate and target
        stmts.push(format!("TRUNCATE {}", intermediate_tbl));
        stmts.push(format!("DELETE FROM \"{}\"", view_name));
    }

    // Update last_update_date
    stmts.push(format!(
        "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = '{}'",
        view_name
    ));

    stmts.join("\n--<<REFLEX_SEP>>--\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::{AggregationPlan, EndQueryMapping, IntermediateColumn};

    fn simple_plan() -> AggregationPlan {
        AggregationPlan {
            group_by_columns: vec!["city".to_string()],
            intermediate_columns: vec![IntermediateColumn {
                name: "__sum_amount".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "SUM".to_string(),
                source_arg: "amount".to_string(),
            }],
            end_query_mappings: vec![EndQueryMapping {
                intermediate_expr: "__sum_amount".to_string(),
                output_alias: "total".to_string(),
                aggregate_type: "SUM".to_string(),
            }],
            has_distinct: false,
            needs_ivm_count: true,
            distinct_columns: vec![],
            is_passthrough: false,
        }
    }

    #[test]
    fn test_build_upsert_add() {
        let plan = simple_plan();
        let delta = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"__reflex_new_v\" GROUP BY city";
        let sql = build_upsert_sql("__reflex_intermediate_v", delta, &plan, DeltaOp::Add);
        assert!(sql.contains("INSERT INTO __reflex_intermediate_v"));
        assert!(sql.contains("ON CONFLICT (\"city\")"));
        assert!(sql.contains("__sum_amount\" + EXCLUDED"));
        assert!(sql.contains("__ivm_count + EXCLUDED"));
    }

    #[test]
    fn test_build_upsert_subtract() {
        let plan = simple_plan();
        let delta = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"__reflex_old_v\" GROUP BY city";
        let sql = build_upsert_sql("__reflex_intermediate_v", delta, &plan, DeltaOp::Subtract);
        assert!(sql.contains("__sum_amount\" - EXCLUDED"));
        assert!(sql.contains("__ivm_count - EXCLUDED"));
    }

    #[test]
    fn test_build_upsert_min_add() {
        let plan = AggregationPlan {
            group_by_columns: vec!["city".to_string()],
            intermediate_columns: vec![IntermediateColumn {
                name: "__min_price".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "MIN".to_string(),
                source_arg: "price".to_string(),
            }],
            end_query_mappings: vec![],
            has_distinct: false,
            needs_ivm_count: true,
            distinct_columns: vec![],
            is_passthrough: false,
        };
        let delta = "SELECT city, MIN(price) AS \"__min_price\", COUNT(*) AS __ivm_count FROM src GROUP BY city";
        let sql = build_upsert_sql("intermediate", delta, &plan, DeltaOp::Add);
        assert!(sql.contains("LEAST(intermediate.\"__min_price\", EXCLUDED.\"__min_price\")"));
    }

    #[test]
    fn test_build_upsert_min_subtract_sets_null() {
        let plan = AggregationPlan {
            group_by_columns: vec!["city".to_string()],
            intermediate_columns: vec![IntermediateColumn {
                name: "__min_price".to_string(),
                pg_type: "NUMERIC".to_string(),
                source_aggregate: "MIN".to_string(),
                source_arg: "price".to_string(),
            }],
            end_query_mappings: vec![],
            has_distinct: false,
            needs_ivm_count: true,
            distinct_columns: vec![],
            is_passthrough: false,
        };
        let delta = "SELECT city, MIN(price) FROM src GROUP BY city";
        let sql = build_upsert_sql("intermediate", delta, &plan, DeltaOp::Subtract);
        assert!(sql.contains("\"__min_price\" = NULL"));
    }

    #[test]
    fn test_min_max_recompute_sql() {
        let plan = AggregationPlan {
            group_by_columns: vec!["city".to_string()],
            intermediate_columns: vec![
                IntermediateColumn {
                    name: "__min_price".to_string(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "MIN".to_string(),
                    source_arg: "price".to_string(),
                },
                IntermediateColumn {
                    name: "__sum_amount".to_string(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "SUM".to_string(),
                    source_arg: "amount".to_string(),
                },
            ],
            end_query_mappings: vec![],
            has_distinct: false,
            needs_ivm_count: true,
            distinct_columns: vec![],
            is_passthrough: false,
        };
        let sql = build_min_max_recompute_sql("intermediate", &plan, "orders");
        assert!(sql.is_some());
        let sql = sql.unwrap();
        assert!(sql.contains("UPDATE intermediate SET"));
        assert!(sql.contains("SELECT MIN(price) FROM orders"));
        assert!(sql.contains("IS NULL"));
        assert!(!sql.contains("__sum_amount"));
    }

    #[test]
    fn test_no_min_max_recompute_for_sum_only() {
        let plan = simple_plan();
        let sql = build_min_max_recompute_sql("intermediate", &plan, "orders");
        assert!(sql.is_none());
    }
}
