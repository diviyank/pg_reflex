use super::*;
use crate::aggregation::{AggregationPlan, EndQueryMapping, IntermediateColumn};
use crate::schema_builder::build_trigger_ddls;

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
            cast_type: None,
        }],
        has_distinct: false,
        needs_ivm_count: true,
        distinct_columns: vec![],
        is_passthrough: false,
        passthrough_columns: vec![],
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: None,
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases: std::collections::HashMap::new(),
        output_column_order: vec![],
    }
}

#[test]
fn test_build_merge_add() {
    let plan = simple_plan();
    let delta = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"__reflex_new_v\" GROUP BY city";
    let sql = build_merge_sql("__reflex_intermediate_v", delta, &plan, DeltaOp::Add);
    assert!(sql.contains("MERGE INTO __reflex_intermediate_v AS t"));
    assert!(sql.contains("t.\"city\" IS NOT DISTINCT FROM d.\"city\""));
    assert!(sql.contains("COALESCE(t.\"__sum_amount\", 0) + COALESCE(d.\"__sum_amount\", 0)"));
    assert!(sql.contains("COALESCE(t.__ivm_count, 0) + COALESCE(d.__ivm_count, 0)"));
    assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
}

#[test]
fn test_build_merge_subtract() {
    let plan = simple_plan();
    let delta = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"__reflex_old_v\" GROUP BY city";
    let sql = build_merge_sql("__reflex_intermediate_v", delta, &plan, DeltaOp::Subtract);
    assert!(sql.contains("COALESCE(t.\"__sum_amount\", 0) - COALESCE(d.\"__sum_amount\", 0)"));
    assert!(sql.contains("COALESCE(t.__ivm_count, 0) - COALESCE(d.__ivm_count, 0)"));
    // Subtract should NOT have WHEN NOT MATCHED
    assert!(!sql.contains("WHEN NOT MATCHED"));
}

#[test]
fn test_build_merge_min_add() {
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
        passthrough_columns: vec![],
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: None,
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases: std::collections::HashMap::new(),
        output_column_order: vec![],
    };
    let delta = "SELECT city, MIN(price) AS \"__min_price\", COUNT(*) AS __ivm_count FROM src GROUP BY city";
    let sql = build_merge_sql("intermediate", delta, &plan, DeltaOp::Add);
    assert!(sql.contains("LEAST(t.\"__min_price\", d.\"__min_price\")"));
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
        passthrough_columns: vec![],
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: None,
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases: std::collections::HashMap::new(),
        output_column_order: vec![],
    };
    let delta = "SELECT city, MIN(price) FROM src GROUP BY city";
    let sql = build_merge_sql("intermediate", delta, &plan, DeltaOp::Subtract);
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
        passthrough_columns: vec![],
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: None,
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases: std::collections::HashMap::new(),
        output_column_order: vec![],
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

#[test]
fn test_replace_source_with_transition_schema_qualified() {
    let base_query = "SELECT sales_simulation.product_id, SUM(amount) FROM alp.sales_simulation INNER JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id GROUP BY sales_simulation.product_id";
    let result = replace_source_with_transition(
        base_query,
        "alp.sales_simulation",
        "__reflex_new_alp_sales_simulation",
    );
    // FROM clause should be replaced
    assert!(
        result.contains("\"__reflex_new_alp_sales_simulation\""),
        "FROM clause not replaced"
    );
    // Column qualifiers should be replaced
    assert!(
        !result.contains(" sales_simulation.product_id"),
        "Column qualifier not replaced: {}",
        result
    );
    assert!(
        !result.contains(" sales_simulation.dem_plan_id"),
        "JOIN qualifier not replaced: {}",
        result
    );
    // Other tables should NOT be replaced
    assert!(
        result.contains("alp.demand_planning"),
        "Other tables should not be affected"
    );
    assert!(
        result.contains("demand_planning.id"),
        "Other table qualifiers should not be affected"
    );
}

#[test]
fn test_replace_source_with_transition_unqualified() {
    let base_query = "SELECT city, SUM(amount) FROM orders GROUP BY city";
    let result = replace_source_with_transition(base_query, "orders", "__reflex_new_orders");
    assert!(result.contains("\"__reflex_new_orders\""));
    assert!(!result.contains(" orders "));
}

// ========================================================================
// Bug fix tests: quoted identifiers in trigger names
// ========================================================================

#[test]
fn test_trigger_ddl_quoted_table_name() {
    // Tables with reserved-word names like "order" should not break trigger naming
    let ddls = build_trigger_ddls("alp.\"order\"");
    for ddl in &ddls {
        // Trigger function names should NOT contain literal quote characters
        assert!(
            !ddl.contains("__reflex_ins_trigger_on_alp_\"order\""),
            "Trigger function name should not contain quotes: {}",
            &ddl[..ddl.len().min(200)]
        );
        // Should contain the clean name
        assert!(
            ddl.contains("__reflex_") && ddl.contains("_on_alp_order"),
            "Trigger should use stripped name 'alp_order': {}",
            &ddl[..ddl.len().min(200)]
        );
        // The source table reference in SQL strings should still use the quoted form
        assert!(
            ddl.contains("ON alp.\"order\""),
            "Trigger DDL should reference the original table with quotes"
        );
    }
}

#[test]
fn test_trigger_ddl_unquoted_table_name_unchanged() {
    let ddls = build_trigger_ddls("public.sales");
    for ddl in &ddls {
        assert!(
            ddl.contains("_on_public_sales"),
            "Unquoted table names should work normally"
        );
    }
}
