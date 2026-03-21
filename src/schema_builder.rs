use std::collections::HashMap;

use crate::aggregation::AggregationPlan;
use crate::query_decomposer::{bare_column_name, intermediate_table_name};

/// Build the DDL for the intermediate (UNLOGGED) table.
///
/// Returns None if no intermediate table is needed (no aggregation, no group by, no distinct).
pub fn build_intermediate_table_ddl(
    view_name: &str,
    plan: &AggregationPlan,
    column_types: &HashMap<String, String>,
) -> Option<String> {
    // Need at least one of: group by, aggregation, or distinct
    if plan.group_by_columns.is_empty()
        && plan.intermediate_columns.is_empty()
        && !plan.has_distinct
        && plan.distinct_columns.is_empty()
    {
        return None;
    }

    let table_name = intermediate_table_name(view_name);
    let mut columns: Vec<String> = Vec::new();

    // For aggregates without GROUP BY: add a sentinel column so we have a PK
    let needs_sentinel = plan.group_by_columns.is_empty()
        && plan.distinct_columns.is_empty()
        && !plan.intermediate_columns.is_empty();
    if needs_sentinel {
        columns.push("    __reflex_group INTEGER DEFAULT 0".to_string());
    }

    // Group by columns as table keys (using bare column names, no table qualifiers)
    for col in &plan.group_by_columns {
        let bare = bare_column_name(col);
        let pg_type = resolve_column_type(bare, column_types, "TEXT");
        columns.push(format!("    \"{}\" {}", bare, pg_type));
    }

    // For DISTINCT without GROUP BY: the projected columns become the keys
    for col in &plan.distinct_columns {
        let bare = bare_column_name(col);
        let pg_type = resolve_column_type(bare, column_types, "TEXT");
        columns.push(format!("    \"{}\" {}", bare, pg_type));
    }

    // Intermediate aggregate columns
    for ic in &plan.intermediate_columns {
        columns.push(format!("    \"{}\" {} DEFAULT 0", ic.name, ic.pg_type));
    }

    // __ivm_count for reference counting
    if plan.needs_ivm_count {
        columns.push("    __ivm_count BIGINT DEFAULT 0".to_string());
    }

    let columns_sql = columns.join(",\n");

    // Primary key on group by columns and/or distinct columns
    let mut pk_cols: Vec<String> = Vec::new();
    if needs_sentinel {
        pk_cols.push("__reflex_group".to_string());
    }
    pk_cols.extend(
        plan.group_by_columns
            .iter()
            .map(|c| format!("\"{}\"", bare_column_name(c))),
    );
    for col in &plan.distinct_columns {
        pk_cols.push(format!("\"{}\"", bare_column_name(col)));
    }
    let pk = if !pk_cols.is_empty() {
        format!(",\n    PRIMARY KEY ({})", pk_cols.join(", "))
    } else {
        String::new()
    };

    Some(format!(
        "CREATE UNLOGGED TABLE IF NOT EXISTS {} (\n{}{}\n)",
        table_name, columns_sql, pk
    ))
}

/// Build the DDL for the target (materialized view result) table.
pub fn build_target_table_ddl(
    view_name: &str,
    plan: &AggregationPlan,
    column_types: &HashMap<String, String>,
) -> String {
    let mut columns: Vec<String> = Vec::new();

    // Group by columns (bare names)
    for col in &plan.group_by_columns {
        let bare = bare_column_name(col);
        let pg_type = resolve_column_type(bare, column_types, "TEXT");
        columns.push(format!("    \"{}\" {}", bare, pg_type));
    }

    // DISTINCT columns (bare names)
    for col in &plan.distinct_columns {
        let bare = bare_column_name(col);
        let pg_type = resolve_column_type(bare, column_types, "TEXT");
        columns.push(format!("    \"{}\" {}", bare, pg_type));
    }

    // Output columns from end query mappings
    for mapping in &plan.end_query_mappings {
        let pg_type = match mapping.aggregate_type.as_str() {
            "SUM" | "AVG" => "NUMERIC",
            "COUNT" => "BIGINT",
            "MIN" | "MAX" => {
                // Try to resolve from column_types, fall back to NUMERIC
                "NUMERIC"
            }
            _ => "TEXT",
        };
        columns.push(format!("    \"{}\" {}", mapping.output_alias, pg_type));
    }

    let columns_sql = columns.join(",\n");

    format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (\n{}\n)",
        view_name, columns_sql
    )
}

/// Build index DDL statements for the intermediate table.
pub fn build_indexes_ddl(view_name: &str, plan: &AggregationPlan) -> Vec<String> {
    let table_name = intermediate_table_name(view_name);
    let mut indexes = Vec::new();

    // For multiple group-by columns, create individual indexes
    // (the composite PK already covers combined lookups)
    if plan.group_by_columns.len() > 1 {
        for (i, col) in plan.group_by_columns.iter().enumerate() {
            let bare = bare_column_name(col);
            indexes.push(format!(
                "CREATE INDEX IF NOT EXISTS \"idx__reflex_{}_{}\" ON {} (\"{}\")",
                view_name, i, table_name, bare
            ));
        }
    }

    indexes
}

/// Build consolidated trigger DDL statements for a source table.
///
/// Returns 4 DDL blocks (INSERT, DELETE, UPDATE, TRUNCATE), each creating a plpgsql
/// wrapper function and a statement-level trigger. One set of triggers per source table
/// handles ALL dependent IMVs via a FOR loop over the reference table.
///
/// Transition tables are referenced directly in EXECUTE context (no temp table copy).
pub fn build_trigger_ddls(source_table: &str) -> Vec<String> {
    let safe_source = source_table.replace('.', "_");
    let ref_new = format!("__reflex_new_{}", safe_source);
    let ref_old = format!("__reflex_old_{}", safe_source);

    // Core loop body shared by INSERT/DELETE/UPDATE triggers.
    // {op} is replaced per-operation.
    // The FOR loop iterates over all IMVs that depend on this source.
    // Transition tables are visible in plpgsql EXECUTE context, no copy needed.
    let body_core = format!(
        "DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; \
         BEGIN \
           FOR _rec IN \
             SELECT name, base_query, end_query, aggregations::text AS aggregations \
             FROM public.__reflex_ivm_reference \
             WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
             ORDER BY graph_depth \
           LOOP \
             PERFORM pg_advisory_xact_lock(hashtext(_rec.name)); \
             _sql := reflex_build_delta_sql(_rec.name, '{source_table}', '{{op}}', _rec.base_query, _rec.end_query, _rec.aggregations); \
             IF _sql <> '' THEN \
               FOREACH _stmt IN ARRAY string_to_array(_sql, E'\\n--<<REFLEX_SEP>>--\\n') LOOP \
                 IF _stmt <> '' THEN EXECUTE _stmt; END IF; \
               END LOOP; \
             END IF; \
           END LOOP; \
           RETURN NULL; \
         END;"
    );

    // INSERT
    let ins_fn = format!("__reflex_ins_trigger_on_{}", safe_source);
    let ins_trig = format!("__reflex_trigger_ins_on_{}", safe_source);
    let ins_body = body_core.replace("{op}", "INSERT");
    let ins_ddl = format!(
        "CREATE OR REPLACE FUNCTION {ins_fn}() RETURNS TRIGGER AS $fn$ {ins_body} $fn$ LANGUAGE plpgsql;\n\
         CREATE OR REPLACE TRIGGER \"{ins_trig}\" \
         AFTER INSERT ON {source_table} \
         REFERENCING NEW TABLE AS \"{ref_new}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION {ins_fn}()"
    );

    // DELETE
    let del_fn = format!("__reflex_del_trigger_on_{}", safe_source);
    let del_trig = format!("__reflex_trigger_del_on_{}", safe_source);
    let del_body = body_core.replace("{op}", "DELETE");
    let del_ddl = format!(
        "CREATE OR REPLACE FUNCTION {del_fn}() RETURNS TRIGGER AS $fn$ {del_body} $fn$ LANGUAGE plpgsql;\n\
         CREATE OR REPLACE TRIGGER \"{del_trig}\" \
         AFTER DELETE ON {source_table} \
         REFERENCING OLD TABLE AS \"{ref_old}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION {del_fn}()"
    );

    // UPDATE
    let upd_fn = format!("__reflex_upd_trigger_on_{}", safe_source);
    let upd_trig = format!("__reflex_trigger_upd_on_{}", safe_source);
    let upd_body = body_core.replace("{op}", "UPDATE");
    let upd_ddl = format!(
        "CREATE OR REPLACE FUNCTION {upd_fn}() RETURNS TRIGGER AS $fn$ {upd_body} $fn$ LANGUAGE plpgsql;\n\
         CREATE OR REPLACE TRIGGER \"{upd_trig}\" \
         AFTER UPDATE ON {source_table} \
         REFERENCING NEW TABLE AS \"{ref_new}\" OLD TABLE AS \"{ref_old}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION {upd_fn}()"
    );

    // TRUNCATE — no REFERENCING clauses; loops over all dependent IMVs
    let trunc_fn = format!("__reflex_trunc_trigger_on_{}", safe_source);
    let trunc_trig = format!("__reflex_trigger_trunc_on_{}", safe_source);
    let trunc_body = format!(
        "DECLARE _rec RECORD; _stmts TEXT; _stmt TEXT; \
         BEGIN \
           FOR _rec IN \
             SELECT name \
             FROM public.__reflex_ivm_reference \
             WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
             ORDER BY graph_depth \
           LOOP \
             PERFORM pg_advisory_xact_lock(hashtext(_rec.name)); \
             _stmts := reflex_build_truncate_sql(_rec.name); \
             IF _stmts <> '' THEN \
               FOREACH _stmt IN ARRAY string_to_array(_stmts, E'\\n--<<REFLEX_SEP>>--\\n') LOOP \
                 IF _stmt <> '' THEN EXECUTE _stmt; END IF; \
               END LOOP; \
             END IF; \
           END LOOP; \
           RETURN NULL; \
         END;"
    );
    let trunc_ddl = format!(
        "CREATE OR REPLACE FUNCTION {trunc_fn}() RETURNS TRIGGER AS $fn$ {trunc_body} $fn$ LANGUAGE plpgsql;\n\
         CREATE OR REPLACE TRIGGER \"{trunc_trig}\" \
         AFTER TRUNCATE ON {source_table} \
         FOR EACH STATEMENT EXECUTE FUNCTION {trunc_fn}()"
    );

    vec![ins_ddl, del_ddl, upd_ddl, trunc_ddl]
}

/// Resolve a column's PostgreSQL type from the catalog lookup map.
///
/// The map keys can be either "table.column" or just "column".
/// Falls back to the provided default type.
fn resolve_column_type(
    col_name: &str,
    column_types: &HashMap<String, String>,
    default_type: &str,
) -> String {
    // Try exact match first (e.g., "emp.city")
    if let Some(t) = column_types.get(col_name) {
        return t.clone();
    }
    // Try just the column name part (strip table prefix if present)
    let bare = col_name.rsplit('.').next().unwrap_or(col_name);
    if let Some(t) = column_types.get(bare) {
        return t.clone();
    }
    // Search for any table.column that ends with this column name
    for (key, val) in column_types {
        if key.ends_with(&format!(".{}", bare)) {
            return val.clone();
        }
    }
    default_type.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::{AggregationPlan, EndQueryMapping, IntermediateColumn};

    fn sample_plan() -> AggregationPlan {
        AggregationPlan {
            group_by_columns: vec!["city".to_string()],
            intermediate_columns: vec![
                IntermediateColumn {
                    name: "__sum_amount".to_string(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "SUM".to_string(),
                    source_arg: "amount".to_string(),
                },
                IntermediateColumn {
                    name: "__count_star".to_string(),
                    pg_type: "BIGINT".to_string(),
                    source_aggregate: "COUNT".to_string(),
                    source_arg: "*".to_string(),
                },
            ],
            end_query_mappings: vec![
                EndQueryMapping {
                    intermediate_expr: "__sum_amount".to_string(),
                    output_alias: "total".to_string(),
                    aggregate_type: "SUM".to_string(),
                },
                EndQueryMapping {
                    intermediate_expr: "__count_star".to_string(),
                    output_alias: "cnt".to_string(),
                    aggregate_type: "COUNT".to_string(),
                },
            ],
            has_distinct: false,
            needs_ivm_count: true,
            distinct_columns: vec![],
            is_passthrough: false,
        }
    }

    fn sample_types() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("city".to_string(), "TEXT".to_string());
        m.insert("amount".to_string(), "NUMERIC".to_string());
        m
    }

    #[test]
    fn test_intermediate_table_ddl() {
        let plan = sample_plan();
        let types = sample_types();
        let ddl = build_intermediate_table_ddl("test_view", &plan, &types).unwrap();
        assert!(ddl.contains("CREATE UNLOGGED TABLE"));
        assert!(ddl.contains("__reflex_intermediate_test_view"));
        assert!(ddl.contains("\"city\" TEXT"));
        assert!(ddl.contains("\"__sum_amount\" NUMERIC DEFAULT 0"));
        assert!(ddl.contains("\"__count_star\" BIGINT DEFAULT 0"));
        assert!(ddl.contains("__ivm_count BIGINT DEFAULT 0"));
        assert!(ddl.contains("PRIMARY KEY (\"city\")"));
    }

    #[test]
    fn test_target_table_ddl() {
        let plan = sample_plan();
        let types = sample_types();
        let ddl = build_target_table_ddl("test_view", &plan, &types);
        assert!(ddl.contains("CREATE TABLE"));
        assert!(ddl.contains("\"test_view\""));
        assert!(ddl.contains("\"city\" TEXT"));
        assert!(ddl.contains("\"total\" NUMERIC"));
        assert!(ddl.contains("\"cnt\" BIGINT"));
    }

    #[test]
    fn test_trigger_ddls_format() {
        let ddls = build_trigger_ddls("orders");
        assert_eq!(ddls.len(), 4);
        // INSERT trigger: references transition table directly, loops over IMVs
        assert!(ddls[0].contains("AFTER INSERT ON orders"));
        assert!(ddls[0].contains("REFERENCING NEW TABLE AS"));
        assert!(ddls[0].contains("reflex_build_delta_sql"));
        assert!(ddls[0].contains("'INSERT'"));
        assert!(ddls[0].contains("FOR _rec IN"));
        assert!(ddls[0].contains("__reflex_ins_trigger_on_orders"));
        // No temp table copy (transition tables used directly)
        assert!(!ddls[0].contains("CREATE TEMP TABLE"));
        // DELETE trigger
        assert!(ddls[1].contains("AFTER DELETE ON orders"));
        assert!(ddls[1].contains("'DELETE'"));
        // UPDATE trigger
        assert!(ddls[2].contains("AFTER UPDATE ON orders"));
        assert!(ddls[2].contains("'UPDATE'"));
        // TRUNCATE trigger
        assert!(ddls[3].contains("AFTER TRUNCATE ON orders"));
        assert!(ddls[3].contains("reflex_build_truncate_sql"));
        assert!(ddls[3].contains("FOR _rec IN"));
    }

    #[test]
    fn test_indexes_ddl_multiple_group_by() {
        let mut plan = sample_plan();
        plan.group_by_columns = vec!["city".to_string(), "year".to_string()];
        let indexes = build_indexes_ddl("test_view", &plan);
        assert_eq!(indexes.len(), 2);
        assert!(indexes[0].contains("\"city\""));
        assert!(indexes[1].contains("\"year\""));
    }

    #[test]
    fn test_indexes_ddl_single_group_by() {
        let plan = sample_plan();
        let indexes = build_indexes_ddl("test_view", &plan);
        // Single group by column already has PK, no extra indexes needed
        assert!(indexes.is_empty());
    }

    #[test]
    fn test_no_intermediate_for_passthrough() {
        let plan = AggregationPlan {
            group_by_columns: vec![],
            intermediate_columns: vec![],
            end_query_mappings: vec![],
            has_distinct: false,
            needs_ivm_count: true,
            distinct_columns: vec![],
            is_passthrough: false,
        };
        let types = HashMap::new();
        assert!(build_intermediate_table_ddl("test_view", &plan, &types).is_none());
    }

    #[test]
    fn test_resolve_column_type() {
        let mut types = HashMap::new();
        types.insert("emp.salary".to_string(), "integer".to_string());
        types.insert("name".to_string(), "varchar".to_string());

        assert_eq!(resolve_column_type("emp.salary", &types, "TEXT"), "integer");
        assert_eq!(resolve_column_type("salary", &types, "TEXT"), "integer");
        assert_eq!(resolve_column_type("name", &types, "TEXT"), "varchar");
        assert_eq!(resolve_column_type("unknown", &types, "TEXT"), "TEXT");
    }
}
