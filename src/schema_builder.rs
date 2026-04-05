use std::collections::HashMap;

use crate::aggregation::{AggregationPlan, EndQueryMapping};
use crate::query_decomposer::{
    intermediate_table_name, normalized_column_name, quote_identifier, split_qualified_name,
};

/// Build the DDL for the intermediate table.
///
/// When `logged` is true, creates a regular (WAL-logged) table for crash safety.
/// When false (default), creates an UNLOGGED table for maximum write performance.
///
/// Returns None if no intermediate table is needed (no aggregation, no group by, no distinct).
pub fn build_intermediate_table_ddl(
    view_name: &str,
    plan: &AggregationPlan,
    column_types: &HashMap<String, String>,
    logged: bool,
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

    // Group by columns as table keys (using normalized lowercase bare names
    // to match PostgreSQL's case folding of unquoted identifiers)
    for col in &plan.group_by_columns {
        let norm = normalized_column_name(col);
        let pg_type = resolve_column_type(&norm, column_types, "TEXT");
        columns.push(format!("    \"{}\" {}", norm, pg_type));
    }

    // For DISTINCT without GROUP BY: the projected columns become the keys
    for col in &plan.distinct_columns {
        let norm = normalized_column_name(col);
        let pg_type = resolve_column_type(&norm, column_types, "TEXT");
        columns.push(format!("    \"{}\" {}", norm, pg_type));
    }

    // Intermediate aggregate columns
    for ic in &plan.intermediate_columns {
        // For MIN/MAX, resolve the actual source column type from catalog
        // instead of using the hardcoded NUMERIC (which breaks for TEXT, DATE, etc.)
        let effective_type = if (ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX")
            && ic.pg_type == "NUMERIC"
        {
            resolve_column_type(&ic.source_arg, column_types, &ic.pg_type)
        } else {
            ic.pg_type.clone()
        };
        let default = match effective_type.as_str() {
            "BOOLEAN" => "FALSE",
            t if t.to_uppercase().starts_with("TEXT")
                || t.to_uppercase().starts_with("VARCHAR")
                || t.to_uppercase().starts_with("CHAR") =>
            {
                "''"
            }
            t if t.to_uppercase().contains("TIMESTAMP") || t.to_uppercase().contains("DATE") => {
                "'epoch'"
            }
            _ => "0",
        };
        columns.push(format!(
            "    \"{}\" {} DEFAULT {}",
            ic.name, effective_type, default
        ));
    }

    // __ivm_count for reference counting
    if plan.needs_ivm_count {
        columns.push("    __ivm_count BIGINT DEFAULT 0".to_string());
    }

    let columns_sql = columns.join(",\n");

    // No inline PRIMARY KEY — we use a hash index for O(1) lookups instead.
    // The B-tree PK is redundant because MERGE handles insert-or-update correctly,
    // the delta query uses GROUP BY (unique output), and advisory locks prevent
    // concurrent MERGEs on the same IMV.

    let create_prefix = if logged {
        "CREATE TABLE"
    } else {
        "CREATE UNLOGGED TABLE"
    };
    Some(format!(
        "{} IF NOT EXISTS {} (\n{}\n)",
        create_prefix, table_name, columns_sql
    ))
}

/// Build the DDL for the target (materialized view result) table.
///
/// When `logged` is true, creates a regular (WAL-logged) table for crash safety.
/// When false (default), creates an UNLOGGED table for maximum write performance.
pub fn build_target_table_ddl(
    view_name: &str,
    plan: &AggregationPlan,
    column_types: &HashMap<String, String>,
    logged: bool,
) -> String {
    let mut columns: Vec<String> = Vec::new();

    // Helper: resolve type for an end_query_mapping
    let mapping_type = |mapping: &EndQueryMapping| -> String {
        if let Some(ref cast) = mapping.cast_type {
            cast.to_string()
        } else {
            match mapping.aggregate_type.as_str() {
                "SUM" | "AVG" | "DERIVED" => "NUMERIC".to_string(),
                "COUNT" => "BIGINT".to_string(),
                "MIN" | "MAX" => {
                    let source_arg = mapping
                        .intermediate_expr
                        .trim_start_matches("__min_")
                        .trim_start_matches("__max_");
                    resolve_column_type(source_arg, column_types, "NUMERIC")
                }
                "BOOL_OR" => "BOOLEAN".to_string(),
                _ => "TEXT".to_string(),
            }
        }
    };

    // Helper: resolve type for a GROUP BY column
    let gb_col_ddl = |col: &str| -> String {
        let output_name = if let Some(alias) = plan.group_by_aliases.get(col) {
            normalized_column_name(alias)
        } else {
            normalized_column_name(col)
        };
        let pg_type = resolve_column_type(&output_name, column_types, "TEXT");
        format!("    \"{}\" {}", output_name, pg_type)
    };

    if !plan.output_column_order.is_empty() {
        // Use output_column_order to match the user's SELECT column order
        for entry in &plan.output_column_order {
            if let Some(gb_expr) = entry.strip_prefix("gb:") {
                columns.push(gb_col_ddl(gb_expr));
            } else if let Some(agg_alias) = entry.strip_prefix("agg:") {
                if let Some(mapping) = plan
                    .end_query_mappings
                    .iter()
                    .find(|m| m.output_alias == agg_alias)
                {
                    columns.push(format!(
                        "    \"{}\" {}",
                        mapping.output_alias,
                        mapping_type(mapping)
                    ));
                }
            }
        }
    } else {
        // Fallback: GROUP BY columns first, then aggregates (legacy order)
        for col in &plan.group_by_columns {
            columns.push(gb_col_ddl(col));
        }
        let has_count_distinct = plan
            .end_query_mappings
            .iter()
            .any(|m| m.intermediate_expr.starts_with("COUNT("));
        if !has_count_distinct {
            for col in &plan.distinct_columns {
                let norm = normalized_column_name(col);
                let pg_type = resolve_column_type(&norm, column_types, "TEXT");
                columns.push(format!("    \"{}\" {}", norm, pg_type));
            }
        }
        for mapping in &plan.end_query_mappings {
            columns.push(format!(
                "    \"{}\" {}",
                mapping.output_alias,
                mapping_type(mapping)
            ));
        }
    }

    let columns_sql = columns.join(",\n");

    let create_prefix = if logged {
        "CREATE TABLE"
    } else {
        "CREATE UNLOGGED TABLE"
    };
    format!(
        "{} IF NOT EXISTS {} (\n{}\n)",
        create_prefix,
        quote_identifier(view_name),
        columns_sql
    )
}

/// Build index DDL statements for the intermediate and target tables.
pub fn build_indexes_ddl(view_name: &str, plan: &AggregationPlan) -> Vec<String> {
    let table_name = intermediate_table_name(view_name);
    let bare_view = split_qualified_name(view_name).1;
    let mut indexes = Vec::new();

    // Index on intermediate table group columns for MERGE lookups.
    // Single-column: hash index for O(1) lookups (~30% faster than B-tree).
    // Multi-column: B-tree (hash doesn't support multi-column in PostgreSQL).
    // No PK constraint — MERGE handles insert-or-update correctly, and advisory
    // locks prevent concurrent modifications.
    {
        let mut idx_cols: Vec<String> = Vec::new();
        if plan.group_by_columns.is_empty()
            && plan.distinct_columns.is_empty()
            && !plan.intermediate_columns.is_empty()
        {
            idx_cols.push("__reflex_group".to_string());
        }
        idx_cols.extend(
            plan.group_by_columns
                .iter()
                .map(|c| format!("\"{}\"", normalized_column_name(c))),
        );
        for col in &plan.distinct_columns {
            idx_cols.push(format!("\"{}\"", normalized_column_name(col)));
        }
        if !idx_cols.is_empty() {
            let using = if idx_cols.len() == 1 {
                "USING hash"
            } else {
                ""
            };
            indexes.push(format!(
                "CREATE INDEX IF NOT EXISTS \"idx__reflex_int_{}\" ON {} {} ({})",
                bare_view,
                table_name,
                using,
                idx_cols.join(", ")
            ));
        }
    }

    // For multiple group-by columns, create individual B-tree indexes on intermediate table
    if plan.group_by_columns.len() > 1 {
        for (i, col) in plan.group_by_columns.iter().enumerate() {
            let norm = normalized_column_name(col);
            indexes.push(format!(
                "CREATE INDEX IF NOT EXISTS \"idx__reflex_{}_{}\" ON {} (\"{}\")",
                bare_view, i, table_name, norm
            ));
        }
    }

    // Composite index on target table for targeted refresh DELETE performance
    if !plan.group_by_columns.is_empty() {
        let target_tbl = quote_identifier(view_name);
        let group_cols: Vec<String> = plan
            .group_by_columns
            .iter()
            .map(|c| {
                let name = if let Some(alias) = plan.group_by_aliases.get(c) {
                    normalized_column_name(alias)
                } else {
                    normalized_column_name(c)
                };
                format!("\"{}\"", name)
            })
            .collect();
        indexes.push(format!(
            "CREATE INDEX IF NOT EXISTS \"idx__reflex_target_{}\" ON {} ({})",
            bare_view,
            target_tbl,
            group_cols.join(", ")
        ));
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
    let safe_source = source_table.replace('.', "_").replace('"', "");
    let ref_new = format!("__reflex_new_{}", safe_source);
    let ref_old = format!("__reflex_old_{}", safe_source);

    // Core loop body shared by INSERT/DELETE/UPDATE triggers.
    // {op} is replaced per-operation. {transition_tbl} is the NEW or OLD table name.
    // The FOR loop iterates over all IMVs that depend on this source.
    // Transition tables are visible in plpgsql EXECUTE context, no copy needed.
    //
    // Early-exit: if the transition table is empty, skip the entire loop (no IMVs to process).
    // This avoids Rust FFI calls and advisory locks when a statement affects 0 relevant rows.
    let body_core = format!(
        "DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN; \
         BEGIN \
           SELECT EXISTS(SELECT 1 FROM \"{{transition_tbl}}\" LIMIT 1) INTO _has_rows; \
           IF NOT _has_rows THEN RETURN NULL; END IF; \
           FOR _rec IN \
             SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate \
             FROM public.__reflex_ivm_reference \
             WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
             ORDER BY graph_depth \
           LOOP \
             IF _rec.where_predicate IS NOT NULL THEN \
               EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '{{transition_tbl}}', _rec.where_predicate) INTO _pred_match; \
               IF NOT _pred_match THEN CONTINUE; END IF; \
             END IF; \
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
    let ins_body = body_core
        .replace("{op}", "INSERT")
        .replace("{transition_tbl}", &ref_new);
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
    let del_body = body_core
        .replace("{op}", "DELETE")
        .replace("{transition_tbl}", &ref_old);
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
    let upd_body = body_core
        .replace("{op}", "UPDATE")
        .replace("{transition_tbl}", &ref_new);
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

/// Build deferred-mode trigger DDL statements for a source table.
///
/// In deferred mode, the statement-level trigger captures delta rows into a staging
/// table and inserts a flag into the deferred-pending table. A constraint trigger
/// (DEFERRABLE INITIALLY DEFERRED) fires at COMMIT to flush all accumulated deltas.
///
/// The immediate triggers still handle IMMEDIATE-mode IMVs on the same source
/// (mixed mode: some IMVs IMMEDIATE, some DEFERRED).
pub fn build_deferred_trigger_ddls(source_table: &str) -> Vec<String> {
    let safe_source = source_table.replace('.', "_").replace('"', "");
    let ref_new = format!("__reflex_new_{}", safe_source);
    let ref_old = format!("__reflex_old_{}", safe_source);
    let delta_tbl = format!("__reflex_delta_{}", safe_source);

    // Mixed-mode body: process IMMEDIATE IMVs inline, stage deltas for DEFERRED IMVs.
    // Early-exit if transition table is empty.
    let body_core = format!(
        "DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_deferred BOOLEAN := FALSE; _has_rows BOOLEAN; _pred_match BOOLEAN; \
         BEGIN \
           SELECT EXISTS(SELECT 1 FROM \"{{transition_tbl}}\" LIMIT 1) INTO _has_rows; \
           IF NOT _has_rows THEN RETURN NULL; END IF; \
           FOR _rec IN \
             SELECT name, base_query, end_query, aggregations::text AS aggregations, \
                    COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, where_predicate \
             FROM public.__reflex_ivm_reference \
             WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
             ORDER BY graph_depth \
           LOOP \
             IF _rec.where_predicate IS NOT NULL THEN \
               EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '{{transition_tbl}}', _rec.where_predicate) INTO _pred_match; \
               IF NOT _pred_match THEN CONTINUE; END IF; \
             END IF; \
             IF _rec.refresh_mode = 'IMMEDIATE' THEN \
               PERFORM pg_advisory_xact_lock(hashtext(_rec.name)); \
               _sql := reflex_build_delta_sql(_rec.name, '{source_table}', '{{op}}', _rec.base_query, _rec.end_query, _rec.aggregations); \
               IF _sql <> '' THEN \
                 FOREACH _stmt IN ARRAY string_to_array(_sql, E'\\n--<<REFLEX_SEP>>--\\n') LOOP \
                   IF _stmt <> '' THEN EXECUTE _stmt; END IF; \
                 END LOOP; \
               END IF; \
             ELSE \
               _has_deferred := TRUE; \
             END IF; \
           END LOOP; \
           IF _has_deferred THEN \
             INSERT INTO {delta_tbl} SELECT '{{op_code}}', * FROM \"{{ref_tbl}}\"; \
             INSERT INTO public.__reflex_deferred_pending (source_table, operation) \
               VALUES ('{source_table}', '{{op}}'); \
           END IF; \
           RETURN NULL; \
         END;"
    );

    // INSERT
    let ins_fn = format!("__reflex_ins_trigger_on_{}", safe_source);
    let ins_trig = format!("__reflex_trigger_ins_on_{}", safe_source);
    let ins_body = body_core
        .replace("{op}", "INSERT")
        .replace("{op_code}", "I")
        .replace("{ref_tbl}", &ref_new)
        .replace("{transition_tbl}", &ref_new);
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
    let del_body = body_core
        .replace("{op}", "DELETE")
        .replace("{op_code}", "D")
        .replace("{ref_tbl}", &ref_old)
        .replace("{transition_tbl}", &ref_old);
    let del_ddl = format!(
        "CREATE OR REPLACE FUNCTION {del_fn}() RETURNS TRIGGER AS $fn$ {del_body} $fn$ LANGUAGE plpgsql;\n\
         CREATE OR REPLACE TRIGGER \"{del_trig}\" \
         AFTER DELETE ON {source_table} \
         REFERENCING OLD TABLE AS \"{ref_old}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION {del_fn}()"
    );

    // UPDATE — capture both old and new rows
    let upd_fn = format!("__reflex_upd_trigger_on_{}", safe_source);
    let upd_trig = format!("__reflex_trigger_upd_on_{}", safe_source);
    let upd_body = format!(
        "DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_deferred BOOLEAN := FALSE; _has_rows BOOLEAN; \
         BEGIN \
           SELECT EXISTS(SELECT 1 FROM \"{ref_new}\" LIMIT 1) INTO _has_rows; \
           IF NOT _has_rows THEN RETURN NULL; END IF; \
           FOR _rec IN \
             SELECT name, base_query, end_query, aggregations::text AS aggregations, \
                    COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode \
             FROM public.__reflex_ivm_reference \
             WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
             ORDER BY graph_depth \
           LOOP \
             IF _rec.refresh_mode = 'IMMEDIATE' THEN \
               PERFORM pg_advisory_xact_lock(hashtext(_rec.name)); \
               _sql := reflex_build_delta_sql(_rec.name, '{source_table}', 'UPDATE', _rec.base_query, _rec.end_query, _rec.aggregations); \
               IF _sql <> '' THEN \
                 FOREACH _stmt IN ARRAY string_to_array(_sql, E'\\n--<<REFLEX_SEP>>--\\n') LOOP \
                   IF _stmt <> '' THEN EXECUTE _stmt; END IF; \
                 END LOOP; \
               END IF; \
             ELSE \
               _has_deferred := TRUE; \
             END IF; \
           END LOOP; \
           IF _has_deferred THEN \
             INSERT INTO {delta_tbl} SELECT 'U_OLD', * FROM \"{ref_old}\"; \
             INSERT INTO {delta_tbl} SELECT 'U_NEW', * FROM \"{ref_new}\"; \
             INSERT INTO public.__reflex_deferred_pending (source_table, operation) \
               VALUES ('{source_table}', 'UPDATE'); \
           END IF; \
           RETURN NULL; \
         END;"
    );
    let upd_ddl = format!(
        "CREATE OR REPLACE FUNCTION {upd_fn}() RETURNS TRIGGER AS $fn$ {upd_body} $fn$ LANGUAGE plpgsql;\n\
         CREATE OR REPLACE TRIGGER \"{upd_trig}\" \
         AFTER UPDATE ON {source_table} \
         REFERENCING NEW TABLE AS \"{ref_new}\" OLD TABLE AS \"{ref_old}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION {upd_fn}()"
    );

    // TRUNCATE — same as immediate (no deferred staging for truncate)
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
           TRUNCATE {delta_tbl}; \
           DELETE FROM public.__reflex_deferred_pending WHERE source_table = '{source_table}'; \
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

/// Build DDL for the deferred-pending table and its constraint trigger.
///
/// The constraint trigger fires at COMMIT time and processes all accumulated
/// staging deltas for each source table.
pub fn build_deferred_flush_ddl() -> Vec<String> {
    vec![
        // Pending queue table
        "CREATE TABLE IF NOT EXISTS public.__reflex_deferred_pending (\
            id BIGSERIAL, \
            source_table TEXT NOT NULL, \
            operation TEXT NOT NULL, \
            batch_ts TIMESTAMPTZ DEFAULT now()\
         )"
        .to_string(),
        // Constraint trigger function: flushes all pending deltas at COMMIT
        "CREATE OR REPLACE FUNCTION __reflex_deferred_flush_fn() RETURNS TRIGGER AS $fn$ \
         DECLARE _src RECORD; \
         BEGIN \
           FOR _src IN \
             SELECT DISTINCT source_table FROM public.__reflex_deferred_pending \
           LOOP \
             PERFORM reflex_flush_deferred(_src.source_table); \
           END LOOP; \
           RETURN NULL; \
         END; \
         $fn$ LANGUAGE plpgsql"
            .to_string(),
        // Constraint trigger — fires at COMMIT for any INSERT into the pending table
        "CREATE CONSTRAINT TRIGGER __reflex_deferred_flush_trigger \
         AFTER INSERT ON public.__reflex_deferred_pending \
         DEFERRABLE INITIALLY DEFERRED \
         FOR EACH ROW EXECUTE FUNCTION __reflex_deferred_flush_fn()"
            .to_string(),
    ]
}

/// Build DDL for a staging (delta) table that captures transition rows in deferred mode.
///
/// The staging table mirrors the source table's columns plus a `__reflex_op` column
/// to identify the operation type (I=insert, D=delete, U_OLD=update old, U_NEW=update new).
pub fn build_staging_table_ddl(source_table: &str) -> String {
    let safe_source = source_table.replace('.', "_");
    let delta_tbl = format!("__reflex_delta_{}", safe_source);
    format!(
        "CREATE UNLOGGED TABLE IF NOT EXISTS {} (\
            __reflex_op TEXT NOT NULL, \
            LIKE {} INCLUDING DEFAULTS\
         )",
        delta_tbl, source_table
    )
}

/// Resolve a column's PostgreSQL type from the catalog lookup map.
///
/// The map keys can be either "table.column" or just "column".
/// Falls back to the provided default type.
pub(crate) fn resolve_column_type(
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
#[path = "tests/unit_schema_builder.rs"]
mod tests;
