use std::collections::HashMap;

use crate::aggregation::{AggregationPlan, EndQueryMapping};
use crate::query_decomposer::{
    delta_scratch_table_name, intermediate_table_name, normalized_column_name,
    passthrough_scratch_new_table_name, passthrough_scratch_old_table_name, quote_identifier,
    safe_identifier, split_qualified_name, staging_delta_table_name, transition_new_table_name,
    transition_old_table_name,
};

/// Returns the SQL column definition list shared by the intermediate and delta scratch tables.
/// Returns None if no intermediate table is needed (no aggregation, no group by, no distinct).
fn intermediate_column_spec(
    plan: &AggregationPlan,
    column_types: &HashMap<String, String>,
) -> Option<Vec<String>> {
    if plan.group_by_columns.is_empty()
        && plan.intermediate_columns.is_empty()
        && !plan.has_distinct
        && plan.distinct_columns.is_empty()
    {
        return None;
    }

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

        // For top-K-enabled MIN/MAX columns, emit the sibling array column.
        // The element type matches the scalar column; the default empty array
        // means "heap empty, must scan to populate" — initial population
        // happens at IMV creation via the base_query.
        if ic.has_topk() {
            columns.push(format!(
                "    \"{}\" {}[] DEFAULT '{{}}'::{}[]",
                ic.topk_column_name(),
                effective_type,
                effective_type
            ));
        }
    }

    // __ivm_count for reference counting
    if plan.needs_ivm_count {
        columns.push("    __ivm_count BIGINT DEFAULT 0".to_string());
    }

    Some(columns)
}

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
    let columns = intermediate_column_spec(plan, column_types)?;
    let table_name = intermediate_table_name(view_name);
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
    // fillfactor=70 leaves 30% slack on every heap page so MERGE WHEN
    // MATCHED UPDATE (which only rewrites aggregate columns — none are
    // indexed) can fit the new tuple version on the same page → HOT
    // update → zero index writes. Bench (perftest, 47K rows): 691ms
    // (fillfactor=100) → 75ms (fillfactor=70). Disk cost: ~14% larger
    // baseline, but eliminates ~20% bloat per UPDATE cycle.
    Some(format!(
        "{} IF NOT EXISTS {} (\n{}\n) WITH (fillfactor=70)",
        create_prefix, table_name, columns_sql
    ))
}

/// Build the DDL for the per-IMV UNLOGGED delta scratch table.
///
/// This scratch table has the same column shape as the intermediate table but is
/// always UNLOGGED, has no indexes, and is TRUNCATE'd before each MERGE.  It
/// exists so that MERGE reads from a plain table rather than an inline transition-
/// table subquery, avoiding the PG cassert that fires when a MERGE USING clause
/// references a transition table inside a dynamically-executed statement.
///
/// Returns None when no intermediate table is needed for this IMV.
pub fn build_delta_scratch_table_ddl(
    view_name: &str,
    plan: &AggregationPlan,
    column_types: &HashMap<String, String>,
) -> Option<String> {
    let columns = intermediate_column_spec(plan, column_types)?;
    let table_name = delta_scratch_table_name(view_name);
    let columns_sql = columns.join(",\n");
    Some(format!(
        "CREATE UNLOGGED TABLE IF NOT EXISTS {} (\n{}\n)",
        table_name, columns_sql
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
    // fillfactor=70: same rationale as build_intermediate_table_ddl.
    // Target MERGE WHEN MATCHED UPDATE rewrites only the end-query
    // aggregate columns; group_by columns (which are the only
    // indexed columns via idx__reflex_target_*) stay untouched →
    // HOT update eligible.
    format!(
        "{} IF NOT EXISTS {} (\n{}\n) WITH (fillfactor=70)",
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
            let idx_name = safe_identifier(&format!("idx__reflex_int_{}", bare_view));
            // For multi-column groups, emit a UNIQUE btree with NULLS NOT
            // DISTINCT (PG 15+). The UNIQUE constraint enforces the
            // one-row-per-group invariant the MERGE codegen assumes, and
            // pairs with `build_merge_using`'s `=`-for-NOT-NULL ON clauses
            // so the planner can do an index range scan over the NOT NULL
            // prefix instead of seq-scanning the intermediate. For
            // single-column groups, hash is the optimal lookup but doesn't
            // support uniqueness — keep it non-unique.
            if idx_cols.len() == 1 {
                indexes.push(format!(
                    "CREATE INDEX IF NOT EXISTS \"{}\" ON {} USING hash ({})",
                    idx_name,
                    table_name,
                    idx_cols.join(", ")
                ));
            } else {
                indexes.push(format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS \"{}\" ON {} ({}) NULLS NOT DISTINCT",
                    idx_name,
                    table_name,
                    idx_cols.join(", ")
                ));
            }
        }
    }

    // 1.4.4 (perftest A→B bench, 47K rows): the per-column intermediate
    // indexes that pre-1.4.4 emitted (one per group-by column) added
    // ~480ms of index maintenance per UPDATE cycle and were never used
    // by any pg_reflex query path — every code path (MERGE ON,
    // EXISTS-by-affected, JOIN to scratch) uses the full composite key,
    // so the composite UNIQUE NULLS NOT DISTINCT btree above is the
    // only index actually probed. Dropped here; migration in
    // sql/pg_reflex--1.4.3--1.4.4.sql drops them on existing IMVs.

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
        let idx_name = safe_identifier(&format!("idx__reflex_target_{}", bare_view));
        indexes.push(format!(
            "CREATE INDEX IF NOT EXISTS \"{}\" ON {} ({})",
            idx_name,
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
    let ref_new = transition_new_table_name(source_table);
    let ref_old = transition_old_table_name(source_table);

    // Core loop body shared by INSERT/DELETE/UPDATE triggers.
    // {op} is replaced per-operation. {transition_tbl} is the NEW or OLD table name.
    // The FOR loop iterates over all IMVs that depend on this source.
    // Transition tables are visible in plpgsql EXECUTE context, no copy needed.
    //
    // Early-exit: if the transition table is empty, skip the entire loop (no IMVs to process).
    // This avoids Rust FFI calls and advisory locks when a statement affects 0 relevant rows.
    // 1.4.5: filter out IMVs that explicitly ignored this source via the
    // `ignore_sources` parameter of `create_reflex_ivm`. The check covers
    // both schema-qualified ('alp.product') and bare ('product') forms so
    // users can write whichever matches their IMV's depends_on entry.
    let bare_source = source_table
        .split('.')
        .next_back()
        .unwrap_or(source_table)
        .to_string();
    // 1.4.5 — Filter-aware spurious-skip (UPDATE only).
    //
    // The IMV's aggregations JSON carries two per-source maps:
    //   * imv_relevant_columns[source] — columns of `source` that the IMV
    //     projects / joins on / groups by (i.e. NOT filter-only). The
    //     analyzer attributes only references that resolve unambiguously
    //     (qualified refs and bare refs in single-source queries), so every
    //     column listed is guaranteed to exist on the source.
    //   * imv_relevant_where[source]   — the source-restricted WHERE
    //     conjuncts, alias-stripped to evaluate against the flat
    //     transition table.
    //
    // When both pre- and post-image multisets project identically on the
    // relevant columns (within the relevant WHERE), the IMV's output
    // cannot change for this UPDATE — skip its body entirely.
    //
    // An absent / empty imv_relevant_columns entry disables the check
    // (CTE-using or SELECT * IMVs land here — they keep the previous path).
    let filter_skip_block_for_update = format!(
        "SELECT string_agg(format('%I', col), ', ' ORDER BY col) \
              INTO _skip_cols \
              FROM jsonb_array_elements_text( \
                   COALESCE(_rec.aggregations::jsonb->'imv_relevant_columns'->'{source_table}', '[]'::jsonb) \
              ) AS t(col); \
         IF _skip_cols IS NOT NULL AND _skip_cols <> '' THEN \
           _skip_pred := _rec.aggregations::jsonb->'imv_relevant_where'->>'{source_table}'; \
           IF _skip_pred IS NULL OR _skip_pred = '' THEN \
             _skip_pred_clause := ''; \
           ELSE \
             _skip_pred_clause := ' WHERE ' || _skip_pred; \
           END IF; \
           _skip_sql := format( \
             'WITH _o AS (SELECT %s FROM %I%s), _n AS (SELECT %s FROM %I%s) ' \
             || 'SELECT NOT EXISTS(TABLE _o EXCEPT ALL TABLE _n) AND NOT EXISTS(TABLE _n EXCEPT ALL TABLE _o)', \
             _skip_cols, '{ref_old}', _skip_pred_clause, \
             _skip_cols, '{ref_new}', _skip_pred_clause \
           ); \
           EXECUTE _skip_sql INTO _filter_skip; \
           IF _filter_skip THEN CONTINUE; END IF; \
         END IF;"
    );

    // Item α (2026-05-15) — Directional UPDATE dispatch.
    //
    // After filter-aware spurious-skip has decided the multisets differ, probe
    // each transition table for post-filter rows. The three outcomes route
    // the call to `reflex_build_delta_sql` with a *promoted* op:
    //   * OLD empty, NEW has rows → 'INSERT' shape (single-direction add)
    //   * OLD has rows, NEW empty → 'DELETE' shape (single-direction sub)
    //   * both have rows          → 'UPDATE' (today's UNION ALL path)
    //
    // The INSERT/DELETE codegen paths read only one transition table by name;
    // they remain visible from the UPDATE trigger's REFERENCING clauses.
    //
    // The probe re-uses `_skip_cols` / `_skip_pred_clause` populated by the
    // filter_skip_block above — same gate (`imv_relevant_columns[source]`
    // non-empty); same WHERE predicate.
    //
    // Promoting UPDATE → INSERT also drops the wasted dead-cleanup DELETE
    // (gated on op ∈ {DELETE, UPDATE} in trigger.rs) and the target DELETE on
    // OUT→IN flips finds 0 pre-existing rows for affected keys.
    //
    // 1.4.6 attempt + revert (2026-05-15): a draft promoted OUT→IN to
    // 'INSERT_PROMOTED' so trigger.rs could replace MERGE with plain INSERT.
    // The pg_test_directional_with_filter_flip_and_data_change_same_row
    // integration test caught the safety hole: for single-source IMVs (and
    // for any IMV where OTHER source rows share the affected group key) the
    // intermediate is NOT empty for those group keys, and bulk INSERT
    // produces duplicates. Safe re-enablement needs JOIN-mapping metadata so
    // we can verify the trigger source's identity uniquely determines the
    // group keys it touches. Deferred — see journal/2026-05-15_bulk_insert_revert.md.
    let directional_probe_for_update = format!(
        "_directional_op := 'UPDATE'; \
         IF _skip_cols IS NOT NULL AND _skip_cols <> '' THEN \
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I%s LIMIT 1)', \
                          '{ref_old}', _skip_pred_clause) INTO _old_has; \
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I%s LIMIT 1)', \
                          '{ref_new}', _skip_pred_clause) INTO _new_has; \
           IF (NOT _old_has) AND _new_has THEN \
             _directional_op := 'INSERT'; \
           ELSIF _old_has AND (NOT _new_has) THEN \
             _directional_op := 'DELETE'; \
           END IF; \
         END IF;"
    );

    // The where_predicate early-skip needs op-aware semantics:
    //   * INSERT trigger only sees `ref_new` — skip if no NEW row passes.
    //   * DELETE trigger only sees `ref_old` — skip if no OLD row passes.
    //   * UPDATE trigger sees BOTH — must skip ONLY if neither side has a
    //     passing row. A NEW-only check would incorrectly skip rows leaving
    //     the IMV's filter (status flips out of whitelist → IMV must delete
    //     that row's contribution). {pred_check_block} substitutes the
    //     correct op-specific SQL.
    let body_core = format!(
        "DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN; \
                 _skip_cols TEXT; _skip_pred TEXT; _skip_pred_clause TEXT; _skip_sql TEXT; _filter_skip BOOLEAN; \
                 _old_has BOOLEAN; _new_has BOOLEAN; _directional_op TEXT; \
         BEGIN \
           SELECT EXISTS(SELECT 1 FROM \"{{transition_tbl}}\" LIMIT 1) INTO _has_rows; \
           IF NOT _has_rows THEN RETURN NULL; END IF; \
           FOR _rec IN \
             SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate, ignored_sources \
             FROM public.__reflex_ivm_reference \
             WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
             ORDER BY graph_depth, name \
           LOOP \
             IF _rec.ignored_sources IS NOT NULL AND ('{source_table}' = ANY(_rec.ignored_sources) OR '{bare_source}' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF; \
             {{pred_check_block}} \
             {{filter_skip_block}} \
             {{directional_probe_block}} \
             PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name))); \
             _sql := public.reflex_build_delta_sql(_rec.name, '{source_table}', {{op_value}}, _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query); \
             IF _sql <> '' THEN \
               FOREACH _stmt IN ARRAY string_to_array(_sql, E'\\n--<<REFLEX_SEP>>--\\n') LOOP \
                 IF _stmt <> '' THEN EXECUTE _stmt; END IF; \
               END LOOP; \
             END IF; \
           END LOOP; \
           RETURN NULL; \
         END;"
    );

    // Op-specific where-predicate early-skip blocks.
    let pred_check_one = |tbl: &str| -> String {
        format!(
            "IF _rec.where_predicate IS NOT NULL THEN \
               EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '{tbl}', _rec.where_predicate) INTO _pred_match; \
               IF NOT _pred_match THEN CONTINUE; END IF; \
             END IF;"
        )
    };
    let pred_check_two = format!(
        "IF _rec.where_predicate IS NOT NULL THEN \
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1) OR EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', \
                          '{ref_new}', _rec.where_predicate, '{ref_old}', _rec.where_predicate) INTO _pred_match; \
           IF NOT _pred_match THEN CONTINUE; END IF; \
         END IF;"
    );
    let pred_check_ins = pred_check_one(&ref_new);
    let pred_check_del = pred_check_one(&ref_old);
    let pred_check_upd = pred_check_two;

    // INSERT
    let ins_fn = safe_identifier(&format!("__reflex_ins_trigger_on_{}", safe_source));
    let ins_trig = safe_identifier(&format!("__reflex_trigger_ins_on_{}", safe_source));
    let ins_body = body_core
        .replace("{transition_tbl}", &ref_new)
        .replace("{pred_check_block}", &pred_check_ins)
        .replace("{filter_skip_block}", "")
        .replace("{directional_probe_block}", "")
        .replace("{op_value}", "'INSERT'");
    let ins_ddl = format!(
        "CREATE OR REPLACE FUNCTION {ins_fn}() RETURNS TRIGGER AS $fn$ {ins_body} $fn$ LANGUAGE plpgsql;\n\
         CREATE OR REPLACE TRIGGER \"{ins_trig}\" \
         AFTER INSERT ON {source_table} \
         REFERENCING NEW TABLE AS \"{ref_new}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION {ins_fn}()"
    );

    // DELETE
    let del_fn = safe_identifier(&format!("__reflex_del_trigger_on_{}", safe_source));
    let del_trig = safe_identifier(&format!("__reflex_trigger_del_on_{}", safe_source));
    let del_body = body_core
        .replace("{transition_tbl}", &ref_old)
        .replace("{pred_check_block}", &pred_check_del)
        .replace("{filter_skip_block}", "")
        .replace("{directional_probe_block}", "")
        .replace("{op_value}", "'DELETE'");
    let del_ddl = format!(
        "CREATE OR REPLACE FUNCTION {del_fn}() RETURNS TRIGGER AS $fn$ {del_body} $fn$ LANGUAGE plpgsql;\n\
         CREATE OR REPLACE TRIGGER \"{del_trig}\" \
         AFTER DELETE ON {source_table} \
         REFERENCING OLD TABLE AS \"{ref_old}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION {del_fn}()"
    );

    // UPDATE
    let upd_fn = safe_identifier(&format!("__reflex_upd_trigger_on_{}", safe_source));
    let upd_trig = safe_identifier(&format!("__reflex_trigger_upd_on_{}", safe_source));
    let upd_body = body_core
        .replace("{transition_tbl}", &ref_new)
        .replace("{pred_check_block}", &pred_check_upd)
        .replace("{filter_skip_block}", &filter_skip_block_for_update)
        .replace("{directional_probe_block}", &directional_probe_for_update)
        .replace("{op_value}", "_directional_op");
    let upd_ddl = format!(
        "CREATE OR REPLACE FUNCTION {upd_fn}() RETURNS TRIGGER AS $fn$ {upd_body} $fn$ LANGUAGE plpgsql;\n\
         CREATE OR REPLACE TRIGGER \"{upd_trig}\" \
         AFTER UPDATE ON {source_table} \
         REFERENCING NEW TABLE AS \"{ref_new}\" OLD TABLE AS \"{ref_old}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION {upd_fn}()"
    );

    // TRUNCATE — no REFERENCING clauses; loops over all dependent IMVs
    let trunc_fn = safe_identifier(&format!("__reflex_trunc_trigger_on_{}", safe_source));
    let trunc_trig = safe_identifier(&format!("__reflex_trigger_trunc_on_{}", safe_source));
    let trunc_body = format!(
        "DECLARE _rec RECORD; _stmts TEXT; \
         BEGIN \
           FOR _rec IN \
             SELECT name \
             FROM public.__reflex_ivm_reference \
             WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
             ORDER BY graph_depth, name \
           LOOP \
             PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name))); \
             _stmts := public.reflex_build_truncate_sql(_rec.name); \
             IF _stmts <> '' THEN PERFORM public.reflex_execute_separated(_stmts); END IF; \
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
    let ref_new = transition_new_table_name(source_table);
    let ref_old = transition_old_table_name(source_table);
    let delta_tbl = staging_delta_table_name(source_table);

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
             ORDER BY graph_depth, name \
           LOOP \
             IF _rec.where_predicate IS NOT NULL THEN \
               EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '{{transition_tbl}}', _rec.where_predicate) INTO _pred_match; \
               IF NOT _pred_match THEN CONTINUE; END IF; \
             END IF; \
             IF _rec.refresh_mode = 'IMMEDIATE' THEN \
               PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name))); \
               _sql := public.reflex_build_delta_sql(_rec.name, '{source_table}', '{{op}}', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query); \
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
    let ins_fn = safe_identifier(&format!("__reflex_ins_trigger_on_{}", safe_source));
    let ins_trig = safe_identifier(&format!("__reflex_trigger_ins_on_{}", safe_source));
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
    let del_fn = safe_identifier(&format!("__reflex_del_trigger_on_{}", safe_source));
    let del_trig = safe_identifier(&format!("__reflex_trigger_del_on_{}", safe_source));
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
    let upd_fn = safe_identifier(&format!("__reflex_upd_trigger_on_{}", safe_source));
    let upd_trig = safe_identifier(&format!("__reflex_trigger_upd_on_{}", safe_source));
    let upd_body = format!(
        "DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_deferred BOOLEAN := FALSE; _has_rows BOOLEAN; _pred_match BOOLEAN; \
         BEGIN \
           SELECT EXISTS(SELECT 1 FROM \"{ref_new}\" LIMIT 1) INTO _has_rows; \
           IF NOT _has_rows THEN RETURN NULL; END IF; \
           FOR _rec IN \
             SELECT name, base_query, end_query, aggregations::text AS aggregations, \
                    COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, where_predicate \
             FROM public.__reflex_ivm_reference \
             WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
             ORDER BY graph_depth, name \
           LOOP \
             IF _rec.where_predicate IS NOT NULL THEN \
               EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '{ref_new}', _rec.where_predicate) INTO _pred_match; \
               IF NOT _pred_match THEN CONTINUE; END IF; \
             END IF; \
             IF _rec.refresh_mode = 'IMMEDIATE' THEN \
               PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name))); \
               _sql := public.reflex_build_delta_sql(_rec.name, '{source_table}', 'UPDATE', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query); \
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
    let trunc_fn = safe_identifier(&format!("__reflex_trunc_trigger_on_{}", safe_source));
    let trunc_trig = safe_identifier(&format!("__reflex_trigger_trunc_on_{}", safe_source));
    let trunc_body = format!(
        "DECLARE _rec RECORD; _stmts TEXT; \
         BEGIN \
           FOR _rec IN \
             SELECT name \
             FROM public.__reflex_ivm_reference \
             WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
             ORDER BY graph_depth, name \
           LOOP \
             PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name))); \
             _stmts := public.reflex_build_truncate_sql(_rec.name); \
             IF _stmts <> '' THEN PERFORM public.reflex_execute_separated(_stmts); END IF; \
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
             PERFORM public.reflex_flush_deferred(_src.source_table); \
           END LOOP; \
           RETURN NULL; \
         END; \
         $fn$ LANGUAGE plpgsql"
            .to_string(),
        // Constraint trigger — fires at COMMIT for any INSERT into the pending table
        "DROP TRIGGER IF EXISTS __reflex_deferred_flush_trigger ON public.__reflex_deferred_pending"
            .to_string(),
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
    let delta_tbl = staging_delta_table_name(source_table);
    format!(
        "CREATE UNLOGGED TABLE IF NOT EXISTS {} (\
            __reflex_op TEXT NOT NULL, \
            LIKE {} INCLUDING DEFAULTS\
         )",
        delta_tbl, source_table
    )
}

/// Build DDL for the per-(IMV, source) UNLOGGED passthrough scratch tables.
///
/// Returns two CREATE statements (new-side and old-side). Each scratch mirrors
/// the source shape via `LIKE source INCLUDING DEFAULTS`. They are populated
/// at trigger time from the transition tables, then read by the passthrough
/// trigger's DML — avoiding transition-table references inside EXECUTE, which
/// trips a PG assertion in nested-trigger contexts.
pub fn build_passthrough_scratch_ddls(view_name: &str, source_table: &str) -> Vec<String> {
    let new_tbl = passthrough_scratch_new_table_name(view_name, source_table);
    let old_tbl = passthrough_scratch_old_table_name(view_name, source_table);
    vec![
        format!(
            "CREATE UNLOGGED TABLE IF NOT EXISTS {} (LIKE {} INCLUDING DEFAULTS)",
            new_tbl, source_table
        ),
        format!(
            "CREATE UNLOGGED TABLE IF NOT EXISTS {} (LIKE {} INCLUDING DEFAULTS)",
            old_tbl, source_table
        ),
    ]
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
    // Expression columns (e.g. CASE, coalesce) can't be resolved from the catalog.
    // Numeric is a safer default than TEXT for aggregate intermediates — if the
    // expression is text-valued it will error loudly at CREATE time, which is
    // preferable to silent behaviour drift.
    if default_type == "TEXT" {
        // Warn only when executing inside Postgres; pgrx::warning! requires a live
        // elog context and panics under `cargo test --lib`.
        #[cfg(not(test))]
        pgrx::warning!(
            "pg_reflex: could not resolve type for '{}' from catalog; defaulting to NUMERIC. \
             If this column is non-numeric, add an explicit CAST in the IMV SQL.",
            col_name
        );
        #[cfg(test)]
        let _ = col_name;
        return "NUMERIC".to_string();
    }
    default_type.to_string()
}

#[cfg(test)]
#[path = "tests/unit_schema_builder.rs"]
mod tests;
