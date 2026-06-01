use std::collections::HashMap;

use crate::aggregation::{AggregationPlan, EndQueryMapping};
use crate::query_decomposer::{
    delta_scratch_table_name, intermediate_table_name, normalized_column_name,
    passthrough_scratch_new_table_name, passthrough_scratch_old_table_name, quote_identifier,
    safe_identifier, split_qualified_name, staging_delta_table_name, transition_new_table_name,
    transition_old_table_name,
};
use crate::sql_writer::{slot_replace, CreateIndex, CreateTable};

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

    // No inline PRIMARY KEY — we use a hash index for O(1) lookups instead.
    // The B-tree PK is redundant because MERGE handles insert-or-update correctly,
    // the delta query uses GROUP BY (unique output), and advisory locks prevent
    // concurrent MERGEs on the same IMV.
    //
    // fillfactor=70 leaves 30% slack on every heap page so MERGE WHEN
    // MATCHED UPDATE (which only rewrites aggregate columns — none are
    // indexed) can fit the new tuple version on the same page → HOT
    // update → zero index writes. Bench (perftest, 47K rows): 691ms
    // (fillfactor=100) → 75ms (fillfactor=70). Disk cost: ~14% larger
    // baseline, but eliminates ~20% bloat per UPDATE cycle.
    //
    // Partitioned IMVs append `PARTITION BY <strategy> (cols)` BEFORE the
    // storage options (Postgres rejects fillfactor on a partitioned parent,
    // but our intermediate parent never holds tuples — only its children
    // do, and each child inherits fillfactor on creation via the parent's
    // reloptions).  We therefore omit the WITH clause on the parent when
    // partitioning and let each child set fillfactor at PARTITION OF time.
    //
    // UNLOGGED on a partitioned parent: PG18 rejects this outright
    // ("partitioned tables cannot be unlogged"), and pre-PG18 silently
    // ignored it.  Either way the parent holds no rows, so storage mode
    // there is meaningless — the children carry UNLOGGED instead (see
    // `build_partition_child_ddl_pair`).
    let is_partitioned = !plan.partition_columns.is_empty() && !plan.partition_strategy.is_empty();
    let mut builder = CreateTable::new(table_name)
        .unlogged(!logged && !is_partitioned)
        .if_not_exists(true)
        .columns(columns);
    if is_partitioned {
        let part = crate::partition::build_partition_by_clause(
            &plan.partition_strategy,
            &plan.partition_columns,
        );
        builder = builder.partition_by(part);
    } else {
        builder = builder.with_options("fillfactor=70");
    }
    Some(builder.build())
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
    Some(
        CreateTable::new(table_name)
            .unlogged(true)
            .if_not_exists(true)
            .columns(columns)
            .build(),
    )
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
                    // For MIN/MAX, find the matching IntermediateColumn from the plan
                    // and resolve its source_arg (e.g., "e.ts") to get the correct type.
                    // This handles qualified columns like MAX(e.ts) where intermediate_expr
                    // is "__max_e_ts" and source_arg is "e.ts" → resolves to TIMESTAMPTZ
                    // instead of falling back to NUMERIC.
                    if let Some(ic) = plan
                        .intermediate_columns
                        .iter()
                        .find(|ic| ic.name == mapping.intermediate_expr)
                    {
                        resolve_column_type(&ic.source_arg, column_types, "NUMERIC")
                    } else {
                        // Fallback: if no matching IntermediateColumn found, use the old
                        // prefix-stripping logic (defensive, should rarely happen).
                        let source_arg = mapping
                            .intermediate_expr
                            .trim_start_matches("__min_")
                            .trim_start_matches("__max_");
                        resolve_column_type(source_arg, column_types, "NUMERIC")
                    }
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

    // fillfactor=70: same rationale as build_intermediate_table_ddl.
    // Target MERGE WHEN MATCHED UPDATE rewrites only the end-query
    // aggregate columns; group_by columns (which are the only
    // indexed columns via idx__reflex_target_*) stay untouched →
    // HOT update eligible.
    //
    // Partitioned IMVs emit `PARTITION BY` on the parent and skip
    // fillfactor (see build_intermediate_table_ddl note).  UNLOGGED is
    // never emitted on partitioned parents (PG18 rejects it, pre-PG18
    // ignored it); children carry UNLOGGED instead.
    let is_partitioned = !plan.partition_columns.is_empty() && !plan.partition_strategy.is_empty();
    let mut builder = CreateTable::quoted_name(view_name)
        .unlogged(!logged && !is_partitioned)
        .if_not_exists(true)
        .columns(columns);
    if is_partitioned {
        let part = crate::partition::build_partition_by_clause(
            &plan.partition_strategy,
            &plan.partition_columns,
        );
        builder = builder.partition_by(part);
    } else {
        builder = builder.with_options("fillfactor=70");
    }
    builder.build()
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
            let mut builder = CreateIndex::quoted_name(&idx_name, table_name.clone())
                .if_not_exists(true)
                .columns(idx_cols.clone());
            if idx_cols.len() == 1 {
                builder = builder.using("hash");
            } else {
                builder = builder.unique(true).nulls_not_distinct(true);
            }
            indexes.push(builder.build());
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
        indexes.push(
            CreateIndex::quoted_name(&idx_name, target_tbl)
                .if_not_exists(true)
                .columns(group_cols)
                .build(),
        );
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
    // 1.4.5 — Filter-aware spurious-skip (UPDATE only). See
    // sql/trigger_body.plpgsql.in for the relocated main body; this sub-
    // template is spliced in at the `__REFLEX_SLOT_FILTER_SKIP_BLOCK__`
    // sentinel.  References to the source / OLD / NEW transition tables
    // are themselves `__REFLEX_SLOT_*__` slots that we substitute up-front
    // (they are static per build_trigger_ddls call).
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
    // The CASE on t.typname casts `json` / `xml` columns to `text` in the
    // EXCEPT ALL projection — neither type has an equality operator, so a
    // bare projection crashes the comparison at runtime. The TEMP VIEW /
    // transition table downstream still sees the raw column; only the
    // skip-check projection casts.
    //
    // The JOIN against pg_attribute also acts as a runtime existence
    // filter: if the analyzer mis-attributes a column to this source
    // (i.e. names a column that doesn't actually exist on `source_table`),
    // the JOIN simply drops it instead of letting the EXCEPT ALL crash
    // with `column "X" does not exist`. The skip-check then runs on a
    // subset and may be conservative (i.e. doesn't skip) — that's safe;
    // it never produces wrong results.
    let filter_skip_block_for_update = slot_replace(
        include_str!("../sql/trigger_filter_skip_block.plpgsql.in"),
        &[
            ("__REFLEX_SLOT_SOURCE_TABLE__", source_table),
            ("__REFLEX_SLOT_REF_OLD__", &ref_old),
            ("__REFLEX_SLOT_REF_NEW__", &ref_new),
        ],
    );

    // Item α (2026-05-15) — Directional UPDATE dispatch.
    //
    // After filter-aware spurious-skip has decided the multisets differ, probe
    // each transition table for post-filter rows. The three outcomes route
    // the call to `reflex_build_delta_sql` with a *promoted* op:
    //   * OLD empty, NEW has rows → 'INSERT_PROMOTED' (the trigger.rs
    //     codegen checks plan.source_join_keys: if the source has a
    //     JOIN-secondary mapping to a GROUP BY column → bulk-INSERT path;
    //     otherwise → falls back to the standard MERGE path.)
    //   * OLD has rows, NEW empty → 'DELETE_PROMOTED' (same gate: if the
    //     safety mapping exists, trigger.rs uses bulk-DELETE — two
    //     indexed DELETEs against transition_old, skipping the scratch
    //     fill JOIN entirely; otherwise → falls back to MERGE Subtract).
    //   * both have rows          → 'UPDATE' (today's UNION ALL path).
    //
    // The INSERT/DELETE codegen paths read only one transition table by name;
    // they remain visible from the UPDATE trigger's REFERENCING clauses.
    //
    // The probe re-uses `_skip_cols` / `_skip_pred_clause` populated by the
    // filter_skip_block above — same gate (`imv_relevant_columns[source]`
    // non-empty); same WHERE predicate.
    //
    // The 'INSERT_PROMOTED' tag is what makes the bulk-INSERT branch
    // selectable: trigger.rs ONLY considers the bulk path on this op
    // string. Regular INSERT triggers (true source INSERTs, not Item α
    // promotions) stay on op='INSERT' and always take MERGE because new
    // fact rows can legitimately aggregate into existing groups.
    let directional_probe_for_update = slot_replace(
        include_str!("../sql/trigger_directional_probe.plpgsql.in"),
        &[
            ("__REFLEX_SLOT_REF_OLD__", &ref_old),
            ("__REFLEX_SLOT_REF_NEW__", &ref_new),
        ],
    );

    // The where_predicate early-skip needs op-aware semantics:
    //   * INSERT trigger only sees `ref_new` — skip if no NEW row passes.
    //   * DELETE trigger only sees `ref_old` — skip if no OLD row passes.
    //   * UPDATE trigger sees BOTH — must skip ONLY if neither side has a
    //     passing row. A NEW-only check would incorrectly skip rows leaving
    //     the IMV's filter (status flips out of whitelist → IMV must delete
    //     that row's contribution). The __REFLEX_SLOT_PRED_CHECK_BLOCK__
    //     in the main body picks the correct op-specific SQL.
    //
    // 1.4.6 — Path B pre-scratch dispatch.  Static SQL in
    // sql/trigger_body.plpgsql.in handles the |transition| / |source| ratio
    // check; the per-IMV `wipe_threshold` override is read at runtime from
    // `public.__reflex_ivm_reference`.

    // Op-specific where-predicate early-skip blocks.
    let pred_check_one_tpl = include_str!("../sql/trigger_pred_check_one.plpgsql.in");
    let pred_check_one = |tbl: &str| -> String {
        slot_replace(
            pred_check_one_tpl,
            &[("__REFLEX_SLOT_TRANSITION_TBL__", tbl)],
        )
    };
    let pred_check_two = slot_replace(
        include_str!("../sql/trigger_pred_check_two.plpgsql.in"),
        &[
            ("__REFLEX_SLOT_REF_NEW__", &ref_new),
            ("__REFLEX_SLOT_REF_OLD__", &ref_old),
        ],
    );
    let pred_check_ins = pred_check_one(&ref_new);
    let pred_check_del = pred_check_one(&ref_old);
    let pred_check_upd = pred_check_two;

    // Path C — EXPLAIN-based fanout dispatch for INSERT_PROMOTED.
    //
    // Only emitted in the UPDATE trigger body (the only place Item α can
    // promote to INSERT_PROMOTED). For dim-source flips where Path B's
    // |transition| / |source| signal is misleading (1-row UPDATE on a
    // 28-row dim that fans out to 8.9M fact rows), Path C asks the planner
    // for the actual scratch-fill cardinality via `EXPLAIN (FORMAT JSON)`,
    // then compares against the IMV's `wipe_threshold`.
    //
    // Wrapped in BEGIN/EXCEPTION so any failure (catalog lookup, planner
    // error on a transient transition state) falls through to the standard
    // path — safe.
    //
    // Path C high-fanout response is a *smart bulk-INSERT*, not a full
    // reconcile.  The Item α INSERT_PROMOTED guarantee says the existing
    // intermediate has zero rows for the affected group keys, so we can:
    //   1. scratch-fill (base_query with source→transition) — same JOIN
    //      cost as reconcile's INSERT INTO intermediate, but restricted
    //      to the new keys (e.g. 8.9 M rows on alp A4 vs 16.6 M total),
    //   2. DROP the intermediate's UNIQUE index — avoids per-row B-tree
    //      probe cost on the bulk INSERT,
    //   3. bulk INSERT scratch → intermediate,
    //   4. CREATE the index back (16.6 M rows sorted in memory, ~13 s),
    //   5. project to target directly from scratch (skip the standard
    //      target-sync block's re-read of intermediate), and
    //   6. ANALYZE intermediate.
    //
    // Beats reflex_reconcile by skipping (a) the redundant re-aggregation
    // of the surviving 7.7 M rows that didn't change, and (b) the target
    // TRUNCATE + re-read of intermediate. Measured ~110 s on alp A4 vs
    // reconcile's ~175 s vs REFRESH MV's ~155 s.
    let path_c_for_update = slot_replace(
        include_str!("../sql/trigger_path_c_block.plpgsql.in"),
        &[("__REFLEX_SLOT_SOURCE_TABLE__", source_table)],
    );

    // Static slot substitutions shared by INSERT/DELETE/UPDATE bodies.
    let body_template = include_str!("../sql/trigger_body.plpgsql.in");
    let render_body = |transition_tbl: &str,
                       pred_check_block: &str,
                       filter_skip_block: &str,
                       directional_probe_block: &str,
                       path_c_block: &str,
                       op_value: &str|
     -> String {
        slot_replace(
            body_template,
            &[
                ("__REFLEX_SLOT_SOURCE_TABLE__", source_table),
                ("__REFLEX_SLOT_BARE_SOURCE__", &bare_source),
                ("__REFLEX_SLOT_TRANSITION_TBL__", transition_tbl),
                ("__REFLEX_SLOT_PRED_CHECK_BLOCK__", pred_check_block),
                ("__REFLEX_SLOT_FILTER_SKIP_BLOCK__", filter_skip_block),
                (
                    "__REFLEX_SLOT_DIRECTIONAL_PROBE_BLOCK__",
                    directional_probe_block,
                ),
                ("__REFLEX_SLOT_PATH_C_BLOCK__", path_c_block),
                ("__REFLEX_SLOT_OP_VALUE__", op_value),
            ],
        )
    };

    // INSERT
    let ins_fn = safe_identifier(&format!("__reflex_ins_trigger_on_{}", safe_source));
    let ins_trig = safe_identifier(&format!("__reflex_trigger_ins_on_{}", safe_source));
    let ins_body = render_body(&ref_new, &pred_check_ins, "", "", "", "'INSERT'");
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
    let del_body = render_body(&ref_old, &pred_check_del, "", "", "", "'DELETE'");
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
    let upd_body = render_body(
        &ref_new,
        &pred_check_upd,
        &filter_skip_block_for_update,
        &directional_probe_for_update,
        &path_c_for_update,
        "_directional_op",
    );
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
    let trunc_body = slot_replace(
        include_str!("../sql/trigger_truncate_body.plpgsql.in"),
        &[("__REFLEX_SLOT_SOURCE_TABLE__", source_table)],
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
///
/// `source_columns` is the ordered list of bare column names of `source_table`
/// (resolved by the caller from `pg_attribute` against the live catalog).
/// It is baked into the trigger body as a named-column INSERT list so that a
/// staging delta from an older incarnation of the source — whose physical
/// column ORDER may differ from the current shape — still receives the rows
/// at the correct named columns instead of mismatching positionally. See the
/// 1.6.2 regression added in `pg_test_deferred.rs`.
pub fn build_deferred_trigger_ddls(source_table: &str, source_columns: &[String]) -> Vec<String> {
    let safe_source = source_table.replace('.', "_").replace('"', "");
    let bare_source = source_table
        .split('.')
        .next_back()
        .unwrap_or(source_table)
        .to_string();
    let ref_new = transition_new_table_name(source_table);
    let ref_old = transition_old_table_name(source_table);
    let delta_tbl = staging_delta_table_name(source_table);
    let col_list = source_columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    // Mixed-mode body for INSERT/DELETE: process IMMEDIATE IMVs inline, stage
    // deltas for DEFERRED IMVs.  Relocated to sql/deferred_trigger_body.plpgsql.in
    // (see plans/1_6_1_refacto.md Phase 4).
    let body_template = include_str!("../sql/deferred_trigger_body.plpgsql.in");
    let render_ins_del = |transition_tbl: &str, op: &str, op_code: &str, ref_tbl: &str| -> String {
        slot_replace(
            body_template,
            &[
                ("__REFLEX_SLOT_SOURCE_TABLE__", source_table),
                ("__REFLEX_SLOT_BARE_SOURCE__", &bare_source),
                ("__REFLEX_SLOT_TRANSITION_TBL__", transition_tbl),
                ("__REFLEX_SLOT_OP__", op),
                ("__REFLEX_SLOT_OP_CODE__", op_code),
                ("__REFLEX_SLOT_REF_TBL__", ref_tbl),
                ("__REFLEX_SLOT_DELTA_TBL__", &delta_tbl),
                ("__REFLEX_SLOT_COL_LIST__", &col_list),
            ],
        )
    };

    // INSERT
    let ins_fn = safe_identifier(&format!("__reflex_ins_trigger_on_{}", safe_source));
    let ins_trig = safe_identifier(&format!("__reflex_trigger_ins_on_{}", safe_source));
    let ins_body = render_ins_del(&ref_new, "INSERT", "I", &ref_new);
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
    let del_body = render_ins_del(&ref_old, "DELETE", "D", &ref_old);
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
    let upd_body = slot_replace(
        include_str!("../sql/deferred_trigger_update_body.plpgsql.in"),
        &[
            ("__REFLEX_SLOT_SOURCE_TABLE__", source_table),
            ("__REFLEX_SLOT_BARE_SOURCE__", &bare_source),
            ("__REFLEX_SLOT_REF_NEW__", &ref_new),
            ("__REFLEX_SLOT_REF_OLD__", &ref_old),
            ("__REFLEX_SLOT_DELTA_TBL__", &delta_tbl),
            ("__REFLEX_SLOT_COL_LIST__", &col_list),
        ],
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
    let trunc_body = slot_replace(
        include_str!("../sql/deferred_trigger_truncate_body.plpgsql.in"),
        &[
            ("__REFLEX_SLOT_SOURCE_TABLE__", source_table),
            ("__REFLEX_SLOT_BARE_SOURCE__", &bare_source),
            ("__REFLEX_SLOT_DELTA_TBL__", &delta_tbl),
        ],
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
        // Constraint trigger function: at COMMIT flush only the source this
        // transaction staged. The trigger is FOR EACH ROW, so NEW.source_table
        // is precisely the pending row this transaction inserted — flushing it
        // (rather than scanning the shared, cross-schema pending queue) keeps
        // deferred maintenance transaction-local: a commit in one schema no
        // longer flushes, advisory-locks, or TRUNCATE-locks the staging tables
        // of sources owned by other, independent schemas. The per-source
        // advisory lock inside reflex_flush_deferred makes the redundant fires
        // for a source touched by several statements collapse into one
        // effective flush, and the cross-source guard still sees every source
        // this transaction staged (all pending rows are present at COMMIT
        // before any flush deletes its own).
        "CREATE OR REPLACE FUNCTION __reflex_deferred_flush_fn() RETURNS TRIGGER AS $fn$ \
         BEGIN \
           PERFORM public.reflex_flush_deferred(NEW.source_table); \
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
    CreateTable::new(delta_tbl)
        .unlogged(true)
        .if_not_exists(true)
        .column("__reflex_op TEXT NOT NULL")
        .like(source_table.to_string())
        .build()
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
        CreateTable::new(new_tbl)
            .unlogged(true)
            .if_not_exists(true)
            .like(source_table.to_string())
            .build(),
        CreateTable::new(old_tbl)
            .unlogged(true)
            .if_not_exists(true)
            .like(source_table.to_string())
            .build(),
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
