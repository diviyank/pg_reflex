use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

use crate::aggregation::{plan_aggregation, plan_aggregation_with_topk};
use crate::query_decomposer::{
    affected_groups_table_name, bare_column_name, format_pg_text_array_literal,
    generate_aggregations_json, generate_base_query, generate_end_query, intermediate_table_name,
    normalized_column_name, quote_identifier, replace_identifier, safe_identifier,
    shrunk_groups_table_name, split_qualified_name,
};
use crate::schema_builder::{
    build_deferred_flush_ddl, build_deferred_trigger_ddls, build_delta_scratch_table_ddl,
    build_indexes_ddl, build_intermediate_table_ddl, build_passthrough_scratch_ddls,
    build_staging_table_ddl, build_target_table_ddl, build_trigger_ddls, resolve_column_type,
};
use crate::sql_analyzer::{analyze, SqlAnalysisError};
use crate::sql_writer::{
    add_graph_child_links, insert_registry_row, AggregationsCast, RegistryRow,
};
use crate::validate_view_name;
use crate::window;

/// Outputs of the parse-and-validate prelude of [`create_reflex_ivm_impl`].
///
/// Bundles the normalized storage/refresh flags, the parsed sqlparser AST and
/// the [`crate::sql_analyzer::SqlAnalysis`] so the decomposition helpers and
/// the main pipeline can read them without re-parsing.
struct ParsedInputs {
    logged: bool,
    deferred: bool,
    storage_upper: String,
    mode_upper: String,
    parsed_sql: Vec<sqlparser::ast::Statement>,
    analysis: crate::sql_analyzer::SqlAnalysis,
}

/// Pipeline state shared across phases of `create_reflex_ivm_impl`. Owns the
/// mutable `plan`; helpers mutate fields in-place. Input refs (`view_name`,
/// `sql`, etc.) borrow from the caller's arg slots.
struct BuildContext<'a> {
    // Inputs
    view_name: &'a str,
    sql: &'a str,
    unique_columns_str: &'a str,
    if_not_exists: bool,
    #[allow(dead_code)]
    topk_k: Option<usize>,
    ignore_sources: &'a [String],
    partition_by: &'a [String],

    // Parsed
    logged: bool,
    deferred: bool,
    storage_upper: String,
    mode_upper: String,
    analysis: crate::sql_analyzer::SqlAnalysis,

    // Plan (mutated through pipeline)
    plan: crate::aggregation::AggregationPlan,

    // Source decomposition (set immediately after plan construction)
    froms: Vec<String>,
    real_source_names: Vec<String>,
    is_join_query: bool,

    // Pre-SPI resolution outputs
    resolved_unique_columns: Vec<String>,
    resolved_partition_cols: Vec<String>,
    resolved_strategy: String,

    // SPI-phase outputs
    ivm_froms: Vec<String>,
    depth: i32,
    unlogged_tables: Vec<String>,
}

/// Shared, by-reference parameter bundle for the decomposition entry points
/// (`try_decompose_set_op`, `try_decompose_ctes`, `try_decompose_distinct_on`).
///
/// One struct so threading a new field reaches every path uniformly — a path
/// cannot silently lack one (this is exactly how Bug 2 happened).
struct DecomposeCtx<'a> {
    view_name: &'a str,
    sql: &'a str,
    unique_columns_str: &'a str,
    cte_unique_columns: &'a std::collections::HashMap<String, String>,
    storage_mode: &'a str,
    refresh_mode: &'a str,
    topk_k: Option<usize>,
    ignore_sources: &'a [String],
    partition_by: &'a [String],
    parsed: &'a ParsedInputs,
    /// When true, decompositions that would otherwise emit a `CREATE VIEW`
    /// wrapper (UNION ALL today; DISTINCT ON / window in future) MUST instead
    /// materialise the wrapper as an UNLOGGED TABLE maintained by per-operand
    /// mirror triggers. Set on every recursive `create_reflex_ivm_impl` call
    /// that builds an intermediate sub-IMV (e.g. a CTE sub-IMV), because
    /// downstream consumers will try to install transition-table triggers on
    /// the wrapper, which PostgreSQL rejects for views.
    materialize_as_table: bool,
}

/// Phase 1 of the create-IMV pipeline.
///
/// Normalizes `storage`/`refresh`, validates the view name, parses the SQL
/// and runs [`crate::sql_analyzer::analyze`] on it. Also rejects DISTINCT on
/// aggregates other than COUNT (only COUNT(DISTINCT) is incrementally
/// maintainable).
///
/// Returns the error string verbatim on the `Err` arm so the caller can
/// `return` it unchanged — preserves byte-identical error reporting.
fn validate_and_parse_inputs(
    view_name: &str,
    sql: &str,
    storage_mode: &str,
    refresh_mode: &str,
) -> Result<ParsedInputs, &'static str> {
    let storage_upper = storage_mode.to_uppercase();
    if storage_upper != "LOGGED" && storage_upper != "UNLOGGED" {
        return Err(crate::reflex_reject(
            "storage must be 'LOGGED' or 'UNLOGGED'",
        ));
    }
    let logged = storage_upper == "LOGGED";
    let mode_upper = refresh_mode.to_uppercase();
    if mode_upper != "IMMEDIATE" && mode_upper != "DEFERRED" {
        return Err(crate::reflex_reject(
            "mode must be 'IMMEDIATE' or 'DEFERRED'",
        ));
    }
    let deferred = mode_upper == "DEFERRED";
    validate_view_name(view_name)?;
    let dialect = PostgreSqlDialect {};
    let parsed_sql = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(e) => {
            warning!("pg_reflex: failed to parse SQL for '{}': {}", view_name, e);
            return Err(crate::reflex_reject(&format!("Failed to parse SQL: {}", e)));
        }
    };
    let analysis = match analyze(&parsed_sql) {
        Err(SqlAnalysisError::MultipleQueries(_)) => {
            return Err(crate::reflex_reject("Expected 1 query, got multiple"));
        }
        Err(SqlAnalysisError::NotASelectQuery) => {
            return Err(crate::reflex_reject("Query is not a SELECT"));
        }
        Ok(a) => {
            if let Some(reason) = a.unsupported_reason() {
                return Err(crate::reflex_reject(&reason));
            }
            // Reject SUM(DISTINCT), AVG(DISTINCT), etc. — DISTINCT modifier is only
            // supported on COUNT. Check the original SQL for the pattern.
            let sql_upper = sql.to_uppercase();
            let has_distinct_agg = sql_upper.contains("SUM(DISTINCT")
                || sql_upper.contains("SUM (DISTINCT")
                || sql_upper.contains("AVG(DISTINCT")
                || sql_upper.contains("AVG (DISTINCT")
                || sql_upper.contains("MIN(DISTINCT")
                || sql_upper.contains("MIN (DISTINCT")
                || sql_upper.contains("MAX(DISTINCT")
                || sql_upper.contains("MAX (DISTINCT")
                || sql_upper.contains("BOOL_OR(DISTINCT")
                || sql_upper.contains("BOOL_OR (DISTINCT");
            if has_distinct_agg {
                return Err(crate::reflex_reject(
                    "DISTINCT modifier on SUM/AVG/MIN/MAX/BOOL_OR is not supported. \
                     Only COUNT(DISTINCT col) is supported. Use a CTE with SELECT DISTINCT \
                     to pre-deduplicate: WITH d AS (SELECT DISTINCT grp, val FROM t) SELECT grp, SUM(val) FROM d GROUP BY grp"
                ));
            }
            a
        }
    };
    Ok(ParsedInputs {
        logged,
        deferred,
        storage_upper,
        mode_upper,
        parsed_sql,
        analysis,
    })
}

/// Extract the output column names from a CTE's query.
/// Compute the subset of partition_by columns that appear in the CTE's output.
/// Case-insensitive comparison per PostgreSQL identifier folding.
fn compute_cte_partition_subset(
    partition_by: &[String],
    cte_output_columns: &[String],
) -> Vec<String> {
    if partition_by.is_empty() {
        return Vec::new();
    }

    let output_lower: Vec<String> = cte_output_columns
        .iter()
        .map(|c| c.to_lowercase())
        .collect();

    partition_by
        .iter()
        .filter(|part_col| {
            let part_col_lower = part_col.to_lowercase();
            output_lower.contains(&part_col_lower)
        })
        .cloned()
        .collect()
}

/// Roll back the sub-IMVs a decomposition step has already materialised when a
/// later step fails with a *soft* (returned-`"ERROR…"`-string) error.
///
/// Hard failures raise a PostgreSQL error and abort the whole transaction, which
/// self-rolls-back every object created so far. Soft errors return normally, so
/// without this the partially-built sub-IMVs would commit and orphan themselves
/// in the IMV space. Each name is dropped with `cascade = true`, in reverse
/// creation order (dependents before the dependencies they read from), which also
/// tears down any nested descendants the sub-IMV itself decomposed into.
fn rollback_partial_sub_imvs(sub_imv_names: &[String]) {
    for name in sub_imv_names.iter().rev() {
        let drop_result = crate::drop_ivm::drop_reflex_ivm_impl(name, true);
        if drop_result.starts_with("ERROR") {
            warning!(
                "pg_reflex: could not roll back partial sub-IMV '{}' after a failed create: {}",
                name,
                drop_result
            );
        }
    }
}

/// Decomposition phase: UNION / INTERSECT / EXCEPT.
///
/// Each operand is materialised as its own sub-IMV (recursively); the user-
/// visible name becomes a `CREATE VIEW` over those sub-IMVs that PostgreSQL
/// evaluates on read. Returns `Some(result)` to short-circuit
/// [`create_reflex_ivm_impl`]; `None` if the query has no top-level set
/// operator.
#[allow(clippy::too_many_arguments)]
fn try_decompose_set_op(ctx: &DecomposeCtx) -> Option<&'static str> {
    let set_op = ctx.parsed.analysis.set_operation.as_ref()?;
    match set_op.op {
        sqlparser::ast::SetOperator::Union
        | sqlparser::ast::SetOperator::Intersect
        | sqlparser::ast::SetOperator::Except => {}
        _ => {
            return Some(crate::reflex_reject(
                "Unsupported set operation. Supported: UNION, INTERSECT, EXCEPT.",
            ));
        }
    }

    // Each operand becomes its own sub-IMV.
    // Propagate unique_columns so passthrough sub-IMVs can use targeted DELETE/UPDATE
    // instead of falling back to full refresh.
    let mut sub_imv_names: Vec<String> = Vec::new();
    for (i, operand_sql) in set_op.operand_sqls.iter().enumerate() {
        let sub_name = safe_identifier(&format!("{}__union_{}", ctx.view_name, i));
        // Propagate materialize_as_table: if this wrapper has to be a table,
        // any nested set-op operands of an operand must also be tables (their
        // wrappers will themselves serve as trigger sources further down).
        let result = create_reflex_ivm_impl_with_materialization(
            &sub_name,
            operand_sql,
            ctx.unique_columns_str,
            false,
            ctx.storage_mode,
            ctx.refresh_mode,
            ctx.topk_k,
            ctx.ignore_sources,
            ctx.partition_by,
            ctx.materialize_as_table,
        );
        if result.starts_with("ERROR") {
            rollback_partial_sub_imvs(&sub_imv_names);
            return Some(result);
        }
        sub_imv_names.push(sub_name);
    }

    // Build the union query over sub-IMV targets
    let union_selects: Vec<String> = sub_imv_names
        .iter()
        .map(|name| format!("SELECT * FROM {}", quote_identifier(name)))
        .collect();

    if set_op.is_all {
        if ctx.materialize_as_table {
            // Wrapper must be a TABLE because a downstream consumer (the IMV
            // that referenced this set-op as a CTE / sub-query) will install
            // transition-table triggers on it — PostgreSQL rejects those on
            // VIEWs. Build via the dedicated helper.
            Spi::connect_mut(|client| {
                install_union_all_intermediate_wrapper(
                    client,
                    ctx.view_name,
                    &sub_imv_names,
                    ctx.sql,
                    &ctx.parsed.storage_upper,
                    &ctx.parsed.mode_upper,
                );
            });
        } else {
            // Top-level UNION ALL with no downstream consumer: zero-overhead VIEW.
            let view_sql = sub_imv_names
                .iter()
                .map(|name| format!("SELECT * FROM {}", quote_identifier(name)))
                .collect::<Vec<_>>()
                .join(" UNION ALL ");
            Spi::connect_mut(|client| {
                client
                    .update(
                        &format!(
                            "CREATE OR REPLACE VIEW {} AS {}",
                            quote_identifier(ctx.view_name),
                            view_sql
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
                let depends_on: Vec<String> = sub_imv_names.clone();
                let depends_on_imv: Vec<String> = sub_imv_names.clone();
                let depth = sub_imv_names.len() as i32 + 1;
                insert_registry_row(
                    client,
                    &RegistryRow::decomposed(
                        ctx.view_name,
                        depth,
                        &depends_on,
                        &depends_on_imv,
                        ctx.sql,
                        &view_sql,
                        &ctx.parsed.storage_upper,
                        &ctx.parsed.mode_upper,
                    ),
                )
                .unwrap_or_report();
                add_graph_child_links(client, ctx.view_name, &depends_on_imv).unwrap_or_report();
            });
        }
    } else {
        // UNION / INTERSECT / EXCEPT (without ALL): VIEW-based set operation.
        // These cannot be materialised as tables (they need deduplication at
        // query time), so they can only be used at the top level (not in a CTE
        // body that a downstream aggregate IMV tries to install transition-table
        // triggers on). Reject if materialize_as_table is true.
        if ctx.materialize_as_table {
            rollback_partial_sub_imvs(&sub_imv_names);
            return Some(crate::reflex_reject(
                "UNION/INTERSECT/EXCEPT (without ALL) cannot be used as an intermediate \
                 sub-IMV because their result is dedup-/order-dependent on the full input \
                 and cannot be incrementally maintained on each operand delta. Options: \
                 (1) hoist this set op to the outermost SELECT so it stays a VIEW; \
                 (2) define this view with kind: mv; \
                 (3) rewrite as UNION ALL if operands are guaranteed disjoint.",
            ));
        }

        let set_keyword = match set_op.op {
            sqlparser::ast::SetOperator::Union => "UNION",
            sqlparser::ast::SetOperator::Intersect => "INTERSECT",
            sqlparser::ast::SetOperator::Except => "EXCEPT",
            _ => "UNION",
        };
        let view_sql = union_selects.join(&format!(" {} ", set_keyword));
        Spi::connect_mut(|client| {
            client
                .update(
                    &format!(
                        "CREATE OR REPLACE VIEW {} AS {}",
                        quote_identifier(ctx.view_name),
                        view_sql
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
        });

        // Register in reference table
        Spi::connect_mut(|client| {
            let depends_on: Vec<String> = sub_imv_names.clone();
            let depends_on_imv: Vec<String> = sub_imv_names.clone();
            let depth = sub_imv_names.len() as i32 + 1;
            insert_registry_row(
                client,
                &RegistryRow::decomposed(
                    ctx.view_name,
                    depth,
                    &depends_on,
                    &depends_on_imv,
                    ctx.sql,
                    &view_sql,
                    &ctx.parsed.storage_upper,
                    &ctx.parsed.mode_upper,
                ),
            )
            .unwrap_or_report();
            add_graph_child_links(client, ctx.view_name, &depends_on_imv).unwrap_or_report();
        });
    }

    Some("CREATE REFLEX INCREMENTAL VIEW")
}

/// Install per-operand mirror triggers on `operand` that propagate every
/// INSERT / UPDATE / DELETE 1:1 into the wrapper TABLE `wrapper`. One pair of
/// trigger function + trigger per DML kind (INS / DEL / UPD). The trigger
/// function lives in `public` and is named per (wrapper, operand_idx) so it
/// doesn't collide with the consolidated source triggers used by aggregate
/// IMVs.
fn install_union_mirror_triggers(
    client: &mut pgrx::spi::SpiClient<'_>,
    wrapper: &str,
    operand: &str,
    operand_idx: usize,
    cols: &[String],
) {
    if cols.is_empty() {
        warning!(
            "pg_reflex: wrapper '{}' has no columns; UNION-ALL mirror triggers skipped",
            wrapper
        );
        return;
    }
    let wrapper_q = quote_identifier(wrapper);
    let operand_q = quote_identifier(operand);
    let safe_wrapper = crate::query_decomposer::sanitized_source_suffix(wrapper);

    // Column list for INSERTs. We always write __reflex_src_idx first.
    let payload_col_list: String = cols
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    let full_col_list = format!("__reflex_src_idx, {payload_col_list}");
    let new_select: String = cols
        .iter()
        .map(|c| format!("__reflex_new.{}", quote_identifier(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let match_pred: String = cols
        .iter()
        .map(|c| {
            let q = quote_identifier(c);
            format!("w.{q} IS NOT DISTINCT FROM o.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    let fn_base = format!("__reflex_union_mirror_{safe_wrapper}_{operand_idx}");
    let fn_ins = format!("{fn_base}_ins");
    let fn_del = format!("{fn_base}_del");
    let fn_upd = format!("{fn_base}_upd");

    // INSERT mirror: tag every NEW row with this operand's index.
    let ins_body = format!(
        "CREATE OR REPLACE FUNCTION public.{fn_ins}() \
         RETURNS TRIGGER LANGUAGE plpgsql AS $body$\n\
         BEGIN\n  \
           INSERT INTO {wrapper_q} ({full_col_list}) \
           SELECT {operand_idx}::SMALLINT, {new_select} FROM __reflex_new;\n  \
           RETURN NULL;\n\
         END;\n$body$"
    );

    // DELETE mirror: remove operand-i rows in the wrapper that match the OLD rows.
    // The __reflex_src_idx = operand_idx filter prevents cross-operand over-delete.
    // Intra-operand duplicate over-delete is still possible (documented).
    let del_body = format!(
        "CREATE OR REPLACE FUNCTION public.{fn_del}() \
         RETURNS TRIGGER LANGUAGE plpgsql AS $body$\n\
         BEGIN\n  \
           DELETE FROM {wrapper_q} w \
           WHERE w.__reflex_src_idx = {operand_idx}::SMALLINT \
             AND EXISTS (SELECT 1 FROM __reflex_old o WHERE {match_pred});\n  \
           RETURN NULL;\n\
         END;\n$body$"
    );

    // UPDATE mirror: DEL then INS (single statement to avoid mid-trigger inconsistency).
    let upd_new_select: String = cols
        .iter()
        .map(|c| format!("__reflex_new.{}", quote_identifier(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let upd_body = format!(
        "CREATE OR REPLACE FUNCTION public.{fn_upd}() \
         RETURNS TRIGGER LANGUAGE plpgsql AS $body$\n\
         BEGIN\n  \
           DELETE FROM {wrapper_q} w \
           WHERE w.__reflex_src_idx = {operand_idx}::SMALLINT \
             AND EXISTS (SELECT 1 FROM __reflex_old o WHERE {match_pred});\n  \
           INSERT INTO {wrapper_q} ({full_col_list}) \
           SELECT {operand_idx}::SMALLINT, {upd_new_select} FROM __reflex_new;\n  \
           RETURN NULL;\n\
         END;\n$body$"
    );

    for stmt in [&ins_body, &del_body, &upd_body] {
        client.update(stmt, None, &[]).unwrap_or_report();
    }

    let trg_ins = format!("__reflex_union_mirror_ins_{safe_wrapper}_{operand_idx}");
    let trg_del = format!("__reflex_union_mirror_del_{safe_wrapper}_{operand_idx}");
    let trg_upd = format!("__reflex_union_mirror_upd_{safe_wrapper}_{operand_idx}");

    for trg in [&trg_ins, &trg_del, &trg_upd] {
        client
            .update(
                &format!("DROP TRIGGER IF EXISTS {trg} ON {operand_q}"),
                None,
                &[],
            )
            .unwrap_or_report();
    }

    client
        .update(
            &format!(
                "CREATE TRIGGER {trg_ins} AFTER INSERT ON {operand_q} \
                 REFERENCING NEW TABLE AS __reflex_new \
                 FOR EACH STATEMENT EXECUTE FUNCTION public.{fn_ins}()"
            ),
            None,
            &[],
        )
        .unwrap_or_report();
    client
        .update(
            &format!(
                "CREATE TRIGGER {trg_del} AFTER DELETE ON {operand_q} \
                 REFERENCING OLD TABLE AS __reflex_old \
                 FOR EACH STATEMENT EXECUTE FUNCTION public.{fn_del}()"
            ),
            None,
            &[],
        )
        .unwrap_or_report();
    client
        .update(
            &format!(
                "CREATE TRIGGER {trg_upd} AFTER UPDATE ON {operand_q} \
                 REFERENCING NEW TABLE AS __reflex_new OLD TABLE AS __reflex_old \
                 FOR EACH STATEMENT EXECUTE FUNCTION public.{fn_upd}()"
            ),
            None,
            &[],
        )
        .unwrap_or_report();
}

/// Build an intermediate UNION-ALL wrapper as an UNLOGGED TABLE with a
/// `__reflex_src_idx SMALLINT NOT NULL` discriminator column followed by
/// the operand columns. Populates it per-operand, installs per-operand
/// mirror triggers, and registers the wrapper in the catalog as a
/// decomposed row.
///
/// The wrapper is decomposed (aggregations_json = "{}") so the consolidated
/// reflex trigger no-ops it; maintenance is done entirely by the per-
/// operand mirror triggers installed here.
///
/// Caller invariant: `operand_sub_imv_names` is non-empty and every name
/// in it already exists as a real relation (TABLE or VIEW). UNION-ALL
/// operand recursion (which builds the sub-IMVs) must have completed
/// before this helper is invoked.
fn install_union_all_intermediate_wrapper(
    client: &mut pgrx::spi::SpiClient<'_>,
    view_name: &str,
    operand_sub_imv_names: &[String],
    user_sql: &str,
    storage_upper: &str,
    mode_upper: &str,
) {
    assert!(
        !operand_sub_imv_names.is_empty(),
        "install_union_all_intermediate_wrapper called with no operands"
    );

    // 1. Discover operand columns from operand 0 (UNION-ALL operands are
    //    union-compatible, so operand 0 defines the column shape).
    //    Resolve via `to_regclass($1)` so the session's `search_path` is
    //    honoured — operand sub-IMV names are created unqualified, so the
    //    earlier `nspname = $1` form silently looked in `public` and missed
    //    tenant-schema operands.
    let operand0 = &operand_sub_imv_names[0];
    let col_rows: Vec<(String, String)> = client
        .select(
            "SELECT a.attname::text AS name, \
                    format_type(a.atttypid, a.atttypmod) AS pg_type \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
             WHERE c.oid = to_regclass($1) \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            None,
            &[unsafe {
                DatumWithOid::new(operand0.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .unwrap_or_report()
        .filter_map(|r| {
            let n = r.get_by_name::<&str, _>("name").ok()??.to_string();
            // Skip __reflex_src_idx in case an operand is itself a
            // UNION-ALL wrapper — we re-tag at this level, not inherit.
            if n == "__reflex_src_idx" {
                return None;
            }
            let t = r.get_by_name::<&str, _>("pg_type").ok()??.to_string();
            Some((n, t))
        })
        .collect();
    if col_rows.is_empty() {
        warning!(
            "pg_reflex: operand '{}' has no columns; UNION-ALL wrapper '{}' creation skipped",
            operand0,
            view_name
        );
        return;
    }
    let payload_cols: Vec<String> = col_rows.iter().map(|(n, _)| n.clone()).collect();

    // 2. Build wrapper DDL: __reflex_src_idx first, then operand columns.
    let wrapper_q = quote_identifier(view_name);
    let col_defs: Vec<String> = col_rows
        .iter()
        .map(|(n, t)| format!("{} {}", quote_identifier(n), t))
        .collect();
    client
        .update(
            &format!("DROP TABLE IF EXISTS {} CASCADE", wrapper_q),
            None,
            &[],
        )
        .unwrap_or_report();
    client
        .update(
            &format!(
                "CREATE UNLOGGED TABLE {} (__reflex_src_idx SMALLINT NOT NULL, {})",
                wrapper_q,
                col_defs.join(", ")
            ),
            None,
            &[],
        )
        .unwrap_or_report();

    // 3. Populate initial rows per operand with the operand index tag.
    let payload_col_list = payload_cols
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    let full_col_list = format!("__reflex_src_idx, {payload_col_list}");
    for (i, operand) in operand_sub_imv_names.iter().enumerate() {
        let operand_q = quote_identifier(operand);
        client
            .update(
                &format!(
                    "INSERT INTO {wrapper_q} ({full_col_list}) \
                     SELECT {i}::SMALLINT, {payload_col_list} FROM {operand_q}"
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    }

    // 4. Install per-operand mirror triggers.
    for (i, operand) in operand_sub_imv_names.iter().enumerate() {
        install_union_mirror_triggers(client, view_name, operand, i, &payload_cols);
    }

    // 5. Register in catalog. Use the user's original SQL as sql_query; the
    //    base_query field stores the UNION-ALL over operand sub-IMVs for
    //    introspection consistency with the previous code.
    let view_sql_for_catalog: String = operand_sub_imv_names
        .iter()
        .map(|n| format!("SELECT * FROM {}", quote_identifier(n)))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let depends_on: Vec<String> = operand_sub_imv_names.to_vec();
    let depends_on_imv: Vec<String> = operand_sub_imv_names.to_vec();
    let depth = operand_sub_imv_names.len() as i32 + 1;
    insert_registry_row(
        client,
        &RegistryRow::decomposed(
            view_name,
            depth,
            &depends_on,
            &depends_on_imv,
            user_sql,
            &view_sql_for_catalog,
            storage_upper,
            mode_upper,
        ),
    )
    .unwrap_or_report();
    add_graph_child_links(client, view_name, &depends_on_imv).unwrap_or_report();
}

/// Decomposition phase: `SELECT DISTINCT ON (...) ... ORDER BY ...`.
///
/// `DISTINCT ON` keeps one row per `(distinct-on-cols)` group. We can't IMV
/// it directly, so we materialise the underlying SELECT as a passthrough
/// sub-IMV and place a `CREATE VIEW` over it that picks `__reflex_rn = 1`
/// via `ROW_NUMBER()`. The window evaluates at read time.
#[allow(clippy::too_many_arguments)]
fn try_decompose_distinct_on(ctx: &DecomposeCtx) -> Option<&'static str> {
    let analysis = &ctx.parsed.analysis;
    if !analysis.has_distinct_on || analysis.distinct_on_columns.is_empty() {
        return None;
    }

    // Build base SQL: original SELECT without DISTINCT ON and ORDER BY
    let select_items: Vec<String> = analysis
        .select_columns
        .iter()
        .map(|c| {
            if let Some(ref alias) = c.alias {
                format!("{} AS {}", c.expr_sql, alias)
            } else {
                c.expr_sql.clone()
            }
        })
        .collect();
    let mut base_sql = format!(
        "SELECT {} FROM {}",
        select_items.join(", "),
        analysis.from_clause_sql
    );
    if let Some(ref wc) = analysis.where_clause {
        base_sql.push_str(&format!(" WHERE {}", wc));
    }

    // Create sub-IMV for the base data
    let base_name = format!("{}__base", ctx.view_name);
    let result = create_reflex_ivm_impl(
        &base_name,
        &base_sql,
        ctx.unique_columns_str,
        false,
        ctx.storage_mode,
        ctx.refresh_mode,
        ctx.topk_k,
        ctx.ignore_sources,
        ctx.partition_by,
    );
    if result.starts_with("ERROR") {
        return Some(result);
    }

    // Build the VIEW: SELECT <cols> FROM (SELECT *, ROW_NUMBER() OVER (...) AS __reflex_rn FROM base) WHERE __reflex_rn = 1
    // Strip table qualifiers — the VIEW reads from the base sub-IMV which has bare column names
    let partition_cols: Vec<String> = analysis
        .distinct_on_columns
        .iter()
        .map(|c| format!("\"{}\"", bare_column_name(c)))
        .collect();
    let partition_by_clause = partition_cols.join(", ");

    // For ORDER BY, strip table qualifiers but preserve ASC/DESC/NULLS modifiers
    let order_parts: Vec<String> = analysis
        .order_by_exprs
        .iter()
        .map(|expr| {
            // Split on first space to separate column from modifiers (e.g., "j2.val DESC")
            let parts: Vec<&str> = expr.splitn(2, ' ').collect();
            let col = format!("\"{}\"", bare_column_name(parts[0]));
            if parts.len() > 1 {
                format!("{} {}", col, parts[1])
            } else {
                col
            }
        })
        .collect();
    let order_by = order_parts.join(", ");

    // Output column list (just names/aliases, no expressions)
    let output_cols: Vec<String> = analysis
        .select_columns
        .iter()
        .map(|c| {
            if let Some(ref alias) = c.alias {
                format!("\"{}\"", alias)
            } else {
                format!("\"{}\"", bare_column_name(&c.expr_sql))
            }
        })
        .collect();

    let view_sql = format!(
        "SELECT {} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}) AS __reflex_rn FROM {}) __sub WHERE __reflex_rn = 1",
        output_cols.join(", "),
        partition_by_clause,
        order_by,
        quote_identifier(&base_name)
    );

    Spi::connect_mut(|client| {
        client
            .update(
                &format!(
                    "CREATE OR REPLACE VIEW {} AS {}",
                    quote_identifier(ctx.view_name),
                    view_sql
                ),
                None,
                &[],
            )
            .unwrap_or_report();

        // Register in reference table for cleanup
        let depends_on = vec![base_name.clone()];
        let depends_on_imv = vec![base_name.clone()];
        insert_registry_row(
            client,
            &RegistryRow::decomposed(
                ctx.view_name,
                2,
                &depends_on,
                &depends_on_imv,
                ctx.sql,
                &view_sql,
                &ctx.parsed.storage_upper,
                &ctx.parsed.mode_upper,
            ),
        )
        .unwrap_or_report();
        add_graph_child_links(client, ctx.view_name, &depends_on_imv).unwrap_or_report();
    });

    Some("CREATE REFLEX INCREMENTAL VIEW")
}

/// Decomposition phase: queries with window functions.
///
/// Window functions like `ROW_NUMBER`, `RANK`, `LAG` depend on the full
/// result set, so they can't be incrementally maintained. We split the query
/// into a base sub-IMV (the aggregate or passthrough underneath) and a
/// `CREATE VIEW` over it that applies the windows at read time. For
/// `GROUP BY + WINDOW` the base result is small, so read-time cost is low.
#[allow(clippy::too_many_arguments)]
fn try_decompose_window(
    view_name: &str,
    sql: &str,
    unique_columns_str: &str,
    storage_mode: &str,
    refresh_mode: &str,
    topk_k: Option<usize>,
    ignore_sources: &[String],
    partition_by: &[String],
    parsed: &ParsedInputs,
) -> Option<&'static str> {
    // Check if the top-level SELECT contains a window function.
    // select_columns are the MAIN query's top-level projection columns.
    // is_window is true iff the column is a top-level ... OVER (...) expression.
    let has_top_level_window = parsed.analysis.select_columns.iter().any(|c| c.is_window);

    if !has_top_level_window {
        // If there's no top-level window, but has_window_function is true, then
        // a window exists somewhere deeper (subquery or derived table).
        // CTEs and set-ops were already decomposed earlier in the pipeline,
        // so any remaining window in a subquery cannot be incrementally maintained.
        if parsed.analysis.has_window_function {
            return Some(crate::reflex_reject(
                "Window functions are only supported in the top-level SELECT. \
A window function inside a subquery or derived table cannot be incrementally \
maintained — move it to the outermost SELECT, or define this view with kind: mv.",
            ));
        }
        return None;
    }
    let decomp = window::decompose_window_query(&parsed.analysis);

    // Create a sub-IMV for the base query (aggregate or passthrough, no windows)
    let base_name = format!("{}__base", view_name);
    let result = create_reflex_ivm_impl(
        &base_name,
        &decomp.base_query,
        unique_columns_str,
        false,
        storage_mode,
        refresh_mode,
        topk_k,
        ignore_sources,
        partition_by,
    );
    if result.starts_with("ERROR") {
        return Some(result);
    }

    // Create a VIEW that applies window functions to the base sub-IMV
    let view_sql = format!(
        "SELECT {} FROM {}",
        decomp.view_select,
        quote_identifier(&base_name)
    );
    Spi::connect_mut(|client| {
        client
            .update(
                &format!(
                    "CREATE OR REPLACE VIEW {} AS {}",
                    quote_identifier(view_name),
                    view_sql
                ),
                None,
                &[],
            )
            .unwrap_or_report();

        // Register in reference table for cleanup
        let depends_on = vec![base_name.clone()];
        let depends_on_imv = vec![base_name.clone()];
        insert_registry_row(
            client,
            &RegistryRow::decomposed(
                view_name,
                2,
                &depends_on,
                &depends_on_imv,
                sql,
                &view_sql,
                &parsed.storage_upper,
                &parsed.mode_upper,
            ),
        )
        .unwrap_or_report();
        add_graph_child_links(client, view_name, &depends_on_imv).unwrap_or_report();
    });

    Some("CREATE REFLEX INCREMENTAL VIEW")
}

/// Decomposition phase: `WITH ... ` (Common Table Expressions).
///
/// Each CTE becomes its own sub-IMV (recursively) and the main body is
/// rewritten to reference those sub-IMVs in place of the original CTE
/// aliases, then re-entered through [`create_reflex_ivm_impl`] without the
/// Split the `unique_columns` argument into the outer view's key and an optional
/// per-CTE key map. The extended form is
/// `'<outer cols> ; <cte alias> : <cols> ; <cte alias2> : <cols>'` — segments
/// separated by `;`, each per-CTE segment binding a CTE alias to its columns
/// with `:`. A plain comma list (no `;`) is unchanged and yields an empty map.
/// CTE aliases match case-insensitively.
fn parse_unique_columns_spec(spec: &str) -> (String, std::collections::HashMap<String, String>) {
    let mut segments = spec.split(';');
    let outer = segments.next().unwrap_or("").trim().to_string();
    let mut cte_map = std::collections::HashMap::new();
    for seg in segments {
        if let Some((alias, cols)) = seg.split_once(':') {
            let alias = alias.trim().to_lowercase();
            let cols = cols.trim().to_string();
            if !alias.is_empty() && !cols.is_empty() {
                cte_map.insert(alias, cols);
            }
        }
    }
    (outer, cte_map)
}

/// WITH clause. Tail-recursive: the final `Some(...)` here is the result of
/// the rewritten-body call.
#[allow(clippy::too_many_arguments)]
fn try_decompose_ctes(ctx: &DecomposeCtx) -> Option<&'static str> {
    let analysis = &ctx.parsed.analysis;
    if analysis.ctes.is_empty() {
        return None;
    }

    // PRE-SCAN: Parse and analyze each CTE exactly once.
    // (1) Check for top-level window function or DISTINCT ON, rejecting early before
    //     creating any orphan objects.
    // (2) Extract output column names for partition propagation in the creation loop.
    //
    // For each CTE, store `None` if it has wildcard projection (cannot determine columns),
    // or `Some(Vec<String>)` if output columns are known. Store separately for quick lookup.
    let mut cte_output_columns: Vec<Option<Vec<String>>> = Vec::new();

    for cte in &analysis.ctes {
        let dialect = PostgreSqlDialect {};
        match Parser::parse_sql(&dialect, &cte.query_sql) {
            Ok(parsed_cte_stmts) => {
                match analyze(&parsed_cte_stmts) {
                    Ok(cte_analysis) => {
                        // Check for top-level window function
                        let has_top_level_window =
                            cte_analysis.select_columns.iter().any(|c| c.is_window);
                        if has_top_level_window {
                            return Some(crate::reflex_reject(
                                "A CTE uses a window function at the top level and is \
referenced by an outer query. A window-function result is a read-time view that cannot be \
incrementally maintained as a join source. Move the window function to the outermost \
SELECT, or define this view with kind: mv.",
                            ));
                        }

                        // Check for DISTINCT ON
                        if cte_analysis.has_distinct_on {
                            return Some(crate::reflex_reject(
                                "A CTE uses DISTINCT ON at the top level and is referenced \
by an outer query. A DISTINCT-ON result is a read-time view that cannot be incrementally \
maintained as a join source. Move DISTINCT ON to the outermost SELECT, or define this view \
with kind: mv.",
                            ));
                        }

                        // Extract output column names from the CTE's analysis.
                        // If the CTE has a wildcard projection (*, table.*), we cannot
                        // determine output columns, so record None.
                        let mut has_wildcard = false;
                        let mut output_cols = Vec::new();

                        for select_col in &cte_analysis.select_columns {
                            // Check if this is a wildcard projection
                            if select_col.expr_sql == "*" || select_col.expr_sql.ends_with(".*") {
                                has_wildcard = true;
                                break;
                            }

                            // Determine the output column name
                            let col_name = if let Some(alias) = &select_col.alias {
                                alias.to_lowercase()
                            } else {
                                bare_column_name(&select_col.expr_sql).to_lowercase()
                            };
                            output_cols.push(col_name);
                        }

                        let output = if has_wildcard || output_cols.is_empty() {
                            None
                        } else {
                            Some(output_cols)
                        };
                        cte_output_columns.push(output);
                    }
                    Err(_) => {
                        // If analysis fails, treat output columns as unknown.
                        // This skips partition propagation (safe behavior).
                        cte_output_columns.push(None);
                    }
                }
            }
            Err(_) => {
                // If parsing fails, treat output columns as unknown.
                // The normal creation path will surface the error.
                cte_output_columns.push(None);
            }
        }
    }

    let mut cte_name_map: Vec<(String, String)> = Vec::new();

    for (cte_idx, cte) in analysis.ctes.iter().enumerate() {
        let alias_lower = cte.alias.to_lowercase();
        if alias_lower.starts_with("__reflex_new_")
            || alias_lower.starts_with("__reflex_old_")
            || alias_lower.starts_with("__reflex_delta_")
        {
            rollback_partial_sub_imvs(&created_sub_imv_names(&cte_name_map));
            return Some(crate::reflex_reject(
                "CTE alias conflicts with pg_reflex reserved prefix (__reflex_new_/old_/delta_)",
            ));
        }

        // Rewrite references to earlier CTEs in this CTE's query
        let mut cte_query = cte.query_sql.clone();
        for (earlier_alias, earlier_imv) in &cte_name_map {
            let quoted = quote_identifier(earlier_imv);
            cte_query = replace_identifier(&cte_query, earlier_alias, &quoted);
        }

        // Compute partition subset using the pre-computed output columns.
        // If output columns are unknown, pass &[] to skip partition propagation.
        let cte_partition_by = if let Some(cte_output_cols) = &cte_output_columns[cte_idx] {
            compute_cte_partition_subset(ctx.partition_by, cte_output_cols)
        } else {
            // Wildcard projection or analysis failed — cannot propagate partitioning
            Vec::new()
        };

        // Note: nested CTEs (CTE body containing WITH) re-enter try_decompose_ctes with
        // view_name = "<view>__cte_<cte_alias>", producing names like "<view>__cte_a__cte_b".
        // A sibling CTE literally named "a__cte_b" would collide with nested CTE "b" inside "a",
        // but this is a pathological edge case requiring an adversarial alias — accepted risk.
        let cte_view_name = safe_identifier(&format!("{}__cte_{}", ctx.view_name, cte.alias));
        let cte_key = ctx
            .cte_unique_columns
            .get(&alias_lower)
            .map(|s| s.as_str())
            .unwrap_or("");
        // Sub-IMVs born of CTE decomposition are joined back into the outer
        // query and are themselves trigger sources for it. Anything they would
        // otherwise emit as a VIEW (UNION ALL today) must instead be a TABLE so
        // PostgreSQL can install the consumer's transition-table triggers on
        // them. Force materialisation here, regardless of the parent's setting.
        let result = create_reflex_ivm_impl_with_materialization(
            &cte_view_name,
            &cte_query,
            cte_key,
            false,
            ctx.storage_mode,
            ctx.refresh_mode,
            ctx.topk_k,
            ctx.ignore_sources,
            &cte_partition_by,
            true,
        );
        if result.starts_with("ERROR") {
            rollback_partial_sub_imvs(&created_sub_imv_names(&cte_name_map));
            return Some(result);
        }
        cte_name_map.push((cte.alias.clone(), cte_view_name));
    }

    // Rewrite main query body: serialize without WITH, replace CTE names
    let body_sql = if let sqlparser::ast::Statement::Query(ref query) = ctx.parsed.parsed_sql[0] {
        let mut body = query.body.to_string();
        // Append ORDER BY / LIMIT if present (shouldn't be for valid IMV queries)
        if let Some(ref ob) = query.order_by {
            body = format!("{} {}", body, ob);
        }
        for (cte_alias, cte_imv_name) in &cte_name_map {
            let quoted = quote_identifier(cte_imv_name);
            body = replace_identifier(&body, cte_alias, &quoted);
        }
        body
    } else {
        rollback_partial_sub_imvs(&created_sub_imv_names(&cte_name_map));
        return Some(crate::reflex_reject("Query is not a SELECT"));
    };

    // Check if the main body is passthrough (no aggregation).
    // If so, all its sources are CTE sub-IMVs which don't get triggers,
    // CTE body (passthrough or aggregate) → create as a normal IMV.
    // Preserve `materialize_as_table` so the outer body itself ends up as a
    // TABLE when this CTE-decomposed IMV is itself an intermediate sub-IMV.
    let body_result = create_reflex_ivm_impl_with_materialization(
        ctx.view_name,
        &body_sql,
        ctx.unique_columns_str,
        false,
        ctx.storage_mode,
        ctx.refresh_mode,
        ctx.topk_k,
        ctx.ignore_sources,
        ctx.partition_by,
        ctx.materialize_as_table,
    );
    if body_result.starts_with("ERROR") {
        rollback_partial_sub_imvs(&created_sub_imv_names(&cte_name_map));
    }
    Some(body_result)
}

/// The materialised sub-IMV names from a CTE decomposition's `(alias, name)` map,
/// in creation order — the input to [`rollback_partial_sub_imvs`].
fn created_sub_imv_names(cte_name_map: &[(String, String)]) -> Vec<String> {
    cte_name_map.iter().map(|(_, name)| name.clone()).collect()
}

/// For passthrough IMVs, resolve the unique-key column set: explicit
/// `unique_columns_str` if non-empty, else probe single-source PKs from
/// pg_index. Multi-source JOINs without an explicit key fall back to
/// full refresh on DELETE/UPDATE (warned). Populates
/// `ctx.resolved_unique_columns` and `ctx.plan.passthrough_columns` /
/// `ctx.plan.passthrough_key_mappings`.
fn resolve_unique_columns(ctx: &mut BuildContext) {
    if !ctx.plan.is_passthrough {
        return;
    }

    if !ctx.unique_columns_str.is_empty() {
        // Explicit unique columns from 3rd parameter
        ctx.resolved_unique_columns = ctx
            .unique_columns_str
            .split(',')
            .map(|s| normalized_column_name(s.trim()))
            .filter(|s| !s.is_empty())
            .collect();
        ctx.plan.passthrough_columns = ctx.resolved_unique_columns.clone();
        info!(
            "pg_reflex: using explicit unique key ({}) for '{}'",
            ctx.resolved_unique_columns.join(", "),
            ctx.view_name
        );

        // Build per-source-table column mappings
        let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
        build_passthrough_key_mappings(
            &mut ctx.plan,
            &ctx.resolved_unique_columns,
            &real_sources,
            &ctx.analysis,
        );
    } else if !ctx.is_join_query {
        // Auto-detect: only for single-source queries (JOINs need explicit key)
        let select_bare_names: std::collections::HashSet<String> = ctx
            .analysis
            .select_columns
            .iter()
            .map(|c| {
                let name = c.alias.as_deref().unwrap_or(&c.expr_sql);
                bare_column_name(name).to_lowercase()
            })
            .collect();

        let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
        for source in &real_sources {
            let pk_cols: Vec<String> = Spi::connect(|client| {
                client
                    .select(
                        "SELECT array_agg(a.attname ORDER BY k.n) as cols \
                         FROM pg_index ix \
                         JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, n) ON true \
                         JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.col \
                         WHERE ix.indrelid = to_regclass($1) \
                           AND ix.indisunique AND ix.indisprimary \
                         GROUP BY ix.indexrelid \
                         ORDER BY count(*) \
                         LIMIT 1",
                        None,
                        &[unsafe {
                            DatumWithOid::new(
                                source.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        }],
                    )
                    .unwrap_or_report()
                    .filter_map(|row| row.get_by_name::<Vec<String>, _>("cols").unwrap_or(None))
                    .next()
                    .unwrap_or_default()
            });

            if !pk_cols.is_empty() {
                let pk_lower: Vec<String> = pk_cols.iter().map(|c| c.to_lowercase()).collect();
                let all_in_select = pk_lower.iter().all(|c| select_bare_names.contains(c));
                if all_in_select {
                    ctx.resolved_unique_columns = pk_lower;
                    ctx.plan.passthrough_columns = ctx.resolved_unique_columns.clone();
                    // Single source: 1:1 mapping (target col == source col)
                    ctx.plan.passthrough_key_mappings.insert(
                        source.to_string(),
                        ctx.resolved_unique_columns
                            .iter()
                            .map(|c| (c.clone(), c.clone()))
                            .collect(),
                    );
                    info!(
                        "pg_reflex: auto-detected PK ({}) from '{}' for '{}'",
                        ctx.resolved_unique_columns.join(", "),
                        source,
                        ctx.view_name
                    );
                    break;
                } else {
                    info!(
                        "pg_reflex: source '{}' has PK ({}) but the SELECT list does not include all PK columns — \
                         passthrough '{}' will fall back to row-matching for DELETE/UPDATE. \
                         Add the PK columns to the SELECT list, or pass them as the 3rd argument to create_reflex_ivm.",
                        source,
                        pk_lower.join(", "),
                        ctx.view_name
                    );
                }
            }
        }
    } else if let Some(inferred) = infer_join_passthrough_unique_key(ctx) {
        // JOIN passthrough with a structurally-sound unique key (anchor PK
        // preserved through to-one equi-joins).
        ctx.resolved_unique_columns = inferred;
        ctx.plan.passthrough_columns = ctx.resolved_unique_columns.clone();
        let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
        build_passthrough_key_mappings(
            &mut ctx.plan,
            &ctx.resolved_unique_columns,
            &real_sources,
            &ctx.analysis,
        );
        info!(
            "pg_reflex: inferred unique key ({}) for JOIN passthrough '{}'",
            ctx.resolved_unique_columns.join(", "),
            ctx.view_name
        );
    } else {
        // JOIN query without a provable key: fall back to full refresh on DELETE/UPDATE
        info!(
            "pg_reflex: JOIN passthrough '{}' has no unique key. \
             Provide 3rd argument to create_reflex_ivm for incremental DELETE/UPDATE. \
             Example: SELECT create_reflex_ivm('{}', '...', 'col1,col2')",
            ctx.view_name, ctx.view_name
        );
    }
}

fn populate_source_join_keys(ctx: &mut BuildContext) {
    if ctx.plan.is_passthrough || !ctx.is_join_query {
        return;
    }
    let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
    build_source_join_keys(&mut ctx.plan, &real_sources, &ctx.analysis);
}

/// Warn on SELECT entries that are neither GROUP BY nor recognized aggregates.
/// These columns will be missing from the IMV (silent data loss is worse than a warning).
fn validate_select_columns(ctx: &BuildContext) {
    if ctx.plan.is_passthrough {
        return;
    }
    let group_by_set: std::collections::HashSet<&str> = ctx
        .plan
        .group_by_columns
        .iter()
        .map(|s| s.as_str())
        .collect();
    for col in &ctx.analysis.select_columns {
        if !col.is_passthrough && col.aggregate.is_none() && !col.is_aggregate_derived {
            warning!(
                "pg_reflex: unsupported expression '{}' in SELECT — column will be missing from IMV '{}'",
                col.alias.as_deref().unwrap_or(&col.expr_sql),
                ctx.view_name
            );
        } else if col.is_passthrough
            && !group_by_set.contains(col.expr_sql.as_str())
            && !ctx.analysis.has_distinct
        {
            let bare = bare_column_name(&col.expr_sql);
            let in_gb = group_by_set.iter().any(|gb| bare_column_name(gb) == bare);
            if !in_gb {
                warning!(
                    "pg_reflex: expression '{}' not in GROUP BY and not a recognized aggregate — column will be missing from IMV '{}'",
                    col.expr_sql,
                    ctx.view_name
                );
            }
        }
    }
}

/// Pre-flight checks: reject duplicate view name (or skip-noop on
/// `if_not_exists`), detect cycles in the IMV dependency DAG. Returns the
/// short-circuit string when the create should stop, `None` to continue.
fn check_existence_and_cycle(ctx: &BuildContext) -> Option<&'static str> {
    let already_exists = Spi::connect(|client| {
        !client
            .select(
                "SELECT 1 FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        ctx.view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>()
            .is_empty()
    });
    if already_exists {
        if ctx.if_not_exists {
            return Some("REFLEX INCREMENTAL VIEW ALREADY EXISTS (skipped)");
        }
        return Some(crate::reflex_reject("IMV with this name already exists"));
    }

    let cycle_detected = if ctx.froms.is_empty() {
        false
    } else {
        Spi::connect(|client| {
            let args = [
                unsafe {
                    DatumWithOid::new(
                        format_pg_text_array_literal(&ctx.froms),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                },
                unsafe {
                    DatumWithOid::new(
                        ctx.view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                },
            ];
            !client
                .select(
                    "WITH RECURSIVE dep_graph(dep) AS (\
                        SELECT unnest(depends_on) \
                        FROM public.__reflex_ivm_reference \
                        WHERE name = ANY($1::TEXT[]) \
                        UNION \
                        SELECT unnest(r.depends_on) \
                        FROM dep_graph dg \
                        JOIN public.__reflex_ivm_reference r ON r.name = dg.dep \
                    ) \
                    SELECT 1 FROM dep_graph WHERE dep = $2 LIMIT 1",
                    Some(1),
                    &args,
                )
                .unwrap_or_report()
                .collect::<Vec<_>>()
                .is_empty()
        })
    };
    if cycle_detected {
        return Some(crate::reflex_reject(
            "circular dependency detected — this IMV would form a cycle in the dependency graph",
        ));
    }
    None
}

/// Resolve `partition_by`: validate explicit columns against plan + anchor, or
/// auto-mirror from the single partitioned source. Populates
/// `ctx.resolved_partition_cols` and `ctx.resolved_strategy`. Returns error
/// message string on validation failure (caller re-leaks).
fn resolve_partitioning(ctx: &mut BuildContext) -> Result<(), String> {
    ctx.resolved_partition_cols = ctx.partition_by.to_vec();

    if !ctx.resolved_partition_cols.is_empty() {
        // Explicit partition_by — validate against plan shape.
        if !ctx.plan.is_passthrough {
            let gb_lower: std::collections::HashSet<String> = ctx
                .plan
                .group_by_columns
                .iter()
                .map(|c| c.to_lowercase())
                .collect();
            // Also normalize qualified GROUP BY columns ("d.region" →
            // "region") so partition_by can name the bare column even
            // when the GROUP BY clause uses a `<table>.col` form.
            let gb_normalized: std::collections::HashSet<String> = ctx
                .plan
                .group_by_columns
                .iter()
                .map(|c| normalized_column_name(c).to_lowercase())
                .collect();
            let projected_aliases: std::collections::HashSet<String> = ctx
                .plan
                .group_by_aliases
                .values()
                .map(|v| v.to_lowercase())
                .collect();
            // Reverse map from alias → GROUP BY expression (the AST string
            // before aliasing).  Used to recover the underlying GROUP BY
            // expression when the user passes the alias in `partition_by`.
            let alias_to_gb_expr: std::collections::HashMap<String, String> = ctx
                .plan
                .group_by_aliases
                .iter()
                .map(|(k, v)| (v.to_lowercase(), k.clone()))
                .collect();
            for col in &ctx.resolved_partition_cols {
                let col_l = col.to_lowercase();
                if !gb_lower.contains(&col_l)
                    && !gb_normalized.contains(&col_l)
                    && !projected_aliases.contains(&col_l)
                {
                    return Err(crate::reflex_reject(&format!(
                        "partition_by column '{}' is not in GROUP BY; \
                         partition columns must be a subset of GROUP BY for aggregate IMVs",
                        col
                    ))
                    .to_string());
                }
                // Phase B (plans/partitioning_3.md §2): reject when the
                // matching GROUP BY entry is a computed expression rather
                // than a bare column reference.  We look up the AST string
                // that produced this `partition_by` column — either the
                // group_by_columns entry itself (when partition_by matches
                // the GROUP BY's lexical form) or, when partition_by names
                // an alias, the GROUP BY entry whose alias is `col`.
                let gb_expr: Option<String> = if gb_lower.contains(&col_l) {
                    ctx.plan
                        .group_by_columns
                        .iter()
                        .find(|gb| gb.to_lowercase() == col_l)
                        .cloned()
                } else if gb_normalized.contains(&col_l) {
                    ctx.plan
                        .group_by_columns
                        .iter()
                        .find(|gb| normalized_column_name(gb).to_lowercase() == col_l)
                        .cloned()
                } else {
                    alias_to_gb_expr.get(&col_l).cloned()
                };
                if let Some(ref gb) = gb_expr {
                    if !crate::sql_analyzer::is_bare_column_reference(gb) {
                        return Err(crate::reflex_reject(&format!(
                            "partition_by column '{}' corresponds to a computed \
                             GROUP BY expression ('{}'). Partition columns must be bare \
                             column references on the source. Workaround: add a generated \
                             / computed column to the source and partition on that.",
                            col, gb
                        ))
                        .to_string());
                    }
                }
            }
        }
        let validate_result: Result<String, String> = Spi::connect(|client| {
            let anchor = crate::partition::resolve_anchor_source(
                client,
                &ctx.resolved_partition_cols[0],
                &ctx.real_source_names,
            )?;
            let desc = crate::partition::introspect_partition_descriptor(client, &anchor)
                .ok_or_else(|| {
                    format!(
                        "anchor source '{}' for column '{}' is not partitioned LIST/RANGE",
                        anchor, ctx.resolved_partition_cols[0]
                    )
                })?;
            let part_col_l = ctx.resolved_partition_cols[0].to_lowercase();
            if !desc.column_names.iter().any(|c| c == &part_col_l) {
                return Err(format!(
                    "anchor source '{}' is partitioned but not on '{}' (partitioned on: {:?})",
                    anchor, ctx.resolved_partition_cols[0], desc.column_names
                ));
            }
            Ok(desc.strategy)
        });
        match validate_result {
            Ok(s) => ctx.resolved_strategy = s,
            Err(e) => {
                return Err(crate::reflex_reject(&format!(
                    "partition_by validation failed — {}",
                    e
                ))
                .to_string());
            }
        }
    } else {
        // Phase 5: auto-mirror when exactly one real source is partitioned.
        let auto: (Vec<String>, String) = Spi::connect(|client| {
            let mut partitioned_sources: Vec<(String, crate::partition::PartitionDescriptor)> =
                Vec::new();
            for s in &ctx.real_source_names {
                if let Some(desc) = crate::partition::introspect_partition_descriptor(client, s) {
                    partitioned_sources.push((s.clone(), desc));
                }
            }
            if partitioned_sources.len() != 1 {
                return (Vec::new(), String::new());
            }
            let (_, desc) = partitioned_sources.into_iter().next().unwrap();
            let part_col = desc.column_names.first().cloned().unwrap_or_default();
            if part_col.is_empty() {
                return (Vec::new(), String::new());
            }
            if ctx.plan.is_passthrough {
                let pt_cols_l: std::collections::HashSet<String> = ctx
                    .plan
                    .passthrough_columns
                    .iter()
                    .map(|c| c.to_lowercase())
                    .collect();
                let projected: std::collections::HashSet<String> = ctx
                    .analysis
                    .select_columns
                    .iter()
                    .map(|c| {
                        let n = c.alias.as_deref().unwrap_or(&c.expr_sql);
                        bare_column_name(n).to_lowercase()
                    })
                    .collect();
                if pt_cols_l.contains(&part_col) || projected.contains(&part_col) {
                    return (vec![part_col], desc.strategy);
                }
            } else {
                let gb_lower: std::collections::HashSet<String> = ctx
                    .plan
                    .group_by_columns
                    .iter()
                    .map(|c| c.to_lowercase())
                    .collect();
                let projected_aliases: std::collections::HashSet<String> = ctx
                    .plan
                    .group_by_aliases
                    .values()
                    .map(|v| v.to_lowercase())
                    .collect();
                if gb_lower.contains(&part_col) || projected_aliases.contains(&part_col) {
                    return (vec![part_col], desc.strategy);
                }
            }
            (Vec::new(), String::new())
        });
        ctx.resolved_partition_cols = auto.0;
        ctx.resolved_strategy = auto.1;
        if !ctx.resolved_partition_cols.is_empty() {
            info!(
                "pg_reflex: auto-mirroring partition column '{}' from source",
                ctx.resolved_partition_cols[0]
            );
        }
    }

    Ok(())
}

/// Resolve existing IMV dependencies among `ctx.froms` and compute graph_depth.
/// Populates `ctx.ivm_froms` and `ctx.depth`.
fn resolve_existing_imv_deps(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
    let args = [unsafe {
        DatumWithOid::new(
            format_pg_text_array_literal(&ctx.froms),
            PgBuiltInOids::TEXTOID.oid().value(),
        )
    }];

    let matching_froms = client
        .select(
            "SELECT name, graph_depth FROM public.__reflex_ivm_reference WHERE name = ANY($1::TEXT[])",
            None,
            &args,
        )
        .unwrap_or_report()
        .collect::<Vec<_>>();

    ctx.ivm_froms = matching_froms
        .iter()
        .filter_map(|row| row.get_by_name::<&str, _>("name").unwrap_or(None))
        .map(|s| s.to_string())
        .collect();

    ctx.depth = matching_froms
        .iter()
        .filter_map(|row| row.get_by_name::<i32, _>("graph_depth").unwrap_or(None))
        .max()
        .unwrap_or(0)
        + 1;
}

/// Stage partition + anchor + partition_join_paths fields onto `ctx.plan`.
/// Mutates: `ctx.plan.partition_columns`, `partition_strategy`, `anchor_source`,
/// `partition_join_paths`.
fn apply_partition_plan(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
    ctx.plan.partition_columns = ctx.resolved_partition_cols.clone();
    ctx.plan.partition_strategy = ctx.resolved_strategy.clone();
    ctx.plan.anchor_source = if ctx.resolved_partition_cols.is_empty() {
        String::new()
    } else {
        crate::partition::resolve_anchor_source(
            client,
            &ctx.resolved_partition_cols[0],
            &ctx.real_source_names,
        )
        .unwrap_or_default()
    };
    if !ctx.plan.partition_columns.is_empty() && !ctx.plan.anchor_source.is_empty() {
        let partition_col = ctx.plan.partition_columns[0].to_lowercase();
        let anchor = ctx.plan.anchor_source.clone();
        let anchor_quoted = quote_identifier(&anchor);
        for (source, mappings) in &ctx.plan.source_join_keys.clone() {
            if source.eq_ignore_ascii_case(&anchor)
                || split_qualified_name(source)
                    .1
                    .eq_ignore_ascii_case(split_qualified_name(&anchor).1)
            {
                continue;
            }
            let matched_source_col = mappings
                .iter()
                .find(|(ic, _)| ic.eq_ignore_ascii_case(&partition_col))
                .map(|(_, sc)| sc.clone());
            if let Some(source_col) = matched_source_col {
                let fragment = format!(
                    "SELECT a.\"{pc}\" AS pkey, t.* \
                     FROM {{transition_alias}} t \
                     JOIN {anchor} a ON a.\"{pc}\" = t.\"{sc}\"",
                    pc = partition_col,
                    anchor = anchor_quoted,
                    sc = source_col,
                );
                ctx.plan
                    .partition_join_paths
                    .insert(source.clone(), fragment);
            }
        }
    }
}

/// Drop `imv_relevant_columns` entries that don't exist on the source table.
/// Mutates: `ctx.plan.imv_relevant_columns`.
fn filter_imv_relevant_columns(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
    let (_t, _nn, per_source_cols_for_filter) =
        query_column_types_from_catalog_with_per_source(client, &ctx.froms);
    for (source, cols) in ctx.plan.imv_relevant_columns.iter_mut() {
        if let Some(actual) = per_source_cols_for_filter.get(source) {
            cols.retain(|c| actual.contains(c.as_str()));
        } else if source.starts_with('<') {
            cols.clear();
        }
    }
    ctx.plan.imv_relevant_columns.retain(|_, v| !v.is_empty());
}

/// Passthrough materialization: CREATE TABLE AS (or partitioned variant),
/// ANALYZE, unique-index, scratch tables.
fn materialize_passthrough(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
    let is_partitioned = !ctx.plan.partition_columns.is_empty();
    // Partitioned parents are never UNLOGGED — PG18 rejects it and pre-PG18
    // ignored it (children carry UNLOGGED via `build_partition_child_ddl_pair`).
    let create_kw = if ctx.logged || is_partitioned {
        "CREATE TABLE"
    } else {
        "CREATE UNLOGGED TABLE"
    };
    if is_partitioned {
        let scratch_name = format!(
            "__reflex_pt_shape_{}",
            safe_identifier(split_qualified_name(ctx.view_name).1)
        );
        client
            .update(
                &format!(
                    "CREATE TEMP TABLE {} AS {} WITH NO DATA",
                    scratch_name, ctx.sql
                ),
                None,
                &[],
            )
            .unwrap_or_report();
        let col_defs: Vec<String> = client
            .select(
                "SELECT attname::text AS name, \
                        format_type(atttypid, atttypmod) AS pg_type \
                 FROM pg_attribute \
                 WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped \
                 ORDER BY attnum",
                None,
                &[unsafe {
                    DatumWithOid::new(scratch_name.clone(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|r| {
                let n = r.get_by_name::<&str, _>("name").ok()??.to_string();
                let t = r.get_by_name::<&str, _>("pg_type").ok()??.to_string();
                Some(format!("\"{}\" {}", n, t))
            })
            .collect();
        client
            .update(&format!("DROP TABLE {}", scratch_name), None, &[])
            .unwrap_or_report();
        let part_clause = crate::partition::build_partition_by_clause(
            &ctx.plan.partition_strategy,
            &ctx.plan.partition_columns,
        );
        client
            .update(
                &format!(
                    "{} IF NOT EXISTS {} ({}) {}",
                    create_kw,
                    quote_identifier(ctx.view_name),
                    col_defs.join(", "),
                    part_clause
                ),
                None,
                &[],
            )
            .unwrap_or_report();
        if let Ok(anchor) = crate::partition::resolve_anchor_source(
            client,
            &ctx.plan.partition_columns[0],
            &ctx.real_source_names,
        ) {
            let src_children = crate::partition::list_partition_children(client, &anchor);
            for src_child in &src_children {
                let (_, tgt_ddl) = crate::partition::build_partition_child_ddl_pair(
                    ctx.view_name,
                    src_child,
                    !ctx.logged,
                );
                client.update(&tgt_ddl, None, &[]).unwrap_or_report();
            }
        }
        client
            .update(
                &format!(
                    "INSERT INTO {} {}",
                    quote_identifier(ctx.view_name),
                    ctx.sql
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    } else {
        client
            .update(
                &format!(
                    "{} {} AS {}",
                    create_kw,
                    quote_identifier(ctx.view_name),
                    ctx.sql
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    }
    client
        .update(
            &format!("ANALYZE {}", quote_identifier(ctx.view_name)),
            None,
            &[],
        )
        .unwrap_or_report();

    if !ctx.resolved_unique_columns.is_empty() {
        let bare_view = split_qualified_name(ctx.view_name).1;
        let uk_cols: Vec<String> = ctx
            .resolved_unique_columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect();
        client
            .update(
                &format!(
                    "CREATE UNIQUE INDEX IF NOT EXISTS \"__reflex_uk_{}\" ON {} ({})",
                    bare_view,
                    quote_identifier(ctx.view_name),
                    uk_cols.join(", ")
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    }

    for source in &ctx.froms {
        if source.starts_with('<') {
            continue;
        }
        for ddl in build_passthrough_scratch_ddls(ctx.view_name, source) {
            client.update(&ddl, None, &[]).unwrap_or_report();
        }
    }
}

/// Aggregate materialization: catalog type discovery, intermediate + target + delta-scratch
/// DDL, partition children. Pushes intermediate table name onto `ctx.unlogged_tables`.
fn materialize_aggregate(client: &mut pgrx::spi::SpiClient<'_>, ctx: &mut BuildContext) {
    let (mut column_types, not_null_cols, per_source_cols) =
        query_column_types_from_catalog_with_per_source(client, &ctx.froms);
    ctx.plan.optimize_not_null_sums(&not_null_cols);
    for (source, cols) in ctx.plan.imv_relevant_columns.iter_mut() {
        if let Some(actual) = per_source_cols.get(source) {
            cols.retain(|c| actual.contains(c.as_str()));
        } else if source.starts_with('<') {
            cols.clear();
        }
    }
    ctx.plan.imv_relevant_columns.retain(|_, v| !v.is_empty());

    let base_q_for_types = generate_base_query(&ctx.analysis, &ctx.plan);
    augment_column_types_from_query(&base_q_for_types, &mut column_types);
    augment_column_types_from_query(ctx.sql, &mut column_types);

    for ic in &mut ctx.plan.intermediate_columns {
        if ic.source_aggregate == "SUM" {
            let base_type = resolve_column_type(&ic.name, &column_types, "").to_uppercase();
            if base_type == "DOUBLE PRECISION" {
                ic.pg_type = "DOUBLE PRECISION".to_string();
            }
        }
    }

    for ic in &mut ctx.plan.intermediate_columns {
        if (ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX")
            && ic.pg_type.eq_ignore_ascii_case("NUMERIC")
        {
            let resolved = resolve_column_type(&ic.source_arg, &column_types, "");
            if !resolved.is_empty() && !resolved.eq_ignore_ascii_case("NUMERIC") {
                ic.pg_type = resolved;
            }
        }
    }

    for mapping in &mut ctx.plan.end_query_mappings {
        if mapping.cast_type.is_none() {
            let discovered = resolve_column_type(&mapping.output_alias, &column_types, "");
            if !discovered.is_empty() {
                let default_type = match mapping.aggregate_type.as_str() {
                    "SUM" | "AVG" | "DERIVED" => "NUMERIC",
                    "COUNT" => "BIGINT",
                    "BOOL_OR" => "BOOLEAN",
                    _ => "",
                };
                if !default_type.is_empty() && discovered.to_uppercase() != default_type {
                    mapping.cast_type = Some(discovered);
                }
            }
        }
    }

    if let Some(ddl) =
        build_intermediate_table_ddl(ctx.view_name, &ctx.plan, &column_types, ctx.logged)
    {
        let tbl = intermediate_table_name(ctx.view_name);
        client.update(&ddl, None, &[]).unwrap_or_report();
        ctx.unlogged_tables.push(tbl.clone());
        if let Some(scratch_ddl) =
            build_delta_scratch_table_ddl(ctx.view_name, &ctx.plan, &column_types)
        {
            client.update(&scratch_ddl, None, &[]).unwrap_or_report();
        }
    }

    let target_ddl = build_target_table_ddl(ctx.view_name, &ctx.plan, &column_types, ctx.logged);
    client.update(&target_ddl, None, &[]).unwrap_or_report();

    if !ctx.plan.partition_columns.is_empty() {
        match crate::partition::resolve_anchor_source(
            client,
            &ctx.plan.partition_columns[0],
            &ctx.real_source_names,
        ) {
            Ok(anchor) => {
                let src_children = crate::partition::list_partition_children(client, &anchor);
                info!(
                    "pg_reflex: creating {} partition children for '{}' (anchor='{}')",
                    src_children.len(),
                    ctx.view_name,
                    anchor
                );
                for src_child in &src_children {
                    let (int_ddl, tgt_ddl) = crate::partition::build_partition_child_ddl_pair(
                        ctx.view_name,
                        src_child,
                        !ctx.logged,
                    );
                    client.update(&int_ddl, None, &[]).unwrap_or_report();
                    client.update(&tgt_ddl, None, &[]).unwrap_or_report();
                }
            }
            Err(e) => {
                warning!(
                    "pg_reflex: could not resolve anchor for '{}' partition children: {}",
                    ctx.view_name,
                    e
                );
            }
        }
    }
}

/// Install consolidated triggers on every real source of the IMV. Skips
/// `<subquery:...>`/`<function:...>` placeholders, ignored sources, and
/// materialized-view sources. Upgrades existing triggers to deferred when
/// any deferred IMV depends on the source.
fn install_source_triggers(client: &mut pgrx::spi::SpiClient<'_>, ctx: &BuildContext) {
    for source in &ctx.froms {
        if source.starts_with("<subquery:") || source.starts_with("<function:") {
            warning!(
                "pg_reflex: source '{}' for '{}' is a subquery — \
                 triggers are created on the underlying tables inside the subquery, \
                 but the subquery itself is re-executed on each delta",
                source,
                ctx.view_name
            );
            continue;
        }
        if source.starts_with('<') {
            continue;
        }

        let (_, source_bare) = split_qualified_name(source);
        if ctx
            .ignore_sources
            .iter()
            .any(|s| s == source || s == source_bare)
        {
            info!(
                "pg_reflex: skipping trigger install on source '{}' for IMV '{}' (ignored)",
                source, ctx.view_name
            );
            continue;
        }

        let is_matview = client
            .select(
                "SELECT 1 FROM pg_class WHERE oid = to_regclass($1) AND relkind = 'm'",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .next()
            .is_some();

        if is_matview {
            warning!(
                "pg_reflex: source '{}' is a materialized view — triggers skipped. \
                 Use SELECT refresh_imv_depending_on('{}') after REFRESH MATERIALIZED VIEW.",
                source,
                source
            );
            continue;
        }

        let safe_source = crate::query_decomposer::sanitized_source_suffix(source);
        let trig_exists = client
            .select(
                &format!(
                    "SELECT 1 FROM pg_trigger WHERE tgname = '__reflex_trigger_ins_on_{}'",
                    safe_source
                ),
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .is_some();

        if ctx.deferred {
            ensure_staging_matches_source(client, source);
            let staging_ddl = build_staging_table_ddl(source);
            client.update(&staging_ddl, None, &[]).unwrap_or_report();
        }

        if !trig_exists {
            let has_any_deferred = ctx.deferred
                || {
                    let check = client
                    .select(
                        &format!(
                            "SELECT 1 FROM public.__reflex_ivm_reference \
                             WHERE '{}' = ANY(depends_on) AND refresh_mode = 'DEFERRED' AND enabled = TRUE",
                            source.replace("'", "''")
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report()
                    .next()
                    .is_some();
                    check
                };

            if has_any_deferred {
                let cols = fetch_source_columns(client, source);
                for ddl in build_deferred_trigger_ddls(source, &cols) {
                    client.update(&ddl, None, &[]).unwrap_or_report();
                }
            } else {
                for ddl in build_trigger_ddls(source) {
                    client.update(&ddl, None, &[]).unwrap_or_report();
                }
            }
        } else if ctx.deferred {
            let cols = fetch_source_columns(client, source);
            for ddl in build_deferred_trigger_ddls(source, &cols) {
                client.update(&ddl, None, &[]).unwrap_or_report();
            }
        }
    }
}

/// Read the ordered list of live column names for `source_table` from
/// `pg_attribute`.  Used at deferred-trigger DDL time so the trigger body
/// can name the columns it stages instead of relying on `SELECT *`'s
/// positional binding (which broke when a per-source staging delta
/// outlived a source DROP+CREATE that reordered columns — see the 1.6.2
/// regression in `pg_test_deferred.rs`).
fn fetch_source_columns(client: &pgrx::spi::SpiClient<'_>, source_table: &str) -> Vec<String> {
    client
        .select(
            "SELECT attname::text AS name \
             FROM pg_attribute \
             WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped \
             ORDER BY attnum",
            None,
            &[unsafe {
                DatumWithOid::new(
                    source_table.to_string(),
                    PgBuiltInOids::TEXTOID.oid().value(),
                )
            }],
        )
        .unwrap_or_report()
        .filter_map(|row| {
            row.get_by_name::<&str, _>("name")
                .ok()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect()
}

/// Guard the create_ivm path against a pre-existing staging delta whose
/// column NAMES no longer agree with the source's live shape.  Two cases:
///
///   * staging column set == source column set (any order):
///     `LIKE source` would produce the same column NAMES, so the named-
///     column INSERT emitted by `build_deferred_trigger_ddls` will route
///     rows correctly.  Leave staging in place (it may carry pending
///     deferred deltas for other DEFERRED IMVs on this source).
///
///   * staging column set differs from source:
///     - if staging is empty → drop + let `build_staging_table_ddl`
///       recreate it from the current source shape.
///     - if staging holds rows → refuse with a clear error directing the
///       user to flush.  Dropping silently would lose other IMVs' staged
///       work; quietly proceeding would mean the new named-column INSERT
///       references columns that don't exist on staging.
fn ensure_staging_matches_source(client: &mut pgrx::spi::SpiClient<'_>, source_table: &str) {
    let staging_qual = crate::query_decomposer::staging_delta_table_name(source_table);
    let staging_exists = client
        .select(
            "SELECT 1 FROM pg_class WHERE oid = to_regclass($1)",
            None,
            &[unsafe {
                DatumWithOid::new(
                    staging_qual.replace('"', ""),
                    PgBuiltInOids::TEXTOID.oid().value(),
                )
            }],
        )
        .unwrap_or_report()
        .next()
        .is_some();
    if !staging_exists {
        return;
    }

    let source_cols: std::collections::HashSet<String> = fetch_source_columns(client, source_table)
        .into_iter()
        .collect();
    let staging_cols: std::collections::HashSet<String> = client
        .select(
            "SELECT attname::text AS name \
             FROM pg_attribute \
             WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped \
               AND attname <> '__reflex_op' \
             ORDER BY attnum",
            None,
            &[unsafe {
                DatumWithOid::new(
                    staging_qual.replace('"', ""),
                    PgBuiltInOids::TEXTOID.oid().value(),
                )
            }],
        )
        .unwrap_or_report()
        .filter_map(|row| {
            row.get_by_name::<&str, _>("name")
                .ok()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    if source_cols == staging_cols {
        return;
    }

    let has_rows = client
        .select(
            &format!("SELECT 1 FROM {} LIMIT 1", staging_qual),
            None,
            &[],
        )
        .unwrap_or_report()
        .next()
        .is_some();
    if has_rows {
        let added: Vec<&String> = source_cols.difference(&staging_cols).collect();
        let dropped: Vec<&String> = staging_cols.difference(&source_cols).collect();
        pgrx::error!(
            "pg_reflex: staging delta table {} is out of sync with source '{}' \
             (added columns: {:?}, removed columns: {:?}) and has pending deferred rows. \
             Run SELECT reflex_flush_deferred('{}') first (or DROP it manually if the rows \
             are no longer needed), then retry create_reflex_ivm.",
            staging_qual,
            source_table,
            added,
            dropped,
            source_table
        );
    }
    // CASCADE because reflex_flush_deferred installs per-session TEMP VIEWs
    // (`__reflex_new_<src>`, `__reflex_old_<src>`) against the staging delta;
    // they outlive the flush call and would otherwise block the DROP.
    client
        .update(&format!("DROP TABLE {} CASCADE", staging_qual), None, &[])
        .unwrap_or_report();
}

/// When the IMV uses deferred refresh, ensure the deferred-flush
/// infrastructure (function + per-source helpers) exists.
fn install_deferred_flush_if_needed(client: &mut pgrx::spi::SpiClient<'_>, ctx: &BuildContext) {
    if ctx.deferred {
        for ddl in build_deferred_flush_ddl() {
            client.update(&ddl, None, &[]).unwrap_or_report();
        }
    }
}

/// Source-side indexes on GROUP BY columns for MIN/MAX recompute performance.
/// Skips IMV sources (the IMV's intermediate has its own indexes) and
/// `<subquery>` placeholders. Only emits indexes for columns that exist on
/// the source table.
fn install_min_max_indexes(client: &mut pgrx::spi::SpiClient<'_>, ctx: &BuildContext) {
    let has_min_max = ctx
        .plan
        .intermediate_columns
        .iter()
        .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX");
    if !has_min_max || ctx.plan.group_by_columns.is_empty() {
        return;
    }
    for source in &ctx.froms {
        if source.starts_with('<') || ctx.ivm_froms.contains(source) {
            continue;
        }
        let (src_schema, src_name) = split_qualified_name(source);
        let src_schema_str = src_schema.unwrap_or("public");
        let source_cols: Vec<String> = client
            .select(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
                None,
                &[
                    unsafe { DatumWithOid::new(src_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(src_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<&str, _>("column_name").unwrap_or(None).map(|s| s.to_lowercase()))
            .collect();

        let idx_cols: Vec<String> = ctx
            .plan
            .group_by_columns
            .iter()
            .map(|c| normalized_column_name(c))
            .filter(|c| source_cols.contains(c))
            .map(|c| format!("\"{}\"", c))
            .collect();

        if idx_cols.is_empty() {
            continue;
        }
        let safe_src = source.replace('.', "_");
        let bare_view = split_qualified_name(ctx.view_name).1;
        let idx_name = safe_identifier(&format!("__reflex_idx_{}_{}", bare_view, safe_src));
        let ddl = format!(
            "CREATE INDEX IF NOT EXISTS \"{}\" ON {} ({})",
            idx_name,
            source,
            idx_cols.join(", ")
        );
        client.update(&ddl, None, &[]).unwrap_or_report();
    }
}

/// Compute base_query / end_query / aggregations_json / index_columns /
/// where_predicate, INSERT a RegistryRow into `__reflex_ivm_reference`, and
/// add this IMV's name to the `graph_child` array of each parent IMV.
fn persist_metadata(client: &mut SpiClient<'_>, ctx: &BuildContext) {
    let base_query = if ctx.plan.is_passthrough {
        ctx.sql.to_string()
    } else {
        generate_base_query(&ctx.analysis, &ctx.plan)
    };
    let end_query = if ctx.plan.is_passthrough {
        String::new()
    } else {
        generate_end_query(ctx.view_name, &ctx.plan)
    };
    let aggregations_json = generate_aggregations_json(&ctx.plan);
    let index_columns: Vec<String> = ctx
        .plan
        .group_by_columns
        .iter()
        .chain(ctx.plan.distinct_columns.iter())
        .map(|c| {
            if let Some(alias) = ctx.plan.group_by_aliases.get(c) {
                normalized_column_name(alias)
            } else {
                normalized_column_name(c)
            }
        })
        .collect();
    let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
    // The per-op early-skip evaluates `where_predicate` against the flat
    // transition table (sql/trigger_pred_check_*.plpgsql.in and the deferred
    // flush run `SELECT 1 FROM <transition> WHERE <where_predicate>`). The
    // transition table is NOT aliased as the source, so the predicate must use
    // bare column names — `imv_relevant_where` is exactly that alias-stripped
    // form. Storing the raw qualified `where_clause` (e.g. `src.amt > 50`)
    // instead errors at maintenance time with "missing FROM-clause entry for
    // table src". Only single-source IMVs carry a predicate, so the lone
    // entry is the full (possibly conservative) stripped predicate; an empty
    // string is coerced to NULL by the registry insert.
    let where_predicate: String = if real_sources.len() <= 1 {
        ctx.analysis
            .imv_relevant_where
            .values()
            .next()
            .cloned()
            .unwrap_or_default()
    } else {
        String::new()
    };
    let ignored_sources_vec: Vec<String> = ctx.ignore_sources.to_vec();
    let max_one_row = !ctx.plan.is_passthrough && ctx.plan.group_by_columns.is_empty();

    insert_registry_row(
        client,
        &RegistryRow {
            view_name: ctx.view_name,
            graph_depth: ctx.depth,
            depends_on: &ctx.froms,
            depends_on_imv: &ctx.ivm_froms,
            unlogged_tables: &ctx.unlogged_tables,
            graph_child: &[],
            sql_query: ctx.sql,
            base_query: &base_query,
            end_query: &end_query,
            aggregations_json: &aggregations_json,
            aggregations_cast: AggregationsCast::Jsonb,
            index_columns: &index_columns,
            unique_columns: &ctx.resolved_unique_columns,
            storage_mode: &ctx.storage_upper,
            refresh_mode: &ctx.mode_upper,
            where_predicate: Some(&where_predicate),
            ignored_sources: Some(&ignored_sources_vec),
            partition_columns: Some(&ctx.plan.partition_columns),
            partition_strategy: Some(&ctx.plan.partition_strategy),
            max_one_row,
        },
    )
    .unwrap_or_report();

    add_graph_child_links(client, ctx.view_name, &ctx.ivm_froms).unwrap_or_report();
}

/// For aggregate IMVs (skipped for passthrough — CREATE TABLE AS already
/// populated): execute the initial INSERT into intermediate + target, build
/// indexes, provision affected-groups + shrunk-groups tables, ANALYZE, and
/// data-probe NOT-NULL columns. Mutates `ctx.plan.not_null_columns` with
/// the probed additions.
fn initial_aggregate_materialization(client: &mut SpiClient<'_>, ctx: &mut BuildContext) {
    if ctx.plan.is_passthrough {
        return;
    }
    let intermediate_tbl = intermediate_table_name(ctx.view_name);
    let base_q = generate_base_query(&ctx.analysis, &ctx.plan);
    let initial_insert = format!("INSERT INTO {} {}", intermediate_tbl, base_q);
    client.update(&initial_insert, None, &[]).unwrap_or_report();

    let end_q = generate_end_query(ctx.view_name, &ctx.plan);
    let target_insert = format!("INSERT INTO {} {}", quote_identifier(ctx.view_name), end_q);
    client.update(&target_insert, None, &[]).unwrap_or_report();

    for index_ddl in build_indexes_ddl(ctx.view_name, &ctx.plan) {
        client.update(&index_ddl, None, &[]).unwrap_or_report();
    }

    // Create persistent affected-groups table (avoids DROP+CREATE per trigger fire).
    // Uses UNLOGGED for speed; lost on crash but rebuilt by reflex_reconcile.
    // Co-located in the IMV's schema (1.4.1) so SQL works under any `search_path`.
    if !ctx.plan.group_by_columns.is_empty() || !ctx.plan.distinct_columns.is_empty() {
        let group_cols_csv = ctx
            .plan
            .group_by_columns
            .iter()
            .chain(ctx.plan.distinct_columns.iter())
            .map(|c| format!("\"{}\"", normalized_column_name(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let affected_ref = affected_groups_table_name(ctx.view_name);
        client
            .update(
                &format!(
                    "CREATE UNLOGGED TABLE IF NOT EXISTS {} AS SELECT {} FROM {} WHERE FALSE",
                    affected_ref, group_cols_csv, intermediate_tbl
                ),
                None,
                &[],
            )
            .unwrap_or_report();

        // N1: per-IMV "shrunk groups" capture table — populated post-Sub
        // on UPDATE for top-K MIN/MAX IMVs to scope the forced recompute
        // to groups whose heap actually shrank below K. Provisioned only
        // when the plan has any top-K column; non-top-K IMVs leave it
        // unallocated.
        let has_topk = ctx.plan.intermediate_columns.iter().any(|ic| ic.has_topk());
        if has_topk {
            let shrunk_ref = shrunk_groups_table_name(ctx.view_name);
            client
                .update(
                    &format!(
                        "CREATE UNLOGGED TABLE IF NOT EXISTS {} AS SELECT {} FROM {} WHERE FALSE",
                        shrunk_ref, group_cols_csv, intermediate_tbl
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
        }
    }

    // ANALYZE so the query planner has accurate statistics
    client
        .update(&format!("ANALYZE {}", intermediate_tbl), None, &[])
        .unwrap_or_report();
    client
        .update(
            &format!("ANALYZE {}", quote_identifier(ctx.view_name)),
            None,
            &[],
        )
        .unwrap_or_report();

    // Structural NOT-NULL inference. Promote group-by / distinct columns that
    // are *provably* non-NULL from the query (INNER-join equi-keys or catalog
    // NOT-NULL base columns on a non-nullable side), so MERGE maintenance can
    // match keys with `=` instead of `IS NOT DISTINCT FROM` — the index-defeating
    // 405 s yse.ivm_sop_forecast_view regression in 1.4.4. Unlike the former
    // data-probe this never trusts transient null-freeness, so a later NULL on a
    // genuinely nullable / outer-join column cannot silently drop rows
    // (docs/fuzz-findings.md findings #1 and #3).
    let probed_nn = infer_not_null_columns(client, &ctx.plan, Some(&ctx.analysis));
    let new_cols: Vec<String> = probed_nn
        .into_iter()
        .filter(|c| !ctx.plan.not_null_columns.contains(c))
        .collect();
    if !new_cols.is_empty() {
        for c in &new_cols {
            ctx.plan.not_null_columns.insert(c.clone());
        }
        persist_probed_not_null_columns(client, ctx.view_name, &new_cols);
        info!(
            "pg_reflex: data-probe added {} effectively-NOT-NULL column(s) to '{}': {:?}",
            new_cols.len(),
            ctx.view_name,
            new_cols
        );
    }
}

/// Returns the first GROUP BY key that is not projected *bare* in the SELECT
/// list (it appears only inside an expression, e.g. `GROUP BY a.sx` with
/// `SELECT COALESCE(a.sx, 0)`, or is omitted from SELECT entirely).
///
/// Such a key becomes an intermediate-table column but has NO column in the
/// result (target) table, since the target carries only the projected outputs.
/// Every target-side operation — the target composite index and the
/// target-sync row matching (`target_group_columns`) — then references a column
/// that does not exist, surfacing as a cryptic `column "<key>" does not exist`
/// at create or a crash on the first incremental maintenance. Reject up front
/// with an actionable message instead. (A query that groups *by the expression*
/// — `GROUP BY DATE_TRUNC('month', ts)` projecting `DATE_TRUNC('month', ts)` —
/// is fine: the key is the expression and it is projected.)
fn first_unprojected_group_key(analysis: &crate::sql_analyzer::SqlAnalysis) -> Option<String> {
    for gb in &analysis.group_by_columns {
        let gb_norm = crate::query_decomposer::normalized_column_name(gb);
        let projected = analysis.select_columns.iter().any(|sc| {
            sc.is_passthrough
                && crate::query_decomposer::normalized_column_name(&sc.expr_sql) == gb_norm
        });
        if !projected {
            return Some(gb.clone());
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_reflex_ivm_impl(
    view_name: &str,
    sql: &str,
    unique_columns_str: &str,
    if_not_exists: bool,
    storage_mode: &str,
    refresh_mode: &str,
    topk_k: Option<usize>,
    ignore_sources: &[String],
    partition_by: &[String],
) -> &'static str {
    create_reflex_ivm_impl_with_materialization(
        view_name,
        sql,
        unique_columns_str,
        if_not_exists,
        storage_mode,
        refresh_mode,
        topk_k,
        ignore_sources,
        partition_by,
        false, // top-level call: zero-overhead VIEW wrappers are still fine
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_reflex_ivm_impl_with_materialization(
    view_name: &str,
    sql: &str,
    unique_columns_str: &str,
    if_not_exists: bool,
    storage_mode: &str,
    refresh_mode: &str,
    topk_k: Option<usize>,
    ignore_sources: &[String],
    partition_by: &[String],
    materialize_as_table: bool,
) -> &'static str {
    let parsed = match validate_and_parse_inputs(view_name, sql, storage_mode, refresh_mode) {
        Ok(p) => p,
        Err(msg) => return msg,
    };

    // The `unique_columns` argument may carry per-CTE keys after the outer key
    // (`'<outer> ; <cte alias> : <cols> ; ...'`). Strip them so only the outer
    // key reaches the non-CTE paths; the map is consumed by CTE decomposition.
    let (outer_unique_columns, cte_unique_columns) = parse_unique_columns_spec(unique_columns_str);
    let unique_columns_str = outer_unique_columns.as_str();

    if let Some(gb) = first_unprojected_group_key(&parsed.analysis) {
        return crate::reflex_reject(&format!(
            "GROUP BY key '{gb}' is not projected in the SELECT list. \
             An aggregate reflex IMV requires every GROUP BY column to appear bare in \
             SELECT — a key used only inside an expression (e.g. COALESCE({gb}, 0)) or \
             omitted from SELECT has no column in the result table, which the target \
             index and incremental refresh both rely on. Fix: add '{gb}' to the SELECT \
             list, or move the wrapping expression into a passthrough outer layer \
             (an outer SELECT over a CTE that projects the bare key + aggregates)."
        ));
    }

    // Build the decomposition context once, covering all fields needed by the three
    // decomposition functions. This ensures every path has access to every field
    // (including sql and cte_unique_columns, even if some functions don't use them).
    let dctx = DecomposeCtx {
        view_name,
        sql,
        unique_columns_str,
        cte_unique_columns: &cte_unique_columns,
        storage_mode,
        refresh_mode,
        topk_k,
        ignore_sources,
        partition_by,
        parsed: &parsed,
        materialize_as_table,
    };

    if let Some(result) = try_decompose_set_op(&dctx) {
        return result;
    }

    if let Some(result) = try_decompose_ctes(&dctx) {
        return result;
    }

    if let Some(result) = try_decompose_distinct_on(&dctx) {
        return result;
    }

    if let Some(result) = try_decompose_window(
        view_name,
        sql,
        unique_columns_str,
        storage_mode,
        refresh_mode,
        topk_k,
        ignore_sources,
        partition_by,
        &parsed,
    ) {
        return result;
    }

    // Reject subqueries with aggregation in FROM — the trigger replaces the inner table
    // with the transition table, so inner aggregations would only see delta rows.
    let has_subquery_with_agg = parsed
        .analysis
        .sources
        .iter()
        .any(|s| s.starts_with("<subquery:"))
        && parsed
            .analysis
            .from_clause_sql
            .to_uppercase()
            .contains("GROUP BY");
    if has_subquery_with_agg {
        return crate::reflex_reject("Subqueries with aggregation in FROM are not supported. \
                Use a CTE (WITH clause) instead — pg_reflex decomposes CTEs into sub-IMVs automatically.");
    }

    let ParsedInputs {
        logged,
        deferred,
        storage_upper,
        mode_upper,
        parsed_sql: _,
        mut analysis,
    } = parsed;

    // Resolve every bare source against the current `search_path` so the
    // identifiers stored in `__reflex_ivm_reference.depends_on`, baked into the
    // generated trigger bodies, and pushed into `__reflex_deferred_pending`
    // all carry the qualified `schema.table` form of the relation the trigger
    // is actually attached to. Without this, a bare `FROM foo` whose
    // search_path-resolved schema is e.g. `alp.foo` flows through unqualified;
    // `reflex_flush_deferred` (trigger.rs ~2885) later splits the schemaless
    // name, falls back to `nspname='public'`, and projects columns from a
    // public-side homonym, crashing the COMMIT-time flush.
    canonicalize_analysis_sources(&mut analysis);

    // COUNT(DISTINCT) mixed-with-other-aggregates check — must reject before
    // building the ctx.
    let has_cd = analysis.select_columns.iter().any(|c| {
        matches!(
            c.aggregate,
            Some(crate::sql_analyzer::AggregateKind::CountDistinct)
        )
    });
    let has_other_agg = analysis.select_columns.iter().any(|c| {
        matches!(c.aggregate, Some(ref k) if !matches!(k,
            crate::sql_analyzer::AggregateKind::CountDistinct))
    });
    if has_cd && has_other_agg {
        return crate::reflex_reject("COUNT(DISTINCT col) cannot be mixed with other aggregates in the same query. \
                Use a CTE to separate them: WITH cd AS (SELECT grp, COUNT(DISTINCT col) ...) SELECT ...");
    }

    let froms = analysis.sources.clone();
    let real_source_names: Vec<String> = froms
        .iter()
        .filter(|s| !s.starts_with('<'))
        .cloned()
        .collect();
    let is_join_query = real_source_names.len() > 1;

    let plan = if topk_k.is_some() {
        plan_aggregation_with_topk(&analysis, topk_k)
    } else {
        plan_aggregation(&analysis)
    };

    let mut ctx = BuildContext {
        view_name,
        sql,
        unique_columns_str,
        if_not_exists,
        topk_k,
        ignore_sources,
        partition_by,
        logged,
        deferred,
        storage_upper,
        mode_upper,
        analysis,
        plan,
        froms,
        real_source_names,
        is_join_query,
        resolved_unique_columns: Vec::new(),
        resolved_partition_cols: Vec::new(),
        resolved_strategy: String::new(),
        ivm_froms: Vec::new(),
        depth: 0,
        unlogged_tables: Vec::new(),
    };

    resolve_unique_columns(&mut ctx);

    // 1.5.3 (plans/partitioning_3.md §4) — populate per-source
    // partition_join_paths fragments.  Skipped here for the unpartitioned
    // / passthrough cases; the real computation runs below once
    // `resolved_partition_cols` is known.  (We can't run it earlier than
    // build_source_join_keys, and we can't run it later than the catalog
    // insert that persists `plan.aggregations`.)  This call is a no-op
    // until the partition path resolves the partition column.
    populate_source_join_keys(&mut ctx);
    validate_select_columns(&ctx);

    if let Some(msg) = check_existence_and_cycle(&ctx) {
        return msg;
    }

    if let Err(msg) = resolve_partitioning(&mut ctx) {
        return Box::leak(msg.into_boxed_str());
    }

    Spi::connect_mut(|client| {
        resolve_existing_imv_deps(client, &mut ctx);
        apply_partition_plan(client, &mut ctx);
        filter_imv_relevant_columns(client, &mut ctx);
        if ctx.plan.is_passthrough {
            materialize_passthrough(client, &mut ctx);
        } else {
            materialize_aggregate(client, &mut ctx);
        }

        install_source_triggers(client, &ctx);
        install_deferred_flush_if_needed(client, &ctx);

        install_min_max_indexes(client, &ctx);

        persist_metadata(client, &ctx);

        initial_aggregate_materialization(client, &mut ctx);
    });

    info!("pg_reflex: created IMV '{}'", view_name);
    "CREATE REFLEX INCREMENTAL VIEW"
}

/// Resolve every real-table entry in `analysis.sources` (and the matching
/// `analysis.table_aliases` values) to its `schema.table` form via the
/// current session's `search_path`. Synthetic markers (`<subquery:…>`,
/// `<function:…>`, `<…>`) are left untouched. Entries that do not resolve
/// (typo, race against a DROP, or already passing through as a CTE label) are
/// kept verbatim — downstream catalog lookups will surface a clear error in
/// that case rather than this helper guessing.
///
/// Sources that resolve to the `public` schema are intentionally left bare:
/// the legacy code path's `unwrap_or("public")` fallback (e.g. trigger.rs's
/// `nspname=$1` lookup) lands on the same relation, so there is nothing for
/// canonicalization to fix and rewriting would churn identifier suffixes
/// across already-deployed installs. The bug class only manifests when a
/// non-`public` source shares a bare name with a `public` homonym — there
/// the canonical form keeps DDL-time and trigger-fire-time in agreement.
///
/// The IMV's persistent identity for a source (depends_on, trigger-body slot,
/// pending-table value) is the canonical form returned here.
fn canonicalize_analysis_sources(analysis: &mut crate::sql_analyzer::SqlAnalysis) {
    use std::collections::HashMap;
    let mut resolutions: HashMap<String, String> = HashMap::new();

    Spi::connect(|client| {
        for raw in &analysis.sources {
            if raw.starts_with('<') || resolutions.contains_key(raw) {
                continue;
            }
            let qualified: Option<String> = client
                .select(
                    "SELECT n.nspname::TEXT || '.' || c.relname::TEXT AS q \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.oid = to_regclass($1)",
                    None,
                    &[unsafe {
                        DatumWithOid::new(raw.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .ok()
                .and_then(|mut it| it.next())
                .and_then(|row| {
                    row.get_by_name::<&str, _>("q")
                        .ok()
                        .flatten()
                        .map(|s| s.to_string())
                });
            if let Some(q) = qualified {
                if q.starts_with("public.") {
                    continue;
                }
                if q != *raw {
                    resolutions.insert(raw.clone(), q);
                }
            }
        }
    });

    if resolutions.is_empty() {
        return;
    }

    for source in analysis.sources.iter_mut() {
        if let Some(q) = resolutions.get(source) {
            *source = q.clone();
        }
    }
    for table in analysis.table_aliases.values_mut() {
        if let Some(q) = resolutions.get(table) {
            *table = q.clone();
        }
    }
}

/// Build per-source-table column mappings for passthrough DELETE/UPDATE.
///
/// For the "key owner" table (whose columns directly match the key), mapping is 1:1.
/// For secondary (joined) tables, the mapping is derived from JOIN conditions:
/// e.g., `ON s.product_id = p.id` maps target "product_id" → source "id" for the products table.
fn build_passthrough_key_mappings(
    plan: &mut crate::aggregation::AggregationPlan,
    key_columns: &[String],
    sources: &[&String],
    analysis: &crate::sql_analyzer::SqlAnalysis,
) {
    use std::collections::HashMap;

    // Build reverse alias map: real table name → alias
    let reverse_aliases: HashMap<&str, &str> = analysis
        .table_aliases
        .iter()
        .map(|(alias, table)| (table.as_str(), alias.as_str()))
        .collect();

    // Build a map from target column name → expr_sql (e.g., "product_id" → "s.product_id")
    let mut target_col_to_expr: HashMap<String, String> = HashMap::new();
    for col in &analysis.select_columns {
        let target_name = col.alias.as_deref().unwrap_or(&col.expr_sql);
        let target_name = normalized_column_name(target_name);
        target_col_to_expr.insert(target_name, col.expr_sql.clone());
    }

    // For each source table, determine if it's the key owner or a secondary table
    for source in sources {
        let source_str = source.as_str();
        let alias = reverse_aliases.get(source_str).copied();

        // Check if this source owns all key columns directly
        // (i.e., for each key column, the SELECT expr references this table)
        let mut is_key_owner = true;
        for kc in key_columns {
            if let Some(expr) = target_col_to_expr.get(kc.as_str()) {
                // expr is like "s.product_id" — check if the table qualifier matches this source
                if let Some(dot_pos) = expr.rfind('.') {
                    let qualifier = &expr[..dot_pos];
                    let matches_alias = alias.is_some_and(|a| a.to_lowercase() == qualifier);
                    let matches_table = bare_column_name(source_str).to_lowercase() == qualifier;
                    if !matches_alias && !matches_table {
                        is_key_owner = false;
                        break;
                    }
                }
                // No qualifier (e.g., single table) — assume it belongs to this source if single source
            } else {
                is_key_owner = false;
                break;
            }
        }

        if is_key_owner {
            // Key owner: target_col == source_col (columns exist directly in this table)
            let mappings: Vec<(String, String)> = key_columns
                .iter()
                .map(|kc| {
                    // Extract the bare source column name from the expression
                    let source_col = target_col_to_expr
                        .get(kc.as_str())
                        .map(|expr| normalized_column_name(expr))
                        .unwrap_or_else(|| kc.clone());
                    (kc.clone(), source_col)
                })
                .collect();
            plan.passthrough_key_mappings
                .insert(source_str.to_string(), mappings);
        } else {
            // Secondary table: derive mapping from JOIN conditions
            let mut mappings: Vec<(String, String)> = Vec::new();
            for join in &analysis.joins {
                if let Some(ref cond) = join.condition_sql {
                    let join_mappings = parse_join_condition_mappings(
                        cond,
                        source_str,
                        &analysis.table_aliases,
                        key_columns,
                        &target_col_to_expr,
                    );
                    mappings.extend(join_mappings);
                }
            }
            if !mappings.is_empty() {
                plan.passthrough_key_mappings
                    .insert(source_str.to_string(), mappings);
            }
            // If no mappings found, this source has no entry → triggers fall back to full refresh
        }
    }
}

/// 1.4.6 — Build the per-source JOIN-key mapping for aggregate plans.
///
/// For each source in the IMV, find JOIN equalities where the source's
/// column equals another table's column AND the other column projects to
/// a GROUP BY column of the intermediate. Then, gate on:
///   * **All** equalities involving the source map to a GROUP BY column
///     (any partial mapping → unsafe, falls back to MERGE).
///   * The mapped source columns cover a UNIQUE key of the source table
///     (PK or unique index). Without a unique key, multiple source rows
///     could share the JOIN-col values and collide in bulk-INSERT.
///
/// Sets `plan.source_join_keys[source] = Vec<(intermediate_col, source_col)>`
/// only when both gates pass. Empty / missing entry → standard MERGE path.
///
/// Skipped for passthrough plans (they use `passthrough_key_mappings`).
pub(crate) fn build_source_join_keys(
    plan: &mut crate::aggregation::AggregationPlan,
    sources: &[&String],
    analysis: &crate::sql_analyzer::SqlAnalysis,
) {
    use std::collections::{HashMap, HashSet};

    if plan.is_passthrough {
        return;
    }

    // 1) Build the lowered set of GROUP BY column names and a target→expr
    //    map so `parse_join_condition_mappings` can recognize "other side"
    //    references whether they're bare (`dem_plan_id`) or qualified
    //    (`sales_simulation.dem_plan_id`).
    let group_by_lowercased: Vec<String> = plan
        .group_by_columns
        .iter()
        .map(|c| normalized_column_name(c).to_lowercase())
        .collect();
    let group_by_set: HashSet<String> = group_by_lowercased.iter().cloned().collect();

    let mut target_col_to_expr: HashMap<String, String> = HashMap::new();
    for col in &analysis.select_columns {
        let target_name = col.alias.as_deref().unwrap_or(&col.expr_sql);
        let target_name = bare_column_name(target_name).to_lowercase();
        if group_by_set.contains(&target_name) {
            target_col_to_expr.insert(target_name, col.expr_sql.to_lowercase());
        }
    }

    // 2) For each source, look only at the JOIN that *introduces* this
    //    source into the FROM clause — i.e. `analysis.joins[i]` with
    //    target_table matching `source`. Other JOINs may reference this
    //    source's columns incidentally (e.g. `pricing` joining on
    //    `demand_planning.assortment_id`), but those say nothing about
    //    THIS source's identity. Counting them inflates the
    //    "all-equalities-mapped" denominator and spuriously refuses the
    //    bulk path.
    let source_bare_lc: std::collections::HashMap<&String, String> = sources
        .iter()
        .map(|s| (*s, bare_column_name(s).to_lowercase()))
        .collect();
    for source in sources {
        let source_str = source.as_str();
        let source_lc = source_str.to_lowercase();
        let source_bare = source_bare_lc.get(source).cloned().unwrap_or_default();

        let mut all_mappings: Vec<(String, String)> = Vec::new();
        let mut all_equalities_count: usize = 0;
        for join in &analysis.joins {
            let target_lc = join.target_table.to_lowercase();
            let target_bare = bare_column_name(&join.target_table).to_lowercase();
            let target_matches = target_lc == source_lc
                || target_bare == source_bare
                || target_lc == source_bare
                || target_bare == source_lc;
            if !target_matches {
                continue;
            }
            if let Some(ref cond) = join.condition_sql {
                let n_eq =
                    count_equalities_involving_source(cond, source_str, &analysis.table_aliases);
                if n_eq == 0 {
                    continue;
                }
                all_equalities_count += n_eq;
                let join_mappings = parse_join_condition_mappings(
                    cond,
                    source_str,
                    &analysis.table_aliases,
                    &group_by_lowercased,
                    &target_col_to_expr,
                );
                all_mappings.extend(join_mappings);
            }
        }

        if all_mappings.is_empty() || all_equalities_count == 0 {
            continue;
        }

        // Dedup mappings before counting coverage.
        all_mappings.sort();
        all_mappings.dedup();

        // Gate A: every JOIN equality involving the source must map to a
        // GROUP BY column. Partial mappings (some equality components map,
        // others don't) leave un-pinned dimensions on the source side, so
        // a single source row's identity doesn't fully determine its
        // intermediate group keys → bulk path unsafe.
        if all_mappings.len() < all_equalities_count {
            continue;
        }

        // Gate B: the source-side columns of the mapping must cover a
        // UNIQUE key on the source table. Otherwise multiple source rows
        // can produce the same mapped values, breaking the "transition row
        // uniquely identifies a slice of intermediate" assumption.
        let source_cols: Vec<String> = all_mappings.iter().map(|(_, sc)| sc.clone()).collect();
        if !source_cols_cover_unique_key(source_str, &source_cols) {
            continue;
        }

        plan.source_join_keys
            .insert(source_str.to_string(), all_mappings);
    }
}

/// Count how many "x = y" equalities in a JOIN ON clause reference
/// `source_table` on one side. Conservative — only counts top-level
/// AND-joined equality predicates (the same shape
/// `parse_join_condition_mappings` understands). Stand-alone helper so
/// the safety gate ("did all equalities map?") can compare against a
/// concrete denominator.
pub(crate) fn count_equalities_involving_source(
    condition: &str,
    source_table: &str,
    table_aliases: &std::collections::HashMap<String, String>,
) -> usize {
    let source_lower = source_table.to_lowercase();
    let source_bare = bare_column_name(source_table).to_lowercase();
    let source_aliases: Vec<String> = table_aliases
        .iter()
        .filter(|(_, table)| table.to_lowercase() == source_lower)
        .map(|(alias, _)| alias.to_lowercase())
        .collect();

    let mut count = 0;
    // Case-insensitive AND split: lowercase first, split once. Avoids the
    // double-iteration footgun that existed in the legacy
    // `parse_join_condition_mappings` (now fixed).
    let condition_lower = condition.to_lowercase();
    for part in condition_lower.split(" and ") {
        let part = part.trim();
        let sides: Vec<&str> = part.splitn(2, '=').collect();
        if sides.len() != 2 {
            continue;
        }
        let left = sides[0].trim();
        let right = sides[1].trim();
        if is_from_table(left, &source_bare, &source_aliases)
            || is_from_table(right, &source_bare, &source_aliases)
        {
            count += 1;
        }
    }
    count
}

/// Query pg_catalog to confirm `cols` cover at least one UNIQUE/PK index
/// on `source`. Returns `true` if any unique index has all of its columns
/// contained in `cols` (case-insensitive). Falls back to `false` on any
/// catalog access error — safe to refuse the bulk path on doubt.
fn source_cols_cover_unique_key(source: &str, cols: &[String]) -> bool {
    let cols_lower: std::collections::HashSet<String> =
        cols.iter().map(|c| c.to_lowercase()).collect();

    let unique_indexes: Vec<Vec<String>> = Spi::connect(|client| {
        client
            .select(
                "SELECT array_agg(a.attname::TEXT ORDER BY k.n) AS cols \
                 FROM pg_index ix \
                 JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, n) ON true \
                 JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.col \
                 WHERE ix.indrelid = to_regclass($1) AND ix.indisunique \
                 GROUP BY ix.indexrelid",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<Vec<String>, _>("cols").unwrap_or(None))
            .collect()
    });

    unique_indexes.iter().any(|idx_cols| {
        idx_cols
            .iter()
            .all(|c| cols_lower.contains(&c.to_lowercase()))
    })
}

/// PRIMARY KEY columns of `source` (lower-cased, in key order). Empty when the
/// table has no primary key or on any catalog error. PK columns are NOT NULL,
/// so they are a true unique key (unlike a nullable UNIQUE index).
fn source_primary_key_columns(source: &str) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                "SELECT a.attname::TEXT AS col \
                 FROM pg_index ix \
                 JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, ord) ON true \
                 JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.col \
                 WHERE ix.indrelid = to_regclass($1) AND ix.indisprimary \
                 ORDER BY k.ord",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<&str, _>("col")
                    .unwrap_or(None)
                    .map(|c| c.to_lowercase())
            })
            .collect()
    })
}

/// Collect `source`'s own bare column names that appear on one side of a
/// top-level `x = y` equality in any JOIN ON clause, plus the total count of
/// equalities involving the source. Used to decide whether the source is
/// reached by a to-one join (its equi-join columns cover a unique key).
fn source_equi_join_columns(
    source: &str,
    joins: &[crate::sql_analyzer::JoinInfo],
    table_aliases: &std::collections::HashMap<String, String>,
) -> (Vec<String>, usize) {
    let source_lower = source.to_lowercase();
    let source_bare = bare_column_name(source).to_lowercase();
    let source_aliases: Vec<String> = table_aliases
        .iter()
        .filter(|(_, table)| table.to_lowercase() == source_lower)
        .map(|(alias, _)| alias.to_lowercase())
        .collect();

    let mut cols: Vec<String> = Vec::new();
    let mut n_eq = 0usize;
    for join in joins {
        let Some(ref cond) = join.condition_sql else {
            continue;
        };
        let cond_lower = cond.to_lowercase();
        for part in cond_lower.split(" and ") {
            let part = part.trim();
            let sides: Vec<&str> = part.splitn(2, '=').collect();
            if sides.len() != 2 {
                continue;
            }
            let left = sides[0].trim();
            let right = sides[1].trim();
            let source_side = if is_from_table(left, &source_bare, &source_aliases) {
                Some(left)
            } else if is_from_table(right, &source_bare, &source_aliases) {
                Some(right)
            } else {
                None
            };
            if let Some(side) = source_side {
                n_eq += 1;
                cols.push(bare_column_name(side).to_lowercase());
            }
        }
    }
    cols.sort();
    cols.dedup();
    (cols, n_eq)
}

/// Fetch all sound unique keys (PRIMARY KEY + NOT-NULL UNIQUE indexes) of
/// a table. Each key is a Vec of column names in definition order.
/// Includes __reflex_uk_* indexes created for keyed CTE sub-IMVs.
#[allow(dead_code)]
fn source_sound_unique_keys(source: &str) -> Vec<Vec<String>> {
    let mut result = Vec::new();

    // PRIMARY KEY
    let pk = source_primary_key_columns(source);
    if !pk.is_empty() {
        result.push(pk);
    }

    // UNIQUE indexes: both NOT-NULL (indnullsnotdistinct) and __reflex_uk_* indexes
    let uks: Vec<Vec<String>> = Spi::connect(|client| {
        client
            .select(
                "SELECT array_agg(a.attname::TEXT ORDER BY k.n) AS cols \
                 FROM pg_index ix \
                 JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(col, n) ON true \
                 JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.col \
                 JOIN pg_class idx ON idx.oid = ix.indexrelid \
                 WHERE ix.indrelid = to_regclass($1) AND ix.indisunique \
                   AND NOT ix.indisprimary \
                   AND (ix.indnullsnotdistinct OR idx.relname LIKE '__reflex_uk_%') \
                 GROUP BY ix.indexrelid",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<Vec<String>, _>("cols").unwrap_or(None))
            .collect()
    });
    result.extend(uks);
    result
}

/// TRUE when `source` is a registered IMV flagged `max_one_row` (ungrouped
/// aggregate → at most one row). Base tables / unknown names → FALSE.
#[allow(dead_code)]
fn source_is_max_one_row(source: &str) -> bool {
    Spi::connect(|client| {
        client
            .select(
                "SELECT COALESCE(max_one_row, FALSE) AS m \
                 FROM public.__reflex_ivm_reference \
                 WHERE name = $1 OR name = $2 \
                 ORDER BY (name = $1) DESC LIMIT 1",
                None,
                &[
                    unsafe {
                        DatumWithOid::new(
                            source.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe {
                        DatumWithOid::new(
                            bare_column_name(source).to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| row.get_by_name::<bool, _>("m").unwrap_or(None))
            .next()
            .unwrap_or(false)
    })
}

/// Find the join type (INNER, LEFT, CROSS, etc.) that reaches a particular
/// target source. Returns the first match or None.
#[allow(dead_code)]
fn join_type_for_target(target: &str, joins: &[crate::sql_analyzer::JoinInfo]) -> Option<String> {
    let target_bare = bare_column_name(target).to_lowercase();
    joins
        .iter()
        .find(|j| bare_column_name(&j.target_table).to_lowercase() == target_bare)
        .map(|j| j.join_type.clone())
}

/// Structural unique-key inference for a JOIN passthrough that was given no
/// explicit key. Returns the OUTPUT column names forming a sound unique key, or
/// `None` when none can be proven.
///
/// Sound rule: pick an anchor whose sound unique key (PK or NOT-NULL / NULLS
/// NOT DISTINCT unique index — see [`source_sound_unique_keys`]) is fully
/// projected. Then every other source must be either (a) to-one — its equi-join
/// columns cover a unique key, or it is `max_one_row` (single-row aggregate,
/// incl. CROSS joins) — contributing nothing, or (b) a to-many INNER join whose
/// own sound key is fully projected, contributing that key (K_anchor ∪ K_other
/// is unique). INNER/LEFT/CROSS anchors qualify; RIGHT/FULL, OR/USING, LEFT or
/// CROSS to-many, and any source without a projectable key are refused.
fn infer_join_passthrough_unique_key(ctx: &BuildContext) -> Option<Vec<String>> {
    let analysis = &ctx.analysis;
    if analysis.joins.is_empty() {
        return None;
    }

    // Per-join admissibility. CROSS is now allowed (to-one iff the joined
    // relation is single-row). RIGHT/FULL multiply or NULL-pad the anchor's
    // rows → refuse. OR / USING conditions defeat the equi analysis → refuse.
    let mut has_left = false;
    for join in &analysis.joins {
        match join.join_type.as_str() {
            "INNER" | "CROSS" => {}
            "LEFT" => has_left = true,
            _ => return None,
        }
        if let Some(cond) = &join.condition_sql {
            let lc = cond.to_lowercase();
            if lc.contains(" or ") || lc.trim_start().starts_with("using") {
                return None;
            }
        }
    }

    let real_sources: Vec<&String> = ctx.real_source_names.iter().collect();
    if real_sources.iter().any(|s| s.starts_with('<')) {
        return None;
    }

    // Base source = the one not introduced by any JOIN. A LEFT join only
    // preserves the base table's rows, so it is the only valid anchor then.
    let join_targets: std::collections::HashSet<String> = analysis
        .joins
        .iter()
        .map(|j| bare_column_name(&j.target_table).to_lowercase())
        .collect();
    let is_base = |s: &str| !join_targets.contains(&bare_column_name(s).to_lowercase());

    // output name → its SELECT expression (lower-cased), e.g. "id" → "o.id".
    let mut target_to_expr: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for col in &analysis.select_columns {
        let name = normalized_column_name(col.alias.as_deref().unwrap_or(&col.expr_sql));
        target_to_expr.insert(name, col.expr_sql.to_lowercase());
    }

    // The output name that projects `source.key_col` as a bare column reference,
    // or None when that key column is not passed through unaltered.
    let projected_output = |source: &str, key_col: &str| -> Option<String> {
        let bare = bare_column_name(source).to_lowercase();
        let aliases: Vec<String> = analysis
            .table_aliases
            .iter()
            .filter(|(_, t)| t.to_lowercase() == source.to_lowercase())
            .map(|(a, _)| a.to_lowercase())
            .collect();
        for (out_name, expr) in &target_to_expr {
            if is_from_table(expr, &bare, &aliases) && bare_column_name(expr) == key_col {
                return Some(out_name.clone());
            }
        }
        None
    };

    // The first sound unique key of `source` that is fully projected as bare
    // output columns, mapped to those output names. None when no key qualifies.
    let projected_sound_key = |source: &str| -> Option<Vec<String>> {
        for key in source_sound_unique_keys(source) {
            let mut outs = Vec::with_capacity(key.len());
            let mut all = true;
            for kc in &key {
                match projected_output(source, kc) {
                    Some(o) => outs.push(o),
                    None => {
                        all = false;
                        break;
                    }
                }
            }
            if all {
                return Some(outs);
            }
        }
        None
    };

    for anchor in &real_sources {
        if has_left && !is_base(anchor) {
            continue;
        }
        let Some(mut result_key) = projected_sound_key(anchor) else {
            continue;
        };

        // Classify every other source against this anchor.
        let mut composable = true;
        for other in &real_sources {
            if other.eq_ignore_ascii_case(anchor) {
                continue;
            }

            let (eq_cols, n_eq) =
                source_equi_join_columns(other, &analysis.joins, &analysis.table_aliases);
            let to_one = (n_eq > 0
                && !eq_cols.is_empty()
                && source_cols_cover_unique_key(other, &eq_cols))
                || source_is_max_one_row(other);
            if to_one {
                // Collapses to ≤1 matching row — contributes nothing to the key.
                continue;
            }

            // to-many: sound only for an INNER join whose joined relation has a
            // fully-projected sound key. Each output row is one distinct
            // (anchor-row, other-row) pair, so K_anchor ∪ K_other is unique.
            // LEFT/CROSS to-many can NULL-pad or multiply unbounded → refuse.
            if join_type_for_target(other, &analysis.joins).as_deref() != Some("INNER") {
                composable = false;
                break;
            }
            match projected_sound_key(other) {
                Some(outs) => result_key.extend(outs),
                None => {
                    composable = false;
                    break;
                }
            }
        }

        if composable {
            result_key.sort();
            result_key.dedup();
            if !result_key.is_empty() {
                return Some(result_key);
            }
        }
    }
    None
}

/// Parse a JOIN condition to extract column mappings between the key-owner table and a secondary table.
///
/// For `s.product_id = p.id AND s.version = p.version`:
/// - Splits by AND
/// - For each equality, identifies which side belongs to the secondary table
/// - Maps the key-owner side's target column name to the secondary side's source column name
fn parse_join_condition_mappings(
    condition: &str,
    secondary_table: &str,
    table_aliases: &std::collections::HashMap<String, String>,
    key_columns: &[String],
    target_col_to_expr: &std::collections::HashMap<String, String>,
) -> Vec<(String, String)> {
    let mut mappings = Vec::new();

    // Build a set for fast lookup: which aliases/names refer to the secondary table?
    let secondary_lower = secondary_table.to_lowercase();
    let secondary_bare = bare_column_name(secondary_table).to_lowercase();
    let secondary_aliases: Vec<String> = table_aliases
        .iter()
        .filter(|(_, table)| table.to_lowercase() == secondary_lower)
        .map(|(alias, _)| alias.to_lowercase())
        .collect();

    // Build reverse: for each key column, which expr_sql does it correspond to?
    // e.g., "product_id" → "s.product_id"

    // Case-insensitive split on AND: lowercase the condition first, then split
    // only on the lowercase form. The previous chain-of-two-splits formulation
    // produced spurious whole-condition iterations when only one of "AND" /
    // "and" was present in the source string (caught by
    // pg_test_source_join_keys_skipped_when_composite_join_partial_map).
    let condition_lower = condition.to_lowercase();
    for part in condition_lower.split(" and ") {
        let part = part.trim();
        let sides: Vec<&str> = part.splitn(2, '=').collect();
        if sides.len() != 2 {
            continue;
        }
        let left = sides[0].trim().to_string();
        let right = sides[1].trim().to_string();

        // Determine which side belongs to the secondary table
        let (secondary_side, other_side) =
            if is_from_table(&left, &secondary_bare, &secondary_aliases) {
                (left, right)
            } else if is_from_table(&right, &secondary_bare, &secondary_aliases) {
                (right, left)
            } else {
                continue;
            };

        let secondary_col = bare_column_name(&secondary_side).to_string();
        let other_col = bare_column_name(&other_side).to_string();

        // Find which key column the other side maps to
        // The other side's bare column might be a key column directly,
        // or the other side's full expression might match a key column's expr_sql
        for kc in key_columns {
            if *kc == other_col {
                // Direct match: key column "product_id" and other side bare name is "product_id"
                mappings.push((kc.clone(), secondary_col.clone()));
                break;
            }
            // Check via expr_sql: key column "product_id" has expr "s.product_id",
            // and other_side is "s.product_id"
            if let Some(expr) = target_col_to_expr.get(kc.as_str()) {
                if *expr == other_side {
                    mappings.push((kc.clone(), secondary_col.clone()));
                    break;
                }
            }
        }
    }

    mappings
}

/// Check if a qualified column reference (e.g., "p.id") belongs to a given table.
fn is_from_table(qualified_col: &str, table_bare_name: &str, table_aliases: &[String]) -> bool {
    if let Some(dot_pos) = qualified_col.rfind('.') {
        let qualifier = &qualified_col[..dot_pos];
        qualifier == table_bare_name || table_aliases.iter().any(|a| a == qualifier)
    } else {
        false
    }
}

/// Return type of [`query_column_types_from_catalog_with_per_source`]:
/// `(types, not_null_cols, per_source_columns)`.
type CatalogColumnInfo = (
    HashMap<String, String>,
    std::collections::HashSet<String>,
    HashMap<String, std::collections::HashSet<String>>,
);

/// Per-source-aware column-type catalog lookup. Returns the cross-source
/// type map and not-null set used elsewhere, plus a per-source map
/// `source-table-name → set of catalog-known column names`.
///
/// Used by the 1.4.5 filter-aware spurious-skip metadata to ground the
/// analyzer's over-inclusive `imv_relevant_columns` (which attributes
/// bare idents to every real source) in the actual catalog shape, so the
/// persisted JSON only ever names columns that exist on the named source.
fn query_column_types_from_catalog_with_per_source(
    client: &pgrx::spi::SpiClient<'_>,
    table_names: &[String],
) -> CatalogColumnInfo {
    let mut types = HashMap::new();
    let mut not_null_cols = std::collections::HashSet::new();
    let mut per_source: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
    for table in table_names {
        // Skip non-real tables (subqueries, functions)
        if table.starts_with('<') {
            continue;
        }
        // Resolve the table via `to_regclass($1)` so we honour the session's
        // `search_path` instead of hard-coding the `public` schema. This is the
        // only way an unqualified source name from the IMV body (e.g.
        // `FROM history_sales_view`) finds the tenant-schema relation when the
        // caller has done `SET search_path = <tenant>, public`. We pull the
        // actual `relname` back out of pg_class so the `table.column` keys in
        // the returned map are consistent regardless of whether the caller
        // passed a qualified or unqualified name.
        //
        // pg_catalog (not information_schema.columns) because the latter omits
        // materialized views — a MIN/MAX over a matview column would then get
        // no type and default to NUMERIC. `format_type` covers every relkind.
        let rows = client
            .select(
                "SELECT a.attname::text AS col_name, \
                        format_type(a.atttypid, a.atttypmod) AS data_type, \
                        c.relname::text AS relname, \
                        CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable \
                 FROM pg_catalog.pg_attribute a \
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
                 WHERE c.oid = to_regclass($1) \
                   AND a.attnum > 0 AND NOT a.attisdropped",
                None,
                &[unsafe {
                    DatumWithOid::new(table.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report();
        for row in rows {
            if let (Some(col_name), Some(data_type), Some(relname)) = (
                row.get_by_name::<String, _>("col_name").unwrap_or(None),
                row.get_by_name::<String, _>("data_type").unwrap_or(None),
                row.get_by_name::<String, _>("relname").unwrap_or(None),
            ) {
                let pg_type = map_information_schema_type(&data_type);
                types.insert(format!("{}.{}", relname, col_name), pg_type.clone());
                // Also insert bare column name for simpler lookups
                types.entry(col_name.to_string()).or_insert(pg_type);

                // Track NOT NULL columns for SUM optimization
                let is_nullable = row
                    .get_by_name::<String, _>("is_nullable")
                    .unwrap_or(None)
                    .unwrap_or_default();
                if is_nullable == "NO" {
                    not_null_cols.insert(col_name.to_string());
                }
                per_source
                    .entry(table.clone())
                    .or_default()
                    .insert(col_name.clone());
            }
        }
    }
    (types, not_null_cols, per_source)
}

/// Canonicalize a column reference for comparison: lower-case, trim, and strip
/// the double-quotes around each dotted part. `"ss"."Dem_Plan_Id"` → `ss.dem_plan_id`.
fn canon_col_ref(s: &str) -> String {
    s.trim()
        .split('.')
        .map(|p| p.trim().trim_matches('"').to_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

/// True if `s` is a bare column reference (`ident` or `qualifier.ident`) rather
/// than a function call, literal, or compound expression. Only such refs can be
/// safely treated as join-key operands.
///
/// Quoted identifiers are rejected: a quoted name may contain a literal `.`
/// (`"weird.name"`), which the dot-splitting in [`canon_col_ref`] /
/// [`resolve_column_source`] would mis-read as a `qualifier.column`, possibly
/// resolving to the wrong table. Treating any quoted ref as non-simple means it
/// is never promoted to NOT NULL — correct (it falls back to `IS NOT DISTINCT
/// FROM`), just not index-optimized.
fn is_simple_col_ref(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() || t.contains('"') {
        return false;
    }
    t.split('.').all(|p| {
        let p = p.trim();
        !p.is_empty()
            && p.chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
    })
}

/// Split a boolean expression on top-level ` AND `, respecting parentheses and
/// single-quoted string literals.
fn split_top_level_and(cond: &str) -> Vec<String> {
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

/// If `conj` is a top-level equality `lhs = rhs` (not `<=`, `>=`, `<>`, `!=`),
/// return the two operands. Respects parentheses and string literals.
fn split_top_level_eq(conj: &str) -> Option<(&str, &str)> {
    let bytes = conj.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
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
            '=' if depth == 0 => {
                let prev = if i > 0 { bytes[i - 1] as char } else { ' ' };
                let next = if i + 1 < bytes.len() {
                    bytes[i + 1] as char
                } else {
                    ' '
                };
                if !matches!(prev, '<' | '>' | '!') && next != '=' {
                    return Some((&conj[..i], &conj[i + 1..]));
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Collect the canonical column references that are guaranteed non-NULL because
/// they appear as an operand of a conjunctive equality in an INNER-join ON
/// condition. An equi-join `x = y` cannot match a NULL on either side, so both
/// operands are non-NULL in the join output. Conditions containing a top-level
/// `OR` are skipped (a disjunct could re-admit a NULL).
fn inner_join_equi_non_null_refs(
    analysis: &crate::sql_analyzer::SqlAnalysis,
) -> std::collections::HashSet<String> {
    let mut refs = std::collections::HashSet::new();
    for j in &analysis.joins {
        if !j.join_type.to_uppercase().contains("INNER") {
            continue;
        }
        let Some(cond) = &j.condition_sql else {
            continue;
        };
        if cond.to_uppercase().contains(" OR ") {
            continue;
        }
        for conj in split_top_level_and(cond) {
            if let Some((l, r)) = split_top_level_eq(&conj) {
                if is_simple_col_ref(l) {
                    refs.insert(canon_col_ref(l));
                }
                if is_simple_col_ref(r) {
                    refs.insert(canon_col_ref(r));
                }
            }
        }
    }
    refs
}

/// Resolve a (possibly aliased) column reference to its real source table.
/// Returns `(real_table, bare_column)` when the reference is a simple
/// `qualifier.col` or a bare `col` with a single source; `None` otherwise.
fn resolve_column_source(
    analysis: &crate::sql_analyzer::SqlAnalysis,
    col_expr: &str,
) -> Option<(String, String)> {
    if !is_simple_col_ref(col_expr) {
        return None;
    }
    let canon = canon_col_ref(col_expr);
    if let Some((qual, col)) = canon.split_once('.') {
        let real = analysis
            .table_aliases
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(qual))
            .map(|(_, v)| v.trim_matches('"').to_lowercase())
            .unwrap_or_else(|| qual.to_string());
        Some((real, col.to_string()))
    } else if analysis.sources.len() == 1 {
        Some((analysis.sources[0].trim_matches('"').to_lowercase(), canon))
    } else {
        None
    }
}

/// True if `col_expr` resolves to a base-table column declared NOT NULL in the
/// catalog AND that table is not on the nullable side of an outer join. Such a
/// column can never be NULL in the IMV.
fn column_base_not_null(
    client: &mut pgrx::spi::SpiClient<'_>,
    analysis: &crate::sql_analyzer::SqlAnalysis,
    col_expr: &str,
    left_target_tables: &std::collections::HashSet<String>,
) -> bool {
    let Some((table, col)) = resolve_column_source(analysis, col_expr) else {
        return false;
    };
    if left_target_tables.contains(&table) {
        return false;
    }
    client
        .select(
            "SELECT a.attnotnull AS nn FROM pg_attribute a \
             WHERE a.attrelid = to_regclass($1) AND a.attname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped",
            Some(1),
            &[
                unsafe { DatumWithOid::new(table, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(col, PgBuiltInOids::TEXTOID.oid().value()) },
            ],
        )
        .ok()
        .and_then(|mut t| {
            t.next()
                .and_then(|r| r.get_by_name::<bool, _>("nn").ok().flatten())
        })
        == Some(true)
}

/// Infer the group-by / distinct columns that are *provably* NOT NULL from the
/// query's structure (NOT from transient create-time data). A column is promoted
/// only when it can never be NULL in the IMV:
///
///   * it is an equi-join operand of an INNER join (`dp.id = ss.dem_plan_id`
///     keeps `ss.dem_plan_id` non-NULL — the yse.ivm_sop_forecast_view 405 s
///     case), or
///   * its base column is declared NOT NULL in the catalog and its table is not
///     on the nullable side of an outer join.
///
/// Promoting a column lets MERGE maintenance match keys with `=` (index-friendly)
/// instead of `IS NOT DISTINCT FROM`. Doing so on a column that can later become
/// NULL silently drops rows (docs/fuzz-findings.md findings #1 and #3), so the
/// inference is deliberately conservative: anything not provably non-NULL (outer-
/// join columns, unconstrained nullable columns, computed expressions, output
/// aliases) is left alone and matched with the always-correct `IS NOT DISTINCT
/// FROM`. With no analysis available, nothing is promoted.
fn infer_not_null_columns(
    client: &mut pgrx::spi::SpiClient<'_>,
    plan: &crate::aggregation::AggregationPlan,
    analysis: Option<&crate::sql_analyzer::SqlAnalysis>,
) -> std::collections::HashSet<String> {
    let mut proven = std::collections::HashSet::new();
    let Some(a) = analysis else {
        return proven;
    };

    // Map the LEFT-join target tables (the nullable side). RIGHT/FULL joins make
    // the nullable side ambiguous, so we promote nothing for those queries.
    let mut left_target_tables: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for j in &a.joins {
        let jt = j.join_type.to_uppercase();
        if jt.contains("RIGHT") || jt.contains("FULL") {
            return proven;
        }
        if jt.contains("LEFT") {
            left_target_tables.insert(j.target_table.trim_matches('"').to_lowercase());
        }
    }

    let from_outer_join = |col_expr: &str| -> bool {
        if left_target_tables.is_empty() || !col_expr.contains('.') {
            return false;
        }
        let alias = col_expr.split('.').next().unwrap_or("").trim_matches('"');
        if left_target_tables.contains(&alias.to_lowercase()) {
            return true;
        }
        a.table_aliases.iter().any(|(k, real)| {
            k.eq_ignore_ascii_case(alias)
                && left_target_tables.contains(&real.trim_matches('"').to_lowercase())
        })
    };

    let equi_refs = inner_join_equi_non_null_refs(a);

    for col_expr in plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
    {
        let norm = normalized_column_name(col_expr);
        if plan.not_null_columns.contains(&norm) {
            continue;
        }
        if from_outer_join(col_expr) {
            continue;
        }
        // Only reason about simple (unquoted) column references; `canon_col_ref`
        // dot-splitting is unsafe for quoted names that may contain a literal dot.
        let proven_not_null = is_simple_col_ref(col_expr)
            && (equi_refs.contains(&canon_col_ref(col_expr))
                || column_base_not_null(client, a, col_expr, &left_target_tables));
        if proven_not_null {
            proven.insert(norm);
        }
    }
    proven
}

/// Persist a delta of newly-discovered NOT NULL columns to
/// `__reflex_ivm_reference.aggregations`. Performs a JSON-level merge with
/// the existing array (no overwrite of catalog-derived entries).
fn persist_probed_not_null_columns(
    client: &mut pgrx::spi::SpiClient<'_>,
    view_name: &str,
    new_cols: &[String],
) {
    if new_cols.is_empty() {
        return;
    }
    let arr_literal = format_pg_text_array_literal(new_cols);
    client
        .update(
            "UPDATE public.__reflex_ivm_reference \
             SET aggregations = jsonb_set( \
                 aggregations::jsonb, \
                 '{not_null_columns}', \
                 ( \
                     SELECT jsonb_agg(DISTINCT col) \
                     FROM ( \
                         SELECT jsonb_array_elements_text( \
                             COALESCE((aggregations::jsonb)->'not_null_columns', '[]'::jsonb) \
                         ) AS col \
                         UNION \
                         SELECT unnest($1::text[]) AS col \
                     ) s \
                 ) \
             )::json \
             WHERE name = $2",
            None,
            &[
                unsafe { DatumWithOid::new(arr_literal, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
            ],
        )
        .unwrap_or_report();
}

/// SQL-callable wrapper: re-probe an existing IMV from data and update its
/// stored aggregations. Used by the 1.4.4→1.4.5 migration to backfill
/// effectively-NOT-NULL columns the catalog missed on existing IMVs, and by
/// operators after a data shape change. Returns a human-readable status.
pub(crate) fn reflex_probe_not_null_columns_impl(view_name: &str) -> String {
    if let Err(msg) = validate_view_name(view_name) {
        return msg.to_string();
    }
    let result: Result<Vec<String>, String> = Spi::connect_mut(|client| {
        let rows = client
            .select(
                "SELECT aggregations::text AS aggregations, sql_query \
                 FROM public.__reflex_ivm_reference \
                 WHERE name = $1 AND enabled = TRUE",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("lookup failed: {}", e))?
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Err(format!("IMV '{}' not found or disabled", view_name));
        }
        let agg_json: String = rows[0]
            .get_by_name::<&str, _>("aggregations")
            .map_err(|e| format!("read aggregations: {}", e))?
            .unwrap_or("{}")
            .to_string();
        let plan: crate::aggregation::AggregationPlan = serde_json::from_str(&agg_json)
            .map_err(|e| format!("aggregations JSON parse: {}", e))?;
        if plan.is_passthrough {
            return Ok(Vec::new());
        }
        // Re-derive the join analysis from the stored query so the same
        // structural NOT-NULL inference used at create time applies here. If the
        // query is missing or unparseable, `analysis` stays None and nothing is
        // promoted — conservative, never unsound.
        let analysis = rows[0]
            .get_by_name::<&str, _>("sql_query")
            .ok()
            .flatten()
            .and_then(|q| {
                Parser::parse_sql(&PostgreSqlDialect {}, q)
                    .ok()
                    .and_then(|stmts| crate::sql_analyzer::analyze(&stmts).ok())
            });
        let probed = infer_not_null_columns(client, &plan, analysis.as_ref());
        let new_cols: Vec<String> = probed
            .into_iter()
            .filter(|c| !plan.not_null_columns.contains(c))
            .collect();
        persist_probed_not_null_columns(client, view_name, &new_cols);
        Ok(new_cols)
    });
    match result {
        Ok(cols) if cols.is_empty() => {
            format!(
                "pg_reflex: probe found no additional NOT NULL columns on '{}'",
                view_name
            )
        }
        Ok(cols) => format!(
            "pg_reflex: probe added {} NOT NULL column(s) on '{}': {:?}",
            cols.len(),
            view_name,
            cols
        ),
        Err(e) => format!("ERROR: {}", e),
    }
}

/// 1.4.5 — `reflex_compact_imv(view_name TEXT) RETURNS TEXT`.
///
/// VACUUM FULL both the intermediate and target tables of an IMV. The
/// 1.4.3→1.4.4 migration set fillfactor=70 on these tables but did not
/// rewrite existing pages — HOT updates can only fire after pages have
/// been written with the new fillfactor. Running VACUUM FULL once per
/// table materializes the fillfactor immediately, restoring HOT-eligibility
/// on legacy-populated IMVs.
///
/// VACUUM FULL takes ACCESS EXCLUSIVE on the rewritten table; for
/// multi-gigabyte IMVs this is a maintenance-window operation. Caller is
/// responsible for scheduling.
///
/// Returns a status string with per-table elapsed times.
/// Plan the SQL commands `reflex_compact_imv_impl` will issue for a
/// given IMV name. Pure function: no SPI, no side effects. Extracted so
/// the planning logic (identifier resolution, command shaping) is
/// unit-testable independent of `VACUUM`, which cannot run inside a
/// transaction (the pgrx test framework wraps every test in one).
///
/// Returns the ordered list of statements; the caller issues each via
/// SPI in a top-level statement.
pub(crate) fn plan_compact_imv(view_name: &str) -> Result<Vec<String>, String> {
    if let Err(msg) = validate_view_name(view_name) {
        return Err(msg.to_string());
    }
    let intermediate_tbl = intermediate_table_name(view_name);
    let target_tbl = quote_identifier(view_name);
    Ok(vec![
        format!("VACUUM (FULL) {}", intermediate_tbl),
        format!("VACUUM (FULL) {}", target_tbl),
    ])
}

/// Format the post-execution summary of a compact run. Pure function:
/// no SPI. Extracted so the message-shaping logic is unit-testable
/// (the executor path inside `reflex_compact_imv_impl` itself can't run
/// in a pgrx-test transaction because `VACUUM` is forbidden there).
pub(crate) fn format_compact_imv_summary(view_name: &str, per_stmt: &[(String, u128)]) -> String {
    let parts: Vec<String> = per_stmt
        .iter()
        .map(|(stmt, ms)| format!("{}: {} ms", stmt, ms))
        .collect();
    format!(
        "pg_reflex: compacted '{}' — {}",
        view_name,
        parts.join(", ")
    )
}

pub(crate) fn reflex_compact_imv_impl(view_name: &str) -> String {
    let stmts = match plan_compact_imv(view_name) {
        Ok(s) => s,
        Err(msg) => return msg,
    };
    let mut per_stmt: Vec<(String, u128)> = Vec::new();
    let result: Result<(), String> = Spi::connect_mut(|client| {
        for stmt in &stmts {
            let t0 = std::time::Instant::now();
            client
                .update(stmt, None, &[])
                .map_err(|e| format!("{} failed: {}", stmt, e))?;
            per_stmt.push((stmt.clone(), t0.elapsed().as_millis()));
        }
        Ok(())
    });
    match result {
        Ok(()) => format_compact_imv_summary(view_name, &per_stmt),
        Err(e) => format!("ERROR: {}", e),
    }
}

/// 1.4.5 — `reflex_compact_all_imv() RETURNS TEXT`.
///
/// VACUUM FULL every enabled IMV's intermediate and target tables. Iterates
/// the catalog in `(graph_depth, name)` order and dispatches to
/// `reflex_compact_imv_impl` for each row. Errors on one IMV are recorded
/// in the result but do not abort processing of the remaining IMVs — this
/// matches the semantics of a maintenance-window operator who wants
/// "compact everything you can".
/// Pure summary builder for `reflex_compact_all_imv_impl`. Given the
/// list of IMV names and per-IMV result messages, return the final
/// summary string. Extracted so the summary-shaping logic is
/// unit-testable independent of `VACUUM`.
pub(crate) fn build_compact_all_summary(
    names: &[String],
    results: &[(String, String)],
    total_ms: u128,
) -> String {
    let successes = results
        .iter()
        .filter(|(_, msg)| !msg.starts_with("ERROR"))
        .count();
    let failures: Vec<String> = results
        .iter()
        .filter(|(_, msg)| msg.starts_with("ERROR"))
        .map(|(name, msg)| format!("{}: {}", name, msg))
        .collect();
    let details: Vec<String> = results.iter().map(|(_, msg)| msg.clone()).collect();
    let mut summary = format!(
        "pg_reflex: compacted {}/{} IMV(s) in {} ms",
        successes,
        names.len(),
        total_ms
    );
    if !failures.is_empty() {
        summary.push_str(" — failures: ");
        summary.push_str(&failures.join("; "));
    }
    summary.push('\n');
    summary.push_str(&details.join("\n"));
    summary
}

pub(crate) fn reflex_compact_all_imv_impl() -> String {
    let names: Vec<String> = Spi::connect(|client| {
        let mut out: Vec<String> = Vec::new();
        if let Ok(table) = client.select(
            "SELECT name FROM public.__reflex_ivm_reference \
             WHERE enabled = TRUE ORDER BY graph_depth, name",
            None,
            &[],
        ) {
            for row in table {
                if let Ok(Some(name)) = row.get::<&str>(1) {
                    out.push(name.to_string());
                }
            }
        }
        out
    });
    if names.is_empty() {
        return "pg_reflex: no enabled IMVs to compact".to_string();
    }
    let t0 = std::time::Instant::now();
    let results: Vec<(String, String)> = names
        .iter()
        .map(|name| (name.clone(), reflex_compact_imv_impl(name)))
        .collect();
    build_compact_all_summary(&names, &results, t0.elapsed().as_millis())
}

/// 1.4.5 — `reflex_rebuild_imv_metadata(view_name TEXT) RETURNS TEXT`.
///
/// Re-analyzes an existing IMV's stored `base_query` and merges the newly
/// computed `imv_relevant_columns` / `imv_relevant_where` maps into its
/// `aggregations` JSON. Idempotent: existing fields (group_by_columns,
/// intermediate_columns, end_query_mappings, not_null_columns, …) are NOT
/// overwritten beyond the two new metadata maps.
///
/// Used by the 1.4.4→1.4.5 migration to backfill IMVs created before the
/// filter-aware spurious-skip metadata was introduced. Also useful after a
/// future analyzer extension shifts what falls into either map.
///
/// Returns a status string for telemetry.
pub(crate) fn reflex_rebuild_imv_metadata_impl(view_name: &str) -> String {
    if let Err(msg) = validate_view_name(view_name) {
        return msg.to_string();
    }
    let result: Result<(usize, usize, usize), String> = Spi::connect_mut(|client| {
        let rows = client
            .select(
                "SELECT base_query FROM public.__reflex_ivm_reference \
                 WHERE name = $1 AND enabled = TRUE",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("lookup failed: {}", e))?
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Err(format!("IMV '{}' not found or disabled", view_name));
        }
        let base_query: String = rows[0]
            .get_by_name::<&str, _>("base_query")
            .map_err(|e| format!("read base_query: {}", e))?
            .unwrap_or("")
            .to_string();

        let parsed = Parser::parse_sql(&PostgreSqlDialect {}, &base_query)
            .map_err(|e| format!("parse base_query: {}", e))?;
        let analysis =
            analyze(&parsed).map_err(|e: SqlAnalysisError| format!("analyze base_query: {}", e))?;

        // Ground analyzer output against the actual catalog shape. The
        // analyzer attributes bare identifiers to every real source as a
        // safe-correctness move; if we serialize that raw, the trigger's
        // filter-aware-skip block SELECTs columns that don't exist on the
        // source's transition table and errors at fire time. Mirror the
        // 1.4.5 filter applied at create-time in the create_reflex_ivm
        // aggregate path (search "1.4.5 — filter `imv_relevant_columns`").
        let froms_list: Vec<String> = analysis.sources.to_vec();
        let (_types, _nn, per_source_cols) =
            query_column_types_from_catalog_with_per_source(client, &froms_list);
        let mut relevant_cols: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for (source, set) in analysis.imv_relevant_columns.iter() {
            if source.starts_with('<') {
                continue;
            }
            let filtered: Vec<String> = if let Some(actual) = per_source_cols.get(source) {
                set.iter()
                    .filter(|c| actual.contains(&c.to_lowercase()))
                    .cloned()
                    .collect()
            } else {
                // Source not found in catalog — preserve raw (conservative)
                set.iter().cloned().collect()
            };
            if !filtered.is_empty() {
                relevant_cols.insert(source.clone(), filtered);
            }
        }

        let cols_json = serde_json::to_string(&relevant_cols)
            .map_err(|e| format!("serialize relevant cols: {}", e))?;
        let where_json = serde_json::to_string(&analysis.imv_relevant_where)
            .map_err(|e| format!("serialize relevant where: {}", e))?;

        let n_cols_sources = relevant_cols.len();
        let n_where_sources = analysis.imv_relevant_where.len();

        // 1.4.6 — also backfill source_join_keys for the bulk-INSERT /
        // bulk-DELETE / Path B paths. Rebuild a plan from base_query so we
        // can run the same build_source_join_keys logic the create path
        // uses. Single-source IMVs skip the call (no JOINs to mine).
        let mut tmp_plan = crate::aggregation::plan_aggregation(&analysis);
        let real_sources: Vec<&String> = analysis
            .sources
            .iter()
            .filter(|s| !s.starts_with('<'))
            .collect();
        let is_join_query = real_sources.len() > 1;
        if !tmp_plan.is_passthrough && is_join_query {
            build_source_join_keys(&mut tmp_plan, &real_sources, &analysis);
        }
        let sjk_json = serde_json::to_string(&tmp_plan.source_join_keys)
            .map_err(|e| format!("serialize source_join_keys: {}", e))?;
        let n_sjk_sources = tmp_plan.source_join_keys.len();

        client
            .update(
                "UPDATE public.__reflex_ivm_reference \
                 SET aggregations = jsonb_set( \
                       jsonb_set( \
                         jsonb_set( \
                           aggregations::jsonb, \
                           '{imv_relevant_columns}', \
                           $1::jsonb, \
                           TRUE \
                         ), \
                         '{imv_relevant_where}', \
                         $2::jsonb, \
                         TRUE \
                       ), \
                       '{source_join_keys}', \
                       $4::jsonb, \
                       TRUE \
                     )::json \
                 WHERE name = $3",
                None,
                &[
                    unsafe { DatumWithOid::new(cols_json, PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(where_json, PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe {
                        DatumWithOid::new(
                            view_name.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe { DatumWithOid::new(sjk_json, PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            )
            .map_err(|e| format!("update aggregations: {}", e))?;
        Ok((n_cols_sources, n_where_sources, n_sjk_sources))
    });
    match result {
        Ok((n_cols, n_where, n_sjk)) => format!(
            "pg_reflex: rebuilt metadata for '{}' ({} relevant_columns sources, {} relevant_where sources, {} source_join_keys sources)",
            view_name, n_cols, n_where, n_sjk
        ),
        Err(e) => format!("ERROR: {}", e),
    }
}

/// 1.4.5 — `reflex_rebuild_triggers(source_table TEXT) RETURNS TEXT`.
///
/// Re-emits the four CREATE OR REPLACE FUNCTION + CREATE OR REPLACE TRIGGER
/// statements for INSERT/DELETE/UPDATE/TRUNCATE on `source_table`, picking
/// up the latest codegen (e.g. the 1.4.5 filter-aware spurious-skip block).
///
/// Idempotent — `CREATE OR REPLACE` overwrites the existing function body
/// without changing trigger identity.
///
/// 1.4.6 — when `source_table` arrives unqualified (`'demand_planning'` from
/// a legacy `depends_on` array), resolve the actual schema via `pg_class`
/// instead of letting the DDL fall through to the caller's `search_path`.
/// Without this, the 1.4.6 migration silently rebuilt triggers on
/// `public.demand_planning` (which doesn't exist) when the real source was
/// `alp.demand_planning`, then deferred-flush failures aborted every
/// UPDATE in the session. If the bare name resolves to multiple schemas,
/// return an error rather than guess.
///
/// Returns a status string.
pub(crate) fn reflex_rebuild_triggers_impl(source_table: &str) -> String {
    // Already schema-qualified: trust the caller.
    let resolved = if source_table.contains('.') {
        source_table.to_string()
    } else {
        // Bare name — resolve via pg_class. We restrict to ordinary tables
        // and materialized views (the two source kinds pg_reflex supports).
        let lookup = Spi::connect(|client| {
            client
                .select(
                    "SELECT n.nspname::TEXT AS schema_name \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.relname = $1 \
                       AND c.relkind IN ('r', 'm', 'p', 'f') \
                       AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
                     ORDER BY n.nspname",
                    None,
                    &[unsafe {
                        DatumWithOid::new(
                            source_table.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    }],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    row.get_by_name::<&str, _>("schema_name")
                        .unwrap_or(None)
                        .map(|s| s.to_string())
                })
                .collect::<Vec<_>>()
        });
        match lookup.len() {
            0 => {
                return format!(
                    "ERROR: source table '{}' not found in any user schema — \
                     update __reflex_ivm_reference.depends_on with the qualified name",
                    source_table
                );
            }
            1 => format!("{}.{}", lookup[0], source_table),
            _ => {
                return format!(
                    "ERROR: source name '{}' is ambiguous (found in schemas: {}); \
                     update __reflex_ivm_reference.depends_on with the qualified name",
                    source_table,
                    lookup.join(", ")
                );
            }
        }
    };

    let result: Result<usize, String> = Spi::connect_mut(|client| {
        // 1.6.2: pick the correct trigger flavour by inspecting the live
        // dependency graph.  If any enabled IMV depending on this source is
        // DEFERRED, the source's trigger function must use the deferred
        // body (which stages into `__reflex_delta_<src>`); otherwise the
        // immediate body suffices.  Pre-1.6.2 this always emitted the
        // immediate body, which silently broke deferred IMVs whose trigger
        // had been rebuilt via this entry point.
        let has_deferred = client
            .select(
                "SELECT 1 FROM public.__reflex_ivm_reference \
                 WHERE $1 = ANY(depends_on) \
                   AND refresh_mode = 'DEFERRED' \
                   AND enabled = TRUE \
                 LIMIT 1",
                None,
                &[unsafe {
                    DatumWithOid::new(resolved.clone(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .map_err(|e| format!("dependency lookup failed: {}", e))?
            .next()
            .is_some();
        let ddls = if has_deferred {
            let cols = fetch_source_columns(client, &resolved);
            crate::schema_builder::build_deferred_trigger_ddls(&resolved, &cols)
        } else {
            crate::schema_builder::build_trigger_ddls(&resolved)
        };
        for ddl in &ddls {
            client
                .update(ddl, None, &[])
                .map_err(|e| format!("CREATE TRIGGER failed: {}", e))?;
        }
        Ok(ddls.len())
    });
    match result {
        Ok(n) => format!("pg_reflex: rebuilt {} trigger DDL(s) for '{}'", n, resolved),
        Err(e) => format!("ERROR: {}", e),
    }
}

/// Map information_schema data_type strings to PostgreSQL type names usable in DDL.
fn map_information_schema_type(data_type: &str) -> String {
    // `format_type` appends type modifiers (`numeric(10,2)`, `character
    // varying(255)`); strip them so the base type matches the arms below.
    // information_schema spellings have no parens, so this is a no-op for them.
    let data_type = data_type.split('(').next().unwrap_or(data_type).trim();
    match data_type {
        "integer" => "INTEGER".to_string(),
        "bigint" => "BIGINT".to_string(),
        "smallint" => "SMALLINT".to_string(),
        "numeric" => "NUMERIC".to_string(),
        "real" => "REAL".to_string(),
        "double precision" => "DOUBLE PRECISION".to_string(),
        "boolean" => "BOOLEAN".to_string(),
        "date" => "DATE".to_string(),
        "timestamp without time zone" => "TIMESTAMP".to_string(),
        "timestamp with time zone" => "TIMESTAMPTZ".to_string(),
        "character varying" => "TEXT".to_string(),
        "character" => "TEXT".to_string(),
        "text" => "TEXT".to_string(),
        "uuid" => "UUID".to_string(),
        "json" => "JSON".to_string(),
        "jsonb" => "JSONB".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Augment the column_types map with actual types from the base_query output.
/// This handles computed expressions (DATE_TRUNC, EXTRACT, COALESCE, etc.)
/// whose types cannot be resolved from the source table catalog alone.
///
/// Creates a temporary view from the base_query, reads column types from
/// pg_attribute, then drops the view.
fn augment_column_types_from_query(base_query: &str, column_types: &mut HashMap<String, String>) {
    Spi::connect_mut(|client| {
        let tmp = "__reflex_typecheck_view";
        let create = format!("CREATE TEMP VIEW {} AS {}", tmp, base_query);
        if client.update(&create, None, &[]).is_err() {
            return;
        }
        let rows = client
            .select(
                "SELECT a.attname::text AS col_name, \
                        format_type(a.atttypid, a.atttypmod)::text AS col_type \
                 FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 WHERE c.relname = $1 AND a.attnum > 0 AND NOT a.attisdropped",
                None,
                &[unsafe {
                    DatumWithOid::new(tmp.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report();
        for row in rows {
            if let (Some(col_name), Some(col_type)) = (
                row.get_by_name::<String, _>("col_name").unwrap_or(None),
                row.get_by_name::<String, _>("col_type").unwrap_or(None),
            ) {
                let pg_type = map_information_schema_type(&col_type);
                column_types
                    .entry(col_name)
                    .or_insert_with(|| pg_type.to_string());
            }
        }
        let _ = client.update(&format!("DROP VIEW IF EXISTS {}", tmp), None, &[]);
    });
}

#[cfg(test)]
#[path = "tests/unit_create_ivm.rs"]
mod tests;
