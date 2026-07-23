use super::*;
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::query_decomposer::{
    bare_column_name, quote_identifier, replace_identifier, safe_identifier,
};
use crate::sql_analyzer::analyze;
use crate::sql_writer::{add_graph_child_links, insert_registry_row, RegistryRow};
use crate::window;

/// Extract the output column names from a CTE's query.
/// Compute the subset of partition_by columns that appear in the CTE's output.
/// Case-insensitive comparison per PostgreSQL identifier folding.
pub(crate) fn compute_cte_partition_subset(
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
pub(crate) fn rollback_partial_sub_imvs(sub_imv_names: &[String]) {
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

/// Record that pg_reflex — not the user — created this node, as one step of
/// decomposing a single `create_reflex_ivm` call.
///
/// Written post-hoc rather than threaded through
/// `create_reflex_ivm_impl_with_materialization` as a twelfth argument: the
/// decomposer is the only caller that knows a node is generated, so the flag
/// belongs at the site that has that knowledge instead of smeared through a
/// signature four other create paths share. Runs in the same transaction as the
/// row's INSERT.
///
/// `reflex_reconcile` recurses into flagged nodes and only flagged nodes.
pub(crate) fn mark_generated_sub_imv(sub_imv_name: &str) {
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE public.__reflex_ivm_reference \
                    SET is_generated_sub_imv = TRUE WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        sub_imv_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report();
    });
}

/// `graph_depth` for a node whose IMV dependencies are `sub_imv_names`: one above
/// the deepest of them, or 1 when none of them is registered.
///
/// The decomposed paths used to hard-code this — `sub_imv_names.len() + 1` for
/// set ops, so a three-operand UNION ALL claimed depth 4 because of its arity
/// rather than its position, and a literal 2 for DISTINCT ON / window, wrong
/// whenever the `__base` sub-IMV is itself decomposed. Both disagreed with what
/// `resolve_existing_imv_deps` computes for the main create path and with what
/// `reflex_repair_dependency_graph` recomputes, which would have left
/// `graph_depth` meaning two different things depending on a row's age.
pub(crate) fn depth_above(
    client: &mut pgrx::spi::SpiClient<'_>,
    sub_imv_names: &[String],
) -> i32 {
    if sub_imv_names.is_empty() {
        return 1;
    }
    client
        .select(
            "SELECT COALESCE(MAX(graph_depth), 0) + 1 AS depth \
             FROM public.__reflex_ivm_reference WHERE name = ANY($1::TEXT[])",
            None,
            &[unsafe {
                DatumWithOid::new(
                    crate::query_decomposer::format_pg_text_array_literal(sub_imv_names),
                    PgBuiltInOids::TEXTOID.oid().value(),
                )
            }],
        )
        .unwrap_or_report()
        .first()
        .get_by_name::<i32, _>("depth")
        .unwrap_or(None)
        .unwrap_or(1)
}

/// Decomposition phase: UNION / INTERSECT / EXCEPT.
///
/// Each operand is materialised as its own sub-IMV (recursively); the user-
/// visible name becomes a `CREATE VIEW` over those sub-IMVs that PostgreSQL
/// evaluates on read. Returns `Some(result)` to short-circuit
/// [`create_reflex_ivm_impl`]; `None` if the query has no top-level set
/// operator.
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_decompose_set_op(ctx: &DecomposeCtx) -> Option<&'static str> {
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
            false, // sub-IMVs auto-mirror; never force-unpartitioned
        );
        if result.starts_with("ERROR") {
            rollback_partial_sub_imvs(&sub_imv_names);
            return Some(result);
        }
        mark_generated_sub_imv(&sub_name);
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
                let depth = depth_above(client, &depends_on_imv);
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
            let depth = depth_above(client, &depends_on_imv);
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
pub(crate) fn install_union_mirror_triggers(
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

    // Register the newly created functions as pg_reflex extension members.
    // Pass the RAW name (not `safe_identifier`): these fns are created above
    // with the raw `fn_*` name, so for names >63 chars PostgreSQL truncates them
    // naively to the first 63 chars. The ADD inside member_register_ddl truncates
    // the same naive way, so it resolves to the same function. `safe_identifier`
    // would instead produce a `<54chars>_<hash>` name that diverges from the
    // stored one and break registration — do NOT wrap these in it.
    for fn_name in [&fn_ins, &fn_del, &fn_upd] {
        client
            .update(
                &crate::schema_builder::member_register_ddl(fn_name),
                None,
                &[],
            )
            .unwrap_or_report();
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
pub(crate) fn install_union_all_intermediate_wrapper(
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
    let depth = depth_above(client, &depends_on_imv);
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
pub(crate) fn try_decompose_distinct_on(ctx: &DecomposeCtx) -> Option<&'static str> {
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

    // Create sub-IMV for the base data. The `__base` holds the PRE-dedup rows, so
    // its natural key is the source's own key — NOT the outer view's declared
    // `unique_columns`, which is the DISTINCT-ON output key (by definition NOT
    // unique in `__base`; forcing it crashes the `__base` unique index at CREATE).
    // Pass empty so `__base` auto-infers its source PK; the outer read-time VIEW
    // (ROW_NUMBER = 1) provides the DISTINCT-ON dedup.
    let base_name = format!("{}__base", ctx.view_name);
    let result = create_reflex_ivm_impl(
        &base_name,
        &base_sql,
        "",
        false,
        ctx.storage_mode,
        ctx.refresh_mode,
        ctx.topk_k,
        ctx.ignore_sources,
        ctx.partition_by,
        false, // sub-IMVs auto-mirror; never force-unpartitioned
    );
    if result.starts_with("ERROR") {
        return Some(result);
    }
    mark_generated_sub_imv(&base_name);

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
        let depth = depth_above(client, &depends_on_imv);
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
pub(crate) fn try_decompose_window(
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
        false, // sub-IMVs auto-mirror; never force-unpartitioned
    );
    if result.starts_with("ERROR") {
        return Some(result);
    }
    mark_generated_sub_imv(&base_name);

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
        let depth = depth_above(client, &depends_on_imv);
        insert_registry_row(
            client,
            &RegistryRow::decomposed(
                view_name,
                depth,
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
pub(crate) fn parse_unique_columns_spec(
    spec: &str,
) -> (String, std::collections::HashMap<String, String>) {
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
pub(crate) fn try_decompose_ctes(ctx: &DecomposeCtx) -> Option<&'static str> {
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
            false, // sub-IMVs auto-mirror; never force-unpartitioned
        );
        if result.starts_with("ERROR") {
            rollback_partial_sub_imvs(&created_sub_imv_names(&cte_name_map));
            return Some(result);
        }
        mark_generated_sub_imv(&cte_view_name);
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
        false, // sub-IMVs auto-mirror; never force-unpartitioned
    );
    if body_result.starts_with("ERROR") {
        rollback_partial_sub_imvs(&created_sub_imv_names(&cte_name_map));
    }
    Some(body_result)
}

/// The materialised sub-IMV names from a CTE decomposition's `(alias, name)` map,
/// in creation order — the input to [`rollback_partial_sub_imvs`].
pub(crate) fn created_sub_imv_names(cte_name_map: &[(String, String)]) -> Vec<String> {
    cte_name_map.iter().map(|(_, name)| name.clone()).collect()
}
