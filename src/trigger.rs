use pgrx::prelude::*;
use pgrx::spi::Spi;
use pgrx::PgBuiltInOids;
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;

use crate::aggregation::AggregationPlan;
use crate::query_decomposer::{intermediate_table_name, normalized_column_name, quote_identifier, replace_identifier, split_qualified_name};

/// Whether a delta adds or subtracts from the intermediate table.
#[derive(Clone, Copy)]
pub enum DeltaOp {
    Add,
    Subtract,
}

/// Build a MERGE statement that merges a delta query into the intermediate table.
/// MERGE is 3-4x faster than INSERT...ON CONFLICT because it uses a hash join
/// strategy instead of per-row index probes for conflict resolution.
pub fn build_merge_sql(
    intermediate_tbl: &str,
    delta_query: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) -> String {
    // Join columns = group_by + distinct (normalized lowercase names)
    let mut join_cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| format!("\"{}\"", normalized_column_name(c)))
        .collect();

    // For aggregates without GROUP BY: use sentinel column
    if join_cols.is_empty() && !plan.intermediate_columns.is_empty() {
        join_cols.push("__reflex_group".to_string());
    }

    let operator = match op {
        DeltaOp::Add => "+",
        DeltaOp::Subtract => "-",
    };

    // ON clause: t."col" = d."col" for each join column
    let on_clause = join_cols
        .iter()
        .map(|c| format!("t.{} = d.{}", c, c))
        .collect::<Vec<_>>()
        .join(" AND ");

    // WHEN MATCHED THEN UPDATE SET clauses
    let mut set_clauses: Vec<String> = Vec::new();
    for ic in &plan.intermediate_columns {
        match (ic.source_aggregate.as_str(), op) {
            ("MIN", DeltaOp::Add) => {
                set_clauses.push(format!(
                    "\"{}\" = LEAST(t.\"{}\", d.\"{}\")",
                    ic.name, ic.name, ic.name
                ));
            }
            ("MAX", DeltaOp::Add) => {
                set_clauses.push(format!(
                    "\"{}\" = GREATEST(t.\"{}\", d.\"{}\")",
                    ic.name, ic.name, ic.name
                ));
            }
            ("BOOL_OR", DeltaOp::Add) => {
                set_clauses.push(format!(
                    "\"{}\" = t.\"{}\" OR d.\"{}\"",
                    ic.name, ic.name, ic.name
                ));
            }
            ("MIN", DeltaOp::Subtract) | ("MAX", DeltaOp::Subtract) | ("BOOL_OR", DeltaOp::Subtract) => {
                set_clauses.push(format!("\"{}\" = NULL", ic.name));
            }
            _ => {
                set_clauses.push(format!(
                    "\"{}\" = t.\"{}\" {} d.\"{}\"",
                    ic.name, ic.name, operator, ic.name
                ));
            }
        }
    }
    if plan.needs_ivm_count {
        set_clauses.push(format!(
            "__ivm_count = t.__ivm_count {} d.__ivm_count",
            operator
        ));
    }

    // WHEN NOT MATCHED THEN INSERT: all columns with values from d
    let mut insert_cols: Vec<String> = join_cols.clone();
    for ic in &plan.intermediate_columns {
        insert_cols.push(format!("\"{}\"", ic.name));
    }
    if plan.needs_ivm_count {
        insert_cols.push("__ivm_count".to_string());
    }

    let insert_vals: Vec<String> = insert_cols
        .iter()
        .map(|c| format!("d.{}", c))
        .collect();

    // For Subtract: omit WHEN NOT MATCHED (can't subtract from non-existent group)
    let not_matched = match op {
        DeltaOp::Add => format!(
            " WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
            insert_cols.join(", "),
            insert_vals.join(", ")
        ),
        DeltaOp::Subtract => String::new(),
    };

    format!(
        "MERGE INTO {} AS t USING ({}) AS d ON {} WHEN MATCHED THEN UPDATE SET {}{}",
        intermediate_tbl,
        delta_query,
        on_clause,
        set_clauses.join(", "),
        not_matched
    )
}

/// Build a net-delta query for UPDATE: combines old (negated) and new transition tables
/// into a single aggregated delta. Halves the MERGE count for SUM/COUNT aggregates.
///
/// Produces: SELECT group_cols, SUM(CASE WHEN __op='N' THEN val ELSE -val END) AS __sum_val, ...
///           FROM (SELECT 'N', * FROM new_tbl UNION ALL SELECT 'O', * FROM old_tbl) GROUP BY ...
fn build_net_delta_query(
    delta_old: &str,
    delta_new: &str,
    plan: &AggregationPlan,
) -> String {
    // Extract the GROUP BY columns and aggregate expressions from the base query pattern.
    // The delta queries look like: SELECT group_col, SUM(amount) AS __sum_amount, COUNT(*) AS __ivm_count FROM transition_table GROUP BY group_col
    // We need to rewrite them into a net-delta form.
    //
    // Approach: UNION ALL the new (positive) and old (negated) delta queries, then re-aggregate.
    // The outer SELECT uses the same GROUP BY and sums the results — since old values are
    // negated in the subtract query, the net effect is (new - old) per group.
    //
    // For SUM: SUM(val_from_new) + SUM(-val_from_old) = net delta
    // For COUNT: COUNT(new) - COUNT(old) = net ivm_count delta
    //
    // We achieve this by treating the Add delta as positive and using the Subtract delta
    // which already produces negative aggregates via the MERGE subtract path.
    // But actually, both delta queries produce POSITIVE aggregates — the negation
    // happens in the MERGE SET clause (t.col - d.col for subtract).
    //
    // Simplest correct approach: just wrap both in a UNION ALL and re-aggregate.
    // The new delta contributes positively, the old delta contributes negatively.

    // Build group column list
    let mut grp_cols: Vec<String> = plan.group_by_columns.iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| format!("\"{}\"", normalized_column_name(c)))
        .collect();

    // For aggregates without GROUP BY: use sentinel column
    let needs_sentinel = grp_cols.is_empty() && !plan.intermediate_columns.is_empty();
    if needs_sentinel {
        grp_cols.push("__reflex_group".to_string());
    }

    let grp_select = if grp_cols.is_empty() {
        String::new()
    } else {
        format!("{}, ", grp_cols.join(", "))
    };

    let grp_by = if grp_cols.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", grp_cols.join(", "))
    };

    // Build aggregate expressions: for each intermediate column, compute net delta
    let mut agg_exprs: Vec<String> = Vec::new();
    for ic in &plan.intermediate_columns {
        // SUM/COUNT: net = positive from new + negative from old
        agg_exprs.push(format!(
            "SUM(CASE WHEN __reflex_sign = 1 THEN \"{}\" ELSE -\"{}\" END) AS \"{}\"",
            ic.name, ic.name, ic.name
        ));
    }
    if plan.needs_ivm_count {
        agg_exprs.push(
            "SUM(CASE WHEN __reflex_sign = 1 THEN __ivm_count ELSE -__ivm_count END) AS __ivm_count".to_string()
        );
    }

    let agg_select = agg_exprs.join(", ");

    // The inner UNION ALL: new delta (sign=+1) UNION ALL old delta (sign=-1)
    let sentinel_col = if needs_sentinel { ", 0 AS __reflex_group" } else { "" };
    format!(
        "SELECT {grp_select}{agg_select} FROM (\
            SELECT 1 AS __reflex_sign, __d.*{sentinel_col} FROM ({delta_new}) AS __d \
            UNION ALL \
            SELECT -1 AS __reflex_sign, __d.*{sentinel_col} FROM ({delta_old}) AS __d\
         ) AS __net{grp_by}"
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
        .filter(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX" || ic.source_aggregate == "BOOL_OR")
        .collect();

    if min_max_cols.is_empty() {
        return None;
    }

    let group_cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| normalized_column_name(c))
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


/// Build the group column list for targeted refresh.
/// Returns quoted column names from group_by + distinct columns (bare names).
/// Returns None if there are no group columns (sentinel-only case).
fn group_columns(plan: &AggregationPlan) -> Option<Vec<String>> {
    let cols: Vec<String> = plan
        .group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .map(|c| format!("\"{}\"", normalized_column_name(c)))
        .collect();
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}

/// Build SELECT DISTINCT clause for affected group columns.
fn affected_groups_select(cols: &[String]) -> String {
    cols.join(", ")
}

/// Build a row-value expression for WHERE ... IN clauses.
/// Single column: "col"   Multi-column: ("col1", "col2")
fn row_expr(cols: &[String]) -> String {
    if cols.len() == 1 {
        cols[0].clone()
    } else {
        format!("({})", cols.join(", "))
    }
}

/// Replace a source table reference in a base_query with a transition table name.
/// Handles both schema-qualified names (e.g., `alp.sales_simulation` in FROM)
/// and bare table names used as column qualifiers (e.g., `sales_simulation.product_id`).
fn replace_source_with_transition(base_query: &str, source_table: &str, transition_tbl: &str) -> String {
    let quoted_tbl = format!("\"{}\"", transition_tbl);
    let replaced = base_query.replace(source_table, &quoted_tbl);
    // Also replace unqualified table name in column qualifiers
    let (_, bare_source) = split_qualified_name(source_table);
    if bare_source != source_table {
        // Only needed when source_table was schema-qualified
        replace_identifier(&replaced, bare_source, &quoted_tbl)
    } else {
        replaced
    }
}

/// Push MERGE + affected-groups population.
/// PG17+: single CTE with MERGE RETURNING (captures affected groups in one statement).
/// PG15/16: separate MERGE + SELECT DISTINCT from delta query (MERGE RETURNING unsupported).
fn push_merge_and_affected(
    stmts: &mut Vec<String>,
    merge_sql: &str,
    affected_tbl: &str,
    select_expr: &str,
    delta_query: &str,
    grp_cols: &[String],
) {
    #[cfg(not(any(feature = "pg15", feature = "pg16")))]
    {
        let _ = delta_query; // only used in PG15/16 fallback path
        let ret_cols = grp_cols.iter()
            .map(|c| format!("t.{}", c))
            .collect::<Vec<_>>()
            .join(", ");
        stmts.push(format!(
            "WITH __m AS ({} RETURNING {}) INSERT INTO \"{}\" SELECT DISTINCT {} FROM __m",
            merge_sql, ret_cols, affected_tbl, select_expr
        ));
    }
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    {
        let _ = grp_cols; // only used in PG17+ RETURNING path
        stmts.push(merge_sql.to_string());
        stmts.push(format!(
            "INSERT INTO \"{}\" SELECT DISTINCT {} FROM ({}) AS __d",
            affected_tbl, select_expr, delta_query
        ));
    }
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
        Err(_) => {
            pgrx::warning!("pg_reflex: invalid aggregations JSON for '{}'", view_name);
            return String::new();
        }
    };

    let intermediate_tbl = intermediate_table_name(view_name);
    let safe_src = source_table.replace('.', "_").replace('"', "");
    // Use the transition table names directly (no temp table copy needed).
    // Transition tables are visible in plpgsql EXECUTE context.
    let new_tbl = format!("__reflex_new_{}", safe_src);
    let old_tbl = format!("__reflex_old_{}", safe_src);

    let mut stmts: Vec<String> = Vec::new();

    if plan.is_passthrough {
        let qv = quote_identifier(view_name);
        // Look up per-source column mappings for targeted DELETE/UPDATE
        let mappings = plan.passthrough_key_mappings.get(source_table);
        match operation {
            "INSERT" => {
                let delta_q = replace_source_with_transition(base_query, source_table, &new_tbl);
                stmts.push(format!("INSERT INTO {} {}", qv, delta_q));
            }
            "DELETE" => {
                if let Some(mappings) = mappings {
                    // Targeted delete using per-source column mapping
                    let target_cols: Vec<String> =
                        mappings.iter().map(|(t, _)| format!("\"{}\"", t)).collect();
                    let source_cols: Vec<String> =
                        mappings.iter().map(|(_, s)| format!("\"{}\"", s)).collect();
                    let row = row_expr(&target_cols);
                    stmts.push(format!(
                        "DELETE FROM {} WHERE {} IN (SELECT {} FROM \"{}\")",
                        qv, row, source_cols.join(", "), old_tbl
                    ));
                } else {
                    // No mapping for this source: full refresh
                    stmts.push(format!("DELETE FROM {}", qv));
                    stmts.push(format!("INSERT INTO {} {}", qv, base_query));
                }
            }
            "UPDATE" => {
                if let Some(mappings) = mappings {
                    // Phase 1: delete old rows using per-source column mapping
                    let target_cols: Vec<String> =
                        mappings.iter().map(|(t, _)| format!("\"{}\"", t)).collect();
                    let source_cols: Vec<String> =
                        mappings.iter().map(|(_, s)| format!("\"{}\"", s)).collect();
                    let row = row_expr(&target_cols);
                    stmts.push(format!(
                        "DELETE FROM {} WHERE {} IN (SELECT {} FROM \"{}\")",
                        qv, row, source_cols.join(", "), old_tbl
                    ));
                    // Phase 2: insert new rows (base_query with source→transition)
                    let delta_new =
                        replace_source_with_transition(base_query, source_table, &new_tbl);
                    stmts.push(format!("INSERT INTO {} {}", qv, delta_new));
                } else {
                    // No mapping for this source: full refresh
                    stmts.push(format!("DELETE FROM {}", qv));
                    stmts.push(format!("INSERT INTO {} {}", qv, base_query));
                }
            }
            _ => {}
        }
    } else {
        let has_min_max = plan
            .intermediate_columns
            .iter()
            .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX" || ic.source_aggregate == "BOOL_OR");

        // Determine if we can use targeted refresh (need group columns)
        let grp_cols = group_columns(&plan);
        let bare_view = split_qualified_name(view_name).1;
        let affected_tbl = format!("__reflex_affected_{}", bare_view);

        match operation {
            "INSERT" => {
                let delta_q = replace_source_with_transition(base_query, source_table, &new_tbl);

                if let Some(ref cols) = grp_cols {
                    let select_expr = affected_groups_select(cols);
                    let merge_sql = build_merge_sql(&intermediate_tbl, &delta_q, &plan, DeltaOp::Add);
                    stmts.push(format!("TRUNCATE \"{}\"", affected_tbl));
                    push_merge_and_affected(&mut stmts, &merge_sql, &affected_tbl, &select_expr, &delta_q, cols);
                } else {
                    stmts.push(build_merge_sql(&intermediate_tbl, &delta_q, &plan, DeltaOp::Add));
                }
            }
            "DELETE" => {
                let delta_q = replace_source_with_transition(base_query, source_table, &old_tbl);

                if let Some(ref cols) = grp_cols {
                    let select_expr = affected_groups_select(cols);
                    let merge_sql = build_merge_sql(&intermediate_tbl, &delta_q, &plan, DeltaOp::Subtract);
                    stmts.push(format!("TRUNCATE \"{}\"", affected_tbl));
                    push_merge_and_affected(&mut stmts, &merge_sql, &affected_tbl, &select_expr, &delta_q, cols);
                } else {
                    stmts.push(build_merge_sql(&intermediate_tbl, &delta_q, &plan, DeltaOp::Subtract));
                }
                if has_min_max {
                    if let Some(recompute) = build_min_max_recompute_sql(&intermediate_tbl, &plan, source_table) {
                        stmts.push(recompute);
                    }
                }
            }
            "UPDATE" => {
                let delta_old = replace_source_with_transition(base_query, source_table, &old_tbl);
                let delta_new = replace_source_with_transition(base_query, source_table, &new_tbl);

                if has_min_max {
                    // MIN/MAX/BOOL_OR need two-phase: subtract → recompute → add.
                    // Can't use net-delta because MIN/MAX have no algebraic inverse.
                    if let Some(ref cols) = grp_cols {
                        let select_expr = affected_groups_select(cols);
                        let merge_sub_sql = build_merge_sql(&intermediate_tbl, &delta_old, &plan, DeltaOp::Subtract);
                        stmts.push(format!("TRUNCATE \"{}\"", affected_tbl));
                        push_merge_and_affected(&mut stmts, &merge_sub_sql, &affected_tbl, &select_expr, &delta_old, cols);
                        if let Some(recompute) = build_min_max_recompute_sql(&intermediate_tbl, &plan, source_table) {
                            stmts.push(recompute);
                        }
                        let merge_add_sql = build_merge_sql(&intermediate_tbl, &delta_new, &plan, DeltaOp::Add);
                        push_merge_and_affected(&mut stmts, &merge_add_sql, &affected_tbl, &select_expr, &delta_new, cols);
                    } else {
                        stmts.push(build_merge_sql(&intermediate_tbl, &delta_old, &plan, DeltaOp::Subtract));
                        if let Some(recompute) = build_min_max_recompute_sql(&intermediate_tbl, &plan, source_table) {
                            stmts.push(recompute);
                        }
                        stmts.push(build_merge_sql(&intermediate_tbl, &delta_new, &plan, DeltaOp::Add));
                    }
                } else if grp_cols.is_some() {
                    // No MIN/MAX + has group columns: use single-pass net-delta MERGE.
                    // Combines old (negated) + new into one delta, halving MERGE count.
                    let cols = grp_cols.as_ref().unwrap();
                    let net_delta = build_net_delta_query(&delta_old, &delta_new, &plan);
                    let select_expr = affected_groups_select(cols);
                    let merge_sql = build_merge_sql(&intermediate_tbl, &net_delta, &plan, DeltaOp::Add);
                    stmts.push(format!("TRUNCATE \"{}\"", affected_tbl));
                    push_merge_and_affected(&mut stmts, &merge_sql, &affected_tbl, &select_expr, &net_delta, cols);
                } else {
                    // No MIN/MAX, no group columns (sentinel): fall back to two-phase
                    stmts.push(build_merge_sql(&intermediate_tbl, &delta_old, &plan, DeltaOp::Subtract));
                    stmts.push(build_merge_sql(&intermediate_tbl, &delta_new, &plan, DeltaOp::Add));
                }
            }
            _ => {}
        }

        // Refresh target from intermediate
        if let Some(ref cols) = grp_cols {
            // Targeted refresh: only update groups that changed
            let cols_str = cols.join(", ");
            let row = row_expr(cols);

            let qv = quote_identifier(view_name);
            stmts.push(format!(
                "DELETE FROM {} WHERE {} IN (SELECT {} FROM \"{}\")",
                qv, row, cols_str, affected_tbl
            ));
            stmts.push(format!(
                "INSERT INTO {} {} AND {} IN (SELECT {} FROM \"{}\")",
                qv, end_query, row, cols_str, affected_tbl
            ));
        } else {
            // No group columns (sentinel-only): full refresh
            stmts.push(format!("TRUNCATE {}", quote_identifier(view_name)));
            stmts.push(format!("INSERT INTO {} {}", quote_identifier(view_name), end_query));
        }
    }

    // Update last_update_date
    stmts.push(format!(
        "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = '{}'",
        view_name.replace("'", "''")
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
        view_name.replace("'", "''")
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
        stmts.push(format!("DELETE FROM {}", quote_identifier(view_name)));
    } else {
        // Aggregate: clear intermediate and target
        stmts.push(format!("TRUNCATE {}", intermediate_tbl));
        stmts.push(format!("DELETE FROM {}", quote_identifier(view_name)));
    }

    // Update last_update_date
    stmts.push(format!(
        "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = '{}'",
        view_name.replace("'", "''")
    ));

    stmts.join("\n--<<REFLEX_SEP>>--\n")
}

/// Flushes all accumulated deferred deltas for a given source table.
///
/// Called by the deferred constraint trigger at COMMIT time.
/// Reads from the staging table (__reflex_delta_<source>), applies deltas
/// to each DEFERRED IMV, then cleans up staging and pending rows.
#[pg_extern]
pub fn reflex_flush_deferred(source_table: &str) -> String {
    let safe_src = source_table.replace('.', "_").replace('"', "");
    let delta_tbl = format!("__reflex_delta_{}", safe_src);

    // Read all DEFERRED IMVs that depend on this source
    let imvs: Vec<(String, String, String, String)> = Spi::connect(|client| {
        let args = [unsafe {
            DatumWithOid::new(
                source_table.to_string(),
                PgBuiltInOids::TEXTOID.oid().value(),
            )
        }];
        client
            .select(
                "SELECT name, base_query, end_query, aggregations::text AS aggregations \
                 FROM public.__reflex_ivm_reference \
                 WHERE $1 = ANY(depends_on) AND enabled = TRUE \
                   AND COALESCE(refresh_mode, 'IMMEDIATE') = 'DEFERRED' \
                 ORDER BY graph_depth",
                None,
                &args,
            )
            .unwrap_or_report()
            .map(|row| {
                (
                    row.get_by_name::<&str, _>("name").unwrap_or(None).unwrap_or("").to_string(),
                    row.get_by_name::<&str, _>("base_query").unwrap_or(None).unwrap_or("").to_string(),
                    row.get_by_name::<&str, _>("end_query").unwrap_or(None).unwrap_or("").to_string(),
                    row.get_by_name::<&str, _>("aggregations").unwrap_or(None).unwrap_or("{}").to_string(),
                )
            })
            .collect()
    });

    if imvs.is_empty() {
        return "NO DEFERRED IMVS".to_string();
    }

    let mut total_processed = 0usize;

    Spi::connect_mut(|client| {
        // Check if staging table has any rows
        let has_rows = client
            .select(
                &format!("SELECT EXISTS(SELECT 1 FROM {} LIMIT 1) AS has", delta_tbl),
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .map(|row| row.get_by_name::<bool, _>("has").unwrap_or(None).unwrap_or(false))
            .unwrap_or(false);

        if !has_rows {
            // No deltas to process — clean up pending rows
            client
                .update(
                    &format!(
                        "DELETE FROM public.__reflex_deferred_pending WHERE source_table = '{}'",
                        source_table.replace("'", "''")
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
            return;
        }

        for (imv_name, base_query, end_query, agg_json) in &imvs {
            // Acquire advisory lock for this IMV
            client
                .update(
                    &format!("SELECT pg_advisory_xact_lock(hashtext('{}'))", imv_name.replace("'", "''")),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // Process INSERT deltas (op = 'I')
            let ins_base = base_query.replace(
                source_table,
                &format!("(SELECT * FROM {} WHERE __reflex_op = 'I') AS __dt", delta_tbl),
            );
            let ins_sql = reflex_build_delta_sql(imv_name, source_table, "INSERT", &ins_base, end_query, agg_json);
            if !ins_sql.is_empty() {
                for stmt in ins_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        client.update(stmt, None, &[]).unwrap_or_report();
                    }
                }
                total_processed += 1;
            }

            // Process DELETE deltas (op = 'D')
            let del_base = base_query.replace(
                source_table,
                &format!("(SELECT * FROM {} WHERE __reflex_op = 'D') AS __dt", delta_tbl),
            );
            let del_sql = reflex_build_delta_sql(imv_name, source_table, "DELETE", &del_base, end_query, agg_json);
            if !del_sql.is_empty() {
                for stmt in del_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        client.update(stmt, None, &[]).unwrap_or_report();
                    }
                }
                total_processed += 1;
            }

            // Process UPDATE deltas: U_OLD as DELETE, U_NEW as INSERT
            let upd_old_base = base_query.replace(
                source_table,
                &format!("(SELECT * FROM {} WHERE __reflex_op = 'U_OLD') AS __dt", delta_tbl),
            );
            let upd_old_sql = reflex_build_delta_sql(imv_name, source_table, "DELETE", &upd_old_base, end_query, agg_json);
            if !upd_old_sql.is_empty() {
                for stmt in upd_old_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        client.update(stmt, None, &[]).unwrap_or_report();
                    }
                }
            }

            let upd_new_base = base_query.replace(
                source_table,
                &format!("(SELECT * FROM {} WHERE __reflex_op = 'U_NEW') AS __dt", delta_tbl),
            );
            let upd_new_sql = reflex_build_delta_sql(imv_name, source_table, "INSERT", &upd_new_base, end_query, agg_json);
            if !upd_new_sql.is_empty() {
                for stmt in upd_new_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        client.update(stmt, None, &[]).unwrap_or_report();
                    }
                }
                total_processed += 1;
            }
        }

        // Clean up: truncate staging table and remove pending rows
        client
            .update(&format!("TRUNCATE {}", delta_tbl), None, &[])
            .unwrap_or_report();
        client
            .update(
                &format!(
                    "DELETE FROM public.__reflex_deferred_pending WHERE source_table = '{}'",
                    source_table.replace("'", "''")
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    });

    format!("FLUSHED {} DEFERRED OPERATIONS", total_processed)
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
                cast_type: None,
            }],
            has_distinct: false,
            needs_ivm_count: true,
            distinct_columns: vec![],
            is_passthrough: false,
            passthrough_columns: vec![],
            passthrough_key_mappings: std::collections::HashMap::new(),
            having_clause: None,
        }
    }

    #[test]
    fn test_build_merge_add() {
        let plan = simple_plan();
        let delta = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"__reflex_new_v\" GROUP BY city";
        let sql = build_merge_sql("__reflex_intermediate_v", delta, &plan, DeltaOp::Add);
        assert!(sql.contains("MERGE INTO __reflex_intermediate_v AS t"));
        assert!(sql.contains("t.\"city\" = d.\"city\""));
        assert!(sql.contains("t.\"__sum_amount\" + d.\"__sum_amount\""));
        assert!(sql.contains("t.__ivm_count + d.__ivm_count"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    #[test]
    fn test_build_merge_subtract() {
        let plan = simple_plan();
        let delta = "SELECT city, SUM(amount) AS \"__sum_amount\", COUNT(*) AS __ivm_count FROM \"__reflex_old_v\" GROUP BY city";
        let sql = build_merge_sql("__reflex_intermediate_v", delta, &plan, DeltaOp::Subtract);
        assert!(sql.contains("t.\"__sum_amount\" - d.\"__sum_amount\""));
        assert!(sql.contains("t.__ivm_count - d.__ivm_count"));
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
        let result = replace_source_with_transition(base_query, "alp.sales_simulation", "__reflex_new_alp_sales_simulation");
        // FROM clause should be replaced
        assert!(result.contains("\"__reflex_new_alp_sales_simulation\""), "FROM clause not replaced");
        // Column qualifiers should be replaced
        assert!(!result.contains(" sales_simulation.product_id"), "Column qualifier not replaced: {}", result);
        assert!(!result.contains(" sales_simulation.dem_plan_id"), "JOIN qualifier not replaced: {}", result);
        // Other tables should NOT be replaced
        assert!(result.contains("alp.demand_planning"), "Other tables should not be affected");
        assert!(result.contains("demand_planning.id"), "Other table qualifiers should not be affected");
    }

    #[test]
    fn test_replace_source_with_transition_unqualified() {
        let base_query = "SELECT city, SUM(amount) FROM orders GROUP BY city";
        let result = replace_source_with_transition(base_query, "orders", "__reflex_new_orders");
        assert!(result.contains("\"__reflex_new_orders\""));
        assert!(!result.contains(" orders "));
    }
}
