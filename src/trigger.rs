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

    // ON clause: IS NOT DISTINCT FROM handles NULL group keys correctly
    // (NULL = NULL is false, but NULL IS NOT DISTINCT FROM NULL is true).
    // Same performance as = (uses same B-tree/hash index).
    let on_clause = join_cols
        .iter()
        .map(|c| format!("t.{} IS NOT DISTINCT FROM d.{}", c, c))
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
                // COALESCE handles NULL in delta (e.g., SUM(NULL)=NULL but we need 0).
                // Use type-appropriate default: 0 for numeric, FALSE for boolean.
                let default_val = if ic.pg_type == "BOOLEAN" { "FALSE" } else { "0" };
                set_clauses.push(format!(
                    "\"{}\" = COALESCE(t.\"{}\", {}) {} COALESCE(d.\"{}\", {})",
                    ic.name, ic.name, default_val, operator, ic.name, default_val
                ));
            }
        }
    }
    if plan.needs_ivm_count {
        set_clauses.push(format!(
            "__ivm_count = COALESCE(t.__ivm_count, 0) {} COALESCE(d.__ivm_count, 0)",
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

    // Determine default values for INSERT COALESCE based on column types.
    // MIN/MAX columns should NOT be coalesced — NULL is valid (means "no value").
    // Only SUM/COUNT need COALESCE to 0 (NULL + 0 = 0, not NULL).
    let insert_vals: Vec<String> = insert_cols
        .iter()
        .map(|c| {
            if c.starts_with("\"__") || c == "__ivm_count" {
                // Check if this is a MIN/MAX column — don't coalesce
                let is_min_max = plan.intermediate_columns.iter()
                    .any(|ic| format!("\"{}\"", ic.name) == *c
                        && (ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX"));
                if is_min_max {
                    format!("d.{}", c) // No COALESCE for MIN/MAX
                } else {
                    let is_bool = plan.intermediate_columns.iter()
                        .any(|ic| format!("\"{}\"", ic.name) == *c && ic.pg_type == "BOOLEAN");
                    let default_val = if is_bool { "FALSE" } else { "0" };
                    format!("COALESCE(d.{}, {})", c, default_val)
                }
            } else {
                format!("d.{}", c)
            }
        })
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
        // SUM/COUNT: net = positive from new + negative from old. COALESCE for NULL safety.
        agg_exprs.push(format!(
            "SUM(CASE WHEN __reflex_sign = 1 THEN COALESCE(\"{}\", 0) ELSE -COALESCE(\"{}\", 0) END) AS \"{}\"",
            ic.name, ic.name, ic.name
        ));
    }
    if plan.needs_ivm_count {
        agg_exprs.push(
            "SUM(CASE WHEN __reflex_sign = 1 THEN COALESCE(__ivm_count, 0) ELSE -COALESCE(__ivm_count, 0) END) AS __ivm_count".to_string()
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


/// Build a NULL-safe match condition for affected groups.
/// Uses EXISTS with IS NOT DISTINCT FROM instead of IN (which fails for NULL keys).
/// `target_alias` is the table being filtered (e.g., target table or intermediate).
/// `affected_tbl` is the affected-groups table.
/// `cols` are the group column names (quoted).
/// `cols` are the group column names (quoted).
fn null_safe_in(affected_tbl: &str, cols: &[String]) -> String {
    let conditions: Vec<String> = cols.iter()
        .map(|c| format!("{} IS NOT DISTINCT FROM __a.{}", c, c))
        .collect();
    format!(
        "EXISTS (SELECT 1 FROM \"{}\" AS __a WHERE {})",
        affected_tbl, conditions.join(" AND ")
    )
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
    // Use word-boundary-aware replacement to avoid corrupting column names
    // that contain the source table name as a substring (e.g., __bool_or_flag
    // contains "bo" when the source table is "bo").
    let replaced = replace_identifier(base_query, source_table, &quoted_tbl);
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

    // Pre-compute group columns and affected-groups table name (used by multiple paths)
    let grp_cols = group_columns(&plan);
    let bare_view = split_qualified_name(view_name).1;
    let affected_tbl = format!("__reflex_affected_{}", bare_view);

    // Detect cases where standard incremental delta is incorrect:
    // 1. Self-join: source_table appears multiple times in base_query
    // 2. LEFT/RIGHT JOIN secondary table DELETE/UPDATE: NULL semantics can't be captured by MERGE subtract
    let bare_source = split_qualified_name(source_table).1;
    // Detect self-join and outer-join-secondary for BOTH aggregate and passthrough queries.
    let occurrences = base_query.split_whitespace()
        .filter(|w| {
            let trimmed = w.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
            trimmed == source_table || trimmed == bare_source
        })
        .count();
    let is_self_join = occurrences > 1;

    let bq_upper = base_query.to_uppercase();
    let is_full_outer = bq_upper.contains("FULL JOIN") || bq_upper.contains("FULL OUTER");
    let is_outer_join_secondary = !is_self_join
        && (bq_upper.contains("LEFT JOIN") || bq_upper.contains("RIGHT JOIN")
            || bq_upper.contains("LEFT OUTER") || bq_upper.contains("RIGHT OUTER")
            || is_full_outer)
        && bq_upper.find("JOIN").is_some_and(|pos| {
            let after = &base_query[pos..];
            after.contains(source_table) || after.contains(bare_source)
        })
        // For LEFT/RIGHT JOIN: only DELETE/UPDATE need special handling (INSERT is correct).
        // For FULL OUTER JOIN: ALL operations need full refresh (both sides produce NULLs).
        && (operation == "DELETE" || operation == "UPDATE" || is_full_outer);

    if is_self_join {
        // Self-join: full refresh (delta itself is wrong — both aliases get replaced).
        let qv = quote_identifier(view_name);
        if plan.is_passthrough {
            stmts.push(format!("DELETE FROM {}", qv));
            stmts.push(format!("INSERT INTO {} {}", qv, base_query));
        } else {
            stmts.push(format!("TRUNCATE {}", intermediate_tbl));
            stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
            if end_query.is_empty() {
                stmts.push(format!("TRUNCATE {}", qv));
                stmts.push(format!("INSERT INTO {} {}", qv, base_query));
            } else {
                stmts.push(format!("TRUNCATE {}", qv));
                stmts.push(format!("INSERT INTO {} {}", qv, end_query));
            }
        }
    } else if is_outer_join_secondary && plan.is_passthrough {
        // Passthrough outer-join secondary: full refresh from source
        let qv = quote_identifier(view_name);
        stmts.push(format!("DELETE FROM {}", qv));
        stmts.push(format!("INSERT INTO {} {}", qv, base_query));
    } else if is_outer_join_secondary && !plan.is_passthrough {
        // LEFT/RIGHT JOIN secondary table DELETE/UPDATE: targeted group reconcile.
        // The delta correctly identifies WHICH groups changed (affected groups),
        // but the MERGE subtract produces wrong values (can't represent NULL from LEFT JOIN).
        // Fix: extract affected groups from delta, delete them from intermediate,
        // re-insert ONLY those groups from the full base_query.
        if let Some(ref cols) = grp_cols {
            let select_expr = affected_groups_select(cols);
            let qv = quote_identifier(view_name);

            // Determine transition table for affected group extraction
            let transition = if operation == "DELETE" { &old_tbl } else { &new_tbl };
            // Build a delta query to extract group keys from transition table
            let delta_q = replace_source_with_transition(base_query, source_table, transition);

            // Create affected groups table
            stmts.push(format!("TRUNCATE \"{}\"", affected_tbl));

            // Extract affected groups from delta
            #[cfg(not(any(feature = "pg15", feature = "pg16")))]
            {
                // PG17+: use a lightweight SELECT from the delta to get group keys
                stmts.push(format!(
                    "INSERT INTO \"{}\" SELECT DISTINCT {} FROM ({}) AS __d",
                    affected_tbl, select_expr, delta_q
                ));
            }
            #[cfg(any(feature = "pg15", feature = "pg16"))]
            {
                stmts.push(format!(
                    "INSERT INTO \"{}\" SELECT DISTINCT {} FROM ({}) AS __d",
                    affected_tbl, select_expr, delta_q
                ));
            }

            // Delete affected groups from intermediate (NULL-safe)
            let ns_in_int = null_safe_in(&affected_tbl, cols);
            stmts.push(format!("DELETE FROM {} WHERE {}", intermediate_tbl, ns_in_int));

            // Re-insert ONLY affected groups from the FULL base_query (reads real source).
            let ns_in_full = null_safe_in(&affected_tbl, cols);
            stmts.push(format!(
                "INSERT INTO {} SELECT * FROM ({}) AS __full WHERE {}",
                intermediate_tbl, base_query, ns_in_full
            ));

            // Targeted refresh of target (NULL-safe)
            let ns_in_tgt = null_safe_in(&affected_tbl, cols);
            stmts.push(format!("DELETE FROM {} WHERE {}", qv, ns_in_tgt));
            stmts.push(format!("INSERT INTO {} {} AND {}", qv, end_query, ns_in_tgt
            ));
        } else {
            // No group columns: full refresh
            stmts.push(format!("TRUNCATE {}", intermediate_tbl));
            stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
            stmts.push(format!("TRUNCATE {}", quote_identifier(view_name)));
            if end_query.is_empty() {
                stmts.push(format!("INSERT INTO {} {}", quote_identifier(view_name), base_query));
            } else {
                stmts.push(format!("INSERT INTO {} {}", quote_identifier(view_name), end_query));
            }
        }
    } else if plan.is_passthrough {
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

        // Refresh target from intermediate.
        // For COUNT(DISTINCT): end_query has a GROUP BY re-aggregation, so targeted refresh
        // (appending AND to end_query) doesn't work. Use full target refresh instead.
        let end_query_has_group_by = end_query.to_uppercase().contains("GROUP BY");
        if end_query_has_group_by {
            // Full target refresh — correct for COUNT(DISTINCT) and other re-aggregating end queries
            let qv = quote_identifier(view_name);
            stmts.push(format!("DELETE FROM {}", qv));
            stmts.push(format!("INSERT INTO {} {}", qv, end_query));
        } else if let Some(ref cols) = grp_cols {
            // Targeted refresh (NULL-safe via IS NOT DISTINCT FROM)
            let qv = quote_identifier(view_name);
            let ns_in = null_safe_in(&affected_tbl, cols);
            stmts.push(format!("DELETE FROM {} WHERE {}", qv, ns_in));
            stmts.push(format!("INSERT INTO {} {} AND {}", qv, end_query, ns_in));
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
            let ins_staging = format!("(SELECT * FROM {} WHERE __reflex_op = 'I') AS __dt", delta_tbl);
            let ins_base = replace_identifier(base_query, source_table, &ins_staging);
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
            let del_staging = format!("(SELECT * FROM {} WHERE __reflex_op = 'D') AS __dt", delta_tbl);
            let del_base = replace_identifier(base_query, source_table, &del_staging);
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
            let upd_old_staging = format!("(SELECT * FROM {} WHERE __reflex_op = 'U_OLD') AS __dt", delta_tbl);
            let upd_old_base = replace_identifier(base_query, source_table, &upd_old_staging);
            let upd_old_sql = reflex_build_delta_sql(imv_name, source_table, "DELETE", &upd_old_base, end_query, agg_json);
            if !upd_old_sql.is_empty() {
                for stmt in upd_old_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        client.update(stmt, None, &[]).unwrap_or_report();
                    }
                }
            }

            let upd_new_staging = format!("(SELECT * FROM {} WHERE __reflex_op = 'U_NEW') AS __dt", delta_tbl);
            let upd_new_base = replace_identifier(base_query, source_table, &upd_new_staging
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
#[path = "tests/unit_trigger.rs"]
mod tests;
