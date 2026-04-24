use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::Spi;
use pgrx::PgBuiltInOids;

use crate::aggregation::AggregationPlan;
use crate::query_decomposer::{
    delta_scratch_table_name, intermediate_table_name, normalized_column_name,
    passthrough_scratch_new_table_name, passthrough_scratch_old_table_name, quote_identifier,
    replace_identifier, replace_source_with_delta, safe_identifier, split_qualified_name,
    staging_delta_table_name, transition_new_table_name, transition_old_table_name,
};

/// Whether a delta adds or subtracts from the intermediate table.
#[derive(Clone, Copy)]
pub enum DeltaOp {
    Add,
    Subtract,
}

/// Build a MERGE statement that merges a delta query into the intermediate table.
/// Used directly by unit tests; production code goes through `push_materialized_merge`.
/// MERGE is 3-4x faster than INSERT...ON CONFLICT because it uses a hash join
/// strategy instead of per-row index probes for conflict resolution.
#[cfg_attr(not(test), allow(dead_code))]
pub fn build_merge_sql(
    intermediate_tbl: &str,
    delta_query: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) -> String {
    build_merge_using(intermediate_tbl, &format!("({})", delta_query), plan, op)
}

/// Like `build_merge_sql` but reads the delta from a pre-materialized table rather
/// than an inline subquery.  Use this when the delta may reference a transition
/// table — PostgreSQL's MERGE rejects transition-table references inside a USING
/// subquery executed via EXECUTE (triggers a PG cassert / SIGABRT on cassert builds).
fn build_merge_from_table_sql(
    intermediate_tbl: &str,
    scratch_tbl: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) -> String {
    build_merge_using(intermediate_tbl, &format!("\"{}\"", scratch_tbl), plan, op)
}

fn build_merge_using(
    intermediate_tbl: &str,
    using_clause: &str,
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
            ("MIN", DeltaOp::Subtract) | ("MAX", DeltaOp::Subtract) => {
                set_clauses.push(format!("\"{}\" = NULL", ic.name));
            }
            _ => {
                // COALESCE handles NULL in delta (e.g., SUM(NULL)=NULL but we need 0).
                // Use type-appropriate default: 0 for numeric, FALSE for boolean.
                let default_val = if ic.pg_type == "BOOLEAN" {
                    "FALSE"
                } else {
                    "0"
                };
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
                let is_min_max = plan.intermediate_columns.iter().any(|ic| {
                    format!("\"{}\"", ic.name) == *c
                        && (ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX")
                });
                if is_min_max {
                    format!("d.{}", c) // No COALESCE for MIN/MAX
                } else {
                    let is_bool = plan
                        .intermediate_columns
                        .iter()
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
        "MERGE INTO {} AS t USING {} AS d ON {} WHEN MATCHED THEN UPDATE SET {}{}",
        intermediate_tbl,
        using_clause,
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
fn build_net_delta_query(delta_old: &str, delta_new: &str, plan: &AggregationPlan) -> String {
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
    let mut grp_cols: Vec<String> = plan
        .group_by_columns
        .iter()
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
    let sentinel_col = if needs_sentinel {
        ", 0 AS __reflex_group"
    } else {
        ""
    };
    format!(
        "SELECT {grp_select}{agg_select} FROM (\
            SELECT 1 AS __reflex_sign, __d.*{sentinel_col} FROM ({delta_new}) AS __d \
            UNION ALL \
            SELECT -1 AS __reflex_sign, __d.*{sentinel_col} FROM ({delta_old}) AS __d\
         ) AS __net{grp_by}"
    )
}

/// Build a SQL UPDATE that recomputes MIN/MAX columns from the original
/// (un-delta-substituted) base_query for groups whose value was set to NULL
/// by a subtract operation. Returns None if the plan has no MIN/MAX columns.
///
/// The recompute source is `orig_base_query` as a subquery — this preserves any
/// JOINs and aliases referenced by the aggregated expression. A scalar subquery
/// `SELECT AGG(expr) FROM source_table WHERE …` would fail for such expressions
/// because `source_table` alone doesn't expose the JOINs.
///
/// When `affected_tbl` is `Some(name)` and the plan has group columns, the
/// `orig_base_query` is wrapped in a filter that restricts its output to groups
/// present in the affected-groups table. Without this filter, every MIN/MAX
/// retraction re-aggregates the full source — the cliff that makes stock_chart
/// IMVs unusable in practice. The wrapper is `SELECT * FROM (<orig>) AS __all
/// WHERE (<gb_cols>) IN (SELECT DISTINCT <gb_cols> FROM "<affected_tbl>")`, which
/// pushes the group-key filter down through the aggregation boundary.
pub fn build_min_max_recompute_sql(
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    orig_base_query: &str,
    affected_tbl: Option<&str>,
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
        .map(|c| normalized_column_name(c))
        .collect();

    let set_parts: Vec<String> = min_max_cols
        .iter()
        .map(|ic| format!("\"{}\" = __src.\"{}\"", ic.name, ic.name))
        .collect();

    let null_check: Vec<String> = min_max_cols
        .iter()
        .map(|ic| format!("{}.\"{}\" IS NULL", intermediate_tbl, ic.name))
        .collect();

    // Sentinel-only (no GROUP BY) case: single row, no join keys to match.
    // The WHERE reduces to the NULL filter only. Affected-groups filter is
    // meaningless without group columns — skip the wrap.
    if group_cols.is_empty() {
        return Some(format!(
            "UPDATE {} SET {} FROM ({}) AS __src WHERE {}",
            intermediate_tbl,
            set_parts.join(", "),
            orig_base_query,
            null_check.join(" OR ")
        ));
    }

    // Scope the recompute to affected groups when a table name is available.
    // The filter must be injected BEFORE the GROUP BY so it scopes the scan
    // of the source, not the output of the aggregation. Wrapping the query
    // in `SELECT * FROM (<orig>) WHERE grp IN (...)` post-aggregation was
    // insufficient: Postgres' planner does not reliably push a filter through
    // GROUP BY, leaving the full source scan intact (verified with EXPLAIN).
    //
    // The injected WHERE references the raw GROUP BY column expressions on
    // the LHS (so it applies to pre-aggregation rows) and the normalized
    // column names in the affected-groups table on the RHS. Postgres matches
    // by value, not by alias, so a raw/normalized pair works.
    let scoped_source = match affected_tbl {
        Some(at) => {
            let raw_csv = plan.group_by_columns.join(", ");
            let norm_csv: Vec<String> =
                group_cols.iter().map(|c| format!("\"{}\"", c)).collect();
            let norm_csv = norm_csv.join(", ");
            let filter = format!(
                " AND ({}) IN (SELECT DISTINCT {} FROM \"{}\")",
                raw_csv, norm_csv, at
            );
            match splice_before_group_by(orig_base_query, &filter) {
                Some(spliced) => spliced,
                None => orig_base_query.to_string(),
            }
        }
        None => orig_base_query.to_string(),
    };

    let join_cond: Vec<String> = group_cols
        .iter()
        .map(|gc| {
            format!(
                "{}.\"{}\" IS NOT DISTINCT FROM __src.\"{}\"",
                intermediate_tbl, gc, gc
            )
        })
        .collect();

    Some(format!(
        "UPDATE {} SET {} FROM ({}) AS __src WHERE {} AND ({})",
        intermediate_tbl,
        set_parts.join(", "),
        scoped_source,
        join_cond.join(" AND "),
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
    let conditions: Vec<String> = cols
        .iter()
        .map(|c| format!("{} IS NOT DISTINCT FROM __a.{}", c, c))
        .collect();
    format!(
        "EXISTS (SELECT 1 FROM \"{}\" AS __a WHERE {})",
        affected_tbl,
        conditions.join(" AND ")
    )
}

/// Splice a SQL fragment (already formatted as ` AND (...)` or similar) into a
/// query immediately before its `GROUP BY` clause. If the query has no
/// existing `WHERE` clause between `FROM` and `GROUP BY`, the leading `AND`
/// is rewritten to `WHERE`. Returns `None` if no `GROUP BY` is found.
///
/// Used by `build_min_max_recompute_sql` to push an affected-groups filter
/// through the base-query aggregation boundary so the source scan is scoped.
fn splice_before_group_by(query: &str, and_fragment: &str) -> Option<String> {
    let upper = query.to_uppercase();
    let gb_marker = " GROUP BY ";
    let gb_pos = upper.rfind(gb_marker)?;

    // Determine whether a WHERE exists between the last FROM/JOIN and GROUP BY.
    let pre_gb_upper = &upper[..gb_pos];
    let has_where = pre_gb_upper.contains(" WHERE ");

    let fragment = if has_where {
        and_fragment.to_string()
    } else {
        // Rewrite leading " AND" to " WHERE"
        let trimmed = and_fragment.trim_start();
        if let Some(rest) = trimmed.strip_prefix("AND ") {
            format!(" WHERE {}", rest)
        } else {
            // Fallback: just prepend WHERE
            format!(" WHERE {}", trimmed)
        }
    };

    let mut out = String::with_capacity(query.len() + fragment.len());
    out.push_str(&query[..gb_pos]);
    out.push_str(&fragment);
    out.push_str(&query[gb_pos..]);
    Some(out)
}

/// Splice an affected-groups filter into `end_query` immediately before its `GROUP BY` clause.
///
/// `output_gb_cols` must be pre-quoted column names matching `plan.group_by_columns`.
/// Returns `None` if `end_query` contains no ` GROUP BY ` marker (defensive fallback).
fn inject_affected_filter_before_group_by(
    end_query: &str,
    output_gb_cols: &[String],
    affected_tbl: &str,
) -> Option<String> {
    let upper = end_query.to_uppercase();
    let gb_marker = " GROUP BY ";
    let pos = upper.rfind(gb_marker)?;
    let filter = null_safe_in(affected_tbl, output_gb_cols);
    Some(format!(
        "{} AND {}{}",
        &end_query[..pos],
        filter,
        &end_query[pos..]
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
fn replace_source_with_transition(
    base_query: &str,
    source_table: &str,
    transition_tbl: &str,
) -> String {
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
///   When `include_cleanup` is true, prepends a DELETE FROM affected CTE (replaces TRUNCATE).
/// PG15/16: separate MERGE + SELECT DISTINCT from delta query (MERGE RETURNING unsupported).
fn push_materialized_merge(
    stmts: &mut Vec<String>,
    scratch_tbl: &str,
    delta_query: &str,
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
) {
    stmts.push(format!("TRUNCATE \"{}\"", scratch_tbl));
    stmts.push(format!("INSERT INTO \"{}\" {}", scratch_tbl, delta_query));
    stmts.push(build_merge_from_table_sql(
        intermediate_tbl,
        scratch_tbl,
        plan,
        op,
    ));
}

#[allow(clippy::too_many_arguments)]
fn push_materialized_merge_and_affected(
    stmts: &mut Vec<String>,
    scratch_tbl: &str,
    delta_query: &str,
    intermediate_tbl: &str,
    plan: &AggregationPlan,
    op: DeltaOp,
    affected_tbl: &str,
    select_expr: &str,
    include_cleanup: bool,
) {
    if include_cleanup {
        stmts.push(format!("TRUNCATE \"{}\"", affected_tbl));
    }
    stmts.push(format!("TRUNCATE \"{}\"", scratch_tbl));
    stmts.push(format!("INSERT INTO \"{}\" {}", scratch_tbl, delta_query));
    stmts.push(build_merge_from_table_sql(
        intermediate_tbl,
        scratch_tbl,
        plan,
        op,
    ));
    stmts.push(format!(
        "INSERT INTO \"{}\" SELECT DISTINCT {} FROM \"{}\" AS __d",
        affected_tbl, select_expr, scratch_tbl
    ));
}

/// Generates the SQL statements to apply a delta to an IMV.
///
/// Called from plpgsql trigger wrappers. Returns a delimiter-separated string
/// of SQL statements for the plpgsql function to EXECUTE.
#[pg_extern(parallel_safe)]
pub fn reflex_build_delta_sql(
    view_name: &str,
    source_table: &str,
    operation: &str,
    base_query: &str,
    end_query: &str,
    aggregations_json: Option<&str>,
    orig_base_query: &str,
) -> String {
    let json = aggregations_json.unwrap_or("{}");
    let plan: AggregationPlan = match serde_json::from_str(json) {
        Ok(p) => p,
        Err(_) => {
            pgrx::warning!("pg_reflex: invalid aggregations JSON for '{}'", view_name);
            return String::new();
        }
    };

    let intermediate_tbl = intermediate_table_name(view_name);
    // Use the transition table names directly (no temp table copy needed).
    // Transition tables are visible in plpgsql EXECUTE context.
    let new_tbl = transition_new_table_name(source_table);
    let old_tbl = transition_old_table_name(source_table);

    let mut stmts: Vec<String> = Vec::new();

    // Pre-compute group columns and affected-groups table name (used by multiple paths)
    let grp_cols = group_columns(&plan);
    let bare_view = split_qualified_name(view_name).1;
    let affected_tbl = safe_identifier(&format!("__reflex_affected_{}", bare_view));
    let scratch_tbl = delta_scratch_table_name(view_name);

    // Detect cases where standard incremental delta is incorrect:
    // 1. Self-join: source_table appears multiple times in base_query
    // 2. LEFT/RIGHT JOIN secondary table DELETE/UPDATE: NULL semantics can't be captured by MERGE subtract
    let bare_source = split_qualified_name(source_table).1;
    // Detect self-join and outer-join-secondary for BOTH aggregate and passthrough queries.
    let occurrences = base_query
        .split_whitespace()
        .filter(|w| {
            let trimmed = w.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
            trimmed == source_table || trimmed == bare_source
        })
        .count();
    let is_self_join = occurrences > 1;

    let bq_upper = base_query.to_uppercase();
    let is_full_outer = bq_upper.contains("FULL JOIN") || bq_upper.contains("FULL OUTER");
    // Check if source_table is the secondary table in a LEFT/RIGHT/FULL JOIN.
    // The source is secondary if it appears as the table being outer-joined,
    // i.e. directly after "LEFT JOIN", "RIGHT JOIN", or "FULL JOIN".
    // Do NOT match if source_table only appears in ON conditions (that's the primary table).
    let src_upper = source_table.to_uppercase();
    let bare_upper = bare_source.to_uppercase();
    let is_outer_join_secondary_table = !is_self_join
        && (bq_upper.contains("LEFT JOIN")
            || bq_upper.contains("RIGHT JOIN")
            || bq_upper.contains("LEFT OUTER")
            || bq_upper.contains("RIGHT OUTER")
            || is_full_outer)
        && {
            // Check if source_table appears directly after an outer JOIN keyword
            let patterns = [
                "LEFT JOIN ",
                "LEFT OUTER JOIN ",
                "RIGHT JOIN ",
                "RIGHT OUTER JOIN ",
                "FULL JOIN ",
                "FULL OUTER JOIN ",
            ];
            patterns.iter().any(|pat| {
                let mut search_from = 0;
                while let Some(pos) = bq_upper[search_from..].find(pat) {
                    let after = &bq_upper[search_from + pos + pat.len()..];
                    let next_token = after.split_whitespace().next().unwrap_or("");
                    if next_token == src_upper || next_token == bare_upper {
                        return true;
                    }
                    search_from += pos + pat.len();
                }
                false
            })
        };
    // For LEFT/RIGHT JOIN: only the secondary table's DELETE/UPDATE needs special handling.
    // For FULL OUTER JOIN: ALL operations on BOTH tables need targeted reconcile,
    // because the FULL JOIN delta always includes unmatched rows from the other side.
    let is_outer_join_secondary = (is_outer_join_secondary_table
        && (operation == "DELETE" || operation == "UPDATE"))
        || (is_full_outer && !is_self_join);

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
            let transition = if operation == "DELETE" {
                &old_tbl
            } else {
                &new_tbl
            };
            // Build a delta query to extract group keys from transition table
            let delta_q = replace_source_with_transition(base_query, source_table, transition);

            // Create affected groups table
            stmts.push(format!("TRUNCATE \"{}\"", affected_tbl));

            // Extract affected groups from delta
            stmts.push(format!(
                "INSERT INTO \"{}\" SELECT DISTINCT {} FROM ({}) AS __d",
                affected_tbl, select_expr, delta_q
            ));

            // Delete affected groups from intermediate (NULL-safe)
            let ns_in_int = null_safe_in(&affected_tbl, cols);
            stmts.push(format!(
                "DELETE FROM {} WHERE {}",
                intermediate_tbl, ns_in_int
            ));

            // Re-insert ONLY affected groups from the FULL base_query (reads real source).
            let ns_in_full = null_safe_in(&affected_tbl, cols);
            stmts.push(format!(
                "INSERT INTO {} SELECT * FROM ({}) AS __full WHERE {}",
                intermediate_tbl, base_query, ns_in_full
            ));

            // Targeted refresh of target (NULL-safe)
            let ns_in_tgt = null_safe_in(&affected_tbl, cols);
            stmts.push(format!("DELETE FROM {} WHERE {}", qv, ns_in_tgt));
            stmts.push(format!(
                "INSERT INTO {} {} AND {}",
                qv, end_query, ns_in_tgt
            ));
        } else {
            // No group columns: full refresh
            stmts.push(format!("TRUNCATE {}", intermediate_tbl));
            stmts.push(format!("INSERT INTO {} {}", intermediate_tbl, base_query));
            stmts.push(format!("TRUNCATE {}", quote_identifier(view_name)));
            if end_query.is_empty() {
                stmts.push(format!(
                    "INSERT INTO {} {}",
                    quote_identifier(view_name),
                    base_query
                ));
            } else {
                stmts.push(format!(
                    "INSERT INTO {} {}",
                    quote_identifier(view_name),
                    end_query
                ));
            }
        }
    } else if plan.is_passthrough {
        let qv = quote_identifier(view_name);
        let pt_new = passthrough_scratch_new_table_name(view_name, source_table);
        let pt_old = passthrough_scratch_old_table_name(view_name, source_table);
        // Look up per-source column mappings for targeted DELETE/UPDATE
        let mappings = plan.passthrough_key_mappings.get(source_table);

        // Materialize the transition tables into per-(IMV, source) UNLOGGED scratch
        // tables BEFORE any downstream DML references them. This is the key fix for
        // the nested-trigger SIGABRT: subquery reads of transition tables inside
        // EXECUTE'd DML (DELETE … WHERE IN (SELECT … FROM transition), INSERT …
        // SELECT … FROM transition) trip a PG assertion when fired from a
        // downstream trigger. Plain `INSERT INTO scratch SELECT * FROM transition`
        // is the one pattern that stays safe — so we confine every transition
        // reference to that pattern and route subsequent statements through the
        // scratch tables.
        let needs_new = matches!(operation, "INSERT" | "UPDATE");
        let needs_old = matches!(operation, "DELETE" | "UPDATE");
        if needs_new {
            stmts.push(format!("TRUNCATE \"{}\"", pt_new));
            stmts.push(format!(
                "INSERT INTO \"{}\" SELECT * FROM \"{}\"",
                pt_new, new_tbl
            ));
        }
        if needs_old {
            stmts.push(format!("TRUNCATE \"{}\"", pt_old));
            stmts.push(format!(
                "INSERT INTO \"{}\" SELECT * FROM \"{}\"",
                pt_old, old_tbl
            ));
        }

        match operation {
            "INSERT" => {
                let delta_q = replace_source_with_transition(base_query, source_table, &pt_new);
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
                        qv,
                        row,
                        source_cols.join(", "),
                        pt_old
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
                        qv,
                        row,
                        source_cols.join(", "),
                        pt_old
                    ));
                    // Phase 2: insert new rows (base_query with source→pt_new scratch)
                    let delta_new =
                        replace_source_with_transition(base_query, source_table, &pt_new);
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
            .any(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX");

        match operation {
            "INSERT" => {
                let delta_q = replace_source_with_transition(base_query, source_table, &new_tbl);

                if let Some(ref cols) = grp_cols {
                    let select_expr = affected_groups_select(cols);
                    push_materialized_merge_and_affected(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_q,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Add,
                        &affected_tbl,
                        &select_expr,
                        true,
                    );
                } else {
                    push_materialized_merge(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_q,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Add,
                    );
                }
            }
            "DELETE" => {
                let delta_q = replace_source_with_transition(base_query, source_table, &old_tbl);

                let recompute_scope: Option<&str> = if let Some(ref cols) = grp_cols {
                    let select_expr = affected_groups_select(cols);
                    push_materialized_merge_and_affected(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_q,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Subtract,
                        &affected_tbl,
                        &select_expr,
                        true,
                    );
                    Some(affected_tbl.as_str())
                } else {
                    push_materialized_merge(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_q,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Subtract,
                    );
                    None
                };
                if has_min_max {
                    if let Some(recompute) = build_min_max_recompute_sql(
                        &intermediate_tbl,
                        &plan,
                        orig_base_query,
                        recompute_scope,
                    ) {
                        stmts.push(recompute);
                    }
                }
            }
            "UPDATE" => {
                let delta_old = replace_source_with_transition(base_query, source_table, &old_tbl);
                let delta_new = replace_source_with_transition(base_query, source_table, &new_tbl);

                if has_min_max {
                    if let Some(ref cols) = grp_cols {
                        let select_expr = affected_groups_select(cols);
                        push_materialized_merge_and_affected(
                            &mut stmts,
                            &scratch_tbl,
                            &delta_old,
                            &intermediate_tbl,
                            &plan,
                            DeltaOp::Subtract,
                            &affected_tbl,
                            &select_expr,
                            true,
                        );
                        if let Some(recompute) = build_min_max_recompute_sql(
                            &intermediate_tbl,
                            &plan,
                            orig_base_query,
                            Some(affected_tbl.as_str()),
                        ) {
                            stmts.push(recompute);
                        }
                        push_materialized_merge_and_affected(
                            &mut stmts,
                            &scratch_tbl,
                            &delta_new,
                            &intermediate_tbl,
                            &plan,
                            DeltaOp::Add,
                            &affected_tbl,
                            &select_expr,
                            false,
                        );
                    } else {
                        push_materialized_merge(
                            &mut stmts,
                            &scratch_tbl,
                            &delta_old,
                            &intermediate_tbl,
                            &plan,
                            DeltaOp::Subtract,
                        );
                        if let Some(recompute) = build_min_max_recompute_sql(
                            &intermediate_tbl,
                            &plan,
                            orig_base_query,
                            None,
                        ) {
                            stmts.push(recompute);
                        }
                        push_materialized_merge(
                            &mut stmts,
                            &scratch_tbl,
                            &delta_new,
                            &intermediate_tbl,
                            &plan,
                            DeltaOp::Add,
                        );
                    }
                } else if grp_cols.is_some() {
                    let cols = grp_cols.as_ref().expect("grp_cols is Some — checked above");
                    let net_delta = build_net_delta_query(&delta_old, &delta_new, &plan);
                    let select_expr = affected_groups_select(cols);
                    push_materialized_merge_and_affected(
                        &mut stmts,
                        &scratch_tbl,
                        &net_delta,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Add,
                        &affected_tbl,
                        &select_expr,
                        true,
                    );
                } else {
                    push_materialized_merge(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_old,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Subtract,
                    );
                    push_materialized_merge(
                        &mut stmts,
                        &scratch_tbl,
                        &delta_new,
                        &intermediate_tbl,
                        &plan,
                        DeltaOp::Add,
                    );
                }
            }
            _ => {}
        }

        // Refresh target from intermediate, clean up dead groups, and update metadata.
        //
        // Emitted as separate statements (not a single CTE chain): sibling CTEs
        // in Postgres share a snapshot, so an `INSERT` sibling cannot observe
        // a `DELETE` sibling — when the target has a unique index on the group
        // key, re-inserting the refreshed row hits a duplicate-key error.
        let end_query_has_group_by = end_query.to_uppercase().contains("GROUP BY");
        let include_dead_cleanup = plan.needs_ivm_count
            && grp_cols.is_some()
            && (operation == "DELETE" || operation == "UPDATE");
        let metadata_sql = format!(
            "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() \
             WHERE name = '{}' AND (last_update_date IS NULL OR last_update_date < NOW() - INTERVAL '1 second')",
            view_name.replace("'", "''")
        );

        if end_query_has_group_by {
            let qv = quote_identifier(view_name);
            if plan.group_by_columns.is_empty() {
                // Global COUNT(DISTINCT) with no output GROUP BY — single output row, full rebuild.
                stmts.push(format!("DELETE FROM {}", qv));
                stmts.push(format!("INSERT INTO {} {}", qv, end_query));
            } else {
                let output_cols: Vec<String> = plan
                    .group_by_columns
                    .iter()
                    .map(|c| format!("\"{}\"", normalized_column_name(c)))
                    .collect();
                match inject_affected_filter_before_group_by(end_query, &output_cols, &affected_tbl)
                {
                    Some(spliced_end_q) => {
                        let ns_in = null_safe_in(&affected_tbl, &output_cols);
                        stmts.push(format!("DELETE FROM {} WHERE {}", qv, ns_in));
                        stmts.push(format!("INSERT INTO {} {}", qv, spliced_end_q));
                    }
                    None => {
                        // No GROUP BY found — defensive fallback to full rebuild.
                        stmts.push(format!("DELETE FROM {}", qv));
                        stmts.push(format!("INSERT INTO {} {}", qv, end_query));
                    }
                }
            }
            stmts.push(metadata_sql);
        } else if let Some(ref cols) = grp_cols {
            let qv = quote_identifier(view_name);
            let ns_in = null_safe_in(&affected_tbl, cols);
            if include_dead_cleanup {
                stmts.push(format!(
                    "DELETE FROM {} WHERE __ivm_count <= 0",
                    intermediate_tbl
                ));
            }
            stmts.push(format!("DELETE FROM {} WHERE {}", qv, ns_in));
            stmts.push(format!("INSERT INTO {} {} AND {}", qv, end_query, ns_in));
            stmts.push(metadata_sql);
        } else {
            let qv = quote_identifier(view_name);
            stmts.push(format!("TRUNCATE {}", qv));
            stmts.push(format!("INSERT INTO {} {}", qv, end_query));
            stmts.push(metadata_sql);
        }
    }

    // Historical note (2026-04-24): an earlier version of this function
    // guarded against *any* transition-table reference outside a sanctioned
    // scratch-populate INSERT. That guard existed under the hypothesis that
    // `EXECUTE '…__reflex_new_*…'` inside a trigger body was the root cause
    // of the backend SIGSEGV/SIGABRT we were seeing. The real root cause
    // turned out to be in build.rs — weak stub definitions of
    // `CurrentMemoryContext` etc. were leaking into the installed cdylib,
    // shadowing postgres's real globals and causing NULL derefs in pgrx's
    // SPI path. With that fixed, transition-table references in EXECUTE
    // are safe again and the guard was over-rejecting legitimate full-
    // refresh SQL (e.g. the LEFT JOIN secondary-table fallback that does
    // `DELETE FROM target; INSERT INTO target <end_query>` where end_query
    // can legitimately read from a transition table in some code paths).

    stmts.join("\n--<<REFLEX_SEP>>--\n")
}

/// Generates SQL statements to handle a TRUNCATE on a source table.
/// TRUNCATE has no transition tables, so we clear intermediate + target entirely.
#[pg_extern(parallel_safe)]
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

    let is_passthrough = if let Ok(plan) = serde_json::from_str::<AggregationPlan>(&agg_json) {
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

    // Update last_update_date (lazy: skip if updated within the last second)
    stmts.push(format!(
        "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() \
         WHERE name = '{}' AND (last_update_date IS NULL OR last_update_date < NOW() - INTERVAL '1 second')",
        view_name.replace("'", "''")
    ));

    stmts.join("\n--<<REFLEX_SEP>>--\n")
}

/// Theme 5.3: execute a `\n--<<REFLEX_SEP>>--\n`-separated SQL string, running
/// each non-empty statement in order. Replaces the `string_to_array + FOREACH`
/// pattern in generated trigger bodies with a single Rust-side call — smaller
/// trigger DDL, no intermediate array allocation.
#[pg_extern]
pub fn reflex_execute_separated(sql: &str) {
    for stmt in sql.split("\n--<<REFLEX_SEP>>--\n") {
        let trimmed = stmt.trim();
        if !trimmed.is_empty() {
            Spi::run(trimmed).unwrap_or_report();
        }
    }
}

/// Flushes all accumulated deferred deltas for a given source table.
///
/// Called by the deferred constraint trigger at COMMIT time.
/// Reads from the staging table (__reflex_delta_<source>), applies deltas
/// to each DEFERRED IMV, then cleans up staging and pending rows.
#[pg_extern]
pub fn reflex_flush_deferred(source_table: &str) -> String {
    let delta_tbl = staging_delta_table_name(source_table);

    // Read all DEFERRED IMVs that depend on this source
    let imvs: Vec<(String, String, String, String, Option<String>)> = Spi::connect(|client| {
        let args = [unsafe {
            DatumWithOid::new(
                source_table.to_string(),
                PgBuiltInOids::TEXTOID.oid().value(),
            )
        }];
        client
            .select(
                "SELECT name, base_query, end_query, aggregations::text AS aggregations, \
                        where_predicate \
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
                    row.get_by_name::<&str, _>("name")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    row.get_by_name::<&str, _>("base_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    row.get_by_name::<&str, _>("end_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    row.get_by_name::<&str, _>("aggregations")
                        .unwrap_or(None)
                        .unwrap_or("{}")
                        .to_string(),
                    row.get_by_name::<&str, _>("where_predicate")
                        .unwrap_or(None)
                        .map(|s: &str| s.to_string()),
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
            .map(|row| {
                row.get_by_name::<bool, _>("has")
                    .unwrap_or(None)
                    .unwrap_or(false)
            })
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

        // Refresh planner stats on the staging delta so queries over it get correct
        // row estimates (TRUNCATE resets stats to zero; without ANALYZE the planner
        // assumes an empty table and may pick a bad plan).
        client
            .update(&format!("ANALYZE {}", delta_tbl), None, &[])
            .unwrap_or_report();

        // Passthrough INSERT/DELETE/UPDATE branches in reflex_build_delta_sql
        // reference the NEW/OLD transition tables literally — either directly
        // (pre-Phase-E paths) or via the Phase E per-(IMV, source) scratch
        // populate `INSERT INTO __reflex_pt_*_<v>_<s> SELECT * FROM __reflex_(new|old)_<s>`.
        // Those transition tables only exist inside an IMMEDIATE trigger's
        // REFERENCING scope; here we're at COMMIT, so stand both sides up as
        // temp views over the staging delta. The views must project the source
        // columns only (no `__reflex_op` metadata column) so downstream DML —
        // including `INSERT INTO pt_scratch SELECT * FROM view` where pt_scratch
        // is shaped `LIKE source` — sees the same column list as a real
        // transition table.
        let (src_schema, src_name_only) = split_qualified_name(source_table);
        let src_schema_lit = src_schema.unwrap_or("public").replace("'", "''");
        let src_name_lit = src_name_only.replace("'", "''");
        let src_cols: Vec<String> = client
            .select(
                "SELECT quote_ident(column_name) AS qc \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                 ORDER BY ordinal_position",
                None,
                &[
                    unsafe {
                        DatumWithOid::new(
                            src_schema_lit.clone(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe {
                        DatumWithOid::new(
                            src_name_lit.clone(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<&str, _>("qc")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
            .collect();
        let projection = src_cols.join(", ");
        let new_view = transition_new_table_name(source_table);
        let old_view = transition_old_table_name(source_table);
        client
            .update(&format!("DROP VIEW IF EXISTS {}", new_view), None, &[])
            .unwrap_or_report();
        client
            .update(
                &format!(
                    "CREATE TEMP VIEW {} AS SELECT {} FROM {} WHERE __reflex_op IN ('I', 'U_NEW')",
                    new_view, projection, delta_tbl
                ),
                None,
                &[],
            )
            .unwrap_or_report();
        client
            .update(&format!("DROP VIEW IF EXISTS {}", old_view), None, &[])
            .unwrap_or_report();
        client
            .update(
                &format!(
                    "CREATE TEMP VIEW {} AS SELECT {} FROM {} WHERE __reflex_op IN ('D', 'U_OLD')",
                    old_view, projection, delta_tbl
                ),
                None,
                &[],
            )
            .unwrap_or_report();

        for (imv_name, base_query, end_query, agg_json, where_pred) in &imvs {
            // Skip this IMV if where_predicate is set and no staged row matches it.
            if let Some(pred) = where_pred {
                let new_subq = format!(
                    "(SELECT * FROM {} WHERE __reflex_op IN ('I', 'U_NEW'))",
                    delta_tbl
                );
                let pred_sql = format!(
                    "SELECT EXISTS(SELECT 1 FROM {} AS __pred_chk WHERE {} LIMIT 1)",
                    new_subq, pred
                );
                let matched = client
                    .select(&pred_sql, None, &[])
                    .unwrap_or_report()
                    .next()
                    .map(|row| {
                        row.get_by_name::<bool, _>("exists")
                            .unwrap_or(None)
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if !matched {
                    continue;
                }
            }

            // Collect every per-IMV statement into an ordered list; we emit them
            // inside a single PL/pgSQL DO block with EXCEPTION so one bad IMV
            // rolls back only its own subtransaction and lets the cascade continue.
            let mut imv_stmts: Vec<String> = Vec::new();

            imv_stmts.push(format!(
                "PERFORM pg_advisory_xact_lock(hashtext('{}'), hashtext(reverse('{}')))",
                imv_name.replace("'", "''"),
                imv_name.replace("'", "''")
            ));

            let ins_subq = format!("(SELECT * FROM {} WHERE __reflex_op = 'I')", delta_tbl);
            let ins_base = replace_source_with_delta(base_query, source_table, &ins_subq, "__dt");
            let ins_sql = reflex_build_delta_sql(
                imv_name,
                source_table,
                "INSERT",
                &ins_base,
                end_query,
                Some(agg_json.as_str()),
                base_query,
            );
            let mut ins_had_stmts = false;
            if !ins_sql.is_empty() {
                for stmt in ins_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        imv_stmts.push(stmt.to_string());
                        ins_had_stmts = true;
                    }
                }
            }

            let del_subq = format!("(SELECT * FROM {} WHERE __reflex_op = 'D')", delta_tbl);
            let del_base = replace_source_with_delta(base_query, source_table, &del_subq, "__dt");
            let del_sql = reflex_build_delta_sql(
                imv_name,
                source_table,
                "DELETE",
                &del_base,
                end_query,
                Some(agg_json.as_str()),
                base_query,
            );
            let mut del_had_stmts = false;
            if !del_sql.is_empty() {
                for stmt in del_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        imv_stmts.push(stmt.to_string());
                        del_had_stmts = true;
                    }
                }
            }

            let upd_old_subq = format!("(SELECT * FROM {} WHERE __reflex_op = 'U_OLD')", delta_tbl);
            let upd_old_base =
                replace_source_with_delta(base_query, source_table, &upd_old_subq, "__dt");
            let upd_old_sql = reflex_build_delta_sql(
                imv_name,
                source_table,
                "DELETE",
                &upd_old_base,
                end_query,
                Some(agg_json.as_str()),
                base_query,
            );
            if !upd_old_sql.is_empty() {
                for stmt in upd_old_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        imv_stmts.push(stmt.to_string());
                    }
                }
            }

            let upd_new_subq = format!("(SELECT * FROM {} WHERE __reflex_op = 'U_NEW')", delta_tbl);
            let upd_new_base =
                replace_source_with_delta(base_query, source_table, &upd_new_subq, "__dt");
            let upd_new_sql = reflex_build_delta_sql(
                imv_name,
                source_table,
                "INSERT",
                &upd_new_base,
                end_query,
                Some(agg_json.as_str()),
                base_query,
            );
            let mut upd_had_stmts = false;
            if !upd_new_sql.is_empty() {
                for stmt in upd_new_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        imv_stmts.push(stmt.to_string());
                        upd_had_stmts = true;
                    }
                }
            }

            // Phase 3.4 — wrap per-IMV statements in a PL/pgSQL DO block. The
            // BEGIN…EXCEPTION…END creates an internal subtransaction: a single
            // bad IMV only rolls back its own work and logs a WARNING instead of
            // aborting the entire flush cascade.
            //
            // Theme 4 (observability): inside the same savepoint, record flush
            // timing + staged row count + clear last_error on success; on
            // failure the EXCEPTION branch captures SQLERRM into last_error.
            let body = imv_stmts
                .into_iter()
                .map(|s| format!("{};", s))
                .collect::<Vec<_>>()
                .join("\n");
            let do_block = format!(
                "DO $_reflex_imv_sp$ \
                 DECLARE _t0 TIMESTAMP := clock_timestamp(); \
                         _rows BIGINT; \
                 BEGIN \
                   SELECT COUNT(*) INTO _rows FROM {delta_tbl}; \
                   \n{body}\n \
                   UPDATE public.__reflex_ivm_reference \
                     SET last_flush_ms = EXTRACT(EPOCH FROM (clock_timestamp() - _t0)) * 1000, \
                         last_flush_rows = _rows, \
                         flush_count = COALESCE(flush_count, 0) + 1, \
                         last_error = NULL \
                     WHERE name = '{imv_name_esc}'; \
                 EXCEPTION WHEN OTHERS THEN \
                   RAISE WARNING 'pg_reflex: IMV % flush failed at cascade: % (SQLSTATE %)', \
                     '{imv_name_esc}', SQLERRM, SQLSTATE; \
                   UPDATE public.__reflex_ivm_reference \
                     SET last_error = LEFT(SQLERRM || ' (SQLSTATE ' || SQLSTATE || ')', 500), \
                         flush_count = COALESCE(flush_count, 0) + 1 \
                     WHERE name = '{imv_name_esc}'; \
                 END $_reflex_imv_sp$",
                delta_tbl = delta_tbl,
                body = body,
                imv_name_esc = imv_name.replace("'", "''"),
            );
            client.update(&do_block, None, &[]).unwrap_or_report();

            if ins_had_stmts {
                total_processed += 1;
            }
            if del_had_stmts {
                total_processed += 1;
            }
            if upd_had_stmts {
                total_processed += 1;
            }
        }

        // Clean up: truncate staging table, drop the temp view shims,
        // and remove pending rows.
        client
            .update(&format!("DROP VIEW IF EXISTS {}", new_view), None, &[])
            .unwrap_or_report();
        client
            .update(&format!("DROP VIEW IF EXISTS {}", old_view), None, &[])
            .unwrap_or_report();
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
