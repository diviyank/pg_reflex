use crate::aggregation::AggregationPlan;
use crate::sql_analyzer::SqlAnalysis;

// The canonical implementations of these identifier utilities live in
// `crate::sql_writer::identifier`. The `pub use` re-exports below keep the
// names available here so existing call sites (and the public-facing test
// surface in `unit_query_decomposer.rs`) compile without churn. They are true
// aliases, not wrappers, so there is no extra function-call indirection.
#[cfg(test)]
pub use crate::sql_writer::identifier::replace_identifier;
pub use crate::sql_writer::identifier::{
    format_pg_text_array as format_pg_text_array_literal, quote as quote_identifier,
    safe as safe_identifier, split_qualified as split_qualified_name,
};

/// The single normalization point for a stored source name. A CTE-decomposed
/// sub-IMV source is persisted double-quoted (`"schema"."view__cte_x"`) to
/// preserve identifier case, while real-table sources are stored bare
/// (`schema.table`). This returns both components **unquoted** so every
/// name-builder works from one canonical form. Routing all consumers through
/// here makes the quoting-bug family structurally impossible.
pub fn canonical_source(name: &str) -> (Option<String>, String) {
    let (schema, bare) = split_qualified_name(name);
    (
        schema.map(|s| unquote_ident_component(s).to_string()),
        unquote_ident_component(bare).to_string(),
    )
}

/// The relation a source spelling names, as `(schema, name)` folded the way
/// PostgreSQL folds them: `t`, `"t"`, `public.t` and `"public"."t"` are one
/// relation, `"T"` is another. An unqualified name is registered only for a
/// relation of `public`.
pub fn named_relation(source: &str) -> (String, String) {
    let folded = |part: &str| match part.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
        Some(quoted) => quoted.to_string(),
        None => part.to_ascii_lowercase(),
    };
    let (schema, name) = split_qualified_name(source);
    (
        schema.map_or_else(|| "public".to_string(), folded),
        folded(name),
    )
}

/// Name of the intermediate (unlogged) table for a given view.
/// For schema-qualified names, the intermediate table is in the same schema.
/// Always returns a quoted identifier reference so usage sites can
/// interpolate it directly without adding outer quotes.
pub fn intermediate_table_name(view_name: &str) -> String {
    let (schema, name) = split_qualified_name(view_name);
    let int_name = safe_identifier(&format!("__reflex_intermediate_{}", name));
    match schema {
        Some(s) => format!("\"{}\".\"{}\"", s, int_name),
        None => format!("\"{}\"", int_name),
    }
}

/// Canonical sanitized form of a source_table name used as a suffix in
/// generated trigger-side identifiers (transition tables, staging delta).
/// Routes through `canonical_source` to unquote both components, then builds
/// the suffix from the canonical form. Strips schema dots and quotes so the
/// suffix is a bare identifier body.
pub fn sanitized_source_suffix(source_table: &str) -> String {
    let (schema, bare) = canonical_source(source_table);
    match schema {
        Some(s) => format!("{s}_{bare}"),
        None => bare,
    }
}

/// Name of the NEW transition table for a source table's triggers.
/// Wraps raw `__reflex_new_<src>` in `safe_identifier` so that long source
/// names do not silently collide under PG's 63-char NAMEDATALEN.
pub fn transition_new_table_name(source_table: &str) -> String {
    safe_identifier(&format!(
        "__reflex_new_{}",
        sanitized_source_suffix(source_table)
    ))
}

/// Name of the OLD transition table for a source table's triggers.
pub fn transition_old_table_name(source_table: &str) -> String {
    safe_identifier(&format!(
        "__reflex_old_{}",
        sanitized_source_suffix(source_table)
    ))
}

/// Wrap a sanitized local identifier in the schema of `qualified_name`.
/// Always returns a quoted identifier reference: `"schema"."local"` when
/// `qualified_name` has a schema, `"local"` otherwise. The local part is run
/// through `safe_identifier` so it always fits PG's 63-char NAMEDATALEN limit.
/// Callers use the output directly in SQL without adding outer quotes.
/// Routes through `canonical_source` to extract the schema unquoted from any form.
fn qualify_in_same_schema(qualified_name: &str, local: String) -> String {
    let local = safe_identifier(&local);
    match canonical_source(qualified_name).0 {
        Some(s) => format!("\"{}\".\"{}\"", s, local),
        None => format!("\"{}\"", local),
    }
}

/// Strip one surrounding pair of double-quotes from an identifier component.
/// A CTE sub-IMV source arrives here already quoted (`"schema"."view"`), so the
/// raw schema slice from `split_qualified_name` is `"schema"`; re-quoting it
/// verbatim would yield `""schema""` (a zero-length delimited identifier). Only
/// the outer pair is removed, so an embedded escaped quote (`"a""b"`) survives.
fn unquote_ident_component(part: &str) -> &str {
    part.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(part)
}

/// Name of the deferred-mode staging (delta) table for a source table.
/// Co-located in the source table's schema so the trigger body can reference
/// it without depending on the session's `search_path`.
pub fn staging_delta_table_name(source_table: &str) -> String {
    qualify_in_same_schema(
        source_table,
        format!("__reflex_delta_{}", sanitized_source_suffix(source_table)),
    )
}

/// Name of the per-IMV UNLOGGED delta scratch table used to materialize the
/// grouped delta before issuing MERGE (avoids referencing transition tables
/// inside EXECUTE'd MERGE statements, which trips a PG assert).
/// Co-located in the IMV's schema so generated MERGE SQL works under any
/// `search_path`.
pub fn delta_scratch_table_name(view_name: &str) -> String {
    qualify_in_same_schema(
        view_name,
        format!("__reflex_scratch_{}", split_qualified_name(view_name).1),
    )
}

/// Name of the per-(IMV, source) UNLOGGED NEW-side passthrough scratch table.
/// Used to materialize the NEW transition for passthrough IMVs so that
/// downstream DML never references a transition table inside EXECUTE — which
/// trips a PG assertion in nested-trigger contexts (cf. delta scratch above).
/// Lives in the IMV's schema for the same `search_path` safety reason.
pub fn passthrough_scratch_new_table_name(view_name: &str, source_table: &str) -> String {
    qualify_in_same_schema(
        view_name,
        format!(
            "__reflex_pt_new_{}_{}",
            split_qualified_name(view_name).1,
            sanitized_source_suffix(source_table)
        ),
    )
}

/// Name of the per-(IMV, source) UNLOGGED OLD-side passthrough scratch table.
/// Separate from the NEW-side scratch to keep DELETE-then-INSERT ordering
/// correct under UPDATE (the DELETE reads OLD while INSERT reads NEW).
pub fn passthrough_scratch_old_table_name(view_name: &str, source_table: &str) -> String {
    qualify_in_same_schema(
        view_name,
        format!(
            "__reflex_pt_old_{}_{}",
            split_qualified_name(view_name).1,
            sanitized_source_suffix(source_table)
        ),
    )
}

/// Name of the per-IMV persistent UNLOGGED "affected groups" table.
/// Co-located in the IMV's schema.
pub fn affected_groups_table_name(view_name: &str) -> String {
    qualify_in_same_schema(
        view_name,
        format!("__reflex_affected_{}", split_qualified_name(view_name).1),
    )
}

/// Name of the per-IMV persistent UNLOGGED "shrunk groups" table (top-K only).
/// Co-located in the IMV's schema.
pub fn shrunk_groups_table_name(view_name: &str) -> String {
    qualify_in_same_schema(
        view_name,
        format!("__reflex_shrunk_{}", split_qualified_name(view_name).1),
    )
}

/// Strip table alias/qualifier from a column expression.
/// E.g., "d.dept_name" -> "dept_name", "city" -> "city"
/// Only strips qualifiers for simple column references — expressions containing
/// parentheses (function calls like `COALESCE(t.x, t.y)`) are returned as-is.
pub fn bare_column_name(col: &str) -> &str {
    if col.contains('(') {
        col
    } else {
        col.rsplit('.').next().unwrap_or(col)
    }
}

/// Strip qualifier and normalise to match PostgreSQL's identifier folding:
/// - Quoted (`"Grp"`)  → strip outer quotes, preserve case verbatim.
/// - Unquoted (`Grp`)  → fold to lowercase (PG's parse-time rule).
///
/// For expressions (containing parentheses), sanitize to a valid identifier suffix.
/// Postgres truncates identifiers to `NAMEDATALEN - 1` (63 bytes by default,
/// respecting multibyte char boundaries). Generated column names that exceed
/// this are silently truncated by Postgres when it stores the attname / creates
/// the column. We must truncate identically so a name we later look up (e.g. in
/// the type-probe `column_types` map keyed by Postgres's stored attname) matches
/// the stored, truncated name — otherwise the lookup misses and the column is
/// mis-typed.
pub(crate) fn truncate_identifier(mut s: String) -> String {
    const MAX_IDENT_BYTES: usize = 63;
    if s.len() > MAX_IDENT_BYTES {
        let mut end = MAX_IDENT_BYTES;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

pub fn normalized_column_name(col: &str) -> String {
    let bare = bare_column_name(col);
    let is_quoted = bare.starts_with('"') && bare.ends_with('"') && bare.len() >= 2;
    let stripped = if is_quoted {
        // Quoted: preserve case; unescape `""` → `"` per SQL standard.
        bare[1..bare.len() - 1].replace("\"\"", "\"")
    } else {
        bare.to_lowercase()
    };
    let result = if stripped.contains('(') {
        stripped
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>()
            .trim_matches('_')
            .to_string()
    } else {
        stripped
    };
    truncate_identifier(result)
}

/// Generate the base query: source data -> intermediate table.
///
/// This query is stored in the reference table and at trigger time the source table
/// names are replaced with transition table names (delta).
///
/// The base_query keeps the original FROM clause (with JOINs, aliases, etc.)
/// and uses the original GROUP BY expressions. But it aliases the group-by columns
/// to their bare names so the intermediate table can use them.
pub fn generate_base_query(analysis: &SqlAnalysis, plan: &AggregationPlan) -> String {
    let mut select_parts: Vec<String> = Vec::new();

    // For aggregates without GROUP BY: add sentinel column for intermediate PK
    let needs_sentinel = plan.group_by_columns.is_empty()
        && plan.distinct_columns.is_empty()
        && !plan.intermediate_columns.is_empty();
    if needs_sentinel {
        select_parts.push("0 AS __reflex_group".to_string());
    }

    // Group by columns: in the base query we keep the original expression
    // but always alias to the normalized (lowercase) column name for the intermediate table.
    // This ensures consistency with PostgreSQL's case folding of unquoted identifiers.
    for col in &plan.group_by_columns {
        let norm = normalized_column_name(col);
        select_parts.push(format!("{} AS \"{}\"", col, norm));
    }

    // DISTINCT columns (for DISTINCT without GROUP BY)
    for col in &plan.distinct_columns {
        let norm = normalized_column_name(col);
        select_parts.push(format!("{} AS \"{}\"", col, norm));
    }

    // Intermediate aggregate columns
    for ic in &plan.intermediate_columns {
        if ic.source_arg == "*" {
            select_parts.push(format!("{}(*) AS \"{}\"", ic.source_aggregate, ic.name));
        } else {
            select_parts.push(format!(
                "{}({}) AS \"{}\"",
                ic.source_aggregate, ic.source_arg, ic.name
            ));
        }

        // For top-K-enabled MIN/MAX columns, also project the K extremum values
        // per group as a sorted array. The companion is sliced [1:K] from a
        // FILTER-clause array_agg so NULLs do not consume slots. MIN sorts ASC,
        // MAX sorts DESC.
        if let Some(k) = ic.topk_k {
            let order = if ic.source_aggregate == "MAX" {
                "DESC"
            } else {
                "ASC"
            };
            let arg_expr = if ic.source_arg == "*" {
                // unreachable in practice (MIN/MAX(*) is not valid SQL), but
                // guard against it by falling back to a constant.
                "1".to_string()
            } else {
                ic.source_arg.clone()
            };
            select_parts.push(format!(
                "(array_agg({arg} ORDER BY {arg} {ord} NULLS LAST) FILTER (WHERE {arg} IS NOT NULL))[1:{k}] AS \"{tname}\"",
                arg = arg_expr,
                ord = order,
                k = k,
                tname = ic.topk_column_name(),
            ));
        }
    }

    // Always add __ivm_count for reference counting
    if plan.needs_ivm_count {
        select_parts.push("COUNT(*) AS __ivm_count".to_string());
    }

    let select_clause = select_parts.join(", ");
    let from_clause = &analysis.from_clause_sql;

    let mut query = format!("SELECT {} FROM {}", select_clause, from_clause);

    if let Some(ref where_clause) = analysis.where_clause {
        query.push_str(&format!(" WHERE {}", where_clause));
    }

    // Group by: use the original group_by_columns expressions (with table qualifiers)
    // because the FROM clause defines those aliases.
    // For DISTINCT without GROUP BY, group by all passthrough columns.
    let mut group_cols = if plan.group_by_columns.is_empty() && plan.has_distinct {
        analysis
            .select_columns
            .iter()
            .filter(|c| c.is_passthrough)
            .map(|c| c.expr_sql.clone())
            .collect::<Vec<_>>()
    } else {
        plan.group_by_columns.clone()
    };
    // Include distinct_columns in GROUP BY (needed for COUNT(DISTINCT) compound key)
    for dc in &plan.distinct_columns {
        if !group_cols.contains(dc) {
            group_cols.push(dc.clone());
        }
    }

    if !group_cols.is_empty() {
        query.push_str(&format!(" GROUP BY {}", group_cols.join(", ")));
    }

    query
}

/// The HAVING text `having` as a predicate over the intermediate columns of
/// `plan`. Planning stores this predicate in `plan.having_clause`; this entry
/// point exists because its tests pin the reading of a HAVING aggregate.
#[cfg(test)]
pub fn rewrite_having(having: &str, plan: &AggregationPlan) -> Option<String> {
    crate::aggregation::having_predicate(having, &plan.intermediate_columns, &[])
        .map(|(predicate, _)| predicate)
}

/// Generate the end query: intermediate table -> target table.
///
/// Uses bare column names since the intermediate table has no table qualifiers.
pub fn generate_end_query(view_name: &str, plan: &AggregationPlan) -> String {
    let table = intermediate_table_name(view_name);
    let mut select_parts: Vec<String> = Vec::new();

    let is_sentinel = plan.group_by_columns.is_empty() && plan.distinct_columns.is_empty();
    let has_count_distinct_mapping = plan
        .end_query_mappings
        .iter()
        .any(|m| m.intermediate_expr.starts_with("COUNT("));

    // Helper: build SELECT expression for a GROUP BY column
    let gb_select = |col: &str| -> String {
        let norm = normalized_column_name(col);
        if let Some(user_alias) = plan.group_by_aliases.get(col) {
            let user_norm = normalized_column_name(user_alias);
            format!("\"{}\" AS \"{}\"", norm, user_norm)
        } else {
            format!("\"{}\"", norm)
        }
    };

    // Helper: build SELECT expression for an end_query mapping
    let agg_select = |mapping: &crate::aggregation::EndQueryMapping| -> String {
        let expr = if is_sentinel
            && matches!(
                mapping.aggregate_type.as_str(),
                "SUM" | "AVG" | "MIN" | "MAX" | "BOOL_OR"
            ) {
            format!(
                "CASE WHEN __ivm_count > 0 THEN {} END",
                mapping.intermediate_expr
            )
        } else {
            mapping.intermediate_expr.clone()
        };
        if let Some(ref cast) = mapping.cast_type {
            format!("({})::{} AS \"{}\"", expr, cast, mapping.output_alias)
        } else {
            format!("{} AS \"{}\"", expr, mapping.output_alias)
        }
    };

    if !plan.output_column_order.is_empty() {
        // Use output_column_order to match the user's SELECT column order
        for entry in &plan.output_column_order {
            if let Some(gb_expr) = entry.strip_prefix("gb:") {
                select_parts.push(gb_select(gb_expr));
            } else if let Some(agg_alias) = entry.strip_prefix("agg:") {
                if let Some(mapping) = plan
                    .end_query_mappings
                    .iter()
                    .find(|m| m.output_alias == agg_alias)
                {
                    select_parts.push(agg_select(mapping));
                }
            }
        }
    } else {
        // Fallback: GROUP BY columns first, then aggregates (legacy order)
        for col in &plan.group_by_columns {
            select_parts.push(gb_select(col));
        }
        let has_count_distinct_mapping = plan
            .end_query_mappings
            .iter()
            .any(|m| m.intermediate_expr.starts_with("COUNT("));
        if !has_count_distinct_mapping {
            for col in &plan.distinct_columns {
                let norm = normalized_column_name(col);
                select_parts.push(format!("\"{}\"", norm));
            }
        }
        for mapping in &plan.end_query_mappings {
            select_parts.push(agg_select(mapping));
        }
    }

    let select_clause = select_parts.join(", ");
    let mut query = format!("SELECT {} FROM {}", select_clause, table);

    // Filter out groups with zero reference count.
    // This ensures deleted groups disappear from the target.
    // Exception: sentinel-only aggregates (no GROUP BY) must always return exactly
    // one row, matching PostgreSQL's behavior of SELECT SUM(x) FROM empty_table → (NULL).
    let filters_live_groups = plan.needs_ivm_count && !is_sentinel;
    if filters_live_groups {
        query.push_str(" WHERE __ivm_count > 0");
    }

    // For COUNT(DISTINCT): add GROUP BY on the original group columns
    // so COUNT(*) re-aggregates from the (grp, val) compound intermediate to just (grp).
    if has_count_distinct_mapping && !plan.group_by_columns.is_empty() {
        let grp_cols: Vec<String> = plan
            .group_by_columns
            .iter()
            .map(|c| format!("\"{}\"", normalized_column_name(c)))
            .collect();
        query.push_str(&format!(" GROUP BY {}", grp_cols.join(", ")));
    }

    // An end query that re-aggregates filters its groups with HAVING; otherwise
    // each intermediate row is one group and the predicate filters rows.
    if let Some(ref having) = plan.having_clause {
        let keyword = if has_count_distinct_mapping {
            "HAVING"
        } else if filters_live_groups {
            "AND"
        } else {
            "WHERE"
        };
        query.push_str(&format!(" {} ({})", keyword, having));
    }

    query
}

/// Serialize the aggregation plan as JSON for the reference table.
/// `AggregationPlan` derives Serialize from compile-time-known field
/// types — serialization cannot fail.
pub fn generate_aggregations_json(plan: &AggregationPlan) -> String {
    serde_json::to_string(plan).expect("AggregationPlan must serialize (derive Serialize)")
}

#[cfg(test)]
#[path = "tests/unit_query_decomposer.rs"]
mod tests;
