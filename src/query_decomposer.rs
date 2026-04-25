use crate::aggregation::{AggregationPlan, IntermediateColumn};
use crate::sql_analyzer::SqlAnalysis;
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Split a potentially schema-qualified name into (Option<schema>, name).
/// "my_view" -> (None, "my_view")
/// "myschema.my_view" -> (Some("myschema"), "my_view")
pub fn split_qualified_name(name: &str) -> (Option<&str>, &str) {
    match name.find('.') {
        Some(pos) => (Some(&name[..pos]), &name[pos + 1..]),
        None => (None, name),
    }
}

/// Quote a potentially schema-qualified name for use in SQL DDL/DML.
/// "my_view" -> "\"my_view\""
/// "myschema.my_view" -> "\"myschema\".\"my_view\""
pub fn quote_identifier(name: &str) -> String {
    let (schema, tbl) = split_qualified_name(name);
    match schema {
        Some(s) => format!("\"{}\".\"{}\"", s, tbl),
        None => format!("\"{}\"", tbl),
    }
}

/// Format a slice of strings as a PostgreSQL array literal in TEXT form
/// (e.g. `{"a","b","c"}`). Use with a `::TEXT[]` cast on the SQL side.
///
/// Motivation: pgrx 0.16/0.18 on PG 17.7 cassert builds trips a
/// `MemoryContextIsValid(context)` assertion when `DatumWithOid::new(Vec<String>,
/// TEXTARRAYOID)` is passed as an SPI parameter — `initArrayResult` fires with a
/// CurrentMemoryContext that PG considers invalid. Avoiding the ArrayResult code
/// path entirely by shipping the array as a TEXT scalar and letting the server
/// parse it sidesteps the crash.
///
/// Empty slice returns `{}`. Each element is double-quoted; embedded `"` and `\`
/// are backslash-escaped per PG's array input syntax.
pub fn format_pg_text_array_literal(values: &[String]) -> String {
    let mut out = String::with_capacity(values.iter().map(|v| v.len() + 3).sum::<usize>() + 2);
    out.push('{');
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        for ch in v.chars() {
            match ch {
                '\\' | '"' => {
                    out.push('\\');
                    out.push(ch);
                }
                _ => out.push(ch),
            }
        }
        out.push('"');
    }
    out.push('}');
    out
}

/// Ensure an identifier fits within PostgreSQL's NAMEDATALEN limit (63 chars).
/// If the raw name exceeds 63 characters, truncate and append a hash suffix
/// so that distinct input names remain distinct after truncation.
pub fn safe_identifier(raw: &str) -> String {
    const MAX_IDENT: usize = 63;
    if raw.len() <= MAX_IDENT {
        return raw.to_string();
    }
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    raw.hash(&mut hasher);
    let hash = hasher.finish();
    // 9 chars for suffix: _ + 8 hex digits
    format!("{}_{:08x}", &raw[..MAX_IDENT - 9], hash as u32)
}

/// Name of the intermediate (unlogged) table for a given view.
/// For schema-qualified names, the intermediate table is in the same schema.
pub fn intermediate_table_name(view_name: &str) -> String {
    let (schema, name) = split_qualified_name(view_name);
    let int_name = safe_identifier(&format!("__reflex_intermediate_{}", name));
    match schema {
        Some(s) => format!("\"{}\".\"{}\"", s, int_name),
        None => int_name,
    }
}

/// Canonical sanitized form of a source_table name used as a suffix in
/// generated trigger-side identifiers (transition tables, staging delta).
/// Strips schema dots and quotes so the suffix is a bare identifier body.
pub fn sanitized_source_suffix(source_table: &str) -> String {
    source_table.replace('.', "_").replace('"', "")
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

/// Name of the deferred-mode staging (delta) table for a source table.
pub fn staging_delta_table_name(source_table: &str) -> String {
    safe_identifier(&format!(
        "__reflex_delta_{}",
        sanitized_source_suffix(source_table)
    ))
}

/// Name of the per-IMV UNLOGGED delta scratch table used to materialize the
/// grouped delta before issuing MERGE (avoids referencing transition tables
/// inside EXECUTE'd MERGE statements, which trips a PG assert).
pub fn delta_scratch_table_name(view_name: &str) -> String {
    safe_identifier(&format!(
        "__reflex_scratch_{}",
        split_qualified_name(view_name).1
    ))
}

/// Name of the per-(IMV, source) UNLOGGED NEW-side passthrough scratch table.
/// Used to materialize the NEW transition for passthrough IMVs so that
/// downstream DML never references a transition table inside EXECUTE — which
/// trips a PG assertion in nested-trigger contexts (cf. delta scratch above).
pub fn passthrough_scratch_new_table_name(view_name: &str, source_table: &str) -> String {
    safe_identifier(&format!(
        "__reflex_pt_new_{}_{}",
        split_qualified_name(view_name).1,
        sanitized_source_suffix(source_table)
    ))
}

/// Name of the per-(IMV, source) UNLOGGED OLD-side passthrough scratch table.
/// Separate from the NEW-side scratch to keep DELETE-then-INSERT ordering
/// correct under UPDATE (the DELETE reads OLD while INSERT reads NEW).
pub fn passthrough_scratch_old_table_name(view_name: &str, source_table: &str) -> String {
    safe_identifier(&format!(
        "__reflex_pt_old_{}_{}",
        split_qualified_name(view_name).1,
        sanitized_source_suffix(source_table)
    ))
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

/// Strip qualifier and lowercase to match PostgreSQL's identifier folding.
/// Unquoted identifiers in SQL are folded to lowercase by PostgreSQL,
/// so all generated SQL should use lowercase column names for consistency.
/// For complex expressions (containing parentheses), sanitizes characters
/// that are invalid in identifiers (parens, commas, spaces, dots) to underscores.
pub fn normalized_column_name(col: &str) -> String {
    let bare = bare_column_name(col).trim_matches('"').to_lowercase();
    if bare.contains('(') {
        bare.chars()
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
        bare
    }
}

/// Replace a SQL identifier with another, respecting word boundaries.
/// Only replaces when the match is NOT part of a longer identifier
/// (i.e., the character before/after is not alphanumeric or `_`).
///
/// **Contract:** `new_name` must itself be a simple identifier (possibly
/// schema-qualified and quoted). If you need to swap a source table for a
/// complex expression like `(SELECT …) AS __dt`, use
/// [`replace_source_with_delta`] — otherwise a match followed by `.col`
/// produces invalid SQL `(SELECT …) AS __dt.col`.
pub fn replace_identifier(sql: &str, old_name: &str, new_name: &str) -> String {
    if old_name.is_empty() {
        return sql.to_string();
    }
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let old_bytes = old_name.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + old_bytes.len() <= bytes.len() && &bytes[i..i + old_bytes.len()] == old_bytes {
            let before_ok = i == 0
                || !(bytes[i - 1].is_ascii_alphanumeric()
                    || bytes[i - 1] == b'_'
                    || bytes[i - 1] == b'.');
            // Don't replace identifiers that follow "AS " (output aliases, not table refs)
            let after_as = i >= 3
                && (bytes[i - 1] == b' ')
                && (bytes[i - 2] == b'S' || bytes[i - 2] == b's')
                && (bytes[i - 3] == b'A' || bytes[i - 3] == b'a')
                && (i < 4 || !bytes[i - 4].is_ascii_alphanumeric());
            let after_pos = i + old_bytes.len();
            let after_ok = after_pos >= bytes.len()
                || !(bytes[after_pos].is_ascii_alphanumeric() || bytes[after_pos] == b'_');
            if before_ok && after_ok && !after_as {
                result.push_str(new_name);
                i += old_bytes.len();
                continue;
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

/// Rewrite every reference to `source_table` in `sql` so it resolves to a
/// delta subquery aliased as `alias`.
///
/// Qualified refs `<source>.<col>` become `<alias>.<col>` (two-pass rewrite,
/// pass 1). Standalone refs (e.g. in `FROM <source>` / `JOIN <source>`)
/// become `<subquery> AS <effective_alias>` (pass 2).
///
/// Pass 2 consumes an existing user alias when present — `FROM order_line AS ol`
/// becomes `FROM (SUBQ) AS ol`, not `FROM (SUBQ) AS __dt AS ol` (the latter is
/// invalid SQL). Supports both `source AS alias` and bare `source alias` forms.
/// Falls back to `alias` (caller-provided default) when no user alias is present.
///
/// This exists because the naive approach — calling `replace_identifier`
/// once with the full `"(SELECT …) AS __dt"` string — corrupts qualified
/// refs, producing invalid SQL like `(SELECT …) AS __dt.col`.
pub fn replace_source_with_delta(
    sql: &str,
    source_table: &str,
    subquery: &str,
    alias: &str,
) -> String {
    if source_table.is_empty() {
        return sql.to_string();
    }
    // Pass 1: rewrite qualified refs `<src>.` -> `<alias>.`.
    let qualified_from = format!("{}.", source_table);
    let qualified_to = format!("{}.", alias);
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let pat = qualified_from.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + pat.len() <= bytes.len() && &bytes[i..i + pat.len()] == pat {
            let before_ok = i == 0
                || !(bytes[i - 1].is_ascii_alphanumeric()
                    || bytes[i - 1] == b'_'
                    || bytes[i - 1] == b'.');
            if before_ok {
                out.push_str(&qualified_to);
                i += pat.len();
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    // Pass 2: rewrite standalone `<src>` (FROM/JOIN positions) -> `<subquery> AS <effective_alias>`.
    replace_standalone_source_with_subquery(&out, source_table, subquery, alias)
}

/// Byte-scanner variant of [`replace_identifier`] that also consumes an existing
/// user alias (`source AS alias` or bare `source alias`) so the emitted replacement
/// uses the user's alias instead of the caller-provided default.
fn replace_standalone_source_with_subquery(
    sql: &str,
    source_table: &str,
    subquery: &str,
    default_alias: &str,
) -> String {
    let bytes = sql.as_bytes();
    let pat = source_table.as_bytes();
    let mut out = String::with_capacity(sql.len() + subquery.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + pat.len() <= bytes.len() && &bytes[i..i + pat.len()] == pat {
            let before_ok = i == 0
                || !(bytes[i - 1].is_ascii_alphanumeric()
                    || bytes[i - 1] == b'_'
                    || bytes[i - 1] == b'.');
            let after_as = i >= 3
                && (bytes[i - 1] == b' ')
                && (bytes[i - 2] == b'S' || bytes[i - 2] == b's')
                && (bytes[i - 3] == b'A' || bytes[i - 3] == b'a')
                && (i < 4 || !bytes[i - 4].is_ascii_alphanumeric());
            let after_pos = i + pat.len();
            let after_ok = after_pos >= bytes.len()
                || !(bytes[after_pos].is_ascii_alphanumeric() || bytes[after_pos] == b'_');
            if before_ok && after_ok && !after_as {
                let (user_alias, consumed_to) = consume_table_alias(bytes, after_pos);
                let effective = user_alias.as_deref().unwrap_or(default_alias);
                out.push_str(subquery);
                out.push_str(" AS ");
                out.push_str(effective);
                i = consumed_to;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Scan forward from `start` looking for an optional table alias. Returns
/// `(Some(alias), new_index)` if an alias (with or without `AS`) was consumed,
/// else `(None, start)` so the caller can fall back to its default.
///
/// Reject bare identifiers that match SQL keywords which can follow a table
/// reference (JOIN, WHERE, ON, …) — those are NOT aliases.
fn consume_table_alias(bytes: &[u8], start: usize) -> (Option<String>, usize) {
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() {
        return (None, start);
    }

    // `AS <ident>` form — AS is unambiguous, but the identifier after it
    // must not be a reserved keyword (SELECT, WHERE, JOIN, …). Blindly
    // consuming `AS SELECT` pushes a mis-parse downstream with a confusing
    // error — reject it here so the original SQL is preserved.
    let has_as = i + 2 <= bytes.len()
        && (bytes[i] == b'A' || bytes[i] == b'a')
        && (bytes[i + 1] == b'S' || bytes[i + 1] == b's')
        && (i + 2 == bytes.len() || bytes[i + 2].is_ascii_whitespace());
    if has_as {
        let mut j = i + 2;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        let name_start = j;
        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
            j += 1;
        }
        if j > name_start {
            let name_bytes = &bytes[name_start..j];
            if is_follow_keyword(name_bytes) {
                return (None, start);
            }
            let name = std::str::from_utf8(name_bytes).unwrap_or("").to_string();
            return (Some(name), j);
        }
        return (None, start);
    }

    // Bare identifier — must not be a SQL keyword that can follow a table.
    let name_start = i;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    if i == name_start {
        return (None, start);
    }
    let name_bytes = &bytes[name_start..i];
    if is_follow_keyword(name_bytes) {
        return (None, start);
    }
    let name = std::str::from_utf8(name_bytes).unwrap_or("").to_string();
    (Some(name), i)
}

/// SQL keywords that can follow a table reference in FROM/JOIN clauses,
/// plus a handful of fully-reserved words that are never valid as an alias
/// (SELECT, FROM, AS, DISTINCT). A bare identifier matching any of these
/// is not a table alias; under the `AS <ident>` form it marks a mis-parse.
fn is_follow_keyword(ident: &[u8]) -> bool {
    const KEYWORDS: &[&[u8]] = &[
        b"ON",
        b"JOIN",
        b"LEFT",
        b"RIGHT",
        b"INNER",
        b"OUTER",
        b"FULL",
        b"CROSS",
        b"NATURAL",
        b"LATERAL",
        b"USING",
        b"WHERE",
        b"GROUP",
        b"ORDER",
        b"HAVING",
        b"LIMIT",
        b"OFFSET",
        b"UNION",
        b"INTERSECT",
        b"EXCEPT",
        b"WITH",
        b"TABLESAMPLE",
        b"AND",
        b"OR",
        b"FETCH",
        b"WINDOW",
        b"FOR",
        b"RETURNING",
        b"SELECT",
        b"FROM",
        b"AS",
        b"DISTINCT",
    ];
    KEYWORDS.iter().any(|kw| {
        kw.len() == ident.len()
            && kw
                .iter()
                .zip(ident.iter())
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
    })
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

/// Generate the end query: intermediate table -> target table.
///
/// Rewrite a HAVING clause, replacing aggregate function calls with intermediate column refs.
/// E.g., "SUM(amount) > 1000" → "\"__sum_amount\" > 1000"
pub fn rewrite_having(having: &str, plan: &AggregationPlan) -> Option<String> {
    let expr = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(having)
        .and_then(|mut p| p.parse_expr())
        .ok()?;
    Some(rewrite_having_expr(&expr, &plan.intermediate_columns))
}

/// Recursively transform a HAVING expression AST, replacing aggregate functions
/// with references to intermediate table columns.
fn rewrite_having_expr(expr: &Expr, columns: &[IntermediateColumn]) -> String {
    match expr {
        Expr::Function(f) => rewrite_aggregate_call(f, columns),
        Expr::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                rewrite_having_expr(left, columns),
                op,
                rewrite_having_expr(right, columns)
            )
        }
        Expr::UnaryOp { op, expr: inner } => {
            format!("{} {}", op, rewrite_having_expr(inner, columns))
        }
        Expr::Nested(inner) => {
            format!("({})", rewrite_having_expr(inner, columns))
        }
        other => other.to_string(),
    }
}

/// Rewrite a single aggregate function call to its intermediate column reference.
fn rewrite_aggregate_call(f: &Function, columns: &[IntermediateColumn]) -> String {
    let func_name = f.name.to_string().to_uppercase();

    // Check for COUNT(*)
    if func_name == "COUNT" {
        if let FunctionArguments::List(list) = &f.args {
            if list.args.len() == 1
                && matches!(
                    &list.args[0],
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                )
            {
                // COUNT(*) → __count_star
                return "\"__count_star\"".to_string();
            }
        }
    }

    // Extract argument string
    let arg_str = if let FunctionArguments::List(list) = &f.args {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) = list.args.first() {
            Some(arg_expr.to_string())
        } else {
            None
        }
    } else {
        None
    };

    let Some(arg) = arg_str else {
        return f.to_string(); // Can't rewrite, pass through
    };

    // AVG(x) → __sum_x / NULLIF(__count_x, 0)
    if func_name == "AVG" {
        let sanitized = sanitize_for_col_name(&arg);
        let sum_col = format!("__sum_{}", sanitized);
        let count_col = format!("__count_{}", sanitized);
        return format!("\"{}\" / NULLIF(\"{}\", 0)", sum_col, count_col);
    }

    // SUM/COUNT/MIN/MAX → find matching intermediate column
    for col in columns {
        if col.source_aggregate.to_uppercase() == func_name && col.source_arg == arg {
            return format!("\"{}\"", col.name);
        }
    }

    // Fallback: pass through as-is
    f.to_string()
}

pub fn sanitize_for_col_name(s: &str) -> String {
    crate::aggregation::sanitize_for_col_name(s)
}

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
                "SUM" | "AVG" | "MIN" | "MAX"
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
    let is_sentinel = plan.group_by_columns.is_empty() && plan.distinct_columns.is_empty();
    if plan.needs_ivm_count && !is_sentinel {
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

    // Apply HAVING clause (rewritten to use intermediate column names)
    if let Some(ref having) = plan.having_clause {
        if let Some(rewritten) = rewrite_having(having, plan) {
            if plan.needs_ivm_count {
                query.push_str(&format!(" AND ({})", rewritten));
            } else {
                query.push_str(&format!(" WHERE ({})", rewritten));
            }
        }
    }

    query
}

/// Serialize the aggregation plan as JSON for the reference table.
pub fn generate_aggregations_json(plan: &AggregationPlan) -> String {
    serde_json::to_string(plan).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
#[path = "tests/unit_query_decomposer.rs"]
mod tests;
