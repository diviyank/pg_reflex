use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::sql_analyzer::{detect_aggregate, AggregateKind, SqlAnalysis};

/// A column in the intermediate (unlogged) table storing partial aggregate state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntermediateColumn {
    /// Column name in intermediate table (e.g., "__sum_salary")
    pub name: String,
    /// PostgreSQL type (e.g., "NUMERIC", "BIGINT")
    pub pg_type: String,
    /// Aggregate function to use in the base_query (e.g., "SUM")
    pub source_aggregate: String,
    /// Argument expression from the original query (e.g., "salary")
    pub source_arg: String,
    /// When `Some(k)`, this is a MIN/MAX column with a sibling top-K array column
    /// named `<name>_topk` of type `<pg_type>[]`. The array stores the K extremum
    /// values seen for each group (smallest K for MIN, largest K for MAX), kept
    /// in sorted order. On retraction the array is updated via multi-set
    /// subtraction; the retraction recompute path is invoked only when the
    /// array underflows.
    ///
    /// `None` keeps legacy 1.2.x behaviour (single scalar MIN/MAX column with
    /// scoped recompute on retraction).
    #[serde(default)]
    pub topk_k: Option<usize>,
}

impl IntermediateColumn {
    /// Returns the companion top-K array column name, e.g. `__min_x_topk`.
    /// Only meaningful when `topk_k.is_some()`.
    pub fn topk_column_name(&self) -> String {
        format!("{}_topk", self.name)
    }

    /// True when this column carries a top-K companion array.
    pub fn has_topk(&self) -> bool {
        self.topk_k.is_some()
    }
}

/// Mapping from intermediate columns to the final output column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndQueryMapping {
    /// SQL expression reading from the intermediate table (e.g., "__sum_salary / NULLIF(__count_salary, 0)")
    pub intermediate_expr: String,
    /// The user-facing output alias
    pub output_alias: String,
    /// The original aggregate type (e.g., "AVG")
    pub aggregate_type: String,
    /// Optional cast to apply in the end query (e.g., "BIGINT" from SUM(x)::BIGINT)
    #[serde(default)]
    pub cast_type: Option<String>,
}

/// Complete plan for how to decompose a query into intermediate + final stages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationPlan {
    pub group_by_columns: Vec<String>,
    pub intermediate_columns: Vec<IntermediateColumn>,
    pub end_query_mappings: Vec<EndQueryMapping>,
    pub has_distinct: bool,
    pub needs_ivm_count: bool,
    /// For DISTINCT without GROUP BY: the projected columns used as group keys.
    pub distinct_columns: Vec<String>,
    /// True when query has no GROUP BY, no aggregates, no DISTINCT.
    /// Passthrough IMVs skip the intermediate table and modify the target directly.
    pub is_passthrough: bool,
    /// Column names in the passthrough SELECT list (used for incremental DELETE/UPDATE matching).
    #[serde(default)]
    pub passthrough_columns: Vec<String>,
    /// Per-source-table column mappings for passthrough DELETE/UPDATE.
    /// Key: source table name. Value: vec of (target_col, source_col) pairs.
    /// For the key-owner table, target_col == source_col.
    /// For secondary (joined) tables, derived from JOIN conditions.
    #[serde(default)]
    pub passthrough_key_mappings: std::collections::HashMap<String, Vec<(String, String)>>,
    /// Rewritten HAVING clause (aggregate refs replaced with intermediate column names).
    #[serde(default)]
    pub having_clause: Option<String>,
    /// Source columns known to be NOT NULL at IMV creation time.
    /// Used to skip companion __nonnull_count columns for SUM aggregates.
    #[serde(default)]
    pub not_null_columns: std::collections::HashSet<String>,
    /// Mapping from GROUP BY expression to user-facing alias for output columns.
    /// E.g., "COALESCE(t1.grp, t2.grp)" -> "grp" when the SELECT has `... AS grp`.
    /// Only populated when the alias differs from the normalized expression name.
    #[serde(default)]
    pub group_by_aliases: std::collections::HashMap<String, String>,
    /// Output column order matching the user's original SELECT.
    /// Each entry is either "gb:col_expr" (GROUP BY column) or "agg:alias" (aggregate/derived).
    /// Used to generate target DDL and end_query with columns in the user's expected order.
    #[serde(default)]
    pub output_column_order: Vec<String>,
    /// Per-source IMV-relevant column set (carried verbatim from
    /// `SqlAnalysis::imv_relevant_columns`). Read by the trigger codegen to
    /// emit the filter-aware spurious-skip block: when an UPDATE's pre- and
    /// post-image multisets project identically onto these columns *and*
    /// both pass the IMV's `where_predicate`, the IMV's output cannot
    /// change and the entire trigger body for that IMV can be skipped.
    ///
    /// An empty value (per-source or overall) disables the optimization for
    /// the affected source(s) — safe, just falls back to the existing path.
    /// Values stored as sorted lists for stable codegen output.
    #[serde(default)]
    pub imv_relevant_columns: std::collections::HashMap<String, Vec<String>>,
    /// Per-source restricted WHERE predicate, carried from
    /// `SqlAnalysis::imv_relevant_where`. Each entry is the AND-joined
    /// SQL string of conjuncts that reference only the source's columns,
    /// with alias prefixes stripped so it evaluates directly against the
    /// trigger's transition table.
    ///
    /// Used by the trigger filter-aware skip block to decide which rows
    /// are "in scope" for the IMV — paired with `imv_relevant_columns` to
    /// build the EXCEPT ALL check.
    #[serde(default)]
    pub imv_relevant_where: std::collections::HashMap<String, String>,
    /// 1.4.6 — per-source JOIN-key mapping for "safe bulk-INSERT/DELETE"
    /// detection and pg_stats-based pre-scratch dispatch.
    ///
    /// Key: source table name (matches keys in `imv_relevant_columns`).
    /// Value: list of `(intermediate_col, source_col)` pairs, derived from
    /// the JOIN equalities where this source's column equals another
    /// table's column AND that other column projects to a GROUP BY
    /// column of the intermediate.
    ///
    /// Presence of an entry for source S means:
    ///   * S is JOIN-secondary in the IMV's base_query (not the single
    ///     source / not a "key owner" that's the natural producer of
    ///     GROUP BY values).
    ///   * The source columns listed *uniquely identify* a slice of the
    ///     intermediate — no other rows in S can produce the same
    ///     intermediate group keys — making Item α OUT→IN / IN→OUT
    ///     promotion safe to short-circuit:
    ///       - OUT→IN: scratch keys do not collide with existing
    ///         intermediate rows → plain INSERT replaces MERGE.
    ///       - IN→OUT: all intermediate rows for transition values can
    ///         be DELETEd directly → skips the scratch-fill JOIN entirely.
    ///   * pg_stats most_common_freqs on the intermediate's
    ///     `intermediate_col` can be looked up to estimate affected rows
    ///     pre-scratch for Path B dispatch.
    ///
    /// Empty / missing entry → source is treated as cardinality-driving
    /// (fact or single-source key-owner) and stays on the standard MERGE
    /// path. Safe fallback.
    #[serde(default)]
    pub source_join_keys: std::collections::HashMap<String, Vec<(String, String)>>,
}

impl AggregationPlan {
    /// Remove redundant `__nonnull_count_*` / `__bool_or_*_nonnull_count` companion
    /// columns from the intermediate.
    ///
    /// Three classes of redundancy are detected:
    ///
    /// 1. **Bare-column SUM with NOT NULL source** (since 1.4.4). `SUM(col)` where
    ///    `col` is NOT NULL in the source: the companion `__nonnull_count_col`
    ///    always equals `__ivm_count` and is dropped. The end-query
    ///    `CASE WHEN ... > 0 THEN __sum END` is flattened to just `__sum`.
    ///
    /// 2. **BOOL_OR over a structurally non-null inner expression** (1.4.6).
    ///    `BOOL_OR(X IS NOT NULL)`, `BOOL_OR(X IS NULL)`, `BOOL_OR(<bare_not_null_col>)`:
    ///    the inner argument can never be NULL, so the `__bool_or_*_nonnull_count`
    ///    companion equals `__ivm_count`. The end-query's
    ///    `CASE WHEN nonnull > 0 THEN true_count > 0 ELSE NULL END` is flattened
    ///    to `true_count > 0`.
    ///
    /// 3. **Multiplier dedup** (1.4.6). `SUM(<X> * COALESCE(<Y>, <non-null-lit>))`
    ///    has the same nullability profile as `SUM(<X>)`. When a sibling `SUM(<X>)`
    ///    is tracked (so `__nonnull_count_<X>` exists), the multiplier's own
    ///    `__nonnull_count_<X_times_Y_coalesce_lit>` is redundant — end-query
    ///    references are redirected to `__nonnull_count_<X>` and the duplicate is
    ///    dropped. When `<X>` is itself in NOT NULL columns, the dedup degenerates
    ///    to "drop unconditionally and flatten the CASE", same as class 1.
    pub fn optimize_not_null_sums(&mut self, not_null_columns: &std::collections::HashSet<String>) {
        // 1.4.4: always record the catalog-derived NOT NULL set on the plan,
        // not just when the SUM-companion-column optimisation fires. The
        // MERGE codegen in `build_merge_using` reads `plan.not_null_columns`
        // to decide between `=` (index-usable) and `IS NOT DISTINCT FROM`
        // (NULL-safe) per group column, and that decision is independent of
        // any SUM rewrite.
        self.not_null_columns = not_null_columns.clone();

        // Set of column names to drop and (orig -> canonical) redirects.
        let mut to_remove: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut redirect: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        // Class 1: SUM(bare_col) where bare_col is NOT NULL.
        for ic in &self.intermediate_columns {
            if ic.source_aggregate == "SUM" && not_null_columns.contains(&ic.source_arg) {
                let arg_sanitized = sanitize_for_col_name(&ic.source_arg);
                to_remove.insert(format!("__nonnull_count_{}", arg_sanitized));
            }
        }

        // Class 2: BOOL_OR(inner) where `inner` is structurally non-null.
        // The aggregation builder emits a pair:
        //   __bool_or_<arg_san>_true_count    src_arg = "CASE WHEN (<inner>) THEN 1 ELSE 0 END"
        //   __bool_or_<arg_san>_nonnull_count src_arg = "CASE WHEN (<inner>) IS NOT NULL THEN 1 ELSE 0 END"
        // We inspect the true_count's source_arg to recover the original inner.
        let true_count_suffix = "_true_count";
        let nonnull_count_suffix = "_nonnull_count";
        for ic in &self.intermediate_columns {
            if !ic.name.starts_with("__bool_or_") || !ic.name.ends_with(true_count_suffix) {
                continue;
            }
            let stem = match ic.name.strip_suffix(true_count_suffix) {
                Some(s) => s,
                None => continue,
            };
            let companion = format!("{}{}", stem, nonnull_count_suffix);
            if !self
                .intermediate_columns
                .iter()
                .any(|c| c.name == companion)
            {
                continue;
            }
            if let Some(inner) = extract_bool_or_true_inner(&ic.source_arg) {
                if expr_is_structurally_not_null(inner, not_null_columns) {
                    to_remove.insert(companion);
                }
            }
        }

        // Class 3: SUM(X * COALESCE(Y, non-null-lit)) — nullability matches X's.
        // `__nonnull_count_*` companions for SUM have source_aggregate "COUNT"
        // (set by the aggregation builder) and source_arg = raw aggregate argument.
        let intermediates_snapshot: Vec<(String, String, String)> = self
            .intermediate_columns
            .iter()
            .map(|ic| {
                (
                    ic.name.clone(),
                    ic.source_aggregate.clone(),
                    ic.source_arg.clone(),
                )
            })
            .collect();
        for (name, src_agg, src_arg) in &intermediates_snapshot {
            if src_agg != "COUNT" || !name.starts_with("__nonnull_count_") {
                continue;
            }
            if let Some(canonical_x) = strip_coalesce_multiplier_to_x(src_arg) {
                if not_null_columns.contains(&canonical_x) {
                    // X is NOT NULL => the multiplier expression is non-null
                    // everywhere; flatten without a redirect (CASE collapses).
                    to_remove.insert(name.clone());
                    continue;
                }
                let canonical_sanitized = sanitize_for_col_name(&canonical_x);
                let canonical_name = format!("__nonnull_count_{}", canonical_sanitized);
                if &canonical_name != name
                    && intermediates_snapshot
                        .iter()
                        .any(|(other, ..)| other == &canonical_name)
                {
                    to_remove.insert(name.clone());
                    redirect.insert(name.clone(), canonical_name);
                }
            }
        }

        if to_remove.is_empty() {
            return;
        }

        // Drop the redundant columns.
        self.intermediate_columns
            .retain(|ic| !to_remove.contains(&ic.name));

        // Rewrite end_query_mappings.
        for mapping in &mut self.end_query_mappings {
            // First, apply Pattern-B redirects: replace dropped name with canonical.
            for (orig, canon) in &redirect {
                let from = format!("\"{}\"", orig);
                let to = format!("\"{}\"", canon);
                mapping.intermediate_expr = mapping.intermediate_expr.replace(&from, &to);
            }
            // Then flatten the CASE for any nonnull_count that was dropped without a redirect.
            for count_name in &to_remove {
                if redirect.contains_key(count_name) {
                    continue;
                }
                let head = format!("CASE WHEN \"{}\" > 0 THEN ", count_name);
                if !mapping.intermediate_expr.starts_with(&head) {
                    continue;
                }
                let body = &mapping.intermediate_expr[head.len()..];
                // BOOL_OR shape: "... ELSE NULL END". Strip first since "ELSE NULL END"
                // ends with " END" — we'd misclassify it otherwise.
                if let Some(stripped) = body.strip_suffix(" ELSE NULL END") {
                    mapping.intermediate_expr = stripped.to_string();
                    continue;
                }
                // SUM shape: "... END"
                if let Some(stripped) = body.strip_suffix(" END") {
                    mapping.intermediate_expr = stripped.to_string();
                }
            }
        }
    }
}

/// Extract `<inner>` from a `CASE WHEN (<inner>) THEN 1 ELSE 0 END` form.
fn extract_bool_or_true_inner(s: &str) -> Option<&str> {
    let trimmed = s.trim();
    let prefix = "CASE WHEN (";
    let suffix = ") THEN 1 ELSE 0 END";
    if trimmed.starts_with(prefix) && trimmed.ends_with(suffix) {
        Some(&trimmed[prefix.len()..trimmed.len() - suffix.len()])
    } else {
        None
    }
}

/// Returns true if the expression is structurally always non-null:
///   * trailing `IS NOT NULL` / `IS NULL` predicate (always boolean, never null)
///   * bare identifier in `not_null_columns`
///   * a non-null literal (number / quoted string / TRUE / FALSE)
fn expr_is_structurally_not_null(
    expr: &str,
    not_null_columns: &std::collections::HashSet<String>,
) -> bool {
    let t = strip_outer_parens(expr.trim());
    let upper = t.to_uppercase();
    if upper.ends_with(" IS NOT NULL") || upper.ends_with(" IS NULL") {
        return true;
    }
    if not_null_columns.contains(t) {
        return true;
    }
    is_non_null_literal(t)
}

/// Strip a single layer of balanced outer parentheses, if present.
fn strip_outer_parens(s: &str) -> &str {
    let t = s.trim();
    if !(t.starts_with('(') && t.ends_with(')')) {
        return t;
    }
    let inner = &t[1..t.len() - 1];
    let mut depth: i32 = 0;
    for ch in inner.chars() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return t;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    if depth == 0 {
        inner.trim()
    } else {
        t
    }
}

fn is_non_null_literal(s: &str) -> bool {
    let t = s.trim();
    if t.parse::<f64>().is_ok() {
        return true;
    }
    if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
        return true;
    }
    matches!(t.to_uppercase().as_str(), "TRUE" | "FALSE")
}

/// If `expr` has the form `<X> * COALESCE(<Y>, <non-null-lit>)`, return `<X>`.
/// `<X>` is whatever appears before the top-level ` * COALESCE(`. Case-insensitive
/// on the keyword `COALESCE`.
fn strip_coalesce_multiplier_to_x(expr: &str) -> Option<String> {
    let lc = expr.to_lowercase();
    let needle = " * coalesce(";
    let idx = lc.find(needle)?;
    let x = expr[..idx].trim().to_string();
    let coalesce_start = idx + needle.len();
    // Find the matching closing paren of the COALESCE.
    let bytes = expr.as_bytes();
    let mut depth = 1usize;
    let mut end = None;
    for (i, b) in bytes.iter().enumerate().skip(coalesce_start) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let coalesce_end = end?;
    let args_str = &expr[coalesce_start..coalesce_end];
    // Extract the last comma-separated argument at depth 0.
    let last_comma = find_top_level_last_comma(args_str)?;
    let last_arg = args_str[last_comma + 1..].trim();
    if !is_non_null_literal(last_arg) {
        return None;
    }
    Some(x)
}

fn find_top_level_last_comma(s: &str) -> Option<usize> {
    let mut depth: i32 = 0;
    let mut last = None;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => last = Some(i),
            _ => {}
        }
    }
    last
}

/// Sanitize a SQL expression to be used as part of a column name.
/// Strips quotes, replaces non-identifier chars with underscores, collapses runs,
/// and truncates with a hash suffix if too long for PostgreSQL's 63-char limit.
pub fn sanitize_for_col_name(s: &str) -> String {
    // Strip quotes but keep dots/table qualifiers for uniqueness
    let stripped = s.replace('"', "");

    let sanitized: String = stripped
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .to_lowercase();

    // Collapse multiple underscores and trim
    let mut collapsed = String::with_capacity(sanitized.len());
    let mut prev_underscore = false;
    for c in sanitized.chars() {
        if c == '_' {
            if !prev_underscore {
                collapsed.push(c);
            }
            prev_underscore = true;
        } else {
            collapsed.push(c);
            prev_underscore = false;
        }
    }
    let result = collapsed.trim_matches('_').to_string();

    // Truncate to avoid PostgreSQL's 63-char identifier limit.
    // Leave room for prefixes like "__nonnull_count_" (max 18 chars).
    if result.len() > 44 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        result.hash(&mut hasher);
        let hash = hasher.finish();
        format!("{}_{:x}", &result[..36], hash)
    } else {
        result
    }
}

// detect_aggregate is imported from sql_analyzer

/// Recursively collect (aggregate_kind, arg_string) pairs from a HAVING expression.
fn collect_having_aggregates(expr: &Expr, out: &mut Vec<(AggregateKind, String)>) {
    match expr {
        Expr::Function(f) => {
            let func_name = f.name.to_string();
            if let Some(kind) = detect_aggregate(&func_name) {
                // Check for COUNT(*)
                if let FunctionArguments::List(list) = &f.args {
                    if list.args.len() == 1
                        && matches!(
                            &list.args[0],
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                        )
                    {
                        out.push((AggregateKind::CountStar, "*".to_string()));
                    } else if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) =
                        list.args.first()
                    {
                        out.push((kind, arg_expr.to_string()));
                    }
                }
            }
            // Also recurse into function args (for nested expressions)
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        collect_having_aggregates(e, out);
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_having_aggregates(left, out);
            collect_having_aggregates(right, out);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            collect_having_aggregates(inner, out);
        }
        Expr::Nested(inner) => {
            collect_having_aggregates(inner, out);
        }
        _ => {}
    }
}

/// Rewrite an aggregate-derived expression: extract constituent aggregates as
/// intermediate columns and replace them with intermediate column references.
///
/// Returns (rewritten_expression, new_intermediate_columns).
fn rewrite_aggregate_derived_expr(
    expr_sql: &str,
    existing_intermediates: &[IntermediateColumn],
) -> (String, Vec<IntermediateColumn>) {
    let mut new_intermediates = Vec::new();

    // Parse the expression
    let parsed = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(expr_sql)
        .and_then(|mut p| p.parse_expr());
    let Ok(expr) = parsed else {
        return (expr_sql.to_string(), new_intermediates);
    };

    let rewritten = rewrite_expr_aggregates(&expr, existing_intermediates, &mut new_intermediates);
    (rewritten, new_intermediates)
}

/// Recursively rewrite an expression, replacing aggregate function calls with
/// intermediate column references (e.g., SUM(x) -> "__sum_x").
fn rewrite_expr_aggregates(
    expr: &Expr,
    existing: &[IntermediateColumn],
    new_cols: &mut Vec<IntermediateColumn>,
) -> String {
    match expr {
        Expr::Function(f) if f.over.is_none() => {
            let func_name = f.name.to_string();
            if let Some(kind) = detect_aggregate(&func_name) {
                let arg = first_arg_string(f);
                let arg_sanitized = sanitize_for_col_name(&arg);
                let col_name = match kind {
                    AggregateKind::Sum => format!("__sum_{}", arg_sanitized),
                    AggregateKind::Count | AggregateKind::CountStar => {
                        format!("__count_{}", arg_sanitized)
                    }
                    AggregateKind::CountDistinct => {
                        format!("__count_distinct_{}", arg_sanitized)
                    }
                    AggregateKind::Min => format!("__min_{}", arg_sanitized),
                    AggregateKind::Max => format!("__max_{}", arg_sanitized),
                    AggregateKind::BoolOr => {
                        let true_col = format!("__bool_or_{}_true_count", arg_sanitized);
                        let nonnull_col = format!("__bool_or_{}_nonnull_count", arg_sanitized);
                        let (has_true, has_nonnull) = {
                            let all: Vec<&str> = existing
                                .iter()
                                .chain(new_cols.iter())
                                .map(|ic| ic.name.as_str())
                                .collect();
                            (
                                all.contains(&true_col.as_str()),
                                all.contains(&nonnull_col.as_str()),
                            )
                        };
                        if !has_true {
                            new_cols.push(IntermediateColumn {
                                name: true_col.clone(),
                                pg_type: "BIGINT".to_string(),
                                source_aggregate: "SUM".to_string(),
                                source_arg: format!("CASE WHEN ({}) THEN 1 ELSE 0 END", arg),
                                topk_k: None,
                            });
                        }
                        if !has_nonnull {
                            new_cols.push(IntermediateColumn {
                                name: nonnull_col.clone(),
                                pg_type: "BIGINT".to_string(),
                                source_aggregate: "SUM".to_string(),
                                source_arg: format!(
                                    "CASE WHEN ({}) IS NOT NULL THEN 1 ELSE 0 END",
                                    arg
                                ),
                                topk_k: None,
                            });
                        }
                        return format!(
                            "CASE WHEN \"{}\" > 0 THEN \"{}\" > 0 ELSE NULL END",
                            nonnull_col, true_col
                        );
                    }
                    AggregateKind::Avg => format!("__sum_{}", arg_sanitized),
                };
                // Add intermediate column if not already present
                let all_names: Vec<&str> = existing
                    .iter()
                    .chain(new_cols.iter())
                    .map(|ic| ic.name.as_str())
                    .collect();
                if !all_names.contains(&col_name.as_str()) {
                    let (source_agg, source_arg) = match kind {
                        AggregateKind::Avg => ("SUM".to_string(), arg.clone()),
                        AggregateKind::CountStar => ("COUNT".to_string(), "*".to_string()),
                        _ => (func_name.to_uppercase(), arg.clone()),
                    };
                    new_cols.push(IntermediateColumn {
                        name: col_name.clone(),
                        pg_type: match kind {
                            AggregateKind::Count
                            | AggregateKind::CountStar
                            | AggregateKind::CountDistinct => "BIGINT".to_string(),
                            _ => "NUMERIC".to_string(),
                        },
                        source_aggregate: source_agg,
                        source_arg,
                        topk_k: None,
                    });
                }
                return format!("\"{}\"", col_name);
            }
            // Not an aggregate function — recursively rewrite arguments
            // (handles COALESCE(SUM(x), 0), GREATEST(SUM(a), SUM(b)), etc.)
            let mut rewritten_args = Vec::new();
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                            rewritten_args.push(rewrite_expr_aggregates(e, existing, new_cols));
                        }
                        other => rewritten_args.push(other.to_string()),
                    }
                }
            }
            format!("{}({})", f.name, rewritten_args.join(", "))
        }
        Expr::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                rewrite_expr_aggregates(left, existing, new_cols),
                op,
                rewrite_expr_aggregates(right, existing, new_cols)
            )
        }
        Expr::UnaryOp { op, expr: inner } => {
            format!(
                "{} {}",
                op,
                rewrite_expr_aggregates(inner, existing, new_cols)
            )
        }
        Expr::Nested(inner) => {
            format!("({})", rewrite_expr_aggregates(inner, existing, new_cols))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut s = "CASE".to_string();
            if let Some(op) = operand {
                s.push_str(&format!(
                    " {}",
                    rewrite_expr_aggregates(op, existing, new_cols)
                ));
            }
            for case_when in conditions {
                s.push_str(&format!(
                    " WHEN {} THEN {}",
                    rewrite_expr_aggregates(&case_when.condition, existing, new_cols),
                    rewrite_expr_aggregates(&case_when.result, existing, new_cols)
                ));
            }
            if let Some(el) = else_result {
                s.push_str(&format!(
                    " ELSE {}",
                    rewrite_expr_aggregates(el, existing, new_cols)
                ));
            }
            s.push_str(" END");
            s
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            format!(
                "{}::{}",
                rewrite_expr_aggregates(inner, existing, new_cols),
                data_type
            )
        }
        other => other.to_string(),
    }
}

/// Extract the first argument of a function as a string.
fn first_arg_string(f: &Function) -> String {
    if let FunctionArguments::List(list) = &f.args {
        if let Some(arg) = list.args.first() {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => return "*".to_string(),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => return e.to_string(),
                FunctionArg::Unnamed(expr) => return expr.to_string(),
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                    return arg.to_string()
                }
            }
        }
    }
    "*".to_string()
}

/// Build an AggregationPlan from a SqlAnalysis.
///
/// When `topk_k` is `Some(k)`, MIN/MAX columns will be configured to maintain
/// a sibling top-K array of size `k`. Pass `None` for legacy behaviour.
pub fn plan_aggregation_with_topk(
    analysis: &SqlAnalysis,
    topk_k: Option<usize>,
) -> AggregationPlan {
    let mut plan = plan_aggregation_inner(analysis);
    if let Some(k) = topk_k {
        if k > 0 {
            for ic in plan.intermediate_columns.iter_mut() {
                if ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX" {
                    ic.topk_k = Some(k);
                }
            }
        }
    }
    plan
}

/// Backwards-compatible plan_aggregation (no top-K).
pub fn plan_aggregation(analysis: &SqlAnalysis) -> AggregationPlan {
    plan_aggregation_inner(analysis)
}

fn plan_aggregation_inner(analysis: &SqlAnalysis) -> AggregationPlan {
    let mut intermediate_columns = Vec::new();
    let mut end_query_mappings = Vec::new();
    let mut count_distinct_columns: Vec<String> = Vec::new();

    // (Mixed COUNT(DISTINCT) + other aggregates validation is done in lib.rs)

    for col in &analysis.select_columns {
        if col.is_passthrough {
            // Passthrough columns (GROUP BY cols) are handled separately as group keys
            continue;
        }

        // Handle aggregate-derived expressions (e.g., CASE WHEN SUM(x) > 0 THEN ...)
        if col.is_aggregate_derived {
            let output_alias = crate::query_decomposer::normalized_column_name(
                col.alias.as_deref().unwrap_or(&col.expr_sql),
            );
            let (rewritten, new_intermediates) =
                rewrite_aggregate_derived_expr(&col.expr_sql, &intermediate_columns);
            intermediate_columns.extend(new_intermediates);
            end_query_mappings.push(EndQueryMapping {
                intermediate_expr: rewritten,
                output_alias,
                aggregate_type: "DERIVED".to_string(),
                cast_type: col.cast_type.clone(),
            });
            continue;
        }

        let Some(ref agg) = col.aggregate else {
            continue;
        };

        let arg = col.aggregate_arg.as_deref().unwrap_or("*");
        let arg_sanitized = sanitize_for_col_name(arg);

        // Determine the user-facing alias for the output
        let output_alias = crate::query_decomposer::normalized_column_name(
            col.alias.as_deref().unwrap_or(&col.expr_sql),
        );

        let cast_type = col.cast_type.clone();

        match agg {
            AggregateKind::Sum => {
                let sum_col = format!("__sum_{}", arg_sanitized);
                let count_col = format!("__nonnull_count_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: sum_col.clone(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "SUM".to_string(),
                    source_arg: arg.to_string(),
                    topk_k: None,
                });
                // Companion COUNT(col) tracks non-NULL contributors.
                // When this drops to 0, SUM should be NULL (not 0).
                // Only add if not already present from another aggregate.
                if !intermediate_columns.iter().any(|ic| ic.name == count_col) {
                    intermediate_columns.push(IntermediateColumn {
                        name: count_col.clone(),
                        pg_type: "BIGINT".to_string(),
                        source_aggregate: "COUNT".to_string(),
                        source_arg: arg.to_string(),
                        topk_k: None,
                    });
                }
                // End query: CASE WHEN non-null count > 0 THEN sum END (returns NULL when all values are NULL)
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: format!(
                        "CASE WHEN \"{}\" > 0 THEN \"{}\" END",
                        count_col, sum_col
                    ),
                    output_alias,
                    aggregate_type: "SUM".to_string(),
                    cast_type,
                });
            }
            AggregateKind::Count => {
                let col_name = format!("__count_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: col_name.clone(),
                    pg_type: "BIGINT".to_string(),
                    source_aggregate: "COUNT".to_string(),
                    source_arg: arg.to_string(),
                    topk_k: None,
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: col_name,
                    output_alias,
                    aggregate_type: "COUNT".to_string(),
                    cast_type,
                });
            }
            AggregateKind::CountStar => {
                let col_name = "__count_star".to_string();
                intermediate_columns.push(IntermediateColumn {
                    name: col_name.clone(),
                    pg_type: "BIGINT".to_string(),
                    source_aggregate: "COUNT".to_string(),
                    source_arg: "*".to_string(),
                    topk_k: None,
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: col_name,
                    output_alias,
                    aggregate_type: "COUNT".to_string(),
                    cast_type,
                });
            }
            AggregateKind::Avg => {
                // AVG decomposes to SUM + COUNT
                let sum_col = format!("__sum_{}", arg_sanitized);
                let count_col = format!("__count_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: sum_col.clone(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "SUM".to_string(),
                    source_arg: arg.to_string(),
                    topk_k: None,
                });
                intermediate_columns.push(IntermediateColumn {
                    name: count_col.clone(),
                    pg_type: "BIGINT".to_string(),
                    source_aggregate: "COUNT".to_string(),
                    source_arg: arg.to_string(),
                    topk_k: None,
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: format!("{} / NULLIF({}, 0)", sum_col, count_col),
                    output_alias,
                    aggregate_type: "AVG".to_string(),
                    cast_type,
                });
            }
            AggregateKind::Min => {
                let col_name = format!("__min_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: col_name.clone(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "MIN".to_string(),
                    source_arg: arg.to_string(),
                    topk_k: None,
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: col_name,
                    output_alias,
                    aggregate_type: "MIN".to_string(),
                    cast_type,
                });
            }
            AggregateKind::Max => {
                let col_name = format!("__max_{}", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: col_name.clone(),
                    pg_type: "NUMERIC".to_string(),
                    source_aggregate: "MAX".to_string(),
                    source_arg: arg.to_string(),
                    topk_k: None,
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: col_name,
                    output_alias,
                    aggregate_type: "MAX".to_string(),
                    cast_type,
                });
            }
            AggregateKind::BoolOr => {
                let true_col = format!("__bool_or_{}_true_count", arg_sanitized);
                let nonnull_col = format!("__bool_or_{}_nonnull_count", arg_sanitized);
                intermediate_columns.push(IntermediateColumn {
                    name: true_col.clone(),
                    pg_type: "BIGINT".to_string(),
                    source_aggregate: "SUM".to_string(),
                    source_arg: format!("CASE WHEN ({}) THEN 1 ELSE 0 END", arg),
                    topk_k: None,
                });
                intermediate_columns.push(IntermediateColumn {
                    name: nonnull_col.clone(),
                    pg_type: "BIGINT".to_string(),
                    source_aggregate: "SUM".to_string(),
                    source_arg: format!("CASE WHEN ({}) IS NOT NULL THEN 1 ELSE 0 END", arg),
                    topk_k: None,
                });
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: format!(
                        "CASE WHEN \"{}\" > 0 THEN \"{}\" > 0 ELSE NULL END",
                        nonnull_col, true_col
                    ),
                    output_alias,
                    aggregate_type: "BOOL_OR".to_string(),
                    cast_type,
                });
            }
            AggregateKind::CountDistinct => {
                // COUNT(DISTINCT val): the intermediate uses (grp, val) as compound key.
                // The end_query counts non-NULL distinct values per original GROUP BY
                // using COUNT(val). COUNT(val) (not COUNT(*)) matches Postgres
                // semantics — COUNT(DISTINCT val) ignores NULLs.
                count_distinct_columns.push(arg.to_string());
                let arg_norm = crate::query_decomposer::normalized_column_name(arg);
                end_query_mappings.push(EndQueryMapping {
                    intermediate_expr: format!("COUNT(\"{}\")", arg_norm),
                    output_alias,
                    aggregate_type: "COUNT".to_string(),
                    cast_type,
                });
            }
        }
    }

    // Auto-add intermediate columns for aggregates referenced in HAVING but not in SELECT
    if let Some(ref having_str) = analysis.having_clause {
        let parse_result = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(having_str)
            .and_then(|mut p| p.parse_expr());
        if let Ok(having_expr) = parse_result {
            let mut having_aggs = Vec::new();
            collect_having_aggregates(&having_expr, &mut having_aggs);
            for (kind, arg) in having_aggs {
                let arg_sanitized = sanitize_for_col_name(&arg);
                match kind {
                    AggregateKind::Sum => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__sum_{}", arg_sanitized),
                            pg_type: "NUMERIC".to_string(),
                            source_aggregate: "SUM".to_string(),
                            source_arg: arg,
                            topk_k: None,
                        });
                    }
                    AggregateKind::Count => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__count_{}", arg_sanitized),
                            pg_type: "BIGINT".to_string(),
                            source_aggregate: "COUNT".to_string(),
                            source_arg: arg,
                            topk_k: None,
                        });
                    }
                    AggregateKind::CountStar => {
                        intermediate_columns.push(IntermediateColumn {
                            name: "__count_star".to_string(),
                            pg_type: "BIGINT".to_string(),
                            source_aggregate: "COUNT".to_string(),
                            source_arg: "*".to_string(),
                            topk_k: None,
                        });
                    }
                    AggregateKind::Avg => {
                        // AVG needs both SUM and COUNT
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__sum_{}", arg_sanitized),
                            pg_type: "NUMERIC".to_string(),
                            source_aggregate: "SUM".to_string(),
                            source_arg: arg.clone(),
                            topk_k: None,
                        });
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__count_{}", arg_sanitized),
                            pg_type: "BIGINT".to_string(),
                            source_aggregate: "COUNT".to_string(),
                            source_arg: arg,
                            topk_k: None,
                        });
                    }
                    AggregateKind::Min => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__min_{}", arg_sanitized),
                            pg_type: "NUMERIC".to_string(),
                            source_aggregate: "MIN".to_string(),
                            source_arg: arg,
                            topk_k: None,
                        });
                    }
                    AggregateKind::Max => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__max_{}", arg_sanitized),
                            pg_type: "NUMERIC".to_string(),
                            source_aggregate: "MAX".to_string(),
                            source_arg: arg,
                            topk_k: None,
                        });
                    }
                    AggregateKind::BoolOr => {
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__bool_or_{}_true_count", arg_sanitized),
                            pg_type: "BIGINT".to_string(),
                            source_aggregate: "SUM".to_string(),
                            source_arg: format!("CASE WHEN ({}) THEN 1 ELSE 0 END", arg),
                            topk_k: None,
                        });
                        intermediate_columns.push(IntermediateColumn {
                            name: format!("__bool_or_{}_nonnull_count", arg_sanitized),
                            pg_type: "BIGINT".to_string(),
                            source_aggregate: "SUM".to_string(),
                            source_arg: format!(
                                "CASE WHEN ({}) IS NOT NULL THEN 1 ELSE 0 END",
                                arg
                            ),
                            topk_k: None,
                        });
                    }
                    AggregateKind::CountDistinct => {
                        // COUNT(DISTINCT) in HAVING is not supported yet
                    }
                }
            }
        }
    }

    // Deduplicate intermediate columns by name (e.g., SUM(x) and AVG(x) both need __sum_x)
    let mut seen_names = std::collections::HashSet::new();
    intermediate_columns.retain(|col| seen_names.insert(col.name.clone()));

    let is_passthrough = analysis.group_by_columns.is_empty()
        && intermediate_columns.is_empty()
        && !analysis.has_distinct;

    // __ivm_count for reference counting (not needed for passthrough)
    let needs_ivm_count = !is_passthrough;

    // For DISTINCT without GROUP BY, the passthrough columns become distinct columns.
    // For COUNT(DISTINCT val), the distinct column extends the intermediate key.
    let mut distinct_columns = if analysis.has_distinct && analysis.group_by_columns.is_empty() {
        analysis
            .select_columns
            .iter()
            .filter(|c| c.is_passthrough)
            .map(|c| c.expr_sql.clone())
            .collect()
    } else {
        Vec::new()
    };
    distinct_columns.extend(count_distinct_columns);

    // For passthrough queries, collect column names for incremental DELETE/UPDATE
    let passthrough_columns = if is_passthrough {
        analysis
            .select_columns
            .iter()
            .map(|c| {
                let name = c.alias.as_deref().unwrap_or(&c.expr_sql);
                crate::query_decomposer::normalized_column_name(name)
            })
            .collect()
    } else {
        Vec::new()
    };

    // Passthrough SELECT columns that aren't in GROUP BY (e.g., EXTRACT(WEEK FROM col)
    // when col is in GROUP BY) are valid in PostgreSQL due to functional dependency.
    // Add them to group_by_columns so they become intermediate table keys.
    // Only applies when there IS a GROUP BY clause (not for DISTINCT-only queries).
    let mut group_by_columns = analysis.group_by_columns.clone();
    if !is_passthrough && !analysis.group_by_columns.is_empty() {
        let gb_norms: Vec<String> = group_by_columns
            .iter()
            .map(|gb| crate::query_decomposer::normalized_column_name(gb))
            .collect();
        let extra_cols: Vec<String> = analysis
            .select_columns
            .iter()
            .filter(|col| {
                if !col.is_passthrough {
                    return false;
                }
                let norm = crate::query_decomposer::normalized_column_name(&col.expr_sql);
                let alias_norm = col
                    .alias
                    .as_deref()
                    .map(crate::query_decomposer::normalized_column_name);
                // Skip if normalized expression or alias already matches a GROUP BY column
                !gb_norms
                    .iter()
                    .any(|gn| *gn == norm || alias_norm.as_deref().is_some_and(|an| gn == an))
            })
            .map(|col| col.expr_sql.clone())
            .collect();
        group_by_columns.extend(extra_cols);
    }

    // Build GROUP BY aliases: map expression -> user alias when they differ
    let mut group_by_aliases = std::collections::HashMap::new();
    for gb in &group_by_columns {
        if let Some(sc) = analysis.select_columns.iter().find(|sc| {
            if !sc.is_passthrough {
                return false;
            }
            // Exact match first, then normalized (handles table.col vs col)
            sc.expr_sql == *gb
                || crate::query_decomposer::normalized_column_name(&sc.expr_sql)
                    == crate::query_decomposer::normalized_column_name(gb)
        }) {
            if let Some(ref alias) = sc.alias {
                let norm_gb = crate::query_decomposer::normalized_column_name(gb);
                let norm_alias = crate::query_decomposer::normalized_column_name(alias);
                if norm_gb != norm_alias {
                    group_by_aliases.insert(gb.clone(), alias.clone());
                }
            }
        }
    }

    // Build output_column_order from the user's SELECT to preserve column ordering.
    // Each entry is "gb:<expr>" for GROUP BY columns or "agg:<alias>" for aggregates/derived.
    let output_column_order: Vec<String> = analysis
        .select_columns
        .iter()
        .filter_map(|col| {
            if col.is_window {
                Some(format!(
                    "agg:{}",
                    crate::query_decomposer::normalized_column_name(
                        col.alias.as_deref().unwrap_or(&col.expr_sql)
                    )
                ))
            } else if col.is_passthrough {
                // Resolve to the matching GROUP BY expression so that keys are consistent
                // with group_by_aliases (e.g. SELECT table.col matches GROUP BY col).
                let norm = crate::query_decomposer::normalized_column_name(&col.expr_sql);
                let gb_key = group_by_columns
                    .iter()
                    .find(|g| crate::query_decomposer::normalized_column_name(g) == norm)
                    .cloned()
                    .unwrap_or_else(|| col.expr_sql.clone());
                Some(format!("gb:{}", gb_key))
            } else if col.aggregate.is_some() || col.is_aggregate_derived {
                Some(format!(
                    "agg:{}",
                    crate::query_decomposer::normalized_column_name(
                        col.alias.as_deref().unwrap_or(&col.expr_sql)
                    )
                ))
            } else {
                None
            }
        })
        .collect();

    let imv_relevant_columns: std::collections::HashMap<String, Vec<String>> = analysis
        .imv_relevant_columns
        .iter()
        .map(|(source, cols)| (source.clone(), cols.iter().cloned().collect()))
        .collect();

    AggregationPlan {
        group_by_columns,
        intermediate_columns,
        end_query_mappings,
        has_distinct: analysis.has_distinct,
        needs_ivm_count,
        distinct_columns,
        is_passthrough,
        passthrough_columns,
        passthrough_key_mappings: std::collections::HashMap::new(),
        having_clause: analysis.having_clause.clone(),
        not_null_columns: std::collections::HashSet::new(),
        group_by_aliases,
        output_column_order,
        imv_relevant_columns,
        imv_relevant_where: analysis.imv_relevant_where.clone(),
        // Populated separately by `build_source_join_keys` in create_ivm
        // after the plan is built — see the call site there.
        source_join_keys: std::collections::HashMap::new(),
    }
}

#[cfg(test)]
#[path = "tests/unit_aggregation.rs"]
mod tests;
