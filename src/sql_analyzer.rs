use serde::Serialize;
use sqlparser::ast::{
    BinaryOperator, Distinct, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    GroupByWithModifier, Ident, Join, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, Visit, VisitMut, Visitor,
    VisitorMut,
};
use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

#[derive(Debug)]
pub enum SqlAnalysisError {
    MultipleQueries(usize),
    NotASelectQuery,
}

impl std::fmt::Display for SqlAnalysisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlAnalysisError::MultipleQueries(n) => {
                write!(f, "Expected exactly 1 query, found {}", n)
            }
            SqlAnalysisError::NotASelectQuery => write!(f, "Query is not a SELECT statement"),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum AggregateKind {
    Sum,
    Count,
    CountStar,
    CountDistinct,
    Avg,
    Min,
    Max,
    BoolOr,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SelectColumn {
    /// The original expression as a SQL string (e.g., "SUM(amount)")
    pub expr_sql: String,
    /// The output alias (explicit or inferred)
    pub alias: Option<String>,
    /// If this is an aggregate, what kind
    pub aggregate: Option<AggregateKind>,
    /// The inner expression of the aggregate as SQL string (e.g., "amount" in SUM(amount))
    pub aggregate_arg: Option<String>,
    /// Is this a plain column passthrough (non-aggregated)?
    pub is_passthrough: bool,
    /// Cast type from wrapping expression (e.g., "BIGINT" from SUM(x)::BIGINT)
    pub cast_type: Option<String>,
    /// True if this is a window function expression (has OVER clause)
    pub is_window: bool,
    /// The FILTER (WHERE ...) predicate, if any (original SQL string)
    pub filter_expr: Option<String>,
    /// True if the expression contains aggregate functions but is not a simple aggregate
    /// (e.g., CASE WHEN SUM(x) > 0 THEN SUM(x)/SUM(y) END). The constituent aggregates
    /// are extracted separately and this expression is rewritten in the end_query.
    pub is_aggregate_derived: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used in tests and will be used by trigger implementation
pub struct JoinInfo {
    pub join_type: String,
    pub target_table: String,
    pub condition_sql: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CteInfo {
    pub alias: String,
    pub query_sql: String,
}

/// Information about a set operation (UNION, UNION ALL, INTERSECT, EXCEPT).
#[derive(Debug, Clone)]
pub struct SetOperationInfo {
    /// The set operator (Union, Intersect, Except)
    pub op: SetOperator,
    /// Whether ALL was specified (true = UNION ALL, false = UNION)
    pub is_all: bool,
    /// SQL strings for each operand (flattened from the binary tree).
    /// For `A UNION ALL B UNION ALL C`, this contains [A_sql, B_sql, C_sql].
    pub operand_sqls: Vec<String>,
}

#[derive(Debug, Default)]
pub struct SqlAnalysis {
    pub ctes: Vec<CteInfo>,
    pub has_recursive_cte: bool,
    pub has_window_function: bool,
    pub has_limit: bool,
    pub has_order_by: bool,
    pub sources: Vec<String>,
    pub select_columns: Vec<SelectColumn>,
    pub group_by_columns: Vec<String>,
    pub has_distinct: bool,
    pub where_clause: Option<String>,
    pub having_clause: Option<String>,
    pub from_clause_sql: String,
    pub joins: Vec<JoinInfo>,
    /// Table alias → real table name (e.g., "s" → "sales_simulation")
    pub table_aliases: std::collections::HashMap<String, String>,
    /// Set operation info if the query uses UNION/INTERSECT/EXCEPT
    pub set_operation: Option<SetOperationInfo>,
    // --- Unsupported feature detection flags ---
    pub has_lateral_join: bool,
    pub has_distinct_on: bool,
    pub has_grouping_sets: bool,
    pub has_tablesample: bool,
    pub has_filter_clause: bool,
    pub has_within_group: bool,
    pub has_scalar_subquery: bool,
    pub unsupported_aggregates: Vec<String>,
    /// Set when any non-deterministic function (NOW, RANDOM, etc.) is found
    /// anywhere in the query — SELECT, WHERE, HAVING, JOIN ON, ORDER BY all
    /// flow through `pre_visit_expr`. The flag name is kept for backwards
    /// compatibility; the rejection is query-wide.
    pub has_nondeterministic_select: bool,
    /// DISTINCT ON columns (e.g., ["city"] from DISTINCT ON (city))
    pub distinct_on_columns: Vec<String>,
    /// ORDER BY expressions as strings (e.g., ["city", "val DESC"])
    pub order_by_exprs: Vec<String>,
    /// Per-source IMV-relevant column set: source-table-name → columns of that
    /// source referenced by SELECT projections, JOIN ON / USING conditions,
    /// GROUP BY expressions, or HAVING — i.e. EVERYWHERE except WHERE.
    ///
    /// A column that appears *only* in WHERE is "filter-only": an UPDATE that
    /// changes its value cannot influence the IMV's output as long as both
    /// old and new pre-images still pass (or both still fail) the predicate.
    /// The trigger's filter-aware spurious-skip uses this set as the projection
    /// for its EXCEPT ALL check.
    ///
    /// Empty when the query uses a SELECT *, when CTEs / subqueries make
    /// attribution ambiguous, or when no non-WHERE column reference resolves
    /// to a known source. An empty set disables the optimization for that
    /// source — safe (fall back to the existing path), just not faster.
    pub imv_relevant_columns: HashMap<String, BTreeSet<String>>,
    /// Per-source restricted WHERE predicate: source-table-name → the
    /// conjunction of WHERE conjuncts that reference only columns of that
    /// source, with alias prefixes stripped so the resulting SQL evaluates
    /// directly against the trigger's transition table.
    ///
    /// Example: for a multi-source IMV with
    ///   `WHERE dp.status IN ('open', 'paid') AND s.qty > 0`
    /// (where `dp` aliases `demand_planning` and `s` aliases `sales`), the
    /// map contains:
    ///   `demand_planning -> "status IN ('open', 'paid')"`
    ///   `sales           -> "qty > 0"`
    ///
    /// Conjuncts spanning multiple sources are dropped (cannot be evaluated
    /// against a single transition table). The original `where_clause`
    /// field is unchanged — it is the full text for downstream uses.
    pub imv_relevant_where: HashMap<String, String>,
}

impl SqlAnalysis {
    /// Returns a human-readable reason if the query uses unsupported SQL features,
    /// or None if the query is fully supported.
    pub fn unsupported_reason(&self) -> Option<String> {
        if self.has_recursive_cte {
            return Some("RECURSIVE CTEs are not supported".into());
        }
        if self.has_limit {
            return Some("LIMIT is not supported (materialized views have no row order)".into());
        }
        if self.has_order_by && !self.has_distinct_on {
            return Some("ORDER BY is not supported (materialized views have no row order)".into());
        }
        if self.has_lateral_join {
            return Some("LATERAL joins are not supported".into());
        }
        // DISTINCT ON is supported via ROW_NUMBER() decomposition (see create_ivm.rs)
        if self.has_distinct_on && self.order_by_exprs.is_empty() {
            return Some(
                "DISTINCT ON without ORDER BY is not supported (row selection would be arbitrary)"
                    .into(),
            );
        }
        if self.has_grouping_sets {
            return Some("GROUPING SETS / CUBE / ROLLUP are not supported".into());
        }
        if self.has_tablesample {
            return Some("TABLESAMPLE is not supported (non-deterministic)".into());
        }
        // FILTER clause is supported via CASE WHEN rewrite (see extract_select_column)
        if self.has_within_group {
            return Some("WITHIN GROUP (ordered-set aggregates) is not supported".into());
        }
        // Scalar subqueries are allowed — they evaluate at trigger time against current
        // table state, producing correct results as effectively static values within
        // one statement execution. E.g., WHERE year >= (SELECT year FROM max_date_view).
        if self.has_nondeterministic_select {
            return Some(
                "Non-deterministic functions (NOW, CURRENT_TIMESTAMP, RANDOM, etc.) \
                 are not supported anywhere in the query — they would cause the IMV \
                 to drift over time without a corresponding source mutation"
                    .into(),
            );
        }
        if !self.unsupported_aggregates.is_empty() {
            let names = self.unsupported_aggregates.join(", ");
            return Some(format!(
                "Unsupported aggregate(s): {}. Supported: SUM, COUNT, AVG, MIN, MAX, BOOL_OR",
                names
            ));
        }
        None
    }
}

struct AnalysisVisitor<'a> {
    analysis: &'a mut SqlAnalysis,
}

impl<'a> Visitor for AnalysisVisitor<'a> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(ref with) = query.with {
            if with.recursive {
                self.analysis.has_recursive_cte = true;
            }
            for cte in &with.cte_tables {
                self.analysis.ctes.push(CteInfo {
                    alias: cte.alias.name.to_string(),
                    query_sql: cte.query.to_string(),
                });
            }
        }
        if query.limit_clause.is_some() {
            self.analysis.has_limit = true;
        }
        if query.order_by.is_some() {
            self.analysis.has_order_by = true;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Function(f) => {
                if f.over.is_some() {
                    self.analysis.has_window_function = true;
                } else {
                    // Detect FILTER clause
                    if f.filter.is_some() {
                        self.analysis.has_filter_clause = true;
                    }
                    // Detect WITHIN GROUP (ordered-set aggregates)
                    if !f.within_group.is_empty() {
                        self.analysis.has_within_group = true;
                    }
                    let func_name = f.name.to_string().to_uppercase();
                    // Detect unsupported aggregate functions
                    if is_known_unsupported_aggregate(&func_name)
                        && !self.analysis.unsupported_aggregates.contains(&func_name)
                    {
                        self.analysis.unsupported_aggregates.push(func_name.clone());
                    }
                    // Detect non-deterministic functions anywhere in the query
                    // (SELECT, WHERE, HAVING, JOIN ON, ORDER BY all flow through
                    // pre_visit_expr). The flag name retains "select" for
                    // backwards compatibility; the rejection is query-wide.
                    if is_nondeterministic_function(&func_name) {
                        self.analysis.has_nondeterministic_select = true;
                    }
                }
            }
            Expr::Subquery(_) => {
                self.analysis.has_scalar_subquery = true;
            }
            Expr::GroupingSets(_) | Expr::Cube(_) | Expr::Rollup(_) => {
                self.analysis.has_grouping_sets = true;
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        match factor {
            TableFactor::Table {
                name,
                alias,
                sample,
                ..
            } => {
                let table_name = name.to_string();
                self.analysis.sources.push(table_name.clone());
                if let Some(a) = alias {
                    self.analysis
                        .table_aliases
                        .insert(a.name.to_string(), table_name);
                }
                if sample.is_some() {
                    self.analysis.has_tablesample = true;
                }
            }
            TableFactor::Derived { lateral, alias, .. } => {
                if *lateral {
                    self.analysis.has_lateral_join = true;
                }
                let label = alias
                    .as_ref()
                    .map(|a| format!("<subquery:{}>", a.name))
                    .unwrap_or_else(|| "<subquery>".to_string());
                self.analysis.sources.push(label);
            }
            TableFactor::TableFunction { alias, .. } => {
                let label = alias
                    .as_ref()
                    .map(|a| format!("<function:{}>", a.name))
                    .unwrap_or_else(|| "<function>".to_string());
                self.analysis.sources.push(label);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Detect aggregate kind from a function name.
/// Recursively check if an expression tree contains any aggregate function calls.
pub fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) if f.over.is_none() => {
            let name = f.name.to_string();
            if detect_aggregate(&name).is_some() {
                return true;
            }
            // Check arguments recursively
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        if expr_contains_aggregate(e) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { expr: inner, .. } => expr_contains_aggregate(inner),
        Expr::Nested(inner) => expr_contains_aggregate(inner),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                if expr_contains_aggregate(op) {
                    return true;
                }
            }
            for case_when in conditions {
                if expr_contains_aggregate(&case_when.condition)
                    || expr_contains_aggregate(&case_when.result)
                {
                    return true;
                }
            }
            if let Some(el) = else_result {
                if expr_contains_aggregate(el) {
                    return true;
                }
            }
            false
        }
        Expr::Cast { expr: inner, .. } => expr_contains_aggregate(inner),
        _ => false,
    }
}

pub fn detect_aggregate(func_name: &str) -> Option<AggregateKind> {
    match func_name.to_uppercase().as_str() {
        "SUM" => Some(AggregateKind::Sum),
        "COUNT" => Some(AggregateKind::Count),
        "AVG" => Some(AggregateKind::Avg),
        "MIN" => Some(AggregateKind::Min),
        "MAX" => Some(AggregateKind::Max),
        "BOOL_OR" => Some(AggregateKind::BoolOr),
        _ => None,
    }
}

/// Check if a function name is a known PostgreSQL aggregate that pg_reflex does NOT support.
fn is_known_unsupported_aggregate(func_name: &str) -> bool {
    matches!(
        func_name,
        "STRING_AGG"
            | "ARRAY_AGG"
            | "JSON_AGG"
            | "JSONB_AGG"
            | "JSON_OBJECT_AGG"
            | "JSONB_OBJECT_AGG"
            | "XMLAGG"
            | "PERCENTILE_CONT"
            | "PERCENTILE_DISC"
            | "MODE"
            | "STDDEV"
            | "STDDEV_POP"
            | "STDDEV_SAMP"
            | "VARIANCE"
            | "VAR_POP"
            | "VAR_SAMP"
            | "CORR"
            | "COVAR_POP"
            | "COVAR_SAMP"
            | "REGR_SLOPE"
            | "REGR_INTERCEPT"
            | "REGR_COUNT"
            | "REGR_R2"
            | "REGR_AVGX"
            | "REGR_AVGY"
            | "REGR_SXX"
            | "REGR_SYY"
            | "REGR_SXY"
            | "BIT_AND"
            | "BIT_OR"
            | "BIT_XOR"
            | "BOOL_AND"
            | "EVERY"
    )
}

/// Check if a function name is non-deterministic (result changes across calls).
fn is_nondeterministic_function(func_name: &str) -> bool {
    matches!(
        func_name,
        "NOW"
            | "CURRENT_TIMESTAMP"
            | "CURRENT_DATE"
            | "CURRENT_TIME"
            | "RANDOM"
            | "CLOCK_TIMESTAMP"
            | "STATEMENT_TIMESTAMP"
            | "TIMEOFDAY"
            | "GEN_RANDOM_UUID"
    )
}

/// Check if function args contain a wildcard (for COUNT(*)).
fn is_wildcard_arg(args: &FunctionArguments) -> bool {
    if let FunctionArguments::List(list) = args {
        list.args.len() == 1
            && matches!(
                &list.args[0],
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
            )
    } else {
        false
    }
}

/// Extract the first argument expression as a SQL string.
fn first_arg_sql(args: &FunctionArguments) -> Option<String> {
    if let FunctionArguments::List(list) = args {
        list.args.first().map(|arg| match arg {
            FunctionArg::Unnamed(expr) => expr.to_string(),
            FunctionArg::Named { arg, .. } => arg.to_string(),
            FunctionArg::ExprNamed { arg, .. } => arg.to_string(),
        })
    } else {
        None
    }
}

/// Extract a SelectColumn from a SelectItem expression.
fn extract_select_column(expr: &Expr, alias: Option<String>) -> SelectColumn {
    // Unwrap casts to detect aggregates inside (e.g., SUM(x)::BIGINT)
    let (inner, cast_type) = match expr {
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => (inner.as_ref(), Some(data_type.to_string())),
        _ => (expr, None),
    };

    if let Expr::Function(f) = inner {
        // Check if this is a window function (has OVER clause)
        if f.over.is_some() {
            return SelectColumn {
                expr_sql: expr.to_string(),
                alias,
                aggregate: None,
                aggregate_arg: None,
                is_passthrough: false,
                cast_type,
                is_window: true,
                filter_expr: None,
                is_aggregate_derived: false,
            };
        }

        let func_name = f.name.to_string();
        if let Some(mut kind) = detect_aggregate(&func_name) {
            // Check for COUNT(*)
            if matches!(kind, AggregateKind::Count) && is_wildcard_arg(&f.args) {
                kind = AggregateKind::CountStar;
            }
            // Check for DISTINCT modifier on aggregates
            if let FunctionArguments::List(list) = &f.args {
                if list.duplicate_treatment == Some(sqlparser::ast::DuplicateTreatment::Distinct)
                    && kind == AggregateKind::Count
                {
                    kind = AggregateKind::CountDistinct;
                }
                // SUM(DISTINCT), AVG(DISTINCT), etc. are rejected in lib.rs via SQL string check
            }
            let mut aggregate_arg = if matches!(kind, AggregateKind::CountStar) {
                Some("*".to_string())
            } else {
                first_arg_sql(&f.args)
            };

            // FILTER (WHERE cond) → rewrite aggregate_arg to CASE WHEN cond THEN arg END
            let filter_expr = f.filter.as_ref().map(|filt| filt.to_string());
            if let Some(ref filter) = filter_expr {
                if matches!(kind, AggregateKind::CountStar) {
                    // COUNT(*) FILTER (WHERE c) → COUNT(CASE WHEN c THEN 1 END)
                    kind = AggregateKind::Count;
                    aggregate_arg = Some(format!("CASE WHEN {} THEN 1 END", filter));
                } else if let Some(ref arg) = aggregate_arg {
                    aggregate_arg = Some(format!("CASE WHEN {} THEN {} END", filter, arg));
                }
            }

            // Rewrite expr_sql to reflect the CASE WHEN form for base_query generation
            let expr_sql = if filter_expr.is_some() {
                let arg_str = aggregate_arg.as_deref().unwrap_or("*");
                format!("{}({})", func_name, arg_str)
            } else {
                inner.to_string()
            };

            return SelectColumn {
                expr_sql,
                alias,
                aggregate: Some(kind),
                aggregate_arg,
                is_passthrough: false,
                cast_type,
                is_window: false,
                filter_expr,
                is_aggregate_derived: false,
            };
        }
    }

    // Check if the expression contains aggregates inside a compound expression
    // (e.g., CASE WHEN SUM(x) > 0 THEN SUM(x)/SUM(y) END)
    let has_nested_agg = expr_contains_aggregate(expr);
    SelectColumn {
        expr_sql: expr.to_string(),
        alias,
        aggregate: None,
        aggregate_arg: None,
        is_passthrough: !has_nested_agg,
        cast_type: None,
        is_window: false,
        filter_expr: None,
        is_aggregate_derived: has_nested_agg,
    }
}

/// Extract join type as a human-readable string.
fn join_type_str(op: &JoinOperator) -> &'static str {
    match op {
        JoinOperator::Inner(_) | JoinOperator::Join(_) => "INNER",
        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => "LEFT",
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => "RIGHT",
        JoinOperator::FullOuter(_) => "FULL OUTER",
        JoinOperator::CrossJoin(_) => "CROSS",
        JoinOperator::Semi(_) | JoinOperator::LeftSemi(_) => "LEFT SEMI",
        JoinOperator::RightSemi(_) => "RIGHT SEMI",
        JoinOperator::Anti(_) | JoinOperator::LeftAnti(_) => "LEFT ANTI",
        JoinOperator::RightAnti(_) => "RIGHT ANTI",
        JoinOperator::StraightJoin(_) => "STRAIGHT",
        _ => "OTHER",
    }
}

/// Extract the JoinConstraint from a JoinOperator.
fn join_constraint(op: &JoinOperator) -> Option<&JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c) => Some(c),
        _ => None,
    }
}

/// Extract JoinInfo from a sqlparser Join struct.
fn extract_join_info(join: &Join) -> JoinInfo {
    let target_table = match &join.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => join.relation.to_string(),
    };
    let condition_sql = join_constraint(&join.join_operator).and_then(|c| match c {
        JoinConstraint::On(expr) => Some(expr.to_string()),
        JoinConstraint::Using(cols) => Some(format!(
            "USING ({})",
            cols.iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
        _ => None,
    });
    JoinInfo {
        join_type: join_type_str(&join.join_operator).to_string(),
        target_table,
        condition_sql,
    }
}

/// Recursively flatten a binary tree of set operations into a list of operand SQL strings.
/// For `(A UNION ALL B) UNION ALL C` with the same operator and quantifier, produces [A, B, C].
/// Mixed operators or quantifiers stop the flattening and wrap the subtree as a single operand.
fn flatten_set_operands(
    top_op: &SetOperator,
    top_is_all: bool,
    left: &SetExpr,
    right: &SetExpr,
    out: &mut Vec<String>,
) {
    // Recurse into left if it's the same operator+quantifier
    match left {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left: ll,
            right: lr,
        } if std::mem::discriminant(op) == std::mem::discriminant(top_op)
            && matches!(set_quantifier, SetQuantifier::All) == top_is_all =>
        {
            flatten_set_operands(top_op, top_is_all, ll, lr, out);
        }
        other => out.push(other.to_string()),
    }
    // Right operand is always a leaf (SQL is left-associative for set ops)
    match right {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left: rl,
            right: rr,
        } if std::mem::discriminant(op) == std::mem::discriminant(top_op)
            && matches!(set_quantifier, SetQuantifier::All) == top_is_all =>
        {
            flatten_set_operands(top_op, top_is_all, rl, rr, out);
        }
        other => out.push(other.to_string()),
    }
}

/// Visitor that records every column reference encountered during traversal.
/// Each entry is the dotted parts of an identifier expression: a bare
/// `Identifier("status")` produces `vec!["status"]`; a `CompoundIdentifier`
/// like `dp.status` produces `vec!["dp", "status"]`.
///
/// Used by [`collect_imv_relevant_columns`] to walk every clause of a SELECT
/// **except** the WHERE clause, so the resulting set captures columns the IMV
/// actually projects / joins on / groups by — not columns that merely gate
/// row inclusion.
#[derive(Default)]
struct ColumnRefCollector {
    refs: Vec<Vec<String>>,
}

impl Visitor for ColumnRefCollector {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(id) => {
                self.refs.push(vec![id.value.clone()]);
            }
            Expr::CompoundIdentifier(ids) => {
                self.refs
                    .push(ids.iter().map(|i| i.value.clone()).collect());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Test whether `source` names a real base table (i.e., not a CTE marker like
/// `<subquery:foo>` or a table function). These placeholders are pushed onto
/// `analysis.sources` by [`AnalysisVisitor::pre_visit_table_factor`] for
/// derived / function table factors and are not valid trigger sources.
fn is_real_source(source: &str) -> bool {
    !source.starts_with('<')
}

/// Resolve `parts` (the dotted segments of a column reference) to a
/// `(source_table, column_name)` pair, using `table_aliases` and the list of
/// real sources to disambiguate.
///
/// Returns a list because a bare identifier in a multi-source query is
/// attributable to all sources (the caller treats that as a conservative
/// over-include — fewer skips fire, but never an incorrect skip). The
/// IMV-create-time pass in `create_ivm.rs` then filters this over-set
/// against each source's actual catalog columns, dropping bogus
/// attributions like `qty_sales` on `demand_planning`.
///
/// Returns an empty vector when the reference clearly belongs to a relation
/// that is not in `sources` (e.g., references inside a correlated subquery
/// that names a CTE or extension table).
fn resolve_column_ref(
    parts: &[String],
    aliases: &HashMap<String, String>,
    sources: &[String],
) -> Vec<(String, String)> {
    let real_sources: Vec<&String> = sources.iter().filter(|s| is_real_source(s)).collect();

    if parts.is_empty() {
        return Vec::new();
    }

    let column = parts.last().unwrap().clone();

    if parts.len() == 1 {
        // Bare identifier — attribute to every real source. Safe over-set;
        // create_ivm filters this against catalog before persisting.
        return real_sources
            .iter()
            .map(|s| ((*s).clone(), column.clone()))
            .collect();
    }

    // Compound identifier: the segment immediately before the column is the
    // table alias or table name. For `schema.table.col` SQL we'd see 3 parts;
    // the prefix `schema.table` won't match an alias but should match a
    // schema-qualified entry in `sources`. We try a few lookup forms.
    let prefix = &parts[parts.len() - 2];

    if let Some(resolved) = aliases.get(prefix) {
        if real_sources.iter().any(|s| s.as_str() == resolved.as_str()) {
            return vec![(resolved.clone(), column)];
        }
    }

    // Bare table name match (no alias): the alias map only contains entries
    // when the user wrote `AS foo`. An unaliased FROM table appears in
    // `sources` directly.
    for s in &real_sources {
        if s.as_str() == prefix {
            return vec![((*s).clone(), column)];
        }
        // Schema-qualified vs bare-name match (e.g., source "alp.product"
        // referenced as "product.id" or vice versa).
        let bare = s.rsplit('.').next().unwrap_or(s.as_str());
        if bare == prefix {
            return vec![((*s).clone(), column)];
        }
    }

    // Couldn't attribute — likely a reference to a CTE or out-of-scope name.
    // Returning empty is the safe choice: we under-attribute, so the column
    // never becomes "IMV-relevant", which makes the multiset check weaker
    // (more skips fire). But the missed columns by construction live outside
    // any real source's transition table, so they can't trigger spurious
    // skips on the wrong table.
    Vec::new()
}

/// Extract just the top-level FROM tables, excluding any tables that
/// appear only inside subqueries. The bare-identifier resolution in
/// [`resolve_column_ref`] uses this list so a bare ref in
/// `SELECT id FROM orders WHERE created_at >= (SELECT cutoff FROM config)`
/// attributes to `orders` only, ignoring `config` (which lives inside the
/// WHERE subquery).
fn top_level_sources(select: &Select) -> Vec<String> {
    let mut sources = Vec::new();
    for twj in &select.from {
        if let TableFactor::Table { name, .. } = &twj.relation {
            sources.push(name.to_string());
        }
        for join in &twj.joins {
            if let TableFactor::Table { name, .. } = &join.relation {
                sources.push(name.to_string());
            }
        }
    }
    sources
}

/// Compute the IMV-relevant column set for each source by walking every
/// clause of the SELECT EXCEPT the WHERE clause: projection, GROUP BY,
/// HAVING, JOIN ON, and JOIN USING column lists.
///
/// Returns an empty map when the optimization can't be safely applied:
/// the query has CTEs, the projection contains a wildcard, or there are no
/// top-level real sources. Resolution is grounded against TOP-LEVEL FROM
/// tables only (not the full analyzer-visitor source list, which also
/// includes relations referenced inside subqueries).
fn collect_imv_relevant_columns(
    select: &Select,
    aliases: &HashMap<String, String>,
    _all_sources: &[String],
    has_ctes: bool,
) -> HashMap<String, BTreeSet<String>> {
    if has_ctes {
        return HashMap::new();
    }
    let sources: Vec<String> = top_level_sources(select);

    // Any wildcard projection means we don't know the exposed columns
    // statically. Bail out — the existing byte-identical fast path remains
    // in place for these IMVs.
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return HashMap::new();
            }
            _ => {}
        }
    }

    let mut collector = ColumnRefCollector::default();

    // Projection
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                let _ = e.visit(&mut collector);
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                let _ = expr.visit(&mut collector);
            }
            _ => {}
        }
    }

    // GROUP BY
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for e in exprs {
            let _ = e.visit(&mut collector);
        }
    }

    // HAVING — references aggregate results, but any column refs inside are
    // refs to source columns that materially shape the aggregate, so include.
    if let Some(e) = &select.having {
        let _ = e.visit(&mut collector);
    }

    // JOIN ON expressions
    let mut using_column_idents: Vec<String> = Vec::new();
    for twj in &select.from {
        for join in &twj.joins {
            if let Some(c) = join_constraint(&join.join_operator) {
                match c {
                    JoinConstraint::On(expr) => {
                        let _ = expr.visit(&mut collector);
                    }
                    JoinConstraint::Using(cols) => {
                        // USING(col, ...) — the columns appear in every joined
                        // relation. Conservatively add them to every real
                        // source. We record them separately so they bypass
                        // alias-resolution (USING never has table prefixes).
                        for c in cols {
                            // ObjectName → take the last segment
                            for part in c.0.iter() {
                                if let sqlparser::ast::ObjectNamePart::Identifier(id) = part {
                                    using_column_idents.push(id.value.clone());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // Resolve every captured ref to (source, column).
    let mut result: HashMap<String, BTreeSet<String>> = HashMap::new();
    for parts in &collector.refs {
        for (src, col) in resolve_column_ref(parts, aliases, &sources) {
            result.entry(src).or_default().insert(col);
        }
    }

    // USING() columns apply to every joined real source.
    let real_sources: Vec<&String> = sources.iter().filter(|s| is_real_source(s)).collect();
    for col in &using_column_idents {
        for s in &real_sources {
            result.entry((*s).clone()).or_default().insert(col.clone());
        }
    }

    result
}

/// Recursively split an Expr by top-level AND into a flat list of conjuncts.
/// Stops descending at the first non-AND BinaryOp or any other node.
fn split_top_level_and(expr: &Expr, out: &mut Vec<Expr>) {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        split_top_level_and(left, out);
        split_top_level_and(right, out);
        return;
    }
    if let Expr::Nested(inner) = expr {
        split_top_level_and(inner, out);
        return;
    }
    out.push(expr.clone());
}

/// In-place visitor that rewrites `Expr::CompoundIdentifier([prefix, ...rest])`
/// to drop the prefix when it matches one of `prefixes_to_strip`. Used to
/// turn `dp.status` into `status` after we've decided a conjunct belongs
/// exclusively to `dp`'s source — the transition table doesn't have the
/// alias, only the bare column name.
///
/// Only strips a single leading prefix; deeper qualifiers (struct field
/// access on JSON columns, schema.table.column) are left alone, since
/// those don't appear when the prefix is just an alias.
struct AliasPrefixStripper<'a> {
    prefixes_to_strip: &'a BTreeSet<String>,
}

impl<'a> VisitorMut for AliasPrefixStripper<'a> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::CompoundIdentifier(ids) = expr {
            if ids.len() == 2 && self.prefixes_to_strip.contains(&ids[0].value) {
                let col = ids.last().unwrap().clone();
                *expr = Expr::Identifier(Ident::new(col.value));
            }
        }
        ControlFlow::Continue(())
    }
}

/// Compute per-source restricted WHERE predicates.
///
/// For each top-level AND conjunct of the WHERE clause:
///   1. Collect every column reference inside the conjunct.
///   2. Resolve each reference to a source table via aliases / source names.
///   3. If every reference resolves to the same single real source, keep the
///      conjunct in that source's bucket; otherwise drop it.
///   4. Strip alias / table-name prefixes so the conjunct evaluates against a
///      flat transition table (which has no aliases).
///
/// Returns a map source-table-name → AND-joined SQL string of the kept
/// conjuncts. Sources with no kept conjuncts are absent from the map.
///
/// Returns an empty map when:
///   - The query has CTEs (refs may resolve through CTE bodies — unsafe).
///   - WHERE is absent.
fn collect_imv_relevant_where(
    select: &Select,
    aliases: &HashMap<String, String>,
    sources: &[String],
    has_ctes: bool,
) -> HashMap<String, String> {
    if has_ctes {
        return HashMap::new();
    }
    let Some(where_expr) = &select.selection else {
        return HashMap::new();
    };

    let real_sources: Vec<&String> = sources.iter().filter(|s| is_real_source(s)).collect();
    if real_sources.is_empty() {
        return HashMap::new();
    }

    // Pre-compute the prefixes we'd need to strip per source: every alias
    // pointing at that source, plus the source's bare table name.
    let mut prefixes_per_source: HashMap<String, BTreeSet<String>> = HashMap::new();
    for s in &real_sources {
        let mut prefixes: BTreeSet<String> = BTreeSet::new();
        let bare = s.rsplit('.').next().unwrap_or(s.as_str()).to_string();
        prefixes.insert(bare);
        prefixes.insert((*s).clone());
        for (alias, table) in aliases {
            if table == *s {
                prefixes.insert(alias.clone());
            }
        }
        prefixes_per_source.insert((*s).clone(), prefixes);
    }

    let mut conjuncts: Vec<Expr> = Vec::new();
    split_top_level_and(where_expr, &mut conjuncts);

    let mut buckets: HashMap<String, Vec<String>> = HashMap::new();

    for conj in conjuncts {
        let mut collector = ColumnRefCollector::default();
        let _ = conj.visit(&mut collector);

        // For each ref, resolve to its source(s). A bare ref in a multi-source
        // query attributes to ALL sources — we treat that as "ambiguous" and
        // drop the conjunct (since we can't know which transition table it'd
        // evaluate against meaningfully).
        let mut resolved_sources: BTreeSet<String> = BTreeSet::new();
        let mut ambiguous_or_unattributable = false;

        for parts in &collector.refs {
            let resolved = resolve_column_ref(parts, aliases, sources);
            if parts.len() == 1 && real_sources.len() > 1 {
                // Bare ident in multi-source query: which source is it?
                // Conservatively treat as ambiguous and drop the conjunct.
                ambiguous_or_unattributable = true;
                break;
            }
            if resolved.is_empty() {
                // Reference to a relation that's not in `sources` — could
                // be a scalar subquery target or an out-of-scope CTE.
                // Drop the conjunct.
                ambiguous_or_unattributable = true;
                break;
            }
            for (src, _col) in &resolved {
                resolved_sources.insert(src.clone());
            }
        }

        if ambiguous_or_unattributable {
            continue;
        }
        if resolved_sources.len() != 1 {
            // 0 refs: it's a literal predicate (e.g., TRUE). Drop — not
            // useful and pollutes both sides equally.
            // 2+ refs: cross-source conjunct, can't evaluate on one
            // transition table.
            continue;
        }

        let source = resolved_sources.into_iter().next().unwrap();
        let empty = BTreeSet::new();
        let prefixes = prefixes_per_source.get(&source).unwrap_or(&empty);
        let mut rewritten = conj.clone();
        let mut stripper = AliasPrefixStripper {
            prefixes_to_strip: prefixes,
        };
        let _ = <Expr as VisitMut>::visit(&mut rewritten, &mut stripper);

        buckets
            .entry(source)
            .or_default()
            .push(rewritten.to_string());
    }

    buckets
        .into_iter()
        .map(|(src, parts)| (src, parts.join(" AND ")))
        .collect()
}

pub fn analyze(statements: &[Statement]) -> Result<SqlAnalysis, SqlAnalysisError> {
    if statements.len() != 1 {
        return Err(SqlAnalysisError::MultipleQueries(statements.len()));
    }

    let query = match &statements[0] {
        Statement::Query(q) => q,
        _ => return Err(SqlAnalysisError::NotASelectQuery),
    };

    let mut analysis = SqlAnalysis::default();

    // Phase 1: Visitor pass for unsupported features and source table extraction
    let mut visitor = AnalysisVisitor {
        analysis: &mut analysis,
    };
    let _ = query.visit(&mut visitor);

    // Phase 2: Handle set operations (UNION/INTERSECT/EXCEPT) or direct SELECT
    if let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = query.body.as_ref()
    {
        let is_all = matches!(set_quantifier, SetQuantifier::All);
        let mut operands = Vec::new();
        flatten_set_operands(op, is_all, left, right, &mut operands);
        analysis.set_operation = Some(SetOperationInfo {
            op: *op,
            is_all,
            operand_sqls: operands,
        });
        return Ok(analysis);
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(SqlAnalysisError::NotASelectQuery),
    };

    // DISTINCT / DISTINCT ON
    match &select.distinct {
        Some(Distinct::On(cols)) => {
            analysis.has_distinct_on = true;
            analysis.distinct_on_columns = cols.iter().map(|e| e.to_string()).collect();
        }
        Some(_) => {
            analysis.has_distinct = true;
        }
        None => {}
    }

    // Capture ORDER BY expressions (needed for DISTINCT ON decomposition)
    if let Some(ref order_by) = query.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
            analysis.order_by_exprs = exprs.iter().map(|e| e.to_string()).collect();
        }
    }

    // SELECT columns
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                analysis
                    .select_columns
                    .push(extract_select_column(expr, None));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                analysis
                    .select_columns
                    .push(extract_select_column(expr, Some(alias.to_string())));
            }
            SelectItem::Wildcard(_) => {
                analysis.select_columns.push(SelectColumn {
                    expr_sql: "*".to_string(),
                    alias: None,
                    aggregate: None,
                    aggregate_arg: None,
                    is_passthrough: true,
                    cast_type: None,
                    is_window: false,
                    filter_expr: None,
                    is_aggregate_derived: false,
                });
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                analysis.select_columns.push(SelectColumn {
                    expr_sql: format!("{}.*", kind),
                    alias: None,
                    aggregate: None,
                    aggregate_arg: None,
                    is_passthrough: true,
                    cast_type: None,
                    is_window: false,
                    filter_expr: None,
                    is_aggregate_derived: false,
                });
            }
        }
    }

    // GROUP BY (and detect GROUPING SETS / CUBE / ROLLUP modifiers)
    if let GroupByExpr::Expressions(exprs, modifiers) = &select.group_by {
        for m in modifiers {
            match m {
                GroupByWithModifier::Rollup
                | GroupByWithModifier::Cube
                | GroupByWithModifier::GroupingSets(_) => {
                    analysis.has_grouping_sets = true;
                }
                _ => {}
            }
        }
        for expr in exprs {
            analysis.group_by_columns.push(expr.to_string());
        }
    }

    // WHERE
    analysis.where_clause = select.selection.as_ref().map(|e| e.to_string());

    // HAVING
    analysis.having_clause = select.having.as_ref().map(|e| e.to_string());

    // FROM clause as SQL string and join extraction
    let from_parts: Vec<String> = select.from.iter().map(|twj| twj.to_string()).collect();
    analysis.from_clause_sql = from_parts.join(", ");

    for twj in &select.from {
        for join in &twj.joins {
            analysis.joins.push(extract_join_info(join));
        }
    }

    // Phase 3: IMV-relevant per-source column set. Walks every clause
    // EXCEPT the WHERE clause and resolves column refs against sources +
    // aliases. CTE-using queries get an empty map (safe; the filter-aware
    // skip is silently disabled). See `collect_imv_relevant_columns`.
    let has_ctes = !analysis.ctes.is_empty();
    analysis.imv_relevant_columns =
        collect_imv_relevant_columns(select, &analysis.table_aliases, &analysis.sources, has_ctes);
    analysis.imv_relevant_where =
        collect_imv_relevant_where(select, &analysis.table_aliases, &analysis.sources, has_ctes);

    Ok(analysis)
}

#[cfg(test)]
#[path = "tests/unit_sql_analyzer.rs"]
mod tests;
