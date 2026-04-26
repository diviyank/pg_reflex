use serde::Serialize;
use sqlparser::ast::{
    Distinct, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    GroupByWithModifier, Join, JoinConstraint, JoinOperator, Query, SelectItem, SetExpr,
    SetOperator, SetQuantifier, Statement, TableFactor, Visit, Visitor,
};
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

    Ok(analysis)
}

#[cfg(test)]
#[path = "tests/unit_sql_analyzer.rs"]
mod tests;
