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
    pub has_nondeterministic_select: bool,
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
        if self.has_order_by {
            return Some("ORDER BY is not supported (materialized views have no row order)".into());
        }
        if self.has_lateral_join {
            return Some("LATERAL joins are not supported".into());
        }
        if self.has_distinct_on {
            return Some("DISTINCT ON is not supported".into());
        }
        if self.has_grouping_sets {
            return Some(
                "GROUPING SETS / CUBE / ROLLUP are not supported".into(),
            );
        }
        if self.has_tablesample {
            return Some("TABLESAMPLE is not supported (non-deterministic)".into());
        }
        if self.has_filter_clause {
            return Some("FILTER clause on aggregates is not supported".into());
        }
        if self.has_within_group {
            return Some("WITHIN GROUP (ordered-set aggregates) is not supported".into());
        }
        if self.has_scalar_subquery {
            return Some("Scalar subqueries in SELECT are not supported".into());
        }
        if self.has_nondeterministic_select {
            return Some("Non-deterministic functions (NOW, RANDOM, etc.) in SELECT are not supported".into());
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
                        && !self.analysis.unsupported_aggregates.contains(&func_name) {
                        self.analysis.unsupported_aggregates.push(func_name.clone());
                    }
                    // Detect non-deterministic functions in SELECT
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
            TableFactor::Derived {
                lateral, alias, ..
            } => {
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
fn detect_aggregate(func_name: &str) -> Option<AggregateKind> {
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
        Expr::Cast { expr: inner, data_type, .. } => (inner.as_ref(), Some(data_type.to_string())),
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
            let aggregate_arg = if matches!(kind, AggregateKind::CountStar) {
                Some("*".to_string())
            } else {
                first_arg_sql(&f.args)
            };
            return SelectColumn {
                expr_sql: inner.to_string(),
                alias,
                aggregate: Some(kind),
                aggregate_arg,
                is_passthrough: false,
                cast_type,
                is_window: false,
            };
        }
    }
    SelectColumn {
        expr_sql: expr.to_string(),
        alias,
        aggregate: None,
        aggregate_arg: None,
        is_passthrough: true,
        cast_type: None,
        is_window: false,
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
        JoinConstraint::Using(cols) => {
            Some(format!("USING ({})", cols.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ")))
        }
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
        SetExpr::SetOperation { op, set_quantifier, left: ll, right: lr }
            if std::mem::discriminant(op) == std::mem::discriminant(top_op)
                && matches!(set_quantifier, SetQuantifier::All) == top_is_all =>
        {
            flatten_set_operands(top_op, top_is_all, ll, lr, out);
        }
        other => out.push(other.to_string()),
    }
    // Right operand is always a leaf (SQL is left-associative for set ops)
    match right {
        SetExpr::SetOperation { op, set_quantifier, left: rl, right: rr }
            if std::mem::discriminant(op) == std::mem::discriminant(top_op)
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
    if let SetExpr::SetOperation { op, set_quantifier, left, right } = query.body.as_ref() {
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
        Some(Distinct::On(_)) => {
            analysis.has_distinct_on = true;
        }
        Some(_) => {
            analysis.has_distinct = true;
        }
        None => {}
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
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_and_analyze(sql: &str) -> SqlAnalysis {
        let parsed = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        analyze(&parsed).unwrap()
    }

    #[test]
    fn test_simple_group_by() {
        let a = parse_and_analyze("SELECT city, SUM(salary) FROM emp GROUP BY city");
        assert_eq!(a.group_by_columns, vec!["city"]);
        assert_eq!(a.sources, vec!["emp"]);
        assert_eq!(a.select_columns.len(), 2);
        assert!(a.select_columns[0].is_passthrough);
        assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Sum));
    }

    #[test]
    fn test_avg_detection() {
        let a = parse_and_analyze("SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept");
        assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Avg));
        assert_eq!(a.select_columns[1].alias.as_deref(), Some("avg_sal"));
        assert_eq!(a.select_columns[1].aggregate_arg.as_deref(), Some("salary"));
    }

    #[test]
    fn test_multiple_aggregates() {
        let a = parse_and_analyze(
            "SELECT city, SUM(amount) AS total, COUNT(id) AS cnt FROM orders GROUP BY city",
        );
        assert_eq!(a.select_columns.len(), 3);
        assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Sum));
        assert_eq!(a.select_columns[2].aggregate, Some(AggregateKind::Count));
    }

    #[test]
    fn test_distinct_detected() {
        let a = parse_and_analyze("SELECT DISTINCT country FROM orders");
        assert!(a.has_distinct);
        assert_eq!(a.select_columns.len(), 1);
        assert!(a.select_columns[0].is_passthrough);
    }

    #[test]
    fn test_where_clause() {
        let a =
            parse_and_analyze("SELECT city, COUNT(*) FROM emp WHERE active = true GROUP BY city");
        assert!(a.where_clause.is_some());
        assert!(a.where_clause.unwrap().contains("active"));
    }

    #[test]
    fn test_join_extraction() {
        let a = parse_and_analyze(
            "SELECT a.city, SUM(b.amount) FROM emp a JOIN sales b ON a.id = b.emp_id GROUP BY a.city",
        );
        assert_eq!(a.sources.len(), 2);
        assert_eq!(a.joins.len(), 1);
        assert_eq!(a.joins[0].join_type, "INNER");
        assert!(a.joins[0].condition_sql.is_some());
    }

    #[test]
    fn test_count_star() {
        let a = parse_and_analyze("SELECT city, COUNT(*) FROM emp GROUP BY city");
        assert_eq!(
            a.select_columns[1].aggregate,
            Some(AggregateKind::CountStar)
        );
        assert_eq!(a.select_columns[1].aggregate_arg.as_deref(), Some("*"));
    }

    #[test]
    fn test_passthrough_columns() {
        let a = parse_and_analyze("SELECT id, name, SUM(amount) FROM orders GROUP BY id, name");
        assert!(a.select_columns[0].is_passthrough);
        assert!(a.select_columns[1].is_passthrough);
        assert!(!a.select_columns[2].is_passthrough);
        assert_eq!(a.group_by_columns, vec!["id", "name"]);
    }

    #[test]
    fn test_min_max_detection() {
        let a = parse_and_analyze(
            "SELECT city, MIN(salary), MAX(salary) FROM emp GROUP BY city",
        );
        assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Min));
        assert_eq!(a.select_columns[2].aggregate, Some(AggregateKind::Max));
    }

    #[test]
    fn test_cte_extracted() {
        let a = parse_and_analyze(
            "WITH regional AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region) SELECT region, total FROM regional",
        );
        assert!(a.unsupported_reason().is_none()); // Non-recursive CTE is now supported
        assert_eq!(a.ctes.len(), 1);
        assert_eq!(a.ctes[0].alias, "regional");
        assert!(a.ctes[0].query_sql.contains("SUM"));
    }

    #[test]
    fn test_cte_extraction_multiple() {
        let a = parse_and_analyze(
            "WITH a AS (SELECT id, COUNT(*) AS cnt FROM t1 GROUP BY id), \
             b AS (SELECT id, SUM(cnt) AS total FROM a GROUP BY id) \
             SELECT * FROM b",
        );
        assert_eq!(a.ctes.len(), 2);
        assert_eq!(a.ctes[0].alias, "a");
        assert_eq!(a.ctes[1].alias, "b");
    }

    #[test]
    fn test_recursive_cte_rejected() {
        let a = parse_and_analyze(
            "WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM nums WHERE n < 10) SELECT * FROM nums",
        );
        assert!(a.has_recursive_cte);
        assert!(a.unsupported_reason().is_some());
    }

    #[test]
    fn test_unsupported_limit() {
        let a = parse_and_analyze("SELECT * FROM emp LIMIT 10");
        assert!(a.unsupported_reason().is_some());
        assert!(a.has_limit);
    }

    #[test]
    fn test_unsupported_order_by() {
        let a = parse_and_analyze("SELECT * FROM emp ORDER BY id");
        assert!(a.unsupported_reason().is_some());
        assert!(a.has_order_by);
    }

    #[test]
    fn test_window_detected() {
        let a = parse_and_analyze("SELECT id, SUM(amount) OVER (PARTITION BY city) FROM orders");
        assert!(a.unsupported_reason().is_none(), "Window functions should no longer be unsupported");
        assert!(a.has_window_function);
        // The window column should be flagged
        let win_col = a.select_columns.iter().find(|c| c.is_window);
        assert!(win_col.is_some(), "Should detect window function in SELECT");
    }

    #[test]
    fn test_multiple_queries_error() {
        let parsed =
            Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 1; SELECT 2").unwrap();
        assert!(matches!(
            analyze(&parsed),
            Err(SqlAnalysisError::MultipleQueries(2))
        ));
    }

    #[test]
    fn test_not_select_error() {
        let parsed =
            Parser::parse_sql(&PostgreSqlDialect {}, "CREATE TABLE t (id INT)").unwrap();
        assert!(matches!(
            analyze(&parsed),
            Err(SqlAnalysisError::NotASelectQuery)
        ));
    }

    #[test]
    fn test_from_clause_sql() {
        let a = parse_and_analyze("SELECT a.x FROM emp a JOIN sales b ON a.id = b.eid");
        assert!(!a.from_clause_sql.is_empty());
    }

    #[test]
    fn test_having_clause() {
        let a = parse_and_analyze(
            "SELECT city, COUNT(*) AS cnt FROM emp GROUP BY city HAVING COUNT(*) > 5",
        );
        assert!(a.having_clause.is_some());
    }

    #[test]
    fn test_cast_aggregate_detected() {
        let a = parse_and_analyze(
            "SELECT city, SUM(amount)::BIGINT AS total FROM orders GROUP BY city",
        );
        assert_eq!(a.select_columns.len(), 2);
        assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Sum));
        assert_eq!(a.select_columns[1].aggregate_arg.as_deref(), Some("amount"));
        assert_eq!(a.select_columns[1].alias.as_deref(), Some("total"));
    }

    #[test]
    fn test_multiple_cast_aggregates() {
        let a = parse_and_analyze(
            "SELECT grp, SUM(a)::BIGINT AS sa, COUNT(*)::INT AS cnt FROM t GROUP BY grp",
        );
        assert_eq!(a.select_columns[1].aggregate, Some(AggregateKind::Sum));
        assert_eq!(a.select_columns[2].aggregate, Some(AggregateKind::CountStar));
    }

    #[test]
    fn test_malformed_sql_parse_error() {
        let result = Parser::parse_sql(&PostgreSqlDialect {}, "SELEC broken garbage !!!");
        assert!(result.is_err(), "Malformed SQL should fail to parse");
    }

    #[test]
    fn test_table_aliases() {
        let a = parse_and_analyze(
            "SELECT s.product_id, s.amount, p.name FROM sales s JOIN products p ON s.product_id = p.id",
        );
        assert_eq!(a.table_aliases.get("s").map(String::as_str), Some("sales"));
        assert_eq!(a.table_aliases.get("p").map(String::as_str), Some("products"));
    }

    #[test]
    fn test_table_aliases_schema_qualified() {
        let a = parse_and_analyze(
            "SELECT s.id FROM alp.sales_simulation s JOIN dim.products p ON s.product_id = p.id",
        );
        assert_eq!(a.table_aliases.get("s").map(String::as_str), Some("alp.sales_simulation"));
        assert_eq!(a.table_aliases.get("p").map(String::as_str), Some("dim.products"));
    }

    // ========================================================================
    // Unsupported feature detection tests
    // ========================================================================

    #[test]
    fn test_detect_lateral_join() {
        let a = parse_and_analyze(
            "SELECT t.id, s.val FROM t, LATERAL (SELECT val FROM t2 WHERE t2.id = t.id) s",
        );
        assert!(a.has_lateral_join);
        assert!(a.unsupported_reason().is_some());
        assert!(a.unsupported_reason().unwrap().contains("LATERAL"));
    }

    #[test]
    fn test_detect_distinct_on() {
        let a = parse_and_analyze(
            "SELECT DISTINCT ON (city) city, val FROM t",
        );
        assert!(a.has_distinct_on);
        assert!(!a.has_distinct, "DISTINCT ON should not set has_distinct");
        assert!(a.unsupported_reason().is_some());
        assert!(a.unsupported_reason().unwrap().contains("DISTINCT ON"));
    }

    #[test]
    fn test_detect_grouping_sets() {
        let a = parse_and_analyze(
            "SELECT city, SUM(val) FROM t GROUP BY GROUPING SETS ((city), ())",
        );
        assert!(a.has_grouping_sets);
        assert!(a.unsupported_reason().is_some());
    }

    #[test]
    fn test_detect_cube() {
        let a = parse_and_analyze(
            "SELECT city, state, SUM(val) FROM t GROUP BY CUBE (city, state)",
        );
        assert!(a.has_grouping_sets);
        assert!(a.unsupported_reason().is_some());
    }

    #[test]
    fn test_detect_rollup() {
        let a = parse_and_analyze(
            "SELECT city, SUM(val) FROM t GROUP BY ROLLUP (city)",
        );
        assert!(a.has_grouping_sets);
        assert!(a.unsupported_reason().is_some());
    }

    #[test]
    fn test_detect_filter_clause() {
        let a = parse_and_analyze(
            "SELECT city, COUNT(*) FILTER (WHERE active) FROM t GROUP BY city",
        );
        assert!(a.has_filter_clause);
        assert!(a.unsupported_reason().is_some());
        assert!(a.unsupported_reason().unwrap().contains("FILTER"));
    }

    #[test]
    fn test_detect_within_group() {
        let a = parse_and_analyze(
            "SELECT city, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) FROM t GROUP BY city",
        );
        assert!(a.has_within_group);
        assert!(a.unsupported_reason().is_some());
    }

    #[test]
    fn test_detect_tablesample() {
        let a = parse_and_analyze(
            "SELECT * FROM t TABLESAMPLE BERNOULLI (10)",
        );
        assert!(a.has_tablesample);
        assert!(a.unsupported_reason().is_some());
        assert!(a.unsupported_reason().unwrap().contains("TABLESAMPLE"));
    }

    #[test]
    fn test_detect_nondeterministic_select() {
        let a = parse_and_analyze(
            "SELECT NOW(), city FROM t GROUP BY city",
        );
        assert!(a.has_nondeterministic_select);
        assert!(a.unsupported_reason().is_some());

        let b = parse_and_analyze(
            "SELECT RANDOM(), id FROM t",
        );
        assert!(b.has_nondeterministic_select);
    }

    #[test]
    fn test_detect_unsupported_aggregate_string_agg() {
        let a = parse_and_analyze(
            "SELECT city, STRING_AGG(name, ', ') FROM t GROUP BY city",
        );
        assert!(!a.unsupported_aggregates.is_empty());
        assert!(a.unsupported_aggregates.contains(&"STRING_AGG".to_string()));
        assert!(a.unsupported_reason().is_some());
    }

    #[test]
    fn test_detect_unsupported_aggregate_array_agg() {
        let a = parse_and_analyze(
            "SELECT city, ARRAY_AGG(val) FROM t GROUP BY city",
        );
        assert!(a.unsupported_aggregates.contains(&"ARRAY_AGG".to_string()));
    }

    #[test]
    fn test_detect_unsupported_aggregate_stddev() {
        let a = parse_and_analyze(
            "SELECT city, STDDEV(val) FROM t GROUP BY city",
        );
        assert!(a.unsupported_aggregates.contains(&"STDDEV".to_string()));
    }

    #[test]
    fn test_detect_scalar_subquery() {
        let a = parse_and_analyze(
            "SELECT (SELECT MAX(x) FROM t2), city FROM t GROUP BY city",
        );
        assert!(a.has_scalar_subquery);
        assert!(a.unsupported_reason().is_some());
    }

    #[test]
    fn test_supported_aggregates_not_flagged() {
        let a = parse_and_analyze(
            "SELECT city, SUM(val), COUNT(*), AVG(val), MIN(val), MAX(val), BOOL_OR(flag) \
             FROM t GROUP BY city",
        );
        assert!(a.unsupported_aggregates.is_empty(),
            "Supported aggregates should not be flagged: {:?}", a.unsupported_aggregates);
        assert!(!a.has_filter_clause);
        assert!(!a.has_within_group);
        assert!(!a.has_nondeterministic_select);
        assert!(a.unsupported_reason().is_none(),
            "Query with only supported features should pass: {:?}", a.unsupported_reason());
    }

    #[test]
    fn test_regular_functions_not_flagged_as_aggregates() {
        // UPPER, LOWER, COALESCE etc. are scalar functions, not aggregates
        let a = parse_and_analyze(
            "SELECT UPPER(name), COALESCE(val, 0) FROM t",
        );
        assert!(a.unsupported_aggregates.is_empty(),
            "Regular scalar functions should not be flagged: {:?}", a.unsupported_aggregates);
    }

    #[test]
    fn test_multiple_unsupported_aggregates() {
        let a = parse_and_analyze(
            "SELECT city, STRING_AGG(name, ','), ARRAY_AGG(val), STDDEV(val) FROM t GROUP BY city",
        );
        assert_eq!(a.unsupported_aggregates.len(), 3);
    }

    mod proptest_tests {
        use super::*;
        use proptest::prelude::*;

        /// Generate a random supported aggregate expression
        fn supported_agg_strategy() -> impl Strategy<Value = (&'static str, String)> {
            prop_oneof![
                Just(("SUM", "SUM(val)".to_string())),
                Just(("COUNT", "COUNT(val)".to_string())),
                Just(("COUNT", "COUNT(*)".to_string())),
                Just(("AVG", "AVG(val)".to_string())),
                Just(("MIN", "MIN(val)".to_string())),
                Just(("MAX", "MAX(val)".to_string())),
                Just(("BOOL_OR", "BOOL_OR(flag)".to_string())),
            ]
        }

        /// Generate a random unsupported aggregate name
        fn unsupported_agg_strategy() -> impl Strategy<Value = &'static str> {
            prop_oneof![
                Just("STRING_AGG"),
                Just("ARRAY_AGG"),
                Just("JSON_AGG"),
                Just("JSONB_AGG"),
                Just("STDDEV"),
                Just("VARIANCE"),
                Just("BOOL_AND"),
                Just("EVERY"),
                Just("BIT_AND"),
                Just("BIT_OR"),
                Just("MODE"),
            ]
        }

        proptest! {
            /// Any query using only supported aggregates should pass validation
            #[test]
            fn supported_sql_passes_validation(
                agg1 in supported_agg_strategy(),
                agg2 in supported_agg_strategy(),
                has_where in any::<bool>(),
            ) {
                let where_clause = if has_where { " WHERE val > 0" } else { "" };
                let sql = format!(
                    "SELECT grp, {} AS a1, {} AS a2 FROM tbl{} GROUP BY grp",
                    agg1.1, agg2.1, where_clause
                );
                let a = parse_and_analyze(&sql);
                prop_assert!(a.unsupported_reason().is_none(),
                    "Supported query should pass: {} => {:?}", sql, a.unsupported_reason());
            }

            /// Any query using an unsupported aggregate should be detected
            #[test]
            fn unsupported_aggregate_always_detected(
                agg_name in unsupported_agg_strategy(),
            ) {
                // STRING_AGG needs two args, others need one
                let expr = if agg_name == "STRING_AGG" {
                    format!("{}(name, ',')", agg_name)
                } else {
                    format!("{}(val)", agg_name)
                };
                let sql = format!(
                    "SELECT grp, {} AS a FROM tbl GROUP BY grp",
                    expr
                );
                let a = parse_and_analyze(&sql);
                prop_assert!(!a.unsupported_aggregates.is_empty(),
                    "{} should be detected as unsupported in: {}", agg_name, sql);
            }

            /// Non-deterministic functions are always detected
            #[test]
            fn nondeterministic_always_detected(
                func in prop_oneof![
                    Just("NOW()"),
                    Just("RANDOM()"),
                    Just("CURRENT_TIMESTAMP"),
                    Just("CLOCK_TIMESTAMP()"),
                    Just("GEN_RANDOM_UUID()"),
                ],
            ) {
                let sql = format!("SELECT {}, grp FROM tbl GROUP BY grp", func);
                let a = parse_and_analyze(&sql);
                prop_assert!(a.has_nondeterministic_select,
                    "{} should be detected as non-deterministic in: {}", func, sql);
            }
        }
    }
}
