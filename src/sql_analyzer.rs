use serde::Serialize;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Join, JoinConstraint,
    JoinOperator, Query, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor,
    Visit, Visitor,
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
}

impl SqlAnalysis {
    pub fn has_unsupported_features(&self) -> bool {
        self.has_recursive_cte || self.has_limit || self.has_order_by
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
        if let Expr::Function(f) = expr {
            if f.over.is_some() {
                self.analysis.has_window_function = true;
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                self.analysis.sources.push(table_name.clone());
                if let Some(a) = alias {
                    self.analysis
                        .table_aliases
                        .insert(a.name.to_string(), table_name);
                }
            }
            TableFactor::Derived { alias, .. } => {
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

    // DISTINCT
    analysis.has_distinct = select.distinct.is_some();

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

    // GROUP BY
    if let GroupByExpr::Expressions(exprs, _modifiers) = &select.group_by {
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
        assert!(!a.has_unsupported_features()); // Non-recursive CTE is now supported
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
        assert!(a.has_unsupported_features());
    }

    #[test]
    fn test_unsupported_limit() {
        let a = parse_and_analyze("SELECT * FROM emp LIMIT 10");
        assert!(a.has_unsupported_features());
        assert!(a.has_limit);
    }

    #[test]
    fn test_unsupported_order_by() {
        let a = parse_and_analyze("SELECT * FROM emp ORDER BY id");
        assert!(a.has_unsupported_features());
        assert!(a.has_order_by);
    }

    #[test]
    fn test_window_detected() {
        let a = parse_and_analyze("SELECT id, SUM(amount) OVER (PARTITION BY city) FROM orders");
        assert!(!a.has_unsupported_features(), "Window functions should no longer be unsupported");
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
}
