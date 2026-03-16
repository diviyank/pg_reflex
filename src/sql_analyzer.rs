use sqlparser::ast::{Expr, GroupByExpr, Query, SetExpr, Statement, TableFactor, Visit, Visitor};
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

#[derive(Debug, Default)]
pub struct SqlAnalysis {
    pub has_cte: bool,
    pub has_window_function: bool,
    pub has_limit: bool,
    pub has_order_by: bool,
    pub sources: Vec<String>,
    pub group_by_columns: Vec<String>,
}

impl SqlAnalysis {
    pub fn has_unsupported_features(&self) -> bool {
        self.has_cte || self.has_window_function || self.has_limit || self.has_order_by
    }
}

struct AnalysisVisitor<'a> {
    analysis: &'a mut SqlAnalysis,
}

impl<'a> Visitor for AnalysisVisitor<'a> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if query
            .with
            .as_ref()
            .map_or(false, |w| !w.cte_tables.is_empty())
        {
            self.analysis.has_cte = true;
        }
        if query.limit_clause.is_some() {
            self.analysis.has_limit = true;
        }
        if query
            .order_by
            .as_ref()
            .map_or(false, |o| matches!(&o.kind, sqlparser::ast::OrderByKind::Expressions(e) if !e.is_empty()))
        {
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
            TableFactor::Table { name, .. } => {
                self.analysis.sources.push(name.to_string());
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

pub fn analyze(statements: &[Statement]) -> Result<SqlAnalysis, SqlAnalysisError> {
    if statements.len() != 1 {
        return Err(SqlAnalysisError::MultipleQueries(statements.len()));
    }

    let query = match &statements[0] {
        Statement::Query(q) => q,
        _ => return Err(SqlAnalysisError::NotASelectQuery),
    };

    let mut analysis = SqlAnalysis::default();
    let mut visitor = AnalysisVisitor {
        analysis: &mut analysis,
    };
    let _ = query.visit(&mut visitor);

    if let SetExpr::Select(ref select) = *query.body {
        if let GroupByExpr::Expressions(ref exprs, _) = select.group_by {
            for expr in exprs {
                analysis.group_by_columns.push(expr.to_string());
            }
        }
    }

    Ok(analysis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_and_analyze(sql: &str) -> Result<SqlAnalysis, SqlAnalysisError> {
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).unwrap();
        analyze(&stmts)
    }

    #[test]
    fn test_simple_select_sources() {
        let a = parse_and_analyze("SELECT a, b FROM foo JOIN bar ON foo.id = bar.id").unwrap();
        assert_eq!(a.sources, vec!["foo", "bar"]);
        assert!(!a.has_unsupported_features());
    }

    #[test]
    fn test_single_table() {
        let a = parse_and_analyze("SELECT * FROM orders").unwrap();
        assert_eq!(a.sources, vec!["orders"]);
    }

    #[test]
    fn test_group_by_columns() {
        let a = parse_and_analyze(
            "SELECT category, region, SUM(amount) FROM sales GROUP BY category, region",
        )
        .unwrap();
        assert_eq!(a.group_by_columns, vec!["category", "region"]);
    }

    #[test]
    fn test_no_group_by() {
        let a = parse_and_analyze("SELECT * FROM sales").unwrap();
        assert!(a.group_by_columns.is_empty());
    }

    #[test]
    fn test_group_by_single_column() {
        let a =
            parse_and_analyze("SELECT status, COUNT(*) FROM orders GROUP BY status").unwrap();
        assert_eq!(a.group_by_columns, vec!["status"]);
    }

    #[test]
    fn test_detects_cte() {
        let a =
            parse_and_analyze("WITH cte AS (SELECT 1) SELECT * FROM cte").unwrap();
        assert!(a.has_cte);
        assert!(a.has_unsupported_features());
    }

    #[test]
    fn test_detects_limit() {
        let a = parse_and_analyze("SELECT * FROM foo LIMIT 10").unwrap();
        assert!(a.has_limit);
        assert!(a.has_unsupported_features());
    }

    #[test]
    fn test_detects_order_by() {
        let a = parse_and_analyze("SELECT * FROM foo ORDER BY id").unwrap();
        assert!(a.has_order_by);
        assert!(a.has_unsupported_features());
    }

    #[test]
    fn test_detects_window_function() {
        let a = parse_and_analyze(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY id) FROM foo",
        )
        .unwrap();
        assert!(a.has_window_function);
        assert!(a.has_unsupported_features());
    }

    #[test]
    fn test_multiple_queries_error() {
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, "SELECT 1; SELECT 2").unwrap();
        let err = analyze(&stmts).unwrap_err();
        assert!(matches!(err, SqlAnalysisError::MultipleQueries(2)));
    }

    #[test]
    fn test_not_a_select_error() {
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, "CREATE TABLE foo (id INT)").unwrap();
        let err = analyze(&stmts).unwrap_err();
        assert!(matches!(err, SqlAnalysisError::NotASelectQuery));
    }

    #[test]
    fn test_subquery_source() {
        let a = parse_and_analyze(
            "SELECT * FROM (SELECT id FROM foo) AS sub",
        )
        .unwrap();
        assert!(a.sources.iter().any(|s| s.contains("subquery")));
        assert!(a.sources.iter().any(|s| s == "foo"));
    }

    #[test]
    fn test_schema_qualified_table() {
        let a = parse_and_analyze("SELECT * FROM public.orders").unwrap();
        assert_eq!(a.sources, vec!["public.orders"]);
    }

    #[test]
    fn test_multiple_joins() {
        let a = parse_and_analyze(
            "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id",
        )
        .unwrap();
        assert_eq!(a.sources, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_group_by_with_join() {
        let a = parse_and_analyze(
            "SELECT o.status, SUM(oi.amount) FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.status",
        )
        .unwrap();
        assert_eq!(a.sources, vec!["orders", "order_items"]);
        assert_eq!(a.group_by_columns, vec!["o.status"]);
    }

    #[test]
    fn test_clean_query_no_unsupported() {
        let a = parse_and_analyze(
            "SELECT category, SUM(price) FROM products WHERE active = true GROUP BY category",
        )
        .unwrap();
        assert!(!a.has_unsupported_features());
        assert!(!a.has_cte);
        assert!(!a.has_limit);
        assert!(!a.has_order_by);
        assert!(!a.has_window_function);
    }
}
