use sqlparser::ast::{Expr, Query, Statement, TableFactor, Visit, Visitor};
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
        if query.limit.is_some() {
            self.analysis.has_limit = true;
        }
        if query
            .order_by
            .as_ref()
            .map_or(false, |o| !o.exprs.is_empty())
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
    query.visit(&mut visitor);

    Ok(analysis)
}
