use serde::Serialize;
use sqlparser::ast::{
    BinaryOperator, Distinct, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, GroupByWithModifier, Ident, Join, JoinConstraint, JoinOperator, Query, Select,
    SelectItem, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, Visit, VisitMut,
    Visitor, VisitorMut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
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
    #[cfg(test)]
    pub filter_expr: Option<String>,
    /// True if the expression contains aggregate functions but is not a simple aggregate
    /// (e.g., CASE WHEN SUM(x) > 0 THEN SUM(x)/SUM(y) END). The constituent aggregates
    /// are extracted separately and this expression is rewritten in the end_query.
    pub is_aggregate_derived: bool,
    /// True if the expression holds an aggregate in a place the aggregate mapping does
    /// not rewrite (under IS NULL, BETWEEN, IN, LIKE, an array constructor, ...). Such an
    /// item is evaluated by PostgreSQL as written: it is neither a group key nor an
    /// aggregate pg_reflex maintains.
    pub holds_undecomposed_aggregate: bool,
}

#[derive(Debug, Clone)]
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

pub(crate) fn bare_function_name(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.to_ascii_lowercase())
        .unwrap_or_default()
}

/// The schema a call is qualified with, as PostgreSQL resolves it: an unquoted name
/// folds to lower case, a quoted one is exact.
fn function_schema(name: &sqlparser::ast::ObjectName) -> Option<String> {
    let schema = name
        .0
        .len()
        .checked_sub(2)
        .and_then(|position| name.0[position].as_ident())?;
    Some(if schema.quote_style.is_some() {
        schema.value.clone()
    } else {
        schema.value.to_ascii_lowercase()
    })
}

/// The lower-case name of a call that can only mean a `pg_catalog` function: the call is
/// unqualified or qualified with `pg_catalog`, and its name is unquoted or quoted in
/// lower case. `"SUM"(x)` and `other.sum(x)` name other functions.
fn builtin_function_name(name: &sqlparser::ast::ObjectName) -> Option<String> {
    let ident = name.0.last()?.as_ident()?;
    let schema_is_builtin = match function_schema(name) {
        None => name.0.len() == 1,
        Some(schema) => schema == "pg_catalog" && name.0.len() == 2,
    };
    let is_exact_lower_case = ident.value == ident.value.to_ascii_lowercase();
    (schema_is_builtin && (ident.quote_style.is_none() || is_exact_lower_case))
        .then(|| ident.value.to_ascii_lowercase())
}

fn builtin_aggregate_kind(f: &Function) -> Option<AggregateKind> {
    builtin_function_name(&f.name).and_then(|name| detect_aggregate(&name))
}

/// A called function as the catalog lookup needs it: `schema` is `None` for an
/// unqualified call, which PostgreSQL resolves through the caller's `search_path`.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionReference {
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CalledFunction {
    pub name: String,
    pub arg_count: usize,
}

/// A call whose spelling cannot be taken for a `pg_catalog` function: `name` is its
/// bare lower-case name, `written` the name as the query spells it.
#[derive(Debug, Clone, PartialEq)]
pub struct NonBuiltinCall {
    pub name: String,
    pub written: String,
}

const KEYWORD_PSEUDO_FUNCTIONS: [&str; 12] = [
    "current_user",
    "session_user",
    "system_user",
    "user",
    "current_role",
    "current_catalog",
    "current_schema",
    "localtime",
    "localtimestamp",
    "current_date",
    "current_time",
    "current_timestamp",
];

/// `None` for a qualified column whose last part is a keyword (`t.user`), which the
/// parser hands over as an argument-less function with a multi-part name.
fn called_function(f: &sqlparser::ast::Function) -> Option<CalledFunction> {
    let arg_count = match &f.args {
        FunctionArguments::List(list) => list.args.len(),
        FunctionArguments::None if f.name.0.len() > 1 => return None,
        _ => 0,
    };
    Some(CalledFunction {
        name: bare_function_name(&f.name),
        arg_count,
    })
}

/// An unquoted bare occurrence of a reserved word that PostgreSQL evaluates as a function.
fn keyword_pseudo_function(ident: &Ident) -> Option<CalledFunction> {
    if ident.quote_style.is_some() {
        return None;
    }
    let name = ident.value.to_ascii_lowercase();
    KEYWORD_PSEUDO_FUNCTIONS
        .contains(&name.as_str())
        .then_some(CalledFunction { name, arg_count: 0 })
}

const MOVING_DATE_TIME_WORDS: [&str; 4] = ["now", "today", "tomorrow", "yesterday"];

const DATE_TIME_TYPES: [&str; 11] = [
    "date",
    "time",
    "timetz",
    "timestamp",
    "timestamptz",
    "daterange",
    "tsrange",
    "tstzrange",
    "datemultirange",
    "tsmultirange",
    "tstzmultirange",
];

/// `type_name` as written (`timestamptz`, `pg_catalog.date`, `time(3) with time zone`,
/// `date[]`) names a built-in type whose input reads the special date/time words.
fn is_date_time_type(type_name: &str) -> bool {
    let lower = type_name.to_ascii_lowercase().replace('"', "");
    let unqualified = lower.strip_prefix("pg_catalog.").unwrap_or(&lower);
    let base = unqualified
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .next()
        .unwrap_or_default();
    DATE_TIME_TYPES.contains(&base)
}

/// The date/time input words that stand for the moment of evaluation. Any other word
/// (`epoch`, `infinity`, `allballs`) is a fixed value.
fn holds_moving_date_time_word(literal: &str) -> bool {
    literal
        .to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphabetic())
        .any(|word| MOVING_DATE_TIME_WORDS.contains(&word))
}

fn string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Nested(inner) => string_literal(inner),
        Expr::Value(value) => value.clone().into_string(),
        _ => None,
    }
}

/// A string literal given a date/time type whose value depends on when it is read:
/// `'now'::timestamptz`, `DATE 'today'`, `date('yesterday')`.
fn is_moving_date_time_literal(expr: &Expr) -> bool {
    let (type_name, literal) = match expr {
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => (data_type.to_string(), string_literal(inner)),
        Expr::TypedString(typed) => (
            typed.data_type.to_string(),
            typed.value.clone().into_string(),
        ),
        Expr::Function(f) => {
            let only_argument = match &f.args {
                FunctionArguments::List(list) => match list.args.as_slice() {
                    [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] => {
                        string_literal(argument)
                    }
                    _ => None,
                },
                _ => None,
            };
            (
                builtin_function_name(&f.name).unwrap_or_default(),
                only_argument,
            )
        }
        _ => return false,
    };
    is_date_time_type(&type_name) && literal.is_some_and(|text| holds_moving_date_time_word(&text))
}

/// A select item PostgreSQL refuses as a grouping key, or reads as a position: a bare
/// string, NULL, boolean or non-integer literal, or a negated number, which its grammar
/// folds into a constant (`+1` and `~1` stay operator expressions). An unsigned integer
/// literal is left alone.
fn is_constant_refused_as_group_key(expr: &Expr) -> bool {
    let is_unsigned_integer =
        |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    match expr {
        Expr::Nested(inner) => is_constant_refused_as_group_key(inner),
        Expr::Value(value) => match &value.value {
            sqlparser::ast::Value::Number(number, _) => !is_unsigned_integer(&number.to_string()),
            sqlparser::ast::Value::Placeholder(_) => false,
            _ => true,
        },
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr: operand,
        } => {
            let mut negated = operand.as_ref();
            while let Expr::Nested(inner) = negated {
                negated = inner;
            }
            matches!(
                negated,
                Expr::Value(value) if matches!(value.value, sqlparser::ast::Value::Number(..))
            )
        }
        _ => false,
    }
}

/// The function a FROM item calls, by the name the query gives it.
fn from_function_name(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table {
            name,
            args: Some(_),
            ..
        } => Some(name.to_string()),
        TableFactor::Function { name, .. } => Some(name.to_string()),
        TableFactor::UNNEST { .. } => Some("unnest".to_string()),
        TableFactor::TableFunction { expr, .. } => Some(expr.to_string()),
        TableFactor::JsonTable { .. } => Some("json_table".to_string()),
        TableFactor::OpenJsonTable { .. } => Some("openjson".to_string()),
        TableFactor::XmlTable { .. } => Some("xmltable".to_string()),
        _ => None,
    }
}

fn factor_is_function_call(factor: &TableFactor) -> bool {
    matches!(factor, TableFactor::Table { args: Some(_), .. })
}

fn collect_from_functions(factor: &TableFactor, functions: &mut Vec<String>) {
    if let TableFactor::NestedJoin {
        table_with_joins, ..
    } = factor
    {
        collect_from_functions(&table_with_joins.relation, functions);
        for join in &table_with_joins.joins {
            collect_from_functions(&join.relation, functions);
        }
    }
    if let Some(name) = from_function_name(factor) {
        if !functions.contains(&name) {
            functions.push(name);
        }
    }
}

const DISTINCT_AGGREGATE_REJECTION: &str = "DISTINCT modifier on SUM/AVG/MIN/MAX/BOOL_OR is not supported. \
     Only COUNT(DISTINCT col) is supported. Use a CTE with SELECT DISTINCT \
     to pre-deduplicate: WITH d AS (SELECT DISTINCT grp, val FROM t) SELECT grp, SUM(val) FROM d GROUP BY grp";

const NESTED_COUNT_DISTINCT_REJECTION: &str = "COUNT(DISTINCT) nested inside an expression is not supported (the DISTINCT would be lost). \
     Compute it in a CTE and apply the expression in the outer SELECT: \
     WITH d AS (SELECT grp, COUNT(DISTINCT val) AS n FROM t GROUP BY grp) SELECT grp, n + 1 AS c FROM d";

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
    /// A CTE, derived table or subquery of any kind below the top-level query.
    pub has_nested_query: bool,
    pub unsupported_aggregates: Vec<String>,
    pub distinct_aggregates: Vec<String>,
    pub nested_count_distinct: bool,
    /// Every non-window function call and keyword pseudo-function in the query, by bare
    /// lower-case name, once per distinct (name, argument count).
    pub called_functions: Vec<CalledFunction>,
    /// The same calls once per distinct (schema, bare name).
    pub function_references: Vec<FunctionReference>,
    pub has_extract: bool,
    /// Calls spelled so that they cannot be taken for a `pg_catalog` function.
    pub non_builtin_calls: Vec<NonBuiltinCall>,
    /// The functions called in the top-level SELECT list, outside subqueries and window
    /// functions.
    pub select_list_calls: Vec<WrittenCall>,
    /// The name PostgreSQL gives each output column that has one by the query's text: an
    /// alias, or the column of a bare column reference. One entry per such select item.
    pub named_outputs: Vec<String>,
    /// Those of `named_outputs` whose select item is a bare, optionally qualified or
    /// parenthesised, reference to a column of that same name.
    pub columns_output_under_their_own_name: Vec<String>,
    /// SELECT items PostgreSQL refuses as grouping keys (see
    /// `is_constant_refused_as_group_key`).
    pub constants_refused_as_group_key: Vec<String>,
    /// Functions used as FROM items: in the top-level FROM clause, or anywhere when
    /// written `name(args)`, which the source collection takes for a table.
    pub from_functions: Vec<String>,
    /// Set when a clock or session-identity function is called anywhere in the
    /// query, not only in the SELECT list.
    pub has_nondeterministic_select: bool,
    /// The clock and session-identity functions that set `has_nondeterministic_select`.
    /// They are matched by name when unqualified or qualified with `pg_catalog`; a call
    /// qualified with another schema is left to the catalog volatility check.
    pub nondeterministic_functions: Vec<String>,
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
            return Some(format!(
                "Non-deterministic functions (NOW, CURRENT_TIMESTAMP, RANDOM, etc.) \
                 are not supported anywhere in the query — they would cause the IMV \
                 to drift over time without a corresponding source mutation. Fix: remove \
                 {} from the IMV query, or compute the value into a source column",
                self.nondeterministic_functions.join(", ")
            ));
        }
        if !self.unsupported_aggregates.is_empty() {
            let names = self.unsupported_aggregates.join(", ");
            return Some(format!(
                "Unsupported aggregate(s): {}. Supported: SUM, COUNT, AVG, MIN, MAX, BOOL_OR",
                names
            ));
        }
        if !self.distinct_aggregates.is_empty() {
            return Some(DISTINCT_AGGREGATE_REJECTION.into());
        }
        if self.nested_count_distinct {
            return Some(NESTED_COUNT_DISTINCT_REJECTION.into());
        }
        if !self.from_functions.is_empty() {
            return Some(format!(
                "A function in FROM ({}) is not supported: an IMV is maintained from the rows \
                 written to its source tables, and a function is not one. Fix: store the \
                 function's rows in a table and join that table; a function of the row's own \
                 columns can be called in the SELECT list instead \
                 (SELECT g, unnest(arr) AS e FROM t)",
                self.from_functions.join(", ")
            ));
        }
        let groups_rows = !self.group_by_columns.is_empty() || self.has_distinct;
        let undecomposed_aggregate = self
            .select_columns
            .iter()
            .find(|column| column.holds_undecomposed_aggregate);
        if let (true, Some(column)) = (groups_rows, undecomposed_aggregate) {
            return Some(format!(
                "'{}' holds an aggregate inside an expression pg_reflex does not decompose \
                 (IS NULL, BETWEEN, IN, LIKE, an array and similar), so it is neither a group \
                 key nor an aggregate it maintains. Fix: compute the aggregate in a CTE and \
                 apply the expression in the outer SELECT: WITH a AS (SELECT grp, SUM(val) AS s \
                 FROM t GROUP BY grp) SELECT grp, s IS NULL AS c FROM a",
                column.alias.as_deref().unwrap_or(&column.expr_sql)
            ));
        }
        if groups_rows && !self.constants_refused_as_group_key.is_empty() {
            return Some(format!(
                "The constant {} in the SELECT list beside GROUP BY or DISTINCT is not \
                 supported: PostgreSQL refuses a bare constant as a grouping key. Fix: cast it \
                 to its type, e.g. 'plain'::text, NULL::text, TRUE::boolean, 3.5::numeric",
                self.constants_refused_as_group_key.join(", ")
            ));
        }
        None
    }

    pub fn timezone_sensitive_functions(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .called_functions
            .iter()
            .filter(|call| is_timezone_sensitive_function(call))
            .map(|call| call.name.clone())
            .collect();
        if self.has_extract {
            names.push("extract".to_string());
        }
        names.sort();
        names.dedup();
        names
    }

    fn record_nondeterministic(&mut self, name: String) {
        self.has_nondeterministic_select = true;
        if !self.nondeterministic_functions.contains(&name) {
            self.nondeterministic_functions.push(name);
        }
    }

    fn record_called_function(&mut self, call: CalledFunction, schema: Option<String>) {
        let is_builtin_spelling = !matches!(schema.as_deref(), Some(s) if s != "pg_catalog");
        if is_builtin_spelling && is_clock_or_session_function(&call) {
            self.record_nondeterministic(call.name.clone());
        }
        let reference = FunctionReference {
            schema,
            name: call.name.clone(),
        };
        if !self.function_references.contains(&reference) {
            self.function_references.push(reference);
        }
        if !self.called_functions.contains(&call) {
            self.called_functions.push(call);
        }
    }
}

struct AnalysisVisitor<'a> {
    analysis: &'a mut SqlAnalysis,
    top_level_query_seen: bool,
}

impl<'a> Visitor for AnalysisVisitor<'a> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        self.analysis.has_nested_query = self.top_level_query_seen;
        self.top_level_query_seen = true;
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
        if is_moving_date_time_literal(expr) {
            self.analysis.record_nondeterministic(expr.to_string());
        }
        match expr {
            Expr::Function(f) => {
                if let FunctionArguments::List(list) = &f.args {
                    if list.duplicate_treatment
                        == Some(sqlparser::ast::DuplicateTreatment::Distinct)
                    {
                        let bare = bare_function_name(&f.name);
                        if matches!(bare.as_str(), "sum" | "avg" | "min" | "max" | "bool_or")
                            && !self.analysis.distinct_aggregates.contains(&bare)
                        {
                            self.analysis.distinct_aggregates.push(bare);
                        }
                    }
                }
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
                    if let Some(call) = called_function(f) {
                        if builtin_function_name(&f.name).is_none() {
                            let spelling = NonBuiltinCall {
                                name: call.name.clone(),
                                written: f.name.to_string(),
                            };
                            if !self.analysis.non_builtin_calls.contains(&spelling) {
                                self.analysis.non_builtin_calls.push(spelling);
                            }
                        }
                        self.analysis
                            .record_called_function(call, function_schema(&f.name));
                    }
                }
            }
            Expr::Identifier(ident) => {
                if let Some(call) = keyword_pseudo_function(ident) {
                    self.analysis.record_called_function(call, None);
                }
            }
            Expr::Extract { .. } => {
                self.analysis.has_extract = true;
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
                if factor_is_function_call(factor) {
                    collect_from_functions(factor, &mut self.analysis.from_functions);
                }
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

/// Whether `expr` holds a call to a built-in aggregate pg_reflex maintains, anywhere
/// but in a subquery or inside a window function.
pub fn expr_contains_aggregate(expr: &Expr) -> bool {
    expr_contains_call(expr, &|f| builtin_aggregate_kind(f).is_some())
}

fn is_count_distinct_call(f: &sqlparser::ast::Function) -> bool {
    bare_function_name(&f.name) == "count"
        && matches!(
            &f.args,
            FunctionArguments::List(list)
                if list.duplicate_treatment == Some(sqlparser::ast::DuplicateTreatment::Distinct)
        )
}

pub(crate) fn is_window_call(expr: &Expr) -> bool {
    matches!(expr, Expr::Function(f) if f.over.is_some())
}

/// Whether `matches_call` accepts a call of `expr` that belongs to the query level of
/// `expr`: not one in a subquery, and not one in the arguments or the window of a window
/// function.
fn expr_contains_call(
    expr: &Expr,
    matches_call: &dyn Fn(&sqlparser::ast::Function) -> bool,
) -> bool {
    struct CallSearch<'a> {
        matches_call: &'a dyn Fn(&sqlparser::ast::Function) -> bool,
        enclosing_queries: usize,
        enclosing_window_calls: usize,
    }
    impl Visitor for CallSearch<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            self.enclosing_queries += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            self.enclosing_queries -= 1;
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if is_window_call(expr) {
                self.enclosing_window_calls += 1;
            } else if let Expr::Function(f) = expr {
                let at_this_level = self.enclosing_queries == 0 && self.enclosing_window_calls == 0;
                if at_this_level && (self.matches_call)(f) {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }

        fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if is_window_call(expr) {
                self.enclosing_window_calls -= 1;
            }
            ControlFlow::Continue(())
        }
    }
    let mut search = CallSearch {
        matches_call,
        enclosing_queries: 0,
        enclosing_window_calls: 0,
    };
    expr.visit(&mut search).is_break()
}

/// The aggregate calls of `expr` that the aggregate mapping
/// (`aggregation::replace_aggregate_calls`) reaches and rewrites: those under arithmetic
/// and boolean operators, parentheses, CASE, casts and positional function arguments.
fn count_decomposable_aggregates(expr: &Expr) -> usize {
    let count = count_decomposable_aggregates;
    match expr {
        Expr::Function(f) if f.over.is_none() => {
            if builtin_aggregate_kind(f).is_some() {
                return 1;
            }
            match &f.args {
                FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => count(e),
                        _ => 0,
                    })
                    .sum(),
                _ => 0,
            }
        }
        Expr::BinaryOp { left, right, .. } => count(left) + count(right),
        Expr::UnaryOp { expr: inner, .. } => count(inner),
        Expr::Nested(inner) => count(inner),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().map_or(0, count)
                + conditions
                    .iter()
                    .map(|when| count(&when.condition) + count(&when.result))
                    .sum::<usize>()
                + else_result.as_deref().map_or(0, count)
        }
        Expr::Cast { expr: inner, .. } => count(inner),
        _ => 0,
    }
}

fn every_aggregate_is_decomposable(expr: &Expr) -> bool {
    let aggregates = std::cell::Cell::new(0usize);
    expr_contains_call(expr, &|f| {
        if builtin_aggregate_kind(f).is_some() {
            aggregates.set(aggregates.get() + 1);
        }
        false
    });
    count_decomposable_aggregates(expr) == aggregates.get()
}

/// A call as PostgreSQL looks it up: `schema` and `name` folded like identifiers
/// (`schema` is `None` for an unqualified call), and the number of arguments written.
#[derive(Debug, Clone, PartialEq)]
pub struct WrittenCall {
    pub schema: Option<String>,
    pub name: String,
    pub arg_count: usize,
}

/// The calls of `expr` at its own query level.
fn written_calls(expr: &Expr) -> Vec<WrittenCall> {
    let calls = std::cell::RefCell::new(Vec::new());
    expr_contains_call(expr, &|f| {
        let name = f.name.0.last().and_then(|part| part.as_ident()).map(folded);
        if let Some(name) = name {
            calls.borrow_mut().push(WrittenCall {
                schema: function_schema(&f.name),
                name,
                arg_count: match &f.args {
                    FunctionArguments::List(list) => list.args.len(),
                    _ => 0,
                },
            });
        }
        false
    });
    calls.into_inner()
}

/// The column a select item is when it is a bare, optionally qualified and optionally
/// parenthesised, column reference.
fn referenced_column(expr: &Expr) -> Option<&Ident> {
    match expr {
        Expr::Nested(inner) => referenced_column(inner),
        Expr::Identifier(column) => Some(column),
        Expr::CompoundIdentifier(parts) => parts.last(),
        _ => None,
    }
}

fn record_output_name(analysis: &mut SqlAnalysis, expr: &Expr, alias: Option<&Ident>) {
    let column = referenced_column(expr).map(folded);
    let output = alias.map(folded).or_else(|| column.clone());
    if let Some(output) = output {
        if column.as_ref() == Some(&output) {
            analysis
                .columns_output_under_their_own_name
                .push(output.clone());
        }
        analysis.named_outputs.push(output);
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

fn is_clock_or_session_function(call: &CalledFunction) -> bool {
    let name = call.name.as_str();
    KEYWORD_PSEUDO_FUNCTIONS.contains(&name)
        || match name {
            "now"
            | "transaction_timestamp"
            | "statement_timestamp"
            | "clock_timestamp"
            | "timeofday"
            | "pg_postmaster_start_time"
            | "pg_conf_load_time"
            | "random"
            | "gen_random_uuid"
            | "current_setting"
            | "current_schemas"
            | "current_database"
            | "pg_backend_pid"
            | "inet_client_addr"
            | "inet_client_port"
            | "inet_server_addr"
            | "inet_server_port"
            | "txid_current"
            | "txid_current_if_assigned"
            | "pg_current_xact_id"
            | "pg_current_xact_id_if_assigned"
            | "pg_current_snapshot"
            | "txid_current_snapshot"
            | "pg_my_temp_schema"
            | "pg_trigger_depth" => true,
            "age" => call.arg_count == 1,
            _ => false,
        }
}

fn is_timezone_sensitive_function(call: &CalledFunction) -> bool {
    match call.name.as_str() {
        "date_trunc" | "to_timestamp" => call.arg_count == 2,
        "date_part" | "to_char" | "to_date" => true,
        "date" => call.arg_count == 1,
        _ => false,
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

fn push_select_column(analysis: &mut SqlAnalysis, expr: &Expr, alias: Option<String>) {
    for call in written_calls(expr) {
        if !analysis.select_list_calls.contains(&call) {
            analysis.select_list_calls.push(call);
        }
    }
    if is_constant_refused_as_group_key(expr) {
        analysis
            .constants_refused_as_group_key
            .push(expr.to_string());
    }
    let column = extract_select_column(expr, alias);
    if column.is_aggregate_derived && expr_contains_call(expr, &is_count_distinct_call) {
        analysis.nested_count_distinct = true;
    }
    analysis.select_columns.push(column);
}

/// A supported aggregate call reduced to what planning needs: `COUNT(*)` and
/// `COUNT(DISTINCT x)` told apart from `COUNT(x)`, and `FILTER (WHERE cond)`
/// folded into the argument as `CASE WHEN cond THEN arg END`.
pub(crate) struct AggregateCall {
    pub kind: AggregateKind,
    pub arg: Option<String>,
    pub filter_expr: Option<String>,
}

pub(crate) fn resolve_aggregate_call(f: &Function) -> Option<AggregateCall> {
    let mut kind = builtin_aggregate_kind(f)?;
    if matches!(kind, AggregateKind::Count) && is_wildcard_arg(&f.args) {
        kind = AggregateKind::CountStar;
    }
    if let FunctionArguments::List(list) = &f.args {
        if list.duplicate_treatment == Some(sqlparser::ast::DuplicateTreatment::Distinct)
            && kind == AggregateKind::Count
        {
            kind = AggregateKind::CountDistinct;
        }
    }
    let mut arg = if matches!(kind, AggregateKind::CountStar) {
        Some("*".to_string())
    } else {
        first_arg_sql(&f.args)
    };

    let filter_expr = f.filter.as_ref().map(|filt| filt.to_string());
    if let Some(ref filter) = filter_expr {
        if matches!(kind, AggregateKind::CountStar) {
            kind = AggregateKind::Count;
            arg = Some(format!("CASE WHEN {} THEN 1 END", filter));
        } else if let Some(ref unfiltered) = arg {
            arg = Some(format!("CASE WHEN {} THEN {} END", filter, unfiltered));
        }
    }

    Some(AggregateCall {
        kind,
        arg,
        filter_expr,
    })
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
                #[cfg(test)]
                filter_expr: None,
                is_aggregate_derived: false,
                holds_undecomposed_aggregate: false,
            };
        }

        if let Some(call) = resolve_aggregate_call(f) {
            // Rewrite expr_sql to reflect the CASE WHEN form for base_query generation
            let expr_sql = if call.filter_expr.is_some() {
                let arg_str = call.arg.as_deref().unwrap_or("*");
                format!("{}({})", f.name, arg_str)
            } else {
                inner.to_string()
            };

            return SelectColumn {
                expr_sql,
                alias,
                aggregate: Some(call.kind),
                aggregate_arg: call.arg,
                is_passthrough: false,
                cast_type,
                is_window: false,
                #[cfg(test)]
                filter_expr: call.filter_expr,
                is_aggregate_derived: false,
                holds_undecomposed_aggregate: false,
            };
        }
    }

    // Check if the expression contains aggregates inside a compound expression
    // (e.g., CASE WHEN SUM(x) > 0 THEN SUM(x)/SUM(y) END)
    let holds_aggregate = expr_contains_aggregate(expr);
    let has_nested_agg = holds_aggregate && every_aggregate_is_decomposable(expr);
    SelectColumn {
        expr_sql: expr.to_string(),
        alias,
        aggregate: None,
        aggregate_arg: None,
        is_passthrough: !has_nested_agg,
        cast_type: None,
        is_window: false,
        #[cfg(test)]
        filter_expr: None,
        is_aggregate_derived: has_nested_agg,
        holds_undecomposed_aggregate: holds_aggregate && !has_nested_agg,
    }
}

/// Extract join type as a human-readable string. SEMI/ANTI/STRAIGHT
/// variants are MySQL-dialect-only — not reachable from
/// `PostgreSqlDialect` parsing — so they fall into the `_ => "OTHER"`
/// catch-all alongside any future sqlparser additions.
fn join_type_str(op: &JoinOperator) -> &'static str {
    match op {
        JoinOperator::Inner(_) | JoinOperator::Join(_) => "INNER",
        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => "LEFT",
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => "RIGHT",
        JoinOperator::FullOuter(_) => "FULL OUTER",
        JoinOperator::CrossJoin(_) => "CROSS",
        _ => "OTHER",
    }
}

/// Extract the JoinConstraint from a JoinOperator. SEMI/ANTI/STRAIGHT
/// variants — not reachable from `PostgreSqlDialect` — fall to None
/// alongside other constraint-less / future-added variants.
pub(crate) fn join_constraint(op: &JoinOperator) -> Option<&JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c) => Some(c),
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

/// The parts of a possibly qualified name as the query writes it (`T`, `public."My T"`),
/// each folded the way PostgreSQL folds an identifier.
fn folded_name_parts(written: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut part_is_quoted = false;
    let mut inside_quotes = false;
    let mut chars = written.chars().peekable();
    let mut finish = |part: &mut String, part_is_quoted: &mut bool| {
        parts.push(if *part_is_quoted {
            std::mem::take(part)
        } else {
            std::mem::take(part).to_lowercase()
        });
        *part_is_quoted = false;
    };
    while let Some(c) = chars.next() {
        match c {
            '"' if inside_quotes && chars.peek() == Some(&'"') => {
                chars.next();
                part.push('"');
            }
            '"' => {
                inside_quotes = !inside_quotes;
                part_is_quoted = true;
            }
            '.' if !inside_quotes => finish(&mut part, &mut part_is_quoted),
            other => part.push(other),
        }
    }
    finish(&mut part, &mut part_is_quoted);
    parts
}

/// The table name of `written`, folded: its last part.
fn folded_relation_name(written: &str) -> String {
    folded_name_parts(written).pop().unwrap_or_default()
}

/// Like [`ColumnRefCollector`] but ignores column references nested inside a
/// subquery — only the OUTER expression's refs are collected. Used by
/// [`collect_imv_relevant_where`] so a WHERE conjunct of the form
/// `outer_col <op> (SELECT … FROM other)` attributes to the outer source and is
/// kept (the scalar subquery is treated as an opaque value), instead of looking
/// "cross-source" because of the subquery's internal refs and being dropped —
/// which would leave filters like `assortment_id = (SELECT … FROM sop_current_view)`
/// with no `where_predicate` and thus no relevance-skip for the IMV.
#[derive(Default)]
struct OuterColumnRefCollector {
    refs: Vec<Vec<String>>,
    subquery_depth: usize,
}

impl Visitor for OuterColumnRefCollector {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.subquery_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.subquery_depth = self.subquery_depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if self.subquery_depth == 0 {
            match expr {
                Expr::Identifier(id) => self.refs.push(vec![id.value.clone()]),
                Expr::CompoundIdentifier(ids) => self
                    .refs
                    .push(ids.iter().map(|i| i.value.clone()).collect()),
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }
}

/// The real (non-derived) table names appearing in the OUTER FROM clause of
/// `select` — the relations whose rows the WHERE clause filters. Relations that
/// appear only inside a subquery are excluded, so a scalar-subquery filter does
/// not make every WHERE conjunct look cross-source. Names match the
/// `name.to_string()` form pushed onto `analysis.sources`.
fn outer_from_real_sources(select: &Select) -> Vec<String> {
    let mut names = Vec::new();
    for twj in &select.from {
        if let TableFactor::Table { name, .. } = &twj.relation {
            names.push(name.to_string());
        }
        for join in &twj.joins {
            if let TableFactor::Table { name, .. } = &join.relation {
                names.push(name.to_string());
            }
        }
    }
    names
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

    let aliased = aliases
        .iter()
        .find(|(alias, _)| folded_relation_name(alias) == *prefix)
        .map(|(_, table)| table);
    if let Some(resolved) = aliased {
        if real_sources.iter().any(|s| s.as_str() == resolved.as_str()) {
            return vec![(resolved.clone(), column)];
        }
    }

    // Bare table name match (no alias): the alias map only contains entries
    // when the user wrote `AS foo`. An unaliased FROM table appears in
    // `sources` directly.
    for s in &real_sources {
        // The reference names the table itself, whatever its schema qualifier
        // (e.g., source "alp.product" referenced as "product.id").
        if folded_relation_name(s) == *prefix {
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
pub(crate) fn top_level_sources(select: &Select) -> Vec<String> {
    top_level_tables(select)
        .into_iter()
        .map(|(n, _)| n)
        .collect()
}

/// Alias → table name for the top-level FROM/JOIN relations of one SELECT.
///
/// Same walk as [`top_level_sources`], so the create-time `ignore_sources`
/// soundness check resolves a qualifier to exactly the source the rest of the
/// analyzer would. Deliberately top-level only: an alias reused inside a
/// subquery must not shadow the outer relation it would otherwise resolve to.
pub(crate) fn alias_map(select: &Select) -> HashMap<String, String> {
    top_level_tables(select)
        .into_iter()
        .filter_map(|(name, alias)| alias.map(|a| (a, name)))
        .collect()
}

/// The single walk behind [`top_level_sources`] and [`alias_map`]: every
/// top-level FROM / JOIN table factor as `(table_name, alias)`.
fn top_level_tables(select: &Select) -> Vec<(String, Option<String>)> {
    let entry = |factor: &TableFactor| match factor {
        TableFactor::Table { name, alias, .. } => {
            Some((name.to_string(), alias.as_ref().map(|a| a.name.to_string())))
        }
        _ => None,
    };
    let mut tables = Vec::new();
    for twj in &select.from {
        tables.extend(entry(&twj.relation));
        for join in &twj.joins {
            tables.extend(entry(&join.relation));
        }
    }
    tables
}

/// Collect the dotted column references of one expression, using the same
/// [`ColumnRefCollector`] the analyzer's own clause walk uses. A bare
/// `Identifier("status")` yields `vec!["status"]`; `dp.status` yields
/// `vec!["dp", "status"]`.
pub(crate) fn collect_column_refs(expr: &Expr) -> Vec<Vec<String>> {
    let mut collector = ColumnRefCollector::default();
    let _ = expr.visit(&mut collector);
    collector.refs
}

pub(crate) fn folded(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_lowercase(),
    }
}

/// Rewrites every unquoted table, alias and column name of `statements` to the lower
/// case PostgreSQL folds it to. Everything downstream matches names as text (the
/// registered source names, the columns a write must touch to matter, the rewrite of a
/// source into its transition table), so `T` and `t` have to be one spelling there.
pub(crate) fn fold_unquoted_names(statements: &mut [Statement]) {
    for statement in statements {
        let _ = statement.visit(&mut UnquotedNameFolder);
    }
}

struct UnquotedNameFolder;

impl UnquotedNameFolder {
    fn fold(id: &mut Ident) {
        id.value = folded(id);
    }

    fn fold_name(name: &mut sqlparser::ast::ObjectName) {
        for part in &mut name.0 {
            if let sqlparser::ast::ObjectNamePart::Identifier(id) = part {
                Self::fold(id);
            }
        }
    }

    fn fold_alias(alias: &mut sqlparser::ast::TableAlias) {
        Self::fold(&mut alias.name);
        for column in &mut alias.columns {
            Self::fold(&mut column.name);
        }
    }

    fn fold_using_columns(table: &mut sqlparser::ast::TableWithJoins) {
        use sqlparser::ast::{JoinConstraint, JoinOperator};
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = &mut table.relation
        {
            Self::fold_using_columns(table_with_joins);
        }
        for join in &mut table.joins {
            if let TableFactor::NestedJoin {
                table_with_joins, ..
            } = &mut join.relation
            {
                Self::fold_using_columns(table_with_joins);
            }
            let constraint = match &mut join.join_operator {
                JoinOperator::Join(constraint)
                | JoinOperator::Inner(constraint)
                | JoinOperator::Left(constraint)
                | JoinOperator::LeftOuter(constraint)
                | JoinOperator::Right(constraint)
                | JoinOperator::RightOuter(constraint)
                | JoinOperator::FullOuter(constraint) => constraint,
                _ => continue,
            };
            if let JoinConstraint::Using(columns) = constraint {
                columns.iter_mut().for_each(Self::fold_name);
            }
        }
    }

    fn fold_select_level_names(body: &mut SetExpr) {
        match body {
            SetExpr::Select(select) => {
                for item in &mut select.projection {
                    if let SelectItem::QualifiedWildcard(
                        sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                        _,
                    ) = item
                    {
                        Self::fold_name(name);
                    }
                }
                select.from.iter_mut().for_each(Self::fold_using_columns);
            }
            SetExpr::SetOperation { left, right, .. } => {
                Self::fold_select_level_names(left);
                Self::fold_select_level_names(right);
            }
            _ => {}
        }
    }
}

impl VisitorMut for UnquotedNameFolder {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                Self::fold_alias(&mut cte.alias);
            }
        }
        Self::fold_select_level_names(&mut query.body);
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &mut sqlparser::ast::ObjectName) -> ControlFlow<()> {
        Self::fold_name(relation);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        match factor {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. }
            | TableFactor::NestedJoin { alias, .. } => {
                if let Some(alias) = alias {
                    Self::fold_alias(alias);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(id) => Self::fold(id),
            Expr::CompoundIdentifier(ids) => ids.iter_mut().for_each(Self::fold),
            Expr::QualifiedWildcard(name, _) => Self::fold_name(name),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// The table name (last part, folded like PostgreSQL folds identifiers) of every
/// relation reference in `stmts`, once per reference, subqueries and CTE bodies
/// included.
pub(crate) fn statement_relation_names(stmts: &[Statement]) -> Vec<String> {
    struct Relations(Vec<String>);
    impl Visitor for Relations {
        type Break = ();

        fn pre_visit_relation(&mut self, relation: &sqlparser::ast::ObjectName) -> ControlFlow<()> {
            if let Some(sqlparser::ast::ObjectNamePart::Identifier(id)) = relation.0.last() {
                self.0.push(folded(id));
            }
            ControlFlow::Continue(())
        }
    }
    let mut relations = Relations(Vec::new());
    for stmt in stmts {
        let _ = stmt.visit(&mut relations);
    }
    relations.0
}

/// Every column reference in `stmts`, subqueries included, as dotted parts
/// folded the way PostgreSQL folds identifiers (unquoted parts lowercased).
/// `None` when the statements can read columns no reference names: a wildcard
/// or whole-row (`alias.*`) projection, or a `USING` / `NATURAL` join.
pub(crate) fn statement_column_refs(stmts: &[Statement]) -> Option<Vec<Vec<String>>> {
    #[derive(Default)]
    struct Refs {
        refs: Vec<Vec<String>>,
        implicit_columns: bool,
    }
    fn has_wildcard(body: &SetExpr) -> bool {
        match body {
            SetExpr::Select(select) => select.projection.iter().any(|item| {
                matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                )
            }),
            SetExpr::SetOperation { left, right, .. } => has_wildcard(left) || has_wildcard(right),
            SetExpr::Query(query) => has_wildcard(&query.body),
            _ => false,
        }
    }
    impl Visitor for Refs {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            self.implicit_columns |= has_wildcard(&query.body);
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            match expr {
                Expr::Identifier(id) => self.refs.push(vec![folded(id)]),
                Expr::CompoundIdentifier(ids) => self.refs.push(ids.iter().map(folded).collect()),
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }

    let mut refs = Refs::default();
    for stmt in stmts {
        let _ = stmt.visit(&mut refs);
        let text = stmt.to_string().to_uppercase();
        refs.implicit_columns |=
            text.contains(".*") || text.contains("USING") || text.contains("NATURAL");
    }
    (!refs.implicit_columns).then_some(refs.refs)
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
            if ids.len() == 2 && self.prefixes_to_strip.contains(&folded(&ids[0])) {
                let col = ids.last().unwrap().clone();
                *expr = Expr::Identifier(col);
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

    // Attribute conjuncts against the OUTER FROM's sources only. A relation
    // that appears solely inside a scalar subquery (e.g.
    // `WHERE col = (SELECT … FROM other)`) is in `sources` but does NOT own the
    // filtered rows; counting it makes the conjunct look cross-source and drops
    // it, leaving the IMV with no `where_predicate` (no relevance-skip).
    let outer_names: BTreeSet<String> = outer_from_real_sources(select).into_iter().collect();
    let real_sources: Vec<&String> = sources
        .iter()
        .filter(|s| is_real_source(s) && outer_names.contains(*s))
        .collect();
    if real_sources.is_empty() {
        return HashMap::new();
    }
    let outer_sources: Vec<String> = real_sources.iter().map(|s| (*s).clone()).collect();

    // Pre-compute the prefixes we'd need to strip per source: every alias
    // pointing at that source, plus the source's bare table name.
    let mut prefixes_per_source: HashMap<String, BTreeSet<String>> = HashMap::new();
    for s in &real_sources {
        let mut prefixes: BTreeSet<String> = BTreeSet::new();
        prefixes.insert(folded_relation_name(s));
        for (alias, table) in aliases {
            if table == *s {
                prefixes.insert(folded_relation_name(alias));
            }
        }
        prefixes_per_source.insert((*s).clone(), prefixes);
    }

    let mut conjuncts: Vec<Expr> = Vec::new();
    split_top_level_and(where_expr, &mut conjuncts);

    let mut buckets: HashMap<String, Vec<String>> = HashMap::new();

    for conj in conjuncts {
        // Only OUTER-level refs attribute the conjunct; a scalar subquery is an
        // opaque value evaluated against its own tables at maintenance time.
        let mut collector = OuterColumnRefCollector::default();
        let _ = conj.visit(&mut collector);

        // For each ref, resolve to its source(s). A bare ref in a multi-source
        // query attributes to ALL sources — we treat that as "ambiguous" and
        // drop the conjunct (since we can't know which transition table it'd
        // evaluate against meaningfully).
        let mut resolved_sources: BTreeSet<String> = BTreeSet::new();
        let mut ambiguous_or_unattributable = false;

        for parts in &collector.refs {
            let resolved = resolve_column_ref(parts, aliases, &outer_sources);
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

/// Returns true if `expr_sql` parses as an `Expr::Identifier(_)` or
/// `Expr::CompoundIdentifier(_)` — i.e. a bare column reference like
/// `region` or `t.region`, NOT a function call (`DATE_TRUNC(...)`), cast,
/// arithmetic, COLLATE, etc.
///
/// Used at IMV-create time to validate that the operator's `partition_by`
/// columns correspond to bare GROUP BY columns rather than computed
/// expressions.  The trigger codegen for per-partition dispatch needs to
/// find the partition key on transition tables by simple column reference;
/// computed GROUP BY expressions would require re-evaluating the function
/// on every transition row, which is fragile (mutable / opaque /
/// volatile functions break it).
pub fn is_bare_column_reference(expr_sql: &str) -> bool {
    let probe = format!("SELECT {} FROM __reflex_probe", expr_sql);
    let stmts = match Parser::parse_sql(&PostgreSqlDialect {}, &probe) {
        Ok(s) => s,
        Err(_) => return false,
    };
    if stmts.len() != 1 {
        return false;
    }
    let body = match &stmts[0] {
        sqlparser::ast::Statement::Query(q) => q.body.as_ref(),
        _ => return false,
    };
    let sel = match body {
        sqlparser::ast::SetExpr::Select(s) => s,
        _ => return false,
    };
    if sel.projection.len() != 1 {
        return false;
    }
    let e = match &sel.projection[0] {
        sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
        sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

/// The SELECT of a single-statement query that is not a set operation.
pub(crate) fn top_level_select(statements: &[Statement]) -> Option<&Select> {
    match statements {
        [Statement::Query(query)] => match query.body.as_ref() {
            SetExpr::Select(select) => Some(select),
            _ => None,
        },
        _ => None,
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
        top_level_query_seen: false,
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
                record_output_name(&mut analysis, expr, None);
                push_select_column(&mut analysis, expr, None);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                record_output_name(&mut analysis, expr, Some(alias));
                push_select_column(&mut analysis, expr, Some(alias.to_string()));
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
                    #[cfg(test)]
                    filter_expr: None,
                    is_aggregate_derived: false,
                    holds_undecomposed_aggregate: false,
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
                    #[cfg(test)]
                    filter_expr: None,
                    is_aggregate_derived: false,
                    holds_undecomposed_aggregate: false,
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
        collect_from_functions(&twj.relation, &mut analysis.from_functions);
        for join in &twj.joins {
            collect_from_functions(&join.relation, &mut analysis.from_functions);
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
    disable_update_skip_for_repeated_sources(&mut analysis);

    Ok(analysis)
}

/// Drops the UPDATE-skip inputs of every source the query reads more than once.
/// They are collected per spelling and per alias, so for a relation read twice
/// they describe one of its roles: an UPDATE of a column only the other role
/// reads would be skipped as irrelevant. Two sources sharing an unquoted bare
/// name count as one relation, whatever their schema or quoting.
fn disable_update_skip_for_repeated_sources(analysis: &mut SqlAnalysis) {
    let bare_name = |source: &str| {
        source
            .rsplit('.')
            .next()
            .unwrap_or(source)
            .trim_matches('"')
            .to_lowercase()
    };
    let read_more_than_once = |source: &String| {
        let bare = bare_name(source);
        analysis
            .sources
            .iter()
            .filter(|other| bare_name(other) == bare)
            .count()
            > 1
    };
    let repeated: Vec<String> = analysis
        .sources
        .iter()
        .filter(|source| read_more_than_once(source))
        .cloned()
        .collect();
    for source in repeated {
        analysis.imv_relevant_columns.remove(&source);
        analysis.imv_relevant_where.remove(&source);
    }
}

#[cfg(test)]
#[path = "tests/unit_sql_analyzer.rs"]
mod tests;
