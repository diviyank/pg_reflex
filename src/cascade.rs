//! Refresh of the IMVs that read a partitioned IMV whose partitions were just
//! rebuilt.
//!
//! One partition rebuild and everything it sets off form a cascade. A dependent
//! is not refreshed where it is found: it is queued with the scope that reaches
//! it, the scopes of every path are merged, and each dependent is refreshed
//! once, in `graph_depth` order, so after every IMV it reads.
//!
//! A dependent is refreshed for the rebuilt partitions only when its query shows
//! that its rows of one partition-key value come from the rebuilt IMV's rows of
//! that same value ([`unproven_partition_alignment`]). Any other dependent is
//! refreshed whole, with a NOTICE that says why.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::PgBuiltInOids;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, JoinConstraint,
    JoinOperator, Select, SelectItem, TableFactor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use crate::query_decomposer::{quote_identifier, split_qualified_name};
use crate::sql_analyzer::folded;
use crate::sql_writer::identifier::format_pg_text_array;
use crate::trigger::deferred::ScopedRebuildGuard;

/// How much of a dependent a cascade refreshes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefreshScope {
    /// The partitions that share the bounds of these rebuilt partitions of its
    /// sources (schema-qualified names).
    SourcePartitions(BTreeSet<String>),
    /// The partitions that hold these values of the partition column.
    PartitionKeys(BTreeSet<String>),
    /// The groups of an unpartitioned aggregate whose `column` is one of `keys`.
    Groups {
        column: String,
        keys: BTreeSet<String>,
    },
    /// The whole IMV, with the reason no narrower scope is proven.
    Whole(String),
}

impl RefreshScope {
    pub(crate) fn merged_with(self, other: RefreshScope) -> RefreshScope {
        use RefreshScope::*;
        match (self, other) {
            (Whole(reason), _) | (_, Whole(reason)) => Whole(reason),
            (SourcePartitions(mut a), SourcePartitions(b)) => {
                a.extend(b);
                SourcePartitions(a)
            }
            (PartitionKeys(mut a), PartitionKeys(b)) => {
                a.extend(b);
                PartitionKeys(a)
            }
            (
                Groups {
                    column,
                    keys: mut a,
                },
                Groups {
                    column: other_column,
                    keys: b,
                },
            ) if column == other_column => {
                a.extend(b);
                Groups { column, keys: a }
            }
            _ => Whole("its sources name the rows to refresh in different ways".to_string()),
        }
    }
}

struct QueuedRefresh {
    scope: RefreshScope,
    rebuilt_source: String,
}

thread_local! {
    /// The dependents the running cascade still has to refresh, by
    /// `(graph_depth, name)`. `None` outside a cascade.
    static QUEUE: RefCell<Option<BTreeMap<(i32, String), QueuedRefresh>>> =
        const { RefCell::new(None) };
}

struct OpenCascade;

impl Drop for OpenCascade {
    fn drop(&mut self) {
        QUEUE.with(|queue| *queue.borrow_mut() = None);
    }
}

fn cascade_is_open() -> bool {
    QUEUE.with(|queue| queue.borrow().is_some())
}

/// Runs `rebuild`, then refreshes the dependents it queued and those their own
/// refreshes queue. Inside a cascade it only runs `rebuild`: the outermost call
/// refreshes everything.
pub(crate) fn run_then_refresh_dependents<T>(rebuild: impl FnOnce() -> T) -> T {
    if cascade_is_open() {
        return rebuild();
    }
    QUEUE.with(|queue| *queue.borrow_mut() = Some(BTreeMap::new()));
    let _open = OpenCascade;
    let result = rebuild();
    while let Some((dependent, refresh)) = QUEUE.with(|queue| {
        queue
            .borrow_mut()
            .as_mut()
            .and_then(|pending| pending.pop_first())
            .map(|((_, dependent), refresh)| (dependent, refresh))
    }) {
        let _snapshot = StatementSnapshot::take();
        refresh_dependent(&dependent, refresh);
    }
    result
}

/// The snapshot a statement run by SPI would get, held for one refresh.
///
/// A refresh starts in a subtransaction that has written nothing yet, where
/// SPI runs catalog queries read-only, on the active snapshot. Without a new
/// one that is the snapshot of the statement that opened the cascade, which
/// predates the partitions the earlier refreshes created.
struct StatementSnapshot;

impl StatementSnapshot {
    fn take() -> Self {
        unsafe {
            pgrx::pg_sys::CommandCounterIncrement();
            pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot());
        }
        StatementSnapshot
    }
}

impl Drop for StatementSnapshot {
    fn drop(&mut self) {
        unsafe {
            if pgrx::pg_sys::ActiveSnapshotSet() {
                pgrx::pg_sys::PopActiveSnapshot();
            }
        }
    }
}

fn queue_refresh(dependent: &str, graph_depth: i32, scope: RefreshScope, rebuilt_source: &str) {
    QUEUE.with(|queue| {
        let mut queue = queue.borrow_mut();
        let Some(pending) = queue.as_mut() else {
            return;
        };
        let key = (graph_depth, dependent.to_string());
        let scope = match pending.remove(&key) {
            Some(queued) => queued.scope.merged_with(scope),
            None => scope,
        };
        pending.insert(
            key,
            QueuedRefresh {
                scope,
                rebuilt_source: rebuilt_source.to_string(),
            },
        );
    });
}

fn text_arg(value: &str) -> DatumWithOid<'static> {
    unsafe { DatumWithOid::new(value.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }
}

/// Inside a cascade, queues the whole refresh of `dependent` after the whole
/// rebuild of `rebuilt_source` and returns `true`. Outside one, returns `false`:
/// the caller refreshes the dependent itself.
pub(crate) fn queue_whole_refresh(dependent: &str, rebuilt_source: &str) -> bool {
    if !cascade_is_open() {
        return false;
    }
    let graph_depth = Spi::get_one_with_args::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = $1",
        &[text_arg(dependent)],
    )
    .unwrap_or(None)
    .unwrap_or(i32::MAX);
    queue_refresh(
        dependent,
        graph_depth,
        RefreshScope::Whole(format!("'{rebuilt_source}' was rebuilt whole")),
        rebuilt_source,
    );
    true
}

/// The partitions of `view_name` a partition reconcile rebuilt.
pub(crate) struct RebuiltPartitions<'a> {
    pub view_name: &'a str,
    pub partition_column: &'a str,
    /// The keys the reconcile was called with; empty when it was called with
    /// source partitions.
    pub partition_keys: &'a [String],
    /// The rebuilt target partitions, schema-qualified.
    pub partitions: &'a [String],
    /// The partition-column values the rebuilt partitions hold.
    pub keys_held: &'a [String],
}

struct Dependent {
    graph_depth: i32,
    partition_columns: Vec<String>,
    group_by_columns: Vec<String>,
    sql_query: String,
    base_query: String,
    end_query: String,
    depends_on: Vec<String>,
}

fn read_dependent(client: &pgrx::spi::SpiClient<'_>, name: &str) -> Option<Dependent> {
    let row = client
        .select(
            "SELECT graph_depth, partition_columns, sql_query, base_query, end_query, depends_on, \
                    aggregations::text AS agg_json \
             FROM public.__reflex_ivm_reference WHERE name = $1 AND enabled",
            Some(1),
            &[text_arg(name)],
        )
        .ok()?
        .next()?;
    let text = |column: &str| {
        row.get_by_name::<&str, _>(column)
            .ok()
            .flatten()
            .unwrap_or("")
            .to_string()
    };
    let texts = |column: &str| {
        row.get_by_name::<Vec<String>, _>(column)
            .ok()
            .flatten()
            .unwrap_or_default()
    };
    Some(Dependent {
        graph_depth: row.get_by_name::<i32, _>("graph_depth").ok().flatten()?,
        partition_columns: texts("partition_columns"),
        group_by_columns: row
            .get_by_name::<&str, _>("agg_json")
            .ok()
            .flatten()
            .and_then(|j| serde_json::from_str::<crate::aggregation::AggregationPlan>(j).ok())
            .map(|plan| plan.group_by_columns)
            .unwrap_or_default(),
        sql_query: text("sql_query"),
        base_query: text("base_query"),
        end_query: text("end_query"),
        depends_on: texts("depends_on"),
    })
}

/// A key the SQL entry point of the partition reconcile reads back unchanged:
/// it splits its keys on commas and trims each one.
fn key_survives_the_key_list(key: &str) -> bool {
    !key.is_empty() && !key.contains(',') && key.trim() == key
}

fn scope_of(dependent: &Dependent, rebuilt: &RebuiltPartitions<'_>) -> RefreshScope {
    let column = rebuilt.partition_column;
    let partitioned_alike = dependent
        .partition_columns
        .first()
        .is_some_and(|c| c.eq_ignore_ascii_case(column));
    let groups_by_it = dependent.partition_columns.is_empty()
        && !dependent.end_query.trim().is_empty()
        && dependent
            .group_by_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(column));
    if !partitioned_alike && !groups_by_it {
        return RefreshScope::Whole(format!(
            "it is neither partitioned nor grouped by \"{column}\""
        ));
    }
    if let Some(reason) = unproven_partition_alignment(
        &dependent.sql_query,
        &dependent.depends_on,
        rebuilt.view_name,
        column,
    ) {
        return RefreshScope::Whole(reason);
    }
    if groups_by_it {
        return if rebuilt.keys_held.is_empty() {
            RefreshScope::Whole(format!(
                "the rebuilt partitions hold no row to name its \"{column}\" groups"
            ))
        } else {
            RefreshScope::Groups {
                column: column.to_string(),
                keys: rebuilt.keys_held.iter().cloned().collect(),
            }
        };
    }
    if !rebuilt.partition_keys.is_empty() {
        return if rebuilt
            .partition_keys
            .iter()
            .all(|k| key_survives_the_key_list(k))
        {
            RefreshScope::PartitionKeys(rebuilt.partition_keys.iter().cloned().collect())
        } else {
            RefreshScope::Whole("a partition key holds a comma or edge whitespace".to_string())
        };
    }
    if rebuilt.partitions.iter().all(|p| !p.contains(',')) {
        RefreshScope::SourcePartitions(rebuilt.partitions.iter().cloned().collect())
    } else {
        RefreshScope::Whole("a rebuilt partition's name holds a comma".to_string())
    }
}

/// Queues the refresh of each enabled IMV of `dependents` after `rebuilt`.
pub(crate) fn queue_dependents(
    client: &pgrx::spi::SpiClient<'_>,
    rebuilt: &RebuiltPartitions<'_>,
    dependents: &[String],
) {
    for name in dependents {
        if let Some(dependent) = read_dependent(client, name) {
            queue_refresh(
                name,
                dependent.graph_depth,
                scope_of(&dependent, rebuilt),
                rebuilt.view_name,
            );
        }
    }
}

fn refresh_dependent(dependent: &str, refresh: QueuedRefresh) {
    let source = &refresh.rebuilt_source;
    match refresh.scope {
        RefreshScope::SourcePartitions(partitions) => {
            let partitions: Vec<String> = partitions.into_iter().collect();
            refresh_partitions(dependent, &[], &partitions.join(","), source);
        }
        RefreshScope::PartitionKeys(keys) => {
            let keys: Vec<String> = keys.into_iter().collect();
            refresh_partitions(dependent, &keys, "", source);
        }
        RefreshScope::Groups { column, keys } => {
            let keys: Vec<String> = keys.into_iter().collect();
            if let Err(reason) = refresh_groups(dependent, &column, &keys) {
                refresh_whole(dependent, source, &reason);
            }
        }
        RefreshScope::Whole(reason) => refresh_whole(dependent, source, &reason),
    }
}

fn refresh_partitions(dependent: &str, keys: &[String], source_partitions: &str, source: &str) {
    let result = crate::partition::reflex_reconcile_partition_impl(
        dependent,
        keys,
        source_partitions,
        false,
    );
    if !result.starts_with("RECONCILED") && result != crate::reconcile::RECONCILE_QUEUED_FOR_COMMIT
    {
        refresh_whole(
            dependent,
            source,
            &format!("its partition-scoped refresh returned: {result}"),
        );
    }
}

/// Rebuilds the groups of an unpartitioned aggregate `dependent` whose `column`
/// is one of `keys`; the reason it cannot when it cannot.
fn refresh_groups(dependent: &str, column: &str, keys: &[String]) -> Result<(), String> {
    Spi::connect_mut(|client| {
        let shape = read_dependent(client, dependent)
            .ok_or_else(|| "it is not an enabled IMV".to_string())?;
        let scoped_with = crate::partition::build_scoped_cascade_reconcile(
            dependent,
            column,
            keys,
            &shape.partition_columns,
            &shape.base_query,
            &shape.end_query,
            &shape.group_by_columns,
        )
        .ok_or_else(|| format!("its query has no GROUP BY to restrict to \"{column}\" values"))?;
        if crate::trigger::deferred::scoped_rebuild_waits_for_commit(dependent) {
            crate::trigger::deferred::list_for_commit_reconcile(dependent);
            return Ok(());
        }
        let literals = keys
            .iter()
            .map(|k| crate::partition::sql_literal_text(k))
            .collect::<Vec<_>>()
            .join(", ");
        let guard = crate::trigger::deferred::guard_scoped_rebuild(
            client,
            dependent,
            crate::trigger::deferred::RebuiltSlice::Keys {
                column,
                literals: &literals,
            },
        );
        let record_rebuild = match &guard {
            ScopedRebuildGuard::RecordWithRebuild(record) => format!("{record};"),
            _ => String::new(),
        };
        let _ = client.update(&scoped_with(&record_rebuild), None, &[]);
        if matches!(guard, ScopedRebuildGuard::ReconcileAtCommit) {
            crate::trigger::deferred::list_for_commit_reconcile(dependent);
        }
        Ok(())
    })
}

fn refresh_whole(dependent: &str, source: &str, reason: &str) {
    pgrx::notice!(
        "pg_reflex: '{}' is refreshed whole, not by partition, after the partition rebuild of \
         '{}': {}",
        dependent,
        source,
        reason
    );
    let result = crate::reconcile::reconcile_as_cascaded_dependent(dependent);
    if result.starts_with("ERROR") {
        pgrx::warning!(
            "pg_reflex: '{}' was rebuilt but refreshing its dependent '{}' returned: {} — that \
             dependent is STALE; run SELECT reflex_rebuild_imv('{}') once the cause is cleared",
            source,
            dependent,
            result,
            dependent
        );
    }
}

/// The top-level partitions of `view_name` (bare names) whose bound is the one
/// `source_partition` falls under in its own tree, when `source_partition`
/// belongs to a source of `view_name` other than the anchor it mirrors.
pub(crate) fn children_sharing_the_bound_of(
    client: &pgrx::spi::SpiClient<'_>,
    view_name: &str,
    depends_on: &[String],
    source_partition: &str,
) -> Result<Vec<String>, String> {
    let top_level = client
        .select(
            "SELECT pg_get_expr(top.relpartbound, top.oid) AS bound, \
                    EXISTS (SELECT 1 FROM unnest($2::text[]) AS source(name) \
                             WHERE to_regclass(source.name) = up.root) AS read_by_view \
             FROM (SELECT a.relid, lead(a.relid) OVER (ORDER BY a.level) AS root, \
                          lead(a.relid, 2) OVER (ORDER BY a.level) AS above_root \
                     FROM pg_partition_ancestors(to_regclass($1)) WITH ORDINALITY \
                          AS a(relid, level)) up \
             JOIN pg_class top ON top.oid = up.relid \
             WHERE up.root IS NOT NULL AND up.above_root IS NULL",
            Some(1),
            &[
                text_arg(source_partition),
                text_arg(&format_pg_text_array(depends_on)),
            ],
        )
        .map_err(|e| format!("reading the tree of '{source_partition}' failed: {e}"))?
        .next()
        .and_then(|row| {
            Some((
                row.get_by_name::<String, _>("bound").ok().flatten()?,
                row.get_by_name::<bool, _>("read_by_view").ok().flatten()?,
            ))
        });
    let Some((bound, true)) = top_level else {
        return Err(format!(
            "'{source_partition}' is not a partition of a source of '{view_name}'"
        ));
    };
    let children: Vec<String> =
        crate::partition::list_partition_children(client, &quote_identifier(view_name))
            .into_iter()
            .filter(|child| child.bound_expr == bound)
            .map(|child| child.bare_name)
            .collect();
    if children.is_empty() {
        return Err(format!(
            "no partition of '{view_name}' has the bound {bound} of '{source_partition}'"
        ));
    }
    Ok(children)
}

fn relation_bare_name(name: &str) -> String {
    split_qualified_name(name).1.trim_matches('"').to_string()
}

/// `qualifier.column` as written, folded the way PostgreSQL folds identifiers.
fn qualified_column(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some((
            folded(&parts[parts.len() - 2]),
            folded(&parts[parts.len() - 1]),
        )),
        Expr::Nested(inner) => qualified_column(inner),
        _ => None,
    }
}

fn unqualified_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(folded(id)),
        Expr::Nested(inner) => unqualified_column(inner),
        _ => None,
    }
}

fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut all = conjuncts(left);
            all.extend(conjuncts(right));
            all
        }
        Expr::Nested(inner) => conjuncts(inner),
        other => vec![other],
    }
}

/// The two columns a top-level conjunct of `condition` equates, in either order.
fn equated_columns(condition: &Expr) -> Vec<((String, String), (String, String))> {
    conjuncts(condition)
        .into_iter()
        .filter_map(|conjunct| match conjunct {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => Some((qualified_column(left)?, qualified_column(right)?)),
            _ => None,
        })
        .collect()
}

fn coalesced_columns(expr: &Expr) -> Option<Vec<(String, String)>> {
    let Expr::Function(function) = expr else {
        return None;
    };
    if !function.name.to_string().eq_ignore_ascii_case("coalesce") {
        return None;
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return None;
    };
    arguments
        .args
        .iter()
        .map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => qualified_column(expr),
            _ => None,
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinKind {
    Inner,
    Left,
    Full,
    Other,
}

struct JoinedRelation<'a> {
    /// The name its columns are qualified with: the alias, or the table name.
    qualifier: String,
    table: String,
    kind: JoinKind,
    condition: Option<&'a Expr>,
}

fn top_level_relations(select: &Select) -> Option<Vec<JoinedRelation<'_>>> {
    let [from] = select.from.as_slice() else {
        return None;
    };
    let named = |factor: &TableFactor| match factor {
        TableFactor::Table { name, alias, .. } => {
            let table = relation_bare_name(&name.to_string());
            let qualifier = alias
                .as_ref()
                .map(|a| folded(&a.name))
                .unwrap_or_else(|| table.clone());
            Some((qualifier, table))
        }
        _ => None,
    };
    let (qualifier, table) = named(&from.relation)?;
    let mut relations = vec![JoinedRelation {
        qualifier,
        table,
        kind: JoinKind::Inner,
        condition: None,
    }];
    for join in &from.joins {
        let (qualifier, table) = named(&join.relation)?;
        let kind = match &join.join_operator {
            JoinOperator::Join(_) | JoinOperator::Inner(_) | JoinOperator::CrossJoin(_) => {
                JoinKind::Inner
            }
            JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => JoinKind::Left,
            JoinOperator::FullOuter(_) => JoinKind::Full,
            _ => JoinKind::Other,
        };
        let condition = match crate::sql_analyzer::join_constraint(&join.join_operator) {
            Some(JoinConstraint::On(condition)) => Some(condition),
            Some(JoinConstraint::None) | None => None,
            Some(_) => return None,
        };
        relations.push(JoinedRelation {
            qualifier,
            table,
            kind,
            condition,
        });
    }
    Some(relations)
}

/// Why a change to the rows of `source` that hold some values of
/// `partition_column` could change rows of the dependent that hold other values
/// of its own `partition_column`; `None` when the dependent's query shows it
/// cannot.
///
/// It cannot when the dependent reads `source` once, as a top-level relation,
/// and every row it outputs takes its `partition_column` from the `source` row
/// it is computed from. Three shapes show that:
///
/// * the output column is `source`'s own column, and no join can produce a row
///   without a `source` row;
/// * the output column is another relation's column that a join condition
///   equates with `source`'s, and no join can produce a row without that
///   relation's row;
/// * the output column is `COALESCE` over the two columns a FULL JOIN equates,
///   one of them `source`'s.
///
/// A window function, DISTINCT ON or set operation makes a row depend on rows
/// of other partitions and is refused.
pub(crate) fn unproven_partition_alignment(
    sql_query: &str,
    depends_on: &[String],
    source: &str,
    partition_column: &str,
) -> Option<String> {
    let source_table = relation_bare_name(source);
    let column = partition_column.to_lowercase();
    let reads = depends_on
        .iter()
        .filter(|read| relation_bare_name(read) == source_table)
        .count();
    if reads != 1 {
        return Some(format!("it reads '{source}' {reads} times"));
    }
    let unreadable = || Some("its query could not be analysed".to_string());
    let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, sql_query) else {
        return unreadable();
    };
    let Ok(analysis) = crate::sql_analyzer::analyze(&statements) else {
        return unreadable();
    };
    if analysis.has_window_function || analysis.has_distinct_on || analysis.set_operation.is_some()
    {
        return Some(
            "a window function, DISTINCT ON or set operation reads across partitions".to_string(),
        );
    }
    let Some(select) = crate::sql_analyzer::top_level_select(&statements) else {
        return unreadable();
    };
    let Some(relations) = top_level_relations(select) else {
        return Some("its FROM clause is not a plain list of joined tables".to_string());
    };
    let read_at_top_level: Vec<&JoinedRelation<'_>> = relations
        .iter()
        .filter(|relation| relation.table == source_table)
        .collect();
    let [source_relation] = read_at_top_level.as_slice() else {
        return Some(format!("'{source}' is not one of its top-level relations"));
    };
    if relations.iter().any(|relation| {
        relations
            .iter()
            .filter(|other| other.qualifier == relation.qualifier)
            .count()
            > 1
    }) {
        return Some("two of its relations share a name".to_string());
    }
    if relations
        .iter()
        .any(|relation| relation.kind == JoinKind::Other)
    {
        return Some("it uses a join other than INNER, LEFT or FULL".to_string());
    }

    let Some(projected) = select.projection.iter().find_map(|item| match item {
        SelectItem::ExprWithAlias { expr, alias } if folded(alias) == column => Some(expr),
        SelectItem::UnnamedExpr(expr)
            if qualified_column(expr)
                .map(|(_, c)| c)
                .or_else(|| unqualified_column(expr))
                == Some(column.clone()) =>
        {
            Some(expr)
        }
        _ => None,
    }) else {
        return Some(format!(
            "its \"{column}\" column is not a column of its query"
        ));
    };

    let from_source = |(qualifier, name): &(String, String)| {
        *qualifier == source_relation.qualifier && *name == column
    };
    let can_be_missing = |qualifier: &str| {
        relations
            .iter()
            .any(|relation| relation.qualifier == qualifier && relation.kind == JoinKind::Left)
    };
    let full_joins = relations
        .iter()
        .filter(|relation| relation.kind == JoinKind::Full)
        .count();
    let not_from_source = || {
        Some(format!(
            "its \"{column}\" column is not shown to be '{source}''s"
        ))
    };

    if let Some(coalesced) = coalesced_columns(projected) {
        let [first, second] = coalesced.as_slice() else {
            return not_from_source();
        };
        let equated_by_the_full_join = relations.iter().any(|relation| {
            relation.kind == JoinKind::Full
                && relation.condition.is_some_and(|condition| {
                    equated_columns(condition)
                        .iter()
                        .any(|(a, b)| (a == first && b == second) || (a == second && b == first))
                })
        });
        let proven = full_joins == 1
            && equated_by_the_full_join
            && (from_source(first) || from_source(second))
            && !can_be_missing(&first.0)
            && !can_be_missing(&second.0);
        return if proven { None } else { not_from_source() };
    }
    if full_joins > 0 {
        return Some(format!(
            "a FULL JOIN can output a row whose \"{column}\" is not '{source}''s"
        ));
    }

    let projected_column = match qualified_column(projected) {
        Some(qualified) => qualified,
        None => match unqualified_column(projected) {
            Some(name) if relations.len() == 1 => (source_relation.qualifier.clone(), name),
            _ => return not_from_source(),
        },
    };
    if can_be_missing(&projected_column.0) {
        return Some(format!(
            "its \"{column}\" column comes from the nullable side of a LEFT JOIN"
        ));
    }
    if from_source(&projected_column) {
        return None;
    }
    let equated_with_source = relations.iter().any(|relation| {
        let joins_on_matching_rows = relation.kind == JoinKind::Inner
            || (relation.kind == JoinKind::Left && relation.qualifier == source_relation.qualifier);
        joins_on_matching_rows
            && relation.condition.is_some_and(|condition| {
                equated_columns(condition).iter().any(|(a, b)| {
                    (from_source(a) && *b == projected_column)
                        || (from_source(b) && *a == projected_column)
                })
            })
    });
    if equated_with_source {
        None
    } else {
        not_from_source()
    }
}
