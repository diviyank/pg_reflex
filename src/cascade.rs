//! Refresh of the IMVs that read a partitioned IMV whose partitions were just
//! rebuilt.
//!
//! One partition rebuild and everything it sets off form a cascade. A dependent
//! is not refreshed where it is found: it is queued with the scope that reaches
//! it, the scopes of every path are merged, and each dependent is refreshed
//! once, in `graph_depth` order, so after every IMV it reads.
//!
//! A dependent is refreshed for part of its rows only when two things are
//! shown. Its query must take a row's partition key from the row of the rebuilt
//! IMV it is built on ([`unproven_partition_alignment`]). And the part must be
//! named without guessing: by the rebuilt partitions when the dependent mirrors
//! the rebuilt IMV's own partition tree, otherwise by the keys the rebuilt
//! partitions held before or after the rebuild. Any other dependent is
//! refreshed whole, with a NOTICE that says why.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::PgBuiltInOids;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, JoinConstraint,
    JoinOperator, Select, SelectItem, TableFactor, UnaryOperator, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use crate::query_decomposer::split_qualified_name;
use crate::sql_analyzer::folded;
use crate::trigger::deferred::ScopedRebuildGuard;

/// How much of a dependent a cascade refreshes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefreshScope {
    /// Of a partitioned dependent: the partitions that mirror
    /// `source_partitions` (schema-qualified partitions of the IMV whose tree it
    /// mirrors) and the partitions that accept one of `keys`.
    Partitions {
        source_partitions: BTreeSet<String>,
        keys: BTreeSet<String>,
    },
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
            (
                Partitions {
                    mut source_partitions,
                    mut keys,
                },
                Partitions {
                    source_partitions: other_partitions,
                    keys: other_keys,
                },
            ) => {
                source_partitions.extend(other_partitions);
                keys.extend(other_keys);
                Partitions {
                    source_partitions,
                    keys,
                }
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
    queue_refresh(
        dependent,
        graph_depth_of(dependent),
        RefreshScope::Whole(format!("'{rebuilt_source}' was rebuilt whole")),
        rebuilt_source,
    );
    true
}

fn graph_depth_of(imv: &str) -> i32 {
    Spi::get_one_with_args::<i32>(
        "SELECT graph_depth FROM public.__reflex_ivm_reference WHERE name = $1",
        &[text_arg(imv)],
    )
    .unwrap_or(None)
    .unwrap_or(i32::MAX)
}

/// The rows of `view_name` a refresh may have added, removed or changed.
pub(crate) struct RebuiltRows<'a> {
    pub view_name: &'a str,
    /// The columns `view_name` is partitioned by, outermost first; for an
    /// unpartitioned aggregate, the column its refreshed groups are named by.
    pub partition_columns: &'a [String],
    pub schema: &'a str,
    /// The rebuilt partitions (bare names); empty for an unpartitioned IMV.
    pub partitions: &'a [String],
    /// The values of the first of `partition_columns` those rows hold or held;
    /// the reason they cannot be listed when they cannot.
    pub keys: Result<Vec<String>, String>,
}

struct Dependent {
    name: String,
    graph_depth: i32,
    graph_child: Vec<String>,
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
            "SELECT graph_depth, graph_child, partition_columns, sql_query, base_query, end_query, \
                    depends_on, aggregations::text AS agg_json \
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
        name: name.to_string(),
        graph_depth: row.get_by_name::<i32, _>("graph_depth").ok().flatten()?,
        graph_child: texts("graph_child"),
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

/// Whether `dependent` mirrors the partition tree of `view_name` itself, so
/// that each of its partitions holds exactly the keys of one partition of
/// `view_name`.
fn mirrors_the_partitions_of(
    client: &pgrx::spi::SpiClient<'_>,
    dependent: &Dependent,
    view_name: &str,
) -> bool {
    let Some(first_column) = dependent.partition_columns.first() else {
        return false;
    };
    let Ok(anchor) =
        crate::partition::resolve_anchor_source(client, first_column, &dependent.depends_on)
    else {
        return false;
    };
    client
        .select(
            "SELECT to_regclass($1) = to_regclass($2) AS same",
            Some(1),
            &[text_arg(&anchor), text_arg(view_name)],
        )
        .ok()
        .and_then(|mut rows| rows.next())
        .and_then(|row| row.get_by_name::<bool, _>("same").ok().flatten())
        .unwrap_or(false)
}

/// The partition of the tree of `schema`.`partition` at `depth` below the root
/// that holds it (the partition itself when it is not deeper), schema-qualified.
fn partition_at_depth(
    client: &pgrx::spi::SpiClient<'_>,
    schema: &str,
    partition: &str,
    depth: usize,
) -> String {
    let qualified = format!("{schema}.{partition}");
    client
        .select(
            "SELECT n.nspname || '.' || c.relname AS holder \
             FROM pg_partition_ancestors(to_regclass(format('%I.%I', $1, $2))) \
                  WITH ORDINALITY AS up(relid, steps_from_partition) \
             JOIN pg_class c ON c.oid = up.relid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             ORDER BY up.steps_from_partition DESC OFFSET $3::bigint LIMIT 1",
            Some(1),
            &[
                text_arg(schema),
                text_arg(partition),
                text_arg(&depth.to_string()),
            ],
        )
        .ok()
        .and_then(|mut rows| rows.next())
        .and_then(|row| row.get_by_name::<String, _>("holder").ok().flatten())
        .unwrap_or(qualified)
}

/// The type, type modifier and collation of `relation`'s column `column`.
fn column_type(client: &pgrx::spi::SpiClient<'_>, relation: &str, column: &str) -> Option<String> {
    client
        .select(
            "SELECT format_type(a.atttypid, a.atttypmod) || ' COLLATE ' || a.attcollation::text \
                    AS typed \
             FROM pg_attribute a \
             WHERE a.attrelid = to_regclass($1) AND a.attname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped",
            Some(1),
            &[text_arg(relation), text_arg(column)],
        )
        .ok()
        .and_then(|mut rows| rows.next())
        .and_then(|row| row.get_by_name::<String, _>("typed").ok().flatten())
}

/// The scope `rebuilt` gives `dependent`.
fn scope_of(
    client: &pgrx::spi::SpiClient<'_>,
    dependent: &Dependent,
    rebuilt: &RebuiltRows<'_>,
) -> RefreshScope {
    let Some(column) = rebuilt.partition_columns.first() else {
        return RefreshScope::Whole(format!(
            "'{}' names no column its refreshed rows share",
            rebuilt.view_name
        ));
    };
    let aligned_on = |level: usize| match (
        dependent.partition_columns.get(level),
        rebuilt.partition_columns.get(level),
    ) {
        (Some(own), Some(theirs)) if own.eq_ignore_ascii_case(theirs) => {
            unproven_partition_alignment(
                &dependent.sql_query,
                &dependent.depends_on,
                rebuilt.view_name,
                theirs,
            )
        }
        _ => Some("its partition column differs at that level".to_string()),
    };
    let groups_by_it = dependent.partition_columns.is_empty()
        && !dependent.end_query.trim().is_empty()
        && dependent
            .group_by_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(column));
    let unproven = if groups_by_it {
        unproven_partition_alignment(
            &dependent.sql_query,
            &dependent.depends_on,
            rebuilt.view_name,
            column,
        )
    } else if dependent.partition_columns.is_empty() {
        Some(format!(
            "it is neither partitioned nor grouped by \"{column}\""
        ))
    } else if !dependent.partition_columns[0].eq_ignore_ascii_case(column) {
        Some(format!("it is not partitioned by \"{column}\" first"))
    } else {
        aligned_on(0)
    };
    if let Some(reason) = unproven {
        return RefreshScope::Whole(reason);
    }

    if !groups_by_it
        && !rebuilt.partitions.is_empty()
        && mirrors_the_partitions_of(client, dependent, rebuilt.view_name)
    {
        let aligned_levels = (1..dependent.partition_columns.len())
            .take_while(|level| aligned_on(*level).is_none())
            .count()
            + 1;
        let source_partitions: BTreeSet<String> = rebuilt
            .partitions
            .iter()
            .map(|partition| {
                if aligned_levels >= dependent.partition_columns.len() {
                    format!("{}.{}", rebuilt.schema, partition)
                } else {
                    partition_at_depth(client, rebuilt.schema, partition, aligned_levels)
                }
            })
            .collect();
        return if source_partitions.iter().all(|p| !p.contains(',')) {
            RefreshScope::Partitions {
                source_partitions,
                keys: BTreeSet::new(),
            }
        } else {
            RefreshScope::Whole("a rebuilt partition's name holds a comma".to_string())
        };
    }

    let rebuilt_type = column_type(client, rebuilt.view_name, column);
    if rebuilt_type.is_none() || rebuilt_type != column_type(client, &dependent.name, column) {
        return RefreshScope::Whole(format!(
            "its \"{column}\" column is not of the type and collation of '{}''s ({})",
            rebuilt.view_name,
            rebuilt_type.unwrap_or_else(|| "unknown".to_string())
        ));
    }
    let keys = match &rebuilt.keys {
        Ok(keys) => keys,
        Err(reason) => return RefreshScope::Whole(reason.clone()),
    };
    if keys.is_empty() {
        return RefreshScope::Whole(
            "its rebuilt partitions held no row before or after the rebuild, so no key names the \
             rows it may have behind"
                .to_string(),
        );
    }
    let keys: BTreeSet<String> = keys.iter().cloned().collect();
    if groups_by_it {
        RefreshScope::Groups {
            column: column.clone(),
            keys,
        }
    } else if keys.iter().all(|k| key_survives_the_key_list(k)) {
        RefreshScope::Partitions {
            source_partitions: BTreeSet::new(),
            keys,
        }
    } else {
        RefreshScope::Whole("a partition key holds a comma or edge whitespace".to_string())
    }
}

/// The statement that catches `dependent` up with rows of `rebuilt` that left
/// without reaching it (a dropped partition, read by an IMV that ignores
/// `rebuilt`): a refresh of the keys they held when its query proves those keys
/// are its own, else a whole refresh. A keyed refresh that does not report
/// success falls back to the whole one. One that finds no partition of
/// `dependent` for those keys left (its pre-sync dropped it, its query filling
/// none) leaves `dependent` right but its own ignoring readers behind: they are
/// caught up on the same keys in turn.
pub(crate) fn catch_up_statement(
    client: &pgrx::spi::SpiClient<'_>,
    dependent: &str,
    rebuilt: &RebuiltRows<'_>,
) -> String {
    let literal = crate::partition::sql_literal_text;
    let whole = format!("PERFORM public.reflex_reconcile({})", literal(dependent));
    let Some(shape) = read_dependent(client, dependent) else {
        return whole;
    };
    match scope_of(client, &shape, rebuilt) {
        RefreshScope::Partitions {
            source_partitions,
            keys,
        } if source_partitions.is_empty() && !keys.is_empty() => {
            let keys: Vec<String> = keys.into_iter().collect();
            let dependent_rows = RebuiltRows {
                view_name: dependent,
                partition_columns: &shape.partition_columns,
                schema: rebuilt.schema,
                partitions: &[],
                keys: Ok(keys.clone()),
            };
            let readers_catch_up: String = crate::reconcile::ignoring_dependents(dependent)
                .iter()
                .map(|reader| format!("{};", catch_up_statement(client, reader, &dependent_rows)))
                .collect();
            let result = format!("refreshed_{}", shape.graph_depth);
            format!(
                "DECLARE {result} TEXT; \
                 BEGIN \
                   {result} := public.reflex_reconcile_partition({dependent}, {keys}); \
                   IF {result} LIKE 'reconcile_partition: no children matched%' THEN \
                     NULL; {readers_catch_up} \
                   ELSIF {result} NOT LIKE 'RECONCILED%' AND {result} <> {queued} THEN \
                     {whole}; \
                   END IF; \
                 END",
                queued = literal(crate::reconcile::RECONCILE_QUEUED_FOR_COMMIT),
                dependent = literal(dependent),
                keys = literal(&keys.join(",")),
            )
        }
        _ => whole,
    }
}

/// Queues the refresh of each enabled IMV of `dependents` after `rebuilt`.
pub(crate) fn queue_dependents(
    client: &pgrx::spi::SpiClient<'_>,
    rebuilt: &RebuiltRows<'_>,
    dependents: &[String],
) {
    for name in dependents {
        let Some(dependent) = read_dependent(client, name) else {
            continue;
        };
        let scope = scope_of(client, &dependent, rebuilt);
        queue_refresh(name, dependent.graph_depth, scope, rebuilt.view_name);
    }
}

fn refresh_dependent(dependent: &str, refresh: QueuedRefresh) {
    let source = &refresh.rebuilt_source;
    match refresh.scope {
        RefreshScope::Partitions {
            source_partitions,
            keys,
        } => {
            let source_partitions: Vec<String> = source_partitions.into_iter().collect();
            let keys: Vec<String> = keys.into_iter().collect();
            let result = crate::partition::reconcile_partitions_and_keys(
                dependent,
                &keys,
                &source_partitions.join(","),
                false,
            );
            if !result.starts_with("RECONCILED")
                && result != crate::reconcile::RECONCILE_QUEUED_FOR_COMMIT
            {
                refresh_whole(
                    dependent,
                    source,
                    &format!("its partition-scoped refresh returned: {result}"),
                );
            }
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

/// Rebuilds the groups of an unpartitioned aggregate `dependent` whose `column`
/// is one of `keys`, then queues its own dependents; the reason it cannot when
/// it cannot.
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
        // The COMMIT-time reconcile it is left to refreshes its dependents.
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
            return Ok(());
        }
        // Its observing dependents received the rewritten groups as DML.
        let unreached = if crate::partition::origin_triggers_fire(client) {
            crate::reconcile::ignoring_dependents(dependent)
        } else {
            shape.graph_child
        };
        queue_dependents(
            client,
            &RebuiltRows {
                view_name: dependent,
                partition_columns: &[column.to_string()],
                schema: "",
                partitions: &[],
                keys: Ok(keys.to_vec()),
            },
            &unreached,
        );
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

/// The values a LIST partition bound lists (`FOR VALUES IN (1, 'a')`), as key
/// texts; `None` for any other bound and for a list that holds anything but
/// plain number and string literals (NULL among them).
pub(crate) fn keys_listed_by(bound: &str) -> Option<Vec<String>> {
    let listed = bound
        .trim()
        .strip_prefix("FOR VALUES IN (")?
        .strip_suffix(')')?;
    let values = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(listed)
        .ok()?
        .parse_comma_separated(Parser::parse_expr)
        .ok()?;
    values
        .iter()
        .map(|value| match value {
            Expr::Value(literal) => match &literal.value {
                Value::Number(number, _) => Some(number.to_string()),
                Value::SingleQuotedString(text) => Some(text.clone()),
                _ => None,
            },
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => match expr.as_ref() {
                Expr::Value(literal) => match &literal.value {
                    Value::Number(number, _) => Some(format!("-{number}")),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        })
        .collect()
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
    /// Its name as the query writes it, schema included when written.
    written: String,
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
            let written = name.to_string();
            let table = relation_bare_name(&written);
            let qualifier = alias
                .as_ref()
                .map(|a| folded(&a.name))
                .unwrap_or_else(|| table.clone());
            Some((qualifier, written, table))
        }
        _ => None,
    };
    let (qualifier, written, table) = named(&from.relation)?;
    let mut relations = vec![JoinedRelation {
        qualifier,
        written,
        table,
        kind: JoinKind::Inner,
        condition: None,
    }];
    for join in &from.joins {
        let (qualifier, written, table) = named(&join.relation)?;
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
            written,
            table,
            kind,
            condition,
        });
    }
    Some(relations)
}

/// Whether each partition column of `imv` is the column `source` is
/// partitioned by at that level, proven to be that column of `source`: a
/// partition of `source` then feeds only the partition of `imv` that mirrors it.
pub(crate) fn partitions_follow_source(
    client: &pgrx::spi::SpiClient<'_>,
    imv: &str,
    source: &str,
) -> bool {
    read_dependent(client, imv).is_some_and(|dependent| {
        crate::partition::source_levels_partitioned_on(client, source, &dependent.partition_columns)
            && dependent.partition_columns.iter().all(|column| {
                unproven_partition_alignment(
                    &dependent.sql_query,
                    &dependent.depends_on,
                    source,
                    column,
                )
                .is_none()
            })
    })
}

/// The output columns of `sql_query` each proven, by
/// [`unproven_partition_alignment`], to take its value from the `source` row a
/// row is built on: a change to the rows of `source` holding some values of
/// these columns can only change the output rows holding the same values.
///
/// Every relation the query reads, subqueries included, counts as a read of
/// `source` when its table name is `source`'s, whatever its schema. A column
/// shown by a join's equality qualifies only when the two equated columns have
/// one type, type modifier and collation: else the join may match values the
/// output column tells apart.
pub(crate) fn columns_following(sql_query: &str, source: &str) -> Vec<String> {
    let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, sql_query) else {
        return Vec::new();
    };
    let Some(select) = crate::sql_analyzer::top_level_select(&statements) else {
        return Vec::new();
    };
    let depends_on = crate::sql_analyzer::statement_relation_names(&statements);
    select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(folded(alias)),
            SelectItem::UnnamedExpr(expr) => qualified_column(expr)
                .map(|(_, column)| column)
                .or_else(|| unqualified_column(expr)),
            _ => None,
        })
        .filter(
            |column| match partition_alignment(sql_query, &depends_on, source, column) {
                Ok(Alignment::OwnColumn) => true,
                Ok(Alignment::Equated(one, other)) => of_one_type_and_collation(&one, &other),
                Err(_) => false,
            },
        )
        .collect()
}

fn of_one_type_and_collation(one: &WrittenColumn, other: &WrittenColumn) -> bool {
    Spi::connect(|client| {
        let one_type = column_type(&client, &one.0, &one.1);
        one_type.is_some() && one_type == column_type(&client, &other.0, &other.1)
    })
}

/// A column of a relation of a query: the relation as the query writes it,
/// and the column's folded name.
type WrittenColumn = (String, String);

/// How an output column is shown to take its value from the `source` row.
enum Alignment {
    /// It is `source`'s own column.
    OwnColumn,
    /// A join's equality of these two columns shows it.
    Equated(WrittenColumn, WrittenColumn),
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
/// * the output column is `COALESCE` over the two columns, one per side and
///   one of them `source`'s, that the ON of the query's first join equates
///   (INNER, LEFT or FULL), when every later join is a LEFT JOIN: a row with
///   both sides carries their common value, a row with one side carries that
///   side's, and a `source` row of one value only matches rows of that value.
///
/// A window function, DISTINCT ON or set operation makes a row depend on rows
/// of other partitions and is refused.
pub(crate) fn unproven_partition_alignment(
    sql_query: &str,
    depends_on: &[String],
    source: &str,
    partition_column: &str,
) -> Option<String> {
    partition_alignment(
        sql_query,
        depends_on,
        source,
        &partition_column.to_lowercase(),
    )
    .err()
}

/// [`unproven_partition_alignment`] for the already folded `column`, with
/// the columns the proof relies on.
fn partition_alignment(
    sql_query: &str,
    depends_on: &[String],
    source: &str,
    column: &str,
) -> Result<Alignment, String> {
    let source_table = relation_bare_name(source);
    let column = column.to_string();
    let reads = depends_on
        .iter()
        .filter(|read| relation_bare_name(read) == source_table)
        .count();
    if reads != 1 {
        return Err(format!("it reads '{source}' {reads} times"));
    }
    let unreadable = || Err("its query could not be analysed".to_string());
    let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, sql_query) else {
        return unreadable();
    };
    let Ok(analysis) = crate::sql_analyzer::analyze(&statements) else {
        return unreadable();
    };
    if analysis.has_window_function || analysis.has_distinct_on || analysis.set_operation.is_some()
    {
        return Err(
            "a window function, DISTINCT ON or set operation reads across partitions".to_string(),
        );
    }
    let Some(select) = crate::sql_analyzer::top_level_select(&statements) else {
        return unreadable();
    };
    let Some(relations) = top_level_relations(select) else {
        return Err("its FROM clause is not a plain list of joined tables".to_string());
    };
    let Some(source_relation) = relations
        .iter()
        .find(|relation| relation.table == source_table)
    else {
        return Err(format!("'{source}' is not one of its top-level relations"));
    };
    if relations.iter().any(|relation| {
        relations
            .iter()
            .filter(|other| other.qualifier == relation.qualifier)
            .count()
            > 1
    }) {
        return Err("two of its relations share a name".to_string());
    }
    if relations
        .iter()
        .any(|relation| relation.kind == JoinKind::Other)
    {
        return Err("it uses a join other than INNER, LEFT or FULL".to_string());
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
        return Err(format!(
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
        Err(format!(
            "its \"{column}\" column is not shown to be '{source}''s"
        ))
    };

    if let Some(coalesced) = coalesced_columns(projected) {
        let [first, second] = coalesced.as_slice() else {
            return not_from_source();
        };
        let later_joins_are_left = relations
            .get(2..)
            .is_some_and(|later| later.iter().all(|r| r.kind == JoinKind::Left));
        let equated_by_the_first_join = relations.get(1).is_some_and(|first_join| {
            first_join.condition.is_some_and(|condition| {
                equated_columns(condition)
                    .iter()
                    .any(|(a, b)| (a == first && b == second) || (a == second && b == first))
            })
        });
        let proven = later_joins_are_left
            && equated_by_the_first_join
            && first.0 != second.0
            && (from_source(first) || from_source(second));
        let written = |(qualifier, name): &(String, String)| -> WrittenColumn {
            let relation = relations.iter().find(|r| r.qualifier == *qualifier);
            (
                relation.map_or_else(String::new, |r| r.written.clone()),
                name.clone(),
            )
        };
        return if proven {
            Ok(Alignment::Equated(written(first), written(second)))
        } else {
            not_from_source()
        };
    }
    if full_joins > 0 {
        return Err(format!(
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
        return Err(format!(
            "its \"{column}\" column comes from the nullable side of a LEFT JOIN"
        ));
    }
    if from_source(&projected_column) {
        return Ok(Alignment::OwnColumn);
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
        let projected_relation = relations
            .iter()
            .find(|relation| relation.qualifier == projected_column.0)
            .map_or_else(String::new, |relation| relation.written.clone());
        Ok(Alignment::Equated(
            (source_relation.written.clone(), column.clone()),
            (projected_relation, projected_column.1.clone()),
        ))
    } else {
        not_from_source()
    }
}
