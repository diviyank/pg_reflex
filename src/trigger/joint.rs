//! Exact deltas for an IMMEDIATE IMV `V = X ⋈ S` whose sources are written by
//! one statement (a writable CTE, a cascading foreign key, a user trigger).
//!
//! Each statement trigger joins its source's delta against the other source as
//! it is now. When X's delta is applied while S holds writes whose delta is not
//! applied yet, both deltas count the joint term `ΔX ⋈ ΔS` (signed: inserts +,
//! deletes −), where the exact change counts it once. S's delta is then preceded
//! by the correction `−(ΔX ⋈ ΔS)`, built from X's transition rows kept for it
//! (the stash) and S's own.
//!
//! The correction is exact only when X's delta saw every row of ΔS and nothing
//! else, which the per-transaction write counts of S prove
//! ([`delta_sources::source_writes`]): S's writes counted when the stash was
//! taken (`seen`) equal its writes counted now, and exceed the rows its applied
//! deltas held (`applied`) by exactly |ΔS|. Otherwise, and for every shape the
//! correction does not cover ([`corrects_exactly`]), V is recomputed, and so is
//! every later delta reaching V in the same statement.
//!
//! `applied` and the stash bookkeeping live in transaction-local settings, so a
//! rolled back subtransaction rolls them back with V's own rows, and the stashed
//! rows in temporary tables dropped at COMMIT.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use pgrx::prelude::*;

use super::delta_sources::{self, Relation};
use super::{
    affected_groups_select, aggregate_epilogue_stmts, delta_statements, group_columns,
    imv_has_no_scoped_delta, passthrough_op_stmts, push_materialized_merge,
    push_materialized_merge_and_affected, recompute_sql, registered_plan, union_delta, DeltaOp,
};
use crate::aggregation::AggregationPlan;
use crate::query_decomposer::{
    affected_groups_table_name, delta_scratch_table_name, intermediate_table_name,
    transition_new_table_name, transition_old_table_name,
};
use crate::sql_writer::identifier::replace_source_with_transition;
use crate::sql_writer::keyword_scan::{has_keyword, keyword_hits};

/// Operation prefix of the second stage of a corrected delta, which runs in a
/// nested SPI call over copies of the transition tables.
pub(crate) const STAGED_OP_PREFIX: &str = "REFLEX_JOINT:";

const SEPARATOR: &str = "\n--<<REFLEX_SEP>>--\n";
const POISONED: &str = "poison";

static PARTITION_DELTAS_RUNNING: AtomicUsize = AtomicUsize::new(0);

/// Marks the partition flush applying the delta of an attached or detached
/// partition, whose rows its statistics never counted: deltas applied meanwhile
/// keep the recompute of a source holding unapplied writes.
pub(crate) struct PartitionDeltaGuard;

impl PartitionDeltaGuard {
    pub(crate) fn enter() -> Self {
        PARTITION_DELTAS_RUNNING.fetch_add(1, Ordering::SeqCst);
        PartitionDeltaGuard
    }
}

impl Drop for PartitionDeltaGuard {
    fn drop(&mut self) {
        PARTITION_DELTAS_RUNNING.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(crate) fn in_partition_delta() -> bool {
    PARTITION_DELTAS_RUNNING.load(Ordering::SeqCst) > 0
}

/// IMVs recomputed in the running top-level statement: every later delta
/// reaching them in it recomputes them again.
fn recomputed_in_statement() -> &'static Mutex<HashMap<String, (i64, i64)>> {
    static RECOMPUTED: OnceLock<Mutex<HashMap<String, (i64, i64)>>> = OnceLock::new();
    RECOMPUTED.get_or_init(|| Mutex::new(HashMap::new()))
}

fn recomputed_earlier_in_statement(view_name: &str, statement: (i64, i64)) -> bool {
    recomputed_in_statement()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(view_name)
        == Some(&statement)
}

fn remember_recompute(view_name: &str, statement: (i64, i64)) {
    let mut recomputed = recomputed_in_statement()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    recomputed.retain(|_, at| at.0 == statement.0);
    recomputed.insert(view_name.to_string(), statement);
}

struct Imv<'a> {
    name: &'a str,
    base_query: &'a str,
    end_query: &'a str,
    aggregations: Option<&'a str>,
    sources: Vec<(String, Relation)>,
    every_source_resolved: bool,
    statement: (i64, i64),
}

impl Imv<'_> {
    fn source_named(&self, name: &str) -> Option<Relation> {
        let relation = delta_sources::relation(name, self.statement.0)?;
        self.sources
            .iter()
            .any(|(_, r)| r.oid == relation.oid)
            .then_some(relation)
    }

    fn source_with_oid(&self, oid: pg_sys::Oid) -> Option<&str> {
        self.sources
            .iter()
            .find(|(_, r)| r.oid == oid)
            .map(|(name, _)| name.as_str())
    }
}

enum Stash {
    Absent,
    Poisoned,
    Taken { seen: i64, by: pg_sys::Oid },
}

fn hashed(parts: &[&str]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    parts.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn oid_text(oid: pg_sys::Oid) -> String {
    oid.to_u32().to_string()
}

fn applied_setting(view_name: &str, source: pg_sys::Oid) -> String {
    format!(
        "pg_reflex.joint_applied_{}",
        hashed(&[view_name, &oid_text(source)])
    )
}

fn stash_setting(view_name: &str, source: pg_sys::Oid) -> String {
    format!(
        "pg_reflex.joint_stash_{}",
        hashed(&[view_name, &oid_text(source)])
    )
}

fn stash_table(view_name: &str, pending: pg_sys::Oid, by: pg_sys::Oid, side: &str) -> String {
    format!(
        "__reflex_joint_{}_{side}",
        hashed(&[view_name, &oid_text(pending), &oid_text(by)])
    )
}

fn setting(name: &str) -> Option<String> {
    let name = CString::new(name).ok()?;
    let value = unsafe { pg_sys::GetConfigOption(name.as_ptr(), true, false) };
    if value.is_null() {
        return None;
    }
    let value = unsafe { CStr::from_ptr(value) }
        .to_string_lossy()
        .into_owned();
    (!value.is_empty()).then_some(value)
}

fn applied(view_name: &str, source: pg_sys::Oid) -> i64 {
    setting(&applied_setting(view_name, source))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn stash(view_name: &str, source: pg_sys::Oid) -> Stash {
    match setting(&stash_setting(view_name, source)) {
        None => Stash::Absent,
        Some(value) => value
            .split_once(':')
            .and_then(|(seen, by)| {
                Some(Stash::Taken {
                    seen: seen.parse().ok()?,
                    by: pg_sys::Oid::from(by.parse::<u32>().ok()?),
                })
            })
            .unwrap_or(Stash::Poisoned),
    }
}

fn set_setting_sql(name: &str, value: &str) -> String {
    format!("SELECT set_config('{name}', '{value}', true)")
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// The transition tables of a trigger running `operation`.
fn transition_tables(source: &str, operation: &str) -> Vec<String> {
    let new = transition_new_table_name(source);
    let old = transition_old_table_name(source);
    match operation {
        "INSERT" => vec![new],
        "DELETE" => vec![old],
        _ => vec![new, old],
    }
}

/// The transition table holding one row per row the statement wrote.
fn counted_transition(source: &str, operation: &str) -> String {
    match operation {
        "DELETE" | "DELETE_PROMOTED" => transition_old_table_name(source),
        _ => transition_new_table_name(source),
    }
}

fn reads_new(operation: &str) -> bool {
    operation != "DELETE"
}

fn reads_old(operation: &str) -> bool {
    operation != "INSERT"
}

fn has_word(sql: &str, word: &str) -> bool {
    let bytes = sql.as_bytes();
    let is_word = |i: usize| {
        bytes
            .get(i)
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
    };
    keyword_hits(sql, word)
        .iter()
        .any(|hit| (hit.pos == 0 || !is_word(hit.pos - 1)) && !is_word(hit.pos + word.len()))
}

/// The shapes whose deltas are linear in each source, without re-reading any
/// base table: inner equi-joins read once each, aggregated by SUM / COUNT (AVG
/// and BOOL_OR store sums) or passed through.
fn corrects_exactly(imv: &Imv, plan: &AggregationPlan) -> bool {
    const NON_LINEAR: [&str; 16] = [
        "LEFT",
        "RIGHT",
        "FULL",
        "OUTER",
        "LATERAL",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "DISTINCT",
        "OVER",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "ROLLUP",
        "CUBE",
        "GROUPING",
    ];
    let linear_aggregates = if plan.is_passthrough {
        true
    } else {
        plan.distinct_columns.is_empty()
            && plan
                .intermediate_columns
                .iter()
                .all(|ic| matches!(ic.source_aggregate.as_str(), "SUM" | "COUNT"))
            && has_keyword(imv.base_query, "__ivm_count")
    };
    !plan.has_distinct
        && linear_aggregates
        && keyword_hits(imv.base_query, "SELECT").len() == 1
        && !NON_LINEAR.iter().any(|w| has_word(imv.base_query, w))
        && imv.sources.iter().all(|(name, _)| {
            !imv_has_no_scoped_delta(imv.base_query, imv.end_query, name)
                && !union_delta::source_requires_recompute(imv.base_query, name)
        })
}

/// Whether the delta `operation` applies for `source` is ΔX ⋈ (other sources):
/// the bulk DELETE of the groups keyed by the deleted rows is not.
fn applies_linear_delta(imv: &Imv, plan: &AggregationPlan, source: &str, operation: &str) -> bool {
    plan.is_passthrough
        || !(matches!(operation, "DELETE" | "DELETE_PROMOTED")
            && group_columns(plan).is_some()
            && plan.source_join_keys.contains_key(source)
            && !has_keyword(imv.end_query, "GROUP BY"))
}

fn temp_relation_exists(name: &str) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT to_regclass('pg_temp.' || quote_ident($1)) IS NOT NULL",
        &[delta_sources::text_arg(name)],
    )
    .ok()
    .flatten()
    .unwrap_or(true)
}

fn row_count(table: &str) -> Option<i64> {
    Spi::get_one::<i64>(&format!("SELECT count(*) FROM \"{table}\""))
        .ok()
        .flatten()
}

/// The delta SQL of a trigger on `source` for an IMMEDIATE IMV.
#[allow(clippy::too_many_arguments)]
pub(crate) fn delta_sql(
    view_name: &str,
    source: &str,
    operation: &str,
    base_query: &str,
    end_query: &str,
    aggregations: Option<&str>,
    orig_base_query: &str,
) -> String {
    let own_delta = || {
        delta_statements(
            view_name,
            source,
            operation,
            base_query,
            end_query,
            aggregations,
            orig_base_query,
        )
    };
    let statement = delta_sources::running_statement();
    if statement == delta_sources::NO_TRANSACTION {
        return own_delta();
    }
    let names = delta_sources::source_names(view_name, base_query);
    if names.as_ref().is_some_and(|names| names.len() < 2) {
        return own_delta();
    }
    delta_sources::record_delta_reached(view_name, source);
    let imv = imv(
        view_name,
        base_query,
        end_query,
        aggregations,
        names,
        statement,
    );
    let Some(source_relation) = imv.source_named(source) else {
        return recompute(&imv);
    };
    if !imv.every_source_resolved || recomputed_earlier_in_statement(view_name, statement) {
        return recompute(&imv);
    }
    match stash(view_name, source_relation.oid) {
        Stash::Absent => {
            let mut stmts = vec![own_delta()];
            stmts.extend(bookkeeping(&imv, source, source_relation.oid, operation));
            stmts.join(SEPARATOR)
        }
        Stash::Poisoned => recompute(&imv),
        Stash::Taken { .. } => staged(&imv, source, operation),
    }
}

fn imv<'a>(
    view_name: &'a str,
    base_query: &'a str,
    end_query: &'a str,
    aggregations: Option<&'a str>,
    names: Option<Vec<String>>,
    statement: (i64, i64),
) -> Imv<'a> {
    let names = names.unwrap_or_default();
    let sources: Vec<(String, Relation)> = names
        .iter()
        .filter_map(|name| {
            let relation = delta_sources::relation(name, statement.0)?;
            Some((name.clone(), relation))
        })
        .collect();
    Imv {
        name: view_name,
        base_query,
        end_query,
        aggregations,
        every_source_resolved: !names.is_empty() && sources.len() == names.len(),
        sources,
        statement,
    }
}

/// Recomputes the IMV from its sources as they are now, and records every
/// source's writes as applied: the sources still holding unapplied writes
/// recompute it again when their deltas reach it in this statement.
fn recompute(imv: &Imv) -> String {
    remember_recompute(imv.name, imv.statement);
    let mut stmts = vec![recompute_sql(
        imv.name,
        imv.base_query,
        imv.end_query,
        imv.aggregations,
    )];
    for (_, relation) in &imv.sources {
        let writes = delta_sources::source_writes(relation)
            .map(|w| w.to_string())
            .unwrap_or_default();
        stmts.push(set_setting_sql(
            &applied_setting(imv.name, relation.oid),
            &writes,
        ));
        stmts.push(set_setting_sql(&stash_setting(imv.name, relation.oid), ""));
    }
    stmts.join(SEPARATOR)
}

/// Records the rows of `source`'s delta as applied, and stashes its transition
/// rows for the other source still holding unapplied writes.
fn bookkeeping(imv: &Imv, source: &str, source_oid: pg_sys::Oid, operation: &str) -> Vec<String> {
    let counted = applied_setting(imv.name, source_oid);
    let mut stmts = vec![format!(
        "SELECT set_config('{counted}', (COALESCE(NULLIF(current_setting('{counted}', true), ''), '0')::bigint \
         + (SELECT count(*) FROM \"{}\"))::text, true)",
        counted_transition(source, operation)
    )];
    let pending: Vec<(pg_sys::Oid, Option<i64>)> = imv
        .sources
        .iter()
        .filter(|(name, relation)| {
            relation.oid != source_oid && delta_sources::holds_unapplied_writes(name, relation)
        })
        .filter_map(|(_, relation)| {
            let writes = delta_sources::source_writes(relation);
            (writes != Some(applied(imv.name, relation.oid))).then_some((relation.oid, writes))
        })
        .collect();
    if pending.is_empty() {
        return stmts;
    }
    let plan = registered_plan(imv.name, imv.aggregations);
    let stashable = pending.len() == 1
        && applies_linear_delta(imv, &plan, source, operation)
        && corrects_exactly(imv, &plan);
    for (oid, writes) in pending {
        let meta = stash_setting(imv.name, oid);
        match (stashable, writes, stash(imv.name, oid)) {
            (true, Some(seen), Stash::Absent) => {
                stmts.extend(stash_rows(
                    imv, &plan, source, source_oid, oid, operation, true,
                ));
                stmts.push(set_setting_sql(
                    &meta,
                    &format!("{seen}:{}", oid_text(source_oid)),
                ));
            }
            (true, Some(writes), Stash::Taken { seen, by })
                if seen == writes
                    && by == source_oid
                    && (plan.is_passthrough
                        || stash_lost_no_group(imv, &plan, source, oid, by)) =>
            {
                stmts.extend(stash_rows(
                    imv, &plan, source, source_oid, oid, operation, false,
                ));
            }
            _ => stmts.push(set_setting_sql(&meta, POISONED)),
        }
    }
    stmts
}

fn stash_rows(
    imv: &Imv,
    plan: &AggregationPlan,
    source: &str,
    source_oid: pg_sys::Oid,
    pending: pg_sys::Oid,
    operation: &str,
    fresh: bool,
) -> Vec<String> {
    if plan.is_passthrough {
        return Vec::new();
    }
    let transitions = transition_tables(source, operation);
    let mut stmts = Vec::new();
    for (side, transition) in [
        ("new", transition_new_table_name(source)),
        ("old", transition_old_table_name(source)),
    ] {
        let table = stash_table(imv.name, pending, source_oid, side);
        if temp_relation_exists(&table) {
            if fresh {
                stmts.push(format!("TRUNCATE pg_temp.\"{table}\""));
            }
        } else {
            stmts.push(format!(
                "CREATE TEMP TABLE \"{table}\" ON COMMIT DROP AS SELECT * FROM \"{}\" WITH NO DATA",
                transitions[0]
            ));
        }
        if transitions.contains(&transition) {
            stmts.push(format!(
                "INSERT INTO pg_temp.\"{table}\" SELECT * FROM \"{transition}\""
            ));
        }
    }
    stmts
}

/// Whether the deltas already stashed dropped no group they subtracted rows of
/// S never held from, so the stasher's next delta cannot bring such a group
/// back unseen by [`group_dropped_by_earlier_delta`]. S's pending inserts are
/// among its rows now.
fn stash_lost_no_group(
    imv: &Imv,
    plan: &AggregationPlan,
    stasher: &str,
    pending: pg_sys::Oid,
    by: pg_sys::Oid,
) -> bool {
    let joined = replace_source_with_transition(
        imv.base_query,
        stasher,
        &stash_table(imv.name, pending, by, "old"),
    );
    !group_missing(imv, plan, &joined)
}

/// First stage of a corrected delta, in the trigger: copies the transition
/// tables into temporary tables of the same names, which the second stage
/// ([`staged_delta_sql`]) reads in a nested SPI call.
fn staged(imv: &Imv, source: &str, operation: &str) -> String {
    let transitions = transition_tables(source, operation);
    if transitions.iter().any(|t| temp_relation_exists(t)) {
        return recompute(imv);
    }
    let mut stmts: Vec<String> = transitions
        .iter()
        .map(|t| format!("CREATE TEMP TABLE \"{t}\" ON COMMIT DROP AS SELECT * FROM \"{t}\""))
        .collect();
    stmts.push(format!(
        "SELECT public.reflex_execute_separated(public.reflex_build_delta_sql({}, {}, {}, '', '', NULL, ''))",
        literal(imv.name),
        literal(source),
        literal(&format!("{STAGED_OP_PREFIX}{operation}")),
    ));
    stmts.extend(
        transitions
            .iter()
            .map(|t| format!("DROP TABLE pg_temp.\"{t}\"")),
    );
    stmts.join(SEPARATOR)
}

fn registry_row(view_name: &str) -> Option<(String, String, String)> {
    Spi::connect(|client| {
        let row = client
            .select(
                "SELECT base_query, COALESCE(end_query, '') AS end_query, \
                        COALESCE(aggregations::text, '{}') AS aggregations \
                 FROM public.__reflex_ivm_reference WHERE name = $1",
                Some(1),
                &[delta_sources::text_arg(view_name)],
            )
            .ok()?
            .next()?;
        Some((
            row.get_by_name::<String, _>("base_query").ok()??,
            row.get_by_name::<String, _>("end_query").ok()??,
            row.get_by_name::<String, _>("aggregations").ok()??,
        ))
    })
}

/// Second stage of a corrected delta, over the copies of the transition tables:
/// `−(ΔX ⋈ ΔS)`, then S's own delta, when the write counts prove X's delta saw
/// exactly ΔS; a recompute otherwise.
pub(crate) fn staged_delta_sql(view_name: &str, source: &str, operation: &str) -> String {
    let Some((base_query, end_query, aggregations)) = registry_row(view_name) else {
        return String::new();
    };
    let statement = delta_sources::running_statement();
    let names = delta_sources::source_names(view_name, &base_query);
    let imv = imv(
        view_name,
        &base_query,
        &end_query,
        Some(&aggregations),
        names,
        statement,
    );
    let plan = registered_plan(view_name, Some(&aggregations));
    let Some(relation) = imv.source_named(source) else {
        return recompute(&imv);
    };
    let Stash::Taken { seen, by } = stash(view_name, relation.oid) else {
        return recompute(&imv);
    };
    let Some(stasher) = imv.source_with_oid(by).map(str::to_string) else {
        return recompute(&imv);
    };
    if !proven_exact(
        &imv, &plan, source, &relation, operation, seen, &stasher, by,
    ) {
        return recompute(&imv);
    }
    let mut stmts = correction(&imv, &plan, source, operation, &stasher, by);
    stmts.push(delta_statements(
        view_name,
        source,
        operation,
        &base_query,
        &end_query,
        Some(&aggregations),
        &base_query,
    ));
    stmts.extend(bookkeeping(&imv, source, relation.oid, operation));
    stmts.push(set_setting_sql(&stash_setting(view_name, relation.oid), ""));
    stmts.join(SEPARATOR)
}

#[allow(clippy::too_many_arguments)]
fn proven_exact(
    imv: &Imv,
    plan: &AggregationPlan,
    source: &str,
    relation: &Relation,
    operation: &str,
    seen: i64,
    stasher: &str,
    by: pg_sys::Oid,
) -> bool {
    if !corrects_exactly(imv, plan) {
        return false;
    }
    let Some(writes) = delta_sources::source_writes(relation) else {
        return false;
    };
    if writes != seen
        || row_count(&counted_transition(source, operation))
            != Some(writes - applied(imv.name, relation.oid))
    {
        return false;
    }
    plan.is_passthrough
        || !reads_new(operation)
        || !group_dropped_by_earlier_delta(imv, plan, source, relation.oid, stasher, by)
}

/// Whether a group X's deleted rows reach through S's inserted rows is missing
/// from the intermediate table: X's delta subtracted those joined rows, which V
/// never held, and its dead-group cleanup may have dropped the group with sums
/// the correction cannot restore.
fn group_dropped_by_earlier_delta(
    imv: &Imv,
    plan: &AggregationPlan,
    source: &str,
    pending: pg_sys::Oid,
    stasher: &str,
    by: pg_sys::Oid,
) -> bool {
    let joined = joint_rows(
        imv,
        stasher,
        &stash_table(imv.name, pending, by, "old"),
        source,
        &transition_new_table_name(source),
    );
    group_missing(imv, plan, &joined)
}

/// Whether a group of the rows of `joined` is missing from the intermediate
/// table. A scalar IMV's one row is never dropped.
fn group_missing(imv: &Imv, plan: &AggregationPlan, joined: &str) -> bool {
    let Some(cols) = group_columns(plan) else {
        return false;
    };
    let scratch = delta_scratch_table_name(imv.name);
    let intermediate = intermediate_table_name(imv.name);
    let same_group = cols
        .iter()
        .map(|c| format!("i.{c} IS NOT DISTINCT FROM d.{c}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let filled = Spi::run(&format!("TRUNCATE {scratch}"))
        .and_then(|_| Spi::run(&format!("INSERT INTO {scratch} {joined}")));
    filled.is_err()
        || Spi::get_one::<bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM {scratch} d \
             WHERE NOT EXISTS (SELECT 1 FROM {intermediate} i WHERE {same_group}))"
        ))
        .ok()
        .flatten()
        .unwrap_or(true)
}

fn joint_rows(imv: &Imv, stasher: &str, stash: &str, source: &str, transition: &str) -> String {
    replace_source_with_transition(
        &replace_source_with_transition(imv.base_query, stasher, stash),
        source,
        transition,
    )
}

/// `−(ΔX ⋈ ΔS)`. Aggregates: the additions first (X⁺ ⋈ S⁻, X⁻ ⋈ S⁺), then the
/// subtractions (X⁺ ⋈ S⁺, X⁻ ⋈ S⁻), each from a group that holds it
/// ([`group_dropped_by_earlier_delta`]), so no group's count drops below its
/// exact value. Passthrough: rows are
/// removed by key, which neither delta counts twice, so only the rows X's delta
/// inserted for S's inserted rows are removed, by S's key, before S inserts them.
fn correction(
    imv: &Imv,
    plan: &AggregationPlan,
    source: &str,
    operation: &str,
    stasher: &str,
    by: pg_sys::Oid,
) -> Vec<String> {
    let mut stmts = Vec::new();
    let new_tbl = transition_new_table_name(source);
    let old_tbl = transition_old_table_name(source);
    if plan.is_passthrough {
        if plan.passthrough_key_mappings.contains_key(source)
            && matches!(operation, "INSERT" | "INSERT_PROMOTED")
        {
            passthrough_op_stmts(
                imv.name,
                source,
                "DELETE",
                imv.base_query,
                plan,
                &new_tbl,
                &new_tbl,
                false,
                &mut stmts,
            );
        }
        return stmts;
    }
    let pending = imv
        .source_named(source)
        .map(|r| r.oid)
        .expect("checked by the caller");
    let stash_new = stash_table(imv.name, pending, by, "new");
    let stash_old = stash_table(imv.name, pending, by, "old");
    let mut pieces: Vec<(&str, &str, DeltaOp)> = Vec::new();
    if reads_old(operation) {
        pieces.push((&stash_new, &old_tbl, DeltaOp::Add));
    }
    if reads_new(operation) {
        pieces.push((&stash_old, &new_tbl, DeltaOp::Add));
    }
    if reads_new(operation) {
        pieces.push((&stash_new, &new_tbl, DeltaOp::Subtract));
    }
    if reads_old(operation) {
        pieces.push((&stash_old, &old_tbl, DeltaOp::Subtract));
    }
    let intermediate = intermediate_table_name(imv.name);
    let scratch = delta_scratch_table_name(imv.name);
    let affected = affected_groups_table_name(imv.name);
    let grp_cols = group_columns(plan);
    for (i, (stash, transition, op)) in pieces.into_iter().enumerate() {
        let delta_q = joint_rows(imv, stasher, stash, source, transition);
        match &grp_cols {
            Some(cols) => push_materialized_merge_and_affected(
                &mut stmts,
                &scratch,
                &delta_q,
                &intermediate,
                plan,
                op,
                &affected,
                &affected_groups_select(cols),
                i == 0,
            ),
            None => {
                push_materialized_merge(&mut stmts, &scratch, &delta_q, &intermediate, plan, op)
            }
        }
    }
    aggregate_epilogue_stmts(
        imv.name,
        source,
        "UPDATE",
        imv.end_query,
        plan,
        &grp_cols,
        &intermediate,
        &affected,
        &scratch,
        None,
        &mut stmts,
    );
    stmts
}

/// Whether Path C's bulk insert, which skips the delta SQL, keeps the IMV
/// exact: no other source holds unapplied writes and no correction is owed.
pub(crate) fn path_c_keeps_exact(view_name: &str, source: &str, base_query: &str) -> bool {
    let statement = delta_sources::running_statement();
    if statement == delta_sources::NO_TRANSACTION {
        return true;
    }
    let names = delta_sources::source_names(view_name, base_query);
    if names.as_ref().is_some_and(|names| names.len() < 2) {
        return true;
    }
    delta_sources::record_delta_reached(view_name, source);
    let imv = imv(view_name, base_query, "", None, names, statement);
    let Some(source_relation) = imv.source_named(source) else {
        return false;
    };
    imv.every_source_resolved
        && !recomputed_earlier_in_statement(view_name, statement)
        && matches!(stash(view_name, source_relation.oid), Stash::Absent)
        && imv.sources.iter().all(|(name, relation)| {
            relation.oid == source_relation.oid
                || !delta_sources::holds_unapplied_writes(name, relation)
                || delta_sources::source_writes(relation) == Some(applied(view_name, relation.oid))
        })
}
