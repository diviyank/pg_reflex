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
//! S holds unapplied writes when its per-transaction write count
//! ([`delta_sources::source_writes`]) differs from the rows its applied deltas
//! held (`applied`), whether or not its trigger events are queued yet. The
//! correction is exact only when X's delta saw every row of ΔS and nothing
//! else: S's writes counted when the stash was taken (`seen`) equal its writes
//! counted now, and exceed `applied` by exactly |ΔS|. Otherwise, and for every
//! shape the correction does not cover ([`corrects_exactly`]), V is recomputed,
//! and so is every later delta reaching V in the same statement.
//!
//! `applied`, the stash and its rows live in transaction-local settings, so a
//! rolled back subtransaction rolls them back with V's own rows, and the
//! correction creates no relation.

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

/// Marks the last statement of a delta, which the trigger replaces with the
/// statements it returns (`sql/trigger_body.plpgsql.in`).
const THEN_MARKER: &str = "--<<REFLEX_THEN>>--";

/// The most transition rows a stash keeps: past it, V is recomputed instead.
const STASH_ROW_LIMIT: i64 = 10_000;

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
    Taken {
        seen: i64,
        by: pg_sys::Oid,
        rows: i64,
    },
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

pub(crate) fn stash_setting(view_name: &str, source: pg_sys::Oid) -> String {
    format!(
        "pg_reflex.joint_stash_{}",
        hashed(&[view_name, &oid_text(source)])
    )
}

/// The transaction-local count of the corrected deltas applied to `view_name`.
pub(crate) fn corrections_setting(view_name: &str) -> String {
    format!("pg_reflex.joint_corrected_{}", hashed(&[view_name]))
}

/// One side (`new` or `old`) of the transition rows of `by` stashed for
/// `pending`: kept as the text of an array of `by`'s row type in a
/// transaction-local setting, read back under `name`.
struct StashedSide {
    name: String,
    setting: String,
}

fn stashed_side(view_name: &str, pending: pg_sys::Oid, by: pg_sys::Oid, side: &str) -> StashedSide {
    let key = hashed(&[view_name, &oid_text(pending), &oid_text(by)]);
    StashedSide {
        name: format!("__reflex_joint_{key}_{side}"),
        setting: format!("pg_reflex.joint_rows_{key}_{side}"),
    }
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
        Some(value) => {
            let mut parts = value.split(':');
            let taken = (|| {
                let seen = parts.next()?.parse().ok()?;
                let by = pg_sys::Oid::from(parts.next()?.parse::<u32>().ok()?);
                let rows = parts.next()?.parse().ok()?;
                Some(Stash::Taken { seen, by, rows })
            })();
            taken.unwrap_or(Stash::Poisoned)
        }
    }
}

fn set_setting_sql(name: &str, value: &str) -> String {
    format!("SELECT set_config('{name}', '{value}', true)")
}

/// Leaves no stash for `pending`: `meta` is the stash setting's new value.
fn replace_stash_sql(view_name: &str, pending: pg_sys::Oid, meta: &str) -> String {
    let mut sets = vec![format!(
        "set_config('{}', '{meta}', true)",
        stash_setting(view_name, pending)
    )];
    if let Stash::Taken { by, .. } = stash(view_name, pending) {
        for side in ["new", "old"] {
            sets.push(format!(
                "set_config('{}', '', true)",
                stashed_side(view_name, pending, by, side).setting
            ));
        }
    }
    format!("SELECT {}", sets.join(", "))
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

/// The schema-qualified row type of `relation`, which its stashed rows are
/// read back as.
fn row_type(relation: pg_sys::Oid) -> Option<String> {
    Spi::get_one_with_args::<String>(
        "SELECT format('%I.%I', n.nspname, t.typname) FROM pg_class c \
         JOIN pg_type t ON t.oid = c.reltype JOIN pg_namespace n ON n.oid = t.typnamespace \
         WHERE c.oid = $1",
        &[unsafe {
            pgrx::datum::DatumWithOid::new(relation, pg_sys::PgBuiltInOids::OIDOID.oid().value())
        }],
    )
    .ok()
    .flatten()
}

/// The delta SQL of a trigger on `source` for an IMMEDIATE IMV. `resolves`:
/// the trigger replaces a last statement marked [`THEN_MARKER`] with the
/// statements it returns, which a corrected delta needs.
#[allow(clippy::too_many_arguments)]
pub(crate) fn delta_sql(
    view_name: &str,
    source: &str,
    operation: &str,
    base_query: &str,
    end_query: &str,
    aggregations: Option<&str>,
    orig_base_query: &str,
    resolves: bool,
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
        Stash::Taken { seen, by, .. } if resolves => {
            exactness_check(&imv, source, &source_relation, operation, seen, by)
        }
        Stash::Taken { .. } | Stash::Poisoned => recompute(&imv),
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
        stmts.push(replace_stash_sql(imv.name, relation.oid, ""));
    }
    stmts.join(SEPARATOR)
}

/// The other sources holding writes no delta applied: their write counts differ
/// from the rows of their applied deltas, or are unknown.
fn pending_sources(imv: &Imv, source_oid: pg_sys::Oid) -> Vec<(pg_sys::Oid, Option<i64>)> {
    imv.sources
        .iter()
        .filter(|(_, relation)| relation.oid != source_oid)
        .filter_map(|(_, relation)| {
            let writes = delta_sources::source_writes(relation);
            (writes != Some(applied(imv.name, relation.oid))).then_some((relation.oid, writes))
        })
        .collect()
}

/// Records the rows of `source`'s delta as applied, and stashes its transition
/// rows for the other source holding unapplied writes.
fn bookkeeping(imv: &Imv, source: &str, source_oid: pg_sys::Oid, operation: &str) -> Vec<String> {
    let counted = applied_setting(imv.name, source_oid);
    let mut stmts = vec![format!(
        "SELECT set_config('{counted}', (COALESCE(NULLIF(current_setting('{counted}', true), ''), '0')::bigint \
         + (SELECT count(*) FROM \"{}\"))::text, true)",
        counted_transition(source, operation)
    )];
    let pending = pending_sources(imv, source_oid);
    if pending.is_empty() {
        return stmts;
    }
    let plan = registered_plan(imv.name, imv.aggregations);
    let source_row_type = row_type(source_oid);
    let stashable = pending.len() == 1
        && applies_linear_delta(imv, &plan, source, operation)
        && corrects_exactly(imv, &plan)
        && (plan.is_passthrough || source_row_type.is_some());
    let row_type = source_row_type.unwrap_or_default();
    for (oid, writes) in pending {
        let stash_sql = match (stashable, writes, stash(imv.name, oid)) {
            (true, Some(seen), Stash::Absent) => stash_rows_sql(
                imv, &plan, source, source_oid, oid, operation, seen, None, &row_type,
            ),
            (true, Some(writes), Stash::Taken { seen, by, rows })
                if seen == writes
                    && by == source_oid
                    && (plan.is_passthrough
                        || stash_lost_no_group(imv, &plan, source, oid, by, &row_type)) =>
            {
                stash_rows_sql(
                    imv,
                    &plan,
                    source,
                    source_oid,
                    oid,
                    operation,
                    seen,
                    Some(rows),
                    &row_type,
                )
            }
            _ => replace_stash_sql(imv.name, oid, POISONED),
        };
        stmts.push(stash_sql);
    }
    stmts
}

/// Stashes the transition rows of `source` for `pending`, after the rows
/// already stashed (`stashed_before`), as the text of arrays of `source`'s row
/// type in transaction-local settings: a rolled back subtransaction rolls them
/// back. Past [`STASH_ROW_LIMIT`] rows the stash is poisoned instead.
/// Passthrough corrections read no stashed rows.
#[allow(clippy::too_many_arguments)]
fn stash_rows_sql(
    imv: &Imv,
    plan: &AggregationPlan,
    source: &str,
    source_oid: pg_sys::Oid,
    pending: pg_sys::Oid,
    operation: &str,
    seen: i64,
    stashed_before: Option<i64>,
    row_type: &str,
) -> String {
    let meta = stash_setting(imv.name, pending);
    let by = oid_text(source_oid);
    if plan.is_passthrough {
        return set_setting_sql(&meta, &format!("{seen}:{by}:0"));
    }
    let transitions = transition_tables(source, operation);
    let sides = [
        ("new", transition_new_table_name(source)),
        ("old", transition_old_table_name(source)),
    ];
    let mut rows = vec![stashed_before.unwrap_or(0).to_string()];
    let mut sets = vec![format!(
        "set_config('{meta}', CASE WHEN c.n <= {STASH_ROW_LIMIT} THEN '{seen}:{by}:' || c.n \
         ELSE '{POISONED}' END, true)"
    )];
    for (side, transition) in sides {
        let stashed = stashed_side(imv.name, pending, source_oid, side);
        let added = if transitions.contains(&transition) {
            rows.push(format!("(SELECT count(*) FROM \"{transition}\")"));
            format!("COALESCE((SELECT array_agg(t)::text FROM \"{transition}\" t), '{{}}')")
        } else {
            "'{}'".to_string()
        };
        let value = match stashed_before {
            None => added,
            Some(_) => format!(
                "(current_setting('{}')::{row_type}[] || {added}::{row_type}[])::text",
                stashed.setting
            ),
        };
        sets.push(format!(
            "set_config('{}', CASE WHEN c.n <= {STASH_ROW_LIMIT} THEN {value} ELSE '' END, true)",
            stashed.setting
        ));
    }
    format!(
        "SELECT {} FROM (SELECT {} AS n) c",
        sets.join(", "),
        rows.join(" + ")
    )
}

/// `query` reading the stashed `sides` of rows of type `row_type` under their
/// names.
fn with_stashed(query: &str, sides: &[&StashedSide], row_type: &str) -> String {
    let ctes = sides
        .iter()
        .map(|side| {
            format!(
                "\"{}\" AS (SELECT * FROM unnest(current_setting('{}')::{row_type}[]))",
                side.name, side.setting
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("WITH {ctes} {query}")
}

/// Whether the deltas already stashed dropped no group they subtracted rows of
/// S never held from, so the stasher's next delta cannot bring such a group
/// back unseen by the exactness check. S's pending inserts are among its rows
/// now.
fn stash_lost_no_group(
    imv: &Imv,
    plan: &AggregationPlan,
    stasher: &str,
    pending: pg_sys::Oid,
    by: pg_sys::Oid,
    row_type: &str,
) -> bool {
    let old = stashed_side(imv.name, pending, by, "old");
    let joined = with_stashed(
        &replace_source_with_transition(imv.base_query, stasher, &old.name),
        &[&old],
        row_type,
    );
    !group_missing(imv, plan, &joined)
}

/// First stage of a corrected delta: the facts proving it exact that only the
/// trigger can read (the rows of `source`'s transition table, and whether a
/// group the correction subtracts from is missing), passed to the second stage
/// ([`resolved_delta_sql`]) by the statement the trigger replaces with its
/// result.
fn exactness_check(
    imv: &Imv,
    source: &str,
    relation: &Relation,
    operation: &str,
    seen: i64,
    by: pg_sys::Oid,
) -> String {
    let plan = registered_plan(imv.name, imv.aggregations);
    let Some(stasher) = imv.source_with_oid(by) else {
        return recompute(imv);
    };
    if !corrects_exactly(imv, &plan) || delta_sources::source_writes(relation) != Some(seen) {
        return recompute(imv);
    }
    let mut stmts = Vec::new();
    let mut group_missing = "false".to_string();
    if !plan.is_passthrough && reads_new(operation) {
        let Some(row_type) = row_type(by) else {
            return recompute(imv);
        };
        if let Some(check) = missing_group_sql(imv, &plan) {
            let old = stashed_side(imv.name, relation.oid, by, "old");
            let joined = joint_rows(
                imv,
                stasher,
                &old,
                &row_type,
                source,
                &transition_new_table_name(source),
            );
            let scratch = delta_scratch_table_name(imv.name);
            stmts.push(format!("TRUNCATE {scratch}"));
            stmts.push(format!("INSERT INTO {scratch} {joined}"));
            group_missing = check;
        }
    }
    stmts.push(format!(
        "{THEN_MARKER}\nSELECT public.reflex_joint_resolved_delta_sql({}, {}, {}, \
         (SELECT count(*) FROM \"{}\"), {group_missing})",
        literal(imv.name),
        literal(source),
        literal(operation),
        counted_transition(source, operation),
    ));
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

/// Second stage of a corrected delta: `−(ΔX ⋈ ΔS)`, then S's own delta, when
/// X's delta saw exactly ΔS — S's writes counted when the stash was taken are
/// its writes now, and exceed the rows of its applied deltas by the `counted`
/// rows of its transition table — and no group the correction subtracts from
/// is missing; a recompute otherwise.
pub(crate) fn resolved_delta_sql(
    view_name: &str,
    source: &str,
    operation: &str,
    counted: i64,
    group_missing: bool,
) -> String {
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
    let Stash::Taken { seen, by, .. } = stash(view_name, relation.oid) else {
        return recompute(&imv);
    };
    let Some(stasher) = imv.source_with_oid(by).map(str::to_string) else {
        return recompute(&imv);
    };
    let row_type = row_type(by);
    let exact = corrects_exactly(&imv, &plan)
        && delta_sources::source_writes(&relation) == Some(seen)
        && counted == seen - applied(view_name, relation.oid)
        && !group_missing
        && (plan.is_passthrough || row_type.is_some());
    if !exact {
        return recompute(&imv);
    }
    let mut stmts = correction(
        &imv,
        &plan,
        source,
        operation,
        &stasher,
        by,
        &row_type.unwrap_or_default(),
    );
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
    stmts.push(replace_stash_sql(view_name, relation.oid, ""));
    let corrections = corrections_setting(view_name);
    stmts.push(format!(
        "SELECT set_config('{corrections}', (COALESCE(NULLIF(current_setting('{corrections}', true), ''), '0')::bigint \
         + 1)::text, true)"
    ));
    stmts.join(SEPARATOR)
}

/// The condition, over the delta scratch table, that a group of its rows is
/// missing from the intermediate table. `None` for a scalar IMV, whose one row
/// is never dropped.
fn missing_group_sql(imv: &Imv, plan: &AggregationPlan) -> Option<String> {
    let cols = group_columns(plan)?;
    let scratch = delta_scratch_table_name(imv.name);
    let intermediate = intermediate_table_name(imv.name);
    let same_group = cols
        .iter()
        .map(|c| format!("i.{c} IS NOT DISTINCT FROM d.{c}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    Some(format!(
        "EXISTS (SELECT 1 FROM {scratch} d \
         WHERE NOT EXISTS (SELECT 1 FROM {intermediate} i WHERE {same_group}))"
    ))
}

/// Whether a group of the rows of `joined` is missing from the intermediate
/// table.
fn group_missing(imv: &Imv, plan: &AggregationPlan, joined: &str) -> bool {
    let Some(check) = missing_group_sql(imv, plan) else {
        return false;
    };
    let scratch = delta_scratch_table_name(imv.name);
    let filled = Spi::run(&format!("TRUNCATE {scratch}"))
        .and_then(|_| Spi::run(&format!("INSERT INTO {scratch} {joined}")));
    filled.is_err()
        || Spi::get_one::<bool>(&format!("SELECT {check}"))
            .ok()
            .flatten()
            .unwrap_or(true)
}

fn joint_rows(
    imv: &Imv,
    stasher: &str,
    stashed: &StashedSide,
    row_type: &str,
    source: &str,
    transition: &str,
) -> String {
    with_stashed(
        &replace_source_with_transition(
            &replace_source_with_transition(imv.base_query, stasher, &stashed.name),
            source,
            transition,
        ),
        &[stashed],
        row_type,
    )
}

/// `−(ΔX ⋈ ΔS)`. Aggregates: the additions first (X⁺ ⋈ S⁻, X⁻ ⋈ S⁺), then the
/// subtractions (X⁺ ⋈ S⁺, X⁻ ⋈ S⁻), each from a group that holds it (the
/// exactness check), so no group's count drops below its exact value.
/// Passthrough: rows are removed by key, which neither delta counts twice, so
/// only the rows X's delta inserted for S's inserted rows are removed, by S's
/// key, before S inserts them.
fn correction(
    imv: &Imv,
    plan: &AggregationPlan,
    source: &str,
    operation: &str,
    stasher: &str,
    by: pg_sys::Oid,
    row_type: &str,
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
    let stash_new = stashed_side(imv.name, pending, by, "new");
    let stash_old = stashed_side(imv.name, pending, by, "old");
    let mut pieces: Vec<(&StashedSide, &str, DeltaOp)> = Vec::new();
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
    for (i, (stashed, transition, op)) in pieces.into_iter().enumerate() {
        let delta_q = joint_rows(imv, stasher, stashed, row_type, source, transition);
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

/// The bookkeeping of a delta of `source` its trigger skips (an ignored source,
/// rows no IMV predicate or relevant column sees, Path C's bulk insert): its
/// rows count as applied, up to the source's writes: a recompute earlier in the
/// statement already counted them. A stash for `source` holding exactly these rows is
/// dropped, as X's delta that took it joined rows contributing nothing to the
/// IMV (or Path C ran with no stash, nothing pending); any other stash is
/// poisoned.
pub(crate) fn skipped_delta_sql(
    view_name: &str,
    source: &str,
    base_query: &str,
    transition: &str,
) -> String {
    let statement = delta_sources::running_statement();
    if statement == delta_sources::NO_TRANSACTION {
        return String::new();
    }
    let names = delta_sources::source_names(view_name, base_query);
    if names.as_ref().is_some_and(|names| names.len() < 2) {
        return String::new();
    }
    let imv = imv(view_name, base_query, "", None, names, statement);
    let Some(relation) = imv.source_named(source) else {
        return String::new();
    };
    let applied_before = applied(view_name, relation.oid);
    let writes = delta_sources::source_writes(&relation);
    let applied_after = match writes {
        Some(writes) => format!("LEAST({writes}, {applied_before} + c.n)"),
        None => format!("{applied_before} + c.n"),
    };
    let mut sets = vec![format!(
        "set_config('{}', ({applied_after})::text, true)",
        applied_setting(view_name, relation.oid)
    )];
    let held = stash(view_name, relation.oid);
    if !matches!(held, Stash::Absent) {
        let accounted = match writes {
            Some(writes) => format!("{writes} = {applied_before} + c.n"),
            None => "false".to_string(),
        };
        sets.push(format!(
            "set_config('{}', CASE WHEN {accounted} THEN '' ELSE '{POISONED}' END, true)",
            stash_setting(view_name, relation.oid)
        ));
    }
    if let Stash::Taken { by, .. } = held {
        for side in ["new", "old"] {
            sets.push(format!(
                "set_config('{}', '', true)",
                stashed_side(view_name, relation.oid, by, side).setting
            ));
        }
    }
    format!(
        "SELECT {} FROM (SELECT count(*) AS n FROM \"{transition}\") c",
        sets.join(", ")
    )
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
        && pending_sources(&imv, source_relation.oid).is_empty()
}
