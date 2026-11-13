//! Which sources of an IMV held writes not yet applied to it when another
//! source's delta was applied, in the running top-level statement; and which
//! sources' deltas reached it in the running transaction.
//!
//! Every AFTER STATEMENT trigger of one query fires after all of its writes: a
//! writable CTE writing A and B leaves B holding ΔB when A's trigger applies
//! ΔA ⋈ B, and B's trigger then applies A ⋈ ΔB against A holding ΔA, counting
//! ΔA ⋈ ΔB twice. A user trigger on A writing B, and a pg_reflex flush fired
//! inside a statement (`SET CONSTRAINTS ALL IMMEDIATE`), are the same. A source
//! holds such writes exactly while after-trigger events on it are queued and
//! not yet fired: when its delta arrives after another source's delta was
//! applied during that window, the IMV is recomputed instead. Statements run
//! one after the other (a function writing A then B) leave nothing queued, and
//! an IMV read as a source is maintained, and its own events fired, before its
//! readers' next delta (deepest first), so both stay incremental.

use pgrx::prelude::*;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

#[derive(Default)]
struct DeltaSources {
    statement: (i64, i64),
    unapplied_at_earlier_delta: Vec<pg_sys::Oid>,
    in_transaction: Vec<String>,
}

fn sources_by_imv() -> &'static Mutex<HashMap<String, DeltaSources>> {
    static SOURCES: OnceLock<Mutex<HashMap<String, DeltaSources>>> = OnceLock::new();
    SOURCES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// What `running_statement` reads outside a PostgreSQL backend (the SQL-generation
/// unit tests), where no source can have been written.
const NO_TRANSACTION: (i64, i64) = (0, 0);

fn running_statement() -> (i64, i64) {
    if cfg!(test) {
        return NO_TRANSACTION;
    }
    unsafe {
        (
            pg_sys::GetCurrentTransactionStartTimestamp(),
            pg_sys::GetCurrentStatementStartTimestamp(),
        )
    }
}

fn text_arg(value: &str) -> pgrx::datum::DatumWithOid<'static> {
    unsafe {
        pgrx::datum::DatumWithOid::new(
            value.to_string(),
            pg_sys::PgBuiltInOids::TEXTOID.oid().value(),
        )
    }
}

fn relation_oid(relation: &str) -> Option<pg_sys::Oid> {
    Spi::get_one_with_args::<pg_sys::Oid>("SELECT to_regclass($1)::oid", &[text_arg(relation)])
        .unwrap_or(None)
}

/// The sources of `view_name` other than `source` with after-trigger events
/// queued and not yet fired, on themselves or on one of their partitions.
/// `None` when they cannot be read: every source then counts as holding writes.
fn other_sources_holding_unapplied_writes(
    view_name: &str,
    source: Option<pg_sys::Oid>,
) -> Option<Vec<pg_sys::Oid>> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "WITH s AS ( \
                   SELECT DISTINCT to_regclass(d.src) AS rel \
                   FROM public.__reflex_ivm_reference r \
                   CROSS JOIN LATERAL unnest(r.depends_on) d(src) \
                   WHERE r.name = $1 AND d.src NOT LIKE '<%') \
                 SELECT s.rel::oid AS source, t.relid::oid AS rel \
                 FROM s CROSS JOIN LATERAL ( \
                   SELECT s.rel AS relid UNION SELECT relid FROM pg_partition_tree(s.rel)) t \
                 WHERE s.rel IS NOT NULL",
                None,
                &[text_arg(view_name)],
            )
            .ok()?;
        let mut holding: Vec<pg_sys::Oid> = Vec::new();
        for row in rows {
            let other = row.get_by_name::<pg_sys::Oid, _>("source").ok()??;
            let relation = row.get_by_name::<pg_sys::Oid, _>("rel").ok()??;
            if Some(other) != source
                && !holding.contains(&other)
                && unsafe { pg_sys::AfterTriggerPendingOnRel(relation) }
            {
                holding.push(other);
            }
        }
        Some(holding)
    })
}

fn entry_for_statement<'a>(
    by_imv: &'a mut HashMap<String, DeltaSources>,
    view_name: &str,
    statement: (i64, i64),
) -> &'a mut DeltaSources {
    let entry = by_imv.entry(view_name.to_string()).or_default();
    if entry.statement.0 != statement.0 {
        entry.in_transaction.clear();
    }
    if entry.statement != statement {
        entry.statement = statement;
        entry.unapplied_at_earlier_delta.clear();
    }
    entry
}

/// An unresolved source, or an earlier delta whose other sources could not be
/// read (`InvalidOid`), owes the recompute.
fn owed(entry: &DeltaSources, source: Option<pg_sys::Oid>) -> bool {
    let earlier = &entry.unapplied_at_earlier_delta;
    match source {
        Some(oid) => earlier.contains(&oid) || earlier.contains(&pg_sys::InvalidOid),
        None => !earlier.is_empty(),
    }
}

/// Whether `source`'s delta, applied to `view_name` now, would join rows
/// already counted, without recording the application.
pub(crate) fn delta_owes_recompute(view_name: &str, source: &str) -> bool {
    let statement = running_statement();
    if statement == NO_TRANSACTION {
        return false;
    }
    let source_oid = relation_oid(source);
    let mut by_imv = sources_by_imv().lock().unwrap_or_else(|e| e.into_inner());
    owed(
        entry_for_statement(&mut by_imv, view_name, statement),
        source_oid,
    )
}

/// Records that `source`'s delta reaches `view_name` now, and tells whether it
/// would join rows already counted: `source` held writes not yet applied when
/// another source's delta reached `view_name` earlier in this statement. Then
/// `view_name` must be recomputed from its sources instead.
pub(crate) fn delta_would_count_twice(view_name: &str, source: &str) -> bool {
    let statement = running_statement();
    if statement == NO_TRANSACTION {
        return false;
    }
    let source_oid = relation_oid(source);
    let holding = other_sources_holding_unapplied_writes(view_name, source_oid);
    let mut by_imv = sources_by_imv().lock().unwrap_or_else(|e| e.into_inner());
    let entry = entry_for_statement(&mut by_imv, view_name, statement);
    let twice = owed(entry, source_oid);
    entry
        .unapplied_at_earlier_delta
        .retain(|oid| Some(*oid) != source_oid || *oid == pg_sys::InvalidOid);
    match holding {
        Some(holding) => {
            for oid in holding {
                if !entry.unapplied_at_earlier_delta.contains(&oid) {
                    entry.unapplied_at_earlier_delta.push(oid);
                }
            }
        }
        None => entry.unapplied_at_earlier_delta.push(pg_sys::InvalidOid),
    }
    if !entry.in_transaction.iter().any(|s| s == source) {
        entry.in_transaction.push(source.to_string());
    }
    twice
}

/// Whether the delta of a source other than `source` reached `view_name` in the
/// running transaction.
pub(crate) fn reached_from_another_source_in_this_transaction(
    view_name: &str,
    source: &str,
) -> bool {
    let transaction = running_statement().0;
    let by_imv = sources_by_imv().lock().unwrap_or_else(|e| e.into_inner());
    by_imv.get(view_name).is_some_and(|entry| {
        entry.statement.0 == transaction && entry.in_transaction.iter().any(|s| s != source)
    })
}
