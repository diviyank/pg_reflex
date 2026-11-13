//! Which sources of an IMV held writes not yet applied to it when another
//! source's delta was applied, in the running top-level statement; and which
//! sources' deltas reached it in the running transaction.
//!
//! Every AFTER STATEMENT trigger of one query fires after all of its writes: a
//! writable CTE writing A and B leaves B holding ΔB when A's trigger applies
//! ΔA ⋈ B, and B's trigger then applies A ⋈ ΔB against A holding ΔA, counting
//! ΔA ⋈ ΔB twice. A user trigger on A writing B, a cascading foreign key between
//! A and B, and a pg_reflex flush fired inside a statement (`SET CONSTRAINTS ALL
//! IMMEDIATE`), are the same. A source holds such writes exactly while
//! after-trigger events on it are queued and not yet fired: when its delta
//! arrives after another source's delta was applied during that window, the IMV
//! is recomputed instead. Statements run one after the other (a function writing
//! A then B) leave nothing queued, and an IMV read as a source is maintained, and
//! its own events fired, before its readers' next delta (deepest first), so both
//! stay incremental.
//!
//! Looking for queued events walks the whole queue once per relation, so a
//! relation is only looked at when this transaction may have written it: a heap
//! table whose statistics hold counts of this transaction (the per-transaction
//! part of its pending statistics, which earlier transactions never leave
//! behind), a partitioned source whose guard recorded a write (then its
//! partitioned tables and its leaves written in this transaction), and any
//! relation neither signal covers.

use pgrx::prelude::*;
use std::collections::HashMap;
use std::ffi::CString;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, OnceLock};

#[derive(Default)]
struct DeltaSources {
    statement: (i64, i64),
    unapplied_at_earlier_delta: Vec<pg_sys::Oid>,
    in_transaction: Vec<String>,
}

#[derive(Clone, Copy)]
enum WriteSignal {
    TransactionCounters,
    Guard,
    None,
}

#[derive(Clone, Copy)]
struct Relation {
    oid: pg_sys::Oid,
    partitioned: bool,
    signal: WriteSignal,
}

#[derive(Default)]
struct Relations {
    transaction: i64,
    by_name: HashMap<String, Option<Relation>>,
}

fn sources_by_imv() -> &'static Mutex<HashMap<String, DeltaSources>> {
    static SOURCES: OnceLock<Mutex<HashMap<String, DeltaSources>>> = OnceLock::new();
    SOURCES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The registered sources of an IMV, by IMV name and hashed definition.
type SourceNames = HashMap<(String, u64), Vec<String>>;

fn source_names_by_definition() -> &'static Mutex<SourceNames> {
    static NAMES: OnceLock<Mutex<SourceNames>> = OnceLock::new();
    NAMES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn relations_of_this_transaction() -> &'static Mutex<Relations> {
    static RELATIONS: OnceLock<Mutex<Relations>> = OnceLock::new();
    RELATIONS.get_or_init(|| Mutex::new(Relations::default()))
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

/// The sources `view_name` reads, `None` when the registry cannot be read.
fn source_names(view_name: &str, base_query: &str) -> Option<Vec<String>> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    base_query.hash(&mut hasher);
    let key = (view_name.to_string(), hasher.finish());
    if let Some(names) = source_names_by_definition()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
    {
        return Some(names.clone());
    }
    let names: Vec<String> = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT DISTINCT d.src::text AS src \
                 FROM public.__reflex_ivm_reference r \
                 CROSS JOIN LATERAL unnest(r.depends_on) d(src) \
                 WHERE r.name = $1 AND d.src NOT LIKE '<%'",
                None,
                &[text_arg(view_name)],
            )
            .ok()?;
        let mut names = Vec::new();
        for row in rows {
            names.push(row.get_by_name::<String, _>("src").ok()??);
        }
        Some(names)
    })?;
    if !names.is_empty() {
        source_names_by_definition()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, names.clone());
    }
    Some(names)
}

fn read_relation(name: &str) -> Option<Relation> {
    Spi::connect(|client| {
        let row = client
            .select(
                "SELECT c.oid, c.relkind::text AS relkind, c.relam = 2 AS heap, \
                        EXISTS (SELECT 1 FROM pg_trigger t \
                                WHERE t.tgrelid = c.oid AND t.tgname = $2) AS guarded \
                 FROM pg_class c WHERE c.oid = to_regclass($1)",
                Some(1),
                &[
                    text_arg(name),
                    text_arg(&crate::schema_builder::partitioned_source_trigger_name(
                        name,
                    )),
                ],
            )
            .ok()?
            .next()?;
        let oid = row.get_by_name::<pg_sys::Oid, _>("oid").ok()??;
        let relkind = row.get_by_name::<String, _>("relkind").ok()??;
        let guarded = row.get_by_name::<bool, _>("guarded").ok()??;
        let heap = row.get_by_name::<bool, _>("heap").ok()?.unwrap_or(false);
        let partitioned = relkind == "p";
        let signal = match relkind.as_str() {
            "r" | "m" | "t" if heap => WriteSignal::TransactionCounters,
            "p" if guarded => WriteSignal::Guard,
            _ => WriteSignal::None,
        };
        Some(Relation {
            oid,
            partitioned,
            signal,
        })
    })
}

fn relation(name: &str, transaction: i64) -> Option<Relation> {
    {
        let mut relations = relations_of_this_transaction()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if relations.transaction != transaction {
            relations.transaction = transaction;
            relations.by_name.clear();
        }
        if let Some(known) = relations.by_name.get(name) {
            return *known;
        }
    }
    let read = read_relation(name);
    relations_of_this_transaction()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .by_name
        .insert(name.to_string(), read);
    read
}

fn counts_writes() -> bool {
    unsafe { pg_sys::pgstat_track_counts }
}

/// `pgstat_fetch_pending_entry`, of `utils/pgstat_internal.h`, with the layout
/// of `PgStat_EntryRef` and the value of `PGSTAT_KIND_RELATION` shared by
/// PostgreSQL 15 to 18.
mod pending_statistics {
    use pgrx::pg_sys;

    pub(super) const KIND_RELATION: u32 = 2;

    #[repr(C)]
    pub(super) struct EntryRef {
        shared_entry: *mut core::ffi::c_void,
        shared_stats: *mut core::ffi::c_void,
        generation: u32,
        pub(super) pending: *mut core::ffi::c_void,
    }

    #[cfg(not(feature = "pg18"))]
    pub(super) type ObjectId = u32;
    #[cfg(feature = "pg18")]
    pub(super) type ObjectId = u64;

    unsafe extern "C" {
        pub(super) fn pgstat_fetch_pending_entry(
            kind: u32,
            dboid: pg_sys::Oid,
            objid: ObjectId,
        ) -> *mut EntryRef;
    }
}

/// Whether the statistics of `oid` count a row written by the running
/// transaction or one of its live subtransactions: the per-transaction counts
/// (`trans`) exist from the first counted write until the (sub)transaction ends.
fn rows_written_in_this_transaction(oid: pg_sys::Oid) -> bool {
    unsafe {
        let entry = pending_statistics::pgstat_fetch_pending_entry(
            pending_statistics::KIND_RELATION,
            pg_sys::MyDatabaseId,
            pending_statistics::ObjectId::from(oid.to_u32()),
        );
        if entry.is_null() || (*entry).pending.is_null() {
            return false;
        }
        let status = (*entry).pending as *const pg_sys::PgStat_TableStatus;
        !(*status).trans.is_null()
    }
}

fn guard_recorded_a_write(name: &str) -> bool {
    let Ok(setting) = CString::new(crate::schema_builder::source_written_setting(name)) else {
        return true;
    };
    let value = unsafe { pg_sys::GetConfigOption(setting.as_ptr(), true, false) };
    !value.is_null() && unsafe { *value } != 0
}

fn may_have_been_written(name: &str, relation: &Relation) -> bool {
    match relation.signal {
        WriteSignal::TransactionCounters => {
            !counts_writes() || rows_written_in_this_transaction(relation.oid)
        }
        WriteSignal::Guard => guard_recorded_a_write(name),
        WriteSignal::None => true,
    }
}

/// The tables of a partitioned source whose queued events can hold its writes:
/// the partitioned tables, which a statement naming them queues on, the leaves
/// this transaction wrote, and any leaf no signal covers. `None` when they
/// cannot be read.
fn tables_that_may_queue_writes(root: pg_sys::Oid) -> Option<Vec<pg_sys::Oid>> {
    let counts = counts_writes();
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT t.relid::oid AS rel, \
                        c.relkind IN ('r', 'm', 't') AND c.relam = 2 AS heap \
                 FROM pg_partition_tree($1) t JOIN pg_class c ON c.oid = t.relid \
                 WHERE t.relid::oid <> $1",
                None,
                &[unsafe {
                    pgrx::datum::DatumWithOid::new(
                        root,
                        pg_sys::PgBuiltInOids::OIDOID.oid().value(),
                    )
                }],
            )
            .ok()?;
        let mut tables = vec![root];
        for row in rows {
            let table = row.get_by_name::<pg_sys::Oid, _>("rel").ok()??;
            let heap = row.get_by_name::<bool, _>("heap").ok()?.unwrap_or(false);
            if !heap || !counts || rows_written_in_this_transaction(table) {
                tables.push(table);
            }
        }
        Some(tables)
    })
}

fn holds_unapplied_writes(name: &str, relation: &Relation) -> bool {
    if !may_have_been_written(name, relation) {
        return false;
    }
    if !relation.partitioned {
        return unsafe { pg_sys::AfterTriggerPendingOnRel(relation.oid) };
    }
    match tables_that_may_queue_writes(relation.oid) {
        Some(tables) => tables
            .into_iter()
            .any(|oid| unsafe { pg_sys::AfterTriggerPendingOnRel(oid) }),
        None => true,
    }
}

/// The sources of `view_name` other than `source` holding writes not yet
/// applied; `InvalidOid` stands for any source that cannot be resolved.
fn other_sources_holding_unapplied_writes(
    names: &[String],
    source: Option<pg_sys::Oid>,
    transaction: i64,
) -> Vec<pg_sys::Oid> {
    let mut holding: Vec<pg_sys::Oid> = Vec::new();
    for name in names {
        let found = match relation(name, transaction) {
            None => Some(pg_sys::InvalidOid),
            Some(other) if Some(other.oid) == source => None,
            Some(other) => holds_unapplied_writes(name, &other).then_some(other.oid),
        };
        if let Some(oid) = found {
            if !holding.contains(&oid) {
                holding.push(oid);
            }
        }
    }
    holding
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
    let nothing_owed = {
        let mut by_imv = sources_by_imv().lock().unwrap_or_else(|e| e.into_inner());
        entry_for_statement(&mut by_imv, view_name, statement)
            .unapplied_at_earlier_delta
            .is_empty()
    };
    if nothing_owed {
        return false;
    }
    let source_oid = relation(source, statement.0).map(|r| r.oid);
    let mut by_imv = sources_by_imv().lock().unwrap_or_else(|e| e.into_inner());
    owed(
        entry_for_statement(&mut by_imv, view_name, statement),
        source_oid,
    )
}

/// Records that `source`'s delta reaches `view_name` now, and tells whether it
/// would join rows already counted: `source` held writes not yet applied when
/// another source's delta reached `view_name` earlier in this statement. Then
/// `view_name` must be recomputed from its sources instead. An IMV over one
/// source is never looked at.
pub(crate) fn delta_would_count_twice(view_name: &str, source: &str, base_query: &str) -> bool {
    let statement = running_statement();
    if statement == NO_TRANSACTION {
        return false;
    }
    let names = source_names(view_name, base_query);
    if names.as_ref().is_some_and(|names| names.len() < 2) {
        return false;
    }
    let source_oid = relation(source, statement.0).map(|r| r.oid);
    let holding = match &names {
        Some(names) => other_sources_holding_unapplied_writes(names, source_oid, statement.0),
        None => vec![pg_sys::InvalidOid],
    };
    let mut by_imv = sources_by_imv().lock().unwrap_or_else(|e| e.into_inner());
    let entry = entry_for_statement(&mut by_imv, view_name, statement);
    let twice = owed(entry, source_oid);
    entry
        .unapplied_at_earlier_delta
        .retain(|oid| Some(*oid) != source_oid || *oid == pg_sys::InvalidOid);
    for oid in holding {
        if !entry.unapplied_at_earlier_delta.contains(&oid) {
            entry.unapplied_at_earlier_delta.push(oid);
        }
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
