//! The sources whose deltas reached an IMV in the running top-level statement
//! and in the running transaction.
//!
//! Every AFTER STATEMENT trigger of one top-level statement fires after all of
//! its writes: a writable CTE writing A and B, or a user trigger on A writing B,
//! leaves B holding ΔB when A's trigger applies ΔA ⋈ B, and B's trigger then
//! applies A ⋈ ΔB against A holding ΔA, counting ΔA ⋈ ΔB twice. A pg_reflex flush
//! fired inside a statement (`SET CONSTRAINTS ALL IMMEDIATE`) is the same. An IMV
//! read as a source is exempt: its rows change only when its own maintenance
//! runs, which fires its trigger at once, after the readers of the base it
//! shares were maintained (deepest first).

use pgrx::prelude::*;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

#[derive(Default)]
struct DeltaSources {
    statement: (i64, i64),
    in_statement: Vec<String>,
    in_transaction: Vec<String>,
}

fn sources_by_imv() -> &'static Mutex<HashMap<String, DeltaSources>> {
    static SOURCES: OnceLock<Mutex<HashMap<String, DeltaSources>>> = OnceLock::new();
    SOURCES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Transaction and statement start. Two transactions or statements sent in one
/// query string share them, which only makes a recompute unneeded, never a
/// double count missed.
fn running_statement() -> (i64, i64) {
    unsafe {
        (
            pg_sys::GetCurrentTransactionStartTimestamp(),
            pg_sys::GetCurrentStatementStartTimestamp(),
        )
    }
}

/// Fails toward the recompute: a relation not found as an IMV counts as written.
fn is_imv(relation: &str) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM public.__reflex_ivm_reference r \
                       WHERE r.name = $1 OR to_regclass(r.name) = to_regclass($1))",
        &[unsafe {
            pgrx::datum::DatumWithOid::new(
                relation.to_string(),
                pg_sys::PgBuiltInOids::TEXTOID.oid().value(),
            )
        }],
    )
    .unwrap_or(None)
    .unwrap_or(false)
}

fn others(sources: &[String], source: &str) -> Vec<String> {
    sources
        .iter()
        .filter(|s| s.as_str() != source)
        .cloned()
        .collect()
}

fn remember(sources: &mut Vec<String>, source: &str) {
    if !sources.iter().any(|s| s == source) {
        sources.push(source.to_string());
    }
}

/// Records that `source`'s delta reaches `view_name` now, and tells whether a
/// written table other than `source` reached it earlier in the same statement:
/// then `source`'s delta joins rows already counted, and `view_name` must be
/// recomputed from its sources instead.
pub(crate) fn delta_would_count_twice(view_name: &str, source: &str) -> bool {
    let statement = running_statement();
    let earlier_in_statement = {
        let mut by_imv = sources_by_imv().lock().unwrap_or_else(|e| e.into_inner());
        let entry = by_imv.entry(view_name.to_string()).or_default();
        if entry.statement.0 != statement.0 {
            entry.in_transaction.clear();
        }
        if entry.statement != statement {
            entry.statement = statement;
            entry.in_statement.clear();
        }
        let earlier = others(&entry.in_statement, source);
        remember(&mut entry.in_statement, source);
        remember(&mut entry.in_transaction, source);
        earlier
    };
    !earlier_in_statement.is_empty()
        && !is_imv(source)
        && earlier_in_statement.iter().any(|earlier| !is_imv(earlier))
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
        entry.statement.0 == transaction && !others(&entry.in_transaction, source).is_empty()
    })
}
