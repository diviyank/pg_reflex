use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::Spi;
use pgrx::PgBuiltInOids;

use crate::query_decomposer::intermediate_table_name;
use crate::sql_writer::identifier::quote;

/// One row of IMV status summary.  Returned by `reflex_ivm_status`.
type IvmStatusRow = (
    String,                         // name
    i32,                            // graph_depth
    bool,                           // enabled
    String,                         // refresh_mode
    i64,                            // row_count (live SELECT count(*) on target)
    Option<i64>,                    // last_flush_ms
    Option<i64>,                    // last_flush_rows
    i64,                            // flush_count
    Option<String>,                 // last_error
    Option<pgrx::datum::Timestamp>, // last_update_date
    bool,                           // known_stale
    Option<String>,                 // stale_reason
    bool,                           // requires_explicit_refresh
);

/// Summary per IMV. `row_count` avoids a full-table `count(*)` on large IMVs:
/// it reports the planner estimate `pg_class.reltuples` when the target has been
/// analyzed (`reltuples > 0`, the common production case), and only falls back to
/// an exact `count(*)` when the estimate is unavailable (`reltuples <= 0`: an
/// empty target — where the count is instant — or one not yet analyzed). This
/// keeps the status view O(1) per IMV instead of O(rows) on mature registries.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn reflex_ivm_status() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(graph_depth, i32),
        name!(enabled, bool),
        name!(refresh_mode, String),
        name!(row_count, i64),
        name!(last_flush_ms, Option<i64>),
        name!(last_flush_rows, Option<i64>),
        name!(flush_count, i64),
        name!(last_error, Option<String>),
        name!(last_update_date, Option<pgrx::datum::Timestamp>),
        name!(known_stale, bool),
        name!(stale_reason, Option<String>),
        name!(requires_explicit_refresh, bool),
    ),
> {
    let rows: Vec<IvmStatusRow> = Spi::connect(|client| {
        let mut out = Vec::new();
        let rs = client
            .select(
                "SELECT name, graph_depth, COALESCE(enabled, TRUE) AS enabled, \
                        COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, \
                        last_flush_ms, last_flush_rows, COALESCE(flush_count, 0) AS flush_count, \
                        last_error, last_update_date, COALESCE(known_stale, FALSE) AS known_stale, stale_reason, \
                        COALESCE(requires_explicit_refresh, FALSE) AS requires_explicit_refresh \
                 FROM public.__reflex_ivm_reference \
                 ORDER BY graph_depth, name",
                None,
                &[],
            )
            .unwrap_or_report();
        for row in rs {
            let name: String = row
                .get_by_name::<&str, _>("name")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let depth = row
                .get_by_name::<i32, _>("graph_depth")
                .unwrap_or(None)
                .unwrap_or(0);
            let enabled = row
                .get_by_name::<bool, _>("enabled")
                .unwrap_or(None)
                .unwrap_or(true);
            let mode: String = row
                .get_by_name::<&str, _>("refresh_mode")
                .unwrap_or(None)
                .unwrap_or("IMMEDIATE")
                .to_string();
            let last_ms = row.get_by_name::<i64, _>("last_flush_ms").unwrap_or(None);
            let last_rows = row.get_by_name::<i64, _>("last_flush_rows").unwrap_or(None);
            let flush_count = row
                .get_by_name::<i64, _>("flush_count")
                .unwrap_or(None)
                .unwrap_or(0);
            let last_err = row
                .get_by_name::<&str, _>("last_error")
                .unwrap_or(None)
                .map(|s| s.to_string());
            let last_upd = row
                .get_by_name::<pgrx::datum::Timestamp, _>("last_update_date")
                .unwrap_or(None);
            let known_stale = row
                .get_by_name::<bool, _>("known_stale")
                .unwrap_or(None)
                .unwrap_or(false);
            let stale_reason = row
                .get_by_name::<&str, _>("stale_reason")
                .unwrap_or(None)
                .map(|s| s.to_string());
            let requires_explicit_refresh = row
                .get_by_name::<bool, _>("requires_explicit_refresh")
                .unwrap_or(None)
                .unwrap_or(false);
            out.push((
                name,
                depth,
                enabled,
                mode,
                0i64,
                last_ms,
                last_rows,
                flush_count,
                last_err,
                last_upd,
                known_stale,
                stale_reason,
                requires_explicit_refresh,
            ));
        }
        out
    });

    // Populate row_count in a separate pass to keep the registry read short.
    // Prefer the planner estimate (reltuples) so a status query never full-scans
    // a large IMV target; fall back to an exact count only when the estimate is
    // unavailable (reltuples <= 0 → empty or never-analyzed), where count(*) is
    // cheap or the only source of truth. Missing target → to_regclass NULL →
    // no row → the -1 sentinel (unchanged from the prior "could not determine").
    let rows: Vec<IvmStatusRow> = rows
        .into_iter()
        .map(|mut row| {
            let name = &row.0;
            let count_sql = format!(
                "SELECT CASE WHEN c.reltuples > 0 THEN c.reltuples::BIGINT \
                             ELSE (SELECT COUNT(*)::BIGINT FROM {ident}) END AS c \
                 FROM pg_class c WHERE c.oid = to_regclass('{name_lit}')",
                ident = quote(name),
                name_lit = name.replace('\'', "''"),
            );
            let c = Spi::get_one::<i64>(&count_sql)
                .unwrap_or(None)
                .unwrap_or(-1);
            row.4 = c;
            row
        })
        .collect();

    TableIterator::new(rows)
}

/// Detailed stats for a single IMV: intermediate size, target size, index count,
/// trigger count, last flush timing.
#[pg_extern]
fn reflex_ivm_stats(
    view_name: &str,
) -> TableIterator<'static, (name!(metric, String), name!(value, String))> {
    let mut out: Vec<(String, String)> = Vec::new();
    let qv = quote(view_name);
    // Co-located intermediate table (1.4.1): same schema as the IMV. The helper
    // returns either `"schema"."local"` or a bare local name.
    let interm = intermediate_table_name(view_name);
    let target = qv.clone();

    let interm_size: Option<String> = Spi::get_one(&format!(
        "SELECT pg_size_pretty(pg_total_relation_size('{}'))",
        interm_quoted(&interm)
    ))
    .unwrap_or(None);
    if let Some(sz) = interm_size {
        out.push(("intermediate_size".to_string(), sz));
    }

    let target_size: Option<String> = Spi::get_one(&format!(
        "SELECT pg_size_pretty(pg_total_relation_size('{}'))",
        target.replace("'", "''")
    ))
    .unwrap_or(None);
    if let Some(sz) = target_size {
        out.push(("target_size".to_string(), sz));
    }

    // Registry metrics
    let args =
        [
            unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            },
        ];
    #[allow(clippy::type_complexity)]
    let rows: Vec<(Option<i64>, Option<i64>, i64, Option<String>)> = Spi::connect(|client| {
        client
            .select(
                "SELECT last_flush_ms, last_flush_rows, COALESCE(flush_count, 0) AS flush_count, last_error \
                 FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &args,
            )
            .unwrap_or_report()
            .map(|r| {
                (
                    r.get_by_name::<i64, _>("last_flush_ms").unwrap_or(None),
                    r.get_by_name::<i64, _>("last_flush_rows").unwrap_or(None),
                    r.get_by_name::<i64, _>("flush_count")
                        .unwrap_or(None)
                        .unwrap_or(0),
                    r.get_by_name::<&str, _>("last_error")
                        .unwrap_or(None)
                        .map(|s| s.to_string()),
                )
            })
            .collect()
    });
    if let Some((ms, rcnt, fcnt, err)) = rows.into_iter().next() {
        out.push((
            "last_flush_ms".to_string(),
            ms.map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
        ));
        out.push((
            "last_flush_rows".to_string(),
            rcnt.map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
        ));
        out.push(("flush_count".to_string(), fcnt.to_string()));
        out.push((
            "last_error".to_string(),
            err.unwrap_or_else(|| "NULL".to_string()),
        ));
    }

    TableIterator::new(out)
}

/// One row in the result of `reflex_ivm_histogram`.
type HistogramRow = (Option<f64>, Option<f64>, Option<f64>, Option<i64>, i64);

/// Returns flush latency percentiles for an IMV computed from the
/// `flush_ms_history` ring buffer (1.3.0). The buffer holds up to 64 most
/// recent samples; the SPI returns p50, p95, p99, max, and the sample count.
/// Returns an empty result if the IMV is not registered or has no recorded
/// flushes.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn reflex_ivm_histogram(
    view_name: &str,
) -> TableIterator<
    'static,
    (
        name!(p50_ms, Option<f64>),
        name!(p95_ms, Option<f64>),
        name!(p99_ms, Option<f64>),
        name!(max_ms, Option<i64>),
        name!(samples, i64),
    ),
> {
    let args =
        [
            unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            },
        ];
    let row: Option<HistogramRow> = Spi::connect(|client| {
        client
            .select(
                "WITH samples AS (\
                       SELECT v::DOUBLE PRECISION AS ms \
                       FROM public.__reflex_ivm_reference, \
                            unnest(COALESCE(flush_ms_history, ARRAY[]::BIGINT[])) AS v \
                       WHERE name = $1 \
                     ) \
                     SELECT \
                       percentile_cont(0.50) WITHIN GROUP (ORDER BY ms) AS p50, \
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95, \
                       percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99, \
                       MAX(ms)::BIGINT AS max_ms, \
                       COUNT(*)::BIGINT AS samples \
                     FROM samples",
                None,
                &args,
            )
            .unwrap_or_report()
            .next()
            .map(|r| {
                (
                    r.get_by_name::<f64, _>("p50").unwrap_or(None),
                    r.get_by_name::<f64, _>("p95").unwrap_or(None),
                    r.get_by_name::<f64, _>("p99").unwrap_or(None),
                    r.get_by_name::<i64, _>("max_ms").unwrap_or(None),
                    r.get_by_name::<i64, _>("samples")
                        .unwrap_or(None)
                        .unwrap_or(0),
                )
            })
    });

    TableIterator::new(row.into_iter().collect::<Vec<_>>())
}

/// One row of partition pending status. Returned by `reflex_partition_pending_status`.
type PartitionPendingRow = (
    String,                             // source_root
    pgrx::datum::TimestampWithTimeZone, // enqueued_at
    i64,                                // age_seconds
    i32,                                // attempts
    Option<String>,                     // last_error
);

/// Per-partition pending work: age, attempt count, and last error message.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn reflex_partition_pending_status() -> TableIterator<
    'static,
    (
        name!(source_root, String),
        name!(enqueued_at, pgrx::datum::TimestampWithTimeZone),
        name!(age_seconds, i64),
        name!(attempts, i32),
        name!(last_error, Option<String>),
    ),
> {
    let rows: Vec<PartitionPendingRow> = Spi::connect(|client| {
        let mut out = Vec::new();
        let rs = client
            .select(
                "SELECT source_root, enqueued_at, \
                        extract(epoch FROM now() - enqueued_at)::int8 AS age_seconds, \
                        attempts, last_error \
                 FROM public.__reflex_partition_pending ORDER BY enqueued_at",
                None,
                &[],
            )
            .unwrap_or_report();
        for row in rs {
            let source_root: String = row
                .get_by_name::<&str, _>("source_root")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let enqueued_at = row
                .get_by_name::<pgrx::datum::TimestampWithTimeZone, _>("enqueued_at")
                .unwrap()
                .unwrap();
            let age_seconds = row
                .get_by_name::<i64, _>("age_seconds")
                .unwrap_or(None)
                .unwrap_or(0);
            let attempts = row
                .get_by_name::<i32, _>("attempts")
                .unwrap_or(None)
                .unwrap_or(0);
            let last_error = row
                .get_by_name::<&str, _>("last_error")
                .unwrap_or(None)
                .map(|s| s.to_string());
            out.push((source_root, enqueued_at, age_seconds, attempts, last_error));
        }
        out
    });

    TableIterator::new(rows)
}

/// Returns the `EXPLAIN` statement for what the next flush would execute for a
/// given IMV, ready to run. Useful for diagnosing plan regressions without
/// firing a flush.
///
/// It returns the SQL rather than executing `EXPLAIN` itself: `EXPLAIN` is a
/// utility statement, and PostgreSQL forbids utility statements under a
/// read-only SPI context — which is the context of this function when it is
/// called from a plain top-level `SELECT` (raising the misleadingly worded
/// "EXPLAIN is not allowed in a non-volatile function" even though this function
/// is `VOLATILE`). Returning the statement sidesteps that entirely: it works in
/// any context (including read-only transactions and standbys) and lets the
/// caller choose `EXPLAIN ANALYZE`, `FORMAT JSON`, etc.
#[pg_extern(volatile)]
fn reflex_explain_flush(view_name: &str) -> String {
    let args =
        [
            unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            },
        ];
    let base: Option<String> = Spi::connect(|client| {
        client
            .select(
                "SELECT base_query FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &args,
            )
            .unwrap_or_report()
            .next()
            .and_then(|r| {
                r.get_by_name::<&str, _>("base_query")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
    });
    match base {
        Some(b) if !b.is_empty() => format!("EXPLAIN (VERBOSE, COSTS ON) {}", b),
        _ => format!("ERROR: no registered IMV '{}'", view_name),
    }
}

fn interm_quoted(name: &str) -> String {
    // pg_total_relation_size accepts an escaped relation literal
    name.replace("'", "''")
}
