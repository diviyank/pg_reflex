use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::Spi;
use pgrx::PgBuiltInOids;

use crate::query_decomposer::intermediate_table_name;
use crate::sql_writer::identifier::quote;

/// One row of IMV status summary.  Returned by `reflex_ivm_status`.
type IvmStatusRow = (
    String,                                     // name
    i32,                                        // graph_depth
    bool,                                       // enabled
    String,                                     // refresh_mode
    i64,                                        // row_count (live SELECT count(*) on target)
    Option<i64>,                                // last_flush_ms
    Option<i64>,                                // last_flush_rows
    i64,                                        // flush_count
    Option<String>,                             // last_error
    Option<pgrx::datum::Timestamp>,             // last_update_date
    bool,                                       // known_stale
    Option<String>,                             // stale_reason
    bool,                                       // requires_explicit_refresh
    i64,                                        // rebuild_count
    Option<pgrx::datum::TimestampWithTimeZone>, // last_rebuild_at
    bool,                                       // is_estimate
);

/// Summary per IMV. `row_count` avoids a full-table `count(*)` on large IMVs:
/// it reports the planner estimate `pg_class.reltuples` when the target has been
/// analyzed (`reltuples > 0`, the common production case), and only falls back to
/// an exact `count(*)` when the estimate is unavailable (`reltuples <= 0`: an
/// empty target — where the count is instant — or one not yet analyzed), or when
/// the IMV carries an anomaly (`known_stale` or a retained `last_error`) — an
/// estimate taken before a caught flush failure can no longer be trusted. `is_estimate`
/// tells the caller which case produced `row_count`. This keeps the status view
/// O(1) per IMV in the common case instead of O(rows) on mature registries.
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
        name!(rebuild_count, i64),
        name!(last_rebuild_at, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(is_estimate, bool),
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
                        COALESCE(requires_explicit_refresh, FALSE) AS requires_explicit_refresh, \
                        COALESCE(rebuild_count, 0) AS rebuild_count, last_rebuild_at \
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
            let rebuild_count = row
                .get_by_name::<i64, _>("rebuild_count")
                .unwrap_or(None)
                .unwrap_or(0);
            let last_rebuild_at = row
                .get_by_name::<pgrx::datum::TimestampWithTimeZone, _>("last_rebuild_at")
                .unwrap_or(None);
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
                rebuild_count,
                last_rebuild_at,
                false,
            ));
        }
        out
    });

    // Populate row_count in a separate pass to keep the registry read short.
    // Prefer the planner estimate (reltuples) so a status query never full-scans
    // a large IMV target; fall back to an exact count when the estimate is
    // unavailable (reltuples <= 0 → empty or never-analyzed), where count(*) is
    // cheap or the only source of truth, or when the IMV carries an anomaly:
    // `known_stale`, a retained `last_error`, or an unresolved 'rebuild' row
    // newer than the target's last ANALYZE.
    //
    // An 'error' event-log row is deliberately NOT a term here: it is written
    // in the exact same handler that sets `known_stale`/`last_error` (the
    // deferred-flush EXCEPTION branch, src/trigger/deferred.rs), so
    // `known_stale`/`last_error` already see it — adding `event = 'error'`
    // here would be redundant AND non-converging, because reconcile clears
    // `known_stale`/`last_error` but never touches `__reflex_event_log`
    // (nothing does but the operator-driven `reflex_prune_event_log`). An
    // always-on 'error' term would therefore pin `has_anomaly` true forever
    // after one caught flush failure — permanent exact `COUNT(*)`,
    // `is_estimate` stuck false — exactly the non-convergence defect this
    // predicate exists to avoid. `known_stale`/`last_error` are the
    // convergent proxy for 'error' rows; only 'rebuild' needs its own term,
    // because it is the one anomaly (a successful-but-destructive rebuild)
    // that `known_stale` cannot see.
    //
    // 'rebuild' is scoped to ANALYZE recency rather than counting forever: a
    // rebuild is written for any slice-changing swap, which is routine, so an
    // unscoped predicate would make has_anomaly permanently true after the
    // first one. "The estimate may be stale" is true exactly when a slice was
    // rebuilt AFTER the planner last saw it, so scoping by
    // `COALESCE(last_analyze, last_autoanalyze)` from `pg_stat_all_tables`
    // (NULL, i.e. never analyzed, counts as always stale) converges by
    // construction: reconcile ANALYZEs the target (reconcile.rs:520 / :627),
    // so running the prescribed remedy retires the condition it caused —
    // reconcile clears `known_stale`/`last_error` directly and clears the
    // 'rebuild' term indirectly via its ANALYZE, never by touching the log.
    // (Left out of scope: a routine partition swap only ANALYZEs the child,
    // and autovacuum never auto-analyzes a partitioned parent, so
    // has_anomaly legitimately re-arms after each count-changing swap until
    // an operator reconciles or explicitly ANALYZEs the root — the root's
    // reltuples really is stale, so this is semantically honest, not a bug.
    // On an IMV swapped at every push the condition never retires and every
    // status call counts it exactly; `ANALYZE <root>` or `reflex_reconcile`
    // retires it. Tracked in untreated_bugs/
    // 2026-09-08_status_rebuild_anomaly_never_retires_on_swapped_partitioned_imv.md)
    // A failed/unavailable `pg_stat_all_tables` lookup fails toward the exact
    // count — the safe direction for a correctness alarm is to do the work.
    // A missing `__reflex_event_log` (e.g. an upgraded install whose
    // migration missed it) is checked for explicitly and skips the 'rebuild'
    // term rather than erroring — this is the primary observability entry
    // point and a missing maintenance table must not take it down for every
    // IMV.
    //
    // Missing target: neither the anomaly branch's `COUNT(*) FROM {ident}`
    // nor the estimate branch's `to_regclass`-scoped query is actually
    // protected against a dropped target — PostgreSQL resolves every
    // relation reference in a query at parse time regardless of which branch
    // or subquery contains it, so a `to_regclass` check elsewhere in the same
    // query does not stop `{ident}` from raising. This is a pre-existing gap
    // in `reflex_ivm_status` as a whole, not something this predicate widens
    // (see untreated_bugs/ for the filed report).
    let event_log_exists =
        Spi::get_one::<bool>("SELECT to_regclass('public.__reflex_event_log') IS NOT NULL AS ok")
            .unwrap_or(Some(false))
            .unwrap_or(false);
    let capped = capped_source_by_imv();
    let rows: Vec<IvmStatusRow> = rows
        .into_iter()
        .map(|mut row| {
            if let Some(source) = capped.get(&row.0) {
                let reason = source.stale_reason();
                row.11 = Some(match (row.10, row.11.take()) {
                    (true, Some(stored)) => format!("{stored} | {reason}"),
                    _ => reason,
                });
                row.10 = true;
            }
            let name = &row.0;
            let name_lit = name.replace('\'', "''");
            let has_unresolved_rebuild = event_log_exists
                && Spi::get_one::<bool>(&format!(
                    "SELECT EXISTS( \
                         SELECT 1 FROM public.__reflex_event_log e \
                         WHERE e.imv_name = '{name_lit}' \
                           AND e.event = 'rebuild' \
                           AND e.at > COALESCE( \
                                  (SELECT COALESCE(last_analyze, last_autoanalyze) \
                                   FROM pg_stat_all_tables \
                                   WHERE relid = to_regclass('{name_lit}')), \
                                  '-infinity'::timestamptz) \
                         LIMIT 1 \
                     ) AS ok"
                ))
                .unwrap_or(Some(true))
                .unwrap_or(true);
            let has_anomaly = row.10 || row.8.is_some() || has_unresolved_rebuild;
            let (c, is_estimate) = if has_anomaly {
                let c = Spi::get_one::<i64>(&format!(
                    "SELECT COUNT(*)::BIGINT AS c FROM {ident}",
                    ident = quote(name)
                ))
                .unwrap_or(None)
                .unwrap_or(-1);
                (c, false)
            } else {
                let count_sql = format!(
                    "SELECT CASE WHEN c.reltuples > 0 THEN c.reltuples::BIGINT \
                                 ELSE (SELECT COUNT(*)::BIGINT FROM {ident}) END AS c, \
                            (c.reltuples > 0) AS is_estimate \
                     FROM pg_class c WHERE c.oid = to_regclass('{name_lit}')",
                    ident = quote(name),
                    name_lit = name.replace('\'', "''"),
                );
                Spi::get_two::<i64, bool>(&count_sql)
                    .map(|(c, est)| (c.unwrap_or(-1), est.unwrap_or(false)))
                    .unwrap_or((-1, false))
            };
            row.4 = c;
            row.15 = is_estimate;
            row
        })
        .collect();

    TableIterator::new(rows)
}

/// A partition source root the flush has given up on, as seen by one IMV.
struct CappedSource {
    root: String,
    failures: i32,
    last_error: String,
}

impl CappedSource {
    fn stale_reason(&self) -> String {
        format!(
            "partition flush for source '{root}' is suspended after {failures} consecutive \
             failures (last error: {err}); changes to it are not reaching this IMV. Fix the \
             cause, then run SELECT reflex_reset_partition_failures('{root}'); \
             SELECT reflex_flush_partition_source('{root}');",
            root = self.root,
            failures = self.failures,
            err = self.last_error,
        )
    }
}

/// Every IMV that depends, directly or through other IMVs, on a partition source
/// root at `PARTITION_FLUSH_FAILURE_CAP`.
///
/// Both flush entry points skip a capped root, so none of its changes reach those
/// IMVs. The registry cannot carry this: a full reconcile clears `known_stale`
/// while the root stays capped, which is how dev tenants reported healthy IMVs
/// that had not been maintained for weeks. Derived live from the queue instead,
/// so the report clears exactly when the root drains.
fn capped_source_by_imv() -> std::collections::HashMap<String, CappedSource> {
    let sql = format!(
        "WITH RECURSIVE capped AS ( \
             SELECT source_root, failures, COALESCE(last_error, 'unknown') AS last_error \
             FROM public.__reflex_partition_pending \
             WHERE failures >= {cap} \
         ), reach(name, source_root, failures, last_error, depth) AS ( \
             SELECT r.name, c.source_root, c.failures, c.last_error, 1 \
             FROM capped c \
             JOIN public.__reflex_ivm_reference r \
               ON r.depends_on && ARRAY[c.source_root, split_part(c.source_root, '.', 2)] \
             UNION \
             SELECT r.name, x.source_root, x.failures, x.last_error, x.depth + 1 \
             FROM reach x \
             JOIN public.__reflex_ivm_reference r \
               ON r.depends_on && ARRAY[x.name, split_part(x.name, '.', 2), 'public.' || x.name] \
             WHERE x.depth < 32 \
         ) \
         SELECT DISTINCT ON (name) name, source_root, failures, last_error \
         FROM reach ORDER BY name, depth, source_root",
        cap = crate::partition::PARTITION_FLUSH_FAILURE_CAP,
    );
    Spi::connect(|client| {
        let mut by_imv = std::collections::HashMap::new();
        let Ok(rows) = client.select(&sql, None, &[]) else {
            return by_imv;
        };
        for row in rows {
            let name = row.get_by_name::<String, _>("name").ok().flatten();
            let root = row.get_by_name::<String, _>("source_root").ok().flatten();
            if let (Some(name), Some(root)) = (name, root) {
                by_imv.insert(
                    name,
                    CappedSource {
                        root,
                        failures: row
                            .get_by_name::<i32, _>("failures")
                            .ok()
                            .flatten()
                            .unwrap_or(0),
                        last_error: row
                            .get_by_name::<String, _>("last_error")
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| "unknown".to_string()),
                    },
                );
            }
        }
        by_imv
    })
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
