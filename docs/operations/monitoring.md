# Monitoring

pg_reflex exposes four read-only SPIs (1.2.0+) plus one histogram SPI (1.3.0+) for observability.

## Daily-driver dashboard

```sql
-- Per-IMV summary, sorted by depth then latency
SELECT name, graph_depth, refresh_mode, row_count,
       last_flush_ms, last_flush_rows, flush_count, last_error
FROM reflex_ivm_status()
ORDER BY graph_depth, last_flush_ms DESC NULLS LAST;
```

## Single-IMV deep dive

```sql
-- Sizes + last-flush counters
SELECT * FROM reflex_ivm_stats('sales_by_region');

-- Latency distribution (1.3.0)
SELECT * FROM reflex_ivm_histogram('sales_by_region');

-- Plan that the next flush would run
SELECT reflex_explain_flush('sales_by_region');
```

## Failed-flush watch

```sql
-- Anything broken or not being maintained right now?
SELECT name, known_stale, stale_reason, last_error, row_count, is_estimate
FROM reflex_ivm_status()
WHERE known_stale OR last_error IS NOT NULL;
```

A row here is the operator signal to read `stale_reason` — since 1.11.4 it names the failure **and** the command that repairs it. Two shapes:

- **A caught flush failure** (`last_error` set): the IMV missed a change. Fix the cause (for a duplicate-key error, the duplicate source rows), then `SELECT reflex_reconcile('<name>')`.
- **A capped partition source** (1.11.4+, `stale_reason` starts with `partition flush for source`): a source root failed five flushes in a row and every later flush skips it, so the IMV and everything built on it stop receiving that source's changes. See the [runbook](runbook.md#imv-stopped-updating-capped-partition-source).

Under the default `pg_reflex.flush_failure_policy = 'warn'` these failures only raise a `WARNING`, which most drivers (asyncpg included) discard. Poll this query, or register a notice listener in the application.

## Wedged partition queue

```sql
SELECT source_root, attempts, age_seconds, last_error
FROM reflex_partition_pending_status()
ORDER BY age_seconds DESC;
```

A root that stays here across pushes is not draining. `reflex_doctor()` reports it as **F2** while it is still retried and **F2b** once it is capped.

## Maintenance event log (1.11.4+)

```sql
-- What happened to an IMV whose row count moved unexpectedly?
SELECT at, event, trigger_reason, slice, rows_before, rows_after, sqlstate, detail
FROM public.__reflex_event_log
WHERE imv_name = '<name>'
ORDER BY at DESC
LIMIT 50;
```

Written only for caught flush failures (`error`) and rebuilds that changed a slice's row count (`rebuild`) — never for an ordinary flush. A `rebuild` from `N → 0` on a slice that should hold data is the fingerprint of a partition swap evaluated while the IMV's own filter excluded that slice. Prune with `SELECT reflex_prune_event_log(INTERVAL '90 days')`.

## pg_stat_statements correlation (1.3.0+)

Each per-IMV flush body sets `application_name = 'reflex_flush:<view>'` for its duration. With `track_application_name = on` and `pg_stat_statements` enabled:

```sql
SELECT application_name,
       SUM(calls) AS calls,
       SUM(total_exec_time)::INT AS total_ms,
       AVG(mean_exec_time)::INT AS mean_ms
FROM pg_stat_statements_info pi
JOIN pg_stat_activity sa ON sa.application_name LIKE 'reflex_flush:%'
GROUP BY application_name
ORDER BY total_ms DESC;
```

Or with `log_line_prefix = '%t [%p] [%a] '`, your log-aggregator pipeline gets per-IMV correlation for free.

## Scheduled drift scan (1.2.1+)

```sql
-- Every 15 minutes, reconcile any IMV with no flush in the last hour.
SELECT cron.schedule('reflex-drift-scan', '*/15 * * * *',
    $$ SELECT * FROM reflex_scheduled_reconcile(60) $$);
```
