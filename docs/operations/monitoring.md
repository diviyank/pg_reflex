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
-- Anything broken right now?
SELECT name, last_error
FROM reflex_ivm_status()
WHERE last_error IS NOT NULL;
```

A row here is the operator signal to (a) read the error string, (b) call `reflex_explain_flush(name)`, (c) likely run `reflex_rebuild_imv(name)`.

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
