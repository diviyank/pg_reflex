# pg_cron recipes

[pg_cron](https://github.com/citusdata/pg_cron) is the canonical scheduler for PostgreSQL. Combined with the 1.2.1 `reflex_scheduled_reconcile` SPI, it covers drift detection without needing a background worker.

## Drift scan every 15 minutes

```sql
SELECT cron.schedule(
    'reflex-drift-scan',
    '*/15 * * * *',
    $$ SELECT * FROM reflex_scheduled_reconcile(60) $$
);
```

Reconciles any IMV whose `last_update_date` is older than 60 minutes (or `NULL`). The "60 minutes" threshold should match your tolerance for drift — set it to 0 to reconcile everything every cron tick.

## Reconcile after crash recovery

UNLOGGED IMVs are TRUNCATEd on crash. Schedule a one-shot reconcile-everything that runs at startup:

```sql
-- Run once 5 minutes after server start
SELECT cron.schedule(
    'reflex-post-restart-reconcile',
    '@reboot',
    $$ SELECT * FROM reflex_scheduled_reconcile(0); SELECT cron.unschedule('reflex-post-restart-reconcile'); $$
);
```

(`@reboot` requires pg_cron 1.5+.)

## Per-IMV nightly reconcile

For an IMV where you want a guaranteed-fresh state at the top of every day:

```sql
SELECT cron.schedule(
    'reflex-nightly-sales',
    '0 2 * * *',
    $$ SELECT reflex_rebuild_imv('sales_by_region') $$
);
```

## Check status

```sql
SELECT * FROM cron.job WHERE jobname LIKE 'reflex-%';
SELECT * FROM cron.job_run_details
WHERE jobid IN (SELECT jobid FROM cron.job WHERE jobname LIKE 'reflex-%')
ORDER BY start_time DESC LIMIT 20;
```

## Why pg_cron and not a Postgres background worker?

A background worker would tie pg_reflex to a specific PostgreSQL major version and complicate the build. pg_cron is already the production-standard scheduler and runs the SPI in a regular session — same SAVEPOINT semantics, same logging, same observability.
