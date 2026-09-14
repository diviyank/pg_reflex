# `reflex_scheduled_reconcile`

(1.2.1+) Reconciles every IMV whose `last_update_date` is older than the threshold. Designed for pg_cron-driven drift scans.

## Signature

```sql
reflex_scheduled_reconcile(max_age_minutes INTEGER DEFAULT 60)
RETURNS TABLE(name TEXT, status TEXT, ms BIGINT)
```

`max_age_minutes = 0` reconciles every enabled IMV.

## Behaviour

0. (1.11.4+) Heals first: every IMV with partitions queued in `__reflex_heal_pending` by a change to an ignored source is rebuilt for exactly those partitions (see [`reflex_heal_ignored_sources`](reflex_heal_ignored_sources.md)), scoped by `target_schema`. Each heal is one result row with status `HEALED` or its error. A healed IMV's `last_update_date` is refreshed, so step 1 does not also rebuild it in full.
1. Selects IMVs where `last_update_date IS NULL OR last_update_date < (CURRENT_TIMESTAMP - max_age_minutes)`.
2. For each, calls `reflex_reconcile(name)` in isolation. A failing reconcile emits a `WARNING` and is recorded with its error string in the result row, but does not abort the rest of the loop.
3. Returns one row per attempt with the wall time of the reconcile call.

## pg_cron recipe

```sql
-- Every 15 minutes, reconcile any IMV that hasn't updated in the last hour.
SELECT cron.schedule('reflex-drift-scan', '*/15 * * * *',
    $$ SELECT * FROM reflex_scheduled_reconcile(60) $$);
```

For UNLOGGED IMVs, schedule a reconcile after every crash recovery — UNLOGGED tables are TRUNCATEd on crash.

## Result example

```sql
SELECT * FROM reflex_scheduled_reconcile(60);
```

| name | status | ms |
|---|---|--:|
| `daily_totals` | `RECONCILED` | 124 |
| `monthly_totals` | `RECONCILED` | 38 |
| `broken_v` | `ERROR: …` | 12 |
