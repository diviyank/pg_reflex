# Runbook

The four scenarios that show up in production.

## Flush keeps failing on one IMV

```sql
-- Find the bad IMV
SELECT name, last_error, flush_count
FROM reflex_ivm_status()
WHERE last_error IS NOT NULL;

-- Inspect the plan the next flush would run
SELECT reflex_explain_flush('<name>');

-- Rebuild from scratch
SELECT reflex_rebuild_imv('<name>');
```

A failing IMV no longer aborts the cascade — its `last_error` is recorded and the next IMV runs normally (per-IMV SAVEPOINT, since 1.2.0).

## IMV drifted after a crash

UNLOGGED intermediates are TRUNCATEd on crash recovery. Run:

```sql
SELECT reflex_rebuild_imv('<name>');
```

…or schedule it for all IMVs at once via [pg_cron](pg-cron.md):

```sql
SELECT * FROM reflex_scheduled_reconcile(0);  -- 0 = every IMV regardless of age
```

For latency-sensitive deployments, use `storage='LOGGED'` at IMV creation — the intermediate becomes a regular WAL-logged table and survives crash recovery without TRUNCATE.

## Source `ALTER TABLE` warning

```
WARNING: pg_reflex: source table orders was altered; IMV daily_totals may be stale — run SELECT reflex_rebuild_imv('daily_totals') to recover
```

Run `reflex_rebuild_imv('<name>')` for each affected IMV. To make this part of your DDL change-control gate from 1.2.1 onwards, set:

```sql
SET pg_reflex.alter_source_policy = 'error';
```

…and the next ALTER on a tracked source rolls back.

## Cascade is slow

```sql
-- Sort by depth, then by last flush latency
SELECT name, graph_depth, last_flush_ms, last_flush_rows, flush_count
FROM reflex_ivm_status()
ORDER BY graph_depth, last_flush_ms DESC NULLS LAST;
```

If one IMV dominates the latency budget, check its plan:

```sql
SELECT reflex_explain_flush('<bottleneck>');
```

Common causes:

| Symptom | Likely cause | Fix |
|---|---|---|
| MIN/MAX IMV with full-source seq-scan in EXPLAIN | Scoped recompute path with too many affected groups | Opt into `topk=K` (1.3.0) — re-create with the topk parameter |
| Passthrough DELETE doing full refresh | No `unique_columns` and no inferable PK | Add a PK to the source, or pass `unique_columns` explicitly |
| `__reflex_intermediate_*` table much larger than expected | Aggregate state is wider than user output | Check `reflex_ivm_stats(name)` — `BOOL_OR` and `AVG` add companion columns |
| First flush after cold start is slow | Stats not analysed yet | Run `ANALYZE __reflex_intermediate_<name>` |

## DELETE on source fails with "missing FROM-clause entry"

This was a 1.0.0 bug — schema-qualified table references with column qualifiers (e.g., `alp.sales_simulation.product_id`) confused the trigger. Fixed in 1.0.1. Upgrade.

## IMV created but DELETE on the source returns the wrong row count

Passthrough IMVs require a unique key for incremental DELETE/UPDATE. Without one, DELETE on the source falls back to a full refresh, which still gets the right answer but is slow. The 1.2.1 release auto-infers the key from the source PK for single-source passthroughs; if your IMV has joins, pass `unique_columns` explicitly:

```sql
SELECT create_reflex_ivm('v', 'SELECT id, name FROM src', 'id');
```
