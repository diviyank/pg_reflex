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

## IMV stopped updating: capped partition source

Symptom: an IMV over a partitioned source (e.g. a forecast view fed by monthly partition swaps) stops reflecting new pushes, with no error in the application. Since 1.11.4 `reflex_ivm_status()` reports it `known_stale` with a `stale_reason` naming the source root; before 1.11.4 it looked healthy.

Cause: the root failed `5` consecutive partition flushes (`PARTITION_FLUSH_FAILURE_CAP`), so every later flush **skips** it with only a `WARNING`. A full `reflex_reconcile` repairs the data once but does not re-arm the root, so the next push is skipped again.

```sql
-- 1. Which roots are capped, and why?
SELECT source_root, failures, last_attempt_at, last_error
FROM public.__reflex_partition_pending
WHERE failures >= 5;

-- 2. Fix the cause first. For "duplicate key value violates unique constraint
--    __reflex_swap_tgt_…": find duplicate source rows on the IMV's unique key.
SELECT unique_columns FROM public.__reflex_ivm_reference WHERE name = '<imv>';
-- SELECT <unique cols>, COUNT(*) FROM <source> GROUP BY <unique cols> HAVING COUNT(*) > 1;

-- 3. Re-arm and drain.
SELECT reflex_reset_partition_failures('<schema.source_root>');
SELECT reflex_flush_partition_source('<schema.source_root>');
```

The report clears as soon as the root drains. A `deadlock detected` last error is usually transient — re-arming alone fixes it. A duplicate-key error re-caps the root on the first retry unless step 2 removed the duplicates. `reflex_doctor(fix => TRUE)` performs step 3 with a single retry (F2b), never step 2.

## IMV slice empty after an ignored source changed

**Symptom:** a partition of an IMV is empty or outdated after a row of an ignored source (e.g. `demand_planning.status`) left and re-entered the query's filter, and `reflex_ivm_status()` reports `known_stale` with a `stale_reason` naming that source.

Since 1.11.4 the change only queues the affected partition keys; the rebuild waits for a sweep. Heal now:

```sql
SELECT partition_key, source, enqueued_at, last_error
FROM public.__reflex_heal_pending WHERE imv_name = '<imv>';

SELECT reflex_heal_ignored_sources('<imv>');
```

Schedule [`reflex_scheduled_reconcile`](../api/reflex_scheduled_reconcile.md) or `reflex_heal_ignored_sources()` with pg_cron so the window stays short. A row with `last_error` set failed to heal. Fix the cause and re-run.

If `stale_reason` says the ignored source was truncated, run the `reflex_reconcile` it prints. If it says the source no longer has a column, the IMV's query refers to a column that was renamed or dropped: recreate the IMV against the current columns.

If the IMV is wrong but nothing is queued, the ignored source cannot be mapped to partitions (see [which ignored sources heal](../api/reflex_heal_ignored_sources.md#which-ignored-sources-heal)), or it predates 1.11.4 and was not backfilled. Run `SELECT reflex_rebuild_imv_metadata('<imv>');` to install the heal triggers, and `SELECT reflex_reconcile('<imv>');` to repair it now.

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
| MIN/MAX IMV with full-source seq-scan in EXPLAIN | Scoped recompute path with too many affected groups | Top-K is auto-enabled (`K=16`) since 1.4.0; the scan fires only for groups whose heap empties. Raise K (re-create with a larger `topk`) for high-churn groups, or `topk=0` to opt out if append-only. |
| Passthrough DELETE doing full refresh | No `unique_columns` and no inferable PK | Add a PK to the source, or pass `unique_columns` explicitly |
| `__reflex_intermediate_*` table much larger than expected | Aggregate state is wider than user output | Check `reflex_ivm_stats(name)` — `BOOL_OR` and `AVG` add companion columns |
| First flush after cold start is slow | Stats not analysed yet | Run `ANALYZE __reflex_intermediate_<name>` |
| Bulk load / no-op UPDATE on a reference (`product`, `location`, …) table hangs for minutes | Statement triggers are column-blind: any DML on the source fires full maintenance, even an UPDATE touching only columns no IMV reads | Exclude the source via `ignore_sources` at create time, then `reflex_reconcile` after the batch. To add it to existing IMVs: `UPDATE public.__reflex_ivm_reference SET ignored_sources = ... WHERE 'src' = ANY(depends_on)` (honored at runtime on both paths since 1.7.6). Unconditional kill switch: drop the four `__reflex_trigger_{ins,del,upd,trunc}_on_<src>` triggers. |

## DELETE on source fails with "missing FROM-clause entry"

This was a 1.0.0 bug — schema-qualified table references with column qualifiers (e.g., `alp.sales_simulation.product_id`) confused the trigger. Fixed in 1.0.1. Upgrade.

## IMV created but DELETE on the source returns the wrong row count

Passthrough IMVs require a unique key for incremental DELETE/UPDATE. Without one, DELETE on the source falls back to a full refresh, which still gets the right answer but is slow. The 1.2.1 release auto-infers the key from the source PK for single-source passthroughs; if your IMV has joins, pass `unique_columns` explicitly:

```sql
SELECT create_reflex_ivm('v', 'SELECT id, name FROM src', 'id');
```

## Flush is looping or stuck

A "stuck" flush is almost always one of three shapes. Run this first to triage:

```sql
-- Long-running flushes
SELECT pid, query_start, NOW() - query_start AS elapsed, state, query
FROM pg_stat_activity
WHERE application_name LIKE 'reflex_flush:%'
ORDER BY elapsed DESC NULLS LAST;
```

| Pattern | Cause | Fix |
|---|---|---|
| Same IMV's flush takes minutes, every time | MIN/MAX recompute hitting full source scan (no `topk` and the affected-groups filter is wider than the source) | Re-create with `topk=K`, or accept the cost as a known shape (see [limitations](../limitations/known-issues.md)) |
| Flush hangs on `pg_advisory_xact_lock` | Two sessions racing on the same `(view, source)` pair | Wait — they serialize cleanly. If wait > 30 s with no progress, kill the older session |
| `last_flush_ms` rows growing over time, with `last_error` blank | Cascade fanout — every source UPDATE triggers N IMVs | Audit `reflex_ivm_status()` for `graph_depth ≥ 4` and consider DEFERRED mode for the deep tail |

If a flush is genuinely stuck (no progress for > 5 minutes, no advisory-lock contention), the fastest recovery is:

```sql
-- Cancel the stuck statement, NOT the backend
SELECT pg_cancel_backend(<pid>);

-- Reconcile the affected IMV
SELECT reflex_rebuild_imv('<name>');
```

`pg_terminate_backend` is heavier and unnecessary here — the per-IMV SAVEPOINT means the cascade rolls back to a consistent state.

## Top-K is enabled by default for MIN/MAX

Since 2026-04-26, `create_reflex_ivm` auto-enables top-K (K=16) on every MIN/MAX intermediate column. Reflex detects MIN/MAX presence in the plan; the parameter is a no-op for SUM/COUNT/AVG/BOOL_OR. Operators on append-only MIN/MAX workloads (where retraction never happens) can opt out via the 6-arg overload:

```sql
SELECT create_reflex_ivm('append_only_v', 'SELECT grp, MAX(seen_at) FROM events GROUP BY grp',
    NULL, NULL, NULL, 0);  -- topk=0 disables
```

The earlier 1.3.0 partial-heap staleness gap on UPDATE has been fixed — UPDATEs on top-K MIN/MAX IMVs now force a scoped source-scan recompute for affected groups, so heap correctness is no longer dependent on the heap pre-state.
