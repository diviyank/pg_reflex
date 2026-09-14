# `reflex_ivm_status`

(1.2.0+) Returns one row per registered IMV with summary status.

## Signature

```sql
reflex_ivm_status() RETURNS TABLE(
    name TEXT,
    graph_depth INT,
    enabled BOOLEAN,
    refresh_mode TEXT,
    row_count BIGINT,                   -- estimate or exact count; see is_estimate
    last_flush_ms BIGINT,
    last_flush_rows BIGINT,
    flush_count BIGINT,
    last_error TEXT,
    last_update_date TIMESTAMP,
    known_stale BOOLEAN,                -- the IMV is known not to reflect its sources
    stale_reason TEXT,                  -- why, and the command that repairs it
    requires_explicit_refresh BOOLEAN,  -- 1.11.0+
    rebuild_count BIGINT,               -- 1.11.1+
    last_rebuild_at TIMESTAMPTZ,        -- 1.11.1+
    is_estimate BOOLEAN                 -- 1.11.4+: TRUE when row_count is the planner estimate
)
```

## Example

```sql
SELECT name, graph_depth, last_flush_ms, flush_count, last_error
FROM reflex_ivm_status()
ORDER BY graph_depth, last_flush_ms DESC NULLS LAST;
```

## Check for stale IMVs

```sql
SELECT name, stale_reason
FROM reflex_ivm_status()
WHERE known_stale;
```

`known_stale` is `TRUE` when either:

- **a maintenance failure was caught for this IMV** — a failed deferred flush (under the default [`pg_reflex.flush_failure_policy`](gucs.md#pg_reflexflush_failure_policy) = `warn`), a failed partition flush, auto-sync or cross-source reconcile. This is stored in the registry and cleared by a successful `reflex_reconcile`.
- **(1.11.4+) a partition source this IMV depends on — directly or through other IMVs — is capped.** A source root that failed `5` consecutive partition flushes is skipped by every later flush, so no change to it reaches its IMVs. This is derived live from `__reflex_partition_pending`, not stored, so a reconcile cannot hide it while the root stays capped. `stale_reason` names the root and its last error and prescribes:

    ```sql
    SELECT reflex_reset_partition_failures('<root>');
    SELECT reflex_flush_partition_source('<root>');
    ```

    The report clears as soon as the root drains. Fix the root cause first (for a duplicate-key error, the duplicate source rows) — a failed retry re-caps it.

## Row count

`row_count` reports the planner estimate `pg_class.reltuples` for an analyzed target (O(1), no scan) and `is_estimate = TRUE`. It falls back to an exact `count(*)` with `is_estimate = FALSE` when the estimate is unavailable (an empty or never-analyzed target) or when the IMV carries an anomaly: `known_stale`, a retained `last_error`, or a slice-changing rebuild logged in `__reflex_event_log` more recently than the target's last `ANALYZE`. An estimate taken before a wipe keeps reporting the pre-wipe size, so an anomalous IMV is always counted exactly.

Before 1.10.8 `row_count` was always an exact `count(*)`. Use [`reflex_ivm_stats(view_name)`](reflex_ivm_stats.md) for a single IMV's full picture.

## Maintenance event log

(1.11.4+) `public.__reflex_event_log` keeps a durable row for every caught flush failure (`event = 'error'`, with `sqlstate`) and every rebuild that changed a slice's row count (`event = 'rebuild'`, with `slice`, `rows_before`, `rows_after`):

```sql
SELECT at, event, trigger_reason, slice, rows_before, rows_after, detail
FROM public.__reflex_event_log
WHERE imv_name = 'omc.sop_forecast_view'
ORDER BY at DESC
LIMIT 20;
```

Prune with [`reflex_prune_event_log`](reflex_prune_event_log.md).
