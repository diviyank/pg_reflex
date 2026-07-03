# `reflex_ivm_status`

(1.2.0+) Returns one row per registered IMV with summary status.

## Signature

```sql
reflex_ivm_status() RETURNS TABLE(
    name TEXT,
    graph_depth INT,
    enabled BOOLEAN,
    refresh_mode TEXT,
    row_count BIGINT,        -- planner estimate (reltuples) for analyzed targets; exact count for empty/unanalyzed
    last_flush_ms BIGINT,
    last_flush_rows BIGINT,
    flush_count BIGINT,
    last_error TEXT,
    last_update_date TIMESTAMP,
    known_stale BOOLEAN,     -- durable health flag, set on any caught failure
    stale_reason TEXT        -- captured error if known_stale = true
)
```

## Example

```sql
SELECT name, graph_depth, last_flush_ms, flush_count, last_error
FROM reflex_ivm_status()
ORDER BY graph_depth, last_flush_ms DESC NULLS LAST;
```

## Filtering broken IMVs

```sql
SELECT name, last_error
FROM reflex_ivm_status()
WHERE last_error IS NOT NULL;
```

## Check for known stale IMVs

```sql
SELECT name, known_stale, stale_reason
FROM reflex_ivm_status()
WHERE known_stale;
```

Returns any IMV flagged with a durable stale marker — set on a caught cascade/flush failure and cleared on successful reconcile.

## Notes

`row_count` reports the planner estimate `pg_class.reltuples` for an analyzed target (O(1), no scan), and falls back to an exact `count(*)` only when the estimate is unavailable (an empty or never-analyzed target, where the count is cheap). It is therefore approximate for large tables and exact for small/empty ones — accuracy tracks the target's last `ANALYZE`/autovacuum. (Before 1.10.8 this was always an exact `count(*)`, which full-scanned every target and made the status view slow on large registries.) Use `reflex_ivm_stats(view_name)` for a single IMV's full picture.
