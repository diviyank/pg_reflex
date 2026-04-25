# `reflex_ivm_status`

(1.2.0+) Returns one row per registered IMV with summary status.

## Signature

```sql
reflex_ivm_status() RETURNS TABLE(
    name TEXT,
    graph_depth INT,
    enabled BOOLEAN,
    refresh_mode TEXT,
    row_count BIGINT,        -- live SELECT count(*) on target
    last_flush_ms BIGINT,
    last_flush_rows BIGINT,
    flush_count BIGINT,
    last_error TEXT,
    last_update_date TIMESTAMP
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

`row_count` triggers a live `SELECT count(*)` per IMV. On registries with hundreds of IMVs this can take a second or two — use `reflex_ivm_stats(view_name)` for a single IMV's full picture.
