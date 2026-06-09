# `reflex_set_wipe_floor_rows`

(1.6.0+) Sets or clears the per-IMV `wipe_floor_rows` override — a floor on the partition-size denominator of the dirty/size ratio, so a tiny or never-`ANALYZE`d partition cannot trip a wipe on a single dirty row.

## Signature

```sql
reflex_set_wipe_floor_rows(view_name TEXT, value BIGINT)
RETURNS TEXT
```

Pass `value = NULL` to clear the override.

## Why it exists

The wipe decision compares `dirty / GREATEST(reltuples, wipe_floor_rows)` against the [wipe threshold](reflex_set_wipe_threshold.md). A brand-new or never-`ANALYZE`d partition reports `reltuples = 0`, which would otherwise make any single dirty row a 100%-dirty batch and force a needless full rebuild. The floor keeps the denominator sane.

## Resolution order

1. The per-IMV `wipe_floor_rows` column (set here).
2. The session GUC [`reflex.wipe_floor_rows`](gucs.md).
3. The compiled default (`1000`).

## Example

```sql
SELECT reflex_set_wipe_floor_rows('sales_by_region', 5000);
```
