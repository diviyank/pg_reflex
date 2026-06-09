# `reflex_set_wipe_threshold`

(1.4.6+) Sets or clears the per-IMV `wipe_threshold` override — the dirty-row fraction at or above which a maintenance batch wipes-and-rebuilds the (partition of the) IMV instead of applying a row-by-row delta.

## Signature

```sql
reflex_set_wipe_threshold(view_name TEXT, value NUMERIC)
RETURNS TEXT
```

Pass `value = NULL` to clear the override.

## Resolution order

The dispatch block emitted by the trigger resolves the threshold in this order:

1. The per-IMV `wipe_threshold` column (set here).
2. The session GUC [`reflex.wipe_threshold`](gucs.md).
3. The compiled default (`0.5`).

A higher value makes the IMV prefer incremental deltas for longer; a lower value flips to a full rebuild sooner. Pair it with [`reflex_set_wipe_floor_rows`](reflex_set_wipe_floor_rows.md) to guard against small/un-analyzed partitions.

## Examples

```sql
SELECT reflex_set_wipe_threshold('sales_by_region', 0.4);  -- wipe when >40% dirty
SELECT reflex_set_wipe_threshold('sales_by_region', NULL); -- back to GUC / default
```
