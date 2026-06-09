# `reflex_rebuild_imv_metadata`

(1.4.5+) Re-analyzes an IMV's stored `base_query` and merges the freshly computed `imv_relevant_columns` / `imv_relevant_where` maps back into its `aggregations` JSON. Idempotent.

## Signature

```sql
reflex_rebuild_imv_metadata(view_name TEXT)
RETURNS TEXT
```

## When to use

This is primarily a migration helper — the 1.4.4→1.4.5 migration used it to backfill the static analysis the filter-aware spurious-skip relies on. It is also useful after an analyzer change shifts what falls into either map. It only rewrites metadata; it does not touch IMV data or triggers (for trigger bodies, see [`reflex_rebuild_triggers`](reflex_rebuild_triggers.md)).

## Example

```sql
SELECT reflex_rebuild_imv_metadata('sales_by_region');
```
