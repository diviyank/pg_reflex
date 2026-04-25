# `reflex_reconcile` and aliases

Rebuilds the intermediate + target tables from source data. Use as a safety net after a crash, after manual edits to the registry, or when an IMV's `last_error` indicates drift.

## Signatures

```sql
reflex_reconcile(view_name TEXT) RETURNS TEXT
reflex_rebuild_imv(view_name TEXT) RETURNS TEXT  -- alias since 1.2.0
refresh_reflex_imv(view_name TEXT) RETURNS TEXT  -- alias since 1.0.x
```

All three return `'RECONCILED'`.

## Behaviour

For aggregate IMVs:

1. Drop all reflex-managed indexes on the intermediate table.
2. `TRUNCATE` intermediate + target.
3. `INSERT … <base_query>` — bulk-rebuild from source.
4. Recreate the indexes.
5. `INSERT … <end_query>` — populate target from intermediate.
6. `ANALYZE` both.

For passthrough IMVs:

1. Save user-created indexes (the IMV's own + any manual ones).
2. `DROP INDEX` all of them.
3. `TRUNCATE` target, `INSERT … <base_query>`.
4. Recreate the saved indexes.
5. `ANALYZE`.

## Refresh all dependents of a source

```sql
refresh_imv_depending_on(source TEXT) RETURNS TEXT
```

Refreshes every IMV whose `depends_on` includes `source`, in `graph_depth` order. Useful after a bulk load with triggers disabled, or after refreshing a `MATERIALIZED VIEW` that's a source for IMVs.

```sql
SELECT refresh_imv_depending_on('orders');
-- REFRESHED 4 IMVs
```
