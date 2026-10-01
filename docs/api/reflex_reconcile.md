# `reflex_reconcile` and aliases

Rebuilds the intermediate + target tables from source data. Use as a safety net after a crash, after manual edits to the registry, or when an IMV's `last_error` indicates drift.

## Signatures

```sql
reflex_reconcile(view_name TEXT) RETURNS TEXT
reflex_rebuild_imv(view_name TEXT) RETURNS TEXT  -- alias since 1.2.0
refresh_reflex_imv(view_name TEXT) RETURNS TEXT  -- alias since 1.0.x
```

All three return `'RECONCILED'`, an `'ERROR: …'` string when the IMV cannot
be rebuilt (not found, disabled, a decomposed wrapper node, a failed partition
rebuild), or (1.11.5+) `'RECONCILE QUEUED FOR COMMIT'`.

`RECONCILE QUEUED FOR COMMIT`: called from inside a trigger on a DEFERRED IMV
that observes a source, the reconcile does not rebuild now. The trigger's own
statement may still stage a delta for the IMV, which the flush would apply on
top of a rebuild that already read the write. The IMV is listed for a full
rebuild at `COMMIT`, after the statement has staged it, and that rebuild
cascades to its dependents. [`reflex_reconcile_partition`](reflex_reconcile_partition.md)
returns the same value in the same case. A disabled IMV is refused
(`'ERROR: IMV not found or disabled'`), never queued.

Outside a trigger, a reconcile of a DEFERRED IMV records the point of its
rebuild: the deltas staged for it earlier in the transaction are in the
rebuild and skipped by the `COMMIT` flush, later ones are applied.

Known limit: a single statement that both writes a source of a DEFERRED IMV
in a data-modifying CTE and calls `reflex_reconcile` on that IMV (e.g.
`WITH w AS (INSERT INTO src …) SELECT reflex_reconcile('imv')`) applies the
CTE's write twice: the reconcile reads it, and its delta is staged after the
reconcile, at the end of the statement.

## Behaviour

**`reflex_rebuild_imv` (alias) is anchor-scoped:** it re-derives every child of the *anchor* source only, and does **not** fill partition keys fed only by a source listed in `ignore_sources`. For that case, use [`reflex_reconcile_partition`](reflex_reconcile_partition.md) or [`reflex_doctor`](reflex_doctor.md) with the `archive_residue` check.

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
