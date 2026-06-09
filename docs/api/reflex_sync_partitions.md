# `reflex_sync_partitions`

(1.6.0+) Reconciles the *partition structure* of a partitioned IMV against its source — creating IMV child partitions for source partitions that lack one, and optionally dropping orphans. Does not touch row data.

## Signature

```sql
reflex_sync_partitions(view_name TEXT, drop_orphans BOOLEAN DEFAULT TRUE)
RETURNS TEXT
```

## Behaviour

1. Compares the source's partition children against the IMV's.
2. Creates an IMV child for every source partition missing one.
3. When `drop_orphans = true`, removes IMV partitions whose source counterpart was dropped via `DROP TABLE ... CASCADE` (touches only pg_reflex-owned objects). When `false`, orphans are preserved and a `NOTICE` is emitted.

Idempotent and advisory-lock protected, so duplicate fires inside one transaction collapse harmlessly. A no-op on unpartitioned IMVs.

## Automatic invocation

After a source `ATTACH PARTITION` or `CREATE TABLE ... PARTITION OF`, the `reflex_on_ddl_command_end` event trigger calls `reflex_sync_partitions(view, drop_orphans => FALSE)` for every partitioned IMV depending on that source — so new partitions appear automatically. Orphan deletion is never automatic (a source `DETACH` is not a delete signal); call this manually with `drop_orphans => TRUE` to prune them.

## Examples

```sql
-- Create missing children and drop orphans
SELECT reflex_sync_partitions('sales_by_region');

-- Create missing children, preserve orphaned IMV partitions
SELECT reflex_sync_partitions('sales_by_region', false);
```

## See also

- [`reflex_reconcile_partition`](reflex_reconcile_partition.md) — rebuild a single partition's *data*.
- [Event triggers](event-triggers.md) — the auto-sync surface.
