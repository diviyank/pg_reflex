# `reflex_refresh_partition_snapshot_if_diverged`

(1.10.8+) Oid-diffs the stored partition snapshot against the live leaf set for a source and heals divergence on-demand.

## Signature

```sql
reflex_refresh_partition_snapshot_if_diverged(source_root TEXT) RETURNS TEXT
```

## Behaviour

Compares `__reflex_source_partition_snapshot` (stored on initial setup or last sync) against the current partition tree of `source_root`. If oids match (no divergence), returns `'OK (no divergence)'`. If oids differ, rebuilds the snapshot to match the live leaf set and returns `'HEALED (<n> divergent leaves)'`.

Use after the pg_reflex extension was installed on pre-existing partitioned sources, or when a partition DDL event was missed (e.g., a concurrent `ATTACH` in another session that fired before the extension was loaded).

## Example

```sql
SELECT reflex_refresh_partition_snapshot_if_diverged('sales');
-- HEALED (12 divergent leaves)
```

## See also

- [`reflex_flush_partitions`](reflex_flush_partitions.md) — drains partition DDL pending queue.
- [`reflex_sync_partitions`](reflex_sync_partitions.md) — reconciles IMV partition *structure*.
- [`reflex_doctor`](reflex_doctor.md) — diagnoses snapshot divergence and calls this as part of repair.
