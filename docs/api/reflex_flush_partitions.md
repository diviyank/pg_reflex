# `reflex_flush_partitions`

(1.6.0+) Resolves pending source-partition changes queued by `ATTACH`/`DETACH` swaps, then swap-fills, creates, or drops the matching IMV partitions. Drains the `__reflex_partition_pending` queue.

## Signature

```sql
reflex_flush_partitions()
RETURNS TEXT
```

## Behaviour

1. Reads each dirty source root recorded in `__reflex_partition_pending`.
2. Oid-diffs the source's current partition tree against the stored snapshot.
3. Swap-fills / creates / drops the matching IMV partitions to match, cascading to dependents.
4. Clears the pending queue.

Call after a batch of source `DETACH`/`ATTACH` swaps to propagate them to the IMVs in one pass. To flush a single known source root without scanning the queue, use [`reflex_flush_partition_source`](reflex_flush_partition_source.md).

## Example

```sql
-- After re-partitioning the source
ALTER TABLE sales DETACH PARTITION sales_2024;
ALTER TABLE sales ATTACH PARTITION sales_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

SELECT reflex_flush_partitions();
```

## See also

- [`reflex_flush_partition_source`](reflex_flush_partition_source.md) — flush one source root.
- [`reflex_sync_partitions`](reflex_sync_partitions.md) — structural sync for a single IMV.
