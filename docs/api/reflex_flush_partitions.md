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

The same flush runs by itself at the COMMIT of a transaction that creates, attaches or detaches a partition of a source: the deferred trigger on `__reflex_partition_pending` calls `reflex_flush_partition_source` for the root, once per COMMIT however many partition DDL commands the transaction ran (1.12.0+; before, once per command). Called without an argument, this function reads every pending root, of every schema, so it can wait behind another schema's partition DDL; the COMMIT-time flush and `reflex_flush_partition_source` keep to one root.

## Failures

Each root is flushed in its own block. When it fails, its work is rolled back, the root stays queued with the error in `last_error`, every IMV that reads it is flagged `known_stale` with that error, and a `WARNING` is raised: `pg_reflex: partition flush for root <root> failed: <error> (SQLSTATE <code>) — left pending for retry`. The other roots go on.

- A deadlock (`40P01`), serialization failure (`40001`) or lock timeout (`55P03`) is not counted (1.12.0+): the root is retried at the next partition DDL COMMIT on the source or the next manual flush, without a cap.
- Any other error adds 1 to `failures`. A root at `5` is skipped by every flush, with a `WARNING`, until [`reflex_reset_partition_failures('<root>')`](../operations/runbook.md#imv-stopped-updating-capped-partition-source) re-arms it. Fix the cause first.

A root drained by a later successful flush leaves the queue; some of its IMVs can stay flagged `known_stale` until `reflex_reconcile('<imv>')`.

The 1.12.0 flush calls `public.__reflex_reads_relation`, which `ALTER EXTENSION pg_reflex UPDATE TO '1.12.0'` creates: with the 1.12.0 library and a 1.11.5 catalog, the flush raises `function public.__reflex_reads_relation(text[], text) does not exist`.

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
