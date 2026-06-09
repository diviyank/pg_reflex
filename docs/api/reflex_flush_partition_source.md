# `reflex_flush_partition_source`

(1.6.0+) Same as [`reflex_flush_partitions`](reflex_flush_partitions.md), but resolves a single source root and skips the pending-queue scan. Use when you know exactly which partitioned source changed.

## Signature

```sql
reflex_flush_partition_source(source_root TEXT)
RETURNS TEXT
```

## Behaviour

Oid-diffs `source_root`'s current partition tree against the stored snapshot and swap-fills / creates / drops the matching IMV partitions (cascading to dependents), without touching pending entries for any other source.

## Example

```sql
SELECT reflex_flush_partition_source('sales');
```

## See also

- [`reflex_flush_partitions`](reflex_flush_partitions.md) — drain the whole pending queue.
