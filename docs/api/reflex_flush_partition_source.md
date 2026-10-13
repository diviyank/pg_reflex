# `reflex_flush_partition_source`

(1.6.0+) Same as [`reflex_flush_partitions`](reflex_flush_partitions.md), but resolves a single source root and skips the pending-queue scan. Use when you know exactly which partitioned source changed.

## Signature

```sql
reflex_flush_partition_source(source_root TEXT)
RETURNS TEXT
```

## Behaviour

Oid-diffs `source_root`'s current partition tree against the stored snapshot and swap-fills / creates / drops the matching IMV partitions (cascading to dependents), without touching pending entries for any other source.

This is the call the COMMIT-time partition flush makes, once per root and COMMIT. A root whose `failures` reached `5` is skipped; transient lock errors are not counted. See [Failures](reflex_flush_partitions.md#failures).

## Example

```sql
SELECT reflex_flush_partition_source('sales');
```

## See also

- [`reflex_flush_partitions`](reflex_flush_partitions.md) — drain the whole pending queue.
