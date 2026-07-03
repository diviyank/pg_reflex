# `reflex_partition_pending_status`

(1.10.8+) Read-only reporter of the partition pending-queue backlog.

## Signature

```sql
reflex_partition_pending_status()
RETURNS TABLE(source_root TEXT, enqueued_at TIMESTAMPTZ, age_seconds BIGINT, attempts INT, last_error TEXT)
```

## Behaviour

Returns one row per source root in the pending flush queue. A row with a growing `age_seconds`, non-zero `attempts`, or non-null `last_error` indicates a wedged root — a prior flush failed and subsequent partition DDL (ATTACH/DETACH/CREATE ... PARTITION OF) on that source is not retrying the flush.

Use to detect silent data-staleness caused by partition-queue wedge.

## Example

```sql
SELECT source_root, age_seconds, attempts, last_error
FROM reflex_partition_pending_status()
ORDER BY age_seconds DESC;
```

## See also

- [`reflex_flush_partitions`](reflex_flush_partitions.md) — drain the entire pending queue.
- [`reflex_flush_partition_source`](reflex_flush_partition_source.md) — manually flush one source root.
- [`reflex_doctor`](reflex_doctor.md) — diagnoses and repairs wedged roots.
