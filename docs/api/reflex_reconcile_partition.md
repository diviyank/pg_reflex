# `reflex_reconcile_partition`

(1.6.0+) Reconciles only the partition(s) of a partitioned IMV that cover the given keys, leaving every other partition live. The lock window collapses from the rebuild duration to the metadata DDL of an atomic `DETACH`/`ATTACH` swap.

## Signature

```sql
reflex_reconcile_partition(view_name TEXT, partition_keys TEXT, source_partition TEXT DEFAULT '')
RETURNS TEXT
```

- `partition_keys` — comma-separated list of partition key values to reconcile (e.g. `'US'`, `'2026-01,2026-02'`).
- `source_partition` — optionally names the source child to read from; defaults to resolving it from the keys.

## Behaviour

1. Resolves the IMV child partition(s) covering `partition_keys`.
2. Rebuilds each child *outside* the partition tree from the base/end query restricted by the child's partition constraint.
3. Flips it in via `DETACH`/`ATTACH` inside one sub-transaction — the `AccessExclusiveLock` on the parent lasts only for the metadata DDL.
4. Cascades to dependent IMVs partitioned on the same column with the same keys; otherwise falls back to a full `reflex_reconcile` of the dependent.

Only valid on partitioned IMVs (see [`create_reflex_ivm`](create_reflex_ivm.md) `partition_by`). On an unpartitioned IMV, use [`reflex_reconcile`](reflex_reconcile.md).

## Examples

```sql
-- LIST: reconcile one region
SELECT reflex_reconcile_partition('sales_by_region', 'US');

-- RANGE: reconcile two month buckets at once
SELECT reflex_reconcile_partition('sales_by_month', '2026-01,2026-02');
```

## See also

- [`reflex_sync_partitions`](reflex_sync_partitions.md) — reconcile partition *structure* (not data).
- [`reflex_flush_partitions`](reflex_flush_partitions.md) — propagate pending source `ATTACH`/`DETACH` swaps.
