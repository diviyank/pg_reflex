# `reflex_reconcile_partition`

(1.6.0+) Reconciles only the partition(s) of a partitioned IMV that cover the given keys, leaving every other partition live. The lock window collapses from the rebuild duration to the metadata DDL of an atomic `DETACH`/`ATTACH` swap.

## Signature

```sql
reflex_reconcile_partition(view_name TEXT, partition_keys TEXT, source_partition TEXT DEFAULT '')
RETURNS TEXT
```

- `partition_keys` — comma-separated list of partition key values to reconcile (e.g. `'US'`, `'2026-01,2026-02'`).
- `source_partition` — optionally names the source child to read from; defaults to resolving it from the keys. (1.10.7+) accepts a comma-separated list of source children, reconciled in one call so a dependent cascade fires once over the union rather than once per child.

## Behaviour

1. Resolves the IMV child partition(s) covering `partition_keys`.
2. Rebuilds each child *outside* the partition tree from the base/end query restricted by the child's partition constraint.
3. Flips it in via `DETACH`/`ATTACH` inside one sub-transaction — the `AccessExclusiveLock` on the parent lasts only for the metadata DDL.
4. Cascades to the IMVs that read it (1.12.0+). Each dependent is queued once per call, with the union of the scopes that reach it, and refreshed after every queued IMV it reads; a refreshed dependent queues its own dependents the same way. The scope of a dependent is:
    - **by partition** when it mirrors the partitions of the rebuilt IMV: its partitions under the rebuilt ones;
    - **by key** when it is partitioned or grouped by the rebuilt IMV's partition column and its query proves that a change of key `k` changes only its rows of key `k` (it reads the rebuilt IMV once, as a top-level relation, takes the column from it, from a table equated to it by an inner join, or as `COALESCE` over the two columns of the first join's equality, with no window, `DISTINCT ON` or set operation; level by level for a partition tree), and the rebuilt keys can be named: the values a `FOR VALUES IN (…)` bound lists, or the keys read before and after the rebuild of a partition not created or altered in the transaction. A NULL key names nothing;
    - **whole** otherwise, or when a partition or key refresh fails, with the NOTICE `pg_reflex: '<imv>' is refreshed whole, not by partition, after the partition rebuild of '<source>': <reason>`.

On a DEFERRED IMV with deltas staged earlier in the transaction (1.11.5+), the rebuilt partitions already hold them: the `COMMIT` flush skips the staged rows that fall in a rebuilt partition and applies the others. This holds when the IMV reads one source table; an IMV that joins another table is instead listed for a full rebuild at `COMMIT`, since a staged row's partition cannot be told from the row. Called from inside a trigger on a DEFERRED IMV it returns `'RECONCILE QUEUED FOR COMMIT'` without rebuilding (see [`reflex_reconcile`](reflex_reconcile.md#signatures)).

With `source_partition`, each named partition must belong to the partition tree of the source the IMV mirrors. A partition of another source the IMV reads returns `ERROR: reconcile_partition: '<p>' is not a partition of '<source>', the source whose partitions '<imv>' mirrors` and rebuilds nothing; use [`reflex_reconcile`](reflex_reconcile.md) for such an IMV. Errors are returned as text starting with `ERROR:`.

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
