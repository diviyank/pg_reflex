# A partition-scoped rebuild of a DEFERRED IMV in a transaction that staged deltas for it double-applies them at COMMIT — S1 (silent wrong aggregate)

Pre-existing (not introduced by 1.11.5). Found while fixing the whole-IMV case
(`reflex_reconcile` of a DEFERRED IMV now records a rebuild watermark, so the flush skips
the deltas staged before it; see `record_rebuild_watermark`, src/trigger/deferred.rs).

## Mechanism

`reflex_reconcile_partition` (src/partition.rs `reflex_reconcile_partition_impl`) and the
key-scoped cascade (`build_scoped_cascade_reconcile`) rebuild only some partitions / keys of
the IMV from the base tables, which already hold the transaction's writes. Neither records a
watermark, so the COMMIT flush applies the staged delta for the rebuilt keys a second time.
Recording the whole-IMV watermark would be wrong: it would also skip the staged deltas of the
keys the scoped rebuild did not touch.

Reached explicitly, by the hot-partition dispatch (`reflex_reconcile_partition(view, hot_keys)`),
by the partition-pending flush, and by the scoped cascade to a same-column or GROUP-BY-key
dependent.

## Reproduction (pg17)

```sql
CREATE TABLE zs (plan INT NOT NULL, id INT NOT NULL, qty INT) PARTITION BY LIST (plan);
CREATE TABLE zs1 PARTITION OF zs FOR VALUES IN (1);
CREATE TABLE zs2 PARTITION OF zs FOR VALUES IN (2);
INSERT INTO zs SELECT p, g, g FROM generate_series(1, 30) g, (VALUES (1), (2)) v(p);
SELECT create_reflex_ivm('zv', 'SELECT plan, SUM(qty) AS q, COUNT(*) AS n FROM zs GROUP BY plan',
  NULL, NULL, 'DEFERRED', NULL, ARRAY['plan']);
UPDATE __reflex_ivm_reference SET wipe_threshold = 1e9 WHERE name = 'zv';
BEGIN;
INSERT INTO zs VALUES (2, 100, 1000);
SELECT reflex_reconcile_partition('zv', '2');
COMMIT;
-- zv plan 2: n and q count the inserted row twice; known_stale = FALSE
```

## Fix direction

Certainly correct: when a scoped rebuild targets a DEFERRED IMV that has rows staged by this
transaction on a source it observes (or runs inside a trigger, where the current statement's
delta may not be staged yet), list it for the COMMIT-time full reconcile
(`list_for_commit_reconcile`). That costs a full rebuild of a possibly very large IMV at COMMIT,
so measure first; the precise alternative is a key-scoped watermark (skip only staged rows
whose partition key is in the rebuilt set).
