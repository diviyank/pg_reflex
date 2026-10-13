# A partitioned IMV whose partition column comes from a joined table loses rows when the anchor's partition goes — S1 (SILENT wrong result)

Pre-existing: reproduced on `7bd2b64` (1.12.0 integration, before the swap cascade work) and on
`fix/v112-swap-cascade-scope` (`317f8ba`), PostgreSQL 17 and 16. Found by the reviewer's corpus
(`zx_map`, `zc_agg_of_x`, step "22 plan dropped") in round 3 of the swap cascade fix.

## Reproduction (every statement committed, IMMEDIATE)

```sql
CREATE TABLE zs (plan INT NOT NULL, k INT NOT NULL, q INT) PARTITION BY LIST (plan);
CREATE TABLE zs_p1 PARTITION OF zs FOR VALUES IN (1);
CREATE TABLE zs_p2 PARTITION OF zs FOR VALUES IN (2);
CREATE TABLE zs_p3 PARTITION OF zs FOR VALUES IN (3);
INSERT INTO zs SELECT p, k, p * 10 + k FROM generate_series(1, 3) p, generate_series(1, 4) k;
CREATE TABLE zmap (src_plan INT NOT NULL, plan INT NOT NULL, PRIMARY KEY (src_plan, plan));
INSERT INTO zmap SELECT 1, p FROM generate_series(1, 3) p;   -- plan 1 feeds plans 1, 2, 3
SELECT create_reflex_ivm('zv', 'SELECT s.plan, s.k, s.q FROM zs AS s', 'plan, k', NULL,
                         'IMMEDIATE', NULL, ARRAY['plan']);
SELECT create_reflex_ivm('zx_map',
  'SELECT m.plan, v.k, v.q FROM zv AS v JOIN zmap AS m ON m.src_plan = v.plan',
  'plan, k', NULL, 'IMMEDIATE', '!zv', ARRAY['plan']);
ALTER TABLE zs DETACH PARTITION zs_p3; DROP TABLE zs_p3;
SELECT reflex_reconcile('zx_map');   -- returns RECONCILED
```

## Expected vs actual

Expected: `zx_map` keeps the four rows of plan 3, which come from plan 1 of `zs` through `zmap`.
Actual: `reflex_reconcile('zx_map')` returns `RECONCILED` and `zx_map` lacks them (bidirectional
`EXCEPT ALL`: 4). Every later reconcile gives the same result; nothing is flagged.

- On `7bd2b64` the drop itself leaves `zx_map` correct (0): the flush keeps `zx_map`'s plan-3
  partition as an orphan ("preserved (drop_orphans=false)") and does not refresh `zx_map`, which
  ignores `zv`. The first reconcile then loses the rows.
- On `fix/v112-swap-cascade-scope` a dropped partition refreshes the readers that ignore the IMV
  it came from, so `zx_map` is reconciled at the drop and is wrong right after it (4), with the
  readers of `zx_map` (`zc_agg_of_x` in the corpus) following.

## Mechanism (from the logs, not traced in code)

`zx_map` is partitioned by `plan` and its partition tree mirrors the tree of the source it reads,
`zv` (itself mirroring `zs`). But its `plan` is `zmap.plan`, not `zv.plan`. When `zs_p3` goes, the
reconcile's sync with `drop_orphans` drops `zx_map`'s plan-3 partition as an orphan, and the rebuild
has no partition to put plan-3 rows in (no DEFAULT), so they are dropped without an error.

## Fix direction (hypothesis to falsify)

Either refuse at create a partitioned IMV whose partition column is not proven to be its anchor's
partition column (the cascade's own proof, `unproven_partition_alignment`, already tells these
apart), or keep such an IMV's partitions by the values it holds rather than by the anchor's tree.
At minimum, a rebuild that has rows for no partition must raise or flag the IMV stale, not return
`RECONCILED`.
