# Swapping a partition of a partitioned source the IMV does not mirror leaves the IMV stale, unflagged — S1 (silent wrong result)

Found by the adversarial review of the swap-cascade work (2026-10-12); present on `d0a3e06`
and unchanged by that work.

## What happens

A partitioned IMV mirrors the partition tree of one of its sources (its anchor). When a leaf of
ANOTHER partitioned source it reads is replaced (DETACH old / ATTACH new / DROP old / RENAME),
the COMMIT-time partition flush does not refresh the IMV: its rows keep the values of the
dropped leaf. `known_stale` stays false and nothing is raised.

## Reproduction

```sql
CREATE TABLE a_t (plan int NOT NULL, k int NOT NULL, w int NOT NULL, PRIMARY KEY (plan, k)) PARTITION BY LIST (plan);
CREATE TABLE a_t_p1 PARTITION OF a_t FOR VALUES IN (1);
CREATE TABLE a_t_p2 PARTITION OF a_t FOR VALUES IN (2);
CREATE TABLE a_t_def PARTITION OF a_t DEFAULT;
INSERT INTO a_t SELECT p, k, p * 10 + k FROM generate_series(1, 4) p, generate_series(1, 4) k;
CREATE TABLE x_t (region int NOT NULL, k int NOT NULL, r int NOT NULL, PRIMARY KEY (region, k)) PARTITION BY LIST (region);
CREATE TABLE x_t_r1 PARTITION OF x_t FOR VALUES IN (1);
CREATE TABLE x_t_r2 PARTITION OF x_t FOR VALUES IN (2);
INSERT INTO x_t VALUES (1, 1, 11), (1, 2, 12), (2, 3, 23), (2, 4, 24);
SELECT create_reflex_ivm('nd', 'SELECT t.plan, t.k, t.w, x.r FROM a_t AS t JOIN x_t AS x ON x.k = t.k',
                         'plan, k', NULL, 'IMMEDIATE', NULL, ARRAY['plan']);   -- mirrors a_t
-- one transaction:
CREATE TABLE x_t_r1_swap (LIKE x_t INCLUDING DEFAULTS);
INSERT INTO x_t_r1_swap SELECT region, k, r + 1000 FROM x_t_r1;
ALTER TABLE x_t DETACH PARTITION x_t_r1;
ALTER TABLE x_t ATTACH PARTITION x_t_r1_swap FOR VALUES IN (1);
DROP TABLE x_t_r1;
ALTER TABLE x_t_r1_swap RENAME TO x_t_r1;
```

After COMMIT `nd` differs from its query (16 mismatching rows in the review's run, both
IMMEDIATE and DEFERRED). The same happens when the other source is partitioned on the same
column name but joined on another column (`JOIN x2_t AS x ON x.k = t.k` with `x2_t`
LIST(plan)).

## What was ruled out

- Not the cascade to dependents: `nd` reads base tables only.
- Not the explicit primitive: `reflex_reconcile_partition('nd', '', 'x_t_r1')` returns an
  `ERROR: reconcile_partition: 'x_t_r1' is not a partition of ...` string (it cannot name the
  rows of `nd` a partition of `x_t` feeds, and says so; on `d0a3e06` the text was
  `missing target bound for child 'nd_x_t_r1'`).

## Not established

- Whether the flush calls the partition reconcile for `nd` and drops its refusal, or never
  considers `nd` because `x_t` is not its anchor.

## Fix direction

A swap of a partition of a non-anchor source changes rows of the IMV in partitions that
nothing names: the flush has to fall to a whole reconcile of that IMV (and say so), or flag it
`known_stale` when it cannot. A narrowed refresh would need the proof that the IMV's partition
key is the swapped source's, which holds for neither reproduction.
