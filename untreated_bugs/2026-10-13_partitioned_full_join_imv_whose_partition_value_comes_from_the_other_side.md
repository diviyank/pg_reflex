# A partitioned FULL JOIN IMV whose partition value comes from the non-partitioned side: IMMEDIATE fails the user's write, DEFERRED goes silently stale — S2

Pre-existing on 1.12.0 (main `339152c`); found by the adversarial review of the 1.12.1 FULL JOIN
delta scoping (`p2-review.md`, F4 in `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`),
reproduced on main and on the branch alike (PostgreSQL 16 and 17).

## Reproduction

```sql
CREATE TABLE f(k INT, plan INT, v INT) PARTITION BY LIST (plan);
CREATE TABLE f_1 PARTITION OF f FOR VALUES IN (1);
CREATE TABLE f_2 PARTITION OF f FOR VALUES IN (2);
CREATE TABLE f_3 PARTITION OF f FOR VALUES IN (3);
CREATE TABLE h(k INT, plan INT, w INT);
-- rows on both sides, plans 1..3
SELECT create_reflex_ivm('pv',
  'SELECT COALESCE(f.k, h.k) AS k, COALESCE(f.plan, h.plan) AS plan, f.v, h.w
     FROM f FULL JOIN h ON f.k = h.k AND f.plan = h.plan',
  'k, plan', 'UNLOGGED', '<mode>', NULL, ARRAY['plan']);
UPDATE h SET plan = 4 WHERE k = 1;   -- no partition of pv holds plan 4
```

- IMMEDIATE: `ERROR: no partition of relation "…pv…" found for row` — loud, but the user's own
  UPDATE of `h` fails.
- DEFERRED: the COMMIT succeeds and `pv` differs from its query by 2 rows (bidirectional
  `EXCEPT ALL`), with no error, no WARNING and no `known_stale`.

## Cause (hypothesis)

The IMV mirrors the partitions of `f`, but its partition column is `COALESCE(f.plan, h.plan)`:
a null-extended row of `h` can carry a value no partition of `f` (so no mirror partition) holds.
The DEFERRED flush's error is swallowed somewhere between the insert and the per-IMV handler.
Neither point is established in code.

## Fix direction

Either refuse at create a partitioned IMV whose partition column can take a value from a
non-partitioned side (COALESCE over a FULL / RIGHT JOIN), or give such an IMV a DEFAULT partition.
Separately, find why the DEFERRED path ends silent instead of flagging the IMV `known_stale`.
