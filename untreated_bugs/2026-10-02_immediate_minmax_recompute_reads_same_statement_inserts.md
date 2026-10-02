# IMMEDIATE MIN / MAX recompute reads the same statement's later rows — S1 (SILENT, pre-existing)

Found in r5 fix round 1 while checking every IMMEDIATE path that reads the source from inside a
statement trigger (the class fixed in 1.11.5 for the rebuild dispatch: one statement fires several
statement triggers, each seeing the source as the WHOLE statement left it).

## Mechanism (by reading; reproduced below)
The aggregate UPDATE arm with MIN / MAX (`aggregate_update_stmts`, src/trigger/ops.rs) subtracts the
old images, then recomputes the scalar / top-K heap of the affected groups from the live source
(`build_min_max_recompute_sql*`, `orig_base_query`), then adds the new images. In an upsert the
AFTER UPDATE trigger fires first, so that recompute already sees the rows the statement INSERTed;
the AFTER INSERT trigger then merges them into the groups again. MIN / MAX of the group is right
at that point (LEAST / GREATEST are idempotent), but the stored top-K heap no longer matches
the source, and a later DELETE of those rows leaves a wrong MIN / MAX.

## Reproduction (pg17, branch sdd/r5-update-dirty)
```sql
CREATE TABLE zmm (id INT PRIMARY KEY, g INT, v INT);
INSERT INTO zmm SELECT i, i % 5, i FROM generate_series(1, 100) i;
SELECT create_reflex_ivm('zmm_v', 'SELECT g, MIN(v) AS mn, MAX(v) AS mx, COUNT(*) AS n FROM zmm GROUP BY g');
INSERT INTO zmm SELECT i, i % 5, CASE WHEN i <= 100 THEN i + 1000 ELSE i - 500 END
  FROM generate_series(1, 120) i ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;   -- IMV correct here
DELETE FROM zmm WHERE id > 100;                                                   -- 10 mismatched rows
```
The same changes as two statements (UPDATE, then INSERT) stay correct. DEFERRED IMVs are not
affected (the flush nets the transaction into one UPDATE call).

## Fix directions
- Scope the recompute to the source as of this trigger's change: exclude the rows of the
  statement's other transition tables (not reachable from the UPDATE trigger), or
- defer MIN / MAX recompute of IMMEDIATE IMVs to a statement-end step, or
- recompute in the later trigger too (make the INSERT arm recompute touched MIN / MAX groups when
  the same statement already recomputed them — needs a statement-scoped marker).
