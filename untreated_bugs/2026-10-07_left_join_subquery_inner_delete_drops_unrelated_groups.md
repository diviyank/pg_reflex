# LEFT JOIN onto a subquery: a DELETE on the table inside the subquery drops unrelated groups — S1 (silent wrong result)

Pre-existing (not introduced by 1.12.0). Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400), on pg17. Found by the Task 3 adversarial reviewer
(A5), who reproduced it on head and on a base-equivalent build.

## Reproduction (pg17)

Fixture `src` as the Task 3 tests build it (`v112_kw_table`, in
`src/tests/pg_test_v112_keyword_literals.rs`):

```sql
CREATE TABLE src(id SERIAL PRIMARY KEY, g TEXT, city TEXT, v INT);
INSERT INTO src(g, city, v)
  SELECT 'g' || (i % 3), CASE WHEN i % 2 = 0 THEN 'Lyon' ELSE 'Paris' END, i
  FROM generate_series(1, 60) i;
CREATE TABLE dim(id SERIAL PRIMARY KEY, g TEXT, w INT);
INSERT INTO dim(g,w) VALUES ('g0',5),('g1',7),('g1',3),('g1',-4),('g9',1);
-- topk 0
SELECT create_reflex_ivm('v', 'SELECT s.g, MIN(d.w) AS mn, MAX(s.v) AS mx FROM src s
  LEFT JOIN (SELECT g, w FROM dim WHERE w > 0) d ON d.g = s.g GROUP BY s.g',
  NULL, NULL, NULL, 0);
DELETE FROM dim WHERE w = 3;
```

Result recorded by the reviewer: the target keeps only `g1`; `g0`, `g2` and `g9` are gone.

## Expected vs actual

Expected: after the DELETE, the IMV equals a fresh run of the query (every `src` group present,
`g1`'s `MIN` recomputed). Actual: groups unrelated to the deleted `dim` row disappear from the
target.

## What is known about the mechanism

By the reviewer's reading (not traced beyond this): the scratch fill joins all of `src` to the
old-rows subquery, so every group is null-extended and its `__ivm_count` is subtracted to zero by
the subtract MERGE. The failing statements are the scratch fill and the subtract MERGE, outside
the 1.12.0 keyword-scan diff. A left-side group key with source-side DML (`zp_3a`) was green, so
the defect is specific to a write on the table inside the subquery.

Not investigated: a `LEFT JOIN` onto a subquery with a different aggregate mix or with an
INSERT/UPDATE on the inner table; only the DELETE reproduction was recorded.

## Fix direction (hypothesis)

When a write hits a table that is only reachable through a derived table on the nullable side of
an outer join, scope the scratch fill to the affected join keys (the old rows' `g` values) rather
than joining all of the preserved side, or fall back to a scoped reconcile for the affected keys.
If that cannot be made provably correct, refuse the shape at create with a tagged message
naming the supported alternative (join the base table directly). Add oracle tests for INSERT,
UPDATE and DELETE on the inner table, grouping by the preserved side and by the subquery side.
