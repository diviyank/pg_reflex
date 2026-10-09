# A WHERE subquery over `unnest(<source column>)` is not re-evaluated when that column is updated — S1 (SILENT wrong result)

Pre-existing. Found on pg15.19 while probing set-returning functions in FROM on the 1.12.0
branch (fix/fold-g5-classify). Not fixed there: the rejection added on that branch covers a
function in the top-level FROM only, and this shape reads the column inside an expression
subquery.

## Reproduction (pg15.19)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, arr INT[]);
INSERT INTO t(g, arr) VALUES ('a','{1,2}'),('b','{3}'),('c','{}');
SELECT create_reflex_ivm('x',
  'SELECT id, g FROM t WHERE EXISTS (SELECT 1 FROM unnest(arr) AS e WHERE e = 5)');
INSERT INTO t(g, arr) VALUES ('d','{7,8,9}'),('e',NULL);   -- IMV = query
UPDATE t SET arr = arr || 5 WHERE g = 'a';
-- the query now returns row 'a'; the IMV does not (1 row differs)
DELETE FROM t WHERE g = 'b';                                -- still 1 row differs
DELETE FROM t;                                              -- IMV = query again
```

Probe output (`create=0 step0=0 step1=1 step2=1 step3=0 step4=0`, counts are rows of the
bidirectional EXCEPT ALL between the IMV and its query).

## Expected vs actual

Expected: the UPDATE of `arr` moves the row into the IMV. Actual: the IMV is unchanged.

## What is known about the mechanism

Not investigated beyond the shape. `arr` is referenced only inside the WHERE subquery; the
UPDATE-skip inputs (`imv_relevant_columns`, `imv_relevant_where` in `src/sql_analyzer.rs`) are
built from the outer references, and a WHERE conjunct with no outer reference is dropped, so an
UPDATE that changes only `arr` may be judged irrelevant. Hypothesis, not verified. If it holds,
any correlated subquery in WHERE whose correlation column appears nowhere else is affected, not
only `unnest`.

## Fix direction (hypothesis to falsify)

Count the outer columns a WHERE subquery reads as IMV-relevant for the UPDATE skip (or disable
the skip when WHERE holds a correlated subquery). Reproduce first with a correlated subquery over
a second table to see whether the function is incidental.
