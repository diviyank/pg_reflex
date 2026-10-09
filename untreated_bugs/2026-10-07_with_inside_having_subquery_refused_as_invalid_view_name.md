# A WITH inside a HAVING subquery is refused with "Invalid view name" — S3 (loud, misleading message)

Pre-existing: same refusal on commit 8ef9743 and on the `fix/fold-g2-having` head, pg16. Found by
the adversarial review of the HAVING fold (its P5); not re-run when this report was written.

## Reproduction

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT);
CREATE TABLE o(id SERIAL PRIMARY KEY, lg TEXT, k INT);
SELECT create_reflex_ivm('h',
  'SELECT g, SUM(v) AS s FROM s GROUP BY g
   HAVING SUM(v) > (WITH w AS (SELECT k FROM o) SELECT COUNT(*) FROM w)');
-- Invalid view name: only alphanumeric, underscore, and period allowed
```

## Expected vs actual

Expected: an IMV, or a rejection that names the WITH inside the subquery. Actual: a refusal about
the view name, which is valid (`h`); nothing is created.

## What is established and what is not

Established: the message and parity. The same subquery without the WITH
(`(SELECT COUNT(*) FROM o)`) is created and correct. Not established: the cause. The message is
the one a generated sub-IMV name fails with, so the CTE decomposition probably picks up the nested
WITH and derives a name from it; not traced.

## Fix direction (hypothesis to falsify)

Keep the CTE decomposition to the WITH of the outer query, or reject a WITH nested in a subquery
with a tagged message whose remedy is to move it to the top level.
