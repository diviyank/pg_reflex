# SUM(v + 1) and SUM(v - 1) in one query collide on the intermediate column name and give wrong data — S1 (SILENT wrong result)

Pre-existing: same on the 1.11.5 base and on the 1.12.0 branch. Found by the Task 4b reviewer
(adjacent A3); listed among the "five more pre-existing bugs".

## Reproduction

The review gives the query shape and result, no full script:

```sql
SELECT SUM(v + 1) AS a, SUM(v - 1) AS b ...   -- table and group by not recorded
-- 6 mismatches at create
```

## Expected vs actual

Expected: IMV equals the query. Actual: wrong data (6 mismatches at create). The review's stated
reason: "both arguments sanitise to the same intermediate column name".

## What is established and what is not

Established: the mismatch count and the reviewer's stated reason. Not established: whether the
same collision affects other aggregates or other operator pairs; the report does not say. A
related, harmless case noted by the Task 4b author: `COUNT(*)` used to be planned as `__count_`
(the sanitised `*`), which is why a second counter column could appear when both forms are used.

## Fix direction (hypothesis to falsify)

Make the intermediate column name injective in the aggregate argument (for example a hash or an
ordinal suffix when two distinct arguments sanitise to the same name), and add a collision check
that fails at plan time instead of silently sharing a column. Pin with the reproduction through
INSERT, UPDATE and DELETE.
