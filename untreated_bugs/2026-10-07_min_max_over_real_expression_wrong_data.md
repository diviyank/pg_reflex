# MIN / MAX over a REAL expression is wrong, top level and nested — S1 (SILENT wrong result)

Pre-existing: identical on the 1.11.5 base and on the 1.12.0 branch. Found by the Task 4b
re-reviewer (finding N2), who groups it with the REAL family (see
`2026-10-07_sum_avg_over_real_column_wrong_data.md`).

## Reproduction (re-review)

```sql
-- table s with a column rl REAL (full DDL not recorded)
SELECT create_reflex_ivm('x', 'SELECT g, MIN(rl * 2) / 3 AS b2 FROM s GROUP BY g');
-- 4 mismatches at create, on base and on head
```

The top-level `MIN(rl * 2)` is wrong on both as well, and its stored column is NUMERIC where the
query gives DOUBLE PRECISION.

## Expected vs actual

Expected: IMV equals the query. Actual: 4 mismatching rows right after create.

## What was ruled out

DOUBLE PRECISION and NUMERIC expressions, and a plain REAL column (no expression), are correct.

## What is established and what is not

Established: the mismatch counts and the type difference. Not established: the cause; the
re-reviewer only notes the NUMERIC-versus-DOUBLE PRECISION column type at top level.

## Fix direction (hypothesis to falsify)

Type the extremum intermediate column from the argument expression through the catalog, then
re-run the oracle for REAL expressions at top level and nested. The mismatch may be a value
round-trip through NUMERIC; check that before assuming the type fix alone clears it.
