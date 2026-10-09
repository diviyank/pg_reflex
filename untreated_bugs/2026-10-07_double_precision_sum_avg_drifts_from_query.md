# SUM / AVG over DOUBLE PRECISION maintained by add and subtract differs from the query — S1 (SILENT wrong result, unbounded on cancellation)

Pre-existing. Measured on pg17 while fixing the REAL family (group G3 of the 1.12.0 fold); not
changed by that work.

## Reproduction (pg17)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, f DOUBLE PRECISION);
INSERT INTO s(g, f) VALUES ('a', 1e16), ('a', 1), ('a', 1);
SELECT create_reflex_ivm('x', 'SELECT g, SUM(f) AS r FROM s GROUP BY g');
INSERT INTO s(g, f) VALUES ('a', -1e16);
DELETE FROM s WHERE f IN (1e16, -1e16);
-- query: 2          IMV: a value that no row order of the two remaining rows produces
```

Measured with the 12-column fixture of `src/tests/pg_test_v112_types.rs` (values 1e16, 1, 1, 0.1,
0.3, -0.7, then -1e16 inserted and both large values deleted), bidirectional `EXCEPT ALL` oracle:

| query | mismatching rows per step |
|---|---|
| `SELECT g, SUM(f) FROM s GROUP BY g` | create 0, ins 0, upd 0, move 0, **del 2**, then 0 |
| `SELECT SUM(f) FROM s` | create 0, **ins 2, upd 2, move 2, del 2**, empty 0, **refill 2** |
| `SELECT g, AVG(f) FROM s GROUP BY g` | create 0, ins 0, upd 0, move 0, **del 2**, then 0 |

## Expected vs actual

Expected: the IMV equals the query. Actual: the intermediate `__sum_*` column is DOUBLE PRECISION and
each write merges `stored + delta` / `stored - delta`. Floating-point addition is not associative, so
the stored value is not the value PostgreSQL computes by adding the rows in scan order; after a large
value is added and later removed the low-order part of the remaining values is lost for good (the
error is then the size of the remaining values, not of one unit in the last place).

## What is established and what is not

Established: the tables above; the ungrouped case stays wrong after the source is emptied and
refilled (the sentinel row keeps its accumulated error). With values that add exactly (small integers,
halves) the existing suite is green, which is why this was read as correct.
Not established: how many production IMVs sum DOUBLE PRECISION columns, and what recomputing their
affected groups on every write would cost.

## Why it was not fixed with the REAL reports

REAL was wrong on ordinary data (the sum went through NUMERIC with 6 significant digits; AVG was
wrong at create) and no test pins it, so 1.12.0 rejects `SUM` / `AVG` over a REAL argument with a
`::numeric` remedy. DOUBLE PRECISION sums are used by the existing suite and by deployed IMVs and
are equal to the query whenever the additions are exact; changing them (rejection, or a recompute
of the affected groups on every write) is a behaviour and performance decision for the owner.

## Fix direction (hypothesis to falsify)

Either treat a DOUBLE PRECISION `__sum_*` column like a MIN / MAX column on every write (recompute the
affected groups from the source: a result some row order of the query produces, at the cost of the
incremental path), or reject with the remedy `SUM(x::numeric)`, which is maintained exactly. Bit-for-bit
equality with a fresh query cannot be promised by any strategy: PostgreSQL's own float sum depends on
the plan's row order.
