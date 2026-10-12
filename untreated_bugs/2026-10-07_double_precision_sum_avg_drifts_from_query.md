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

## Measured size of the error (added 2026-10-12, from the full re-review of 1.12.0, seat R2)

PostgreSQL 18.6, tree `0324f47` (`drift8` lines of `full-review-r2-out-head3.txt` in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`; figures copied from
`full-review-r2.md`, the probe SQL is not printed there). 20 000 rows of two-decimal values in
5 groups, then 30 rounds of one DELETE, one UPDATE (`x + 0.1`) and one INSERT of 1 500 rows;
group sums about 4.5 million.

| After | Largest absolute error of SUM | Relative | Differs at 2 decimals |
|---|---|---|---|
| create | 0 | 0 | no |
| 30 statements | 1.2e-8 | 4.6e-15 | no |
| 90 statements | 7.5e-9 | 1.6e-15 | no |
| one 1e9 value inserted then deleted | 4.0e-7 | 8.8e-14 | no |
| one 1e12 value inserted then deleted | 5.5e-5 | 1.2e-11 | no |
| one 1e15 value inserted then deleted | 0.055 | 1.2e-8 | yes |
| one 1e18 value inserted then deleted | 10.07 | 2.2e-6 | yes |

- The error does not heal while the group has rows: a group reduced to one row of 389.55 shows
  `389.5499999988824`. It disappears when the group empties and refills (exact `3.75`).
- AVG follows SUM: largest 2.7e-12 in ordinary use, 5.9e-4 after the 1e18 outlier. Counts never
  differ.
- For comparison, REAL (refused since 1.12.0) drifted by 20.5 on a sum of 4.6 million on
  `6946400`.

Reading: in ordinary use the error stays in the last digits (not visible at two decimals after 90
statements). It becomes visible at two decimals, and stays, once one value of 1e15 or more has
been inserted into and deleted from a group whose sum is about 4.5e6.

Decision recorded for 1.12.0 (ledger `progress.md`): DOUBLE PRECISION `SUM` / `AVG` stay accepted
and are documented as a limitation; this report stays open. The reviewer noted that "last-digit
drift" understates the outlier case.
