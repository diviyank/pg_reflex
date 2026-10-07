# A CTE alias equal to one of its column names yields a whole-row column in the IMV — S1 (suspected wrong type and value; mechanism not investigated)

Pre-existing (identical on the base). Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400), on pg16. Found by the Task 2 adversarial reviewer
(adjacent finding A3), who recorded it as "not investigated further". Everything below the
reproduction about the cause is the reviewer's inference.

## Reproduction (pg16)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, v INT);
SELECT create_reflex_ivm('v', $$WITH c AS (SELECT g, COUNT( DISTINCT v) AS c FROM t GROUP BY g)
  SELECT g, c FROM c$$);
```

The reviewer ran this inside their create plus INSERT/UPDATE/DELETE sequence with a bidirectional
`EXCEPT ALL` oracle. The `COUNT( DISTINCT v)` is incidental to what the reviewer suspected; the
trigger is the CTE named `c` having a column named `c`. A repro without DISTINCT was not run.

## Expected vs actual

Expected: the IMV column `c` holds the bigint count and the oracle matches. Actual: the oracle
fails with `EXCEPT types <imv>__cte_c and bigint cannot be matched`, on head and on base.

## What is known about the mechanism

Only the reviewer's suggestion: the final `SELECT g, c FROM c` resolves `c` as the whole-row
reference to the CTE's sub-IMV relation (`<imv>__cte_c`) rather than as its column `c`, so the
IMV column holds a composite value of the sub-IMV's row type instead of the bigint. PostgreSQL
resolves a bare name that is both a column and a relation alias to the column, so the suspicion is
that the rewritten text (the alias replaced by the quoted sub-IMV name by `replace_identifier`, see
`src/create_ivm/decompose.rs` around lines 1013 and 1070) changes which one the bare `c` means. Not
confirmed by looking at the generated SQL.

## Fix direction (hypothesis)

Dump the stored defining query and the generated base query for this shape to see how the bare
`c` is rewritten, then qualify the CTE reference in the rewritten FROM clause (for example give
the sub-IMV an alias that cannot collide with a column) so that a bare column name always keeps its
column meaning. Add an oracle test with the CTE alias equal to a column name, with and without
an aggregate.
