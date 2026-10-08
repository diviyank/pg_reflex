# Two COUNT(DISTINCT) over different columns fail at create with `cannot cast type bigint to boolean` — S3 (loud, at create)

Pre-existing: same raw error on commit 8ef9743 and on the `fix/fold-g2-having` head, pg17.

## Reproduction

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, v INT, n NUMERIC);
INSERT INTO t(g, v, n) VALUES ('a', 1, 1.5), ('a', 2, 1.5), ('b', 5, 6.25);
SELECT create_reflex_ivm('cd2',
  'SELECT g, COUNT(DISTINCT v) AS a, COUNT(DISTINCT n) AS b FROM t GROUP BY g');
-- ERROR:  cannot cast type bigint to boolean
```

## Expected vs actual

Expected: an IMV, or the tagged rejection that COUNT(DISTINCT) cannot be mixed. Actual: raw
PostgreSQL error at create; nothing is left behind.

## What is established and what is not

Established: the error and parity. The "cannot be mixed with other aggregates" check
(`src/create_ivm/mod.rs`) only looks for an aggregate that is not a COUNT(DISTINCT), so two of
them pass it. Not traced: where the boolean cast comes from. Risk to keep in mind when fixing: the
planner keys the intermediate on every distinct argument (`g, v, n`) and the end query counts each
column over those rows, which counts one value once per combination, so making the create succeed
without a second look would turn this loud error into wrong counts.

## Fix direction (hypothesis to falsify)

Reject more than one distinct argument at create with the tagged message and the CTE remedy (one
CTE per COUNT(DISTINCT), joined on the group key), proven by an oracle test.
