# Two GROUP BY columns with the same bare name fail at create with a raw error — S3 (loud)

Pre-existing. Observed during the 1.12.0 hardening work on the `release/1.12.0` branch (base
commit 6946400; Task 1b probe branch based on c71c65d), on pg15. Found by the Task 1b adversarial
reviewer (P5) while building a fixture; "not probed further".

## Reproduction (pg15)

The reviewer recorded only the shape `SELECT a.label, b.label, … GROUP BY a.label, b.label`. The
tables and join below are an illustrative sketch of it, not the recorded fixture.

```sql
CREATE TABLE ta(id SERIAL PRIMARY KEY, label TEXT);
CREATE TABLE tb(id SERIAL PRIMARY KEY, ta_id INT, label TEXT);
SELECT create_reflex_ivm('v', 'SELECT a.label, b.label, COUNT(*) AS c
  FROM ta a JOIN tb b ON b.ta_id = a.id GROUP BY a.label, b.label');
-- ERROR:  column "label" specified more than once
```

## Expected vs actual

Expected: the create succeeds with two distinct output columns, or returns a tagged rejection
asking for aliases. Actual: a raw error `column "label" specified more than once`.

## What is known about the mechanism

Not investigated. The message suggests the generated intermediate table is built with both
group columns under their bare name `label`. Note that the user's own query is valid PostgreSQL
(a result set may repeat a column name), so this is a limitation of the generated DDL.

## Fix direction (hypothesis)

Name the intermediate group-key columns by position or by a collision-safe derived name instead
of the bare output name, or reject at create with a tagged message telling the user to alias the
columns. Add a create test that the aliased form (`a.label AS la, b.label AS lb`) works and the
unaliased form behaves as decided.
