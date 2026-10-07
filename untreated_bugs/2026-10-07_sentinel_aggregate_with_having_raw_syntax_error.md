# A sentinel (no GROUP BY) aggregate with HAVING fails at create with a raw syntax error — S3 (loud)

Pre-existing (identical on the base). Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400), on pg17. Found by the Task 3 adversarial reviewer
(A4, probes `zp_4e` and `zp_4g`).

## Reproduction (pg17)

```sql
CREATE TABLE src(id SERIAL PRIMARY KEY, g TEXT, city TEXT, v INT);
INSERT INTO src(g, city, v)
  SELECT 'g' || (i % 3), CASE WHEN i % 2 = 0 THEN 'Lyon' ELSE 'Paris' END, i
  FROM generate_series(1, 60) i;
SELECT create_reflex_ivm('s', 'SELECT SUM(v) AS s FROM src HAVING SUM(v) > 0');
-- raw syntax error at create
```

The reviewer's report gives the query text above and states "raw syntax error at create"; the
exact error message text was not recorded. The error is raised, not returned as an `ERROR:`
result string.

## Expected vs actual

Expected: a created IMV (a single-row aggregate filtered by `HAVING`) or a tagged
`[reflex-unsupported]` rejection. Actual: a raw PostgreSQL syntax error.

## What is known about the mechanism

By the reviewer's reading: for a sentinel aggregate `generate_end_query`
(`src/query_decomposer.rs`, about lines 530-553) skips the ` WHERE __ivm_count > 0` filter but
still appends the `HAVING` as ` AND (...)`, producing `... AND (...)` with no preceding `WHERE`.

## Fix direction (hypothesis)

Emit ` WHERE (<having>)` when there is no preceding `WHERE` (the non-sentinel path already does
this in the other direction: ` AND (<having>)` or ` WHERE (<having>)` depending on the previous
clause), and pin with an oracle test over INSERT, UPDATE and DELETE that move the sum across the
threshold, including a state where the single row disappears. Or reject the shape with a tagged
message.
