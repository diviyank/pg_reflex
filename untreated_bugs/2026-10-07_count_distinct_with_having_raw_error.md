# COUNT(DISTINCT) with HAVING raises a raw PostgreSQL error — S3 (loud, create or first write)

Pre-existing (identical on the base). Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400), on pg17 (Task 3 implementer, minimal reproduction)
and on pg16 (Task 2 reviewer, A2). Nothing wrong is stored; the failure is loud, but the error is
raised, not returned as a `[reflex-unsupported]` result string, so the caller's transaction aborts.

## Reproduction (pg17)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, city TEXT, v INT);
INSERT INTO t(g, city, v) SELECT 'g' || (i % 3), 'Lyon', i FROM generate_series(1, 60) i;
SELECT create_reflex_ivm('cd', 'SELECT g, COUNT(DISTINCT v) AS c FROM t GROUP BY g HAVING COUNT(DISTINCT v) > 0');
-- ERROR:  argument of AND must be type boolean, not type text
```

The Task 3 implementer ran it through `create_reflex_ivm_with_topk(..., 0, None)`. The Task 2
reviewer saw the same error with `SELECT g, COUNT(DISTINCT v) AS d FROM t GROUP BY g HAVING
COUNT(DISTINCT v) > 1` during a create plus INSERT/UPDATE/DELETE sequence on pg16 and did not
isolate which step raised; the pg17 reproduction above raises at create.

## Expected vs actual

Expected: either the IMV is created and correct, or a tagged `[reflex-unsupported]` rejection is
returned. Actual: PostgreSQL error `argument of AND must be type boolean, not type text`.

## What is known about the mechanism

By the implementer's reading: `generate_end_query` (`src/query_decomposer.rs`, about lines
543-556 on the reviewed tree) appends the `HAVING` as ` AND (...)` after the ` GROUP BY ` it
emits for `COUNT(DISTINCT)`, so the text becomes `... GROUP BY g AND (...)`. No literal is
involved. A second, related fact from the same review: a `COUNT(DISTINCT)` end query with a
`HAVING` is also the only way a depth-0 `GROUP BY` and a `HAVING` can coexist in one end query,
so fixing this will exercise code paths the 1.12.0 end-query work treated as unreachable.

## Fix direction (hypothesis)

Emit the `HAVING` clause after the `GROUP BY` with the `HAVING` keyword (instead of ` AND (...)`)
when the end query regroups; then pin with oracle tests for `COUNT(DISTINCT)` with and without
other aggregates, with `HAVING` on the distinct count and on another aggregate. If supporting it
is not worth it, reject the combination at create with a tagged message.
