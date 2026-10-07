# A SELECT-list literal beside GROUP BY raises `non-integer constant in GROUP BY` — S3 (loud)

Pre-existing (identical on the base). Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400), on pg16. Found by the Task 2 adversarial reviewer
(F2) while trying to pin "a SELECT literal `'SUM(DISTINCT x)'` is created and correct". The
failure is a raw PostgreSQL error, not a tagged rejection.

## Reproduction (pg16)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, v INT);
SELECT create_reflex_ivm('l', $$SELECT g, 'SUM(DISTINCT x)' AS d, SUM(v) AS s FROM t GROUP BY g$$);
-- ERROR:  non-integer constant in GROUP BY
```

The literal text is incidental: the reviewer states `SELECT g, 'plain' AS d, SUM(v) AS s FROM t
GROUP BY g` raises the same error on the base.

## Expected vs actual

Expected: either the IMV is created and correct (a constant output column beside grouped
aggregates is valid SQL) or a tagged `[reflex-unsupported]` rejection is returned. Actual: the
create fails with `non-integer constant in GROUP BY`.

## What is known about the mechanism

Not investigated. The error text suggests the literal select item is classified as a group key
and emitted into a generated `GROUP BY` clause, where PostgreSQL treats a non-integer constant as
invalid. This is an inference from the message, not from the generated SQL.

Side effect on the release: the 1.12.0 design asked for an end-to-end oracle test that a SELECT
literal `'SUM(DISTINCT x)'` is created and correct. It cannot pass; only the analyzer-level unit
test (`src/tests/unit_sql_analyzer.rs`, about line 1127) covers it, and its "not rejected" holds
only for the analyzer.

## Fix direction (hypothesis)

Classify a constant select item as neither group key nor aggregate and emit it only in the
projection (end query), or reject it with a tagged message. Add an oracle test for a text
literal, a numeric literal and a `NULL` literal beside a `GROUP BY`.
