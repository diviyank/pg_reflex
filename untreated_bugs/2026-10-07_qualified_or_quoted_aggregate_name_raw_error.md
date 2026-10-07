# A schema-qualified or quoted spelling of a supported aggregate raises `aggregate functions are not allowed in GROUP BY` — S3 (loud)

Pre-existing. Observed during the 1.12.0 hardening work on the `release/1.12.0` branch (base
commit 6946400), on pg17.7 at commit 6946400 (audit reproduction) and on pg16 (Task 2 reviewer,
for `pg_catalog.count(DISTINCT v)`, "on both" head and base).

## Reproduction (pg17.7, commit 6946400)

```sql
CREATE TABLE t(id serial primary key, g text, v int);
SELECT create_reflex_ivm('t8', 'SELECT g, pg_catalog.sum(v) AS s FROM t GROUP BY g');
-- ERROR:  aggregate functions are not allowed in GROUP BY
```

The `"sum"(v)` spelling is named in the audit as the same class; the sources record the error for
`pg_catalog.sum(v)` and `pg_catalog.count(DISTINCT v)` but not an executed `"sum"(v)` run (labelled
reasoned here). Task 2's review also records `s.mymax(v)` for a user-defined aggregate
(`CREATE AGGREGATE s.mymax(int) (SFUNC = int4larger, STYPE = int)`) raising the same error on
base and head, so user-defined aggregates are not recognised as aggregates either.

## Expected vs actual

Expected: `pg_catalog.sum(v)` is recognised as `SUM` and handled like the bare spelling, or the
create returns a tagged `[reflex-unsupported]` rejection. Actual: a raw PostgreSQL error from
the generated SQL; nothing is created.

## What is known about the mechanism

By the Task 2 reviewer's reading: `detect_aggregate` and `is_known_unsupported_aggregate` in
`src/sql_analyzer.rs` match on the full upper-cased function name (`PG_CATALOG.SUM`), which is
not a known aggregate, so the call is classified as an ordinary expression and the select item
becomes a group key; the aggregate then lands in a generated `GROUP BY`. The 1.12.0 DISTINCT
check uses `bare_function_name` (last path segment, lower-cased), so `pg_catalog.sum(DISTINCT v)`
is now cleanly rejected while `pg_catalog.sum(v)` and `pg_catalog.count(DISTINCT v)` are not: an
inconsistency left in place. Note that `bare_function_name` lower-cases a quoted name, so it
treats `"SUM"` as `sum`; a consumer that used it to accept or classify would wrongly treat the
nonexistent function `"SUM"` as the built-in.

## Fix direction (hypothesis)

Resolve the function once to a canonical identity before classification: strip a `pg_catalog`
qualifier and accept it as the built-in, keep other schemas and quoted-case names as non-built-in
and reject them at create with a tagged message (they cannot be maintained incrementally). Use
the same resolver in `detect_aggregate`, `is_known_unsupported_aggregate` and the DISTINCT
check. Add tests for `pg_catalog.sum`, `"sum"`, `"SUM"`, `other_schema.max` and a user-defined
aggregate.
