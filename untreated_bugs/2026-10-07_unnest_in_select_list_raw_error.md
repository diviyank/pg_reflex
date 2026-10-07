# `SELECT id, unnest(arr) FROM t` fails at create with `could not create unique index` — S3 (loud, at create)

Pre-existing (release branch before 1.12.0 changes). Found by the Task 4 reviewer.

## Reproduction (from the review)

```sql
-- table t with an array column arr (DDL not recorded)
SELECT id, unnest(arr) FROM t
-- raw error: could not create unique index
```

The `create_reflex_ivm` wrapper call is not recorded.

## Expected vs actual

Expected: an IMV, or a tagged `[reflex-unsupported]` rejection. Actual: raw PostgreSQL error at
create.

## What is established and what is not

Established: the error text. Not established: the cause. By the error text only, a unique index
is built on a key that repeats once the set-returning function expands each source row.

## Related report

`2026-10-07_set_returning_function_in_from_raw_error.md`

## Fix direction (hypothesis to falsify)

Reject a set-returning function in the SELECT list at create with a tagged message, or make the
passthrough key include the expanded value.
