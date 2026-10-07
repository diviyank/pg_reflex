# A set-returning function in FROM raises `relation "generate_series" does not exist` — S3 (loud, at create)

Pre-existing (release branch before 1.12.0 changes). Found by the Task 4 reviewer.

## Reproduction (from the review)

```sql
-- table t(id serial primary key, ...)
SELECT ... FROM t, generate_series(1, 2) AS s(i)
-- raw error: relation "generate_series" does not exist
```

The review gives the FROM clause only; the select list and the `create_reflex_ivm` call are not
recorded.

## Expected vs actual

Expected: a tagged `[reflex-unsupported]` rejection (a function FROM item cannot be maintained
incrementally from transition tables) or support. Actual: raw PostgreSQL error at create.

## What is established and what is not

Established: the error text. Not established: the cause (the name `generate_series` appears to be
treated as a source table; by the error text only).

## Related report

`2026-10-07_unnest_in_select_list_raw_error.md`

## Fix direction (hypothesis to falsify)

Reject a function call used as a FROM item at create with a tagged message.
