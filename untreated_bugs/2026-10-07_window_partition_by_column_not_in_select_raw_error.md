# A window function whose PARTITION BY column is not selected raises `column "g" does not exist` — S3 (loud, at create)

Pre-existing (release branch before 1.12.0 changes). Found by the Task 4 reviewer.

## Reproduction (from the review)

```sql
-- table t(id serial primary key, g text, v int)
SELECT id, v, SUM(v) OVER (PARTITION BY g) AS w FROM t
-- raw error: column "g" does not exist
```

The `create_reflex_ivm` wrapper call is not recorded.

## Expected vs actual

Expected: an IMV, or a tagged `[reflex-unsupported]` rejection. Actual: raw PostgreSQL error at
create.

## What is established and what is not

Established: the error text for this query. Not established: the cause, or whether
`PARTITION BY` on a selected column works (the review implies it does, by contrast).

## Fix direction (hypothesis to falsify)

Either carry the PARTITION BY and ORDER BY columns of window calls into the generated SQL, or
reject the shape at create with a tagged message naming the cause.
