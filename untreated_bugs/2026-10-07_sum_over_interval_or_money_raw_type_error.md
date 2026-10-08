# SUM / AVG over an interval (or money) argument fails at create with a raw type error — S3 (loud, at create)

Pre-existing. Measured on pg17 while fixing the typed-intermediate reports of the 1.12.0 fold (G3).

## Reproduction (pg17)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, iv INTERVAL);
SELECT create_reflex_ivm('x', 'SELECT g, SUM(iv) AS r FROM s GROUP BY g');
-- ERROR:  column "__sum_iv" is of type numeric but expression is of type interval
```

## Expected vs actual

Expected: an IMV, or a tagged `[reflex-unsupported]` rejection. Actual: a raw PostgreSQL error;
nothing is created. `money` was not run; it takes the same path (the `__sum_*` column is NUMERIC
unless PostgreSQL types the sum DOUBLE PRECISION).

## What is established

The error text. MIN / MAX over an interval are correct since 1.12.0 (their column takes the type
PostgreSQL gives the aggregate).

## Fix direction (hypothesis to falsify)

`single_precision_sum_argument` in `src/create_ivm/mod.rs` already describes the base query before
any DDL to reject a REAL sum: reject there every `__sum_*` column whose type is not bigint, numeric
or double precision, naming the argument. Maintaining an interval sum would also need the MERGE
`COALESCE(x, 0)` neutral element to be typed.
