# MIN / MAX over a text-returning function call fails at create with a raw type error — S3 (loud, at create)

Pre-existing (the 1.12.0 Task 4 report records it as "on the release branch before this task").
Found while implementing the function-contract task (Task 4) of the 1.12.0 hardening, on pg17: a
brief test fixture used `MAX(upper(g))` and could not be created.

## Reproduction (pg17, release branch)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, v INT);
INSERT INTO t(g, v) VALUES ('a',1),('a',2),('b',5);
SELECT create_reflex_ivm('x', 'SELECT g, SUM(v) AS s, MAX(upper(g)) AS u FROM t GROUP BY g');
-- ERROR:  column "__max_upper_g" is of type numeric but expression is of type text
-- HINT:  You will need to rewrite or cast the expression.
```

The shorter form `SELECT g, MAX(upper(g)) AS u FROM t GROUP BY g` raises the same error.
`MAX(length(g))` (an integer-returning call) is created and is correct.

## Expected vs actual

Expected: an IMV whose `__max_upper_g` column is `text` (or a tagged `[reflex-unsupported]`
rejection). Actual: the stored intermediate column is typed `numeric` and the generated INSERT
fails with a raw PostgreSQL error; nothing is created.

## What is established and what is not

Established: the error text and that it is raised at create. Not established: the cause. The
Task 4b report says, for the nested integer-expression variant, that "the column type is resolved
from the argument text, which fails for an expression, so the column is NUMERIC"; the same family
is named for this error ("Same family as the `MAX(upper(g))` report"), but the source does not
trace the MIN/MAX-over-text path itself.

## Related reports

- `2026-10-07_min_max_filter_over_text_raw_type_error.md`
- `2026-10-07_nested_max_text_filter_raw_type_error_at_create.md`
- `2026-10-07_top_level_min_max_integer_expression_stored_as_numeric.md`

## Fix direction (hypothesis to falsify)

Resolve the intermediate column's type from the argument expression through the catalog (for
example by asking PostgreSQL for the type of the argument over the source) instead of from the
argument text. Pin with an oracle test over INSERT, UPDATE and DELETE of the extreme, for text,
varchar and an integer-returning call.
