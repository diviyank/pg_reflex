# MIN / MAX with FILTER over a TEXT column fails at create with a raw type error — S3 (loud, at create)

Pre-existing: the top-level form raises the same error on the 1.11.5 base and on the 1.12.0
branch. Found by the Task 4b author (cell survey) and confirmed by the Task 4b re-reviewer
("on head for both forms").

## Reproduction

Reproduction for the top-level form not recorded as SQL. The source records the error text for
"a FILTER on MIN/MAX over a TEXT column, top level and nested":

```
column "__max_case_when_v_1_then_t_end" is of type numeric but expression is of type text
```

The re-reviewer's fixture was `CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT, t TEXT)` and
the nested probe was `COALESCE(MAX(t) FILTER (WHERE v > 1), '-')` (see the nested report). A
top-level shape of the same class is `MAX(t) FILTER (WHERE v > 1)` (constructed from the error
name, not copied from a recorded run).

## Expected vs actual

Expected: an IMV with a `text` extremum column, or a tagged rejection. Actual: raw PostgreSQL
error at create; nothing is created.

## What is established and what is not

Established: the error text, top level and nested, on head; on base the top level raises the same
error. Not established: the cause. The intermediate column is named after the FILTER-wrapped
argument (`CASE WHEN v > 1 THEN t END`), so the argument is an expression and not a plain column;
the author calls it the same family as `MAX(upper(g))`.

## Related reports

- `2026-10-07_max_over_text_function_expression_raw_type_error.md`
- `2026-10-07_nested_max_text_filter_raw_type_error_at_create.md`

## Fix direction (hypothesis to falsify)

Same as the related report: type the intermediate column from the argument expression via the
catalog. Pin with a FILTERed MIN and MAX over text, grouped and ungrouped, through INSERT, UPDATE
and DELETE of the extreme.
