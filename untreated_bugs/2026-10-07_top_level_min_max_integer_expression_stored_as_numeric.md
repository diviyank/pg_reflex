# Top-level MIN / MAX over an integer expression or with FILTER is stored as NUMERIC where the query gives INTEGER or BIGINT — S3 (type difference, data correct)

Pre-existing: identical on the 1.11.5 base and on the 1.12.0 branch. Found by the Task 4b reviewer
(adjacent A2, second half) and re-confirmed by the re-reviewer with 16 controls.

## Reproduction

Reproduction not recorded as a single statement. The re-reviewer ran 16 top-level controls
(`MIN` / `MAX` x BIGINT FILTER, BIGINT expression, INT FILTER, INT expression x grouped /
ungrouped) on a fixture of `s` with an integer `v` and a generated column
`q BIGINT GENERATED ALWAYS AS (v::bigint * 3) STORED`. Cells named in the report:
`MAX(q) FILTER (WHERE v > 1)`, `MAX(v * 3)`. Result: "on head and on base alike, correct data and
a NUMERIC column where the query gives INTEGER or BIGINT".

## Expected vs actual

Expected: IMV column has the query's type. Actual: NUMERIC; values equal. A consumer that reads
the IMV column type (client drivers, downstream views, integer division in a dependent query)
sees a different type than the query's.

## What was ruled out

A plain column of any type in every wrapper, and NUMERIC arguments, are typed correctly. Nested
MIN / MAX cells have the right type on the branch.

## Related reports

`2026-10-07_max_over_text_function_expression_raw_type_error.md` (the same expression-typing
family; the author attributes the NUMERIC to the type being resolved from the argument text).

## Fix direction (hypothesis to falsify)

Type the extremum column from the argument expression via the catalog. Compare `pg_typeof` of the
IMV column with the query's in the oracle for integer, bigint and smallint expressions and FILTER
forms.
