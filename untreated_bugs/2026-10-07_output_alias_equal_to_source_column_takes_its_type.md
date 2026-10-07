# An output alias equal to a source column name takes that source column's type — S3 (type difference, data correct)

Pre-existing: same on the 1.11.5 base and on the 1.12.0 branch. Found by the Task 4b reviewer
(adjacent A5); the re-reviewer hit it again with an alias `f` over a table that has
`f DOUBLE PRECISION`.

## Reproduction

The review's example, without full DDL: `SUM(v) / 2 AS rl` over a table that has a column
`rl REAL` gives a REAL column in the IMV where the query gives BIGINT. The re-reviewer: an output
alias `f` over a table with `f DOUBLE PRECISION` got a DOUBLE PRECISION column; "with the alias
renamed the types match".

## Expected vs actual

Expected: the IMV column has the query's result type. Actual: the type of the same-named source
column.

## What is established and what is not

Established: the observation and that renaming the alias avoids it. Not established: the
mechanism (the review implies the column type is looked up by name in the source and the output
alias hits that lookup) or whether a value is converted lossily when the types differ (a
BIGINT result through REAL would lose precision; not measured).

## Fix direction (hypothesis to falsify)

Resolve the output column types from the query itself (the planner's result types) rather than
by name against the source's columns. Pin with an alias equal to a source column of a different
type and compare `pg_typeof` of the IMV and the query.
