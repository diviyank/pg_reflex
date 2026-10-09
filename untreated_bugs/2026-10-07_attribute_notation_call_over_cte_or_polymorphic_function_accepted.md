# A function called in attribute notation over a CTE / derived table, or one taking `record`, is not seen — limitation

Residual of "the volatile function check does not see a call written in attribute notation".
Closed on the 1.12.0 branch: `t.f`, `(t).f`, `(t.*).f`, `tbl.f`, `schema.tbl.f` and
`(schema.tbl).f`, where the qualifier names a table of the FROM clause in scope that has no
column `f` and a one-argument function `f(<row type of that table>)` exists, are rejected at
create, whatever the function's volatility (`attribute_notation_calls`,
`src/create_ivm/mod.rs`). Step 0 showed that the volatile case was the smaller problem: an
IMMUTABLE row function in attribute notation was created and then raised
`column t.f does not exist` on the first write to the source.

## What remains

Not executed, read from the code. The lookup resolves the qualifier to a table of the query and
compares the function's argument type with that table's row type, so it does not see:

- a qualifier naming a CTE or a derived table (`WITH c AS (...) SELECT c.f FROM c`);
- a function declared over `record`, a polymorphic type or a parent table's row type;
- a qualifier that one query level binds both to a table and to a derived table (the two
  branches of a UNION using the same alias for each): the reference is left alone.

## Expected vs actual

Expected: rejected like the table-qualified form. Actual: unknown; by the Step 0 result for the
table-qualified form, a create that succeeds and a first write that raises is likely.

## Fix direction

Resolve the reference through PostgreSQL rather than by name (plan the query and look for a
function call where the text has a column reference).

## Related

`2026-10-07_whole_row_function_argument_blocks_writes.md` (the same function called as `f(t)`).
