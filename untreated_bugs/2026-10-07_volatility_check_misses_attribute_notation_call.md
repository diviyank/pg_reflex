# The volatile function check does not see a call written in attribute notation (t.volfn) — limitation

Documented limitation of the 1.12.0 function-contract check (Task 4). Found by the Task 4
reviewer (M3) and recorded in the Task 4 report.

## Reproduction

Reproduction not recorded as a script. The review names the shape: `t.zz_rowvol` and
`(t).zz_rowvol` for a volatile function `zz_rowvol(t)` taking the table's row type; both were
created ("ran, all created on head"). The function's DDL is not recorded.

## Expected vs actual

Expected: rejection of a volatile function, as for `volfn(t)`. Actual: accepted. The Task 4
report: attribute notation "is a column reference to the parser and is accepted".

## What is established and what is not

Established: acceptance on the branch. Not established: what the created IMV returns after DML
compared to the query, since the volatile function is evaluated at a different time in an IMV.

## Related reports

- `2026-10-07_volatility_check_misses_literal_cast_now.md`
- `2026-10-07_clock_and_session_names_outside_the_function_list_accepted.md`

## Fix direction (hypothesis to falsify)

A compound identifier that resolves to no column of its table but to a one-argument function over
the row type could be looked up in `pg_proc`; the check would need the source's columns to tell
the two apart. Whether that is worth the complexity versus documenting the limitation is open.
