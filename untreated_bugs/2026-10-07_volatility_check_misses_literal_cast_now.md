# The volatile / clock function check does not see literal casts such as 'now'::timestamptz — limitation

Documented limitation of the 1.12.0 function-contract check (Task 4). The string forms were also
accepted before 1.12.0.

## Reproduction

Constructed from the report: the review ran `'now'::timestamptz`, `timestamptz 'now'` and
`'today'::date` and records them as "ran, all created on head". The exact `create_reflex_ivm`
statement is not recorded.

## Expected vs actual

Expected: an IMV whose definition depends on the time of evaluation is rejected like
`now()` / `transaction_timestamp()`. Actual: it is created. The Task 4 report: "Literal casts
evaluated at run time are not function calls and are accepted".

## What is established and what is not

Established: the three spellings are accepted. The check works from the parsed function calls
and from a catalog lookup of volatility by name, so a cast of a string literal is not seen. Not
measured: what such an IMV then returns after DML versus the query (the source does not say).

## Related reports

- `2026-10-07_volatility_check_misses_attribute_notation_call.md`
- `2026-10-07_clock_and_session_names_outside_the_function_list_accepted.md`

## Fix direction (hypothesis to falsify)

Detect a cast whose operand is a string literal naming a special date/time input (`now`,
`today`, `tomorrow`, `yesterday`, `epoch`, `infinity`, `allballs`), or accept the limitation and
document it in the user-facing function-contract notes. Whether the added rejection breaks a
legitimate literal such as `'2020-01-01'::date` has to be tested.
