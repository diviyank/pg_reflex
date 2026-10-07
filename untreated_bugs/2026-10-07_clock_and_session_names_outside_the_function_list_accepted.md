# Clock and session values outside the function-contract name list are accepted, and zone-dependent date casts get no warning — limitation

Found by the Task 4 reviewer (M3). Only part was handled in the release: `EXTRACT(... FROM
timestamptz)` was added to the time-zone warning list. The rest is documented as not covered.

## Reproduction

Reproduction not recorded as scripts. The review ran these and states all were created on head:
`SYSTEM_USER`, `pg_backend_pid()`, `inet_client_addr()`, `pg_current_xact_id_if_assigned()`,
`pg_current_snapshot()`. Separately `date(ts)` and `ts::date` on a `timestamptz` column are
zone-dependent and produced 0 warnings (ran).

## Expected vs actual

Expected: session- or time-dependent values are rejected (as `now()` and `clock_timestamp()`
are), and zone-dependent casts carry the time-zone warning. Actual: accepted; no warning for the
date casts. The review notes these are STABLE or special-syntax names outside the spec's list;
`pg_current_xact_id_if_assigned` and `SYSTEM_USER` are "siblings of listed names and cheap to
add".

## What is established and what is not

Established: acceptance and zero warnings as above. A related, unverified-by-execution note from
the Task 4 re-review: on PG15, `SELECT system_user, SUM(v) FROM t GROUP BY system_user` over a
table with an unquoted column named `system_user` could be falsely rejected if `SYSTEM_USER` is
added as a keyword (loud, the column can be quoted). Not established: what these IMVs return
after DML compared to the query.

## Related reports

- `2026-10-07_volatility_check_misses_literal_cast_now.md`
- `2026-10-07_volatility_check_misses_attribute_notation_call.md`

## Fix direction (hypothesis to falsify)

Extend the name lists with the siblings the review calls cheap (`pg_current_xact_id_if_assigned`,
`SYSTEM_USER`, `pg_backend_pid`, `inet_client_addr`, `pg_current_snapshot`), and add `date` and a
`::date` cast on `timestamptz` to the time-zone warning. Alternatively classify by the catalog's
volatility and session-dependence rather than by a name list. Whether stable-but-session-bound
functions should be rejected at all is a policy decision.
