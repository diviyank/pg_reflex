# Session-dependent functions are rejected from a name list, and a cast of timestamptz to date gets no time-zone warning — limitation

Residual of "clock and session values outside the function-contract name list are accepted".
Of the names that report lists, `pg_backend_pid()`, `inet_client_addr()`,
`pg_current_xact_id_if_assigned()` and `SYSTEM_USER` were already rejected on the 1.12.0 branch.
Added: `pg_current_snapshot()`, `txid_current_snapshot()`, `pg_my_temp_schema()` and
`pg_trigger_depth()` (Step 0: `pg_current_snapshot()::text` was created and differed from the
query at create and after every write), and `date(x)` joins the time-zone warning
(`is_clock_or_session_function`, `is_timezone_sensitive_function`, `src/sql_analyzer.rs`).

## What remains

1. The list is a list. A STABLE function whose result depends on the session or the transaction
   and is not named there is accepted: `has_table_privilege('t', 'select')`, `pg_has_role(...)`,
   `pg_is_in_recovery()`, `to_regclass('t')` (search_path), `current_setting`-backed user
   functions. Not executed; read from the list.
2. `ts::date` and `CAST(ts AS date)` on a `timestamptz` column depend on the session TimeZone and
   get no warning: the cast is not a function call and the operand's type is not known to the
   analyzer. Reported as executed by the Task 4 review (0 warnings); not re-run here.
3. PG15 only: `system_user` is not a keyword before PG16, so a table may have an unquoted column
   of that name. `SELECT system_user, SUM(v) AS s FROM t GROUP BY system_user` over such a table is
   rejected as non-deterministic (executed on pg15.19). Quoting the column (`"system_user"`) is
   accepted. `backend_connection_and_transaction_identity_functions_rejected`
   (`src/tests/unit_sql_analyzer.rs`) pins the rejection of the bare word on every version, so it
   was not gated here.

## Fix direction

For 1: classify by the catalog (`provolatile = 's'` plus a list of session-bound built-ins is
still a list; a policy decision). For 2: needs expression types. For 3: gate the `system_user`
keyword on the server version and the unit test with it.
