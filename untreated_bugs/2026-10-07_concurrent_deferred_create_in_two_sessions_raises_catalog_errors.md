# `create_reflex_ivm(…, 'DEFERRED')` in one session while another session creates or commits a DEFERRED IMV: raw catalog errors — S2 (LOUD at create)

Pre-existing as far as observed (the code involved is not touched by the 1.12.0 fold branches).
Observed on pg18 during the G4 fix round, when tests that COMMIT DEFERRED IMVs over a second
connection ran beside the ordinary `#[pg_test]`s, each of which creates its IMVs in an open
transaction.

## Observation

While a second connection ran, in committed transactions,
`SELECT create_reflex_ivm('<unique name>', '<query over its own tables>', NULL, 'UNLOGGED', 'DEFERRED')`,
concurrent sessions calling `create_reflex_ivm(…, 'DEFERRED')` on unrelated tables failed with

- `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`, or
- `tuple concurrently updated`

and so did some creates of the committing connections. In the one run where four committing
tests ran beside the others, five `#[pg_test]`s and nine committed cells failed at create that way;
with the committing test switched off, or run alone on one connection, none does. All names
involved (tables, IMVs) were unique per session.

## Expected vs actual

Expected: two sessions creating DEFERRED IMVs over different tables both succeed (one may wait
for the other). Actual: one of them raises a raw catalog error.

## What is established and what is not

Established: the two errors, only with a concurrently committing DEFERRED create.
Not established: a two-session script that reproduces it deterministically, and which shared
object is created or altered. The errors point at a relation created with `IF NOT EXISTS` and at
a catalog row rewritten by every DEFERRED create (a shared function or table), neither of which
is serialised across sessions before the DDL runs.

## Consequence for the test suite

Tests that COMMIT cannot run in the default parallel run. `v112_sq_commit_path`
(`src/tests/pg_test_v112_subquery_writes.rs`) returns at once unless the test filter names it:
`cargo pgrx test <pg> v112_sq_commit`.

## Fix direction (hypothesis to falsify)

Take one advisory lock for the shared DEFERRED infrastructure before creating or replacing any
of it, and create it once (extension install or first use under that lock) instead of on every
create. Then the COMMIT-path tests can join the default run.
