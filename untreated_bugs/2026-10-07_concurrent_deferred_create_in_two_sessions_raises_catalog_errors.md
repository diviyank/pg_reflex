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

## Narrowed on 2026-10-12 (branch `fix/v112-partition-catalog-locks`)

The two shared objects are `public.__reflex_deferred_pending` (created with `IF NOT EXISTS`) and
`public.__reflex_deferred_flush_fn` with its trigger (replaced and re-created by every DEFERRED
create, `schema_builder::build_deferred_flush_ddl`). A create now leaves them alone when they are
installed as it would create them, which removes the rewrite behind `tuple concurrently updated`
and the table lock that made DEFERRED creates and DEFERRED writes of different schemas wait on
each other.

What remains is the first install: sessions that all find the table missing race on
`CREATE TABLE IF NOT EXISTS`. Observed on pg15 with the whole suite run with `--include-ignored`:
`v112_sq_commit_path` passed, and at the instant its first create committed, four `#[pg_test]`s
that were creating a DEFERRED IMV in their own open transaction failed with
`duplicate key value violates unique constraint "pg_type_typname_nsp_index"`; no
`tuple concurrently updated`. In a database where a DEFERRED IMV was ever committed the table
exists and the race cannot occur; in the shared test database no `#[pg_test]` commits it, so every
test is a first installer. Installing the table, function and trigger at `CREATE EXTENSION` (and
in the migration) closes it.

## Consequence for the test suite

Tests that COMMIT cannot run in the default parallel run. `v112_sq_commit_path`
(`src/tests/pg_test_v112_subquery_writes.rs`) is `#[ignore]`d, so the default run reports it
as ignored, not as passed. Run it alone (`cargo pgrx test` has no way to pass `--ignored`):
`cargo test --no-default-features --features "pg18 pg_test" --lib v112_sq_commit_path -- --ignored`.
CI does not run it yet: it needs one serial step with that command.

## Fix direction (hypothesis to falsify)

Take one advisory lock for the shared DEFERRED infrastructure before creating or replacing any
of it, and create it once (extension install or first use under that lock) instead of on every
create. Then the COMMIT-path tests can join the default run.
