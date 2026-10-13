# An error inside the COMMIT-time partition flush aborts the backend (SIGABRT) — S1 (crash)

Found while writing the round-5 test of `fix/v112-swap-cascade-scope` (flags set by a failed
flush). Reproduced on `0a1884c` (the branch before round 5, CHECK variant) and on the round-5
tree (trigger variant), PostgreSQL 16 and 17. Not bisected further back.

## Reproduction (pg16, IMMEDIATE, through a dblink worker session)

```sql
CREATE TABLE tt (plan INT NOT NULL, k INT NOT NULL, q INT NOT NULL) PARTITION BY LIST (plan);
CREATE TABLE tt_p1 PARTITION OF tt FOR VALUES IN (1);
CREATE TABLE tt_p2 PARTITION OF tt FOR VALUES IN (2);
INSERT INTO tt SELECT p, k, p * 10 + k FROM generate_series(1, 2) p, generate_series(1, 3) k;
SELECT create_reflex_ivm('tv', 'SELECT s.plan, s.k, s.q FROM tt AS s', 'plan, k', NULL,
                         'IMMEDIATE', NULL, ARRAY['plan']);
CREATE FUNCTION refuse() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'no'; END $$;
CREATE TRIGGER refuse BEFORE DELETE ON tv FOR EACH STATEMENT EXECUTE FUNCTION refuse();
BEGIN;
ALTER TABLE tt DETACH PARTITION tt_p2;
DO $$ BEGIN PERFORM reflex_flush_partitions(); END $$;  -- fails, caught: WARNING, IMVs flagged
COMMIT;                                                  -- server process terminated by signal 6
```

Same with no explicit flush (only the COMMIT), and when the error is a CHECK constraint on the
IMV violated by a swapped leaf's rows (two-level tree, swap of a leaf with `q + 10000` and
`CHECK (q < 10000)` on the IMV).

Also reached by a lock error: two sessions swapping a leaf of sources in two schemas at once
deadlocked in the default drain of the auto-sync (its `pg_get_expr` filter opened every
partition of the database), and the COMMIT of the session that lost aborted the backend. That
route is closed by `1143253` (the drain reads `partdefid`; pinned again by
`v112_sc_swaps_in_two_schemas_do_not_wait_on_each_other_*`, RED on `0a1884c` by this abort),
but any other error at COMMIT still aborts.

## Expected vs actual

Expected: the COMMIT-time flush fails like the explicit one (its `DO` block catches the error,
leaves the root pending, flags the IMVs, prints a WARNING) and the COMMIT succeeds. Actual: the
backend aborts at COMMIT (`server process ... was terminated by signal 6: Aborted`, `Failed
process was running: COMMIT`); every other session is disconnected by the postmaster restart.

## What was ruled out

- The explicit flush inside the transaction survives the same error (its WARNING is printed and
  the flags are set); only the COMMIT-time run aborts.
- The `DO` block's own `EXCEPTION` handler (registry `UPDATE` in an `EXCEPTION` branch) runs fine
  on its own, and so does a `DELETE` through the IMV root that raises inside a plain `DO` block.
- Not every COMMIT-time flush failure: the review of rounds 3 + 4 saw one (`relation ... does not
  exist`, from the swap's orphan heal) print its WARNING without a crash.

## Fix direction (hypothesis to falsify)

The COMMIT-time flush runs from a deferred trigger during pre-commit; an error caught by the
`DO` block's subtransaction there, with Rust frames of the flush (`run_as_flush`, an open
`Spi::connect_mut`) around it, ends in an abort, plausibly a panic during unwinding or an
invariant pgrx asserts on subtransaction end. Get a backtrace (core dump with the probe above),
then make the COMMIT-time path survive a caught error like the explicit one does.
