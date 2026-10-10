# The audit is clean while `__reflex_delta_<source>` or `__reflex_deferred_pending` is missing; every write to the source then raises — S2 (loud)

Pre-existing: identical on `6946400` (1.11.5) and on the 1.12.0 integration branch at `0324f47`.
Found by review seat R3 of the 1.12.0 full re-review (pg16) while deleting one internal object at
a time under each IMV kind (`head2/r3_rem_*.txt` under `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-out/`).
The reduction below was run on pg16 on `0324f47` and on `fix/full-review-r3` (probe `zz_file_probe`, kept at `/private/tmp/rfx-remote/b-r3fix-file-probe.rs` with its outputs `b-r3fix-fp-{base,head}.log`): same outcome on both (the `__reflex_delta_<source>` half; the `__reflex_deferred_pending` half is the review's).

## Reproduction (pg16)

```sql
CREATE TABLE t(id INT, g TEXT, v INT);
SELECT create_reflex_ivm('d', 'SELECT g, SUM(v) AS sv FROM t GROUP BY g', NULL, NULL, 'DEFERRED');
DROP TABLE public."__reflex_delta_t";      -- or: DROP TABLE public.__reflex_deferred_pending CASCADE;
SELECT reflex_audit();                     -- OK, no findings
INSERT INTO t VALUES (1,'a',1);            -- ERROR: relation "__reflex_delta_t" does not exist
```

## Expected vs actual

Expected: an ERROR finding naming the missing relation and a remedy that restores it.
`reflex_rebuild_triggers('t')` answers `rebuilt 8 trigger DDL(s)` and does not recreate the staging
table: the next INSERT raises the same error. Actual: clean
audit, `reflex_doctor` returns no row, and every INSERT / UPDATE / DELETE on the source aborts the
writer's transaction. Loud, so no wrong data.

## What was ruled out

`staging-shape` (`src/audit/checks_a_catastrophic.rs`) only compares columns when the staging
table exists: it skips a missing one. The other 90-odd single-object deletions of the review either
were reported with a remedy that converges or were refused by PostgreSQL.

## Fix direction (hypothesis to falsify)

Add to `staging-shape` (or a sibling check) a finding when a DEFERRED IMV's source has no staging
table, and a global finding when `__reflex_deferred_pending` or its flush trigger is missing while
a DEFERRED IMV is registered. Prove the remedy in the test: run it, the audit is clean, a write
succeeds and the IMV is correct after COMMIT.
