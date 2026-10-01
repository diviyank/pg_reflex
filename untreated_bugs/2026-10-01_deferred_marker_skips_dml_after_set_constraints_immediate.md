# DEFERRED: DML after a mid-transaction `SET CONSTRAINTS ALL IMMEDIATE` is skipped for an IMV already rebuilt in that transaction — S1 (silent, narrow trigger)

Once a DEFERRED IMV has been rebuilt by the COMMIT-time pass — after a source TRUNCATE (new in
1.11.5) or by the multi-source cross-source guard (pre-existing) — it is recorded in the
transaction-local marker `__reflex_deferred_reconciled_batch`, and every flush of the same
transaction skips every delta staged for it (`flush_staged_deltas`,
src/trigger/deferred.rs:978-1003). The marker assumes the rebuild ran at COMMIT, after the last
write. `SET CONSTRAINTS ALL IMMEDIATE` breaks that: the queued flush fires at once, the IMV is
rebuilt and marked mid-transaction, and the transaction's later writes are then dropped for it.
The IMV is not flagged stale. Documented as a limit on `rebuild_truncated_imvs`
(deferred.rs:263).

## Reproduction (1.11.5, pg17)
```sql
CREATE TABLE s (k INT PRIMARY KEY, v INT);
INSERT INTO s SELECT g, g FROM generate_series(1, 100) g;
SELECT create_reflex_ivm('sv', 'SELECT k, v FROM s', 'k', 'LOGGED', 'DEFERRED');
BEGIN;
TRUNCATE s;
INSERT INTO s SELECT g, g FROM generate_series(1, 50) g;
SET CONSTRAINTS ALL IMMEDIATE;
INSERT INTO s VALUES (1000, 1000);
COMMIT;
SELECT count(*) FROM s;   -- 51
SELECT count(*) FROM sv;  -- 50, known_stale = f
```

## Ruled out
Ordinary transactions (no `SET CONSTRAINTS`) are not affected: the rebuild runs at COMMIT and
reads the final state (`pg_toj_deferred_truncate_*`, `pg_rbc_*` tests).

## Fix direction
Scope the skip to deltas staged before the rebuild: record, with the marker, the staging
position at rebuild time (e.g. the max staged row id / xmin+cmin), skip only rows at or before
it, and flush the later ones incrementally. Failing that, drop the IMV from the marker and
re-list it when a write after its rebuild stages a delta for it. Must keep the
diamond / nested-flush tests green.
