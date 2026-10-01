# Two sessions flushing different sources of one DEFERRED IMV can deadlock at COMMIT — S3 (aborted COMMIT, retryable)

Pre-existing in shape; the 1.11.5 COMMIT-time rebuild pass adds a lock taken between two
source flushes. No data is lost: PostgreSQL detects the cycle and aborts one COMMIT (40P01).

## Mechanism

Each source flush takes `pg_advisory_xact_lock(hashtext('reflex_flush:<src>'))`
(src/trigger/deferred.rs:847) and keeps it to the end of the transaction. A transaction that
wrote two sources S1 and S2 of a join IMV V flushes S1, lists V for the cross-source guard, and
the rebuild pass that runs after S1's flush rebuilds V (`rebuild_for_cross_source_guard`,
deferred.rs:696): it locks V (TRUNCATE / `LOCK TABLE … EXCLUSIVE`, and V's advisory lock in
`rebuild_target_rows`, src/rebuild_diff.rs:118, when V has dependents) **before** it flushes S2
and requests `reflex_flush:S2`.

A second transaction that wrote only S2 takes `reflex_flush:S2` and then, maintaining V, needs
V's advisory lock (deferred.rs:1436) and a `RowExclusiveLock` on V. If it arrives while the
first holds `reflex_flush:S1` and is about to lock V, the two wait on each other:

```
A: holds reflex_flush:S1, holds V (rebuild) → waits reflex_flush:S2
B: holds reflex_flush:S2                     → waits V
```

## Reproduction (pg17, 1.11.5; three psql sessions)

```sql
CREATE TABLE s1 (k INT PRIMARY KEY, v INT); CREATE TABLE s2 (k INT PRIMARY KEY, v INT);
INSERT INTO s1 SELECT g, g FROM generate_series(1,10) g;
INSERT INTO s2 SELECT g, g FROM generate_series(1,10) g;
SELECT create_reflex_ivm('dv', 'SELECT s1.k, s1.v, s2.v AS w FROM s1 JOIN s2 ON s2.k = s1.k',
  'k', NULL, 'DEFERRED');
-- X (only to order A before B): BEGIN; LOCK TABLE dv IN ACCESS SHARE MODE; SELECT pg_sleep(4); COMMIT;
-- A, 1 s later: BEGIN; UPDATE s1 SET v = v + 1 WHERE k = 1; UPDATE s2 SET v = v + 1 WHERE k = 2; COMMIT;
-- B, 1 s later: BEGIN; UPDATE s2 SET v = v + 1 WHERE k = 3; COMMIT;
```

A's COMMIT fails: `deadlock detected — Process A waits for ExclusiveLock on advisory lock …;
blocked by process B. Process B waits for RowExclusiveLock on relation dv; blocked by
process A`, context `reflex_flush_deferred(NEW.source_table)`. B commits; `dv` is correct and
not stale. Without X the window is the time between A's S1 flush and its S2 flush, so it
needs concurrent writers to different sources of one DEFERRED join IMV.

## Fix direction

Take the per-source flush locks in a global order: the first flush of a transaction locks
`reflex_flush:<src>` for every source with a pending row, sorted by name, before doing any
work (the later flushes re-take locks they already hold). Then no transaction holds one
source's flush lock while waiting for another's. Re-run the repro (no 40P01) and
`tests/test_concurrent_flush.sh`.
