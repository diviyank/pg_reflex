# DROP of a source partition, then of its root in a later statement of the same transaction, leaves a partition-queue row for a vanished root — S4 (unverified)

Follow-up of the 1.12.5 DROP-partition fix (branch `fix/drop-source-partition-queues-root`).
1.12.5's `__reflex_on_sql_drop` queues the root of a dropped source partition in
`__reflex_partition_pending` so the COMMIT-time partition flush removes its rows from the IMVs.
The fix removed a "root dropped in the same statement" guard; a root dropped in the **same**
statement is handled (tests: `DROP ... CASCADE`, `DROP TABLE a, b, root`, `DROP SCHEMA CASCADE`).

## Suspected shape

```sql
BEGIN;
DROP TABLE src_p1;          -- queues root src
DROP TABLE src;             -- root vanishes (its IMVs are dropped or refuse, per drop_ivm rules)
COMMIT;                     -- the queue row for src remains or the flush errors on a missing root
```

Same question for the `ddl_command_end` DETACH path that also queues a root, followed by a DROP
of the root in the same transaction. A second, related edge: a registered source spelling that
`__reflex_reads_relation` matches but the `depends_on` forms do not, which can queue a root no
IMV reads.

## What was ruled out

- Same-statement drops of partition and root: covered by tests, no queue row is left.
- Nothing here was reproduced; the shape is inferred from the removed guard.

## Severity

S4 if confirmed: a stale queue row (flush WARNING on every COMMIT, or a doctor finding), no wrong
IMV data — the IMVs over a dropped root are gone with it.

## Fix direction

Reproduce first. If the row survives: have the partition flush drop queue rows whose `source_root`
no longer resolves (`to_regclass` NULL) with an INFO, rather than failing, and make sure
`reflex_doctor` reports and clears such rows with a remedy that converges.
