# PG15: MERGE into a multi-level partitioned source gives the UPDATE trigger INSERTed rows, all IMV modes — S2 (PostgreSQL 15 only)

Found in r5 fix round 1 (`pg_smd_immediate_passthrough_merge` failed only on pg15 with a 23505 on
the IMV's leaf index). A PostgreSQL 15 transition-capture defect, not a pg_reflex one; pg16/17/18
behave correctly. Confirmed standalone, without pg_reflex installed (r5 fix-round-1 review): on
15.15 the UPDATE trigger's NEW table holds 1467 inserted ids, on 16.11 it holds 0.

## Reproduction (PostgreSQL 15.15, pgrx test instance)
Two-level partitioned source (`rcu_source`: LIST plan -> RANGE month, 2000 rows per plan) with an
AFTER UPDATE statement trigger logging its NEW transition table:
```sql
MERGE INTO src t USING (SELECT plan, m, id, v FROM src WHERE plan = 1 AND id <= 1600
                        UNION ALL SELECT 1, g % 12, 100000 + g, g FROM generate_series(1, 400) g) s
   ON t.plan = s.plan AND t.m = s.m AND t.id = s.id
 WHEN MATCHED THEN UPDATE SET v = t.v + 1
 WHEN NOT MATCHED THEN INSERT VALUES (s.plan, s.m, s.id, s.v);
```
The UPDATE trigger's NEW table holds 1600 rows of which 1467 are INSERTed ids (> 100000); its OLD
table and the INSERT trigger's NEW table (400) are right. On pg16+ the NEW table holds the 1600
updated rows. A single-level partitioned table and a plain table are not affected (probed).

## Effect
Every IMV over such a source maintained from that MERGE gets wrong deltas, whatever its mode:
- IMMEDIATE: the statement trigger applies the bad NEW table — a passthrough fails with 23505,
  an aggregate can diverge silently. `pg_smd_immediate_passthrough_merge` is gated off pg15.
- DEFERRED: the staging trigger stages the same bad NEW table, and the flush applies it — probe
  `zrv14` (DEFERRED, cold flush, `wipe_threshold = 100` so no rebuild hides it) ends with 3440
  mismatching rows on pg15. A flush that happens to rebuild the touched partitions masks it.

## Directions
Document: on PostgreSQL 15, a MERGE with both UPDATE and INSERT actions into a multi-level
partitioned source is unsupported for every IMV mode — split it into an UPDATE and an INSERT, or
upgrade to PostgreSQL 16+ (recommended). Possible guard: on PG15, detect MERGE (no direct signal
in a statement trigger; an UPDATE transition table whose NEW keys are not a permutation of OLD
keys is the symptom) and fall back to a reconcile of the touched partitions. Check whether the
newest 15.x minor fixes it.
