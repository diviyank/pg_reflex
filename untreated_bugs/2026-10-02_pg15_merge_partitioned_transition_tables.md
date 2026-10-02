# PG15: MERGE into a partitioned source gives the UPDATE trigger INSERTed rows — S2 (PostgreSQL 15 only)

Found in r5 fix round 1 (`pg_smd_immediate_passthrough_merge` failed only on pg15 with a 23505 on
the IMV's leaf index). A PostgreSQL 15 transition-capture defect, not a pg_reflex one; pg16/17/18
behave correctly.

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
Any IMMEDIATE IMV over such a source maintained from that MERGE applies wrong deltas: a passthrough
fails with 23505, an aggregate can diverge. The test is gated off pg15.

## Directions
Document (MERGE with both UPDATE and INSERT actions on a multi-level partitioned source is unsupported
on PG15 for IMMEDIATE IMVs; use DEFERRED, separate statements, or PG16+), or detect MERGE on PG15 and
fall back to a reconcile of the touched partitions. Check whether the newest 15.x minor fixes it.
