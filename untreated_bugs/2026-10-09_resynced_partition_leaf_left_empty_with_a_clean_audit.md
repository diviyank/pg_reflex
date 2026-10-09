# A partition leaf of an IMV recreated by `reflex_sync_partitions` stays empty and the audit is clean — S2 (wrong data after a manual or automatic sync; needs a leaf dropped by hand first)

Pre-existing. Residual of the 1.12.0 fix that made the `partition-mirror` audit remedy refill the
leaf (`src/audit/checks_b_drift.rs`: the printed fix is now
`SELECT reflex_sync_partitions('<imv>'); SELECT reflex_reconcile_partition('<imv>', '', '<source partition>');`).

## Reproduction (pg16)

```sql
CREATE TABLE t(id INT NOT NULL, season TEXT NOT NULL, v INT) PARTITION BY LIST (season);
CREATE TABLE t_p0 PARTITION OF t FOR VALUES IN ('e', 'f');
CREATE TABLE t_p1 PARTITION OF t FOR VALUES IN ('y');
CREATE TABLE t_pd PARTITION OF t DEFAULT;
INSERT INTO t VALUES (1,'e',10),(2,'e',40),(3,'f',5),(4,'y',2),(5,'z',3);
SELECT create_reflex_ivm('v', 'SELECT season, SUM(v) AS sv, COUNT(*) AS c FROM t GROUP BY season',
                         NULL, NULL, 'IMMEDIATE', NULL, ARRAY['season']);
DROP TABLE "__reflex_intermediate_v_t_p0";
SELECT reflex_sync_partitions('v');      -- only the first half of the printed remedy
SELECT reflex_audit('v');                -- no partition-mirror, no archive_residue finding
INSERT INTO t VALUES (6,'e',4);
-- v differs from its query for group 'e' (2 mismatches in the bidirectional EXCEPT ALL;
-- the values were not recorded)
```

## Expected vs actual

Expected: either the sync refills a leaf it had to recreate while the other side of the IMV
(target or intermediate) still holds that partition's rows, or the audit keeps reporting the IMV
until the partition is reconciled. Actual: the intermediate leaf is recreated empty, the target
leaf keeps its rows, the audit is clean, and the next write to a group of that leaf publishes
aggregates computed from the empty intermediate.

## What is established and what is not

Established: the outcome above (this is the sequence the 1.12.0 test
`v112_dp_dropped_intermediate_leaf_remedy_restores_the_data_*` fails on when the reconcile half
of the remedy is removed), and that the full printed remedy converges to a correct IMV. A dropped
TARGET leaf is different: after the sync the `archive_residue` check reports the empty partition
and its own remedy refills it. Not established: whether the automatic sync the DDL event trigger runs on
the next partition change of the source recreates the leaf the same way (expected, not tried);
whether `reflex_sync_partitions` can tell a
recreated leaf (its sibling on the other side exists and holds rows) from a new one cheaply.

## Fix direction (hypothesis to falsify)

In `reflex_sync_partitions_impl`, when an intermediate child is created for a source partition
whose target child already exists and is not empty (or the reverse), reconcile that partition in
the same call. Or add an audit check: intermediate leaf empty while its target leaf holds rows.
