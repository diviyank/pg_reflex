# Ungrouped aggregate IMV is wrong after a partition is created and filled in one transaction — S1 (SILENT wrong result)

Pre-existing; no TRUNCATE involved. Found on pg16 by the truncate matrix of
`src/tests/pg_test_v112_deferred_partition.rs` on branch `fix/fold-g8a-deferred-partition`, which
skips this one (step, IMV) pair (`V112_DP_KNOWN_WRONG`).

## Reproduction (pg16; the last two statements in ONE transaction, then COMMIT)

```sql
CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a', 'b');
CREATE TABLE s_pc PARTITION OF s FOR VALUES IN ('c') PARTITION BY RANGE (id);
CREATE TABLE s_pc1 PARTITION OF s_pc FOR VALUES FROM (0) TO (100);
CREATE TABLE s_pc2 PARTITION OF s_pc FOR VALUES FROM (100) TO (200);
CREATE TABLE s_pd PARTITION OF s DEFAULT;
INSERT INTO s VALUES (1,'a',1),(2,'a',2),(3,'b',5),(4,'c',7),(104,'c',9),(6,'p',7);
SELECT create_reflex_ivm('total', 'SELECT COUNT(*) AS c, SUM(v) AS sv FROM s');
-- one transaction:
CREATE TABLE s_pi PARTITION OF s FOR VALUES IN ('i');
INSERT INTO s VALUES (241,'i',1),(242,'i',2);
```

## Expected vs actual

Expected: `total` = `(8, 34)`. Actual: one wrong row (2 mismatches in the bidirectional
`EXCEPT ALL`), IMMEDIATE and DEFERRED; the values were not recorded. The six other IMVs of the
same fixture (grouped SUM/COUNT, grouped MIN/MAX, keyed and keyless passthrough, partitioned
aggregate and passthrough) are correct after the same transaction. Creating the partition in one
transaction and inserting in the next is correct for `total` too.

## What is established and what is not

Established: the outcome above, and that it does not depend on a TRUNCATE (the same transaction
followed by `TRUNCATE s_pi; INSERT ...` gives the same single wrong IMV). Not established: the
cause. The INSERT is applied by the statement trigger, then the COMMIT-time partition flush
handles the new partition (`reflex_flush_partitions_impl`, the unpartitioned branch:
`reflex_apply_partition_delta` or a full reconcile); a double application of the partition's rows
to the single-row aggregate would explain a wrong `total` with correct grouped IMVs only if the
grouped ones take another branch (the fresh-partition skip?). Not traced.

## Fix direction (hypothesis to falsify)

Trace which branch of the partition flush an ungrouped aggregate takes for a partition created in
the flushing transaction, and whether it applies rows the statement trigger already applied. Pin
with the reproduction, IMMEDIATE and DEFERRED, then drop the pair from `V112_DP_KNOWN_WRONG`.
