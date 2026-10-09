# TRUNCATE of one partition of a partitioned source is not propagated — S1 (SILENT wrong result)

Pre-existing: same outcome on commit 8ef9743 and on the `fix/fold-g2-having` head, pg16. Found by
the adversarial review of the HAVING fold (its P2) with a real IMV and the bidirectional oracle;
not re-run when this report was written.

## Reproduction

```sql
CREATE TABLE s(id INT, g TEXT, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a', 'b');
CREATE TABLE s_pd PARTITION OF s DEFAULT;
INSERT INTO s VALUES (1,'a',1),(2,'a',2),(3,'a',3),(4,'b',5),(6,'p',7);
SELECT create_reflex_ivm('x', 'SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM s GROUP BY g');
TRUNCATE s_pa;
-- x still holds groups a and b
```

## Expected vs actual

Expected: `x` equals a fresh run of its query (group `p` only). Actual: the rows of the truncated
partition stay in the IMV.

## What is established and what is not

Established by the review: wrong with and without HAVING, for aggregate and passthrough IMVs,
IMMEDIATE and DEFERRED. INSERT, UPDATE, a partition-key move and DELETE on the same source are
correct, and so is `TRUNCATE` of an unpartitioned source. Not established: the cause. A statement
trigger on the partitioned parent does not fire for a `TRUNCATE` issued on a partition, which
would explain it; not traced.

## Fix direction (hypothesis to falsify)

Install the TRUNCATE trigger on every partition (and on partitions attached later), or have the
partition-maintenance path detect a truncated leaf. Pin with the reproduction above, IMMEDIATE and
DEFERRED, aggregate and passthrough.
