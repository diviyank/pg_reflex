# DEFERRED IMV over a CTE whose body reads two tables: both written in one transaction, the parent IMV is not refreshed at COMMIT — S1 (SILENT wrong result)

Pre-existing: same outcome on `8ef9743` and on the 1.12.0 fold branches. Found by the adversarial
review of the G4 fold (cell `rd_ctl_ctejoin`, real COMMIT, pg18). No expression subquery is
involved; the CTE-with-subquery forms fail identically.

## Reproduction (pg18)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT, w INT);
INSERT INTO s(g,v,w) VALUES ('a',1,2),('a',2,NULL),('a',3,3),('a',NULL,NULL),('b',5,NULL),
  ('b',9,1),('n',NULL,4),('z',0,5),('m',-4,2),('k',6,7);
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, label TEXT, v INT);
INSERT INTO t(g,label,v) VALUES ('a','A',1),('b','B',2),('c','C',3),('m','M',4),('zz','ZZ',5),('a','A2',0);
SELECT create_reflex_ivm('v', 'WITH c AS (SELECT m.g, m.v FROM s m JOIN t j ON j.g = m.g)
  SELECT g, SUM(v) AS sv, COUNT(*) AS c FROM c GROUP BY g', NULL, 'UNLOGGED', 'DEFERRED');
BEGIN;
INSERT INTO s(g,v,w) VALUES ('a',10,1),('c',7,NULL),('q',3,2),('b',8,3);
INSERT INTO t(g,label,v) VALUES ('z','Z',0),('k','K',1);
INSERT INTO t(g,label,v) VALUES ('b','B2',1);
COMMIT;
-- query: (a,32,10) (b,44,6) (c,7,1) (k,6,1) (z,0,1)
-- IMV v: still (a,12,8) (b,14,2)
```

## Expected vs actual

Expected: after COMMIT the IMV equals the query. Actual: the IMV keeps its pre-transaction rows,
no error, no stale flag reported by the review.

## What is established and what is not

Established: wrong with a real COMMIT when both tables of the CTE body are written in one
transaction. One statement per transaction is correct; a CTE over one table is correct; the same
join without the CTE is correct. Flushing by hand inside one transaction, the sub-IMV `v__cte_c`
is right after the flush and the parent `v` is never refreshed.

Not established: the exact step that drops the propagation.

## Fix direction (hypothesis to falsify)

When two sources of one IMV carry pending deltas in the same flush, the cross-source guard
rebuilds the sub-IMV (`reflex_reconcile` through `__reflex_deferred_rebuild`) instead of applying
deltas. That rebuild appears not to reach the sub-IMV's dependents. Reproduce with a real COMMIT
(a second connection; a hand flush inside one transaction is a different path), then make the
rebuild of a sub-IMV propagate to its dependent IMVs, or rebuild them in graph order.
