# MAX with `IN (subquery) OR …` in WHERE goes wrong when the subquery's table is written — S1 (SILENT wrong result)

Pre-existing: same outcome on the 1.11.5 base and on the 1.12.0 branch unless stated. Found by the 1.12.0 whole-branch review; fixture and steps are the probes it ran (pg17).

## Reproduction

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT, t TEXT, w INT, n NUMERIC);
INSERT INTO s(g,v,t,w,n) VALUES ('a',1,'Été',2,1.25),('a',2,'x',NULL,2.5),('a',3,'Été',3,3.75),
  ('a',NULL,NULL,NULL,NULL),('b',5,'Hiver',NULL,6.25),('b',9,'Été',1,1),('n',NULL,'Été',4,NULL),
  ('z',0,'k',5,0),('m',-4,'q',2,-5);
CREATE TABLE s2(id SERIAL PRIMARY KEY, g TEXT, label TEXT, v INT);
INSERT INTO s2(g,label,v) VALUES ('a','A',1),('b','B',2),('c','C',3),('m','M',4),('zz','s2',5);
SELECT create_reflex_ivm('v', 'SELECT g, MAX(v) AS mx FROM s WHERE g IN (SELECT g FROM s2 WHERE label <> ''x'' OR v > 2) OR w IS NULL GROUP BY g');
```

The review then ran a ten-step DML sequence over both tables (not recorded here step by step).
The IMV matched the query through step 6. At step 7, `DELETE FROM s2 WHERE g = 'm'`, the IMV
held `(a, 1)` where the query gives `(a, 2)`. At step 10, `DELETE FROM s`, the IMV kept a row
`(a, NULL)` where the query is empty.

## Expected vs actual

Expected: the IMV equals the query after every write to `s` or `s2`. Actual: wrong MAX and a
phantom group, with no error.

## What is established and what is not

Established: the wrong rows above on base and head. The same shape without `OR w IS NULL` was
correct for MAX and SUM (head only). Not established: the cause, and whether SUM/COUNT with the
OR are affected.

## Fix direction (hypothesis to falsify)

A write to the subquery's table changes membership of rows of `s` that the delta does not
contain; with a disjunction the affected-group computation may miss groups whose membership
changes only through one branch. Either recompute affected groups from the full predicate on a
subquery-table write, or reject a top-level OR that has a subquery branch. Reproduce the minimal
step sequence first.
