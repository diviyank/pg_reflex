# An aggregate whose WHERE is the bound of a two-column RANGE partition goes stale on an UPDATE that moves a row out of it — S1 (SILENT wrong result)

Pre-existing as far as known (the predicate path is untouched by 1.12.0; not run on 1.11.5).
Met on pg18 while proving the remedy of the partition-source refusal: the predicate
`pg_get_partition_constraintdef` gives for a partition of a table partitioned on two columns is
not maintained, on a plain table as well. The refusal therefore prints no such predicate.

## Reproduction (pg18, IMMEDIATE, a plain table, one transaction)

```sql
CREATE TABLE t(id INT NOT NULL, g TEXT, v INT NOT NULL);
INSERT INTO t VALUES (1,'a',1),(2,'b',5),(102,'b',7),(150,'a',9);
SELECT create_reflex_ivm('x', $$SELECT COUNT(*) AS c, SUM(v) AS sv FROM t
  WHERE ((id IS NOT NULL) AND (v IS NOT NULL)
    AND ((id > 0) OR ((id = 0) AND (v >= 0)))
    AND ((id < 100) OR ((id = 100) AND (v < 0))))$$);
UPDATE t SET v = v + 10;                      -- x correct
UPDATE t SET id = id + 100 WHERE id = 2;      -- the row leaves the predicate
TABLE x;                                      -- still counts it: (2, 26), the query gives (1, 11)
```

## Expected vs actual

Expected: `x` equals its query. Actual: wrong after the second UPDATE and after every later
write (`UPDATE … SET id = id - 100`, `DELETE`); no error. The same over the partitioned table
with `partition_by => ARRAY[]::text[]`, and for the grouped shape (`GROUP BY g`).

## What is established and what is not

Established (a temporary probe, bidirectional `EXCEPT ALL` after each statement): the outcome
above; the same four statements are correct with `WHERE (id < 100) OR ((id = 100) AND (v < 0))`
alone, with `WHERE id < 100`, and with no WHERE. The single-column bounds the partition refusal
prints (LIST, RANGE, DEFAULT, HASH, a leaf below a branch) are maintained through rows entering
and leaving them. Not established: which conjunct, or which combination, is misjudged (the
UPDATE changes `id`, a column only the WHERE reads, so the filter-aware skip of unchanged
rows is the first suspect); DEFERRED on a plain table; the passthrough shape.

## Fix direction (hypothesis to falsify)

Reduce the predicate to the smallest one that fails, then check what
`collect_imv_relevant_where` keeps for it and what the UPDATE path compares: a row that passes
the stored per-source filter before the UPDATE and fails it after must be retracted.
