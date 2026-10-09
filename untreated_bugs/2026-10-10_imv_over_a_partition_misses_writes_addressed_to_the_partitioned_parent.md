# IMV whose source is a partition misses writes addressed to the partitioned parent — S1 (SILENT wrong result)

Pre-existing. The mirror image of the 1.12.0 fix for writes addressed to a partition
(`src/source_partition_writes.rs`), found while testing it on pg16; not fixed there.

## Reproduction (pg16, one transaction, DEFERRED flushed with `reflex_flush_deferred`)

```sql
CREATE TABLE s(id INT, g TEXT, v INT) PARTITION BY LIST (g);
CREATE TABLE s_a PARTITION OF s FOR VALUES IN ('a');
INSERT INTO s VALUES (1,'a',1);
SELECT create_reflex_ivm('x', 'SELECT COUNT(*) AS c, SUM(v) AS sv FROM s_a');   -- IMMEDIATE
INSERT INTO s VALUES (2,'a',2);       -- routed to s_a
SELECT c FROM x;                      -- 1, s_a holds 2 rows
```

## Expected vs actual

Expected: `x` counts the 2 rows of `s_a`. Actual: 1, IMMEDIATE; the same with a DEFERRED IMV
created next (created over 2 rows, still 2 after a third row is inserted through `s` and
`reflex_flush_deferred('s_a')`). No error, no warning. `INSERT INTO s_a` is maintained.

## What is established and what is not

Established: the two outcomes above (a temporary probe, not a committed test). PostgreSQL fires
the statement triggers of the table the statement names: the triggers of `s_a` do not fire for a
statement on `s`. Not established: UPDATE / DELETE through the parent, a row moved into or out
of `s_a` by an UPDATE of the partition key on the parent, a sub-partitioned source partition,
`TRUNCATE s` (the truncate trigger of `s_a` does fire for it, PostgreSQL fires TRUNCATE triggers
per truncated relation).

## Fix direction (hypothesis to falsify)

The transition tables of a statement on an ancestor hold the rows of every partition, in the
ancestor's column order and without `tableoid`, so the partition's trigger function cannot be put
on the ancestor as is. Either reject at create a source that is a partition (remedy: read the
partitioned parent with a `WHERE` on the partition key), or install on each ancestor a trigger
that filters its transition rows with the partition's bound before handing them to the
partition's maintenance. The first is simple and loud.
