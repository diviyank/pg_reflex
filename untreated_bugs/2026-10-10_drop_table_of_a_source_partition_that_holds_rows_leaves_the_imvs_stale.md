# `DROP TABLE` of a partition of a partitioned source that holds rows leaves every IMV stale — S1 (SILENT wrong result)

Pre-existing. Found on pg16 while fixing writes addressed to partitions on the 1.12.0 fold branch
`fix/fold-g8b1-partitions`; not caused by it and not fixed there.

## Reproduction (pg16, every statement committed)

```sql
CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a', 'b');
CREATE TABLE s_pe PARTITION OF s FOR VALUES IN ('e');
CREATE TABLE s_pd PARTITION OF s DEFAULT;
INSERT INTO s VALUES (1,'a',1),(2,'e',2),(3,'e',5),(6,'p',7);
SELECT create_reflex_ivm('x', 'SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM s GROUP BY g');
SELECT create_reflex_ivm('t', 'SELECT COUNT(*) AS c, SUM(v) AS sv FROM s');
SELECT create_reflex_ivm('k', 'SELECT id, g, v FROM s', 'id, g');
DROP TABLE s_pe;          -- x keeps group 'e', t keeps its rows, k keeps ids 2 and 3
```

## Expected vs actual

Expected: the IMVs equal a fresh run of their query (no row of `s_pe`). Actual, IMMEDIATE (the
only mode run): grouped aggregate, MIN/MAX, ungrouped aggregate, filtered aggregate, keyed and
keyless passthrough, partitioned aggregate and partitioned keyed passthrough all still hold the
dropped partition's rows (bidirectional `EXCEPT ALL` non-empty for the 9 IMVs of the fixture of
`src/tests/pg_test_v112_partition_dml.rs`, with the step added by hand; the step is not in the
committed matrix; run once, on an earlier state of that branch). No error, no warning;
`known_stale` was not read. After the next writes through the source, 7 of the 9 were still wrong
(the two unpartitioned passthroughs without an aggregate were not).

`ALTER TABLE s DETACH PARTITION s_pe; DROP TABLE s_pe;` in one transaction is correct (it is a
step of the committed matrix, IMMEDIATE and DEFERRED).

## What is established and what is not

Established: the outcome above. The mechanism is read from the code, not traced:
`__reflex_on_ddl_command_end` queues the partition root for the COMMIT-time partition flush on
`ALTER TABLE` and `CREATE TABLE` only, and `__reflex_on_sql_drop` handles a dropped table only when
it is a registered source or an IMV target (`src/lib.rs`), so a dropped partition queues nothing.
Not established: DEFERRED mode; whether a later partition change of the same source (which does
queue the root) removes the rows through the snapshot diff; `DROP TABLE` of a sub-partitioned
branch.

## Fix direction (hypothesis to falsify)

In `__reflex_on_sql_drop`, queue the partition root of a dropped table that appears in
`__reflex_source_partition_snapshot` (the table is gone when the trigger runs, so the root has to
come from the snapshot, not from the catalog), and let the existing flush handle the `Drop`
action: the child is gone, so the unpartitioned IMVs take the bound probe or a full reconcile.
The event trigger is bootstrap DDL and needs its migration twin.
