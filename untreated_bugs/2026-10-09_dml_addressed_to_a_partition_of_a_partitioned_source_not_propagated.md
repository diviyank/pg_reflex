# INSERT / UPDATE / DELETE addressed to a partition of a partitioned source is not propagated — S1 (SILENT wrong result)

Pre-existing (the statement triggers of a source have always been installed on the table the
query names). Found on pg16 while fixing the TRUNCATE-of-one-partition bug on the 1.12.0 fold
branch `fix/fold-g8a-deferred-partition`; same mechanism, different statement kind, left out of
that fix on purpose.

## Reproduction (pg16, every statement committed)

```sql
CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a', 'b');
CREATE TABLE s_pd PARTITION OF s DEFAULT;
INSERT INTO s VALUES (1,'a',1),(2,'a',2),(3,'b',5),(6,'p',7);
SELECT create_reflex_ivm('x', 'SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM s GROUP BY g');
SELECT create_reflex_ivm('k', 'SELECT id, g, v FROM s', 'id, g');
INSERT INTO s_pa VALUES (301,'a',1);      -- x and k unchanged
DELETE FROM s_pa WHERE id = 1;            -- x and k unchanged
UPDATE s_pa SET v = v + 5;                -- x and k unchanged
```

## Expected vs actual

Expected: the IMVs equal a fresh run of their query. Actual: no IMV of `s` moves, IMMEDIATE and
DEFERRED, grouped aggregate, MIN/MAX, ungrouped aggregate, keyed and keyless passthrough,
partitioned and unpartitioned IMV (7 IMVs x 2 modes x 3 statements, bidirectional `EXCEPT ALL`,
all wrong). No error, no warning, `known_stale` stays false, `reflex_audit` not run.

## What is established and what is not

Established: PostgreSQL fires a statement-level trigger only for the table the statement names;
the `__reflex_trigger_{ins,del,upd}_on_<source>` triggers sit on the partitioned root, so a
statement naming a partition fires none of them. The same statements addressed to `s` are
correct. Since 1.12.0 a `TRUNCATE` of a partition is propagated (truncate triggers are installed
on every table of the partition tree, `src/source_truncate.rs`). Not established: whether a
leaf-level row trigger or a leaf-level statement trigger with transition tables is the cheaper
capture; how the leaf triggers would coexist with the partition-aware dispatch (a row moved
between partitions by an UPDATE on the root must not be captured twice).

## Fix direction (hypothesis to falsify)

Install the INSERT / DELETE / UPDATE statement triggers on every table of the partition tree as
`src/source_truncate.rs` does for TRUNCATE (same sync points: create, `reflex_rebuild_triggers`,
the partition flush, drop), after proving that a statement on the root does not also fire them
(for DML, unlike TRUNCATE, PostgreSQL fires only the named table's statement triggers). Check
`ATTACH PARTITION` / `DETACH PARTITION` of a table that carries statement triggers with transition
tables (PostgreSQL restricts transition tables on partitions for ROW triggers; the statement-level
case was not tried). Until then, refuse loudly: a `BEFORE` statement trigger on
each partition that raises, naming the root to write to. Pin with the matrix of
`src/tests/pg_test_v112_deferred_partition.rs`.
