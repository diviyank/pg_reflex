# A source attached as a partition after its IMV was created misses writes addressed to the parent — S1 (SILENT wrong result)

Pre-existing. The residual of the report about an IMV over a partition: `create_reflex_ivm`
now refuses a source that is a partition (a leaf, a sub-partitioned branch, a DEFAULT
partition; `src/create_ivm/source_relations.rs`), with the remedy of reading the partitioned
table with a `WHERE` on the partition key. The refusal looks at the source when the IMV is
created. A plain table that becomes a partition afterwards is not seen.

## Reproduction (pg17, one transaction; the same with a DEFERRED IMV flushed with `reflex_flush_deferred`)

```sql
CREATE TABLE lone(id INT, g TEXT, v INT);
INSERT INTO lone VALUES (1,'a',1);
SELECT create_reflex_ivm('x', 'SELECT COUNT(*) AS c, SUM(v) AS sv FROM lone');
CREATE TABLE par(id INT, g TEXT, v INT) PARTITION BY LIST (g);
ALTER TABLE par ATTACH PARTITION lone FOR VALUES IN ('a');   -- no error, no warning
INSERT INTO par VALUES (2,'a',2);                             -- routed to lone
SELECT c FROM x;                                              -- 1, lone holds 2 rows
```

## Expected vs actual

Expected: `x` equals its query, or the `ATTACH` is refused or warned about. Actual: `x` is
stale after `INSERT`, `UPDATE` and `DELETE` addressed to `par`, IMMEDIATE and DEFERRED; a later
write addressed to `lone` is applied on top of the stale contents. No error, no warning.

## What is established and what is not

Established: the outcomes above (a temporary probe with the bidirectional `EXCEPT ALL` oracle
after each statement, pg17). PostgreSQL fires the statement triggers of the table a statement
names, so the triggers of `lone` do not fire for a statement on `par`. Not established: what
`reflex_audit` and `reflex_doctor` report for such an IMV; `reflex_rebuild_imv` on it (the
create it runs is now refused, so the rebuild raises); `TRUNCATE par`; pg15, pg16, pg18.

## Fix direction (hypothesis to falsify)

In the DDL event trigger that already follows `ATTACH PARTITION` for partitioned sources,
refuse (or flag stale, with the same remedy as the create-time message) an `ATTACH` whose
partition is itself a registered source; and have the audit report a registered source with
`pg_class.relispartition`. Installing triggers on the ancestors that filter their transition
rows by the partition bound would keep the shape, at the cost of a second maintenance path.
