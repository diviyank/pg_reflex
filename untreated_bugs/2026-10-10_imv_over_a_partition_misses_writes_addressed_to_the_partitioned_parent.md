# IMV whose source is a partition (a leaf or a sub-partitioned branch) misses writes addressed to an ancestor — S1 (SILENT wrong result)

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

Established on pg17, IMMEDIATE and DEFERRED (flushed with `reflex_flush_deferred` on every
source), with the bidirectional `EXCEPT ALL` oracle after each statement, over
`s LIST (g)` with a leaf `s_a`, a sub-partitioned branch `s_b RANGE (id)` and its leaves:

| Statement | IMV over the leaf `s_a` | IMV over the branch `s_b` |
|---|---|---|
| `INSERT INTO s` | stale | stale |
| `UPDATE s` (no key change) | stale | stale |
| `UPDATE s SET g = ...` moving a row into or out of the partition | stale | not run |
| `DELETE FROM s` | stale | stale |
| `INSERT INTO` the source partition itself | maintained | maintained |
| `INSERT INTO` a leaf of the branch | — | maintained (the partitions of a source report to it) |
| `TRUNCATE s` | correct afterwards (the truncate trigger of the partition fires) | not run |

So a sub-partitioned branch read as a source has the same defect for a write addressed to the
root. PostgreSQL fires the statement triggers of the table the statement names: the triggers of
`s_a` do not fire for a statement on `s`.

Established: pg_reflex itself never creates an IMV or a sub-IMV over a partition. With a
create-time refusal of every source that has `pg_class.relispartition` in place, the whole
pg17 suite (3636 tests) passes but one.

Not established: pg15, pg16, pg18; `MERGE` and `COPY` addressed to the parent; a partition
attached to a parent after the IMV over it was created (the table becomes a partition under an
existing IMV, which no create-time check sees).

## A create-time refusal is written and withheld

The refusal (every create function, both modes, the partition named in FROM, JOIN, a CTE, a
derived table, a subquery or a set operation; a leaf, a branch or a DEFAULT partition; nothing
left behind) was written and tested, then left out of the branch: the existing test
`v112_pd_partition_that_is_also_a_source_feeds_both_imvs`
(`src/tests/pg_test_v112_partition_dml.rs`) creates an IMV over a partition on purpose and
writes to it through the partition only, where it is correct. That test pins the shape as
supported; refusing it is a product decision, not a side effect of a bug fix.

The message it gives, and the remedy it prints, which was created as written and compared after
every write (rows, an ungrouped aggregate, a grouped aggregate; LIST leaf, sub-partitioned
branch, RANGE leaves of the branch, DEFAULT partition, HASH partitions; writes through the
root, through the partition and through its neighbours, rows changing partition):

```
[reflex-unsupported] the source s_a is a partition of s: a write addressed to s does not fire
the triggers of s_a, and the IMV would miss it. Fix: read s and keep the rows of the partition
with a WHERE on the partition key: FROM s WHERE ((g IS NOT NULL) AND (g = 'a'::text)).
```

The root is `pg_partition_root`, the predicate `pg_get_partition_constraintdef`, which holds
the bounds of every ancestor. For a HASH partition the predicate is
`satisfies_hash_partition('<oid of the parent>'::oid, ...)`: maintained, but it names an OID,
which a dump and restore changes.

## Fix direction (hypothesis to falsify)

Either retire the test above and ship the refusal; or keep the shape and install on each
ancestor of a source partition a trigger that filters its transition rows with the partition's
bound before handing them to the partition's maintenance (the transition tables of a statement
on an ancestor hold the rows of every partition, in the ancestor's column order and without
`tableoid`, so the partition's trigger function cannot be put on the ancestor as is). A WARNING
at create that names the remedy closes nothing: the wrong result stays silent at write time.
