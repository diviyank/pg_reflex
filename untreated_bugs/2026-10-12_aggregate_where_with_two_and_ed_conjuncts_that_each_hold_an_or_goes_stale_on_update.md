# An aggregate whose WHERE is two AND-ed conjuncts that each hold an OR goes stale on an UPDATE that moves a row out of it — S1 (SILENT wrong result)

Pre-existing: identical on 1.11.5 (`6946400`, PostgreSQL 18.6) and on the release head (pg17).
First met on pg18 while proving the remedy of the partition-source refusal, as the predicate
`pg_get_partition_constraintdef` gives for a partition of a table partitioned on two columns
(hence the earlier title, "two-column RANGE bound"). The partition is incidental: the table below
is a plain table, and the smallest failing predicate has no `IS NOT NULL` conjunct.

## Reproduction (IMMEDIATE, a plain table)

```sql
CREATE TABLE t(id INT NOT NULL, g TEXT, v INT NOT NULL);
INSERT INTO t VALUES (1,'a',1),(2,'b',5),(102,'b',7),(150,'a',9);
SELECT create_reflex_ivm('x', $$SELECT COUNT(*) AS c, SUM(v) AS sv FROM t
  WHERE ((id > 0) OR ((id = 0) AND (v >= 0)))
    AND ((id < 100) OR ((id = 100) AND (v < 0)))$$);
UPDATE t SET v = v + 10;
UPDATE t SET id = id + 100 WHERE id = 2;      -- the row leaves the predicate
TABLE x;                                      -- (2, 26); the query gives (1, 11)
```

## Expected vs actual

Expected: `x` equals its query. Actual: wrong after the second UPDATE, no error.

One probe, the same fixture and the same two UPDATEs for every predicate, run on both trees
(`logs/f-probe-base.log`: built as 1.11.5, PostgreSQL 18.6; `logs/a-probe-head.log`: release
head, pg17, in the ledger `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`). The
two trees agree on every line:

| WHERE | 1.11.5 | head |
|---|---|---|
| `(id IS NOT NULL) AND (v IS NOT NULL) AND ((id > 0) OR ((id = 0) AND (v >= 0))) AND ((id < 100) OR ((id = 100) AND (v < 0)))` | WRONG `(2,26)` for `(1,11)` | same |
| `((id > 0) OR ((id = 0) AND (v >= 0))) AND ((id < 100) OR ((id = 100) AND (v < 0)))` | WRONG `(2,26)` for `(1,11)` | same |
| `(id IS NOT NULL) AND ((id < 100) OR ((id = 100) AND (v < 0)))` | correct | correct |
| `(v IS NOT NULL) AND (id < 100)` | correct | correct |
| `(id IS NOT NULL) AND (id < 100)` | correct | correct |
| `(id > 0) AND (id < 100)` | correct | correct |
| `(id < 100) OR ((id = 100) AND (v < 0))` | correct | correct |
| `id < 100` | correct | correct |
| `id BETWEEN 1 AND 99` | correct | correct |
| `(id < 100) AND (v > 0)` | correct | correct |

## What is established and what is not

Established: the two failing rows above, on both trees; the smallest failing shape found is two
top-level conjuncts joined by AND, each of which contains an OR. One OR alone, one OR beside a
plain conjunct, and two plain conjuncts are maintained. From the earlier probe (pg18, release
branch, bidirectional `EXCEPT ALL` after each statement): the IMV stays wrong after every later
write (`UPDATE … SET id = id - 100`, `DELETE`); the same over the partitioned table with
`partition_by => ARRAY[]::text[]`, and for the grouped shape (`GROUP BY g`); the single-column
bounds the partition refusal prints (LIST, RANGE, DEFAULT, HASH, a leaf below a branch) are
maintained through rows entering and leaving them.

Not established: whether two OR conjuncts over one column only fail too (both failing predicates
read `id` and `v`); whether a single UPDATE (without the first `SET v = v + 10`) is enough; three
conjuncts; DEFERRED on a plain table; the passthrough shape. The probe source was not kept, only
its two logs: the predicates are in their `CONTEXT` lines.

## What was ruled out

- Not a 1.12.0 regression, and not the partition bound as such (plain table; no `IS NOT NULL`
  needed).
- Not an OR in WHERE in general, and not a row leaving a range in general (`id < 100`,
  `id BETWEEN 1 AND 99`, `(id > 0) AND (id < 100)` are correct).

## Consequence for the partition-source refusal

The refusal of a source that is itself a partition prints no predicate for a partition of a table
partitioned on several columns, because the predicate PostgreSQL gives has this shape.

## Fix direction (hypothesis to falsify)

The UPDATE changes `id`, which both conjuncts read. Check what `collect_imv_relevant_where` keeps
for a WHERE of this shape and what the UPDATE path compares: a row that passes the stored
per-source filter before the UPDATE and fails it after must be retracted. First step: find out
whether the stored filter is the whole conjunction or one conjunct of it. Until it is understood,
recomputing the IMV in full for a WHERE with more than one OR-holding conjunct is the safe
direction.
