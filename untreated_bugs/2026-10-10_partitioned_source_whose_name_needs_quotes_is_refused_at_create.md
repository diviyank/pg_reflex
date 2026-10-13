# A partitioned source whose schema or name needs double quotes is refused at create — S3 (LOUD, by design until the bookkeeping is fixed)

1.12.0 refuses, with a tagged message, an IMV over a partitioned table that PostgreSQL prints
quoted (`"S".p`, `s."P"`, `"position"`, `s."select"`). This report tracks lifting the refusal.

The refusal covers exactly the partitioned sources registered under a spelling that needs its
quotes: an upper-case letter, a space, a leading digit, a reserved or column-name keyword. A
name PostgreSQL prints quoted but reads back bare (`é_part`, `my$part`, in any schema) is
registered bare, followed by the partition bookkeeping, and accepted
(`v112_sr_adm_partitioned_source_*`). For partitions (not the parent) whose names need quotes
see `2026-10-10_attached_partition_whose_name_does_not_read_back_unquoted_is_lost.md`.

## Why it is refused

Two families of readers match a source against `depends_on`, and they spell it differently:

- from `object_identity` (what `quote_ident` prints): the `sql_drop` hook, the ALTER TABLE
  branch of `__reflex_on_ddl_command_end` (`src/lib.rs`);
- from `nspname || '.' || relname`, unquoted: the partition root the same hook enqueues
  (`_parent`, `_part_root`), `__reflex_partition_pending.source_root`,
  `canonical_root_key` and the snapshot tables (`src/partition.rs`), the wedged-root check of
  `reflex_doctor` (`src/doctor.rs`), the heal reach query (`src/heal.rs`).

No single registered spelling satisfies both for a name that needs quotes. 1.12.0 registers what
`quote_ident` prints (so that a dropped source takes its IMVs with it). With that spelling, on
the fix branch before the refusal was added (pg15):

```sql
CREATE SCHEMA s;
CREATE TABLE s."position"(id INT, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s.p_a PARTITION OF s."position" FOR VALUES IN ('a');
SELECT create_reflex_ivm('x', 'SELECT g, SUM(v) AS s, COUNT(*) AS c FROM s."position" GROUP BY g');
CREATE TABLE s.p_c(id INT, g TEXT NOT NULL, v INT);  INSERT INTO s.p_c VALUES (7,'c',70);
ALTER TABLE s."position" ATTACH PARTITION s.p_c FOR VALUES IN ('c');
SELECT reflex_flush_partitions();
-- x has no group 'c'; the query has. No error. Same with "S".p.
```

Writes through the parent and through a partition were maintained; only the partition-tree
changes (ATTACH with rows, and by the same matching DETACH / DROP) did not reach the IMV.

## What is established and what is not

Established: the silent stale result above for a mixed-case schema and for a column-name
keyword; the same cells with plain names, with an unreserved keyword (`zone`, printed bare) and
with quoted *partition* names under a plain parent are correct
(`v112_sr_adm_partitioned_source_*`). Not established: which of the unquoted readers besides
the event trigger would also need the change; a partition (not the parent) used as a source.

On 1.11.5 and on `integrate/fold` before the fix round, a column-name keyword written bare
(`FROM position`) was registered bare, the partition bookkeeping followed it, and dropping the
table left its IMVs behind. So for that one class the refusal takes away a definition that was
maintained while the table existed.

## Fix direction (hypothesis to falsify)

Key the partition bookkeeping by what `quote_ident` prints (or by OID): `_parent` and
`_part_root` in `__reflex_on_ddl_command_end`, `canonical_root_key`, and the readers of
`source_root`. The event trigger is bootstrap SQL, so this needs a migration that replaces the
function, and existing rows of `__reflex_partition_pending` / the snapshot tables keyed the old
way. Then remove `partitioned_source_whose_name_needs_quotes` and turn the three `…_is_refused`
cells into `ok` cells.

## See also (added 2026-10-12)

The cross-schema lock work of 1.12.0 (`p3-c-report.md` R2.9 in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`) added
`public.__reflex_named_relation(TEXT)` and notes, read from the code and not run: a root key
derived from the catalog is unquoted (`n.nspname || '.' || c.relname`), so a root whose name
needs quotes folds wrongly in `__reflex_named_relation`. Nothing reaches it today because create
refuses such a source; lifting this refusal has to fix that key too.
