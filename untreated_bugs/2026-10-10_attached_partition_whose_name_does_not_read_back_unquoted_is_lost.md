# A partition whose name does not read back unquoted is lost when attached with rows — S1 (SILENT wrong result)

Pre-existing (finding 5 of the adversarial review of the 1.12.0 fix wave: same on `0324f47`).
The parent has a plain name, so the 1.12.0 refusal of quoted partitioned sources does not apply.

## Reproduction (pg17, one transaction, `reflex_flush_partitions()` after each DDL)

```sql
CREATE TABLE c2(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE c2_a PARTITION OF c2 FOR VALUES IN ('a');
INSERT INTO c2 VALUES (1,'a',1);
SELECT create_reflex_ivm('m', 'SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM c2 GROUP BY g');
CREATE TABLE "C2_D" (LIKE c2);  INSERT INTO "C2_D" VALUES (15,'d',6),(16,'d',6);
ALTER TABLE c2 ATTACH PARTITION "C2_D" FOR VALUES IN ('d');
SELECT reflex_flush_partitions();   -- m has no group 'd'
INSERT INTO "C2_D" VALUES (17,'d',9);   -- still no group 'd'
INSERT INTO c2 VALUES (18,'d',1);       -- still wrong
```

## Which names (probe on the fix branch, IMMEDIATE and DEFERRED alike)

Each partition name was attached filled, written through the partition and through the parent,
deleted from, detached, and a second partition of the same spelling created with
`CREATE TABLE … PARTITION OF` and written. Rows of the IMV differing from the query after each
step:

| Partition name | Attached with rows | After DETACH | `PARTITION OF` (empty) then writes |
|---|---|---|---|
| `plain_d`, `"select"` / `"table"`, `é_d`, `my$d`, `"1d"` | correct | correct | correct |
| `"PD"` (upper case) | wrong, stays wrong | wrong until the next flush | correct |
| `"p d"` (space) | wrong, stays wrong | wrong until the next flush | correct |
| `"p.d"` (dot) | wrong, stays wrong | correct | correct |
| `"p""d"` (embedded quote) | wrong; IMMEDIATE writes raise `no partition of relation "__reflex_intermediate_…" found for row` | — | raises / wrong |

So the lost names are exactly those whose unquoted text does not parse back to the same
relation: an upper-case letter, a space, a dot, a double quote. A lower-case keyword, a
non-ASCII letter, a `$` and a leading digit are followed.

The review also reports (not re-run here): a quoted sub-partitioned branch under a plain root
(`"BR_b"`) loses the rows inserted into a new leaf under it and counts an attached filled leaf
twice.

## What is established and what is not

Established: the table above. The partition bookkeeping carries partition names as bare text
(`PartitionNode.bare_name`, `list_partition_children`, `intermediate_child_name` /
`target_child_name` in `src/partition.rs`; `_parent` in `__reflex_on_ddl_command_end`,
`src/lib.rs`) and splices them into SQL and into `to_regclass`. Not established: which of
those sites loses the attached rows; whether an IMV with a PARTITION BY mirror behaves
differently from an unpartitioned one; behaviour at COMMIT rather than with an explicit flush.

`v112_sr_adm_partitioned_source_plain_parent_quoted_partitions`
(`src/tests/pg_test_v112_seam_review.rs`) says "Quotes on a partition do not matter". That is
true of the keyword it uses (`"select"`) and false for the names above; the cell was left as it
is (tests are not edited after the fact) and its sentence should be narrowed when this is fixed.

## Fix direction (hypothesis to falsify)

Carry partitions by OID, or by what `quote_ident` prints, through the bookkeeping. This touches
the event trigger (bootstrap SQL, so a migration), the snapshot tables keyed by child name, and
the mirror-child naming; it is the same change the report
`2026-10-10_partitioned_source_whose_name_needs_quotes_is_refused_at_create.md` asks for on the
parent side, and is best done together. A refusal is not available here: the name is only known
at ATTACH time, and raising there would abort the user's DDL.
