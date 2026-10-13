# A partition change and a write to another source of a join IMV in one transaction count the joined rows twice — S1 (SILENT wrong result)

Pre-existing: identical on `6946400` (1.11.5) and on the 1.12.0 integration branch at `0324f47`.
Found by review seat R3 of the 1.12.0 full re-review (pg16; `*/r3b_pc_*.txt` steps 05 to 08 under
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-out/`).
The reduction below was run on pg16 on `0324f47` and on `fix/full-review-r3` (probe `zz_file_probe`, kept at `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-fix-evidence/b-r3fix-file-probe.rs` with its outputs `b-r3fix-fp-{base,head}.log`): same outcome on both.

## Reproduction (pg16)

```sql
CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a');
CREATE TABLE d(g TEXT PRIMARY KEY, label TEXT);
INSERT INTO s VALUES (1,'a',1); INSERT INTO d VALUES ('a','aa');
SELECT create_reflex_ivm('join_agg',
  'SELECT d.label, COUNT(*) AS c, SUM(s.v) AS sv FROM s JOIN d ON d.g = s.g GROUP BY d.label');
CREATE TABLE t_j (LIKE s INCLUDING ALL); INSERT INTO t_j VALUES (10,'j',2),(11,'j',3);
-- one transaction:
ALTER TABLE s ATTACH PARTITION t_j FOR VALUES IN ('j'); INSERT INTO d VALUES ('j','jj');
```

## Expected vs actual

Expected `(jj,2,5)`. Actual `(jj,4,10)` (IMMEDIATE run here; the review saw the same in DEFERRED,
there also with the two statements swapped). `DETACH` of a partition + `UPDATE d` of the matching row in one transaction
leaves the group of the detached partition behind. No error, not flagged stale.

## Mechanism (read from the code, not traced)

The write of `d` is applied by its statement trigger against `s` as it is then, which already
holds the attached partition. At COMMIT the partition flush applies the partition as a delta of
`s` joined to `d` as it is then, which already holds the new row. Each side sees the other's
change, so the pair is counted twice. The flush falls back to a full reconcile only when *this*
source was written (`source_written_in_this_transaction`, `src/partition.rs`); a write of another
source of the IMV does not count.

## What was ruled out

Not the single-source case: an attach plus a write to `s` itself is correct since 1.12.0. Related
but different statement shape: `2026-10-02_single_statement_two_source_write_double_counts_join_delta.md`.

## Fix direction (hypothesis to falsify)

In the partition flush, force the full reconcile of an unpartitioned IMV when *any* of its sources
was written in the transaction (each source's statement and truncate triggers raise their own
`source_written_setting`; plain sources would need the same mark, which today only the guard of a
partitioned source and the truncate function raise). Fail toward the reconcile.
