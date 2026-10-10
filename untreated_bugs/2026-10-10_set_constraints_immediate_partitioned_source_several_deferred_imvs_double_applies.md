# `SET CONSTRAINTS ALL IMMEDIATE` on a partitioned source read by several DEFERRED IMVs applies a write twice to the IMV grouped by the partition key — S1 (SILENT wrong result, opt-in mode)

Pre-existing: identical on `6946400` (1.11.5) and on the 1.12.0 integration branch at `0324f47`.
Found by review seat R3 of the 1.12.0 full re-review (pg16); filed from its probe outputs
(`*/r3b_sc.txt`, variants `part` and `part+mix`, steps A to E, under `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-out/`; probe
`zz_r3b_set_constraints` in `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-probes.rs`).
The reduction below was run on pg16 on `0324f47` and on `fix/full-review-r3` (probe `zz_file_probe`, kept at `/private/tmp/rfx-remote/b-r3fix-file-probe.rs` with its outputs `b-r3fix-fp-{base,head}.log`): same outcome on both.
A third shape beside
`2026-10-01_set_constraints_immediate_in_trigger_reconcile_double_applies.md` and
`2026-10-01_set_constraints_immediate_two_source_statement_double_counts.md`.

## Reproduction (pg16)

```sql
CREATE TABLE x(id INT NOT NULL, g TEXT NOT NULL, h INT, v INT) PARTITION BY LIST (g);
CREATE TABLE x_a PARTITION OF x FOR VALUES IN ('a');
CREATE TABLE x_d PARTITION OF x DEFAULT;
INSERT INTO x VALUES (1,'a',1,1),(2,'b',2,2);
SELECT create_reflex_ivm('agg', 'SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM x GROUP BY g', NULL, NULL, 'DEFERRED');
SELECT create_reflex_ivm('nk',  'SELECT h, COUNT(*) AS c, SUM(v) AS sv FROM x GROUP BY h', NULL, NULL, 'DEFERRED');
SELECT create_reflex_ivm('tot', 'SELECT COUNT(*) AS c, SUM(v) AS sv FROM x', NULL, NULL, 'DEFERRED');
-- one transaction:
INSERT INTO x VALUES (604,'a',2,4); SET CONSTRAINTS ALL IMMEDIATE;
```

## Expected vs actual

`agg` (which mirrors the partitions of `x`: its registry row has `partition_columns = {g}`) holds
the inserted row twice in group `a`: `(a,3,9)` for `(a,2,5)`. `nk` and `tot` are correct. No error,
not flagged stale.

## What is established and what is not

Established: wrong with the three IMVs together, with and without an extra IMMEDIATE IMV on `x`.
Not reproduced with `agg` alone, nor on an unpartitioned `x`. With dependent IMVs the small
fixture was correct, and the 20-IMV fixture of the review was wrong (`head1/r3_sa_def.txt` step
14). Not minimised further; the mechanism is not established.

## Fix direction (hypothesis to falsify)

Trace which flush applies the staged delta to `agg` the second time (the IMMEDIATE-fired flush of
one pending row, then the flush of a later one for the same source and transaction). The two filed
reports describe the neighbouring mechanisms; the remedy there (keep the flush deferred while a
statement trigger is running, or key the consumed staging rows) should be checked against this
shape before a separate fix is written.
