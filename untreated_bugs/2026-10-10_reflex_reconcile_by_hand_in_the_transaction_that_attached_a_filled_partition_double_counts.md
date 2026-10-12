# `reflex_reconcile` called in the transaction that attached a filled partition: the flush adds the partition's rows again — S1 (SILENT wrong result, edge)

Pre-existing: identical on `6946400` (1.11.5) and on the 1.12.0 integration branch at `0324f47`.
Found by review seat R3 of the 1.12.0 full re-review (pg16; `*/r3b_pc_*.txt` step 14 under
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-out/`).
The reduction below was run on pg16 on `0324f47` and on `fix/full-review-r3` (probe `zz_file_probe`, kept at `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-fix-evidence/b-r3fix-file-probe.rs` with its outputs `b-r3fix-fp-{base,head}.log`): same outcome on both.

## Reproduction (pg16; IMMEDIATE run here, DEFERRED by the review)

```sql
CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a');
INSERT INTO s VALUES (1,'a',1);
SELECT create_reflex_ivm('total', 'SELECT COUNT(*) AS c, SUM(v) AS sv FROM s');
CREATE TABLE t_n (LIKE s INCLUDING ALL); INSERT INTO t_n VALUES (10,'n',2),(11,'n',3);
-- one transaction:
ALTER TABLE s ATTACH PARTITION t_n FOR VALUES IN ('n'); SELECT reflex_reconcile('total');
```

## Expected vs actual

Expected `(3,6)`. Actual `(5,11)`: the reconcile rebuilds `total` from a source that already holds
the partition, then the COMMIT-time partition flush applies the partition as an INSERT delta. No
error, not flagged stale.

## What was ruled out

A TRUNCATE or a write of the source in that transaction is covered: both raise
`source_written_setting`, and the flush then reconciles instead of applying the delta. A reconcile
called by hand raises nothing.

## Fix direction (hypothesis to falsify)

Have `reflex_reconcile` / `reflex_rebuild_imv` of an IMV raise the written setting of each of its
partitioned sources (the rebuild reflects every partition the source has now, exactly like the
truncate rebuild), so the flush takes its reconcile path. One `set_config` per source in
`src/reconcile.rs`; test with attach, create-and-load and detach, both modes, reconcile before
and after the partition change.
