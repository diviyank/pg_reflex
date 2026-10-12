# `DETACH PARTITION` of a source leaves the IMVs built on its partition-mirroring IMVs stale — S1 (SILENT wrong result)

Pre-existing: identical on `6946400` (1.11.5) and on the 1.12.0 integration branch at `0324f47`.
Found by review seat R3 of the 1.12.0 full re-review (pg16).
The reduction below was run on pg16 on `0324f47` and on `fix/full-review-r3` (probe `zz_file_probe`, kept at `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-fix-evidence/b-r3fix-file-probe.rs` with its outputs `b-r3fix-fp-{base,head}.log`): same outcome on both.

## Reproduction (pg16, every statement committed; IMMEDIATE run here, DEFERRED by the review)

```sql
CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a', 'b');
CREATE TABLE s_pc PARTITION OF s FOR VALUES IN ('c');
INSERT INTO s VALUES (1,'a',1),(2,'a',2),(3,'b',5),(4,'c',7);
-- agg and keyed mirror the partitions of s (partition column g)
SELECT create_reflex_ivm('agg', 'SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM s GROUP BY g',
                         NULL, NULL, 'IMMEDIATE', NULL, ARRAY['g']);
SELECT create_reflex_ivm('keyed', 'SELECT id, g, v FROM s', 'id, g', NULL, 'IMMEDIATE', NULL, ARRAY['g']);
SELECT create_reflex_ivm('dep_agg', 'SELECT COUNT(*) AS n, SUM(sv) AS t FROM agg');
SELECT create_reflex_ivm('dep_keyed', 'SELECT g, COUNT(*) AS c FROM keyed GROUP BY g');
ALTER TABLE s DETACH PARTITION s_pa;
```

## Expected vs actual

Expected: every IMV equals a fresh run of its query. Actual: `agg` and `keyed` lose the rows of
`s_pa` (correct); `dep_agg` keeps the totals that include them (`(3,15)` for `(1,7)`) and `dep_keyed`
keeps groups `a` and `b`. No error, `known_stale` not set.
Also with detach + `DROP TABLE`, detach + a write through `s`, and two detaches in one
transaction (review outputs `*/r3b_pc_*.txt`, steps 02, 07, 08, 09, 17, 18, 20, under `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-out/`).
`reflex_reconcile` of the dependents repairs them; `reflex_reconcile('agg')` does not.

## What was ruled out

- Not the swap: DETACH + ATTACH of a replacement with the same bound in one transaction is correct.
- Not the direct IMVs: `agg` and `keyed` are correct after every step.
- Unpartitioned direct IMVs are not affected (their partition delta goes through the trigger
  pipeline, which reaches the dependents).

## Mechanism (read from the code, not traced)

The partition flush removes the mirror partition of a partitioned IMV with `DROP TABLE … CASCADE`
(`src/partition.rs`, the `to_drop` loop of `reflex_flush_partition_source`). A dropped partition
fires no statement trigger of the IMV root, and nothing cascades the removal in code, so no reader
of the IMV sees the rows leave.

## Fix direction (hypothesis to falsify)

Before dropping a mirror node, apply its rows to the readers as a DELETE (`DELETE FROM <imv root>
WHERE <partition bound>` with the IMV's own statement triggers enabled, then drop the emptied
partition), or reconcile the dependents of an IMV whose mirror lost a node. Test with the fixture
above, both modes, and a dependent of a dependent.

## Later observation (added 2026-10-12)

From the review of the swap-cascade work (`p3-b-review.md` in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`, "Pre-existing, seen on both
trees"; trees `d0a3e06` and the branch `fix/v112-swap-cascade-scope`): in its `main` scenario 22
(a partition detached and dropped) 32 nodes per mode are wrong on both trees, and **three stay
wrong after `reflex_reconcile`** (`za_full` and two `__cte_pm` sub-IMVs). The fixture and the
exact `reflex_reconcile` calls are in that review's probe, not printed in its text; which IMV was
reconciled is not stated. This qualifies "reconciling the dependents repairs them" above: it was
shown for the two-level fixture of this report, not for a chain through CTE sub-IMVs.
