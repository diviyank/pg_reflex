# `drop_reflex_ivm` leaves rows in `__reflex_source_partition_snapshot` and `__reflex_deferred_pending` — S3 (residue, no wrong result seen)

Pre-existing: identical on `6946400` (1.11.5) and on the 1.12.0 integration branch at `0324f47`.
Found by review seat R3 of the 1.12.0 full re-review (pg16); filed from its probe outputs
(`head2/r3_drop.txt`, `head1/r3_odd_*.txt`, `head2/r3b_snap.txt` under `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/full-review-r3-out/`).
The reduction below was run on pg16 on `0324f47` and on `fix/full-review-r3` (probe `zz_file_probe`, kept at `/private/tmp/rfx-remote/b-r3fix-file-probe.rs` with its outputs `b-r3fix-fp-{base,head}.log`): same outcome on both.

## What is left

After the drop of every IMV kind tried (aggregate, ungrouped, MIN/MAX, keyed, keyless, partitioned
aggregate and keyed, join, LEFT JOIN passthrough, DISTINCT, CTE, UNION ALL; both modes) no
relation, function, trigger, registry row or partition-pending row of the IMV remains, except:

1. rows of `__reflex_source_partition_snapshot` for the source root after its last IMV is dropped,
   and for a dropped partitioned IMV that was itself read by a dependent (`public.agg/agg_s_pa`,
   `public.cte__cte_t/…`);
2. a row of `__reflex_deferred_pending` for the source when the IMV is dropped in the transaction
   that staged a write to it.

```sql
CREATE TABLE s(id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g);
CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a');
SELECT create_reflex_ivm('agg', 'SELECT COUNT(*) AS c, SUM(v) AS sv FROM s', NULL, NULL, 'DEFERRED');
-- one transaction:
INSERT INTO s VALUES (1,'a',1); SELECT drop_reflex_ivm('agg');
SELECT source_root, child_name FROM public.__reflex_source_partition_snapshot;  -- public.s / s_pa
SELECT source_table, operation FROM public.__reflex_deferred_pending;           -- s / INSERT
```

## What was ruled out

A later `create_reflex_ivm` on the same source refreshes the snapshot (`r3b_snap`): no stale
baseline was observed to reach a flush. The shared DEFERRED infrastructure
(`__reflex_deferred_pending`, its flush function and trigger) stays by design.

## Severity

Residue only as far as tested. The risk to check before closing: a snapshot row surviving a drop,
the source's partitions changing while no IMV reads it, and a create that for some reason does not
reseed, would give the first flush a wrong baseline.

## Fix direction (hypothesis to falsify)

In `drop_reflex_ivm`, when the dropped IMV was the last enabled reader of a source root, delete that
root's snapshot rows; delete the snapshot rows keyed by the dropped IMV itself; delete the pending
row of a source that no DEFERRED IMV reads any more. Test by counting the rows of the three tables
after dropping each IMV kind, and after dropping one of two IMVs of a source (rows must stay).
