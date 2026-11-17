# `reflex_repair_dependency_graph` and the partition flush's failure handler write registry rows without the IMV lock — S3 (possible 40P01, retryable; not reproduced)

Residual of the 1.12.5 lock-order fix (branch `fix/rebuild-flush-lock-order`). 1.12.5 makes every
operator entry point that writes an IMV's `__reflex_ivm_reference` row take the IMV's advisory lock
(`registry::lock_imv`, src/sql_writer/registry.rs) first, so it queues behind a running flush. Two
writers were left out of that audit:

1. `reflex_repair_dependency_graph` (src/graph_repair.rs): `mark_legacy_generated_sub_imvs`,
   `backfill_depends_on_imv`, `backfill_graph_child` and `recompute_graph_depth` each run one
   multi-row `UPDATE public.__reflex_ivm_reference` over many IMVs, with no IMV lock.
2. The partition flush's `EXCEPTION WHEN OTHERS` branch (src/partition.rs, the `DO $_reflex_part_sp$`
   block built in the drain, ~line 5150) marks every IMV reading the failed root, and their
   `graph_child` closure, `known_stale` in one multi-row UPDATE, with no IMV lock.

## Why it may deadlock

A COMMIT flush takes IMV V's advisory lock, then updates V's registry row, and may go on to
other IMVs. A multi-row UPDATE from (1) or (2) row-locks registry rows in scan order. If it holds
W's row and waits for V's row (held by flush F), while F waits for W's row (or W's advisory lock
held elsewhere in a chain), PostgreSQL aborts one side with 40P01. (1) is operator-invoked and rare;
(2) runs only after a partition flush already failed.

## What was ruled out

- No data loss: registry updates are transactional; the victim is aborted and retryable.
- Not seen in the field or in the batch's test runs; the cycle is inferred from the lock order,
  not reproduced.

## Reproduction (to build)

Two sessions on one DEFERRED join IMV V over sources S1, S2 and a second IMV W over S1:
session A `BEGIN; INSERT INTO S1 …; INSERT INTO S2 …;` (flush at COMMIT updates V's and W's rows);
session B `SELECT reflex_repair_dependency_graph();` with `pg_sleep` injected between its UPDATEs
(or a `lock_timeout`-free wait), then COMMIT A. Expect 40P01 on one side if the scan reaches W
before V.

## Severity

S3: a retryable aborted transaction (operator call or COMMIT), no wrong data.

## Fix direction

(1) Take `lock_imv` for every registry row the repair touches, in a fixed order (graph order,
then name), before the UPDATEs — or take a single repair-wide lock that flushes also take.
(2) Inside the failure handler, either take the affected IMVs' locks in graph order before the
UPDATE or document that the handler only runs in the flushing transaction, which already holds
them; verify that claim first. A two-session test must show the 40P01 before the fix.
