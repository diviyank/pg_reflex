# Path C writes the IMV and its registry row without the IMV's advisory lock — S3 (possible 40P01, retryable; reasoned, not reproduced)

Found by the 1.12.5 final review (M1). The trigger-side twin of the cycle 1.12.5 closed for
operator calls (`2026-11-17_repair_dependency_graph_writes_registry_rows_without_the_imv_lock.md`
holds the other unlocked registry writers).

## Mechanism

In `sql/trigger_body.plpgsql.in` the Path C block (`__REFLEX_SLOT_PATH_C_BLOCK__`, line 20) runs
before `PERFORM pg_advisory_xact_lock(hashtext(_rec.name), …)` (line 21). When Path C fires (an
INSERT promoted to a bulk insert whose estimated rows reach the wipe threshold), the block in
`sql/trigger_path_c_block.plpgsql.in`:

- drops and recreates the intermediate table's unique index, inserts into the intermediate and the
  target (lines 41-70), then
- runs `UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = _rec.name`
  (line 71), and `CONTINUE`s: that iteration never takes the IMV's advisory lock.

The registry row lock is then held to the end of the writer's transaction. Cycle:

1. Writer W, statement 1: Path C on IMV V row-locks V's registry row.
2. Operator O: `reflex_rebuild_imv('V')` (or any 1.12.5 `lock_imv` entry point) takes V's advisory
   lock, then waits on V's registry row, held by W.
3. W, statement 2: a normal delta for V waits on V's advisory lock, held by O. PostgreSQL aborts
   one of them with 40P01.

A COMMIT-time flush of V in another session (advisory lock, then registry row) is the same O.

## What was ruled out

- No wrong data: the victim is aborted and retryable; the table locks Path C takes (DROP INDEX on
  the intermediate: ACCESS EXCLUSIVE) serialize it against another writer's delta on V.
- Pre-existing: the block order predates 1.12.5 (1.12.5 only moved IMV locking for operator
  calls). Not seen in the field or in the batch's tests.

## Reproduction (to build)

Two dblink sessions (`probe_db_open` / `worker_exec`), an IMMEDIATE aggregate IMV V with
`reflex_set_wipe_threshold('V', 0)` so every INSERT takes Path C:
session W `BEGIN; INSERT INTO src …;` (Path C), session O `SELECT reflex_rebuild_imv('V');`
(blocks), then W `UPDATE src … ;` (normal delta). Expect 40P01 in W or O.

## Severity

S3: a retryable aborted transaction, no wrong data.

## Fix direction

Take the advisory lock before the Path C block (move line 21 above line 20 of
`sql/trigger_body.plpgsql.in` and its DEFERRED-flavour twins). This changes every rendered
trigger body, so the release migration's body rewrite (step 4 of the 1.12.4→1.12.5 script) and
`pg_test_upgrade_1_12_5` must follow. A two-session test must show the 40P01 before the fix.
