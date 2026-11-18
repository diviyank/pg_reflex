# Minor follow-ups from the 1.12.5 batch reviews — S4 / test-quality

Items the reviewers of the 1.12.5 batch deferred. None is a known wrong result. Each is small; file
a dedicated report before working on one if it turns out larger.

## Two-source correction (`src/trigger/joint*`, branch `fix/two-source-statement-recompute`)

- **Stash growth from positive write offsets.** A statement that writes only X while Y has a
  positive offset re-stashes; a run of such statements grew the stash from 102 to 510 rows until
  the 10 000-row cap, after which the delta recomputes. Bound it by the rows the correction can
  still need.
- **Whole-row stash.** Filed on its own:
  `2026-11-18_wide_row_stash_can_hit_the_1_gb_allocation_limit.md`.
- **Missing registry row → empty SQL.** `reflex_joint_skipped_delta_sql` returns `''` when the IMV's
  registry row is missing, silently. Refuse loudly (WARNING naming the IMV) instead.
- **Skip hook on single-source IMVs.** Every skipped delta of a single-source IMV now calls
  `reflex_joint_delta_may_be_skipped`; the 1.12.5 benchmark measures it (see the release report).
- **Cost of a correction.** Profiled and reduced in 1.12.5 (one `ANALYZE` per correction instead of
  one per signed piece). Release build, pg17, IMMEDIATE grouped SUM over a 200 K ⋈ 200 K join, a
  one-row two-source CTE, 7 interleaved rounds: 2 000 groups 14.4 ms (1.12.4, which double
  counts) → 28.6 ms corrected; 100 000 groups 100.1 → 162.7 ms. A one-source write to the same join
  IMV is unchanged (7.69 → 8.41 ms, 50.4 → 51.8 ms). What remains on the large IMV is mostly the
  correction's one `ANALYZE` of the intermediate (48–55 ms at 100 K rows). Two options were left to
  the maintainer: drop that `ANALYZE` (a plan-quality safeguard, about 50 ms at 100 K groups) and
  cache the resolved OIDs of `EXACT_TEXT_TYPES` (about 1.2 ms).
- **Tests that do not assert a correction ran.** The 2PC (`PREPARE TRANSACTION`) and lock-wait tests
  check the oracle but not `corrected > 0`, so a silent fallback to recompute would stay green.

## Lock order (`fix/rebuild-flush-lock-order`)

- The combined mutation run of the three setter tests hung once and was not diagnosed (each test
  went RED alone).
- Registry writers outside the audit: see
  `2026-11-17_repair_dependency_graph_writes_registry_rows_without_the_imv_lock.md`, and for the
  trigger-side twin (Path C)
  `2026-11-18_path_c_writes_the_imv_and_its_registry_row_without_the_imv_lock.md`.

## Readers-first flush order (`fix/joint-delta-flush-order`)

- The diamond test was never shown RED with the cross-source guard disabled; it may be a false green
  for that guard.

## DROP of a source partition (`fix/drop-source-partition-queues-root`)

- `__reflex_partition_snapshot` has no index on `child_oid`: one scan per dropped table.
- A role without SELECT on the snapshot still skips silently (the hook cannot classify the dropped
  table); a WARNING naming `reflex_flush_partition_source` would match the other refusal paths.
- Vanished-root queue rows: see
  `2026-11-17_drop_partition_then_drop_root_in_one_transaction_queues_a_vanished_root.md`.
