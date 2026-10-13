# A partition flush that succeeds on retry leaves most of the root's IMVs flagged `known_stale` although they match their query — S4 (over-reported staleness, no wrong data)

Found while fixing `2026-10-12_one_commit_with_a_transient_lock_error_suspends_a_partition_root`
(branch `fix/v112-transient-lock-suspension`).

## Observation

When the COMMIT-time flush of a partition root fails, its `EXCEPTION` handler
(`src/partition.rs`, `reflex_flush_partitions_impl`, the `do_block`) sets
`known_stale = TRUE, stale_reason = SQLERRM` on every IMV that reads the root. When a later flush of
the root succeeds, every IMV is back to its query (bidirectional `EXCEPT ALL` oracle), but only the
IMV whose own reconcile had failed has its flag cleared.

Reproduction: `src/tests/pg_test_v112_transient_suspension.rs` (any test; the fixture is a
LIST-partitioned `sim` with three leaves read by the seven IMVs of `V112_DP_TRUNCATE_IMVS`,
IMMEDIATE). A three-leaf swap commits while another session holds `sim_total` in SHARE mode under
`lock_timeout`; the flush fails and all seven IMVs are flagged. After the holder releases, a write
and `reflex_flush_partition_source('public.sim')` drain the root and every IMV matches its query,
yet `known_stale` is still TRUE on `sim_agg, sim_agg_part, sim_keyed, sim_keyed_part, sim_keyless,
sim_minmax` (only `sim_total` is cleared). Same result on the unmodified branch after
`reflex_reset_partition_failures` + flush (logic-failure scenario).

Since round 5 of `fix/v112-swap-cascade-scope` the handler also flags, recursively, every reader of
those IMVs (`graph_child`), so the flags left behind by a successful retry include the readers; the
fix below must clear theirs too.

## Not established

- Which path reconciles the other IMVs without clearing the flag (the flush issues
  `reflex_reconcile` for a SwapFill on an unpartitioned IMV, which clears it in
  `src/reconcile.rs`; the partitioned IMVs go through `reflex_reconcile_partition`, which does not
  appear to). Not investigated.

## Impact

No wrong data. `reflex_ivm_status` / doctor keep reporting these IMVs stale with the old lock error
as reason until something clears it; `reflex_reconcile(<imv>)` does, so the remedy converges.

## Fix direction (hypothesis)

Clear, in the successful branch of the root's flush, the flag the failure handler set: only rows
whose `stale_reason` is the root's recorded `last_error`, so a flag set for another reason survives.
