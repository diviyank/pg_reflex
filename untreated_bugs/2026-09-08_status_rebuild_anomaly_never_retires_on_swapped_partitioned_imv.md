# 2026-09-08 — `reflex_ivm_status`'s rebuild anomaly never retires on an actively swapped partitioned IMV

**Status: untreated — deliberately deferred from 1.11.4.** Raised by the final
whole-branch review of `reflex-1.11.4`.

Severity: **medium — a performance cliff, not a wrong result.**

---

## Mechanism

`has_unresolved_rebuild` (`src/introspect.rs`) is true when a `rebuild` event in
`__reflex_event_log` is newer than the IMV target's last ANALYZE, read from
`pg_stat_all_tables` for the **root**. It forces the exact `COUNT(*)` branch.

A partition-scoped rebuild (the swap path db-bus drives on every forecast push)
ANALYZEs only the swapped child, and autovacuum never auto-analyzes a partitioned
parent. So on an IMV whose partitions are swapped routinely — the production
`sop_forecast_view` shape — the root's `last_analyze` never advances and the condition
latches permanently.

Consequence: every `reflex_ivm_status()` call runs `SELECT COUNT(*)` on that IMV,
forever. On `omc.sop_forecast_view` (17 M rows on db_dev) that is O(rows) per status
call, per IMV.

## Converging remedy (works today)

`ANALYZE <imv root>;` or `SELECT reflex_reconcile('<imv>');` (reconcile ANALYZEs the
target). Either retires the condition until the next count-changing swap.

## Ruled out for 1.11.4

Adding a root `ANALYZE` to the end of every partition swap: it puts a statistics pass
over the whole partition tree on the latency-sensitive swap path, which already runs
inside the pushing client's `COMMIT`. It needs its own benchmark.

## Fix direction

Either:

- compare against the newest ANALYZE across the partition tree
  (`max(last_analyze)` over `pg_partition_tree(root)` leaves) instead of the root's, or
- ANALYZE the root at the end of a partition-scoped rebuild, gated on a measured cost.
