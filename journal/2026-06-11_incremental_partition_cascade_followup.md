# Follow-up: incremental partition delta in the reconcile_partition cascade

**Date:** 2026-06-11
**Status:** TODO — deferred follow-up to the landed work in
`plans/2026-06-11-incremental-partition-delta-unpartitioned-imv.md`.

## What landed (context)

Attaching/detaching a partition on a partitioned source no longer full-reconciles
an **unpartitioned** IMV that depends on it. The flush now applies the
attached/detached child as the bulk INSERT/DELETE it semantically is, via a new
`reflex_apply_partition_delta(imv, source, op, child, trans)` (mirrors the
INSERT/DELETE trigger pipeline: child-first pred-check skip → Path B → 
`reflex_build_delta_sql` → execute; falls back to `reflex_reconcile` on any
uncertainty). Measured on db_clone `current_assortment_activity_view`: a
non-current assortment attach went from a **398 s** full reconcile + 12-IMV
cascade to a **~3–25 ms** one-partition probe. 1222 tests green.

## The gap this follow-up targets

When a **partitioned** IMV (e.g. `sop_forecast_view`, `partition_by [dem_plan_id]`)
gets a new partition, the *pre-existing* partitioned-IMV flush branch reconciles
only the new partition via `reflex_reconcile_partition` (per-partition swap). But
its cascade to children (`reflex_reconcile_partition_impl`, `partition.rs`
~1444-1484) splits:

- child partitioned on the **same** key → `reflex_reconcile_partition(child, keys)`
  — stays scoped ✓
- child **unpartitioned** → `reflex_reconcile(child)` — **full rebuild** ✗

So an unpartitioned child of a partitioned IMV is still fully rebuilt when one
parent partition changes.

### Important caveat on how big this gap actually is

The unpartitioned-child case is **over-represented on db_clone**, which runs an old
extension (1.9.0) with **stale view definitions** created *before* the join-key
anchor-ambiguity fix. Example: `forecast_analysis_view__cte_forecast_sales` is
unpartitioned on db_clone because its partition column `dem_plan_id` is an equi-join
key shared by two partitioned inputs (`sop_forecast_view` + `cte_date_limits`), and
the old `resolve_anchor_source` left it unpartitioned on ambiguity. **On the prod
cluster that view is already partitioned** (per user) — the ambiguity fix landed —
so the cascade there stays scoped and the gap does not apply to it.

Net: before investing in this follow-up, confirm which sub-IMVs on a *current*
(prod-like) instance genuinely stay unpartitioned. The gap is real only for those.

## Two ways to close it

1. **Incremental cascade delta (this follow-up).** When a partitioned IMV's
   partition is reconciled, feed its unpartitioned children an incremental delta
   instead of a full reconcile — reusing `reflex_apply_partition_delta` with
   `source = parent IMV`, `child_table = parent's new partition target child`
   (`target_child_name(parent_imv, node)`), `op = INSERT`. Confirmed feasible:
   the child `base_query` references the parent IMV by name, so
   `replace_source_with_transition` rewrites it; the swap leaves the parent
   partition child populated before the cascade runs.

2. **Ensure children stay partitioned (cleaner, possibly already done on prod).**
   The anchor-ambiguity fix makes co-partitioned-join-key sub-IMVs partition on
   the shared key, keeping the whole cascade per-partition with **no new delta
   path**. If prod already does this for the relevant views, option 1 may be
   unnecessary — verify first.

## Difficulty of option 1

Moderate; the delta engine already exists.

- **Easy slice — AttachNew (brand-new partition):** correct as a pure INSERT
  delta (no old rows to retract). Work: change the cascade `else` branch
  (`partition.rs` ~1478-1482) to emit `reflex_apply_partition_delta(child,
  parent_imv, 'INSERT', target_child_name(parent, node), trans)`; thread an
  `is_attach_new` flag into `reflex_reconcile_partition` (pg_extern signature
  change); + oracle tests. ~60-70% of the landed work's effort.
- **Hard slice — recompute of an EXISTING partition / SwapFill:** the swap
  replaces all of a partition's rows, so the child needs an **old-vs-new diff**
  (capture old child before the swap drops it; `EXCEPT`-diff → INSERT-delta +
  DELETE-delta). Netting-correctness risk (cf. the deferred new/old netting bug
  class). Keep the `reflex_reconcile` fallback here.

### Risks

- The cascade is recursive and shared (same-key children recurse via
  `reflex_reconcile_partition`; others use `reflex_reconcile`). Must not
  double-apply.
- Must gate strictly on AttachNew — a pure-INSERT delta on an existing-partition
  recompute would double-count.
- Multi-partition flushes → multiple deltas per child.

## Worth check before building

Measure the unpartitioned child's full-reconcile cost on a prod-like instance:
`\timing` + `SELECT reflex_reconcile('alp.forecast_analysis_view__cte_forecast_sales')`
(or whichever sub-IMVs are *actually* unpartitioned on current views). If seconds,
the recursion risk likely isn't worth it; if minutes (like
`current_assortment_activity_view`), it is.

## Recommended order

1. Land + package the current optimization (migration creating
   `reflex_apply_partition_delta`, version bump, CHANGELOG).
2. On a current/prod-like instance, list sub-IMVs that genuinely stay
   unpartitioned and measure their reconcile cost.
3. Only then choose option 1 (incremental cascade) vs option 2 (anchor fix /
   already-partitioned) based on real data.
