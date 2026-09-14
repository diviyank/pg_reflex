# 2026-09-08 — a partition swap under a status filter that excludes the slice empties it, and it never heals

**Status: untreated in pg_reflex — detection closed in 1.11.4, prevention owned by base-db.**
Residual of the `omc.sop_forecast_view` incident (DP 471, 1.86 M rows showing 0).

Severity: **high**, mitigated by 1.11.4's detection.

---

## Mechanism

`sop_forecast_view` filters `demand_planning.status` and lists `demand_planning` in
`ignore_sources`. When db-bus swaps a month of `sales_simulation` while the DP is in a
status the filter excludes (e.g. `creating_sop`), the partition rebuild evaluates the
query and correctly writes zero rows for that slice. When the DP's status returns to an
included value, nothing refreshes the IMV — `demand_planning` is ignored — so the slice
stays empty. The unswapped months keep their (stale) rows, which is why the client saw
Feb–Aug at 0 and Sept–Oct at 35.

The flush succeeded, so on ≤ 1.11.3 there was no error, no `known_stale`, and
`reflex_ivm_status` reported the pre-wipe `reltuples`.

Reproduced by `swi_status_window_swap_wipes_slice_silently`
(`src/tests/pg_test_status_window_wipe.rs`).

## What 1.11.4 closes (detection)

- The swap writes a `rebuild` / `partition_swap` event with `rows_before` → `rows_after`
  to `__reflex_event_log`.
- `reflex_ivm_status` reports the exact count (`is_estimate = false`) while that rebuild
  is newer than the target's last ANALYZE.
- `reflex_audit` raises `ignore-soundness` on the IMV, and `create_reflex_ivm` refuses the
  unsound ignore unless acknowledged.

## What it does not close (prevention)

A swap under an excluding predicate still empties the slice, and it still does not heal.
Owned by the base-db companion work, not pg_reflex: invert the `sop_forecast_view` status
filter to an exclusion list generated from `DemandPlanningStatus` (so a transient status
no longer excludes a live DP), plus a per-leaf unique index on `sales_simulation`.
See `docs/superpowers/plans/2026-09-08-base-db-silent-wipe-companion.md`.

A pg_reflex-side runtime wipe guard (refusing a rebuild that takes a non-empty slice to
zero) was considered and declined in the 1.11.4 design.
