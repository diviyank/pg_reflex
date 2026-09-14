# 2026-09-08 — a partition swap under a status filter that excludes the slice empties it

**Status: narrowed in 1.11.4 — healing closed for the incident shape; the transient window and unmappable shapes remain.**
Residual of the `omc.sop_forecast_view` incident (DP 471, 1.86 M rows showing 0).

Severity: **medium** (was high).

---

## Mechanism

`sop_forecast_view` filters `demand_planning.status` and lists `demand_planning` in
`ignore_sources`. When db-bus swaps a month of `sales_simulation` while the DP is in a
status the filter excludes (e.g. `creating_sop`), the partition rebuild evaluates the
query and correctly writes zero rows for that slice. On ≤ 1.11.3, when the status
returned to an included value, nothing refreshed the IMV, so the slice stayed empty
with no error, no `known_stale`, and the pre-wipe `reltuples` in `reflex_ivm_status`.

## What 1.11.4 closes

- Detection: the swap writes a `rebuild` event to `__reflex_event_log`, and
  `reflex_audit` / `create_reflex_ivm` flag the unsound ignore.
- Healing: the ignored source joins onto the IMV's first partition column
  (`dp.id = ss.dem_plan_id`), so statement triggers on it queue the changed DP into
  `__reflex_heal_pending`. The IMV reports `known_stale` until
  `reflex_heal_ignored_sources`, `reflex_scheduled_reconcile` or
  `reflex_doctor(fix => TRUE)` rebuilds exactly those partitions.
  Pinned by `src/tests/pg_test_ignored_source_heal.rs`.

## What remains

1. **The window itself.** Between the status returning and the next sweep the slice is
   still empty. It is reported (`known_stale`, F14), not prevented. Prevention stays with
   base-db: an exclusion-list status filter generated from `DemandPlanningStatus`, plus
   a per-leaf unique index on `sales_simulation`
   (`docs/superpowers/plans/2026-09-08-base-db-silent-wipe-companion.md`).
2. **Unmappable ignored sources get no heal.** An ignored source that does not join by a
   top-level equality onto the expression the first partition column projects (an
   `OR` in the condition, a `RIGHT` / `FULL` join, a source read more than once,
   `USING`, a comma join filtered in `WHERE`, a key of a user-defined type or of a type
   other than the partition column's (bar integer pairs and `text`/`varchar`), a
   non-partitioned IMV) keeps the pre-1.11.4 contract. Its create is refused unless
   acknowledged, so that is an accepted risk, but nothing reports a change to it.
3. **Writes addressed directly to a partition of a partitioned ignored source** queue
   nothing, as for the regular maintenance triggers, which are also on the root only.
4. **A second read of the ignored source hidden in a SQL function body** is invisible
   to the single-read check, so such a source is still mapped and a change seen only
   through the function's read can be missed.
5. **The heal helpers are callable by any role.** `__reflex_heal_enqueue` lets a role
   with no rights on the source queue keys for IMVs mapped to it. That costs partition
   rebuilds that restore what the query returns, never wrong data;
   `__reflex_heal_mark_truncated` acts only on an empty source.

(`TRUNCATE` of a mapped ignored source is reported: it marks the IMV `known_stale`.)

A pg_reflex-side runtime wipe guard (refusing a rebuild that takes a non-empty slice to
zero) was considered and declined in the 1.11.4 design.
