# 2026-07-24 — `reflex_rebuild_imv` retried 1020× externally because it cannot converge a matview-LEFT-JOIN / ignore_sources IMV, and nothing makes the futile repeats visible

**Status: untreated.** Diagnosed under PS-8 (B7 S3). Field evidence: db_prod
`pg_stat_statements` (pg_reflex 1.10.11) shows
`reflex_rebuild_imv('yse.sop_last_forecast_view')` with **1020 calls**, 12.4 s
mean, 558.9 s max, 3.5 h total.

## What was ruled out

The loop is **not** inside the extension. `reflex_rebuild_imv` is a thin alias
(`src/lib.rs:813` -> `reflex_reconcile`) with **zero internal call sites**; every
in-repo occurrence is a hint string, a doctor/audit `suggested_fix` string, or a
comment. `reflex_doctor(fix=>true)` repairs via `reflex_reconcile`
(`src/doctor.rs:337,369,486,502,522,643`), a distinct statement, so a
doctor-driven loop would not show as `reflex_rebuild_imv`. PS-1's recursive
reconcile recurses through the internal `reconcile_one`
(`src/reconcile.rs:428-474`), not a SQL-level `reflex_rebuild_imv`, so it does not
inflate the call count. **The 1020 calls are external** (operator/cron/client
retry).

## Why it loops (the real defect)

`yse.sop_last_forecast_view` (field def
`…/base_db/sql/views/sop_last_forecast_view.sql`) is a DEFERRED, RANGE-partitioned
passthrough IMV that LEFT JOINs two matviews (`latest_price_view`,
`current_assortment_activity_view` — untriggerable, the B2 family) and declares
`ignore_sources: [location, latest_price_view]`.

`reflex_rebuild_imv`'s own docstring (`src/lib.rs:800-811`) states it re-derives
partitions from the anchor source only and does **not** refill partitions fed by
`ignore_sources` tables. So when this IMV goes stale on data arriving through the
matview LEFT JOINs or an `ignore_sources` table, `reflex_rebuild_imv` returns a
success-ish result and **does not converge**. Whatever automation watches the
staleness symptom re-issues the call indefinitely. Each call still pays the full
anchor re-derivation cost (12.4 s mean), so the futile loop burned 3.5 h.

Two distinct gaps:

1. **Invisibility.** No signal tells an operator "you have called
   `reflex_rebuild_imv` on this IMV N times and its freshness did not change."
   Targeted recovery primitives silently repeat.
2. **Wrong primitive, silently.** `reflex_rebuild_imv` is structurally unable to
   converge an IMV whose stale data arrives via a matview source or an
   `ignore_sources` table, yet it neither says so nor points at the primitive that
   can (`reflex_reconcile_partition`, or a chain drop+recreate — see
   `docs/untreated.md` §F6). This is the caller-side face of B1/B2.

## Fix direction

- Count and surface repeat targeted-recovery calls (e.g. a `rebuild_count` /
  `last_rebuild_at` on `__reflex_ivm_reference`, exposed in `reflex_ivm_status`),
  so a non-converging retry loop is observable rather than only visible in
  `pg_stat_statements`.
- When `reflex_rebuild_imv` targets an IMV with matview sources or
  `ignore_sources`-fed partitions, either refuse with an actionable message
  naming `reflex_reconcile_partition` / chain-recreate, or warn loudly that it
  cannot refill those partitions — instead of returning a bare success.

## Relationship to other work

The convergence half overlaps B2 (matview-only source visibility) and
`docs/untreated.md` §F6 (ignore_sources partition refill), i.e. **PS-3's**
territory. This entry is the caller-side symptom (1020 invisible retries) and the
"make repeat calls visible" finding; it should be de-duplicated against PS-3's
fix rather than implemented in parallel.
