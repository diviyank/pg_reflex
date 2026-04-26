# Deployment profile

Synthesised from the 2026-04-24 production-readiness audit and refreshed
2026-04-26 after the auto-on top-K + UPDATE staleness fix landed.

## :material-check-circle:{ .light-green } Green light

- Analytical dashboards backed by `SUM` / `COUNT` / `AVG` / `COUNT(DISTINCT)` / `BOOL_OR` over append-mostly or narrowly-mutated sources.
- `MIN` / `MAX` over wide fact tables — top-K is auto-enabled (K=16) so retraction stays bounded; for append-only workloads opt out with `topk = 0`.
- Low-hundreds-of-IMVs registries, cascade depth ≤ 3.
- `DEFERRED` mode where commit latency is acceptable at p99 = (single largest flush) × cascade width.
- Environments where schema changes are rare and operators can run `reflex_rebuild_imv` post-DDL as part of their change-control runbook.

## :material-alert:{ .light-amber } Yellow light

- **UPDATE-heavy workloads on top-K MIN/MAX IMVs** where group cardinality substantially exceeds K. Every UPDATE pays a scoped source-scan recompute for affected groups — the correctness guarantee for partial-heap states. If UPDATEs dominate INSERT+DELETE for such IMVs, the recompute scan can outweigh the heap-maintenance benefit. Opt out via `topk = 0` to fall back to the legacy 1.2.0 scoped-recompute behaviour on retraction.
- **Multi-session concurrent DDL** on the same IMV graph. Tested with 4 concurrent flush sessions; not stress-tested beyond.
- **`DEFERRED` cascade fan-out > 1000 IMVs.** Single-session flush at COMMIT means commit latency scales linearly with cascade width. No correctness hazard; latency only.

## :material-close-octagon:{ .light-red } Red light

- Views relying on `WITH RECURSIVE`, `FULL OUTER JOIN` deltas, or `ARRAY_AGG` / `JSON_AGG` — structurally unsupported. Use plain `MATERIALIZED VIEW`.
- Mission-critical read paths where stale-on-schema-change is worse than downtime — set `pg_reflex.alter_source_policy = 'error'` to reject ALTERs on tracked sources.
- Multi-tenant platforms where untrusted users can define IMV SQL — `create_reflex_ivm` is admin-facing by design. See [multi-tenant guards](multi-tenant-guards.md).

## Audit risks scorecard

| Risk | Status |
|---|---|
| R1: source DROP orphans | ✅ Closed — auto-drop event trigger (1.2.0) |
| R2: ALTER TABLE warns but continues | ✅ Mitigated — `pg_reflex.alter_source_policy='error'` (1.2.1) |
| R3: top-K retraction cliff | ✅ Closed — auto-enabled K=16 for MIN/MAX (2026-04-26) |
| R4: DEFERRED single-session flush | ⚠️ Latency-only, no correctness hazard |
| R5: passthrough unique key | ✅ Closed — auto-PK inference (1.1.x), clearer messaging (1.2.1) |
| R6: no histogram | ✅ Closed — `reflex_ivm_histogram` + `pg_stat_statements` tagging (1.3.0) |
| R7: no automated drift detection | ✅ Closed via `reflex_scheduled_reconcile` + pg_cron recipe (1.2.1) |
| R8: adversarial SQL in IMV defs | ⚠️ Architectural — gate behind RPC layer ([multi-tenant guards](multi-tenant-guards.md)) |
