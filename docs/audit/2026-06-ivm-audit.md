# pg_reflex IVM Correctness & Performance Audit — 2026-06

Spec: `docs/superpowers/specs/2026-06-10-pg-reflex-ivm-audit-design.md`.
Dual axis: **correctness** (Postgres `EXCEPT ALL` recompute via `assert_imv_correct`,
cross-checked by `oracle_pure.rs`) and **plan quality** (`assert_sublinear` over
`reflex_ivm_status().last_flush_ms`). Phase 1 documents holes; it fixes nothing.

## §1 Coverage matrix
_(Task 2)_

## §2 Escape analysis — why each field bug since 1.7.2 slipped
_(Task 3)_

## §3 Instrumented gap confirmations

**Probe protocol.** `last_flush_ms` is recorded only on the DEFERRED flush path,
so plan-scaling tests create the IMV with mode `DEFERRED`, apply an identical
single-row delta against a small (20k) and a 25x-larger (500k) base, drain each
with `reflex_flush_deferred('<source>')`, and compare the two flush times with
`assert_sublinear`. The discriminator (`flush_scales_with_base`) flags a shape
only when the large-base flush is operationally heavy (≥30ms) *and* grows with
base size — it ignores cheap-at-scale flushes (the bugs this guards against are
multi-second/​minute flushes, e.g. the 1.10.1 18-minute re-aggregation) and
heavy-but-flat constant factors.

**Known limitation (Phase-2 successor).** This is a wall-clock heuristic. It is
self-validated per run — the calibration test measures a real O(delta) shape
*and* asserts the discriminator fires on synthetic O(base) growth — but the
rigorous successor is a white-box assertion on the generated maintenance plan
(`EXPLAIN`-actual-rows at the base relation), which Phase 2 should add. Note
`last_flush_rows` is **not** a usable signal: it counts the delta-table size
(`COUNT(*) FROM <delta>`), not the work done — which is exactly why the 1.10.1
bug reported `last_flush_rows=2` while taking 18 minutes.

Calibration (Task 1): `audit_probe_calibration_passthrough_is_sublinear` PASSED —
keyed passthrough judged sublinear at 20k→500k, discriminator confirmed to fire
on a 2ms→60ms (25x) pattern and stay quiet on flat/​sublinear ones.

_(Tasks 4–7 append verdicts here)_

## §4 Risk-ranked gap backlog
_(Task 8)_
