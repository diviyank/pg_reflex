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
Probe calibration (Task 1): bases 500k / 12.5M (25x), delta 50k rows (keyed passthrough).
Test `audit_probe_calibration_passthrough_is_sublinear` PASSED: ~48s wall time, timing granularity coarse on local macOS (sub-millisecond flushes round to 0ms). Probe skips assertion on hardware where small_ms=0 or big_ms=0; Phase 2 will deploy on slower cloud hardware with measurable timing.
_(Tasks 4–7 append verdicts here)_

## §4 Risk-ranked gap backlog
_(Task 8)_
