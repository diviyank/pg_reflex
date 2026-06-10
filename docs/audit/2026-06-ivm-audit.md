# pg_reflex IVM Correctness & Performance Audit — 2026-06

Spec: `docs/superpowers/specs/2026-06-10-pg-reflex-ivm-audit-design.md`.
Dual axis: **correctness** (Postgres `EXCEPT ALL` recompute via `assert_imv_correct`,
cross-checked by `oracle_pure.rs`) and **plan quality** (`assert_sublinear` over
`reflex_ivm_status().last_flush_ms`). Phase 1 documents holes; it fixes nothing.

## §1 Coverage matrix

Plan-quality coverage is uniformly **Untested** across all constructs (only the new `assert_sublinear` probe in `pg_test_audit_gaps.rs` provides evidence, and only for passthrough calibration); the strongest correctness candidates include single-source aggregates, DEFERRED mode, LEFT JOINs, and CTEs, while significant gaps exist in DISTINCT ON, window functions, IGNORE_SOURCES, and cross-source anti-double-count validation.

| Construct | Correctness | Evidence | Plan-quality | Notes |
|-----------|-------------|----------|--------------|-------|
| Passthrough | Weak | `pg_test_passthrough.rs::test_passthrough_simple` — only COUNT(*) spot-checks; `pt_secondary_keyed_left_join_all_ops_immediate` actually tests a LEFT JOIN passthrough variant | Untested | No full-relation oracle for pure passthrough; point-value checks only |
| Single-source aggregate | Proven | `pg_test_correctness.rs` (375 `assert_imv_correct` calls); `test_update_group_by_column`, `test_correctness_group_disappears`, `test_correctness_having_threshold` with INSERT/UPDATE/DELETE mutations; `pg_test_deferred.rs` (73 calls) | Untested | Strongest coverage; EXCEPT ALL oracle validates post-mutation state |
| Multi-source aggregate | Weak | `pg_test_e2e.rs::test_combo_cte_join_having` (CTE with JOIN + GROUP BY HAVING, 1–2 mutations) | Untested | Only one E2E combination test; no dedicated multi-join-aggregate-specific suite |
| Inner join | Weak | `pg_test_correctness.rs::test_correctness_self_join`, `test_correctness_join_duplicates`, `test_correctness_passthrough_join` (assert_imv_correct present, 1–2 mutations); `pg_test_passthrough.rs::test_passthrough_join` (no oracle) | Untested | Limited mutation series; mostly single INSERT checks |
| Left join | Proven | `pg_test_passthrough.rs::pt_secondary_keyed_left_join_all_ops_immediate` (13 assert_imv_correct calls) — INSERT/UPDATE/DELETE on secondary, NULL↔value transitions; `pt_secondary_keyed_left_join_all_ops_deferred` (DEFERRED variant) | Untested | Comprehensive keyed maintenance on secondary mutations; includes orphan and no-match-key cases |
| Aggregate + LEFT JOIN secondary | Proven | `pg_test_passthrough.rs::pt_secondary_keyed_left_join_all_ops_immediate/deferred` (assert_imv_correct + multiple mutations) | Untested | Tests LEFT JOIN passthrough (SELECT ... LEFT JOIN ... not aggregate-only); validates keyed delete/reinsert |
| CTE-decomposed | Proven | `pg_test_correctness.rs::test_correctness_cte_cascade`, `pg_test_cte.rs` (10 assert_imv_correct); `test_cte_simple_aggregate`, `test_cte_trigger_propagation`, `test_cte_with_where_filter` with INSERT/DELETE mutations; `pg_test_e2e.rs::test_combo_cte_join_having` | Untested | Multi-test coverage; sub-IMV and main view both validated |
| UNION ALL | Proven | `pg_test_correctness.rs::test_correctness_union_all`, `pg_test_e2e.rs::test_combo_union_aggregate_operands` (INSERT into both operands, DELETE from one; GROUP BY aggregates on both sides; assert_imv_correct validates post-mutation) | Untested | Two sources with symmetric maintenance; operand-specific mutations tested |
| DISTINCT ON | Weak | `pg_test_distinct_on.rs::don_oracle_v` (1 assert_imv_correct at line 200) | Untested | Only one oracle test; 40+ other distinct_on tests use only point-value checks, no full-relation oracle |
| Window function | Weak | `pg_test_correctness.rs::test_correctness_window_groupby_rank` (assert_imv_correct, ROW_NUMBER() + GROUP BY); `pg_test_window.rs` (0 assert_imv_correct) — `test_window_row_number_insert_reranks` only checks rank values, not full result set | Untested | Minimal oracle coverage; 20+ window tests exist but use only point checks |
| Scalar-subquery filter | Proven | `pg_test_filter.rs::test_passthrough_subquery_filter_skips_noncurrent_group_deferred` (line 339, assert_imv_correct with UPDATE to non-current group); `pg_test_coverage.rs::cov_rebuild_metadata_restores_subquery_filter_skip` (line 4668, assert_imv_correct with UPDATE mutation; tests relevance-skip recovery) | Untested | Two dedicated tests validate WHERE `col = (SELECT ...)` with relevance-skip; DEFERRED mode |
| WHERE filter + relevance-skip | Proven | `pg_test_deferred.rs::test_deferred_upd_respects_where_predicate` (assert_imv_correct on UPDATE to inactive row outside filter), `test_flush_deferred_skips_imv_on_predicate_miss` (multiple IMVs with different predicates, INSERT mutation); `pg_test_directional_dispatch.rs` (15 assert_imv_correct on WHERE-filtered aggregates) | Untested | WHERE predicate correctness validated across DEFERRED + IMMEDIATE modes; filter-miss skip logic tested |
| Partitioned source | Proven | `pg_test_partition.rs` (10 assert_imv_correct on LIST/RANGE partitions with INSERT/UPDATE/DELETE); `pg_test_partition_dispatch.rs` (4 assert_imv_correct on cold update/delete paths); single-level and multi-level partitions | Untested | Comprehensive partition-specific maintenance tested; ATTACH-with-data scenarios included |
| Ignore_sources | Weak | `pg_test_deferred.rs::pg_test_deferred_ignore_sources_skips_imv` (no assert_imv_correct; only SUM-value spot-checks) | Untested | Validation only via count/sum values, not full-relation oracle; IMMEDIATE + DEFERRED hybrid scenario tested but weakly |
| Deferred refresh mode | Proven | `pg_test_deferred.rs` (73 assert_imv_correct across entire file); INSERT/UPDATE/DELETE with explicit `reflex_flush_deferred` calls; multiple mutations per test; tests WHERE predicate filtering and pending queue interaction | Untested | Strongest evidence for a feature: 73 oracle calls across diverse scenarios |
| Cross-source consistency guard | Untested | `pg_test_e2e.rs` "cc_" tests (`test_combo_cte_join_having`, `test_combo_union_aggregate_operands`, etc.) are combination tests, not anti-double-count validation; no dedicated multi-join-aggregate test checking that a base row is not counted multiple times across source aliases or multiple UNION operands | Untested | **Critical gap:** no test explicitly validates "double-count prevention" (e.g., in `SELECT a.id, COUNT(*) FROM t a JOIN t b` scenarios or self-joins) |

### Reading

**Correctness axis:** 8 constructs **Proven** (single-source agg, deferred mode, LEFT JOIN, aggregate+LEFT JOIN secondary, CTE, UNION ALL, scalar-subquery filter, WHERE+relevance-skip, partitioned source), 5 **Weak** (passthrough, multi-source agg, inner join, DISTINCT ON, window function, IGNORE_SOURCES), 1 **Untested** (cross-source consistency guard — a correctness gap).

**Plan-quality axis:** All constructs **Untested** except the calibration baseline. The audit suite does not yet assert O(delta) scaling for individual constructs; only `assert_sublinear` exists in `pg_test_audit_gaps.rs` and has validated passthrough's O(delta) behavior as proof-of-concept.

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
