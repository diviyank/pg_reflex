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
| Aggregate + LEFT JOIN secondary | Weak | `pg_test_passthrough.rs::pt_secondary_keyed_left_join_all_ops_immediate/deferred` exercises a **passthrough** LEFT JOIN, NOT the aggregate-with-secondary shape; no test covers the exact 1.10.1 shape (`SELECT g, SUM(x) ... LEFT JOIN <dim/sub-agg>`) | Untested | **This is the 1.10.1 shape.** Cited evidence is a non-aggregate join — aggregate-secondary correctness AND plan-scaling are both open. Task 4 instruments the plan-scaling directly. |
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

**Correctness axis (16 constructs):** 8 **Proven** (single-source agg, deferred mode, LEFT JOIN, CTE, UNION ALL, scalar-subquery filter, WHERE+relevance-skip, partitioned source), 7 **Weak** (passthrough, multi-source agg, inner join, aggregate+LEFT JOIN secondary, DISTINCT ON, window function, ignore_sources), 1 **Untested** (cross-source consistency guard). Note the aggregate+LEFT-JOIN-secondary row (the 1.10.1 shape) is Weak, not Proven — its only nearby test is a passthrough join; Task 4 instruments it.

**Plan-quality axis:** All constructs **Untested** except the calibration baseline. The audit suite does not yet assert O(delta) scaling for individual constructs; only `assert_sublinear` exists in `pg_test_audit_gaps.rs` and has validated passthrough's O(delta) behavior as proof-of-concept.

### Phase 2 update (supersedes the cells above)

Phase 2 (modules M1+M2) moved several cells. Current state:

- **Cross-source consistency guard — correctness: Untested → Proven** (`pg_test_cross_source.rs`, 7 oracle tests; §3 M1). The single remaining correctness gap from the Phase-1 snapshot is closed; correctness is now **9 Proven / 7 Weak / 0 Untested**.
- **Plan-quality: Untested → Proven** for **passthrough** (calibration), **single-source aggregate**, **multi-source / inner-join aggregate**, **aggregate + LEFT-JOIN secondary** (Phase-1 Task 4), **CTE-decomposed**, and **UNION ALL** (`audit_*_is_sublinear`, §3 M2). Plan-quality is no longer "uniformly Untested": the six aggregate/passthrough shapes are Proven O(delta); DISTINCT ON, window, inner-join-passthrough, partitioned, filter shapes remain Untested on plan-quality (Phase 2B).
- Caveat unchanged: these are wall-clock heuristics under one mutation each; the white-box `EXPLAIN`-rows probe (Phase 2B M3) and the combinatorial gate axes (M4–M6) are the rigorous generalization.

## §2 Escape analysis — why each field bug since 1.7.2 slipped

| Release | Bug (1 line) | Escaped axis-combo | Root cause | Covered today? | Phase-2 axis that closes it |
|---------|--------------|-------------------|-----------|----------------|---------------------------|
| 1.7.2 | `drop_reflex_ivm` orphaned target + aux tables of bare-name IMVs under non-`public` `search_path` | IMV creation & drop DDL (not query maintenance) | correctness | yes, `regression_*` drop path | IMV lifecycle management (DDL, not query-shape) |
| 1.7.1 | Path C smart bulk-INSERT failed on bare-name/long-name single-source aggregates; dual: silent double-count when no `source_join_keys` entry | Single-source aggregate × UPDATE promotion × long identifier/bare names (Path C name reconstruction bypass) | correctness | yes, `test_path_c_block_does_not_*` + gate lifecycle axis | IMV lifecycle / identifier canonicalization (DDL, not query-shape) |
| 1.7.3 (fix #1) | Failed CTE decomposition creation orphaned already-created sub-IMVs | CTE decomposition × creation failure (not maintenance) | correctness | yes, `test_cte_decomposition_failure_rolls_back_sub_imvs` | IMV lifecycle (DDL/creation, not query-shape) |
| 1.7.3 (fix #2) | Partition-anchor resolution ambiguously failed when decomposed query had two partitioned owners (base + CTE intermediate) | Partitioned source × CTE decomposition × partition-depth inference (anchor resolution, not maintenance) | correctness | yes, `pg_part_anchor_prefers_base_over_cte_intermediate` | IMV lifecycle / partition-depth inference (DDL, not query-shape) |
| 1.7.4 | Partition-anchor resolution accepted sources partitioned on *different* column and co-partitioned sources triggered "ambiguous" error | Partitioned source × multi-source JOIN × partition-key inference (anchor resolution, not maintenance) | correctness | yes, `pg_part_copartitioned_full_join_of_cte_intermediates` | IMV lifecycle / partition-key matching (DDL, not query-shape) |
| 1.7.5 | Chained-CTE unique-key inference failed on equi-join equivalence (e.g., `f.k = dl.k`) and cross-JOIN-to-aggregate cardinality probes | CTE-decomposed × multi-source JOIN × unique-key cardinality inference (anchor/key inference, not maintenance) | correctness | yes, CTE integration tests in `pg_test_cte.rs` | IMV lifecycle / key inference (DDL, not query-shape) |
| 1.7.6 | `ignore_sources` was silently ignored on DEFERRED trigger path (INSERT/DELETE/UPDATE/TRUNCATE triggers and flush) | DEFERRED mode × `ignore_sources` axis × trigger/flush code paths (source filtering, not query shape) | correctness | yes, `pg_test_deferred_ignore_sources_skips_imv` | DEFERRED mode × source filtering (missing from pairwise, Table sources only) |
| 1.8.1 | No partition ATTACH/DETACH/SWAP capture on multi-level partitioned sources; DDL event trigger + flush omitted for sub-partitions | Partitioned source × multi-level depth × DDL operations (ATTACH/DETACH not captured by row triggers) | correctness | yes, `pg_test_partition.rs` multi-level reconcile tests | Partitioned source × multi-level depth (not in pairwise; Table sources, single-level only) |
| 1.8.2 | Unpartitioned IMV on partitioned source went stale on source partition SWAP (which fires no row trigger) | Partitioned source × IMV shallow depth (depth 0) × partition SWAP (DDL, no row trigger) | correctness | yes, partition-depth-aware flush tests | Partitioned source × variable IMV depth (not in pairwise; Table sources only) |
| 1.9.0 (fix #1) | Inner CTE sub-IMV never detected source PRIMARY KEY — catalog lookup silent type mismatch → keyless → full rebuild every flush | CTE-decomposed × single-source passthrough × PRIMARY KEY detection (type binding mismatch, not query shape) | plan | yes, `pg_test_cte.rs::test_cte_cascade_keyed_maintenance` | CTE-decomposed passthrough × keyed inference (not in pairwise; Table sources only) |
| 1.9.0 (fix #2) | Partition-aware trigger dispatch was O(rows) instead of O(partitions) due to per-row child OID lookup | Partitioned source × dispatch codegen (per-row iteration, not query shape) | plan | yes, partition dispatch tests measure via EXPLAIN | Partitioned source (not in pairwise dispatch cost assertions; Table sources only) |
| 1.9.1 | In-place UPDATE optimization fallback path was never guarded by `source_join_keys` check, risking silent double-count on multi-source aggregates | Partitioned source × UPDATE promotion × single-source aggregate PATH C (missing source_join_keys gate, not query shape) | correctness | yes, `reflex.assert_inplace_update` GUC + related tests | Partitioned UPDATE dispatch × multi-source aggregate (not in pairwise; Table sources only) |
| 1.9.2 | Zero-length delimited identifier at COMMIT: trigger-time `replace_source_with_transition` rewriter missed quoted source names, injecting transition name *inside* existing quotes | CTE-decomposed × DEFERRED mode × schema-qualified source × quoted spellings (rewriter edge case, not query shape) | correctness | yes, `unit_trigger.rs` rewriter regression tests + `test_cte_trigger_propagation_deferred` | CTE-decomposed × DEFERRED mode × rewriter quote-handling (not in pairwise; implicit in decomposition tests) |
| 1.10.0 (fix #1) | ATTACH PARTITION with data left IMV partition empty — `__reflex_partition_pending` enqueued but never auto-drained | Partitioned source × ATTACH DDL × pending queue (no auto-drain trigger, not row-triggered maintenance) | correctness | yes, `test_attach_with_data_auto_syncs_at_commit` | Partitioned source × DDL event trigger (ATTACH/DETACH; Table sources only, not in pairwise) |
| 1.10.0 (fix #2) | One broken root wedged entire partition flush — `reflex_flush_partitions` aborted on first error, rolled back all batch work | Partitioned source × flush error handling (per-root isolation missing, not query shape) | correctness | yes, `test_flush_isolates_failing_root_from_healthy_root` | Partitioned source × error isolation (not in pairwise; Table sources only) |
| 1.10.0 (fix #3) | Shape drift made reconcile throw "is not partitioned" — wrong-relkind child not detected on source rebuild | Partitioned source × partition-depth mismatch × CREATE TABLE IF NOT EXISTS silent-skip (shape validation, not query shape) | correctness | yes, `test_sync_heals_leaf_child_into_partitioned` | Partitioned source × shape validation (not in pairwise; Table sources only) |
| 1.10.1 | Aggregate LEFT-JOIN-secondary updates re-aggregated entire base instead of affected groups (18 min for 2-row delta) | Multi-source aggregate × LEFT-JOIN secondary × affected-groups inference (optimization omission, not correctness bug, but caught in field) | plan | yes, `test_outer_join_secondary_aggregate_scopes_recompute_by_join_keys` | Multi-source aggregate × LEFT-JOIN secondary × plan-quality assertions (not in pairwise; no plan-quality axis at all) |
| 1.10.2 | Scalar-subquery WHERE filters were dropped from per-source metadata → irrelevant updates maintained + silent loss of correct rows on key collision | Single-source aggregate × scalar-subquery WHERE filter × metadata attribution (subquery depth-awareness, not query shape per se) | correctness | yes, `test_passthrough_subquery_filter_skips_noncurrent_group_deferred` + `cov_rebuild_metadata_restores_subquery_filter_skip` | Single-source aggregate × scalar-subquery filter × relevance-skip (WHERE filters not in pairwise; Table sources only) |

### Pattern

The 20 distinct bugs map to two families:

1. **DDL & lifecycle gaps (10 bugs):** drop, creation, partition anchor/depth/shape inference, CTE decomposition rollback, and partition ATTACH/DETACH/SWAP capture — all outside query *maintenance* correctness. These involve IMV creation, drop, partition DDL handling, and metadata reconstruction, which the pairwise harness does not exercise (it tests maintenance only, and only on Table sources). The harness has **zero lifecycle axis** and **no Table-vs-View/MatView/CTE-sub-IMV source-kind variation** beyond the regression gate's cosmetic stub.

2. **Query-shape maintenance gaps (10 bugs):** six are **correctness** (scalar-subquery filters, CTE sub-IMV PK detection bypass, in-place UPDATE without join-key gate, quoted-source rewriter, partition queue drain, 1.10.2 filter metadata), and one is **plan quality** (1.10.1 LEFT-JOIN secondary re-aggregation). The common pattern: **no axis for WHERE filters** (scalar subqueries, predicates), **no dedicated assertion for plan-quality scaling** (1.10.1 was found in the field as "this takes 18 minutes"), and for partitioned sources, **no axis for DDL-triggered operations** (ATTACH/DETACH/SWAP) or **multi-source aggregate + secondary mutations** (the 1.10.1 shape).

### Phase-2 seed axes

The 20 bugs collapse to the following deduplicated Phase-2 axis additions:

- **IMV lifecycle management** — drop, creation (incl. decomposition rollback), partition anchor/depth inference, partition shape validation. Currently zero coverage; pairwise tests maintenance only.
- **Partition DDL triggers** — ATTACH/DETACH/SWAP capture, pending-queue auto-drain, per-root error isolation, shape-drift healing. Currently tested post-hoc via `cargo pgrx test`; no axis model.
- **WHERE-filter shapes** — scalar-subquery filters, correlated subqueries in WHERE, non-Table sources as filters. Currently zero coverage in pairwise (6 shape axes, all Table sources).
- **Multi-source aggregate × LEFT-JOIN secondary mutations** — the 1.10.1 shape: correctness oracle coverage exists (secondary keyed tests) but **zero plan-quality assertions** (no `assert_sublinear` for any aggregate shape). Axis: secondary source mutation while aggregate is maintained.
- **Plan-quality assertions** — extend `assert_sublinear` from passthrough calibration to all aggregate shapes (single-source, multi-source, LEFT-JOIN secondary). Currently untested across the board.
- **Rewriter quote-safety** — trigger-time delta-table rewriter (`replace_source_with_transition`) must handle quoted schema-qualified sources; currently regex-based, fragile. Axis: source quoting style (bare, quoted, schema-qualified).
- **Deferred mode × source filtering** — `ignore_sources` path on all deferred triggers + flush. Fixed in 1.7.6; currently tested post-hoc, not a pairwise axis.

(Note: several "Phase-2 axes" are actually already tested post-create in oracle suites — e.g., partition ATTACH, LEFT-JOIN secondary keyed maint — but were found by field use, not by the pairwise gate. The audit distinction is that the pairwise gate does not exercise them systematically before release.)

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

Task 4 — multi-source aggregate + LEFT JOIN secondary: **PASS** (small=18ms, big=8ms) — maintenance O(delta), VERDICT Proven.

Task 5 — window ROW_NUMBER update re-rank: PASS — correctness Proven.

Task 6 — DISTINCT ON winner demotion: PASS — correctness Proven.

Task 7 — IN-subquery filter relevance: PASS — correctness Proven. Strengthened to be faithful to the 1.10.2 class: the IMV declares unique key `k`, and the out-of-filter row (k=1,p=2) collides on `k` with the in-filter row (k=1,p=1), so the keyed-delete path that silently removed in-filter rows in 1.10.2 is actually exercised. The fix covers `IN (SELECT …)`, not just `= (SELECT …)`.

### Phase 2B M6 — New gate shapes (WindowFn, DistinctOn) — **first gate-found bug**

Added `WindowFn` and `DistinctOn` as pairwise-gate `QueryShape`s (they were absent,
so the gate could not catch *any* window/DISTINCT-ON maintenance bug). Pairwise
case count 42 → 54.

- **WindowFn:** gate GREEN across the pairwise space — `ROW_NUMBER() OVER
  (PARTITION BY d ORDER BY id)` maintains correctly.
- **DistinctOn:** gate GREEN on the auto-inferred-key path (the path Phase 1
  proved correct). **BUT** the gate surfaced a real bug on the *declared-output-key*
  path:

**🐛 CONFIRMED BUG #1 (gate-found): DISTINCT ON + declared output key crashes CREATE.**
A `SELECT DISTINCT ON (d) …` IMV that declares its natural output unique key `d`
(via `unique_columns`) fails at CREATE:
```
INFO:  pg_reflex: using explicit unique key (d) for 'v__base'
ERROR: could not create unique index "__reflex_uk_v__base"
DETAIL:  Key (d)=(g0) is duplicated.
```
Root cause: pg_reflex classifies DISTINCT ON as **passthrough**, so
`resolve_unique_columns` (`src/create_ivm/mod.rs:208`) applies the declared key to
the `__base` decomposition table — but `__base` holds the **pre-dedup** rows where
`d` repeats. Aggregates with the same `GROUP BY d` key are immune (not passthrough
→ they skip that path). Declaring the *correct* output key crashes creation.
Repro: `audit_distinct_on_declared_output_key_should_not_crash_create` (`#[ignore]`,
RED). Fix target: don't put the output key on `__base` for dedup shapes. Severity:
medium (create-time hard error, not silent corruption; workaround = omit the key
and let pg_reflex auto-infer). Tracked in §4.

### Phase 2B M4 / M1-axis — new gate axes

- **M4 `FilterMode` axis** (None / Simple / ScalarSubquery / InSubquery), scoped to
  Passthrough + SingleAggregate (the 1.10.2 region). Pairwise 54 → 63 cases, gate
  GREEN — no bug. The gate now systematically exercises `= (SELECT)` / `IN (SELECT)`
  filters × shape × refresh.
- **M1 `MutationSpread` axis** (SingleSource / AllSources). AllSources mutates ≥2
  sources in one deferred batch so the cross-source guard engages inside the gate
  (complements the Phase-2A oracle tests). Pairwise 63 → 64, gate GREEN.
  Model limitation recorded: AllSources is numeric-measure-only because the
  generator's `m = m + 1` update is numeric (not a pg_reflex bug).

**Gate-scope honesty.** The pairwise gate cases carry only the **seed INSERTs**
(`base_case_for_shape` sets `dml: vec![seed]`); the IMV is created first, then the
rows are inserted and maintained incrementally — so the gate tests CREATE +
**INSERT-maintenance** per axis combo, plus UPDATE/DELETE only on the AllSources
path. Full UPDATE/DELETE maintenance for the new shapes is covered by the Phase-1
targeted oracle tests (window re-rank, DISTINCT ON winner-change — both PASS) and
the random proptest path. The new shapes/axes are tested at the same fidelity as
every existing gate shape.

### Phase 2B M3 / M5 — worth-assessment (DEFER, with rationale)

Per the project's "assess the worth / don't over-complexify" rule, the two
remaining §4 items were assessed rather than force-built:

- **M3 (white-box `EXPLAIN`-rows plan probe): DEFER.** It would deterministically
  assert delta-scoping by EXPLAINing `reflex_build_delta_sql` output — but that SQL
  references transient transition tables (`__reflex_new_<src>`) that exist only
  inside a live trigger, so the probe must fabricate stand-in temp tables and parse
  FORMAT JSON (brittle to codegen changes). Marginal value is currently low: the
  wall-clock `assert_sublinear` probe already covered all six aggregate/passthrough
  shapes and found **zero** O(base) plans. Revisit if a future plan regression slips
  the heuristic. Design recorded here for when it's needed.
- **M5 (PartitionOp axis — ATTACH/DETACH/SWAP in the gate): DEFER.** Highest harness
  cost (partition-DDL rendering + a partition-aware oracle + multi-level depth)
  against an already-fixed, already-regression-tested region (1.8.x / 1.10.0 each
  shipped with targeted `#[pg_test]` coverage). Disproportionate cost-to-value now;
  a dedicated partition-DDL fuzz is a sensible future standalone effort.

### Phase 2 M1 — Cross-source consistency guard

The rank-1 Untested item. The guard (`src/trigger/deferred.rs:401-533`) engages
when ≥2 distinct sources stage deltas in one transaction and an IMV joins ≥2 of
them, full-reconciling that IMV once (via the `__reflex_deferred_reconciled_batch`
marker) so ΔA⋈ΔB is not double-counted. Seven oracle tests
(`src/tests/pg_test_cross_source.rs`), each diffing the IMV against a fresh
recompute via `assert_imv_correct`, **all PASS** (0 mismatches):

- `xs_inner_join_both_sources_mutated_matches_recompute` — inner join, both mutated.
- `xs_left_join_both_sources_mutated_matches_recompute` — left join, both mutated.
- `xs_insert_a_delete_b_matches_recompute` — INSERT×DELETE.
- `xs_update_a_update_b_matches_recompute` — UPDATE×UPDATE.
- `xs_single_source_mutated_incremental_matches_recompute` — one source only
  (guard must NOT engage; incremental path still correct).
- `xs_three_source_two_mutated_matches_recompute` — 3-source join, 2 mutated.
- `xs_two_imvs_shared_source_each_reconciled_once` — marker-skip path.

Verdict: the guard is correct across join types, mutation kinds, the no-engage
path, multi-way joins, and the marker. The rank-1 gap is closed as live coverage;
no bug found. (Same honest caveat as below: one mutation batch per shape; the
combinatorial gate axis — Phase 2B `MutationSpread` — generalizes it.)

### Phase 2 M2 — Plan-quality scaling

Four aggregate shapes instrumented with `assert_sublinear` across 20k and 500k row bases with identical 1-row deltas:

- M2.1 — `audit_single_source_aggregate_is_sublinear`: **PASS** (small=9ms, big=6ms) — O(delta), GROUP BY on single source scopes recompute to affected group.
- M2.2 — `audit_inner_join_aggregate_is_sublinear`: **PASS** (small=14ms, big=6ms) — O(delta), inner-join dimension lookup stays bounded.
- M2.3 — `audit_cte_decomposed_is_sublinear`: **PASS** (small=9ms, big=8ms) — O(delta), CTE sub-IMV recompute scoped by aggregation keys.
- M2.4 — `audit_union_all_is_sublinear`: **PASS** (small=6ms, big=4ms) — O(delta), delta routed to correct operand sub-IMV.

**Verdict-reading caveat (honest framing).** All four instrumented probes PASSED. That is genuine, confidence-restoring evidence that the recent fixes generalize — but each probe refutes a *specific named hypothesis* under *one* mutation; it is "not refuted," not "proven correct in general." Adversarial, multi-mutation, key-colliding coverage of these same shapes is exactly what Phase 2's combinatorial fuzz must add (see §4).

## §4 Risk-ranked gap backlog

Ranked by impact, with the project's priority order (correctness > simplicity >
performance) but with plan-quality elevated this cycle because it is the
specific pain that triggered the audit ("absolute worst query plans"). Each row
names its target phase. P2 = harden the combinatorial harness; P3 = field-replay.

| # | Gap | Axis | Evidence | Target |
|---|-----|------|----------|--------|
| 1 | **Cross-source consistency guard is entirely untested** — the anti-double-count mechanism has zero oracle coverage. A regression here is a *silent wrong result* (worst failure class). | correctness | §1 row "Cross-source consistency guard" = Untested | **P2** — add a multi-source/self-join double-count axis with oracle + an adversarial refuter |
| 2 | **No plan-quality assertion for any aggregate shape** — the entire 1.10.1 bug class (correct-but-O(base)) is unguarded; the field found it as "18 minutes". This audit added the *only* two plan probes (passthrough calib + multi-source-agg, both PASS). | plan | §1 plan-quality column = Untested everywhere; §3 Task 4 | **P2** — extend `assert_sublinear` to single-source, multi-source, and LEFT-JOIN-secondary aggregates; add white-box `EXPLAIN`-rows successor |
| 3 | **The pairwise gate has no axis for the field-bug regions** — all 20 escapes (§2) lived in WHERE-filters, scalar subqueries, partition DDL, or multi-source aggregates, none of which the gate exercises systematically before release. | both | §2 escape table (every "covered today? = yes" was field-found, not gate-found) | **P2** — add axes: WHERE-filter shapes, scalar/IN subquery filters, partition-DDL (ATTACH/DETACH/SWAP), IMV lifecycle (create/drop/decomposition rollback) |
| 4 | **Weak correctness coverage on window, DISTINCT ON, inner join, multi-source aggregate, ignore_sources** — point-value checks, no full-relation oracle except the single mutation each this audit just added (§3 Tasks 5–7 PASS, now active regressions). | correctness | §1 rows rated Weak; §3 Tasks 5–7 | **P2** — fuzz these shapes adversarially: multi-mutation, key-colliding, winner-change, mid-frame |
| 5 | **Plan-quality probe is a wall-clock heuristic** — trustworthy enough for Phase 1 confirm/refute, but not a structural guarantee. | plan (tooling) | §3 "Known limitation" | **P2** — white-box assertion on the generated maintenance plan (`EXPLAIN` actual-rows at the base relation) |
| 6 | **No regression derived from real production queries** — every field bug came from base-db views the suite never replayed. | both | §2 "found in field, not gate" meta-pattern | **P3** — distil base-db views (`current_assortment_activity_view`, `sop_incoming_stock_baseline_view`, …) into minimal oracle-checked regressions |
| **B1** | **🐛 GATE-FOUND BUG: DISTINCT ON + declared output key crashes CREATE** — pg_reflex applies the declared passthrough key to the pre-dedup `__base`, failing its unique index. Declaring the correct output key for a DISTINCT ON IMV is a hard create error. | correctness (create-time) | §3 Phase 2B M6; RED `audit_distinct_on_declared_output_key_should_not_crash_create` | **fix** — `resolve_unique_columns` / DISTINCT ON decomposition must not key `__base` by the output key. Medium severity (hard error, not silent; workaround = auto-infer). |

### Phase-2 entry point (deduplicated axis list to brainstorm from)

The next phase starts from this list, not a blank page (distilled from §2 §4):

1. **Cross-source double-count** oracle + adversarial refuter (rank 1 — correctness, silent-wrong).
2. **Plan-quality axis** — `assert_sublinear` for every aggregate shape, then a white-box `EXPLAIN`-rows successor (ranks 2, 5).
3. **WHERE-filter / subquery-filter axis** — `= (SELECT)`, `IN (SELECT)`, correlated, with relevance-skip + key-collision (rank 3; §3 Task 7 shows the shape).
4. **Partition-DDL axis** — ATTACH/DETACH/SWAP + pending-queue drain + per-root isolation (rank 3).
5. **IMV-lifecycle axis** — create/drop/decomposition-rollback/anchor-inference (rank 3; the 10 DDL-family escapes).
6. **Adversarial multi-mutation fuzz** for the Weak shapes — window, DISTINCT ON, inner join, ignore_sources (rank 4).

### Phase-1 bottom line

Four named suspected gaps were instrumented; **all four PASSED**, converting Weak/Untested cells into live oracle regressions and showing the 1.10.1/1.10.2 fixes generalize. The audit found **no new live bug** — but it found the *real* systemic holes: the cross-source guard is untested (rank 1), plan-quality is unguarded everywhere (rank 2), and the release gate never exercises the regions where all 20 field bugs lived (rank 3). Those are Phase 2's mandate.
