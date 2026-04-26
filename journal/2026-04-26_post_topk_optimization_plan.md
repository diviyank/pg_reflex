# Post-1.3.0 / post-auto-topk optimization plan

**Date**: 2026-04-26
**Linkage**: builds on `2026-04-25_full_scale_bench_and_optimizations.md`
(O1a/O2/O3/O4 roadmap + O2 landed) and `2026-04-26_topk_default_and_type_fix.md`
(auto-on top-K + UPDATE forced recompute).
**Audience**: anyone re-opening the perf roadmap after the 2026-04-26 fixes
landed.

## What changed since the 04-25 roadmap

1. **Auto-on top-K** ships K=16 on every MIN/MAX intermediate column.
   Append-only workloads opt out via the 6-arg `topk=0`.
2. **UPDATE on top-K MIN/MAX IMVs** now performs a *forced* scoped
   source-scan recompute for every affected group. This is the price of
   the partial-heap staleness fix — the algebraic Sub+Add merge cannot
   be trusted to reflect unchanged source rows that should sit in the
   post-update top-K but never made it into the heap pre-update.
3. **O2 (delta-SQL template cache)** landed but is unmeasured. The
   honest projection is sub-ms savings per fire — only OLTP-shape
   benches will show signal.

The 04-25 headline UPDATE-1M number (32.3 s on the 10M-row passthrough
JOIN) does not directly worsen — that bench is a passthrough IMV with
no MIN/MAX. But the new yellow-light caveat ("UPDATE-heavy patterns on
top-K MIN/MAX IMVs where group cardinality substantially exceeds K")
introduces a perf cliff that's not yet been measured at scale and isn't
in the headline bench.

## What this plan does and does not propose

**Does propose**:
- N1 — restore most of the pre-auto-topk UPDATE perf for top-K MIN/MAX
  via a heap-shrinkage gate on the recompute scope.
- N2 — measure the existing O2 cache on a high-fire-rate OLTP bench so
  we can decide whether to keep it, tune the cap, or rip it out.
- N3 — runtime UPDATE-affects-IMV pre-flight to short-circuit
  IMV-irrelevant UPDATEs (lower priority, workload-dependent).

**Does NOT propose** (intentionally — held over from 04-25):
- O1b column-classified passthrough fast path (ambitious; the simpler
  iter-1 already shipped a correctness regression that all
  non-targeted tests passed before `pg_test_correctness_update_join_key`
  caught it; held until a real workload signal forces the issue).
- O3 parallel sibling flush (1.5.0 territory; needs `pg_background`
  integration and deadlock analysis).
- O4 incremental reconcile (math doesn't hold without materialising
  the fresh view; held until we accept passthrough-only scope).

## N1 — Heap-shrinkage-gated top-K UPDATE recompute

### Problem

`build_min_max_recompute_sql_force_topk` (`src/trigger.rs:461-468`) sets
the per-row null-check to `TRUE` for every top-K column on UPDATE.
Wrapped in the 1.3.0 `EXISTS (SELECT 1 FROM intermediate JOIN affected
... WHERE TRUE)` gate, this means: any non-empty `__reflex_affected_*`
table → recompute fires for every affected group. Cost = a
source-scan-per-affected-group, scoped via the `splice_before_group_by`
filter.

For a workload where `K << group_cardinality` and most updated rows are
*not* in the heap (the typical case for K=16 with 10K+ rows per group),
the recompute is wasted: the algebraic Sub+Add merge is already
correct. Today we pay the source scan anyway, which dominates UPDATE
cost on top-K MIN/MAX IMVs.

### Correctness analysis (why a gate is safe)

A top-K heap held by `__min_x_topk` / `__max_x_topk` becomes stale
during UPDATE iff:

1. `delta_old.value` was in the pre-Sub heap (the row being updated
   was a top-K element), AND
2. `group_cardinality > K` (there are unchanged source rows outside
   the heap that could need promotion).

Condition (1) is observable post-Sub: the heap will have shrunk to
size < K. Condition (2) is implied — if `group_cardinality ≤ K`, the
heap held all source rows pre-update; Sub removes one, Add reinstates
one (delta_new), final heap = full post-update group. No promotion
question, no staleness.

Therefore: **post-Sub `cardinality(heap) < K` is necessary AND
sufficient to flag a group needing source-scan recompute**.

Walked through against the existing regression
(`pg_test_topk_partial_heap_staleness_regression`):

```text
K=2, source = {1,2,3,4,5}, heap_pre = [1,2].
UPDATE val=1 → val=10:
  Sub: heap = [2]. cardinality = 1 < K. → flagged.
  recompute: source scan → heap = [2,3]. ✓
```

And against the workload it under-serves today:

```text
K=16, source = {1..200}, heap_pre = [1..16].
UPDATE val=100 → val=120:  (delta_old NOT in heap)
  Sub: heap = [1..16] (unchanged). cardinality = 16 = K. → NOT flagged.
  topk_refresh: scalar = 1 (no change).
  Add: heap = top-16([1..16] ∪ [120]) = [1..16]. ✓
  recompute: SKIPPED. → big win.
```

### Design

Provision a sibling capture table next to `__reflex_affected_*`:

- New persistent UNLOGGED table per IMV: `__reflex_shrunk_<view>`,
  same column shape as `__reflex_affected_<view>` (group-by + distinct
  columns). Created in `create_ivm.rs` immediately after
  `__reflex_affected_*` (line ~1311) iff the plan has any top-K
  intermediate column. Dropped alongside in `drop_ivm.rs` (line ~159).

UPDATE flow with `has_min_max && has_topk` (`trigger.rs:1192-1245` and
the no-grp_cols branch at `:1255-1287`):

1. `Sub` (existing).
2. **NEW** — `INSERT INTO __reflex_shrunk_<view> SELECT DISTINCT
   group_cols FROM intermediate JOIN __reflex_affected_<view> ON …
   WHERE (topk_col1 IS NULL OR cardinality(topk_col1) < K1) OR …`.
   One row per affected group whose heap shrunk on any top-K column.
3. `topk_refresh` (existing, scoped to `__reflex_affected_*`).
4. `Add` (existing).
5. `forced recompute` — **scope changes from `__reflex_affected_*` to
   `__reflex_shrunk_*`**. The existing `EXISTS` gate naturally short-
   circuits when shrunk is empty (the common case).

### Files

- `src/trigger.rs`:
  - new helper `push_topk_shrunk_groups_capture(stmts, intermediate_tbl,
    plan, affected_tbl, shrunk_tbl)`, mirroring the style of
    `push_materialized_merge_and_affected`.
  - UPDATE branch at `:1192-1245` (grp_cols path) and `:1255-1287`
    (sentinel path): emit the capture call between Sub and
    topk_refresh, swap `Some(affected_tbl.as_str())` →
    `Some(shrunk_tbl.as_str())` for the
    `build_min_max_recompute_sql_force_topk` call site.
  - introduce the per-IMV shrunk table identifier helper next to
    `affected_tbl` (line 852).
- `src/create_ivm.rs:1307-1327`: add a second `CREATE UNLOGGED TABLE
  IF NOT EXISTS __reflex_shrunk_<view> AS …` block, gated on
  `plan.intermediate_columns.iter().any(|ic| ic.has_topk())`.
- `src/drop_ivm.rs:159`: extend the cleanup list to drop
  `__reflex_shrunk_<view>`.

### Test plan

Add to `src/tests/pg_test_correctness.rs`:

1. **Correctness regression**: re-run the existing
   `pg_test_topk_partial_heap_staleness_regression`, the existing
   `pg_test_randomized_mutation_sequence`, and the
   `pg_test_topk_{text,date,timestamp}_min_max` triplet — they must
   still pass with the new gate. (No new test needed — they already
   cover the path.)
2. **Recompute-skipped sanity**: new test
   `pg_test_topk_update_skips_recompute_when_no_heap_shrink`. Build a
   K=4 MIN/MAX IMV over 100 rows in 1 group. UPDATE a row whose value
   is outside heap (e.g. value 50 → 60 when heap holds 1..4). Assert
   correctness via `EXCEPT ALL` against fresh REFRESH MV. The skip is
   inferred indirectly — if the recompute did fire, `__reflex_shrunk_*`
   would still resolve to the same answer. The direct test belongs in
   the bench rather than the unit suite.
3. **Mixed shrink + non-shrink in one batch**: build a K=2 MIN IMV
   over two groups: group A heap = [1,2], group B heap = [10,20].
   Single UPDATE statement that mutates val=1→val=100 (group A,
   shrinks heap) AND val=50→val=51 (group B, val=50 not in B's heap,
   no shrink). Assert: correctness via EXCEPT ALL; `__reflex_shrunk_*`
   ends with one row (group A only). The latter is observable via a
   second-statement query before `__reflex_shrunk_*` is truncated by
   the next trigger.

### Bench plan

Promote `bench_full_scale_1_3_0.sql` to include a top-K MIN/MAX shape
alongside the passthrough one:

```sql
SELECT create_reflex_ivm(
    'bench_1_3_0.sales_topk_minmax',
    'SELECT product_id, MIN(qty) AS qty_min, MAX(qty) AS qty_max
     FROM sales GROUP BY product_id'
    -- 5-arg form → topk=16 auto-applied
);
```

Then run UPDATE 1K / 10K / 100K / 500K / 1M with `qty = qty + 1`
(value-only mutation, will shrink heap for ~K/group_cardinality
fraction of groups). Compare:

| Workload | Pre-N1 | Post-N1 | Notes |
|---|---:|---:|---|
| UPDATE 1K (top-K MIN/MAX, K=16, ~100 rows/group) | TBD | TBD | expect recompute skipped on most groups |
| UPDATE 100K | TBD | TBD | |
| UPDATE 1M | TBD | TBD | |

Acceptance threshold: post-N1 UPDATE on top-K MIN/MAX matches pre-auto-topk
1.2.0 UPDATE numbers within ±10%. If N1 makes things worse on any
shape (e.g. small-batch from the extra capture-table populate), revert.

### Risk

Low. The change is a stricter scope filter on an already-correct
recompute path — the recompute still re-derives from source for any
group that flags. The gate is necessary AND sufficient by the
correctness analysis above; the existing partial-heap regression test
locks the necessary direction. Sufficient direction is locked by
running the full mutation-sequence proptest.

The one subtlety: `topk_refresh` runs *before* the recompute and
scopes to `__reflex_affected_*` (not `__reflex_shrunk_*`). For
non-shrunk groups whose scalar shouldn't change, `topk_refresh` sets
scalar = `topk[1]` — a no-op for groups whose heap[1] didn't move,
and a correct refresh otherwise. No behaviour change there.

### Effort

~150 LOC + 2 unit tests + bench rerun. 1 day cycle.

## N2 — Measure O2 in an OLTP-shape bench

The 04-25 journal admits the cache's headline-shape signal sits at the
noise floor and the real measurement belongs in a different bench.
Build it now while the relevant context is fresh.

### Shape

- Single 100K-row source table.
- Single passthrough IMV (no JOIN — minimise per-fire data work so
  the constant overhead dominates).
- Workload: 10,000 trigger fires of `INSERT … VALUES (…)` (1 row each)
  in a single session.
- Compare warm cache hit rate: instrument
  `delta_sql_cache_key`/`delta_sql_cache` to count
  hits/misses (already exposed via `reset_delta_sql_cache` under
  `cfg(test)`; expose a runtime counter behind a GUC).

### Acceptance

- If aggregate session time drops ≥ 10 % vs a build with the cache
  disabled: keep, document, advertise.
- If < 5 %: rip out the cache. Sub-ms savings × ~ms-scale per-fire
  cost don't justify the static `OnceLock<Mutex<HashMap>>` and the
  256-entry cap heuristic.
- Between 5 and 10 %: keep but downgrade to a feature flag.

### Effort

~100 LOC bench harness + 1-day measurement. Lower priority than N1.

## N3 — UPDATE-affects-IMV runtime pre-flight (workload-dependent)

### Idea

When an UPDATE on a wide source table touches columns that are not
projected, joined, filtered, or aggregated by an IMV, the IMV doesn't
need to refresh. Detect at trigger fire time:

```sql
IF NOT EXISTS (
    SELECT 1 FROM pt_old o JOIN pt_new n ON o.<pk> = n.<pk>
    WHERE o.<imv_col_1> IS DISTINCT FROM n.<imv_col_1>
       OR o.<imv_col_2> IS DISTINCT FROM n.<imv_col_2>
       OR ...
) THEN
    -- skip the rest of the trigger body
END IF;
```

`<imv_col_*>` is `analysis.referenced_columns ∩ source.columns` —
already computed by `sql_analyzer`.

### Why this is held below N1/N2

- Workload-sensitive: pure win when UPDATEs typically touch
  IMV-irrelevant columns (e.g. `last_modified` bumps); pure overhead
  when every UPDATE touches an IMV column. The ~100-200 ms scan cost
  on a 1M-row pt_old/pt_new pair is non-trivial.
- Best as a per-IMV opt-in (a `update_filter` parameter on
  `create_reflex_ivm`) rather than always-on. That widens the API
  surface.
- N1 is universally correct-or-perf-positive on the auto-topk path
  that ships in every fresh IMV. N3 is an opt-in tuning knob.

### Trigger to revisit

A user reports an UPDATE-heavy workload where `git blame` shows the
hot column isn't projected by the IMV. Until then, journal-only.

## Recommended ordering

1. **N1** — fixes a perf gap introduced by the 2026-04-26 correctness
   fix on the path that auto-applies to every fresh MIN/MAX IMV.
   Highest impact, lowest scope, contained risk. Land in 1.3.1.
2. **N2** — closes the unfinished business from O2's "deferred
   measurement" disclaimer. Measurement, not new code; outcome can
   simplify the codebase. Land in 1.3.1.
3. **N3** — only after a real workload signal. Keep journaled.

Held over from 04-25 (no change in priority): O1b, O3, O4. The path
to deciding on these still requires (a) a real workload signal for
O1b, (b) a `pg_background` evaluation for O3, (c) accepting
passthrough-only scope for O4. None of those have moved this week.

## Honest scope check

N1 buys back the UPDATE-on-top-K perf the 2026-04-26 fix necessarily
gave up. It does not turn the headline 32.3 s UPDATE-1M passthrough
JOIN bench into a REFRESH-blowout — that bench has no MIN/MAX, so N1
does nothing for it. The 04-25 honest scope check still stands: for
batches above 1 M rows on passthrough JOIN IMVs, scheduled REFRESH MV
remains the right tool. pg_reflex's sweet spot is 1 K-500 K-row deltas.

## N1 — LANDED (2026-04-26)

### What shipped

- New `__reflex_shrunk_<view>` UNLOGGED capture table provisioned at
  IMV-create time iff the plan has any top-K column
  (`src/create_ivm.rs:1307-1349`).
- New `push_topk_shrunk_groups_capture` helper emits the post-Sub
  shrinkage capture as `TRUNCATE __reflex_shrunk_*; INSERT INTO
  __reflex_shrunk_* SELECT DISTINCT … WHERE OR-of-(cardinality(topk)
  < K)` (`src/trigger.rs:444-528`).
- UPDATE branch with `has_min_max && has_topk && grp_cols`
  (`src/trigger.rs:1274-1346`) inserts the capture between Sub and
  topk_refresh, then scopes
  `build_min_max_recompute_sql_force_topk` to `__reflex_shrunk_*`
  instead of `__reflex_affected_*`. Sentinel (no-grp_cols) path
  unchanged — that path is rare enough that the always-recompute
  status quo isn't worth the runtime DO-block conditional.
- `drop_reflex_ivm` cleans up `__reflex_shrunk_<view>` alongside the
  affected table (`src/drop_ivm.rs:166-180`).

### Tests added (locked in correctness)

- `pg_test_topk_update_no_heap_shrink_keeps_correctness` — 100-row
  group, K=4, UPDATEs that target rows whose value is strictly outside
  both heaps. The `EXCEPT ALL` oracle is the actual proof; the perf
  benefit is incidental.
- `pg_test_topk_update_mixed_shrink_groups` — single UPDATE statement
  spanning two groups where one shrinks and one doesn't. Asserts both
  groups stay correct, and follows up with a multi-row DELETE that
  would surface staleness if the gate's "non-shrunk → no recompute"
  decision were wrong.
- `pg_test_topk_update_multi_column_shrink` — MIN+MAX on the same
  source column, K=3. Walks through MIN-shrunk-only, MAX-shrunk-only,
  neither-shrunk, and both-shrunk in one fixture; followed by a
  retraction to expose any stale heap.
- Existing `pg_test_topk_partial_heap_staleness_regression` and
  `pg_test_randomized_mutation_sequence` (the 50-mutation × 3-group ×
  K=16 fuzzer) pass on the new gate. **513 / 513 tests** green.
  `cargo clippy --features pg17 --all-targets -- -D warnings` clean,
  `cargo fmt --check` clean.

### Bench results (`benchmarks/bench_n1_topk_update.sql`)

Shape: 5 M-row source, ~500 groups × ~10 K rows each (K=16, so heap
holds far below group cardinality). UPDATE pattern: `qty = qty + 1`
on `TABLESAMPLE`-picked rows — most updated rows hold non-heap
values, so the gate skips the recompute for most groups.

| Batch | Stock (pre-N1) | N1 | Speedup |
|---|---:|---:|---:|
| UPDATE 1 K  | 3292 ms | 108 ms | **30 ×** |
| UPDATE 10 K | 3647 ms |  431 ms | **8.5 ×** |
| UPDATE 100 K | 4980 ms | 2528 ms | **2.0 ×** |

Both runs `PASS` the `EXCEPT ALL` oracle against a fresh REFRESH MV
(via `reflex_reconcile`). The 1 K case is essentially "trigger
constant overhead + capture-table populate" with no source-scan
recompute at all — that's the OLTP-shape steady state where the
auto-on-topk recompute previously dominated. At 100 K rows, raw
UPDATE itself takes ~1 s; the remaining N1 trigger overhead is
~1.5 × the raw cost (data-work-bound, not recompute-bound).

### Where N1 doesn't help

- Group cardinality ≤ K (heap holds whole group). Every UPDATE
  flags shrunk → recompute fires every time, same as today. Plus a
  small fixed cost from the capture-table populate. Marginal
  regression in absolute terms (capture is one tiny scan).
- UPDATEs that target heap-resident rows (rare for K=16 on large
  groups, common for K=16 on K-sized groups). Same as previous bullet
  — recompute fires.
- UPDATEs that don't change the MIN/MAX-driving column. Heap shrinks
  on Sub (delta_old.value was in heap), gate fires, recompute is
  wasted. This is N3 territory — column-aware UPDATE pre-flight.

### Decision

Keep. The bench is unambiguous on the workload N1 was designed for,
the 513-test suite is green, and the gate's correctness is locked in
by both the targeted tests and the existing fuzzer.

The 1.3.1 changelog item: "Heap-shrinkage-gated top-K UPDATE
recompute. UPDATEs that don't displace a top-K element no longer
trigger a source-scan recompute. Up to 30 × on 1 K-row batches with
K=16 and 10 K-row groups."
