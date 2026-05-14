# Post-1.4.5 optimization options — pick-list

**Context**: `journal/2026-05-14_sop_forecast_levers_probe.md` measured the
SOP-forecast UPDATE workload after the 1.4.5 skip-filter-aware spurious-skip
landed. The pivot-without-output-change path is fast (~1 ms). The bulk
filter-flip path (rows entering/leaving the IMV) is the remaining pain
point: at 24 %+ selectivity, pg_reflex crosses below REFRESH MATERIALIZED
VIEW, and reflex_reconcile (called by the dispatch DO block at 30 %+) is
slower than REFRESH MV by ~2.6×.

**Goal of this plan**: enumerate concrete options, rank by ratio of expected
wall-clock saved to implementation complexity, and let the next session
pick from this menu. Nothing here is committed — each item is journal-grade
analysis with a sized scope.

## Headline shape today (1.4.5, post-77ca786)

| Workload | pg_reflex | REFRESH MV | Verdict |
|---|---:|---:|---|
| Spurious in-filter pivot | 1 ms | 5400 ms | Won (1.4.5) |
| Low-selectivity data UPDATE (≤1 K rows) | 19–63 ms | 5400 ms | Won |
| Mid-selectivity (10 K data rows) | 658 ms | 5400 ms | Won (8×) |
| Filter-flip 20 K rows (2.6 %) | 657 ms | 5400 ms | Won (8×) |
| Filter-flip 80 K rows (10 %) | 2 719 ms | 5400 ms | Won (2×) |
| **Filter-flip 180 K rows (24 %)** | **7 064 ms** | **5400 ms** | **LOST (-30 %)** |
| Reconcile path (≥30 %) | 14 000 ms | 5400 ms | **LOST (-2.6×)** |

## Cost contribution at the 180 K-row out→in case (7 s total)

| Step | Wall ms | % | Pure overhead vs REFRESH MV |
|---|---:|---:|:---|
| scratch INSERT (net-delta JOIN + groupby) | 3 100 | 44 % | partial (same JOIN, but extra `UNION ALL` wrapper) |
| affected DISTINCT INSERT | 190 | 3 % | redundant (scratch is already distinct) |
| MERGE intermediate | 1 629 | 23 % | full overhead (REFRESH skips intermediate) |
| dead-cleanup DELETE on intermediate | 388 | 5.5 % | full overhead, AND wasted on OUT→IN |
| target DELETE | 372 | 5.3 % | wasted on OUT→IN (no rows match) |
| target INSERT | 1 291 | 18 % | matches REFRESH's INSERT |
| plpgsql glue / metadata | 94 | 1.3 % | trigger overhead |

REFRESH MV at 5 400 ms is essentially `JOIN-scan + aggregate + INSERT` once.
At 180 K rows we're doing the JOIN scan on 24 % of source (3.1 s for the
delta vs ~5 s for the whole), but stacking 2.4 s of MERGE/scratch/cleanup
overhead on top.

## Option menu — ranked by win × ease

### Option 1 — Effective-INSERT shortcut for OUT→IN UPDATEs (HIGH win, MEDIUM complexity)

**The lever**: when an UPDATE moves rows from filter-rejected → filter-
accepted, the scratch contains only +1 contributions and intermediate +
target have no pre-existing rows for those keys. The current path runs:

- MERGE (every row hits WHEN NOT MATCHED → INSERT — but pays MERGE planner cost)
- dead-cleanup DELETE (0 rows match; pure scan waste)
- target DELETE (0 rows match; pure scan waste)
- target INSERT (the only step that does work)

**Proposed change**: after building scratch, run a 1-row probe:

```sql
SELECT bool_and(__ivm_count > 0) FROM __reflex_scratch
```

If true, the scratch is pure-add. Emit a fast path that:

1. `INSERT INTO __reflex_intermediate SELECT * FROM __reflex_scratch`
   (no MERGE — just bulk insert).
2. Skip dead-cleanup entirely.
3. Skip target DELETE entirely.
4. `INSERT INTO target SELECT … FROM __reflex_intermediate WHERE
   __ivm_count > 0 AND EXISTS (…affected)` — same as today.

For 180 K rows: saves ~2 400 ms (MERGE 1 629 ms + dead-cleanup 388 ms +
target DELETE 372 ms). Brings the 24 % case from 7 s to ~4.6 s, putting
pg_reflex back ahead of REFRESH MV (5.4 s) at this selectivity.

**Symmetric variant** for pure-remove (IN→OUT with all `__ivm_count <
0`): bulk DELETE FROM intermediate WHERE keys IN scratch, skip MERGE,
skip target INSERT.

**Risk**: medium. Need to handle MIN/MAX/BOOL_OR's top-K refresh paths
correctly (probably not relevant on pure-add since heaps fill from
zero). Need to handle the `passthrough` outer-join-secondary branches
which already have their own full-refresh fallback. Tests: add pgrx
correctness tests that exercise OUT→IN on aggregate IMVs with various
selectivities and assert EXCEPT-ALL = 0 against `REFRESH MATERIALIZED
VIEW`.

**Implementation**: ~150 LOC in `src/trigger.rs` (one new branch in the
UPDATE-with-grp_cols-no-min-max path; the MIN/MAX path is untouched
since heap state can't be inferred from scratch alone).

**Estimated effort**: 1 day code + 1 day tests + benchmark.

---

### Option 2 — Drop redundant `SELECT DISTINCT` on affected groups (TRIVIAL win, TRIVIAL complexity)

**The lever**: the scratch table is the output of a GROUP BY — every row
is unique on group keys. The trigger then runs `INSERT INTO __reflex_affected
SELECT DISTINCT group_cols FROM __reflex_scratch`. The DISTINCT is
redundant.

**Proposed change**: in `src/trigger.rs`, change the `SELECT DISTINCT
group_cols` to plain `SELECT group_cols` for paths fed by a grouped
scratch. The outer-join-secondary path's `INSERT INTO __reflex_affected
SELECT DISTINCT … FROM (delta_q)` keeps the DISTINCT because delta_q
isn't pre-grouped.

For 180 K rows: saves ~190 ms (HashAggregate over 180 K rows). For 20 K
rows: saves ~22 ms.

**Risk**: trivial. The scratch's GROUP BY guarantee is a documented
invariant — adding the DISTINCT was defensive only.

**Implementation**: ~5 LOC. Two callers in `reflex_build_delta_sql`
(`src/trigger.rs:1683-1685` and the corresponding place in the
outer-join-secondary branch, which keeps DISTINCT).

**Estimated effort**: 30 minutes + 1 test.

---

### Option 3 — `INSERT … ON CONFLICT DO UPDATE` for target sync (MEDIUM win, MEDIUM complexity)

**The lever**: target sync today is `DELETE FROM target WHERE EXISTS
(…affected); INSERT INTO target SELECT … FROM intermediate WHERE
__ivm_count > 0 AND EXISTS (…affected)`. Two scans of intermediate via
the EXISTS-affected predicate, plus a separate target index scan for
DELETE.

**Proposed change**: replace with `INSERT INTO target (cols) SELECT …
FROM intermediate WHERE __ivm_count > 0 AND EXISTS (…affected) ON
CONFLICT (target_keys) DO UPDATE SET (cols) = (EXCLUDED.cols)`. Single
statement, single scan, lets PG's heap_update use HOT when the target's
fillfactor=70 leaves slack (the 1.4.4 schema change).

A second statement still handles row *removals* (groups that existed in
target but are no longer in intermediate post-MERGE): `DELETE FROM target
WHERE EXISTS (…affected) AND NOT EXISTS (SELECT 1 FROM intermediate WHERE
keys match AND __ivm_count > 0)`. This is still cheaper than today's
unconditional DELETE-then-INSERT because most groups stay alive in
intermediate; the DELETE only touches the small subset that net-collapsed.

For 180 K rows: estimated ~600 ms savings (the current INSERT is 1 291
ms; ON CONFLICT path with HOT updates can be ~700 ms). The DELETE
shrinks to nearly nothing for typical workloads where most affected
groups stay alive.

**Risk**: medium. Reverted plan #10 (`plans/2026-05-13-merge-on-target-10.md`)
attempted the LEFT-JOIN MERGE form and hit a planner failure at scale
(400 M row Join Filter). `INSERT ON CONFLICT` is the simpler primitive
that PG plans deterministically. Need correctness tests for:
- Pure update of existing groups (heap_update HOT path)
- New group insertion
- Group removal via the secondary DELETE
- Mixed batches.

**Implementation**: ~200 LOC in `src/trigger.rs` (rewriting the
`target_delete_sql` + `target_insert_sql` pair). The dispatch DO block
needs to accept the new 2-statement form (it already takes a `&[&str]`
slice). Migration not required — codegen changes apply to all future
fires.

**Estimated effort**: 2 days code + 2 days tests (need to cover all
operation × shape combinations).

---

### Option 4 — Calibrated dispatch cost model (MEDIUM win, MEDIUM complexity)

**The lever**: `WIPE_THRESHOLD_DEFAULT = 0.3` was set when reconcile was
cheaper than incremental at high selectivity. On the SOP-forecast shape:

- 24 % selectivity → incremental → 7 s (slower than REFRESH MV's 5.4 s)
- 30 % selectivity → dispatch → reconcile → 14 s (much slower)

The fixed threshold doesn't capture the actual cost crossover.

**Proposed change**: the dispatch DO block already has affected count
(`_aff`) and intermediate count (`_imm`). Use a *cost model*:

```sql
-- Approximate costs (calibrated per-IMV at create time + amortized):
_cost_incremental := _aff * (cost_per_row_join + cost_per_row_merge);
_cost_reconcile := _imm * cost_per_row_full_refresh;
_cost_refresh_equiv := _imm * cost_per_row_refresh;  -- if Option 5 lands

IF _cost_refresh_equiv < LEAST(_cost_incremental, _cost_reconcile) THEN
    -- Take Option 5 path
ELSIF _cost_reconcile < _cost_incremental THEN
    PERFORM reflex_reconcile(...);
ELSE
    EXECUTE merge_sql; EXECUTE target_sync_sqls;
END IF;
```

Per-IMV calibration data lives in `__reflex_ivm_reference` (already has
`aggregations` JSON; add a `cost_model` sub-key). Initial values from
build-time profile of the IMV. Self-tuning is a future stretch.

For our shape: at 24 % `_cost_incremental ≈ 180 K × 39 µs = 7 s`,
`_cost_reconcile ≈ 760 K × 14 ms = 10.6 s` (no, 14 s / 760 K =
18 µs/row → 760 K × 18 µs = 13.7 s ✓), `_cost_refresh_equiv (if Opt 5)
≈ 760 K × 7 µs = 5.3 s`. Dispatcher picks Opt 5 path.

**Risk**: medium. Cost model needs calibration; over-trusting a
miscalibrated model could pick the slower path. Default to incremental on
ambiguity. Tests: simulate the three paths at several selectivities,
assert dispatcher picks correctly.

**Implementation**: ~100 LOC in `src/trigger.rs` (rewriting
`build_high_selectivity_dispatch_sql`) + ~50 LOC in `src/aggregation.rs`
or `src/create_ivm.rs` to materialize the calibration. Migration:
backfill calibration for existing IMVs (or default everyone to a
conservative model and let it self-adjust).

**Estimated effort**: 2 days code + 2 days bench/calibration + 1 day
tests.

---

### Option 5 — REFRESH-MV-equivalent fast path (HIGH win, HIGH complexity)

**The lever**: reflex_reconcile rebuilds intermediate (`9.5 s` here) +
target (`3.6 s`). REFRESH MV rebuilds target only (5.4 s). At any
selectivity where rebuild is the right call, we're paying 2× the cost.

**Proposed change**: when dispatch chooses "full rebuild", emit a
REFRESH-MV-equivalent: `BEGIN; TRUNCATE target; INSERT INTO target
<end_query — but with intermediate substituted out, referencing base_query
directly>; COMMIT`. Then re-derive intermediate from target lazily on the
next fire.

For SUM-only IMVs: intermediate fields are equal to the projection of
target × (`__nonnull_count == final non-null count`, `__sum_x == final
sum`). Recovering intermediate from target is straightforward.

For MIN/MAX/BOOL_OR IMVs: the algebraic intermediate fields (top-K heaps,
true-count, nonnull-count) can't be recovered from target alone. So this
fast path is **gated on plan shape** — SUM-only IMVs only.

For our SOP query (all SUM, no MIN/MAX): rebuild from base_query directly
into target = 5.4 s. Beat REFRESH MV by definition since the IMV-managed
target lookup index is already there.

**Risk**: high.
- Need a clean "rebuild intermediate from target" routine for the lazy
  reconstitution.
- Need to confine the fast path to SUM-only plans (the
  `AggregationPlan` already has enough metadata).
- Concurrent triggers on other source tables during rebuild: take an
  AccessExclusive on target briefly? Risk of lock waits.
- Catches the user out if they query target while it's empty mid-rebuild
  (between TRUNCATE and INSERT). UNLOGGED workaround: build the new
  state into a side table, ALTER TABLE rename. PG14+ supports atomic
  rename for unlogged tables.

**Implementation**: ~400 LOC across `src/trigger.rs` (codegen for the
fast path) + `src/aggregation.rs` (the SUM-only gate) +
`src/reconcile.rs` (lazy intermediate reconstitution). Migration: none
required (only changes runtime path selection).

**Estimated effort**: 5 days code + 3 days tests + benchmark.

---

### Option 6 — Drop `__nonnull_count_*` for NOT-NULL source columns (LOW–MEDIUM win, MEDIUM complexity)

**The lever**: every numeric aggregate in the intermediate carries two
columns: `__sum_x` and `__nonnull_count_x`. The nonnull count exists so
that "SUM of all-NULL group" can be distinguished from "SUM of zero-rows
group" (PG: SUM([NULL]) = NULL but SUM([]) and arithmetic with 0 collide
with COALESCE).

When the source column is NOT NULL (catalog says so or the data-probe says
so), every input contributes; `__nonnull_count_x` equals `__ivm_count`
and is redundant. We can drop the column entirely.

For the SOP query: 12 metrics × 2 columns = 24 aggregate columns today.
Source columns (`forecast_base`, `qty_sales`, …) are typically NOT NULL
but the schema has them as NULL-able. With the data-probe extended to
aggregate columns, we'd find them NOT NULL → drop 8-12 of the 24 columns.

For 180 K rows in scratch: ~30 % less data to aggregate and write.
Estimate 800–1 000 ms savings on the 3.1 s scratch step. Plus smaller
intermediate (lower I/O, faster MERGE).

**Risk**: medium. Catalog NOT-NULL is rare in customer schemas; data-
probed NOT-NULL was added in 1.4.5 for group columns but not for
aggregate columns. Need migration for existing IMVs to drop the columns
(intermediate ALTER TABLE DROP COLUMN). Need the build_merge_using +
output reconstruction code to conditionally emit the nonnull_count form.

**Implementation**: ~200 LOC in `src/aggregation.rs` (extend
`optimize_not_null_sums` and the codegen helpers) + migration script for
existing IMVs to drop redundant columns.

**Estimated effort**: 3 days code + 2 days migration + tests.

---

### Option 7 — Single-direction delta when one transition is empty after filter (LOW win, LOW complexity)

**The lever**: `build_net_delta_query` always emits `UNION ALL` of
delta_old (sign=-1) + delta_new (sign=+1) then a top-level GROUP BY. The
planner already short-circuits an empty side (the Bitmap Index Scan is
"never executed"), but plan startup + the outer GROUP BY still costs
~10–30 ms.

**Proposed change**: at codegen time, when the trigger body knows the
operation type, emit:

- `INSERT` → just delta_new (sign=+1, no UNION ALL).
- `DELETE` → just delta_old (sign=-1).
- `UPDATE` → keep UNION ALL (both sides may be non-empty).

For our shape the `UPDATE` case already dominates and this doesn't help.
But for source-table INSERTs and DELETEs (which the customer also does)
the saving applies.

**Risk**: low. The UNION ALL form already works for INSERT/DELETE today
(the absent side just returns 0 rows). This is a pure codegen-shape
optimization.

**Implementation**: ~50 LOC in `src/trigger.rs` (split
`build_net_delta_query` into UPDATE-only vs single-direction forms; or
just check operation == "UPDATE" before wrapping in UNION ALL).

**Estimated effort**: half a day.

---

### Option 8 — Catalog the dead-cleanup conditional (LOW win, LOW complexity)

**The lever**: dead-cleanup runs on every UPDATE/DELETE. For UPDATE
specifically, it's wasted when the operation can be proven not to net any
`__ivm_count` to ≤0.

**Proposed change**: gate `include_dead_cleanup` on either:
- `op == DELETE` (today: always cleanup),
- `op == UPDATE` AND analyzer can't prove "pure add" (Option 1's check).

If Option 1 lands, this is subsumed by the pure-add fast path. If
Option 1 doesn't land, this is a separate ~40-380 ms saving on the
specific UPDATE-with-pure-add subcase.

**Implementation**: piggyback on Option 1's runtime probe; ~30 LOC.

---

### Option 9 — Filter-aware row-level skip in IMMEDIATE mode (LOW–MEDIUM win, MEDIUM complexity)

**The lever**: the current spurious-skip is statement-level: if no row in
the transition multiset changes the IMV output, skip everything. But
within a bulk UPDATE that mutates 10 K source rows where only 100 affect
the IMV, we still scan all 10 K through the JOIN.

**Proposed change**: pre-filter the transition tables to only rows
whose `imv_relevant_columns` actually changed (multiset-row-wise compare,
not statement-wide). The JOIN scan then runs on the reduced subset.

**Risk**: medium. The row-level filter itself costs O(transition rows)
to compute. Worth it only when the trimmed set is much smaller than the
original.

**Implementation**: ~100 LOC + analyzer extension.

**Estimated effort**: 2 days. Has the worst payoff-to-complexity ratio
on this menu; defer until we have a workload that *needs* it.

---

## Recommended ordering

If a single development cycle is the constraint, ship in this order:

1. **Option 2** (DISTINCT removal) — half hour, free win.
2. **Option 1** (effective-INSERT shortcut) — biggest single win on the
   filter-flip path that the customer hits. Brings 24 % case from 7 s to
   ~4.6 s — pg_reflex back ahead of REFRESH MV.
3. **Option 7** (single-direction delta for INSERT/DELETE ops) — half
   day, helps source-table-INSERT/DELETE workloads.
4. **Option 4** (calibrated dispatch) **OR** **Option 5** (REFRESH-MV-
   equivalent fast path). Option 5 is the more aggressive — it makes
   pg_reflex strictly ≥ REFRESH MV at any selectivity for SUM-only IMVs.
   Option 4 is the safer half: pick the better of the three paths at
   each fire, but without the new REFRESH-MV-equivalent path it's only
   choosing the lesser of two evils. Pair them: Option 5 first, then
   Option 4 to expose it via the dispatch.
5. **Option 3** (INSERT ON CONFLICT for target sync) — replaces the
   DELETE+INSERT pair across all paths. ~600 ms savings on 180 K case.
6. **Option 6** (drop redundant nonnull_count) — large refactor, mostly
   pays off on UPDATE-heavy workloads where the scratch step dominates.
7. **Option 9** (row-level filter-skip) — defer until a workload signal.

## Acceptance criteria

For each shipped option:

- Existing 528-test suite green.
- New pgrx correctness tests covering the targeted shape (EXCEPT-ALL = 0
  against REFRESH MATERIALIZED VIEW).
- Benchmark on `bench_sop` synthetic at 1 M source, 760 K intermediate,
  exercising:
  - status pivot in-filter (must stay ≤2 ms)
  - filter-flip 20 K / 80 K / 180 K rows (must improve)
  - data UPDATE 100 / 1 K / 10 K rows (must not regress).

## Non-goals

- Multi-IMV cascade shared-scratch (deferred — needs cross-IMV
  analyzer hooks).
- Eliminating the intermediate for SUM-only IMVs (high redesign cost; not
  worth without a clear motivating workload).
- Parallel maintenance worker tuning (not characterized; defer until
  we've measured `parallel_setup_cost=0` impact).
- JIT cost characterization (defer; first-fire cost may be a red
  herring if plan cache keeps it low across normal session activity).
