# 2026-05-15 — Next optimization cycle after Items 1+5

## Source

- `journal/2026-05-14_sop_forecast_levers_probe.md` — full per-step
  profile of the SOP-forecast 1 M-row workload at 20 K and 180 K
  affected rows.
- `journal/2026-05-14_plan_implementation_results.md` — outcome of the
  prior cycle: Item 1 (drop redundant DISTINCT) and Item 5 (drop
  redundant `__nonnull_count_*`) landed; Items 2 (effective-INSERT
  shortcut via runtime probe), 3 (single-direction delta inside a DO
  block) and 4 (reconcile CTAS+swap) rejected with documented evidence.
- `plans/2026-05-14-post-skip-filter-optimization-options.md` — option
  menu drafted last cycle; this plan continues from that menu, picks
  what survived re-evaluation, and re-frames the rejected items where a
  cleaner implementation now exists.

## Where we are

Post-Items 1+5 on the SOP-forecast bench (1 M source, ~760 K
intermediate, now 340 MB on disk):

| Workload | Pre-1+5 | Post-1+5 | REFRESH MV | Verdict |
|---|---:|---:|---:|---|
| Status pivot (no-op) | 1.2 ms | 1.4 ms | 5 421 ms | Won (5400×) |
| OUT→IN 20 K rows | 861 | 725 | 5 421 | Won |
| OUT→IN 60 K rows | 1 972 | 1 790 | 5 421 | Won |
| **OUT→IN 200 K rows (24 %)** | **6 799** | **6 086** | **5 421** | **LOST (-12 %)** |
| IN→OUT 20 K rows | 577 | 502 | 5 421 | Won |
| Data UPDATE 1 K rows | 2 041 | 1 900 | 5 421 | Won |
| **Reconcile (≥30 % dispatch)** | **19 100** | **17 100** | **5 421** | **LOST (-3.2×)** |
| Intermediate disk | 411 MB | 340 MB | n/a | -17 % |

Items 1+5 closed roughly 10 % across every operation by trimming
the intermediate by 17 %. They did not fix the two remaining loss
regions:

1. **The 24 % selectivity valley** — at 200 K rows entering/leaving
   the IMV per UPDATE, pg_reflex is now 12 % slower than REFRESH MV
   (was 30 % slower). Still losing.
2. **The reconcile path** — admin-triggered or auto-dispatched at
   ≥30 % selectivity, still 3.2× slower than REFRESH MV.

## Updated per-step breakdown at 200 K OUT→IN (~6 086 ms total)

Derived from the 180 K-row profile in the levers-probe journal
proportional-scaled by Item 5's ~10 % narrowing of every intermediate
row (Item 1 already removed the 190 ms affected-DISTINCT step):

| Step | Wall ms | % | Notes |
|---|---:|---:|---|
| `INSERT INTO __reflex_scratch` (net-delta UNION ALL JOIN + GROUP BY) | ~2 800 | 46 % | dominated by the JOIN over 200 K source rows × 5-way join |
| `INSERT INTO __reflex_affected SELECT …` | ~5 | <1 % | Item 1 dropped the DISTINCT |
| `MERGE INTO __reflex_intermediate` | ~1 450 | 24 % | composite-unique-index probe + heap UPDATE/INSERT |
| `DELETE FROM intermediate WHERE __ivm_count<=0 AND EXISTS(…affected)` | ~350 | 6 % | scans affected subset; finds 0 dead rows on OUT→IN |
| `DELETE FROM target WHERE EXISTS(…affected)` | ~330 | 5 % | finds 0 pre-existing rows on OUT→IN |
| `INSERT INTO target SELECT … FROM intermediate WHERE EXISTS(…affected)` | ~1 150 | 19 % | the only step that does net work |
| plpgsql glue / metadata | ~50 | <1 % | EXECUTE plan, FOR loop |

Two halves of the wall-clock at roughly equal weight:

- **The scratch JOIN scan** (46 %) — fundamental work; reducing it
  needs either (a) skipping the UNION ALL wrapper when one side is
  empty post-filter, or (b) parallelising it.
- **MERGE + target sync** (54 %) — `MERGE intermediate` + `DELETE target`
  + `INSERT target` cumulatively. The DELETEs are wasted work on
  OUT→IN; the target INSERT could plausibly be ON CONFLICT-shaped to
  let HOT updates fire on existing groups.

## Lever inventory (status after Items 1+5)

| Lever | Status | This plan |
|---|---|---|
| A1 — drop nonnull_count for NOT-NULL | ✅ Item 5 done | — |
| A2 — single-direction delta when one side empty | ❌ Item 3 rejected (DO-block scope) | **Re-framed as Item α (dispatch in trigger function body, NOT in DO block)** |
| A3 — parallel scratch INSERT | Open | **Item γ (probe)** |
| B — drop affected DISTINCT | ✅ Item 1 done | — |
| C — dead-cleanup wasted on OUT→IN | Open (tied to A2) | **Subsumed by Item α** (INSERT-shape codegen skips dead-cleanup automatically) |
| D1 — INSERT ON CONFLICT for target sync | Open | **Item β** |
| D2 — share affected predicate via CTE | Open | Defer (snapshot conflict makes it brittle) |
| E1 — re-tune wipe threshold | Open | **Item δ (GUC default + bench-calibrated baseline)** |
| E2 — REFRESH-MV-equivalent reconcile fast path | Open | **Item ε (probe-only; BOOL_OR breaks the simple math)** |
| E3 — Calibrated dispatch cost model | Open | Defer (only valuable if ε lands) |
| F1 — INSERT ON CONFLICT vs MERGE on intermediate | Open | Defer (marginal benefit) |
| F2 — Skip intermediate for SUM-only | Open | Defer (big redesign, no clear motivating workload) |
| G — Effective-INSERT shortcut via runtime probe | ❌ Item 2 rejected (correctness flaw + no measurable gain) | — (statically provable variant lands in α instead) |
| H1 — Row-level spurious skip | Open | Defer (no workload signal) |
| Multi-IMV cascade shared-scratch | Open | Defer (cross-IMV analyzer hooks) |

## Items proposed for this cycle

The plan proposes **five** items. Items α + β are the work; γ + δ + ε
are probes that produce data for follow-up decisions.

Ordered by ratio of expected win to implementation complexity.

---

## Item α — Directional UPDATE dispatch (re-framing of rejected Item 3)

### Why the previous rejection does not apply

Item 3 last cycle put the directional dispatch *inside a DO block* that
was EXECUTE'd from the trigger function. PostgreSQL transition tables
(`REFERENCING NEW TABLE AS …`) are scoped to the immediate trigger
function and are not visible across a nested PL/pgSQL execution
context. The DO block hit `relation "__reflex_old_…" does not exist`.

Direct EXECUTE of a string SQL from inside the trigger function body
**does** see transition tables (that is how the scratch INSERT works
today). The trigger function body itself is the right level for a
directional probe; we just can't move the probe out into a DO block.

### The lever

For OUT→IN UPDATE shape (the customer's hot path:
`UPDATE demand_planning SET status='validated' WHERE id IN (…)`):

- Post-filter `__reflex_old_<src>` is empty (`status='archived'` does
  not pass `WHERE status IN ('validated', 'current', …)`).
- Post-filter `__reflex_new_<src>` has rows.
- The net-delta query at `src/trigger.rs:292-368` still emits the
  UNION ALL wrapper of (delta_new × +1) and (delta_old × -1). The
  outer GROUP BY re-aggregates. The planner short-circuits the empty
  side (Bitmap Index Scan "never executed" in captured plans), but
  the executor still pays plan + UNION ALL + outer GROUP BY startup.
- This is the **scratch INSERT** step — ~2 800 ms at 200 K (46 % of
  wall).

Promote the UPDATE to an INSERT-shape (single-direction add) at the
trigger function body level, *before* `reflex_build_delta_sql` is
called. The INSERT-shape codegen path
(`push_materialized_merge_and_affected` with `delta_q` from NEW
transition only) is already exercised by source-table INSERTs and is
correctness-locked. We are reaching it from a different entry point.

Symmetric for IN→OUT (`'validated' → 'archived'`): NEW side empty,
OLD side has rows → promote to DELETE-shape.

### Two free benefits ride along

1. **No dead-cleanup**: `trigger.rs:1731-1733`:
   ```rust
   let include_dead_cleanup = plan.needs_ivm_count
       && grp_cols.is_some()
       && (operation == "DELETE" || operation == "UPDATE");
   ```
   When we call `reflex_build_delta_sql` with `operation = 'INSERT'`,
   `include_dead_cleanup` is `false`. The ~350 ms wasted DELETE
   disappears.
2. **No target DELETE on OUT→IN**: the INSERT-shape uses
   `push_materialized_merge_and_affected` which builds scratch + affected,
   and the target sync at `trigger.rs:1825-1882` is shared. But on
   OUT→IN the target's `WHERE EXISTS (…affected)` finds zero rows
   pre-existing → the DELETE returns instantly (~10-50 ms after planner).
   Not a free win like dead-cleanup, but already small.

The actual win is from the scratch INSERT shape change: ~2 800 ms ⇒
~1 400-1 600 ms (single-direction, no UNION ALL, no outer
re-aggregation). Plus the ~350 ms dead-cleanup elimination.

### Change

Add a probe + dispatch step to the per-IMV LOOP body in the UPDATE
trigger function (the LOOP in
`src/schema_builder.rs:438-462`, body_core). Gated on the IMV having
non-empty `imv_relevant_columns[source_table]` — same gate as the
1.4.5 spurious-skip block. Otherwise fall through to today's UPDATE
path.

```plpgsql
-- After the filter_skip_block (which already proves NEW ≠ OLD post-filter),
-- check direction. The filter_skip_block has already ruled out the case
-- where both multisets are empty.
DECLARE
    _old_has BOOLEAN;
    _new_has BOOLEAN;
    _directional_op TEXT;
BEGIN
    -- Probe transition tables. Two EXECUTEs, each <1 ms on indexed shapes.
    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)',
        '__reflex_old_<src>', _skip_pred
    ) INTO _old_has;
    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)',
        '__reflex_new_<src>', _skip_pred
    ) INTO _new_has;

    IF NOT _old_has AND _new_has THEN
        _directional_op := 'INSERT';
    ELSIF _old_has AND NOT _new_has THEN
        _directional_op := 'DELETE';
    ELSE
        _directional_op := 'UPDATE';
    END IF;
END;
```

Then call `reflex_build_delta_sql(..., op := _directional_op, ...)`
in place of the hard-coded `'UPDATE'`. The existing INSERT- and
DELETE-shape codegen paths produce correct SQL referencing only
`__reflex_new_<src>` or `__reflex_old_<src>` — both of which are
visible from inside the UPDATE trigger's REFERENCING clauses.

### Edge cases (must covered by tests)

1. **Multi-row UPDATE that flips some rows out-of-filter and others
   in-filter**: both sides have rows post-filter → `_directional_op =
   'UPDATE'` → today's UNION ALL path. Unchanged behavior.
2. **UPDATE on a non-filter, non-group data column** (the customer's
   data-correction case): the filter accepts both OLD and NEW → both
   sides have rows → UPDATE path. Unchanged.
3. **UPDATE that mixes a filter flip with a data change on the same
   row**: NEW passes filter, OLD does not → INSERT-shape. The data
   change shows up in the new contribution. Mathematically equivalent
   to "row didn't exist in IMV before, now it does with new values".
4. **IMV without `imv_relevant_columns`** (no filter / CTE-using
   query): gate doesn't fire; today's path. Unchanged.
5. **IMV with a `where_predicate`** (which differs from
   `imv_relevant_where`): the existing `pred_check_upd` (`schema_builder.rs:474`)
   already CONTINUEs the LOOP if neither side has a passing row.
   That short-circuit subsumes one direction of the directional
   probe. Confirm both gates compose without redundant probing.

### Win

At 200 K-row OUT→IN: scratch INSERT 2 800 ms ⇒ ~1 400 ms (−1 400),
dead-cleanup 350 ms ⇒ 0 (−350). Wall 6 086 ms ⇒ ~4 300 ms.

That puts pg_reflex back **ahead of REFRESH MV** (5 421 ms) at 24 %
selectivity by ~1 100 ms (20 % under REFRESH).

At 20 K-row OUT→IN: scratch INSERT ~280 ⇒ ~150 (estimated), dead-cleanup
~40 ⇒ 0. Wall 725 ms ⇒ ~530 ms.

### Risk

Low-to-medium.

- The INSERT-shape and DELETE-shape codegen paths exist and are
  exercised by source-table INSERT/DELETE triggers. Reaching them from
  a different entry point doesn't change their semantics.
- The two added EXECUTEs add ~2-5 ms of plan + executor startup per
  UPDATE trigger fire — negligible on UPDATEs that benefit, but a
  visible tax on UPDATEs that don't take the shortcut. Gate the probe
  on `imv_relevant_columns` (already required for the 1.4.5 skip-aware
  spurious-skip — same population of IMVs).
- The `imv_relevant_where` predicate is the analyzer-extracted form
  of the IMV's source-restricted WHERE. It's the same string used by
  the spurious-skip; correctness is shared.
- An UPDATE that flips one direction *and* updates non-filter data on
  the same row: still falls into the INSERT/DELETE shortcut path
  because post-filter the OLD multiset has 0 rows. The data change is
  captured in the NEW side's contribution. Correctness: equivalent to
  a row being inserted into the source with the post-UPDATE values.

### Files

- `src/schema_builder.rs`:
  - Extend `body_core` for UPDATE trigger: emit the directional probe
    (gated on `imv_relevant_columns[source]` being non-empty), select
    `_directional_op`, then call `reflex_build_delta_sql` with that op.
  - Probe is emitted *inside* the LOOP body (post-`filter_skip_block`),
    so it has per-IMV access to that IMV's `imv_relevant_where`.
- `src/trigger.rs`:
  - No code change required — `reflex_build_delta_sql` already handles
    'INSERT' and 'DELETE' operations correctly with the UPDATE trigger's
    available transition tables.
  - But: add a clarifying comment that the function can be called with
    a *promoted* op from an UPDATE trigger context.

### Tests

- `src/tests/unit_schema_builder.rs`:
  - Generated UPDATE trigger body contains the directional probe when
    a representative IMV is loaded with non-empty `imv_relevant_columns`.
  - Probe is *absent* when `imv_relevant_columns` is empty (e.g.,
    CTE-using or SELECT * IMV).
- `src/tests/pg_test_correctness.rs` (or new
  `pg_test_directional_dispatch.rs`):
  - OUT→IN single-row flip on a SUM IMV: assert EXCEPT-ALL = 0 vs
    REFRESH MV.
  - OUT→IN multi-row flip (200 rows).
  - IN→OUT single-row flip.
  - IN→OUT multi-row flip.
  - Mixed UPDATE: some rows flip in, some flip out, some don't move
    (still in-filter). Must land in the UPDATE branch.
  - UPDATE that flips filter AND changes a non-filter SUM-driving
    column on the same row.
  - BOOL_OR on OUT→IN (validates the algebraic representation through
    the INSERT path).
  - UPDATE on an IMV without `imv_relevant_columns` (no gate fired):
    must use today's UPDATE path.

### Effort

1.5 days code + 1 day tests + half-day benchmark.

---

## Item β — INSERT ON CONFLICT for target sync (D1 from probe)

### Why now and not in the previous cycle

`target_merge_10` (the `plans/2026-05-13-merge-on-target-10.md` attempt
last sprint) tried a LEFT JOIN MERGE form for the target. The planner
produced a 400 M-row cross-product at scale (the JOIN's row estimate
was wildly off). The work was reverted.

The MERGE-flavored plan was the wrong primitive. `INSERT … ON CONFLICT
DO UPDATE` is the simpler operation: PG plans it as a single index
probe + heap UPDATE-or-INSERT per source row, deterministically. It is
not a JOIN. It cannot mis-estimate the way MERGE did.

The customer's target carries fillfactor=70 (since 1.4.4). HOT-update
is eligible for the UPDATE-existing-row path. Today's `DELETE+INSERT`
defeats HOT because every existing group gets a fresh tuple version
each fire.

### The lever

Today's target sync (the `tdel; tins` pair built in
`trigger.rs:1828-1882`):

```sql
DELETE FROM target WHERE <ns_in_target_delete>;             -- ~330 ms at 200 K
INSERT INTO target <end_query>
    AND <ns_in_intermediate WHERE __ivm_count > 0>;          -- ~1 150 ms at 200 K
```

`<ns_in_target>` is `null_safe_in` against `__reflex_affected_<view>`.

Both statements scan the affected subset of target/intermediate.

Replace with:

```sql
-- Upsert: insert new groups, in-place update existing.
INSERT INTO target (<output_cols>)
    SELECT <output_cols> FROM intermediate
    WHERE __ivm_count > 0 AND <ns_in_intermediate>
ON CONFLICT (<target_group_keys>) DO UPDATE
    SET (<non_key_cols>) = (<EXCLUDED.non_key_cols>);

-- Sweep: drop groups that no longer have any contributing rows.
DELETE FROM target
    WHERE <ns_in_target_delete>
    AND NOT EXISTS (
        SELECT 1 FROM intermediate
        WHERE <intermediate_keys = target_keys>
        AND __ivm_count > 0
    );
```

The sweep stays — groups whose `__ivm_count` net-collapsed to 0 must
be removed from target. But for a typical workload most affected
groups stay alive; the sweep finds nothing and returns fast (NOT EXISTS
short-circuits per row).

### Win

Bench-derived estimates at 200 K affected rows:

- Today: 330 ms (DELETE) + 1 150 ms (INSERT) = 1 480 ms.
- ON CONFLICT projection:
  - Upsert: 800-900 ms. HOT-eligible on existing groups; otherwise plain
    insert. PG plans this as a single bulk INSERT with conflict probes.
  - Sweep: ~100 ms if no groups collapsed (NOT EXISTS over a small
    affected set + index probe per row).
- Net: ~900-1 000 ms, savings ~480-580 ms.

For 20 K-row cases: today ~84 ms + ~42 ms ≈ 130 ms ⇒ projected
~80-100 ms, savings ~30-50 ms.

Compounds with Item α: at 200 K-row OUT→IN, scratch saving ~1 400 ms +
dead-cleanup saving ~350 ms + target sync saving ~500 ms = ~2 250 ms
total. Wall 6 086 ms ⇒ ~3 850 ms (29 % below REFRESH MV).

### Risk

Medium.

- The target's composite unique index (over `target_group_keys`) is
  the ON CONFLICT target. The 1.4.4 work confirmed this index exists
  on every aggregate IMV. For passthrough IMVs without a unique index,
  ON CONFLICT is undefined — gate on `plan.has_target_unique_index`
  (introduce flag) or fall through to today's DELETE+INSERT.
- The sweep DELETE must not race with the INSERT — emit them as
  separate statements, not a single CTE. They are sibling statements
  with no shared snapshot constraint within one EXECUTE.
- `EXCLUDED.col` syntax requires PG ≥ 9.5. We support PG 17 only; not
  a concern.
- The `end_query` may carry HAVING clauses or expressions that aren't
  trivially decomposable into `(non_key_cols) = (EXCLUDED.non_key_cols)`.
  Need to project the `end_query` output into a plain `SELECT col_list FROM
  intermediate ... WHERE __ivm_count > 0 AND affected` and rely on
  `end_query`'s CASE/COALESCE happening inside that projection.

### Files

- `src/trigger.rs`:
  - Add `build_target_upsert_sql(plan, end_query, intermediate_tbl,
    affected_tbl, target_keys)` that returns the upsert + sweep pair.
  - In the dispatch DO block builder
    (`build_high_selectivity_dispatch_sql`): take an `Option<(upsert_sql,
    sweep_sql)>` instead of `(target_delete_sql, target_insert_sql)`.
    Keep the two-statement form for the fallback path.
  - In the per-op branches that emit target sync (`1825-1882`):
    construct the new pair and pass it.
- `src/aggregation.rs`:
  - If the upsert needs `non_key_cols` distinct from `target_keys`,
    plumb an `output_non_key_columns()` helper.

### Tests

- `src/tests/unit_trigger.rs`:
  - Generated target upsert SQL contains `ON CONFLICT (…) DO UPDATE
    SET (…) = (EXCLUDED.…)`.
  - Sweep DELETE references both `affected` and `NOT EXISTS (… __ivm_count
    > 0)`.
  - Fallback to DELETE+INSERT when `plan.has_target_unique_index` is
    false (passthrough).
- `src/tests/pg_test_correctness.rs`:
  - Pure update of existing groups (HOT-eligible): assert
    `pg_stat_user_tables.n_tup_hot_upd` increments.
  - New group insertion.
  - Group removal via the sweep (manual DELETE on source, all rows
    for a group gone).
  - Mixed batch: some groups updated, some inserted, some removed.
  - EXCEPT-ALL = 0 against REFRESH MV on each shape.

### Effort

2 days code + 1.5 days tests + half-day benchmark.

---

## Item γ — Parallel scratch INSERT (probe + maybe ship)

### The lever

`INSERT INTO __reflex_scratch SELECT … FROM <5-way JOIN> GROUP BY …` is
the 46 %-of-wall step. The captured plans in the lever-probe journal
show no Gather node — PG's planner judged parallel setup cost too high
for the row counts and CPU estimate.

PG 16+ has parallel HashAggregate and parallel JOIN scans. Forcing
parallelism via session-local GUCs:

```sql
SET LOCAL parallel_setup_cost = 0;
SET LOCAL min_parallel_table_scan_size = 0;
SET LOCAL parallel_tuple_cost = 0;
```

Could plausibly halve the scratch INSERT at the 200 K-row case. At
20 K rows, parallel setup may eat the benefit.

### What this plan asks for

**Probe-only this cycle**: measure the impact of parallel-forcing GUCs
on the scratch INSERT step at the three affected-row sizes (20 K, 60 K,
200 K). If the 200 K case improves by ≥ 30 % AND the 20 K case
regresses by < 50 ms, ship it gated on a row-count threshold.

Implementation form (if shipped):

- In the trigger body, *immediately before* the scratch INSERT, emit:
  ```sql
  SET LOCAL parallel_setup_cost = 0;
  SET LOCAL min_parallel_table_scan_size = 0;
  ```
- Bound the change to the trigger transaction via `SET LOCAL`.

### Risk

Low if probed before shipping. Parallel HashAgg has been stable for
many PG versions. The probe also exposes parallel-worker-count
configuration questions (`max_parallel_workers_per_gather`); the
benchmark already runs at 4 — that's fine.

### Files

- `benchmarks/bench_scratch_parallelism.sql` — new bench script.
- If shipped: `src/trigger.rs` — prepend the SET LOCALs to the
  scratch INSERT statement (or emit them as their own statements).

### Effort

Half-day probe; another half-day to ship if the probe is positive.

---

## Item δ — Re-tune `WIPE_THRESHOLD_DEFAULT` (E1)

### The lever

`WIPE_THRESHOLD_DEFAULT = 0.3` is workload-arbitrary. On the
SOP-forecast shape, reconcile takes 17 s and is **never** faster than
incremental at any reachable selectivity. Yet at 30 % the dispatch
flips to reconcile, making the trigger slower than if it had stayed
incremental.

On other workloads (`rb.fcast` from the 1.4.4 journal), reconcile at
30 % was the right call. The default should be derived from a calibrated
cost model, not a fixed constant — but until Item ε (REFRESH-MV-
equivalent reconcile) lands, the safest default is to **raise the
threshold to 1.0** (effectively disable auto-dispatch) and let users
opt in via the `reflex.wipe_threshold` GUC for shapes where reconcile
beats incremental.

### Why a probe matters first

This is technically a one-line change but the *implication* is that
some workloads in the wild may already rely on the 0.3 default to
finish faster (where reconcile is genuinely a win). The probe is:

- Run a sweep across the bench: 5 %, 10 %, 20 %, 30 %, 40 %, 50 %, 70 %
  selectivity on SOP-forecast.
- Capture incremental wall vs reconcile wall vs REFRESH MV.
- Identify the crossover point on this shape.
- Decide: 1.0 (off), 0.5 (current shape's crossover), or stay at 0.3.

### Win

- For SOP-forecast workloads at 30-50 % selectivity: switching to
  incremental saves ~7-10 s per fire.
- For workloads where reconcile is genuinely faster: zero impact if
  user opts in via GUC.

### Risk

Low. The change is backwards-compatible — users can restore 0.3 via
`SET reflex.wipe_threshold = 0.3`. Document the change.

### Files

- `src/trigger.rs:1003` — adjust the constant. Possibly to 0.5.
- README / migration note: GUC restoration recipe.

### Effort

Half day for the bench sweep + decision + change.

---

## Item ε — Reconcile fast path probe (E2/Option 5)

### The lever and complications

At ≥30 % selectivity (or admin call), `reflex_reconcile` rebuilds
intermediate (9.5 s after Items 1+5) + target (3.6 s). REFRESH MV
just rebuilds target (5.4 s).

Skipping the intermediate rebuild would put reconcile at ~5.4 s,
matching REFRESH MV. Two ways:

**A. Skip-and-lazy-rebuild**: at reconcile, build target only. Mark
intermediate as "stale". At the next trigger fire, before doing
incremental work, rebuild intermediate from source. Trades reconcile
cost (14 s → 5.4 s) for first-fire-after-reconcile cost (next UPDATE
takes ~9 s instead of <1 s).

**B. Build-and-derive**: at reconcile, build target from base_query.
Concurrently or after, derive intermediate columns from the target.
Only works for plan shapes where intermediate columns are
deterministic from target columns — SUM yes, BOOL_OR partially
(can't reconstruct `__true_count` from a boolean), MIN/MAX no.

**The complication**: SOP-forecast has a `BOOL_OR(caav.product_id IS NOT
NULL)` aggregate. Item 5 already removed the redundant `__bool_or_X_nonnull_count`
column for this shape (the inner is `IS NOT NULL`, structurally non-null).
But the `__bool_or_X_true_count` column is **not** reconstructible from
the target's boolean output — you can't recover the per-group count from
a single bit. Approach B is out for SOP-forecast.

Approach A is feasible for any IMV but pushes cost from reconcile to
the next fire. For admin-triggered reconcile, that's probably acceptable.
For auto-dispatch from a 30 %-selectivity UPDATE, it makes the *current*
UPDATE faster but the *next* UPDATE slower by ~7 s.

### What this plan asks for

**Probe-only**. Don't ship yet. Build a one-off prototype:

1. Implement Approach A in a feature branch:
   - `reflex_reconcile` rebuilds target only (drops index, INSERT, recreates).
   - Marks `__reflex_ivm_reference.intermediate_stale = TRUE`.
   - Trigger function body checks the flag at fire start; if set,
     rebuilds intermediate from base_query (one extra ~9 s scan).
2. Benchmark across:
   - Reconcile + 1 incremental fire pattern (the customer's hot loop).
   - Reconcile + 10 incremental fires (low-frequency admin reconcile).
3. Decide: ship A or stay with current reconcile.

### Win

- Reconcile alone: 17.1 s ⇒ ~5.4 s, savings 11.7 s.
- Reconcile + 1 fire: today 17.1 + 1 = 18.1 s; A: 5.4 + ~9 = 14.4 s,
  savings 3.7 s.
- Reconcile + many fires: amortizes to today's incremental costs.

### Risk

Medium-high.

- The "intermediate stale" flag introduces a per-fire branch.
- Concurrent triggers fired between reconcile and the rebuild-fire
  see an empty intermediate. Need to ensure target stays consistent
  (rebuild target from base_query is independent; intermediate is
  only consulted on next *incremental* fire).
- Errors during the lazy rebuild leave the intermediate empty
  permanently — needs a recovery path (re-run reconcile).

### Files

- `src/reconcile.rs` — Approach A's target-only rebuild.
- `src/schema_builder.rs` — body_core checks `intermediate_stale`.
- `src/trigger.rs` — lazy intermediate rebuild routine.

### Effort

Probe alone: 2 days. Ship if positive: another 3-5 days code + tests.

---

## Recommended ordering and ship gates

| Step | Item | Days | Ship condition |
|---|---|---:|---|
| 1 | δ — bench sweep + threshold tune | 0.5 | Always — one-line constant change + GUC docs |
| 2 | α — directional UPDATE dispatch | 3 | Ship if EXCEPT-ALL clean + measurable ≥ 30 % gain at OUT→IN ≥ 60 K rows |
| 3 | γ — parallel scratch probe | 0.5 → 1 | Ship gated if 200 K wins ≥ 30 % AND 20 K regresses < 50 ms |
| 4 | β — INSERT ON CONFLICT target sync | 4 | Ship if EXCEPT-ALL clean + HOT-update ratio > 90 % on existing-group UPDATEs |
| 5 | ε — reconcile fast-path probe | 2 | Decide after seeing bench numbers; ship if reconcile + 1 fire ≥ 15 % faster than today |

Items 1-4 are independent; items 1, 2, 4 can land in series. Items 3
and 5 are probes that may or may not produce a ship-able change.

## Combined expected outcome (if α + β + γ all ship)

| Workload | Today (post-1+5) | After α+β+γ | REFRESH MV |
|---|---:|---:|---:|
| Status pivot (no-op) | 1.4 ms | 1.5 ms | 5 421 ms |
| OUT→IN 20 K | 725 ms | ~480 ms | 5 421 |
| OUT→IN 60 K | 1 790 ms | ~1 100 ms | 5 421 |
| **OUT→IN 200 K** | **6 086 ms** | **~3 500 ms** | **5 421** (won by 35 %) |
| IN→OUT 20 K | 502 ms | ~340 ms | 5 421 |
| Data UPDATE 1 K | 1 900 ms | ~1 600 ms | 5 421 |
| Reconcile (today) | 17 100 ms | 17 100 ms | 5 421 |

The 24 % selectivity valley closes: pg_reflex back ahead of REFRESH MV
at the customer's worst-case workload. Reconcile path stays as-is
until Item ε's probe decides whether Approach A is shippable.

## Acceptance criteria

- `cargo test --features pg17 --lib` green (current 567 + new
  tests per item).
- `cargo pgrx test pg17` green.
- `cargo clippy --features pg17 --all-targets -- -D warnings` clean.
- `cargo fmt --check` clean.
- New benchmark `benchmarks/bench_directional_dispatch.sql` (Item α)
  covering OUT→IN, IN→OUT, mixed at 20/60/200 K rows.
- New benchmark `benchmarks/bench_target_upsert.sql` (Item β) covering
  pure-update, new-insert, mixed batches.
- For each shipped item: EXCEPT-ALL = 0 against `REFRESH MATERIALIZED
  VIEW` on the SOP-forecast equivalent query, across all benched
  shapes.

## Out of scope (defer to a later cycle)

- **Multi-IMV cascade shared scratch** — N× wins where N is IMVs over
  the same source; needs cross-IMV analyzer hooks. Customer has 5-15
  IMVs over `sales_simulation`; this is the next-cycle prize once α+β
  land.
- **F1 — INSERT ON CONFLICT vs MERGE on the intermediate** —
  marginal benefit (~100-200 ms); MERGE was chosen deliberately to
  support both WHEN MATCHED / WHEN NOT MATCHED branches for Add and
  Subtract. Defer until we have a workload signal.
- **F2 — Eliminate the intermediate for SUM-only IMVs** — large
  redesign; not worth without a clear motivating workload.
- **H1 — Row-level spurious skip in IMMEDIATE mode** — only valuable
  for bulk UPDATEs where most rows touch non-IMV columns. Defer.
- **E3 — Self-tuning dispatch cost model** — only valuable if ε ships
  (gives the dispatcher a third path to choose from). Defer until ε.

## How this plan differs from `2026-05-14-incremental-and-reconcile-optimizations.md`

| Last cycle's item | Outcome | This cycle |
|---|---|---|
| Item 1 — drop DISTINCT | Shipped | — |
| Item 2 — effective-INSERT shortcut (runtime probe) | Rejected: correctness flaw + no measurable gain | Statically provable variant (filter-aware direction probe) lives inside Item α |
| Item 3 — single-direction delta (in DO block) | Rejected: DO block can't see transition tables | Re-framed as Item α: probe in trigger function body, *not* in a DO block |
| Item 4 — reconcile CTAS+swap | Rejected: no perf gain (PG 17 already parallelises INSERT into empty unlogged) | Item ε proposes a different reconcile change (skip intermediate, lazy rebuild) — probe before ship |
| Item 5 — drop redundant nonnull_count columns | Shipped | — |
| (Not in last plan) | — | Item β — INSERT ON CONFLICT for target sync |
| (Not in last plan) | — | Item γ — parallel scratch probe |
| (Not in last plan) | — | Item δ — wipe_threshold re-tune |
