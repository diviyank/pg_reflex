# Plan #9 — Filter-aware smart spurious-skip (1.4.6)

## TL;DR

Detect UPDATEs whose `where_predicate` evaluates identically on `__reflex_old`/`__reflex_new` **and** whose changed columns are referenced only inside the IMV's WHERE clause (not in SELECT / JOIN / GROUP BY). Skip the entire trigger body — no scratch INSERT, no MERGE, no target rewrite. Expected impact on customer's dominant workload: **14.8 s → ~50 ms** per status-flip UPDATE.

## Customer context

`yse.ivm_sop_forecast_view` (production-shape IMV, ~76 M source / 867 K intermediate). The IMV filters demand_planning rows by:

```sql
WHERE dp.status IN ('creating_supply_plan', 'running_optimizer',
                    'refreshing_views_sp', 'sent_to_sop',
                    'validated', 'current')
```

The customer's SOP workflow flips `status` between these whitelist members on a single `demand_planning` row at a time:

```sql
UPDATE yse.demand_planning SET status='validated' WHERE id=172;
```

Both the old status (e.g., `'sent_to_sop'`) and the new (`'validated'`) pass the whitelist. `status` does **not** appear in any GROUP BY, JOIN condition, or aggregate argument of the IMV's base_query. Therefore the IMV's output for the affected groups is **byte-identical** pre/post.

Empirically proved by the customer in their bench:

```sql
CREATE TEMP TABLE pre AS SELECT * FROM yse.ivm_sop_forecast_view WHERE dem_plan_id=172;
UPDATE yse.demand_planning SET status='running_optimizer' WHERE id=172;
UPDATE yse.demand_planning SET status='validated' WHERE id=172;

SELECT count(*) FROM (TABLE pre EXCEPT TABLE yse.ivm_sop_forecast_view) d;
-- 0 (every dp=172 row in `pre` still in the IMV)
SELECT count(*) FROM (TABLE yse.ivm_sop_forecast_view EXCEPT TABLE pre) d;
-- 311494 (other dem_plans — unchanged, just not in pre's snapshot)
```

Yet each UPDATE took **14.8 s** post-data-probe (and 15.3 s on the second one). The trigger body produced zero net IMV change but executed:

- `INSERT scratch` (JOIN scan) — 4.88 s
- `MERGE intermediate` — 6.00 s (555 K probes)
- `INSERT affected` — 0.42 s
- `DELETE intermediate __ivm_count<=0` — 0.26 s
- `DELETE FROM target` — 0.90 s (full target scan + hash join)
- `INSERT INTO target` — 3.19 s (full intermediate scan + hash join)
- Total: ~15.7 s of pure waste.

The customer pushed back on this perf class twice. From their messages:

> Ok so there a huge issue here. The data probe is quite important. without that fix it takes more than 6min. We are still not set on the <1s updates that should be standard. This "smart" simplification is not the main point, it *is* an optimization, but it's sidestepping the real performance issue here.

And:

> 2M is reasonable workload for the production. Compare to REFRESH MATERIALIZED VIEW as well.

After the bench, my counter-argument: at high selectivity REFRESH MV is 3.3 s and the structural path (dispatch-via-reconcile, #7) hits 24 s = 6× REFRESH — still above the 2× bar. **The only way to hit <2× REFRESH on the customer's workload is to skip the work entirely when it's a no-op.** That's #9.

## Why 1.4.3's existing spurious-skip doesn't catch this

`trigger.rs:2004-2047` implements a spurious-skip for DEFERRED-mode triggers:

```rust
// Multiset compare on ALL source columns:
let cols_csv = src_cols.join(", ");
let is_spurious = ...
    "WITH ... only_old AS (
       SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_OLD'
       EXCEPT ALL
       SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_NEW'
     ), only_new AS (...)
     SELECT NOT EXISTS(only_old) AND NOT EXISTS(only_new)";
```

It catches `SET col = col` no-ops (identical bytes both sides). It does **not** catch `SET status='validated' WHERE status='sent_to_sop'` because `status` bytes differ in U_OLD vs U_NEW. And it lives in the deferred path only — the customer uses IMMEDIATE mode.

For 1.4.6 we extend the skip in two dimensions:

1. **Port to IMMEDIATE mode** (the customer's actual mode). The IMMEDIATE trigger has `__reflex_old_<src>` and `__reflex_new_<src>` transition tables directly.
2. **Compare projections excluding "filter-only" columns**, where "filter-only" = columns referenced in `where_predicate` (and possibly elsewhere) but **NOT** in the IMV's SELECT / JOIN / GROUP BY.

If the filter outcome is identical for U_OLD/U_NEW *and* the non-filter projection is byte-equal, the IMV's per-row contribution to the join is identical → no work to do.

## Architecture

### Step 1: extract "IMV-relevant" source columns at create time

A new field on `AggregationPlan`: `imv_relevant_columns: HashMap<String, HashSet<String>>` keyed by source table name. The set contains source-column names referenced in **base_query outside the WHERE clause** for that source.

Implementation lives in `src/sql_analyzer.rs` — extend the existing `SqlAnalysis` pass to record per-source column references separately for:
- **SELECT** projections (including aggregate arguments)
- **JOIN** ON conditions
- **GROUP BY** expressions
- **WHERE** clause (existing — already tracked via `where_clause`)
- **HAVING** clause

The `imv_relevant_columns[source]` set is the union of the first four minus the fifth. (HAVING references aggregate results, not source columns — irrelevant here, but for robustness include it on the "relevant" side.)

Edge cases:
- Column referenced in WHERE *and* SELECT → relevant (must compare it).
- Column referenced ONLY in WHERE → filter-only (don't compare).
- Aggregate arguments like `SUM(c.qty * c.discount)` → both `qty` and `discount` relevant.
- Subqueries inside WHERE → their inner column refs are filter-only too (they only affect the filter outcome).
- Computed columns in GROUP BY (`GROUP BY date_trunc('day', c.ts)`) → underlying `ts` is relevant.

Persist in the `aggregations` JSON via a new `imv_relevant_columns` map field on `AggregationPlan`.

### Step 2: at trigger time, the filter-aware spurious-skip query

The check runs **before** scratch INSERT (so we skip the JOIN scan too). Inside the immediate-mode trigger body (post `_has_rows` check, pre per-IMV loop body):

```plpgsql
-- For each IMV, before doing any work:
IF TG_OP = 'UPDATE' AND _rec.where_predicate IS NOT NULL THEN
    -- Filter-equivalent check: do all OLD rows pass the predicate iff
    -- the corresponding NEW rows do, AND project identically excluding
    -- filter-only columns?
    EXECUTE format(
        'WITH old_pass AS (
             SELECT %s FROM %I WHERE %s
         ),
         new_pass AS (
             SELECT %s FROM %I WHERE %s
         )
         SELECT
             NOT EXISTS(SELECT 1 FROM old_pass EXCEPT ALL SELECT 1 FROM new_pass)
             AND
             NOT EXISTS(SELECT 1 FROM new_pass EXCEPT ALL SELECT 1 FROM old_pass)',
        _imv_relevant_cols_csv,
        '__reflex_old_<src>',
        _rec.where_predicate,
        _imv_relevant_cols_csv,
        '__reflex_new_<src>',
        _rec.where_predicate
    ) INTO _spurious;
    -- Also: a row that DIDN'T pass before doesn't matter; same for after.
    -- The two EXCEPT ALL checks combined assert that the multiset of
    -- "rows passing predicate, projected to IMV-relevant cols" is
    -- identical between OLD and NEW.
    IF _spurious THEN
        CONTINUE;  -- skip this IMV's trigger body entirely
    END IF;
END IF;
```

The `_imv_relevant_cols_csv` is built from `aggregations->'imv_relevant_columns'->'<source>'` at codegen time. For the customer's IMV on `yse.demand_planning`, the relevant cols are `(id, assortment_id)` — `status` is filter-only.

For an UPDATE that changes only `status`:
- `old_pass` projects `(id, assortment_id)` from rows where `dp.status` passes whitelist.
- `new_pass` projects same. Since `id` and `assortment_id` didn't change and the filter outcome is identical, multisets match → skip.

For an UPDATE that changes `assortment_id` (a JOIN key in the IMV's downstream):
- `old_pass` projects `(id, OLD.assortment_id)`, `new_pass` projects `(id, NEW.assortment_id)` → multisets differ → skip doesn't fire → existing trigger path runs.

### Step 3: trigger-body codegen change

`build_trigger_ddls` in `src/schema_builder.rs` (line 367-446 currently) constructs the trigger function as a single `format!()` string template. Extend the template to include the filter-aware check before the existing where_predicate early-skip.

The check is **specific to UPDATE triggers** (INSERT/DELETE always change the multiset). Only the `__reflex_upd_trigger_on_<source>` function gets the new check.

The relevant-columns set is read from the per-IMV `_rec` record, fetched in the `FOR _rec IN SELECT ...` loop. Extend the SELECT to also fetch `aggregations::jsonb->'imv_relevant_columns'->'<source>'`. Need to substitute `<source>` at codegen time (each trigger function is per-source-table, so this is fine).

### Step 4: cache invalidation across cdylib swap

The 1.4.3 deferred-mode spurious-skip lives entirely in the cdylib (built at flush time inside `reflex_flush_deferred`). For IMMEDIATE-mode, the trigger function body is plpgsql DDL emitted at IMV-create time and **persisted** in pg_proc. Changing the trigger body shape means existing IMVs created on 1.4.5 won't have the new check.

Migration must re-emit trigger function bodies. The 1.4.4→1.4.5 migration didn't need to (only the cdylib changed); 1.4.5→1.4.6 must. Add Part X to the migration:

```sql
DO $$
DECLARE rec RECORD;
BEGIN
    FOR rec IN
        SELECT DISTINCT unnest(depends_on) AS src
        FROM public.__reflex_ivm_reference
        WHERE enabled = TRUE AND unnest(depends_on) NOT LIKE '<%'
    LOOP
        -- Recreate triggers for this source via the cdylib
        PERFORM public.reflex_rebuild_triggers(rec.src);
    END LOOP;
END $$;
```

A new SQL-callable function `reflex_rebuild_triggers(source_table TEXT)` invokes `build_trigger_ddls(source)` and CREATEs the new function bodies (replacing the existing ones).

## Files touched

| Area | File | Change |
|---|---|---|
| Schema | `src/sql_analyzer.rs` | Track per-source col refs by clause (SELECT/JOIN/GROUP BY/WHERE/HAVING). New struct field on `SqlAnalysis`. |
| Plan | `src/aggregation.rs` | New field `imv_relevant_columns: HashMap<String, HashSet<String>>` on `AggregationPlan`. Populated in `plan_aggregation_inner` from the analysis. |
| Persistence | `src/query_decomposer.rs` | `generate_aggregations_json` serializes the new field (`#[serde(default)]`). |
| Trigger codegen | `src/schema_builder.rs:367-446` | Extend `body_core` template for UPDATE-only path: emit the filter-aware skip check before the where_predicate early-skip. |
| Migration | `sql/pg_reflex--1.4.5--1.4.6.sql` (new) | (1) Backfill `imv_relevant_columns` for existing IMVs via a new `reflex_recompute_relevant_columns(view_name)` admin fn. (2) Rebuild trigger function bodies via `reflex_rebuild_triggers(source)` per source. |
| New SQL fn | `src/lib.rs` + `src/create_ivm.rs` | `reflex_recompute_relevant_columns(view_name TEXT) RETURNS TEXT` — re-analyzes the stored base_query and updates aggregations. |
| New SQL fn | `src/lib.rs` + `src/schema_builder.rs` | `reflex_rebuild_triggers(source_table TEXT) RETURNS TEXT` — emits the trigger DDLs for all IMVs depending on `source_table` (CREATE OR REPLACE). |
| Tests | `src/tests/pg_test_basic.rs` | (1) `pg_test_filter_aware_skip_status_whitelist` — exact yse shape. (2) `pg_test_filter_aware_skip_uses_full_path_on_non_filter_col_change` — UPDATE of an IMV-relevant col must NOT skip. (3) `pg_test_filter_aware_skip_when_filter_outcome_changes` — UPDATE moves row out of filter → must NOT skip. |
| Tests | `src/tests/unit_sql_analyzer.rs` | Per-clause column extraction. Edge cases: aliased cols, subqueries in WHERE, computed GROUP BY expressions. |

## Correctness invariants & tests

The skip is safe iff every dependent IMV's output is byte-identical pre/post for **every** row in the source delta. Three invariants:

1. **Filter outcome stability**: the set of rows passing `where_predicate` is identical between U_OLD and U_NEW. (If a row was passing and now doesn't, the IMV must DELETE that row's contribution.)
2. **IMV-relevant projection stability**: for every row in the (passing) intersection, the projection to IMV-relevant columns is byte-identical. (If a JOIN key changed, the IMV must re-shuffle.)
3. **The IMV doesn't reference filter-only columns in any non-WHERE clause**. (If `status` were in SELECT, even an identical filter-outcome transition would change the IMV's output column.)

#3 is the static check (done at IMV-create time via SqlAnalysis).
#1 + #2 are the runtime check (the EXCEPT ALL query).

Required test coverage:

- **Positive**: filter-whitelist transition (status='X' → status='Y', both pass) skips. Oracle: post-UPDATE EXCEPT ALL against fresh aggregate = 0.
- **Positive**: no-op UPDATE (SET col = col) where col is filter-only — skips.
- **Positive**: no-op UPDATE on multiple rows in one statement (multi-row transition table) — skips.
- **Negative**: UPDATE of an IMV-relevant column (e.g., `SET assortment_id = ...`) — does NOT skip, full path runs, post-UPDATE oracle = 0.
- **Negative**: filter-outcome transition (status='X' → status='unknown' which is NOT in whitelist) — does NOT skip, post-UPDATE oracle = 0 (the IMV must lose the dp's contribution).
- **Negative**: UPDATE row 1 (filter-equivalent) + UPDATE row 2 (filter-relevant) in same statement — does NOT skip (the multiset comparison rules out partial skip).
- **Negative**: IMV with status referenced in SELECT (e.g., `SELECT status, COUNT(*)...`) — even filter-equivalent changes must not skip because the IMV exposes status as an output column. Static check at create time disables filter-aware skip for this IMV.
- **Correctness across the migration backfill**: existing IMVs created on 1.4.5 get `imv_relevant_columns` populated by re-analyzing their stored base_query. Verify the backfill is correct for: (a) simple GROUP BY IMVs, (b) JOIN IMVs (the customer's shape), (c) DISTINCT IMVs, (d) WITH-clause IMVs (the column refs need to track across CTE boundaries).

## Performance expectations

| Scenario | Pre-1.4.6 | Post-1.4.6 |
|---|---:|---:|
| Customer status flip (yse, 76 M src, 64 % selectivity) | 14.8 s (post data-probe + dispatch) | **~50 ms** (filter-aware skip fires) |
| Customer real source change (price update) | 14.8 s | 14.8 s (skip doesn't fire — work is real) |
| Synthetic 2M filter-equivalent UPDATE | ~24 s | **~30 ms** |
| Synthetic 2M filter-relevant UPDATE | ~24 s | ~24 s (existing path) |

The filter-aware skip's overhead is one `EXCEPT ALL` between two small transition tables (typically <1000 rows per source-statement UPDATE). At even 10K transition rows on 5 columns, this completes in <20 ms. The penalty when the skip *doesn't* fire is therefore <20 ms — negligible vs the ~15 s work it would gate.

## Why this is bigger than #2 / #7

The data-probe (#2) and dispatch (#7) only changed:
- A boolean field on `not_null_columns` (probe) — local to one site.
- The trigger codegen's choice between MERGE and reconcile (dispatch) — local to one branch.

Smart spurious-skip touches:
- The SQL analyzer (per-clause column tracking — new pass).
- The plan struct (new persisted field).
- The migration (full trigger-body rebuild per existing IMV — not just a column add).
- The trigger function template (every IMV's source-table triggers need to be re-created).

The migration cost is the biggest risk: rebuilding triggers for all IMVs requires the cdylib to be loaded AND backends to be reconnected to pick up the new SQL. The 1.4.4 customer fix had this property too and it was noted in the journal ("backends connected before the upgrade will continue to serve cached MERGE SQL").

## Open design questions

1. **What about INSERT and DELETE triggers?** An INSERT trigger fires on a `__reflex_new_<src>` populated with the new rows. A filter-aware skip can fire iff none of the new rows pass the predicate. The existing `where_predicate` early-skip at `schema_builder.rs:390-392` already handles this (skips if no row passes). No extension needed. Same logic for DELETE.

2. **Multi-source IMVs**: an UPDATE on source A fires the trigger on A. We need to check filter-equivalence only for rows from A — other sources are unchanged. The current design handles this (the check is per-trigger, scoped to the firing source's transition tables).

3. **Cascading IMVs (depth ≥ 2)**: if an L1 IMV's source UPDATE is filter-equivalent and the skip fires, the L2 IMV depending on L1 also doesn't fire (L1's target wasn't touched). Correct by transitivity.

4. **Concurrent writes**: the skip check runs inside the trigger's transaction. Concurrent transactions writing to the same source are isolated by MVCC. The skip is a per-statement decision, not per-row, so no cross-row race.

5. **DEFERRED mode**: the existing 1.4.3 spurious-skip in DEFERRED catches byte-identical multisets only. Could we backport the filter-aware version to DEFERRED too? Yes — same logic, applied to the staging delta table instead of the transition tables. Defer to 1.4.7 to keep 1.4.6 scope focused on IMMEDIATE.

## Out of scope for this plan

- **Column-level UPDATE filtering** (`UPDATE … OF status`). PG triggers can be `OF column` to fire only when specific columns are touched. We could install separate triggers per "column class" to short-circuit at trigger-fire time without even reading transition tables. Bigger refactor, deferred.
- **Per-IMV skip thresholds**. The current design is binary (skip or don't). Could imagine "skip if cost-to-check < cost-to-execute × probability-of-skip". Premature; the skip check is cheap enough.
- **JIT-cache of the skip SQL**. Right now each trigger fire EXECUTE's the same skip SQL with substituted strings. PG plan caches it. No further work needed.

## Risk and rollback

- **Risk**: incorrect identification of "IMV-relevant columns" — false negative skip means correctness regression. Static analysis is the safety belt; thorough tests on edge cases (aliases, CTEs, subqueries, computed GROUP BY) are mandatory.
- **Mitigation**: a runtime "double-check" mode — opt-in via `SET reflex.skip_double_check = on` — that, after a skip fires, runs a sample of the would-have-been work and asserts the IMV state is identical. Off by default in production but useful in staging.
- **Rollback**: feature can be gated by `SET reflex.smart_skip = off` (codegen-time check; if off, emit the old trigger body). Then the migration is purely additive and reversible.

## Effort estimate

- SQL analyzer per-clause tracking: 2-3 days (the analyzer is non-trivial; needs CTE / subquery handling).
- Plan struct + serialization: 0.5 day.
- Trigger codegen: 1 day.
- Migration (backfill + rebuild triggers): 1 day.
- Tests (unit + pg_test, all edge cases): 1-2 days.
- Bench validation (customer-shape synthetic): 0.5 day.
- Total: **5-8 days** focused work.

## Acceptance criteria

1. All existing 531 tests still pass.
2. New tests in `pg_test_basic.rs` (5 positive + 3 negative cases) pass.
3. Bench on customer-shape synthetic (2 M, 75 % selectivity, status whitelist flip): post-fix < 100 ms (vs 24 s on 1.4.5 dispatch path).
4. Bench on customer-shape synthetic (price-update style): post-fix matches 1.4.5 dispatch path within 5 %.
5. EXCEPT ALL oracle = 0 on all UPDATE shapes across all test IMVs.
6. Migration backfill verified on an `ALTER EXTENSION pg_reflex UPDATE FROM '1.4.5' TO '1.4.6'` chain (no errors, all triggers rebuilt).

## Reference: where in the codebase to start

- `src/sql_analyzer.rs:706-770` — `for item in &select.projection` — extend to track which clause each column ref came from.
- `src/aggregation.rs:61-98` — `AggregationPlan` — add `imv_relevant_columns` field.
- `src/aggregation.rs:494-941` — `plan_aggregation_inner` — populate the new field from analysis.
- `src/schema_builder.rs:380-403` — `body_core` template — insert the filter-aware skip block.
- `src/trigger.rs:2004-2047` — existing DEFERRED spurious-skip for reference (similar query shape, different scope).
- `journal/2026-05-12_1_4_3_and_1_4_4_customer_unblock.md` — the 1.4.3 spurious-skip rationale.
- `benchmarks/bench_1_4_4_yse_instrumented.sql` — instrumented bench; adapt to also instrument the skip path.

## Dependency on #7 (already shipped)

#9 supersedes the high-selectivity dispatch path for filter-equivalent UPDATEs (because the skip fires before the dispatch ever runs). For non-filter-equivalent UPDATEs both paths can coexist — the skip doesn't fire and the dispatch decides MERGE vs reconcile. No conflict.
