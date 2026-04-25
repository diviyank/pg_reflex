# Full-scale 1.3.0 INSERT/DELETE/UPDATE bench + optimization roadmap

**Date**: 2026-04-25
**Setting**: PG18, shared_buffers = 4GB, isolated `test_bench_1_3_0` DB.
**Shape**: 10M-row source, 5-table JOIN (sales × product × location × calendar
× pricing), passthrough IMV (no aggregation), IMMEDIATE refresh mode, PK keyed
on `id`. Build time: 10M-row INSERT 24s, ANALYZE 0.1s, matview 24s, IMV 23s.

## Headline numbers

REFRESH MATERIALIZED VIEW baseline: **24,130 ms** (warm).

| Op | Batch | Reflex | Raw | Reflex / raw | Advantage vs raw + REFRESH |
|---|---:|---:|---:|---:|---:|
| INSERT | 1K | 36 ms | 10 ms | 3.6× | **99.8%** |
| INSERT | 10K | 362 ms | 62 ms | 5.8× | 98.5% |
| INSERT | 100K | 2.4 s | 546 ms | 4.4× | 90.2% |
| INSERT | 500K | 11.4 s | 3.0 s | 3.7× | 58.0% |
| INSERT | 1M | 22.3 s | 6.8 s | 3.3× | 27.9% |
| DELETE | 1K | 445 ms | 88 ms | 5.0× | 98.2% |
| DELETE | 10K | 115 ms | 93 ms | 1.2× | **99.5%** |
| DELETE | 100K | 357 ms | 141 ms | 2.5× | 98.5% |
| DELETE | 500K | 1.7 s | 464 ms | 3.6× | 93.2% |
| DELETE | 1M | 3.5 s | 721 ms | 4.8× | **86.0%** |
| UPDATE | 1K | 363 ms | 92 ms | 3.9× | 98.5% |
| UPDATE | 10K | 414 ms | 195 ms | 2.1× | 98.3% |
| UPDATE | 100K | 3.4 s | 1.2 s | 2.9× | 86.5% |
| UPDATE | 500K | 17.1 s | 5.5 s | 3.1× | 42.2% |
| UPDATE | 1M | **32.3 s** | 11.6 s | 2.8× | **9.6%** |

**Correctness**: `EXCEPT ALL` against fresh REFRESH MV → **PASS** (0 mismatches).

**Reconcile cost**: `reflex_reconcile` 28.7s vs `REFRESH MV` 24s → 20% slower
than full rebuild. Worth optimizing.

## What the numbers say

1. **DELETE is the standout.** Even at 1M rows, 3.5s vs 24s REFRESH (86%
   advantage). Per-row trigger overhead drops to ~2.7 µs at scale because the
   delete path is essentially `DELETE ... WHERE id IN (delta)` — no dim-table
   JOIN, no MERGE materialization.

2. **INSERT scales linearly** at ~22 µs / row of trigger overhead (5-table JOIN
   on the delta + MERGE). Crossover with REFRESH MV is past 2M rows.

3. **UPDATE 1M is the only loss.** 32s reflex vs 24s REFRESH (9.6% "advantage"
   that includes raw UPDATE cost). The trigger does DELETE-old + INSERT-new
   for every row even though the bench only mutates `qty` (not a JOIN column,
   not the PK). This is the **clearest optimization target**.

4. **Trigger constant cost is ~250-450 ms.** DELETE 1K = 445ms (mostly setup),
   DELETE 10K = 115ms (anomalously cached). Small-batch performance is
   dominated by per-statement template build + advisory lock + EXISTS check.

## Optimization strategies

The numbers point to four targeted wins, ordered by expected payoff /
implementation cost.

### O1 — PK-stable UPDATE in-place propagation

**Target**: UPDATE on passthrough IMVs where no JOIN/FK column was changed.

**Today**: passthrough UPDATE emits two statements (`src/trigger.rs:971-995`):
1. `DELETE FROM target WHERE (cols) IN (SELECT cols FROM pt_old)` — already
   PK-fast.
2. `INSERT INTO target <base_query with sales → pt_new>` — re-runs the
   5-table JOIN against the new-rows transition table.

For 1M rows the DELETE costs ~2.8s and the JOIN-materializing INSERT costs
~17.6s. Together = trigger overhead 20.4s on top of the 11.6s raw UPDATE.

**Two variants of the optimization** (different payoffs):

**O1a — collapse DELETE-old + INSERT-new into single UPDATE-FROM-delta**.
```sql
UPDATE target t
SET (col1, col2, ...) = (sub.col1, sub.col2, ...)
FROM (<base_query with sales → pt_new>) sub
WHERE t.<unique> = sub.<unique>;
```
The JOIN still runs (inside the FROM subquery), but we skip the DELETE phase.
**Realistic payoff: ~10-15% on UPDATE 1M (32s → ~29s).** The win is the
DELETE-old elimination (2.8s saved) + slightly better PK-update access
pattern. NOT a 4× win — earlier estimate was wrong.

**O1b — full column classification + skip-JOIN on no-key-change** (the
ambitious version): at IMV-create time, classify each source column as
*key-affecting* (JOIN ON, WHERE, derived expressions like `qty * base_price`)
or *value-only* (just projected as-is). On UPDATE, if only value-only columns
changed, emit:
```sql
UPDATE target SET col_a = pt_new.col_a, col_b = pt_new.col_b, ...
FROM pt_new
WHERE target.<unique> = pt_new.<unique>;
```
No JOIN at all. **Payoff: 3-4× on UPDATE 1M when the workload only mutates
value-only columns** (the bench case: only `qty` changes). Drops to O1a
behavior when key columns change.

The catch: classification needs to handle derived expressions
(`qty * base_price` is value-only IF `base_price` doesn't change in the same
batch — which we can't know at create time). Conservative rule: a derived
expression is value-only iff every column it references is value-only AND
comes from the source table being UPDATEd. The bench's `turnover` would be
classified as value-only (depends on `qty` from sales × `base_price` from
pricing — but pricing isn't being UPDATEd). Wait, `base_price` comes from a
JOINed table, so to recompute `turnover` we'd still need to look it up.

So O1b reduces to: "value-only" = the column is *itself* a source-table
column, not a JOIN-derived expression. Derived expressions still require
re-evaluating via JOIN → fall back to O1a for those rows.

In the bench's IMV, value-only cols are `qty`, `qty_ub`, `qty_lb`. Derived
`turnover` is JOIN-dependent. So O1b would update `qty/ub/lb` directly but
still need the JOIN for `turnover` recomputation. Net: small win over O1a.

**Conclusion**: implement O1a only. O1b's complexity isn't justified by the
realistic bench data.

**Files**: `src/trigger.rs:971-995` (rewrite passthrough UPDATE branch).

**Test plan**:
- pg_test_correctness: 5-table JOIN passthrough IMV with PK-stable UPDATE,
  assert match against fresh REFRESH MV.
- pg_test_correctness: UPDATE that DOES change unique key → assert correct
  fallback to DELETE+INSERT.
- proptest in `unit_proptest.rs`: random INSERT/DELETE/UPDATE with mix of
  key-stable and key-changing UPDATEs.
- Re-run `bench_full_scale_1_3_0.sql` and confirm UPDATE 1M ≤ 30s.

**Risk**: low. The single-statement UPDATE is a strict refactor of the
two-statement DELETE+INSERT (same row-level effect when unique key is
stable). Need an explicit test for "UPDATE changes unique_columns" to verify
fallback.

**Effort**: ~80 LOC + 3-4 tests. 1 day cycle.

### O2 — Trigger template caching

**Target**: trigger constant overhead. Bench shows DELETE 1K at 445ms (5× raw)
while DELETE 10K is 115ms — strong evidence of a per-statement-fire fixed
cost. INSERT 1K is only 36ms (the advisory lock, EXISTS check, and
`reflex_build_delta_sql` parse/build sit on top of the actual JOIN+INSERT
work).

**Today**: every trigger fire calls `reflex_build_delta_sql` from plpgsql,
which re-parses the registry row, re-deserializes `aggregations` JSON, and
rebuilds the SQL string. For the same IMV firing 1000 times in a session,
that's 1000× redundant work.

**Note on passthrough DELETE**: the existing code already emits the PK-fast
form when `passthrough_key_mappings` is set
(`src/trigger.rs:951-964`) — `DELETE FROM target WHERE (cols) IN (SELECT
cols FROM pt_old)`. So the "DELETE-by-PK" half of the originally-imagined
O2 is already implemented. The remaining win is template caching.

**Change**: an `lru<(view_name, op), String>` in pg_reflex's static
storage (per-backend, not shared memory — sufficient for the steady-state
case). Populated on first fire, looked up on subsequent. Invalidated on
ALTER EXTENSION or `reflex_rebuild_imv`.

**Expected payoff**:
- DELETE 1K: 445 ms → ~250 ms (skip parse/build of ~150ms).
- INSERT 1K: 36 ms → ~25 ms.
- 30-50% reduction on small-batch trigger overhead. Larger batches dominated
  by data work, no measurable change.

**Files**: `src/trigger.rs` (cache; existing code path `reflex_build_delta_sql`
just gets a pre-cache lookup).

**Risk**: low. Cache key includes view_name + op + base_query hash; any IMV
mutation invalidates.

**Effort**: ~120 LOC + 2 tests. 1 day cycle.

### O3 — Parallel sibling IMV flush

**Target**: cascade scenarios where one source change triggers N independent
IMVs. Out-of-bench observation from db_clone: 6 IMVs per `sales_simulation`
hit. Currently they flush serially in `reflex_flush_deferred`.

**Today**: per-IMV DO blocks run serially in a single backend.

**Change**: when N ≥ 3 sibling IMVs exist (same source, same graph_depth),
fan out via `pg_background` (PG 18+ ships this). Each IMV in its own
backend, parent waits for all. Cap at `max_parallel_workers_per_gather - 1` to
respect the planner budget.

**Expected payoff**: linear in IMV count for the sibling case. With 6 sibling
IMVs the worst-case flush drops 4-6×. Synthetic bench (1 IMV) sees no change.

**Files**: `src/trigger.rs` (parallel dispatch in `reflex_flush_deferred`),
`Cargo.toml` (no new deps; pg_background is via SQL/extension).

**Risk**: medium. pg_background requires careful error handling. Can hit
deadlocks if two parallel flushes target the same downstream chain.
Mitigation: parallel only when sibling IMVs share *no* graph_child overlap.

**Effort**: ~400 LOC + 6 tests + benchmark on db_clone-like setup. 1 week.

### O4 — Reconcile via incremental delta (replace full rebuild)

**Target**: `reflex_reconcile` is 28.7s vs REFRESH 24s — 20% slower than the
sledgehammer. Today reconcile drops + repopulates the entire IMV.

**Change**: compute IMV - source_view diff (one EXCEPT ALL each direction),
issue targeted DELETE + INSERT for the diff. For drift-free IMVs the diff is
0 rows → 1 scan + 1 EXCEPT ALL → ~5s instead of 28.7s.

**Expected payoff**:
- Healthy IMV reconcile: 28.7s → 5s (5-6× speedup).
- Drifted IMV with small diff: 28.7s → ~5-15s.
- Drifted IMV with large diff: 28.7s → similar to today (no win).

**Files**: `src/reconcile.rs` (replace the truncate-and-repopulate path).

**Risk**: low. Correctness preserved by the EXCEPT ALL diff applied as deltas.
Mitigation: keep the old "full rebuild" path behind a force=TRUE param for
operators who want belt-and-suspenders.

**Effort**: ~150 LOC + 3 tests. 1-2 day cycle.

## Cross-cutting: what these don't fix

- **INSERT 1M trigger overhead is fundamental** — the 5-table JOIN of a 1M-row
  delta to the dim tables is real work. ~22 µs/row trigger overhead vs
  ~7 µs/row raw is essentially "doing the JOIN twice" (once for the source
  insert, once for the IMV materialization). No magic optimization here. The
  win is bounded to ~2× via planner hints, and crossover with REFRESH stays at
  ~2-3M rows.
- **REFRESH crossover for very large operations** is a fundamental property,
  not a bug. Operators with >1M-row write batches should consider switching to
  scheduled `REFRESH` for those workloads — the docs operations page already
  flags this. **What 1.3.0 + the optimizations above buy is a wider operational
  sweet spot, not infinite scaling.**

## O1a — ATTEMPTED, REVERTED (2026-04-25)

Two iterations were tried and rolled back for the reasons below.

### Iteration 1 (unsafe): unconditional in-place UPDATE-FROM-delta

Replaced the DELETE-old + INSERT-new pair with a single
`UPDATE target SET (cols) = (sub.cols) FROM (<delta_query>) sub
WHERE t.<uniq> = sub.<uniq>`. On the 10M-row JOIN bench this looked great
(UPDATE 1K dropped from 363ms to 42ms — 8.6× faster, all-pass-on-paper
against EXCEPT ALL after-the-fact). But the full test suite caught the
correctness bug: `pg_test_correctness_update_join_key` fails. The case:

- IMV: `SELECT s.id, s.val, d.label FROM ujk_src s JOIN ujk_dim d ON s.did = d.id`
- UPDATE: `UPDATE ujk_src SET did = 999 WHERE id = 1` (orphans the row by
  pointing it at a non-existent dim ID)

After the unsafe UPDATE, the orphaned row's `did` no longer JOINs, so the
delta query returns 0 rows for that `id`. The in-place UPDATE then matches
0 rows and the OLD row stays in target — the IMV is now wrong (still has
`(1, 'x', 'A')` when it should have nothing for `id=1`). Verified: this is
the failure mode whenever a key-affecting source column changes such that
the JOIN result loses the row entirely.

### Iteration 2 (safe): single-table-passthrough only

Restricted the fast path to IMVs whose base_query has no JOIN. Bench the
single-table case (5M-row staging × passthrough IMV with id-PK):

| Batch | With O1a | Without O1a (fallback) |
|------:|---------:|-----------------------:|
| 1K | 11 ms | 10 ms |
| 10K | 75 ms | 75 ms |
| 100K | 768 ms | 797 ms |
| 500K | 4,557 ms | 4,840 ms |
| 1M | 10,486 ms | 10,141 ms |

**Within measurement noise.** The `information_schema.columns` lookup +
DO-block dispatch eats whatever savings come from skipping the DELETE-old
phase; the original 2-statement form (`DELETE FROM target WHERE id IN (...);
INSERT INTO target SELECT * FROM pt_new`) is already as cheap as it can be
for this shape.

### Outcome

Reverted to the original 2-statement passthrough UPDATE codegen. All 504
tests green. JOIN-bench numbers in `benchmarks/bench_full_scale_1_3_0.sql`
are unchanged from the original (UPDATE 1K = 363ms, UPDATE 1M = 32.3s). The
optimization roadmap below stands; O1a is **not** considered closed but
needs a different mechanism (e.g. proper column classification at create
time) to deliver a real win without breaking JOIN-key UPDATE correctness.

## Recommended ordering

After this iteration:

1. **O1b (column-classified fast path)** — re-attempt of O1, but now with
   proper key-affecting / value-only classification at IMV-create time.
   The fast path fires only when the UPDATE touches no key-affecting
   column (so the JOIN result row count is provably preserved), avoiding
   the iteration-1 correctness hole. ~250 LOC + sql_analyzer changes.
   Expected payoff: bring back the 8.6× small-batch win on JOIN
   passthroughs whose UPDATEs touch only value columns (the common
   bench pattern).
2. **O2 (template caching)** — biggest practical win for OLTP-shape
   workloads (small batches, frequent fires). Drops constant overhead 30-50%.
   Land in 1.3.1.
3. **O4 (incremental reconcile)** — fixes the only place reconcile is
   materially slower than REFRESH. Modest scope. Land in 1.4.0.
4. **O3 (parallel sibling flush)** — biggest absolute win for multi-IMV
   sources (the db_clone shape with 6 sibling IMVs per source) but highest
   implementation complexity. Land in 1.5.0.

**Honest scope check**: none of these turn UPDATE 1M from a REFRESH-parity
case into a REFRESH-blowout. The fundamental cost is the JOIN. For batches
above 1M rows, scheduled REFRESH MV remains the right tool; pg_reflex's
sweet spot is 1K-500K-row deltas where the trigger overhead is amortized
against avoiding the 24s full rebuild.

## O2 — LANDED (2026-04-26)

Implemented `reflex_build_delta_sql` per-backend cache:
- Static `OnceLock<Mutex<HashMap<u64, String>>>` keyed on a hash of every
  input (`view_name`, `source_table`, `operation`, `base_query`, `end_query`,
  `aggregations_json`, `orig_base_query`).
- Content-addressable: any IMV rebuild that mutates `base_query` /
  `aggregations` produces a different hash → natural miss, no explicit
  invalidation required.
- Cap at 256 entries; on overflow the cache is cleared (no LRU complexity —
  the working set is bounded by `#IMVs × #sources × 3 ops`, which sits well
  under 256 for any realistic deployment).
- `reset_delta_sql_cache` exposed under `cfg(any(test, feature = "pg_test"))`
  for the consistency unit test.

**Critical re-read of the journal's own claim**: the original O2 section
projects "DELETE 1K: 445 ms → ~250 ms" (≈150 ms saved per fire). That
estimate was not grounded — the actual `reflex_build_delta_sql` body is a
serde_json parse plus a fixed sequence of `format!` / `replace_identifier`
calls. On the bench's aggregation JSON (~2-5 KB) that's hundreds of µs to a
small number of ms, not 150 ms. The 30-50% projection is overstated; the
realistic per-fire saving is in the 200 µs – 2 ms range. So the ROI is on
**OLTP-shape workloads** (many trigger fires per session, small batches),
not on the headline 1K/10K/1M bench numbers.

**Risk profile**: cache is purely a function-output memoization. The
existing 504-test suite covers SQL correctness; adding the cache could only
break things by returning a stale entry — which the content-addressable key
prevents. Added `test_delta_sql_cache_consistency` to lock that in
explicitly (cold == warm, rebuild-after-reset == cold, key dimensions
diverge as expected).

**Validation**: `cargo pgrx test --features pg17` → 505 / 505 pass
(includes 337 #[pg_test] integration tests). `cargo clippy --tests` clean.

**Out of scope for this commit**: re-running `bench_full_scale_1_3_0.sql`
to quantify the win. Given the realistic per-call saving is sub-ms, the
bench-level signal will be at the noise floor for the headline shapes; the
real OLTP-workload measurement belongs in a different bench (high-fire-rate,
small-batch). Deferred.

## O1b / O3 / O4 — DEFERRED

Critical re-read of the recommended ordering against CLAUDE.md priority
order (correctness > simplicity > performance):

- **O1b** is a re-attempt of an optimization whose simpler form (O1a
  iteration 1) already shipped a correctness regression that *all
  non-targeted tests passed* before `pg_test_correctness_update_join_key`
  caught it. The ambitious version moves part of the safety logic to
  IMV-create time (column classification) and part to runtime (which
  columns this trigger event touched). That widens, rather than narrows,
  the surface where a hole could hide. The single-table-passthrough
  iteration 2 of O1a came in within measurement noise on a comparable
  shape — concrete evidence that the family of optimizations is not the
  free lunch the bench numbers suggest. Held until a real workload signal
  forces the issue.

- **O4** as written claims `reflex_reconcile` 28.7 s → 5 s for healthy
  IMVs, but that math doesn't hold without materializing the fresh view
  (which is itself the 24 s in the bench). A genuine fast path requires
  either (a) restricting to passthrough IMVs and PK-keyed FULL OUTER JOIN
  diff, which is plausibly worth ~80 LOC, or (b) re-deriving aggregate
  intermediate state from base_query and diffing — which is the same cost
  as the rebuild it's replacing. Held until we either accept the
  passthrough-only scope or measure that the bench's healthy-reconcile
  path is dominated by index-rebuild rather than INSERT.

- **O3** is explicitly tagged 1.5.0 in the recommended ordering, requires
  `pg_background` integration, and carries deadlock risk on shared
  graph_child chains. Out of scope for this iteration.

## Reproduce

```bash
psql -U postgres -d test_bench_1_3_0 -f /tmp/bench_1_3_0_full_scale.sql  # full
psql -U postgres -d test_bench_1_3_0 -f /tmp/bench_1_3_0_resume2.sql      # resume from existing data
```

Logs: `/tmp/bench_1_3_0_resume2.log`.

The bench script lives in `/tmp/`; it should be promoted to `benchmarks/` once
we re-run after O1/O2 to capture the speedup.
