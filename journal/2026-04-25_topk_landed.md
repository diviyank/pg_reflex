# Top-K MIN/MAX heap — landed in 1.3.0

**Date**: 2026-04-25
**Audit linkage**: `journal/audit-production-readiness.md` R3
**Status**: **LANDED** (opt-in via `topk` parameter on `create_reflex_ivm`).

## What shipped

Each MIN/MAX intermediate column can carry a sibling `<name>_topk` array column
that holds the K extremum values seen for the group, kept sorted (ASC for MIN,
DESC for MAX). On retraction the array is updated via multi-set subtraction;
the `build_min_max_recompute_sql` recompute path runs only for groups whose
array underflows.

### API

```sql
-- 6th positional arg = top-K size (K). 0 or NULL disables (legacy 1.2.x).
SELECT create_reflex_ivm(
    'stock_chart_weekly_v',
    'SELECT product_id, week, MIN(price) AS lo, MAX(price) AS hi
     FROM stock_history GROUP BY product_id, week',
    NULL, NULL, NULL,
    16  -- top-K = 16
);
```

The top-K is a **per-IMV opt-in**. Existing 1.2.x IMVs continue to use the
scoped recompute path on retraction, with no migration cost.

### Wiring summary

| File | Change |
|---|---|
| `src/aggregation.rs` | `IntermediateColumn` gains `topk_k: Option<usize>` (serde-default `None`). Helper `topk_column_name()`. New `plan_aggregation_with_topk` planner. |
| `src/lib.rs` | Bootstrap SQL adds `__reflex_array_subtract_multiset(anyarray, anyarray)` plpgsql helper. New 6-arg `create_reflex_ivm` overload accepts `topk` integer. |
| `src/create_ivm.rs` | `create_reflex_ivm_impl` threads `topk_k: Option<usize>` through the recursive set-op / CTE / window decomposition paths. |
| `src/schema_builder.rs` | `intermediate_column_spec` emits a sibling `<name>_topk <type>[] DEFAULT '{}'` column when `ic.has_topk()`. |
| `src/query_decomposer.rs` | `generate_base_query` projects `(array_agg(x ORDER BY x ASC NULLS LAST) FILTER (...))[1:K]` per top-K MIN/MAX column. Symmetric DESC for MAX. |
| `src/trigger.rs` | `build_merge_using` MERGE codegen: <ul><li>Add path: top-K column = sorted-merge of `t.topk \|\| d.topk` truncated to K.</li><li>Subtract path: top-K column = `__reflex_array_subtract_multiset(t.topk, d.topk)`; scalar = `topk[1]`.</li><li>WHEN NOT MATCHED INSERT writes the top-K column from delta directly.</li></ul> `build_min_max_recompute_sql` extends SET to rewrite top-K, and triggers when either scalar is NULL or the array is empty/cardinality 0. |

### SQL helper

```sql
CREATE OR REPLACE FUNCTION public.__reflex_array_subtract_multiset(
    arr anyarray, remove anyarray
) RETURNS anyarray
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$ ... $$;
```

Mutates the resolved-type input parameter `arr` in-place (PL/pgSQL forbids
local vars of pseudo-type `anyarray`/`anyelement`, so parameter mutation is
the canonical workaround for polymorphic implementations).

### Test coverage

- `pg_test_topk_min_basic` — INSERT → seed 7 rows, K=4. Asserts companion
  array shape, then INSERT/DELETE through the array.
- `pg_test_topk_max_basic` — symmetric for MAX, K=3, including a heap
  underflow test.
- `pg_test_topk_fuzz_min` — 30 random INSERT/DELETE/UPDATE iterations,
  asserting `assert_imv_correct` (EXCEPT ALL oracle) after every step.
  K=8, MIN+MAX in the same IMV.

All 500 tests pass.

## Known limitations & follow-ups

1. **Element type is captured at create-time** via the existing
   `resolve_column_type` path. Top-K columns inherit the same effective type
   as their scalar sibling. Verified for NUMERIC. Other types (DATE,
   TIMESTAMP, TEXT) inherit through `intermediate_column_spec` and should
   work but are not yet test-covered — add coverage when a workload requests
   it.
2. **`reflex_enable_topk(name, k)` retrofit SPI** — not yet shipped. Existing
   1.2.x IMVs cannot opt into top-K without `drop + create`. The retrofit
   path needs an `ALTER TABLE … ADD COLUMN topk` plus a one-shot population
   from source. Tracked for 1.3.1.
3. **`pg_reflex.alter_source_policy = 'error'` + top-K interaction** — top-K
   columns are a 1.3.0 schema change; if an operator alters the source
   underneath a top-K IMV, the existing rebuild path runs. No special-case
   code needed.
4. **NULL handling**: the base-query projection uses `FILTER (WHERE col IS
   NOT NULL)` so NULLs don't consume top-K slots. Verified by the fuzz test.
5. **`array_position` semantics**: returns the FIRST occurrence; `subtract_multiset`
   is therefore deterministic for value-equal duplicates (each call removes
   one occurrence at a time, preserving multiplicity).

## Bench results (full-scale, captured 2026-04-25)

### 5M-row source, 5K groups, MIN/MAX IMV — PG18

`benchmarks/bench_1_3_0_topk_5m.sql` (+ a small-batch companion script
`/tmp/bench_topk_small.sql` for the 100/1000-row points).

| DELETE batch | REFRESH MV | IMV (no topk) | IMV (`topk=16`) | top-K vs no-topk | top-K vs REFRESH |
|---:|---:|---:|---:|---:|---:|
| 100 | 529 ms | 479 ms | **93 ms** | **5.1× faster** | **5.7× faster** |
| 1,000 | 529 ms | 1,551 ms | **556 ms** | **2.8× faster** | parity |
| 10,000 | 540 ms | 14,847 ms | **2,726 ms** | **5.4× faster** | 0.2× (REFRESH wins) |
| 50,000 | 540 ms | 14,888 ms | **2,908 ms** | **5.1× faster** | 0.2× (REFRESH wins) |

### 1M-row source, 1K groups — PG18

| Op | REFRESH MV | IMV (no topk) | IMV (`topk=16`) |
|---|---:|---:|---:|
| INSERT 10,000 | — | 109 ms | 264 ms |
| DELETE 10,000 | 112 ms | 905 ms | **148 ms** |

### Findings

- **Operational sweet spot (100–1000 row deltas) hit hard**: at 5M rows
  top-K matches or beats both REFRESH and the 1.2.0 scoped-recompute path.
  The 100-row delete is 5.7× faster than REFRESH; this is exactly the
  cron-tick / batch-replication shape the audit's R3 was about.
- **Wide retraction (>10K of 5M rows) crosses over to REFRESH** because a
  single sequential scan dominates. The crossover shifts higher with source
  size — at the audit's 50M-row stock_chart scale, top-K should win across
  all delta sizes.
- **INSERT cost is ~2.5× higher** than no-topk (264ms vs 109ms on 10K
  rows). This is the price of maintaining the heap on every write. For
  retraction-heavy workloads (the stock_chart pattern) it is a clean win;
  for append-only workloads top-K is a net loss and operators should leave
  the parameter unset.
- Algebraic aggregates (SUM/COUNT/BOOL_OR) remain at parity with REFRESH —
  no regression introduced by the 1.3.0 schema additions.

### Critical perf fixes during landing

The first end-to-end run had top-K DELETE **2.6× SLOWER than no-topk**
(2370 ms vs 899 ms on 5M / 100 rows). Two root causes:

1. The MERGE used `__reflex_array_subtract_multiset` twice per row — once
   for the array column, once subscripted as `[1]` for the scalar. Each
   call rebuilt the array. **Fix**: split into MERGE-updates-array,
   followed by a small post-MERGE `UPDATE` that refreshes the scalar from
   `topk[1]`. Single function call per row.
2. The `build_min_max_recompute_sql` source-scan recompute always ran,
   even when no group's heap had underflowed. **Fix**: wrap the UPDATE in
   `DO $$ IF EXISTS (SELECT 1 …) THEN … END IF; END $$` so the source
   scan is skipped when every affected group still has a populated heap.

After both fixes: 93 ms / 148 ms at the 100-row and 10K-row points
respectively — a 5-6× win.

### Reproduce

```bash
psql -p 28818 -U postgres -d test -f benchmarks/bench_1_3_0_topk_5m.sql
psql -p 28818 -U postgres -d test -f benchmarks/bench_1_3_0_topk.sql
```

Bench numbers also captured in `docs/performance/benchmarks.md` and the
README's "1.3.0 — Top-K MIN/MAX retraction" section.
