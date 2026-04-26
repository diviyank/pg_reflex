# Top-K — auto-enable + UPDATE staleness fix + non-NUMERIC type fix

**Date**: 2026-04-26
**Linkage**: `journal/audit-production-readiness.md` Item 5 follow-ups,
`journal/2026-04-25_topk_landed.md` "known limitations" §1 + §4.

## Tldr

Two correctness fixes and one default change ship together:

1. **MIN/MAX intermediate `pg_type` propagation** — top-K on TEXT / DATE /
   TIMESTAMP source columns used to fail with
   `COALESCE could not convert type numeric[] to text[]`. Fixed by
   propagating the resolved source-arg type onto `IntermediateColumn.pg_type`
   after catalog introspection.
2. **Partial-heap staleness on UPDATE** — UPDATEs on top-K MIN/MAX IMVs
   could land the heap with K elements that aren't the true top-K. Fixed
   by re-ordering the UPDATE flow so top-K MIN/MAX recomputes after
   `Sub + Add` for every affected group.
3. **Auto-enable** — `create_reflex_ivm` now defaults to `topk = 16` on
   any IMV containing MIN/MAX. Operators opt out via the 6-arg overload
   with `topk = 0`.

## Bug 1 — MIN/MAX intermediate `pg_type` propagation

### Repro

```
CREATE TABLE tmm_src (id SERIAL, grp TEXT, val TEXT);
INSERT INTO tmm_src ...;
SELECT create_reflex_ivm('tmm_view',
    'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM tmm_src GROUP BY grp');
-- ERROR: COALESCE could not convert type numeric[] to text[]
```

### Root cause

`aggregation::plan_aggregation_inner` hardcodes `pg_type: "NUMERIC"` on
every MIN/MAX `IntermediateColumn`. The schema builder
(`schema_builder.rs:54`) special-cases this and resolves the actual
source-column type via `resolve_column_type` for DDL purposes — so the
table column was created correctly. The trigger MERGE codegen
(`trigger.rs:139,156`) reads `ic.pg_type` directly to emit
`COALESCE(d."__min_x_topk", '{}'::NUMERIC[])`. When the column is
`TEXT[]`, the cast fails.

### Fix

`create_ivm.rs`: after `query_column_types_from_catalog +
augment_column_types_from_query`, walk `plan.intermediate_columns` and
replace any MIN/MAX `pg_type == "NUMERIC"` with the resolved source-arg
type. Mirrors the existing SUM → DOUBLE PRECISION pass.

### Test coverage

`pg_test_topk_text_min_max`, `pg_test_topk_date_min_max`,
`pg_test_topk_timestamp_min_max` — INSERT / DELETE / UPDATE /
heap-underflow each on K=4 with non-NUMERIC source columns, EXCEPT ALL
oracle after every step.

## Bug 2 — Partial-heap staleness on UPDATE

### Repro

`pg_test_randomized_mutation_sequence` (50 mutations, 3 groups, K=16)
caught it first; `pg_test_topk_partial_heap_staleness_regression` is
the minimal isolated shape.

```text
Source = {1, 2, 3, 4, 5}.   K=2.   heap = [1, 2].   scalar = 1.
UPDATE val=1 → val=10.
  delta_old.topk = [1].   delta_new.topk = [10].
  Sub:           heap = [2].             scalar = NULL.
  topk_refresh:  scalar = heap[1] = 2.
  recompute(legacy gate `scalar IS NULL`): scalar=2 → no recompute.
  Add:           heap = top-2([2] ∪ [10]) = [2, 10].   scalar = LEAST(2, 10) = 2.

True source post = {2, 3, 4, 5, 10}.   true MIN = 2.   IMV scalar = 2.   ✓ at this point.
True top-2 = [2, 3].   IMV heap = [2, 10].   <- stale, val=3 was never in heap.

DELETE val=2.
  delta_old.topk = [2].
  Sub:           heap = [10].            scalar = NULL.
  topk_refresh:  scalar = heap[1] = 10.

True source post = {3, 4, 5, 10}.   true MIN = 3.   IMV scalar = 10.   ❌
```

### Root cause

The 1.3.0 recompute trigger fires only on
`scalar IS NULL OR cardinality(heap) = 0` — heap empty. A *partial* heap
(non-empty but missing rows that should be there) slips through. The
algebraic Sub+Add merge can't see source rows that weren't in the heap
or in the delta — those rows exist in the source but never make it into
heap, so the heap silently goes stale.

### Fix

Split the UPDATE flow's recompute trigger into two paths in
`reflex_build_delta_sql`:

- **Non-top-K MIN/MAX** keeps the legacy ordering
  `Sub → recompute(if scalar IS NULL) → Add`. The recompute MUST run
  before Add because Sub leaves `scalar = NULL` and Add would otherwise
  compute `LEAST(NULL, d.scalar) = d.scalar`, swallowing any unchanged
  source row that should be the new MIN/MAX.

- **Top-K MIN/MAX** uses
  `Sub → topk_refresh → Add → forced recompute`. The new helper
  `build_min_max_recompute_sql_force_topk` skips the `null_check` for
  top-K columns — every affected group gets a source-scan recompute.

INSERT and DELETE flows are unchanged. Top-K still maintains heap
algebraically through INSERT (sorted-merge of `heap ∪ delta` truncated
to K) and DELETE (multiset_subtract; recompute only on heap underflow).

### Cost shape after fix

| Operation | Before fix | After fix |
|---|---|---|
| INSERT (top-K MIN/MAX) | sorted-merge, no recompute | unchanged |
| DELETE (top-K MIN/MAX, heap survives) | multiset_subtract, no recompute | unchanged |
| DELETE (top-K MIN/MAX, heap empties) | recompute affected groups | unchanged |
| **UPDATE (top-K MIN/MAX)** | algebraic merge, no recompute (often **wrong**) | algebraic merge **+** scoped recompute |

UPDATEs on top-K MIN/MAX IMVs now pay roughly the same source-scan cost
the 1.2.0 scoped-recompute path paid on retraction. INSERT-only and
DELETE-only workloads keep the full top-K speedup.

This is an explicit correctness-over-perf tradeoff. A smarter UPDATE
trigger that distinguishes "delta_old shrunk a heap-eligible element"
from "delta_old removed a non-heap row" could let UPDATE skip the
recompute in the second case — but it requires comparing delta_old
against the pre-state heap content, which the current MERGE codegen
doesn't materialise. Out of scope for this fix; tracked as a future
optimization in known-issues.

### Test coverage

- `pg_test_randomized_mutation_sequence` — caught the bug first
  (50 mutations × 3 groups, K=16 default). Now passes.
- `pg_test_topk_partial_heap_staleness_regression` — minimal
  isolated reproduction (5 rows / 1 group / K=2 / UPDATE then DELETE).
- `pg_test_topk_text_min_max`, `_date_`, `_timestamp_` — exercise
  UPDATE on non-NUMERIC top-K columns.

## Decision — auto-enable top-K

Once both correctness fixes were in, the original 1.3.0 reservation
about default-on top-K (operators couldn't predict the correctness
gaps) no longer applied. Reflex auto-detects MIN/MAX in the plan, so
the parameter is a *capability gate*, not an operator-knob:

- IMV has MIN/MAX  →  `topk = 16` applies (heap maintenance + scoped
  recompute on UPDATE; legacy scoped recompute on retraction).
- IMV is pure SUM/COUNT/AVG/BOOL_OR  →  `topk` is a no-op (no MIN/MAX
  intermediate column to attach a heap to).

Operators on append-only MIN/MAX workloads (e.g. "max temperature ever
seen") can opt out by calling the 6-arg overload with `topk = 0`. The
INSERT-overhead difference is small in absolute terms (~150 ms on a
10 K-row INSERT in the bench) and the audit's R3 retraction cliff is
closed automatically rather than by operator decision.

### Tests no longer regress

- `pg_test_correctness_text_min_max` — was the trigger that exposed
  the type-resolution bug; passes with auto-on + type-fix.
- `pg_test_randomized_mutation_sequence` — was the trigger that exposed
  the heap-staleness bug; passes with auto-on + recompute reorder.

### Validation

```
cargo test --features pg17 --lib  →  509/509 pass
cargo clippy --features pg17 --all-targets -- -D warnings  →  clean
```
