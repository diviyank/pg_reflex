# Top-K default decision + non-NUMERIC type fix

**Date**: 2026-04-26
**Linkage**: `journal/audit-production-readiness.md` Item 5 follow-ups, `journal/2026-04-25_topk_landed.md` "known limitations" §1.

## Question raised

Top-K (1.3.0) ships opt-in via the 6-arg `create_reflex_ivm(name, sql, …, topk)`.
The 5-arg overload defaults `topk = None`. With no users to migrate, why
should it stay optional?

## Two real costs against default-on

### Cost 1 — INSERT slowdown for append-only MIN/MAX

Bench data from `journal/2026-04-25_topk_landed.md`:

| Op | IMV (no topk) | IMV (`topk=16`) |
|---|---:|---:|
| INSERT 10K | 109 ms | 264 ms |

≈2.4× cost on INSERT. For a workload that never retracts (audit logs,
event streams, "max temperature ever recorded"), every row pays array
sort-merge cost for a benefit that never lands.

### Cost 2 — UPDATE-heap-staleness correctness gap

The deciding factor. Default-on was tried; `pg_test_randomized_mutation_sequence`
caught it.

**Reproduction (K=2 for clarity, same shape applies for K=16 on wide groups)**:

```text
Source pre  = {1, 2, 3}.   K=2.   Heap = [1, 2].   scalar = 1.
UPDATE val=1 → val=10.
  delta_old = [1].   delta_new = [10].
  Sub:           heap = [2].             scalar = NULL.
  topk_refresh:  scalar = heap[1] = 2.
  recompute:     scalar=2 (not NULL), heap=[2] (not empty) → does NOT fire.
  Add:           heap = top-2([2] ∪ [10]) = [2, 10].   scalar = LEAST(2, 10) = 2.

Source post = {2, 3, 10}.   true MIN = 2.   IMV scalar = 2.   ✓ at this point.

DELETE val=2 (a SUBSEQUENT statement).
  delta_old = [2].
  Sub:           heap = subtract([2, 10], [2]) = [10].   scalar = NULL.
  topk_refresh:  scalar = heap[1] = 10.

Source post = {3, 10}.   true MIN = 3.   IMV scalar = 10.   ❌
```

The heap is *stale* after the UPDATE: it holds `[2, 10]` instead of the
true top-2 `[2, 3]`. The unchanged source row `val=3` was never in the
heap (it was rank 3 in the original 3-element source, K=2 displaced it).
On the next DELETE, `heap[1]` is read as authoritative and produces a
wrong scalar.

The 1.3.0 recompute trigger fires only on `scalar IS NULL OR heap IS NULL OR
cardinality(heap) = 0` — i.e. **empty** heap. A *partial* heap that's
internally consistent but missing source rows that *should* be promoted
slips through.

### Why the existing fuzz test passes

`pg_test_topk_fuzz_min` (1.3.0) uses K=8 and 30 random INSERT/DELETE/UPDATE
on ~50 rows / 4 groups (≈12 per group). Two reasons it doesn't trigger:

1. K=8 vs ~12 per group means heap usually holds *most* of the source —
   stale-heap failure mode requires heap size ≪ source size.
2. Random `setseed(0.42)` happens to avoid the specific UPDATE-then-DELETE
   sequence that exposes the bug.

`pg_test_randomized_mutation_sequence` (pre-existing, non-topk) hits 50
rows / 3 groups (~17 per group), K=16 default-on, which puts heap size
≈ source size for every group with a 1-row margin. Two-step retraction
patterns reliably fail.

## Decision

**Keep top-K opt-in until the recompute-trigger condition is widened to
catch partial-heap staleness.** The required fix is non-trivial:

- The cleanest signal is `cardinality(heap) < LEAST(K, __ivm_count)`,
  which fires whenever the heap is missing rows it should have.
- That condition fires after *every* retraction on wide groups —
  reintroducing the source-scan cliff top-K was meant to remove.
- A smarter trigger needs to compare `delta_old`'s contribution against
  `heap`'s pre-state to decide whether the retraction left a "hole" that
  needs filling. That logic doesn't exist today.

Until that lands, the failure mode is hidden behind opt-in. Operators
who enable top-K accept the limitation explicitly.

This is documented as a 1.3.x known issue, not a regression — the bug
was present in 1.3.0 release, just not exposed because top-K was
opt-in and the bench/fuzz patterns avoided the trigger.

## Side-fix — MIN/MAX intermediate `pg_type` propagation

While running the default-on experiment, `pg_test_correctness_text_min_max`
failed with:

```
COALESCE could not convert type numeric[] to text[]
```

**Root cause**: `aggregation::plan_aggregation_inner` hardcodes
`pg_type: "NUMERIC"` on every MIN/MAX `IntermediateColumn`. The schema
builder (`schema_builder.rs:54`) special-cases this and resolves the
actual source-column type via `resolve_column_type` for DDL purposes.
But the trigger MERGE codegen (`trigger.rs:139,156`) reads `ic.pg_type`
directly to emit:

```sql
COALESCE(d."__min_x_topk", '{}'::NUMERIC[])
```

When the column is `TEXT[]`, this typecast fails. The schema column was
built correctly, the codegen was not.

**Fix**: in `create_ivm.rs`, after `query_column_types_from_catalog` +
`augment_column_types_from_query`, walk `plan.intermediate_columns` and
replace any MIN/MAX `pg_type == "NUMERIC"` with the resolved source-arg
type. Mirrors the existing SUM → DOUBLE PRECISION pass on lines 977-984.

This is **independent of the topk default decision** — it's a real bug
on opt-in top-K with non-NUMERIC source columns, not an artifact of the
default change. Lands as part of this commit.

## Test coverage added

`pg_test_topk_text_min_max`, `pg_test_topk_date_min_max`,
`pg_test_topk_timestamp_min_max` — INSERT / DELETE / UPDATE / heap-underflow
each on K=4 with non-NUMERIC source columns, EXCEPT ALL oracle after
every step. Closes the audit's "DATE / TIMESTAMP / TEXT inherit through
the same path and 'should work' but are unverified" gap from
`2026-04-25_topk_landed.md` §1.

## Validation

```
cargo test --features pg17 --lib  →  508/508 pass (505 + 3 new)
cargo clippy --features pg17 --all-targets -- -D warnings  →  clean
```
