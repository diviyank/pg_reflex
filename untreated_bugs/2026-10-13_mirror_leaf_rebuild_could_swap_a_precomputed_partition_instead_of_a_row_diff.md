# A mirror leaf rebuild could swap in a precomputed partition instead of applying a row diff — perf (improvement, not a defect)

Recorded at the owner's request during the 1.12.0 partition-swap cascade work (2026-10-13). Not a
bug: results are correct today. This is a performance direction to evaluate after 1.12.0, with
the measurements that motivate it.

## Current behaviour

When a source leaf of a partitioned source is replaced (the application's push: build a detached
table, COPY, DETACH old / ATTACH new / DROP old / RENAME), a partition-mirroring IMV
(`sop_forecast_view`-shaped: keyed passthrough mirroring LIST(plan) × RANGE(month)) recomputes
the matching partition of its own and applies it as a ROW DIFF against the old partition (since
1.11.5). The diff exists so that the IMV's readers receive the change as ordinary DML.

## Measurements (1.12.0 benchmark fixture, PostgreSQL 16, alone on the build server)

Fixture: 30 plans × 24 months × 3,000 rows per leaf = 2.16 M source rows, field names,
DEFERRED, real COMMIT (`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/p3-b-bench.rs`).

| Step | Wall time |
|---|---|
| Passthrough alone (no dependents), one leaf | 3.8 s |
| Passthrough alone (no dependents), 20 leaves of one plan | 56.5 s |
| Whole chain, one leaf, COMMIT (1.12.0) | ~24–25 s |
| Whole chain, 20 leaves of one plan, COMMIT (1.12.0) | ~80 s |

So the leaf rebuild of the mirroring IMV itself costs about 2.8 s per 3,000-row leaf, and it
dominates a 20-leaf push (56.5 s of ~80 s). The 1.12.0 cascade work did not touch this part.

## Idea

Build the new mirror partition detached (bulk `INSERT … SELECT` from the IMV's query restricted to
the leaf's bound, indexes built after the load, no per-row comparison), then DETACH the old mirror
partition and ATTACH the new one — the same pattern the application uses for the source.

For a DROP of a source partition (plan deletion), the equivalent is dropping or emptying the
reader's partition directly, with no row work.

## Why it is not done today (what must be proven first)

A swap produces no row delta. Readers of the IMV that rely on its row changes would never see
what changed. A swap is only safe when either:

1. every reader of that partition is itself refreshed by partition for the same key (it mirrors
   the same plan), which needs the same alignment proof the 1.12.0 cascade uses for scoping
   (`src/cascade.rs`, `partitions_follow_source` / the scope proof); or
2. the diff between old and new partition is computed anyway — which brings back much of the
   cost the swap is meant to remove.

For the field chain (`forecast_analysis_view` and its sub-IMVs partitioned by `dem_plan_id`)
condition 1 plausibly holds, but that is not established.

## Open questions

- How much of the 2.8 s per leaf is the diff itself versus computing the IMV's query for the leaf
  (if the query dominates, a swap saves little). Measure first: time the leaf's query alone, the
  diff alone, and a detached build + ATTACH alone on the fixture.
- Index build cost on the detached partition versus incremental index maintenance during the diff.
- Locking: ATTACH takes a SHARE UPDATE EXCLUSIVE lock on the IMV's parent; DETACH (non-concurrent)
  takes ACCESS EXCLUSIVE. Readers of the IMV would block for the swap's duration (the diff path
  takes only row locks).
- DEFERRED readers and the COMMIT-time flush ordering.
- Interaction with the readers that ignore the IMV (they are refreshed by the cascade anyway).

## Fix direction (hypothesis, untested)

A spike: on the benchmark fixture, implement the detached build + swap for the mirroring IMV's
leaf only when every reader of that partition is proven to mirror the same key, and fall back to
the row diff otherwise. Compare wall time and lock waits against the current path; keep the
change only if it is a clear win.
