# 2026-07-25 — synchronous partition-sync orphan-collision probe is O(n²) per bulk sync, not O(n)

**Status: untreated.** Found by adversarial performance review of the full 1.11.1 batch, not
from a field report. Correctness is not affected — this is a pure cost-shape regression
introduced by the 2026-07-25 F3-heal fix (`ee68d64`/`209f781`, merged `1bed952`).

## The mechanism

`drop_bound_collision_orphan` (`src/partition.rs`) calls `list_partition_children` — a fresh
`pg_inherits` join plus a `pg_get_expr` deparse per child — **per node, twice** (once for the
intermediate namespace, once for the target namespace). The calling loop in
`reflex_sync_partitions_impl`'s per-node create path (`src/partition.rs`, around line 1281)
iterates every node in `nodes`, not just the ones actually missing (`CREATE ... IF NOT EXISTS`
already makes existing ones cheap no-ops, but the probe itself still runs before that check).

For a source root with n new leaves synced in one event-trigger firing (a bulk `ATTACH`/backfill
touching many partitions at once), this is 2n SPI round-trips, each deparsing on the order of n
sibling bounds — **O(n²)** deparse work where the probe's own inputs make O(n) achievable.

## Why it's fixable cheaply

`int_children`/`tgt_children`, computed once earlier in the same function
(`src/partition.rs`, around lines 1078-1083), already hold the full IMV-side tree as
`PartitionNode { parent_bare, bare_name, bound_expr }` — exactly the probe's inputs. A single
pre-loop pass building a `(parent_bare, bound_expr) -> child` map needs no invalidation during
the loop: `expected.contains(child.bare_name)` (the existing skip condition for a child the loop
is about to create) already excludes every child this same call is in the process of adding, so
the map built once up front stays valid for the whole loop.

## Reproduction / measurement

**Measured 2026-07-28 and confirmed quadratic.** `benchmarks/bench_partition_scaling.sh`
sweeps N = partition count with total data held constant, so any growth is per-child
overhead. PostgreSQL 17.7 (homebrew), `max_locks_per_transaction = 2048`, 20 000 source
rows spread over N leaves, median of 6 reps, tree already in sync (so this is the probe
cost alone — no node is actually created):

| metric | N=10 | N=25 | N=50 | N=100 | N=200 | fitted slope |
|---|---:|---:|---:|---:|---:|---:|
| `reflex_sync_partitions` (aggregate IMV, 2 probes/node) | 4.7 ms | 13.3 | 33.6 | 104.9 | 412.1 | **1.48** |
| `reflex_sync_partitions` (passthrough IMV, 1 probe/node) | 3.0 ms | 8.0 | 18.6 | 54.7 | 208.6 | **1.40** |

The local slope climbs to **1.97 over N=100→200** (3.93x the time for 2x the partitions) —
a linear term dominated by a quadratic one, exactly the predicted shape. Identical on
`2f8b786` (main), `integration/s1-batch` and `fix/swap-flattens-subpartitioned-child`, so
this is pre-existing and **not** introduced by the current batch.

Two independent confirmations that the cost is this probe and not the work itself:

1. **The aggregate IMV costs almost exactly 2x the passthrough at every N** (412.1 / 208.6
   at N=200; 2.0x, 1.9x, 1.8x, 1.9x, 2.0x across the sweep). An aggregate IMV has an
   intermediate, so the loop calls `drop_bound_collision_orphan` **twice** per node; a
   passthrough calls it **once**. Halving the number of probe calls halves the time, and
   both halves are quadratic.
2. **The lock footprint is exactly linear** — `2N + 25` locks held at the end of the sync,
   identical on all three builds. The sync touches a linear number of objects and spends
   quadratic time doing it, which is the signature of repeated catalog re-enumeration
   rather than of more work being done.

Extrapolating the quadratic term: ~0.41 s at N=200 becomes **~10 s at N=1000 and ~41 s at
N=2000**, per sync. Every `reflex_reconcile_partition` pays it in its pre-sync, and so does
every COMMIT-time flush of an attached partition — `attach_txn` measures 1.0 s at N=200
with a 100→200 local slope of 1.69.

## Fix direction

Hoist the `(parent, bound) -> child` lookup out of the per-node loop in
`reflex_sync_partitions_impl`, built once from `int_children`/`tgt_children` before the loop
starts, and have `drop_bound_collision_orphan` consult the map instead of re-querying
`list_partition_children` per node. Keep the existing exact-immediate-parent scoping
(`209f781`'s fix) — only the per-node re-fetch is the regression, not the scoping logic itself.
