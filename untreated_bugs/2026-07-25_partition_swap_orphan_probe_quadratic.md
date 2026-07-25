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

Not yet measured. At the scale of the existing test fixture (~48 leaves, per the co-partitioned
DP test family) the absolute cost is small and this is why it wasn't caught by that suite. The
concern is daily/weekly-partitioned production roots with hundreds to low-thousands of leaves
synced in one bulk backfill — the same order of magnitude where the empty-subpartition-skip
optimization (`journal`/memory: 48-leaf DP attach 898s→51s) mattered.

## Fix direction

Hoist the `(parent, bound) -> child` lookup out of the per-node loop in
`reflex_sync_partitions_impl`, built once from `int_children`/`tgt_children` before the loop
starts, and have `drop_bound_collision_orphan` consult the map instead of re-querying
`list_partition_children` per node. Keep the existing exact-immediate-parent scoping
(`209f781`'s fix) — only the per-node re-fetch is the regression, not the scoping logic itself.
