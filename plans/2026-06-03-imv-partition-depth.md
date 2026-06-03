# IMV partition depth ≤ source partition depth — design

**Date:** 2026-06-03
**Status:** approved design, pre-implementation
**Builds on:** `plans/sub_partitioning.md` (1.8.1 multi-level source mirroring), `plans/partitioning_2.md`, `plans/partitioning_3.md`

## Problem

1.8.1 mirrors the *full* source partition hierarchy onto the IMV. Every code path
derives the IMV's partition depth from `list_partition_tree(anchor)` — the complete
source tree — and validation demands that **every** partition-key column at **every**
source level be a bare projected output column in the IMV's unique key.

This rejects legitimate IMVs whose query shape cannot expose a deeper level's column as
a bare reference. The motivating case, `forecast_analysis_view`:

```
ERROR: [reflex-unsupported] partition_by validation failed — partition key column
'order_date' (a partition level of source 'omc.sop_forecast_view') is not a bare
projected output column in the IMV's unique key. Add it to the SELECT list and
unique_columns.
```

The IMV declares `partition_by: [dem_plan_id]` and projects
`COALESCE(forecast.order_date, history_sales.order_date) AS order_date` — a `FULL JOIN`
coalesced key. The anchor `sales_simulation` is `LIST(dem_plan_id) → RANGE(order_date)`.
`order_date` is a source RANGE sub-level, but it does **not** exist as a single bare
source reference at the output, so the all-levels rule rejects creation. The user asked
only to partition by `dem_plan_id`; the deeper level was imposed by the unconditional
full-tree walk.

There is currently **no notion of an IMV being shallower than its source**. That is the
gap this feature closes.

## Goal

Allow the IMV's partition depth to be **less than or equal to** the source's, chosen
either explicitly (`partition_by`) or by automatic pruning, and keep the capture /
sync / reconcile / snapshot paths correct at the shallower depth.

Priority order (per `CLAUDE.md`): **correctness first** — a shallower IMV must never
silently drift or re-deepen; **simplicity second** — one consistent truncation lever,
no per-call-site special-casing; **performance third** — a coarser IMV refreshes at a
coarser (heavier but atomic) granularity, by design.

## Core concept: the IMV partition level list

Introduce an ordered list of source partition-key columns the IMV mirrors, root-first —
**the level list**. Its length is the IMV's `mirror_depth`. The source tree is truncated
to this depth everywhere it is consumed.

### Depth resolution (at `create_reflex_ivm`)

- **Explicit `partition_by`** → authoritative. The level list *is* the declared columns.
  They must match the anchor's partition-key columns top-down, in order:
  - `[dem_plan_id]` → matches level 1 → `mirror_depth = 1`.
  - `[dem_plan_id, order_date]` → matches levels 1+2 → `mirror_depth = 2`.
  - A declared column that does not match the anchor's level-`k` partition key, or whose
    level cannot be satisfied (see "satisfiability" below), → **reject** with a per-level
    error. The user asked for it explicitly, so we do not silently drop it.
- **Omitted `partition_by`** (auto-mirror) → walk the source tree top-down; keep a level
  while its partition column is **satisfiable**; **stop at the first non-satisfiable
  level**. The level list is the satisfiable prefix — possibly the full hierarchy
  (preserving 1.8.1 behavior), possibly just the root.

### Satisfiability of a level

A level (its partition-key column `c`) is **satisfiable** when `c` is a **bare projected
output column** of the IMV present in the resolved unique key / GROUP-BY set — i.e. the
same rule 1.8.1 applied to every level, now applied selectively. "Bare" means a direct
column reference projected under its own name, not a computed expression (`COALESCE(...)`,
arithmetic) and not renamed. This is required both for PostgreSQL's
unique-index-on-partitioned-table rule and for reconcile resolution to map a partition
back to a base-query predicate.

For `forecast_analysis_view`: explicit `[dem_plan_id]` → level list `[dem_plan_id]`,
depth 1. The `order_date` RANGE sub-level is **never inspected**, so the
`COALESCE(...) AS order_date` projection is irrelevant. Creation succeeds as a
single-level `LIST(dem_plan_id)` IMV.

## Section 1 — Truncating the source tree

A single new primitive in `src/partition.rs`:

```rust
fn truncate_partition_tree(nodes: Vec<PartitionNode>, mirror_depth: usize) -> Vec<PartitionNode>
```

- Keeps nodes whose tree-depth ≤ `mirror_depth`.
- **Demotes** the nodes at exactly tree-depth `mirror_depth` to leaves: clears
  `sub_strategy` and `sub_columns` so the DDL builder emits no `PARTITION BY` suffix.
- Drops all nodes deeper than `mirror_depth`.

A `LIST(dem_plan_id) → RANGE(order_date)` source truncated to depth 1 yields plain
`imv_p_172 PARTITION OF imv_root FOR VALUES IN ('172')` leaves, each holding all of 172's
months. Codegen needs no change: `build_partition_child_ddl_pair` (`src/partition.rs:348`)
already omits the suffix when `sub_columns` is empty — only the **tree fed to it** changes.

Every site that consumes `list_partition_tree(anchor)` for mirroring / sync / snapshot
wraps the result in `truncate_partition_tree(..., mirror_depth)`. This is the single
consistent lever guaranteeing create, sync, and snapshot agree on shape, so sync never
re-deepens a deliberately-shallow IMV.

Tree-depth is computed from the `pg_inherits` recursion already present in
`list_partition_tree` (the `depth` column in the recursive CTE, `src/partition.rs:192`);
`PartitionNode` gains a `depth: usize` field (or the truncation derives it from the
`parent_bare` chain rooted at the anchor).

## Section 2 — Catalog / metadata

The IMV's depth must be persisted, because today `partition_columns` holds only the
**root** column (`[dem_plan_id]`, length 1) even for a full multi-level auto-mirror — so
its length cannot be trusted as the depth.

**Decision:** repurpose `partition_columns` in `public.__reflex_ivm_reference` to carry
the full **ordered level-column list** (`[dem_plan_id]` or `[dem_plan_id, order_date]`),
and derive `mirror_depth = partition_columns.len()`. `resolve_partitioning` populates it
with the resolved level list (declared columns, or the auto-pruned satisfiable prefix);
for an auto full mirror this now lists *all* source level columns, not just the root.

`reflex_sync_partitions_impl` (`src/partition.rs:823`) reads `partition_columns`, derives
`mirror_depth = partition_columns.len()`, and passes it into `truncate_partition_tree`.

**No migration / backfill for existing IMVs.** Pre-1.8.1-era rows store a length-1
`partition_columns` (root column only) whose length would now be misread as `mirror_depth
= 1`, truncating a previously full-depth IMV on its next sync. We do **not** handle this
in code. Instead, this is a **breaking representation change**: IMVs created before this
version must be **dropped and recreated** to pick up the new `partition_columns` semantics.
The package emits a startup / sync notice (see Upgrade notice below) so the requirement is
visible; no self-healing is attempted.

### Upgrade notice

`CHANGELOG.md` and `docs/concepts/internals.md` state plainly:

> **Breaking (partitioned IMVs):** `partition_columns` now records the full ordered
> partition **level list**, not just the root column. IMVs created on an earlier version
> must be **recreated** (drop + `create_reflex_ivm`); otherwise a sync would truncate them
> to a single level. Unpartitioned IMVs are unaffected.

`reflex_sync_partitions` additionally raises a one-line `warning!` when it encounters a
partitioned IMV whose stored `partition_columns` length is **shorter** than its
materialized target-tree depth — the signature of an un-recreated legacy row — pointing the
user at the recreate requirement rather than silently truncating without a word. (It still
proceeds per the new semantics; the warning is advisory, not a backfill.)

## Section 3 — Reconcile / capture resolution

The only genuinely new logic beyond truncation. When the IMV is shallower than the source,
a dirty or swapped source node at tree-depth > `mirror_depth` (e.g. a month leaf
`sales_simulation_p_172_2025_03`) has **no 1:1 IMV leaf**.

**Resolution rule:** map a source node at depth `d` to the IMV node at
`min(d, mirror_depth)` by walking **up** the source node's ancestor chain to its
depth-`mirror_depth` ancestor, then mapping that ancestor's bare name to the IMV node name
via the existing `target_child_name` / `intermediate_child_name` scheme. Multiple deeper
source nodes under the same depth-`mirror_depth` ancestor **dedupe to a single** IMV-leaf
refill.

The swap-fill itself is **unchanged**: `pg_get_partition_constraintdef` on the IMV leaf
`<view>_p_172` returns `dem_plan_id = 172`, and the existing
`SELECT * FROM (base_query) __src WHERE (<constraint_def>)` refills the whole partition
correctly (recompute of all of 172's rows from the base query — always correct, heavier
than a single month).

Touched:
- `reflex_reconcile_partition` resolution (`src/partition.rs:1420`, `:1437`) — expand a
  source partition to its source leaves, then map each up to its depth-`mirror_depth` IMV
  ancestor and dedupe, instead of assuming a 1:1 IMV leaf.
- Snapshot / flush oid-diff (`src/partition.rs:1595`) — the source snapshot still tracks
  the source's full recursive **leaf set** (deepest leaves); a changed deep leaf resolves
  **up** to the IMV depth-`mirror_depth` node before swap-fill. No change to what is
  snapshotted on the *source* side; the change is purely in mapping a dirty source leaf to
  the IMV node to refill.
- Audit drift-check (`src/audit/checks_b_drift.rs`) — compare the source recursive leaf
  set/row-counts **aggregated up to `mirror_depth`** against the IMV's tree, so a
  deliberately-shallow IMV is not flagged as drift for missing the source's deeper levels.

### Capture correctness argument

A source month swap (DETACH old / ATTACH new under `dem_plan_id=172`) changes a source
leaf oid. Flush oid-diffs the source leaf set, detects the changed leaf, resolves it up to
IMV node `<view>_p_172`, and swap-fills that node from the base query — recomputing all of
172. The result is identical to a from-scratch recompute of 172's slice, so
`IMV EXCEPT source-recompute = ∅` holds. Coarser than a month-granular swap, never
incorrect.

## Section 4 — Validation

`resolve_partitioning` (`src/create_ivm/mod.rs:444`) changes:

- **Explicit branch:** after matching declared columns to the anchor's level columns
  top-down, validate satisfiability **only for the declared levels**. The unconditional
  walk over *all* source sub-levels (`all_sub_cols`, `src/create_ivm/mod.rs:550–578`) is
  replaced by a walk bounded at `mirror_depth = partition_by.len()`. A declared level that
  is unsatisfiable → reject with a per-level message naming the level and column.
- **Auto branch:** walk the source tree top-down, accumulating satisfiable levels; stop at
  the first non-satisfiable one. Emit an `info!` when pruning occurs ("source level N
  (`order_date`) is not a bare projected column; mirroring at depth N-1"). Never reject on
  this basis.

The resolved level list is written to `partition_columns` (Section 2).

## Section 5 — Passthrough vs aggregate

Unchanged from 1.8.1: passthrough IMVs mirror only the target tree; aggregate IMVs mirror
both intermediate and target trees identically. Truncation applies to **both** trees at the
same `mirror_depth` (the intermediate and target stay shape-identical). The
`has_intermediate` guard (`src/partition.rs:860`) generalizes unchanged.

## Section 6 — Testing

**Unit (pure Rust):**
- `truncate_partition_tree`: depth bounds, leaf demotion (`sub_strategy`/`sub_columns`
  cleared at the boundary), deeper-node drop, depth-0 / depth-≥source-depth (no-op) edges.
- Depth resolution: explicit honored; explicit-unsatisfiable rejects with per-level
  message; explicit column not matching anchor level rejects; auto-prune stops at the first
  non-satisfiable level; auto full-mirror unchanged when all levels satisfiable.
- Reconcile up-mapping: deep source leaf → depth-`mirror_depth` IMV node; multiple deep
  leaves under one ancestor dedupe to one IMV-leaf refill.

**Integration (`src/tests/pg_test_subpartition.rs`):**
- Create the motivating shape (sub-partitioned source, `partition_by:[dem_plan_id]`,
  coalesced `order_date`) → single-level `LIST(dem_plan_id)` IMV, data correct.
- Source month swap → whole-`dem_plan_id` refill (only that IMV leaf's relfilenode
  changes), data correct.
- Attach-new-month under existing `dem_plan_id` → **no** new IMV leaf (collapses into the
  existing one), data correct.
- `reflex_sync_partitions` does **not** re-deepen the IMV (target tree stays depth 1).
- `reflex_sync_partitions` warns (does not error) when `partition_columns` is shorter than
  the materialized target-tree depth (legacy un-recreated row signature).
- Audit drift-check clean on the deliberately-shallow IMV.
- Explicit `partition_by:[dem_plan_id, order_date]` on a shape where `order_date` *is*
  bare-projected → two-level IMV (opt-in finer granularity still works).

**Differential fuzz (`src/tests/pg_test_fuzz.rs` harness):**
- Add a shallow-IMV-on-deep-source case to the random attach/detach/swap + flush sequence;
  assert `IMV EXCEPT source-recompute = ∅` after each flush.

**CI gates:** `cargo pgrx check && cargo pgrx test && cargo clippy && cargo fmt --check` —
all green.

## User-facing contract

Finer sub-partitioning is **opt-in via projection shape + `partition_by`**; the package
falls back to the **coarsest correct level it can actually satisfy**:

- Project a sub-partition column as a **bare column** and declare it in `partition_by`
  (or let auto-mirror pick it up) → finer-granularity, lighter per-event refresh.
- Don't, or can't (e.g. `FULL JOIN`-coalesced keys like `forecast_analysis_view`'s
  `order_date`) → correct-but-heavier macro refresh at the coarsest satisfiable level.

The perf/granularity tradeoff sits where the user can express it; correctness holds
regardless.

## Considered & rejected

- **Projectability-only (ignore `partition_by` depth):** always walk the full tree and
  prune any non-satisfiable level for both explicit and auto. Simpler single rule, but
  `partition_by` could no longer *force* coarseness when columns happen to be projectable,
  and gives the user less control. Rejected in favor of `partition_by`-authoritative.
- **Relax validation only, leave mirroring full-depth:** creation would succeed, but sync
  would rebuild the sub-level and the swap path would assume month-leaves that don't exist
  — silent drift. Violates correctness-first.
- **Per-call-site depth handling instead of one truncation primitive:** scatters the depth
  invariant across create / sync / reconcile / snapshot, inviting one path to disagree and
  re-deepen. Rejected for the single `truncate_partition_tree` lever.

## Files to touch

- `src/partition.rs` — `truncate_partition_tree` primitive; `PartitionNode.depth` (or
  ancestor-chain depth derivation); depth-aware `reflex_sync_partitions_impl`
  (derive `mirror_depth` from `partition_columns` + legacy-row warning); reconcile
  up-mapping resolution; snapshot/flush up-mapping.
- `src/create_ivm/mod.rs` — depth resolution in `resolve_partitioning` (explicit-honored +
  auto-prune); bounded validation; write resolved level list to `partition_columns`;
  truncate at create-time mirroring (`:829`, `:985`).
- `src/audit/checks_b_drift.rs` — compare source leaf set aggregated to `mirror_depth`.
- `src/tests/pg_test_subpartition.rs`, `src/tests/pg_test_fuzz.rs`,
  `src/tests/unit_partition.rs` (or equivalent unit module).
- `CHANGELOG.md`; `docs/concepts/internals.md` (Partitioning section); version bump.

## Phasing

Each phase independently committable + testable:

- **Phase 1 — Depth resolution + truncation + create-time mirroring + validation**
  (Sections 1, 4, 5; create-time half of Section 2). The motivating IMV creates as a
  single-level `LIST(dem_plan_id)` table. Tests: unit truncation/resolution, create-shape
  integration.
- **Phase 2 — Depth-aware sync** (Section 2 sync half). Sync derives `mirror_depth` from
  `partition_columns` and respects it; warns on the legacy-row signature. Tests:
  no-re-deepen, legacy-row warning.
- **Phase 3 — Reconcile / snapshot / flush up-mapping** (Section 3). Source-side swaps at
  any depth refill the correct IMV node. Tests: leaf swap, attach-new-month collapse,
  end-to-end flush.
- **Phase 4 — Audit drift-check + differential fuzz** (Sections 3, 6). Correctness backstop
  at the shallower depth. Tests: drift clean, fuzz oracle.

Phase 1 unblocks creation; Phases 2–3 deliver correct maintenance; Phase 4 is the safety
layer.
