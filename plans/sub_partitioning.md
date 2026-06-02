# Sub-partition (multi-level) source support — design

**Date:** 2026-06-02
**Status:** approved design, pre-implementation
**Builds on:** `plans/partitioning_2.md`, `plans/partitioning_3.md` (single-level partition mirroring, atomic DETACH/ATTACH swap, per-partition dispatch)

## Problem

The main production IMV `sop_forecast_view` reads from `yse.sales_simulation`, which is
**multi-level partitioned**: `LIST (dem_plan_id)` at level 1, and each LIST child is itself
`RANGE (order_date)` at level 2. Today pg_reflex mirrors only the **first** partition level
(`list_partition_children` walks one level of `pg_inherits`), so the RANGE sub-level is invisible
to the IMV.

Two gaps follow:

1. **Structural** — the IMV target can mirror only the `dem_plan_id` level. Reconcile cannot
   operate at the finer `order_date` (month) granularity, so a single-month change forces a
   whole-`dem_plan_id` rebuild (or a non-atomic in-partition `DELETE`+`INSERT`).
2. **Capture** — the production write path is **partition swap** (`DETACH` old / `ATTACH` new),
   at *both* the `dem_plan_id` level and the `order_date` (month) level. `DETACH`/`ATTACH` are
   **DDL**: they fire **no** `INSERT`/`UPDATE`/`DELETE`/`TRUNCATE` trigger, and an `ATTACH`ed
   partition's rows pre-exist any trigger we could place on it. So pg_reflex's trigger model
   captures **nothing** for swaps — a correctness gap (the package's #1 priority).

This was confirmed during brainstorming:
- Write path: *"the most efficient method would be detach old (sub-)partitions, and attach new ones."*
- Swap granularity: **both** — sometimes a whole `dem_plan_id`, sometimes individual months.

## Why triggers cannot capture swaps (settled)

Triggers — at *any* partition level — cannot capture an `ATTACH`/`DETACH`:

1. `ATTACH`/`DETACH` are DDL; no DML trigger fires.
2. An `ATTACH`ed partition's rows were loaded into the standalone staging table **before** it
   became a partition, so no `INSERT` fires retroactively even if a trigger is installed at attach
   time.

Recursively installing DML triggers on attached sub-partitions therefore captures nothing for the
swap path. It would only ever cover ordinary row-level DML issued *directly against a leaf after it
is attached* — a vector the production pipeline does not use. The capture mechanism for swaps must
be a **reconcile**, driven by a PostgreSQL **event trigger** on the DDL — not a DML trigger.

For completeness, the full write-vector coverage map:

| Write vector to `sales_simulation`           | Captured by                                              | Per-sub-partition trigger? |
|----------------------------------------------|----------------------------------------------------------|----------------------------|
| `DETACH`/`ATTACH` swap (main path)           | Event trigger → flush → partition-scoped reconcile       | No                         |
| Ordinary DML through the **root**            | Existing root statement trigger (transition table covers routed rows, incl. new sub-partitions) | No |
| Ordinary DML **directly against a leaf**     | Nothing (out of scope; audit drift-check is the backstop)| No                         |

A newly-attached sub-partition needs **zero** trigger management: root-routed DML is covered by the
single root trigger, and swaps are covered by the event-trigger→flush path below.

## Approach (chosen)

**Mirror the full source partition hierarchy** onto the IMV, and capture swaps via an event trigger
that enqueues the affected source root, resolved at flush time by an oid-diff against a stored
snapshot. Rejected alternatives:

- *Single-level IMV + in-partition `DELETE`+`INSERT` for month changes* — non-atomic, blocking,
  dead-tuple bloat exactly where the swap advantage is wanted.
- *Recursive DML triggers on attached partitions* — captures nothing for swaps (see above).
- *Event trigger that reconciles inline in the DDL txn* — makes the user's `ATTACH` statement pay
  the rebuild cost and hold locks longer; enqueue→flush bounds the DDL-txn cost.

## Section 1 — Recursive hierarchy mirroring + codegen + validation

### Introspection (`src/partition.rs`)

Replace the single-level `list_partition_children` (at create + sync time) with a recursive walk
returning a **tree**:

```rust
struct PartitionNode {
    bare_name: String,            // source node relname, e.g. sales_simulation_p_172_2025_03
    parent_bare: String,          // immediate parent relname (root's parent = anchor root)
    bound_expr: String,           // FOR VALUES … relative to immediate parent
    sub_strategy: Option<String>, // Some("RANGE") if this node is itself partitioned; None = leaf
    sub_columns: Vec<String>,     // sub-partition key cols (for the PARTITION BY suffix)
}
```

Walked top-down via recursive `pg_inherits` from the anchor root. `list_partition_children` is kept
as the "immediate children" primitive the recursion calls per node.

### DDL codegen

`build_partition_child_ddl_pair` gains a sub-`PARTITION BY` suffix for internal nodes:

- **Internal node** → `CREATE TABLE imv_p_172 PARTITION OF imv_root FOR VALUES IN ('172') PARTITION BY RANGE (order_date)`.
  Internal partitioned tables are LOGGED (storage mode applies only at leaves).
- **Leaf** → `CREATE [UNLOGGED] TABLE imv_p_172_2025_03 PARTITION OF imv_p_172 FOR VALUES FROM (…) TO (…)`.

Nodes are emitted **top-down** (parent before child).

### Naming (existing scheme, applied per node)

IMV node name = `<bare_view>_<source_node_bare>` (target) and
`__reflex_intermediate_<bare_view>_<source_node_bare>` (intermediate). Source node bare-names are
already globally unique across levels (`sales_simulation_p_172` vs `sales_simulation_p_172_2025_03`),
so the existing scheme gives a clean 1:1 mapping at every level with no collisions.

### Validation (extends `plans/partitioning_3.md` §2 to all levels)

At `create_reflex_ivm`:

1. **Every** partition-key column at **every** level (`dem_plan_id`, `order_date`) must appear as a
   **bare projected output column** of the IMV with a matching name (not computed, not renamed) —
   required for PG's unique-index rule *and* for the swap-fill constraint substitution to resolve.
2. Aggregate IMVs: each level's cols ⊆ GROUP BY (passthrough: ⊆ unique key). For `sop_forecast_view`,
   `dem_plan_id` + `order_date` ⊆ `[dem_plan_id, product_id, location_id, order_date]` ✓.
3. Reject HASH at any level; reject missing/computed level columns with an error naming the level.

### Passthrough vs aggregate

`sop_forecast_view` is passthrough → only the **target** tree is mirrored (no intermediate).
Aggregate IMVs mirror **both** intermediate and target trees identically. The recursion applies to
whichever tables exist (the existing `has_intermediate` guard generalizes).

## Section 2 — Capture model (event trigger → enqueue → flush)

### Cascade is code-driven, not event-driven

`reflex_reconcile_partition_impl` (`src/partition.rs:1031`) already loops over `graph_child` and
calls `reflex_reconcile_partition(dep, keys)` / `reflex_reconcile(dep)` for each dependent. So
dependent IMV partitions **are** reconciled, at per-partition granularity, through that programmatic
cascade. The event trigger therefore watches **source** tables only, as the *entry point*. Reacting
to pg_reflex's own IMV `ATTACH`/`DETACH` (from the Phase-A swap) is **forbidden** — it would
re-fire and race the programmatic cascade. Ignoring reflex-owned swaps is required, not just safe.

### DETACH = DROP the IMV partition (not DELETE)

Each IMV partition maps 1:1 to a source partition, and the partition key ⊆ unique-key/GROUP-BY
(whole groups live in one partition). A source-side removal therefore becomes a **`DROP`** of the
matching IMV partition — an O(1) catalog op, no row scan — which recursive
`reflex_sync_partitions(drop_orphans=true)` already performs for orphaned IMV children.

### Snapshot oid-diff resolves swap / attach / detach at flush time

Deciding attach-new vs detach-remove vs same-bound-swap at *event-trigger* time is fiddly, and a
same-bound swap is structurally invisible (partition set unchanged). DDL-text parsing is too fragile
for a correctness-first package. Instead, decide nothing at event-trigger time — mark the source
root dirty (the root is the one thing the event-trigger SRF reports reliably) and resolve at flush
via an **oid-diff** against a stored snapshot.

`__reflex_source_partition_snapshot(source_root, child_name, child_oid, bound)` — seeded at
`create_reflex_ivm` (anchor's full recursive leaf set), refreshed after every flush/sync/reconcile.
At flush, for each dirty source root, re-list its full recursive **leaf set** and diff by
`(child_name, oid)`:

| Diff result                 | Meaning                          | Action on IMV                              |
|-----------------------------|----------------------------------|--------------------------------------------|
| name present, **oid changed** | same-bound **swap** (detach+attach) | swap-fill the matching IMV leaf (Phase-A) |
| name new                    | **attach-new**                   | sync-create IMV leaf, then swap-fill       |
| name gone                   | **detach / remove**              | **DROP** the matching IMV leaf             |
| unchanged                   | no-op                            | skip                                       |

The **oid change** makes a same-bound swap detectable without parsing — the replacement is a
different relation even with an identical bound. After processing, the snapshot is refreshed.

### Pieces

- **Event trigger** `__reflex_partition_ddl_watch` on `ddl_command_end`: inspect
  `pg_event_trigger_ddl_commands()` for `ALTER TABLE`; resolve the affected relation's partition
  **root**; if that root is in some IMV's `depends_on` **and** is not reflex-owned (`__reflex_*` /
  an IMV target), enqueue the root into `__reflex_partition_pending`. No rebuild in the DDL txn.
- **Flush** `reflex_flush_partitions([source_root])`: per dirty root, oid-diff the live recursive
  leaf set vs snapshot → swap-fill / create / DROP the affected IMV leaves → cascade via
  `graph_child` → refresh snapshot → clear pending. The pipeline calls it once after its batch of
  swaps (so intermediate states are not reconciled); optionally auto-wired into the existing
  DEFERRED commit hook in a follow-up.
- **`reflex_reconcile_partition(view, source_partition)`** — the manual building block the flush
  calls, and a hand-callable fallback.

### Documented capture limitation + backstop

The oid-diff detects "attach a freshly-built table" (the stated production pattern) and
"detach/remove." It does **not** detect `detach → modify-in-place → reattach the *same* table`
(same oid); that pattern requires an explicit `reflex_reconcile_partition` call. As a correctness
backstop for *any* missed vector, an **audit drift-check** (`src/audit/checks_b_drift.rs`) compares
the source recursive leaf set + per-leaf row counts against the IMV's, flagging divergence so silent
drift is always *detectable*.

## Section 3 — Reconcile & leaf-swap mechanics

The swap engine already works at leaf granularity, **unchanged**. `execute_partition_swap_for_child`
(`src/partition.rs:1095`) fills via `SELECT * FROM (base_query) __src WHERE (<constraint_def>)`, and
`pg_get_partition_constraintdef` on a sub-leaf returns the **full ancestral predicate**
(`dem_plan_id = 172 AND order_date >= … AND order_date < …`), so an IMV leaf fills with exactly its
rows. The only new work is **resolution**: mapping a source partition (any level) to the IMV node(s)
to swap.

**Unified rule: reconcile always operates at leaf granularity.** A source-partition input expands to
its source **leaves**; a leaf expands to itself. Then sync the IMV subtree (create/drop to match)
and swap-fill each corresponding IMV leaf.

- `reflex_reconcile_partition(view, source_partition := 'sales_simulation_p_172_2025_03')` →
  one leaf → swap one IMV leaf.
- `reflex_reconcile_partition(view, source_partition := 'sales_simulation_p_172')` (internal) →
  expand to all 172 month-leaves → swap each.
- Existing `reflex_reconcile_partition(view, '172')` (dem_plan_id CSV) → resolves to the LIST
  partition → expands to leaves → swaps each. **Backward-compatible**, and now **correct** on
  sub-partitioned sources (today it would flat-swap a node that is itself partitioned).

A new optional argument `source_partition DEFAULT NULL` is added to `reflex_reconcile_partition`;
when provided it resolves at any level and takes precedence over the `partition_keys` CSV.

## Section 4 — Lifecycle (`reflex_sync_partitions` recursion)

`reflex_sync_partitions_impl` becomes recursive:

1. Walk the source tree (recursive) → set of `(node, parent, bound, sub_strategy, sub_columns)`.
2. Walk the IMV target (and intermediate, if present) tree (recursive) → current set.
3. **Create** missing nodes **top-down** via the recursive DDL builder (internal nodes get the
   sub-`PARTITION BY` suffix).
4. **Drop** orphan nodes (when `drop_orphans=true`) — `DROP … CASCADE` handles subtrees.
5. Refresh `__reflex_source_partition_snapshot`.

Consequences: a new month attached under an existing `dem_plan_id` → IMV gains a matching month
leaf; a new `dem_plan_id` attached → IMV gains an internal node + its month leaves; a removed source
leaf → IMV orphan leaf dropped. A single `reflex_reconcile_partition(view, source_partition := new)`
call after `ATTACH` performs create (via sync) + fill (via swap).

## Section 5 — Validation, testing, bench, scope

### Tests

Unit (pure Rust):
- Recursive-tree DDL emission order + sub-`PARTITION BY` suffix correctness.
- Multi-level IMV node-naming uniqueness.
- All-levels bare-projected-column validation (reject computed/renamed/missing level column).
- oid-diff classification (swap / new / gone / unchanged).

Integration (`#[pg_test]`, new `src/tests/pg_test_subpartition.rs`):
- Mirror-shape assertion: IMV target tree matches source tree (node count, bounds, strategies).
- Leaf swap touches only that IMV leaf (other leaves' relfilenodes unchanged).
- Whole-`dem_plan_id` reconcile rebuilds all its month leaves.
- Attach-new-month → IMV gains a matching leaf, data correct.
- Attach-new-`dem_plan_id` → IMV gains an internal node + leaves.
- Detach-remove → IMV orphan leaf dropped.
- Event-trigger enqueue + `reflex_flush_partitions` end-to-end (incl. reflex-owned swaps ignored).
- Cascade to a `dem_plan_id`-partitioned dependent after a leaf swap.
- Audit drift-check flags an intentionally-skipped reconcile.
- **Differential fuzz** (`pg_test_fuzz.rs` harness): random attach/detach/swap sequences interleaved
  with flush; assert `IMV EXCEPT source-recompute = ∅`.

### Bench

Extend `benchmarks/bench_partitioned_imv.sql`: leaf swap vs whole-`dem_plan_id` reconcile vs full
reconcile on a 2-level (`LIST → RANGE`) source. Confirm no regression on single-level partitioned
and unpartitioned IMVs.

### CI gates

`cargo pgrx check && cargo pgrx test && cargo clippy && cargo fmt --check` — all green.

### Out of scope (v1)

- HASH partitioning at any level.
- Multi-hop Tier-2 JOIN-derived dispatch through the hierarchy (anchor + co-partitioned only).
- Auto-flush-at-commit wiring into the DEFERRED hook (explicit `reflex_flush_partitions` first).
- Finer-than-`dem_plan_id` cascade to dependents (dependents reconcile at their own LIST level —
  correct if coarser).
- Auto-detection of `detach → modify-in-place → reattach-same-table` (explicit reconcile call +
  audit drift-check cover it).
- Recursive DML triggers on sub-partitions (captures nothing for swaps; root trigger + reconcile
  cover the real vectors).

## Files to touch

- `src/partition.rs` — recursive `PartitionNode` walk; sub-`PARTITION BY` codegen; recursive sync;
  level-agnostic reconcile resolution; snapshot read/refresh helpers.
- `src/create_ivm/mod.rs` — recursive mirroring at create time; all-levels validation; snapshot seed.
- `src/lib.rs` — `reflex_flush_partitions` SQL function; `__reflex_source_partition_snapshot` +
  `__reflex_partition_pending` catalog tables; `source_partition` arg on `reflex_reconcile_partition`.
- Event-trigger registration (`__reflex_partition_ddl_watch`) + its handler function.
- `src/audit/checks_b_drift.rs` — recursive source-vs-IMV partition-tree + per-leaf row-count check.
- `src/tests/pg_test_subpartition.rs` (new); extend `pg_test_fuzz.rs`, `unit_partition.rs`.
- `benchmarks/bench_partitioned_imv.sql`; `docs/concepts/internals.md` (Partitioning section);
  `CHANGELOG.md`.

## Phasing

Each phase independently committable + testable:

- **Phase 1 — Recursive mirroring + codegen + validation** (Section 1). Create-time + sync produce a
  correct multi-level IMV tree. Tests: mirror-shape, validation, recursive DDL unit tests.
- **Phase 2 — Level-agnostic reconcile** (Section 3) + recursive sync (Section 4). Manual
  `reflex_reconcile_partition(view, source_partition)` works at any level. Tests: leaf/internal
  swap, attach/detach lifecycle.
- **Phase 3 — Event trigger + flush + snapshot** (Section 2). Automatic capture. Tests: enqueue +
  flush end-to-end, reflex-owned-swap filtering, oid-diff classification.
- **Phase 4 — Audit drift-check + differential fuzz + bench** (Section 5). Correctness backstop +
  performance validation.

Phase 1 is the foundation; Phase 2 depends on it; Phase 3 builds on Phase 2's reconcile; Phase 4 is
the safety/measurement layer over all of it.
