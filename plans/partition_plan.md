# pg_reflex partitioning support

## Context

pg_reflex creates an intermediate table and a target table per IMV. Source
tables in the user's environment are themselves partitioned (typically LIST /
discrete). Today the IMV tables are flat, which means:

- The IMV cannot benefit from partition pruning even though the source can.
- Maintenance and retention require row-by-row DELETEs instead of DETACH.
- The IMV indexes are global (one big B-tree) — they grow unboundedly.
- IMV ↔ source joins cannot use partition-wise join planning.

We want `create_imv` and siblings to accept partitioning columns; pg_reflex
will create both the **intermediate** and **target** tables as declarative
partitioned tables, mirror the existing partitions on the source at create
time, and expose a `reflex_sync_partitions` function so users can pick up
newly-added source partitions on demand. v1 supports **LIST and RANGE**, no
HASH. Passthrough IMVs auto-mirror source partitioning without requiring an
explicit column arg.

Critically, v1 also includes **partition-scoped reconcile**: bulk writes
that flip a large fraction of one partition reconcile only that partition
instead of the whole IMV. Without it, partitioning only helps reads, vacuum,
and retention — maintenance benefits would be marginal. Path B dispatch in
the trigger body is upgraded to per-partition ratios, so partition-local
bulk writes never cause a full IMV reconcile.

## Pros / Cons

### Pros

1. **Partition-scoped reconcile** — a bulk write that touches only one
   partition reconciles only that partition. Largest practical win for
   workloads with localized bulk loads (e.g., daily ingest of one day's
   data, or single-tenant backfill).
2. **Partition pruning on user reads** of the target table when the query
   filters on the partition key.
3. **Partition pruning during MERGE maintenance** — incremental deltas
   localized to one partition only touch one partition's index/heap.
4. **Bulk retention via `DETACH PARTITION`** — O(1) drop of old data instead
   of `DELETE` + reconcile rebuild.
5. **Smaller, faster per-partition indexes**, including the
   `UNIQUE … NULLS NOT DISTINCT` index on the intermediate.
6. **Partition-wise joins** between IMV target and source become eligible
   when both share the partition key.
7. **Per-partition vacuum/analyze** — less bloat, more accurate statistics.
8. **Mirrors what the source already does** — operationally consistent.

### Cons

1. **Hard Postgres constraint**: a unique index on a partitioned table must
   include the partition key. Our intermediate has a
   `UNIQUE (group_by_cols + distinct_cols) NULLS NOT DISTINCT` index that the
   MERGE codegen relies on. Therefore **partition_by must be a subset of
   GROUP BY** for aggregate IMVs. We will validate and error out clearly when
   violated. (For passthrough IMVs the partition column just needs to be a
   selected source column — usually trivially true since passthrough mirrors
   the source.)
2. **Source partition changes don't auto-propagate.** If the user adds a new
   partition to the source after `create_imv`, INSERTs that route to the new
   value will fail until `reflex_sync_partitions` is called. We mitigate by
   calling sync at the top of every `reflex_reconcile` and documenting it.
3. **Multi-source IMVs need an anchor.** The IMV's partition column comes
   from exactly one source table (the one carrying that column per
   `source_join_keys`). That source must itself be partitioned LIST/RANGE on
   that same column; the other source tables don't need to be partitioned.
   If the anchor isn't partitioned, we error out.
4. **PG15+ required for MERGE on partitioned target.** pg_reflex already
   targets PG15+, so this is no regression — but it does mean we cannot
   backport partitioning to PG14 if that ever becomes a goal.
5. **`LIKE source INCLUDING DEFAULTS` doesn't clone partition structure.**
   The current scratch/staging tables (`__reflex_delta_*`,
   `__reflex_staging_delta_*`, passthrough scratches) are built that way.
   These tables are *transient* — keeping them unpartitioned is fine and
   actually preferable (no router overhead on short-lived data). We
   intentionally don't partition them.
6. **Test matrix grows.** Cross of {LIST, RANGE} × {aggregate, passthrough}
   × {INSERT, UPDATE, DELETE, TRUNCATE} × {reconcile, refresh, drop}. We add
   targeted tests rather than a combinatorial sweep.
7. **DDL-in-trigger is avoided.** We did not pick "auto-create partition on
   first-seen value" because it requires running DDL inside the trigger
   function under unknown locks — a known bug magnet. Sync is explicit
   instead.
8. **Repartitioning an existing IMV is not supported in v1.** Changing the
   partition column or strategy requires `drop_reflex_ivm` + `create_imv`.
9. **Cascading reconcile partial-scope is only preserved when the dependent
   IMV shares the same partition column.** When a downstream IMV is
   partitioned on a different column (or not partitioned), the cascade
   falls back to full reconcile on that dependent. We deliberately do not
   attempt cross-column partition-set translation in v1.

## Design

### Public API

Add an optional `partition_by TEXT[] DEFAULT NULL` argument to each of:

- `create_reflex_ivm` (both overloads)
- `create_reflex_ivm_with_topk`
- `create_reflex_ivm_if_not_exists`

Semantics:
- `NULL` or empty → no partitioning (current behavior, byte-for-byte).
- Non-empty → partition the intermediate and target on those columns. The
  strategy (LIST vs RANGE) and bounds are **derived** from the anchor source
  table — we never ask the user to specify bounds inline.

Add two new SQL functions:

- `reflex_sync_partitions(view_name TEXT) RETURNS TEXT` — diffs source
  partitions against IMV partitions and creates any missing ones on both
  intermediate and target. Returns a summary. Idempotent. Never drops.
- `reflex_reconcile_partition(view_name TEXT, partition_keys ANYARRAY)
  RETURNS TEXT` — reconciles only the partition(s) covering the supplied
  keys, on both intermediate and target. Cascades to dependent IMVs (see
  *Cascading* below). The existing `reflex_reconcile(view_name)` keeps its
  semantics (full reconcile).

### Catalog changes (`public.__reflex_ivm_reference`)

Add two columns:
- `partition_columns TEXT[]` — output-column names; NULL when not
  partitioned.
- `partition_strategy TEXT` — `'LIST'` or `'RANGE'`; NULL when not
  partitioned.

These are populated at `create_imv` time after we resolve the anchor source
and inspect its partition descriptor. Bounds themselves are *not* stored —
they're always looked up live from the source so we never drift.

### DDL generation

Three things change in `src/schema_builder.rs`:

1. **`build_intermediate_table_ddl`** (~line 107) and
   **`build_target_table_ddl`** (~line 165): when partition columns are set,
   append `PARTITION BY {strategy} ({cols})` to the `CREATE TABLE`.
2. New helper `build_partition_children_ddl(view_name, source_anchor,
   partition_cols)`: reads `pg_partitioned_table` + `pg_class.relpartbound`
   + `pg_inherits` for the anchor source, then for each child source
   partition emits two `CREATE TABLE __reflex_intermediate_<view>_<bound>
   PARTITION OF … FOR VALUES …` and the matching one for the target. Reuses
   the source partition's bound expression verbatim (via
   `pg_get_expr(relpartbound, oid)`).
3. **`build_indexes_ddl`** (~line 270): no structural change required — the
   existing `UNIQUE … NULLS NOT DISTINCT` already includes the GROUP BY
   columns, so partition columns ⊆ GROUP BY ⇒ the unique index is valid on
   a partitioned table. Postgres will automatically propagate the index to
   each partition.

Scratch / staging / passthrough-scratch tables stay **unpartitioned** — they
are transient and the LIKE pattern is correct for them.

### Validation (at `create_reflex_ivm_impl`)

Before any DDL runs:

1. Every column in `partition_by` exists in the IMV's projection.
2. For aggregate IMVs: `partition_by ⊆ AggregationPlan.group_by_columns`.
   Error message names the offending column and the GROUP BY set.
3. For passthrough IMVs without an explicit `partition_by`: auto-derive from
   the source's `pg_partitioned_table.partattrs`. If the user *did* pass
   `partition_by` on a passthrough, validate it matches the source.
4. Resolve the *anchor source* — the source table that owns the partition
   column per `AggregationPlan.source_join_keys`. Exactly one source must
   own it; error if zero or multiple.
5. Anchor source must be partitioned (`pg_partitioned_table` row exists) on
   the same column, strategy ∈ {LIST, RANGE}. Otherwise error.

### `reflex_sync_partitions(view_name)`

Logic:

1. Look up `partition_columns` + `partition_strategy` + anchor source from
   catalog.
2. Read current source partitions via `pg_inherits` + `pg_class.relpartbound`.
3. Read current IMV partitions (both intermediate and target) the same way.
4. For each source partition not present on the IMV: emit
   `CREATE TABLE … PARTITION OF … FOR VALUES …` on both intermediate and
   target. Run inside `pg_advisory_xact_lock` keyed by view to avoid
   concurrent racing.
5. Return summary `"sync: +N intermediate, +N target"`.
6. Never drop — log a NOTICE when a partition exists on the IMV but not the
   source.

Wire into `reflex_reconcile` (call at the very start) so a stale partition
set can never make reconcile fail.

### Reconcile / refresh / triggers

- **Full reconcile** (`reflex_reconcile`): `TRUNCATE` on parent cascades to
  all partitions naturally; `INSERT INTO parent SELECT …` routes correctly.
  Call `reflex_sync_partitions` at entry.
- **Partition-scoped reconcile** (`reflex_reconcile_partition`):
  1. Resolve the target child partition(s) matching the supplied keys via
     `pg_partition_tree(parent)` + `satisfies_hash_partition` /
     constraint match.
  2. For each matching child P, fetch
     `pg_get_partition_constraintdef(child_oid)` — a Boolean expression
     that exactly characterises P's bounds (works uniformly for LIST and
     RANGE; no bound parser needed).
  3. Inside a single transaction per partition:
     ```sql
     TRUNCATE __reflex_intermediate_<view>_<child>;
     INSERT INTO __reflex_intermediate_<view>
       SELECT … FROM (<base_query>) src WHERE <constraintdef>;
     TRUNCATE <view>_<child>;
     INSERT INTO <view>
       SELECT … FROM (<end_query, on intermediate>) WHERE <constraintdef>;
     ```
  4. Cascade to dependents (see below).
- **Triggers**: stay attached to source root tables, fire once per
  statement through transition tables — unchanged.
- **Path B dispatch upgrade**: replace the single-IMV ratio with a
  per-partition ratio:
  ```
  SELECT partition_key, count(*) AS dirty FROM transition GROUP BY …
  -- for each (partition_key, dirty):
  IF dirty / partition.reltuples > wipe_threshold THEN
      PERFORM reflex_reconcile_partition(view, ARRAY[partition_key]);
      -- mark partition handled; remaining rows fall through to Path A
  END IF;
  ```
  The remaining rows (rows in partitions that didn't trip the threshold)
  flow through Path A (scratch + MERGE) as today, restricted via WHERE to
  only those partitions. This means a single bulk statement that flips
  partition X and trickles into partitions Y, Z is handled as
  "reconcile X, MERGE Y+Z" in one trigger invocation.
- **MERGE codegen**: targets the partitioned intermediate parent; the
  planner prunes when the delta is partition-local. No codegen change
  required (verified against `trigger.rs:65-284`).

### Cascading dependent IMVs

When `reflex_reconcile_partition(IMV_B, keys)` finishes, we cascade to each
dependent IMV_A (from `__reflex_ivm_reference.depends_on_imv`):

- If IMV_A is partitioned **on the same column** as IMV_B → call
  `reflex_reconcile_partition(IMV_A, keys)` with the same keys. Narrow
  scope preserved.
- If IMV_A is partitioned **on a different column** → call
  `reflex_reconcile(IMV_A)` (full). We do not attempt to derive the
  cross-column partition set; the mapping isn't generally well-defined.
- If IMV_A is **not partitioned** → call `reflex_reconcile(IMV_A)` (full).

Cascade is depth-first and reuses the existing `graph_depth` ordering from
`__reflex_ivm_reference` so deeper IMVs reconcile after their parents.
`refresh_imv_depending_on(source)` gains a `partition_keys` overload that
threads keys through this same logic.

### Drop

`drop_reflex_ivm` uses `DROP TABLE … CASCADE`, which already drops all
partitions of a partitioned parent. No change.

## Implementation phases

Each phase is independently committable and individually testable. Per the
project's stated discipline (correctness > simplicity > performance), we
also benchmark before each phase merges.

**Phase 1 — API + persistence (no DDL change yet)**
- Thread `partition_by: Option<Vec<String>>` through every public SQL
  function, into `create_reflex_ivm_impl`, into `AggregationPlan` (extend
  the struct + JSONB serialization).
- Add `partition_columns TEXT[]` and `partition_strategy TEXT` to
  `__reflex_ivm_reference` (handle existing-deployment migration via the
  same idempotent CREATE/ALTER block at extension load).
- Run validation; populate catalog. **No** `PARTITION BY` emitted yet.
- Tests: unit tests asserting catalog rows for valid inputs; error tests for
  invalid inputs (column not in GROUP BY, anchor not partitioned, etc.).

**Phase 2 — DDL generation**
- Emit `PARTITION BY …` in intermediate + target DDL.
- New `build_partition_children_ddl` helper; introspects anchor source.
- Tests: integration test that creates a partitioned source, calls
  `create_reflex_ivm` with `partition_by`, verifies via
  `pg_partitioned_table` that intermediate and target are partitioned and
  that each source partition has a matching IMV partition.

**Phase 3 — Sync function + reconcile wiring**
- Implement `reflex_sync_partitions(view_name)`.
- Call it at the top of `reflex_reconcile` / `reflex_rebuild_imv`.
- Tests: source-side ATTACH new partition → reconcile → assert IMV
  partition added; verify reconcile no longer errors on out-of-range rows.

**Phase 4 — Partition-scoped reconcile + cascade**
- Implement `reflex_reconcile_partition(view_name, partition_keys)` using
  `pg_get_partition_constraintdef` for the WHERE clause (uniform for LIST
  and RANGE).
- Cascade logic: walk `depends_on_imv` graph in `graph_depth` order; for
  each dependent IMV pick partition-scoped or full per the rules above.
- Upgrade Path B dispatch in the trigger function body
  (`src/schema_builder.rs:481+`) to compute per-partition ratios and call
  partition-scoped reconcile for partitions that breach the threshold;
  the remaining partitions stay on Path A with a WHERE filter so they
  don't pay scratch fill for rows we already reconciled.
- Tests:
  - Bulk insert into partition X (≥ threshold) + trickle into Y →
    trigger emits `reflex_reconcile_partition(view, ARRAY[X])` and a
    scratch MERGE limited to Y; assert via row counts and via
    `pg_stat_user_tables` that other partitions' relfilenodes are
    untouched.
  - Cascade test: two IMVs A depends on B, both partitioned on the same
    key → partition-reconcile B → assert A's partition-reconcile fired
    for the same partition, not the whole IMV.
  - Cascade fallback test: A depends on B, partitioned on different
    keys → assert A falls back to full reconcile.

**Phase 5 — Passthrough auto-mirror**
- In the passthrough branch of `create_reflex_ivm_impl`, when the user did
  not pass `partition_by` but the source is partitioned, auto-derive
  partition cols and strategy from the source.
- Tests: passthrough IMV from a partitioned source without `partition_by`
  arg → IMV ends up partitioned identically.

**Phase 6 — Bench + docs**
- Add a benchmark to `benchmarks/` covering: (a) incremental MERGE latency
  on a partition-local delta, (b) Path B dispatch with one hot partition
  + trickle elsewhere, (c) full reconcile vs partition-scoped reconcile,
  (d) cascading partitioned IMVs. Compare partitioned vs unpartitioned on
  the same db_clone fixture. Per `feedback_optimization_approach`, we keep
  each phase only if the numbers justify the code volume; partition-scoped
  reconcile (phase 4) is the load-bearing one — if it underperforms on
  representative workloads, the whole feature's value is at risk.
- README section: "Partitioning your IMV": API, the GROUP BY constraint,
  sync semantics, partition-scoped reconcile, cascade behaviour, and what
  happens if you forget to call sync.

## Critical files

- `src/lib.rs:152-217` — public SQL function declarations; add
  `partition_by` parameter to each.
- `src/lib.rs:63-95` — `__reflex_ivm_reference` schema; add two columns and
  the idempotent migration.
- `src/create_ivm.rs:25` — `create_reflex_ivm_impl`; argument plumbing +
  validation + anchor resolution.
- `src/aggregation.rs:61-151` — `AggregationPlan`; extend struct + JSONB
  ser/de.
- `src/schema_builder.rs:107-136` — `build_intermediate_table_ddl`; add
  PARTITION BY.
- `src/schema_builder.rs:165-267` — `build_target_table_ddl`; add
  PARTITION BY.
- `src/schema_builder.rs:270-358` — `build_indexes_ddl`; verify unique
  index includes partition cols (should already by construction; add a
  defensive assert).
- `src/reconcile.rs:64-280` — call `reflex_sync_partitions` at entry of
  both passthrough and aggregate reconcile paths; add
  `reflex_reconcile_partition` implementation parallel to
  `reflex_reconcile` and share the index-drop/rebuild + ANALYZE helpers.
- `src/schema_builder.rs:481+` — Path B dispatch upgrade: replace
  per-IMV ratio with per-partition ratio, emit
  `reflex_reconcile_partition` call, restrict Path A scratch fill to the
  remaining partitions via WHERE filter.
- `src/trigger.rs:65-284` — `build_merge_sql` and the affected-groups
  helpers: no codegen changes, but verify with a partitioned target that
  MERGE/UPDATE/DELETE prune correctly (defensive integration tests).
- `src/drop_ivm.rs:134-192` — verify CASCADE drop semantics for
  partitioned parents (should already work; add a regression test).
- New file `src/partition.rs` — partition introspection + child DDL
  generation + sync function implementation, kept isolated for revertability.
- `src/tests/pg_test_basic.rs`, `src/tests/pg_test_e2e.rs`,
  `src/tests/unit_schema_builder.rs`, `src/tests/unit_create_ivm.rs` —
  new tests per phase.

## Verification

- `cargo pgrx test` passes (existing + new tests).
- `cargo clippy` and `cargo fmt` clean.
- Manual end-to-end on local `db_clone`:
  1. Pick a partitioned source (LIST on a discrete column).
  2. `create_reflex_ivm(..., partition_by => ARRAY['<col>'])`; check
     `\d+ __reflex_intermediate_<view>` and `\d+ <view>` both show
     `Partition key:` and one child partition per source partition.
  3. INSERT into a source partition → trigger fires → MERGE routes to the
     correct IMV partition → `SELECT … FROM <view> WHERE <col> = …` plan
     shows partition pruning in `EXPLAIN`.
  4. `ALTER TABLE source ATTACH PARTITION …` a new partition;
     `reflex_sync_partitions('<view>')`; verify new IMV partitions present.
  5. `reflex_reconcile('<view>')` after the sync — counts match a
     ground-truth `SELECT … GROUP BY …` over the source.
  6. Bulk INSERT a partition-sized batch into one source partition; check
     trigger log / `pg_stat_user_tables` to confirm Path B dispatched a
     **partition-scoped** reconcile (only that child's relfilenode
     changed) rather than a full reconcile.
  7. Build a two-IMV chain (B depends on A) both partitioned on the same
     key; trigger partition-scoped reconcile on A; confirm B's cascade
     also stayed partition-scoped.
  8. Repeat (7) with B partitioned on a different key; confirm B falls
     back to full reconcile.
  9. `drop_reflex_ivm('<view>')` — verify all partitions are dropped.
- Bench (`benchmarks/`) shows: (a) partition-scoped reconcile is
  meaningfully faster than full reconcile when only one partition is
  dirty; (b) Path B per-partition dispatch beats today's whole-IMV Path B
  on partition-local bulk loads. If either fails to materialise, document
  the gap and decide whether the feature still ships.
