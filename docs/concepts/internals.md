# Internals

Engine-level details behind the public API. Useful when tuning, debugging, or reading the source. Separate from [architecture](architecture.md), which covers *what* objects exist; this page covers *how* they behave at the PostgreSQL level.

## Trigger model

pg_reflex uses **AFTER, statement-level triggers** with `REFERENCING NEW TABLE` / `OLD TABLE` transition tables. The choice matters:

- **Statement-level** fires once per DML statement, not once per row. A `COPY ... FROM` or 1 M-row `INSERT … SELECT` fires the trigger exactly once with the full transition table. Row-level triggers would multiply trigger overhead by row count.
- **AFTER** — the trigger sees the post-statement state of the source. Required so the delta MERGE writes from a stable, consistent view.
- **Transition tables** are PG-native ephemeral relations (no copy, no temp table created). pg_reflex reads them like any other table: `SELECT ... FROM new_rows JOIN ...`.

Four triggers per source, regardless of how many IMVs depend on it:

```
__reflex_trigger_ins_on_<source>     AFTER INSERT
__reflex_trigger_del_on_<source>     AFTER DELETE
__reflex_trigger_upd_on_<source>     AFTER UPDATE
__reflex_trigger_trunc_on_<source>   AFTER TRUNCATE
```

The trigger body loops over `__reflex_ivm_reference` and computes the delta per IMV. Adding a second IMV on the same source piggybacks on existing triggers — no extra trigger overhead per source row.

## The dispatch DO block

Every grouped non-MIN/MAX flush is wrapped in a per-IMV `DO` block that picks one of two paths based on selectivity (`src/trigger/dispatch.rs` · `build_high_selectivity_dispatch_sql`):

```sql
SELECT count(*) INTO _aff FROM <affected>;
SELECT GREATEST(reltuples, 1) INTO _imm FROM pg_class WHERE oid = '<intermediate>'::regclass;
_thr := COALESCE(per-IMV column, GUC, compiled default);
IF (_aff / _imm) >= _thr THEN
    PERFORM reflex_reconcile('<view>');     -- TRUNCATE + bulk INSERT
ELSE
    EXECUTE merge_sql; ANALYZE intermediate;
    EXECUTE target_delete_sql; EXECUTE target_insert_sql;
END IF;
```

The crossover defaults to `0.5` from 1.4.6 onward. Setting it lower routes more workloads through the rebuild branch; setting it higher keeps everything incremental at the cost of pathological in-place updates on bulk filter flips. Per-IMV tuning via `reflex_set_wipe_threshold` is preferred — the right threshold is shape-dependent (see [optimization](../performance/optimization.md)).

The `ANALYZE intermediate` between MERGE and target sync is non-optional. The MERGE shifts the `__ivm_count` distribution; without fresh stats the planner has picked NestedLoop+SeqScan plans for the target DELETE that ran for 12+ minutes on 100 K groups. Cost of the ANALYZE itself: ~150 ms on the SOP-forecast shape (`src/trigger/dispatch.rs`).

## Pre-scratch dispatch — Path B and Path C

The dispatch DO block above fires *after* scratch fill, which means the JOIN against the source has already run. On bulk filter flips that fanout to millions of fact rows, scratch fill itself is the dominant cost — dispatching to reconcile *after* scratch has already paid that cost is a net loss. Two pre-scratch probes route the work *before* scratch fills.

**Path B — `|transition| / |source|` ratio**. Cheap. Catches the case where a sweeping DML on a source produced a transition table that is itself a meaningful fraction of the source. For `UPDATE sales_simulation SET …` touching 40 M of 76 M rows, the ratio is 0.52 and dispatch routes to reconcile. Fails when the transition is tiny but the JOIN fanout is huge — see Path C.

**Path C — planner row estimate**. Only emitted in the UPDATE trigger body, gated on `_directional_op = 'INSERT_PROMOTED'` (the only place Item α promotes a bulk OUT→IN dim flip). Runs `EXPLAIN (FORMAT JSON)` on `reflex_build_path_c_explain_sql(view, source)` — the rewritten scratch-fill query (`base_query` with `source_table → transition_new`) — and reads the planner's row estimate without executing the JOIN. Compared against the IMV's `wipe_threshold`. Catches the *dim-flip fanout* case where Path B's ratio is misleading (1 dim row of 28 = 3.6 %, but JOINs to 8.9 M fact rows).

**Path C dispatches to a *smart bulk-INSERT***, not to `reflex_reconcile`. The Item α `INSERT_PROMOTED` precondition (OLD-side filter-rejected ⇒ intermediate has zero rows for affected keys) makes a surgical add safe and cheaper than full rebuild:

```sql
-- emitted inline in the UPDATE trigger body when ratio >= wipe_threshold
TRUNCATE <scratch>;
INSERT INTO <scratch> <base_query with source→transition_new>;  -- only the new keys
DROP INDEX <intermediate UNIQUE>;       -- skip per-row B-tree probes
INSERT INTO <intermediate> SELECT * FROM <scratch>;
CREATE UNIQUE INDEX ...;                -- rebuild bottom-up over the union
INSERT INTO <target>
    <end_query with intermediate→scratch>;  -- project from scratch, no intermediate re-read
ANALYZE <intermediate>;
```

Wins on alp.bench_user_imv 8.9 M-row OUT→IN flip: ~175 s reconcile → ~90 s smart path (measured standalone), beating `REFRESH MV` (~160 s) on the same post-state. Reconcile would re-aggregate the unchanged 7.7 M survivors; smart-bulk-INSERT touches only the 8.9 M new keys.

Any failure (catalog lookup, parse error, EXECUTE failure) is caught by `EXCEPTION WHEN OTHERS` and falls through to the standard incremental path — never aborts the trigger.

The smart bulk-INSERT only ever issues `INSERT` against the target (no `TRUNCATE`), so it takes `RowExclusiveLock` on the target — concurrent readers are not blocked. The brief `AccessExclusiveLock` during the intermediate UNIQUE drop is on an internal table users don't query. See [delta-processing — Locking and reader impact](delta-processing.md#locking-and-reader-impact) for the full lock table across all paths.

Two subtleties caught during landing and worth remembering when editing the codegen:

- **No `--` SQL comments inside the trigger body**. The whole body is concatenated to one line in the emitted DDL, so a `--` comment swallows everything after it until end-of-input. Postgres reports `syntax error at end of input`. Keep comments as Rust `//` source comments only.
- **Identifier quoting must match what `end_query` stores**. `end_query`'s FROM clause uses `intermediate_table_name`, which always emits `"schema"."table"`. `format('%I.%I', schema, table)` drops the quotes when names are plain lowercase, so `REPLACE(end_query, ...)` silently fails to substitute — the projection then re-reads the just-bulk-INSERTed intermediate and double-inserts existing rows. Build the reference with explicit `'"' || schema || '"."' || table || '"'`.

## HOT updates and fillfactor

PostgreSQL's HOT (Heap-Only Tuple) optimization avoids index updates when no indexed column changes **and** there's free space on the same heap page. Both conditions must hold.

pg_reflex hits the first condition naturally: a MERGE on the intermediate updates `__sum_*`, `__count_*`, `__min_*` and similar non-indexed columns; the group-by columns (which are indexed) don't change. The second condition is enforced by **`fillfactor = 70` on the intermediate** since 1.4.4, leaving 30 % free page space for new tuple versions on the same page.

The 2026-05-13 bench (`journal/2026-05-13_intermediate_idx_and_fillfactor.md`):

| Configuration | int_update | tgt_update | total |
|---|---|---|---|
| 6 single-col indexes + composite, fillfactor=100 | 691 ms | 179 ms | **870 ms** |
| Composite index only, fillfactor=100 | 208 ms | 169 ms | 377 ms |
| Composite index only, fillfactor=70 (HOT) | **75 ms** | **64 ms** | **139 ms** |

HOT update ratio jumps from 0 % to 100 % across the change. Combined 6.2× speedup, achieved by **removing** vestigial single-column B-tree indexes (the composite already serves every pg_reflex-generated query) and **lowering** fillfactor.

Operator-visible: `pg_stat_all_tables.n_tup_hot_upd / n_tup_upd` for the intermediate should stay near 100 %. If it drops, the page is full — usually because autovacuum has fallen behind or because the row width grew past the per-page budget.

## Heap behaviour: UNLOGGED, bloat, vacuum

**UNLOGGED is the default** for intermediate and target tables. The win is 2–4× lower flush latency on write-heavy workloads — every MERGE skips WAL. The cost: tables are empty after a crash. `reflex_scheduled_reconcile` (or `reflex_reconcile` per IMV) rebuilds them from source. See [crash recovery](../operations/crash-recovery.md).

LOGGED per-IMV is available for workloads with strict post-crash SLAs:

```sql
SELECT create_reflex_ivm('hourly_kpi', ..., NULL, 'LOGGED');
```

**Bloat behaviour**: every flush's MERGE+target DELETE+INSERT creates dead tuples. Autovacuum on an UNLOGGED table works the same as on a LOGGED one — but because there's no WAL, the only memory pressure is from `n_tup_ins + n_tup_upd + n_tup_del`. Tighten `autovacuum_vacuum_scale_factor` per IMV when the working set grows during the day:

```sql
ALTER TABLE __reflex_intermediate_v SET (autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE v SET (autovacuum_vacuum_scale_factor = 0.05);
```

**Long reader interaction**: an open snapshot from a 10-minute analytical query holds the visibility horizon back; autovacuum cannot reclaim tuples that snapshot still sees, even on UNLOGGED tables. The IMV grows until the reader finishes. The blast radius is bounded by the working set (changes during the reader's lifetime), not the full table.

## Indexes auto-created

| Object | Index | Rationale |
|---|---|---|
| Intermediate, single GROUP BY column | `USING hash` on the group column | ~30 % faster MERGE probes than B-tree for equality-only lookups |
| Intermediate, multi-column GROUP BY | One composite B-tree `UNIQUE NULLS NOT DISTINCT` on the full key | Single index satisfies the MERGE `ON` clause's full-equality predicate |
| Target, when `unique_columns` resolved | `UNIQUE INDEX __reflex_uk_<view>` | Backs targeted DELETE / UPDATE |
| Source, MIN/MAX with GROUP BY | B-tree on GROUP BY columns | Scoped-recompute scan locality |

`NULLS NOT DISTINCT` (PG 15+) is mandatory — NULL group keys in a `SUM ... GROUP BY x` are valid output rows, and a regular `UNIQUE` would allow duplicate `(NULL)` rows.

The 1.4.4 change above removed the **per-column B-trees** that earlier versions created in addition to the composite. They were vestigial: `build_merge_using` always emits a full-equality predicate, so the composite's leading column was always bound, and the per-column indexes were unreachable by any pg_reflex-generated query.

## Optimization history and reverted attempts

Moved to [Contributing → Development log](../contributing/development-log.md). That page is the version-by-version timeline of what shipped, and an index of approaches that were reverted with links to their journal entries.

## Partitioning

pg_reflex supports **declarative partitioning** of the intermediate and target tables as an opt-in feature (`plans/partitioning_2.md`). The motivation is partitioning is the cleanest fix for the "whole IMV unreachable during `reflex_reconcile`" reader-availability problem: a partition-scoped rebuild only locks the affected child.

The public API exposes one new argument and two new functions:

- `create_reflex_ivm(..., partition_by => ARRAY['col'])` — opt-in.
- `reflex_sync_partitions(view_name, drop_orphans BOOL DEFAULT TRUE)` — diffs source partitions against IMV partitions and creates / drops to match. Idempotent.
- `reflex_reconcile_partition(view_name, partition_keys TEXT, source_partition TEXT DEFAULT '')` — atomic DETACH/ATTACH swap (1.6.0). `source_partition` (unreleased) reconciles a named source partition at **any** level (expands to its leaves) — see "Multi-level (sub-partition) sources" below.
- `reflex_flush_partitions()` / `reflex_flush_partition_source(root)` (unreleased) — apply pending source `DETACH`/`ATTACH` swaps captured by the event trigger, via a snapshot oid-diff. Call once after a batch of partition swaps.
- `reflex_set_wipe_floor_rows(view_name, n)` (1.6.0) — per-IMV floor for the per-partition denominator in the trigger-time dispatch ratio.
- `reflex_set_partition_dispatch_cost_cap(view_name, n)` (1.6.0) — per-IMV cap for the Tier 2 JOIN cost estimate.

The "anchor source" is the single source table that physically owns the partition column. Bounds are never cached — `pg_get_partition_constraintdef` and `pg_get_expr(relpartbound, oid)` are queried live from `pg_inherits` so we cannot drift from the source. v1 supports LIST and RANGE only; HASH is deferred.

### Atomic swap (1.6.0, `plans/partitioning_3.md` §1)

Both `reflex_reconcile_partition` (one partition) and `reflex_reconcile` (every partition, when the IMV is partitioned) use the same per-child swap helper (`partition::execute_partition_swap_for_child`).  The flow per partition:

1. Build the new partition outside the partition tree: `CREATE [UNLOGGED] TABLE __reflex_swap_int_<view>_<src_child> (LIKE old_child INCLUDING ALL)`.
2. Fill it from `(base_query)` restricted by the partition's constraint def.  The fill happens BEFORE any lock on the parent — `AccessShareLock` only on the source.
3. Add a `CHECK` constraint matching the partition bound — PG sees this and skips its own ATTACH validation scan, shortening the parent-lock window further.
4. Atomically inside one SPI sub-transaction:
   - `DETACH PARTITION` the old child from the parent.
   - `ATTACH PARTITION` the new (swap) child to the parent with the same bound.
   - `DROP TABLE` the now-orphaned old child.
   - `RENAME` the swap table to the canonical child name so a subsequent reconcile / sync can find it.

The intermediate must be swapped before the target — the target swap's fill reads `end_query` from the intermediate, so the intermediate must be at post-rebuild state. Order: build + swap intermediate, build + swap target. Passthrough IMVs skip the intermediate step.

**Idempotent recovery**: every entry point drops leftover `__reflex_swap_*` tables for the view (signaled purely by name prefix). If DETACH+ATTACH fails mid-way, the outer Spi::connect_mut sub-transaction rolls back the entire reconcile call — the IMV stays in its pre-call state.

**Lock window**: AccessExclusiveLock on the parent is taken for the DETACH/ATTACH DDL itself (~µs) and held until the transaction commits. Readers on other partitions are blocked only during the DDL window, not during the data fill.

**Global reconcile**: `reflex_reconcile` on a partitioned IMV (1.6.0 follow-up) iterates over every source partition child and calls the swap helper per child — bypassing the legacy TRUNCATE-on-parent + INSERT-via-tuple-routing pattern that held the parent's AccessExclusiveLock for the entire rebuild.  Per-partition swap is also ~30% faster on the synthetic bench (1474 ms vs 2148 ms on a 10M-row 4-partition IMV) because direct per-child INSERT skips PG's tuple-routing overhead.

### Per-partition trigger dispatch (1.6.0, `plans/partitioning_3.md` §3)

A bulk write concentrated in one or two partitions used to trip the per-IMV `wipe_threshold` and route to global `reflex_reconcile`, rebuilding all partitions. The Phase C dispatch (`build_partition_aware_dispatch_sql`) replaces the global dispatch for partitioned LIST IMVs:

1. After scratch + affected are populated, GROUP the affected table by the partition column to get per-partition dirty counts.
2. Look up the matching child via `__reflex_partition_child_for_key(parent, part_col, key)` and read its `reltuples`.
3. Classify partitions as hot (`dirty / GREATEST(reltuples, wipe_floor_rows) >= wipe_threshold`) or cold.
4. Trip-cap: if `hot_count > total / 2`, fall back to global `reflex_reconcile` (sequentially DETACHing > half the partitions is worse than one rebuild).
5. Hot partitions → `reflex_reconcile_partition(view, hot_keys_csv)` (uses the atomic swap from §1).
6. Cold partitions → standard MERGE / dead-cleanup / target DELETE / target INSERT with a `<partition_col> <> ALL($1::TEXT[])` filter spliced into the USING / WHERE clauses (`$1` is the hot-keys array, bound via EXECUTE USING).

The `wipe_floor_rows` floor on the denominator avoids tripping the dispatch on never-ANALYZE'd partitions where `reltuples = 0` would yield infinite ratios.

### `partition_by` must be bare columns (1.6.0, `plans/partitioning_3.md` §2)

`partition_by` columns must correspond to bare column references in `GROUP BY` (`Expr::Identifier` / `Expr::CompoundIdentifier`). Computed GROUP BY expressions (`DATE_TRUNC('month', d)`, `UPPER(col)`, casts, arithmetic) are rejected at `create_reflex_ivm` time. Rationale: the per-partition dispatch needs to find the partition key on transition tables by simple column reference; computed expressions would require re-evaluating the function on every transition row. **Workaround**: add a generated / computed column to the source and partition on that.

### Tier 2 metadata (1.6.0, `plans/partitioning_3.md` §4)

`AggregationPlan.partition_join_paths` is a per-source SQL fragment that derives the IMV's partition column from each non-anchor source's transition table by JOINing to the anchor. Populated at create time for sources where `source_join_keys` contains a `(partition_col, source_col)` pair. Format: `SELECT a."{partition_col}" AS pkey, t.* FROM {transition_alias} t JOIN {anchor} a ON a."{partition_col}" = t."{source_col}"`. The `{transition_alias}` placeholder is substituted at trigger fire time with the actual transition table name.

In the current implementation the post-scratch dispatch reads pkey counts from the affected table directly (which already carries the partition column from the JOIN), so the Tier 2 fragment is held in reserve for a future pre-scratch dispatch optimisation.

### Sync semantics

`reflex_sync_partitions(view, true)` is symmetric: source partitions absent from the IMV are created (CASCADE-safe DDL, advisory-lock protected), and IMV partitions absent from the source are dropped (`DROP TABLE ... CASCADE` — only touches the pg_reflex-owned partition child). `drop_orphans => FALSE` preserves orphans and emits a NOTICE instead. Sync runs automatically at the start of every `reflex_reconcile` and `reflex_reconcile_partition`.

### Auto-mirror

When `partition_by` is NULL, pg_reflex introspects the source tables and *auto-mirrors* if:

- Exactly one source table is partitioned LIST/RANGE.
- For aggregate IMVs, the partition column is in `GROUP BY`.
- For passthrough IMVs, the partition column is in the projected SELECT list.

Otherwise a NOTICE is emitted and the IMV stays unpartitioned. The explicit `partition_by` argument always wins over auto-mirror.

### Constraint: partition_by ⊆ GROUP BY (aggregate IMVs)

The intermediate has a `UNIQUE ... NULLS NOT DISTINCT` index on the group-by columns, and PostgreSQL requires a unique index on a partitioned table to include the partition key. So partition columns must be a subset of GROUP BY for aggregate IMVs — validated at `create_reflex_ivm` time.

### Cascade

`reflex_reconcile_partition(B, keys)` cascades to every IMV depending on B. If the dependent IMV is partitioned **on the same column**, the cascade calls `reflex_reconcile_partition(dep, keys)`. Otherwise it falls back to full `reflex_reconcile(dep)` — cross-column partition mapping is not generally well-defined and is deferred.

### Source partition ATTACH auto-propagates (1.6.0)

When the user adds a new partition to a source — `ALTER TABLE parent ATTACH PARTITION child …` or `CREATE TABLE child PARTITION OF parent …` — pg_reflex's `ddl_command_end` event trigger (`reflex_on_ddl_command_end`, function `public.__reflex_on_ddl_command_end`) fires. The function:

1. Iterates `pg_event_trigger_ddl_commands()`.
2. For each `ALTER TABLE` it treats the object identity as the candidate parent. For each `CREATE TABLE` it looks up `pg_inherits` to determine whether the new table was attached as a partition of some parent (this catches both `CREATE TABLE … PARTITION OF` and `ATTACH PARTITION` paths regardless of how Postgres labels the `object_type`).
3. For every partitioned IMV (`partition_columns IS NOT NULL`) whose `depends_on` contains that parent, it calls `reflex_sync_partitions(view, drop_orphans=>FALSE)`. The sync is idempotent and advisory-lock protected; duplicates within one transaction collapse harmlessly.

`drop_orphans=FALSE` is deliberate: DETACH on the source is not a delete signal. The IMV partition may still hold data the operator wants to query. To drop orphans, call `reflex_sync_partitions(view, true)` manually.

Non-partition `ALTER TABLE` variants (column add/drop, …) on a tracked source still trip the existing `pg_reflex.alter_source_policy = warn|error` contract — the auto-sync branch is a no-op for IMVs without `partition_columns` set.

`reflex_reconcile` also runs `reflex_sync_partitions` at entry as a defense-in-depth (event triggers can be disabled by superusers via `ALTER EVENT TRIGGER ... DISABLE`).

## Multi-level (sub-partition) sources and partition-change capture

`plans/sub_partitioning.md`. Production sources are often **multi-level** partitioned — e.g. `yse.sales_simulation` is `LIST (dem_plan_id)` at level 1 and each LIST child is itself `RANGE (order_date)` at level 2. pg_reflex mirrors the **entire** source partition hierarchy onto the IMV and can reconcile at **any level**. This section is the end-to-end description of how partition changes are detected and applied.

### How writes to a partitioned source are captured

The capture mechanism depends on *how* rows reach the source. The full coverage map:

| Write vector | Captured by | Per-sub-partition trigger? |
|---|---|---|
| `DETACH`/`ATTACH PARTITION` swap | **Event trigger → enqueue → flush** (this section) | No |
| Ordinary DML through the **root** (`INSERT/UPDATE/DELETE yse.sales_simulation`) | The root statement trigger — its transition table captures all routed rows, including rows landing in newly-attached sub-partitions | No |
| Ordinary DML **directly against a leaf** (`UPDATE sales_simulation_p_172_2025_03 …`) | Not captured automatically — detectable via the audit drift-check | No |

The key fact: **statement-level triggers with transition tables fire only for the table named in the SQL command, and `ATTACH`/`DETACH` are DDL that fire no DML trigger at all** (and an attached partition's rows pre-exist any trigger). So a swap can only be captured by a *reconcile*, driven by a PostgreSQL **event trigger** — never by a row/statement trigger on the partition. This is why pg_reflex places **no triggers on sub-partitions**: a newly-attached sub-partition needs zero trigger management.

### Full-hierarchy mirroring

At `create_reflex_ivm` (and on every `reflex_sync_partitions`) the source tree is walked recursively (`partition::list_partition_tree`, a `WITH RECURSIVE` over `pg_inherits`, returned top-down). Each `PartitionNode` records its immediate parent, its `FOR VALUES` bound, and — when it is itself partitioned — its sub-strategy/columns. `build_partition_node_ddl_pair` emits a matching IMV child per node: an internal node gets a `… PARTITION OF <parent-imv-child> FOR VALUES … PARTITION BY <sub-strategy> (<sub-cols>)` (always LOGGED), a leaf gets `… PARTITION OF <parent-imv-child> FOR VALUES …` (honouring the IMV's storage mode). IMV node names are `<bare_view>_<source_node_bare>` — source relnames are globally unique across levels, so the mapping is 1:1 at every level. Validation requires **every** partition-key column at **every** level to be a bare projected column in the IMV's unique key / GROUP BY (else PG could not build the required unique index, and the swap-fill constraint could not resolve).

### Reconcile is level-agnostic

`reflex_reconcile_partition(view, partition_keys, source_partition DEFAULT '')` resolves to a set of IMV **leaves** and swaps each via `execute_partition_swap_for_child`:

- `source_partition := '<source partition at any level>'` — expands to that node's source leaves (a leaf expands to itself), each mapped to its IMV leaf. This is the form the pipeline calls after a swap.
- `partition_keys := '172'` (legacy CSV of LIST keys) — probes the top-level child whose constraint matches the key, then expands that (internal) node to its leaves. Backward-compatible, and now correct on sub-partitioned sources.

The swap engine fills each IMV leaf from `SELECT * FROM (base_query) WHERE (<pg_get_partition_constraintdef>)`; on a sub-leaf that constraint is the **full ancestral predicate** (`dem_plan_id = 172 AND order_date >= … AND order_date < …`), so the fill is exact. Because a leaf's immediate parent is an *internal node* (not the IMV root), the executor resolves each leaf's current immediate parent live (`read_immediate_parent_qual` via `pg_inherits`) and DETACH/ATTACHes against it — for single-level IMVs the immediate parent *is* the root, so behaviour is byte-identical to before.

### Capture pipeline: event trigger → enqueue → flush → oid-diff

1. **Event trigger** (`public.__reflex_on_ddl_command_end`, on `ddl_command_end`): for each `ALTER TABLE`/`CREATE TABLE` it resolves the affected relation's **partition root** via `pg_partition_root` (a multi-level attach reports an *intermediate* level, but IMVs depend on the top-level source). If that root is a registered source of a partitioned IMV and is **not** pg_reflex-owned, it enqueues the root into `public.__reflex_partition_pending`. The reflex-owned exclusion is mandatory: pg_reflex's own atomic swap DETACH/ATTACHes IMV partitions, and reacting to those would race the code-driven cascade.
2. **Flush** (`reflex_flush_partitions()` drains the queue; `reflex_flush_partition_source(root)` flushes one). For each dirty root it **oid-diffs** the live recursive leaf set against `public.__reflex_source_partition_snapshot`:

   | Diff result | Meaning | Action |
   |---|---|---|
   | name present, **oid changed** | same-bound swap (detach + attach a freshly-built table) | `SwapFill` — partition-scoped reconcile of that IMV leaf |
   | name new | attach-new | `AttachNew` — sync creates the IMV leaf, then swap-fills |
   | name gone | detach / remove | `Drop` — `DROP` the matching IMV leaf (O(1), no row scan) |
   | unchanged | no-op | skip |

   The oid change is what makes a *same-bound* swap detectable without parsing the DDL. **Drops are applied before attaches** within a flush: a rename-style swap yields `Drop(old)+AttachNew(new)` on the same bound, and attaching first would overlap the old leaf's range. After applying, the snapshot is refreshed and the pending row cleared. Dependent IMVs propagate through the existing `graph_child` cascade inside `reflex_reconcile_partition`.

3. **Snapshot key.** `__reflex_source_partition_snapshot` is keyed by a **canonical schema-qualified** root (`partition::canonical_root_key`) on both write (create-time seed, post-flush refresh) and read, so the bare anchor used at create (`fz`) and the `pg_partition_root` form enqueued by the trigger (`public.fz`) resolve to the same key.

Typical pipeline usage: after a batch of `DETACH`/`ATTACH` swaps, call `SELECT reflex_flush_partitions();` once (so intermediate states are not reconciled). A removal (`DETACH` then `DROP`/leave-detached) needs no special call — the flush sees the leaf gone and drops the IMV leaf.

### Lifecycle summary

- New month leaf attached under an existing `dem_plan_id` → flush creates + fills the matching IMV leaf.
- New `dem_plan_id` attached → flush (via sync) creates the internal node + its leaves, then fills.
- Source leaf detached/removed → flush drops the orphaned IMV leaf.
- The whole `dem_plan_id` regenerated → `reflex_reconcile_partition(view, source_partition := '…_p_172')` (or the flush, if swapped leaf-by-leaf).

### Correctness backstop and limitations

- **Audit drift-check** (`audit::checks_b_drift::PartitionTreeDrift`, surfaced by `reflex_audit(view)`): compares the source's recursive leaf set against the IMV's and flags any divergence — so a forgotten flush or any uncaptured vector is always *detectable*.
- **Known limitation:** `detach → modify the same table in place → re-attach the same table` is invisible to the oid-diff (the oid is unchanged). The supported pipeline pattern attaches a freshly-built table (new oid). For the in-place pattern, call `reflex_reconcile_partition(view, source_partition := '<leaf>')` explicitly; the audit check catches it otherwise.
- HASH at any level, and finer-than-top-level cascade to dependents, are out of scope (dependents reconcile at their own LIST level — correct, possibly coarser).

### Shallow mirroring (partition depth, 1.8.2)

An IMV may mirror **fewer** partition levels than its source has. The mirror depth is resolved at create time and stored in `__reflex_ivm_reference.partition_depth` (`NULL` = mirror the full source depth — the default, so pre-1.8.2 IMVs are unaffected):

- **Explicit `partition_by` is authoritative.** `partition_by => ARRAY['dem_plan_id']` on a `LIST(dem_plan_id) → RANGE(order_date)` source mirrors **only** `dem_plan_id` (depth 1). Declaring `ARRAY['dem_plan_id','order_date']` opts into both levels. Each declared level must be a bare projected output column in the unique key, matching the source's partition key top-down.
- **Auto-mirror prunes.** With `partition_by` omitted, auto-mirror keeps the longest prefix of source levels whose partition column is a **bare projected output column**, and stops at the first that isn't — instead of rejecting. This is what lets a `FULL JOIN`-coalesced key like `COALESCE(a.order_date, b.order_date) AS order_date` (which is not a bare column, so cannot be a partition level) mirror at `dem_plan_id` only.

The lever is one pure function, `partition::truncate_partition_tree(nodes, mirror_depth)`: it drops source nodes deeper than `mirror_depth` and **demotes** the depth-`mirror_depth` nodes to leaves (clears their sub-`PARTITION BY`). Every site that mirrors/syncs/snapshots the source tree feeds it through this, so create / `reflex_sync_partitions` / the audit drift-check all agree on shape — a shallow IMV is never re-deepened.

Capture stays **leaf-granular and maps up**: a sub-partition `DETACH`/`ATTACH` does not change the parent node's oid, so the snapshot keeps tracking the deepest source leaves; the changed leaf is mapped up to its depth-`mirror_depth` ancestor (via `ancestor_bare_at_depth` over the live tree, or the snapshot's `ancestors TEXT[]` column for a vanished leaf) and that whole top-level IMV partition is atomically refilled. Coarser than a month-granular swap, never incorrect. When `partition_depth` is `NULL`, `mirror_depth == leaf depth` and every up-map is the identity, so full-depth IMVs behave exactly as in 1.8.1.

#### Depth 0 — unpartitioned target on a partitioned source (1.8.3)

The floor of the ladder is an **unpartitioned** IMV on a partitioned source. Passing an **empty** `partition_by` (`ARRAY[]::text[]`, the `explicit_unpartitioned` flag) suppresses auto-mirror so the target is a plain table; omitting `partition_by` still auto-mirrors. Ordinary DML is captured by the source-root statement trigger as usual. Partition **swaps** fire no DML trigger and the IMV has no partitions to scope a reconcile, so capture is by **full reconcile**: the `ddl_command_end` event trigger enqueues the source root for *any* enabled dependent IMV (not only partitioned ones), and `reflex_flush_partitions` runs `reflex_reconcile(view)` for each unpartitioned dependent of a dirty root. This also closes a prior silent-staleness gap for any unpartitioned IMV on a swap-driven source. The cost is a whole-IMV reconcile per swap (no partition-scoped reader availability) — the documented tradeoff of opting out of partitioning.

## Per-IMV SAVEPOINT in DEFERRED flush

`reflex_flush_deferred` wraps each IMV's drain in its own SAVEPOINT. A failing IMV (e.g. constraint violation on the target) doesn't abort the whole transaction's cascade — the failure is recorded against that IMV and the next one runs. See [crash recovery](../operations/crash-recovery.md) and `__reflex_ivm_reference.last_error`.

## Where to look in the source

Cross-references for readers walking the code:

Referenced by symbol rather than line number — line numbers drift every
release; grep the symbol or hint to land on the current location.

| Topic | File · symbol / search hint |
|---|---|
| Empty-delta short-circuit | `src/schema_builder.rs` trigger body |
| Per-source DEFERRED-flush serialisation lock | `src/trigger/deferred.rs` · `reflex_flush_deferred` (lock key `reflex_flush:<source>`) |
| Per-IMV advisory lock (2-arg hash form) | `src/trigger/deferred.rs` · search `hashtext(reverse(` |
| MERGE codegen | `src/trigger/merge.rs` · `build_merge_sql` |
| Dispatch DO block | `src/trigger/dispatch.rs` · `build_high_selectivity_dispatch_sql` |
| Self-join full-refresh branch | `src/trigger/ops.rs` · `self_join_full_refresh_stmts` |
| Bulk-INSERT / Bulk-DELETE paths | `src/trigger/dispatch.rs` · `push_bulk_insert_and_affected`, `push_bulk_delete_via_transition` |
| Path B pre-scratch dispatch | `src/schema_builder.rs` trigger body (search `Path B: dispatching`) |
| Path C smart bulk-INSERT | `src/schema_builder.rs` · `path_c_for_update` in `build_trigger_ddls` |
| Path C EXPLAIN dispatch | `src/trigger/mod.rs` · `reflex_build_path_c_explain_sql` |
| TRUNCATE codegen | `src/trigger/mod.rs` · `reflex_build_truncate_sql` |
| `ignore_sources` runtime skip | `sql/trigger_body.plpgsql.in` + `sql/deferred_trigger_*.plpgsql.in` (search `ignored_sources`); flush side in `src/trigger/deferred.rs` · `reflex_flush_deferred` |
| `reflex_reconcile` TRUNCATE+INSERT | `src/reconcile.rs` · `reflex_reconcile` |
| Source-join-keys metadata | `src/aggregation.rs` (`source_join_keys` on `AggregationPlan`) |
| Partitioning module (introspect, sync, reconcile_partition) | `src/partition.rs` |
| Partition validation + auto-mirror | `src/create_ivm/mod.rs` (`resolve_partitioning`) |
| PARTITION BY clause in intermediate/target DDL | `src/schema_builder.rs:build_intermediate_table_ddl`, `:build_target_table_ddl` |

[Delta processing :material-arrow-right-bold:](delta-processing.md){ .md-button }
[Architecture tour (contributor) :material-arrow-right-bold:](../contributing/architecture-tour.md){ .md-button }
