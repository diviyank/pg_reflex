# Partitioning — Phase 2: atomic swap, per-partition trigger dispatch

## Context

`plans/partitioning_2.md` shipped the foundation: opt-in `partition_by`, per-IMV partition children, `reflex_sync_partitions`, `reflex_reconcile_partition`, auto-mirror. Two gaps remain:

1. **`reflex_reconcile_partition` holds `AccessExclusiveLock` on the rebuilt partition child for the full rebuild duration.** TRUNCATE + INSERT on the child means readers querying that partition block for minutes on large rebuilds. The reader-availability story for partition-scoped reconcile is therefore "block one partition instead of the whole IMV" — better than full reconcile, but still blocking.

2. **The trigger's Path B dispatch on partitioned IMVs is unaware of partitions.** A bulk write that hits one partition still trips the per-IMV ratio and calls `PERFORM reflex_reconcile(view)` — full IMV rebuild via TRUNCATE on the parent (cascades to all children). The partition-scoped path exists but is unreachable from the trigger.

This plan addresses both, with two additional gates:

- **Validation** to require partition columns to be bare-column references (not expressions), eliminating a whole class of trigger-codegen pitfalls.
- **Tier 2 partition derivation** so JOIN-secondary sources (e.g. `fact` joined to a partitioned `dim`) can dispatch per-partition too — not just the anchor source.

## Scope

Five additions, each independently committable:

1. **Atomic DETACH/ATTACH swap in `reflex_reconcile_partition_impl`** — collapses lock hold on the rebuilt child from "full rebuild duration" to "the ALTER TABLE swap itself" (~µs). The new partition is built outside the partition tree, then swapped atomically.

2. **Bare-column-reference validation for `partition_by`** — error out at `create_reflex_ivm` time when the partition column corresponds to a GROUP BY *expression* (e.g. `DATE_TRUNC('month', d)`) rather than a bare projected column. Allows the trigger codegen to find the partition key on transition tables by simple column reference.

3. **Per-partition trigger dispatch — 2-group shape, Tier 1 (anchor source)** — replaces the per-IMV ratio in `build_high_selectivity_dispatch_sql` with: compute per-partition affected counts via GROUP BY on the transition, partition-scoped reconcile for the hot keys, restrict the standard MERGE path to the remaining (cold) partitions via a WHERE filter. Trip-cap so a fully-hot workload falls back to global reconcile.

4. **Tier 2 — JOIN-mapped partition derivation for non-anchor sources** — at `create_reflex_ivm` time, compute and persist a per-source SQL fragment (`partition_join_path`) that derives the IMV's partition column from each source's transition table by JOINing to the anchor. Trigger uses this fragment to dispatch per-partition even when the source isn't the anchor.

5. **Bench harness** — `benchmarks/bench_partitioned_imv.sql` exercising (a) full reconcile, (b) manual partition-scoped reconcile, (c) trigger-dispatched per-partition path, (d) cascade with same-column dependents. The bench validates that all four phases actually deliver wins; if not, ship-or-revert per `feedback_optimization_approach`.

## Files to modify

- **`src/partition.rs`** — `reflex_reconcile_partition_impl` rewritten to use DETACH/ATTACH swap; new helper `build_swap_partition_ddl(view, child)`.
- **`src/aggregation.rs`** — `AggregationPlan` gains `partition_join_paths: HashMap<String, String>` (per-source SQL fragment, `#[serde(default)]`).
- **`src/create_ivm.rs`** — strict validation for partition_by (bare-column-ref check); per-source `partition_join_path` derivation by walking analysis JOIN graph.
- **`src/lib.rs`** — new catalog column `wipe_floor_rows BIGINT` (idempotent ADD COLUMN IF NOT EXISTS); helper `reflex_set_wipe_floor_rows(view, n)` analogous to `reflex_set_wipe_threshold`.
- **`src/schema_builder.rs:481+`** (or `src/trigger.rs:1048+`) — `build_high_selectivity_dispatch_sql` replaced (or wrapped) by a partition-aware variant when the plan is partitioned. Emits the GROUP-BY-on-transition probe, hot-key array, partition-reconcile call, and WHERE-filtered standard path.
- **`src/trigger.rs:65-284`** (`build_merge_sql` and friends) — accept an optional `restrict_partitions_where` parameter that splices a `WHERE <partition_col> NOT IN (…)` into the scratch fill, MERGE USING, target DELETE, target INSERT.
- **`benchmarks/bench_partitioned_imv.sql`** — new bench script.
- **`src/tests/pg_test_partition.rs`** — extended with swap correctness, trigger-dispatch tests.
- **`src/tests/pg_test_partition_dispatch.rs`** (new) — focused tests for the trigger-time per-partition dispatch + Tier 2 JOIN-mapped derivation.
- **`docs/concepts/delta-processing.md`** — lock table rows reflect the swap pattern.
- **`docs/concepts/internals.md`** — extend the "Partitioning" section.
- **`CHANGELOG.md`** — Unreleased entry.

## Existing utilities to reuse

- `crate::partition::{introspect_partition_descriptor, list_partition_children, resolve_anchor_source}` — partition introspection from Phase 1.
- `AggregationPlan.source_join_keys` (1.4.6) — already records per-source JOIN-key mappings; Tier 2 derivation builds on this.
- `WIPE_THRESHOLD_DEFAULT` + per-IMV `wipe_threshold` catalog column — same precedence chain used for `wipe_floor_rows`.
- `pg_get_partition_constraintdef` — already used by `reflex_reconcile_partition_impl` for the WHERE clause filter.
- `pg_advisory_xact_lock` — per-IMV serialization for the swap.

## Design

### 1. Atomic DETACH/ATTACH swap

Replace today's TRUNCATE+INSERT on the child with build-then-swap. New shape of `reflex_reconcile_partition_impl` per matching child:

```sql
-- Pre-flight: resolve old child OIDs + bounds.
SELECT pg_get_expr(relpartbound, oid) INTO _bound
    FROM pg_class WHERE oid = '<old_int_child>'::regclass;

-- 1. Build NEW partitions outside the partition tree (concurrent readers on
--    the old child still served).
CREATE UNLOGGED TABLE __reflex_swap_int_<view>_<src_child> (
    LIKE <intermediate_parent> INCLUDING ALL
);
INSERT INTO __reflex_swap_int_<view>_<src_child>
    SELECT * FROM (<base_query>) __src WHERE (<constraint_def>);

CREATE UNLOGGED TABLE __reflex_swap_tgt_<view>_<src_child> (
    LIKE <target_parent> INCLUDING ALL
);
INSERT INTO __reflex_swap_tgt_<view>_<src_child>
    SELECT * FROM (<end_query>) __end WHERE (<constraint_def>);

ANALYZE __reflex_swap_int_<view>_<src_child>;
ANALYZE __reflex_swap_tgt_<view>_<src_child>;

-- 2. Atomic swap inside the writer's transaction. ShareUpdateExclusiveLock on
--    the parent for DETACH+ATTACH (does NOT block SELECT on other children).
ALTER TABLE <intermediate_parent> DETACH PARTITION <old_int_child>;
ALTER TABLE <intermediate_parent>
    ATTACH PARTITION __reflex_swap_int_<view>_<src_child> FOR VALUES <bound>;

ALTER TABLE <target_parent> DETACH PARTITION <old_tgt_child>;
ALTER TABLE <target_parent>
    ATTACH PARTITION __reflex_swap_tgt_<view>_<src_child> FOR VALUES <bound>;

-- 3. Drop old data — no longer in the partition tree, so DROP TABLE only
--    locks the now-orphaned old children. Readers have already migrated to
--    the new children via PG's catalog-snapshot semantics.
DROP TABLE <old_int_child>;
DROP TABLE <old_tgt_child>;

-- 4. Rename the swap children to the canonical partition-child names so a
--    subsequent reconcile can find them.
ALTER TABLE __reflex_swap_int_<view>_<src_child>
    RENAME TO <intermediate_child_name>;
ALTER TABLE __reflex_swap_tgt_<view>_<src_child>
    RENAME TO <target_child_name>;
```

#### Lock analysis

| Step | Lock taken | Conflicts with |
|------|-----------|----------------|
| 1 (build new) | AccessExclusive on new (orphan) tables only | nothing user-visible |
| 2 (DETACH+ATTACH on parent) | ShareUpdateExclusive on parent | other DDL on parent; **not** SELECT, **not** RowExclusive on children |
| 2 (DETACH+ATTACH on children) | AccessExclusive on old + new children for the swap duration (µs) | reads of those specific children for that instant |
| 3 (DROP old children) | AccessExclusive on the now-detached old children | nothing (they're orphaned) |

Net effect: a reader doing `SELECT … FROM <view> WHERE <partition_col> = X` continues to see the OLD child's data until the swap commits, then sees the NEW child's data on the next snapshot. Block time on the partition being rebuilt drops from "full rebuild duration" to "the ALTER TABLE swap itself" — measured in µs in PG. Block time on *other* partitions is the brief ShareUpdateExclusiveLock on the parent — also µs.

#### Failure handling

| Failure point | Recovery |
|---|---|
| `CREATE TABLE … LIKE` fails | nothing changed; orphan cleanup not needed |
| `INSERT INTO swap` fails | DROP swap tables in the exception handler; old children untouched |
| First `DETACH` succeeds, second fails | re-ATTACH the first child (catalog still consistent); error out |
| `ATTACH` fails (bound mismatch — should never happen since we copied `pg_get_expr`) | DETACH was already done; need to re-ATTACH old child; abort |
| `DROP TABLE old` fails | retry; the partition is already detached so it's not in user's view path |
| `RENAME TO` fails (name conflict — would mean another concurrent swap raced) | abort; next reconcile picks up the unrenamed swap table as the canonical child |

All recoverable via a single PL/pgSQL exception block. The most subtle case is the second DETACH failing — must re-ATTACH the first child to keep the IMV's catalog consistent with the data. Implement as a small SAVEPOINT/ROLLBACK TO SAVEPOINT around the two DETACHes + two ATTACHes.

#### Naming + idempotent recovery (resolved Q1)

Decision: keep the canonical child name (`__reflex_intermediate_<view>_<src_child>`) and RENAME the swap table to it after DROP. The DROP-then-RENAME order has one failure mode worth handling: if DROP-old fails (e.g. another session still holds a lock on the orphaned old child), the RENAME can't proceed and the next reconcile arrives to find an extra `__reflex_swap_*` table sitting next to the still-present old child.

Mitigation: every entry to `reflex_reconcile_partition_impl` runs a one-line cleanup pass before touching anything else — DROP any `__reflex_swap_<view>_<child>` tables that exist for this view (they're known-orphan from a prior failed swap). Then proceed with the new swap. Idempotent, no catalog state needed; the swap-name prefix is the signal.

If both the DROP and the post-recovery cleanup fail (e.g. persistent reader lock on the old child), the swap aborts and the reconcile call returns an error message naming the blocking lock — operator-recoverable via `pg_terminate_backend` or by waiting out the holder. Old child is still attached and serving consistent data, so the IMV stays correct.

#### Storage cost

Temporarily 2× partition storage during steps 1–3. Acceptable for partition-sized data (vs. 2× full-IMV during today's `reflex_reconcile` rebuild path on unpartitioned IMVs, where it's a much larger swing).

#### Cascade ordering

Cascade to dependent IMVs runs *after* the swap commits in the catalog snapshot visible to the same SPI transaction. PG handles this — `reflex_reconcile_partition(dep, keys)` called from within the same `Spi::connect_mut` block sees the new children. Verified mentally; needs an integration test (`pg_part_cascade_after_swap_sees_new_data`).

#### Why not `DETACH PARTITION CONCURRENTLY`?

PG14+'s `DETACH PARTITION … CONCURRENTLY` takes only `ShareUpdateExclusiveLock` and waits for in-flight transactions to drain — strictly better for reader-availability. But it requires *two* transactions (one to mark the partition pending detach, one to finalize). Our SPI flow is single-transaction. Adopting CONCURRENTLY would require splitting `reflex_reconcile_partition` into a two-phase commit pattern — significant rework. Defer to a follow-up if measurements show the non-CONCURRENTLY ShareUpdateExclusiveLock is materially problematic.

### 2. Bare-column-reference validation for `partition_by`

At `create_reflex_ivm_impl`, after building the `AggregationPlan` but before any partition resolution, walk each `partition_by` entry against `analysis.group_by` (the AST nodes, not the lexical strings). Reject when the corresponding GROUP BY entry is not an `Expr::Identifier` or `Expr::CompoundIdentifier` (i.e. anything wrapped in a function call, cast, arithmetic, etc).

```rust
// In create_reflex_ivm_impl, replace the existing lexical check:
for col in &resolved_partition_cols {
    let matched_gb_expr = analysis.group_by.iter().find(|gb| {
        // … match by name → either bare alias or normalized column name
    });
    let Some(gb_expr) = matched_gb_expr else {
        return /* existing not-in-GROUP-BY error */;
    };
    if !is_bare_column_reference(gb_expr) {
        return Box::leak(format!(
            "ERROR: partition_by column '{}' corresponds to a computed \
             GROUP BY expression. Partition columns must be bare column \
             references on the source. Workaround: add a generated/computed \
             column to the source and partition on that.", col).into_boxed_str());
    }
}
```

`is_bare_column_reference(expr)` is a one-line match — `Expr::Identifier(_) | Expr::CompoundIdentifier(_)`. Add the helper to `sql_analyzer.rs`.

Auto-mirror is already safe by construction (it derives partition cols from the source's `pg_partitioned_table.partattrs`, which are always real columns on the source).

#### Test coverage

- `pg_part_explicit_computed_partition_by_errors` — `partition_by => ARRAY['month']` on `GROUP BY DATE_TRUNC('month', d)` returns the error message.
- `pg_part_explicit_bare_partition_by_accepted` — `partition_by => ARRAY['region']` on `GROUP BY region` works.

### 3. Per-partition trigger dispatch — 2-group, Tier 1

Add a **sibling function** `build_partition_aware_dispatch_sql` next to `build_high_selectivity_dispatch_sql`. The trigger body picks one or the other based on whether the plan is partitioned AND the firing source qualifies (Tier 1: anchor source; Tier 2 in §4: non-anchor with a JOIN path). Unpartitioned plans keep the existing function unchanged — byte-for-byte (resolved Q2).

Rationale for the split rather than extending the existing function with a flag: the existing function is 75 lines of careful string construction; doubling its branches via a flag tangles the two state machines and makes the partitioned vs unpartitioned codegen harder to unit-test in isolation. A sibling function shares helpers (e.g. WHERE-filter splicing) but keeps the top-level shape readable.

The new function emits:

```sql
DO $reflex_dispatch$
DECLARE
    _thr NUMERIC;
    _per_imv NUMERIC;
    _floor BIGINT;
    _per_imv_floor BIGINT;
    _hot_keys TEXT[];
    _cold_count BIGINT;
    _hot_count BIGINT;
    _partition_total INT;
BEGIN
    -- Resolve threshold + floor (per-IMV → GUC → compiled default).
    SELECT wipe_threshold, wipe_floor_rows INTO _per_imv, _per_imv_floor
        FROM public.__reflex_ivm_reference WHERE name = '<view>';
    _thr   := COALESCE(_per_imv,       current_setting('reflex.wipe_threshold', true)::NUMERIC, {default_thr});
    _floor := COALESCE(_per_imv_floor, current_setting('reflex.wipe_floor_rows', true)::BIGINT, {default_floor});

    -- 1. Compute per-partition affected counts via the transition table.
    CREATE TEMP TABLE __per_partition_dispatch ON COMMIT DROP AS
    SELECT
        <partition_col> AS pkey,
        count(*)        AS dirty
    FROM <affected_groups_table>           -- already populated upstream
    GROUP BY <partition_col>;

    -- 2. Decide hot vs cold per partition.
    SELECT array_agg(pp.pkey::text)
      INTO _hot_keys
      FROM __per_partition_dispatch pp
      JOIN pg_inherits i ON i.inhparent = '<intermediate_parent>'::regclass
      JOIN pg_class c ON c.oid = i.inhrelid
                     AND c.relname = '<intermediate_child_for_pkey(pp.pkey)>'
     WHERE pp.dirty::NUMERIC
           / GREATEST(c.reltuples::NUMERIC, _floor::NUMERIC)
           >= _thr
       AND c.reltuples >= 100;   -- skip-ratio short-circuit

    SELECT count(*) INTO _partition_total
        FROM pg_inherits WHERE inhparent = '<intermediate_parent>'::regclass;

    _hot_count  := COALESCE(array_length(_hot_keys, 1), 0);
    _cold_count := _partition_total - _hot_count;

    -- 3. Trip-cap: if too many partitions are hot, fall back to global.
    IF _hot_count > _partition_total / 2 THEN
        RAISE DEBUG 'pg_reflex: % hot of % partitions — global reconcile', _hot_count, _partition_total;
        PERFORM public.reflex_reconcile('<view>');
        RETURN;
    END IF;

    -- 4. Hot partitions → partition-scoped reconcile (uses the atomic swap).
    IF _hot_count > 0 THEN
        PERFORM public.reflex_reconcile_partition(
            '<view>', array_to_string(_hot_keys, ','));
    END IF;

    -- 5. Cold partitions → standard MERGE/scratch path, restricted by WHERE.
    IF _cold_count > 0 THEN
        EXECUTE $reflex_inner${merge_sql_with_partition_filter}$reflex_inner$;
        EXECUTE 'ANALYZE <intermediate>';
        EXECUTE $reflex_inner${target_delete_with_partition_filter}$reflex_inner$;
        EXECUTE $reflex_inner${target_insert_with_partition_filter}$reflex_inner$;
    END IF;
END
$reflex_dispatch$;
```

#### Computing `intermediate_child_for_pkey(pp.pkey)`

LIST case: child name is deterministic from the source's bare child name (`__reflex_intermediate_<view>_<src_child>`). Reverse mapping: query `pg_inherits` + `pg_get_partition_constraintdef` to find which child's bound covers each pkey. The same `substitute_identifier` trick used in `reflex_reconcile_partition_impl` works here. RANGE case: same approach (constraint-def boolean test).

To avoid running N constraint-tests per trigger, cache the (pkey → child_relname) map once at trigger fire time:

```sql
CREATE TEMP TABLE __pkey_child_map ON COMMIT DROP AS
SELECT
    pp.pkey,
    (SELECT c.relname
       FROM pg_inherits i
       JOIN pg_class c ON c.oid = i.inhrelid
      WHERE i.inhparent = '<intermediate_parent>'::regclass
        AND pp.pkey::text = ANY(/* enumerate IN-bound or test RANGE-bound */)
      LIMIT 1) AS child_relname
FROM __per_partition_dispatch pp;
```

LIST simplification: `relpartbound` for LIST is a small array; ANY() works directly. RANGE: more involved, may need `pg_get_partition_constraintdef` + EXECUTE. **Recommendation**: ship LIST-fast-path first (covers most discrete-key workloads), defer RANGE-fast-path. RANGE partitions fall back to global Path B for the per-partition dispatch decision (still correct, just less optimized).

#### WHERE-filter splicing into the standard path

The standard `merge_sql`, `target_delete_sql`, `target_insert_sql` all reference the `<affected_groups_table>` (which has the partition column among its group-by columns). Splicing `WHERE <partition_col> NOT IN (SELECT unnest(_hot_keys::text[]))` against that table:

```sql
-- BEFORE
MERGE INTO __reflex_intermediate_v t
USING __reflex_delta_v d ON t.region IS NOT DISTINCT FROM d.region
… ;

-- AFTER (partition-filtered)
MERGE INTO __reflex_intermediate_v t
USING (SELECT * FROM __reflex_delta_v WHERE region NOT IN (SELECT unnest(_hot_keys::text[]))) d
ON t.region IS NOT DISTINCT FROM d.region
… ;
```

Same wrapping for the target DELETE / INSERT (filter on the partition column in the affected-groups subquery). Implementation: add a new `restrict_partitions_where: Option<String>` parameter to `build_merge_using`, `build_target_delete_sql`, `build_target_insert_sql` — when set, wraps the USING / IN subqueries with the filter.

Subtle correctness gate: the filter is on the IMV-side partition column (which equals one of the GROUP BY cols by design). A delta row in the affected-groups table belongs to exactly one partition. So filtering at the affected-groups level is equivalent to filtering at the intermediate-row level. No risk of dropping a row that should be processed.

#### Stats mitigations (Q4 recommendations)

- `GREATEST(c.reltuples::NUMERIC, _floor::NUMERIC)` — floor on denominator (Q4 #1).
- `AND c.reltuples >= 100` in the hot-key filter — skip-ratio short-circuit (Q4 #2).
- `_per_imv_floor` from per-IMV `wipe_floor_rows` catalog column → GUC → compiled default — operator override (Q4 #3).

Compiled default for `wipe_floor_rows`: 1000. Rationale: 1000-row partition is small enough that incremental MERGE is faster than swap-based reconcile regardless of ratio. Operator can override per-IMV when the shape is unusual.

#### Trip-cap rationale

If hot_count > total_partitions / 2, sequentially holding AccessExclusiveLock on > half the partitions during DETACH/ATTACH is worse than one global reconcile. The cap is per-trigger-fire, not persistent — next fire re-evaluates.

#### When this codegen is emitted

Only when the plan is partitioned AND the firing source is the anchor (Tier 1). For non-partitioned plans, emit today's `build_high_selectivity_dispatch_sql` unchanged. For non-anchor sources on a partitioned plan, Tier 1 falls through to today's global Path B; Tier 2 (next section) adds the per-partition dispatch via JOIN-derived partition keys.

### 4. Tier 2 — JOIN-mapped partition derivation for non-anchor sources

When a fact source has a JOIN to the anchor dim, the trigger can derive partition keys for the fact's transition rows by JOINing transition → dim at trigger time.

#### Metadata at create-time

Extend `AggregationPlan` with:

```rust
/// Per-source SQL fragment that derives the IMV's partition column
/// from this source's transition table. None for the anchor source itself
/// (which projects the partition column directly via its transition table).
/// For non-anchor sources, the fragment is a SELECT that yields a single
/// column `pkey` keyed to the transition rows.
#[serde(default)]
pub partition_join_paths: std::collections::HashMap<String, String>,
```

Population logic in `create_reflex_ivm_impl`, after `build_source_join_keys` runs:

For each real source S ≠ anchor:
1. Look up `source_join_keys[S]` — vec of `(intermediate_col, source_col)` already computed.
2. Find the (source_col, anchor_col) pair where `intermediate_col` is the partition column. If none, S has no JOIN path to the partition key → leave empty (falls back to global Path B at trigger time).
3. Otherwise emit:
   ```sql
   SELECT a."{partition_col}" AS pkey, t.*
     FROM {transition_alias} t
     JOIN {anchor} a ON a."{anchor_col}" = t."{source_col}"
   ```

   Persist this fragment in `partition_join_paths[S]`. The trigger body substitutes `{transition_alias}` with the actual transition table name at fire time.

Restriction: only one-hop JOINs are supported in v1. Multi-hop (`fact → bridge → dim`) requires walking the JOIN graph — defer.

Validation: if a source has no JOIN path to the anchor's partition column (e.g. the source is an unrelated outer-join secondary), `partition_join_paths[S]` stays empty. The trigger logic for S simply falls through to the existing global Path B — safe.

#### Trigger codegen for Tier 2

When the firing source S is non-anchor AND `partition_join_paths[S]` is set, emit:

```sql
CREATE TEMP TABLE __per_partition_dispatch ON COMMIT DROP AS
SELECT pkey, count(*) AS dirty FROM (
    {partition_join_paths[S]}  -- substitutes transition_alias = NEW table
) __derive
GROUP BY pkey;

-- … same hot/cold logic as Tier 1 from here onward.
```

The fragment's JOIN runs against the anchor's *current state* in the writer's snapshot — which is correct because (a) the trigger fires AFTER STATEMENT, so the anchor reflects any same-transaction changes, and (b) any update to the anchor that moved a key between partitions already fired the anchor's own trigger (which dispatches independently and moves the corresponding intermediate rows).

Cost: one JOIN per trigger fire on S. For dim tables with indexed PK, negligible. For large secondaries, the JOIN could dominate. Guard: skip Tier 2 dispatch when EXPLAIN row estimate for the JOIN exceeds a per-IMV cap.

**Cap shape (resolved Q3): absolute, with per-IMV override.** Default cap is `100_000` rows. A new catalog column `partition_dispatch_cost_cap BIGINT` (NULLable) on `__reflex_ivm_reference` lets operators raise or lower the cap for IMVs whose shape diverges (e.g. small IMVs where 100k > the entire intermediate). Set via a `reflex_set_partition_dispatch_cost_cap(view, n)` helper, same precedence pattern as `wipe_threshold` / `wipe_floor_rows`. We pick absolute over relative because (a) it's predictable from a fixed-cost standpoint — the cap is "how much JOIN am I willing to pay per trigger fire", which is operator-meaningful in isolation — and (b) the per-IMV override gives the shape-aware tuning when the default is wrong, without making every fire's decision relative to a moving `reltuples` denominator.

**EXPLAIN frequency (resolved Q4): per fire, not cached.** Run the EXPLAIN inline in the trigger body on every Tier 2 fire. Cost is in the low-ms range for the small JOINs Tier 2 targets; for the JOINs that would dominate we skip them via the cap anyway. Caching the EXPLAIN per (view, source) would need invalidation on ANALYZE / data-shape shifts — a tax that's bigger than the saving for the workloads Tier 2 fits. Revisit if measurements show per-fire EXPLAIN dominating; the change to a cached version is local.

The EXPLAIN-based cap reuses the Path C machinery from 1.5.0 (`reflex_build_path_c_explain_sql`).

#### Tier 2 correctness corner case

Concurrent anchor update + fact transition referencing the updated anchor row:

- T1: `UPDATE dim SET region = 'NEW' WHERE id = 5` → fires dim's trigger → moves intermediate rows for `region='OLD'` to `region='NEW'` (existing 1.5.x behaviour, predates this plan).
- T2: `INSERT INTO fact (dim_id, val) VALUES (5, 100)` → fires fact's trigger → Tier 2 derivation JOINs to dim, sees `region='NEW'` → dispatches per-partition for `'NEW'`.

Both T1 and T2 are serializable; the per-partition dispatch sees the post-T1 anchor state. Correct.

But: if T1 hasn't fired yet (e.g. fact's INSERT happened in the same statement before dim's UPDATE), the Tier 2 derivation reads the OLD anchor state, dispatches to OLD partition. T1 later moves OLD→NEW intermediate rows. Final state: intermediate has the fact's contribution in NEW correctly (because T1 saw the post-INSERT fact row). No correctness bug, just sub-optimal per-partition routing for that particular fire.

This case is rare enough (same-transaction interleaved dim UPDATE + fact INSERT) that the simplification is worth it. Document but don't try to solve.

### 5. Bench harness

`benchmarks/bench_partitioned_imv.sql` — measures the cumulative win across the four phases. Each scenario uses a 4-partition LIST setup with 10M rows total (2.5M per partition).

Scenarios:

1. **Manual `reflex_reconcile_partition('view', 'A')`** — measures swap vs old TRUNCATE+INSERT.
   - Pre-Phase 1: full reconcile = T_full
   - Phase 1 manual partition reconcile (TRUNCATE+INSERT): T_full/4 + lock duration
   - **Phase 2 atomic swap**: T_full/4 + lock duration ~ µs
   - Expected: equivalent wall-clock, vastly better reader-block scope.

2. **Bulk INSERT to one partition** — measures trigger dispatch path.
   - Phase 1: trips per-IMV Path B → `reflex_reconcile(view)` (full rebuild).
   - **Phase 3+4 (Tier 1, anchor source)**: trips per-partition dispatch → `reflex_reconcile_partition(view, 'A')`.
   - Expected: ~4× faster (rebuild only the affected partition).

3. **Bulk INSERT into JOIN-secondary** — measures Tier 2.
   - Setup: `fact JOIN partitioned_dim ON fact.dim_id = dim.id`, partitioned on `dim.region`.
   - Phase 1+3 (Tier 1 only): falls back to global Path B → full reconcile.
   - **Phase 4 Tier 2**: JOIN-derives partition key, per-partition dispatch.
   - Expected: ~4× faster for partition-concentrated bulk writes on fact.

4. **Mixed-partition bulk + trickle** — measures 2-group dispatch.
   - 1M rows to partition A (hot), 1k rows to each of B, C, D (cold).
   - Phase 1: per-IMV ratio trips → full reconcile.
   - **Phase 3 2-group**: partition-reconcile A, MERGE for B+C+D restricted by WHERE.
   - Expected: ~4× faster than full reconcile; equivalent to Phase 3 scenario 2 plus the cost of one filtered MERGE pass.

5. **All-hot-partitions** — measures the trip-cap fallback.
   - 2M rows to each of A, B, C, D.
   - 2-group dispatch sees 4/4 hot → trips cap → falls back to global reconcile.
   - Expected: equivalent to full reconcile (the cap is the right call here).

6. **Cascade post-swap** — measures cascade correctness.
   - Two-IMV chain B depends on A, both partitioned on the same key.
   - Partition-reconcile A on key X → A swap commits → B's cascade sees new A data.
   - **Randomized-mutation oracle (resolved Q5)**: rather than a fixed-input assert, drive the scenario with a small randomized workload — pick a random sequence of (INSERT, UPDATE, DELETE) on the source spanning multiple partitions, interleave with partition-reconcile calls, then assert `SELECT * FROM B EXCEPT ALL SELECT … FROM (B's ground-truth-recomputed-from-source)` returns zero rows. The catalog-snapshot ordering edge cases that the cascade-post-swap path is most likely to expose (e.g. dependent reads stale A data because the cascade ran before the swap committed visibly) are exactly the kind of bugs a fixed-input test misses. Other scenarios (1–5) keep fixed-input asserts — the cost of randomized oracles isn't justified there.

Pass/fail criteria:
- Scenario 1 (swap): equivalent wall-clock to old TRUNCATE+INSERT (within 20%). Reader-block scope: 0 for scenario 6 cascade reader during the swap (measured via concurrent SELECT thread).
- Scenarios 2, 3, 4: ≥ 3× faster than Phase 1 baseline.
- Scenario 5: equivalent to full reconcile (within 20%).
- Scenario 6: cascade produces correct counts.

If 2 or 3 fail to deliver the expected speedup, revisit the design before shipping. The whole feature's value hinges on the trigger dispatch.

## Verification

### Unit tests (pure Rust)

- `partition::tests::test_build_swap_partition_ddl_shape` — DDL string shape for swap.
- `partition::tests::test_partition_join_path_emits_correct_join_for_one_hop_dim`
- `create_ivm::tests::test_bare_column_ref_validation_rejects_function_wrapped_group_by`
- `schema_builder::tests::test_dispatch_emits_per_partition_block_only_for_partitioned_plans`
- `trigger::tests::test_merge_using_accepts_restrict_partitions_filter`

### Integration tests (`#[pg_test]`)

In addition to Phase 1's tests:

- `pg_part_reconcile_partition_swap_concurrent_reader_unblocked` — two sessions; bulk reconcile of partition A in session 1, concurrent `SELECT … WHERE pkey = B` in session 2 returns without blocking, and `SELECT … WHERE pkey = A` blocks only for the swap window (measured < 100ms even on a large rebuild). Two-session SPI test using `dblink` or pgrx's parallel test pattern.
- `pg_part_explicit_computed_partition_by_errors` — see §2.
- `pg_part_trigger_dispatch_routes_bulk_to_partition_scoped` — bulk INSERT to one partition; assert via `pg_stat_user_tables.n_tup_ins` that only the hot partition's child was touched (other children's relfilenodes unchanged).
- `pg_part_trigger_dispatch_two_group_handles_mixed_workload` — bulk + trickle in one statement; assert partition-reconcile fired for the bulk side and MERGE filter restricted the rest.
- `pg_part_trigger_dispatch_trip_cap_falls_back_to_global` — all-hot scenario; assert global `reflex_reconcile` was called.
- `pg_part_tier2_join_derived_dispatch_on_fact_source` — fact bulk INSERT, dim partitioned; assert per-partition dispatch fired on fact's trigger.
- `pg_part_tier2_falls_back_when_no_join_path` — source with no JOIN to anchor (e.g. outer-join secondary); assert global Path B was used.
- `pg_part_swap_failure_rollback` — inject a failure during the second DETACH (via a temporary constraint); assert old partition is re-ATTACHed and IMV stays consistent.
- `pg_part_cascade_after_swap_sees_new_data` — two-IMV chain, partition-reconcile parent, assert child sees new data via cascade.

### Bench

`benchmarks/bench_partitioned_imv.sql` per §5. Each scenario reports wall-clock + reader-block measurements. Compared against `benchmarks/bench_sop_forecast.sql` (existing unpartitioned baseline) to confirm no regression on unpartitioned IMVs.

### CI gates

`cargo pgrx check && cargo pgrx test && cargo clippy && cargo fmt --check` — all green.

## Phasing

Five phases, each independently committable + revertable + benchable:

**Phase A — Atomic swap** (~1 week)
- §1: rewrite `reflex_reconcile_partition_impl` to use DETACH/ATTACH swap.
- Tests: swap correctness, failure rollback, cascade post-swap, reader-unblock (two-session).
- Bench: scenario 1 (swap vs TRUNCATE+INSERT). Ship gate: scope of reader-block on other partitions = 0.

**Phase B — Bare-column-ref validation** (~1 day)
- §2: add `is_bare_column_reference` check, error message.
- Tests: explicit-computed rejection + explicit-bare acceptance.
- No bench needed.

**Phase C — Trigger dispatch (Tier 1, anchor source, LIST)** (~2 weeks)
- §3: new dispatch DO block + WHERE-filter splicing into MERGE / target paths.
- Stats mitigations + `wipe_floor_rows` catalog column + setter helper.
- LIST-fast-path only; RANGE partitions on the anchor still get global Path B.
- Tests: trigger-dispatch routing, 2-group correctness, trip-cap fallback.
- Bench: scenarios 2, 4, 5. Ship gate: scenario 2 ≥ 3× faster than baseline; scenarios 4, 5 deliver expected shape.

**Phase D — Tier 2 JOIN-mapped derivation** (~1 week)
- §4: `partition_join_paths` metadata, EXPLAIN-cost-cap gate, fact-source dispatch codegen.
- Tests: Tier 2 routing on JOIN-secondary, fallback when no JOIN path, EXPLAIN-cap fallback.
- Bench: scenario 3. Ship gate: scenario 3 ≥ 3× faster on the fact-bulk case.

**Phase E — Docs + bench harness** (~3 days)
- Update lock table in `delta-processing.md` (swap row), extend `internals.md` Partitioning section.
- CHANGELOG Unreleased entry.
- Land `benchmarks/bench_partitioned_imv.sql` as a reproducible artifact.

Phases A and B can land independently (and probably should). Phase C blocks on B (codegen relies on bare-column-ref assumption). Phase D blocks on C. Phase E is concurrent with D.

## Out of scope

- `DETACH PARTITION CONCURRENTLY` — requires two-transaction split; defer.
- RANGE-partitioned-key trigger-dispatch fast path (only LIST in Phase C). RANGE falls back to global Path B at trigger time but still benefits from manual `reflex_reconcile_partition` (Phase A).
- Multi-hop JOIN derivation in Tier 2 (`fact → bridge → dim`) — single-hop only.
- HASH partitioning — still deferred from Phase 1.
- Repartitioning a live IMV without rebuild — still `drop_reflex_ivm` + recreate.
- Cross-column cascade mapping — still falls back to full reconcile.
- Trigger-side ANALYZE before dispatch — chicken-and-egg, not worth the cost (Q4 mitigation rejected).
- `pg_stat_user_tables.n_live_tup` fallback for `reltuples=0` partitions — depends on `track_counts` GUC, marginal accuracy gain (Q4 mitigation rejected).
- Operator-runbook automation for autovacuum tuning of partition children — doc paragraph only, no code.

## Resolved decisions

The five design choices that were open during plan drafting are now resolved and folded into the relevant sections above. Summary:

1. **Phase A swap naming + recovery** (§1 "Naming + idempotent recovery"): keep the canonical child name; RENAME the swap table to it after DROP-old; idempotent cleanup pass at every `reflex_reconcile_partition_impl` entry drops any leftover `__reflex_swap_*` tables from prior failed swaps.
2. **Phase C codegen split** (§3 intro): sibling function `build_partition_aware_dispatch_sql` alongside the existing `build_high_selectivity_dispatch_sql`. Unpartitioned plans keep the existing function byte-for-byte.
3. **Tier 2 cost cap shape** (§4): absolute cap of 100k row estimate, with per-IMV `partition_dispatch_cost_cap BIGINT` catalog column override (same precedence pattern as `wipe_threshold`).
4. **Tier 2 EXPLAIN frequency** (§4): per fire, not cached. Cached version is a local change if measurements later show per-fire EXPLAIN dominates.
5. **Bench correctness oracle** (§5 scenario 6): randomized-mutation `EXCEPT ALL` oracle for cascade-post-swap only; fixed-input asserts for the other five scenarios.

All resolved decisions are reflected inline in the plan above — this section is a tracking summary, not a separate specification.
