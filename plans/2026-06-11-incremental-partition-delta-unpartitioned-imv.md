# Incremental partition delta for unpartitioned IMVs

**Date:** 2026-06-11
**Status:** Design approved — TDD in progress

## Problem

Creating or attaching a partition on a `LIST`/`RANGE`-partitioned source forces a
**full `reflex_reconcile`** of every *unpartitioned* IMV that depends on it
(`reflex_flush_partitions_impl`, the unpartitioned branch at
`src/partition.rs:2055-2081`):

```rust
for imv in &unpartitioned_imvs {
    root_stmts.push(format!("PERFORM public.reflex_reconcile({})", sql_literal_text(imv)));
}
```

`reflex_reconcile` is `TRUNCATE` + `INSERT INTO target <base_query>`. The `TRUNCATE`
fires each child IMV's `AFTER TRUNCATE` trigger (`reflex_build_truncate_sql` →
full `DELETE`/`TRUNCATE` of the child, **no partition scoping**), and the full
`INSERT` re-presents the *entire* dataset to each child's INSERT trigger. So the
whole downstream graph rebuilds fully — even when the partition change adds **no
rows that the IMV actually keeps**.

### Concrete case (`base-db-anchor-evm` / db_clone)

- `assortment_activity_relation` is `LIST (assortment_id)` partitioned.
- `current_assortment_activity_view` is **unpartitioned**, depends on it directly,
  and filters `WHERE assortment_id = (SELECT assortment_id FROM sop_current_view)`.
- It has 6 direct children feeding ~20 heavy `sop_*` / `forecast_*` / `stock_*` IMVs.

Attaching a partition for a **non-current** assortment yields zero rows in
`current_assortment_activity_view` (everything filtered out), yet still detonates a
full rebuild of the entire downstream subtree. That is the cost this change removes.

## Key insight

`reflex_reconcile`'s `TRUNCATE`+full-`INSERT` is **not write-proportional**: it always
writes the full dataset, so the trigger-driven cascade always runs in full,
regardless of net change.

A partition change is semantically a bulk DML on the source:

- **ATTACH-with-data / `CREATE TABLE … PARTITION OF` with data** ≡ bulk `INSERT`.
- **DETACH** ≡ bulk `DELETE`.

If we feed the attached/detached partition child's rows through the IMV's **normal
incremental maintenance path** (the same `reflex_build_delta_sql` pipeline the
INSERT/DELETE triggers use), then:

- Propagation becomes **write-driven**: a child only fires when its parent's target
  is actually written. A net-zero delta writes nothing → no child trigger → the
  cascade **dies at whatever depth the delta nets to zero**, via the existing
  empty-transition early-exit, the `where_predicate` pred-check (1.10.2), and the
  UPDATE multiset-equality filter-skip (1.4.5).
- All of `build_delta_sql`'s existing correctness (joins, aggregates, self-join /
  outer-join-secondary handling) is reused — because this **is** the DML path,
  exercised exactly as a real bulk INSERT/DELETE into the source would exercise it.

## Design

### New function: `reflex_apply_partition_delta(imv, source, op, child_table)`

SQL-callable, generic over `(imv, source)`. Mirrors `sql/trigger_body.plpgsql.in`
but reads `where_predicate` / `wipe_threshold` from `__reflex_ivm_reference` at
runtime instead of via compile-time slots. Steps:

1. **Synthesize the transition table.** Create a `TEMP` table named
   `transition_new_table_name(source)` (op = `INSERT`) or
   `transition_old_table_name(source)` (op = `DELETE`), populated
   `AS SELECT * FROM <child_table>`. These are the conventional names
   `reflex_build_delta_sql` reads from (`src/trigger/mod.rs:104-107`).
2. **Pred-check skip (no-op short-circuit).** If `where_predicate IS NOT NULL` and
   `NOT EXISTS(SELECT 1 FROM <transition> WHERE <where_predicate> LIMIT 1)` → drop
   the transition and return `SKIPPED`. No write → no cascade. O(1) win for
   non-current assortments. (When `where_predicate` is NULL the step is skipped;
   correctness still holds — the delta SQL filters anyway — but the scan is paid.)
3. **Path B ratio dispatch.** If `source reltuples ≥ 1000` and
   `|transition| / |source| ≥ COALESCE(per-IMV wipe_threshold, reflex.wipe_threshold, 0.5)`
   → `PERFORM reflex_reconcile(imv)` and return. Same decision a real bulk INSERT
   makes for a large transition.
4. **Build + execute delta.** `_sql := reflex_build_delta_sql(imv, source, op,
   base_query, end_query, aggregations, base_query)`. If `_sql = ''` (the
   "unsupported / needs targeted reconcile" signal, e.g. FULL-JOIN secondary) →
   `PERFORM reflex_reconcile(imv)` (safe). Else `reflex_execute_separated(_sql)`.
   The MERGE writes only the real delta → children fire on only that delta.
5. **Drop the transition table.**

Advisory-lock the IMV (`pg_advisory_xact_lock(hashtext(imv), hashtext(reverse(imv)))`)
as the trigger body does, so concurrent maintenance on the same IMV serializes.

### Wiring: `flush_impl` unpartitioned branch (`src/partition.rs:2055-2081`)

Replace the unconditional `PERFORM reflex_reconcile(imv)` with, per changed
partition (from the already-computed `actions` / live tree / snapshot):

| Action | Statement |
|---|---|
| `AttachNew` | `reflex_apply_partition_delta(imv, root, 'INSERT', <live_child>)` |
| `Drop`, detached child accessible | `reflex_apply_partition_delta(imv, root, 'DELETE', <detached_child>)` |
| `Drop`, child gone/inaccessible | `reflex_reconcile(imv)` (conservative) |
| `SwapFill` | `reflex_reconcile(imv)` (full reconcile fallback, v1) |

All statements stay inside the existing per-root subtransaction (the `DO` block at
`src/partition.rs:2108`); snapshot refresh + pending drain are unchanged. The
trigger hot path (`trigger_body.plpgsql.in`) is **not** modified.

### Eligibility

All IMVs are eligible. `reflex_build_delta_sql`'s own guards (self-join,
outer-join-secondary, etc.) plus the `''`→`reflex_reconcile` fallback in step 4
handle every case the incremental path can't represent. The whole change is gated
by the universal invariant below.

### Correctness invariant

Every uncertain branch falls back to today's `reflex_reconcile`, which is always
correct. The optimization only *replaces* a full reconcile with a delta when the
delta path is known-correct (it is the same path a real DML mutation takes); it
never produces a result the trigger-driven INSERT/DELETE path wouldn't.

### Decisions (locked)

1. **DETACH** → DELETE delta when the detached child is accessible, else reconcile.
2. **Eligibility** → all IMVs; rely on `build_delta_sql` guards + `''`→reconcile.
3. **SwapFill** → full reconcile fallback for v1.

## Testing (TDD — written first, then frozen)

Oracle style: compare IMV (and a downstream child) content against a from-scratch
`base_query` rebuild; assert cascade firing via a child's `last_update_date`.

1. **Headline no-op:** attach a non-current LIST partition *with data* → the IMV's
   content is unchanged **and** a downstream child's `last_update_date` is
   unchanged (cascade did not fire). RED before the change (full reconcile bumps
   it), GREEN after.
2. **Relevant attach:** attach the current assortment's partition with data → IMV +
   children updated; content matches a from-scratch rebuild.
3. **Empty partition:** `CREATE TABLE … PARTITION OF` with no data → `SKIPPED`, no
   cascade, content correct.
4. **DETACH:** detach a non-current partition → no-op; detach the current
   partition → its rows removed correctly (matches rebuild).
5. **Path B:** attach a partition whose row count exceeds `wipe_threshold` × source
   → falls back to reconcile; content correct (no regression).
6. **Join IMV:** an unpartitioned IMV joining the partitioned source to another
   table → incremental result matches rebuild; a self-join / outer-join-secondary
   IMV exercises the `''`→reconcile fallback.
7. **No backfill:** `where_predicate` empty → still correct (delta SQL filters),
   just without the O(1) skip.

## Out of scope

- Making a *legitimately needed* reconcile cheaper than a full rebuild beyond what
  the delta path already gives (e.g. delta-cascading vs. TRUNCATE+re-INSERT in the
  fallback reconcile itself).
- Partitioned-IMV flush path (already per-partition via swap).
- The subquery-filter-source-change hazard (a change to `sop_current_view` switching
  the current assortment) — handled by that source's own trigger path; orthogonal.
