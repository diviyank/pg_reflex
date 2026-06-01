# CTE Unique-Key Propagation for Sub-IMVs

**Date:** 2026-05-31
**Status:** Approved (design) — pending implementation plan
**Area:** `src/create_ivm.rs` (unique-key inference), `__reflex_ivm_reference` registry

## Problem

When a query is decomposed into CTE sub-IMVs, each passthrough sub-IMV needs a
unique key so incremental `DELETE`/`UPDATE` can target rows instead of doing a
full `DELETE` + re-`INSERT`. Today `infer_join_passthrough_unique_key`
(`src/create_ivm.rs:3177`) only proves a key for INNER/LEFT **equi-joins** where
every non-anchor source is reached by a **to-one** equi-join and the anchor has a
**PRIMARY KEY**. Anything else returns `None` → the sub-IMV silently falls back
to full refresh on every source change.

On real views this leaves many CTE sub-IMVs keyless. Motivating case —
`omc.forecast_analysis_view` emits:

```
INFO: pg_reflex: JOIN passthrough 'omc.forecast_analysis_view__cte_date_limits'   has no unique key.
INFO: pg_reflex: JOIN passthrough 'omc.forecast_analysis_view__cte_history_sales'  has no unique key.
INFO: pg_reflex: JOIN passthrough 'omc.forecast_analysis_view__cte_forecast_sales' has no unique key.
```

### Verified failure points

| CTE | Shape | Why `infer_join_passthrough_unique_key` returns `None` today |
|---|---|---|
| `date_limits` | `target_dp CROSS JOIN history_bounds JOIN forecast_bounds ON dpid` | `CROSS` hits the `_ => return None` arm (line 3188); anchor `target_dp` is a UNION-ALL sub-IMV with no PRIMARY KEY (anchor probe at line 3227 reads `indisprimary` only). |
| `forecast_sales` | `sop_forecast_view JOIN date_limits ON dpid=dpid AND order_date BETWEEN …` | to-one on `dpid` *would* hold, but requires `date_limits` to cover a unique key on `dpid`; `date_limits` got no key → cascade failure. |
| `history_sales` | `history_sales_view JOIN date_limits ON order_date BETWEEN …` | pure range join, no equi → `n_eq == 0` (line 3273). Genuinely **to-many**: each `hsv` row matches every `dpid` whose window covers its `order_date`. |

### The linchpin: discriminant-dropping UNION ALL

`target_dp` is `UNION ALL` of (archived dem_plans) and (the current dem_plan).
Its sound key is `(dem_plan_id, __reflex_src_idx)`, but `date_limits` projects
only `dem_plan_id` and drops the discriminant. So `dem_plan_id` is **not
provably unique** for `date_limits`. No inference widening can soundly fix this.

**Accepted boundary (design decision):** a CTE anchored on a discriminant-dropping
UNION ALL requires an explicit per-CTE seed key via the existing
`unique_columns` spec (`'<outer> ; date_limits : dem_plan_id ; …'`). This is
sound for the real data (archived vs. current dem_plans are disjoint). Once
`date_limits` carries a key, every downstream CTE resolves automatically. We do
**not** attempt to auto-key unions — that would require an unprovable disjointness
proof.

## Goals

- Automatically infer a sound unique key for CTE sub-IMVs across to-one **and**
  to-many INNER joins, CROSS joins to single-row relations, and mixed
  equi+range joins, by "trickling down" upstream sub-IMVs' resolved keys.
- Keep the explicit per-CTE `unique_columns` spec as the seed for the
  irreducibly-ambiguous cases (union-anchored CTEs).
- Preserve correctness as the top priority: never infer a key that isn't
  structurally provable; the `__reflex_uk_*` unique index remains the loud
  safety net.

## Non-goals

- Auto-keying discriminant-dropping UNION ALL CTEs.
- LEFT/RIGHT/FULL to-many key composition (null-padding subtleties) — refused for
  now, kept as full-refresh fallback.
- In-memory key threading through `BuildContext` (the registry is the channel).

## Design

### 1. Propagation channel: the registry

Sub-IMVs are created in dependency order; each writes its resolved
`unique_columns` to `__reflex_ivm_reference` **before** the next CTE is built
(`try_decompose_ctes`, `src/create_ivm.rs:1061`). So a dependent CTE's inference
reads its upstream CTEs' keys directly from the registry, and base tables from
`pg_index`. No new threading is required — this is the literal "trickle down."

**Registry addition:** one new column `max_one_row BOOLEAN` on
`__reflex_ivm_reference` (default `false`), set `true` for ungrouped aggregate
sub-IMVs (aggregate plan with empty GROUP BY → at most one row). Used to classify
a `CROSS JOIN` to such a relation as to-one. Migration added like prior columns.

### 2. Per-join cardinality classification

Replace the all-or-nothing gate. For a chosen anchor `A` whose sound key `K_A`
is fully projected, classify every other source `S`:

- **to-one** — `S`'s equi-join columns cover a sound key of `S` (registry for CTE
  sources, catalog for base tables), **or** `S` is `max_one_row`. Range
  predicates alongside a to-one-proving equi-join are treated as additional
  filters. Contributes nothing to the result key. *(fixes `forecast_sales`; the
  CROSS-to-`history_bounds` part of `date_limits`.)*
- **to-many** — `S` is not collapsed to-one but its **full sound key is projected**
  in the SELECT. Restricted to **INNER** joins. Contributes `S`'s projected key
  to the result key. *(fixes `history_sales`: key = `hsv` key ∪
  `date_limits.dem_plan_id`.)*
- **unprovable** — to-many with no projected key, or LEFT/RIGHT/FULL/CROSS to a
  multi-row relation → return `None` (keep full-refresh fallback + guidance log).

**Soundness of to-many composition:** a join emits at most one output row per
`(anchor-row, S-row)` combination, so `K_A ∪ K_S` uniquely identifies each output
row. Generalizes to N to-many sources: result key = `K_A ∪ (⋃ K_S over to-many S)`.

### 3. Widen the anchor probe

Replace `source_primary_key_columns` (PRIMARY KEY only) for the anchor with a
**sound-unique-key** probe that accepts:
- a base-table PRIMARY KEY, or a NOT-NULL unique index, or
- a CTE sub-IMV's registry `unique_columns` (its `__reflex_uk_*` index;
  reflex builds these so the columns are a true key — verify NOT NULL or
  `NULLS NOT DISTINCT` before trusting an inferred key as an anchor).

This lets union/aggregate CTE sub-IMVs serve as anchors.

### 4. Result key and safety net

`resolved_unique_columns` = anchor projected sound key ∪ (projected keys of all
to-many INNER sources). The `__reflex_uk_*` unique index build
(`src/create_ivm.rs:1821`) is unchanged and remains the loud failure net: if the
inferred key is not actually unique in the data, index creation fails at build
time rather than corrupting incremental maintenance.

### 5. Conservative refusals (correctness first)

Return `None` (→ documented fallback) for: LEFT/RIGHT/FULL to-many,
CROSS to a multi-row relation, `OR`/`USING` conditions, any source whose sound
key is not fully projected, and any catalog/registry access error.

## Affected code

- `src/create_ivm.rs`
  - `infer_join_passthrough_unique_key` (3177) — rewrite to per-join
    classification + to-many key union.
  - `source_primary_key_columns` (3092) — augment / add a sound-unique-key probe
    that consults the registry for CTE sources.
  - `source_cols_cover_unique_key` (3059) — extend to consult the registry's
    `unique_columns` for CTE sub-IMV sources (not only `pg_index`).
  - `resolve_unique_columns` (1175) — unchanged control flow; benefits from the
    widened inference.
  - aggregate materialization path — set `max_one_row` for empty-GROUP-BY plans.
- `src/lib.rs` — `__reflex_ivm_reference` schema (add `max_one_row`); migration.
- `src/sql_writer/registry.rs` — persist/read `max_one_row`.

## Testing (TDD — red first, do not modify after)

`src/tests/pg_test_cte.rs`:
1. CROSS join to a single-row (ungrouped aggregate) CTE → key inferred from anchor.
2. Equi + range mixed join → to-one, key inferred (range treated as filter).
3. Pure-range to-many INNER join to a keyed relation → key = anchor key ∪ joined key.
4. UNION-ALL-anchored CTE with discriminant dropped → stays keyless **without** an
   explicit seed (assert the warning), keyed **with** the explicit seed.
5. Full `forecast_analysis_view` chain with `date_limits` seeded → zero
   "has no unique key" warnings; assert each sub-IMV's `unique_columns`.
6. Negative: LEFT to-many, CROSS to multi-row → remain `None` (fallback).

Then: `cargo pgrx test`, `cargo clippy`, `cargo fmt`. Benchmark incremental
DELETE/UPDATE on the now-keyed sub-IMVs vs. the current full-refresh path on
db_clone; evaluate worth before keeping (per project process).

## Risks / open questions for the plan phase

- Confirm the reflex `__reflex_uk_*` index nullability handling so an upstream
  CTE key used as an anchor is a *true* key (NOT NULL or `NULLS NOT DISTINCT`).
- Confirm `max_one_row` is the minimal metadata needed (vs. re-probing the
  upstream aggregation plan) — chosen: registry column.
- Partition interaction: inferred keys feed the partitioned `__reflex_uk_*`
  index, which must include the partition column (existing constraint in
  `src/partition.rs`).
