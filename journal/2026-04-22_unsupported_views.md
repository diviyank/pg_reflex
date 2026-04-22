# Unsupported-view inventory — pg_reflex 1.1.2

**Date**: 2026-04-22
**Source**: `/home/diviyan/fentech/algorithm/api/base-db-anchor-evm/base_db/sql/reflex/*.sql`

Inventory of views that were intentionally **kept as plain `MATERIALIZED VIEW`** (not converted to a pg_reflex IMV). Grouped by root cause. Each entry lists the root cause, why the current pg_reflex engine cannot maintain it incrementally, and what would need to change for it to become supportable.

---

## 1. DISTINCT ON / ROW_NUMBER / window-function top-1 picks

| View | Root cause |
|---|---|
| `latest_price_reflex` (02_pricing.sql) | `DISTINCT ON + ORDER BY` |
| `assortment_orders_reflex` (03_assortment.sql) | `ROW_NUMBER + EXISTS + CROSS JOIN` |
| `assortment_characteristics_reflex` (03_assortment.sql) | `DISTINCT ON` subqueries + `AVG` |
| `product_computed_features_reflex` (03_assortment.sql) | `DISTINCT ON + LEFT JOIN` |
| `latest_inventory_reflex` (04_supply.sql) | `ROW_NUMBER + WHERE rn=1` |
| `current_assortment_activity_reflex` (01_metadata.sql) | `DISTINCT + WHERE subquery` |

**Why unsupported**: `DISTINCT ON`/`ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)` is a *top-1-per-group* selection. On DELETE of the currently-top row, the engine must scan the remaining rows of the group to find the new top — the same full-scan problem that `MIN`/`MAX` have, but with an additional ordering constraint. pg_reflex 1.1.1 already supports `DISTINCT ON` at IMV creation time (decomposed into a passthrough sub-IMV plus a `ROW_NUMBER` view, see 1.1.0→1.1.1 migration notes), but **only as a read-time view**, not an incrementally-maintained table — the top-K maintenance needed for DELETE is still missing.

**What would unlock it**: A top-K companion structure (see optimization idea #2 in `2026-04-22_optimization_ideas.md`). On DELETE, remove the row from the heap; on INSERT, compare against the current top-1 and evict.

---

## 2. Scalar-aggregate (no meaningful group key)

| View | Root cause |
|---|---|
| `max_order_date_reflex` (01_metadata.sql) | `MAX` with no `GROUP BY`, `CROSS JOIN` of two scalar CTEs |

**Why unsupported**: A single-row aggregate has no group to address on delta. Every row-level delta potentially affects the single output row. Combined with `MAX` this degenerates to a full-table scan on every DELETE.

**What would unlock it**: Either (a) treat scalar aggregates as a single implicit group and use the top-K companion from §1, or (b) memoize last-known `MAX` + add a retract path that falls back to full scan only when the retracted row equals the current `MAX`.

---

## 3. LIMIT / ORDER BY … LIMIT 1 passthrough

| View | Root cause |
|---|---|
| `sop_current_reflex` (01_metadata.sql) | `ORDER BY + LIMIT 1 + subquery` |

**Why unsupported**: `LIMIT`/`ORDER BY` picks one specific row out of many; arbitrary deletes can change the winner. Same family as §1 and §2.

**What would unlock it**: top-K structure, or model it as a read-time `SELECT … LIMIT 1` over a sorted IMV.

---

## 4. FULL JOIN passthrough

| View | Root cause |
|---|---|
| `incoming_stock_reflex` (04_supply.sql) | `FULL JOIN` |
| `forecast_analysis_reflex` (08_sop.sql) | `FULL JOIN + UNION + complex CTEs` |
| `inventory_detail_reflex` (09_supply_serving.sql) | `FULL JOIN + complex CTEs` |

**Why unsupported**: For a `FULL OUTER JOIN`, every insert/delete on either side can create or destroy rows on **both** sides of the output (matched ↔ NULL-extended transitions). The current delta logic only reasons about one "source" side at a time. Additionally, when the FULL JOIN inputs are themselves matviews (not base tables), there are no transition tables to drive the delta from. The SQL comment `"Cannot be an IMV (all FULL JOIN inputs are matviews/IMVs)"` in `08_sop.sql:176` captures both problems.

**What would unlock it**: (a) generalize delta computation to compute MATCH ↔ NULL transitions on both sides simultaneously, and (b) propagate transition tables through intermediate matviews (requires registering matviews as IMVs or introducing a "virtual" delta source).

---

## 5. Window functions in SELECT

| View | Root cause |
|---|---|
| `last_month_pdm` (03_assortment.sql) | `SUM OVER()` |
| `latest_inventory_repartition_reflex` (04_supply.sql) | `SUM OVER()` |

**Why unsupported**: Window aggregates are not associative across disjoint row sets. A new row can change the partition total that appears alongside unrelated rows, so the "affected group" of a single delta row is potentially the entire partition. Correct incremental maintenance requires storing both the per-partition aggregate *and* the per-row result, plus re-emitting all rows of the partition on any change.

**What would unlock it**: A two-stage engine that (a) maintains the partition aggregate incrementally (already possible today) and (b) re-emits every row of the partition on change — the second step is essentially a full refresh of the partition.

---

## 6. MIN / MAX / BOOL_OR on wide fact tables

| View | Root cause |
|---|---|
| `forecast_stock_chart_monthly_reflex` (04_supply.sql) | `MIN + MAX + BOOL_OR` |
| `stock_chart_weekly_reflex` (04_supply.sql) | `MIN + MAX + BOOL_OR` |
| `stock_chart_monthly_reflex` (04_supply.sql) | `MIN + MAX + BOOL_OR` |

**Why unsupported-in-practice**: Technically pg_reflex now *supports* these after the 2026-04-21 recompute fix, but the cost model is unattractive — every DELETE triggers a full `base_query` scan per aggregate column. For wide fact tables (10–30 M rows) this makes deletes catastrophically slow compared to a batched `REFRESH`. The SQL comment `"MIN on delete requires group rescan"` encodes this as a deliberate "kept as matview" decision.

**What would unlock it**: Algebraic `MIN`/`MAX`/`BOOL_OR` maintenance (optimization ideas #1 and #2 in `2026-04-22_optimization_ideas.md`). Until then these are matviews by policy, not by impossibility.

---

## 7. UNION ALL inside a CTE

| View | Root cause |
|---|---|
| `sop_incoming_stock_reflex` (09_supply_serving.sql) | `UNION ALL` in CTE + SUM GROUP BY |
| `sop_incoming_stock_baseline_reflex` (09_supply_serving.sql) | `UNION ALL` in CTE |
| `unsent_sop_incoming_stock_baseline_reflex` (09_supply_serving.sql) | `UNION ALL` in CTE |
| `sop_received_stock_reflex` (09_supply_serving.sql) | `UNION ALL` in CTE |

**Why unsupported**: pg_reflex decomposes top-level `UNION ALL` into per-branch sub-IMVs and a union-driven upper IMV (see `src/create_ivm.rs::union_selects`), but the decomposer does **not recurse into CTEs**. A `WITH x AS (SELECT … UNION ALL SELECT …)` stays as a single unsplit block, so delta substitution cannot be applied cleanly.

**What would unlock it**: Extend the set-operation flattening pass to descend into CTE bodies. Non-trivial because it requires rewriting every CTE reference to point to the split sub-IMVs and preserving column ordering / uniqueness across branches.

---

## 8. Passthrough over non-IMV matviews

| View | Root cause |
|---|---|
| `assortment_details_reflex` (05_assortment_serving.sql) | JOIN on `history_sales_reflex`, `latest_inventory_reflex` (no triggers) |
| `allocation_summary_reflex` (09_supply_serving.sql) | JOIN on `life_cycle`, `unit_pricing` (no unique-key info) |
| `appro_summary_reflex` (09_supply_serving.sql) | complex CTEs referencing multiple matviews |

**Why unsupported**: A passthrough IMV needs transition-table coverage on every dependency. When an upstream dependency is a plain `MATERIALIZED VIEW`, it has no DML triggers — pg_reflex never sees deltas on it, so any change to the matview silently escapes the IMV. The `allocation_summary_reflex` case is subtler: even if triggers existed, pg_reflex needs a unique key on each side to build the delta, and `life_cycle`/`unit_pricing` lack one that the analyzer can derive from catalog.

**What would unlock it**: Either (a) support "virtual" delta sources that propagate through IMV chains even when the intermediate is a matview, or (b) require every ancestor in the dependency tree to be either a base table or an IMV.

---

## 9. Heavy-structure aggregations (ARRAY_AGG, JSON, schema introspection)

| View | Root cause |
|---|---|
| `kind_serving.sql` views (`10_kind_serving.sql`) | 10+ CTEs, `UNION ALL` hierarchy, `ARRAY_AGG` |
| `tcd_schema` (`11_tcd.sql`) | JSON aggregation, schema introspection |

**Why unsupported**: `ARRAY_AGG` is not algebraically maintainable without storing intermediate arrays keyed by group, and `ARRAY_AGG ORDER BY` adds the same top-K problem as `DISTINCT ON`. Schema introspection reads from `information_schema` / `pg_catalog`, which pg_reflex has no delta signal for.

**What would unlock it**: `ARRAY_AGG` with UNORDERED semantics could be modeled via a side-table of per-group row references (add on INSERT, remove on DELETE) but the user-visible array would have to be re-materialized on every read, which defeats the purpose. Catalog-driven views are out of scope.

---

## 10. `SUM FILTER` with CROSS JOIN to matviews

| View | Root cause |
|---|---|
| `next_month_sales_reflex` (06_forecast.sql) | `SUM FILTER + CROSS JOIN` to `sop_current_reflex + max_order_date_reflex` |

**Why unsupported**: `SUM FILTER` is now supported in isolation (1.1.0→1.1.1), but the cross-join to two matviews that are themselves unsupported IMVs (`sop_current_reflex`, `max_order_date_reflex`, both in category §1/§2) means there is no delta propagation path. As soon as either matview is refreshed, `next_month_sales_reflex` would need a full rebuild anyway.

**What would unlock it**: Unblocking `sop_current_reflex` (category §1/§2) first.

---

## Summary

| Category | Views | Count |
|---|---|---:|
| 1. DISTINCT ON / ROW_NUMBER | latest_price, assortment_orders, assortment_characteristics, product_computed_features, latest_inventory, current_assortment_activity | 6 |
| 2. Scalar aggregate | max_order_date | 1 |
| 3. LIMIT 1 | sop_current | 1 |
| 4. FULL JOIN | incoming_stock, forecast_analysis, inventory_detail | 3 |
| 5. Window function | last_month_pdm, latest_inventory_repartition | 2 |
| 6. MIN/MAX/BOOL_OR cost | forecast_stock_chart_monthly, stock_chart_weekly, stock_chart_monthly | 3 |
| 7. UNION ALL in CTE | sop_incoming_stock(*), sop_received_stock, unsent_sop_incoming_stock_baseline | 4 |
| 8. Passthrough over matviews | assortment_details, allocation_summary, appro_summary | 3 |
| 9. ARRAY_AGG / JSON / catalog | kind_serving.*, tcd_schema | 2 |
| 10. Mixed (depends on 1-2) | next_month_sales | 1 |
| **Total** | | **26** |

The biggest wins if we could unlock these: **(§1 + §3 + §6) top-K maintenance**, which alone would release 10 IMVs. Everything else is either architectural (§4 FULL JOIN, §7 set-ops in CTEs) or out of scope (§9).
