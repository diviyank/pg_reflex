# 2026-04-17 — Rebuilding `db_clone` views with pg_reflex

Goal: run the full `sql/reflex/*.sql` sequence from
`base-db-anchor-evm/base_db` against `db_clone`, inventory every error or
warning pg_reflex raises, and catch any correctness bugs in the generated
triggers / flush path.

Environment:
- pg_reflex `1.1.1` (same as `Cargo.toml`).
- Source tables live in schema `alp`; IMVs land in `alp` via search_path.
- All 13 scripts (`00_setup` → `11_tcd`) executed after `00_drop_existing`.

## Summary

| Category | Count |
|---|---|
| Script-level errors when search_path not set | 153 |
| Script-level errors with `search_path=alp,public` | 0 |
| IMVs registered (main + CTE helpers) | 20 |
| IMVs where initial population matched expected row count | 2 / 2 spot-checked |
| IMVs where delta flush crashes | **2 confirmed (zscore_reflex, stock_transfer_*)** |

Two real correctness bugs surfaced from running this — details below.

## 1. Scripts depend on a search_path that doesn't persist

`00_setup.sql` ends with `SET search_path TO alp, public;`. Every subsequent
script is run in a fresh psql session, so the `SET` is gone. Every reference
to an `alp`-only table then either hits `public` (where a stale twin may
exist) or errors.

First run (no `PGOPTIONS`) produced these errors per script:

```
01_metadata.sql       4    -- column sop.status does not exist (hits public.sop)
03_assortment.sql    13
04_supply.sql        30
05_assortment_serving 4
06_forecast.sql       8
07_forecast_serving   7
08_sop.sql           30
09_supply_serving    33
10_kind_serving      11
```

Root cause for `01_metadata` — `public.sop` exists (no `status` column)
alongside `alp.sop` (has `status`). Without `search_path=alp,public`, the
planner resolves to `public.sop` and fails on `sop.status`. Once all
downstream refs cascade-fail (`sop_current_reflex` etc. never get created),
the rest of the errors are just fallout.

Re-running with `PGOPTIONS='-c search_path=alp,public'` passed all 13
scripts with **0 errors**.

**Suggested fix**: put `SET search_path TO alp, public;` at the top of every
script (or have the ops wrapper set it), not only in `00_setup.sql`. The
current layout is a footgun whenever someone invokes a single script.

## 2. `zscore_reflex` — duplicate-key failure on flush

Trigger mode: `DEFERRED`. Definition: grouped aggregate over
`sales_simulation` with unique index
`idx_zscore_reflex_unique (product_id, location_id, dem_plan_id)`.

Repro: insert one row into `sales_simulation` that maps to an existing
`(product_id, location_id, dem_plan_id)` group, then call
`reflex_flush_deferred('sales_simulation')`.

```
ERROR:  duplicate key value violates unique constraint "idx_zscore_reflex_unique"
DETAIL:  Key (product_id, location_id, dem_plan_id)=(6773958, 50, 622) already exists.
```

The generated statement is the classic

```sql
WITH del AS (DELETE FROM "zscore_reflex" WHERE EXISTS (...) RETURNING 1),
     ins AS (INSERT INTO "zscore_reflex" SELECT ... RETURNING 1)
UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() ...
```

Sibling CTEs in Postgres run against the same snapshot — the `INSERT` does
**not** see `DELETE`'s effect, so when the incoming row replaces an
existing group the unique index blocks it.

Target IMVs that land on this code path today:
- `zscore_reflex` (confirmed)
- Any other IMV with a unique index + grouped aggregate delta path —
  `sop_forecast_reflex`, `sop_last_forecast_reflex`,
  `demand_planning_characteristics_reflex` etc. all have unique indices
  and would be vulnerable if they go through the same DELETE/INSERT-CTE
  emitter. Needs a second pass to confirm which of those use MERGE vs the
  CTE form.

**Suggested fix**: replace the sibling-CTE DELETE+INSERT with a single
`MERGE` (already used elsewhere in the codebase) or split the statement
into two so the INSERT sees the DELETE.

## 3. `stock_transfer_baseline_reflex` / `stock_transfer_reflex` — malformed MERGE

Warnings at create time:

```
WARNING: pg_reflex: expression 'stock_transfer_baseline.to_location_id'
         not in GROUP BY and not a recognized aggregate — column will be
         missing from IMV 'stock_transfer_baseline_reflex'
WARNING: pg_reflex: expression 'stock_transfer.to_location_id' ...  (same)
```

The column *is* created (verified via `\d`), so the warning itself is a
false-ish positive — but the delta-SQL generator has the same parser bug
and it *does* break. Repro: insert any row into `stock_transfer_baseline`
and call `reflex_flush_deferred('stock_transfer_baseline')`:

```
ERROR:  syntax error at or near "."
QUERY:  ... USING (SELECT ..., (SELECT * FROM __reflex_delta_stock_transfer_baseline
        WHERE __reflex_op = 'I') AS __dt.product_id AS "product_id", ...
```

The emitter substituted the `__dt` alias with the full subquery text in
most positions, producing `(SELECT ...) AS __dt.product_id AS "product_id"`
which is not valid SQL. It happens on every column that was written as
`stock_transfer_baseline.<col>` in the original query — pg_reflex's parser
seems to canonicalize qualified references differently from unqualified
ones and the alias-substitution pass misses the qualified ones.

Affected columns in these two IMVs:
`stock_transfer_baseline.transfer_date`, `.product_id`, `.to_location_id`,
`.from_location_id`, `.supply_plan_id` (and the same set on `stock_transfer`).

**Suggested fix options**:
1. Rewrite the reflex IMV SQL to use unqualified column names in both
   SELECT and GROUP BY (unblocks db_clone now).
2. Fix pg_reflex's parser to normalise `<src>.col` ↔ `col` before emitting
   the delta SQL — addresses the root cause.

Both tables are empty in the current dump, so this bug wouldn't have been
caught by data checks; only an exercising insert flushed it out.

## 4. Other warnings (non-blocking, but worth tracking)

- **Subquery sources** on `sop_purchase_reflex__cte_po_base` and
  `sop_purchase_baseline_reflex__cte_pb_base` (the `caav` LEFT JOIN
  subquery): triggers exist on the underlying tables, but the subquery
  is re-executed on every delta. Flag for performance regression if
  those views get hot.
- **No unique key** on four passthrough IMVs:
  `history_sales_reflex`,
  `last_month_sales_reflex__cte_order_data_with_windows`,
  `sop_purchase_reflex__cte_po_base`,
  `sop_purchase_baseline_reflex__cte_pb_base`.
  DELETE/UPDATE deltas degrade to full-group rebuild. If these see
  mutations in production, passing an explicit key to
  `create_reflex_ivm` is cheap.
- **Matview-sourced triggers skipped** for `max_order_date_reflex`,
  `current_assortment_activity_reflex`, `sop_current_reflex`,
  `latest_price_reflex` — expected (matviews have no triggers), but
  means ops must call `refresh_imv_depending_on(<matview>)` after every
  `REFRESH MATERIALIZED VIEW`. Easy to forget.

## 5. Correctness spot-checks on populated IMVs

Recomputed the source query against the IMV row count:

| IMV | Expected | IMV rows | Match |
|---|---:|---:|:-:|
| `history_sales_reflex` | 1,683,115 | 1,683,115 | ✓ |
| `sop_forecast_reflex`  | 7,732,941 | 7,732,941 | ✓ |
| `event_demand_planning_sales` | 0 | 0 | ✓ (source empty) |
| `stock_transfer_reflex` | 0 | 0 | ✓ (source empty; flush untested) |

Initial population works. The two bugs above are in the **delta** path,
not the bulk build.

## Open items

- Confirm whether `sop_forecast_reflex`, `sop_last_forecast_reflex`,
  `demand_planning_characteristics_reflex`, `last_month_sales_reflex` also
  hit the CTE DELETE+INSERT duplicate-key issue from bug #2. They all have
  unique indices; needs one insert+flush each to check.
- Re-test bugs #2 and #3 after fix with the existing `tests/` harness —
  both should be reproducible with tiny fixtures (a unique-keyed grouped
  aggregate for #2, a qualified-column GROUP BY for #3).
