# pg_reflex vs REFRESH MATERIALIZED VIEW benchmark

**Date**: 2026-04-21
**DB**: `db_clone` (PG18, work_mem=128MB, maintenance_work_mem=2GB)
**Extension**: pg_reflex 1.1.1, 14 IMVs in `alp` schema, all `DEFERRED / UNLOGGED`.
**Method**: per IMV, build a monolithic twin via `CREATE MATERIALIZED VIEW bench_<name> AS ...` (parsing original matview SQL, rewriting dangling `*_view` refs to their `*_reflex` equivalents) and time it; then in a `BEGIN/UPDATE 1000 rows/ROLLBACK` transaction, dirty the most-relevant source table and call `reflex_flush_deferred(source_table)` to time incremental propagation. `pg_stat_reset()` was run once at the start. Times from psql `\timing on`.

## Results table

| IMV | rows | size | REFRESH ms | Flush ms (1K delta) | Speedup |
|---|---:|---:|---:|---:|---:|
| demand_planning_characteristics_reflex | 9 | 56 kB | 164,724 | 139,088* | 1.2x |
| sop_forecast_history_reflex | 2,584,454 | 1,034 MB | 6,689 | 173,455* | 0.04x |
| event_demand_planning_sales | 0 | 96 kB | 28,041 | 192,911* | 0.15x |
| zscore_reflex | 720,593 | 185 MB | 159,214 | 184,916* | 0.86x |
| forecast_stock_chart_weekly_reflex | 1,283,232 | 328 MB | 4,089 | flush-bug (syntax error, 2 ms) | n/a |
| stock_transfer_baseline_reflex | 0 | 96 kB | 36 | n/a (source table empty) | n/a |
| stock_transfer_reflex | 0 | 80 kB | 16 | n/a (source table empty) | n/a |
| history_sales_reflex | 1,682,995 | 495 MB | 132,093 | flush-bug (syntax error, 62,582 ms) | n/a |
| last_month_sales_reflex | 22,128 | 3,952 kB | 1,794 | flush-bug (syntax error, 57,956 ms) | n/a |
| sop_forecast_reflex | 7,732,314 | 3,345 MB | 39,115 | 130,469* | 0.30x |
| sop_last_forecast_reflex | 2,935,247 | 966 MB | 7,966 | flush aborted after 16 ms (unsent_sop bug) | n/a |
| unsent_sop_forecast_reflex | 30,334,372 | 9,794 MB | 125,733 | 154,902* | 0.81x |
| sop_purchase_reflex | 64,800 | 22 MB | 745 | flush-bug (syntax error, 1.6 ms) | n/a |
| sop_purchase_baseline_reflex | 141,938 | 50 MB | 675 | 277 | 2.4x |

`*` — flush errored partway through (see below) yet the transaction had already processed most dependent IMVs before aborting, so the time reflects "flush attempt until first downstream bug fires" rather than a clean successful flush.

## Errored / twin-build notes

- `forecast_stock_chart_weekly` — original matview in `supply.sql` references `sales_simulation_impacted_stock`, which does not exist in `db_clone`. The reflex IMV body stored in `__reflex_ivm_reference.sql_query` uses a different (simpler) join off `location_inventory_baseline` and `supply_plan.is_sent_to_sop`. The twin was rebuilt using the reflex body so the REFRESH figure is comparable to what pg_reflex actually maintains.
- `stock_transfer_*` and `event_demand_planning_sales` source tables are empty in this DB — so the 16–36 ms REFRESH is essentially just catalog work, and the incremental tests are not meaningful.
- All twin builds used `current_assortment_activity_reflex`, `max_order_date_reflex`, `latest_price_reflex`, `sop_current_reflex` in place of the non-existent `*_view` matviews.

## Flush bugs surfaced

Three distinct code-generation bugs in pg_reflex were triggered by the incremental tests, all pre-existing (independent of this benchmark):

1. **`ERROR: missing FROM-clause entry for table "caav"`** — the generated update for `__reflex_intermediate_unsent_sop_forecast_reflex` embeds `BOOL_OR(caav.product_id IS NOT NULL)` without joining the `caav` alias into the correlated subquery. Every flush of `sales_simulation` eventually hits this and aborts. The time recorded reflects the work done on preceding IMVs in the cascade (demand_planning_characteristics, sop_forecast_history, event_demand_planning_sales, zscore, sop_forecast, unsent_sop_forecast all touched).
2. **`ERROR: syntax error at or near "AS"`** — generated delta-insert for `last_month_sales_reflex__cte_order_data_with_windows` emits `... AS __dt AS ol ON ...` (double alias). Hits on any `order_line` flush.
3. **`ERROR: syntax error at or near "lib"` / "AS"`** — generated code for `forecast_stock_chart_weekly_reflex` and `sop_purchase_reflex` flush produces malformed SQL and fails immediately (1–2 ms).

Because of bugs 1–3, the flush column is dominated by the *cascade work accomplished before the abort*, not a clean incremental update. The only cleanly-completed flush in the benchmark is `sop_purchase_baseline_reflex` (277 ms incremental vs 675 ms full REFRESH → 2.4× speedup).

## Commentary

The headline is muted by the pre-existing code-gen bugs (unsent_sop/caav, last_month_sales/order_data_with_windows, fscw/sop_purchase). Where the IMV source is small or the upstream table is modest (`sop_purchase_baseline_reflex`, `demand_planning_characteristics_reflex`), incremental flush is competitive or wins: 2.4× on sop_purchase_baseline is a real win. Full REFRESH remains startlingly fast on wide fact tables that aggregate well (`sop_forecast_history` 6.7 s, `forecast_stock_chart_weekly` 4 s, `sop_last_forecast` 8 s) because PG18 parallel aggregation with 2 GB `maintenance_work_mem` can blast through them. Pg_reflex currently loses on any IMV that depends on `sales_simulation` because the cascade visits ~5 downstream IMVs plus the 30 M-row `__reflex_intermediate_unsent_sop_forecast_reflex` update. Fixing the three code-gen bugs listed above is a prerequisite to any credible speedup claim; once that is done a clean incremental measurement on sales_simulation/order_line should be repeated.

## Artifacts

- `/tmp/pg_reflex_bench/*.sql` — per-IMV twin SQL
- `/tmp/pg_reflex_bench/run_refresh.sh`, `run_flush.sh` — drivers
- `/tmp/pg_reflex_bench/results_refresh.txt`, `results_flush.txt` — raw psql output

## Resolution — 2026-04-21 (later same day)

All three code-gen bugs fixed. Bugs 2 and 3 (`AS __dt AS ol` /
`AS __dt lib` / `AS __dt AS pol`) were the same root cause:
`replace_source_with_delta`'s standalone-replacement pass didn't
consume an existing user alias. Fixed by making pass 2 detect `AS
<ident>` or bare `<ident>` immediately following the source match
(reject the bare form only when it's a SQL follow-keyword like JOIN /
WHERE / ON / …) and adopt that alias instead of emitting the default.
Bug 1 (`caav` missing) fixed per companion journal
`2026-04-21_min_max_recompute_bug.md`.

Regression tests in `src/tests/pg_test_deferred.rs`:
`pg_test_deferred_bool_or_with_join_alias_recompute`,
`pg_test_deferred_flush_consumes_user_alias_in_from`. Unit tests for
alias consumption in `src/tests/unit_query_decomposer.rs` and
JOIN-aware recompute in `src/tests/unit_trigger.rs`. Full suite: 431
passing on PG18.

**Rerun bench** to get clean flush numbers for the IMVs that
previously aborted (`unsent_sop_forecast_reflex`,
`last_month_sales_reflex`, `forecast_stock_chart_weekly_reflex`,
`sop_purchase_reflex`, plus the cascade-aborted ones).
