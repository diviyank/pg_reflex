# pg_reflex 1.1.2 vs REFRESH MATERIALIZED VIEW benchmark — rerun

**Date**: 2026-04-22
**Previous run**: `journal/2026-04-21_db_clone_benchmark.md` (blocked by three code-gen bugs, all fixed same day)

## Setup

| Item | Value |
|---|---|
| PostgreSQL | 18.3 (x86_64-pc-linux-gnu, gcc 15.2.1) |
| pg_reflex | **1.1.2** (bumped from 1.1.1 to ship the 7-arg `reflex_build_delta_sql` + in-place plpgsql patch migration) |
| `work_mem` | 128 MB |
| `maintenance_work_mem` | 2 GB |
| DB | `db_clone` (same snapshot as 2026-04-21) |
| IMVs registered | 14 (all `DEFERRED / UNLOGGED`, 20 rows in `__reflex_ivm_reference` once CTE-derived sub-IMVs are counted) |
| Install path | `cargo pgrx install --release --features pg18 --pg-config /usr/bin/pg_config`, then `ALTER EXTENSION pg_reflex UPDATE TO '1.1.2'` |
| Upgrade migration | `sql/pg_reflex--1.1.1--1.1.2.sql` — drops 6-arg `reflex_build_delta_sql`, creates 7-arg, and patches 141 existing trigger-function bodies in `pg_proc.prosrc` to pass `_rec.base_query` as the new 7th argument (see journal note below) |
| `pg_stat_reset()` | yes, once before the refresh bench |
| Timing | psql `\timing on` |
| Bench drivers | `/tmp/pg_reflex_bench_rerun/run_refresh.sh`, `.../run_flush.sh` (copies of 2026-04-21 originals, results written to `results_refresh.txt` / `results_flush.txt` in the same folder) |

Each flush trial pattern: `BEGIN; UPDATE <src> SET id=id WHERE ctid IN (SELECT ctid FROM <src> LIMIT 1000); SELECT LEFT(reflex_flush_deferred('<src>'), 200); ROLLBACK;`. Each REFRESH trial: `CREATE MATERIALIZED VIEW bench_<name> AS <original SQL>; DROP MATERIALIZED VIEW bench_<name>;`.

### Migration-path note (the reason the extension needed a version bump)

The 2026-04-21 fixes changed `reflex_build_delta_sql`'s signature from 6 args to 7 (adding `orig_base_query`). `db_clone` had already been populated with 141 pgrx-generated plpgsql trigger bodies that called the 6-arg version. A `.so` swap alone would have made every DML on a trigger-owning table fail with a function-signature error. The 1.1.1→1.1.2 upgrade script:

1. `DROP FUNCTION reflex_build_delta_sql(TEXT,TEXT,TEXT,TEXT,TEXT,TEXT)` and `CREATE FUNCTION … (7 args) …` pointing at `reflex_build_delta_sql_wrapper`.
2. For each plpgsql function whose body contains the 6-arg call pattern (`…_rec.aggregations)`), rewrite the body in-place via `UPDATE pg_proc SET prosrc = regexp_replace(...)` to append `, _rec.base_query`. Superuser required. 141 bodies patched, 0 stale after migration.

This migration is obviously a catalog-rewrite operation and deserves the caution the tooling already flagged — kept in the journal so future upgrades of long-lived tenants know they have to go through the same path rather than a DROP/CREATE EXTENSION cycle (which would lose every `__reflex_*` trigger and every IMV).

## Results table

Speedup = (REFRESH ms ÷ flush ms). >1 means pg_reflex flush wins. `n/a` = source table empty, no incremental signal.

| IMV | target rows | REFRESH ms | flush ms | FLUSHED ops | UPDATE-trigger ms | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| demand_planning_characteristics_reflex | 16 | 121,150 | 234,309 | 18 | 2,676 | 0.52× |
| sop_forecast_history_reflex | 2,584,388 | **4,400** | 251,740 | 18 | 1,165 | 0.017× (57× slower) |
| event_demand_planning_sales | 0 | 10,351 | 359,031 | 18 | 999 | 0.029× |
| zscore_reflex | 720,593 | 58,748 | 239,531 | 18 | 1,017 | 0.25× |
| forecast_stock_chart_weekly_reflex | 1,283,232 | 1,890 | **13** | 3 | 70 | **145×** |
| stock_transfer_baseline_reflex | 0 | 16 | n/a | — | — | n/a (empty source) |
| stock_transfer_reflex | 0 | 14 | n/a | — | — | n/a (empty source) |
| history_sales_reflex | 1,683,115 | 12,272 | 76,556 | 6 | 575 | 0.16× |
| last_month_sales_reflex | 22,128 | 1,718 | 71,722 | 6 | 27 | 0.024× |
| sop_forecast_reflex | 7,732,941 | 22,607 | 188,421 | 18 | 2,151 | 0.12× |
| sop_last_forecast_reflex | 2,935,231 | 8,302 | **7,046,595** | 3 | 133 | **0.0012× (849× slower, worst case)** |
| unsent_sop_forecast_reflex | 22,534,897 | 83,645 | 229,964 | 18 | 1,571 | 0.36× |
| sop_purchase_reflex | 64,800 | 870 | **233** | 3 | 117 | **3.75×** |
| sop_purchase_baseline_reflex | 141,938 | 724 | **309** | 3 | 112 | **2.35×** |

All 12 non-empty flushes **completed cleanly** (no code-gen errors). The three 2026-04-21 bugs are fully resolved — no `missing FROM-clause`, no `AS __dt AS ol` syntax errors, no flushed-3-and-abort cascades.

## Commentary

### Clean wins

- **`sop_purchase_baseline_reflex` 2.4×**, **`sop_purchase_reflex` 3.8×**, **`forecast_stock_chart_weekly_reflex` 145×**. The common shape: purely-additive aggregates (`SUM` + `COUNT`), small-to-moderate source touches, few downstream IMVs, no BOOL_OR/MIN/MAX retract cost. These are the workload pattern pg_reflex is clearly net-positive on.
- `fscw`'s 145× is inflated because the 1000-row UPDATE on `location_inventory_baseline` hit rows that the IMV's WHERE predicate filtered out — effectively a no-op flush in 13 ms. The true steady-state speedup on a row actually inside the filter would be smaller, but still positive.

### Losses: dominated by cascade + BOOL_OR recompute

- Every `sales_simulation`-sourced IMV flush processes **18 deferred operations** because sales_simulation fans out into 6 direct + indirect IMVs and each operation records INSERT/DELETE/UPDATE-derived deltas. The per-IMV flush time (3–6 minutes) is the **cascade total**, not the time the one named IMV took — the table column "flush ms" is therefore the cost of a full cascade flush, not a single-IMV flush.
- Within each cascade the dominant cost is the **full-scan recompute for BOOL_OR / MIN / MAX aggregates** introduced by the 2026-04-21 correctness fix. On `unsent_sop_forecast_reflex`, this is an `UPDATE ... FROM (orig_base_query) AS __src ...` scan of a query whose inputs include the 30 M-row intermediate — every flush pays ~2 minutes just for that one recompute. See optimization idea #1 (`journal/2026-04-22_optimization_ideas.md`) for the `__bool_or_true_count_X` companion column plan to eliminate this.
- `sop_last_forecast_reflex` flush took **1 h 57 min** (7,046,595 ms). This is an outlier driven by the same recompute pattern, amplified by the `last_sales_simulation` source carrying a schema where the recompute query degenerates into a cartesian-product-like plan. The recompute-correctness trade-off from 2026-04-21 was accepted knowing this cost existed; the benchmark numerically confirms it is unusable on this specific IMV without the algebraic-BOOL_OR fix.

### REFRESH baseline on PG18 is stronger than expected

Numbers from the 2026-04-21 run (cold) vs 2026-04-22 (warm, pg_stat fresh):

| IMV | 2026-04-21 REFRESH ms | 2026-04-22 REFRESH ms |
|---|---:|---:|
| sop_forecast_history_reflex | 6,689 | 4,400 |
| zscore_reflex | 159,214 | 58,748 |
| history_sales_reflex | 132,093 | 12,272 |
| unsent_sop_forecast_reflex | 125,733 | 83,645 |

Most of the speedup is PG18's warm cache + autovacuum-fresh statistics. The REFRESH-warm baseline on wide fact tables with parallel aggregation is a tough bar for any incremental engine to clear — especially one that pays full-scan recompute on every retract. This is consistent with the 2026-04-21 conclusion that REFRESH is the right default on wide, periodically-rebuilt fact tables, and pg_reflex wins only on tables where (a) retracts are rare, (b) cascades are shallow, or (c) the IMV is not BOOL_OR/MIN/MAX heavy.

## Bugs and issues surfaced during this run

- **Identifier-truncation NOTICE spam.** Every flush that touches a sub-IMV (CTE-derived, like `demand_planning_characteristics_reflex__cte_sales_stats` at 59 chars) emits a `NOTICE: identifier "__reflex_old_<…>" will be truncated to 63 chars`. The transition / delta table names in `trigger.rs:467-468`, `trigger.rs:950`, and `schema_builder.rs:304-305, 426-428, 624` do not go through `safe_identifier`. Documented as bug #1 in `journal/2026-04-22_bug_report.md`. High-severity because two long sub-IMV names that share a 63-char prefix would silently collide into the same staging table.
- **`sop_last_forecast_reflex` flush plan degenerates to ~2 hours.** The full-scan recompute introduced 2026-04-21 is responsible. Not a code bug — a known cost of the correctness fix. Optimization idea #1 (BOOL_OR via counter) removes this cost.
- **Other audit findings** (HAVING-only MIN/MAX not recomputed, COUNT DISTINCT NULL-key match, recursive-CTE hang risk, UNION ALL quote loss, etc.) are not exercised by this bench — they are documented in `journal/2026-04-22_bug_report.md` from source-tree audit.

## Artifacts

- `/tmp/pg_reflex_bench_rerun/` — driver scripts + raw psql output
  - `results_refresh.txt` — REFRESH timings
  - `results_flush.txt` — flush timings (includes the NOTICE lines documenting bug #1)
  - `run_refresh.sh`, `run_flush.sh` — bash drivers (same as 2026-04-21, just retargeted output paths)
  - `01_*.sql` … `14_*.sql` — per-IMV twin MATVIEW bodies
- `sql/pg_reflex--1.1.1--1.1.2.sql` — the upgrade migration that ships the 7-arg `reflex_build_delta_sql` and patches existing trigger bodies

## Related journals

- `journal/2026-04-22_bug_report.md` — latent bugs discovered in source audit + this bench
- `journal/2026-04-22_optimization_ideas.md` — ranked optimization opportunities (BOOL_OR counter, end-query incremental UPDATE, topological skip, etc.)
- `journal/2026-04-22_unsupported_views.md` — views intentionally kept as matviews and why

## Next steps

The credible-speedup story is now bimodal:

- **Pro-incremental workloads** (additive aggregates, shallow cascades, few BOOL_OR/MIN/MAX): pg_reflex wins 2–4× today. `sop_purchase*` are the pattern.
- **Retract-heavy or BOOL_OR-heavy workloads**: pg_reflex is currently worse than REFRESH by 10–800×. Requires either the BOOL_OR counter optimization (idea #1) or the end-query incremental UPDATE (idea #5) to close the gap.

A realistic next development cycle: ship ideas #3 (empty-affected-groups short-circuit, 1 day), #9 (PARALLEL SAFE marking, 1 hour), #1 (BOOL_OR counter, ~week), and benchmark again. That pairing should move the sales_simulation cascades from "3–6 min" to a range where incremental is at least competitive with REFRESH, and would let us delete the "full-scan recompute" caveat from this journal.
