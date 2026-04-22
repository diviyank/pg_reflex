# Optimization ideas — pg_reflex 1.1.2 (2026-04-22)

**Scope**: flush-path performance improvements surfaced by the 2026-04-22 `db_clone` rerun and by source audit. **Not implemented** — noted here for a future development cycle. Ordered by expected ROI.

For each idea: **What** (concrete change), **Why** (the wasteful behavior it removes), **How** (code path and rough design), **What for** (which workload benefits), and a complexity/risk note.

---

## 1. Algebraic `BOOL_OR` via `__bool_or_true_count_X` companion column

- **What**: Add a counter column per `BOOL_OR` intermediate that tracks the number of TRUE-valued contributing rows. On delta add-TRUE, `+1`; on delta subtract-TRUE, `-1`. `BOOL_OR` = `count > 0`.
- **Why**: Today every DELETE / UPDATE that touches a `BOOL_OR`-bearing IMV triggers a full-scan `UPDATE … FROM (orig_base_query) AS __src` recompute (correctness fix from 2026-04-21). On `unsent_sop_forecast_reflex` that is a 30 M-row scan per flush. The counter approach makes the aggregate algebraically maintainable, same trick as `SUM + COUNT` already uses for average-style aggregates.
- **How**:
  - In `src/aggregation.rs`, add a `BoolOrWithCount` column variant that allocates a sibling `__bool_or_true_count_<expr>` INT column next to the boolean value.
  - In `src/trigger.rs::build_merge_sql`, the SET clause becomes `__bool_or_X = (__bool_or_X_count + delta.count) > 0, __bool_or_X_count = __bool_or_X_count + delta.count`.
  - In `reflex_build_delta_sql`, the INSERT-delta computes `count = SUM(CASE WHEN expr THEN 1 ELSE 0 END)`; the DELETE-delta computes `count = -SUM(CASE WHEN expr THEN 1 ELSE 0 END)`.
  - Remove the MIN/MAX recompute dispatch branch for `BOOL_OR` (keep it only for `MIN`/`MAX`).
- **What for**: Any IMV using `BOOL_OR` — today `sop_purchase_reflex` and `unsent_sop_forecast_reflex` (via `caav.product_id IS NOT NULL`). Expected flush time drops from 100+ s to under a second on those IMVs (matching `SUM` performance).
- **Complexity**: medium. Needs migration (intermediate tables grow a column per BOOL_OR), regression tests for NULL-input rows (`BOOL_OR(NULL)` contributes 0 to count), and one unit test for the counter underflow edge case.
- **Expected ROI**: 10–100× on BOOL_OR-heavy flushes. Highest single win in this list.

---

## 2. Algebraic `MIN`/`MAX` via bounded top-K companion heap

- **What**: Maintain a per-group bounded min-heap (for MIN) / max-heap (for MAX) of size K (e.g., 16). Store heap as an ordered-array companion column. On add: heap-insert. On subtract: heap-remove; if heap size drops to 0, fall back to a full recompute only for that group.
- **Why**: Same full-scan problem as BOOL_OR, but MIN/MAX can't be algebraically maintained with a single counter — the retracted value might be the current top-1. A bounded top-K amortizes the rare case where retraction hits the top and no alternative is in the cache.
- **How**:
  - In `src/aggregation.rs`, add `MinMaxWithTopK { k: 16 }` variant. Emit an extra `<agg>_topk` column (arrays of the element type).
  - In `src/trigger.rs::build_merge_sql`, replace today's subtract-and-recompute with heap-update SQL. Falling back to full recompute only when `array_length(heap, 1) = 0` after subtract (rare).
  - Optionally persist the heap as a separate per-IMV table to keep the intermediate row width small.
- **What for**: `stock_chart_weekly_reflex`, `stock_chart_monthly_reflex`, `forecast_stock_chart_monthly_reflex` — today kept as plain matviews *because* of the MIN/MAX delete cost. Unlocking these would move ~3 views from §6 of `2026-04-22_unsupported_views.md` into IMV territory.
- **Complexity**: high. SQL-level heap maintenance is painful in plain MERGE; likely easier as a set-returning plpgsql helper.
- **Expected ROI**: 5–50× on MIN/MAX flush. Unlocks currently-unsupported views.

---

## 3. Skip target refresh when `affected_groups` is empty (zero-net-delta)

- **What**: After the MERGE populates `__reflex_affected_<view>`, add a `WHERE __ivm_count != 0` filter when propagating to the target. If the table ends up empty, skip the `DELETE FROM target … + INSERT INTO target …` pair entirely.
- **Why**: Insert-then-delete of the same row inside a single flush yields zero net delta, but today the target-refresh code runs unconditionally. On large intermediates (sop_forecast 7 M, unsent_sop_forecast 30 M) a skip saves the whole DELETE/INSERT pass.
- **How**:
  - In `src/trigger.rs:398-439` (`push_merge_and_affected`), add `WHERE __reflex_op != 'NOOP'` or compute `__ivm_count` sum per group and filter.
  - In `src/trigger.rs:862-895` (target refresh), guard the DELETE/INSERT with `IF EXISTS(SELECT 1 FROM __reflex_affected_X) THEN …`.
- **What for**: Any flush where deltas cancel locally. Expected 5–20 % on mixed update/delete workloads; the biggest win is when a `sales_simulation` UPDATE touches 1000 rows but modifies only columns unrelated to the aggregates.
- **Complexity**: low. Single boolean guard + one WHERE clause. No correctness risk.
- **Expected ROI**: 5–20 % across the board.

---

## 4. Topological cascading flush + skip downstream when upstream produced no affected groups

- **What**: In `reflex_flush_deferred` restructure the IMV loop to:
  1. Process all IMVs at `graph_depth = d` first.
  2. Between depth levels, materialize `__reflex_affected_*` as the signal for the next level.
  3. If an IMV at depth `d+1` only depends on IMVs at depth `d` that produced an empty affected-groups table, skip it entirely.
- **Why**: Today the flush loop iterates IMVs in depth order but treats each independently. When `demand_planning_characteristics_reflex` (depth 1) detects that its delta reduces to zero groups, the chain (`sop_forecast_history_reflex`, `sop_forecast_reflex`, `unsent_sop_forecast_reflex`) still runs its full delta path because it doesn't know the upstream was a no-op.
- **How**:
  - Extend `__reflex_deferred_pending` to carry an "affected_rows" counter per IMV.
  - In `reflex_flush_deferred`, after each IMV completes, write the affected-rows count; downstream IMVs check their upstreams and early-exit.
  - Optionally use `SAVEPOINT` at each depth level so one IMV's abort doesn't cascade-abort the whole flush.
- **What for**: Cascades involving `sales_simulation` (touches 5+ IMVs). Best win when users repeatedly UPDATE the same rows: each flush after the first should see near-zero downstream work.
- **Complexity**: medium. Requires modification to the deferred-pending bookkeeping.
- **Expected ROI**: 10–30 % on cascades.

---

## 5. End-query: emit incremental `UPDATE target … FROM intermediate` instead of full truncate + insert

- **What**: Replace the "DELETE FROM target + INSERT FROM intermediate" refresh with a targeted `UPDATE target SET col = i.col FROM intermediate i WHERE target.key = i.key AND target.key IN (SELECT key FROM __reflex_affected_<view>)`.
- **Why**: Currently when `end_query_has_group_by=true` the target is always rebuilt in full from the intermediate. On large IMVs (30 M-row intermediate, 1K affected groups) we rewrite the entire target table — this is the dominant cost of flush for `unsent_sop_forecast_reflex`.
- **How**:
  - Add a new branch in `src/trigger.rs:862-895` that emits an UPDATE statement per aggregate column.
  - For groups where `__ivm_count = 0`, emit a DELETE instead (group was fully retracted).
  - Leave the full-rebuild path as a fallback for the first flush or when no unique key is available.
- **What for**: Large IMVs with many rows but small deltas (all `db_clone` fact IMVs). Expected 5–50× speedup on those.
- **Complexity**: medium. Correctness hinges on having a reliable unique key on the target — already enforced by `unique_columns` in the registry.
- **Expected ROI**: 5–50× on large-target IMVs — probably the highest realistic gain on the current bench.

---

## 6. CTE deduplication across sibling IMVs

- **What**: Detect CTE bodies that appear verbatim in multiple IMV base queries at the same flush. Materialize once to a temp table, rewrite each IMV's base query to reference the temp table.
- **Why**: `sop_forecast_reflex`, `sop_last_forecast_reflex`, `unsent_sop_forecast_reflex` all share the `current_assortment_activity_reflex` lookup and a handful of sales_simulation aggregations. Each IMV flush re-executes those CTEs independently.
- **How**:
  - Hash each CTE body at `create_reflex_ivm` time; store the hash in `__reflex_ivm_reference.depends_on_cte`.
  - At flush time, before processing depth N IMVs, materialize any CTE whose hash appears ≥2 times across the upcoming batch.
  - Rewrite each referencing base_query to `WITH cte AS (SELECT * FROM __reflex_tmp_cte_<hash>) …`.
- **What for**: Cascades with repeated CTEs, very common in the sop/supply plan schemas.
- **Complexity**: medium-high. Requires careful AST-level CTE extraction + body hashing + rewrite. Temp table lifecycle pinned to flush transaction.
- **Expected ROI**: 10–30 % on CTE-heavy cascades.

---

## 7. Cache trigger-body SQL templates (amortize `reflex_build_delta_sql` round-trips)

- **What**: At `create_reflex_ivm` time, pre-generate the delta SQL template and store it alongside `base_query`. At trigger-fire time, the plpgsql body does `FORMAT(_rec.delta_template, _rec.new_tbl, _rec.old_tbl, …)` + `EXECUTE` instead of calling `reflex_build_delta_sql` as a Rust FFI.
- **Why**: `reflex_build_delta_sql` is a heavy Rust call — it re-parses `aggregations_json`, re-builds the SQL string byte-by-byte, and re-applies `replace_source_with_delta` on every trigger firing. For IMMEDIATE-mode IMVs with high trigger-firing frequency this is real overhead.
- **How**:
  - Add `delta_template_insert`, `delta_template_delete`, `delta_template_update` columns to `__reflex_ivm_reference`.
  - Update `src/schema_builder.rs` trigger bodies to use stored templates.
  - Invalidate cache on any IMV metadata change (add a `schema_version` counter).
- **What for**: IMMEDIATE mode IMVs and high-frequency small-transaction workloads. DEFERRED mode wins less here because `reflex_flush_deferred` already batches per source.
- **Complexity**: medium. Template substitution must handle the transition-table-name substitution that pgrx currently does at Rust level.
- **Expected ROI**: 5–20 % on IMMEDIATE; smaller on DEFERRED.

---

## 8. Lazy index maintenance on UNLOGGED intermediate tables during bulk rebuild

- **What**: When a flush does a full rebuild of the intermediate (TRUNCATE + INSERT), drop the intermediate's indexes before the INSERT and re-`CREATE INDEX` after. For targeted flushes (small affected-groups), keep incremental maintenance.
- **Why**: Bulk INSERT into an indexed table pays index-maintenance cost per row. Drop-build-recreate amortizes that over a single index scan. PG's `CREATE INDEX` uses parallel workers on PG18 which makes this very cheap.
- **How**:
  - In `reflex_build_delta_sql`, detect "full rebuild" case (e.g., when affected_groups > some threshold relative to intermediate size).
  - Emit `DROP INDEX IF EXISTS`, then the INSERT, then `CREATE INDEX` as appended statements.
- **What for**: Large intermediate tables (unsent_sop_forecast_reflex 30 M rows). First flush after cold start is the main beneficiary.
- **Complexity**: low-medium. Need to avoid dropping indexes that other queries hold locks on.
- **Expected ROI**: 5–15 % on big rebuilds.

---

## 9. Mark `reflex_build_delta_sql` as `PARALLEL SAFE`

- **What**: Add `PARALLEL SAFE` annotation to the pgrx `#[pg_extern]` definition.
- **Why**: The function only reads its arguments + `__reflex_ivm_reference` (read-only catalog-like). Today pgrx defaults to `PARALLEL UNSAFE`, which blocks the planner from parallelizing any plpgsql loop that calls it.
- **How**: `#[pg_extern(parallel_safe)]` in `src/trigger.rs:446`. Verify by looking at the generated SQL — should emit `PARALLEL SAFE` in the CREATE FUNCTION.
- **What for**: Breadth-first cascades where multiple IMVs live at the same depth and could in principle flush in parallel.
- **Complexity**: very low. Audit only required: confirm no SPI writes happen inside the function (they don't, as of 1.1.2).
- **Expected ROI**: 1–3× on breadth-heavy graphs (modest in `db_clone` but the change itself is a one-liner).

---

## 10. Streaming statement-split in plpgsql (replace `string_to_array` on `REFLEX_SEP`)

- **What**: Replace `FOREACH _stmt IN ARRAY string_to_array(_sql, E'\\n--<<REFLEX_SEP>>--\\n') LOOP EXECUTE _stmt; END LOOP;` with a streaming-split helper that finds each delimiter, executes the slice, and moves on without materializing an array.
- **Why**: `string_to_array` on a multi-megabyte SQL string allocates a plpgsql array of TEXT values. On large cascades (5+ IMVs × 5+ statements each) the concatenated SQL can exceed 500 KB. Streaming avoids the allocation and reduces plpgsql overhead.
- **How**: Write a C / pgrx helper `reflex_execute_separated(sql text)` that walks the buffer, calling `SPI_execute` on each slice.
- **What for**: Very large cascades. Marginal on the current bench.
- **Complexity**: low. pgrx helper + one line change in the trigger body.
- **Expected ROI**: 1–5 % on large flushes.

---

## 11. Delta-table `ANALYZE` after TRUNCATE

- **What**: After `reflex_flush_deferred` TRUNCATEs `__reflex_delta_<src>`, emit `ANALYZE __reflex_delta_<src>` at the end of the transaction. Optionally, run it *before* the subsequent flush if the delta has grown significantly.
- **Why**: UNLOGGED TRUNCATEs reset pg_class statistics to zero-row assumptions. On the next flush the planner uses worst-case cardinality estimates for delta-filter predicates (`__reflex_op = 'I'` etc.). For large deltas this produces bad join orders.
- **How**: Append an `ANALYZE` statement to the flush script or to the deferred_pending cleanup path.
- **What for**: High-frequency flush workloads.
- **Complexity**: very low.
- **Expected ROI**: 1–5 % in steady state; higher after a cold start.

---

## 12. Move advisory-lock acquisition to *after* the `where_predicate` filter

- **What**: In `src/schema_builder.rs:329, 449, 517, 557` and `src/trigger.rs:1052-1062`, move the `PERFORM pg_advisory_xact_lock(hashtext(_rec.name))` call to after the `CONTINUE` decision on `where_predicate`.
- **Why**: Today the lock is acquired for every IMV in the loop, even for those whose `where_predicate` will skip them. The lock is cheap but not free, and when many IMVs have selective predicates, the skipped ones hold lock-table entries unnecessarily.
- **How**: Reorder the two blocks in the four trigger-body templates.
- **What for**: Partitioned IMVs with many predicates.
- **Complexity**: trivial.
- **Expected ROI**: 1–3 %.

---

## Quick-wins summary (sorted by value ÷ effort)

| # | Idea | Effort | Gain | Best target |
|--:|---|---|---|---|
| 3 | Empty affected-groups short-circuit | 1 day | 5–20 % | every IMV |
| 5 | End-query incremental UPDATE | 3–5 days | 5–50× | large-target IMVs |
| 1 | BOOL_OR counter column | 3–5 days | 10–100× | BOOL_OR IMVs |
| 9 | PARALLEL SAFE marking | 1 hour | 1–3× | breadth graphs |
| 11 | ANALYZE after TRUNCATE | 1 hour | 1–5 % | steady-state |
| 12 | Advisory lock reorder | 1 hour | 1–3 % | selective-predicate IMVs |
| 8 | Lazy index maintenance | 2 days | 5–15 % | full rebuilds |
| 4 | Topological skip | 4 days | 10–30 % | sales_simulation cascade |
| 7 | Trigger template cache | 5 days | 5–20 % | IMMEDIATE |
| 6 | CTE dedup | 1 week | 10–30 % | sop/supply |
| 2 | MIN/MAX top-K | 1–2 weeks | 5–50× | unlocks 3+ views |
| 10 | Streaming statement-split | 2 days | 1–5 % | large cascades |

Recommended first sprint (ordered): #3, #9, #11, #12 in one day (quick correctness-safe wins), then #5 as the anchor feature (biggest impact on measurable bench), then #1 to unlock the BOOL_OR cliff. #2 is the followup that would let us remove the "kept as matview" annotations from 3 additional views.
