# Plan #10 — MERGE on target (eliminate DELETE+INSERT duplication, 1.4.6)

## TL;DR

Replace the trigger's two-statement target sync (`DELETE FROM target WHERE in_affected; INSERT INTO target SELECT … WHERE in_affected;`) with a single `MERGE INTO target USING intermediate`. Saves one full intermediate scan per UPDATE on the MERGE-path codepath (the path that fires below the wipe-and-replace threshold). Estimated impact at customer scale: ~1 s saved per fire. Smaller than #7 or #9 but stackable.

## Background

The 1.4.5 trigger codegen for GROUP-BY IMVs emits this target-sync sequence (when the high-selectivity dispatch is NOT taken):

```sql
-- ~src/trigger.rs:1703-1716 (the `else if let Some(ref cols) = grp_cols` branch)

DELETE FROM "schema"."target_view"
WHERE EXISTS (
    SELECT 1 FROM "schema"."__reflex_affected_view" AS __a
    WHERE target_view."gb1" = __a."gb1" AND ...
);

INSERT INTO "schema"."target_view"
SELECT <end_query expressions>
FROM "schema"."__reflex_intermediate_view"
WHERE __ivm_count > 0
  AND EXISTS (
    SELECT 1 FROM "schema"."__reflex_affected_view" AS __a
    WHERE intermediate_view."gb1" = __a."gb1" AND ...
  );
```

Two passes. Each pass at customer scale (867 K target, 555 K affected):
- DELETE pass: scan target (867 K rows), hash-join against affected (555 K), delete 555 K → ~0.9 s.
- INSERT pass: scan intermediate (867 K rows), hash-join against affected (555 K), insert 555 K → ~3.2 s.

**Total: ~4.1 s of target-side work**, dominated by the two seq scans.

The post-1.4.4 instrumented bench (`journal/2026-05-12_1_4_3_and_1_4_4_customer_unblock.md`) attributes 4.1 s of the 14.8 s warm UPDATE to these two statements. On the customer's 76 M-source / 867 K-target shape they are unavoidable today.

## What MERGE buys us

`MERGE INTO target USING (intermediate filtered by affected) ON gb-cols WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT WHEN NOT MATCHED BY SOURCE THEN DELETE` collapses the two passes into one. Specifically:

- **Single scan** of the target (the MERGE's outer relation).
- **Per-row dispatch** to UPDATE / INSERT / DELETE based on intermediate's state for that group.
- **Possible HOT updates** on WHEN MATCHED UPDATE if no indexed column changes (target's only index is the composite on group cols, which never changes during this MERGE — only the aggregate-output columns do).

The `WHEN NOT MATCHED BY SOURCE THEN DELETE` clause (PG 17+ MERGE) is the killer feature. It handles the "group existed in target but no longer in intermediate after the delta" case directly, eliminating the DELETE-first pattern.

The journal already flagged this:

> To get the target side into HOT territory too, the target-sync codegen would need to be rewritten to use MERGE (with WHEN MATCHED UPDATE for existing target rows and WHEN NOT MATCHED INSERT for new ones). Estimated win: another ~100 ms on this workload (the target DELETE+INSERT phase). Tradeoff: significant codegen complexity, would need to handle DERIVED column recomputation in UPDATE form.

That estimate (100 ms) was on the 47 K-affected bench. At customer's 555 K-affected the absolute win scales — closer to 1 s.

## Why this isn't subsumed by #7

The high-selectivity dispatch (#7) takes a different path entirely (full reconcile via TRUNCATE+rebuild). When the dispatch fires (selectivity ≥ 0.3), the target-sync codepath doesn't run.

But at **low-to-medium selectivity** (e.g., a precise sales_simulation UPDATE affecting <100 K rows), the MERGE incremental path is faster than reconcile. The current DELETE+INSERT target sync remains the bottleneck *of that path*. #10 optimizes the path #7 chose NOT to take.

Stacked perf picture at the customer scale (76 M source, 867 K intermediate, various selectivities):

| Selectivity | 1.4.5 path | 1.4.5 time | 1.4.6 path (with #10) | 1.4.6 time | Notes |
|---:|---|---:|---|---:|---|
| 1 % (8.7 K affected) | MERGE incremental | 0.8 s | MERGE + target-MERGE | ~0.6 s | save ~25 % of target-sync (0.5 s → 0.3 s) |
| 10 % (87 K affected) | MERGE incremental | 3 s | MERGE + target-MERGE | ~2.4 s | similar |
| 30 % (260 K affected, just below threshold) | MERGE incremental | 6 s | MERGE + target-MERGE | ~4.5 s | |
| 64 % (555 K affected) | dispatch → reconcile | 24 s | unchanged | 24 s | #10 doesn't fire |
| 75 % (650 K affected) | dispatch → reconcile | 25 s | unchanged | 25 s | #10 doesn't fire |

So #10 is incremental-path-only optimization. Acceptable.

## Detailed codegen

The current trigger emits these two strings (constructed at `trigger.rs:1700-1716`):

```rust
let tdel = format!("DELETE FROM {} WHERE {}", qv, ns_in_target_delete);
let tins = format!("INSERT INTO {} {} AND {}", qv, end_query, ns_in_intermediate);
```

`end_query` already contains `WHERE __ivm_count > 0` as its filter; the codegen appends `AND <ns_in_intermediate>` to scope to affected groups.

For #10, replace with one MERGE statement:

```sql
MERGE INTO "schema"."target_view" AS t
USING (
    SELECT <end_query column list — gb cols + aggregate output expressions>
    FROM "schema"."__reflex_intermediate_view"
    WHERE __ivm_count > 0
      AND EXISTS (
          SELECT 1 FROM "schema"."__reflex_affected_view" AS __a
          WHERE __reflex_intermediate_view."gb1" = __a."gb1" AND ...
      )
) AS d
ON t."gb1" = d."gb1" AND ...
WHEN MATCHED THEN UPDATE SET <aggregate output cols> = d.<aggregate output cols>
WHEN NOT MATCHED THEN INSERT (gb cols, agg cols) VALUES (d.gb cols, d.agg cols)
WHEN NOT MATCHED BY SOURCE AND EXISTS (
    SELECT 1 FROM "schema"."__reflex_affected_view" AS __a
    WHERE t."gb1" = __a."gb1" AND ...
) THEN DELETE;
```

The trickiest part is the **`WHEN NOT MATCHED BY SOURCE`** clause: it fires for target rows that no longer appear in the USING relation. Without the EXISTS filter against affected, MERGE would delete every target row that doesn't appear in the (filtered-to-affected) USING — which is most of them. The EXISTS scopes the DELETE to rows that *were* affected but no longer satisfy the intermediate's `__ivm_count > 0` filter.

In other words:
- USING relation: groups in affected that have post-delta data (intermediate rows with __ivm_count > 0).
- Target rows matching USING on gb cols → UPDATE (these rows still exist post-delta, values may have changed).
- USING rows not matching any target → INSERT (new groups).
- Target rows where gb cols are in affected but no match in USING → DELETE (group existed pre-delta but post-delta `__ivm_count` dropped to 0).

A subtle thing: the existing DELETE+INSERT approach removes ALL affected target rows then re-inserts only the ones with __ivm_count > 0. MERGE achieves the same end-state via separate WHEN clauses. Need to verify the WHEN NOT MATCHED BY SOURCE clause's EXISTS predicate correctly identifies the "should be deleted" rows.

### Building the USING column list

`end_query` already projects exactly the output columns. We can wrap it as a CTE-like subquery in MERGE's USING. The shape:

```rust
let using_subquery = format!(
    "({})",
    end_query  // already has WHERE __ivm_count > 0
        .trim_end_matches(';')
        .to_string()
        + &format!(" AND {}", ns_in_intermediate)
);
```

The MERGE ON clause uses the same `=` vs `IS NOT DISTINCT FROM` choice as the intermediate MERGE in `build_merge_using` — driven by `plan.not_null_columns`. Reuse that logic.

### Composite of `target_group_columns` mapping

When the user aliases group-by columns (`SELECT dp.id AS dem_plan_id`), the target table column is `dem_plan_id` but the intermediate column is `dem_plan_id` too (per the AggregationPlan normalization). The MERGE ON clause keys against the target's column names, which is what `target_group_columns(&plan)` returns.

The aggregate output column names in the SET clause come from `plan.end_query_mappings` — each mapping has an `output_alias` that's the target column.

Build SET clauses from end_query_mappings:

```rust
let set_clauses: Vec<String> = plan.end_query_mappings.iter().map(|m| {
    format!("\"{0}\" = d.\"{0}\"", m.output_alias)
}).collect();
```

The MERGE then SETs each aggregate column from the corresponding USING column.

## Files touched

| Area | File | Change |
|---|---|---|
| Codegen | `src/trigger.rs:1703-1717` | Replace `tdel + tins` pair with a single MERGE-emitting helper. Keep the dispatch DO block path intact (it still uses MERGE for low-selectivity, but the inner statements switch). |
| Helper | `src/trigger.rs` (new) | `build_target_merge_sql(target, intermediate, affected, end_query, plan)` — emits the full MERGE statement. |
| Dispatch | `src/trigger.rs:1860-1864` | The high-selectivity dispatch's `else` branch (MERGE incremental) currently constructs `tdel + tins`. Pass the target-MERGE through the dispatch as a single statement instead. |
| Tests | `src/tests/unit_trigger.rs` | Unit tests for `build_target_merge_sql`: NOT NULL group cols emit `=`, aliased group cols use target-side names, end_query_mappings → SET clauses. |
| Tests | `src/tests/pg_test_basic.rs` | (1) `pg_test_target_merge_correctness_insert_delete_update` — INSERT/DELETE/UPDATE on source produces target state matching fresh aggregate. (2) `pg_test_target_merge_when_not_matched_by_source_deletes` — when an UPDATE moves __ivm_count to 0, target row gets DELETED. (3) `pg_test_target_merge_hot_update_observed` — pg_stat_user_tables.n_tup_hot_upd > 0 after the MERGE (the win signal). |
| Bench | `benchmarks/bench_1_4_6_target_merge.sql` (new) | Synthetic at 600 K target / 60 K affected (10 % selectivity, dispatch doesn't fire). Compare pre-#10 (DELETE+INSERT) vs post-#10 (MERGE). |

## Correctness invariants

The MERGE on target must produce the exact same end-state as the DELETE+INSERT pair for every combination of:

1. **INSERT op**: new group keys added → MERGE WHEN NOT MATCHED THEN INSERT fires.
2. **DELETE op**: group keys removed → if __ivm_count → 0, WHEN NOT MATCHED BY SOURCE THEN DELETE fires.
3. **UPDATE op**: aggregate values change → WHEN MATCHED THEN UPDATE fires.
4. **Mixed op**: a single trigger fire may produce all three (group X dropped to 0, group Y added, group Z aggregate changed).

Key correctness gates:
- The `WHEN NOT MATCHED BY SOURCE` clause MUST be guarded by `EXISTS (... __reflex_affected ...)`. Without it, every non-affected target row gets DELETEd.
- The MERGE ON clause MUST use `=` for NOT NULL group cols (same as intermediate MERGE) so the planner can use the target's composite index for the join.
- Self-join detection: if the source is multi-referenced in the IMV (the existing `is_self_join` check), the WHEN NOT MATCHED BY SOURCE may delete rows that are still valid via the other reference. Stay with DELETE+INSERT for self-join IMVs (just like the existing path stays with full refresh).

## Performance expectations

| Workload | Pre-#10 target sync | Post-#10 target MERGE | Δ |
|---|---:|---:|---:|
| 47 K affected (db_clone bench) | 421 ms (157 + 264) | ~250 ms | -170 ms |
| 87 K affected (10 % selectivity) | ~800 ms | ~500 ms | -300 ms |
| 555 K affected (customer scale, hypothetical incr path) | ~4.1 s | ~2.5 s | -1.6 s |

The win comes from:
1. Single target scan instead of two (saves ~30-40 % of scan time).
2. Per-row HOT updates on WHEN MATCHED branch (no index updates because target's composite index covers gb cols which don't change). This was *the* win for the intermediate MERGE post-fillfactor.
3. Reduced WAL — one tuple update instead of one DELETE + one INSERT.

The target's fillfactor=70 (set by 1.4.4 migration) is already in place to support HOT. Operators who ran `reflex_compact_imv()` already get the HOT benefit on their target.

## Risk and rollback

- **MERGE syntax is PG 17+**. PG 15/16 builds need a fallback. The Cargo.toml supports pg15/pg16/pg17/pg18 as features. The `pg17` feature gates the MERGE codegen; the other feature flags keep the DELETE+INSERT path.
  - Implementation: `#[cfg(feature = "pg17")]` or `#[cfg(feature = "pg18")]` on the MERGE codegen; the older feature flags fall through to the existing path. (Actually the existing intermediate MERGE in `build_merge_using` is already PG 15+ — MERGE was introduced in PG 15. The `WHEN NOT MATCHED BY SOURCE` clause is PG 17+. Need to confirm.)
- **MERGE planner choices**: PG's MERGE planner may pick a hash join or a merge join over the target × USING depending on stats. If stats are stale, the plan can be suboptimal. Add an ANALYZE on intermediate before the target MERGE? Probably not — the intermediate is the IMV's "live" state and gets ANALYZE'd by autovacuum.
- **Rollback**: gate behind `SET reflex.use_target_merge = on/off`. Default off in 1.4.6.0, on in 1.4.6.1 after field validation.

## Edge cases

1. **No aggregate output columns to SET** (degenerate IMV where the output is just the GROUP BY cols). MERGE's WHEN MATCHED UPDATE needs at least one SET — emit a no-op `SET "<some_col>" = t."<some_col>"`. PG accepts this.

2. **End_query has computed expressions** (e.g., `CASE WHEN __nonnull_count_x > 0 THEN __sum_x END::BIGINT AS x`). The USING subquery already has these as columns; the SET clause uses the USING's column name (`d.x`). Works.

3. **DISTINCT IMVs** (no GROUP BY but with DISTINCT clause). The target is keyed on distinct_columns. MERGE works the same way — ON clause uses distinct_columns.

4. **Single-group IMVs** (`GROUP BY ()`). Plan.group_by_columns is empty; the trigger codegen takes the `else` branch at the bottom of the dispatch (which does TRUNCATE+rebuild). Skip MERGE here.

5. **Passthrough IMVs**. Don't use MERGE — passthrough has its own DELETE/INSERT pattern keyed on unique columns. Already separate code path.

6. **Top-K IMVs** (`MIN`/`MAX` with `topk_k`). The target carries the top-K array as a column. MERGE WHEN MATCHED UPDATE replaces the array — should work as long as the USING subquery (which is `end_query`) projects the array correctly. The existing end_query already does this.

## Acceptance criteria

1. All existing tests pass.
2. 3 new tests in `pg_test_basic.rs` pass (correctness on INSERT/DELETE/UPDATE mix, WHEN NOT MATCHED BY SOURCE, HOT update observed).
3. Bench at 600 K target / 60 K affected: post-#10 target-sync time < 50 % of pre-#10 (the journal's "~100 ms estimated" extrapolated up).
4. EXCEPT ALL oracle = 0 across all test scenarios.
5. pg_stat_user_tables.n_tup_hot_upd > 0 on a test that previously had n_tup_upd = 0 (proving HOT updates fire on the target).

## Effort estimate

- Codegen helper (`build_target_merge_sql`): 1 day (most complexity is in WHEN NOT MATCHED BY SOURCE + the EXISTS guard).
- Dispatch integration: 0.5 day.
- Tests (unit + pg_test): 1 day.
- Bench: 0.5 day.
- PG 15/16 compat check + fallback: 0.5 day.
- Total: **2.5-3.5 days**.

## Reference: where to start in the codebase

- `src/trigger.rs:1700-1717` — current target sync emission (the lines to replace).
- `src/trigger.rs:94-285` — `build_merge_using` — template for the new `build_target_merge_sql`. Reuse the `=` vs `IS NOT DISTINCT FROM` logic from `plan.not_null_columns`.
- `src/trigger.rs:705-792` — `null_safe_in` — emits the EXISTS predicate used in the WHEN NOT MATCHED BY SOURCE guard.
- `src/aggregation.rs:47-57` — `EndQueryMapping` — the source of truth for output-column → SET-clause mapping.
- `journal/2026-05-13_intermediate_idx_and_fillfactor.md` — the journal section "A surprise: target's `n_tup_upd` is zero" — flagged this opportunity originally.
- `journal/2026-05-13_data_probe_not_null_columns.md` — performance section noting 4.1 s target-sync cost on customer scale.

## Dependency relationships

- **Depends on #7 (shipped)**: #10 modifies the MERGE-incremental path. #7 added the dispatch around it. The dispatch's `else` branch (low-selectivity) is where #10 fires.
- **Subsumed by #9 (planned)**: for filter-equivalent UPDATEs, #9 skips the trigger body before #10's MERGE runs. #10's win applies to UPDATEs that #9 doesn't skip (real data changes).
- **Compatible with #2 (shipped)**: the data-probe's `not_null_columns` set drives the `=` vs `IS NOT DISTINCT FROM` choice in #10's MERGE ON clause too. Reused unchanged.
- **No interaction with #11 (shipped)**: ignore_sources affects whether the trigger fires at all; orthogonal.

## Out of scope

- Backporting MERGE to pg15/pg16 with `WHEN NOT MATCHED BY SOURCE` polyfill via DELETE + UPDATE + INSERT pattern. Significant complexity, defer until customer demand.
- Hand-tuning the MERGE plan via planner hints. Premature.
- Combining target MERGE with intermediate MERGE into a single statement (would require chained USING). PG MERGE doesn't support this; would need CTEs which are non-modifying in MERGE context. Defer.
- Auto-detection of when target MERGE would be slower than DELETE+INSERT (e.g., target has very large rows and the per-row UPDATE is more expensive than DELETE+INSERT). Premature; bench data first.
