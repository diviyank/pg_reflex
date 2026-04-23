# 2026-04-23 — Algebraic `BOOL_OR` via two BIGINT counter columns

Implements optimization #1 from `journal/2026-04-22_optimization_ideas.md`.

## Problem

Every DELETE or UPDATE on a `BOOL_OR`-bearing IMV triggered a full-scan recompute:

```sql
UPDATE intermediate t
SET "__bool_or_X" = __src."__bool_or_X", ...
FROM (orig_base_query) AS __src
WHERE t.gc IS NOT DISTINCT FROM __src.gc AND (t."__bool_or_X" IS NULL OR ...)
```

This was the correct fix introduced 2026-04-21 to handle JOIN aliases in `BOOL_OR` arguments. But it means every flush on `unsent_sop_forecast_reflex` or `sop_purchase_reflex` scans the full 30 M / 7 M row source — 100+ s per flush.

## Design

Replace the single `BOOLEAN` intermediate column with two `BIGINT` `SUM` counters:

| column | type | source expression |
|---|---|---|
| `__bool_or_{arg}_true_count` | BIGINT | `SUM(CASE WHEN ({arg}) THEN 1 ELSE 0 END)` |
| `__bool_or_{arg}_nonnull_count` | BIGINT | `SUM(CASE WHEN ({arg}) IS NOT NULL THEN 1 ELSE 0 END)` |

End-query mapping (target table value):

```sql
CASE WHEN "__bool_or_{arg}_nonnull_count" > 0
     THEN "__bool_or_{arg}_true_count" > 0
     ELSE NULL
END
```

This correctly handles all three cases:
- `nonnull_count = 0` → all inputs were NULL → output NULL (matches Postgres `BOOL_OR` semantics)
- `nonnull_count > 0, true_count = 0` → all non-NULL inputs were FALSE → output FALSE
- `true_count > 0` → at least one TRUE → output TRUE

Since both counters use `source_aggregate = "SUM"`, they fall through to the generic `COALESCE(t.col, 0) ± COALESCE(d.col, 0)` arm in `build_merge_sql` — no special subtract handling, no recompute. Algebraically maintainable in both directions.

## Code changes

### `src/aggregation.rs`

**`plan_aggregation` SELECT arm** (was ~14 lines): replaced single `BOOL_OR`/`BOOLEAN` column push with two `SUM`/`BIGINT` column pushes and a CASE `intermediate_expr`. `aggregate_type` in `EndQueryMapping` stays `"BOOL_OR"` so `schema_builder` still emits a `BOOLEAN` output column on the target table.

**`plan_aggregation` HAVING arm**: same two-column pattern, no `end_query_mapping` (HAVING-only aggregates don't project to the target).

**`rewrite_expr_aggregates`**: added an early-return branch inside the `col_name` match for `BoolOr`. Computes `(has_true, has_nonnull)` before mutably borrowing `new_cols` to push (avoids borrow-checker conflict). Returns a CASE expression string directly rather than a quoted column name.

### `src/trigger.rs`

- **`build_merge_sql`**: removed `("BOOL_OR", DeltaOp::Add)` OR arm and `("BOOL_OR", DeltaOp::Subtract)` NULL arm. Both counter columns now use the default `SUM` path.
- **`build_min_max_recompute_sql`**: removed `|| ic.source_aggregate == "BOOL_OR"` from the filter — algebraic BOOL_OR never triggers recompute.
- **`has_min_max`**: same removal — no source-table index hint for BOOL_OR plans.
- Updated the function docstring to drop stale `BOOL_OR` references.

### `src/create_ivm.rs`

- **`has_min_max`**: removed `|| ic.source_aggregate == "BOOL_OR"` — no extra GROUP BY source index for BOOL_OR views.

## Tests

All 8 TDD tests pass (written before implementation per CLAUDE.md):

| file | test | status |
|---|---|---|
| `unit_aggregation.rs` | `test_plan_bool_or_emits_two_counter_columns` | ✓ |
| `unit_aggregation.rs` | `test_plan_bool_or_end_query_mapping_uses_case_expression` | ✓ |
| `unit_aggregation.rs` | `test_plan_bool_or_no_raw_bool_or_aggregate` | ✓ |
| `unit_aggregation.rs` | `test_having_only_bool_or_creates_intermediate_column` | ✓ |
| `unit_trigger.rs` | `test_build_merge_sql_bool_or_algebraic_subtract` | ✓ |
| `unit_trigger.rs` | `test_build_delta_sql_bool_or_has_no_recompute` | ✓ |
| `unit_trigger.rs` | `test_min_max_recompute_sql_handles_join_aliases` | ✓ (updated: verifies None for algebraic plan) |
| `unit_schema_builder.rs` | `test_intermediate_ddl_bool_or_emits_bigint_counters` | ✓ |

Full unit suite: 60 passed, 0 failed.

## Migration impact

Existing IMVs using `BOOL_OR` have an intermediate table with a `__bool_or_{arg} BOOLEAN` column. After this change the column layout is `__bool_or_{arg}_true_count BIGINT` + `__bool_or_{arg}_nonnull_count BIGINT`. A migration script (`pg_reflex--1.1.2--1.1.3.sql`) must `DROP` and `recreate_reflex_ivm` for any such view, or at minimum `ALTER TABLE` + `UPDATE` to backfill the counters.

## What this unlocks

- `sop_purchase_reflex` and `unsent_sop_forecast_reflex` flush paths no longer scan 7–30 M rows per flush on DELETE/UPDATE.
- Expected flush time on those IMVs drops from 100+ s to sub-second (counter increment is the same cost as any SUM update).
- Removes the last use of `build_min_max_recompute_sql` for BOOL_OR; that function now covers only genuine MIN/MAX recompute, reducing its conceptual scope.

## Remaining work

- `sql/pg_reflex--1.1.2--1.1.3.sql` migration script for existing BOOL_OR intermediates.
- Phase C (#2 from the plan): algebraic MIN/MAX via bounded top-K heap.
