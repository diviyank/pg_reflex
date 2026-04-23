# Next sprint — pg_reflex 1.1.3: algebraic aggregates + targeted end-query (#5, #1, #2)

## Context

1.1.3 is still unreleased — the quick wins (#3, #9, #11, #12) have landed on the branch but `version = "1.1.3"` is staged, not shipped. Fold these three larger features into the **same 1.1.3 release** rather than cutting a 1.1.4. That means extending `sql/pg_reflex--1.1.2--1.1.3.sql` with the new migration steps instead of creating a fresh `1.1.3--1.1.4.sql`.

The three features come from `journal/2026-04-22_optimization_ideas.md`:

- **#5** — targeted filter for the `end_query_has_group_by=true` full-rebuild branch in `reflex_build_delta_sql` (`trigger.rs:872-876`). This branch fires for COUNT(DISTINCT) IMVs, and today it drops the entire target and reinserts via `end_query` on every flush, regardless of how small the delta was.
- **#1** — algebraic `BOOL_OR` via a `true_count + nonnull_count` pair of BIGINT companion columns. Removes the full-scan recompute (`build_min_max_recompute_sql`) for `BOOL_OR` on DELETE/UPDATE. On `unsent_sop_forecast_reflex` this is the cliff that made the 2026-04-22 bench 849× slower than REFRESH.
- **#2** — bounded top-K heap for `MIN`/`MAX`. Replaces the scalar intermediate column with a sorted array of the K smallest/largest per group, with per-group recompute fallback when the heap empties. Unlocks `stock_chart_weekly_reflex`, `stock_chart_monthly_reflex`, and `forecast_stock_chart_monthly_reflex` (currently matview-only because MIN/MAX retraction is too slow).

Ship as part of **1.1.3** (same version as the quick wins already on the branch).

Post-exploration adjustments to the journal's recipe:

- **#5 scope is narrower than the journal suggested.** The journal framed #5 as "DELETE+INSERT → targeted UPDATE" for all large-target IMVs. In practice, the existing `else if let Some(ref cols) = grp_cols` branch at `trigger.rs:877-906` is already targeted — it DELETEs and INSERTs only the rows whose group keys appear in `__reflex_affected_<view>`. The genuine regression is the `end_query_has_group_by=true` branch at `:872-876`, which falls back to a full `DELETE FROM target` + `INSERT … end_query` because it does not know how to splice the affected-groups filter into an `end_query` that already has a `GROUP BY` clause. That is the only path that rewrites the entire target. The fix is to inject the filter **before** the `GROUP BY` in `end_query`. Converting the non-group-by branch's DELETE+INSERT to a single `MERGE` is a smaller follow-up and kept out of this sprint.
- **#1 design is single-column + two counters, not the single counter the journal proposed.** Journal said "one `true_count`", but `BOOL_OR(NULL)` must return NULL when all inputs were NULL, and FALSE when at least one input was FALSE but none was TRUE. A single counter can't distinguish those three states. Two counters (`true_count`, `nonnull_count`) do, and the delta arithmetic is still pure SUM.
- **#2 needs a per-group recompute fallback.** When a Subtract empties the K-array, the true MIN/MAX is not in the cache (we only kept K contributors — the K+1-th might be the new extreme). Fall back to a `UPDATE … SET top_k = (SELECT array_agg(col ORDER BY col LIMIT K) FROM <orig_base_query> WHERE grp = intermediate.grp)` for just that group. This is still drastically cheaper than the current full-scan recompute because it hits only empty-heap groups (rare once K is moderately sized).

## Files touched

| # | File | Role | Key locations |
|---|---|---|---|
| #5 | `src/trigger.rs` | new helper `inject_affected_filter_before_group_by`, rewrite `:872-876` branch | `:862-912` |
| #5 | `src/tests/unit_trigger.rs` | unit tests for the filter splice | append |
| #5 | `src/tests/pg_test_distinct_on.rs` | integration tests (COUNT(DISTINCT) flush correctness + no-op gate) | append |
| #1 | `src/aggregation.rs` | rewrite the `AggregateKind::BoolOr` arm in `plan_aggregation` + HAVING path | `:555-568`, `:653-660` |
| #1 | `src/aggregation.rs` | rewrite `rewrite_expr_aggregates` `BoolOr` arm (aggregate-derived path) | `:275-301` |
| #1 | `src/trigger.rs` | remove `BOOL_OR` from `build_min_max_recompute_sql` filter; remove `BOOL_OR` from `has_min_max`; remove `BOOL_OR` from `build_merge_sql` special cases | `:63-84`, `:263-268`, `:694-698` |
| #1 | `src/schema_builder.rs` | drop the `BOOL_OR` → `BOOLEAN` branch in `mapping_type` (no longer reachable once end_query uses the CASE expression) | `:137` |
| #1 | `sql/pg_reflex--1.1.2--1.1.3.sql` | migration: `reflex_migrate_bool_or_aggregates()` helper + call | extend |
| #2 | `src/aggregation.rs` | rewrite `AggregateKind::Min`/`Max` arms to emit `__min_<col>_topk` array intermediate | `:525-553`, `:637-651` |
| #2 | `src/aggregation.rs` | rewrite `rewrite_expr_aggregates` `Min`/`Max` arms | `:273-274`, `:290-298` |
| #2 | `src/schema_builder.rs` | in `build_intermediate_table_ddl`, emit array types for `MIN_TOPK`/`MAX_TOPK` columns | `:58-85` |
| #2 | `src/query_decomposer.rs` | `generate_base_query`: emit `reflex_topk_min(col, 16)` / `reflex_topk_max` for top-K cols | `:403-412` |
| #2 | `src/trigger.rs` | new `build_topk_merge_sql` + `build_topk_subtract_sql`; rewrite `has_min_max` branch + recompute to `has_topk_refill` | `:60-84`, `:255-321`, `:694-850` |
| #2 | `src/lib.rs` | pgrx register `reflex_topk_min` / `reflex_topk_max` aggregates + `reflex_topk_merge` / `reflex_topk_subtract` scalar helpers | new |
| #2 | `src/topk.rs` | new module: pgrx helpers (`topk_merge`, `topk_subtract`, aggregate state functions) | new |
| #2 | `sql/pg_reflex--1.1.2--1.1.3.sql` | migration: extend the same rebuild loop to cover MIN/MAX | extend |
| — | `Cargo.toml` | already `version = "1.1.3"` | `:3` (no change) |

Tests: new cases in `src/tests/unit_trigger.rs`, `src/tests/unit_aggregation.rs`, `src/tests/unit_schema_builder.rs`, `src/tests/pg_test_deferred.rs`, `src/tests/pg_test_distinct_on.rs`, `src/tests/pg_test_e2e.rs`, `src/tests/pg_test_correctness.rs`.

---

## #5 — Targeted end-query refresh when `end_query_has_group_by=true`

### Today

`trigger.rs:862-876`:

```rust
let end_query_has_group_by = end_query.to_uppercase().contains("GROUP BY");
…
if end_query_has_group_by {
    let qv = quote_identifier(view_name);
    stmts.push(format!("DELETE FROM {}", qv));
    stmts.push(format!("INSERT INTO {} {}", qv, end_query));
    stmts.push(metadata_sql);
}
```

The branch fires exclusively for `COUNT(DISTINCT val)` IMVs. The intermediate is keyed by the compound `(grp, val)` — finer than the target's `grp`. The `end_query` template, generated by `generate_end_query` in `query_decomposer.rs:638-646`, looks like:

```sql
SELECT "grp", COUNT("val") AS "cd"
  FROM "__reflex_intermediate_<view>"
 WHERE __ivm_count > 0
 GROUP BY "grp"
```

On every flush we `DELETE FROM target; INSERT INTO target <full end_query>` — a full re-aggregation regardless of delta size. For 30 M-row intermediates this is the dominant cost.

### Fix

Splice the affected-groups filter into `end_query` **before** the `GROUP BY` clause. Project `__reflex_affected_<view>` down to the target's output group columns (`plan.group_by_columns`, not `grp_cols` — the latter is the compound key including `distinct_columns`).

New trigger.rs emit for the `end_query_has_group_by` branch:

```sql
DO $reflex_refresh$ BEGIN
  IF EXISTS(SELECT 1 FROM "__reflex_affected_<view>") THEN
    DELETE FROM <target>
     WHERE (<output_gb_cols>) IN (
       SELECT DISTINCT <output_gb_cols> FROM "__reflex_affected_<view>");
    INSERT INTO <target>
      <end_query with "AND (<output_gb_cols>) IN (…)" spliced before GROUP BY>;
  END IF;
END $reflex_refresh$
```

`output_gb_cols` are the `plan.group_by_columns` normalized + quoted. `affected_tbl` stores the compound key, but `SELECT DISTINCT <output_gb_cols>` projects it down to just the output grouping.

### Splice strategy

Since we generate `end_query` ourselves we know its shape (`… WHERE __ivm_count > 0 [AND (having)]? GROUP BY <cols> [HAVING …]?`). A safe splice:

1. Find the last occurrence of `" GROUP BY "` in the uppercased `end_query` (using byte offset into the original).
2. Insert `" AND (<output_gb_cols>) IN (SELECT DISTINCT <output_gb_cols> FROM "<affected_tbl>")"` at that position.
3. If no `GROUP BY` is found (shouldn't happen under this branch — defensive), fall back to the old full-rebuild.

`HAVING` comes after `GROUP BY`, so no conflict. The existing `WHERE __ivm_count > 0` is already present; we append with `AND`.

A new helper `inject_affected_filter_before_group_by(end_query, output_gb_cols, affected_tbl) -> Option<String>` lives in `trigger.rs` near `null_safe_in` (`:329`). Returns `None` if the `GROUP BY` marker isn't found.

Edge case: if `plan.group_by_columns.is_empty()` (a hypothetical global-COUNT-DISTINCT, no GROUP BY in user query), we can't project affected groups to a target filter. Fall back to the old full-rebuild path — that case has only one output row anyway.

### NULL semantics

Use `IS NOT DISTINCT FROM` for NULL-safe matching when any output group column is nullable. Safest default: always use the NULL-safe form (matches `null_safe_in` in `trigger.rs:329-339`).

### Metadata row

Keep `UPDATE __reflex_ivm_reference SET last_update_date = NOW()` **outside** the DO gate (same as 1.1.3 does for the non-group-by branch). `last_update_date` tracks "last attempted flush", not "last effectful flush".

---

## #1 — Algebraic `BOOL_OR` via `true_count + nonnull_count`

### Today

`aggregation.rs:555-568`: BOOL_OR emits a single intermediate column:

```rust
let col_name = format!("__bool_or_{}", arg_sanitized);
intermediate_columns.push(IntermediateColumn {
    name: col_name.clone(),
    pg_type: "BOOLEAN".to_string(),
    source_aggregate: "BOOL_OR".to_string(),
    source_arg: arg.to_string(),
});
```

On DELETE/UPDATE, `build_merge_sql` sets the column to NULL (`trigger.rs:79-83`), then `build_min_max_recompute_sql` (`trigger.rs:255-321`) emits a full-scan `UPDATE … FROM (orig_base_query) …` to re-derive it. On 30 M-row sources this is a multi-second scan per flush.

### Fix

Replace the single BOOLEAN with two BIGINT counters, reusing the existing SUM algebraic machinery:

- `__bool_or_<arg>_true_count` — `SUM(CASE WHEN (arg) = TRUE THEN 1 ELSE 0 END)`.
- `__bool_or_<arg>_nonnull_count` — `SUM(CASE WHEN (arg) IS NOT NULL THEN 1 ELSE 0 END)`.

Both columns have `source_aggregate = "SUM"` and a CASE-expression `source_arg`. `generate_base_query` already handles arbitrary expressions in `source_arg` (it textually substitutes — see `query_decomposer.rs:407-411`). `build_merge_sql` treats them algebraically with `+`/`-` (the existing SUM/COUNT code path). `build_intermediate_table_ddl` resolves `SUM` → the declared pg_type, which we set to `BIGINT`.

### End-query

Replace the `BOOL_OR` mapping with a CASE expression:

```
EndQueryMapping {
  intermediate_expr: "CASE \
    WHEN __bool_or_X_true_count > 0 THEN TRUE \
    WHEN __bool_or_X_nonnull_count > 0 THEN FALSE \
    ELSE NULL END",
  output_alias: <same>,
  aggregate_type: "BOOL_OR",  // retained for target DDL mapping
  cast_type: <same>,
}
```

### `build_min_max_recompute_sql`

Drop the `BOOL_OR` branch (`trigger.rs:263-268`):

```rust
.filter(|ic| ic.source_aggregate == "MIN" || ic.source_aggregate == "MAX")
```

### `build_merge_sql` — drop `BOOL_OR` special-casing

Remove lines `:73-77` (BOOL_OR Add as `OR`) and from the Subtract case in `:79-83`. The fallthrough `_` arm at `:84-97` handles algebraic `+`/`-` correctly once the columns are `source_aggregate = "SUM"`, `pg_type = "BIGINT"`.

### `reflex_build_delta_sql`

Drop `"BOOL_OR"` from the `has_min_max` detection (`trigger.rs:694-698`). BOOL_OR no longer takes the recompute path — it rides the net-delta UPDATE path (the GROUP BY branch at `:821-837`).

### Target DDL

`build_target_table_ddl` in `schema_builder.rs:127-140` resolves `aggregate_type = "BOOL_OR"` to `BOOLEAN`. Keep that mapping — the end-query still emits a boolean value.

### Migration

Existing IMVs have a `__bool_or_<arg>` BOOLEAN column in their intermediate tables and a corresponding aggregations JSON. Options:

1. **In-place ALTER**: scan `__reflex_ivm_reference` for rows with BOOL_OR aggregates, ADD the two counter columns, backfill from source, DROP the old column, rewrite aggregations JSON, regenerate end_query, re-emit trigger DDLs.
2. **DROP + recreate**: for each BOOL_OR IMV, DROP the intermediate table, rewrite registry rows (base_query, end_query, aggregations), CREATE fresh intermediate, re-populate from source.

Option 2 is simpler and fits the existing reconcile-style migration the extension already uses on upgrade. Provide a helper in the migration:

```sql
CREATE FUNCTION reflex_migrate_bool_or_aggregates() RETURNS INT AS …
```

that iterates every row of `__reflex_ivm_reference` whose `aggregations::text LIKE '%"BOOL_OR"%'`, calls a Rust-side `reflex_rebuild_imv(view_name)` that re-runs the plan/build pipeline from the stored `sql_query`. Migration ends with:

```sql
SELECT reflex_migrate_bool_or_aggregates();
```

`reflex_rebuild_imv` is a thin wrapper that re-enters the 1.1.3 version of `create_reflex_ivm`'s plan → build → re-materialize steps for a single existing row. It reuses `create_ivm.rs::regenerate_intermediate` if present, or adds one.

**Correctness concern**: rebuilding reads the source table in full to repopulate the intermediate. For a 30 M-row source this is minutes. Document this in the migration preamble. The alternative — in-place ALTER — is error-prone and has the same scan cost for the backfill anyway.

---

## #2 — Bounded top-K heap for `MIN`/`MAX`

### Today

`aggregation.rs:525-553`: MIN/MAX emit a scalar `__min_<arg>` / `__max_<arg>` column, type resolved to the source column type in `build_intermediate_table_ddl:61-65`. On DELETE/UPDATE-old, `build_merge_sql` sets it to NULL, and `build_min_max_recompute_sql` full-scans the source to re-derive. On `stock_chart_weekly_reflex` (20 M rows, ~30 M deletes over the bench) this is the dominant cost.

### Fix

Replace the scalar with a **bounded, sorted array** of the K smallest (MIN) or K largest (MAX) values per group. K = 16 (compile-time constant for v1, configurable in a follow-up).

- Intermediate column: `__min_<arg>_topk <elem_type>[]` (array of the source column's type).
- end_query_mapping: `(__min_<arg>_topk)[1] AS <alias>` — Postgres arrays are 1-indexed. If the heap is empty, `[1]` returns NULL (matching `MIN` over an empty group).
- base_query: a new user-defined aggregate `reflex_topk_min(col <type>, k INT)` that returns `<type>[]` — the sorted array of the K smallest contributors, and a mirror `reflex_topk_max`.

### Delta arithmetic

**Add (INSERT or UPDATE-new)**:
```sql
__min_X_topk = reflex_topk_merge(t.__min_X_topk, d.__min_X_topk, 16, 'asc')
```

`reflex_topk_merge(existing <type>[], incoming <type>[], k INT, dir TEXT) RETURNS <type>[]` — union-sort-truncate. Implemented in `topk.rs` via pgrx using `AnyElement` + internal sort; or in plain SQL as a generic helper that works on `anyarray`.

**Subtract (DELETE or UPDATE-old)**:
```sql
__min_X_topk = reflex_topk_subtract(t.__min_X_topk, d.__min_X_topk, 'asc')
```

`reflex_topk_subtract(existing <type>[], to_remove <type>[], dir TEXT) RETURNS <type>[]` — walk both sorted arrays in lockstep, remove one occurrence per incoming element.

### Empty-heap fallback

After Subtract, some groups may end up with `array_length(__min_X_topk, 1) IS NULL` (empty). Those groups' true MIN/MAX is not cached — we only kept K contributors, so if all K are retracted, the K+1-th (which might be the new extreme) was never seen.

Replace `build_min_max_recompute_sql` with `build_topk_refill_sql`:

```sql
UPDATE <intermediate> AS t SET
  __min_X_topk = (
    SELECT array_agg(v ORDER BY v ASC)
      FROM (SELECT <arg> AS v FROM (<orig_base_query>) AS __src
            WHERE __src.<grp> IS NOT DISTINCT FROM t.<grp>
              [… join conds …]
            ORDER BY v ASC LIMIT 16) s
  )
WHERE array_length(t.__min_X_topk, 1) IS NULL
  AND __ivm_count > 0
  [AND the affected-groups filter from __reflex_affected_<view>]
```

Two critical differences vs. 1.1.3's `build_min_max_recompute_sql`:

1. **Per-empty-group**, not full-scan: the outer `WHERE array_length IS NULL` prunes to groups where the heap emptied. On realistic deltas this is 0 rows; when it is >0, only those specific groups get rescanned.
2. **Scoped to affected groups**: add the `__reflex_affected_<view>` join so we don't consider groups unaffected by this flush.

The inner correlated `SELECT …` still reads the original source. For retention-heavy workloads where the heap regularly empties, the user can raise K via a registry setting (follow-up).

### Self-join / outer-join caveat

The per-group refill uses the same `orig_base_query` the current recompute uses, so the existing `missing FROM-clause entry for table "alias"` fix from `journal/2026-04-21_min_max_recompute_bug.md` still applies. Reuse the existing subquery-rewriting helper that wraps the `orig_base_query` inside `(… )` and aliases it `__src`.

### Custom aggregate

Register in `topk.rs`:

```rust
#[pg_aggregate]
pub struct ReflexTopKMin;

impl pgrx::aggregate::Aggregate for ReflexTopKMin {
    type State = Vec<AnyElement>;
    type Args = (AnyElement, i32);  // value, k
    type Finalize = Vec<AnyElement>;
    …
}
```

(Pseudocode — the real impl uses `Internal` + a heap; see pgrx docs for typed aggregates.)

An alternate path is a plain SQL + plpgsql wrapper that wraps `array_agg(col ORDER BY col LIMIT K)`, but that doesn't stream — the state grows to every contributing row before the LIMIT applies. The pgrx-aggregate version is `O(N log K)`.

### Migration

Same DROP-and-rebuild pattern as #1. Migration function:

```sql
CREATE FUNCTION reflex_migrate_min_max_aggregates() RETURNS INT AS …
```

Iterates `__reflex_ivm_reference` rows whose `aggregations::text LIKE '%"MIN"%' OR aggregations::text LIKE '%"MAX"%'`, drops-and-rebuilds each one. Same scan cost concern as #1.

---

## Tests (write first; don't modify after — per CLAUDE.md)

### #5 — `src/tests/unit_trigger.rs`

1. `test_build_delta_sql_splice_injects_filter_before_group_by` — build a plan with `end_query = "SELECT grp, COUNT(val) FROM int WHERE __ivm_count > 0 GROUP BY grp"`; call `reflex_build_delta_sql` with `end_query_has_group_by=true`. Assert output contains `DO $reflex_refresh$`, and the emitted INSERT query contains `AND ("grp") IN (SELECT DISTINCT "grp" FROM "__reflex_affected_…")` **before** `GROUP BY "grp"`.
2. `test_build_delta_sql_splice_falls_back_when_no_group_by_cols` — plan with no `group_by_columns` but `end_query` still containing GROUP BY (synthetic case). Assert output falls back to the full-rebuild DELETE + INSERT without a DO gate.
3. `test_splice_helper_handles_having_clause` — unit test for `inject_affected_filter_before_group_by` directly. Input: `"SELECT grp, COUNT(val) FROM int WHERE __ivm_count > 0 GROUP BY grp HAVING COUNT(val) > 0"`. Expect: filter spliced before GROUP BY, HAVING untouched after.
4. `test_splice_helper_returns_none_when_no_group_by` — helper returns `None` if the input has no `GROUP BY` marker.
5. `test_build_delta_sql_splice_uses_distinct_projection_for_compound_key` — plan with `group_by_columns=["grp"]` and `distinct_columns=["val"]` (i.e., COUNT(DISTINCT val) GROUP BY grp). Assert the emitted filter projects affected_tbl down to `grp` only (not `grp, val`).

### #5 — `src/tests/pg_test_distinct_on.rs` or new `pg_test_count_distinct.rs`

6. `test_count_distinct_flush_only_touches_affected_groups` — create IMV `SELECT grp, COUNT(DISTINCT val) AS cd FROM t GROUP BY grp` in DEFERRED mode. Seed rows for grps {a, b, c}. INSERT rows into only `grp='a'`. Flush. Assert:
   - `cd` for `a` is correct.
   - `cd` for `b` and `c` is unchanged (byte-equal to pre-flush value — serialize and compare). This proves the target was not fully rewritten.
7. `test_count_distinct_flush_matches_oracle_over_mixed_ops` — sequence of INSERT → flush, DELETE → flush, UPDATE → flush. Assert `assert_imv_correct` after each.
8. `test_count_distinct_empty_delta_skips_target_refresh` — seed; UPDATE rows but set same value (net-zero delta). Flush. Assert no change in target. (Uses the DO-gate path.)

### #1 — `src/tests/unit_aggregation.rs`

9. `test_plan_bool_or_emits_two_counter_columns` — plan `SELECT grp, BOOL_OR(flag) FROM t GROUP BY grp`. Assert `intermediate_columns` contains exactly two columns matching `__bool_or_flag_true_count` and `__bool_or_flag_nonnull_count`, both `source_aggregate = "SUM"`, `pg_type = "BIGINT"`.
10. `test_plan_bool_or_end_query_mapping_uses_case_expression` — same plan. Assert `end_query_mappings` contains a single mapping with `intermediate_expr` matching `CASE WHEN "__bool_or_flag_true_count" > 0 THEN TRUE WHEN "__bool_or_flag_nonnull_count" > 0 THEN FALSE ELSE NULL END` and `aggregate_type = "BOOL_OR"`.
11. `test_plan_bool_or_in_having_emits_counter_columns` — HAVING-only BOOL_OR (mirror of existing `test_having_only_bool_or_creates_intermediate_column` at `unit_aggregation.rs:214`). Assert the two counter columns exist; no `__bool_or_flag` BOOLEAN column.
12. `test_bool_or_complex_expression_argument` — `SELECT grp, BOOL_OR(x > 10 AND y IS NOT NULL) FROM t GROUP BY grp`. Assert the CASE expressions in `source_arg` are constructed from the full expression, not reduced.

### #1 — `src/tests/unit_trigger.rs`

13. `test_build_delta_sql_bool_or_has_no_recompute` — plan with BOOL_OR, operation DELETE. Assert the emitted SQL does **not** contain `UPDATE "<intermediate>" SET` for any BOOL_OR column (the recompute step is gone).
14. `test_build_merge_sql_bool_or_subtract_uses_algebraic_difference` — plan with BOOL_OR, op=Subtract. Assert SET clause contains `"__bool_or_flag_true_count" = COALESCE(t."__bool_or_flag_true_count", 0) - COALESCE(d."__bool_or_flag_true_count", 0)` and the mirror for `nonnull_count`. No `NULL` assignment.

### #1 — `src/tests/unit_schema_builder.rs`

15. `test_intermediate_ddl_bool_or_emits_bigint_counters` — plan with BOOL_OR; call `build_intermediate_table_ddl`. Assert DDL has two `BIGINT DEFAULT 0` columns (`__bool_or_flag_true_count`, `__bool_or_flag_nonnull_count`), and **no** `__bool_or_flag BOOLEAN` column.

### #1 — `src/tests/pg_test_e2e.rs` or `pg_test_correctness.rs`

16. `test_bool_or_counter_insert_delete_update_correctness` — full behavioral mirror of the existing `test_bool_or_insert_delete_update` (e2e.rs:1220) against the new counter-backed plan. Cover all three truthiness states (TRUE, FALSE, NULL) to exercise the CASE expression. Each op followed by `assert_imv_correct`.
17. `test_bool_or_all_null_column_returns_null` — source has all NULL flags. After INSERT+DELETE of non-flag rows. Assert IMV emits NULL for that group (not FALSE).
18. `test_bool_or_retraction_to_false` — seed a group with one TRUE + one FALSE. DELETE the TRUE. Assert IMV now emits FALSE for that group (the NULL-vs-FALSE distinction the single-counter design would have broken).
19. `test_bool_or_no_recompute_on_delete_deferred` — DEFERRED BOOL_OR IMV. DELETE rows, flush. Scrape `pg_stat_statements` (if available in the test harness) or check the flush's emitted SQL via a probe query: assert no `__src` UPDATE hit. Fallback if pg_stat_statements is unavailable: verify correctness only.

### #2 — `src/tests/unit_aggregation.rs`

20. `test_plan_min_emits_topk_array_column` — plan `SELECT grp, MIN(val) FROM t GROUP BY grp`. Assert `intermediate_columns` has one column named `__min_val_topk`, `source_aggregate = "MIN_TOPK"`, `pg_type` matches the source column's array type (or a sentinel like `"__SOURCE_ARRAY__"` to be resolved in schema_builder).
21. `test_plan_max_emits_topk_array_column` — mirror for MAX.
22. `test_plan_min_end_query_subscripts_first_element` — assert `end_query_mappings[0].intermediate_expr` matches `("__min_val_topk")[1]`.

### #2 — `src/tests/unit_schema_builder.rs`

23. `test_intermediate_ddl_min_topk_resolves_to_array_type` — plan with MIN over an INT column. `column_types` map includes `val => INTEGER`. DDL must have `__min_val_topk INTEGER[]`.
24. `test_intermediate_ddl_min_topk_date_column` — MIN over a DATE column. DDL must have `__min_val_topk DATE[]`.

### #2 — `src/tests/unit_trigger.rs`

25. `test_build_merge_sql_min_topk_add_uses_topk_merge` — plan with `source_aggregate = "MIN_TOPK"`, op=Add. Assert SET clause contains `reflex_topk_merge(t."__min_val_topk", d."__min_val_topk", 16, 'asc')`.
26. `test_build_merge_sql_min_topk_subtract_uses_topk_subtract` — same but op=Subtract; assert `reflex_topk_subtract(…)`.
27. `test_build_topk_refill_sql_targets_empty_heaps_only` — new helper unit test. Assert generated SQL has `WHERE array_length(t."__min_val_topk", 1) IS NULL` and the affected-groups filter.
28. `test_build_topk_refill_handles_join_alias_orig_base_query` — mirror of existing `test_min_max_recompute_sql_handles_join_aliases` (unit_trigger.rs:178): JOIN-alias arg; the emitted refill SQL must wrap the `orig_base_query` as `(…) AS __src` and reference `__src.grp`.
29. `test_bool_or_no_longer_in_min_max_recompute` — plan with BOOL_OR only. Assert `build_min_max_recompute_sql` returns None (post-#1 change — doubles as #1 coverage).
30. `test_min_max_topk_recompute_not_emitted_when_no_empty_heap` — the emission logic should still always emit the refill SQL (we can't know empty-heap state at plan time), but the runtime WHERE makes it a no-op. This test asserts the SQL is emitted unconditionally in the MIN/MAX path.

### #2 — `src/tests/pg_test_correctness.rs`

31. `test_min_topk_insert_delete_update_correctness` — classic MIN IMV, all three ops, assert oracle each time.
32. `test_min_topk_retraction_past_k_triggers_refill` — K=16; seed a group with 20 ascending values. DELETE the 16 smallest in one batch. Flush. The heap empties; refill SQL runs. Assert the IMV's MIN equals the 17th value (now the true min).
33. `test_max_topk_similar_retraction` — mirror for MAX.
34. `test_min_topk_text_type_correctness` — MIN over a TEXT column (should work with array-of-text).
35. `test_min_topk_null_values_ignored` — seed group with values `{5, NULL, 3, NULL, 7}`. Assert IMV MIN = 3 (matches Postgres MIN behavior: NULLs ignored).
36. `test_min_topk_empty_group_after_delete_returns_null` — seed group, delete all rows. Flush. Assert IMV row is removed (the `__ivm_count = 0` gate drops it via the existing `include_dead_cleanup` flow).

### #2 — `src/tests/pg_test_deferred.rs`

37. `test_deferred_min_topk_delete_does_not_full_scan` — DEFERRED MIN IMV; DELETE a few rows that don't empty the heap. Flush. Stretch assertion: `pg_stat_user_tables.seq_scan` for the source table didn't increment (we want the refill to skip). If that's flaky, fall back to correctness-only.

### Migration smoke (bash)

38. Install **1.1.2** in a scratch DB; create a mix of IMVs:
    - one SUM GROUP BY (no migration effect)
    - one BOOL_OR GROUP BY (migrated)
    - one MIN GROUP BY (migrated)
    - one COUNT(DISTINCT) GROUP BY (no migration effect)
    - Run flushes that update each IMV's target.
    - `ALTER EXTENSION pg_reflex UPDATE TO '1.1.3'`.
    - Verify each IMV's intermediate now has the new column shape (`\d __reflex_intermediate_<view>`): BOOL_OR IMV has two BIGINT counter cols (no BOOLEAN), MIN IMV has an array column.
    - Re-run the same update sequence. Assert oracle matches on all four IMVs.
    - Stretch: confirm `RAISE NOTICE` from the migration reports the correct `_rebuilt` count.

---

## Migration — extend `sql/pg_reflex--1.1.2--1.1.3.sql`

The existing 1.1.2→1.1.3 migration already handles `PARALLEL SAFE` and the `where_predicate` catalog patch. Append the new rebuild loop at the bottom of that same file (after the existing quick-wins migration statements):

```sql
-- === #5: End-query targeted filter ===
--    Code-gen only, no catalog patching. Effective after the shared library is
--    reloaded.
--
-- === #1: BOOL_OR algebraic counters ===
--    Two BIGINT counter columns replace the single BOOLEAN intermediate.
--    Requires rebuilding each affected IMV's intermediate from source.
--
-- === #2: MIN/MAX top-K heap ===
--    Array-of-element top-K companion column replaces the scalar intermediate.
--    Same rebuild requirement as #1.
--
-- IMPORTANT: for #1 and #2, each affected IMV is DROPPED and rebuilt from
-- its stored sql_query — the rebuild re-scans the source table in full.
-- On large sources this can take minutes. Plan the upgrade accordingly.

-- #1 + #2: rebuild BOOL_OR / MIN / MAX IMVs via a dispatcher that reads
-- the registry and calls the Rust-side reflex_rebuild_imv(name).
DO $migration$
DECLARE
    _rec RECORD;
    _rebuilt INT := 0;
BEGIN
    FOR _rec IN
        SELECT name, aggregations::text AS aggs
        FROM public.__reflex_ivm_reference
        WHERE enabled = TRUE
          AND (aggregations::text LIKE '%"BOOL_OR"%'
            OR aggregations::text LIKE '%"MIN"%'
            OR aggregations::text LIKE '%"MAX"%')
    LOOP
        PERFORM reflex_rebuild_imv(_rec.name);
        _rebuilt := _rebuilt + 1;
    END LOOP;
    RAISE NOTICE 'pg_reflex 1.1.2 -> 1.1.3: rebuilt % IMV(s) with BOOL_OR / MIN / MAX aggregates', _rebuilt;
END;
$migration$;
```

Because this migration file is shared with the quick-wins release, anyone testing against the current branch head automatically gets the rebuild step. There is no separate `ALTER EXTENSION … UPDATE TO '1.1.4'` path to maintain — the whole feature set ships as `1.1.2 → 1.1.3`.

`reflex_rebuild_imv(view_name TEXT)` is a new Rust-side SPI entrypoint added in `src/lib.rs` (or `src/create_ivm.rs`). The sqlparser-generated DDL must be registered so it appears alongside `create_reflex_ivm`. It:

1. Loads the registry row for `view_name`.
2. Re-runs `plan_aggregation` + `generate_base_query` + `generate_end_query` against the stored `sql_query` using the current (1.1.3) logic.
3. DROPs the intermediate table and target table (preserving indexes as IF EXISTS for re-creation).
4. Re-creates intermediate + target via `build_intermediate_table_ddl` + `build_target_table_ddl`.
5. Re-populates the intermediate from source (equivalent of the initial `INSERT INTO intermediate <base_query>` step in `create_reflex_ivm`).
6. Re-populates the target from the intermediate (`INSERT INTO target <end_query>`).
7. UPDATEs the registry row with the new base_query, end_query, and aggregations JSON.
8. Regenerates trigger DDLs for the source tables (to pick up any schema changes in the trigger body).

This is basically a refactor of the existing create_ivm flow: factor the "plan → DDL → re-populate" steps into a reusable function so both `create_reflex_ivm` and `reflex_rebuild_imv` share code.

### Cargo.toml

Already at `version = "1.1.3"` — no bump. `.control` uses `@CARGO_VERSION@`, so nothing else to touch.

### Guard against partial migration

Wrap the `reflex_rebuild_imv` call in a savepoint per IMV. If one fails (e.g., source table has since been dropped), log a WARNING and continue with the remaining IMVs. The migration reports both `_rebuilt` and `_skipped` counts.

---

## Implementation order

The three features are independent but share the migration scaffolding. Recommended sequence:

### Phase A — #5 (3–5 days)

1. Write tests #1–#8.
2. Add `inject_affected_filter_before_group_by` helper in `trigger.rs`.
3. Rewrite `:872-876` branch to the DO-gated splice path (fallback to full rebuild if splice returns None).
4. `cargo fmt && cargo clippy && cargo pgrx test`.
5. Bench: rerun db_clone driver. Target: `sop_last_forecast_reflex` and `unsent_sop_forecast_reflex` (COUNT-DISTINCT-heavy) move from "849× slower" to near-parity with REFRESH.

### Phase B — #1 (3–5 days)

6. Write tests #9–#19.
7. Rewrite `aggregation.rs::plan_aggregation` BOOL_OR arm + HAVING arm + `rewrite_expr_aggregates` arm. Keep `aggregate_type = "BOOL_OR"` in `EndQueryMapping` for target DDL.
8. Remove BOOL_OR from `build_merge_sql` (`trigger.rs:73-84`) and from `build_min_max_recompute_sql` filter (`:263-268`) and from the `has_min_max` detection (`:694-698`).
9. Add `reflex_rebuild_imv` SPI function + migration dispatcher (needed by both #1 and #2).
10. `cargo fmt && cargo clippy && cargo pgrx test`.
11. Smoke: upgrade 1.1.2 → 1.1.3 in a scratch DB with pre-existing BOOL_OR IMVs. Verify intermediate has two BIGINT cols, not a BOOLEAN.
12. Bench: `sop_purchase_reflex` and `unsent_sop_forecast_reflex` (both use `BOOL_OR(caav.product_id IS NOT NULL)`) should flush in <1 s.

### Phase C — #2 (1–2 weeks)

13. Write tests #20–#37.
14. Add `src/topk.rs` module: pgrx aggregate `reflex_topk_min(col, k)`, `reflex_topk_max(col, k)`, scalar `reflex_topk_merge`, `reflex_topk_subtract`. Unit test the helpers in `src/tests/unit_topk.rs`.
15. Rewrite `aggregation.rs` MIN/MAX arms to emit `MIN_TOPK` / `MAX_TOPK` intermediate columns + `(topk)[1]` end_query mapping.
16. Update `schema_builder.rs::build_intermediate_table_ddl` to resolve `MIN_TOPK`/`MAX_TOPK` to array types.
17. Update `query_decomposer.rs::generate_base_query` to emit `reflex_topk_min(col, 16)` / `reflex_topk_max(col, 16)` for these columns.
18. Replace `build_min_max_recompute_sql` with `build_topk_refill_sql` (empty-heap WHERE clause + affected-groups scope).
19. Update `build_merge_sql` SET clauses for MIN_TOPK/MAX_TOPK to call the merge/subtract helpers.
20. Migration: extend `reflex_rebuild_imv` dispatcher to also hit MIN/MAX IMVs (one pass over the registry does both #1 and #2).
21. `cargo fmt && cargo clippy && cargo pgrx test`.
22. Smoke: upgrade 1.1.2 → 1.1.3 in a scratch DB with pre-existing MIN IMVs.
23. Bench: rerun db_clone driver on the three "currently matview-only" views (`stock_chart_weekly_reflex`, `stock_chart_monthly_reflex`, `forecast_stock_chart_monthly_reflex`). Convert them to IMV; measure flush vs REFRESH.

### Phase D — bench + journal

24. Consolidated bench rerun. Compare against the 1.1.2 baseline from `journal/2026-04-22_db_clone_benchmark_rerun.md` **and** against the quick-wins-only 1.1.3 snapshot captured after Phase A's final commit (keep a separate branch/commit so the #5-vs-#1-vs-#2 contribution is attributable).
25. Document results in `journal/<date>_1_1_3_bench.md`. For each of #5, #1, #2 record wins/losses and decide if any revert is warranted per CLAUDE.md's "worth the hassle" bar.
26. Update `journal/2026-04-22_unsupported_views.md` to move `stock_chart_*` views into the "supported" bucket if #2 unlocks them.

---

## Evaluation — per CLAUDE.md

- **#5**: target is a 30× reduction on COUNT(DISTINCT) flush time for `sop_last_forecast_reflex` and `unsent_sop_forecast_reflex`. If the measured gain is <3×, review whether the splice approach missed an obvious planner hazard (e.g., the WHERE → GROUP BY → HAVING sequence defeats the index scan). Keep if correctness is clean and any real gain is visible.
- **#1**: target is 100× on `sop_purchase_reflex` flush. The `+0.5% complexity` cost is two new counter columns per BOOL_OR; acceptable. If migration is painful (rebuild takes >10 minutes per IMV on production data), consider shipping an opt-in flag (`CREATE REFLEX … WITH (bool_or_strategy = 'counter')`) and leaving existing IMVs on the old path — but this is a fallback, not the default.
- **#2**: target is unlocking the 3 stock_chart views. If the top-K refill fallback fires >10% of flushes in realistic workloads, K is too small — retune up to 32 or 64 and re-measure. If the aggregate registration fights pgrx (array types on polymorphic aggregates are famously finicky), split MIN_TOPK and MAX_TOPK into per-element-type aggregates (`_int`, `_numeric`, `_date`, `_text`) and select by source type at plan time — more code but cleaner types.

If any single feature shows <1.5× on its target workload, consider reverting pre-release. Sprint scope is "ship three wins" not "ship three features."

---

## Out of scope

- **Journal #5 broader scope** (MERGE-based DELETE+INSERT → single UPDATE for non-GROUP-BY end_query branch): deferred. The current targeted DELETE+INSERT already scopes to affected rows; the MERGE version would be a smaller incremental gain than #5 proper.
- **K as a per-IMV setting**: K=16 is compile-time in v1. If the empty-heap refill rate is workload-dependent, add a per-IMV `WITH (topk_size = N)` option in a follow-up.
- **Streaming top-K recompute** (incremental rescan once heap hits a low-water mark like K/4, so retractions don't have to wait for empty): follow-up optimization if the refill-per-empty-group proves insufficient.
- **BOOL_AND** as an algebraic aggregate: the symmetric change to #1 is trivial once the BOOL_OR plumbing lands. Add in a follow-up if a user asks.
- **Array containment migration for #2**: if a user's MIN column is over an already-composite type (e.g., DOMAIN-wrapped INTEGER), the array-type inference might misresolve. Document as "MIN/MAX supported only on plain built-in scalar types" for v1.
- **Cross-feature interaction with topological cascade skip (#4 from the journal)**: if both #2 and #4 land, the refill-scope filter must also honor the upstream no-op signal. Design pass deferred to the #4 sprint.
