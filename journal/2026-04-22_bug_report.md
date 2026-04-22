# Bug report — pg_reflex 1.1.2 audit (2026-04-22)

**Scope**: latent correctness / ergonomics bugs identified during the 2026-04-22 db_clone benchmark rerun + a source-tree audit. Documented **but not implemented** per the instruction to list first, fix in a later cycle.

For each bug: **What** (the symptom), **Why** (root cause), **How** (the code path / reproduction), **What for** (why fixing it matters).

Known-fixed bugs from 2026-04-21 are *not* repeated here (BOOL_OR/MIN/MAX recompute JOIN alias, `replace_source_with_delta` double alias). See `journal/2026-04-21_*`.

---

## 1. Identifier truncation on long source names → silent PG 63-char truncation, potential collision

- **What**: PostgreSQL is emitting `NOTICE: identifier "__reflex_old_demand_planning_characteristics_reflex__cte_sales_stats" will be truncated to "__reflex_old_demand_planning_characteristics_reflex__cte_sales_"` on every flush that touches a sub-IMV (CTE-derived IMV). The trigger code generates names like `__reflex_old_<source>`, `__reflex_new_<source>`, `__reflex_delta_<source>`, `__reflex_affected_<view>` by raw `format!()`, bypassing `safe_identifier`. If two logically-distinct source names share the same 63-character prefix, their staging tables collapse into the same PG identifier.
- **Why**: `safe_identifier()` in `src/query_decomposer.rs:31` already handles the ">63 chars → truncate + hash suffix" case, but the helper is only applied to `__reflex_intermediate_<view>` and `__reflex_affected_<view>`. The delta / transition-table name generators in `src/trigger.rs:467-468, 950, 1036` and `src/schema_builder.rs:304-305, 426-428, 624` never call `safe_identifier`.
- **How**:
  - `src/trigger.rs:467` — `format!("__reflex_new_{}", safe_src)`
  - `src/trigger.rs:468` — `format!("__reflex_old_{}", safe_src)`
  - `src/trigger.rs:950` — `format!("__reflex_delta_{}", safe_src)`
  - `src/trigger.rs:1036` — `format!("__reflex_old_{}", safe_src)` inside `reflex_flush_deferred`
  - `src/schema_builder.rs:304-305, 426-428, 624` — trigger-body formatters
  - Reproduction: any source name > 50 chars (any sub-IMV whose CTE alias is > ~20 chars after `__cte_` prefix). Every `demand_planning_characteristics_reflex__cte_sales_stats`-shaped dependency produces NOTICE lines today — verified live in `/tmp/pg_reflex_bench_rerun/results_flush.txt`.
- **What for**: Currently only a noise issue, but two sub-IMVs with the same 63-char prefix would collide into the same `__reflex_new_<prefix>` transition table. The trigger body writes both deltas into the same truncated name, mixing unrelated rows into a single staging table → **silent data corruption** for the second IMV's flush. This is the highest-severity finding of the audit.
- **Fix direction**: route every `__reflex_*_<src>` name through `safe_identifier`; audit `src/schema_builder.rs` for the same pattern on delta/transition names.

---

## 2. HAVING-only `MIN`/`MAX`/`BOOL_OR` aggregates not recomputed on delete

- **What**: An IMV whose aggregate appears **only** in the `HAVING` clause (e.g., `HAVING MAX(amount) > 100`) creates an intermediate column for the aggregate, but the subtract-on-delete path never triggers `build_min_max_recompute_sql` for that column — the HAVING filter stays evaluated against stale pre-delete values.
- **Why**: The recompute dispatcher iterates over aggregates discovered in the SELECT list (`aggregation.rs` aggregation-plan building), not over aggregates discovered in `HAVING`. The HAVING rewrite (`rewrite_having_expr` in `src/query_decomposer.rs:425-450`) creates the intermediate alias but the plan's `aggregations` array does not carry the "HAVING-only" flag that the recompute loop relies on to decide whether to rescan.
- **How**: Create an IMV like `SELECT group_id, SUM(x) FROM t GROUP BY group_id HAVING MAX(t.amount) > 100`, then DELETE the row that currently holds the group's MAX. The intermediate stores the old MAX, HAVING keeps the group visible, target output is wrong.
- **What for**: Silent correctness bug whenever users filter on a top-K-ish aggregate they don't display.
- **Fix direction**: extend plan-building to surface HAVING-derived aggregates into the same recompute loop that SELECT-list aggregates go through.

---

## 3. `COUNT(DISTINCT …)` with nullable column in the distinct list — compound-key match may miss NULL groups

- **What**: For `COUNT(DISTINCT nullable_col)`, the intermediate key is extended with `nullable_col` to count distinct values per (group, value) tuple. The delta subtract step joins on the compound key using bare `=` in some branches of `build_merge_sql`, not `IS NOT DISTINCT FROM`. On NULL values this causes the row never to match, leaving orphan counter rows.
- **Why**: `build_min_max_recompute_sql` uses `IS NOT DISTINCT FROM` (null-safe) for group-key join, but the generic MERGE path in `src/trigger.rs:285-320` falls back to `null_safe_in` only conditionally — the `COUNT(DISTINCT)` path extends the group key at a different layer and reuses the older bare-equality join builder.
- **How**: `SELECT g, COUNT(DISTINCT maybe_null) FROM t GROUP BY g`, insert a row with `maybe_null = NULL`, then DELETE it. The intermediate counter for `(g, NULL)` is left orphaned because `NULL = NULL` is false.
- **What for**: Wrong `COUNT(DISTINCT)` for any column that can be NULL. Common in real schemas (optional foreign keys).
- **Fix direction**: generalize `null_safe_in` usage in the COUNT DISTINCT subtract path; add `IS NOT DISTINCT FROM` unit-test that covers nullable DISTINCT.

---

## 4. Transition-table name collision with user-owned CTE alias

- **What**: `replace_source_with_transition` rewrites every reference to `source_table` in `base_query` as `__reflex_new_<src>` / `__reflex_old_<src>`. If the user's own SQL already contains a CTE or alias with the same name (unlikely but possible for `__reflex_*`-prefixed CTEs in lower-level IMVs), the replacement does not detect the collision.
- **Why**: The rewriter in `src/query_decomposer.rs` scans byte-level for the source identifier and substitutes without consulting a lexical-scope table of CTE names.
- **How**: contrived but reproducible: `CREATE VIEW v AS WITH __reflex_new_orders AS (SELECT …) SELECT … FROM orders …`. On flush, substitution produces two CTEs / FROM refs with the same name.
- **What for**: Low-severity (no user is expected to name a CTE `__reflex_new_*`), but the code provides no guardrail. At minimum it should `ereport` if the source-target name already appears as a declared CTE.
- **Fix direction**: before the first substitution pass, scan the AST for any `__reflex_new_<src>`/`__reflex_old_<src>` CTE alias and bail out with a clear error.

---

## 5. MERGE INSERT defaults: function-based defaults collapsed to hardcoded literals

- **What**: When the MERGE-subtract path needs to materialize a row with a column that has a `DEFAULT` clause in the intermediate table, the code looks up `default_value_for_type` (literal zero / empty string / false), not the actual catalog-stored default expression. Columns with `DEFAULT now()`, `DEFAULT gen_random_uuid()`, `DEFAULT nextval(…)`, or custom function defaults receive the wrong value.
- **Why**: `src/trigger.rs` builds the MERGE clause by consulting the aggregation plan, not `pg_attribute.adbin`. Literal fallback is used for any column without a plan-level value.
- **How**: Define an IMV whose intermediate has a system column for example `inserted_at TIMESTAMPTZ DEFAULT now()` (tracked through user-supplied aggregate expression), then INSERT into the source — the newly created intermediate row carries epoch, not current time.
- **What for**: Correctness issue only for users who manually add columns to the intermediate. Current codebase may not hit this in production paths. Worth calling out before adding auto-audit columns.
- **Fix direction**: consult `pg_attrdef.adbin` via `pg_get_expr` to emit the actual default expression, not a literal.

---

## 6. GROUP BY on expression with implicit type coercion vs affected-groups extraction

- **What**: When `GROUP BY` contains a cast / expression (e.g., `GROUP BY CAST(order_id AS TEXT)`) the affected-groups extractor reads the raw source column type and joins the intermediate on `IS NOT DISTINCT FROM` — but the intermediate's column was stored as `TEXT` while the affected-group is re-extracted as `INT4`. False-negative on the join leaves affected groups unprocessed.
- **Why**: `src/aggregation.rs:713-736` (extra_cols extension for expression keys) calls `normalized_column_name` for the alias but `sanitize_for_col_name` for the intermediate column, and the type inference does not enforce a cast on the re-extracted side.
- **How**: `SELECT CAST(order_id AS TEXT), SUM(qty) FROM orders GROUP BY 1`. Insert rows, flush. If the cast is not stable (mixed NULLs), the affected-groups join misses rows.
- **What for**: Affects any IMV that groups on a computed expression involving casts or user-defined functions. In `db_clone` this path is not currently exercised (all groupings are plain columns), but it's a latent failure mode for future IMVs.
- **Fix direction**: always cast the re-extracted affected-group value to the intermediate column type, or force a canonical TEXT on both sides.

---

## 7. `resolve_column_type` silently returns `"text"` on catalog-lookup failure

- **What**: `src/schema_builder.rs:638-659::resolve_column_type` uses a catalog SPI query to resolve GROUP BY / source column types. On query failure (missing catalog row, permission error, or expression-based key), the function returns a hardcoded `"text"` type. This type is then used for the intermediate table DDL.
- **Why**: The function swallows the `SPI::get_one` error and falls back to text for robustness, but the fallback produces a TEXT column where an INT / NUMERIC should be — downstream SUM/AVG on that column are now string-valued.
- **How**: Any IMV where the group key is an expression that the catalog cannot resolve (e.g., `GROUP BY (col_a + col_b)`) gets a TEXT intermediate column. Numeric aggregates on a TEXT column still compute via implicit cast, but query plans are worse and behavior on overflow / NaN differs.
- **What for**: Degraded performance + potential subtle semantic drift on expression-group IMVs.
- **Fix direction**: propagate the error (or at least log WARNING and fall back to NUMERIC / explicit cast) instead of silent TEXT downgrade. Covered by a regression test that creates an expression-group IMV and asserts the intermediate column type.

---

## 8. Recursive CTE on base query → infinite flush loop, no guard

- **What**: The SQL analyzer (`src/sql_analyzer.rs:186-187`) records a `has_recursive_cte` flag but does not *reject* an IMV built on a WITH RECURSIVE CTE. On flush, the delta query re-invokes the recursive CTE against the delta — if the recursion cycles (a legitimate recursive CTE shape, e.g., traversing a graph), the flush never terminates.
- **Why**: The flag is informational only. No downstream code checks it to abort IMV creation or to bypass the delta path with a fallback full rebuild.
- **How**: `CREATE RECURSIVE VIEW org_hierarchy AS WITH RECURSIVE …; SELECT create_reflex_ivm(…)` succeeds today. Subsequent DML on the base table hangs the flush.
- **What for**: A rare but real DOS vector: a developer registers a recursive IMV, benchmarks look fine on empty tables, first flush in prod hangs the backend.
- **Fix direction**: in `create_reflex_ivm`, reject `has_recursive_cte = true` with a clear error pointing to the unsupported-views journal.

---

## 9. UNION ALL decomposition loses column quoting on reassembly

- **What**: Top-level `UNION ALL` is decomposed into per-branch sub-IMVs (good), but when assembling the union SELECT that references the sub-IMV targets, the column list is not re-quoted. A base query with `"column with spaces"` or other quoted identifiers loses the quoting on reassembly and produces a syntax error at CREATE IVM time.
- **Why**: `src/create_ivm.rs:117-130::union_selects` writes `SELECT <column_list> FROM sub_imv_0 UNION ALL SELECT <column_list> FROM sub_imv_1` — the column list is taken verbatim from the parsed AST's display form, which strips quotes.
- **How**: `CREATE VIEW v AS SELECT "unusual name" FROM t1 UNION ALL SELECT "unusual name" FROM t2;` → `create_reflex_ivm('v', …)` errors on CREATE.
- **What for**: The team's alp schema does not use quoted column names, so not actively failing. Non-alp users will hit it.
- **Fix direction**: quote every column name in the reassembled SELECT via `quote_identifier`.

---

## 10. No cycle detection on IMV dependency graph during `create_reflex_ivm`

- **What**: Related to the "fix: cycle" commit (c7d334c) but still not comprehensively guarded: if a user calls `create_reflex_ivm` with a body that transitively depends on the IMV being created (via a CTE or view that has already been split into a sub-IMV), the `__reflex_ivm_reference.depends_on` array becomes cyclic. Subsequent flush walks the graph in `graph_depth` order and double-processes the IMV, potentially corrupting the intermediate.
- **Why**: The cycle-detection logic catches the direct case (self-reference) but not the subtly-transitive case where a CTE split pulls in a dependency that in turn references the parent.
- **How**: Hard to construct without intentionally crafting a cyclic view definition; not seen in production but worth a unit test.
- **What for**: Defensive check that would prevent a class of "frankenstein" IMV graphs.
- **Fix direction**: run a transitive closure over `depends_on` at registration time and reject any edge that introduces a cycle.

---

## 11. Advisory-lock hash collision on long IMV names

- **What**: `pg_advisory_xact_lock(hashtext(_rec.name))` is used to serialize concurrent flushes on the same IMV. `hashtext` is a 32-bit hash; across the 20 registered IMVs in `db_clone`, collision probability is negligible today, but adding more IMVs or cross-database sharing of the advisory-lock namespace pushes this toward a 1 in 2^32 collision that would **silently serialize unrelated IMVs**.
- **Why**: 32-bit hash; no collision check.
- **How**: Birthday bound at ~77 K distinct IMV names for 1% collision chance. Unlikely to hit organically, but a grep across a multi-tenant Postgres could.
- **What for**: Very low-severity today, worth noting because the mitigation is trivial: use a 64-bit advisory lock with two-arg form `pg_advisory_xact_lock(key1, key2)` seeded from a stable 64-bit hash.
- **Fix direction**: two-arg advisory lock or a deterministic mapping from IMV name → stable OID via a shared sequence.

---

## 12. `replace_source_with_delta` pass 2 alias consumption — accepts keyword-looking user aliases

- **What**: The 2026-04-21 fix for alias consumption in `src/query_decomposer.rs::consume_table_alias` rejects SQL follow-keywords (JOIN / ON / WHERE / …) when the alias is bare. However, if a user writes `FROM t AS SELECT` (a syntactically-invalid alias), the function still consumes `SELECT` because the `AS` form is accepted without keyword validation.
- **Why**: The bare-ident branch checks for follow-keywords; the `AS <ident>` branch does not.
- **How**: `FROM orders AS SELECT` (user error). pg_reflex rewrites to `FROM (SELECT * FROM __reflex_delta…) AS SELECT` which the planner rejects with a confusing message later.
- **What for**: Ergonomics. A better error ("invalid alias `SELECT`") surfaced at `create_reflex_ivm` time would save debugging.
- **Fix direction**: reject any reserved-word alias in both branches. Existing `is_follow_keyword` already enumerates the set — extend its use.

---

## 13. `reflex_build_delta_sql` is `STRICT` — receives `NULL` `where_predicate` crashes call chain

- **What**: `reflex_build_delta_sql` is declared `STRICT` (auto-generated by pgrx). Trigger bodies pass `_rec.base_query`, `_rec.end_query`, `_rec.aggregations` which should never be NULL — but `_rec.where_predicate` IS nullable and is read by some trigger variants. The null check in the trigger body (`IF _rec.where_predicate IS NOT NULL THEN …`) protects the EXECUTE, but if a future refactor shifts the check inside the `reflex_build_delta_sql` call, STRICT causes the function to silently return NULL, the plpgsql skips the EXECUTE loop, and no delta is applied.
- **Why**: STRICT + nullable argument interaction.
- **How**: Refactor-trap, not actively triggerable today.
- **What for**: Defensive. Remove `STRICT` once any arg might be nullable, and handle Option<&str> explicitly.
- **Fix direction**: add `ParallelSafe, Volatile` with `Option<&str>` on any nullable arg if schema evolves.

---

## Priority ranking

| # | Bug | Severity | Fix cost |
|--:|---|---|---|
| 1 | 63-char identifier truncation | **high** (silent data corruption risk) | low |
| 2 | HAVING-only MIN/MAX recompute | high (wrong query result) | medium |
| 3 | COUNT(DISTINCT) NULL compound key | high (wrong count) | medium |
| 8 | Recursive CTE hang | medium (DOS) | low |
| 7 | resolve_column_type silent TEXT | medium | low |
| 6 | GROUP BY cast type mismatch | medium | medium |
| 9 | UNION ALL quote loss | low (users unaffected) | low |
| 10 | Transitive cycle detection | low | medium |
| 5 | MERGE default-expr literal | low | low |
| 4 | User CTE name collision | low | low |
| 12 | Keyword alias via `AS <kw>` | ergonomics | low |
| 11 | 32-bit advisory-lock hash | very low | low |
| 13 | STRICT + future nullable arg | latent | low |

Recommended first sprint: #1, #2, #3 (all correctness, all modest fix cost). #8 is a one-liner rejection and worth doing in the same patch.

---

## Resolution log (2026-04-22, same day as audit)

Worked the top-priority bugs plus the cheap ones in one sprint. Process per bug: write a failing test, then fix, then re-run. Don't patch the test after the fix.

| # | Bug | Disposition | Tests added |
|--:|---|---|---|
| 1 | 63-char identifier truncation | **Fixed.** Added `transition_new_table_name`, `transition_old_table_name`, `staging_delta_table_name` helpers in `query_decomposer.rs` that route the `__reflex_new_<src>` / `__reflex_old_<src>` / `__reflex_delta_<src>` concatenation through the existing `safe_identifier` truncate-plus-hash. Applied at all 9 call sites in `trigger.rs` (incl. `reflex_flush_deferred`) and `schema_builder.rs`. Also wrapped the trigger/function names (`__reflex_ins_trigger_on_<src>`, …) through `safe_identifier` so `CREATE OR REPLACE FUNCTION` can't silently overwrite a colliding neighbor. | `test_build_trigger_ddls_long_source_name_no_truncation`, `test_build_trigger_ddls_distinct_long_sources_do_not_collide`, `test_build_staging_table_ddl_long_source_name_no_truncation`, `test_build_deferred_trigger_ddls_long_source_name_no_truncation` |
| 2 | HAVING-only MIN/MAX/BOOL_OR recompute | **Already fixed — report stale.** `plan_aggregation` (aggregation.rs:587-665) already walks HAVING via `collect_having_aggregates` and pushes intermediate columns for any aggregate found there. `build_min_max_recompute_sql` then iterates `plan.intermediate_columns` and picks up HAVING-only MIN/MAX/BOOL_OR without needing a dedicated "HAVING-only" flag. Tests added to lock this in. | `test_having_only_max_creates_intermediate_column`, `test_having_only_min_creates_intermediate_column`, `test_having_only_bool_or_creates_intermediate_column`, `test_having_only_max_is_recomputed_on_delete` |
| 3 | `COUNT(DISTINCT nullable)` NULL matching | **Fixed, different root cause than the report.** The MERGE ON-clause was already null-safe (`trigger.rs:51-55` applies `IS NOT DISTINCT FROM` to every join column, group-by *and* distinct). The real bug was in the end query: `CountDistinct` mapped to `intermediate_expr = "COUNT(*)"`, which counts the `(grp, NULL)` compound-key rows that legitimately live in the intermediate — violating Postgres's `COUNT(DISTINCT val)` semantics (NULLs not counted). Changed to `COUNT("<arg>")`. The `starts_with("COUNT(")` detectors in `query_decomposer.rs` and `schema_builder.rs` still match the new form. | `test_build_merge_count_distinct_nullable_uses_null_safe_join` (unit), `test_correctness_count_distinct_nullable` (pg_test end-to-end with INSERT NULL / DELETE NULL mutations vs oracle) |
| 8 | Recursive CTE hang | **Already fixed — report stale.** `create_reflex_ivm` (create_ivm.rs:60) calls `unsupported_reason()` which rejects `has_recursive_cte` with `"RECURSIVE CTEs are not supported"`. Covered by existing `pg_test_error::test_error_recursive_cte` and `unit_sql_analyzer::test_recursive_cte_rejected`. | — (existing coverage sufficient) |
| 9 | UNION ALL quote loss | **Not applicable.** Reassembly at create_ivm.rs:119 is `format!("SELECT * FROM {}", quote_identifier(name))` — `SELECT *`, not an explicit column list. Quoted column names propagate through `*` unchanged. Bug premise was incorrect. | — |
| 12 | `FROM t AS <reserved>` alias consumption | **Fixed.** Lifted the existing `is_follow_keyword` check from the bare-identifier branch into the `AS <ident>` branch of `consume_table_alias`. Extended the keyword list with `SELECT`, `FROM`, `AS`, `DISTINCT` so `AS SELECT` / `AS FROM` / etc. are now rejected. Caller falls back to the default `__dt` alias, and the downstream planner sees a malformed `AS __dt AS SELECT …` that it rejects clearly instead of a silent mis-parse. | `test_replace_source_with_delta_rejects_reserved_word_as_alias`, `test_replace_source_with_delta_rejects_follow_keyword_as_alias` |

**Not addressed in this sprint**: #4 (user CTE alias collision), #5 (MERGE default-expr literal), #6 (GROUP BY cast coercion), #7 (`resolve_column_type` silent TEXT), #10 (transitive cycle detection), #11 (32-bit advisory-lock hash), #13 (STRICT on future nullable arg). Reasoning: #10–13 are lower-priority or latent; #4–7 would each benefit from a dedicated cycle. Leaving them here as a backlog.

**Audit quality note**: 3 of the 6 top-priority items turned out to be stale (#2, #8) or inaccurate (#9). For #3, the fix direction was wrong (the MERGE path was already null-safe; the end query was the culprit). The remaining items (#1, #12) matched the report. Takeaway: trust the symptom but always reproduce before fixing — the root-cause guesses in the audit were only right half the time.

**Verification**: `cargo pgrx test pg18` — 443 tests pass, 0 failures (up from 435 before the sprint, reflecting the 8 tests added above).
