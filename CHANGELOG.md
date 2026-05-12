# Changelog

## [1.4.2] - 2026-05-12

### Fixed
- **DEFERRED-mode flush failed on schema-qualified IMVs that JOIN across
  multiple sources and qualify columns with bare table names.** Customer
  workload: `UPDATE alp.demand_planning SET status = 'validated' WHERE id = …`
  on an IMV defined as
  ```sql
  SELECT … FROM alp.sales_simulation
  INNER JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
  …
  ```
  raised
  ```
  WARNING:  pg_reflex: IMV alp.ivm_sop_forecast_view flush failed at cascade:
  missing FROM-clause entry for table "__reflex_new_alp_demand_planning"
  (SQLSTATE 42P01)
  ```
  inside `reflex_flush_deferred`. Root cause: the deferred path pre-rewrites
  the base query (`replace_source_with_delta`) and then hands it back into
  `reflex_build_delta_sql`, which calls `replace_source_with_transition`. The
  first pass only rewrote schema-qualified column refs (`alp.demand_planning.col`
  → `__dt.col`) and left bare `demand_planning.col` untouched; the second pass
  then wholesale-replaced the surviving bare token with the transition-table
  name — but that table is not in the deferred-flush FROM clause (a delta
  subquery aliased `__dt` is). Pass 1b in `replace_source_with_delta` now also
  rewrites bare-name column qualifiers when the source is schema-qualified.

- **Two adjacent gaps in source rewriting fixed for parity:**
  - `FROM bare_table` written without a schema when the registered source is
    `schema.bare_table` is now also rewritten (Pass 2b in
    `replace_source_with_delta`, symmetric with the bare-name pass in
    `replace_source_with_transition`).
  - `FROM schema.t AS t` (alias matching the bare-source name): the alias is
    now consumed and the default alias (`__dt` in deferred, the unaliased
    transition table in immediate) is emitted instead. Without this, the
    rewritten qualifiers (`__dt.col` or `"__reflex_new_alp_t".col`) point at
    a name the user's explicit alias hides, and PG raises 42P01.
    Mirrored in `replace_source_with_transition` via a new
    `strip_redundant_bare_alias` pre-pass.

### Tests
- 526 lib tests (up from 519): 6 new unit tests covering the three
  qualifier-rewrite shapes (bare column qualifier under schema-qualified
  source; bare-name FROM under schema-qualified source; alias-equals-bare
  collision in both `replace_source_with_delta` and
  `replace_source_with_transition`).
- 1 new `pg_test_deferred_join_schema_qualified_with_bare_column_qualifiers`
  integration test reproducing the customer IMV shape end-to-end across
  INSERT, DELETE and UPDATE on the JOIN sources, with an `EXCEPT ALL` oracle
  against the fresh query.

### Migration
- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.2'`. No catalog rewrites; no IMV
  rebuilds. Existing backends may still serve cached delta SQL from before
  the upgrade — open a fresh connection to pick up the fix.

## [1.4.1] - 2026-05-11

### Fixed
- **`search_path`-dependent failures in internal trigger bodies.** Internal
  reflex tables (`__reflex_delta_<src>`, `__reflex_scratch_<view>`,
  `__reflex_pt_new/old_<view>_<src>`, `__reflex_affected_<view>`,
  `__reflex_shrunk_<view>`) were created with unqualified names and so
  ended up in whichever schema topped the *creating* session's
  `search_path`. Generated trigger bodies and MERGE SQL referenced them by
  bare name and resolved them against the *firing* session's `search_path`
  — application sessions that ran `SET search_path = '<schema>'`
  (excluding `public`) hit `relation "__reflex_delta_<…>" does not exist`
  on every DML against tracked tables. Reproduced against a customer
  workload where `alp.demand_planning` fed `alp.ivm_sop_forecast_view`
  and the writer set `search_path = 'alp'`.

  Fix: every internal artefact is now co-located with its owning IMV
  (per-IMV scratch / passthrough / affected / shrunk) or source table
  (staging delta), and both the DDL and every generated SQL reference are
  schema-qualified. Trigger bodies' internal SPI calls
  (`reflex_build_delta_sql`, `reflex_build_truncate_sql`,
  `reflex_execute_separated`, `reflex_flush_deferred`) are qualified to
  `public.` — they live in the extension's schema (public by convention)
  and the same `search_path` rule applied to them. `reflex_ivm_stats`
  also now reads the intermediate table from the IMV's schema instead of
  hard-coding `public.__reflex_intermediate_<bare>`, fixing a pre-existing
  reporting bug on schema-qualified IMVs.

### Breaking
- Existing IMVs upgraded from 1.4.0 (or earlier) carry the old bare-name
  trigger bodies and old bare-name internal tables in postgres' catalog.
  `ALTER EXTENSION pg_reflex UPDATE TO '1.4.1'` cannot rewrite them in
  place. Drop and recreate each affected IMV after upgrade:
  ```sql
  SELECT drop_reflex_ivm('<schema>.<view>');
  SELECT create_reflex_ivm('<schema>.<view>', '<SELECT …>', …);
  ```
  The 1.4.0 → 1.4.1 migration script emits a per-IMV `NOTICE` listing
  what to rebuild.

### Tests
- 518 lib tests (up from 513 in 1.4.0): 5 new integration tests in
  `pg_test_search_path.rs` exercising IMMEDIATE / DEFERRED / passthrough /
  top-K MIN-MAX / shared-source IMVs under `SET search_path = '<custom>'`
  (excluding `public`), each verifying schema co-location and correctness
  via an `EXCEPT ALL` oracle against the fresh query.

## [1.4.0] - 2026-05-10

### Behaviour change
- **Top-K is auto-enabled (`K=16`) on every MIN/MAX intermediate column.**
  `create_reflex_ivm` and `create_reflex_ivm_if_not_exists` now pass `topk=16`
  by default; the parameter is a no-op for SUM / COUNT / AVG / BOOL_OR. This
  closes the audit R3 retraction cliff for MIN/MAX IMVs without operator
  opt-in. Append-only MIN/MAX workloads that prefer the lower INSERT
  overhead can opt out via the 6-arg overload with `topk = 0`.

### Performance
- **N1 — heap-shrinkage-gated UPDATE recompute on top-K MIN/MAX.** UPDATEs
  that don't displace a top-K element no longer trigger a source-scan
  recompute. A new persistent `__reflex_shrunk_<view>` UNLOGGED capture
  table (provisioned at IMV-create time iff the plan has any top-K column)
  records groups whose heap shrank below `K` during the algebraic Sub step.
  The forced recompute that follows is then scoped to that subset rather
  than to every affected group. Bench (5M-row source, K=16, ~10K rows/group):
  ~30× on 1K-row UPDATE batches, ~8.5× on 10K, ~2× on 100K
  (`benchmarks/bench_n1_topk_update.sql`). Workloads with group cardinality
  ≤ K still pay the recompute on every UPDATE — the heap always shrinks.
- **O2 — `reflex_build_delta_sql` per-backend template cache.** Identical
  delta-SQL templates fired repeatedly inside one session now reuse the
  cached string instead of re-running the SQL builder. Benefit is sub-ms
  per fire; primary win is OLTP-shape sessions with tight trigger loops.
  No public API surface; bounded at 256 entries per backend.

### Fixed
- **Top-K MIN/MAX over non-NUMERIC source columns (TEXT / DATE / TIMESTAMP).**
  `IntermediateColumn.pg_type` was hardcoded to `"NUMERIC"` by the planner;
  the schema builder special-cased the resolved type for DDL but the trigger
  MERGE codegen read `pg_type` directly and emitted `'{}'::NUMERIC[]`,
  producing `COALESCE could not convert type numeric[] to text[]`. After
  catalog introspection, `create_reflex_ivm_impl` now propagates the
  resolved source-arg type back onto the MIN/MAX intermediate column.
- **Top-K partial-heap staleness on UPDATE.** When `K < group_cardinality`,
  an UPDATE that retracted a heap-resident value left the heap non-empty
  but missing the unchanged source rows that should have been promoted into
  it. The 1.3.0 recompute trigger fired only on `cardinality(heap) = 0`,
  so a *partial* heap slipped through and a subsequent DELETE then read a
  stale `heap[1]`. Fix: split the UPDATE flow's recompute trigger into two
  paths — non-top-K MIN/MAX keeps the legacy `Sub → recompute(if scalar
  IS NULL) → Add` order; top-K MIN/MAX uses
  `Sub → topk_refresh → Add → forced recompute` (gated to the shrunk-
  groups capture table from N1 above). Regression locked in by
  `pg_test_topk_partial_heap_staleness_regression` and the existing
  50-mutation × 3-group × K=16 fuzzer.
- **Non-deterministic-function rejection is query-wide.** The analyzer
  flag `has_nondeterministic_select` always rejected `NOW()` / `RANDOM()` /
  etc. anywhere `pre_visit_expr` reached (SELECT, WHERE, HAVING, JOIN ON,
  ORDER BY) — the user-facing message claimed "in SELECT" only. The
  message now reads "anywhere in the query" and explains why (drift over
  time without a corresponding source mutation). Behaviour unchanged.

### Tests
- 513 lib tests (up from 503 in 1.3.0).
- New: `pg_test_topk_text_min_max`, `pg_test_topk_date_min_max`,
  `pg_test_topk_timestamp_min_max` (non-NUMERIC element types);
  `pg_test_topk_partial_heap_staleness_regression` (UPDATE-then-DELETE
  staleness minimal repro); `pg_test_topk_update_no_heap_shrink_keeps_correctness`,
  `pg_test_topk_update_mixed_shrink_groups`,
  `pg_test_topk_update_multi_column_shrink` (N1 gate correctness).

### Docs
- Operator runbook gains a LOGGED-vs-UNLOGGED decision matrix, a
  stuck-flush triage recipe (`pg_stat_activity` filter on
  `application_name = 'reflex_flush:%'` + `reflex_explain_flush`), and an
  auto-on top-K caveat.
- `docs/limitations/unsupported-shapes.md` rewritten as a three-bucket
  taxonomy (hard-rejected / supported-with-fallback / operator workaround)
  and refreshed against current behaviour. Stale "needs top-K opt-in"
  language replaced with the auto-on guarantee.
- `docs/limitations/known-issues.md` pruned: the 1.3.x top-K closed items
  moved into release notes (this CHANGELOG); only items that are still
  open or still surprising remain on the page.

## [1.3.0] - 2026-04-25

### Performance
- **Bounded top-K heap for MIN/MAX (audit R3)** — opt-in `topk` parameter on
  `create_reflex_ivm` (6th positional arg, integer K, default disabled). When
  enabled, each MIN/MAX intermediate column gains a sibling
  `__<name>_topk <type>[]` array maintained on every flush:
  - INSERT path: top-K sorted-merge of `t.topk || d.topk` truncated to K.
  - DELETE/UPDATE path: multi-set subtraction via the new
    `public.__reflex_array_subtract_multiset(anyarray, anyarray)` plpgsql
    helper; the scalar `__min_x` / `__max_x` is rebuilt from `topk[1]`.
  - Heap-underflow fallback: when the array empties, the existing scoped
    recompute (1.2.0) takes over and rebuilds both the scalar and the array
    from the source.
  Existing IMVs continue to use the scoped-recompute path with no migration
  cost. Closes the `stock_chart_*` cliff documented in
  `journal/2026-04-22_unsupported_views.md` §6 / audit R3 — the 3 IMVs there
  become eligible for incremental maintenance once operators opt in.

### Added
- **Per-IMV flush histogram (audit R6)** — `__reflex_ivm_reference` gains
  a `flush_ms_history BIGINT[]` ring buffer (size 64) populated by
  `reflex_flush_deferred`. New SPI
  `reflex_ivm_histogram(view_name) -> (p50_ms, p95_ms, p99_ms, max_ms, samples)`.
- **`pg_stat_statements` correlation** — each per-IMV flush body sets
  `application_name = 'reflex_flush:<view>'` for its duration, so operators
  with `track_application_name = on` can filter pg_stat_statements rows by
  IMV.
- **Scalar MIN/MAX (no GROUP BY)** is now a tested supported shape (audit
  unsupported §2). Two new correctness tests in `pg_test_correctness.rs`.
  With `topk=K`, scalar retraction becomes O(K) instead of O(N).

### Tests
- 503 lib tests (up from 497 in 1.2.1).
- New: 3 top-K integration tests including a 30-iteration random fuzz with
  EXCEPT ALL oracle, 2 scalar MIN/MAX tests, 2 histogram tests.

## [1.2.1] - 2026-04-25

### Added
- **`pg_reflex.alter_source_policy` GUC** — controls how the
  `reflex_on_ddl_command_end` event trigger reacts when a tracked source is
  altered. Default `'warn'` preserves 1.2.0 behaviour. Set to `'error'` to roll
  back the ALTER instead of warning (useful for change-control gates). Closes
  audit risk R2.
- **`reflex_scheduled_reconcile(max_age_minutes INTEGER DEFAULT 60)`** — SPI
  designed for pg_cron-driven drift scans. Iterates IMVs whose
  `last_update_date` is older than the threshold (or NULL), reconciles each in
  isolation, and returns `(name, status, ms)` per attempt. Closes audit risk
  R7 with a code-and-recipe approach instead of a background worker.

### Improved
- **Passthrough PK auto-detection** (audit R5) — already worked for single-source
  passthroughs; 1.2.1 adds a clearer info message when the source has a PK but
  the SELECT list does not include all PK columns, telling operators what to add.

### Tests
- 493 lib tests (up from 487 in 1.2.0).
- New: 3 alter-source-policy tests, 2 PK auto-detection tests, 2
  scheduled-reconcile tests, 3 source-drop cleanup tests.

## [1.2.0] - 2026-04-24

### Performance
- **Affected-groups-scoped MIN/MAX recompute** — `build_min_max_recompute_sql` now wraps the `orig_base_query` in a filter that restricts it to groups present in `__reflex_affected_<view>`. On retractions, only groups actually touched by the delta get re-aggregated, instead of every group in the source. For IMVs with MIN/MAX over large sources (stock_chart-style workloads), this turns a full-scan recompute into an O(delta) operation when the affected-group set is small.

### Added
- **Operational safety — per-IMV SAVEPOINT in cascade flush** — `reflex_flush_deferred` wraps each per-IMV flush body in its own `SAVEPOINT`. One bad IMV (e.g. a broken base_query after a source schema change) logs a `WARNING` and allows the cascade to continue instead of aborting every upstream update.
- **Event trigger — auto-drop on source drop** — new `reflex_on_sql_drop` event trigger (`sql_drop`). Dropping a source table now drops every artifact owned by the IMV (target, intermediate, affected-groups, delta-scratch and passthrough-scratch tables, plus the standalone trigger functions) and removes the registry row. Cascades through `graph_child` so child IMVs are cleaned up too. Closes audit risk R1.
- **Event trigger — warn on source `ALTER TABLE`** — new `reflex_on_ddl_command_end` event trigger (`ddl_command_end`, tag `ALTER TABLE`). Raises a `WARNING` suggesting `reflex_rebuild_imv` when a tracked source is altered.
- **`reflex_rebuild_imv(name)`** — public alias over `reflex_reconcile` for consistency with post-schema-change recovery guidance.
- **Observability — registry columns** — `__reflex_ivm_reference` gains `last_flush_ms`, `last_flush_rows`, `flush_count`, `last_error`. Populated by each per-IMV `SAVEPOINT` block inside `reflex_flush_deferred` (success clears `last_error`; failure records it).
- **Observability — SPIs** — `reflex_ivm_status()`, `reflex_ivm_stats(view_name)`, `reflex_explain_flush(view_name)` let operators inspect registered IMVs, their sizes, and the next-flush plan without firing a write.
- **Streaming separator for trigger bodies** — `reflex_execute_separated(sql)` #[pg_extern] consumes a `--<<REFLEX_SEP>>--`-delimited statement stream. Used by the `TRUNCATE` trigger body; INSERT/DELETE/UPDATE trigger bodies still use the `string_to_array` loop because calling an extension function from inside those trigger bodies drops transition-table scope.

### Fixed
- **Bug #10 — transitive cycle detection** in `create_reflex_ivm`. Walks existing `depends_on` edges before registering the new row; rejects circular dependencies with a clear error.
- **Bug #11 — 64-bit advisory lock keys** — `pg_advisory_xact_lock(key1, key2)` seeded from a 64-bit hash, replacing the single-`hashtext`-arg form that could collide across names.
- **Bug #7 — `resolve_column_type` silent TEXT** — emits `pgrx::warning!` on catalog-lookup failure and defaults to `NUMERIC` instead of `TEXT`. Cast errors at CREATE time are preferable to silent behaviour drift.
- **Bug #4 — reserved CTE alias collision** — `create_reflex_ivm` rejects user CTEs named `__reflex_new_<src>` / `__reflex_old_<src>` / `__reflex_delta_<src>` rather than silently corrupting rewrites.
- **Bug #13 — STRICT vs nullable `where_predicate`** — handled inside `reflex_flush_deferred` rather than at the function signature, keeping the one-arg extension API stable.

### Tests
- 485 lib tests (up from 481 in 1.1.3).
- New: 4 unit tests for the affected-groups-scoped recompute SQL shape (`test_min_max_recompute_scoped_to_affected_groups_when_provided`, `test_min_max_recompute_no_affected_filter_when_none_passed`, `test_min_max_recompute_affected_filter_uses_multiple_group_columns`, `test_min_max_recompute_skips_affected_filter_for_sentinel_plan`).

### Deferred to 1.3.0
- **MIN/MAX bounded top-K heap (`__min_X_topk`)** — originally scoped for 1.2.0; deferred after evaluating complexity-vs-payoff. The affected-groups-scoped recompute above captures the common-case win at a fraction of the code and migration cost. Top-K revisits once benchmark data shows retractions repeatedly hitting the same hot groups.
- **Lazy index maintenance on bulk rebuild** — `DROP INDEX … INSERT … CREATE INDEX` when the affected set exceeds 50 % of the intermediate. Niche payoff and risky under concurrent flushers with advisory locks; left out of 1.2.0 pending a realistic workload that benefits.

## [1.1.3] - 2026-04-22

### Performance
- **Algebraic `BOOL_OR`** — `BOOL_OR(expr)` now decomposes into two BIGINT companion columns (`__bool_or_<arg>_true_count` and `__bool_or_<arg>_nonnull_count`), both maintained with pure `SUM(+)/SUM(-)` algebra. Removes the full-scan recompute on DELETE/UPDATE. End-query maps the two counters back to boolean via a `CASE` expression that preserves Postgres `BOOL_OR` NULL semantics (`NULL` when every input was NULL, `FALSE` when at least one was non-NULL and none TRUE, `TRUE` otherwise).
- **Empty-affected DO-block gate** — the targeted `DELETE + INSERT` path for group-by IMVs is now wrapped in a `DO $$ … IF EXISTS(…) THEN … END IF; END $$` block that short-circuits when the affected-groups staging table is empty. Avoids a full target-table scan on transactions that produce no matching groups.
- **`parallel_safe` SQL-building functions** — `reflex_build_delta_sql` and `reflex_build_truncate_sql` are annotated `PARALLEL SAFE`. They read no shared state and produce deterministic SQL given identical arguments.
- **Staging-delta `ANALYZE`** — `reflex_flush_deferred` runs `ANALYZE` on the staging delta table before processing so the planner gets non-zero row estimates after the `TRUNCATE` that reset stats.
- **Per-IMV `where_predicate` registry column** — the IMV registry stores each view's `where_predicate`. Deferred UPDATE trigger bodies check the predicate against the transition table before taking the advisory lock; `reflex_flush_deferred` skips IMVs whose predicate matches no staged row. Particularly effective for sub-IMVs of a `UNION` with disjoint filters.
- **End-query targeted splice for `GROUP BY` end_queries** — `reflex_build_delta_sql` splices `AND (<gb_cols>) IN (SELECT DISTINCT <gb_cols> FROM "<affected_tbl>")` before the `GROUP BY` clause instead of falling back to a full `DELETE + INSERT … end_query`. Primary beneficiary: `COUNT(DISTINCT)` IMVs.

### Fixed
- **63-char identifier truncation** — `transition_new_table_name`, `transition_old_table_name`, and `staging_delta_table_name` now generate guaranteed-unique, ≤63-byte identifiers via a sanitize-then-truncate helper. Previously, long source names could produce colliding transition-table names across IMVs.
- **MIN/MAX / BOOL_OR recompute scalar-subquery bug** — `build_min_max_recompute_sql` wraps `orig_base_query` as `(…) AS __src` before referencing group keys. Previously the direct-column reference failed with `missing FROM-clause entry for table "alias"` on JOIN-aliased base queries.
- **Concurrent-flush advisory-lock collision** — the deferred-flush advisory-lock key now derives from a hash of `(view_name, source_table)` jointly, so two concurrent sessions flushing different IMVs on the same source don't serialize on the same integer key.

### Tests
- 481 lib tests (up from 406 in 1.1.1).

## [1.1.1] - 2026-03-29

### Added
- **FILTER clause support** — `SUM(x) FILTER (WHERE cond)`, `COUNT(*) FILTER (WHERE cond)`, `AVG(x) FILTER (WHERE cond)`, `MIN/MAX(x) FILTER (WHERE cond)`, and `BOOL_OR(x) FILTER (WHERE cond)` are now supported. Internally rewritten to `CASE WHEN` expressions, so all existing incremental maintenance (MERGE, delta, triggers) works transparently. Multiple FILTER aggregates alongside regular aggregates in the same query are supported.
- **DISTINCT ON support** — `SELECT DISTINCT ON (cols) ... ORDER BY ...` is decomposed into a passthrough sub-IMV (incrementally maintained) + a VIEW with `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) WHERE rn = 1`. INSERT/DELETE/UPDATE on source data is reflected instantly. Supports multiple partition columns, WHERE clause, and JOINs.

### Fixed
- **DROP CASCADE** — `drop_reflex_ivm(name, true)` now issues `DROP TABLE ... CASCADE` on target, intermediate, and affected-groups tables. Previously, cascade only dropped child IMVs in the reflex dependency graph but left external PostgreSQL objects (views, foreign keys) intact, causing the drop to fail if any existed.
- **DROP VIEW/TABLE detection** — `drop_reflex_ivm` now detects whether the target is a VIEW (window/DISTINCT ON decompositions) or TABLE and issues the correct DROP command. Previously, dropping a window-function or DISTINCT ON IMV would fail with "is not a table".

### Internal
- **Codebase restructured** — `lib.rs` reduced from 10,548 to 189 lines. Implementation split into focused modules: `create_ivm.rs` (IVM creation), `drop_ivm.rs` (drop logic), `reconcile.rs` (reconcile/refresh). Submodule tests extracted into separate files under `src/tests/`.
- **Tests reorganized** — tests split into 20 categorized files (basic, trigger, passthrough, CTE, set ops, window, drop, reconcile, deferred, error, e2e, correctness, filter, distinct_on, plus 6 unit test files).

### Tests
- 406 tests (up from 375 in v1.0.4)
- New: 7 FILTER unit tests, 9 FILTER integration tests, 5 DISTINCT ON unit tests, 9 DISTINCT ON integration tests, 1 non-SELECT rejection test

## [1.1.0] - 2026-03-29

### Fixed
- **DROP CASCADE** — `drop_reflex_ivm(name, true)` now issues `DROP TABLE ... CASCADE` on target, intermediate, and affected-groups tables.

### Internal
- **Codebase restructured** — `lib.rs` reduced from 10,548 to 189 lines. Implementation split into focused modules.
- **Tests reorganized** — tests split into categorized files under `src/tests/`.

### Tests
- 376 tests (up from 375 in v1.0.4)

## [1.0.4] - 2026-03-26

### Performance
- **Empty-delta early-exit** — triggers check if the transition table is empty before entering the IMV processing loop. Skips all Rust FFI calls, advisory locks, and MERGE generation when a statement doesn't produce relevant rows. Saves 5-15ms per trigger fire for empty deltas.
- **Predicate-filtered trigger skip** — WHERE clauses from IMV queries are stored in `__reflex_ivm_reference.where_predicate`. Before processing an IMV, the trigger evaluates the predicate against the transition table. Non-matching IMVs are skipped entirely (no advisory lock, no delta SQL). Particularly effective for UNION sub-IMVs with disjoint filters.
- **Persistent affected-groups table** — replaced per-trigger-fire `DROP TABLE + CREATE TEMP TABLE` with a persistent UNLOGGED table created at IMV setup time. Uses `TRUNCATE` (0.17ms) instead of `DROP+CREATE` (0.65ms) — 3.9x faster per trigger fire.
- **Single-pass UPDATE MERGE** — for aggregate queries without MIN/MAX, UPDATE operations use a single net-delta MERGE combining old and new transition tables, halving the MERGE count.

### Added
- **INTERSECT support** — `SELECT ... INTERSECT SELECT ...` decomposes into sub-IMVs, same pattern as UNION.
- **EXCEPT support** — `SELECT ... EXCEPT SELECT ...` decomposes into sub-IMVs.

### Tests
- 218 tests (up from 214 in v1.0.3)
- New: 2 INTERSECT tests, 2 EXCEPT tests

### Benchmarks (single-IMV, warm cache, 1M source rows)
- GROUP BY UPDATE 100 rows: **4.4ms** (vs 55ms REFRESH MATERIALIZED VIEW)
- PASSTHROUGH INSERT 1K rows: **10ms** (vs 2,500ms REFRESH — 250x faster)
- Per-IMV overhead: ~4ms warm, scales linearly with number of IMVs on same source

## [1.0.3] - 2026-03-26

### Added
- **WINDOW function support** — queries with `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `SUM() OVER (...)`, and any other PostgreSQL window function are now supported. Decomposed into a base sub-IMV (incrementally maintained) + a VIEW that applies window functions at read time. For GROUP BY + WINDOW, the VIEW scans only the small intermediate result (one row per group).
- **UNION ALL / UNION support** — set operations are decomposed into per-operand sub-IMVs. `UNION ALL` creates a zero-overhead VIEW over the sub-IMV targets. `UNION` (dedup) creates a VIEW with PostgreSQL's native deduplication. Supports 2+ operands, aggregates in operands, and mixed WHERE filters on the same source table.
- **`storage` parameter** — `create_reflex_ivm('v', 'SELECT ...', NULL, 'LOGGED')` creates WAL-logged tables for crash safety. Default: `'UNLOGGED'` (current behavior). Propagated to CTE sub-IMVs and UNION sub-IMVs.
- **`mode` parameter** — `create_reflex_ivm('v', 'SELECT ...', NULL, 'UNLOGGED', 'DEFERRED')` accumulates deltas during the transaction and flushes at COMMIT via a two-stage trigger design (immediate capture to staging table + deferred constraint trigger). Default: `'IMMEDIATE'` (current behavior).
- **Materialized view auto-refresh** — event trigger on `ddl_command_end` automatically cascades `REFRESH MATERIALIZED VIEW` to dependent pg_reflex IMVs. No manual `refresh_imv_depending_on()` needed.
- New `window.rs` module for window function query decomposition
- `reflex_flush_deferred(source_table)` function for manual deferred delta processing

### Performance
- **Single-pass UPDATE MERGE** — for aggregate queries without MIN/MAX, UPDATE operations now use a single net-delta MERGE (combining old and new transition tables) instead of two separate MERGEs. Reduces MERGE count by 50% for UPDATE operations.

### Migration
- New columns in `__reflex_ivm_reference`: `storage_mode` (default `'UNLOGGED'`), `refresh_mode` (default `'IMMEDIATE'`). Existing IMVs backfilled automatically.
- Deferred processing infrastructure: `__reflex_deferred_pending` table + constraint trigger created automatically.
- Materialized view event trigger installed automatically.
- Migration is automatic via `ALTER EXTENSION pg_reflex UPDATE`.

### Tests
- 214 tests (up from 172 in v1.0.2)
- New test coverage: 9 UNION ALL tests, 5 UNION dedup tests, 18 WINDOW function tests, 5 LOGGED mode tests, 3 DEFERRED mode tests

### API
```sql
-- Full signature (all params have defaults, backward-compatible)
SELECT create_reflex_ivm(
    'view_name',                -- TEXT: view name
    'SELECT ...',               -- TEXT: query
    NULL,                       -- TEXT: unique_columns (optional)
    'UNLOGGED',                 -- TEXT: storage mode ('LOGGED' or 'UNLOGGED')
    'IMMEDIATE'                 -- TEXT: refresh mode ('IMMEDIATE' or 'DEFERRED')
);
```

## [1.0.2] - 2026-03-24

### Performance
- **UNLOGGED target table** — target tables are now `UNLOGGED` (matching intermediate tables). Eliminates WAL writes on every targeted refresh (DELETE+INSERT), reducing write overhead. Crash recovery already required `reflex_reconcile()` due to the UNLOGGED intermediate, so this adds zero additional risk.
- **Hash index on intermediate** — single-column GROUP BY keys now use a hash index instead of a B-tree primary key for O(1) MERGE lookups (~30% faster MERGE for single-column groups). Multi-column GROUP BY falls back to B-tree (hash doesn't support multi-column in PostgreSQL). The B-tree PK is removed because MERGE handles insert-or-update correctly and advisory locks prevent concurrent modifications.
- **MERGE RETURNING** — the delta query now runs once per trigger fire instead of twice. The MERGE into intermediate uses `RETURNING` in a CTE to capture affected group keys, eliminating the separate `SELECT DISTINCT groups FROM (delta_query)` statement. For UPDATE operations, delta_old and delta_new each run once instead of twice (4 → 2 executions).

### Benchmarks (100K groups, 1M source, single-column GROUP BY)
- INSERT 10K: 236ms → 171ms (**28% faster**)
- INSERT 50K: 1,170ms → 865ms (**26% faster**)
- INSERT 100K: 2,298ms → 1,802ms (**22% faster**)

### Migration
- Existing aggregate IMVs: intermediate PK dropped and replaced with hash/B-tree index, target table converted to UNLOGGED. Migration is automatic via `ALTER EXTENSION pg_reflex UPDATE`.
- Existing passthrough IMVs: target table converted to UNLOGGED.

### Tests
- 172 tests (unchanged from v1.0.1, all passing)

## [1.0.1] - 2026-03-23

### Added
- **`bool_or(expr)` aggregate** — incremental via OR on INSERT, recomputes from source on DELETE (same pattern as MIN/MAX)
- **Cast propagation** — `SUM(x)::BIGINT` now produces a BIGINT column in the target table (cast applied in end query, intermediate still stores NUMERIC for precision)
- **Target table index** — composite index on group columns for faster targeted refresh DELETE performance
- **Unsupported aggregate warnings** — unrecognized aggregates (e.g., `string_agg`) now emit a WARNING instead of being silently dropped
- Materialized view support as source tables (triggers auto-skipped, warning emitted)
- `refresh_reflex_imv(view_name)` — refresh a single IMV (alias for `reflex_reconcile`)
- `refresh_imv_depending_on(source)` — refresh all IMVs depending on a source table or materialized view
- HAVING clause support with AST-based rewriting (handles complex expressions like `AVG(x) > COUNT(*)`)
- Auto-detection of HAVING aggregates not in SELECT list (added to intermediate table automatically)
- Incremental passthrough DELETE/UPDATE (O(delta) row-matching instead of O(N) full refresh)
- Multi-level cascade confirmed and tested (works to arbitrary depth)
- CTE passthrough support (passthrough CTEs become sub-IMV tables)
- `create_reflex_ivm_if_not_exists(name, sql)` / `create_reflex_ivm_if_not_exists(name, sql, unique_columns)` — idempotent IMV creation that returns a notice instead of an error if the view already exists
- `install.sh` wrapper script — copies migration files alongside `cargo pgrx install`
- Subquery warning — subqueries in FROM now emit an informational warning (like materialized views)

### Fixed
- **Trigger table reference replacement** — schema-qualified tables with column qualifiers (e.g., `sales_simulation.product_id` from `alp.sales_simulation`) now work correctly in triggers. Previously caused `missing FROM-clause entry` on every INSERT/UPDATE/DELETE.
- **Cast expressions no longer silently dropped** — `SUM(x)::BIGINT` is now correctly detected as an aggregate. Previously, the cast wrapper hid the function from the aggregate detector.
- **Column name case normalization** — unquoted identifiers like `MONTH` are now lowercased consistently (matching PostgreSQL's case folding), preventing `column "MONTH" does not exist` errors at trigger time.
- **Source index creation** — index creation on source tables for MIN/MAX/BOOL_OR recompute now checks column existence first, so it no longer fails when group columns come from JOIN tables.
- Materialized views no longer cause "cannot have triggers" error
- Passthrough DELETE/UPDATE no longer does full table refresh
- **Passthrough JOIN key mapping** — unique key detection for passthrough JOINs now uses per-source-table column mappings derived from JOIN conditions. Previously, DELETE/UPDATE triggers on secondary tables (e.g., `products` in a `sales JOIN products` query) could corrupt data by matching the wrong column. Auto-detection is now restricted to single-source queries; JOINs require the explicit 3rd argument.
- Dropped PostgreSQL 13/14 from supported versions (MERGE statement requires PG15+)
- **BOOL_OR recompute on DELETE** — the recompute SQL was generated but never executed because the guard condition only checked for MIN/MAX, not BOOL_OR. Now fixed.
- **Subqueries with aggregation in FROM** — now rejected at creation time with a clear error suggesting CTE as the alternative (pg_reflex decomposes CTEs into sub-IMVs automatically). Previously, these silently produced incorrect results because the trigger replaced the inner table with the transition table, making the inner aggregation see only delta rows.

### Performance
- **Deferred index creation** — indexes on intermediate and target tables are now created after bulk data insertion (not before), reducing IMV creation time by ~60% on large datasets
- **Faster `reflex_reconcile`** — drops all indexes (including user-created) before bulk rebuild, recreates them after. Saves index DDL and restores it faithfully. Reduced reconcile time by ~38% on large datasets (6:29 → 4:00 on 7.7M rows). Also uses TRUNCATE instead of DELETE for instant table clearing.
- **ANALYZE** — intermediate and target tables are analyzed after initial materialization and after reconcile for better query planner statistics

### Tests
- 172 tests (up from 138 in v1.0.0) covering BOOL_OR, LEFT/RIGHT JOIN, cast propagation, subqueries, passthrough JOINs with per-source key mapping, chained IMVs with passthrough layers, and multiple mixed IMVs on same source

## [1.0.0] - 2026-03-22

### Added
- `drop_reflex_ivm(view_name)` and `drop_reflex_ivm(view_name, cascade)` for removing IMVs and all artifacts
- `reflex_reconcile(view_name)` for rebuilding IMVs from source data
- TRUNCATE trigger support (clears intermediate and target on source TRUNCATE)
- Targeted group refresh (only affected groups re-materialized, not the full target table)
- CTE decomposition (each CTE becomes a sub-IMV, passthrough outer queries become VIEWs)
- Passthrough CTE support (CTEs without aggregation work as passthrough sub-IMVs)
- MERGE-based delta processing (replaces INSERT ON CONFLICT for better performance)
- Schema-qualified view names (`myschema.my_view`) — views, intermediate tables, and triggers are created in the correct schema
- View name validation (rejects names with special characters to prevent SQL injection)
- Duplicate view name detection (returns error instead of crashing)
- PostgreSQL logging for key operations (`info!` on create/drop/reconcile, `warning!` on errors)
- GitHub Actions CI testing on PostgreSQL 15, 17, 18
- Automated release workflow with `.deb` package builds on version tags
- Concurrent operation test suite (parallel psql sessions)
- Property-based testing with proptest for input validation and query decomposition
- Multi-run benchmark harness (`benchmarks/run_bench.sh`) with variance reporting
- Deterministic benchmark seeds (`setseed`) for reproducible results
- 138 tests (63 unit + 7 proptest + 68 integration) covering all aggregate types, CTEs, JOINs, schema support, cascading, and edge cases
- 17 SQL benchmark scripts covering scales from 1K to 5M rows
- Apache 2.0 license

### Fixed
- SQL parser no longer panics on malformed input (returns error string instead of crashing PostgreSQL backend)
- SQL injection vectors eliminated via parameterized queries and input validation
- Catalog queries (`information_schema.columns`) now use parameterized queries
- Passthrough DELETE/UPDATE now incremental (O(delta) row-matching instead of O(N) full refresh)
- Multi-level cascade propagation works automatically to arbitrary depth (was incorrectly listed as a limitation)

### Supported
- PostgreSQL 15, 16, 17, 18 (requires MERGE statement, PG15+)
- Aggregates: SUM, COUNT, COUNT(*), AVG, MIN, MAX, BOOL_OR
- DISTINCT, GROUP BY, WHERE, INNER/LEFT/RIGHT JOIN
- Non-recursive CTEs (decomposed into sub-IMVs)
- Multi-level IMV cascading (A → B → C, tested up to 4 levels)
- Schema-qualified view names and source tables
