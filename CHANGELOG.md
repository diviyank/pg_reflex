# Changelog

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
