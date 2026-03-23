# Changelog

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
- `install.sh` wrapper script — copies migration files alongside `cargo pgrx install`

### Fixed
- **Trigger table reference replacement** — schema-qualified tables with column qualifiers (e.g., `sales_simulation.product_id` from `alp.sales_simulation`) now work correctly in triggers. Previously caused `missing FROM-clause entry` on every INSERT/UPDATE/DELETE.
- **Cast expressions no longer silently dropped** — `SUM(x)::BIGINT` is now correctly detected as an aggregate. Previously, the cast wrapper hid the function from the aggregate detector.
- **Column name case normalization** — unquoted identifiers like `MONTH` are now lowercased consistently (matching PostgreSQL's case folding), preventing `column "MONTH" does not exist` errors at trigger time.
- **Source index creation** — index creation on source tables for MIN/MAX/BOOL_OR recompute now checks column existence first, so it no longer fails when group columns come from JOIN tables.
- Materialized views no longer cause "cannot have triggers" error
- Passthrough DELETE/UPDATE no longer does full table refresh
- **Passthrough JOIN key mapping** — unique key detection for passthrough JOINs now uses per-source-table column mappings derived from JOIN conditions. Previously, DELETE/UPDATE triggers on secondary tables (e.g., `products` in a `sales JOIN products` query) could corrupt data by matching the wrong column. Auto-detection is now restricted to single-source queries; JOINs require the explicit 3rd argument.
- Dropped PostgreSQL 13/14 from supported versions (MERGE statement requires PG15+)

### Performance
- **Deferred index creation** — indexes on intermediate and target tables are now created after bulk data insertion (not before), reducing IMV creation time by ~60% on large datasets
- **Faster `reflex_reconcile`** — drops all indexes (including user-created) before bulk rebuild, recreates them after. Saves index DDL and restores it faithfully. Reduced reconcile time by ~38% on large datasets (6:29 → 4:00 on 7.7M rows). Also uses TRUNCATE instead of DELETE for instant table clearing.
- **ANALYZE** — intermediate and target tables are analyzed after initial materialization and after reconcile for better query planner statistics

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
