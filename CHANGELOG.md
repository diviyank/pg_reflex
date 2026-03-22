# Changelog

## [1.0.1] - 2026-03-22

### Added
- Materialized view support as source tables (triggers auto-skipped, warning emitted)
- `refresh_reflex_imv(view_name)` — refresh a single IMV (alias for `reflex_reconcile`)
- `refresh_imv_depending_on(source)` — refresh all IMVs depending on a source table or materialized view
- HAVING clause support with AST-based rewriting (handles complex expressions like `AVG(x) > COUNT(*)`)
- Auto-detection of HAVING aggregates not in SELECT list (added to intermediate table automatically)
- Incremental passthrough DELETE/UPDATE (O(delta) row-matching instead of O(N) full refresh)
- Multi-level cascade confirmed and tested (works to arbitrary depth)
- CTE passthrough support (passthrough CTEs become sub-IMV tables)

### Fixed
- Materialized views no longer cause "cannot have triggers" error
- Passthrough DELETE/UPDATE no longer does full table refresh

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
- PostgreSQL 13, 14, 15, 16, 17, 18
- Aggregates: SUM, COUNT, COUNT(*), AVG, MIN, MAX
- DISTINCT, GROUP BY, WHERE, INNER/LEFT/RIGHT JOIN
- Non-recursive CTEs (decomposed into sub-IMVs)
- Multi-level IMV cascading (A → B → C, tested up to 4 levels)
- Schema-qualified view names and source tables
