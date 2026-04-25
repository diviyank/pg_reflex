# Changelog

The full changelog tracks every release. The latest version's headlines are on the [home page](index.md).

For each version below, see [`CHANGELOG.md`](https://github.com/diviyank/pg_reflex/blob/main/CHANGELOG.md) on GitHub for the canonical text.

## [1.3.0] — 2026-04-25

**Performance**

- **Bounded top-K heap for MIN/MAX (audit R3)** — opt-in `topk` parameter on `create_reflex_ivm`. INSERT path keeps the K extremum values per group; DELETE path subtracts retracted values via the new `__reflex_array_subtract_multiset` plpgsql helper; heap underflow falls back to the existing scoped recompute. Closes the `stock_chart_*` cliff.

**Added**

- **Per-IMV flush histogram** — `flush_ms_history BIGINT[]` ring buffer (size 64) populated by `reflex_flush_deferred`. New SPI `reflex_ivm_histogram(view) → (p50_ms, p95_ms, p99_ms, max_ms, samples)`.
- **`pg_stat_statements` correlation** — each per-IMV flush body sets `application_name = 'reflex_flush:<view>'`.
- **Scalar MIN/MAX (no GROUP BY)** is now a tested supported shape.

**Tests** — 504 (up from 497).

## [1.2.1] — 2026-04-25

- `pg_reflex.alter_source_policy` GUC — `'warn'` or `'error'` (audit R2).
- `reflex_scheduled_reconcile(max_age_minutes)` SPI for pg_cron-driven drift scans (audit R7).
- Clearer info message when passthrough PK auto-detection finds a PK that isn't in the SELECT list (audit R5).

**Tests** — 497 (up from 487).

## [1.2.0] — 2026-04-24

- **Scoped MIN/MAX recompute** — restricts retraction scan to affected groups.
- **Operational safety** — per-IMV SAVEPOINT in cascade flush, auto-drop on source DROP (audit R1), warn on source ALTER, `reflex_rebuild_imv` alias.
- **Observability** — `last_flush_ms`, `last_flush_rows`, `flush_count`, `last_error` columns; `reflex_ivm_status`, `reflex_ivm_stats`, `reflex_explain_flush` SPIs.
- **Streaming statement-split** — `reflex_execute_separated` for the TRUNCATE trigger.
- **Bug fixes** — transitive cycle detection, 64-bit advisory-lock keys, silent-TEXT in `resolve_column_type`, reserved-CTE-alias collision, STRICT vs nullable `where_predicate`.

**Tests** — 487 (up from 481).

## [1.1.3] — 2026-04-22

- **Algebraic BOOL_OR** via two BIGINT counter columns.
- **Empty-affected DO-block gate** for group-by IMVs.
- `parallel_safe` annotation on `reflex_build_delta_sql` / `reflex_build_truncate_sql`.
- Staging-delta `ANALYZE` after TRUNCATE.
- Per-IMV `where_predicate` registry column.
- End-query targeted splice for `GROUP BY` end_queries (`COUNT(DISTINCT)` IMVs).
- 63-char identifier truncation fixes.
- MIN/MAX/BOOL_OR recompute scalar-subquery bug fix.
- Concurrent-flush advisory-lock collision fix.

## [1.1.1] — 2026-03-29

- FILTER clause support for SUM/COUNT/AVG/MIN/MAX/BOOL_OR.
- DISTINCT ON support via passthrough sub-IMV + ROW_NUMBER VIEW.
- DROP CASCADE.
- DROP VIEW/TABLE detection.
- Codebase split into focused modules.

## [1.0.4] — 2026-03-26

- Empty-delta early-exit.
- Predicate-filtered trigger skip.
- Persistent affected-groups table.
- Single-pass UPDATE MERGE.
- INTERSECT / EXCEPT support.

## [1.0.3] — 2026-03-26

- WINDOW function support.
- UNION ALL / UNION dedup support.
- `storage` parameter (LOGGED / UNLOGGED).
- `mode` parameter (IMMEDIATE / DEFERRED).
- Materialized view auto-refresh event trigger.
- Single-pass UPDATE MERGE.

## [1.0.2] — 2026-03-24

- UNLOGGED target tables.
- Hash index on intermediate group keys.
- MERGE RETURNING for affected-group capture.

## [1.0.1] — 2026-03-23

- BOOL_OR aggregate.
- Cast propagation through aggregates.
- HAVING clause support.
- Multi-level cascade.
- CTE passthrough support.
- Subquery warning.

## [1.0.0] — 2026-03-22

Initial release. SUM/COUNT/AVG/MIN/MAX/BOOL_OR aggregates, GROUP BY / WHERE / JOIN / HAVING / DISTINCT, non-recursive CTE, multi-level cascading, schema-qualified names, 138 tests.
