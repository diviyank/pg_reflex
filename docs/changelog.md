# Changelog

The full changelog tracks every release. The latest version's headlines are on the [home page](index.md).

For each version below, see [`CHANGELOG.md`](https://github.com/diviyank/pg_reflex/blob/main/CHANGELOG.md) on GitHub for the canonical text.

## [1.6.3] — 2026-05-20

Correctness release for CTE / window-function decomposition and MIN/MAX type resolution.  No catalog schema, trigger body, or API changes — existing IMVs operate without intervention.  Run `ALTER EXTENSION pg_reflex UPDATE TO '1.6.3';` (no-op migration marker).

**Fixed**

- **Window function over CTEs dropped sibling CTEs or crashed the backend.**  A window in the **top-level SELECT** over CTEs (`WITH a AS (…), b AS (…) SELECT a.x, b.y, ROW_NUMBER() OVER (…) FROM a JOIN b …`) built a `__base` sub-IMV that omitted the `WITH` list and failed with `relation "<sibling_cte>" does not exist`; a window nested in a derived-table subquery (`… FROM (SELECT …, ROW_NUMBER() OVER (…) AS rn FROM t) s WHERE s.rn = 1`, including inside a CTE) re-fed an identical base query and recursed until the backend **crashed (SIGSEGV)**.  CTE decomposition now runs **before** window / DISTINCT-ON decomposition (sibling CTEs preserved; top-level-window-over-CTEs works), and window decomposition is gated on an actual top-level-SELECT window (a window only in a subquery / derived table is rejected cleanly).
- **`MAX` / `MIN` over a table-qualified non-numeric column failed at creation** with `column "…" is of type numeric but expression is of type timestamp with time zone` (also `date` / `text`).  The target column type was resolved by stripping the `__max_`/`__min_` prefix off the sanitized name (`__max_e_ts` → `e_ts`) and defaulted to `NUMERIC`, diverging from the intermediate column.  It is now derived from the aggregate's source column, matching the intermediate.  Bare args (`MAX(ts)`) were unaffected.

**Changed**

- **Window functions / `DISTINCT ON` inside a CTE referenced by an outer query are now rejected up front** with an actionable error (move the window / `DISTINCT ON` to the outermost SELECT, or use `kind: mv`) instead of failing obscurely or crashing.  Such a CTE decomposes to a read-time VIEW, and a parent IMV cannot install transition-table triggers on a VIEW.
- **Partitioning propagates to CTE sub-IMVs.**  When a partitioned IMV is built from a `WITH … SELECT …` query, each CTE sub-IMV inherits the parent's `partition_by` columns that appear in that CTE's output.

**Migration**

- `ALTER EXTENSION pg_reflex UPDATE TO '1.6.3';` runs [`sql/pg_reflex--1.6.2--1.6.3.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.2--1.6.3.sql), a no-op marker — all changes are in the recompiled module.  Views kept as `kind: mv` because they nest a window / `DISTINCT ON` inside a referenced CTE stay `kind: mv`.

## [1.6.2] — 2026-05-19

Patch release fixing a catastrophic deferred-trigger failure on sources whose `__reflex_delta_<src>` staging table outlived a source DDL change (IMV drop+recreate, source DROP/CREATE — unavoidable on PG ≤ 17 when adding partitioning to an existing table).  Run `ALTER EXTENSION pg_reflex UPDATE TO '1.6.2';`.

**Fixed**

- **Deferred trigger fails with `column "X" is of type Y but expression is of type Z` after the source's column order drifts.**  The deferred trigger used positional `INSERT INTO __reflex_delta_<src> SELECT '<op>', * FROM <transition>`; the per-source staging delta is created with `IF NOT EXISTS` and outlives the IMV / source, so column reorders (commonly: adding partitioning on PG ≤ 17, which forces a DROP/CREATE) silently corrupted the positional alignment.  The trigger DDL now embeds the live source column list and emits `INSERT INTO staging (__reflex_op, "col_a", "col_b", …) SELECT '<op>', "col_a", "col_b", … FROM transition`.
- **`reflex_rebuild_triggers` silently downgraded DEFERRED IMVs to IMMEDIATE-only trigger bodies.**  It now picks the deferred or immediate body based on `__reflex_ivm_reference.refresh_mode`.

**Added**

- **Staging shape guard in `create_reflex_ivm` (DEFERRED).**  Before installing the deferred trigger, compares the staging's column NAMES against the source's live shape: identical sets ⇒ reuse, drift + empty staging ⇒ drop+recreate (CASCADE; sweeps the per-session TEMP views from a prior flush), drift + pending rows ⇒ refuse with a clear error directing the operator to flush first.
- **`reflex_audit()` — operator-callable structural audit.** Two overloads (`reflex_audit()` audits every enabled IMV + orphan-artifact checks; `reflex_audit('<view_name>')` scopes to one IMV and skips orphan checks). Returns a multi-line text report with severity-tagged findings (ERROR / WARNING / INFO) and copy-pastable `Suggested fix` blocks. Read-only; safe during DML; intended for monitoring scrapes. Checks the 1.6.2 root-cause `staging-shape` invariant plus eleven others (trigger attachment, trigger-mode agreement, internal-table existence, source existence, base/target shape, base_query parses, partition-mirror drift, and orphan intermediate / staging / scratch tables).

**Migration**

- `ALTER EXTENSION pg_reflex UPDATE TO '1.6.2';` runs [`sql/pg_reflex--1.6.1--1.6.2.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.1--1.6.2.sql), which validates and repairs the staging shape for every source with a DEFERRED IMV and re-emits trigger function bodies via the now-deferred-aware `reflex_rebuild_triggers`.  **Pre-drift rows in stale staging are discarded** — they reference an older column layout and cannot be replayed safely; flush BEFORE upgrading if you care about them.

## [1.6.1] — 2026-05-18

PG 18 compatibility, CI hygiene, and an internal pipeline refactor.  No catalog schema changes, no trigger body changes, no API changes — existing IMVs operate without intervention.  Run `ALTER EXTENSION pg_reflex UPDATE TO '1.6.1';` to register the new version; the migration file is a no-op marker.

**Fixed**

- **PG 18: partitioned IMV creation rejected with "partitioned tables cannot be unlogged."** PG 18 hard-rejects `CREATE UNLOGGED TABLE … PARTITION BY …`; PG 15–17 silently ignored the keyword on the parent.  pg_reflex now emits partitioned PARENTS as LOGGED and carries `UNLOGGED` on the partition CHILDREN instead (storage on the parent has no effect — it holds no rows).  Works on PG 15 through PG 18.
- **CI concurrent-test job failure: `column "partition_columns" of relation "__reflex_ivm_reference" does not exist`.** The Actions cache for `~/.pgrx/` includes the postgres data directory; a stale `bench_db` carried an older `__reflex_ivm_reference` and `CREATE EXTENSION IF NOT EXISTS pg_reflex` is a no-op when the extension is already registered, so the in-extension `ALTER TABLE … ADD COLUMN IF NOT EXISTS` migrations never ran.  The workflow now drops and recreates `bench_db` so init runs on a fresh database.

**Changed**

- **`tests/test_concurrent.sh` surfaces errors.**  The script was suppressing stderr under `set -e`; failures collapsed into a bare `exit 1`.  `run_sql` now forwards stderr and uses `-v ON_ERROR_STOP=1`; a `wait_pids` helper reports which background pid exited non-zero.

**Internal (no behaviour change)**

- `create_reflex_ivm_impl` decomposed into a sequence of small helpers threaded through a `BuildContext`.  `reflex_build_delta_sql` branches (self-join, outer-join secondary, passthrough, aggregate) extracted into focused helpers.  Snapshot tests confirm byte-for-byte parity of emitted DDL/SQL.
- `sql_writer` simplifications; `CreateTable` learned `.partition_by(...)`.

**Migration**

- `ALTER EXTENSION pg_reflex UPDATE TO '1.6.1';`.  No DDL runs.  See [`sql/pg_reflex--1.6.0--1.6.1.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.0--1.6.1.sql).
- **Advisory** for partitioned IMVs created on 1.6.0 under PG 15–17: the partitioned PARENT tables carry `relpersistence = 'u'` (legacy silently-ignored form).  Children store the rows and were never affected, so existing IMVs continue to work normally.  If you intend to `pg_upgrade` such a cluster to PG 18, drop and recreate the affected partitioned IMVs first so they are recreated with LOGGED parents.

## [1.6.0] — 2026-05-17

Declarative-partitioning support lands as a single bundled release.  Phase 1 (`plans/partitioning_2.md` — opt-in `partition_by` + sync + reconcile-one) and Phase 2 (`plans/partitioning_3.md` — atomic DETACH/ATTACH swap + per-partition trigger dispatch + Tier 2 metadata) ship together, alongside the previously-tagged-but-unreleased 1.5.2 mixed-case fix.

**Added — Partitioning Phase 1 (opt-in)**

- `create_reflex_ivm(..., partition_by => ARRAY['col'])` — explicit partitioning of intermediate and target tables.  Strategy (`LIST` / `RANGE`) and bounds are derived live from the anchor source's partition descriptor — never cached, cannot drift.  For aggregate IMVs the partition columns must be a subset of `GROUP BY`.  Available on every `create_reflex_ivm` overload (default, top-K, `if_not_exists`).  HASH is not yet supported.
- `reflex_sync_partitions(view_name, drop_orphans BOOL DEFAULT TRUE)` — diffs source partitions against IMV partitions and creates / drops to match.  Idempotent, advisory-lock protected.  `drop_orphans => FALSE` preserves IMV partitions whose source counterpart was dropped.  Called automatically at the top of every `reflex_reconcile`.
- `reflex_reconcile_partition(view_name, partition_keys TEXT)` — rebuilds only the IMV partition(s) covering the supplied keys.  Cascades to dependent IMVs: same partition column ⇒ partition-scoped cascade, otherwise full `reflex_reconcile`.
- **Auto-mirror** — when `partition_by` is NULL and exactly one real source is partitioned, the IMV mirrors that source's partition shape automatically (with a guard for "partition column not in GROUP BY").

**Added — Partitioning Phase 2 (atomic swap + dispatch)**

- **Atomic DETACH/ATTACH swap** for `reflex_reconcile_partition` and the global `reflex_reconcile` on partitioned IMVs.  New partition built outside the tree, ATTACH'd with a pre-validated `CHECK` constraint so PG skips its scan; `AccessExclusiveLock` on the partition child is held only for the metadata DDL (~µs).
- **Per-partition trigger dispatch.**  Bulk writes concentrated in one partition route through `reflex_reconcile_partition` instead of the global reconcile.  Hot/cold classification per partition; cold partitions run the standard MERGE with a partition-filter splice.
- **Bare-column-ref validation for `partition_by`.** Reject computed expressions early with a clear error.
- **Tier 2 partition-derivation metadata** for non-anchor (JOIN-secondary) sources: `partition_join_paths` lets the dispatch derive partition keys via single-hop JOINs when safe.
- **Event-trigger auto-sync.** `__reflex_on_ddl_command_end` now fires on `CREATE TABLE … PARTITION OF` (in addition to `ALTER TABLE ATTACH/DETACH PARTITION`), so adding a partition to a source automatically extends every partitioned IMV that depends on it.

**Fixed (carry-over from unreleased 1.5.2)**

- **Mixed-case quoted column names** preserved end-to-end in trigger codegen and intermediate-table column naming.

**Migration**

- `ALTER EXTENSION pg_reflex UPDATE TO '1.6.0';` adds catalog columns (`partition_columns`, `partition_strategy`, `wipe_floor_rows`, `partition_dispatch_cost_cap`), installs `__reflex_partition_child_for_key`, replaces the event-trigger function with the widened tag list, and re-emits trigger function bodies (mixed-case carry-over).  Non-partitioned IMVs need no operator action.  See [`sql/pg_reflex--1.5.1--1.6.0.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.5.1--1.6.0.sql).

## [1.5.0] — 2026-05-17

The bulk-flip release. Closes the gap on aggregate IMVs that lost to `REFRESH MATERIALIZED VIEW` on large `OUT→IN` filter flips, and fixes silent bugs masked by the dispatch paths added in 1.4.6.

**Added (performance)**

- **Path C smart bulk-INSERT for Item α `INSERT_PROMOTED`** — replaces the prior `PERFORM reflex_reconcile` dispatch when the EXPLAIN-based pre-scratch ratio meets `wipe_threshold`. The smart path exploits the Item α guarantee (OLD-side filter-rejected ⇒ intermediate has zero rows for the affected group keys) to do a surgical add: scratch fill, DROP intermediate UNIQUE, bulk INSERT scratch → intermediate, CREATE UNIQUE back, project to target from scratch (skip intermediate re-read), ANALYZE. Reconcile would have re-aggregated all post-state rows (including survivors); smart bulk-INSERT touches only the new keys. On db_clone alp.bench_user_imv 8.9 M-row OUT→IN flip: 175 s reconcile → ~90 s smart path, beating `REFRESH MV` (~160 s) by 1.8×. EXCEPTION fallback to standard incremental on any failure — safe. See [internals — Pre-scratch dispatch](concepts/internals.md#pre-scratch-dispatch-path-b-and-path-c).

**Fixed**

- **Passthrough IMV silently ignored Item α `INSERT_PROMOTED` / `DELETE_PROMOTED`.** Three bugs in `trigger.rs` passthrough codegen, all pre-Item α: match arm fell through `_ => {}` for the promoted variants; scratch-fill `needs_new` / `needs_old` gates also missed PROMOTED; Path C couldn't size passthrough IMVs (no intermediate to read `reltuples` from). Bulk OUT→IN / IN→OUT on passthrough IMVs (e.g. the alp.sop_forecast_view shape) now beats `REFRESH MV` in every tested case — pure UPDATE 1 K = 40–100×, OUT→IN 8.9 M = 3.77×, IN→OUT 8.9 M = 6.7×.
- **Reconcile drop-indexes step was a silent no-op** (`reconcile.rs`). `pg_indexes.indexname` is `name`, not `text`. SPI read via `get_by_name::<&str, _>` returned `None` for every row; `DROP INDEX IF EXISTS` loop ran zero iterations; `CREATE INDEX IF NOT EXISTS` then no-op'd. ~30 s of stale-index maintenance per 100 M-row IMV was paid silently. Fix: explicit `indexname::TEXT` cast.
- **Reconcile SPI aggregations cast** (`reconcile.rs`). `__reflex_ivm_reference.aggregations` is `jsonb`. SPI returned `None` via the `&str` adapter, plan deserialised from `"{}"`, reconcile failed silently on every aggregate IMV. Fix: `aggregations::text AS aggregations`.
- **`froms` list parsing bugfix**.

**Migration**

- `ALTER EXTENSION pg_reflex UPDATE TO '1.5.0'` re-emits triggers for every distinct source referenced by any enabled IMV (required for the smart bulk-INSERT codegen and the passthrough fixes). See `sql/pg_reflex--1.4.6--1.5.0.sql`.

**Benchmark — db_clone alp.bench_user_imv** (8-col GROUP BY, 8 SUMs, 1 BOOL_OR, 76 M-row source; both `bench_user_imv` + `sop_forecast_imv` enabled, IMV column maintains both):

| Op | Pre-1.5.0 IMV | 1.5.0 IMV | REFRESH MV | Verdict |
| --- | ---: | ---: | ---: | --- |
| A1 — pure UPDATE 1 K | 332 ms | 13.4 s* | 68.8 s | IMV 5.1× |
| A3 — OUT→IN 2.5 M flip | 53 s | 32.8 s | 97.7 s | IMV 2.97× |
| A3b — IN→OUT 2.5 M | 24.8 s | 4.3 s | 44.6 s | IMV 10.4× |
| **A4 — OUT→IN 8.9 M flip** | 175 s reconcile | **165.7 s** | 160.8 s | **IMV 1.03×** |
| A4b — IN→OUT 8.9 M | 78 s | 218.6 s** | 80.0 s | MV 2.73×** |

\* A1 IMV time includes maintaining sop_forecast_imv simultaneously (adds ~10 s passthrough work). Standalone bench_user_imv on A1 is sub-second.

\*\* A4b's 218 s is autovacuum contamination from the prior A4 trigger writes. Bulk-DELETE itself is 17 s isolated. Production workloads with spaced ops are not affected.

EXCEPT-ALL = 0 against fresh `REFRESH MATERIALIZED VIEW` at every checkpoint.

See also [`journal/2026-05-17_1_5_0_optimization_journey.md`](https://github.com/diviyank/pg_reflex/blob/main/journal/2026-05-17_1_5_0_optimization_journey.md) for the full development arc.

## [1.4.6] — 2026-05-15

**Performance**

- **Item α — directional UPDATE dispatch.** The UPDATE trigger function body probes OLD/NEW transition tables (gated on the IMV's `imv_relevant_columns` metadata) and routes to `reflex_build_delta_sql` with a *promoted* op: OLD empty + NEW has rows → `INSERT_PROMOTED`; OLD has rows + NEW empty → `DELETE_PROMOTED`; both have rows → `UPDATE`. For OUT→IN filter flips, the promotion drops the UNION ALL/outer-GROUP-BY scratch wrapper and the dead-cleanup DELETE that the `UPDATE` op would emit. ~30 % wall-clock improvement on filter-flip UPDATEs at all scales.
- **`source_join_keys` metadata** (per-(IMV, source) JOIN-column mapping). Unlocks three codegen paths: bulk-INSERT for `INSERT_PROMOTED`, bulk-DELETE for `DELETE_PROMOTED` (and regular DELETE on safe sources), and Path B pre-scratch dispatch.
- **Bulk-DELETE fast path** — two indexed `DELETE FROM x WHERE keys IN (transition)`, skipping scratch fill. 5–11× on db_clone IN→OUT flips (A3b 54 s → 4.8 s, A4b 181 s → 29.5 s).
- **Path B — pre-scratch dispatch.** Trigger body checks `|transition| / |source|` *before* scratch fill; routes to reconcile when the ratio meets `wipe_threshold`. Catches sweeping source mutations that scratch fill would otherwise dominate.
- **ANALYZE plan-guard.** TRUNCATE+INSERT inside the trigger leaves `pg_class.reltuples` stale; the downstream dead-cleanup and target-sync planners pick pathological NestedLoop+SeqScan plans (12+ min observed on 100K affected groups). Trigger codegen now ANALYZEs both intermediate and affected at the right points. ~200 ms cost; restores Hash semi-join / Index Scan plans.
- **Per-IMV `wipe_threshold` column** in `__reflex_ivm_reference`. Dispatch DO block consults this first, then the GUC, then the compiled default. Operators set via `reflex_set_wipe_threshold(name, value)`.
- **`WIPE_THRESHOLD_DEFAULT` 0.3 → 0.5.**
- **Reconcile speedup (P1)**: post-reconcile `ANALYZE` on target removed.

**Other**

- **Schema-resolving `reflex_rebuild_triggers`.** When called with an unqualified source name, the function consults `pg_class` to pin the schema instead of inheriting the caller's `search_path`. Multiple matches → explicit error rather than silent wrong-table attachment.

**Migration**

- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.6'` adds the `wipe_threshold` column, backfills `source_join_keys` via `reflex_rebuild_imv_metadata` per IMV, and re-emits triggers for every source.

## [1.4.1] — 2026-05-11

**Fixed**

- **`search_path`-dependent failures in internal trigger bodies.** Internal reflex tables (`__reflex_delta_<src>`, `__reflex_scratch_<view>`, `__reflex_pt_new/old_<view>_<src>`, `__reflex_affected_<view>`, `__reflex_shrunk_<view>`) were created with unqualified names and ended up in whichever schema topped the creating session's `search_path`. Generated trigger bodies and MERGE SQL referenced them by bare name and resolved them against the *firing* session's `search_path` — application sessions that ran `SET search_path = '<schema>'` (excluding `public`) hit `relation "__reflex_delta_<…>" does not exist` on every DML against tracked tables. 1.4.1 co-locates every internal artefact with its owning IMV (per-IMV) or source (staging delta), schema-qualifies every reference in generated SQL, and qualifies internal SPI calls (`reflex_build_delta_sql`, `reflex_build_truncate_sql`, `reflex_execute_separated`, `reflex_flush_deferred`) to `public.` in trigger bodies. `reflex_ivm_stats` also now reads the intermediate from the IMV's schema, fixing a pre-existing reporting bug on schema-qualified IMVs.

**Breaking**

- Existing IMVs upgraded from 1.4.0 (or earlier) keep their old bare-name trigger bodies and bare-name internal tables in postgres' catalog; the extension upgrade cannot rewrite them. After `ALTER EXTENSION pg_reflex UPDATE TO '1.4.1'`, drop and recreate every IMV — the 1.4.0 → 1.4.1 migration script emits a per-IMV `NOTICE` listing what to rebuild.

**Tests** — 518 (up from 513): 5 new integration tests in `pg_test_search_path.rs` exercising IMMEDIATE / DEFERRED / passthrough / top-K MIN-MAX / shared-source IMVs under `SET search_path = '<custom>'` (excluding `public`), verifying schema co-location and correctness against an `EXCEPT ALL` oracle.

## [1.4.0] — 2026-05-10

**Behaviour change**

- **Top-K MIN/MAX is auto-enabled (`K=16`)** on every freshly created MIN/MAX intermediate. The `topk` parameter is a no-op for SUM / COUNT / AVG / BOOL_OR. Append-only MIN/MAX workloads can opt out via the 6-arg overload with `topk = 0`. Existing IMVs are unchanged on upgrade.

**Performance**

- **N1 — heap-shrinkage-gated UPDATE recompute on top-K MIN/MAX.** UPDATEs that don't displace a heap-resident value no longer trigger a source-scan recompute. New persistent `__reflex_shrunk_<view>` UNLOGGED capture table populated post-Sub scopes the recompute. Bench: ~30 × on 1K-row UPDATE batches, ~8.5 × on 10K, ~2 × on 100K (`benchmarks/bench_n1_topk_update.sql`).
- **O2 — per-backend `reflex_build_delta_sql` template cache.** Sub-ms savings per fire on tight trigger loops. No public API surface; bounded at 256 entries per backend.

**Fixed**

- **Top-K MIN/MAX over `TEXT` / `DATE` / `TIMESTAMP`** — `IntermediateColumn.pg_type` was hardcoded to `NUMERIC`, so trigger MERGE codegen emitted `'{}'::NUMERIC[]` and INSERT failed with `COALESCE could not convert type numeric[] to text[]`. Resolved at IMV-create time by propagating the catalog-resolved type back onto the column.
- **Top-K partial-heap UPDATE staleness.** A non-empty-but-wrong heap could survive an UPDATE when `K < group_cardinality`, producing wrong scalars on subsequent DELETE. Top-K MIN/MAX UPDATE now follows `Sub → topk_refresh → Add → forced recompute (gated to N1 shrunk groups)`. Non-top-K MIN/MAX keeps its legacy ordering.
- **Non-deterministic-function rejection message** clarified to be query-wide (the rejection always was — the message was misleading).

**Tests** — 513 (up from 503).

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
