# Changelog

The full changelog tracks every release. The latest version's headlines are on the [home page](index.md).

For each version below, see [`CHANGELOG.md`](https://github.com/diviyank/pg_reflex/blob/main/CHANGELOG.md) on GitHub for the canonical text.

## [1.10.4] — 2026-06-11

Two follow-up fixes to 1.10.x: the partition attach/detach no-op skip now also covers DETACH-then-DROP, and `drop_reflex_ivm` no longer leaks the per-source DEFERRED staging delta table. Replace the `.so`, then `ALTER EXTENSION pg_reflex UPDATE TO '1.10.4';`.

**Fixed**

- DETACH-then-DROP of an irrelevant partition force-reconciled dependent unpartitioned IMVs. The 1.10.3 no-op skip works by probing the partition child's rows against the IMV `WHERE` filter, but the partition flush is a DEFERRED trigger firing at COMMIT — so detaching **and** dropping a partition in the *same transaction* (the common migration-tool pattern) leaves no child to probe, and the IMV fell back to a full `reflex_reconcile` + downstream cascade. On `base-db-anchor-evm`, dropping a non-`sop_current_view` assortment partition rebuilt the whole `current_assortment_activity_view` ~20-IMV subtree. Performance only — the reconcile was correct. The fix proves irrelevance from the partition's captured `LIST` bound (now stored in `__reflex_source_partition_snapshot.bound`) via the new `reflex_partition_drop_maybe_skip` SPI: sound by construction, since the probe exposes only the partition key column and every inconclusive case (non-key predicate, `RANGE`/`HASH`, multi-key, no filter) falls back to `reflex_reconcile`.
- `drop_reflex_ivm` leaked the per-source DEFERRED staging delta table (`__reflex_delta_<source>`) — flagged by `reflex_audit`'s `OrphanStaging` check. It is now dropped when the last DEFERRED IMV on a source is dropped (`IF EXISTS`, so a no-op for IMMEDIATE IMVs). Pre-1.10.4 orphans heal at create time.

**Migration**

- Both fixes recompile into the module. The DETACH-then-DROP fix adds one new SQL function: [`sql/pg_reflex--1.10.3--1.10.4.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.3--1.10.4.sql) installs `reflex_partition_drop_maybe_skip`. The `drop_reflex_ivm` staging fix is pure Rust — no migration DDL. No IMV rebuild required; existing partitioned-source snapshots gain bounds on their next flush (the *first* detach-drop after upgrade still reconciles, then self-heals).

## [1.10.3] — 2026-06-11

Two independent changes: a correctness + performance fix for IMVs whose FROM clause contains a `UNION ALL` subquery, and a new incremental partition-delta path so partition attach/detach no longer full-rebuilds dependent unpartitioned IMVs. Replace the `.so`, then `ALTER EXTENSION pg_reflex UPDATE TO '1.10.3';`.

**Added**

- Incremental partition delta for unpartitioned IMVs — attaching/detaching a partition on a `LIST`/`RANGE`-partitioned source previously forced a full `reflex_reconcile` (TRUNCATE + rebuild) of every dependent unpartitioned IMV, detonating the whole downstream cascade even when no kept rows changed. The new `reflex_apply_partition_delta` SPI feeds the partition child through the same incremental INSERT/DELETE pipeline (`reflex_build_delta_sql`): a `where_predicate` pred-check skips a filtered-out partition in O(1), Path B falls back to reconcile for large bulk changes, and any unsupported shape falls back to `reflex_reconcile`. Propagation is write-driven, so a net-zero delta dies at whatever depth it nets to zero — attaching a non-current LIST assortment to a filtered IMV now skips its ~20-IMV downstream subtree entirely.

**Fixed**

- `UNION ALL`-subquery aggregates double-counted unchanged operands — a mutation to one operand was maintained as if the whole subquery were the delta, re-counting the unchanged sibling operands (silent wrong `SUM` in overlapping groups) and full-scanning the base (O(base); a 1-row delta took ~18 min on production `sop_incoming_stock_baseline_view`). The delta query now prunes the subquery to only operands referencing the changed source before the transition swap — correct and O(delta). Passthrough over a `UNION ALL` subquery (duplicated sibling rows) and non-distributive set-ops (`UNION`/`INTERSECT`/`EXCEPT`, now a correct full recompute) are fixed by the same scoping.

**Migration**

- The `UNION ALL` fix is pure Rust codegen — existing IMVs are fixed automatically once the recompiled module is loaded. The partition-delta optimization adds one new SQL function: [`sql/pg_reflex--1.10.2--1.10.3.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.2--1.10.3.sql) installs `reflex_apply_partition_delta` on existing databases. No IMV rebuild or refresh required.

## [1.10.2] — 2026-06-10

Correctness + efficiency fix for IMVs filtered by an uncorrelated scalar subquery (e.g. `WHERE assortment_id = (SELECT assortment_id FROM sop_current_view)`). The filter was dropped from per-source metadata, so the relevance-skip never fired — irrelevant updates were maintained anyway, and an out-of-filter update colliding on the unique key silently deleted in-filter rows. Pure Rust analyzer fix, recompiled into the module: `ALTER EXTENSION pg_reflex UPDATE TO '1.10.2';`.

**Fixed**

- Scalar-subquery WHERE filters were dropped from per-source metadata — `collect_imv_relevant_where` attributed conjuncts against every relation including the subquery's (also a registered source), so `col = (SELECT … FROM other)` looked cross-source and was discarded, leaving `imv_relevant_where` empty and the relevance-skip off (needless maintenance + silent wrong-deletes). The analyzer now attributes against the outer FROM's sources only and treats scalar subqueries as opaque, so single-outer-source IMVs with a subquery filter capture the predicate. Multi-source IMVs unchanged.

**Migration**

- [`sql/pg_reflex--1.10.1--1.10.2.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.1--1.10.2.sql) — no-op marker. Per-source metadata is persisted at CREATE time, so **existing** IMVs with a scalar-subquery filter need an in-place refresh (no DROP/recreate): after the upgrade run `SELECT reflex_rebuild_imv_metadata('<schema.imv>');` for each. New IMVs get it automatically.

## [1.10.1] — 2026-06-10

Performance fix for the incremental maintenance of aggregate IMVs that `LEFT JOIN` a secondary table: a tiny change to the secondary re-aggregated the entire base instead of the few affected groups, so a single-row source update propagating through such an IMV took minutes (18 min for a 2-row delta on a 9-source view). Pure Rust codegen fix, recompiled into the module: `ALTER EXTENSION pg_reflex UPDATE TO '1.10.1';`.

**Fixed**

- Aggregate `LEFT JOIN`-secondary updates re-aggregated the whole base — `outer_join_secondary_stmts` built its affected-groups set by re-running the full aggregation with the secondary swapped for its transition table, but the outer join preserves every primary row so the affected set was *all* groups (then recomputed a second time). With a `source_join_keys` mapping the recompute is now scoped by `(group_cols) IN (changed keys from OLD∪NEW)`, pushed below the aggregation into the indexed base scan (18 min → ~50 ms). Falls back to the broad recompute when no mapping is available.

**Internal**

- The weak-stub archive is now built and force-loaded (`+whole-archive`) on macOS too, so `cargo test` / `cargo pgrx test` run locally on mac (previously aborted with `dyld: … '_CacheMemoryContext'`). The cdylib is unaffected (`cfg(test)`-scoped).

**Migration**

- [`sql/pg_reflex--1.10.0--1.10.1.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.0--1.10.1.sql) — no-op marker; the fix ships in the recompiled module. Replace the `.so`, then `ALTER EXTENSION pg_reflex UPDATE TO '1.10.1';`.

## [1.10.0] — 2026-06-10

Fixes the hole where `ALTER TABLE source ATTACH PARTITION child_with_data` created the IMV partition but never synced its data (it stayed empty): ATTACH re-parents rows without firing row triggers, and nothing drained the partition pending queue automatically. `ALTER EXTENSION pg_reflex UPDATE TO '1.10.0';` after replacing the module.

**Fixed**

- ATTACH-with-data left the IMV partition empty — the event trigger enqueued the source root and created the empty structure, but no path ever drained the queue to run the data-filling reconcile.
- One broken root wedged the whole partition flush — `reflex_flush_partitions` drained all roots in one transaction with `?`-propagation, so the first failure rolled back the batch and drained nothing; each root now reconciles/drains atomically in its own subtransaction, failing roots stay pending with a `WARNING`.
- Shape drift threw `"… is not partitioned"` — a same-name source child rebuilt partitioned left the IMV child a plain table (skipped by `CREATE … IF NOT EXISTS`); `reflex_sync_partitions` now drops and rebuilds the mismatched child with the correct shape.

**Added**

- Commit-time auto-drain: a `DEFERRABLE INITIALLY DEFERRED` constraint trigger on `__reflex_partition_pending` runs a scoped flush per enqueued root at COMMIT, so ATTACH-with-data auto-syncs with no manual `reflex_flush_partitions()` call.
- Internal `__reflex_refresh_partition_snapshot(text)` wrapper so the per-root flush refreshes the snapshot atomically (not public API).

**Migration**

- [`sql/pg_reflex--1.9.2--1.10.0.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.9.2--1.10.0.sql) registers the new wrapper and installs the auto-drain trigger (pure DDL). The two Rust fixes ship in the recompiled module and need no DDL. Clear any pre-1.10.0 backlog by running `SELECT reflex_flush_partitions();` **after** the upgrade as its own statement — it cannot run inside the `ALTER EXTENSION` script (extension-creation mode rejects DDL on the runtime partition tables, SQLSTATE 55000).

## [1.9.2] — 2026-06-08

Correctness fix for the commit-time cascade flush of CTE-decomposed views. No catalog/schema or function-signature changes — the fix is in the Rust trigger-time SQL rewriter, recompiled into the module: `ALTER EXTENSION pg_reflex UPDATE TO '1.9.2';`.

**Fixed**

- `zero-length delimited identifier` (SQLSTATE 42601) at COMMIT for DEFERRED CTE-decomposed views: a CTE whose inner body is a passthrough feeding an outer aggregate decomposes into a passthrough sub-IMV (`s.v__cte_base`) and an aggregate parent (`s.v`); the cascade flushed the parent with its source **unquoted** while its `base_query` referenced it **quoted** (`"s"."v__cte_base"`). `replace_source_with_transition` then replaced the bare name inside the existing quotes, emitting `"s".""__reflex_old_…""` and aborting the transaction at commit (or, where swallowed as a warning, silently leaving the parent stale). The fix rewrites the source's quoted spellings first; it generalizes to any schema-qualified source quoted in `base_query`, not just CTE sub-IMVs.

**Tests**

- Unit regressions for `replace_source_with_transition` (quoted schema-qualified and quoted-unqualified sources) plus a `#[pg_test]` that drives a real decomposed passthrough→aggregate `base_query` through the parent's delta builder with the unquoted cascade source and asserts no `""` is emitted.

**Migration**

`ALTER EXTENSION pg_reflex UPDATE TO '1.9.2';` after replacing the module. No DDL runs; the corrected rewriter applies to the next flush of every affected IMV. [`sql/pg_reflex--1.9.1--1.9.2.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.9.1--1.9.2.sql)

## [1.9.1] — 2026-06-05

Performance release for partitioned passthrough maintenance. No catalog/schema or function-signature changes — all changes ship in the recompiled module: `ALTER EXTENSION pg_reflex UPDATE TO '1.9.1';`. The one new knob, the `reflex.assert_inplace_update` GUC, is registered in `_PG_init`, not via catalog DDL.

**Added**

- In-place upsert for the partitioned passthrough UPDATE cold path: a pure-data UPDATE is applied via `INSERT … ON CONFLICT (<key>) DO UPDATE` on the existing `__reflex_uk_` index plus a keyed delete-gone for rows that disappeared or left the query's `WHERE` filter, instead of a full DELETE + recompute INSERT. ~3–4.5× faster flush on a 33.7M-row, 837-leaf passthrough IMV (~11.2 s → ~2.5–3.9 s for 100–110k pure-data rows). Falls back to DELETE+INSERT for non-partitioned, keyless, or non-UPDATE cases.
- `reflex.assert_inplace_update` GUC (boolean, default `off`): re-derives the affected key set after the in-place path and raises on any divergence from a fresh recompute — a runtime correctness self-check for CI/fuzz and canary rollout.

**Changed**

- LIST partition-key pruning of the cold passthrough DELETE/INSERT: the cold body constrains the partition key to the touched cold partitions so the planner prunes to the affected leaves (cold keyed-delete planning ~110 ms → ~7 ms, execution ~3.7× on the 837-leaf benchmark). Semantic no-op; LIST only.

**Migration**

`ALTER EXTENSION pg_reflex UPDATE TO '1.9.1';` after replacing the module. No DDL runs; the new cold-UPDATE codegen applies to the next flush of every existing partitioned passthrough IMV.

## [1.9.0] — 2026-06-04

Performance + correctness release for passthrough maintenance: partitioned passthrough/aggregate IMVs dispatch DML only to affected child partitions, passthrough LEFT-JOIN secondaries are maintained with a keyed delete + delta insert instead of a full rebuild, and inner CTE sub-IMVs (and single-source passthrough IMVs generally) now detect a source PRIMARY KEY and maintain incrementally instead of full-rebuilding every flush. No catalog/schema or function-signature changes — all changes ship in the recompiled module: `ALTER EXTENSION pg_reflex UPDATE TO '1.9.0';`.

**Added**

- Keyed incremental maintenance for passthrough LEFT-JOIN secondaries (audit #3); FULL OUTER secondaries keep the correct full-rebuild fallback.
- Auto-indexing of passthrough secondary join keys (coverage-checked).

**Performance**

- Partition-aware trigger dispatch (LIST + RANGE): a DML batch touches only the affected ("hot") child partitions instead of re-scanning the mirrored tree.
- Child resolution during dispatch is now O(partitions), not O(rows).

**Fixed**

- Inner CTE sub-IMVs (and single-source passthrough IMVs) now maintain incrementally: the source-PK catalog lookup read `attname` (type `name`) into a `text[]` binding and silently swallowed the type mismatch, so no key was ever detected and the IMV full-rebuilt every flush; it now casts `attname::text`. Sources with no provable key keep the correct keyless full-rebuild fallback.
- Case preserved in the secondary-key auto-index for mixed-case columns.

**Tests**

- Oracle coverage for keyed passthrough secondaries (IMMEDIATE + DEFERRED), LIST/RANGE hot-cold dispatch, and keyed inner CTE maintenance (catalog key, IMMEDIATE correctness incl. filter transitions, O(K) DEFERRED delta, keyless fallback).

**Migration**

- No-op marker: [`sql/pg_reflex--1.8.2--1.9.0.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.8.2--1.9.0.sql) — no DDL; replace the `.so`, then `ALTER EXTENSION pg_reflex UPDATE TO '1.9.0';`.

## [1.8.2] — 2026-06-03

Partition depth is decoupled from the source's: a partitioned IMV can mirror its source at a shallower depth, all the way down to unpartitioned. Explicit `partition_by` is now authoritative for the IMV's depth; omitting it auto-mirrors the leading levels that have a bare projected column and prunes the rest. An empty `partition_by => ARRAY[]::text[]` forces an unpartitioned target on a partitioned source. Additive, non-breaking migration: `ALTER EXTENSION pg_reflex UPDATE TO '1.8.2';`.

**Added**

- Shallow partition mirroring — explicit `partition_by` declares the IMV's depth; auto-mirror prunes to the deepest bare-projected level. New nullable catalog column `partition_depth INT` (NULL = full source depth).
- Unpartitioned IMV on a partitioned source via `partition_by => ARRAY[]::text[]`.

**Fixed**

- `reflex_sync_partitions`, `reflex_reconcile_partition`, `reflex_flush_*`, and the audit partition-tree-drift check are depth-aware: a shallow IMV is never re-deepened by a sync, and a source leaf change reconciles up to the IMV's mirror-depth partition.
- Unpartitioned IMVs on a partitioned source no longer go silently stale on a source partition swap — the flush full-reconciles them.

**Migration**

- [`sql/pg_reflex--1.8.1--1.8.2.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.8.1--1.8.2.sql) — additive/non-breaking: adds nullable `__reflex_ivm_reference.partition_depth` and `__reflex_source_partition_snapshot.ancestors TEXT[]` columns and redefines the `ddl_command_end` event trigger. `ALTER EXTENSION pg_reflex UPDATE TO '1.8.2';`.

## [1.7.6] — 2026-06-01

Correctness release: `ignore_sources` is now honored on the DEFERRED trigger path (previously only IMMEDIATE). Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.6';` and replace the `.so`; the migration rebuilds source triggers so the fix applies without re-creating IMVs.

**Fixed**

- **`ignore_sources` silently ignored on the DEFERRED path.** The skip guard existed only in the IMMEDIATE trigger body; the three deferred trigger bodies and `reflex_flush_deferred` never checked `ignored_sources`. When a source's trigger was the *deferred flavour* (some sibling IMV on it is DEFERRED), an IMV that ignored that source was maintained anyway — both inline (IMMEDIATE IMVs in the deferred trigger) and at flush (DEFERRED IMVs). Deferred bodies now emit the same skip guard, and `reflex_flush_deferred` excludes IMVs whose `ignored_sources` overlaps the (qualified, bare) source name.

**Testing**

- 1120 tests pass; `clippy` + `fmt` clean. New: `pg_test_deferred_ignore_sources_skips_imv`.

## [1.7.5] — 2026-05-31

Feature release: widened CTE/JOIN passthrough unique-key inference so chained-CTE cascades (e.g. `forecast_analysis_view`) auto-resolve sound unique keys and get incremental DELETE/UPDATE instead of full refresh. Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.5';` and replace the `.so`. One additive catalog column (`max_one_row`), no backfill.

**Added**

- **Sound unique-key inference across JOINs and chained CTEs.** Equi-join equivalence in projected-key matching, aggregate-IMV GROUP BY keys registered as sound keys, CROSS-JOIN-to-ungrouped-aggregate classified to-one, and `__reflex_uk_*` index detection in the anchor probe. New `__reflex_ivm_reference.max_one_row` flag (default FALSE).

**Fixed**

- Dropped an unsound `LIKE` wildcard in registry lookups.

**Testing**

- forecast-shape unique-key cascade integration test + cross-join/chained-CTE coverage.

## [1.7.4] — 2026-05-31

Correctness release for partitioned IMV creation. Compiled-only fix (no catalog or SQL-signature change). Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.4';` and replace the `.so`.

**Fixed**

- **Partition-anchor resolution accepts sources co-partitioned on the join key, and ignores sources partitioned on a different column.** Extends the 1.7.3 anchor fix: (1) a candidate anchor must be partitioned *on the partition column itself* (new `source_partitioned_on` helper), so a source partitioned on an unrelated column is no longer a candidate; (2) several sources co-partitioned on the *same* column are no longer "ambiguous" — when the JOIN key is the partition column their layouts align, so any is a sound anchor. This covers the case where every owner is a reflex intermediate with no base table — the `forecast_analysis_view` shape (`…__cte_forecast_sales FULL JOIN …__cte_history_sales ON dem_plan_id` → two partitioned `__cte_` owners, zero base). Base owners are still preferred; otherwise the anchor is chosen deterministically, and non-anchor co-owners fall through to Path B. The error fires only when no source is partitioned on the column.

**Testing**

- 1111 tests pass; `clippy` + `fmt` clean. New: `pg_part_copartitioned_full_join_of_cte_intermediates`.

## [1.7.3] — 2026-05-31

Correctness release for IMV creation. Two compiled-only fixes (no catalog or SQL-signature change). Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.3';` and replace the `.so`.

**Fixed**

- **Failed creation of a decomposed IMV no longer orphans its sub-IMVs.** Creation rejections return as `"ERROR…"` strings, so the function returns normally and the transaction is not aborted — a CTE/`UNION ALL` query that materialises several sub-IMVs and is then soft-rejected on a later operand/CTE or the outer body used to leave the already-created sub-IMVs behind. Every soft-reject path in `try_decompose_ctes` and `try_decompose_set_op` now rolls back the sub-IMVs it created (cascade, reverse order). Hard failures were already covered by transaction abort.
- **Partition-anchor resolution prefers the base source over derived intermediates.** When a decomposed query has two partitioned owners of the partition column (a base partitioned table and a partition-inheriting `__cte_`/`__union_`/`__base` sub-IMV), `resolve_anchor_source` no longer errors `multiple sources own partition column … ambiguous`; it picks the sole base partitioned owner — the table whose partition children are mirrored — and errors only on genuine ambiguity.

**Testing**

- 1108 tests pass; `clippy` + `fmt` clean. New: `test_cte_decomposition_failure_rolls_back_sub_imvs`, `test_set_op_decomposition_failure_rolls_back_sub_imvs`, `pg_part_anchor_prefers_base_over_cte_intermediate`.

## [1.7.2] — 2026-05-31

Correctness release fixing `drop_reflex_ivm`, which silently orphaned the target + auxiliary tables of any IMV created with a bare (unqualified) name under a non-`public` `search_path`. All teardown DDL derived its relation names from the stored bare `name` and resolved the target via `to_regclass(name)`, both honouring the session `search_path` *at drop time* — so an IMV whose objects landed in `alp` (created while `search_path = alp`) was torn down with unqualified `DROP TABLE IF EXISTS …` that resolved against the wrong schema, deleted only the catalog row, and left the table + `__reflex_intermediate_*` / `__reflex_affected_*` / `__reflex_uk_*` behind. A same-named decoy relation of a different relkind in the path (e.g. a materialized view) could be hit instead (`ERROR: "<name>" is not a table`). Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.2';`.

**Fixed**

- **`drop_reflex_ivm` orphaned bare-name IMVs created under a non-`public` `search_path`.** Creation now records the object schema (`current_schema()` for bare names, the explicit schema for qualified names) in a new nullable catalog column `__reflex_ivm_reference.target_schema`, and `drop_reflex_ivm` re-qualifies the relkind probe + target DROP + every aux-table DROP with it — making teardown independent of the session `search_path`. Legacy rows with a NULL `target_schema` fall back to the prior `search_path` resolution.

**Testing**

- New regression `test_drop_resolves_creation_schema_for_bare_name` in `src/tests/pg_test_drop.rs` (create bare-name IMV under `search_path = drop_sch`, drop under `search_path = public`, assert target + intermediate are removed from `drop_sch`). Full suite: 1105 tests pass.

**Migration**

`ALTER EXTENSION pg_reflex UPDATE TO '1.7.2';` runs [`sql/pg_reflex--1.7.1--1.7.2.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.7.1--1.7.2.sql) which adds the nullable `target_schema` column. No data backfill — existing rows keep NULL and use the legacy fallback; IMVs created after the upgrade get search_path-independent teardown automatically. No IMV needs to be dropped or recreated.

## [1.7.1] — 2026-05-31

Correctness release fixing Path C (the INSERT_PROMOTED smart bulk-INSERT dispatch fired only by UPDATE triggers). Two compounding defects: derived relation names were re-built by raw `split_part` + string concat that bypassed the canonical `safe_identifier` hash (manifesting as `ERROR: relation "<…>" does not exist` for bare-name and long-name IMVs, surfaced as a `WARNING: pg_reflex Path C smart bulk-INSERT failed for <imv>` log line), and the bulk-INSERT entry gate did not match the Rust-side `aggregate_insert_stmts` safety check (silently duplicate-counted group rows for single-source aggregates). Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.1';` — the migration registers three new SQL-callable name helpers and automatically calls `reflex_rebuild_triggers` for every distinct source in `__reflex_ivm_reference.depends_on`, so existing IMVs pick up the fix without operator intervention. No catalog schema changes; no IMV needs to be dropped or recreated.

**Fixed**

- **`ERROR: relation "<…>" does not exist` during UPDATE** for default-schema IMVs and for any IMV whose `__reflex_intermediate_<bare>` crosses PG's 63-char NAMEDATALEN. Path C now resolves intermediate / scratch / target names through three new SQL-callable wrappers (`reflex_intermediate_table_name`, `reflex_delta_scratch_table_name`, `reflex_quote_identifier`) that route through the same `split_qualified_name` + `safe_identifier` helpers every other call site uses. The unique-index lookup is rewritten to join via `to_regclass(...)::regclass` + `pg_index.indisunique` instead of `pg_indexes.indexdef ILIKE '%UNIQUE%'` (which false-positived on comments / column names).
- **Silent double-counting in Path C for single-source aggregates.** `reflex_build_path_c_explain_sql` now returns the empty string when the plan has no `source_join_keys` entry for the trigger source — matching the Rust-side `aggregate_insert_stmts` gate — so Path C falls through to the standard MERGE path for shapes where bulk-INSERT cannot prove that the affected group keys are absent from the intermediate.

**Testing**

- Two new regression locks in `src/tests/unit_trigger.rs` assert Path C never re-introduces `split_part(_rec.name` or raw `'__reflex_*_' || …` concat. Full suite: 1104 tests pass.

**Migration**

`ALTER EXTENSION pg_reflex UPDATE TO '1.7.1';` runs [`sql/pg_reflex--1.7.0--1.7.1.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.7.0--1.7.1.sql) which registers the three new SQL-callable name-helper functions. To pick up the new Path C body on every existing trigger, run the rebuild loop printed in that file's header comment **after** the ALTER EXTENSION completes, in a normal SQL session — the per-source trigger functions were created outside any extension-creation context, so PG's `creating_extension`-mode safety check refuses to `CREATE OR REPLACE` them mid-upgrade with `"function … is not a member of extension \"pg_reflex\""`. Running the loop separately sidesteps the check. View / matview sources (`relation … cannot have triggers`) are expected to be skipped — those don't have pg_reflex triggers and are maintained via cascade. No IMV needs to be dropped or recreated.

## [1.7.0] — 2026-05-28

Refactor + correctness release for intermediate `UNION ALL` CTE-body wrappers. The inline wrapper construction in `try_decompose_set_op` is centralised into one helper; the wrapper table gains a `__reflex_src_idx SMALLINT NOT NULL` discriminator column that fixes a cross-operand `DELETE` over-delete; non-`ALL` set ops used as CTE bodies are now rejected at create time with an actionable error; and `drop_reflex_ivm` cascade no longer leaks `__reflex_union_mirror_*` trigger functions in `pg_proc`. No catalog schema changes, no trigger body changes, no API changes. Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.0';` (no-op migration marker). **Existing UNION-ALL CTE IMVs created under ≤1.6.5 must be dropped and recreated** to pick up the cross-operand `DELETE` fix.

**Fixed**

- **Cross-operand `DELETE` over-delete in intermediate `UNION ALL` CTE wrappers.** A `DELETE` from operand A would also remove operand B's wrapper row when both projected the same column values, because the mirror `DELETE` matched by all-column `IS NOT DISTINCT FROM` with no operand-identity filter. The wrapper now carries `__reflex_src_idx SMALLINT NOT NULL` and the `DELETE` predicate scopes to `__reflex_src_idx = <operand_idx> AND …`. *Create-time fix — recreate any UNION-ALL CTE IMV built under ≤1.6.5.*
- **`__reflex_union_mirror_*` trigger functions orphaned in `pg_proc` after drop cascade.** Operand-sub-IMV cascade dropped the triggers but not their plpgsql functions. `drop_reflex_ivm` now detects UNION-ALL wrappers via the `__union_<i>` suffix on `depends_on_imv` and issues `DROP FUNCTION IF EXISTS … CASCADE` per operand. *Drop-time fix — applies to any UNION-ALL IMV dropped under 1.7.0.*

**Changed**

- **`UNION` / `INTERSECT` / `EXCEPT` (without `ALL`) used as a CTE body are now rejected at create time** with an actionable error message (workarounds: hoist to outermost SELECT, use `kind: mv`, or rewrite as `UNION ALL` if operands are disjoint). Previously emitted a broken VIEW that failed deep in the consumer's trigger install. Outer-level (top of SELECT) usage is unchanged. *Create-time validation.*
- **Wrapper construction is centralised** in a new private helper `install_union_all_intermediate_wrapper`. `try_decompose_set_op` no longer carries inline `CREATE UNLOGGED TABLE` + per-operand trigger install + registry insert. The now-dead helper `query_table_column_names` is removed.

**Testing**

- Six regression tests added in `pg_test_drop.rs` and `pg_test_error.rs`: wrapper `__reflex_src_idx` column presence, cross-operand DELETE isolation, mirror-function cleanup on drop, and three reject tests for `UNION` / `INTERSECT` / `EXCEPT` as CTE body. Full suite: 1102 tests pass.

**Migration**

No DDL required. UNION-ALL CTE IMVs created under ≤1.6.5 are NOT auto-migrated: their wrapper table lacks `__reflex_src_idx` and the cross-operand DELETE fix won't reach them. Drop and recreate: `SELECT drop_reflex_ivm('<name>', TRUE); SELECT create_reflex_ivm('<name>', '<SELECT …>', …);` The full CHANGELOG entry includes a one-shot `DO $do$ … END $do$` block to clear pre-existing `__reflex_union_mirror_*` orphans from `pg_proc` after the upgrade.

## [1.6.5] — 2026-05-26

Correctness release fixing three independent create-time defects hit while migrating real views (CTE-decomposed, `DEFERRED`, materialized-view-sourced) to IMVs.  No catalog schema, trigger body, or API changes.  Run `ALTER EXTENSION pg_reflex UPDATE TO '1.6.5';` (no-op migration marker).  All three are create-time fixes; an IMV already created successfully is unaffected.

**Fixed**

- **`DEFERRED` IMV over a CTE failed at creation with `zero-length delimited identifier at or near ""`.**  A CTE-decomposed sub-IMV is referenced in already-quoted form (`"schema"."view__cte_x"`); in `DEFERRED` mode the staging-delta name builder re-quoted the already-quoted schema, emitting `""schema""`.  The schema component is now unquoted before re-quoting.  `IMMEDIATE` mode was unaffected.  *Create-time fix — unblocks a previously-uncreatable shape.*
- **Explicit `unique_columns` were silently dropped for any query containing CTEs.**  The CTE-decomposition path did not thread `unique_columns` into the outer passthrough IMV (the set-op and `DISTINCT ON` paths did), so a JOIN passthrough over CTEs fell back to **full refresh** on `DELETE`/`UPDATE` despite a supplied key.  The key now reaches the stored metadata.  *Create-time fix — recreate CTE IMVs built under ≤1.6.4 to gain incremental `DELETE`/`UPDATE`.*
- **`MIN`/`MAX` over a materialized-view column failed at creation with `… is of type numeric but expression is of type timestamp with time zone`.**  Source types came from `information_schema.columns`, which **omits materialized views**, so `MIN`/`MAX` defaulted to `NUMERIC`.  Types are now read from `pg_catalog` (all relkinds), completing the 1.6.3 type-resolution fix for matview sources.  *Create-time fix — unblocks a previously-uncreatable shape.*

**Migration**

No DDL required.  Fixes 1 and 3 have no existing-IMV impact.  For fix 2, drop and recreate a CTE IMV with an explicit key to gain incremental `DELETE`/`UPDATE`: `SELECT drop_reflex_ivm('<name>'); SELECT create_reflex_ivm('<name>', '<SELECT …>', '<key cols>');`

## [1.6.4] — 2026-05-24

Correctness release hardened by a new differential fuzz harness.  No catalog schema, trigger body, or API changes.  Run `ALTER EXTENSION pg_reflex UPDATE TO '1.6.4';` (no-op migration marker).  Runtime fixes reach existing IMVs on recompile; create-time fixes affect only newly-created IMVs.

**Fixed**

- **LEFT / RIGHT JOIN secondary-side maintenance dropped or duplicated rows.**  Inserting / updating / deleting a row on the **secondary** side of an outer join could drop or duplicate joined rows, and a primary row that gained or lost its match could be deleted instead of reverting to a NULL-filled row.  `INSERT` routing, affected-group scoping, and quoted-source detection are corrected in `reflex_build_delta_sql`.  *Runtime fix.*
- **DEFERRED-mode duplicate-key flush.**  A batch that `INSERT`ed a new key then `UPDATE`d the **same** key before flush emitted both delta sides, so `reflex_flush_deferred` failed with `duplicate key value violates unique constraint`.  The two sides are now netted per unique key before the `MERGE`.  *Runtime fix.*
- **Silent row loss from unsound NOT-NULL inference.**  The former runtime data-probe marked a column `NOT NULL` whenever the create-time data happened to be NULL-free, so maintenance matched that key with `=` instead of `IS NOT DISTINCT FROM` and **silently dropped rows** when a NULL appeared later (an unmatched primary-side `LEFT JOIN` insert, or a `GROUP BY` key that became NULL).  `NOT NULL` is now promoted only when the query **structurally** guarantees it (INNER-join equi-key, or a catalog-`NOT NULL` base column on a non-nullable join side); quoted / qualified refs are rejected.  *Create-time fix — recreate existing aggregate IMVs to clear a stale over-promotion (see Migration).*
- **Filtered-IMV maintenance emitted invalid SQL from a qualified WHERE.**  A query-level `WHERE` carried into maintenance kept its table-qualified columns and failed against the transition table; it is now alias-stripped.  *Runtime fix.*
- **Long generated column identifiers exceeded the 63-byte limit** and failed at creation; they are now truncated to 63 bytes on a char boundary.  *Create-time fix.*

**Changed**

- **An aggregate IMV whose `GROUP BY` key is not projected bare in the `SELECT` is now rejected up front** with a clear error instead of failing later in codegen.  *Create-time validation.*

**Testing**

- **Differential fuzz harness** (proptest): for each generated query it builds a real `MATERIALIZED VIEW` and a pg_reflex IMV, applies the same DML, and asserts they agree row-for-row (exact for non-float columns; NULL-safe relative epsilon for floats).  Covers single-table + 2-source `LEFT JOIN` aggregates, carried scalars, CTE decomposition, and basic `WHERE` filters in `IMMEDIATE` and `DEFERRED` modes.  The NOT-NULL / deferred fixes above were found by it and frozen as regression tests.

**Migration**

- `ALTER EXTENSION pg_reflex UPDATE TO '1.6.4';` runs [`sql/pg_reflex--1.6.3--1.6.4.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.3--1.6.4.sql), a no-op marker.  Runtime fixes (JOIN secondary-side, deferred netting, filtered WHERE) reach every existing IMV at its next trigger fire.  The create-time NOT-NULL fix does **not** reach an aggregate IMV created under an earlier version — its over-promotion is baked into stored metadata and the intermediate-table schema.  **Drop and recreate any aggregate IMV created before 1.6.4** to clear a latent over-promotion.

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
