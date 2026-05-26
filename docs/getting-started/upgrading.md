# Upgrading

Existing IMVs, triggers, and registry data are preserved across upgrades. There is **no need to recreate views**.

## From the prebuilt `.deb` package

```bash
sudo dpkg -i pg-reflex-NEW_VERSION-pg17-amd64.deb
psql -d mydb -c "ALTER EXTENSION pg_reflex UPDATE TO 'NEW_VERSION';"
```

## From source

```bash
cd pg_reflex
git pull
./install.sh --release --pg-config $(which pg_config)
psql -d mydb -c "ALTER EXTENSION pg_reflex UPDATE;"
```

## Migration chain

| From → to | What changes | Operator action |
|---|---|---|
| 1.0.0 → 1.0.1 | bug fixes, no schema changes | none |
| 1.0.1 → 1.0.2 | UNLOGGED target tables, hash index, MERGE RETURNING | none |
| 1.0.2 → 1.0.3 | `storage_mode`, `refresh_mode` columns; deferred infra | none |
| 1.0.3 → 1.0.4 | persistent affected-groups table, predicate-filter skip | none |
| 1.0.4 → 1.1.0 → 1.1.1 | DROP CASCADE, FILTER + DISTINCT ON | none |
| 1.1.1 → 1.1.2 → 1.1.3 | algebraic BOOL_OR, perf | none |
| 1.1.3 → 1.2.0 | observability columns, event triggers, scoped MIN/MAX recompute | none |
| 1.2.0 → 1.2.1 | `pg_reflex.alter_source_policy` GUC, scheduled reconcile, PK inference UX | none |
| 1.2.1 → 1.3.0 | top-K MIN/MAX (opt-in), flush histogram, pg_stat_statements tagging | none for existing IMVs; opt-in to top-K per IMV |
| 1.3.0 → 1.4.0 | top-K auto-enabled (K=16) on freshly created IMVs, N1 heap-shrinkage gate, non-NUMERIC top-K element types, UPDATE staleness fix | none for existing IMVs; the migration provisions `__reflex_shrunk_<view>` for IMVs already on top-K. Existing top-K IMVs over `TEXT` / `DATE` / `TIMESTAMP` should run `reflex_rebuild_imv('<name>')` to pick up the corrected trigger codegen. |
| 1.4.0 → 1.4.1 | bug fix: internal reflex tables and trigger-body SPI calls are now schema-qualified so DML under `SET search_path = '<schema>'` (excluding `public`) no longer fails with `relation "__reflex_delta_<…>" does not exist` | **breaking for existing IMVs**: the upgrade cannot rewrite already-installed trigger function bodies or move legacy bare-name internal tables. Drop and recreate every existing IMV (`SELECT drop_reflex_ivm('<name>'); SELECT create_reflex_ivm('<name>', '<SELECT …>', …);`). The migration script emits a per-IMV `NOTICE` listing what to rebuild. |
| 1.5.1 → 1.6.0 | partitioning Phase 1 + Phase 2 (`partition_by`, `reflex_sync_partitions`, `reflex_reconcile_partition`, atomic DETACH/ATTACH swap, per-partition trigger dispatch); new catalog columns (`partition_columns`, `partition_strategy`, `wipe_floor_rows`, `partition_dispatch_cost_cap`); event-trigger widened to `CREATE TABLE PARTITION OF`; mixed-case quoted column-name fix carried over from unreleased 1.5.2 | none for non-partitioned IMVs; the migration re-emits trigger function bodies so the mixed-case codegen takes effect. Operators with existing mixed-case quoted source columns should DROP + recreate the affected IMVs to get case-preserved target column names. |
| 1.6.0 → 1.6.1 | PG 18 compatibility (partitioned parents now LOGGED, children UNLOGGED — see [`CHANGELOG.md`](../changelog.md)); CI hygiene; internal pipeline refactor.  No catalog or trigger changes. | none for existing IMVs.  Advisory: if you `pg_upgrade` to PG 18 a cluster that has partitioned IMVs created on 1.6.0, drop and recreate those IMVs first so they are recreated with LOGGED parents. |
| 1.6.1 → 1.6.2 | deferred trigger body uses named-column INSERT (fixes `column "X" is of type Y but expression is of type Z` after source DDL drift); `reflex_rebuild_triggers` is now deferred-aware; create_ivm guards staging shape against drift. The migration repairs every drifted `__reflex_delta_<src>` and re-emits trigger bodies. | none for existing IMVs whose source schemas have not drifted. **If a previously-existing IMV's source was dropped+recreated (e.g. to add partitioning on PG ≤ 17), the migration will drop the stale staging delta — including any pending deferred rows.** Run `SELECT reflex_flush_deferred('<src>')` on each affected source BEFORE upgrading if those rows matter. |
| 1.6.2 → 1.6.3 | CTE / window-function decomposition correctness: CTE decomposition runs before window/DISTINCT-ON decomposition (window in the top-level SELECT over CTEs no longer fails with `relation "<cte>" does not exist`); a window nested in a subquery/derived table is rejected instead of crashing (SIGSEGV); a window / `DISTINCT ON` inside a CTE referenced by an outer query is rejected with guidance (`kind: mv`); `partition_by` propagates to CTE sub-IMVs; `MAX`/`MIN` over a table-qualified non-numeric column (e.g. `MAX(t.ts)` on `timestamptz`/`date`/`text`) no longer fails with `… is of type numeric …`. No catalog or trigger changes (no-op migration marker). | none for existing IMVs. Views kept as `kind: mv` because they nest a window / `DISTINCT ON` inside a referenced CTE stay `kind: mv` — that shape is still not an IMV, but now fails fast with guidance rather than crashing. |
| 1.6.3 → 1.6.4 | Correctness fixes hardened by a new differential fuzz harness. **Runtime** (reach existing IMVs on recompile): LEFT/RIGHT JOIN secondary-side maintenance no longer drops/duplicates rows; DEFERRED flush no longer raises `duplicate key value violates unique constraint` on insert-then-update of the same key in one batch; filtered-IMV `WHERE` is alias-stripped. **Create-time** (newly-created IMVs only): structural NOT-NULL inference replaces the unsound runtime data-probe that could silently drop rows; long generated identifiers truncated to 63 bytes; an aggregate IMV with a `GROUP BY` key not projected bare is rejected. No catalog or trigger changes (no-op migration marker). | none required for the runtime fixes. **The create-time NOT-NULL fix does not reach aggregate IMVs created under ≤1.6.3** — the over-promotion is baked into stored metadata and the intermediate-table schema, and cannot be corrected in place. Drop and recreate any aggregate IMV created before 1.6.4 to clear a latent over-promotion: `SELECT drop_reflex_ivm('<name>'); SELECT create_reflex_ivm('<name>', '<SELECT …>', …);` |
| 1.6.4 → 1.6.5 | Three create-time correctness fixes for view shapes hit during real migrations. A `DEFERRED` IMV over a CTE no longer fails at creation with `zero-length delimited identifier at or near ""` (a CTE sub-IMV's already-quoted schema was being re-quoted into `""schema""`). Explicit `unique_columns` are now threaded into the outer passthrough IMV of a CTE query (previously dropped, so a JOIN passthrough over CTEs silently fell back to full refresh on DELETE/UPDATE). `MIN`/`MAX` over a **materialized-view** column no longer fails with `… is of type numeric …` — column types now come from `pg_catalog` (which covers matviews) instead of `information_schema.columns` (which omits them), completing the 1.6.3 type-resolution fix. No catalog or trigger changes (no-op migration marker). | none required for fixes 1 and 3 (they unblock shapes that could not be created before). **For the `unique_columns` fix, an IMV built from a CTE query under ≤1.6.4 keeps its empty key (full refresh on DELETE/UPDATE).** Drop and recreate it with an explicit key to gain incremental DELETE/UPDATE: `SELECT drop_reflex_ivm('<name>'); SELECT create_reflex_ivm('<name>', '<SELECT …>', '<key cols>');` |

`ALTER EXTENSION pg_reflex UPDATE` walks the chain automatically.

## After upgrade

Run the smoke check:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'pg_reflex';
SELECT count(*) FROM public.__reflex_ivm_reference;
SELECT * FROM reflex_ivm_status();
```

If any IMV shows `last_error` after upgrade, run `SELECT reflex_rebuild_imv('<name>')`.

[Operations runbook :material-arrow-right-bold:](../operations/runbook.md){ .md-button }
