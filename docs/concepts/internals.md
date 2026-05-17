# Internals

Engine-level details behind the public API. Useful when tuning, debugging, or reading the source. Separate from [architecture](architecture.md), which covers *what* objects exist; this page covers *how* they behave at the PostgreSQL level.

## Trigger model

pg_reflex uses **AFTER, statement-level triggers** with `REFERENCING NEW TABLE` / `OLD TABLE` transition tables. The choice matters:

- **Statement-level** fires once per DML statement, not once per row. A `COPY ... FROM` or 1 M-row `INSERT … SELECT` fires the trigger exactly once with the full transition table. Row-level triggers would multiply trigger overhead by row count.
- **AFTER** — the trigger sees the post-statement state of the source. Required so the delta MERGE writes from a stable, consistent view.
- **Transition tables** are PG-native ephemeral relations (no copy, no temp table created). pg_reflex reads them like any other table: `SELECT ... FROM new_rows JOIN ...`.

Four triggers per source, regardless of how many IMVs depend on it:

```
__reflex_trigger_ins_on_<source>     AFTER INSERT
__reflex_trigger_del_on_<source>     AFTER DELETE
__reflex_trigger_upd_on_<source>     AFTER UPDATE
__reflex_trigger_trunc_on_<source>   AFTER TRUNCATE
```

The trigger body loops over `__reflex_ivm_reference` and computes the delta per IMV. Adding a second IMV on the same source piggybacks on existing triggers — no extra trigger overhead per source row.

## The dispatch DO block

Every grouped non-MIN/MAX flush is wrapped in a per-IMV `DO` block that picks one of two paths based on selectivity (`trigger.rs:1051`):

```sql
SELECT count(*) INTO _aff FROM <affected>;
SELECT GREATEST(reltuples, 1) INTO _imm FROM pg_class WHERE oid = '<intermediate>'::regclass;
_thr := COALESCE(per-IMV column, GUC, compiled default);
IF (_aff / _imm) >= _thr THEN
    PERFORM reflex_reconcile('<view>');     -- TRUNCATE + bulk INSERT
ELSE
    EXECUTE merge_sql; ANALYZE intermediate;
    EXECUTE target_delete_sql; EXECUTE target_insert_sql;
END IF;
```

The crossover defaults to `0.5` from 1.4.6 onward. Setting it lower routes more workloads through the rebuild branch; setting it higher keeps everything incremental at the cost of pathological in-place updates on bulk filter flips. Per-IMV tuning via `reflex_set_wipe_threshold` is preferred — the right threshold is shape-dependent (see [optimization](../performance/optimization.md)).

The `ANALYZE intermediate` between MERGE and target sync is non-optional. The MERGE shifts the `__ivm_count` distribution; without fresh stats the planner has picked NestedLoop+SeqScan plans for the target DELETE that ran for 12+ minutes on 100 K groups. Cost of the ANALYZE itself: ~150 ms on the SOP-forecast shape (`trigger.rs:1103-1110`).

## Pre-scratch dispatch — Path B and Path C

The dispatch DO block above fires *after* scratch fill, which means the JOIN against the source has already run. On bulk filter flips that fanout to millions of fact rows, scratch fill itself is the dominant cost — dispatching to reconcile *after* scratch has already paid that cost is a net loss. Two pre-scratch probes route the work *before* scratch fills.

**Path B — `|transition| / |source|` ratio**. Cheap. Catches the case where a sweeping DML on a source produced a transition table that is itself a meaningful fraction of the source. For `UPDATE sales_simulation SET …` touching 40 M of 76 M rows, the ratio is 0.52 and dispatch routes to reconcile. Fails when the transition is tiny but the JOIN fanout is huge — see Path C.

**Path C — planner row estimate**. Only emitted in the UPDATE trigger body, gated on `_directional_op = 'INSERT_PROMOTED'` (the only place Item α promotes a bulk OUT→IN dim flip). Runs `EXPLAIN (FORMAT JSON)` on `reflex_build_path_c_explain_sql(view, source)` — the rewritten scratch-fill query (`base_query` with `source_table → transition_new`) — and reads the planner's row estimate without executing the JOIN. Compared against the IMV's `wipe_threshold`. Catches the *dim-flip fanout* case where Path B's ratio is misleading (1 dim row of 28 = 3.6 %, but JOINs to 8.9 M fact rows).

**Path C dispatches to a *smart bulk-INSERT***, not to `reflex_reconcile`. The Item α `INSERT_PROMOTED` precondition (OLD-side filter-rejected ⇒ intermediate has zero rows for affected keys) makes a surgical add safe and cheaper than full rebuild:

```sql
-- emitted inline in the UPDATE trigger body when ratio >= wipe_threshold
TRUNCATE <scratch>;
INSERT INTO <scratch> <base_query with source→transition_new>;  -- only the new keys
DROP INDEX <intermediate UNIQUE>;       -- skip per-row B-tree probes
INSERT INTO <intermediate> SELECT * FROM <scratch>;
CREATE UNIQUE INDEX ...;                -- rebuild bottom-up over the union
INSERT INTO <target>
    <end_query with intermediate→scratch>;  -- project from scratch, no intermediate re-read
ANALYZE <intermediate>;
```

Wins on alp.bench_user_imv 8.9 M-row OUT→IN flip: ~175 s reconcile → ~90 s smart path (measured standalone), beating `REFRESH MV` (~160 s) on the same post-state. Reconcile would re-aggregate the unchanged 7.7 M survivors; smart-bulk-INSERT touches only the 8.9 M new keys.

Any failure (catalog lookup, parse error, EXECUTE failure) is caught by `EXCEPTION WHEN OTHERS` and falls through to the standard incremental path — never aborts the trigger.

The smart bulk-INSERT only ever issues `INSERT` against the target (no `TRUNCATE`), so it takes `RowExclusiveLock` on the target — concurrent readers are not blocked. The brief `AccessExclusiveLock` during the intermediate UNIQUE drop is on an internal table users don't query. See [delta-processing — Locking and reader impact](delta-processing.md#locking-and-reader-impact) for the full lock table across all paths.

Two subtleties caught during landing and worth remembering when editing the codegen:

- **No `--` SQL comments inside the trigger body**. The whole body is concatenated to one line in the emitted DDL, so a `--` comment swallows everything after it until end-of-input. Postgres reports `syntax error at end of input`. Keep comments as Rust `//` source comments only.
- **Identifier quoting must match what `end_query` stores**. `end_query`'s FROM clause uses `intermediate_table_name`, which always emits `"schema"."table"`. `format('%I.%I', schema, table)` drops the quotes when names are plain lowercase, so `REPLACE(end_query, ...)` silently fails to substitute — the projection then re-reads the just-bulk-INSERTed intermediate and double-inserts existing rows. Build the reference with explicit `'"' || schema || '"."' || table || '"'`.

## HOT updates and fillfactor

PostgreSQL's HOT (Heap-Only Tuple) optimization avoids index updates when no indexed column changes **and** there's free space on the same heap page. Both conditions must hold.

pg_reflex hits the first condition naturally: a MERGE on the intermediate updates `__sum_*`, `__count_*`, `__min_*` and similar non-indexed columns; the group-by columns (which are indexed) don't change. The second condition is enforced by **`fillfactor = 70` on the intermediate** since 1.4.4, leaving 30 % free page space for new tuple versions on the same page.

The 2026-05-13 bench (`journal/2026-05-13_intermediate_idx_and_fillfactor.md`):

| Configuration | int_update | tgt_update | total |
|---|---|---|---|
| 6 single-col indexes + composite, fillfactor=100 | 691 ms | 179 ms | **870 ms** |
| Composite index only, fillfactor=100 | 208 ms | 169 ms | 377 ms |
| Composite index only, fillfactor=70 (HOT) | **75 ms** | **64 ms** | **139 ms** |

HOT update ratio jumps from 0 % to 100 % across the change. Combined 6.2× speedup, achieved by **removing** vestigial single-column B-tree indexes (the composite already serves every pg_reflex-generated query) and **lowering** fillfactor.

Operator-visible: `pg_stat_all_tables.n_tup_hot_upd / n_tup_upd` for the intermediate should stay near 100 %. If it drops, the page is full — usually because autovacuum has fallen behind or because the row width grew past the per-page budget.

## Heap behaviour: UNLOGGED, bloat, vacuum

**UNLOGGED is the default** for intermediate and target tables. The win is 2–4× lower flush latency on write-heavy workloads — every MERGE skips WAL. The cost: tables are empty after a crash. `reflex_scheduled_reconcile` (or `reflex_reconcile` per IMV) rebuilds them from source. See [crash recovery](../operations/crash-recovery.md).

LOGGED per-IMV is available for workloads with strict post-crash SLAs:

```sql
SELECT create_reflex_ivm('hourly_kpi', ..., NULL, 'LOGGED');
```

**Bloat behaviour**: every flush's MERGE+target DELETE+INSERT creates dead tuples. Autovacuum on an UNLOGGED table works the same as on a LOGGED one — but because there's no WAL, the only memory pressure is from `n_tup_ins + n_tup_upd + n_tup_del`. Tighten `autovacuum_vacuum_scale_factor` per IMV when the working set grows during the day:

```sql
ALTER TABLE __reflex_intermediate_v SET (autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE v SET (autovacuum_vacuum_scale_factor = 0.05);
```

**Long reader interaction**: an open snapshot from a 10-minute analytical query holds the visibility horizon back; autovacuum cannot reclaim tuples that snapshot still sees, even on UNLOGGED tables. The IMV grows until the reader finishes. The blast radius is bounded by the working set (changes during the reader's lifetime), not the full table.

## Indexes auto-created

| Object | Index | Rationale |
|---|---|---|
| Intermediate, single GROUP BY column | `USING hash` on the group column | ~30 % faster MERGE probes than B-tree for equality-only lookups |
| Intermediate, multi-column GROUP BY | One composite B-tree `UNIQUE NULLS NOT DISTINCT` on the full key | Single index satisfies the MERGE `ON` clause's full-equality predicate |
| Target, when `unique_columns` resolved | `UNIQUE INDEX __reflex_uk_<view>` | Backs targeted DELETE / UPDATE |
| Source, MIN/MAX with GROUP BY | B-tree on GROUP BY columns | Scoped-recompute scan locality |

`NULLS NOT DISTINCT` (PG 15+) is mandatory — NULL group keys in a `SUM ... GROUP BY x` are valid output rows, and a regular `UNIQUE` would allow duplicate `(NULL)` rows.

The 1.4.4 change above removed the **per-column B-trees** that earlier versions created in addition to the composite. They were vestigial: `build_merge_using` always emits a full-equality predicate, so the composite's leading column was always bound, and the per-column indexes were unreachable by any pg_reflex-generated query.

## Optimization history and reverted attempts

Moved to [Contributing → Development log](../contributing/development-log.md). That page is the version-by-version timeline of what shipped, and an index of approaches that were reverted with links to their journal entries.

## Per-IMV SAVEPOINT in DEFERRED flush

`reflex_flush_deferred` wraps each IMV's drain in its own SAVEPOINT. A failing IMV (e.g. constraint violation on the target) doesn't abort the whole transaction's cascade — the failure is recorded against that IMV and the next one runs. See [crash recovery](../operations/crash-recovery.md) and `__reflex_ivm_reference.last_error`.

## Where to look in the source

Cross-references for readers walking the code:

| Topic | File:line |
|---|---|
| Empty-delta short-circuit | `src/schema_builder.rs` trigger body |
| Per-source DEFERRED-flush serialisation lock | `src/trigger.rs:2357` (`reflex_flush:<source>`) |
| Per-IMV advisory lock (2-arg hash form) | `src/trigger.rs:2671` |
| MERGE codegen | `src/trigger.rs:70` (`build_merge_sql`) |
| Dispatch DO block | `src/trigger.rs:1051` (`build_high_selectivity_dispatch_sql`) |
| Self-join full-refresh branch | `src/trigger.rs:1435` |
| Bulk-INSERT / Bulk-DELETE paths | `src/trigger.rs:1211`, `:1289` |
| Path B pre-scratch dispatch | `src/schema_builder.rs` trigger body (search for `Path B: dispatching`) |
| Path C smart bulk-INSERT | `src/schema_builder.rs` (`path_c_for_update` in `build_trigger_ddls`) |
| `reflex_build_path_c_explain_sql` | `src/trigger.rs:2257` |
| TRUNCATE codegen | `src/trigger.rs:2227` (`reflex_build_truncate_sql`) |
| `reflex_reconcile` TRUNCATE+INSERT | `src/reconcile.rs:102`, `:241` |
| Source-join-keys metadata | `src/aggregation.rs` (`source_join_keys` on `AggregationPlan`) |

[Delta processing :material-arrow-right-bold:](delta-processing.md){ .md-button }
[Architecture tour (contributor) :material-arrow-right-bold:](../contributing/architecture-tour.md){ .md-button }
