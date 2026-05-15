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

## Optimization history

Compressed timeline of what shipped, with version and date. Only changes that materially moved the cost model.

| Version | Date | Change | Effect |
|---|---|---|---|
| 1.1.1 | 2026-02 | `FILTER (WHERE …)` rewritten to `CASE WHEN` | Inherits underlying aggregate's path; no penalty vs hand-written CASE |
| 1.1.3 | 2026-03 | Algebraic `BOOL_OR` (true-count + non-null-count) | Retraction goes from full source scan to `O(delta)` |
| 1.1.3 | 2026-03 | 2-arg advisory lock hash | Eliminated cross-name lock collisions |
| 1.1.3 | 2026-03 | Empty-affected `DO`-block gate | Short-circuits MERGE when no rows changed |
| 1.2.0 | 2026-04 | Streaming exec for delta SQL | Removed intermediate array allocation in trigger bodies |
| 1.2.1 | 2026-04 | Auto-infer `unique_columns` from source PK | No operator action for typical passthroughs |
| 1.3.0 | 2026-04 | Top-K MIN/MAX heap (default K=16) | Retraction `O(K)` instead of `O(group_size)` |
| 1.4.0 | 2026-04 | Top-K default-on for every MIN/MAX intermediate | Same effect, applied without opt-in |
| 1.4.0 | 2026-04 | Scoped recompute for MIN/MAX heap underflow | 4.2× on narrow updates vs full-source rescan |
| 1.4.0 | 2026-04 | N1 heap-shrinkage gate | Skips forced UPDATE recompute when the heap stayed at K |
| 1.4.3 | 2026-05 | Per-IMV `wipe_threshold` column + dispatch DO block | Routes bulk flips to `reflex_reconcile` |
| 1.4.4 | 2026-05 | `null_safe_in` correctness fix | Closed a silent full-refresh of grouped targets — restored the entire IMV win for grouped shapes |
| 1.4.4 | 2026-05 | Drop vestigial single-col B-trees, intermediate `fillfactor=70` | 6.2× warm-UPDATE speedup; HOT ratio 0 % → 100 % |
| 1.4.6 | 2026-05 | `source_join_keys` metadata on `AggregationPlan` | Enables bulk-INSERT/DELETE and Path B scratch-dispatch |
| 1.4.6 | 2026-05 | Bulk-DELETE for `IN→OUT` filter flips | 5–11× speedup on db_clone (A3b 54 s → 4.8 s, A4b 181 s → 29.5 s) |
| 1.4.6 | 2026-05 | Reconcile P1 — drop post-reconcile target ANALYZE | Reconcile critical path doesn't need target stats; intermediate ANALYZE is what pg_reflex's planner reads |
| 1.4.6 | 2026-05 | Schema-resolving `reflex_rebuild_triggers` | Multi-schema match → explicit error instead of `search_path` footgun |

## What didn't work

Equally important — records of approaches that looked promising and were reverted. Each reverted attempt has a journal entry; this table is the index.

| Attempt | Outcome | Journal |
|---|---|---|
| Wire dispatch into the INSERT and DELETE branches of `reflex_build_delta_sql` (1.4.6 dev) | Reverted. Scratch fill dominated; the dispatch DO block never had a chance to route bulk INSERT/DELETE through reconcile because the scratch INSERT had already run. Path B (pre-scratch dispatch) needed instead | `journal/2026-05-15_dispatch_wiring_revert.md` |
| Bulk-INSERT fast path for Item α `OUT→IN` flips (1.4.6 dev) | Reverted on correctness. The assumed precondition "intermediate has zero rows for affected keys" was false when same-row UPDATE simultaneously flips the filter and changes data — `pg_test_directional_with_filter_flip_and_data_change_same_row` failed the EXCEPT-ALL oracle | `journal/2026-05-15_bulk_insert_revert.md` |
| CTAS+RENAME for `reflex_reconcile` | Reverted as default. Slower than the existing TRUNCATE+INSERT on the warm-cache case; the WAL volume and index-rebuild cost outweighed the swap-in-place benefit on a 7.7 M-row IMV. Considered again per-call as the bulk-rebuild option, not yet shipped | (paths analyzed in 2026-04-21 / 2026-04-22 benches) |
| Per-source ANALYZE in the trigger body | Rejected at design time. Source-side stats are the user's responsibility; pg_reflex's planning need is fully covered by the post-MERGE `ANALYZE intermediate` |  |
| Row-level triggers for finer transition data | Rejected at design time. The transition-table API delivers everything needed at statement level; row-level multiplies overhead by row count without gaining information |  |

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
| TRUNCATE codegen | `src/trigger.rs:2227` (`reflex_build_truncate_sql`) |
| `reflex_reconcile` TRUNCATE+INSERT | `src/reconcile.rs:102`, `:241` |
| Source-join-keys metadata | `src/aggregation.rs` (`source_join_keys` on `AggregationPlan`) |

[Delta processing :material-arrow-right-bold:](delta-processing.md){ .md-button }
[Architecture tour (contributor) :material-arrow-right-bold:](../contributing/architecture-tour.md){ .md-button }
