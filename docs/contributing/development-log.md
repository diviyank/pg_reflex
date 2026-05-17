# Development log

Compressed timeline of the engine's evolution: what shipped, what was reverted, and where to read the full story. Aimed at contributors and maintainers — operators of the extension should usually read [Internals](../concepts/internals.md) instead, which covers *current* behaviour without the history.

Each row points at the underlying [journal](https://github.com/diviyank/pg_reflex/tree/main/journal) entry when one exists, so the reasoning behind a change survives the version that introduced it.

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
| 1.5.0 | 2026-05 | Reconcile drop-indexes step: text-cast fix | `pg_indexes.indexname` is `name`, not `text`; SPI silently returned `None` and skipped every drop, leaving stale indexes that `CREATE … IF NOT EXISTS` then no-op'd. ~30 s saved per 100 M-row IMV |
| 1.5.0 | 2026-05 | Reconcile SPI aggregations cast | `__reflex_ivm_reference.aggregations` is `jsonb`; SPI read needs `::text` cast or the column reads `None` and downstream codegen sees empty plan. Was failing reconcile silently on aggregate IMVs |
| 1.5.0 | 2026-05 | Path C — EXPLAIN-based pre-scratch dispatch for `INSERT_PROMOTED` | Catches the 1-dim-row → 8.9 M-fact-row fanout case that Path B's `\|transition\|/\|source\|` ratio misses |
| 1.5.0 | 2026-05 | Path C smart bulk-INSERT (replaces reconcile dispatch) | Drop intermediate UNIQUE, bulk INSERT only the new keys from scratch, recreate, project from scratch to target. A4 8.9 M-row OUT→IN: ~175 s reconcile → ~90 s, beats `REFRESH MV` (~160 s) |
| 1.5.0 | 2026-05 | Passthrough trigger: handle Item α `INSERT_PROMOTED` / `DELETE_PROMOTED` | Three-bug fix in `trigger.rs` — passthrough codegen pre-dated Item α and silently emitted nothing for the promoted ops; bulk OUT→IN/IN→OUT on passthrough IMVs now beats `REFRESH MV` in every tested case (e.g. 8.9 M flip 3.77× faster via Path C) |

## What didn't work

Equally important — records of approaches that looked promising and were reverted. Each reverted attempt has a journal entry; this table is the index.

| Attempt | Outcome | Journal |
|---|---|---|
| Wire dispatch into the INSERT and DELETE branches of `reflex_build_delta_sql` (1.4.6 dev) | Reverted. Scratch fill dominated; the dispatch DO block never had a chance to route bulk INSERT/DELETE through reconcile because the scratch INSERT had already run. Path B (pre-scratch dispatch) needed instead | `journal/2026-05-15_dispatch_wiring_revert.md` |
| Bulk-INSERT fast path for Item α `OUT→IN` flips (1.4.6 dev) | Reverted on correctness. The assumed precondition "intermediate has zero rows for affected keys" was false when same-row UPDATE simultaneously flips the filter and changes data — `pg_test_directional_with_filter_flip_and_data_change_same_row` failed the EXCEPT-ALL oracle | `journal/2026-05-15_bulk_insert_revert.md` |
| CTAS+RENAME for `reflex_reconcile` | Reverted as default. Slower than the existing TRUNCATE+INSERT on the warm-cache case; the WAL volume and index-rebuild cost outweighed the swap-in-place benefit on a 7.7 M-row IMV. Considered again per-call as the bulk-rebuild option, not yet shipped | (paths analyzed in 2026-04-21 / 2026-04-22 benches) |
| Per-source ANALYZE in the trigger body | Rejected at design time. Source-side stats are the user's responsibility; pg_reflex's planning need is fully covered by the post-MERGE `ANALYZE intermediate` |  |
| Row-level triggers for finer transition data | Rejected at design time. The transition-table API delivers everything needed at statement level; row-level multiplies overhead by row count without gaining information |  |
| Target btree drop+rebuild in Path C smart bulk-INSERT (1.5.0 dev) | Reverted. Saved per-row maintenance during the bulk INSERT, but the rebuild cost (~13 s) on the 7.7 M-row target survivors exceeded the saving (~5 s). Only worth it when most of the table is being added — not the OUT→IN flip shape | `journal/2026-05-17_1_5_0_optimization_journey.md` |
| Single-table layout (drop the intermediate, project on read) | Held. Investigated in 2026-05-16 bench; dual-table cost overhead is real (~18 % vs `REFRESH MV` on the same state) but removing the intermediate would either re-aggregate on every read or rebuild the same maintenance logic into the target — both wreck the in-place UPDATE win. Re-examine after another shape-bound optimization round | `journal/2026-05-16_single_table_vs_intermediate_bench.md` |

[Architecture tour :material-arrow-right-bold:](architecture-tour.md){ .md-button }
[Internals :material-arrow-right-bold:](../concepts/internals.md){ .md-button }
