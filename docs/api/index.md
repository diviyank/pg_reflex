# API reference

Every SQL-callable function pg_reflex installs. Click a name for the full page.

## Lifecycle

| Function | Returns | Purpose |
|---|---|---|
| [`create_reflex_ivm(view_name, sql [, unique_columns [, storage [, mode [, ignore_sources]]]])`](create_reflex_ivm.md) | `TEXT` | Register and build an IMV. Overloads add `topk` (1.3.0+) and `partition_by` (1.5.0+). |
| [`create_reflex_ivm_if_not_exists(view_name, sql [, unique_columns [, storage [, mode [, ignore_sources]]]])`](create_reflex_ivm_if_not_exists.md) | `TEXT` | Idempotent variant — skips silently if `view_name` already exists. Same overloads. |
| [`drop_reflex_ivm(view_name)`](drop_reflex_ivm.md) | `TEXT` | Drop an IMV. Refuses if children exist. |
| [`drop_reflex_ivm(view_name, cascade)`](drop_reflex_ivm.md) | `TEXT` | Drop an IMV, recursively dropping children when `cascade = true`. |

## Reconcile / refresh

| Function | Returns | Purpose |
|---|---|---|
| [`reflex_reconcile(view_name)`](reflex_reconcile.md) | `TEXT` | Rebuild intermediate + target from source. Safety net against drift. |
| [`refresh_reflex_imv(view_name)`](reflex_reconcile.md) | `TEXT` | Alias of `reflex_reconcile`. |
| [`reflex_rebuild_imv(view_name)`](reflex_reconcile.md) | `TEXT` | Alias of `reflex_reconcile` (1.2.0+). Anchor-scoped. |
| [`refresh_imv_depending_on(source)`](refresh_imv_depending_on.md) | `TEXT` | Reconcile every IMV whose `depends_on` lists `source`, in graph-depth order. |
| [`reflex_scheduled_reconcile(max_age_minutes)`](reflex_scheduled_reconcile.md) | `SETOF (name, status, ms)` | 1.2.1+. Reconcile every IMV staler than the threshold. pg_cron-friendly. |

## Recovery and diagnostics

| Function | Returns | Purpose |
|---|---|---|
| [`reflex_doctor(target, fix, drop_orphans, max_attempts)`](reflex_doctor.md) | `SETOF (check_id, severity, object, finding, action, outcome)` | 1.10.8+. One-stop diagnostic and repair entrypoint. Dry-run by default. |
| [`reflex_rebuild_chain(view_name)`](reflex_rebuild_chain.md) | `TEXT` | 1.10.8+. Atomic CASCADE drop + recreate of a corrupted decomposed IMV chain. |

## Partitioning

| Function | Returns | Purpose |
|---|---|---|
| [`reflex_reconcile_partition(view_name, partition_keys [, source_partition])`](reflex_reconcile_partition.md) | `TEXT` | 1.6.0+. Rebuild only the partition(s) covering `partition_keys` via an atomic `DETACH`/`ATTACH` swap; other partitions stay live. |
| [`reflex_sync_partitions(view_name [, drop_orphans])`](reflex_sync_partitions.md) | `TEXT` | 1.6.0+. Reconcile partition *structure* — create missing IMV children, optionally drop orphans. |
| [`reflex_flush_partitions()`](reflex_flush_partitions.md) | `TEXT` | 1.6.0+. Drain pending source `ATTACH`/`DETACH` swaps and propagate to IMV partitions. |
| [`reflex_flush_partition_source(source_root)`](reflex_flush_partition_source.md) | `TEXT` | 1.6.0+. Flush a single source root without scanning the pending queue. |
| [`reflex_partition_pending_status()`](reflex_partition_pending_status.md) | `SETOF (source_root, enqueued_at, age_seconds, attempts, last_error)` | 1.10.8+. Read-only reporter of pending partition DDL queue. Detect wedged roots. |
| [`reflex_refresh_partition_snapshot_if_diverged(source_root)`](reflex_refresh_partition_snapshot_if_diverged.md) | `TEXT` | 1.10.8+. Heal partition snapshot divergence (oid-diffs and rebuilds on divergence). |

## Maintenance

| Function | Returns | Purpose |
|---|---|---|
| [`reflex_compact_imv(view_name)`](reflex_compact_imv.md) | `TEXT` | 1.4.5+. `VACUUM (FULL)` an IMV's intermediate + target to materialize `fillfactor=70`. |
| [`reflex_compact_all_imv()`](reflex_compact_imv.md) | `TEXT` | 1.4.5+. Run `reflex_compact_imv` over every enabled IMV. |
| [`reflex_probe_not_null_columns(view_name)`](reflex_probe_not_null_columns.md) | `TEXT` | 1.4.5+. Re-probe effectively-`NOT NULL` group-by columns to keep codegen index-friendly. |
| [`reflex_rebuild_imv_metadata(view_name)`](reflex_rebuild_imv_metadata.md) | `TEXT` | 1.4.5+. Re-analyze `base_query` and refresh the relevant-columns/where maps. Migration helper. |
| [`reflex_rebuild_triggers(source_table)`](reflex_rebuild_triggers.md) | `TEXT` | 1.4.5+. Re-emit consolidated trigger bodies for a source table. Migration helper. |
| [`reflex_audit([view_name])`](reflex_audit.md) | `TEXT` | 1.5.0+. Consistency audit (catastrophic / drift / orphan checks) over all or one IMV. |

## Tuning

| Function | Returns | Purpose |
|---|---|---|
| [`reflex_set_wipe_threshold(view_name, value)`](reflex_set_wipe_threshold.md) | `TEXT` | 1.4.6+. Per-IMV override of the wipe-vs-delta dirty-fraction cutoff. `NULL` clears. |
| [`reflex_set_wipe_floor_rows(view_name, value)`](reflex_set_wipe_floor_rows.md) | `TEXT` | 1.6.0+. Per-IMV floor on the dirty-ratio denominator. `NULL` clears. |
| [`reflex_set_partition_dispatch_cost_cap(view_name, value)`](reflex_set_partition_dispatch_cost_cap.md) | `TEXT` | 1.6.0+. Per-IMV Tier 2 dispatch cost cap. **Reserved — not yet wired.** |

## Deferred mode

| Function | Returns | Purpose |
|---|---|---|
| [`reflex_flush_deferred(source_table)`](reflex_flush_deferred.md) | `TEXT` | Drain pending deltas for `source_table` and apply them to every `DEFERRED` IMV that depends on it. Called automatically at COMMIT; safe to call manually. |

## Introspection

| Function | Returns | Purpose |
|---|---|---|
| [`reflex_ivm_status()`](reflex_ivm_status.md) | `SETOF` row per IMV | One-row summary per IMV: enabled, mode, row count, last flush, last error. |
| [`reflex_ivm_stats(view_name)`](reflex_ivm_stats.md) | `SETOF (metric, value)` | Detailed key/value stats for a single IMV (sizes, flush counters). |
| [`reflex_ivm_histogram(view_name)`](reflex_ivm_histogram.md) | one row | 1.3.0+. p50/p95/p99 flush latency from the 64-sample ring buffer. |
| [`reflex_explain_flush(view_name)`](reflex_explain_flush.md) | `TEXT` | `EXPLAIN (VERBOSE, COSTS ON)` of the IMV's `base_query` without executing it. |

## Event triggers (extension-owned)

See [Event triggers](event-triggers.md) for details.

| Trigger | Event | Purpose |
|---|---|---|
| `reflex_on_sql_drop` | `sql_drop` | Auto-cleans IMVs whose source table is being dropped. |
| `reflex_on_ddl_command_end` | `ddl_command_end` (`ALTER TABLE`) | Warns or errors when a tracked source is altered; controlled by [`pg_reflex.alter_source_policy`](gucs.md). |

## GUCs

| Name | Default | Purpose |
|---|---|---|
| [`reflex.wipe_threshold`](gucs.md#reflexwipe_threshold) | `0.5` | 1.4.6+. Dirty-row fraction at or above which a batch wipes-and-rebuilds instead of delta-applying. |
| [`reflex.wipe_floor_rows`](gucs.md#reflexwipe_floor_rows) | `1000` | 1.6.0+. Floor on the partition-size denominator of the dirty ratio. |
| [`pg_reflex.alter_source_policy`](gucs.md#pg_reflexalter_source_policy) | `'warn'` | 1.2.1+. `'warn'` or `'error'` — reaction to `ALTER TABLE` on a tracked source. |
| [`pg_reflex.debug_resolve_anchor`](gucs.md#pg_reflexdebug_resolve_anchor) | `off` | 1.10.8+. Gates diagnostic NOTICEs during anchor resolution. Off by default to avoid burying real WARNINGs. |

## Internal / codegen helpers

These are `pg_extern` for trigger codegen reuse — they are not part of the user API and may change between releases. Listed here for completeness only.

| Function | Used by |
|---|---|
| `reflex_build_delta_sql(view_name, source_table, operation, base_query, end_query, aggregations_json, orig_base_query)` | Per-source trigger bodies. Returns a `\n--<<REFLEX_SEP>>--\n`-separated statement script for the delta. |
| `reflex_build_truncate_sql(view_name)` | TRUNCATE trigger bodies. Returns the clear-intermediate-and-target script. |
| `reflex_execute_separated(sql)` | Trigger bodies. Splits on the `\n--<<REFLEX_SEP>>--\n` separator and `EXECUTE`s each statement in order. |
| `public.__reflex_array_subtract_multiset(arr, remove)` | Top-K MIN/MAX retraction codegen. Multi-set subtraction over arrays. |

Do not call these from application code.
