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
| [`reflex_rebuild_imv(view_name)`](reflex_reconcile.md) | `TEXT` | Alias of `reflex_reconcile` (1.2.0+). |
| [`refresh_imv_depending_on(source)`](refresh_imv_depending_on.md) | `TEXT` | Reconcile every IMV whose `depends_on` lists `source`, in graph-depth order. |
| [`reflex_scheduled_reconcile(max_age_minutes)`](reflex_scheduled_reconcile.md) | `SETOF (name, status, ms)` | 1.2.1+. Reconcile every IMV staler than the threshold. pg_cron-friendly. |

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
| [`pg_reflex.alter_source_policy`](gucs.md) | `'warn'` | 1.2.1+. `'warn'` or `'error'` — reaction to `ALTER TABLE` on a tracked source. |

## Internal / codegen helpers

These are `pg_extern` for trigger codegen reuse — they are not part of the user API and may change between releases. Listed here for completeness only.

| Function | Used by |
|---|---|
| `reflex_build_delta_sql(view_name, source_table, operation, base_query, end_query, aggregations_json, orig_base_query)` | Per-source trigger bodies. Returns a `\n--<<REFLEX_SEP>>--\n`-separated statement script for the delta. |
| `reflex_build_truncate_sql(view_name)` | TRUNCATE trigger bodies. Returns the clear-intermediate-and-target script. |
| `reflex_execute_separated(sql)` | Trigger bodies. Splits on the `\n--<<REFLEX_SEP>>--\n` separator and `EXECUTE`s each statement in order. |
| `public.__reflex_array_subtract_multiset(arr, remove)` | Top-K MIN/MAX retraction codegen. Multi-set subtraction over arrays. |

Do not call these from application code.
