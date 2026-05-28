use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;

use crate::aggregation;
use crate::query_decomposer::{intermediate_table_name, quote_identifier, split_qualified_name};
use crate::schema_builder::build_indexes_ddl;
use crate::validate_view_name;

/// Reconcile an IMV by rebuilding intermediate + target from scratch.
/// Use this as a safety net (manually or via pg_cron) to fix drift.
pub(crate) fn reflex_reconcile(view_name: &str) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
    // Best-effort partition sync: keep the IMV partition set aligned with
    // the source before rebuilding.  No-op for unpartitioned IMVs.  Sync
    // failures are surfaced as a NOTICE but do not abort reconcile — the
    // operator may have deliberately stale state, and the rebuild itself
    // is the recovery action.
    let sync_msg = crate::partition::reflex_sync_partitions_impl(view_name, true);
    if sync_msg.starts_with("ERROR") {
        pgrx::notice!("pg_reflex: sync before reconcile returned: {}", sync_msg);
    }
    Spi::connect_mut(|client| {
        let rows = client
            .select(
                "SELECT base_query, end_query, aggregations::text AS aggregations \
                 FROM public.__reflex_ivm_reference WHERE name = $1 AND enabled = TRUE",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

        if rows.is_empty() {
            warning!(
                "pg_reflex: reconcile failed — IMV '{}' not found or disabled",
                view_name
            );
            return "ERROR: IMV not found or disabled";
        }

        let row = &rows[0];
        let base_query: String = row
            .get_by_name::<&str, _>("base_query")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string();
        let end_query: String = row
            .get_by_name::<&str, _>("end_query")
            .unwrap_or(None)
            .unwrap_or("")
            .to_string();
        let agg_json: String = row
            .get_by_name::<&str, _>("aggregations")
            .unwrap_or(None)
            .unwrap_or("{}")
            .to_string();

        let parsed_plan: Option<aggregation::AggregationPlan> =
            serde_json::from_str::<aggregation::AggregationPlan>(&agg_json).ok();
        let is_passthrough = parsed_plan
            .as_ref()
            .map(|p| p.is_passthrough)
            .unwrap_or(false);

        // 1.5.3 (plans/partitioning_3.md §1 follow-up): partitioned IMVs
        // skip the TRUNCATE-on-parent pattern and rebuild each child via
        // the same atomic DETACH/ATTACH swap used by
        // `reflex_reconcile_partition`.  This keeps the
        // AccessExclusiveLock window on the parent to per-child DDL only
        // (microseconds) instead of holding it for the entire rebuild
        // duration.  Readers pruning to a not-yet-swapped partition stay
        // live throughout.
        if let Some(ref plan) = parsed_plan {
            if !plan.partition_columns.is_empty()
                && !plan.partition_strategy.is_empty()
                && !plan.anchor_source.is_empty()
            {
                let (schema_opt, _) = split_qualified_name(view_name);
                let schema = schema_opt.unwrap_or("public").to_string();

                // Resolve storage mode from the catalog so swap tables
                // match the parent's persistence.
                let storage_mode: String = client
                    .select(
                        "SELECT storage_mode FROM public.__reflex_ivm_reference WHERE name = $1",
                        Some(1),
                        &[unsafe {
                            DatumWithOid::new(
                                view_name.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        }],
                    )
                    .ok()
                    .and_then(|mut it| it.next())
                    .and_then(|r| {
                        r.get_by_name::<&str, _>("storage_mode")
                            .ok()
                            .flatten()
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_else(|| "UNLOGGED".to_string());
                let unlogged = storage_mode.eq_ignore_ascii_case("UNLOGGED");

                // Walk every source partition child and swap each.
                let src_children =
                    crate::partition::list_partition_children(client, &plan.anchor_source);
                for src in &src_children {
                    if let Err(e) = crate::partition::execute_partition_swap_for_child(
                        client,
                        view_name,
                        &schema,
                        &src.bare_name,
                        &base_query,
                        &end_query,
                        unlogged,
                    ) {
                        warning!(
                            "pg_reflex: per-partition reconcile of '{}' for child '{}' failed: {}",
                            view_name,
                            src.bare_name,
                            e
                        );
                        return "ERROR: partition reconcile failed";
                    }
                }

                // ANALYZE the parents so the planner sees the freshly
                // attached children's stats.
                let _ = client.update(
                    &format!("ANALYZE {}", intermediate_table_name(view_name)),
                    None,
                    &[],
                );
                let _ = client.update(
                    &format!("ANALYZE {}", quote_identifier(view_name)),
                    None,
                    &[],
                );

                let _ = client.update(
                    "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = $1",
                    None,
                    &[unsafe {
                        DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                );

                info!(
                    "pg_reflex: reconciled IMV '{}' (partitioned, {} children swapped)",
                    view_name,
                    src_children.len()
                );
                return "RECONCILED";
            }
        }

        if is_passthrough || end_query.is_empty() {
            // Passthrough: optimized refresh — drop indexes, TRUNCATE, INSERT, recreate, ANALYZE.
            // `indexname` is `name` (fixed 64B), not `text` — cast to text or
            // pgrx's `get_by_name::<&str, _>` silently returns None and we'd
            // skip every drop, leaving stale indexes that the recreate path
            // then no-ops via `CREATE … IF NOT EXISTS`. ~30s/100M-row IMV.
            //
            // Resolve via `to_regclass($1)` so a bare `view_name` honours the
            // session search_path (the legacy `(schemaname, tablename)` form
            // silently fell back to `public` for non-public tenants).
            // `qname` is the pre-quoted `"schema"."idx"` string used directly
            // by the DROP loop below — sourced from the same row so the index
            // we found is the index we drop, with no parallel schema-string
            // computation that could disagree with the catalog.
            let saved_indexes: Vec<(String, String, String)> = client
                .select(
                    "SELECT format('%I.%I', n.nspname, i.relname) AS qname, \
                            i.relname::TEXT AS indexname, \
                            pg_get_indexdef(ix.indexrelid) AS indexdef \
                     FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid \
                     JOIN pg_namespace n ON n.oid = i.relnamespace \
                     WHERE ix.indrelid = to_regclass($1)",
                    None,
                    &[unsafe {
                        DatumWithOid::new(
                            view_name.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    }],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    let qname = row
                        .get_by_name::<&str, _>("qname")
                        .unwrap_or(None)?
                        .to_string();
                    let name = row
                        .get_by_name::<&str, _>("indexname")
                        .unwrap_or(None)?
                        .to_string();
                    let def = row
                        .get_by_name::<&str, _>("indexdef")
                        .unwrap_or(None)?
                        .to_string();
                    Some((qname, name, def))
                })
                .collect();

            for (qname, _, _) in &saved_indexes {
                client
                    .update(&format!("DROP INDEX IF EXISTS {qname}"), None, &[])
                    .unwrap_or_report();
            }

            // Bulk refresh without indexes
            client
                .update(
                    &format!("TRUNCATE {}", quote_identifier(view_name)),
                    None,
                    &[],
                )
                .unwrap_or_report();
            client
                .update(
                    &format!("INSERT INTO {} {}", quote_identifier(view_name), base_query),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // Recreate all indexes
            for (_, _, idx_def) in &saved_indexes {
                client.update(idx_def, None, &[]).unwrap_or_report();
            }

            // ANALYZE
            client
                .update(
                    &format!("ANALYZE {}", quote_identifier(view_name)),
                    None,
                    &[],
                )
                .unwrap_or_report();
        } else {
            // Aggregate: rebuild intermediate + target
            // Drop pg_reflex-managed indexes first for faster bulk insert.
            // The registry's `aggregations` is written by pg_reflex itself
            // (via `serde_json::to_string` over an AggregationPlan); a
            // malformed value would mean catalog corruption, not user error.
            // Failing loudly there beats silently constructing a degenerate
            // plan that produces wrong output. `expect` is correct.
            let plan: aggregation::AggregationPlan = serde_json::from_str(&agg_json)
                .expect("pg_reflex: __reflex_ivm_reference.aggregations must be valid JSON written by pg_reflex");

            let intermediate = intermediate_table_name(view_name);

            // Collect and drop reflex-managed indexes on intermediate table.
            // `indexname` is `name`, not `text` — cast (see analogous note in
            // the passthrough path above).
            //
            // `intermediate_table_name` returns the already-qualified quoted
            // form (`"schema"."__reflex_intermediate_<view>"`), so feeding it
            // to `to_regclass($1)` resolves to the exact relation regardless
            // of session search_path.
            let int_indexes: Vec<String> = client
                .select(
                    "SELECT format('%I.%I', n.nspname, i.relname) AS qname \
                     FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid \
                     JOIN pg_namespace n ON n.oid = i.relnamespace \
                     WHERE ix.indrelid = to_regclass($1)",
                    None,
                    &[unsafe {
                        DatumWithOid::new(
                            intermediate.clone(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    }],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    row.get_by_name::<&str, _>("qname")
                        .unwrap_or(None)
                        .map(|s| s.to_string())
                })
                .collect();

            for qname in &int_indexes {
                client
                    .update(&format!("DROP INDEX IF EXISTS {qname}"), None, &[])
                    .unwrap_or_report();
            }

            // Collect ALL indexes on target table (save DDL for user-created ones).
            // Lookup via `to_regclass($1)` — the legacy `(schemaname, tablename)`
            // form fell back to `public` when `view_name` was bare, missing
            // non-public tenants.
            let tgt_saved_indexes: Vec<(String, String, String)> = client
                .select(
                    "SELECT format('%I.%I', n.nspname, i.relname) AS qname, \
                            i.relname::TEXT AS indexname, \
                            pg_get_indexdef(ix.indexrelid) AS indexdef \
                     FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid \
                     JOIN pg_namespace n ON n.oid = i.relnamespace \
                     WHERE ix.indrelid = to_regclass($1)",
                    None,
                    &[unsafe {
                        DatumWithOid::new(
                            view_name.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    }],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    let qname = row
                        .get_by_name::<&str, _>("qname")
                        .unwrap_or(None)?
                        .to_string();
                    let name = row
                        .get_by_name::<&str, _>("indexname")
                        .unwrap_or(None)?
                        .to_string();
                    let def = row
                        .get_by_name::<&str, _>("indexdef")
                        .unwrap_or(None)?
                        .to_string();
                    Some((qname, name, def))
                })
                .collect();

            for (qname, _, _) in &tgt_saved_indexes {
                client
                    .update(&format!("DROP INDEX IF EXISTS {qname}"), None, &[])
                    .unwrap_or_report();
            }

            // Bulk insert without indexes
            client
                .update(&format!("TRUNCATE {}", intermediate), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!("INSERT INTO {} {}", intermediate, base_query),
                    None,
                    &[],
                )
                .unwrap_or_report();
            client
                .update(
                    &format!("TRUNCATE {}", quote_identifier(view_name)),
                    None,
                    &[],
                )
                .unwrap_or_report();
            client
                .update(
                    &format!("INSERT INTO {} {}", quote_identifier(view_name), end_query),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // Recreate reflex-managed indexes (hash index on intermediate + target indexes)
            for index_ddl in build_indexes_ddl(view_name, &plan) {
                client.update(&index_ddl, None, &[]).unwrap_or_report();
            }

            // Recreate user-created indexes on target (skip reflex-managed ones already recreated above)
            for (_, idx_name, idx_def) in &tgt_saved_indexes {
                if idx_name.starts_with("idx__reflex_") || idx_name.starts_with("__reflex_") {
                    continue; // Already handled by build_indexes_ddl
                }
                client.update(idx_def, None, &[]).unwrap_or_report();
            }

            // ANALYZE intermediate so the planner has stats for any
            // subsequent incremental MERGE / dead-cleanup / target sync.
            //
            // 1.4.6 (P1) — target ANALYZE was 3-7 s on alp's 7.7 M-row IMV
            // and contributed nothing: pg_reflex's own SQL never plans
            // against the target (only end_query reads from it, and
            // operator queries are out of scope). User analytic queries
            // benefit from a separate ANALYZE if needed, but blocking the
            // reconcile path on it is wasteful. autovacuum picks up the
            // stats within a few minutes anyway.
            client
                .update(&format!("ANALYZE {}", intermediate), None, &[])
                .unwrap_or_report();
        }

        // Update last_update_date
        client
            .update(
                "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report();

        info!("pg_reflex: reconciled IMV '{}'", view_name);
        "RECONCILED"
    })
}

/// One row in the result set of `reflex_scheduled_reconcile`.
type ScheduledReconcileRow = (String, String, i64);

/// Reconcile every IMV whose `last_update_date` is older than `max_age_minutes`,
/// or which has not been updated yet. Designed to be invoked on a cadence by
/// pg_cron or another scheduler. Each per-IMV reconcile runs in isolation so
/// one failure does not block the rest.
///
/// Returns one row per attempted IMV: `(name, status, ms)` where `status` is
/// either `'RECONCILED'` or an error string, and `ms` is the wall time of the
/// reconcile call.
///
/// ```sql
/// -- Run every 15 minutes via pg_cron
/// SELECT cron.schedule('reflex-drift-scan', '*/15 * * * *',
///     'SELECT * FROM reflex_scheduled_reconcile(60)');
/// ```
#[pg_extern]
pub fn reflex_scheduled_reconcile(
    max_age_minutes: default!(i32, 60),
) -> TableIterator<'static, (name!(name, String), name!(status, String), name!(ms, i64))> {
    let candidates: Vec<String> = Spi::connect(|client| {
        let sql = "SELECT name FROM public.__reflex_ivm_reference \
                   WHERE COALESCE(enabled, TRUE) = TRUE \
                     AND (last_update_date IS NULL \
                          OR last_update_date < (CURRENT_TIMESTAMP - make_interval(mins => $1))) \
                   ORDER BY graph_depth, name";
        client
            .select(
                sql,
                None,
                &[unsafe {
                    DatumWithOid::new(max_age_minutes, PgBuiltInOids::INT4OID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<&str, _>("name")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
            .collect()
    });

    let mut out: Vec<ScheduledReconcileRow> = Vec::with_capacity(candidates.len());
    for name in candidates {
        let started = std::time::Instant::now();
        let result = reflex_reconcile(&name);
        let ms = started.elapsed().as_millis() as i64;
        let status = if result == "RECONCILED" {
            result.to_string()
        } else {
            warning!(
                "pg_reflex: scheduled reconcile of '{}' returned: {}",
                name,
                result
            );
            result.to_string()
        };
        out.push((name, status, ms));
    }

    TableIterator::new(out)
}

/// Refresh ALL IMVs that depend on a given source table or materialized view.
/// Processes IMVs in graph_depth order (L1 before L2).
pub(crate) fn refresh_imv_depending_on(source: &str) -> &'static str {
    // Collect IMV names in a separate SPI connection (closed before reconcile calls)
    let names: Vec<String> = Spi::connect(|client| {
        client
            .select(
                "SELECT name FROM public.__reflex_ivm_reference \
                 WHERE $1 = ANY(depends_on) AND enabled = TRUE \
                 ORDER BY graph_depth, name",
                None,
                &[unsafe {
                    DatumWithOid::new(source.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<&str, _>("name")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
            .collect()
    });

    if names.is_empty() {
        warning!("pg_reflex: no IMVs depend on '{}'", source);
        return "REFRESHED 0 IMVs";
    }

    let count = names.len();
    for name in &names {
        let result = reflex_reconcile(name);
        if result.starts_with("ERROR") {
            warning!("pg_reflex: failed to refresh '{}': {}", name, result);
        }
    }

    info!(
        "pg_reflex: refreshed {} IMV(s) depending on '{}'",
        count, source
    );
    Box::leak(format!("REFRESHED {} IMVs", count).into_boxed_str())
}
