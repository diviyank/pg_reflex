use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;

use crate::aggregation;
use crate::query_decomposer::{
    intermediate_table_name, quote_identifier, safe_identifier, split_qualified_name,
};
use crate::schema_builder::build_indexes_ddl;
use crate::validate_view_name;

/// Reconcile an IMV by rebuilding intermediate + target from scratch.
/// Use this as a safety net (manually or via pg_cron) to fix drift.
pub(crate) fn reflex_reconcile(view_name: &str) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
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

        let is_passthrough =
            if let Ok(plan) = serde_json::from_str::<aggregation::AggregationPlan>(&agg_json) {
                plan.is_passthrough
            } else {
                false
            };

        if is_passthrough || end_query.is_empty() {
            // Passthrough: optimized refresh — drop indexes, TRUNCATE, INSERT, recreate, ANALYZE
            let (tgt_schema, tgt_name) = split_qualified_name(view_name);
            let tgt_schema_str = tgt_schema.unwrap_or("public");

            // Save and drop all indexes on target
            let saved_indexes: Vec<(String, String)> = client
                .select(
                    "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
                    None,
                    &[
                        unsafe { DatumWithOid::new(tgt_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(tgt_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    let name = row.get_by_name::<&str, _>("indexname").unwrap_or(None)?.to_string();
                    let def = row.get_by_name::<&str, _>("indexdef").unwrap_or(None)?.to_string();
                    Some((name, def))
                })
                .collect();

            for (idx_name, _) in &saved_indexes {
                client
                    .update(
                        &format!(
                            "DROP INDEX IF EXISTS \"{}\".\"{}\"",
                            tgt_schema_str, idx_name
                        ),
                        None,
                        &[],
                    )
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
            for (_, idx_def) in &saved_indexes {
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
            // Aggregate: rebuild intermediate via CTAS+rename, target via
            // TRUNCATE+INSERT.
            //
            // The intermediate is internal — no user-visible triggers, FKs,
            // or grants — so DROP+RENAME of its heap is safe. CTAS gives PG
            // the bulkload write path and avoids the "empty-table visible
            // to readers" window of TRUNCATE+INSERT.
            //
            // The target table carries user grants, the user's analytic
            // queries, and (critically) pg_reflex's own propagation
            // triggers for chained IMVs. DROP+RENAME would lose all three.
            // Keep TRUNCATE+INSERT for it.
            let plan: aggregation::AggregationPlan = serde_json::from_str(&agg_json)
                .unwrap_or_else(|_| aggregation::AggregationPlan {
                    group_by_columns: vec![],
                    intermediate_columns: vec![],
                    end_query_mappings: vec![],
                    has_distinct: false,
                    needs_ivm_count: false,
                    distinct_columns: vec![],
                    is_passthrough: false,
                    passthrough_columns: vec![],
                    passthrough_key_mappings: std::collections::HashMap::new(),
                    having_clause: None,
                    not_null_columns: std::collections::HashSet::new(),
                    group_by_aliases: std::collections::HashMap::new(),
                    output_column_order: vec![],
                    imv_relevant_columns: std::collections::HashMap::new(),
                    imv_relevant_where: std::collections::HashMap::new(),
                    source_join_keys: std::collections::HashMap::new(),
                });

            let intermediate = intermediate_table_name(view_name);
            let (_, bare_view) = split_qualified_name(view_name);
            let int_bare = safe_identifier(&format!("__reflex_intermediate_{}", bare_view));
            let int_unquoted = intermediate.replace('"', "");
            let (int_schema, _) = split_qualified_name(&int_unquoted);
            let int_schema_str = int_schema.unwrap_or("public");

            let (tgt_schema, tgt_name) = split_qualified_name(view_name);
            let tgt_schema_str = tgt_schema.unwrap_or("public");

            // Probe intermediate persistence + owner so the new heap matches.
            // relpersistence: 'p' = permanent (logged), 'u' = unlogged.
            let int_logged: bool = client
                .select(
                    "SELECT relpersistence::TEXT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = $1 AND c.relname = $2",
                    None,
                    &[
                        unsafe { DatumWithOid::new(int_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(int_bare.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                )
                .unwrap_or_report()
                .next()
                .and_then(|r| r.get_by_name::<&str, _>("relpersistence").unwrap_or(None).map(|s| s.to_string()))
                .map(|p| p == "p")
                .unwrap_or(false);

            let int_owner: Option<String> = client
                .select(
                    "SELECT pg_get_userbyid(c.relowner) AS owner FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = $1 AND c.relname = $2",
                    None,
                    &[
                        unsafe { DatumWithOid::new(int_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(int_bare.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                )
                .unwrap_or_report()
                .next()
                .and_then(|r| r.get_by_name::<&str, _>("owner").unwrap_or(None).map(|s| s.to_string()));

            // Temp name for the new intermediate heap.
            let int_new_bare =
                safe_identifier(&format!("__reflex_intermediate_{}_recon", bare_view));
            let int_new_q = format!("\"{}\".\"{}\"", int_schema_str, int_new_bare);

            // Cleanup any leftover _recon table from a prior interrupted run.
            client
                .update(
                    &format!("DROP TABLE IF EXISTS {} CASCADE", int_new_q),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // === Phase A: rebuild intermediate via LIKE-copy + INSERT + atomic rename ===
            //
            // `CREATE TABLE ... AS base_query` would infer column types from
            // the SELECT — for an AVG IMV with INTEGER source, the inferred
            // `__sum_val` would be BIGINT, breaking the SUM/COUNT division
            // in end_query (integer truncation). Use LIKE INCLUDING ALL so
            // the new heap matches the old's explicit column types exactly,
            // then populate via INSERT.
            let create_int = if int_logged {
                "CREATE TABLE"
            } else {
                "CREATE UNLOGGED TABLE"
            };
            client
                .update(
                    &format!(
                        "{} {} (LIKE {} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING STATISTICS) WITH (fillfactor=70)",
                        create_int, int_new_q, intermediate
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
            client
                .update(
                    &format!("INSERT INTO {} {}", int_new_q, base_query),
                    None,
                    &[],
                )
                .unwrap_or_report();
            if let Some(owner) = &int_owner {
                client
                    .update(
                        &format!("ALTER TABLE {} OWNER TO \"{}\"", int_new_q, owner),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            }

            // Atomic swap: drop old heap, rename new heap to canonical name.
            // Both happen in the same SPI transaction, so other backends see
            // either the old table or the new one — never an empty/half-built
            // state. The intra-swap DROP is suppressed by the
            // reconcile_in_progress GUC so chained IMVs don't cascade-drop.
            client
                .update(&format!("DROP TABLE {} CASCADE", intermediate), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!("ALTER TABLE {} RENAME TO \"{}\"", int_new_q, int_bare),
                    None,
                    &[],
                )
                .unwrap_or_report();

            // === Phase B: target rebuild via TRUNCATE+INSERT ===
            // Save all user-created target indexes (we drop them for bulk
            // INSERT speed and recreate after). Reflex-managed ones are
            // rebuilt from `build_indexes_ddl`.
            let tgt_saved_indexes: Vec<(String, String)> = client
                .select(
                    "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
                    None,
                    &[
                        unsafe { DatumWithOid::new(tgt_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(tgt_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                )
                .unwrap_or_report()
                .filter_map(|row| {
                    let name = row.get_by_name::<&str, _>("indexname").unwrap_or(None)?.to_string();
                    let def = row.get_by_name::<&str, _>("indexdef").unwrap_or(None)?.to_string();
                    Some((name, def))
                })
                .collect();

            for (idx_name, _) in &tgt_saved_indexes {
                client
                    .update(
                        &format!(
                            "DROP INDEX IF EXISTS \"{}\".\"{}\"",
                            tgt_schema_str, idx_name
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            }

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

            // === Phase C: rebuild indexes ===
            // Reflex-managed indexes on both intermediate and target.
            for index_ddl in build_indexes_ddl(view_name, &plan) {
                client.update(&index_ddl, None, &[]).unwrap_or_report();
            }
            // User-created indexes on target — skip reflex-managed names
            // already handled above.
            for (idx_name, idx_def) in &tgt_saved_indexes {
                if idx_name.starts_with("idx__reflex_") || idx_name.starts_with("__reflex_") {
                    continue;
                }
                client.update(idx_def, None, &[]).unwrap_or_report();
            }

            // ANALYZE intermediate so the planner has stats for any
            // subsequent incremental MERGE / dead-cleanup / target sync.
            //
            // 1.4.6 (P1) — target ANALYZE was 3-7 s on alp's 7.7 M-row IMV
            // and contributed nothing: pg_reflex's own SQL never plans
            // against the target (only end_query reads from it, and
            // operator queries are out of scope).
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
