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
    Spi::connect_mut(|client| {
        let rows = client
            .select(
                "SELECT base_query, end_query, aggregations \
                 FROM public.__reflex_ivm_reference WHERE name = $1 AND enabled = TRUE",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

        if rows.is_empty() {
            warning!("pg_reflex: reconcile failed — IMV '{}' not found or disabled", view_name);
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

        let is_passthrough = if let Ok(plan) =
            serde_json::from_str::<aggregation::AggregationPlan>(&agg_json)
        {
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
                client.update(&format!("DROP INDEX IF EXISTS \"{}\".\"{}\"", tgt_schema_str, idx_name), None, &[]).unwrap_or_report();
            }

            // Bulk refresh without indexes
            client
                .update(&format!("TRUNCATE {}", quote_identifier(view_name)), None, &[])
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
            client.update(&format!("ANALYZE {}", quote_identifier(view_name)), None, &[]).unwrap_or_report();
        } else {
            // Aggregate: rebuild intermediate + target
            // Drop pg_reflex-managed indexes first for faster bulk insert
            let plan: aggregation::AggregationPlan =
                serde_json::from_str(&agg_json).unwrap_or_else(|_| {
                    aggregation::AggregationPlan {
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
                    }
                });

            let intermediate = intermediate_table_name(view_name);
            let (_, bare_view) = split_qualified_name(view_name);
            let int_unquoted = intermediate.replace('"', "");
            let (int_schema, _) = split_qualified_name(&int_unquoted);
            let int_schema_str = int_schema.unwrap_or("public");

            // Collect and drop reflex-managed indexes on intermediate table
            let int_indexes: Vec<String> = client
                .select(
                    "SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
                    None,
                    &[
                        unsafe { DatumWithOid::new(int_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                        unsafe { DatumWithOid::new(format!("__reflex_intermediate_{}", bare_view), PgBuiltInOids::TEXTOID.oid().value()) },
                    ],
                )
                .unwrap_or_report()
                .filter_map(|row| row.get_by_name::<&str, _>("indexname").unwrap_or(None).map(|s| s.to_string()))
                .collect();

            for idx in &int_indexes {
                client.update(&format!("DROP INDEX IF EXISTS \"{}\".\"{}\"", int_schema_str, idx), None, &[]).unwrap_or_report();
            }

            // Collect ALL indexes on target table (save DDL for user-created ones)
            let (tgt_schema, tgt_name) = split_qualified_name(view_name);
            let tgt_schema_str = tgt_schema.unwrap_or("public");
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
                client.update(&format!("DROP INDEX IF EXISTS \"{}\".\"{}\"", tgt_schema_str, idx_name), None, &[]).unwrap_or_report();
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
                .update(&format!("TRUNCATE {}", quote_identifier(view_name)), None, &[])
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
            for (idx_name, idx_def) in &tgt_saved_indexes {
                if idx_name.starts_with("idx__reflex_") || idx_name.starts_with("__reflex_") {
                    continue; // Already handled by build_indexes_ddl
                }
                client.update(idx_def, None, &[]).unwrap_or_report();
            }

            // ANALYZE for query planner
            client.update(&format!("ANALYZE {}", intermediate), None, &[]).unwrap_or_report();
            client.update(&format!("ANALYZE {}", quote_identifier(view_name)), None, &[]).unwrap_or_report();
        }

        // Update last_update_date
        client
            .update(
                "UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        view_name.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report();

        info!("pg_reflex: reconciled IMV '{}'", view_name);
        "RECONCILED"
    })
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
                 ORDER BY graph_depth",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        source.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
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
