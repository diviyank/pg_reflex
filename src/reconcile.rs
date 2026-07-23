use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;

use crate::aggregation;
use crate::query_decomposer::{intermediate_table_name, quote_identifier, split_qualified_name};
use crate::schema_builder::build_indexes_ddl;
use crate::validate_view_name;

/// Rebuild ONE IMV's intermediate + target from its own `base_query`.
///
/// The single-node primitive. [`reflex_reconcile`] is the entry point, and it
/// first repairs the sub-IMVs pg_reflex generated for `view_name`.
///
/// `drop_orphans` is forwarded to the pre-rebuild partition sync. The operator
/// entry point passes `true` (unchanged from 1.10.11); the recursive descent
/// passes `false` so reconciling a chain does not multiply an orphan-dropping
/// side effect the caller never asked for across every generated node in it.
fn reconcile_one(view_name: &str, drop_orphans: bool) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
    // Best-effort partition sync: keep the IMV partition set aligned with
    // the source before rebuilding.  No-op for unpartitioned IMVs.  Sync
    // failures are surfaced as a NOTICE but do not abort reconcile — the
    // operator may have deliberately stale state, and the rebuild itself
    // is the recovery action.
    let sync_msg = crate::partition::reflex_sync_partitions_impl(view_name, drop_orphans);
    if sync_msg.starts_with("ERROR") {
        pgrx::notice!("pg_reflex: sync before reconcile returned: {}", sync_msg);
    }
    Spi::connect_mut(|client| {
        let record = match crate::sql_writer::registry::read_imv(client, view_name) {
            Some(r) if r.enabled => r,
            _ => {
                warning!(
                    "pg_reflex: reconcile failed — IMV '{}' not found or disabled",
                    view_name
                );
                return "ERROR: IMV not found or disabled";
            }
        };

        let base_query: String = record.base_query.clone();
        let end_query: String = record.end_query.clone();
        let parsed_plan: Option<aggregation::AggregationPlan> = record.plan.clone();
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
                let storage_mode: String = record.storage_mode.clone();
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
                // attached children's stats. Passthrough IMVs have no
                // intermediate table (`execute_partition_swap_for_child`
                // skips it via the `end_query.is_empty()` guard), so
                // ANALYZE-ing `__reflex_intermediate_<view>` would raise
                // 42P01 and abort the whole reconcile.
                if !is_passthrough {
                    let _ = client.update(
                        &format!("ANALYZE {}", intermediate_table_name(view_name)),
                        None,
                        &[],
                    );
                }
                let _ = client.update(
                    &format!("ANALYZE {}", quote_identifier(view_name)),
                    None,
                    &[],
                );

                let _ = client.update(
                    "UPDATE public.__reflex_ivm_reference SET last_update_date = clock_timestamp() WHERE name = $1",
                    None,
                    &[unsafe {
                        DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                );

                let _ = client.update(
                    "UPDATE public.__reflex_ivm_reference \
                        SET known_stale = FALSE, stale_reason = NULL, stale_since = NULL WHERE name = $1",
                    None,
                    &[unsafe { DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }],
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
            // The registry's `aggregations` is written by pg_reflex itself
            // (via `serde_json::to_string` over an AggregationPlan); a
            // malformed value would mean catalog corruption, not user error.
            // We already parsed it once above as `parsed_plan` — if it was
            // invalid, we wouldn't have reached here (it would be None,
            // and passthrough would be false, landing us in the passthrough
            // branch, not the aggregate branch). So `parsed_plan` is
            // guaranteed to be Some(valid_plan) here.
            let plan: aggregation::AggregationPlan = parsed_plan
                .clone()
                .expect("pg_reflex: parsed_plan must be valid in aggregate branch");

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

        // Update last_update_date. clock_timestamp() (wall-clock at statement
        // time), NOT NOW() (transaction start): a reflex_scheduled_reconcile
        // batch runs many reconciles in one transaction, and NOW() stamped every
        // one of them identically, so the column could not date an individual
        // rebuild. clock_timestamp() stamps each rebuild's own end.
        client
            .update(
                "UPDATE public.__reflex_ivm_reference SET last_update_date = clock_timestamp() WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report();

        client
            .update(
                "UPDATE public.__reflex_ivm_reference \
                    SET known_stale = FALSE, stale_reason = NULL, stale_since = NULL WHERE name = $1",
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

/// Reconcile an IMV by rebuilding intermediate + target from scratch, first
/// rebuilding the sub-IMVs pg_reflex generated for it.
/// Use this as a safety net (manually or via pg_cron) to fix drift.
///
/// A decomposed IMV's `base_query` reads a node pg_reflex invented while
/// splitting one `create_reflex_ivm` call — a CTE sub-IMV, a set-op operand, a
/// DISTINCT-ON / window base. Rebuilding only the named IMV re-derives it from
/// that node's possibly-stale contents and reports `RECONCILED`, which is how a
/// chain whose generated child reads a MATERIALIZED VIEW (untriggerable, so the
/// child is frozen at create time) served month-old rows through every recovery
/// primitive an operator reaches for first. So: rebuild the generated
/// descendants bottom-up, then the named IMV.
///
/// A *user-declared* IMV dependency keeps the old non-recursive behaviour.
/// Reconciling someone else's IMV is not this call's business, and the operator
/// can name it directly.
pub(crate) fn reflex_reconcile(view_name: &str) -> &'static str {
    reflex_reconcile_with_orphans(view_name, true)
}

/// [`reflex_reconcile`] with explicit control over whether the pre-rebuild
/// partition sync may drop orphan IMV partitions.
///
/// `reflex_reconcile` hardcodes `true`, which is 1.10.11 behaviour and stays the
/// default for operators. `reflex_doctor` needs `false`: it refuses an F3 orphan
/// drop when the operator did not pass `drop_orphans`, and then reached the same
/// destruction through its F4 `reflex_reconcile` repair — a health tool removing a
/// partition after authorization was declined. This entry point is what lets that
/// caller honour its own gate.
pub(crate) fn reflex_reconcile_with_orphans(view_name: &str, drop_orphans: bool) -> &'static str {
    let mut child_failed = false;
    if inside_trigger() {
        warn_if_recursion_skipped_with_generated_children(view_name);
    } else {
        warn_about_skipped_decomposed_nodes(view_name);
        let children = generated_dependencies_shallowest_first(view_name);
        if !children.is_empty() {
            // Tell `reflex_on_ddl_command_end` which chain we are rebuilding, so
            // it suppresses the spurious "source altered — run reflex_rebuild_imv"
            // alarm (and, under 'error' policy, the abort) that our own
            // DISABLE/ENABLE TRIGGER on each generated child would otherwise
            // raise. Transaction-scoped; a different root reading a shared node
            // still warns.
            set_internal_reconcile_root(Some(view_name));
            for child in &children {
                let result = reconcile_generated_child_without_propagating(child);
                if result.starts_with("ERROR") {
                    child_failed = true;
                    warning!(
                        "pg_reflex: reconcile of generated sub-IMV '{}' of '{}' returned: {} \
                         — rebuilding '{}' anyway, but reporting failure",
                        child,
                        view_name,
                        result,
                        view_name
                    );
                }
            }
            set_internal_reconcile_root(None);
        }
    }

    let own = reconcile_one(view_name, drop_orphans);

    // The parent is rebuilt even when a child failed — that still repairs any
    // drift local to the parent, and leaving it untouched would be no fresher.
    // But the RETURN VALUE must not claim success: the parent has just been
    // re-derived from a sub-IMV we know is stale. `reflex_doctor`'s
    // verified-repair check reads only `known_stale` on the named IMV, which this
    // rebuild clears, and the failed child was never `known_stale` in the first
    // place, so this string is the only signal that survives.
    if child_failed {
        return "ERROR: generated sub-IMV reconcile failed";
    }
    own
}

/// WARN when the trigger-depth gate suppresses recursion for an IMV that
/// actually has generated children.
///
/// Returning `RECONCILED` there is the B1 bug in miniature — a rebuild that
/// re-derives from a child it did not refresh. It is the right behaviour on this
/// path (the delta that fired the trigger made the child fresh, and DDL on the
/// relation under its own statement trigger would error), but it must not be
/// silent, because an operator reading the log needs to know a nested reconcile
/// did less than the top-level one would.
fn warn_if_recursion_skipped_with_generated_children(view_name: &str) {
    let generated_children = generated_dependencies_shallowest_first(view_name);
    if !generated_children.is_empty() {
        warning!(
            "pg_reflex: reconcile of '{}' ran inside a trigger, so its {} generated \
             sub-IMV(s) ({}) were NOT rebuilt; run SELECT reflex_reconcile('{}') outside \
             a trigger to refresh the whole chain",
            view_name,
            generated_children.len(),
            generated_children.join(", "),
            view_name
        );
    }
}

/// TRUE when we are executing inside a data-change trigger.
///
/// pg_reflex calls `reflex_reconcile` from its own generated trigger bodies —
/// the high-selectivity "wipe" branch and the partition trip-cap branch in
/// `trigger/dispatch.rs`, and the partition-flush plpgsql in `lib.rs` /
/// `partition.rs`. Those callers must keep the single-node behaviour for two
/// independent reasons: their sources are fresh by construction (a delta just
/// arrived), and recursing would run `TRUNCATE` / `ALTER TABLE` against the very
/// relation whose statement trigger is mid-execution, with its transition tables
/// live. Since rebuilding a generated child is itself a 100%-of-rows change it
/// trips the wipe branch, so without this gate the recursion would re-enter
/// itself.
/// Publish (or clear, with `None`) the name of the chain reflex_reconcile is
/// rebuilding, in a transaction-scoped GUC the `reflex_on_ddl_command_end` event
/// trigger reads to suppress the stale alarm for our own trigger-suppression
/// ALTERs. `SET LOCAL` so it reverts at transaction end even on an error path;
/// the placeholder GUC name (contains a dot) needs no prior definition.
fn set_internal_reconcile_root(root: Option<&str>) {
    let sql = match root {
        Some(name) => format!(
            "SET LOCAL pg_reflex.internal_reconcile_root = '{}'",
            name.replace('\'', "''")
        ),
        None => "SET LOCAL pg_reflex.internal_reconcile_root = ''".to_string(),
    };
    let _ = Spi::run(&sql);
}

/// Fails CLOSED: an unreadable probe is treated as "inside a trigger", which
/// disables the recursion. The gate is mandatory, so a probe failure must not be
/// the thing that enables re-entrant DDL on a relation under its own trigger.
fn inside_trigger() -> bool {
    Spi::get_one::<i32>("SELECT pg_trigger_depth()")
        .unwrap_or(None)
        .unwrap_or(1)
        > 0
}

/// SQL predicate identifying a node the recursion must NOT hand to
/// `reconcile_one`.
///
/// `RegistryRow::decomposed` writes `aggregations = '{}'`; every node built by the
/// main create path stores a serialised `AggregationPlan`, which is never `'{}'`
/// even for a passthrough (`is_passthrough` alone makes the object non-empty). So
/// this selects exactly the decomposed rows — the UNION-ALL intermediate wrapper,
/// the set-op VIEW, the DISTINCT-ON and window VIEWs — and leaves passthrough
/// sub-IMVs in the recursion set where they belong.
///
/// Those nodes are not rebuildable from their registry row: the UNION-ALL wrapper
/// is `(__reflex_src_idx SMALLINT NOT NULL, <payload…>)` while its `base_query` is
/// `SELECT * FROM op0 UNION ALL SELECT * FROM op1`, so the passthrough branch's
/// `INSERT INTO <wrapper> SELECT * FROM …` puts N values into N+1 columns —
/// PostgreSQL left-shifts when the types line up and raises when they do not. The
/// set-op / DISTINCT-ON / window nodes are VIEWs, which cannot be TRUNCATEd at
/// all. Giving them a correct rebuild is a separate piece of work; until then the
/// recursion skips them and says so.
const REBUILDABLE_NODE: &str = "COALESCE(ref.aggregations::text, '{}') <> '{}'";

/// The extension-generated IMVs `view_name` transitively reads from, shallowest
/// first — the order they must be rebuilt in, since `depends_on_imv` points at
/// *shallower* nodes.
///
/// The walk starts from all of `view_name`'s IMV dependencies but descends only
/// through generated ones and returns only generated ones, so a user-declared
/// dependency is neither rebuilt nor traversed. `UNION` dedupes, so a CTE
/// sub-IMV shared by two siblings is rebuilt once.
///
/// Decomposed nodes ([`REBUILDABLE_NODE`]) are excluded, and the walk does not
/// descend *past* one either: rebuilding a wrapper's operands while leaving the
/// wrapper stale would feed the parent from a half-refreshed subtree, which is
/// worse than leaving that subtree exactly as a pre-1.11.0 reconcile left it.
fn generated_dependencies_shallowest_first(view_name: &str) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                &format!(
                    "WITH RECURSIVE gen AS ( \
                           SELECT unnest(COALESCE(r.depends_on_imv, ARRAY[]::TEXT[])) AS nm \
                             FROM public.__reflex_ivm_reference r WHERE r.name = $1 \
                         UNION \
                           SELECT unnest(COALESCE(c.depends_on_imv, ARRAY[]::TEXT[])) \
                             FROM public.__reflex_ivm_reference c JOIN gen ON c.name = gen.nm \
                            WHERE COALESCE(c.is_generated_sub_imv, FALSE) \
                              AND COALESCE(c.aggregations::text, '{{}}') <> '{{}}' \
                       ) \
                     SELECT ref.name FROM gen \
                       JOIN public.__reflex_ivm_reference ref ON ref.name = gen.nm \
                      WHERE COALESCE(ref.is_generated_sub_imv, FALSE) \
                        AND COALESCE(ref.enabled, TRUE) \
                        AND {REBUILDABLE_NODE} \
                      ORDER BY ref.graph_depth, ref.name"
                ),
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<&str, _>("name")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
            .collect()
    })
}

/// WARN for each generated sub-IMV the recursion had to skip because it is a
/// decomposed node with no safe rebuild.
///
/// Silence here would be the B1 failure mode again: a `RECONCILED` over a subtree
/// nothing refreshed. Naming the node gives the operator something to act on.
fn warn_about_skipped_decomposed_nodes(view_name: &str) {
    let skipped: Vec<String> = Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT ref.name FROM public.__reflex_ivm_reference parent \
                       JOIN public.__reflex_ivm_reference ref \
                         ON ref.name = ANY(COALESCE(parent.depends_on_imv, ARRAY[]::TEXT[])) \
                      WHERE parent.name = $1 \
                        AND COALESCE(ref.is_generated_sub_imv, FALSE) \
                        AND NOT ({REBUILDABLE_NODE}) \
                      ORDER BY ref.name"
                ),
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
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
    if !skipped.is_empty() {
        warning!(
            "pg_reflex: reconcile of '{}' skipped {} decomposed sub-IMV node(s) ({}) — \
             a UNION-ALL/set-op/DISTINCT-ON/window node has no rebuild from its registry \
             row, so that part of the chain was NOT refreshed. To refresh it, drop and \
             recreate the chain: SELECT drop_reflex_ivm('{}', true); then re-run the \
             original create_reflex_ivm.",
            view_name,
            skipped.len(),
            skipped.join(", "),
            view_name
        );
    }
}

/// Rebuild a generated sub-IMV with its user triggers suppressed, so the rebuild
/// does not propagate into its consumers.
///
/// `reconcile_one` does `TRUNCATE` + full `INSERT`. Left to propagate, the
/// TRUNCATE empties the consumer immediately (the AFTER TRUNCATE trigger fires
/// even for DEFERRED IMVs) while the INSERT only *stages* a delta in DEFERRED
/// mode — so a consumer rebuilt in between ends up correct and then has the
/// staged full-table delta applied again at COMMIT, doubling its aggregates.
/// Suppression removes the delta rather than trying to net it out, and as a side
/// effect skips a full-size incremental flush whose result the consumer's own
/// rebuild discards anyway.
///
/// Nothing is lost for the chain being rebuilt: every consumer of a generated
/// sub-IMV within it is either a deeper member of the same generated set or the
/// named IMV itself, and [`reflex_reconcile`] rebuilds both explicitly. A
/// consumer OUTSIDE that set — a hand-written IMV reading a generated node, or a
/// generated node shared with a different root — does miss this delta, and the
/// `reflex_on_ddl_command_end` alarm the `ALTER TABLE` trips flags it stale, which
/// for that consumer is the correct signal.
///
/// `ALTER TABLE` is transactional, so a hard error inside `reconcile_one` rolls
/// the DISABLE back with everything else; a soft `"ERROR: …"` return still
/// reaches the ENABLE.
///
/// The `relkind` probe quotes `child` before `to_regclass`. Unquoted, PostgreSQL
/// down-cases, so every mixed-case IMV resolved to NULL, `triggerable` came out
/// false, and the child was rebuilt with triggers LIVE — silently reinstating the
/// DEFERRED double-count this function exists to prevent. The codebase persists
/// sub-IMV sources double-quoted for exactly this reason
/// (`query_decomposer.rs:18-23`).
fn reconcile_generated_child_without_propagating(child: &str) -> &'static str {
    let quoted = quote_identifier(child);
    let triggerable = Spi::get_one::<bool>(&format!(
        "SELECT COALESCE((SELECT relkind IN ('r','p') FROM pg_class \
          WHERE oid = to_regclass('{}')), FALSE)",
        quoted.replace('\'', "''")
    ))
    .unwrap_or(None)
    .unwrap_or(false);

    if !triggerable {
        return reconcile_one(child, false);
    }

    Spi::run(&format!("ALTER TABLE {} DISABLE TRIGGER USER", quoted)).unwrap_or_report();
    let result = reconcile_one(child, false);
    Spi::run(&format!("ALTER TABLE {} ENABLE TRIGGER USER", quoted)).unwrap_or_report();
    result
}

/// One row in the result set of `reflex_scheduled_reconcile`.
type ScheduledReconcileRow = (String, String, i64, i64);

/// Reconcile stale IMVs on a cadence, as a RESUMABLE batched driver.
///
/// Reconciles up to `batch_size` IMVs whose `last_update_date` is older than
/// `max_age_minutes` (or never set), optionally scoped to one `target_schema`.
/// Each per-IMV reconcile runs in isolation so one soft failure does not block
/// the rest.
///
/// Why a driver, not one call that does everything (B6): a single loop over a
/// 190-IMV / 415-schema registry cannot finish under a sane `statement_timeout`,
/// and a timeout discards every rebuild already done — pgrx cannot commit
/// per-IMV in-process (a set-returning function runs in an atomic context, and
/// pgrx 0.18 exposes no SPI transaction control), so the durable unit has to be
/// the CALL. Bound the work with `batch_size` so each call finishes and commits,
/// and let the scheduler call again. Resumability needs no cursor: a reconciled
/// IMV's `last_update_date` is advanced (to `clock_timestamp()`) past the age
/// gate, so it drops out of the candidate set and the next call continues with
/// the next stale IMV.
///
/// Arguments (all optional; defaults reproduce the pre-1.11.0 "one call does
/// everything" behaviour on a small registry):
/// * `max_age_minutes` (default 60) — freshness threshold.
/// * `batch_size` (default 0 = unlimited) — max IMVs attempted per call.
/// * `target_schema` (default `''` = all schemas) — restrict to one tenant's
///   `target_schema`; legacy rows with NULL `target_schema` count as `public`.
///
/// Returns one row per ATTEMPTED IMV: `(name, status, ms, remaining)` where
/// `status` is `'RECONCILED'` or an error string (PS-1 surfaces a decomposed
/// chain's child failure here — it is reported, never swallowed), `ms` is the
/// reconcile wall time, and `remaining` is the number of candidates this call
/// DEFERRED because `batch_size` capped it.
///
/// `remaining = 0` is NOT a health signal — it only means this call attempted
/// every remaining candidate, not that they all succeeded. A failed / poison
/// IMV keeps its stale `last_update_date`, stays a candidate, and is attempted
/// again next call while `remaining` may already read 0. Monitoring must check
/// per-row `status` for failures, not just `remaining`.
///
/// The age gate is the resume cursor, so `remaining` reaching 0 is meaningful
/// only under a POSITIVE `max_age_minutes`: a reconciled IMV's `clock_timestamp()`
/// write then falls inside the fresh window and it drops out. With
/// `max_age_minutes <= 0` every enabled IMV is perpetually eligible, so a bounded
/// (`batch_size > 0`) sweep never reports `remaining = 0` — it instead rotates
/// oldest-first through the registry each call (continuous maintenance). Do not
/// loop-until-zero on a non-positive gate; drive it from a scheduler tick.
///
/// ```sql
/// -- Small registry: unchanged from 1.10.11 — one call sweeps everything.
/// SELECT cron.schedule('reflex-drift-scan', '*/15 * * * *',
///     'SELECT * FROM reflex_scheduled_reconcile(60)');
///
/// -- Large registry: bound each tick (positive gate); repeat until remaining = 0.
/// SELECT cron.schedule('reflex-drift-scan', '*/5 * * * *',
///     'SELECT * FROM reflex_scheduled_reconcile(60, 25)');
/// ```
#[pg_extern]
pub fn reflex_scheduled_reconcile(
    max_age_minutes: default!(i32, 60),
    batch_size: default!(i32, 0),
    target_schema: default!(&str, "''"),
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(status, String),
        name!(ms, i64),
        name!(remaining, i64),
    ),
> {
    let candidates: Vec<String> = Spi::connect(|client| {
        // Skip a generated sub-IMV when a candidate that transitively reads it is
        // also in this batch: `reflex_reconcile` on that candidate already
        // rebuilds it, and the scan visits children first, so without this filter
        // a depth-d decomposed chain costs 1+2+…+d rebuilds instead of d.
        // Measured at 15 rebuilds for a 4-generated-child chain — 3x, and
        // quadratic in depth.
        //
        // The filter is deliberately "covered by a candidate", not "is
        // generated": a generated node whose consumer is NOT stale enough to be a
        // candidate stays in the batch, so it is still reconciled rather than
        // silently skipped. A covered child whose parent then FAILS is not
        // orphaned: the failed parent keeps a stale `last_update_date`, so it
        // stays a candidate and covers (and retries) the child on the next call —
        // and the candidate set is re-derived live every call, so the resumable
        // cursor inherits no hole.
        //
        // `$2` scopes to one tenant's `target_schema` (empty string = all). The
        // covered CTE derives from the already-scoped candidate set, so a covered
        // child is scoped with its parent.
        //
        // ORDER BY keeps `graph_depth` primary (children before parents), then
        // rotates oldest-first within a depth via `last_update_date`. A depth
        // level holds only mutually independent IMVs — `graph_depth` exceeds
        // every dependency's depth, so same-depth nodes never depend on each
        // other — hence reordering within a level cannot perturb dependency
        // order. The staleness tiebreaker is what lets a bounded (`batch_size`)
        // call advance through the whole registry instead of re-doing the same
        // leading rows and starving the tail when the age gate stays open on
        // just-reconciled IMVs (max_age <= 0). `name` is the final, deterministic
        // tiebreaker.
        let sql = "WITH candidate AS ( \
                       SELECT name, graph_depth, depends_on_imv, last_update_date \
                         FROM public.__reflex_ivm_reference \
                        WHERE COALESCE(enabled, TRUE) = TRUE \
                          AND ($2 = '' OR COALESCE(target_schema, 'public') = $2) \
                          AND (last_update_date IS NULL \
                               OR last_update_date < (CURRENT_TIMESTAMP - make_interval(mins => $1))) \
                   ), \
                   covered AS ( \
                       WITH RECURSIVE reachable AS ( \
                             SELECT unnest(COALESCE(c.depends_on_imv, ARRAY[]::TEXT[])) AS nm \
                               FROM candidate c \
                           UNION \
                             SELECT unnest(COALESCE(r.depends_on_imv, ARRAY[]::TEXT[])) \
                               FROM public.__reflex_ivm_reference r JOIN reachable ON r.name = reachable.nm \
                              WHERE COALESCE(r.is_generated_sub_imv, FALSE) \
                       ) \
                       SELECT reachable.nm AS name \
                         FROM reachable \
                         JOIN public.__reflex_ivm_reference ref ON ref.name = reachable.nm \
                        WHERE COALESCE(ref.is_generated_sub_imv, FALSE) \
                   ) \
                   SELECT name FROM candidate \
                    WHERE name NOT IN (SELECT name FROM covered) \
                    ORDER BY graph_depth, last_update_date ASC NULLS FIRST, name";
        client
            .select(
                sql,
                None,
                &[
                    unsafe {
                        DatumWithOid::new(max_age_minutes, PgBuiltInOids::INT4OID.oid().value())
                    },
                    unsafe {
                        DatumWithOid::new(
                            target_schema.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                ],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<&str, _>("name")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
            .collect()
    });

    // batch_size <= 0 means unlimited (one call sweeps everything — the
    // pre-1.11.0 default). Otherwise attempt at most batch_size, and report the
    // rest as deferred to a future call.
    let total = candidates.len();
    let limit = if batch_size <= 0 {
        total
    } else {
        (batch_size as usize).min(total)
    };
    let remaining = (total - limit) as i64;

    let mut out: Vec<ScheduledReconcileRow> = Vec::with_capacity(limit);
    for name in candidates.into_iter().take(limit) {
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
        out.push((name, status, ms, remaining));
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
