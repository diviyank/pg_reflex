use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;

use crate::aggregation;
use crate::query_decomposer::{intermediate_table_name, quote_identifier, split_qualified_name};
use crate::schema_builder::build_indexes_ddl;
use crate::validate_view_name;

/// Recreate `__reflex_intermediate_<view>` and its companions when the IMV expects
/// an intermediate and it is absent — dropped by hand, lost to a
/// `DROP … CASCADE`, or removed with a schema.
///
/// Until 1.11.1 nothing in the extension re-issued that DDL (it was emitted only at
/// create time), so `internal-tables-exist` reported the absence at Error severity
/// and prescribed `reflex_rebuild_imv`, a literal alias for [`reflex_reconcile`]
/// (`src/lib.rs:823`) — which could not repair it: the partitioned path needs an
/// existing parent to ATTACH swap children to (`ERROR: partition reconcile failed`,
/// `missing intermediate bound for child …`) and the unpartitioned path raises
/// 42P01 on `TRUNCATE {intermediate}`. An operator following the tool's own
/// instruction retried forever.
///
/// Runs BEFORE the pre-rebuild partition sync: `reflex_sync_partitions` gates every
/// intermediate-child DDL on a `to_regclass` probe of the parent
/// (`src/partition.rs:1055`), so with the parent restored its existing loop mirrors
/// the partition tree and no new partition code is needed here.
///
/// Everything needed is in the registry row — `aggregations` (the plan, with the
/// column types create time resolved), `storage_mode`, and the sources in
/// `depends_on` — so the missing `create_args` of a generated sub-IMV does not block
/// recovery. `column_types` is reconstructed the way create time built it: from the
/// sources' catalog entries, then from a temp view over `base_query`, whose output
/// column names ARE the intermediate's column names.
///
/// Gated on [`crate::sql_writer::registry::owns_intermediate`] — the same predicate
/// the audit classifies with — so it cannot build an intermediate for a passthrough
/// IMV or a decomposed wrapper (the mirror image of the phantom drift PS-9 removed).
///
/// Two INDEPENDENT probes, in build order: the intermediate first, then the
/// group-capture companions — which a CTAS leaves with no catalog dependency on
/// the intermediate, so either side can be dropped without the other and a single
/// probe of the intermediate would silently skip half the damage.
fn heal_missing_intermediate(view_name: &str) {
    heal_missing_intermediate_table(view_name);
    heal_missing_group_capture_tables(view_name);
}

fn heal_missing_intermediate_table(view_name: &str) {
    let intermediate = intermediate_table_name(view_name);

    let record = Spi::connect(|client| {
        if relation_present(client, &intermediate) {
            return None;
        }
        crate::sql_writer::registry::read_imv(client, view_name)
    });
    let record = match record {
        Some(r) if r.enabled && r.owns_intermediate() => r,
        _ => return,
    };
    // A row whose `aggregations` will not parse carries no shape to rebuild from;
    // leave it to the audit rather than guessing one.
    let plan = match record.plan.clone() {
        Some(p) => p,
        None => return,
    };

    // The three inputs create time uses, in create time's order — the map is
    // `or_insert`-filled, so earlier sources win and adding later ones is purely
    // additive (`materialize_aggregate`, `src/create_ivm/mod.rs`).
    //
    // The `base_query` augment is load-bearing, not belt-and-braces: an EXPRESSION
    // group key such as `date_trunc('day', ts)` exists in no catalog, so without it
    // `resolve_column_type` falls through to its NUMERIC default and the healed key
    // column is `numeric` where create time made it `timestamptz`.
    //
    // HAZARD, deliberately guarded below: `augment_column_types_from_query` returns
    // SILENTLY when its `CREATE TEMP VIEW` fails (`src/create_ivm/admin.rs:513-515`).
    // At create time a wrongly-typed intermediate dies immediately on the following
    // `INSERT INTO {intermediate} {base_query}`, which raises and rolls the whole
    // CREATE back. At heal time that protection is absent on the partitioned path:
    // `reconcile_one` turns a failed swap fill into a returned `ERROR: …` STRING
    // rather than a raise, so the transaction is not rolled back and a wrongly-typed
    // intermediate would be COMMITTED — and the next reconcile would skip the heal
    // (the relation now exists) and fail forever. So instead of trusting the map, we
    // refuse to build a shape we cannot type.
    let mut column_types = Spi::connect(|client| {
        crate::create_ivm::query_column_types_from_catalog_with_per_source(
            client,
            &record.depends_on,
        )
        .0
    });
    crate::create_ivm::augment_column_types_from_query(&record.base_query, &mut column_types);
    if let Some((sql_query, _)) =
        Spi::connect(|client| crate::sql_writer::registry::read_imv_for_rebuild(client, view_name))
    {
        if !sql_query.is_empty() {
            crate::create_ivm::augment_column_types_from_query(&sql_query, &mut column_types);
        }
    }

    if let Some(unresolved) = first_unresolved_group_key(&plan, &column_types) {
        warning!(
            "pg_reflex: cannot recreate the missing internal table(s) of IMV '{}' — \
             the type of group key '{}' could not be resolved from its sources or its \
             stored queries, and guessing one would commit an intermediate that no \
             later reconcile can repair. Recreate the IMV from its original \
             definition instead.",
            view_name,
            unresolved
        );
        return;
    }

    let logged = !record.storage_mode.eq_ignore_ascii_case("UNLOGGED");
    let intermediate_ddl = match crate::schema_builder::build_intermediate_table_ddl(
        view_name,
        &plan,
        &column_types,
        logged,
    ) {
        Some(ddl) => ddl,
        None => return,
    };

    let healed = Spi::connect_mut(|client| {
        // Serialize the DDL against a concurrent healer of the same IMV, then
        // RE-PROBE under the lock. `CREATE TABLE IF NOT EXISTS` is not race-safe:
        // two sessions that both saw `to_regclass` NULL (each other's CREATE being
        // uncommitted) would both emit it and the loser would abort on
        // `pg_type_typname_nsp_index`. Because the lock is transaction-scoped, the
        // second session cannot acquire it until the first has committed — at which
        // point the re-probe sees the table and it skips the DDL entirely.
        //
        // INVARIANT: every IMV-name advisory lock in pg_reflex uses the two-key
        // `(hashtext(name), hashtext(reverse(name)))` form (trigger bodies, the
        // deferred flush in `trigger/deferred.rs`, the partition flush in `lib.rs`,
        // and `reflex_sync_partitions` at `src/partition.rs:1099`). A one-key
        // `bigint` lock occupies a different advisory-lock space in PostgreSQL and
        // would never mutually exclude with those, so it MUST be the two-key form.
        //
        // Taken here rather than at the top of the function so the healthy path —
        // every reconcile of an IMV whose intermediate is present — keeps its
        // current locking behaviour exactly. Only a session that is about to emit
        // DDL waits.
        //
        // Considered and accepted: `build_indexes_ddl` also emits
        // `CREATE INDEX IF NOT EXISTS idx__reflex_target_<view>` on the LIVE target,
        // so a heal briefly holds a ShareLock there — and the heal is reachable from
        // the COMMIT-time deferred flush. It is bounded (one index on an
        // already-existing table, only on the absent-intermediate path) and it is
        // the same DDL create time issues, but a reader should know it was weighed.
        let _ = client.update(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext(reverse($1)))",
            None,
            &[unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        );
        if relation_present(client, &intermediate) {
            return false;
        }

        client
            .update(&intermediate_ddl, None, &[])
            .unwrap_or_report();
        if let Some(scratch_ddl) =
            crate::schema_builder::build_delta_scratch_table_ddl(view_name, &plan, &column_types)
        {
            client.update(&scratch_ddl, None, &[]).unwrap_or_report();
        }
        for ddl in build_indexes_ddl(view_name, &plan) {
            client.update(&ddl, None, &[]).unwrap_or_report();
        }
        for (_, ddl) in crate::schema_builder::build_group_capture_ddl(view_name, &plan) {
            client.update(&ddl, None, &[]).unwrap_or_report();
        }
        true
    });

    if healed {
        warning!(
            "pg_reflex: recreated the missing internal table(s) of IMV '{}' ({} and companions). \
             The IMV was not being maintained while they were absent; this reconcile refills it.",
            view_name,
            intermediate
        );
    }
}

/// Recreate the group-capture companions — `__reflex_affected_<view>` and, for a
/// top-K MIN/MAX plan, `__reflex_shrunk_<view>` — that are absent while the
/// intermediate itself is present.
///
/// They are built as `CREATE UNLOGGED TABLE … AS SELECT … FROM <intermediate>
/// WHERE FALSE`, and a CTAS records NO catalog dependency on the relation it read.
/// So `DROP TABLE __reflex_affected_<view>` leaves the intermediate untouched and
/// vice versa: the two absences are independent, and a heal gated on the
/// intermediate alone can never repair the companion-only shape. Nothing else in
/// the extension re-issues that DDL, yet `internal-tables-exist` reports a missing
/// `__reflex_affected_<view>` at Error severity and prints `reflex_rebuild_imv` —
/// an alias for [`reflex_reconcile`] — as its remedy, so before this the finding
/// could not be cleared by the fix the tool itself prescribed.
///
/// Runs AFTER [`heal_missing_intermediate_table`] so the intermediate these DDLs
/// select from is present whenever it can be. When it is still absent — that heal
/// refused an untypable shape, or the row carries no rebuildable plan — this is a
/// no-op rather than a 42P01: the missing intermediate is the finding to act on.
fn heal_missing_group_capture_tables(view_name: &str) {
    let record =
        match Spi::connect(|client| crate::sql_writer::registry::read_imv(client, view_name)) {
            Some(r) if r.enabled && r.owns_intermediate() => r,
            _ => return,
        };
    let plan = match record.plan {
        Some(p) => p,
        None => return,
    };
    let capture_ddl = crate::schema_builder::build_group_capture_ddl(view_name, &plan);
    if capture_ddl.is_empty() {
        return;
    }
    let intermediate = intermediate_table_name(view_name);

    let missing: Vec<(String, String)> = Spi::connect(|client| {
        if !relation_present(client, &intermediate) {
            return Vec::new();
        }
        capture_ddl
            .into_iter()
            .filter(|(name, _)| !relation_present(client, name))
            .collect()
    });
    if missing.is_empty() {
        return;
    }

    let recreated = Spi::connect_mut(|client| {
        // Same two-key advisory lock and re-probe-under-the-lock pattern as the
        // intermediate heal above, for the same reason: `CREATE TABLE IF NOT
        // EXISTS … AS SELECT` is not race-safe against a concurrent healer whose
        // CREATE is still uncommitted.
        let _ = client.update(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext(reverse($1)))",
            None,
            &[unsafe {
                DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        );
        let mut recreated: Vec<String> = Vec::new();
        for (name, ddl) in &missing {
            if relation_present(client, name) {
                continue;
            }
            client.update(ddl, None, &[]).unwrap_or_report();
            recreated.push(name.clone());
        }
        recreated
    });

    if !recreated.is_empty() {
        warning!(
            "pg_reflex: recreated the missing group-capture table(s) of IMV '{}' ({}). \
             Maintenance could not record affected groups while they were absent; \
             this reconcile refills the IMV.",
            view_name,
            recreated.join(", ")
        );
    }
}

/// The first GROUP BY / DISTINCT key whose type the reconstructed `column_types`
/// cannot supply, if any.
///
/// `resolve_column_type` with an empty default reports exactly that: unlike its
/// `"TEXT"` default — which silently substitutes NUMERIC — an empty default returns
/// an empty string when nothing in the map matches. Only the key columns are checked
/// because they are the ones typed from `column_types`; the aggregate columns carry
/// the `pg_type` create time already resolved into the stored plan.
fn first_unresolved_group_key(
    plan: &aggregation::AggregationPlan,
    column_types: &std::collections::HashMap<String, String>,
) -> Option<String> {
    plan.group_by_columns
        .iter()
        .chain(plan.distinct_columns.iter())
        .find(|col| {
            let norm = crate::query_decomposer::normalized_column_name(col);
            crate::schema_builder::resolve_column_type(&norm, column_types, "").is_empty()
        })
        .cloned()
}

/// `to_regclass` presence probe for an already-quoted, qualified relation name.
fn relation_present(client: &pgrx::spi::SpiClient<'_>, qualified: &str) -> bool {
    client
        .select(
            "SELECT to_regclass($1) IS NOT NULL AS present",
            Some(1),
            &[unsafe {
                DatumWithOid::new(qualified.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .unwrap_or_report()
        .first()
        .get_by_name::<bool, _>("present")
        .unwrap_or(None)
        .unwrap_or(false)
}

/// Whether `view_name`'s registry row is a decomposed WRAPPER node — the class
/// `reconcile_one` must refuse. Shares its definition with the audit's
/// `ImvRow::is_decomposed_wrapper` (`src/audit/mod.rs`): a row written only by
/// `RegistryRow::decomposed`, which hardcodes `end_query = ''` AND
/// `aggregations = '{}'`. No `persist_metadata` row can be both — it always
/// serialises a full `AggregationPlan`, non-empty even for a passthrough
/// (`is_passthrough` alone makes the object non-empty) — so the pair is exact.
///
/// NOT `is_generated_sub_imv`: a TOP-LEVEL UNION-ALL / set-op / DISTINCT-ON /
/// window IMV registers its wrapper under the USER's own name (`ctx.view_name`),
/// which `decompose.rs` never marks generated (only operand and CTE sub-nodes are)
/// — yet that VIEW is exactly the one whose `TRUNCATE` raises. `aggregations = '{}'`
/// catches it; `is_generated_sub_imv` would let it through.
///
/// `aggregations::text = '{}'` is a PLAIN equality, not `COALESCE(…, '{}')`: a NULL
/// `aggregations` is a row of UNKNOWN shape (the column is nullable; legacy rows
/// predate it), and refusing an unknown row would break reconcile for it. Only a
/// row KNOWN to carry an empty-object plan is refused — the same safe direction
/// `ImvRow::is_decomposed_wrapper` takes (`Some(0)`, not NULL), and the opposite of
/// `REBUILDABLE_NODE`, whose question ("may reconcile rewrite this?") wants the
/// other default.
fn is_decomposed_wrapper_row(view_name: &str) -> bool {
    Spi::connect(|client| {
        client
            .select(
                "SELECT (end_query = '' AND aggregations::text = '{}') AS is_wrapper \
                 FROM public.__reflex_ivm_reference WHERE name = $1",
                Some(1),
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .first()
            .get_by_name::<bool, _>("is_wrapper")
            .unwrap_or(None)
            .unwrap_or(false)
    })
}

/// Rebuild ONE IMV's intermediate + target from its own `base_query`.
///
/// The single-node primitive. [`reflex_reconcile`] is the entry point, and it
/// first repairs the sub-IMVs pg_reflex generated for `view_name`.
///
/// `drop_orphans` is forwarded to the pre-rebuild partition sync. The operator
/// entry point passes `true` (unchanged from 1.10.11); the recursive descent
/// passes `false` so reconciling a chain does not multiply an orphan-dropping
/// side effect the caller never asked for across every generated node in it.
pub(crate) fn reconcile_one(view_name: &str, drop_orphans: bool) -> &'static str {
    if let Err(msg) = validate_view_name(view_name) {
        return msg;
    }
    // Backstop: refuse a decomposed WRAPPER node before touching anything. A
    // wrapper is a set-op / DISTINCT ON / window VIEW, or a materialised UNION-ALL
    // mirror TABLE, maintained through its operands — it has no rebuild from its
    // own registry row. Reaching the passthrough branch below would `TRUNCATE` +
    // `INSERT <base_query>`, which RAISES on a VIEW (`"<v>" is not a table`,
    // aborting the caller's transaction) and column-shifts a materialised wrapper
    // (its base_query yields N payload values for the wrapper's N+1 columns —
    // `__reflex_src_idx` + payload — silently wrong data that propagates to the
    // parent). Refusing here with a returned error STRING converts both faces into
    // one loud, transaction-preserving refusal that every caller — direct
    // `reflex_reconcile`, `refresh_imv_depending_on`, doctor F4/F4b — already
    // handles as a non-fatal `ERROR:`.
    //
    // Placed before `heal_missing_intermediate` / `reflex_sync_partitions_impl` /
    // the `TRUNCATE`: those are no-ops on a wrapper (it owns no intermediate and no
    // partition plan), but the refusal must not depend on that staying true.
    if is_decomposed_wrapper_row(view_name) {
        warning!(
            "pg_reflex: refusing to reconcile '{}' — it is a decomposed wrapper node \
             (UNION-ALL/set-op/DISTINCT-ON/window) with no standalone rebuild. It is \
             maintained by its operand sub-IMVs; reconcile those, or the parent IMV that \
             reads it, instead.",
            view_name
        );
        return "ERROR: cannot reconcile a decomposed wrapper node — it is maintained by \
                its operand sub-IMVs; reconcile the operands or the parent that reads it";
    }
    // Recreate the aggregate aux relations first when they are gone, so the rest of
    // this function — and the partition sync below it — have something to work on.
    heal_missing_intermediate(view_name);
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
        // skip the TRUNCATE-on-parent pattern and rebuild each mirror LEAF via
        // the same atomic DETACH/ATTACH swap used by
        // `reflex_reconcile_partition`.
        //
        // Each swap DETACHes from the leaf's IMMEDIATE parent and PostgreSQL
        // holds that `AccessExclusiveLock` to commit, so the lock lands on a
        // branch — not the IMV root — whenever the mirror is deeper than one
        // level. At mirror depth 1 the leaf's immediate parent IS the root, and
        // readers of the IMV do block for the rest of the transaction; that
        // residual case is tracked separately.
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

                // The set to swap is the mirror's LEAF set, derived from the
                // source tree through `truncate_partition_tree` — the same
                // function `create_ivm` and `reflex_sync_partitions` use to
                // build the mirror. Deriving it from the builder makes the two
                // agree by construction rather than by parallel reasoning.
                //
                // Iterating the source's IMMEDIATE children instead would hand
                // the swap a top-level child, which on a depth->=2 mirror is a
                // PARTITIONED relation the swap cannot rebuild — its
                // `LIKE ... INCLUDING ALL` replacement carries no partitioning,
                // so the mirror was silently flattened and the next sync then
                // recreated those children empty.
                //
                // Truncation keeps `depth <= mirror_depth` and demotes the nodes
                // sitting exactly at `mirror_depth` to leaves, which is what
                // makes a CHILDLESS source branch come out right: at
                // `mirror_depth` it is demoted and included (its mirror child is
                // a plain table that can hold rows, so it must be rebuilt);
                // above `mirror_depth` it stays partitioned and is excluded (its
                // mirror child is a partitioned relation, which holds no rows
                // itself and has nothing to rebuild). Filtering the raw source
                // tree to leaves instead drops such a branch entirely — it is
                // not a leaf and owns no leaf to stand in for it.
                //
                // At mirror depth 1 this yields exactly the source's immediate
                // children, so single-level IMVs are unaffected.
                let source_tree =
                    crate::partition::list_partition_tree(client, &plan.anchor_source);
                let mirror_depth = record
                    .partition_depth
                    .map(|d| d as usize)
                    .unwrap_or_else(|| crate::partition::max_tree_depth(&source_tree));
                let mirror_nodes: std::collections::BTreeSet<String> =
                    crate::partition::truncate_partition_tree(source_tree, mirror_depth)
                        .into_iter()
                        .filter(|n| n.sub_strategy.is_none())
                        .map(|n| n.bare_name)
                        .collect();
                for src_bare in &mirror_nodes {
                    if let Err(e) = crate::partition::execute_partition_swap_for_child(
                        client,
                        view_name,
                        &schema,
                        src_bare,
                        &base_query,
                        &end_query,
                        unlogged,
                    ) {
                        warning!(
                            "pg_reflex: per-partition reconcile of '{}' for child '{}' failed: {}",
                            view_name,
                            src_bare,
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
                    mirror_nodes.len()
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
                let result = reconcile_generated_child_without_propagating(child, false);
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

    let own = reconcile_named_node(view_name, drop_orphans);

    if !own.starts_with("ERROR") {
        cascade_partitioned_rebuild_to_dependents(view_name);
    }

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

/// Refresh the dependents of an IMV that was just rebuilt through the
/// PARTITIONED path, which propagates nothing on its own.
///
/// The unpartitioned rebuild is `TRUNCATE` + `INSERT`, and every source carries
/// an `AFTER TRUNCATE … FOR EACH STATEMENT` trigger plus the statement-level
/// INSERT trigger, so consumers follow it. The partitioned rebuild moves rows
/// with `CREATE TABLE AS` into a detached table and then DETACH/ATTACH/RENAME —
/// pure DDL, no DML on the live target, so no data trigger can fire and no
/// consumer ever learns the IMV changed. Left alone the dependent serves stale
/// rows with `known_stale = f` and no signal at all.
///
/// This is the same fan-out `reflex_reconcile_partition` performs after its own
/// swap (`partition.rs`), reached here for the whole-IMV rebuild. Restricted to
/// the partitioned case on purpose: cascading after an unpartitioned rebuild
/// would rebuild a consumer that ALSO has the rebuild's delta staged for COMMIT,
/// double-counting it — the hazard
/// `reconcile_generated_child_without_propagating` exists to avoid.
///
/// Deliberately at the public entry point rather than inside `reconcile_one`:
/// the chain descent rebuilds generated sub-IMVs through `reconcile_one` with
/// their triggers suppressed *because* they must not propagate, and a cascade
/// there would send a generated child back up to the root currently rebuilding
/// it.
///
/// Cost is O(D) dispatches for D direct dependents, once per reconcile — not
/// once per swapped partition. Each dependent's own rebuild is the work its
/// staleness requires, and recursion carries the refresh down the chain.
fn cascade_partitioned_rebuild_to_dependents(view_name: &str) {
    let dependents: Vec<String> = Spi::connect(|client| {
        client
            .select(
                "SELECT unnest(r.graph_child) AS dep \
                 FROM public.__reflex_ivm_reference r \
                 WHERE r.name = $1 \
                   AND r.partition_columns IS NOT NULL \
                   AND array_length(r.partition_columns, 1) > 0",
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
                row.get_by_name::<&str, _>("dep")
                    .unwrap_or(None)
                    .map(|s| s.to_string())
            })
            .collect()
    });

    for dep in &dependents {
        let result = reflex_reconcile(dep);
        if result.starts_with("ERROR") {
            warning!(
                "pg_reflex: '{}' was rebuilt but refreshing its dependent '{}' returned: {} \
                 — that dependent is STALE; run SELECT reflex_rebuild_imv('{}') once the \
                 cause is cleared",
                view_name,
                dep,
                result,
                dep
            );
        }
    }
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

/// Record a targeted-recovery call and, when the target cannot be converged by an
/// anchor-scoped rebuild, WARN loudly with the primitive that can (PS-14).
///
/// Called only from the SQL entry-point wrappers `reflex_rebuild_imv`,
/// `reflex_reconcile`, and the two-arg `reflex_reconcile` overload — the calls an
/// operator (or a retry loop) makes directly on one IMV. It is NOT called from the
/// internal recursive/cascade descent (`reconcile_one` /
/// `reconcile_generated_child_without_propagating`), so a decomposed chain rebuild
/// bumps only the named node's counter, not each generated child's.
///
/// `inside_trigger()` gates both effects: the runtime fires
/// `PERFORM public.reflex_reconcile(...)` from its own generated trigger bodies
/// and partition-flush plpgsql, which reach this same entry point; those are
/// maintenance, not targeted recovery, and must neither inflate the retry counter
/// nor emit an operator advisory.
pub(crate) fn stamp_targeted_recovery(view_name: &str) {
    if inside_trigger() {
        return;
    }
    Spi::connect_mut(|client| {
        let _ = crate::sql_writer::registry::bump_rebuild_count(client, view_name);
    });
    if let Some(advisory) = rebuild_convergence_advisory(view_name) {
        warning!("{}", advisory);
    }
}

/// Returns an actionable advisory when an anchor-scoped rebuild
/// (`reflex_rebuild_imv` / `reflex_reconcile`) structurally cannot converge this
/// IMV, else `None`. Pure classifier over the registry — the testable seam behind
/// the WARN in [`stamp_targeted_recovery`].
///
/// Reuses PS-3's stored `requires_explicit_refresh` verdict and the existing
/// `ignored_sources` / `partition_strategy` metadata rather than re-deriving
/// matview-ness. Two non-convergence shapes:
///   * `requires_explicit_refresh` — every real source is a materialized view, so
///     the node never self-maintains; a manual rebuild does re-derive it, but the
///     canonical recovery after `REFRESH MATERIALIZED VIEW` is
///     `refresh_imv_depending_on('<mv>')`.
///   * partitioned + `ignore_sources` — the archive-residue shape: an anchor-empty
///     partition whose rows arrive only via an authoritative ignored source is
///     never refilled by the anchor-scoped re-derivation, so repeated rebuilds do
///     not converge. The primitives that do are `reflex_reconcile_partition` (one
///     partition) or a `reflex_rebuild_chain` full recreate.
///
/// Deliberately conservative — a WARN, never a refusal: a matview-source IMV whose
/// stale data reached an anchor-non-empty partition DOES converge under a plain
/// rebuild, and turning a working recovery into an error would be worse than the
/// advisory. A partitioned IMV WITHOUT ignored sources refills every partition from
/// its anchor and is not flagged.
pub(crate) fn rebuild_convergence_advisory(view_name: &str) -> Option<String> {
    let risk =
        Spi::connect(|client| crate::sql_writer::registry::read_rebuild_risk(client, view_name))?;

    if risk.has_ignored_sources && risk.is_partitioned {
        return Some(format!(
            "pg_reflex: reflex_rebuild_imv/reflex_reconcile re-derives '{v}' only from its \
             anchor source(s); it CANNOT refill a partition whose rows arrive solely via an \
             ignore_sources / authoritative table (archive-residue, anchor-empty partitions \
             stay stale — repeated calls will NOT converge). To force-fill such a partition \
             use reflex_reconcile_partition('{v}', '<partition_keys>'), or fully recreate the \
             chain with reflex_rebuild_chain('{v}'). See docs/untreated.md § F6.",
            v = view_name
        ));
    }

    if risk.requires_explicit_refresh {
        return Some(format!(
            "pg_reflex: '{v}' is fed only by materialized view source(s); it does not \
             self-maintain (PostgreSQL fires no trigger on a matview). A rebuild re-derives it, \
             but the canonical recovery after REFRESH MATERIALIZED VIEW is \
             refresh_imv_depending_on('<matview>'), which cascades the whole chain.",
            v = view_name
        ));
    }

    None
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
///
/// `drop_orphans` is forwarded to `reconcile_one`. The chain descent and the
/// DEFERRED cross-source guard pass `false` (neither caller was asked to drop
/// partitions); the direct-entry operand routing forwards whatever the operator
/// asked for, so naming an operand keeps the same orphan semantics as naming any
/// other IMV.
fn reconcile_generated_child_without_propagating(child: &str, drop_orphans: bool) -> &'static str {
    let quoted = quote_identifier(child);
    let triggerable = Spi::get_one::<bool>(&format!(
        "SELECT COALESCE((SELECT relkind IN ('r','p') FROM pg_class \
          WHERE oid = to_regclass('{}')), FALSE)",
        quoted.replace('\'', "''")
    ))
    .unwrap_or(None)
    .unwrap_or(false);

    if !triggerable {
        return reconcile_one(child, drop_orphans);
    }

    Spi::run(&format!("ALTER TABLE {} DISABLE TRIGGER USER", quoted)).unwrap_or_report();
    let result = reconcile_one(child, drop_orphans);
    Spi::run(&format!("ALTER TABLE {} ENABLE TRIGGER USER", quoted)).unwrap_or_report();
    result
}

/// Rebuild the NAMED node of a [`reflex_reconcile_with_orphans`] call, routing a
/// UNION-ALL operand of a MATERIALISED wrapper through the trigger-suppressed
/// rebuild plus an explicit wrapper-slice resync.
///
/// `reconcile_one` rebuilds with `TRUNCATE` + `INSERT`. An operand carries an
/// `__reflex_union_mirror_{ins,del,upd}_*` trigger mirroring its rows into the
/// wrapper's `__reflex_src_idx` slice, and that mirror has no `TRUNCATE` handler:
/// the TRUNCATE removes nothing from the wrapper while the INSERT re-appends the
/// operand's whole row set, silently DOUBLING the slice and every consumer of it.
/// The chain descent never hit this (it suppresses triggers first), but three
/// direct callers do — an operator naming a `…__union_N` node, `reflex_doctor`'s
/// F4 repair, and `reflex_scheduled_reconcile`, which reconciles operands
/// STANDALONE by design (they are `REBUILDABLE_NODE` and deliberately not
/// "covered" by the wrapper above them), making every unattended drift sweep over
/// such a chain double the wrapper. This is the 2026-07-24 report's residual, and
/// the same mechanism the DEFERRED cross-source guard already routes around via
/// [`reconcile_generated_child_for_cross_source_guard`].
///
/// Any other target — including a plain CTE sub-IMV, and a VIEW-based
/// set-op/DISTINCT-ON/window wrapper's operand (that wrapper stores nothing, so
/// there is no slice to double) — is byte-identical to before: one extra registry
/// probe, then the same `reconcile_one`.
///
/// Brackets with the WRAPPER as reconcile root, not the operand: the
/// DISABLE/ENABLE TRIGGER trips `reflex_on_ddl_command_end`'s alter_source alarm
/// for every IMV whose `depends_on` names the operand — which for a materialised
/// union-ALL operand is the wrapper — and under `alter_source_policy = 'error'`
/// that alarm RAISES, turning a plain `reflex_reconcile` into an aborted
/// transaction. The root also prefix-matches `<wrapper>__%`, covering the operand
/// itself, while any UNRELATED consumer of the operand still gets its legitimate
/// stale warning.
fn reconcile_named_node(view_name: &str, drop_orphans: bool) -> &'static str {
    let Some((wrapper, operand_idx)) =
        Spi::connect(|client| materialized_union_wrapper_of(client, view_name))
    else {
        return reconcile_one(view_name, drop_orphans);
    };

    set_internal_reconcile_root(Some(&wrapper));
    let result = reconcile_generated_child_without_propagating(view_name, drop_orphans);
    set_internal_reconcile_root(None);
    // A failed rebuild leaves the operand's contents undefined; resyncing the
    // wrapper from it would publish that. Leave the wrapper's existing slice
    // alone and report the failure — stale beats wrong.
    if result.starts_with("ERROR") {
        return result;
    }
    resync_wrapper_operand_slice(&wrapper, operand_idx, view_name);
    result
}

/// Safe repair for a GENERATED sub-IMV reached by the DEFERRED cross-source
/// guard (`trigger/deferred.rs`'s `imv_has_multiple_sources` branch).
///
/// That guard runs from inside the deferred COMMIT trigger
/// (`pg_trigger_depth() > 0`), so calling the public `reflex_reconcile` there
/// is unsafe: `reflex_reconcile_with_orphans`'s `inside_trigger` gate skips the
/// trigger-suppressed descent entirely and falls straight to `reconcile_one`
/// with `child`'s own triggers LIVE. For a UNION-ALL operand that live trigger
/// is the `__reflex_union_mirror_*` mirror `install_union_mirror_triggers`
/// installs — it covers INSERT/UPDATE/DELETE but not `TRUNCATE`, so
/// `reconcile_one`'s TRUNCATE-then-INSERT rebuild re-appends the operand's
/// entire row set into its wrapper a second time, with nothing having removed
/// the wrapper's stale slice. The wrapper (and anything reading it) silently
/// doubles that operand's contribution — the 2026-07-25 bug this fixes.
///
/// Always rebuilds with propagation suppressed
/// ([`reconcile_generated_child_without_propagating`], safe for any
/// consumer — not just a union wrapper). When `child` also turns out to feed a
/// MATERIALISED union-ALL wrapper (one with a stored `__reflex_src_idx`
/// column — a VIEW-based set-op/DISTINCT-ON/window wrapper stores nothing and
/// needs no resync), additionally replaces that wrapper's slice for `child`
/// from the freshly rebuilt operand. That resync is exactly what suppressing
/// propagation otherwise leaves undone, and it is the one piece
/// [`reconcile_generated_child_without_propagating`]'s CHAIN-DESCENT caller
/// (`reflex_reconcile`'s top-down rebuild) does not need: that descent
/// (`generated_dependencies_shallowest_first`) stops at a decomposed node, so it
/// never reaches an operand to rebuild in the first place — see
/// `pg_scheduled_reconcile_reconciles_operands_below_decomposed_wrapper`
/// (`src/tests/pg_test_decomposed_chain.rs`), which pins that `reflex_reconcile`
/// on the wrapper's own top-level consumer halts AT the wrapper.
///
/// [`reconcile_named_node`] does the same suppress-then-resync for the DIRECT
/// entry points (`reflex_reconcile` / `reflex_rebuild_imv` / the scheduled sweep,
/// which reach an operand by NAME rather than by descent). The two paths are
/// deliberately separate: this one is reached from inside a COMMIT trigger and
/// has its own failure contract with the deferred flush.
pub(crate) fn reconcile_generated_child_for_cross_source_guard(child: &str) -> &'static str {
    let result = reconcile_generated_child_without_propagating(child, false);
    if result.starts_with("ERROR") {
        return result;
    }
    if let Some((wrapper, idx)) =
        Spi::connect(|client| materialized_union_wrapper_of(client, child))
    {
        resync_wrapper_operand_slice(&wrapper, idx, child);
    }
    result
}

/// `child`'s materialised UNION-ALL wrapper and its `__reflex_src_idx` tag, if
/// any. `child` is an operand exactly when some decomposed row's
/// `depends_on_imv` names it — `install_union_all_intermediate_wrapper` writes
/// that array in the same order it assigns operand indices
/// (`decompose.rs:594-611`), so the operand's position in the array (0-based)
/// IS its `__reflex_src_idx` tag. The `__reflex_src_idx` column existence check
/// excludes VIEW-based set-op/DISTINCT-ON/window wrappers, which store nothing
/// and so have no slice to resync.
fn materialized_union_wrapper_of(
    client: &pgrx::spi::SpiClient<'_>,
    child: &str,
) -> Option<(String, i16)> {
    client
        .select(
            "SELECT w.name AS wrapper, \
                    (array_position(w.depends_on_imv, $1::text) - 1)::SMALLINT AS idx \
             FROM public.__reflex_ivm_reference w \
             WHERE $1 = ANY(COALESCE(w.depends_on_imv, ARRAY[]::TEXT[])) \
               AND w.end_query = '' AND w.aggregations::text = '{}' \
               AND EXISTS ( \
                     SELECT 1 FROM pg_attribute a \
                      WHERE a.attrelid = to_regclass(w.name) \
                        AND a.attname = '__reflex_src_idx' \
                        AND NOT a.attisdropped)",
            Some(1),
            &[unsafe {
                DatumWithOid::new(child.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .unwrap_or_report()
        .next()
        .and_then(|row| {
            let wrapper = row.get_by_name::<&str, _>("wrapper").ok()??.to_string();
            let idx = row.get_by_name::<i16, _>("idx").ok()??;
            Some((wrapper, idx))
        })
}

/// Replace `wrapper`'s stored `__reflex_src_idx = operand_idx` slice wholesale
/// with `child`'s freshly rebuilt rows — the resync
/// [`reconcile_generated_child_for_cross_source_guard`] needs after rebuilding
/// `child` with its mirror trigger suppressed (which, by design, propagates
/// nothing to the wrapper). Named columns, not `SELECT idx, *`: matching by
/// name (not position) means a column reorder on either relation can't
/// silently shift the payload the way `reconcile_one`'s wrapper backstop
/// (`is_decomposed_wrapper_row`) exists to prevent for the whole-wrapper case.
fn resync_wrapper_operand_slice(wrapper: &str, operand_idx: i16, child: &str) {
    let wrapper_q = quote_identifier(wrapper);
    let child_q = quote_identifier(child);
    let payload_cols: Vec<String> = Spi::connect(|client| {
        client
            .select(
                "SELECT a.attname::text AS name FROM pg_attribute a \
                 WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 \
                   AND NOT a.attisdropped AND a.attname <> '__reflex_src_idx' \
                 ORDER BY a.attnum",
                None,
                &[unsafe {
                    DatumWithOid::new(wrapper.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                row.get_by_name::<&str, _>("name")
                    .ok()
                    .flatten()
                    .map(|s| s.to_string())
            })
            .collect()
    });
    let col_list = payload_cols
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    Spi::run(&format!(
        "DELETE FROM {wrapper_q} WHERE __reflex_src_idx = {operand_idx}::SMALLINT"
    ))
    .unwrap_or_report();
    Spi::run(&format!(
        "INSERT INTO {wrapper_q} (__reflex_src_idx, {col_list}) \
         SELECT {operand_idx}::SMALLINT, {col_list} FROM {child_q}"
    ))
    .unwrap_or_report();
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
        // Both the base seed and the recursive descent stop at a decomposed node
        // (`aggregations = '{}'`), mirroring `generated_dependencies_shallowest_first`,
        // which `reflex_reconcile` uses to decide what it rebuilds. A decomposed
        // wrapper (UNION-ALL/set-op/DISTINCT-ON/window) is neither standalone-
        // reconcilable (reconcile_one would column-shift it) nor descended past by
        // the rebuild, so a candidate reading one does NOT rebuild the operands
        // below it. Treating that wrapper as a coverer would mark those operands
        // covered and skip them forever, even though nothing ever reconciles them.
        // The wrapper itself still gets covered (its rebuildable parent seeds it in
        // the base step) and correctly stays skipped, while its operands drop out of
        // the covered set and are reconciled standalone.
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
        // A decomposed wrapper is not a candidate at all — the same
        // `aggregations <> '{}'` test the `covered` CTE below already applies, and
        // for the same reason `REBUILDABLE_NODE` exists: `reflex_reconcile` cannot
        // operate on such a node. Worse than "cannot": a VIEW wrapper RAISES from
        // the passthrough branch's `TRUNCATE` (`"<view>" is not a table`), and the
        // raise propagates out of `reflex_scheduled_reconcile`, so a single set-op
        // IMV in the database killed the ENTIRE sweep — every other IMV in the batch
        // went unreconciled and the function returned nothing. Permanently, too: a
        // wrapper's `last_update_date` is stamped at create and never advances,
        // because the only writer is the tail of `reconcile_one`, which a wrapper
        // never reaches, so it stays a candidate from `max_age_minutes` after
        // creation onwards.
        //
        // Excluding wrappers loses no maintenance: a wrapper is kept current by its
        // sub-IMVs (VIEW) or by its per-operand `__reflex_union_mirror_*` triggers
        // (TABLE), and its operands are separate rows that the sweep still visits —
        // they were never "covered" by the wrapper (see the note above), so they are
        // reconciled standalone exactly as before.
        //
        // `COALESCE(…, '{}')` puts a NULL `aggregations` on the excluded side, which
        // is the safe direction for a writer: an unknown shape is one not to rewrite.
        // (`ImvRow::is_decomposed_wrapper` resolves the same NULL the other way, and
        // for the same reason — for a *reporter*, unknown must not mean "silence the
        // Error".)
        let sql = "WITH candidate AS ( \
                       SELECT name, graph_depth, depends_on_imv, last_update_date, aggregations \
                         FROM public.__reflex_ivm_reference \
                        WHERE COALESCE(enabled, TRUE) = TRUE \
                          AND COALESCE(aggregations::text, '{}') <> '{}' \
                          AND ($2 = '' OR COALESCE(target_schema, 'public') = $2) \
                          AND (last_update_date IS NULL \
                               OR last_update_date < (CURRENT_TIMESTAMP - make_interval(mins => $1))) \
                   ), \
                   covered AS ( \
                       WITH RECURSIVE reachable AS ( \
                             SELECT unnest(COALESCE(c.depends_on_imv, ARRAY[]::TEXT[])) AS nm \
                               FROM candidate c \
                              WHERE COALESCE(c.aggregations::text, '{}') <> '{}' \
                           UNION \
                             SELECT unnest(COALESCE(r.depends_on_imv, ARRAY[]::TEXT[])) \
                               FROM public.__reflex_ivm_reference r JOIN reachable ON r.name = reachable.nm \
                              WHERE COALESCE(r.is_generated_sub_imv, FALSE) \
                                AND COALESCE(r.aggregations::text, '{}') <> '{}' \
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
