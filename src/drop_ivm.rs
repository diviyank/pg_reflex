use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;

use crate::query_decomposer::{
    affected_groups_table_name, delta_scratch_table_name, intermediate_table_name,
    passthrough_scratch_new_table_name, passthrough_scratch_old_table_name, quote_identifier,
    shrunk_groups_table_name, split_qualified_name,
};

pub(crate) fn drop_reflex_ivm_impl(view_name: &str, cascade: bool) -> &'static str {
    Spi::connect_mut(|client| {
        // 1. Check if view exists
        let exists = client
            .select(
                "SELECT name, graph_child, depends_on, depends_on_imv \
                 FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report()
            .collect::<Vec<_>>();

        if exists.is_empty() {
            warning!("pg_reflex: drop failed — IMV '{}' not found", view_name);
            return "ERROR: IMV not found";
        }

        let row = &exists[0];
        let children: Vec<String> = row
            .get_by_name::<Vec<String>, _>("graph_child")
            .unwrap_or(None)
            .unwrap_or_default();
        let depends_on: Vec<String> = row
            .get_by_name::<Vec<String>, _>("depends_on")
            .unwrap_or(None)
            .unwrap_or_default();
        let depends_on_imv: Vec<String> = row
            .get_by_name::<Vec<String>, _>("depends_on_imv")
            .unwrap_or(None)
            .unwrap_or_default();

        // 2. Check children
        if !children.is_empty() && !cascade {
            return "ERROR: IMV has children. Use drop_reflex_ivm(name, true) to cascade.";
        }

        // 3. Cascade: drop children first
        if cascade {
            for child in &children {
                let result = drop_reflex_ivm_impl(child, true);
                if result.starts_with("ERROR") {
                    return result;
                }
            }
        }

        // 3b. For decomposed views (SetOpUnionAll, CteDecomposed, DistinctOn), also drop
        // the sub-IMVs listed in depends_on_imv. These are synthetic IMVs created exclusively
        // for this decomposed view. When the parent is dropped with cascade=true, these
        // sub-IMVs should also be dropped (they serve no purpose without the parent).
        if cascade && !depends_on_imv.is_empty() {
            for sub_imv in &depends_on_imv {
                // First, remove the parent from the sub-IMV's graph_child to break the circular
                // dependency before we recursively drop the sub-IMV.
                client
                    .update(
                        "UPDATE public.__reflex_ivm_reference \
                         SET graph_child = array_remove(graph_child, $1) \
                         WHERE name = $2",
                        None,
                        &[
                            unsafe {
                                DatumWithOid::new(
                                    view_name.to_string(),
                                    PgBuiltInOids::TEXTOID.oid().value(),
                                )
                            },
                            unsafe {
                                DatumWithOid::new(sub_imv.clone(), PgBuiltInOids::TEXTOID.oid().value())
                            },
                        ],
                    )
                    .unwrap_or_report();

                // Check if this sub_imv is actually an IMV in the reference table
                // (it should be, but check to be safe)
                let sub_exists = client
                    .select(
                        "SELECT 1 FROM public.__reflex_ivm_reference WHERE name = $1",
                        None,
                        &[unsafe {
                            DatumWithOid::new(sub_imv.clone(), PgBuiltInOids::TEXTOID.oid().value())
                        }],
                    )
                    .unwrap_or_report()
                    .is_empty() == false;

                if sub_exists {
                    let result = drop_reflex_ivm_impl(sub_imv, true);
                    if result.starts_with("ERROR") {
                        return result;
                    }
                }
            }
        }

        // 4. Drop consolidated triggers only if no OTHER IMV depends on this source.
        //    The trigger function is shared; it discovers IMVs via the reference table.
        //    We must delete the reference row FIRST (step 8) so the trigger stops
        //    finding this IMV. But we need the reference row for step 7. So we check
        //    here and drop triggers AFTER deleting the row (moved below step 8).
        //    For now, just collect sources that need trigger cleanup.
        let mut sources_to_cleanup: Vec<(String, String)> = Vec::new(); // (source, safe_source)
        for source in &depends_on {
            // Synthetic `<subquery:...>`/`<function:...>` placeholders own no
            // relation of their own — triggers live on the real underlying tables,
            // which appear as separate real sources. Skip them, mirroring the guard
            // in create's install_source_triggers; interpolating the placeholder into
            // teardown DDL would emit an unparsable identifier.
            if source.starts_with('<') {
                continue;
            }
            // Sub-IMV sources are persisted in `depends_on` already double-quoted
            // (`"schema"."view__cte_x"`), but the source triggers were installed
            // under the normalized name. Normalize here using the canonical path,
            // matching how install_source_triggers builds the trigger name.
            let safe_source = crate::query_decomposer::sanitized_source_suffix(source);
            let other_count = client
                .select(
                    "SELECT COUNT(*) AS cnt FROM public.__reflex_ivm_reference \
                     WHERE $1 = ANY(depends_on) AND name != $2",
                    None,
                    &[
                        unsafe {
                            DatumWithOid::new(source.clone(), PgBuiltInOids::TEXTOID.oid().value())
                        },
                        unsafe {
                            DatumWithOid::new(
                                view_name.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                    ],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<i64, _>("cnt")
                .unwrap_or(None)
                .unwrap_or(0);

            if other_count == 0 {
                sources_to_cleanup.push((source.clone(), safe_source));
            }
        }

        // 5. Drop target (could be a TABLE or a VIEW for window/DISTINCT ON decompositions)
        let cascade_suffix = if cascade { " CASCADE" } else { "" };
        let (tgt_schema, tgt_name) = split_qualified_name(view_name);
        let tgt_schema_str = tgt_schema.unwrap_or("public");
        let relkind: String = client
            .select(
                "SELECT COALESCE((SELECT relkind::TEXT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
                 WHERE n.nspname = $1 AND c.relname = $2), 'r')",
                None,
                &[
                    unsafe { DatumWithOid::new(tgt_schema_str.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                    unsafe { DatumWithOid::new(tgt_name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            )
            .unwrap_or_report()
            .first()
            .get_by_name::<&str, _>("coalesce")
            .unwrap_or(None)
            .unwrap_or("r")
            .to_string();

        if relkind == "v" {
            client
                .update(
                    &format!(
                        "DROP VIEW IF EXISTS {}{}",
                        quote_identifier(view_name),
                        cascade_suffix
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
        } else {
            client
                .update(
                    &format!(
                        "DROP TABLE IF EXISTS {}{}",
                        quote_identifier(view_name),
                        cascade_suffix
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
        }

        // 6. Drop intermediate table
        let intermediate = intermediate_table_name(view_name);
        client
            .update(
                &format!("DROP TABLE IF EXISTS {}{}", intermediate, cascade_suffix),
                None,
                &[],
            )
            .unwrap_or_report();


        // 6b. Drop persistent affected-groups table (qualified in IMV's schema, 1.4.1).
        client
            .update(
                &format!(
                    "DROP TABLE IF EXISTS {}{}",
                    affected_groups_table_name(view_name),
                    cascade_suffix
                ),
                None,
                &[],
            )
            .unwrap_or_report();

        // 6b'. Drop persistent N1 shrunk-groups table (only present for top-K
        // IMVs; IF EXISTS makes the call safe when the IMV had no top-K cols).
        client
            .update(
                &format!(
                    "DROP TABLE IF EXISTS {}{}",
                    shrunk_groups_table_name(view_name),
                    cascade_suffix
                ),
                None,
                &[],
            )
            .unwrap_or_report();

        // 6c. Drop delta scratch table
        client
            .update(
                &format!(
                    "DROP TABLE IF EXISTS {}{}",
                    delta_scratch_table_name(view_name),
                    cascade_suffix
                ),
                None,
                &[],
            )
            .unwrap_or_report();

        // 6d. Drop passthrough scratch tables (one new-side + one old-side per source).
        //     DROP ... IF EXISTS is harmless when the IMV is an aggregate.
        for source in &depends_on {
            if source.starts_with('<') {
                continue;
            }
            for tbl in [
                passthrough_scratch_new_table_name(view_name, source),
                passthrough_scratch_old_table_name(view_name, source),
            ] {
                client
                    .update(
                        &format!("DROP TABLE IF EXISTS {}{}", tbl, cascade_suffix),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            }
        }

        // 7. Update parent IMVs: remove this view from their graph_child
        for parent in &depends_on_imv {
            client
                .update(
                    "UPDATE public.__reflex_ivm_reference \
                     SET graph_child = array_remove(graph_child, $1) \
                     WHERE name = $2",
                    None,
                    &[
                        unsafe {
                            DatumWithOid::new(
                                view_name.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                        unsafe {
                            DatumWithOid::new(
                                parent.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        },
                    ],
                )
                .unwrap_or_report();
        }

        // 8. Delete from reference table
        client
            .update(
                "DELETE FROM public.__reflex_ivm_reference WHERE name = $1",
                None,
                &[unsafe {
                    DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                }],
            )
            .unwrap_or_report();

        // 9. Drop consolidated triggers on sources where no other IMV depends.
        //    When called from the sql_drop event trigger the source table itself is
        //    already gone (its triggers cascaded with it). DROP TRIGGER ON <missing>
        //    would error on "relation does not exist" — gate on to_regclass.
        for (source, safe_source) in &sources_to_cleanup {
            let source_still_exists: bool = client
                .select(
                    "SELECT to_regclass($1) IS NOT NULL AS present",
                    None,
                    &[unsafe {
                        DatumWithOid::new(source.clone(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<bool, _>("present")
                .unwrap_or(None)
                .unwrap_or(false);

            for op in &["ins", "del", "upd", "trunc"] {
                if source_still_exists {
                    let trig_name = format!("__reflex_trigger_{}_on_{}", op, safe_source);
                    client
                        .update(
                            &format!("DROP TRIGGER IF EXISTS \"{}\" ON {}", trig_name, source),
                            None,
                            &[],
                        )
                        .unwrap_or_report();
                }

                let fn_name = format!("__reflex_{}_trigger_on_{}", op, safe_source);
                client
                    .update(&format!("DROP FUNCTION IF EXISTS {}()", fn_name), None, &[])
                    .unwrap_or_report();
            }
        }

        info!("pg_reflex: dropped IMV '{}'", view_name);
        "DROP REFLEX INCREMENTAL VIEW"
    })
}
