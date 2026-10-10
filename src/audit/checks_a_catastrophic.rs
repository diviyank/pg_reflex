use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::spi::SpiClient;
use pgrx::PgBuiltInOids;

use super::{read_attname_set, relation_exists};
use super::{Check, Finding, ImvRow, Severity};
use crate::source_truncate::TriggerState;

pub(super) struct StagingShape;

impl Check for StagingShape {
    fn id(&self) -> &'static str {
        "staging-shape"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if imv.refresh_mode != "DEFERRED" || !imv.enabled {
            return vec![];
        }
        let mut out = Vec::new();
        for src in imv.real_sources() {
            let staging = crate::query_decomposer::staging_delta_table_name(src);

            // Resolve schema + local for both sides via to_regclass.
            let staging_oid = match client
                .select(
                    "SELECT to_regclass($1)::oid::bigint AS oid",
                    None,
                    &[unsafe {
                        DatumWithOid::new(staging.clone(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<i64, _>("oid")
                .unwrap_or(None)
            {
                Some(0) | None => continue,
                Some(o) => o,
            };
            let source_oid = match client
                .select(
                    "SELECT to_regclass($1)::oid::bigint AS oid",
                    None,
                    &[unsafe {
                        DatumWithOid::new(src.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<i64, _>("oid")
                .unwrap_or(None)
            {
                Some(0) | None => continue,
                Some(o) => o,
            };

            let src_cols = read_attname_set(client, source_oid, &[]);
            let stg_cols = read_attname_set(client, staging_oid, &["__reflex_op"]);

            if src_cols != stg_cols {
                out.push(Finding {
                    imv: Some(imv.name.clone()),
                    severity: Severity::Error,
                    category: "staging-shape",
                    finding: format!(
                        "{} has columns\n  {{{}}}\nbut source {} has\n  {{{}}}\n\
                         Trigger INSERTs would mismatch on differing column set.",
                        staging,
                        stg_cols.join(", "),
                        src,
                        src_cols.join(", "),
                    ),
                    suggested_fix: format!(
                        "SELECT count(*) FROM {staging};\n\
                         -- if 0:\n\
                         DROP TABLE {staging} CASCADE;\n\
                         SELECT reflex_rebuild_triggers('{src}');\n\
                         -- if >0:\n\
                         SELECT reflex_flush_deferred('{src}');\n\
                         -- then re-run the above DROP + rebuild."
                    ),
                });
            }
        }
        out
    }
}

pub(super) struct TriggerAttached;

impl Check for TriggerAttached {
    fn id(&self) -> &'static str {
        "trigger-attached"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        // A decomposed WRAPPER row is not maintained by the consolidated
        // per-source triggers this check looks for, so their absence on its
        // "sources" (which are its own sub-IMVs) is not a defect. Probed on pg17:
        // a VIEW wrapper's operands carry no trigger at all — each sub-IMV
        // maintains its own target and the wrapper is evaluated on read — while a
        // materialised UNION-ALL wrapper is maintained by
        // `__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>` triggers, checked
        // separately below since their absence is exactly the same class of
        // silent-divergence defect this check exists to catch.
        //
        // The consolidated remedy is worse than noise here: `reflex_rebuild_triggers`
        // on a sub-IMV target INSTALLS four consolidated triggers there
        // (`__reflex_trigger_ins_on_public_<sub>`, note the qualified suffix) and
        // does not clear a mirror-trigger finding, so an operator following it
        // accumulates junk triggers on every retry. `reflex_rebuild_union_mirror`
        // is the correct repair primitive for this shape.
        if imv.is_decomposed_wrapper() {
            // Bare registry name, NOT `resolved_imv_name` — this is the exact
            // key `reflex_rebuild_union_mirror` looks up with `WHERE name =
            // $1`, which stores whatever `create_reflex_ivm` was given
            // verbatim. A schema-qualified variant would not match that row
            // and the printed remedy would silently fail to converge.
            let wrapper_name = imv.name.clone();
            let relkind = client
                .select(
                    "SELECT c.relkind::text AS k FROM pg_class c WHERE c.oid = to_regclass($1)",
                    None,
                    &[unsafe {
                        DatumWithOid::new(
                            wrapper_name.clone(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    }],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<&str, _>("k")
                .unwrap_or(None)
                .map(|s| s.to_string());
            // Only a materialised (TABLE) wrapper is maintained by mirror
            // triggers; a VIEW wrapper has none by design, and an unresolved
            // relation is covered by `source-exists`, not this check.
            if relkind.as_deref() != Some("r") {
                return vec![];
            }
            let safe_wrapper = crate::query_decomposer::sanitized_source_suffix(&wrapper_name);
            let mut out = Vec::new();
            for (i, operand) in imv.depends_on.iter().enumerate() {
                // PostgreSQL silently truncates a CREATE TRIGGER name >63 bytes
                // to NAMEDATALEN-1 at a char boundary (`decompose.rs`'s own
                // installer relies on this for TRIGGER names specifically —
                // it deliberately does NOT pre-truncate them via
                // `safe_identifier`, see the comment there; the mirror
                // FUNCTION names are a different story and ARE
                // `safe_identifier`-hashed, since triggers are scoped per
                // relation and cannot collide across operands the way global
                // function names can). Comparing against the UNTRUNCATED
                // trigger name would report a healthy, longer-named
                // wrapper's triggers as permanently "missing" — an
                // unclearable finding, since the printed remedy re-creates
                // the same (truncated) trigger every time.
                let expected = [
                    crate::query_decomposer::truncate_identifier(format!(
                        "__reflex_union_mirror_ins_{}_{}",
                        safe_wrapper, i
                    )),
                    crate::query_decomposer::truncate_identifier(format!(
                        "__reflex_union_mirror_del_{}_{}",
                        safe_wrapper, i
                    )),
                    crate::query_decomposer::truncate_identifier(format!(
                        "__reflex_union_mirror_upd_{}_{}",
                        safe_wrapper, i
                    )),
                ];
                let operand_oid = match client
                    .select(
                        "SELECT to_regclass($1)::oid::bigint AS oid",
                        None,
                        &[unsafe {
                            DatumWithOid::new(
                                operand.to_string(),
                                PgBuiltInOids::TEXTOID.oid().value(),
                            )
                        }],
                    )
                    .unwrap_or_report()
                    .first()
                    .get_by_name::<i64, _>("oid")
                    .unwrap_or(None)
                {
                    Some(0) | None => continue,
                    Some(o) => o,
                };
                let mut present: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                let rs = client
                    .select(
                        "SELECT tgname::text AS n FROM pg_trigger \
                         WHERE tgrelid = $1::oid AND NOT tgisinternal",
                        None,
                        &[unsafe {
                            DatumWithOid::new(operand_oid, PgBuiltInOids::INT8OID.oid().value())
                        }],
                    )
                    .unwrap_or_report();
                for row in rs {
                    if let Ok(Some(n)) = row.get_by_name::<&str, _>("n") {
                        present.insert(n.to_string());
                    }
                }
                let missing: Vec<&String> =
                    expected.iter().filter(|e| !present.contains(*e)).collect();
                if !missing.is_empty() {
                    let names: Vec<String> = missing.iter().map(|s| (*s).clone()).collect();
                    out.push(Finding {
                        imv: Some(imv.name.clone()),
                        severity: Severity::Error,
                        category: "trigger-attached",
                        finding: format!(
                            "Materialised UNION-ALL wrapper operand {} (index {}) is \
                             missing mirror trigger(s): {}. Any writes to this operand \
                             while the trigger was absent were NOT mirrored into the \
                             wrapper and will not be backfilled by reinstalling it — \
                             verify row counts (or reconcile the whole chain) if writes \
                             happened during the gap.",
                            operand,
                            i,
                            names.join(", ")
                        ),
                        suggested_fix: format!(
                            "SELECT reflex_rebuild_union_mirror('{}'); -- restores FUTURE \
                             maintenance only, does not backfill missed deltas",
                            wrapper_name
                        ),
                    });
                }
            }
            return out;
        }
        let mut out = Vec::new();
        for src in imv.real_sources() {
            let statement_triggers = ["ins", "del", "upd", "trunc"]
                .map(|op| crate::schema_builder::source_trigger_name(src, op));
            // Resolve source oid; skip if missing (source-exists check covers it).
            let source_oid = match client
                .select(
                    "SELECT to_regclass($1)::oid::bigint AS oid",
                    None,
                    &[unsafe {
                        DatumWithOid::new(src.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<i64, _>("oid")
                .unwrap_or(None)
            {
                Some(0) | None => continue,
                Some(o) => o,
            };
            let mut carried: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();
            let rs = client
                .select(
                    "SELECT tgname::text AS n, tgenabled::text AS e FROM pg_trigger \
                     WHERE tgrelid = $1::oid AND NOT tgisinternal",
                    None,
                    &[unsafe {
                        DatumWithOid::new(source_oid, PgBuiltInOids::INT8OID.oid().value())
                    }],
                )
                .unwrap_or_report();
            for row in rs {
                if let (Ok(Some(n)), Ok(Some(e))) = (
                    row.get_by_name::<&str, _>("n"),
                    row.get_by_name::<&str, _>("e"),
                ) {
                    carried.insert(n.to_string(), e.to_string());
                }
            }
            let in_state = |state: TriggerState| -> Vec<String> {
                statement_triggers
                    .iter()
                    .filter(|name| {
                        TriggerState::of(carried.get(*name).map(String::as_str)) == state
                    })
                    .cloned()
                    .collect()
            };
            let suggested_fix = format!(
                "SELECT reflex_rebuild_triggers('{}');\nSELECT reflex_reconcile('{}');",
                src.replace('\'', "''"),
                imv.name.replace('\'', "''")
            );
            let missing = in_state(TriggerState::Missing);
            if !missing.is_empty() {
                out.push(Finding {
                    imv: Some(imv.name.clone()),
                    severity: Severity::Error,
                    category: "trigger-attached",
                    finding: format!(
                        "Source {} is missing trigger(s): {}",
                        src,
                        missing.join(", ")
                    ),
                    suggested_fix: suggested_fix.clone(),
                });
            }
            let disabled = in_state(TriggerState::Disabled);
            if !disabled.is_empty() {
                out.push(Finding {
                    imv: Some(imv.name.clone()),
                    severity: Severity::Error,
                    category: "trigger-attached",
                    finding: format!(
                        "Source {} has disabled trigger(s): {}",
                        src,
                        disabled.join(", ")
                    ),
                    suggested_fix: suggested_fix.clone(),
                });
            }

            // (table, missing, disabled) for the tables of the partition tree,
            // the source included, in tree order.
            let mut damaged_tables: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();
            for trigger in
                crate::source_partition_writes::tree_triggers(client, src).unwrap_or_report()
            {
                let reported_above =
                    trigger.on_source && statement_triggers.contains(&trigger.name);
                if trigger.state == TriggerState::Enabled || reported_above {
                    continue;
                }
                let position = damaged_tables
                    .iter()
                    .position(|(table, _, _)| *table == trigger.table)
                    .unwrap_or_else(|| {
                        damaged_tables.push((trigger.table.clone(), Vec::new(), Vec::new()));
                        damaged_tables.len() - 1
                    });
                let (_, missing, disabled) = &mut damaged_tables[position];
                match trigger.state {
                    TriggerState::Missing => missing.push(trigger.name),
                    _ => disabled.push(trigger.name),
                }
            }
            for (table, missing, disabled) in damaged_tables {
                let mut damage = Vec::new();
                if !missing.is_empty() {
                    damage.push(format!("is missing trigger(s): {}", missing.join(", ")));
                }
                if !disabled.is_empty() {
                    damage.push(format!("has disabled trigger(s): {}", disabled.join(", ")));
                }
                out.push(Finding {
                    imv: Some(imv.name.clone()),
                    severity: Severity::Error,
                    category: "trigger-attached",
                    finding: format!(
                        "Table {} of the partition tree of source {} {}. Statements \
                         addressed to {} are not maintained, or refused, until the \
                         triggers are back.",
                        table,
                        src,
                        damage.join(" and "),
                        table
                    ),
                    suggested_fix: suggested_fix.clone(),
                });
            }
        }
        out
    }
}

pub(super) struct TriggerModeMatches;

impl Check for TriggerModeMatches {
    fn id(&self) -> &'static str {
        "trigger-mode-matches"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        let mut out = Vec::new();
        for src in imv.real_sources() {
            let fn_names = ["ins", "del", "upd", "trunc"]
                .map(|op| crate::schema_builder::source_trigger_function_name(src, op));
            // Check each trigger function for consistency with refresh_mode.
            for fn_name in &fn_names {
                let body: Option<String> = client
                    .select(
                        "SELECT prosrc::text AS body FROM pg_proc p \
                         JOIN pg_namespace n ON n.oid = p.pronamespace \
                         WHERE p.proname = $1 AND n.nspname = 'public' LIMIT 1",
                        None,
                        &[unsafe {
                            DatumWithOid::new(fn_name.clone(), PgBuiltInOids::TEXTOID.oid().value())
                        }],
                    )
                    .unwrap_or_report()
                    .first()
                    .get_by_name::<&str, _>("body")
                    .unwrap_or(None)
                    .map(|s| s.to_string());
                let body = match body {
                    Some(b) => b,
                    None => continue, // trigger-attached check covers absence
                };
                let body_is_deferred = body.contains("__reflex_delta_");
                let expected_deferred = imv.refresh_mode == "DEFERRED";
                if body_is_deferred != expected_deferred {
                    out.push(Finding {
                        imv: Some(imv.name.clone()),
                        severity: Severity::Error,
                        category: "trigger-mode-matches",
                        finding: format!(
                            "Trigger function {} is in {} mode but IMV {} refresh_mode is {}.",
                            fn_name,
                            if body_is_deferred {
                                "DEFERRED"
                            } else {
                                "IMMEDIATE"
                            },
                            imv.name,
                            imv.refresh_mode,
                        ),
                        suggested_fix: format!("SELECT reflex_rebuild_triggers('{}');", src),
                    });
                }
            }
        }
        out
    }
}

pub(super) struct InternalTablesExist;

impl Check for InternalTablesExist {
    fn id(&self) -> &'static str {
        "internal-tables-exist"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }

        let mut required: Vec<String> = Vec::new();

        // Sources whose per-(IMV, source) passthrough scratch pair is missing.
        // Recovery has TWO halves: `reflex_rebuild_triggers(<source>)`
        // recreates the scratch (reconcile alone never does — it rebuilds the
        // target and would report success while the IMV stays wedged), and
        // `reflex_reconcile(<imv>)` backfills the deltas the wedge silently lost
        // (the deferred flush swallows the per-IMV 42P01 yet still unconditionally
        // purges the staged delta, so recreating the scratch fixes only future
        // flushes). Prescribing rebuild alone would leave a diverged IMV that
        // audit then reports green.
        let mut sources_missing_scratch: Vec<String> = Vec::new();

        // A decomposed WRAPPER row owns no internal relation of EITHER branch, so
        // `required` stays empty and the check is silent. In the aggregate branch
        // — `RegistryRow::decomposed` writes `aggregations = '{}'`, so
        // `is_passthrough()` `unwrap_or(false)`s — every set-op / DISTINCT ON /
        // window IMV would carry a permanent Error-severity finding demanding
        // `__reflex_intermediate_<view>` + `__reflex_affected_<view>` for a node
        // that must not have them. Its printed remedy could not clear it and, on a
        // VIEW wrapper, would raise `"<view>" is not a table`.
        //
        // Sending wrappers to the passthrough branch instead would only move the
        // false positive: that branch demands a scratch PAIR per real source, and
        // a wrapper's real sources are its sub-IMVs, for which no pair exists
        // either. See `ImvRow::is_decomposed_wrapper`.
        if imv.is_decomposed_wrapper() {
            return vec![];
        }

        if !imv.owns_intermediate() {
            // Passthrough IMVs use per-source scratch tables instead of an intermediate
            for src in imv.real_sources() {
                let pt_new =
                    crate::query_decomposer::passthrough_scratch_new_table_name(&imv.name, src);
                let pt_old =
                    crate::query_decomposer::passthrough_scratch_old_table_name(&imv.name, src);
                let missing_here =
                    !relation_exists(client, &pt_new) || !relation_exists(client, &pt_old);
                required.push(pt_new);
                required.push(pt_old);
                if missing_here {
                    sources_missing_scratch.push(src.to_string());
                }
            }
        } else {
            // Aggregate IMVs use an intermediate table and, when the plan groups,
            // group-capture tables. All are recreated by `reflex_reconcile`'s heal
            // step, which is what makes the `reflex_rebuild_imv` remedy below converge.
            required.push(crate::query_decomposer::intermediate_table_name(&imv.name));
            required.push(crate::query_decomposer::delta_scratch_table_name(&imv.name));
            required.extend(imv.group_capture_tables());
        }

        let mut missing: Vec<String> = Vec::new();
        for name in &required {
            if !relation_exists(client, name) {
                missing.push(name.clone());
            }
        }

        if missing.is_empty() {
            return vec![];
        }
        let suggested_fix = if sources_missing_scratch.is_empty() {
            format!("SELECT reflex_rebuild_imv('{}');", imv.name)
        } else {
            let mut stmts: Vec<String> = sources_missing_scratch
                .iter()
                .map(|src| format!("SELECT reflex_rebuild_triggers('{}');", src))
                .collect();
            stmts.push(format!("SELECT reflex_reconcile('{}');", imv.name));
            stmts.join(" ")
        };
        vec![Finding {
            imv: Some(imv.name.clone()),
            severity: Severity::Error,
            category: "internal-tables-exist",
            finding: format!(
                "Missing internal table(s) for IMV {}:\n  {}",
                imv.name,
                missing.join("\n  ")
            ),
            suggested_fix,
        }]
    }
}

pub(super) struct SourceExists;

impl Check for SourceExists {
    fn id(&self) -> &'static str {
        "source-exists"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        let mut missing: Vec<String> = Vec::new();
        for dep in &imv.depends_on {
            if dep.starts_with("<subquery:") || dep.starts_with("<function:") {
                continue;
            }
            if !relation_exists(client, dep) {
                missing.push(dep.clone());
            }
        }
        if missing.is_empty() {
            return vec![];
        }
        vec![Finding {
            imv: Some(imv.name.clone()),
            severity: Severity::Error,
            category: "source-exists",
            finding: format!(
                "IMV {} depends on source(s) that do not exist: {}",
                imv.name,
                missing.join(", ")
            ),
            suggested_fix: format!(
                "-- Recreate the source(s) listed above OR drop the IMV:\nSELECT drop_reflex_ivm('{}');",
                imv.name
            ),
        }]
    }
}
