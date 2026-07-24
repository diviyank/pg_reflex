#![allow(unused_imports)]

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use pgrx::PgBuiltInOids;

use super::{read_attname_set, relation_exists};
use super::{Check, Finding, ImvRow, Severity};

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
        // `__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>` triggers whose names
        // this check does not know.
        //
        // The remedy it printed was worse than noise: `reflex_rebuild_triggers`
        // on a sub-IMV target INSTALLS four consolidated triggers there
        // (`__reflex_trigger_ins_on_public_<sub>`, note the qualified suffix) and
        // the finding still does not clear, so an operator following the tool
        // accumulates junk triggers on every retry.
        //
        // Mirror-trigger absence on a materialised wrapper is consequently
        // unchecked; adding that check needs a repair primitive first (nothing
        // reinstalls them today) or it recreates the unclearable-remedy
        // anti-pattern. Filed as
        // `untreated_bugs/2026-07-24_union_mirror_triggers_unchecked.md`.
        if imv.is_decomposed_wrapper() {
            return vec![];
        }
        let mut out = Vec::new();
        for src in imv.real_sources() {
            let suffix = crate::query_decomposer::sanitized_source_suffix(src);
            let expected = [
                format!("__reflex_trigger_ins_on_{}", suffix),
                format!("__reflex_trigger_del_on_{}", suffix),
                format!("__reflex_trigger_upd_on_{}", suffix),
                format!("__reflex_trigger_trunc_on_{}", suffix),
            ];
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
            let mut present: std::collections::HashSet<String> = std::collections::HashSet::new();
            let rs = client
                .select(
                    "SELECT tgname::text AS n FROM pg_trigger \
                     WHERE tgrelid = $1::oid AND NOT tgisinternal",
                    None,
                    &[unsafe {
                        DatumWithOid::new(source_oid, PgBuiltInOids::INT8OID.oid().value())
                    }],
                )
                .unwrap_or_report();
            for row in rs {
                if let Ok(Some(n)) = row.get_by_name::<&str, _>("n") {
                    present.insert(n.to_string());
                }
            }
            let missing: Vec<&String> = expected.iter().filter(|e| !present.contains(*e)).collect();
            if !missing.is_empty() {
                let names: Vec<String> = missing.iter().map(|s| (*s).clone()).collect();
                out.push(Finding {
                    imv: Some(imv.name.clone()),
                    severity: Severity::Error,
                    category: "trigger-attached",
                    finding: format!("Source {} is missing trigger(s): {}", src, names.join(", ")),
                    suggested_fix: format!("SELECT reflex_rebuild_triggers('{}');", src),
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
            let suffix = crate::query_decomposer::sanitized_source_suffix(src);
            let fn_names = [
                format!("__reflex_ins_trigger_on_{}", suffix),
                format!("__reflex_del_trigger_on_{}", suffix),
                format!("__reflex_upd_trigger_on_{}", suffix),
                format!("__reflex_trunc_trigger_on_{}", suffix),
            ];
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
        // Recovery has TWO halves (PS-6): `reflex_rebuild_triggers(<source>)`
        // recreates the scratch (reconcile alone never does — it rebuilds the
        // target and would report success while the IMV stays wedged), and
        // `reflex_reconcile(<imv>)` backfills the deltas the wedge silently lost
        // (the deferred flush swallows the per-IMV 42P01 yet still unconditionally
        // purges the staged delta, so recreating the scratch fixes only future
        // flushes). Prescribing rebuild alone would leave a diverged IMV that
        // audit then reports green.
        let mut sources_missing_scratch: Vec<String> = Vec::new();

        // A decomposed WRAPPER row owns no internal relation of EITHER branch, so
        // `required` stays empty and the check is silent. Before 1.11.1 it fell
        // into the aggregate branch — `RegistryRow::decomposed` writes
        // `aggregations = '{}'`, so `is_passthrough()` `unwrap_or(false)`d — and
        // every set-op / DISTINCT ON / window IMV in the database carried a
        // permanent Error-severity finding demanding
        // `__reflex_intermediate_<view>` + `__reflex_affected_<view>` for a node
        // that must not have them. Its printed remedy could not clear it and, on a
        // VIEW wrapper, raised `"<view>" is not a table`.
        //
        // Sending wrappers to the passthrough branch instead (the fix direction the
        // field report proposed) would only move the false positive: that branch
        // demands a scratch PAIR per real source, and a wrapper's real sources are
        // its sub-IMVs, for which no pair exists either. Probed — see
        // `ImvRow::is_decomposed_wrapper`.
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
            // Aggregate IMVs use an intermediate table and affected groups table.
            // Both are recreated by `reflex_reconcile`'s heal step (1.11.1), which
            // is what makes the `reflex_rebuild_imv` remedy below converge.
            required.push(crate::query_decomposer::intermediate_table_name(&imv.name));
            required.push(crate::query_decomposer::affected_groups_table_name(
                &imv.name,
            ));
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
