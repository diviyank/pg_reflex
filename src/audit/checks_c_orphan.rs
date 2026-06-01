use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::spi::SpiClient;
use pgrx::PgBuiltInOids;
use std::collections::HashSet;

use super::{Check, Finding, ImvRow, Severity};

// --- shared scan ---------------------------------------------------------

/// Scans pg_class for relations with names starting with any of the given prefixes.
/// Returns (schema-qualified-quoted, bare-relname) pairs.
fn scan_relations_with_prefixes(
    client: &SpiClient<'_>,
    prefixes: &[&str],
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for prefix in prefixes {
        let like_pattern = format!("{}%", prefix);
        let rs =
            client
                .select(
                    "SELECT n.nspname::text AS schema, c.relname::text AS local \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind = 'r' AND c.relname LIKE $1",
                    None,
                    &[unsafe {
                        DatumWithOid::new(like_pattern, PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .unwrap_or_report();

        for row in rs {
            let schema = row
                .get_by_name::<&str, _>("schema")
                .ok()
                .flatten()
                .unwrap_or("")
                .to_string();
            let local = row
                .get_by_name::<&str, _>("local")
                .ok()
                .flatten()
                .unwrap_or("")
                .to_string();
            if !schema.is_empty() && !local.is_empty() {
                let qualified = format!("\"{}\".\"{}\"", schema, local);
                out.push((qualified, local));
            }
        }
    }
    out
}

// --- orphan-intermediate -------------------------------------------------

pub(super) struct OrphanIntermediate;

impl Check for OrphanIntermediate {
    fn id(&self) -> &'static str {
        "orphan-intermediate"
    }
    fn is_per_imv(&self) -> bool {
        false
    }
    fn run_global(&self, client: &SpiClient<'_>, imvs: &[ImvRow]) -> Vec<Finding> {
        let expected: HashSet<String> = imvs
            .iter()
            .filter(|i| i.enabled)
            .map(|i| crate::query_decomposer::intermediate_table_name(&i.name))
            .collect();

        let actual = scan_relations_with_prefixes(client, &["__reflex_intermediate_"]);
        let mut findings = Vec::new();

        for (qualified, _bare) in actual {
            if !expected.contains(&qualified) {
                findings.push(Finding {
                    imv: None,
                    severity: Severity::Warning,
                    category: "orphan-intermediate",
                    finding: format!("{} has no owning enabled IMV.", qualified),
                    suggested_fix: format!("DROP TABLE {} CASCADE;", qualified),
                });
            }
        }
        findings
    }
}

// --- orphan-staging ------------------------------------------------------

pub(super) struct OrphanStaging;

impl Check for OrphanStaging {
    fn id(&self) -> &'static str {
        "orphan-staging"
    }
    fn is_per_imv(&self) -> bool {
        false
    }
    fn run_global(&self, client: &SpiClient<'_>, imvs: &[ImvRow]) -> Vec<Finding> {
        let expected: HashSet<String> = imvs
            .iter()
            .filter(|i| i.enabled && i.refresh_mode == "DEFERRED")
            .flat_map(|i| {
                i.real_sources()
                    .map(crate::query_decomposer::staging_delta_table_name)
                    .collect::<Vec<_>>()
            })
            .collect();

        let actual = scan_relations_with_prefixes(client, &["__reflex_delta_"]);
        let mut findings = Vec::new();

        for (qualified, _bare) in actual {
            if !expected.contains(&qualified) {
                findings.push(Finding {
                    imv: None,
                    severity: Severity::Warning,
                    category: "orphan-staging",
                    finding: format!(
                        "{} has no enabled DEFERRED IMV depending on its source.",
                        qualified
                    ),
                    suggested_fix: format!("DROP TABLE {} CASCADE;", qualified),
                });
            }
        }
        findings
    }
}

// --- orphan-scratch ------------------------------------------------------

pub(super) struct OrphanScratch;

impl Check for OrphanScratch {
    fn id(&self) -> &'static str {
        "orphan-scratch"
    }
    fn is_per_imv(&self) -> bool {
        false
    }
    fn run_global(&self, client: &SpiClient<'_>, imvs: &[ImvRow]) -> Vec<Finding> {
        let mut expected = HashSet::new();

        for imv in imvs.iter().filter(|i| i.enabled) {
            if imv.is_passthrough() {
                for src in imv.real_sources() {
                    expected.insert(crate::query_decomposer::passthrough_scratch_new_table_name(
                        &imv.name, src,
                    ));
                    expected.insert(crate::query_decomposer::passthrough_scratch_old_table_name(
                        &imv.name, src,
                    ));
                }
            } else {
                expected.insert(crate::query_decomposer::affected_groups_table_name(
                    &imv.name,
                ));
                expected.insert(crate::query_decomposer::delta_scratch_table_name(&imv.name));
                expected.insert(crate::query_decomposer::shrunk_groups_table_name(&imv.name));
            }
        }

        let actual = scan_relations_with_prefixes(
            client,
            &[
                "__reflex_pt_new_",
                "__reflex_pt_old_",
                "__reflex_scratch_",
                "__reflex_affected_",
                "__reflex_shrunk_",
            ],
        );
        let mut findings = Vec::new();

        for (qualified, _bare) in actual {
            if !expected.contains(&qualified) {
                findings.push(Finding {
                    imv: None,
                    severity: Severity::Info,
                    category: "orphan-scratch",
                    finding: format!("{} has no owning enabled IMV.", qualified),
                    suggested_fix: format!("DROP TABLE {} CASCADE;", qualified),
                });
            }
        }
        findings
    }
}

// --- duplicate-function --------------------------------------------------

pub(super) struct DuplicateTriggerFunction;

impl Check for DuplicateTriggerFunction {
    fn id(&self) -> &'static str {
        "duplicate-function"
    }
    fn is_per_imv(&self) -> bool {
        false
    }
    fn run_global(&self, client: &SpiClient<'_>, _imvs: &[ImvRow]) -> Vec<Finding> {
        let mut findings = Vec::new();
        let rs = client
            .select(
                "SELECT p.proname::text AS fn, \
                        count(*)::bigint AS n, \
                        string_agg(n.nspname, ', ' ORDER BY n.nspname)::text AS schemas \
                 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
                 WHERE p.proname LIKE '__reflex\\_%' \
                 GROUP BY p.proname HAVING count(*) > 1",
                None,
                &[],
            )
            .unwrap_or_report();
        for row in rs {
            let fname = row
                .get_by_name::<&str, _>("fn")
                .ok()
                .flatten()
                .unwrap_or("")
                .to_string();
            let schemas = row
                .get_by_name::<&str, _>("schemas")
                .ok()
                .flatten()
                .unwrap_or("")
                .to_string();
            let n = row.get_by_name::<i64, _>("n").ok().flatten().unwrap_or(0);
            if fname.is_empty() {
                continue;
            }
            findings.push(Finding {
                imv: None,
                severity: Severity::Warning,
                category: "duplicate-function",
                finding: format!("{fname} has {n} copies across schemas: [{schemas}]"),
                suggested_fix: format!(
                    "Consolidate to public: re-point any trigger to the public copy, then \
                     DROP the non-public copies of {fname}; or run reflex_rebuild_triggers(<source>)."
                ),
            });
        }
        findings
    }
}
