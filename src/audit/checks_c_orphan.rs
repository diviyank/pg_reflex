use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::spi::SpiClient;
use pgrx::PgBuiltInOids;
use std::collections::HashSet;

use super::{Check, Finding, ImvRow, Severity};

// --- shared scan ---------------------------------------------------------

/// One relation found by a prefix scan: its schema-qualified quoted name, its
/// oid, and the oid of its top-level partition root (its own oid when it is not
/// a partition).
type ScannedRelation = (String, i64, i64);

/// Scans pg_class for relations with names starting with any of the given prefixes.
///
/// Includes partitioned parents (`relkind = 'p'`), not just ordinary tables: a
/// partitioned IMV's intermediate table IS the parent, so scanning only `'r'`
/// meant the parent was never seen while every one of its leaves — which are
/// `'r'` and are never in the expected set — was reported as unowned.
fn scan_relations_with_prefixes(client: &SpiClient<'_>, prefixes: &[&str]) -> Vec<ScannedRelation> {
    let mut out = Vec::new();
    for prefix in prefixes {
        let like_pattern = format!("{}%", prefix);
        let rs =
            client
                .select(
                    "SELECT n.nspname::text AS schema, c.relname::text AS local, \
                        c.oid::bigint AS oid, \
                        COALESCE(pg_partition_root(c.oid)::oid::bigint, c.oid::bigint) AS root_oid \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind IN ('r','p') AND c.relname LIKE $1",
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
            let oid = row.get_by_name::<i64, _>("oid").ok().flatten().unwrap_or(0);
            let root_oid = row
                .get_by_name::<i64, _>("root_oid")
                .ok()
                .flatten()
                .unwrap_or(oid);
            if !schema.is_empty() && !local.is_empty() && oid != 0 {
                out.push((format!("\"{}\".\"{}\"", schema, local), oid, root_oid));
            }
        }
    }
    out
}

/// Resolve expected aux-table names to oids.
///
/// Ownership is a catalog fact, so it is decided on oids and never on name
/// strings: the name-builders return `"schema"."x"` for a schema-qualified IMV
/// but a bare `"x"` for a bare one, while the catalog scan always yields the
/// qualified form — so for a bare-name IMV the two sets could never intersect and
/// every one of its aux tables was reported as an orphan. A name that does not
/// resolve owns nothing, which is the correct answer.
fn resolve_expected_oids(client: &SpiClient<'_>, names: &HashSet<String>) -> HashSet<i64> {
    let mut oids = HashSet::new();
    for name in names {
        let oid: Option<i64> =
            client
                .select(
                    "SELECT to_regclass($1)::oid::bigint AS oid",
                    Some(1),
                    &[unsafe {
                        DatumWithOid::new(name.clone(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .ok()
                .and_then(|mut it| it.next())
                .and_then(|row| row.get_by_name::<i64, _>("oid").ok().flatten());
        if let Some(o) = oid {
            if o != 0 {
                oids.insert(o);
            }
        }
    }
    oids
}

/// A scanned relation is owned when it, or its partition root, is expected.
fn is_owned(expected: &HashSet<i64>, oid: i64, root_oid: i64) -> bool {
    expected.contains(&oid) || expected.contains(&root_oid)
}

/// The unowned relations worth reporting: for an orphaned *partitioned* tree,
/// only its root.
///
/// An unowned leaf always has an unowned root (if the root were expected,
/// `is_owned` would have covered the leaf), so reporting every leaf as well as the
/// parent is pure duplication — dropping the root CASCADEs the leaves. A leaf is
/// suppressed only when its root is itself in this scan and therefore reported in
/// its place; a leaf attached to some relation the scan never saw is still
/// reported on its own, so nothing can hide.
fn unowned_relations(expected: &HashSet<i64>, scanned: Vec<ScannedRelation>) -> Vec<String> {
    let scanned_oids: HashSet<i64> = scanned.iter().map(|(_, oid, _)| *oid).collect();
    scanned
        .into_iter()
        .filter(|(_, oid, root_oid)| {
            !is_owned(expected, *oid, *root_oid)
                && (*oid == *root_oid || !scanned_oids.contains(root_oid))
        })
        .map(|(qualified, _, _)| qualified)
        .collect()
}

/// The registry name re-qualified with `target_schema` when it is bare, so the
/// aux-table names built from it resolve without depending on the audit session's
/// search_path. Mirrors the `resolved_name` in `src/drop_ivm.rs`.
///
/// Applies to the IMV name only. A *source* name must be passed through verbatim:
/// `sanitized_source_suffix` folds the schema into the identifier body
/// (`schema_table`), so re-qualifying a bare source would compute a table name
/// that has never existed.
///
/// Rows created before 1.7.2 keep `target_schema` NULL
/// (`sql/pg_reflex--1.7.1--1.7.2.sql`), and a bare name left unqualified resolves
/// through the session `search_path` — under a narrow one it resolves to nothing,
/// putting every aux table of a live legacy IMV back in the orphan report with a
/// `DROP … CASCADE`. `COALESCE(target_schema, 'public')` is the codebase's
/// existing convention for that gap (`src/lib.rs:963`,
/// `sql/pg_reflex--1.10.5--1.10.6.sql:65`).
fn resolved_imv_name(imv: &ImvRow) -> String {
    match crate::query_decomposer::canonical_source(&imv.name).0 {
        Some(_) => imv.name.clone(),
        None => {
            let schema = match imv.target_schema.as_deref() {
                Some(s) if !s.is_empty() => s,
                _ => "public",
            };
            format!("{}.{}", schema, imv.name)
        }
    }
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
            .map(|i| crate::query_decomposer::intermediate_table_name(&resolved_imv_name(i)))
            .collect();
        let expected = resolve_expected_oids(client, &expected);

        let actual = scan_relations_with_prefixes(client, &["__reflex_intermediate_"]);
        let mut findings = Vec::new();

        for qualified in unowned_relations(&expected, actual) {
            findings.push(Finding {
                imv: None,
                severity: Severity::Warning,
                category: "orphan-intermediate",
                finding: format!("{} has no owning enabled IMV.", qualified),
                suggested_fix: format!("DROP TABLE {} CASCADE;", qualified),
            });
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

        let expected = resolve_expected_oids(client, &expected);

        let actual = scan_relations_with_prefixes(client, &["__reflex_delta_"]);
        let mut findings = Vec::new();

        for qualified in unowned_relations(&expected, actual) {
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
            let view = resolved_imv_name(imv);
            if imv.is_passthrough() {
                for src in imv.real_sources() {
                    expected.insert(crate::query_decomposer::passthrough_scratch_new_table_name(
                        &view, src,
                    ));
                    expected.insert(crate::query_decomposer::passthrough_scratch_old_table_name(
                        &view, src,
                    ));
                }
            } else {
                expected.insert(crate::query_decomposer::affected_groups_table_name(&view));
                expected.insert(crate::query_decomposer::delta_scratch_table_name(&view));
                expected.insert(crate::query_decomposer::shrunk_groups_table_name(&view));
            }
        }
        let expected = resolve_expected_oids(client, &expected);

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

        for qualified in unowned_relations(&expected, actual) {
            findings.push(Finding {
                imv: None,
                severity: Severity::Info,
                category: "orphan-scratch",
                finding: format!("{} has no owning enabled IMV.", qualified),
                suggested_fix: format!("DROP TABLE {} CASCADE;", qualified),
            });
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
