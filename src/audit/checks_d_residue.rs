use super::{Check, Finding, ImvRow, Severity};
use pgrx::spi::SpiClient;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;

pub(super) struct ArchiveResidue;

/// Upper bound on a single definition probe. `statement_timeout` is USERSET
/// (no superuser dependency) and set best-effort — if the SET is rejected the
/// probe runs unbounded, still bounded in COUNT by the empty-partition gate.
const RESIDUE_PROBE_TIMEOUT: &str = "30s";

/// Qualify a bare child relname with the IMV's schema so it resolves regardless
/// of the caller's search_path.
fn qualify(schema: Option<&str>, bare: &str) -> String {
    match schema {
        Some(s) => format!("\"{}\".\"{}\"", s, bare),
        None => format!("\"{}\"", bare),
    }
}

/// Confirmed residue: the IMV definition would populate this partition but it is
/// empty. The fix MUST start with `SELECT` so reflex_doctor treats it as runnable
/// (see doctor.rs is_runnable_fix / collapse parser).
fn confirmed_finding(imv: &ImvRow, src_child: &str) -> Finding {
    Finding {
        imv: Some(imv.name.clone()),
        severity: Severity::Warning,
        category: "archive_residue",
        finding: format!(
            "Partition {} is empty but the IMV definition would populate it (archive residue)",
            src_child
        ),
        suggested_fix: format!(
            "SELECT reflex_reconcile_partition('{}', '', '{}');",
            imv.name, src_child
        ),
    }
}

/// Unverifiable: the probe could not be evaluated (bad definition, missing
/// constraint, or timeout). The fix MUST be prose (no leading `SELECT`) so
/// reflex_doctor reports it rather than executing it.
fn unverifiable_finding(imv: &ImvRow, src_child: &str) -> Finding {
    Finding {
        imv: Some(imv.name.clone()),
        severity: Severity::Warning,
        category: "archive_residue",
        finding: format!(
            "Partition {}: could not evaluate the IMV definition (query failed or timed out); cannot confirm archive residue status",
            src_child
        ),
        suggested_fix: format!(
            "Investigate source access for partition '{}', then re-run reflex_doctor or reconcile it manually",
            src_child
        ),
    }
}

impl Check for ArchiveResidue {
    fn id(&self) -> &'static str {
        "archive-residue"
    }

    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };

        let part_cols = match imv.partition_columns.as_ref() {
            Some(p) if !p.is_empty() => p,
            _ => return vec![],
        };

        // Resolve the anchor source (owns the first partition column), including
        // ignored sources so the anchor still resolves in the residue scenario.
        let mut all_sources_for_anchor: Vec<String> =
            imv.real_sources().map(|s| s.to_string()).collect();
        if let Some(ignored) = imv.ignored_sources.as_ref() {
            all_sources_for_anchor.extend(ignored.clone());
        }
        all_sources_for_anchor.sort();
        all_sources_for_anchor.dedup();

        let anchor = match crate::partition::resolve_anchor_source(
            client,
            &part_cols[0],
            &all_sources_for_anchor,
        ) {
            Ok(a) => a,
            Err(_) => return vec![],
        };

        let src_children = crate::partition::list_partition_children(client, &anchor);
        if src_children.is_empty() {
            return vec![];
        }

        // Target partition children with row counts, keyed by bare name.
        let tgt_parent = crate::query_decomposer::quote_identifier(&imv.name);
        let tgt_children = crate::partition::list_partition_children(client, &tgt_parent);
        let (imv_schema, _) = crate::query_decomposer::split_qualified_name(&imv.name);

        let existing_tgt: HashSet<String> =
            tgt_children.iter().map(|c| c.bare_name.clone()).collect();

        let mut imv_row_counts: HashMap<String, i64> = HashMap::new();
        for tgt_child in &tgt_children {
            let child_ref = qualify(imv_schema, &tgt_child.bare_name);
            let count = self
                .safe_count(
                    client,
                    &format!("SELECT count(*) AS cnt FROM {}", child_ref),
                )
                .unwrap_or(0);
            imv_row_counts.insert(tgt_child.bare_name.clone(), count);
        }

        // Only EMPTY, EXISTING target children can be residue. A missing child is
        // another check's concern; a non-empty child cannot be residue.
        let empty_src_children: Vec<String> = src_children
            .iter()
            .filter_map(|src_child| {
                let tgt = crate::partition::target_child_name(&imv.name, &src_child.bare_name);
                if existing_tgt.contains(&tgt)
                    && imv_row_counts.get(&tgt).copied().unwrap_or(0) == 0
                {
                    Some(src_child.bare_name.clone())
                } else {
                    None
                }
            })
            .collect();

        if empty_src_children.is_empty() {
            return vec![];
        }

        // Load the IMV definition once. Without base_query nothing is verifiable.
        let (base_query, has_intermediate) = match self.load_definition(client, &imv.name) {
            Some(v) => v,
            None => {
                return empty_src_children
                    .iter()
                    .map(|c| unverifiable_finding(imv, c))
                    .collect();
            }
        };

        let prior_timeout = self.set_probe_timeout(client);

        let mut findings = Vec::new();
        for src_child in &empty_src_children {
            // base_query fills the intermediate for aggregates (filtered by the
            // intermediate constraint) and the target for passthrough (target
            // constraint). Mirror build_swap_partition_ddl exactly.
            let constraint_child = if has_intermediate {
                crate::partition::intermediate_child_name(&imv.name, src_child)
            } else {
                crate::partition::target_child_name(&imv.name, src_child)
            };
            let constraint =
                match self.partition_constraint(client, &qualify(imv_schema, &constraint_child)) {
                    Some(c) => c,
                    None => {
                        findings.push(unverifiable_finding(imv, src_child));
                        continue;
                    }
                };

            let probe = format!(
                "SELECT (CASE WHEN EXISTS (SELECT 1 FROM ({}) __src WHERE ({})) THEN 1 ELSE 0 END)::bigint AS cnt",
                base_query, constraint
            );
            match self.safe_count(client, &probe) {
                Some(1) => findings.push(confirmed_finding(imv, src_child)),
                Some(_) => {} // definition yields no rows -> legitimately empty
                None => findings.push(unverifiable_finding(imv, src_child)),
            }
        }

        self.restore_probe_timeout(client, prior_timeout);
        findings
    }
}

impl ArchiveResidue {
    /// Execute a `... AS cnt` query, catching Postgres errors at the FFI
    /// boundary. Returns None on any failure (error or timeout).
    fn safe_count(&self, client: &SpiClient<'_>, sql: &str) -> Option<i64> {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            client
                .select(sql, None, &[])
                .ok()
                .and_then(|mut iter| iter.next())
                .and_then(|row| row.get_by_name::<i64, _>("cnt").ok().flatten())
        }))
        .unwrap_or_default()
    }

    /// Fetch (base_query, has_intermediate) for the IMV. has_intermediate mirrors
    /// reconcile's `!end_query.is_empty()` guard. None if base_query is
    /// absent/empty or the lookup fails.
    fn load_definition(&self, client: &SpiClient<'_>, name: &str) -> Option<(String, bool)> {
        let sql = format!(
            "SELECT base_query, COALESCE(end_query, '') AS end_query \
             FROM public.__reflex_ivm_reference WHERE name = '{}'",
            name.replace('\'', "''")
        );
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut iter = client.select(&sql, Some(1), &[]).ok()?;
            let row = iter.next()?;
            let base = row
                .get_by_name::<&str, _>("base_query")
                .ok()
                .flatten()?
                .to_string();
            if base.trim().is_empty() {
                return None;
            }
            let end = row
                .get_by_name::<&str, _>("end_query")
                .ok()
                .flatten()
                .unwrap_or("");
            Some((base, !end.trim().is_empty()))
        }))
        .ok()
        .flatten()
    }

    /// Read a partition child's constraint definition. None if the child is
    /// absent or the lookup fails.
    fn partition_constraint(&self, client: &SpiClient<'_>, child_qual: &str) -> Option<String> {
        let sql = format!(
            "SELECT pg_get_partition_constraintdef('{}'::regclass) AS constraint",
            child_qual.replace('\'', "''")
        );
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            client
                .select(&sql, Some(1), &[])
                .ok()
                .and_then(|mut iter| iter.next())
                .and_then(|row| row.get_by_name::<&str, _>("constraint").ok().flatten())
                .map(|s| s.to_string())
        }))
        .ok()
        .flatten()
        .filter(|c| !c.trim().is_empty())
    }

    /// Best-effort: set the probe timeout, returning the prior value to restore.
    /// Any failure (e.g. read-only SPI rejecting SET) is swallowed and the probe
    /// runs unbounded.
    fn set_probe_timeout(&self, client: &SpiClient<'_>) -> Option<String> {
        let prior = std::panic::catch_unwind(AssertUnwindSafe(|| {
            client
                .select("SHOW statement_timeout", Some(1), &[])
                .ok()
                .and_then(|mut it| it.next())
                .and_then(|r| r.get_by_name::<&str, _>("statement_timeout").ok().flatten())
                .map(|s| s.to_string())
        }))
        .ok()
        .flatten();

        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = client.select(
                &format!("SET statement_timeout = '{}'", RESIDUE_PROBE_TIMEOUT),
                None,
                &[],
            );
        }));

        prior
    }

    /// Restore the timeout captured by `set_probe_timeout` (best-effort).
    fn restore_probe_timeout(&self, client: &SpiClient<'_>, prior: Option<String>) {
        if let Some(prior) = prior {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let _ = client.select(
                    &format!("SET statement_timeout = '{}'", prior.replace('\'', "''")),
                    None,
                    &[],
                );
            }));
        }
    }
}
