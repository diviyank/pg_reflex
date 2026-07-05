use pgrx::datum::TimestampWithTimeZone;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::spi::Spi;

use crate::validate_view_name;

/// One row of doctor report output. Returned by `reflex_doctor`.
type DoctorReportRow = (
    String, // check_id
    String, // severity
    String, // object
    String, // finding
    String, // action
    String, // outcome
);

/// Core implementation of reflex_doctor. This is the orchestrator that:
/// 1. Detects inconsistencies from existing primitives (pending queue, known_stale, audit)
/// 2. When fix => TRUE, applies repairs in graph order with individual subtransactions
/// 3. Returns a structured report with outcomes
pub(crate) fn reflex_doctor_impl(
    target: Option<&str>,
    fix: bool,
    drop_orphans: bool,
    max_attempts: i32,
) -> Vec<DoctorReportRow> {
    let mut rows = Vec::new();

    // F3 hardening: validate the target argument if provided
    if let Some(target_name) = target {
        if let Err(err_msg) = validate_view_name(target_name) {
            rows.push((
                "F3".to_string(),
                "ERROR".to_string(),
                target_name.to_string(),
                "Invalid target argument name".to_string(),
                "none".to_string(),
                err_msg.to_string(),
            ));
            return rows;
        }
    }

    // In dry-run mode (fix => FALSE), we only report findings without mutating anything
    if !fix {
        // Collect all findings from various sources
        rows.extend(detect_pending_queue_issues(
            target,
            max_attempts,
            false,
            drop_orphans,
        ));
        rows.extend(detect_known_stale_imvs(target, false, drop_orphans));
        rows.extend(detect_audit_findings(target, false));

        return rows;
    }

    // Fix mode: collect findings and apply repairs
    rows.extend(detect_pending_queue_issues(
        target,
        max_attempts,
        true,
        drop_orphans,
    ));
    rows.extend(detect_known_stale_imvs(target, true, drop_orphans));
    rows.extend(detect_audit_findings(target, true));

    rows
}

/// Detect pending queue issues (F1/F2): rows that are too old or have too many attempts
fn detect_pending_queue_issues(
    target: Option<&str>,
    max_attempts: i32,
    fix: bool,
    _drop_orphans: bool,
) -> Vec<DoctorReportRow> {
    let mut rows = Vec::new();

    let query = match target {
        Some(t) => format!(
            "SELECT source_root, enqueued_at, attempts, last_error FROM public.__reflex_partition_pending WHERE source_root = '{}' ORDER BY enqueued_at",
            t.replace("'", "''")
        ),
        None => "SELECT source_root, enqueued_at, attempts, last_error FROM public.__reflex_partition_pending ORDER BY enqueued_at".to_string(),
    };

    let pending_rows: Vec<(String, Option<TimestampWithTimeZone>, i32, Option<String>)> =
        Spi::connect(|client| {
            let mut result = Vec::new();
            let rs = client.select(&query, None, &[]).unwrap_or_report();
            for row in rs {
                let source_root: String = row
                    .get_by_name::<&str, _>("source_root")
                    .unwrap_or(None)
                    .unwrap_or("")
                    .to_string();
                let enqueued_at = row
                    .get_by_name::<TimestampWithTimeZone, _>("enqueued_at")
                    .unwrap_or(None);
                let attempts: i32 = row
                    .get_by_name::<i32, _>("attempts")
                    .unwrap_or(None)
                    .unwrap_or(0);
                let last_error: Option<String> = row
                    .get_by_name::<&str, _>("last_error")
                    .unwrap_or(None)
                    .map(|s| s.to_string());
                result.push((source_root, enqueued_at, attempts, last_error));
            }
            result
        });

    for (source_root, enqueued_at, attempts, last_error) in pending_rows {
        // Calculate age in seconds from enqueued_at to now
        let age_seconds = if let Some(ts) = enqueued_at {
            Spi::get_one::<i64>(&format!(
                "SELECT extract(epoch FROM now() - '{}'::timestamptz)::int8",
                ts
            ))
            .unwrap_or(None)
            .unwrap_or(0)
        } else {
            0
        };

        let check_id = if attempts >= max_attempts {
            "F2".to_string()
        } else if age_seconds > 3600 {
            // Older than 1 hour
            "F1".to_string()
        } else {
            continue; // Not old enough or too many attempts yet
        };

        let action = format!(
            "SELECT reflex_flush_partition_source('{}');",
            source_root.replace("'", "''")
        );
        let finding = format!(
            "Partition source {} enqueued for {} seconds, {} attempts, last error: {}",
            source_root,
            age_seconds,
            attempts,
            last_error.as_deref().unwrap_or("(none)")
        );

        let outcome = if fix {
            // Try to flush the partition source
            apply_partition_flush_repair(&source_root)
        } else {
            "reported".to_string()
        };

        rows.push((
            check_id,
            "WARNING".to_string(),
            source_root,
            finding,
            action,
            outcome,
        ));
    }

    rows
}

/// Apply a partition flush repair in a subtransaction
fn apply_partition_flush_repair(source_root: &str) -> String {
    // Build the repair SQL with proper escaping
    let repair_sql = format!(
        "SELECT public.reflex_flush_partition_source('{}')",
        source_root.replace("'", "''")
    );
    // Call the helper function, which executes the repair and returns 'fixed' or 'failed:...'
    let helper_call = format!(
        "SELECT public.__reflex_doctor_try_repair('{}')",
        repair_sql.replace("'", "''")
    );
    match Spi::get_one::<String>(&helper_call) {
        Ok(Some(outcome)) => outcome,
        Ok(None) => "failed:no result".to_string(),
        Err(e) => format!("failed:{}", e),
    }
}

/// Apply a reconcile repair in a subtransaction
fn apply_reconcile_repair(imv_name: &str) -> String {
    // Build the repair SQL with proper escaping
    let repair_sql = format!(
        "SELECT public.reflex_reconcile('{}')",
        imv_name.replace("'", "''")
    );
    // Call the helper function, which executes the repair and returns 'fixed' or 'failed:...'
    let helper_call = format!(
        "SELECT public.__reflex_doctor_try_repair('{}')",
        repair_sql.replace("'", "''")
    );
    match Spi::get_one::<String>(&helper_call) {
        Ok(Some(outcome)) => outcome,
        Ok(None) => "failed:no result".to_string(),
        Err(e) => format!("failed:{}", e),
    }
}

/// Apply a sync partitions repair in a subtransaction
fn apply_sync_partitions_repair(imv_name: &str, drop_orphans: bool) -> String {
    // Build the repair SQL with proper escaping
    let repair_sql = format!(
        "SELECT public.reflex_sync_partitions('{}', {})",
        imv_name.replace("'", "''"),
        drop_orphans
    );
    // Call the helper function, which executes the repair and returns 'fixed' or 'failed:...'
    let helper_call = format!(
        "SELECT public.__reflex_doctor_try_repair('{}')",
        repair_sql.replace("'", "''")
    );
    match Spi::get_one::<String>(&helper_call) {
        Ok(Some(outcome)) => outcome,
        Ok(None) => "failed:no result".to_string(),
        Err(e) => format!("failed:{}", e),
    }
}

/// Detect known_stale IMVs (F3, F4, F4b)
fn detect_known_stale_imvs(
    target: Option<&str>,
    fix: bool,
    drop_orphans: bool,
) -> Vec<DoctorReportRow> {
    let mut rows = Vec::new();

    let query = match target {
        Some(t) => format!(
            "SELECT name, graph_depth, known_stale, stale_reason FROM public.__reflex_ivm_reference WHERE (name = '{}' OR depends_on @> ARRAY['{}']) AND known_stale = TRUE ORDER BY graph_depth",
            t.replace("'", "''"),
            t.replace("'", "''")
        ),
        None => "SELECT name, graph_depth, known_stale, stale_reason FROM public.__reflex_ivm_reference WHERE known_stale = TRUE ORDER BY graph_depth".to_string(),
    };

    let stale_rows = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client.select(&query, None, &[]).unwrap_or_report();
        for row in rs {
            let name: String = row
                .get_by_name::<&str, _>("name")
                .unwrap_or(None)
                .unwrap_or("")
                .to_string();
            let graph_depth: i32 = row
                .get_by_name::<i32, _>("graph_depth")
                .unwrap_or(None)
                .unwrap_or(0);
            let stale_reason: Option<String> = row
                .get_by_name::<&str, _>("stale_reason")
                .unwrap_or(None)
                .map(|s| s.to_string());
            result.push((name, graph_depth, stale_reason));
        }
        result
    });

    for (imv_name, _graph_depth, stale_reason) in stale_rows {
        // Determine check_id:
        // (1) If this IMV has registered sub-IMVs (decomposed chain), it's F4b
        // (2) Else if stale_reason contains "overlap" → F3
        // (3) Else → F4
        let check_id = {
            // Check for decomposed chain: sub-IMVs named with the pattern <bare_name>__*
            let (_, bare_imv) = crate::query_decomposer::canonical_source(&imv_name);

            // F10 fix: escape LIKE metacharacters (\ _ %) in bare_imv to match literally.
            // This ensures underscore positions in real names don't act as wildcards.
            // For example, "base_v" should NOT match "base_xy" under the pattern "base_v__%".
            let escaped_bare = bare_imv
                .replace('\\', "\\\\")
                .replace('_', "\\_")
                .replace('%', "\\%");

            let pattern_suffix = format!("{}__", escaped_bare);
            // Escape single quotes for SQL string literal
            let escaped_pattern = pattern_suffix.replace("'", "''");

            let is_decomposed_chain = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(SELECT 1 FROM public.__reflex_ivm_reference WHERE name LIKE '{}%' ESCAPE '\\')",
                escaped_pattern
            ))
            .unwrap_or(None)
            .unwrap_or(false);

            if is_decomposed_chain {
                "F4b".to_string()
            } else if stale_reason
                .as_deref()
                .map(|r| r.to_lowercase().contains("overlap"))
                == Some(true)
            {
                "F3".to_string()
            } else {
                "F4".to_string()
            }
        };

        let (action, outcome) = match check_id.as_str() {
            "F4b" => {
                (
                    format!(
                        "SELECT reflex_rebuild_chain('{}');",
                        imv_name.replace("'", "''")
                    ),
                    "reported".to_string(), // F4b is never auto-performed
                )
            }
            "F3" => {
                let sync_action = format!(
                    "SELECT reflex_sync_partitions('{}', true);",
                    imv_name.replace("'", "''")
                );
                if drop_orphans {
                    // drop_orphans is enabled, so we can attempt the repair
                    let outcome_val = if fix {
                        apply_sync_partitions_repair(&imv_name, true)
                    } else {
                        "reported".to_string() // dry run
                    };
                    (sync_action, outcome_val)
                } else {
                    // drop_orphans is disabled, so skip this repair
                    (sync_action, "skipped(needs drop_orphans)".to_string())
                }
            }
            _ => {
                // F4 case
                (
                    format!(
                        "SELECT reflex_reconcile('{}');",
                        imv_name.replace("'", "''")
                    ),
                    if fix {
                        apply_reconcile_repair(&imv_name)
                    } else {
                        "reported".to_string()
                    },
                )
            }
        };

        rows.push((
            check_id,
            "WARNING".to_string(),
            imv_name,
            format!(
                "IMV is known_stale: {}",
                stale_reason.unwrap_or_else(|| "(unknown reason)".to_string())
            ),
            action,
            outcome,
        ));
    }

    rows
}

/// Threshold for collapsing multiple residual partitions into a single reconcile(imv) action.
/// At or below this count, use per-partition reconcile_partition calls (surgical).
/// Above this count, collapse to a single reconcile(imv) call (one pass through the tree).
const RESIDUE_COLLAPSE_THRESHOLD: usize = 3;

/// Detect audit findings (F5/F6 for archive_residue, F8 for bare_name_ambiguity).
///
/// Consumes the audit's *structured* findings rather than scraping the formatted
/// text report, so each row carries the real IMV name and an executable
/// suggested fix. In fix mode, archive_residue is repaired via its
/// (per-partition) reconcile command; bare_name_ambiguity stays reported because
/// its remedy is a manual schema-qualification, not a runnable statement.
///
/// For archive_residue findings with many (> RESIDUE_COLLAPSE_THRESHOLD) confirmed
/// residual partitions, collapses them into a single reflex_reconcile(imv) action
/// instead of per-partition reflex_reconcile_partition actions.
fn detect_audit_findings(target: Option<&str>, fix: bool) -> Vec<DoctorReportRow> {
    let scope = match target {
        Some(t) => crate::audit::AuditScope::One(t.to_string()),
        None => crate::audit::AuditScope::All,
    };

    let mut rows = Vec::new();
    let findings = crate::audit::collect_audit_findings(scope);

    // Group confirmed-residue findings by IMV to enable collapsing logic.
    // Confirmed-residue = runnable (starts with SELECT).
    // Advisory-residue = prose, never executed.
    use std::collections::BTreeMap;
    let mut residue_by_imv: BTreeMap<String, Vec<_>> = BTreeMap::new();
    let mut non_residue_findings = Vec::new();

    for finding in findings {
        match finding.category {
            "archive_residue" => {
                let is_runnable = is_runnable_fix(&finding.suggested_fix);
                if is_runnable {
                    // Confirmed residue: group by IMV
                    let imv = finding.imv.clone().unwrap_or_default();
                    residue_by_imv
                        .entry(imv)
                        .or_insert_with(Vec::new)
                        .push(finding);
                } else {
                    // Advisory residue: treat as non-residue (report as-is)
                    non_residue_findings.push(finding);
                }
            }
            _ => {
                // bare_name_ambiguity and any other categories: report as-is
                non_residue_findings.push(finding);
            }
        }
    }

    // Process confirmed-residue findings with collapse logic
    for (imv_name, imv_findings) in residue_by_imv {
        if imv_findings.len() > RESIDUE_COLLAPSE_THRESHOLD {
            // Collapse: emit one row with reflex_reconcile(imv)
            let partition_list = imv_findings
                .iter()
                .filter_map(|f| {
                    // Extract partition name from suggested_fix
                    // Format: "SELECT reflex_reconcile_partition('<imv>', '', '<child>');"
                    // The last quoted string is the partition name
                    let parts = f.suggested_fix.split('\'').collect::<Vec<_>>();
                    if parts.len() >= 7 {
                        // parts[0] = "SELECT reflex_reconcile_partition("
                        // parts[1] = imv_name
                        // parts[2] = ", "
                        // parts[3] = empty (second '')
                        // parts[4] = ", "
                        // parts[5] = partition_name
                        // parts[6] = rest
                        Some(parts[5].to_string())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            let partition_names = partition_list.join(", ");
            let finding_summary = format!(
                "{} partitions show archive residue: {}",
                imv_findings.len(),
                partition_names
            );

            let collapsed_action = format!(
                "SELECT reflex_reconcile('{}');",
                imv_name.replace("'", "''")
            );
            let outcome = if fix {
                apply_doctor_repair(&collapsed_action)
            } else {
                "reported".to_string()
            };

            rows.push((
                "F5/F6".to_string(),
                imv_findings[0].severity.to_string(),
                imv_name,
                finding_summary,
                collapsed_action,
                outcome,
            ));
        } else {
            // Below threshold: emit per-partition rows as-is
            for finding in imv_findings {
                let object = finding.imv.clone().unwrap_or_default();
                let outcome = if fix {
                    apply_doctor_repair(&finding.suggested_fix)
                } else {
                    "reported".to_string()
                };

                rows.push((
                    "F5/F6".to_string(),
                    finding.severity.to_string(),
                    object,
                    finding.finding,
                    finding.suggested_fix,
                    outcome,
                ));
            }
        }
    }

    // Process non-residue findings (advisory residue, F8, orphan objects, etc.).
    // All are report-only: their remedies are either manual prose (F8,
    // duplicate-function) or a destructive DROP (orphan objects) that an operator
    // must authorize, so none are auto-executed here.
    for finding in non_residue_findings {
        let check_id = match finding.category {
            "archive_residue" => "F5/F6", // advisory "could not verify" variant
            "bare_name_ambiguity" => "F8",
            "orphan-intermediate" | "orphan-staging" | "orphan-scratch" => "F9",
            "duplicate-function" => "F11",
            _ => continue,
        };

        // Global checks (orphans, duplicate-function) carry no IMV; name the
        // object from the leading token of the finding text (the qualified table
        // or function name) so the row is not blank.
        let object = finding.imv.clone().unwrap_or_else(|| {
            finding
                .finding
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_string()
        });
        let outcome = "reported".to_string();

        rows.push((
            check_id.to_string(),
            finding.severity.to_string(),
            object,
            finding.finding,
            finding.suggested_fix,
            outcome,
        ));
    }

    rows
}

/// Check if a suggested fix is runnable (i.e., a SELECT statement).
/// Returns true if the trimmed text (after removing trailing `;`) starts with `SELECT` (case-insensitive).
fn is_runnable_fix(suggested_fix: &str) -> bool {
    let trimmed = suggested_fix.trim().trim_end_matches(';');
    trimmed.to_uppercase().starts_with("SELECT")
}

/// Execute an already-formed repair statement in a subtransaction, returning
/// 'fixed' or 'failed:...'. Unlike the `apply_*_repair` helpers, the caller
/// supplies the full SQL (e.g. an audit finding's `suggested_fix`).
///
/// IMPORTANT: Caller must ensure the suggested_fix is runnable (i.e., a SELECT statement).
/// Prose suggestions are never passed to this function.
fn apply_doctor_repair(repair_sql: &str) -> String {
    let trimmed = repair_sql.trim().trim_end_matches(';');
    let helper_call = format!(
        "SELECT public.__reflex_doctor_try_repair('{}')",
        trimmed.replace("'", "''")
    );
    match Spi::get_one::<String>(&helper_call) {
        Ok(Some(outcome)) => outcome,
        Ok(None) => "failed:no result".to_string(),
        Err(e) => format!("failed:{}", e),
    }
}
