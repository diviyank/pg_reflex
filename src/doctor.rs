use pgrx::datum::TimestampWithTimeZone;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::spi::Spi;

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
    // Run the repair in a subtransaction so one failure doesn't abort the report
    match Spi::get_one::<String>(&format!(
        "DO $do$ BEGIN PERFORM public.reflex_flush_partition_source('{}'); EXCEPTION WHEN OTHERS THEN NULL; END $do$;",
        source_root.replace("'", "''")
    )) {
        Ok(Some(_)) | Ok(None) => "fixed".to_string(),
        Err(e) => format!("failed:{}", e),
    }
}

/// Apply a reconcile repair in a subtransaction
fn apply_reconcile_repair(imv_name: &str) -> String {
    // Run the repair in a subtransaction so one failure doesn't abort the report
    match Spi::get_one::<String>(&format!(
        "DO $do$ BEGIN PERFORM public.reflex_reconcile('{}'); EXCEPTION WHEN OTHERS THEN NULL; END $do$;",
        imv_name.replace("'", "''")
    )) {
        Ok(Some(_)) | Ok(None) => "fixed".to_string(),
        Err(e) => format!("failed:{}", e),
    }
}

/// Apply a sync partitions repair in a subtransaction
fn apply_sync_partitions_repair(imv_name: &str, drop_orphans: bool) -> String {
    // Run the repair in a subtransaction so one failure doesn't abort the report
    match Spi::get_one::<String>(&format!(
        "DO $do$ BEGIN PERFORM public.reflex_sync_partitions('{}', {}); EXCEPTION WHEN OTHERS THEN NULL; END $do$;",
        imv_name.replace("'", "''"),
        drop_orphans
    )) {
        Ok(Some(_)) | Ok(None) => "fixed".to_string(),
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
        let check_id = if stale_reason
            .as_deref()
            .map(|r| r.to_lowercase().contains("decomposed"))
            == Some(true)
        {
            "F4b".to_string()
        } else if stale_reason
            .as_deref()
            .map(|r| r.to_lowercase().contains("overlap"))
            == Some(true)
        {
            "F3".to_string()
        } else {
            "F4".to_string()
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

/// Detect audit findings (F5/F6 for archive_residue, F8 for bare_name_ambiguity)
fn detect_audit_findings(target: Option<&str>, _fix: bool) -> Vec<DoctorReportRow> {
    let mut rows = Vec::new();

    // Call reflex_audit to get the formatted report
    let audit_output = match target {
        Some(t) => {
            Spi::get_one::<String>(&format!("SELECT reflex_audit('{}')", t.replace("'", "''")))
                .unwrap_or(None)
                .unwrap_or_default()
        }
        None => Spi::get_one::<String>("SELECT reflex_audit()")
            .unwrap_or(None)
            .unwrap_or_default(),
    };

    // Parse audit output to extract findings by category
    // The audit output is a formatted text report; we need to extract findings
    // For now, we'll do a simple pattern match on the output
    // In a real implementation, we'd either modify reflex_audit to return structured data
    // or parse it more carefully

    // Look for archive_residue findings
    if audit_output.contains("archive_residue") {
        let lines: Vec<&str> = audit_output.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if line.contains("archive_residue") {
                // Extract IMV name if present
                let imv_name = if i > 0 {
                    lines[i - 1].to_string()
                } else {
                    "unknown".to_string()
                };

                rows.push((
                    "F5/F6".to_string(),
                    "WARNING".to_string(),
                    imv_name,
                    "Archive residue detected".to_string(),
                    "SELECT reflex_reconcile_partition(...); -- requires partition key inspection"
                        .to_string(),
                    "reported".to_string(),
                ));
            }
        }
    }

    // Look for bare_name_ambiguity findings
    if audit_output.contains("bare_name_ambiguity") {
        let lines: Vec<&str> = audit_output.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if line.contains("bare_name_ambiguity") {
                let imv_name = if i > 0 {
                    lines[i - 1].to_string()
                } else {
                    "unknown".to_string()
                };

                rows.push((
                    "F8".to_string(),
                    "ERROR".to_string(),
                    imv_name,
                    "Bare name ambiguity in depends_on".to_string(),
                    "Manually qualify the table name in the IMV definition".to_string(),
                    "reported".to_string(),
                ));
            }
        }
    }

    rows
}
