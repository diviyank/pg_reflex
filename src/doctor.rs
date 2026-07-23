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

/// Detect pending-queue issues (F1/F2).
///
/// Classification reads `failures` — the counter the drain's EXCEPTION handler
/// bumps and the one `PARTITION_FLUSH_FAILURE_CAP` gates on — never `attempts`,
/// which the *enqueue* path bumps once per partition ATTACH. A busy source
/// crosses any retry threshold within a day, so classifying on `attempts` made
/// every such root permanently "too many attempts".
///
/// Age is measured from `last_attempt_at`, falling back to `enqueued_at` only
/// when no drain has ever fired for the row. `enqueued_at` is reset by every
/// re-enqueue, so on a busy source it reports a fresh age over an old failure —
/// which is how a fixed bug's `last_error` came to be presented as the current
/// cause.
fn detect_pending_queue_issues(
    target: Option<&str>,
    max_attempts: i32,
    fix: bool,
    _drop_orphans: bool,
) -> Vec<DoctorReportRow> {
    let mut rows = Vec::new();

    let filter = match target {
        Some(t) => format!("WHERE p.source_root = '{}' ", t.replace("'", "''")),
        None => String::new(),
    };
    // `wedged_since` is the earliest stale_since among the IMVs fed by this root.
    // It is the only timestamp neither the enqueue nor the drain resets, so it is
    // the one honest answer to "how long has this actually been broken".
    let query = format!(
        "SELECT p.source_root, \
                p.attempts, \
                p.failures, \
                p.last_error, \
                p.last_attempt_at IS NOT NULL AS attempted, \
                extract(epoch FROM now() - COALESCE(p.last_attempt_at, p.enqueued_at))::int8 AS attempt_age, \
                extract(epoch FROM now() - p.enqueued_at)::int8 AS pending_age, \
                (SELECT min(r.stale_since) FROM public.__reflex_ivm_reference r \
                  WHERE COALESCE(r.enabled, TRUE) \
                    AND (r.depends_on @> ARRAY[p.source_root] \
                         OR r.depends_on @> ARRAY[split_part(p.source_root, '.', 2)]))::text AS wedged_since \
           FROM public.__reflex_partition_pending p {filter}\
          ORDER BY p.enqueued_at"
    );

    let pending_rows: Vec<PendingQueueRow> = Spi::connect(|client| {
        let mut result = Vec::new();
        let rs = client.select(&query, None, &[]).unwrap_or_report();
        for row in rs {
            result.push(PendingQueueRow {
                source_root: row
                    .get_by_name::<&str, _>("source_root")
                    .unwrap_or(None)
                    .unwrap_or("")
                    .to_string(),
                attempts: row
                    .get_by_name::<i32, _>("attempts")
                    .unwrap_or(None)
                    .unwrap_or(0),
                failures: row
                    .get_by_name::<i32, _>("failures")
                    .unwrap_or(None)
                    .unwrap_or(0),
                last_error: row
                    .get_by_name::<&str, _>("last_error")
                    .unwrap_or(None)
                    .map(|s| s.to_string()),
                attempted: row
                    .get_by_name::<bool, _>("attempted")
                    .unwrap_or(None)
                    .unwrap_or(false),
                attempt_age: row
                    .get_by_name::<i64, _>("attempt_age")
                    .unwrap_or(None)
                    .unwrap_or(0),
                pending_age: row
                    .get_by_name::<i64, _>("pending_age")
                    .unwrap_or(None)
                    .unwrap_or(0),
                wedged_since: row
                    .get_by_name::<&str, _>("wedged_since")
                    .unwrap_or(None)
                    .map(|s| s.to_string()),
            });
        }
        result
    });

    for p in pending_rows {
        // F2b before F2: a capped root is not merely failing, it has been given
        // up on — no flush will touch it again until it is re-armed, which is a
        // different instruction to the operator.
        let check_id = if p.is_capped() {
            "F2b"
        } else if p.failures >= max_attempts {
            "F2"
        } else if p.attempt_age > PENDING_ROW_STALE_SECONDS {
            "F1"
        } else {
            continue;
        };

        let escaped_root = p.source_root.replace("'", "''");
        // A capped root needs re-arming first: the flush this action prescribes
        // declines to touch it otherwise, which made the F1/F2 remedy a
        // guaranteed no-op on exactly the rows most likely to be capped.
        let action = if p.is_capped() {
            format!(
                "SELECT reflex_reset_partition_failures('{0}'); \
                 SELECT reflex_flush_partition_source('{0}');",
                escaped_root
            )
        } else {
            format!("SELECT reflex_flush_partition_source('{}');", escaped_root)
        };
        let finding = p.describe();
        let outcome = if fix {
            if p.is_capped() {
                // Grant exactly ONE attempt, by re-arming to CAP - 1 rather than 0:
                // the flush below then runs, and if it fails the drain's own
                // `failures + 1` re-caps the root immediately. Zeroing would hand
                // the commit-time drain a full fresh budget every time a cron ran
                // the doctor, so a poison root would never be permanently skipped —
                // and would also hide the root from this report for an hour.
                if crate::partition::rearm_capped_partition_root(&p.source_root) == 0 {
                    rows.push((
                        check_id.to_string(),
                        "WARNING".to_string(),
                        p.source_root,
                        finding,
                        action,
                        "failed:could not re-arm the capped root".to_string(),
                    ));
                    continue;
                }
            }
            verify_pending_drained(&p.source_root, apply_partition_flush_repair(&p.source_root))
        } else {
            "reported".to_string()
        };

        rows.push((
            check_id.to_string(),
            "WARNING".to_string(),
            p.source_root,
            finding,
            action,
            outcome,
        ));
    }

    rows
}

/// A pending row is "old" past this many seconds since its last drain attempt.
const PENDING_ROW_STALE_SECONDS: i64 = 3600;

/// One `__reflex_partition_pending` row, read with everything a finding needs to
/// be both classified and dated.
struct PendingQueueRow {
    source_root: String,
    /// Enqueues since the last successful drain — NOT retries.
    attempts: i32,
    /// Consecutive drain failures. The classification column.
    failures: i32,
    last_error: Option<String>,
    /// False when no drain has ever fired for this row (the F1 re-arm hole).
    attempted: bool,
    attempt_age: i64,
    pending_age: i64,
    wedged_since: Option<String>,
}

/// A pending-queue repair is `fixed` only when the row actually left the queue.
///
/// A successful drain DELETEs the row, so a row that is still present means the
/// drain either declined (capped) or failed and rolled back. Neither is visible in
/// the flush's return value: the capped path returns a perfectly normal
/// "OK — nothing pending", and a per-root failure is swallowed into a WARNING by
/// the drain's own EXCEPTION handler. Without this check the doctor reported
/// `fixed` for a repair that did nothing.
///
/// Scope: this proves the QUEUE drained, not that downstream maintenance
/// succeeded. The drain issues `PERFORM public.reflex_reconcile(imv)`, and
/// `PERFORM` discards the `ERROR: …` string that reflex_reconcile returns for a
/// soft failure. A drained row means the flush committed, not that every dependent
/// IMV is now correct.
fn verify_pending_drained(source_root: &str, outcome: String) -> String {
    if outcome != "fixed" {
        return outcome;
    }
    let escaped = source_root.replace("'", "''");
    let still_queued = Spi::get_one::<i32>(&format!(
        "SELECT failures FROM public.__reflex_partition_pending WHERE source_root = '{}'",
        escaped
    ))
    .unwrap_or(None);
    match still_queued {
        Some(failures) => format!("failed:still queued after flush (failures = {})", failures),
        None => outcome,
    }
}

impl PendingQueueRow {
    /// At or past the cap both flush entry points decline this root, so nothing
    /// short of a re-arm can move it.
    fn is_capped(&self) -> bool {
        self.failures >= crate::partition::PARTITION_FLUSH_FAILURE_CAP
    }

    fn describe(&self) -> String {
        let cap_note = if self.is_capped() {
            " — auto-retry suppressed, failure cap reached"
        } else {
            ""
        };
        let attempt_phrase = if self.attempted {
            format!("last drain attempt {}s ago", self.attempt_age)
        } else {
            "never attempted".to_string()
        };
        let wedge_phrase = match &self.wedged_since {
            Some(ts) => format!("dependent IMVs stale since {}", ts),
            None => "no dependent IMV flagged stale".to_string(),
        };
        format!(
            "{}: {} consecutive drain failure(s){}; {}; pending {}s; {} enqueue(s); {}; last error: {}",
            self.source_root,
            self.failures,
            cap_note,
            attempt_phrase,
            self.pending_age,
            self.attempts,
            wedge_phrase,
            self.last_error.as_deref().unwrap_or("(none)")
        )
    }
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

/// Apply a reconcile repair in a subtransaction.
///
/// Calls the two-argument `reflex_reconcile(view, drop_orphans)` overload (1.11.0)
/// so an F4 / F4b repair honours the operator's `drop_orphans` choice instead of
/// silently dropping orphan partitions the way the one-argument form does. F3
/// already gates its orphan drop; this keeps F4/F4b consistent with it.
fn apply_reconcile_repair(imv_name: &str, drop_orphans: bool) -> String {
    // Build the repair SQL with proper escaping
    let repair_sql = format!(
        "SELECT public.reflex_reconcile('{}', {})",
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

/// F3's authorized repair: drop the colliding orphan partitions, then refill.
///
/// `reflex_sync_partitions(imv, true)` removes IMV-side children that map to no
/// live source partition, NOTICEing each one — which is how the operator learns
/// what blocked the swap. `reflex_reconcile` then refills and is the only path
/// that clears `known_stale`. Sync alone reported success over an IMV the health
/// surface still called broken, so the same finding came back on every run.
fn apply_f3_repair(imv_name: &str) -> String {
    let escaped = imv_name.replace("'", "''");
    let sync_outcome = apply_doctor_repair(&format!(
        "SELECT public.reflex_sync_partitions('{}', true)",
        escaped
    ));
    if sync_outcome != "fixed" {
        return sync_outcome;
    }
    apply_doctor_repair(&format!("SELECT public.reflex_reconcile('{}')", escaped))
}

/// A repair is only `fixed` when the registry agrees it is.
///
/// `reflex_sync_partitions` never clears `known_stale` and `reflex_reconcile` can
/// fail softly, so an unverified outcome lets the doctor claim it fixed something
/// while its own health surface still reports the IMV as stale.
fn verify_stale_cleared(imv_name: &str, outcome: String) -> String {
    if outcome != "fixed" {
        return outcome;
    }
    let still_stale = Spi::get_one::<bool>(&format!(
        "SELECT COALESCE(known_stale, FALSE) FROM public.__reflex_ivm_reference WHERE name = '{}'",
        imv_name.replace("'", "''")
    ))
    .unwrap_or(None)
    .unwrap_or(false);
    if still_stale {
        "failed:known_stale still set after repair".to_string()
    } else {
        outcome
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
        // (1) If this IMV is a decomposed parent (depends on a pg_reflex-generated
        //     sub-IMV), it's F4b
        // (2) Else if stale_reason contains "overlap" → F3
        // (3) Else → F4
        let check_id = {
            // 1.11.0: classify on the authoritative registry graph, not a name
            // heuristic. The old `name LIKE '<bare>__%'` probe silently missed
            // every schema-qualified decomposed IMV — its registry name is
            // `s.qv__cte_b`, which does not begin with the parent's bare `qv__`,
            // so `s.qv` was misclassified F4 and given a repair that could not
            // handle its chain. PS-1 now records `is_generated_sub_imv` on the
            // child and the edge in the parent's `depends_on_imv`; a decomposed
            // parent is exactly a row with a generated node in `depends_on_imv`.
            let is_decomposed_chain = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS( \
                     SELECT 1 FROM public.__reflex_ivm_reference child \
                      WHERE child.name = ANY( \
                          SELECT unnest(COALESCE(parent.depends_on_imv, ARRAY[]::TEXT[])) \
                            FROM public.__reflex_ivm_reference parent WHERE parent.name = '{}') \
                        AND COALESCE(child.is_generated_sub_imv, FALSE))",
                imv_name.replace("'", "''")
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
                // Pre-1.11.0 this prescribed reflex_rebuild_chain and never ran it.
                // reflex_rebuild_chain drop+recreates from the registry sql_query,
                // which for a CTE-decomposed parent is the *rewritten* body naming
                // a child the cascade just dropped — so it hard-errors on exactly
                // this shape (D22, still open, PS-2's file). PS-1 made
                // reflex_reconcile rebuild the generated sub-IMVs bottom-up first,
                // so it is now the correct — and safe, rebuild-in-place — remedy,
                // repaired under `fix` like F4. drop_orphans is honoured via the
                // scoped overload.
                (
                    format!(
                        "SELECT reflex_reconcile('{}', {});",
                        imv_name.replace("'", "''"),
                        drop_orphans
                    ),
                    if fix {
                        verify_stale_cleared(
                            &imv_name,
                            apply_reconcile_repair(&imv_name, drop_orphans),
                        )
                    } else {
                        "reported".to_string()
                    },
                )
            }
            "F3" => {
                let sync_action = format!(
                    "SELECT reflex_sync_partitions('{0}', true); SELECT reflex_reconcile('{0}');",
                    imv_name.replace("'", "''")
                );
                if drop_orphans {
                    // drop_orphans is enabled, so we can attempt the repair
                    let outcome_val = if fix {
                        verify_stale_cleared(&imv_name, apply_f3_repair(&imv_name))
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
                        "SELECT reflex_reconcile('{}', {});",
                        imv_name.replace("'", "''"),
                        drop_orphans
                    ),
                    if fix {
                        verify_stale_cleared(
                            &imv_name,
                            apply_reconcile_repair(&imv_name, drop_orphans),
                        )
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
            // The IMV-vs-source partition-structure findings. These name an orphan
            // mirror partition of a LIVE parent — the case the orphan-* checks
            // deliberately no longer claim (it is not "unowned by any IMV", it is
            // "owned but no longer backed by a source partition"). The audit
            // already detects it with a correct suggested fix; without forwarding
            // it here, nothing reaches reflex_doctor, because F3 only exists once a
            // maintenance attempt has already failed.
            "partition-mirror" | "partition-tree-drift" => "F3",
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
