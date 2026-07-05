use super::{quote_qualified_for_regclass, Check, Finding, ImvRow, Severity};
use pgrx::spi::SpiClient;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;

pub(super) struct ArchiveResidue;

impl Check for ArchiveResidue {
    fn id(&self) -> &'static str {
        "archive-residue"
    }

    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };

        // Skip non-partitioned IMVs
        let part_cols = match imv.partition_columns.as_ref() {
            Some(p) if !p.is_empty() => p,
            _ => return vec![],
        };

        let mut findings = Vec::new();

        // Resolve the anchor source (the source that owns the first partition column)
        let sources_vec: Vec<String> = imv.real_sources().map(|s| s.to_string()).collect();

        // Also include ignored sources when finding the anchor
        let mut all_sources_for_anchor = sources_vec.clone();
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
            Err(_) => {
                // If we can't resolve the anchor, the IMV may not be partitioned correctly
                // or the source is not visible. Skip the check in this case.
                return vec![];
            }
        };

        // Get partition children from the source
        let src_children = crate::partition::list_partition_children(client, &anchor);
        if src_children.is_empty() {
            return vec![];
        }

        // Get IMV target partition children with row counts
        let tgt_parent = crate::query_decomposer::quote_identifier(&imv.name);
        let tgt_children = crate::partition::list_partition_children(client, &tgt_parent);

        // Build map of partition key -> IMV row count.
        // Partition children carry only their bare relname; qualify with the
        // IMV's schema so the count resolves regardless of the caller's
        // search_path (the IMV, and thus its children, may live in a
        // non-public schema not on the current path).
        let (imv_schema, _) = crate::query_decomposer::split_qualified_name(&imv.name);
        let mut imv_row_counts: HashMap<String, i64> = HashMap::new();
        for tgt_child in &tgt_children {
            let child_ref = match imv_schema {
                Some(schema) => format!("\"{}\".\"{}\"", schema, tgt_child.bare_name),
                None => format!("\"{}\"", tgt_child.bare_name),
            };
            let count: i64 = self
                .safe_count(
                    client,
                    &format!("SELECT count(*) AS cnt FROM {}", child_ref),
                )
                .unwrap_or(0);

            imv_row_counts.insert(tgt_child.bare_name.clone(), count);
        }

        // For each source partition key, count source rows (applying WHERE predicate if possible)
        for src_child in &src_children {
            match self.count_source_rows_for_partition(
                client,
                &anchor,
                &src_child.bare_name,
                &imv.where_predicate,
            ) {
                Some(src_count) => {
                    // Get the corresponding IMV partition row count
                    let tgt_partition =
                        crate::partition::target_child_name(&imv.name, &src_child.bare_name);
                    let imv_count = imv_row_counts.get(&tgt_partition).copied().unwrap_or(0);

                    // If source has rows but IMV partition is empty: archive residue
                    if src_count > 0 && imv_count == 0 {
                        findings.push(Finding {
                            imv: Some(imv.name.clone()),
                            severity: Severity::Warning,
                            category: "archive_residue",
                            finding: format!(
                                "Partition {} has source rows ({}) but IMV partition is empty (0)",
                                src_child.bare_name, src_count
                            ),
                            suggested_fix: format!(
                                "SELECT reflex_reconcile_partition('{}', '', '{}');",
                                imv.name, src_child.bare_name
                            ),
                        });
                    }
                }
                None => {
                    // Failed to count source rows - emit unverifiable finding
                    findings.push(Finding {
                        imv: Some(imv.name.clone()),
                        severity: Severity::Warning,
                        category: "archive_residue",
                        finding: format!(
                            "Partition {}: could not verify source row count (query failed); cannot confirm archive residue status",
                            src_child.bare_name
                        ),
                        suggested_fix: format!(
                            "Investigate source table access for partition '{}' and re-run audit",
                            src_child.bare_name
                        ),
                    });
                }
            }
        }

        findings
    }
}

impl ArchiveResidue {
    fn safe_count(&self, client: &pgrx::spi::SpiClient<'_>, sql: &str) -> Option<i64> {
        // Execute SQL safely, catching Postgres errors that would panic the FFI boundary.
        // Returns None if the query fails (either via error or panic), gracefully degrading
        // to "could not verify" findings instead of aborting the whole check.
        // Err(_) => query crashed at the FFI boundary; unwrap_or_default() maps it
        // to None so we degrade to a "could not verify" finding.
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            client
                .select(sql, None, &[])
                .ok()
                .and_then(|mut iter| iter.next())
                .and_then(|row| row.get_by_name::<i64, _>("cnt").ok().flatten())
        }))
        .unwrap_or_default()
    }

    fn count_source_rows_for_partition(
        &self,
        client: &pgrx::spi::SpiClient<'_>,
        source: &str,
        partition_name: &str,
        where_predicate: &Option<String>,
    ) -> Option<i64> {
        // Count rows in the partition by querying the source with the partition constraint
        // The partition_name is the bare name (e.g. 'f6_src_us') of a child partition

        // First, get the partition bounds to identify which rows belong to this
        // partition. `partition_name` is a bare child relname; qualify it with
        // the source's schema so the ::regclass cast resolves even when the
        // source lives in a schema not on the current search_path.
        let (source_schema, _) = crate::query_decomposer::split_qualified_name(source);
        let qualified_partition = match source_schema {
            Some(schema) => format!("\"{}\".\"{}\"", schema, partition_name),
            None => format!("\"{}\"", partition_name),
        };
        let bound_query = format!(
            "SELECT pg_get_partition_constraintdef('{}'::regclass) AS constraint",
            qualified_partition
        );

        // A failed constraint lookup degrades to None (fall back to an unfiltered
        // source count) rather than aborting the whole residue check.
        let constraint: Option<String> = std::panic::catch_unwind(AssertUnwindSafe(|| {
            client
                .select(&bound_query, None, &[])
                .ok()
                .and_then(|mut iter| iter.next())
                .and_then(|row| row.get_by_name::<&str, _>("constraint").ok().flatten())
                .map(|s| s.to_string())
        }))
        .unwrap_or_default();

        // Quote the source table name to handle schema-qualified or special-char names
        let quoted_source = quote_qualified_for_regclass(source);

        // Build the count query
        let count_query = if let Some(constraint) = constraint {
            // Use the partition constraint to filter rows from the source
            if let Some(pred) = where_predicate {
                // Try to apply the WHERE predicate along with the partition constraint
                format!(
                    "SELECT count(*) AS cnt FROM {} WHERE ({}) AND ({})",
                    quoted_source, constraint, pred
                )
            } else {
                // Just use the partition constraint
                format!(
                    "SELECT count(*) AS cnt FROM {} WHERE ({})",
                    quoted_source, constraint
                )
            }
        } else {
            // Fall back: count all rows in source (over-report rather than miss)
            if let Some(pred) = where_predicate {
                format!(
                    "SELECT count(*) AS cnt FROM {} WHERE {}",
                    quoted_source, pred
                )
            } else {
                format!("SELECT count(*) AS cnt FROM {}", quoted_source)
            }
        };

        self.safe_count(client, &count_query)
    }
}
