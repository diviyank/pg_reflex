#![allow(unused_imports)]

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use pgrx::PgBuiltInOids;
use std::collections::HashSet;

use super::{
    probe_query_columns, quote_qualified_for_regclass, relation_attname_set_quoted,
    relation_exists, shape_matches,
};
use super::{Check, Finding, ImvRow, Severity};

pub(super) struct IntermediateShape;

impl Check for IntermediateShape {
    fn id(&self) -> &'static str {
        "intermediate-shape"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        let intermediate = crate::query_decomposer::intermediate_table_name(&imv.name);
        let actual = match relation_attname_set_quoted(client, &intermediate) {
            Some(v) => v,
            None => return vec![], // internal-tables-exist covers absence
        };
        let expected = match probe_query_columns(&imv.base_query) {
            Ok(v) => v,
            Err(_) => return vec![], // base-query-runs covers parse/plan failures
        };
        if shape_matches(&actual, &expected) {
            return vec![];
        }
        vec![Finding {
            imv: Some(imv.name.clone()),
            severity: Severity::Warning,
            category: "intermediate-shape",
            finding: format!(
                "{} has columns\n  {{{}}}\nbut base_query produces\n  {{{}}}",
                intermediate,
                actual.join(", "),
                expected.join(", "),
            ),
            suggested_fix: format!("SELECT reflex_rebuild_imv('{}');", imv.name),
        }]
    }
}

pub(super) struct TargetShape;

impl Check for TargetShape {
    fn id(&self) -> &'static str {
        "target-shape"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        // For passthrough IMVs, end_query is empty and target-shape check doesn't apply
        if imv.end_query.is_empty() {
            return vec![];
        }
        let target_quoted = quote_qualified_for_regclass(&imv.name);
        let actual = match relation_attname_set_quoted(client, &target_quoted) {
            Some(v) => v,
            None => return vec![],
        };
        let expected = match probe_query_columns(&imv.end_query) {
            Ok(v) => v,
            Err(_) => return vec![],
        };
        if shape_matches(&actual, &expected) {
            return vec![];
        }
        vec![Finding {
            imv: Some(imv.name.clone()),
            severity: Severity::Warning,
            category: "target-shape",
            finding: format!(
                "{} has columns\n  {{{}}}\nbut end_query produces\n  {{{}}}",
                imv.name,
                actual.join(", "),
                expected.join(", "),
            ),
            suggested_fix: format!("SELECT reflex_rebuild_imv('{}');", imv.name),
        }]
    }
}

pub(super) struct BaseQueryRuns;

impl Check for BaseQueryRuns {
    fn id(&self) -> &'static str {
        "base-query-runs"
    }
    fn run(&self, _client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        match probe_query_columns(&imv.base_query) {
            Ok(_) => vec![],
            Err(e) => vec![Finding {
                imv: Some(imv.name.clone()),
                severity: Severity::Warning,
                category: "base-query-runs",
                finding: format!(
                    "Probe of base_query failed: {}\nbase_query: {}",
                    e, imv.base_query
                ),
                suggested_fix: format!(
                    "-- Inspect: SELECT base_query FROM public.__reflex_ivm_reference WHERE name = '{}';\n\
                     -- Then either ALTER the source(s) to restore referenced columns or:\n\
                     SELECT drop_reflex_ivm('{}');",
                    imv.name, imv.name
                ),
            }],
        }
    }
}

pub(super) struct PartitionMirror;

impl Check for PartitionMirror {
    fn id(&self) -> &'static str {
        "partition-mirror"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        let part_cols = match imv.partition_columns.as_ref() {
            Some(p) if !p.is_empty() => p,
            _ => return vec![],
        };

        // Anchor source = the source in depends_on that owns part_cols[0].
        let sources: Vec<String> = imv.real_sources().map(|s| s.to_string()).collect();
        let anchor = match crate::partition::resolve_anchor_source(client, &part_cols[0], &sources)
        {
            Ok(a) => a,
            Err(_) => return vec![],
        };

        let src_children = crate::partition::list_partition_children(client, &anchor);
        if src_children.is_empty() {
            // Anchor not actually partitioned (or no children). Nothing to mirror.
            return vec![];
        }
        let int_parent = crate::query_decomposer::intermediate_table_name(&imv.name);
        let tgt_parent = crate::query_decomposer::quote_identifier(&imv.name);

        // Whether an intermediate table participates in this IMV at all.
        //
        // A PASSTHROUGH IMV owns none: `intermediate_column_spec` returns None at
        // create time, so `__reflex_intermediate_<view>` is never created.
        // `reflex_sync_partitions` gates every intermediate-child DDL on the same
        // `to_regclass` probe (`src/partition.rs:1055`) precisely because
        // `CREATE TABLE … PARTITION OF <absent>` raises 42P01. Diffing the
        // anchor's child set against an absent parent's (necessarily empty) child
        // list therefore reported every child as missing and prescribed a sync
        // that structurally cannot create any of them — an unrepairable phantom
        // finding, observed on 42 production IMVs.
        //
        // `end_query.is_empty()` is the authoritative signal. It is the branch the
        // runtime itself takes when deciding whether to touch an intermediate
        // (`src/partition.rs:633`/`644`/`1054`, `src/trigger/ops.rs:436`/`653`/`783`),
        // so it answers the question this check actually asks — "does an
        // intermediate participate in maintenance?" — rather than "what did create
        // time intend?". It is equivalent to `aggregations->>'is_passthrough'` for
        // every row `create_reflex_ivm` writes (`src/create_ivm/mod.rs:1572`).
        let intermediate_expected = !imv.end_query.is_empty();
        let intermediate_present = relation_exists(client, &int_parent);

        // Expected but gone entirely: no amount of child mirroring helps, and sync
        // would skip the intermediate half outright. Distinct, more severe, and
        // prescribing the only primitive that recreates the relation *and*
        // re-derives its children.
        if intermediate_expected && !intermediate_present {
            return vec![Finding {
                imv: Some(imv.name.clone()),
                severity: Severity::Error,
                category: "partition-mirror",
                finding: format!(
                    "Intermediate table {} does not exist, so none of anchor source {}'s \
                     {} child partition(s) can be mirrored onto it. \
                     reflex_sync_partitions skips the intermediate half of an absent parent.",
                    int_parent,
                    anchor,
                    src_children.len()
                ),
                suggested_fix: format!("SELECT reflex_reconcile('{}');", imv.name),
            }];
        }

        let tgt_children = crate::partition::list_partition_children(client, &tgt_parent);
        let tgt_have: HashSet<String> = tgt_children.iter().map(|c| c.bare_name.clone()).collect();
        let src_expected_tgt: HashSet<String> = src_children
            .iter()
            .map(|c| crate::partition::target_child_name(&imv.name, &c.bare_name))
            .collect();

        // Not expected (passthrough): no intermediate finding at all, whether or
        // not a relation of that name happens to exist. Gating on presence alone
        // would re-open the phantom by a second route — a stray childless
        // intermediate left over from an earlier definition of the same IMV name
        // makes every anchor child "missing" again, and the prescribed sync would
        // materialise partition children on a table no maintenance path reads or
        // writes. A stray relation is an orphan-family concern, not mirror drift.
        //
        // The target-side comparison still runs, so a clean target yields zero
        // findings rather than an empty-bodied one.
        let (missing_int, extra_int): (Vec<String>, Vec<String>) = if intermediate_expected {
            let int_children = crate::partition::list_partition_children(client, &int_parent);
            let int_have: HashSet<String> =
                int_children.iter().map(|c| c.bare_name.clone()).collect();
            let src_expected_int: HashSet<String> = src_children
                .iter()
                .map(|c| crate::partition::intermediate_child_name(&imv.name, &c.bare_name))
                .collect();
            (
                src_expected_int.difference(&int_have).cloned().collect(),
                int_have.difference(&src_expected_int).cloned().collect(),
            )
        } else {
            (Vec::new(), Vec::new())
        };
        let missing_tgt: Vec<String> = src_expected_tgt.difference(&tgt_have).cloned().collect();
        let extra_tgt: Vec<String> = tgt_have.difference(&src_expected_tgt).cloned().collect();

        if missing_int.is_empty()
            && extra_int.is_empty()
            && missing_tgt.is_empty()
            && extra_tgt.is_empty()
        {
            return vec![];
        }

        let mut msg = String::new();
        if !missing_int.is_empty() {
            msg.push_str(&format!(
                "Intermediate is missing child partitions: {}\n",
                missing_int.join(", ")
            ));
        }
        if !extra_int.is_empty() {
            msg.push_str(&format!(
                "Intermediate has unexpected child partitions: {}\n",
                extra_int.join(", ")
            ));
        }
        if !missing_tgt.is_empty() {
            msg.push_str(&format!(
                "Target is missing child partitions: {}\n",
                missing_tgt.join(", ")
            ));
        }
        if !extra_tgt.is_empty() {
            msg.push_str(&format!(
                "Target has unexpected child partitions: {}\n",
                extra_tgt.join(", ")
            ));
        }
        msg.push_str(&format!(
            "Anchor source {} has {} child partition(s).",
            anchor,
            src_children.len()
        ));

        vec![Finding {
            imv: Some(imv.name.clone()),
            severity: Severity::Warning,
            category: "partition-mirror",
            finding: msg,
            suggested_fix: format!("SELECT reflex_sync_partitions('{}');", imv.name),
        }]
    }
}

pub(super) struct PartitionTreeDrift;

impl Check for PartitionTreeDrift {
    fn id(&self) -> &'static str {
        "partition-tree-drift"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        let part_cols = match imv.partition_columns.as_ref() {
            Some(p) if !p.is_empty() => p,
            _ => return vec![],
        };

        // Anchor source = the source in depends_on that owns part_cols[0].
        let sources: Vec<String> = imv.real_sources().map(|s| s.to_string()).collect();
        let anchor = match crate::partition::resolve_anchor_source(client, &part_cols[0], &sources)
        {
            Ok(a) => a,
            Err(_) => return vec![],
        };

        // Respect the IMV's mirror depth: a deliberately-shallow IMV mirrors
        // fewer levels than the source, so compare the source tree TRUNCATED to
        // that depth (NULL partition_depth = full source depth = no truncation).
        let partition_depth: Option<i32> = crate::sql_writer::registry::read_imv(client, &imv.name)
            .and_then(|r| r.partition_depth);
        let full_src_tree = crate::partition::list_partition_tree(client, &anchor);
        let mirror_depth = partition_depth
            .map(|d| d as usize)
            .unwrap_or_else(|| crate::partition::max_tree_depth(&full_src_tree));
        let src_tree = crate::partition::truncate_partition_tree(full_src_tree, mirror_depth);

        // Get the IMV's recursive partition tree
        // (including all levels: leaves have sub_strategy == None)
        let imv_tree = crate::partition::list_partition_tree(
            client,
            &crate::query_decomposer::quote_identifier(&imv.name),
        );

        // Filter to leaves only (nodes with no sub-partitioning)
        let src_leaves: HashSet<String> = src_tree
            .iter()
            .filter(|node| node.sub_strategy.is_none())
            .map(|node| crate::partition::target_child_name(&imv.name, &node.bare_name))
            .collect();

        let imv_leaves: HashSet<String> = imv_tree
            .iter()
            .filter(|node| node.sub_strategy.is_none())
            .map(|node| node.bare_name.clone())
            .collect();

        let missing_leaves: Vec<String> = src_leaves.difference(&imv_leaves).cloned().collect();
        let extra_leaves: Vec<String> = imv_leaves.difference(&src_leaves).cloned().collect();

        if missing_leaves.is_empty() && extra_leaves.is_empty() {
            return vec![];
        }

        let mut findings: Vec<Finding> = Vec::new();

        for leaf in missing_leaves {
            findings.push(Finding {
                imv: Some(imv.name.clone()),
                severity: Severity::Warning,
                category: "partition-tree-drift",
                finding: format!(
                    "Partition drift: source leaf maps to IMV leaf '{}' which is missing",
                    leaf
                ),
                suggested_fix: format!(
                    "SELECT reflex_flush_partitions(); -- or SELECT reflex_sync_partitions('{}', TRUE);",
                    imv.name
                ),
            });
        }

        for leaf in extra_leaves {
            findings.push(Finding {
                imv: Some(imv.name.clone()),
                severity: Severity::Warning,
                category: "partition-tree-drift",
                finding: format!(
                    "Partition drift: IMV leaf '{}' has no source counterpart",
                    leaf
                ),
                suggested_fix: format!(
                    "SELECT reflex_sync_partitions('{}', TRUE); -- drop_orphans=TRUE to clean up",
                    imv.name
                ),
            });
        }

        findings
    }
}
