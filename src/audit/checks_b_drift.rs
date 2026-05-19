#![allow(unused_imports)]

use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use super::{
    probe_query_columns, quote_qualified_for_regclass, relation_attname_set_quoted, shape_matches,
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
