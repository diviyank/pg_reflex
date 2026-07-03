use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use pgrx::PgBuiltInOids;

pub mod checks_a_catastrophic;
pub mod checks_b_drift;
pub mod checks_c_orphan;
pub mod checks_e_barename;

use checks_a_catastrophic::{
    InternalTablesExist, SourceExists, StagingShape, TriggerAttached, TriggerModeMatches,
};
use checks_b_drift::{
    BaseQueryRuns, IntermediateShape, PartitionMirror, PartitionTreeDrift, TargetShape,
};
use checks_c_orphan::{DuplicateTriggerFunction, OrphanIntermediate, OrphanScratch, OrphanStaging};
use checks_e_barename::BareNameAmbiguity;

pub enum AuditScope {
    All,
    One(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum Severity {
    Error,
    Warning,
    Info,
}

impl Severity {
    fn label(self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Warning => "WARNING",
            Severity::Info => "INFO",
        }
    }

    fn rank(self) -> u8 {
        match self {
            Severity::Error => 0,
            Severity::Warning => 1,
            Severity::Info => 2,
        }
    }
}

pub struct Finding {
    pub imv: Option<String>,
    pub severity: Severity,
    pub category: &'static str,
    pub finding: String,
    pub suggested_fix: String,
}

#[allow(dead_code)]
pub struct ImvRow {
    pub name: String,
    pub depends_on: Vec<String>,
    pub refresh_mode: String,
    pub base_query: String,
    pub end_query: String,
    pub aggregations_json: Option<String>,
    pub partition_columns: Option<Vec<String>>,
    pub enabled: bool,
}

impl ImvRow {
    #[allow(dead_code)]
    pub fn is_passthrough(&self) -> bool {
        match self.aggregations_json.as_deref() {
            Some(s) => serde_json::from_str::<serde_json::Value>(s)
                .ok()
                .and_then(|v| v.get("is_passthrough").and_then(|x| x.as_bool()))
                .unwrap_or(false),
            None => false,
        }
    }

    pub fn real_sources(&self) -> impl Iterator<Item = &str> {
        self.depends_on
            .iter()
            .map(|s| s.as_str())
            .filter(|s| !s.starts_with("<subquery:") && !s.starts_with("<function:"))
    }
}

pub trait Check {
    #[allow(dead_code)]
    fn id(&self) -> &'static str;
    fn run(&self, _client: &SpiClient<'_>, _imv: Option<&ImvRow>) -> Vec<Finding> {
        vec![]
    }
    fn is_per_imv(&self) -> bool {
        true
    }
    fn run_global(&self, _client: &SpiClient<'_>, _imvs: &[ImvRow]) -> Vec<Finding> {
        vec![]
    }
}

fn registry() -> Vec<Box<dyn Check>> {
    vec![
        Box::new(StagingShape),
        Box::new(TriggerAttached),
        Box::new(TriggerModeMatches),
        Box::new(InternalTablesExist),
        Box::new(SourceExists),
        Box::new(IntermediateShape),
        Box::new(TargetShape),
        Box::new(BaseQueryRuns),
        Box::new(PartitionMirror),
        Box::new(PartitionTreeDrift),
        Box::new(OrphanIntermediate),
        Box::new(OrphanStaging),
        Box::new(OrphanScratch),
        Box::new(DuplicateTriggerFunction),
        Box::new(BareNameAmbiguity),
    ]
}

pub(super) fn read_attname_set(
    client: &SpiClient<'_>,
    relid: i64,
    exclude: &[&str],
) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let rs = client
        .select(
            "SELECT attname::text AS n FROM pg_attribute \
             WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped \
             ORDER BY attname",
            None,
            &[unsafe { DatumWithOid::new(relid, PgBuiltInOids::INT8OID.oid().value()) }],
        )
        .unwrap_or_report();
    for row in rs {
        if let Ok(Some(n)) = row.get_by_name::<&str, _>("n") {
            if !exclude.contains(&n) {
                names.push(n.to_string());
            }
        }
    }
    names
}

pub(super) fn relation_exists(client: &SpiClient<'_>, qualified: &str) -> bool {
    let oid: Option<i64> = client
        .select(
            "SELECT to_regclass($1)::oid::bigint AS oid",
            None,
            &[unsafe {
                DatumWithOid::new(qualified.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .unwrap_or_report()
        .first()
        .get_by_name::<i64, _>("oid")
        .unwrap_or(None);
    !matches!(oid, None | Some(0))
}

pub(super) fn probe_query_columns(query: &str) -> Result<Vec<String>, String> {
    // Try to execute the query and extract column names from the result set
    let limited_query = format!("SELECT * FROM ({}) AS subq LIMIT 0", query);

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Spi::connect(|client| {
            match client.select(&limited_query, None, &[]) {
                Ok(rs) => {
                    let mut cols: Vec<String> = Vec::new();
                    // Iterate through ordinals to get column names
                    for ordinal in 1..=128 {
                        // Reasonable upper limit for column count
                        match rs.column_name(ordinal) {
                            Ok(name) => cols.push(name),
                            Err(_) => break, // No more columns
                        }
                    }

                    if cols.is_empty() {
                        Err("no columns found in query".to_string())
                    } else {
                        Ok(cols)
                    }
                }
                Err(_) => Err("query execution failed".to_string()),
            }
        })
    })) {
        Ok(result) => result,
        Err(_) => Err("query crashed during execution".to_string()),
    }
}

pub(super) fn relation_attname_set_quoted(
    client: &SpiClient<'_>,
    qualified: &str,
) -> Option<Vec<String>> {
    let oid: Option<i64> = client
        .select(
            "SELECT to_regclass($1)::oid::bigint AS oid",
            None,
            &[unsafe {
                DatumWithOid::new(qualified.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .unwrap_or_report()
        .first()
        .get_by_name::<i64, _>("oid")
        .unwrap_or(None);
    match oid {
        None | Some(0) => None,
        Some(o) => Some(read_attname_set(client, o, &[])),
    }
}

pub(super) fn quote_qualified_for_regclass(name: &str) -> String {
    match name.split_once('.') {
        Some((schema, local)) => format!("\"{}\".\"{}\"", schema, local),
        None => format!("\"{}\"", name),
    }
}

pub(super) fn shape_matches(actual: &[String], expected: &[String]) -> bool {
    let mut a: Vec<&String> = actual.iter().collect();
    a.sort();
    let mut e: Vec<&String> = expected.iter().collect();
    e.sort();
    a == e
}

fn load_imv_rows(client: &SpiClient<'_>, scope: &AuditScope) -> Vec<ImvRow> {
    let mut out = Vec::new();
    match scope {
        AuditScope::All => {
            let rs = client
                .select(
                    "SELECT name, depends_on, COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, \
                            base_query, end_query, aggregations::text AS aggregations_json, \
                            partition_columns, COALESCE(enabled, TRUE) AS enabled \
                     FROM public.__reflex_ivm_reference \
                     WHERE COALESCE(enabled, TRUE) = TRUE \
                     ORDER BY graph_depth, name",
                    None,
                    &[],
                )
                .unwrap_or_report();
            for row in rs {
                out.push(ImvRow {
                    name: row
                        .get_by_name::<&str, _>("name")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    depends_on: row
                        .get_by_name::<Vec<String>, _>("depends_on")
                        .unwrap_or(None)
                        .unwrap_or_default(),
                    refresh_mode: row
                        .get_by_name::<&str, _>("refresh_mode")
                        .unwrap_or(None)
                        .unwrap_or("IMMEDIATE")
                        .to_string(),
                    base_query: row
                        .get_by_name::<&str, _>("base_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    end_query: row
                        .get_by_name::<&str, _>("end_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    aggregations_json: row
                        .get_by_name::<&str, _>("aggregations_json")
                        .unwrap_or(None)
                        .map(|s| s.to_string()),
                    partition_columns: row
                        .get_by_name::<Vec<String>, _>("partition_columns")
                        .unwrap_or(None),
                    enabled: row
                        .get_by_name::<bool, _>("enabled")
                        .unwrap_or(None)
                        .unwrap_or(true),
                });
            }
        }
        AuditScope::One(name) => {
            let args = [unsafe {
                DatumWithOid::new(name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }];
            let rs = client
                .select(
                    "SELECT name, depends_on, COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, \
                            base_query, end_query, aggregations::text AS aggregations_json, \
                            partition_columns, COALESCE(enabled, TRUE) AS enabled \
                     FROM public.__reflex_ivm_reference \
                     WHERE name = $1",
                    None,
                    &args,
                )
                .unwrap_or_report();
            for row in rs {
                out.push(ImvRow {
                    name: row
                        .get_by_name::<&str, _>("name")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    depends_on: row
                        .get_by_name::<Vec<String>, _>("depends_on")
                        .unwrap_or(None)
                        .unwrap_or_default(),
                    refresh_mode: row
                        .get_by_name::<&str, _>("refresh_mode")
                        .unwrap_or(None)
                        .unwrap_or("IMMEDIATE")
                        .to_string(),
                    base_query: row
                        .get_by_name::<&str, _>("base_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    end_query: row
                        .get_by_name::<&str, _>("end_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    aggregations_json: row
                        .get_by_name::<&str, _>("aggregations_json")
                        .unwrap_or(None)
                        .map(|s| s.to_string()),
                    partition_columns: row
                        .get_by_name::<Vec<String>, _>("partition_columns")
                        .unwrap_or(None),
                    enabled: row
                        .get_by_name::<bool, _>("enabled")
                        .unwrap_or(None)
                        .unwrap_or(true),
                });
            }
        }
    }
    out
}

fn count_real_sources(imvs: &[ImvRow]) -> usize {
    use std::collections::HashSet;
    let mut set: HashSet<String> = HashSet::new();
    for imv in imvs {
        for src in imv.real_sources() {
            set.insert(src.to_string());
        }
    }
    set.len()
}

fn format_report(scope: &AuditScope, imvs: &[ImvRow], mut findings: Vec<Finding>) -> String {
    findings.sort_by(|a, b| {
        a.severity
            .rank()
            .cmp(&b.severity.rank())
            .then_with(|| match (&a.imv, &b.imv) {
                (Some(x), Some(y)) => x.cmp(y),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
            .then_with(|| a.category.cmp(b.category))
    });

    if findings.is_empty() {
        return format!(
            "pg_reflex audit: OK ({} IMV(s), {} source(s) checked, no findings)",
            imvs.len(),
            count_real_sources(imvs)
        );
    }

    let mut out = String::new();
    let header = match scope {
        AuditScope::All => format!(
            "pg_reflex audit  ({} IMV(s), {} source(s))",
            imvs.len(),
            count_real_sources(imvs)
        ),
        AuditScope::One(n) => format!("pg_reflex audit ({})", n),
    };
    out.push_str(&header);
    out.push('\n');
    out.push_str(&"=".repeat(header.len()));
    out.push_str("\n\n");

    let (mut e, mut w, mut i) = (0u32, 0u32, 0u32);
    for f in &findings {
        match f.severity {
            Severity::Error => e += 1,
            Severity::Warning => w += 1,
            Severity::Info => i += 1,
        }
        let imv_part = f.imv.as_deref().unwrap_or("(orphan)");
        out.push_str(&format!(
            "[{}] {}  {}\n  {}\n  Suggested fix:\n",
            f.severity.label(),
            imv_part,
            f.category,
            f.finding.replace('\n', "\n  ")
        ));
        for line in f.suggested_fix.lines() {
            out.push_str("    ");
            out.push_str(line);
            out.push('\n');
        }
        out.push('\n');
    }
    out.push_str(&format!(
        "{} finding(s):  {} ERROR, {} WARNING, {} INFO\n",
        findings.len(),
        e,
        w,
        i
    ));
    out
}

pub fn reflex_audit_impl(scope: AuditScope) -> String {
    Spi::connect(|client| {
        let imvs = load_imv_rows(client, &scope);

        if let AuditScope::One(ref n) = scope {
            if imvs.is_empty() {
                pgrx::error!("reflex_audit: IMV '{}' not registered or not enabled", n);
            }
        }

        let mut findings: Vec<Finding> = Vec::new();
        let checks = registry();

        for chk in &checks {
            if chk.is_per_imv() {
                for imv in &imvs {
                    findings.extend(chk.run(client, Some(imv)));
                }
            } else if matches!(scope, AuditScope::All) {
                findings.extend(chk.run_global(client, &imvs));
            }
        }

        format_report(&scope, &imvs, findings)
    })
}
