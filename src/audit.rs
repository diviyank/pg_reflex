use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use pgrx::PgBuiltInOids;

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
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding>;
    fn is_per_imv(&self) -> bool {
        true
    }
}

struct StagingShape;

impl Check for StagingShape {
    fn id(&self) -> &'static str {
        "staging-shape"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if imv.refresh_mode != "DEFERRED" || !imv.enabled {
            return vec![];
        }
        let mut out = Vec::new();
        for src in imv.real_sources() {
            let staging = crate::query_decomposer::staging_delta_table_name(src);

            // Resolve schema + local for both sides via to_regclass.
            let staging_oid = match client
                .select(
                    "SELECT to_regclass($1)::oid::bigint AS oid",
                    None,
                    &[unsafe {
                        DatumWithOid::new(staging.clone(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<i64, _>("oid")
                .unwrap_or(None)
            {
                Some(0) | None => continue,
                Some(o) => o,
            };
            let source_oid = match client
                .select(
                    "SELECT to_regclass($1)::oid::bigint AS oid",
                    None,
                    &[unsafe {
                        DatumWithOid::new(src.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<i64, _>("oid")
                .unwrap_or(None)
            {
                Some(0) | None => continue,
                Some(o) => o,
            };

            let src_cols = read_attname_set(client, source_oid, &[]);
            let stg_cols = read_attname_set(client, staging_oid, &["__reflex_op"]);

            if src_cols != stg_cols {
                out.push(Finding {
                    imv: Some(imv.name.clone()),
                    severity: Severity::Error,
                    category: "staging-shape",
                    finding: format!(
                        "{} has columns\n  {{{}}}\nbut source {} has\n  {{{}}}\n\
                         Trigger INSERTs would mismatch on differing column set.",
                        staging,
                        stg_cols.join(", "),
                        src,
                        src_cols.join(", "),
                    ),
                    suggested_fix: format!(
                        "SELECT count(*) FROM {staging};\n\
                         -- if 0:\n\
                         DROP TABLE {staging} CASCADE;\n\
                         SELECT reflex_rebuild_triggers('{src}');\n\
                         -- if >0:\n\
                         SELECT reflex_flush_deferred('{src}');\n\
                         -- then re-run the above DROP + rebuild."
                    ),
                });
            }
        }
        out
    }
}

struct TriggerAttached;

impl Check for TriggerAttached {
    fn id(&self) -> &'static str {
        "trigger-attached"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        let mut out = Vec::new();
        for src in imv.real_sources() {
            let suffix = crate::query_decomposer::sanitized_source_suffix(src);
            let expected = [
                format!("__reflex_trigger_ins_on_{}", suffix),
                format!("__reflex_trigger_del_on_{}", suffix),
                format!("__reflex_trigger_upd_on_{}", suffix),
                format!("__reflex_trigger_trunc_on_{}", suffix),
            ];
            // Resolve source oid; skip if missing (source-exists check covers it).
            let source_oid = match client
                .select(
                    "SELECT to_regclass($1)::oid::bigint AS oid",
                    None,
                    &[unsafe {
                        DatumWithOid::new(src.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    }],
                )
                .unwrap_or_report()
                .first()
                .get_by_name::<i64, _>("oid")
                .unwrap_or(None)
            {
                Some(0) | None => continue,
                Some(o) => o,
            };
            let mut present: std::collections::HashSet<String> = std::collections::HashSet::new();
            let rs = client
                .select(
                    "SELECT tgname::text AS n FROM pg_trigger \
                     WHERE tgrelid = $1::oid AND NOT tgisinternal",
                    None,
                    &[unsafe {
                        DatumWithOid::new(source_oid, PgBuiltInOids::INT8OID.oid().value())
                    }],
                )
                .unwrap_or_report();
            for row in rs {
                if let Ok(Some(n)) = row.get_by_name::<&str, _>("n") {
                    present.insert(n.to_string());
                }
            }
            let missing: Vec<&String> = expected.iter().filter(|e| !present.contains(*e)).collect();
            if !missing.is_empty() {
                let names: Vec<String> = missing.iter().map(|s| (*s).clone()).collect();
                out.push(Finding {
                    imv: Some(imv.name.clone()),
                    severity: Severity::Error,
                    category: "trigger-attached",
                    finding: format!("Source {} is missing trigger(s): {}", src, names.join(", ")),
                    suggested_fix: format!("SELECT reflex_rebuild_triggers('{}');", src),
                });
            }
        }
        out
    }
}

struct TriggerModeMatches;

impl Check for TriggerModeMatches {
    fn id(&self) -> &'static str {
        "trigger-mode-matches"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        let mut out = Vec::new();
        for src in imv.real_sources() {
            let suffix = crate::query_decomposer::sanitized_source_suffix(src);
            let fn_names = [
                format!("__reflex_ins_trigger_on_{}", suffix),
                format!("__reflex_del_trigger_on_{}", suffix),
                format!("__reflex_upd_trigger_on_{}", suffix),
                format!("__reflex_trunc_trigger_on_{}", suffix),
            ];
            // Check each trigger function for consistency with refresh_mode.
            for fn_name in &fn_names {
                let body: Option<String> = client
                    .select(
                        "SELECT prosrc::text AS body FROM pg_proc p \
                         JOIN pg_namespace n ON n.oid = p.pronamespace \
                         WHERE p.proname = $1 AND n.nspname = 'public' LIMIT 1",
                        None,
                        &[unsafe {
                            DatumWithOid::new(fn_name.clone(), PgBuiltInOids::TEXTOID.oid().value())
                        }],
                    )
                    .unwrap_or_report()
                    .first()
                    .get_by_name::<&str, _>("body")
                    .unwrap_or(None)
                    .map(|s| s.to_string());
                let body = match body {
                    Some(b) => b,
                    None => continue, // trigger-attached check covers absence
                };
                let body_is_deferred = body.contains("__reflex_delta_");
                let expected_deferred = imv.refresh_mode == "DEFERRED";
                if body_is_deferred != expected_deferred {
                    out.push(Finding {
                        imv: Some(imv.name.clone()),
                        severity: Severity::Error,
                        category: "trigger-mode-matches",
                        finding: format!(
                            "Trigger function {} is in {} mode but IMV {} refresh_mode is {}.",
                            fn_name,
                            if body_is_deferred {
                                "DEFERRED"
                            } else {
                                "IMMEDIATE"
                            },
                            imv.name,
                            imv.refresh_mode,
                        ),
                        suggested_fix: format!("SELECT reflex_rebuild_triggers('{}');", src),
                    });
                }
            }
        }
        out
    }
}

struct InternalTablesExist;

impl Check for InternalTablesExist {
    fn id(&self) -> &'static str {
        "internal-tables-exist"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }

        let mut required: Vec<String> = Vec::new();

        if imv.is_passthrough() {
            // Passthrough IMVs use per-source scratch tables instead of an intermediate
            for src in imv.real_sources() {
                required.push(crate::query_decomposer::passthrough_scratch_new_table_name(
                    &imv.name, src,
                ));
                required.push(crate::query_decomposer::passthrough_scratch_old_table_name(
                    &imv.name, src,
                ));
            }
        } else {
            // Aggregate IMVs use an intermediate table and affected groups table
            required.push(crate::query_decomposer::intermediate_table_name(&imv.name));
            required.push(crate::query_decomposer::affected_groups_table_name(
                &imv.name,
            ));
        }

        let mut missing: Vec<String> = Vec::new();
        for name in &required {
            if !relation_exists(client, name) {
                missing.push(name.clone());
            }
        }

        if missing.is_empty() {
            return vec![];
        }
        vec![Finding {
            imv: Some(imv.name.clone()),
            severity: Severity::Error,
            category: "internal-tables-exist",
            finding: format!(
                "Missing internal table(s) for IMV {}:\n  {}",
                imv.name,
                missing.join("\n  ")
            ),
            suggested_fix: format!("SELECT reflex_rebuild_imv('{}');", imv.name),
        }]
    }
}

fn relation_exists(client: &SpiClient<'_>, qualified: &str) -> bool {
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

fn registry() -> Vec<Box<dyn Check>> {
    vec![
        Box::new(StagingShape),
        Box::new(TriggerAttached),
        Box::new(TriggerModeMatches),
        Box::new(InternalTablesExist),
    ]
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

fn read_attname_set(client: &SpiClient<'_>, relid: i64, exclude: &[&str]) -> Vec<String> {
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
                findings.extend(chk.run(client, None));
            }
        }

        format_report(&scope, &imvs, findings)
    })
}
