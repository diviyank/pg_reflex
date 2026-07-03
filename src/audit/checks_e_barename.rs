use super::{Check, Finding, ImvRow, Severity};
use pgrx::spi::SpiClient;

pub(super) struct BareNameAmbiguity;

impl Check for BareNameAmbiguity {
    fn id(&self) -> &'static str {
        "bare-name-ambiguity"
    }

    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };

        let mut findings = Vec::new();

        for dep in &imv.depends_on {
            // Skip subqueries and function references
            if dep.starts_with("<subquery:") || dep.starts_with("<function:") {
                continue;
            }

            // Only check bare (unqualified) names
            if dep.contains('.') {
                continue;
            }

            // Does this bare relation name exist in more than one schema?
            let schemas: Option<String> = client
                .select(
                    "SELECT string_agg(DISTINCT n.nspname, ', ') AS schemas \
                       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                      WHERE c.relname = $1 AND c.relkind IN ('r','p') \
                      GROUP BY c.relname HAVING count(DISTINCT n.nspname) > 1",
                    None,
                    &[dep.into()],
                )
                .ok()
                .and_then(|mut it| it.next())
                .and_then(|r| r.get_by_name::<String, _>("schemas").ok().flatten());

            if let Some(schemas) = schemas {
                findings.push(Finding {
                    imv: Some(imv.name.clone()),
                    severity: Severity::Error,
                    category: "bare_name_ambiguity",
                    finding: format!(
                        "depends_on entry '{}' is unqualified and exists in schemas: {}",
                        dep, schemas
                    ),
                    suggested_fix: "Register the IMV with a schema-qualified name (pass schema=…)."
                        .to_string(),
                });
            }
        }

        findings
    }
}
