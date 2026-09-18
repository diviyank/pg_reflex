//! A2: does the registry still hold an unsound `ignore_sources` entry that was
//! never acknowledged?
//!
//! A1 (`src/create_ivm/ignore_soundness.rs`) refuses this shape at create time,
//! but only protects IMVs created from 1.11.4 onward. Everything installed
//! before then — including any IMV whose ignore predates the '!' ack — carries
//! the same silent-wipe hazard and is invisible to A1. This check is the audit
//! surface for those: it re-runs the same resolver against the registry's
//! stored `base_query` and flags anything not already acknowledged.

use super::*;

pub(super) struct IgnoreSoundness;

impl Check for IgnoreSoundness {
    fn id(&self) -> &'static str {
        "ignore-soundness"
    }
    fn run(&self, client: &SpiClient<'_>, imv: Option<&ImvRow>) -> Vec<Finding> {
        let imv = match imv {
            Some(i) => i,
            None => return vec![],
        };
        if !imv.enabled {
            return vec![];
        }
        let ignored = imv.ignored_sources.clone().unwrap_or_default();
        if ignored.is_empty() {
            return vec![];
        }
        let acked = read_ignore_ack(client, &imv.name);
        crate::create_ivm::ignore_soundness::unsound_ignored_sources(&imv.base_query, &ignored)
            .into_iter()
            .filter(|(src, _)| !acked.contains(src))
            .map(|(src, reason)| Finding {
                imv: Some(imv.name.clone()),
                severity: Severity::Warning,
                category: "ignore-soundness",
                finding: format!(
                    "Ignoring source '{src}' is unsound: {reason}. Changes to '{src}' \
                     will not refresh this IMV, so it can silently diverge."
                ),
                suggested_fix: format!(
                    "-- Accept the risk (records the ack in both the registry and create_args):\n\
                     SELECT reflex_ack_ignore_source('{}', '{src}');\n\
                     -- Or remove the ignore and let the source maintain the IMV:\n\
                     --   re-create without '{src}' in ignore_sources.",
                    imv.name
                ),
            })
            .collect()
    }
}

/// The recorded acknowledgements for `name`, or an empty vec when the row is
/// absent or the column holds nothing — never an error, since a missing ack is
/// exactly the case this check exists to surface, not to fail on.
pub(super) fn read_ignore_ack(client: &SpiClient<'_>, name: &str) -> Vec<String> {
    let args =
        [unsafe { DatumWithOid::new(name.to_string(), PgBuiltInOids::TEXTOID.oid().value()) }];
    client
        .select(
            "SELECT ignore_ack FROM public.__reflex_ivm_reference WHERE name = $1",
            None,
            &args,
        )
        .ok()
        .and_then(|rs| rs.first().get_by_name::<Vec<String>, _>("ignore_ack").ok())
        .flatten()
        .unwrap_or_default()
}
