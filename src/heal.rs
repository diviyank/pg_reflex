//! Healing IMVs after a change to one of their `ignore_sources`.
//!
//! An ignored source gets no maintenance trigger, so when the IMV's query filters
//! on it (`demand_planning.status` leaving and re-entering the included set) the
//! IMV stays wrong for every slice the change affected. When the ignored source
//! joins by equality onto the expression the IMV's first partition column
//! projects, the affected slices are known from the changed rows alone:
//! statement-level triggers queue those partition keys into
//! `__reflex_heal_pending`, and a sweep rebuilds only those partitions. The write
//! itself pays for a queue insert, never for a rebuild.
//!
//! An ignored source that cannot be mapped that way keeps the pre-1.11.4
//! contract (no heal): its create was already refused or acknowledged by the
//! ignore-soundness check.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::PgBuiltInOids;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// How a change to one ignored source maps to IMV partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IgnoreHealKey {
    /// The source column equal to the IMV's first partition column.
    pub source_column: String,
    /// The source relation, resolved and quoted at create time.
    pub relation: String,
    /// Every source column the query can read; empty means all of them.
    pub watched_columns: Vec<String>,
}

/// An ignored source the heal can map, before catalog resolution.
struct HealCandidate {
    source_column: String,
    watched_candidates: Option<Vec<String>>,
}

/// Columns of the source that `column_refs` can read, named by any of `names`
/// (the source's aliases and table name). Bare references are kept too: in a
/// multi-source query they may resolve to this source, and the catalog check
/// discards those that don't exist on it. `None` means every column: the query
/// reads implicit columns, or references the source's whole row.
fn watched_column_candidates(
    column_refs: Option<&[Vec<String>]>,
    names: &[String],
) -> Option<Vec<String>> {
    let mut candidates = Vec::new();
    for parts in column_refs? {
        match parts.as_slice() {
            [only] if names.contains(only) => return None,
            [only] => candidates.push(only.clone()),
            [qualifier, column, ..] if names.contains(qualifier) => candidates.push(column.clone()),
            [_, qualifier, column, ..] if names.contains(qualifier) => {
                candidates.push(column.clone())
            }
            _ => {}
        }
    }
    Some(candidates)
}

fn bare_name(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim()
        .trim_matches('"')
        .to_lowercase()
}

/// `{ignored source as written in the query: its column equal to the IMV's first
/// partition column}`, for every ignored source joined by a top-level equality
/// onto the exact expression that partition column projects.
///
/// Refuses (omits the source) whenever a changed source row could affect a
/// partition other than its own key: an OR in the join condition, a join type
/// that preserves the ignored side's unmatched rows, or the source read more than
/// once anywhere in the statement (`relation_names` has one entry per read).
fn heal_candidates(
    analysis: &crate::sql_analyzer::SqlAnalysis,
    column_refs: Option<&[Vec<String>]>,
    relation_names: &[String],
    ignored_clean: &[String],
    partition_columns: &[String],
) -> HashMap<String, HealCandidate> {
    let mut mapped = HashMap::new();
    let Some(partition_col) = partition_columns.first().map(|c| c.to_lowercase()) else {
        return mapped;
    };
    let Some(partition_expr) = analysis.select_columns.iter().find_map(|c| {
        let output = bare_name(c.alias.as_deref().unwrap_or(&c.expr_sql));
        (output == partition_col).then(|| c.expr_sql.trim().to_lowercase())
    }) else {
        return mapped;
    };

    for source in &analysis.sources {
        if source.starts_with('<') {
            continue;
        }
        let source_bare = bare_name(source);
        if !ignored_clean
            .iter()
            .any(|ig| ig.eq_ignore_ascii_case(source) || bare_name(ig) == source_bare)
        {
            continue;
        }
        let aliases: Vec<String> = analysis
            .table_aliases
            .iter()
            .filter(|(alias, table)| {
                bare_name(table) == source_bare && !alias.eq_ignore_ascii_case(table)
            })
            .map(|(alias, _)| alias.to_lowercase())
            .collect();
        let reads = relation_names
            .iter()
            .filter(|name| bare_name(name) == source_bare)
            .count();
        if aliases.len() > 1 || reads != 1 {
            continue;
        }

        let source_col = analysis.joins.iter().find_map(|join| {
            let joins_source = bare_name(&join.target_table) == source_bare;
            let preserves_matches = match join.join_type.as_str() {
                "INNER" => true,
                "LEFT" => joins_source,
                _ => false,
            };
            let condition = join.condition_sql.as_deref()?.to_lowercase();
            if !preserves_matches || condition.split_whitespace().any(|w| w == "or") {
                return None;
            }
            condition.split(" and ").find_map(|equality| {
                let (left, right) = equality.split_once('=')?;
                if left.ends_with(['<', '>', '!']) || right.starts_with(['<', '>', '=']) {
                    return None;
                }
                let (left, right) = (left.trim(), right.trim());
                let from_source =
                    |side: &str| crate::create_ivm::is_from_table(side, &source_bare, &aliases);
                match (from_source(left), from_source(right)) {
                    (true, false) if right == partition_expr => Some(bare_name(left)),
                    (false, true) if left == partition_expr => Some(bare_name(right)),
                    _ => None,
                }
            })
        });
        if let Some(source_column) = source_col {
            let mut names = aliases;
            names.push(source_bare);
            names.push(source.to_lowercase());
            mapped.insert(
                source.clone(),
                HealCandidate {
                    source_column,
                    watched_candidates: watched_column_candidates(column_refs, &names),
                },
            );
        }
    }
    mapped
}

/// Compute the heal mapping for an IMV and install the three statement-level
/// heal triggers on each mapped ignored source. Returns what the plan persists
/// as `ignore_heal_keys`.
///
/// The relation is resolved here, under the creator's search_path, so the
/// trigger matches it by OID whatever search_path the writer runs under. A
/// source the caller may not put a trigger on gets no heal, with a WARNING,
/// rather than failing the create.
///
/// The source column's type must render keys the partition column parses back
/// to the same value: the same type, two integer types, or two string types.
/// Otherwise (e.g. `numeric` 5.0 against a `bigint` partition) no heal.
pub(crate) fn install_heal_triggers(
    client: &mut pgrx::spi::SpiClient<'_>,
    view_name: &str,
    analysis: &crate::sql_analyzer::SqlAnalysis,
    stmts: &[sqlparser::ast::Statement],
    ignored_clean: &[String],
    partition_columns: &[String],
) -> HashMap<String, IgnoreHealKey> {
    let mut installed = HashMap::new();
    let Some(partition_column) = partition_columns.first() else {
        return installed;
    };
    let column_refs = crate::sql_analyzer::statement_column_refs(stmts);
    let relation_names = crate::sql_analyzer::statement_relation_names(stmts);
    for (source, candidate) in heal_candidates(
        analysis,
        column_refs.as_deref(),
        &relation_names,
        ignored_clean,
        partition_columns,
    ) {
        let resolved = client
            .select(
                "SELECT format('%I.%I', n.nspname, c.relname) AS rel, \
                        has_table_privilege(c.oid, 'TRIGGER') AS can_trigger, \
                        ARRAY(SELECT a.attname::text FROM pg_attribute a \
                               WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped \
                                 AND (a.attname = ANY($3::text[]) OR a.attname = $2) \
                               ORDER BY a.attnum) AS watched \
                   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                  WHERE c.oid = to_regclass($1) AND c.relkind IN ('r', 'p') \
                    AND EXISTS ( \
                        SELECT 1 FROM pg_attribute s \
                          JOIN pg_type st ON st.oid = s.atttypid \
                          JOIN pg_attribute p ON p.attrelid = to_regclass($4) AND p.attname = $5 \
                                             AND p.attnum > 0 AND NOT p.attisdropped \
                          JOIN pg_type pt ON pt.oid = p.atttypid \
                         WHERE s.attrelid = c.oid AND s.attname = $2 \
                           AND s.attnum > 0 AND NOT s.attisdropped \
                           AND (s.atttypid = p.atttypid \
                                OR (s.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype) \
                                    AND p.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype)) \
                                OR (st.typcategory = 'S' AND pt.typcategory = 'S')))",
                Some(1),
                &[
                    unsafe {
                        DatumWithOid::new(source.clone(), PgBuiltInOids::TEXTOID.oid().value())
                    },
                    unsafe {
                        DatumWithOid::new(
                            candidate.source_column.clone(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe {
                        DatumWithOid::new(
                            candidate.watched_candidates.clone().unwrap_or_default(),
                            PgBuiltInOids::TEXTARRAYOID.oid().value(),
                        )
                    },
                    unsafe {
                        DatumWithOid::new(view_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    },
                    unsafe {
                        DatumWithOid::new(
                            partition_column.to_lowercase(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                ],
            )
            .ok()
            .and_then(|mut rows| rows.next())
            .and_then(|row| {
                let rel = row.get_by_name::<String, _>("rel").ok().flatten()?;
                let can_trigger = row.get_by_name::<bool, _>("can_trigger").ok().flatten()?;
                let watched = row
                    .get_by_name::<Vec<String>, _>("watched")
                    .ok()
                    .flatten()?;
                Some((rel, can_trigger, watched))
            });
        let Some((relation, can_trigger, watched)) = resolved else {
            continue;
        };
        let watched_columns = match candidate.watched_candidates {
            Some(_) => watched,
            None => Vec::new(),
        };
        if !can_trigger {
            pgrx::warning!(
                "pg_reflex: no TRIGGER privilege on ignored source '{}': changes to it will not \
                 heal IMV '{}'",
                source,
                view_name
            );
            continue;
        }
        for (name, event, transition) in [
            (
                "__reflex_heal_ins",
                "INSERT",
                "REFERENCING NEW TABLE AS __reflex_heal_new",
            ),
            (
                "__reflex_heal_upd",
                "UPDATE",
                "REFERENCING OLD TABLE AS __reflex_heal_old NEW TABLE AS __reflex_heal_new",
            ),
            (
                "__reflex_heal_del",
                "DELETE",
                "REFERENCING OLD TABLE AS __reflex_heal_old",
            ),
            ("__reflex_heal_trunc", "TRUNCATE", ""),
        ] {
            client
                .update(
                    &format!(
                        "CREATE OR REPLACE TRIGGER {name} AFTER {event} ON {relation} \
                         {transition} FOR EACH STATEMENT \
                         EXECUTE FUNCTION public.__reflex_heal_on_ignored_change()"
                    ),
                    None,
                    &[],
                )
                .unwrap_or_else(|e| pgrx::error!("pg_reflex: heal trigger on {relation}: {e}"));
        }
        installed.insert(
            source,
            IgnoreHealKey {
                source_column: candidate.source_column,
                relation,
                watched_columns,
            },
        );
    }
    installed
}

/// One heal attempt: the IMV, what the partition reconcile returned, and how long
/// it took.
pub(crate) struct HealResult {
    pub imv: String,
    pub result: String,
    pub ms: i64,
}

impl HealResult {
    pub(crate) fn failed(&self) -> bool {
        self.result.starts_with("ERROR")
    }
}

/// Rebuild every queued partition, per IMV, shallowest first. `imv` narrows to
/// one IMV; a non-empty `target_schema` to one tenant.
///
/// A queue row is removed only if it is exactly the row read before the rebuild
/// (same key, same `enqueued_at`). A change committed after that read either adds
/// a row or re-stamps one, so it survives for the next sweep even if the rebuild's
/// snapshot missed it.
pub(crate) fn heal_ignored_sources_impl(imv: Option<&str>, target_schema: &str) -> Vec<HealResult> {
    let heal_table_exists =
        Spi::get_one::<bool>("SELECT to_regclass('public.__reflex_heal_pending') IS NOT NULL")
            .unwrap_or(Some(false))
            .unwrap_or(false);
    if !heal_table_exists {
        return Vec::new();
    }
    let batches: Vec<(String, Vec<String>, Vec<String>)> = Spi::connect(|client| {
        client
            .select(
                "SELECT p.imv_name, \
                        array_agg(p.partition_key ORDER BY p.partition_key) AS keys, \
                        array_agg(p.enqueued_at::text ORDER BY p.partition_key) AS stamps \
                   FROM public.__reflex_heal_pending p \
                   JOIN public.__reflex_ivm_reference r \
                     ON r.name = p.imv_name AND COALESCE(r.enabled, TRUE) \
                  WHERE ($1::text IS NULL OR p.imv_name = $1) \
                    AND ($2 = '' OR COALESCE(r.target_schema, 'public') = $2) \
                  GROUP BY p.imv_name, r.graph_depth \
                  ORDER BY r.graph_depth NULLS FIRST, p.imv_name",
                None,
                &[
                    unsafe {
                        DatumWithOid::new(
                            imv.map(str::to_string),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                    unsafe {
                        DatumWithOid::new(
                            target_schema.to_string(),
                            PgBuiltInOids::TEXTOID.oid().value(),
                        )
                    },
                ],
            )
            .map(|rows| {
                rows.filter_map(|row| {
                    Some((
                        row.get_by_name::<String, _>("imv_name").ok().flatten()?,
                        row.get_by_name::<Vec<String>, _>("keys").ok().flatten()?,
                        row.get_by_name::<Vec<String>, _>("stamps").ok().flatten()?,
                    ))
                })
                .collect()
            })
            .unwrap_or_default()
    });

    batches
        .into_iter()
        .map(|(name, keys, stamps)| {
            let started = std::time::Instant::now();
            let result = reconcile_keys_isolated(&name, &keys);
            let heal = HealResult {
                imv: name,
                result,
                ms: started.elapsed().as_millis() as i64,
            };
            record_heal_outcome(&heal, keys, stamps);
            heal
        })
        .collect()
}

/// Rebuild `keys` of `imv` in a subtransaction. A PostgreSQL error raised inside
/// (a key the partition column cannot parse, say) rolls back only this heal and
/// comes back as an `ERROR:` string, so one bad IMV cannot abort the sweep and its
/// failure is recorded like any other.
fn reconcile_keys_isolated(imv: &str, keys: &[String]) -> String {
    pgrx::PgTryBuilder::new(|| {
        let subxact = crate::partition::SubTransaction::begin();
        let result = crate::partition::reflex_reconcile_partition_impl(imv, keys, "", false);
        subxact.release();
        result
    })
    .catch_others(|error| {
        use pgrx::pg_sys::panic::CaughtError;
        let message = match &error {
            CaughtError::PostgresError(report)
            | CaughtError::ErrorReport(report)
            | CaughtError::RustPanic {
                ereport: report, ..
            } => report.message().to_string(),
        };
        format!("ERROR: {message}")
    })
    .execute()
}

fn record_heal_outcome(heal: &HealResult, keys: Vec<String>, stamps: Vec<String>) {
    let (sql, detail) = if heal.failed() {
        pgrx::warning!("pg_reflex: heal of '{}' failed: {}", heal.imv, heal.result);
        (
            "UPDATE public.__reflex_heal_pending p SET last_error = left($4, 2000) \
               FROM unnest($2::text[], $3::text[]) AS q(k, at) \
              WHERE p.imv_name = $1 AND p.partition_key = q.k AND p.enqueued_at = q.at::timestamptz",
            heal.result.clone(),
        )
    } else {
        (
            "DELETE FROM public.__reflex_heal_pending p \
               USING unnest($2::text[], $3::text[]) AS q(k, at) \
              WHERE p.imv_name = $1 AND p.partition_key = q.k AND p.enqueued_at = q.at::timestamptz \
                AND $4 IS NOT NULL",
            String::new(),
        )
    };
    Spi::connect_mut(|client| {
        client
            .update(
                sql,
                None,
                &[
                    unsafe {
                        DatumWithOid::new(heal.imv.clone(), PgBuiltInOids::TEXTOID.oid().value())
                    },
                    unsafe { DatumWithOid::new(keys, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe { DatumWithOid::new(stamps, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe { DatumWithOid::new(detail, PgBuiltInOids::TEXTOID.oid().value()) },
                ],
            )
            .unwrap_or_else(|e| pgrx::error!("pg_reflex: heal queue bookkeeping: {e}"));
    });
}

/// Heal every IMV (or just `imv`) whose ignored source changed: rebuild exactly
/// the queued partitions.
#[pg_extern]
fn reflex_heal_ignored_sources(imv: default!(Option<&str>, "NULL")) -> String {
    let heals = heal_ignored_sources_impl(imv, "");
    let failures: Vec<String> = heals
        .iter()
        .filter(|h| h.failed())
        .map(|h| format!("{}: {}", h.imv, h.result))
        .collect();
    if failures.is_empty() {
        format!("HEALED {} IMVs", heals.len())
    } else {
        format!(
            "ERROR: {} of {} heals failed: {}",
            failures.len(),
            heals.len(),
            failures.join("; ")
        )
    }
}

/// The enabled IMVs with queued heals, by name, with the ignored sources that
/// queued them, the queued keys and the oldest enqueue time. A disabled IMV's
/// keys wait for it to be enabled; nothing heals or reports them meanwhile.
pub(crate) struct QueuedHeal {
    pub sources: String,
    pub keys: String,
    pub since: String,
}

pub(crate) fn queued_heals_by_imv() -> BTreeMap<String, QueuedHeal> {
    let heal_table_exists =
        Spi::get_one::<bool>("SELECT to_regclass('public.__reflex_heal_pending') IS NOT NULL")
            .unwrap_or(Some(false))
            .unwrap_or(false);
    if !heal_table_exists {
        return BTreeMap::new();
    }
    Spi::connect(|client| {
        let Ok(rows) = client.select(
            "SELECT p.imv_name, string_agg(DISTINCT p.source, ', ') AS sources, \
                    string_agg(p.partition_key, ', ' ORDER BY p.partition_key) AS keys, \
                    min(p.enqueued_at)::text AS since \
               FROM public.__reflex_heal_pending p \
               JOIN public.__reflex_ivm_reference r \
                 ON r.name = p.imv_name AND COALESCE(r.enabled, TRUE) \
              GROUP BY p.imv_name",
            None,
            &[],
        ) else {
            return BTreeMap::new();
        };
        rows.filter_map(|row| {
            let text = |col: &str| {
                row.get_by_name::<String, _>(col)
                    .ok()
                    .flatten()
                    .unwrap_or_default()
            };
            Some((
                row.get_by_name::<String, _>("imv_name").ok().flatten()?,
                QueuedHeal {
                    sources: text("sources"),
                    keys: text("keys"),
                    since: text("since"),
                },
            ))
        })
        .collect()
    })
}

impl QueuedHeal {
    pub(crate) fn stale_reason(&self, imv: &str) -> String {
        format!(
            "ignored source(s) {} changed for partition key(s) {} since {}; those partitions no \
             longer match the query. Run SELECT reflex_heal_ignored_sources('{}');",
            self.sources,
            self.keys,
            self.since,
            imv.replace('\'', "''"),
        )
    }
}

/// Stale reasons for every IMV with queued heals and every IMV reading one,
/// transitively: a downstream IMV is exactly as wrong as the slice it reads.
pub(crate) fn heal_stale_reasons_by_imv() -> HashMap<String, String> {
    let queued = queued_heals_by_imv();
    if queued.is_empty() {
        return HashMap::new();
    }
    let mut reasons: HashMap<String, String> = queued
        .iter()
        .map(|(imv, heal)| (imv.clone(), heal.stale_reason(imv)))
        .collect();
    let origins: Vec<String> = queued.into_keys().collect();
    Spi::connect(|client| {
        let Ok(rows) = client.select(
            "WITH RECURSIVE reach(name, origin, depth) AS ( \
                 SELECT r.name, o.origin, 1 \
                   FROM unnest($1::text[]) AS o(origin) \
                   JOIN public.__reflex_ivm_reference r \
                     ON r.depends_on && ARRAY[o.origin, split_part(o.origin, '.', 2), 'public.' || o.origin] \
                 UNION \
                 SELECT r.name, x.origin, x.depth + 1 \
                   FROM reach x \
                   JOIN public.__reflex_ivm_reference r \
                     ON r.depends_on && ARRAY[x.name, split_part(x.name, '.', 2), 'public.' || x.name] \
                  WHERE x.depth < 32 \
             ) \
             SELECT DISTINCT ON (name) name, origin FROM reach ORDER BY name, depth, origin",
            None,
            &[unsafe { DatumWithOid::new(origins, PgBuiltInOids::TEXTARRAYOID.oid().value()) }],
        ) else {
            return;
        };
        for row in rows {
            let name = row.get_by_name::<String, _>("name").ok().flatten();
            let origin = row.get_by_name::<String, _>("origin").ok().flatten();
            if let (Some(name), Some(origin)) = (name, origin) {
                reasons.entry(name).or_insert_with(|| {
                    format!(
                        "reads IMV '{origin}', whose partitions are queued for heal after an ignored \
                         source changed. Run SELECT reflex_heal_ignored_sources('{}');",
                        origin.replace('\'', "''")
                    )
                });
            }
        }
    });
    reasons
}
