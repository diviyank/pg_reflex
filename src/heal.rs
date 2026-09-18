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
/// The source column's type must be built in (the trigger renders keys with
/// `to_jsonb`, which a type owner's cast to json would otherwise hook) and render
/// keys the partition column reads back as the same value: the same type, two
/// integer types, or `text` and `varchar`. Otherwise (`numeric` 5.0 against a
/// `bigint` partition, `char(n)` padding against `text`) no heal. An integer key
/// the partition column is too narrow to hold is drained by the heal.
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
                         WHERE s.attrelid = c.oid AND s.attname = $2 \
                           AND s.attnum > 0 AND NOT s.attisdropped \
                           AND st.typnamespace = 'pg_catalog'::regnamespace \
                           AND (s.atttypid = p.atttypid \
                                OR (s.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype) \
                                    AND p.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype)) \
                                OR (s.atttypid IN ('text'::regtype, 'varchar'::regtype) \
                                    AND p.atttypid IN ('text'::regtype, 'varchar'::regtype))))",
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
    let batches: Vec<HealBatch> = Spi::connect(|client| {
        client
            .select(
                "SELECT p.imv_name, \
                        array_agg(p.partition_key ORDER BY p.partition_key) AS keys, \
                        array_agg(p.enqueued_at::text ORDER BY p.partition_key) AS stamps, \
                        (SELECT a.atttypid::regtype::text FROM pg_attribute a \
                          WHERE a.attrelid = to_regclass(p.imv_name) \
                            AND a.attname = r.partition_columns[1] \
                            AND NOT a.attisdropped) AS key_type \
                   FROM public.__reflex_heal_pending p \
                   JOIN public.__reflex_ivm_reference r \
                     ON r.name = p.imv_name AND COALESCE(r.enabled, TRUE) \
                  WHERE ($1::text IS NULL OR p.imv_name = $1) \
                    AND ($2 = '' OR COALESCE(r.target_schema, 'public') = $2) \
                  GROUP BY p.imv_name, r.graph_depth, r.partition_columns \
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
                    Some(HealBatch {
                        imv: row.get_by_name::<String, _>("imv_name").ok().flatten()?,
                        keys: row.get_by_name::<Vec<String>, _>("keys").ok().flatten()?,
                        stamps: row.get_by_name::<Vec<String>, _>("stamps").ok().flatten()?,
                        key_type: row.get_by_name::<String, _>("key_type").ok().flatten(),
                    })
                })
                .collect()
            })
            .unwrap_or_default()
    });

    batches.into_iter().map(heal_batch).collect()
}

/// One IMV's queue rows as read before its heal.
struct HealBatch {
    imv: String,
    keys: Vec<String>,
    stamps: Vec<String>,
    /// The IMV's partition column type, as `regtype` text.
    key_type: Option<String>,
}

fn heal_batch(batch: HealBatch) -> HealResult {
    let started = std::time::Instant::now();
    let (holdable, unholdable): (Vec<_>, Vec<_>) = batch
        .keys
        .into_iter()
        .zip(batch.stamps)
        .partition(|(key, _)| partition_type_can_hold(batch.key_type.as_deref(), key));
    let (unholdable_keys, unholdable_stamps): (Vec<String>, Vec<String>) =
        unholdable.into_iter().unzip();
    if !unholdable_keys.is_empty() {
        update_queue_rows(
            DRAIN_QUEUE_ROWS,
            &batch.imv,
            unholdable_keys,
            unholdable_stamps,
            "",
        );
    }
    let (keys, stamps): (Vec<String>, Vec<String>) = holdable.into_iter().unzip();
    if keys.is_empty() {
        return HealResult {
            imv: batch.imv,
            result: "HEALED: no partition can hold the queued keys".to_string(),
            ms: started.elapsed().as_millis() as i64,
        };
    }
    let result = reconcile_keys_isolated(&batch.imv, &keys);
    let heal = HealResult {
        imv: batch.imv,
        result,
        ms: started.elapsed().as_millis() as i64,
    };
    record_heal_outcome(&heal, keys, stamps);
    heal
}

/// Whether a value of the IMV's partition column type can equal `key`. Keys come
/// from a compatible source column (see `install_heal_triggers`), so only a
/// narrower integer partition column can reject one, and a key it cannot hold
/// has no partition to rebuild.
fn partition_type_can_hold(key_type: Option<&str>, key: &str) -> bool {
    let bounds = match key_type {
        Some("smallint") => i16::MIN as i128..=i16::MAX as i128,
        Some("integer") => i32::MIN as i128..=i32::MAX as i128,
        _ => return true,
    };
    key.parse::<i128>()
        .is_ok_and(|value| bounds.contains(&value))
}

/// Rebuild `keys` of `imv` in a subtransaction. A PostgreSQL error raised inside
/// (a key the partition column cannot parse, say) rolls back only this heal and
/// comes back as an `ERROR:` string, so one bad IMV cannot abort the sweep and its
/// failure is recorded like any other. A query cancel or shutdown is re-raised: it
/// is the caller stopping the sweep, not a failed heal.
fn reconcile_keys_isolated(imv: &str, keys: &[String]) -> String {
    pgrx::PgTryBuilder::new(|| {
        let subxact = crate::partition::SubTransaction::begin();
        let result = crate::partition::reflex_reconcile_partition_impl(imv, keys, "", false);
        subxact.release();
        result
    })
    .catch_others(|error| {
        use pgrx::pg_sys::panic::CaughtError;
        let (code, message) = match &error {
            CaughtError::PostgresError(report)
            | CaughtError::ErrorReport(report)
            | CaughtError::RustPanic {
                ereport: report, ..
            } => (report.sql_error_code(), report.message().to_string()),
        };
        if matches!(
            code,
            PgSqlErrorCode::ERRCODE_QUERY_CANCELED
                | PgSqlErrorCode::ERRCODE_ADMIN_SHUTDOWN
                | PgSqlErrorCode::ERRCODE_CRASH_SHUTDOWN
        ) {
            error.rethrow();
        }
        format!("ERROR: {message}")
    })
    .execute()
}

const DRAIN_QUEUE_ROWS: &str = "DELETE FROM public.__reflex_heal_pending p \
       USING unnest($2::text[], $3::text[]) AS q(k, at) \
      WHERE p.imv_name = $1 AND p.partition_key = q.k AND p.enqueued_at = q.at::timestamptz \
        AND $4 IS NOT NULL";

const FAIL_QUEUE_ROWS: &str =
    "UPDATE public.__reflex_heal_pending p SET last_error = left($4, 2000) \
       FROM unnest($2::text[], $3::text[]) AS q(k, at) \
      WHERE p.imv_name = $1 AND p.partition_key = q.k AND p.enqueued_at = q.at::timestamptz";

fn record_heal_outcome(heal: &HealResult, keys: Vec<String>, stamps: Vec<String>) {
    if heal.failed() {
        pgrx::warning!("pg_reflex: heal of '{}' failed: {}", heal.imv, heal.result);
        update_queue_rows(FAIL_QUEUE_ROWS, &heal.imv, keys, stamps, &heal.result);
    } else {
        update_queue_rows(DRAIN_QUEUE_ROWS, &heal.imv, keys, stamps, "");
    }
}

/// Apply `sql` to exactly the queue rows `(keys[i], stamps[i])` of `imv`.
fn update_queue_rows(sql: &str, imv: &str, keys: Vec<String>, stamps: Vec<String>, detail: &str) {
    Spi::connect_mut(|client| {
        client
            .update(
                sql,
                None,
                &[
                    unsafe {
                        DatumWithOid::new(imv.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    },
                    unsafe { DatumWithOid::new(keys, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe { DatumWithOid::new(stamps, PgBuiltInOids::TEXTARRAYOID.oid().value()) },
                    unsafe {
                        DatumWithOid::new(detail.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                    },
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

/// IMVs a heal can no longer reach, with the reason: a mapped or watched column
/// of one of their ignored sources was renamed or dropped, or the mapped column
/// was retyped to a type outside `pg_catalog`, so the trigger skips every change
/// to that source. Derived from the catalog on read, not recorded by the
/// trigger, which must not write a registry row every writer of the source
/// would then contend on.
pub(crate) fn unhealable_reasons_by_imv() -> BTreeMap<String, String> {
    Spi::connect(|client| {
        let Ok(rows) = client.select(
            "WITH heal AS ( \
                 SELECT r.name, k.value->>'relation' AS relation, \
                        k.value->>'source_column' AS source_column, \
                        COALESCE(k.value->'watched_columns', '[]'::jsonb) AS watched \
                   FROM public.__reflex_ivm_reference r \
                  CROSS JOIN LATERAL jsonb_each(COALESCE(r.aggregations->'ignore_heal_keys', '{}'::jsonb)) k \
                  WHERE COALESCE(r.enabled, TRUE)) \
             SELECT h.name, format('ignored source %s no longer has column(s) %s', h.relation, \
                                   string_agg(c.col, ', ' ORDER BY c.col)) AS cause \
               FROM heal h \
              CROSS JOIN LATERAL ( \
                    SELECT DISTINCT col FROM jsonb_array_elements_text( \
                        h.watched || jsonb_build_array(h.source_column)) AS col) c \
              WHERE NOT EXISTS (SELECT 1 FROM pg_attribute a \
                                 WHERE a.attrelid = to_regclass(h.relation) \
                                   AND a.attname = c.col AND a.attnum > 0 AND NOT a.attisdropped) \
              GROUP BY h.name, h.relation \
             UNION ALL \
             SELECT h.name, format('ignored source %s column %s is no longer of a built-in type', \
                                   h.relation, h.source_column) \
               FROM heal h \
               JOIN pg_attribute a ON a.attrelid = to_regclass(h.relation) \
                                  AND a.attname = h.source_column \
                                  AND a.attnum > 0 AND NOT a.attisdropped \
               JOIN pg_type t ON t.oid = a.atttypid \
              WHERE t.typnamespace <> 'pg_catalog'::regnamespace \
              ORDER BY 1, 2",
            None,
            &[],
        ) else {
            return BTreeMap::new();
        };
        let mut reasons: BTreeMap<String, String> = BTreeMap::new();
        for row in rows {
            let text = |col: &str| row.get_by_name::<String, _>(col).ok().flatten();
            let (Some(name), Some(cause)) = (text("name"), text("cause")) else {
                continue;
            };
            let reason = format!(
                "{cause}, so its changes can no longer be healed. Recreate the IMV against its \
                 current columns."
            );
            reasons
                .entry(name)
                .and_modify(|existing| {
                    existing.push_str(" | ");
                    existing.push_str(&reason);
                })
                .or_insert(reason);
        }
        reasons
    })
}

/// Stale reasons for every IMV with queued heals or an ignored source it can no
/// longer heal from, and every IMV reading one, transitively: a downstream IMV
/// is exactly as wrong as the slice it reads.
pub(crate) fn heal_stale_reasons_by_imv() -> HashMap<String, String> {
    let mut reasons: HashMap<String, String> = queued_heals_by_imv()
        .iter()
        .map(|(imv, heal)| (imv.clone(), heal.stale_reason(imv)))
        .collect();
    for (imv, reason) in unhealable_reasons_by_imv() {
        reasons
            .entry(imv)
            .and_modify(|existing| {
                existing.push_str(" | ");
                existing.push_str(&reason);
            })
            .or_insert(reason);
    }
    if reasons.is_empty() {
        return reasons;
    }
    let origins: Vec<String> = reasons.keys().cloned().collect();
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
                        "reads IMV '{origin}', which is known stale after one of its ignored \
                         sources changed; repair '{origin}' first (see its stale_reason)."
                    )
                });
            }
        }
    });
    reasons
}
