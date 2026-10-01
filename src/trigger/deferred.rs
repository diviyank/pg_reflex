use super::*;
use crate::query_decomposer::staging_delta_table_name;
use pgrx::datum::DatumWithOid;
use pgrx::PgBuiltInOids;

/// Builds the `CREATE TEMP VIEW` that nets one side of a staged delta against
/// the other (multiset difference / `EXCEPT ALL` semantics): a row appearing
/// identically on both the new side (`I`, `U_NEW`) and the old side (`D`,
/// `U_OLD`) contributes zero net change and is dropped from both. This
/// telescopes `I→U→…→U` chains to the single surviving row per key and is a
/// no-op for every IMV shape (an old/new pair already nets to zero in both the
/// passthrough delete+insert and the aggregate decrement+increment). Without it
/// a key touched twice before the flush stages two new-side rows and trips the
/// unique constraint (docs/fuzz-findings.md finding #2).
///
/// Two equivalent strategies, chosen by `any_text_cast`:
///
/// * No column needs a text cast (the common case): emit `EXCEPT ALL` directly.
///   It is exactly multiset difference, NULL-safe, and the planner runs it as a
///   single hashed/sorted set op — O(n).
///
/// * A `json`/`xml` column is present: those types have no equality operator, so
///   `EXCEPT ALL` over the raw projection cannot run. Fall back to an anti-join
///   that compares a `ROW(...)` of the text-cast `cmp` columns. Comparing the
///   materialised record with `=` uses NULL-safe `record_eq` (so identical
///   NULL-bearing rows still cancel), and `row_number()` over the identical-row
///   partition preserves multiplicity. The record key is sortable/hashable, so
///   the planner uses a merge/hash anti-join — O(n log n). The earlier form put
///   the per-column `IS NOT DISTINCT FROM` in the join *filter* with only
///   `row_number` as the equi-key; when every row is unique that key collapses
///   to the constant 1, collapsing the anti-join to an O(n²) cross-comparison.
pub(crate) fn build_netted_view_sql(
    view: &str,
    projection: &str,
    cmp_csv: &str,
    delta_tbl: &str,
    keep: &str,
    drop: &str,
    any_text_cast: bool,
) -> String {
    if !any_text_cast {
        return format!(
            "CREATE OR REPLACE TEMP VIEW {view} AS \
             SELECT {projection} FROM {delta_tbl} WHERE __reflex_op IN ({keep}) \
             EXCEPT ALL \
             SELECT {projection} FROM {delta_tbl} WHERE __reflex_op IN ({drop})"
        );
    }
    format!(
        "CREATE OR REPLACE TEMP VIEW {view} AS \
         SELECT {projection} FROM ( \
           SELECT {projection}, ROW({cmp_csv}) AS __reflex_nk, \
                  row_number() OVER (PARTITION BY {cmp_csv}) AS __reflex_rn \
           FROM {delta_tbl} WHERE __reflex_op IN ({keep}) \
         ) n \
         WHERE NOT EXISTS ( \
           SELECT 1 FROM ( \
             SELECT ROW({cmp_csv}) AS __reflex_nk, \
                    row_number() OVER (PARTITION BY {cmp_csv}) AS __reflex_rn \
             FROM {delta_tbl} WHERE __reflex_op IN ({drop}) \
           ) o WHERE o.__reflex_rn = n.__reflex_rn AND o.__reflex_nk = n.__reflex_nk \
         )"
    )
}

/// Transaction-local marker of the IMVs already rebuilt in this transaction, with
/// the position of each rebuild ([`Watermark`]): a delta staged for one before it
/// is reflected by the rebuild and skipped, a later one is applied.
const RECONCILED_BATCH_TABLE_DDL: &str =
    "CREATE TEMP TABLE IF NOT EXISTS __reflex_deferred_reconciled_batch \
     (name TEXT PRIMARY KEY, watermark BIGINT NOT NULL, watermark_xid xid NOT NULL) \
     ON COMMIT DROP";

/// Re-arms the COMMIT-time rebuild of `view_name` (a source of it was truncated
/// again): drops it from the marker, if this transaction has one.
pub(crate) fn rearm_rebuild_sql(view_name: &str) -> String {
    format!(
        "DO $_reflex_rearm$ BEGIN \
           IF to_regclass('pg_temp.__reflex_deferred_reconciled_batch') IS NOT NULL THEN \
             DELETE FROM pg_temp.__reflex_deferred_reconciled_batch WHERE name = '{}'; \
           END IF; \
         END $_reflex_rearm$",
        view_name.replace('\'', "''")
    )
}

/// Where a rebuild sits in its transaction: `command` is the command id current
/// when it started (marked used, so every later command has a higher one) and
/// `next_xid` the next transaction id to be assigned then.
///
/// A staged row is written by this transaction's staging triggers (INSERT only)
/// and removed by a flush's DELETE or a source TRUNCATE; nothing else writes
/// staging tables. So a visible row of this transaction with `xmax = 0` was never
/// deleted and its `cmin` is the command that staged it. With `xmax <> 0` a
/// flush of this transaction deleted it in a subtransaction that aborted, and its
/// `cmin` is a combo command id that says nothing about when it was staged; but
/// a deleter whose xid precedes `next_xid` was assigned it before the rebuild, so
/// the row was staged even earlier (a subtransaction cannot be entered and left
/// by its ancestors' writes), and is reflected by the rebuild.
#[derive(Clone, Copy)]
struct Watermark {
    command: i64,
    next_xid: u32,
}

impl Watermark {
    fn now() -> Self {
        let command = i64::from(unsafe { pg_sys::GetCurrentCommandId(true) });
        let next_xid = unsafe { pg_sys::ReadNextFullTransactionId() }.value as u32;
        Self { command, next_xid }
    }

    /// Staged by this transaction after the rebuild (`xmax = 0`), or possibly so
    /// (an aborted deleter that is not older than the rebuild).
    fn after_rebuild_predicate(self) -> String {
        format!(
            "{WRITTEN_BY_THIS_XACT} AND CASE WHEN xmax = '0'::xid \
               THEN cmin::text::bigint > {command} \
               ELSE NOT public.__reflex_xid_precedes(xmax, '{next_xid}'::xid) END",
            command = self.command,
            next_xid = self.next_xid
        )
    }

    /// Rows whose staging time relative to the rebuild cannot be told.
    fn ambiguous_predicate(self) -> String {
        format!(
            "{WRITTEN_BY_THIS_XACT} AND xmax <> '0'::xid \
             AND NOT public.__reflex_xid_precedes(xmax, '{}'::xid)",
            self.next_xid
        )
    }
}

/// The rows of `delta_tbl` staged after the rebuild at `watermark`. Applied only
/// when none of them is ambiguous, so all of them have `xmax = 0`.
fn delta_staged_after(delta_tbl: &str, watermark: Watermark) -> String {
    format!(
        "(SELECT * FROM {delta_tbl} WHERE {}) __reflex_post",
        watermark.after_rebuild_predicate()
    )
}

fn delta_has_rows(client: &pgrx::spi::SpiClient<'_>, delta_tbl: &str, predicate: &str) -> bool {
    let select_bool = |sql: &str| {
        client
            .select(sql, None, &[])
            .unwrap_or_report()
            .first()
            .get_one::<bool>()
            .unwrap_or(None)
            .unwrap_or(false)
    };
    select_bool(&format!(
        "SELECT to_regclass('{}') IS NOT NULL",
        delta_tbl.replace('\'', "''")
    )) && select_bool(&format!(
        "SELECT EXISTS (SELECT 1 FROM {delta_tbl} WHERE {predicate})"
    ))
}

/// Wraparound-safe `a < b` on transaction ids.
#[pg_extern(name = "__reflex_xid_precedes", immutable, parallel_safe)]
fn reflex_xid_precedes(a: pg_sys::TransactionId, b: pg_sys::TransactionId) -> bool {
    unsafe { pg_sys::TransactionIdPrecedes(a, b) }
}

/// Sources `imv_name` observes (its `depends_on` minus `ignored_sources`).
fn observed_sources(client: &pgrx::spi::SpiClient<'_>, imv_name: &str) -> Vec<String> {
    client
        .select(
            "SELECT d FROM public.__reflex_ivm_reference r, unnest(r.depends_on) d \
             WHERE r.name = $1 \
               AND NOT (COALESCE(r.ignored_sources, ARRAY[]::TEXT[]) \
                        && ARRAY[d, regexp_replace(d, '^.*\\.', '')])",
            None,
            &[unsafe {
                DatumWithOid::new(imv_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .unwrap_or_report()
        .filter_map(|row| row.get_by_name::<String, _>("d").unwrap_or(None))
        .collect()
}

fn rebuild_watermark_of(client: &pgrx::spi::SpiClient<'_>, imv_name: &str) -> Option<Watermark> {
    client
        .select(
            "SELECT watermark, watermark_xid::text::bigint AS watermark_xid \
             FROM pg_temp.__reflex_deferred_reconciled_batch WHERE name = $1",
            None,
            &[unsafe {
                DatumWithOid::new(imv_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .unwrap_or_report()
        .next()
        .and_then(|row| {
            let command = row.get_by_name::<i64, _>("watermark").unwrap_or(None)?;
            let next_xid = row.get_by_name::<i64, _>("watermark_xid").unwrap_or(None)?;
            Some(Watermark {
                command,
                next_xid: next_xid as u32,
            })
        })
}

/// `pg_reflex.flush_failure_policy`: `error` aborts the caller on a per-IMV
/// failure; unset, `warn`, or anything unrecognised (with a WARNING) contains it.
fn flush_fail_hard(client: &pgrx::spi::SpiClient<'_>) -> bool {
    let flush_failure_policy = client
        .select(
            "SELECT lower(NULLIF(current_setting('pg_reflex.flush_failure_policy', true), '')) AS v",
            None,
            &[],
        )
        .unwrap_or_report()
        .next()
        .and_then(|row| row.get_by_name::<String, _>("v").unwrap_or(None));
    match flush_failure_policy.as_deref() {
        None | Some("warn") => false,
        Some("error") => true,
        Some(invalid) => {
            pgrx::warning!(
                "pg_reflex: invalid pg_reflex.flush_failure_policy={}, falling back to 'warn'",
                invalid
            );
            false
        }
    }
}

fn temp_table_names(client: &pgrx::spi::SpiClient<'_>, table: &str) -> Vec<String> {
    let present = client
        .select(
            &format!("SELECT to_regclass('pg_temp.{table}') IS NOT NULL AS e"),
            None,
            &[],
        )
        .unwrap_or_report()
        .first()
        .get_one::<bool>()
        .unwrap_or(None)
        .unwrap_or(false);
    if !present {
        return Vec::new();
    }
    client
        .select(&format!("SELECT name FROM pg_temp.{table}"), None, &[])
        .unwrap_or_report()
        .filter_map(|row| row.get_by_name::<String, _>("name").unwrap_or(None))
        .collect()
}

/// Runs `ddl` (a `CREATE TEMP TABLE IF NOT EXISTS`) only when `table` is absent,
/// so a batch does not re-issue it on every flush.
fn ensure_temp_table(client: &mut pgrx::spi::SpiClient<'_>, table: &str, ddl: &str) {
    let present = client
        .select(
            &format!("SELECT to_regclass('pg_temp.{table}') IS NOT NULL"),
            None,
            &[],
        )
        .unwrap_or_report()
        .first()
        .get_one::<bool>()
        .unwrap_or(None)
        .unwrap_or(false);
    if !present {
        client.update(ddl, None, &[]).unwrap_or_report();
    }
}

/// Commit-time rebuilds postponed while upstream DEFERRED IMVs are still pending,
/// per IMV, before rebuilding anyway and marking the IMV stale.
const TRUNCATE_REBUILD_MAX_DEFERRALS: i32 = 64;

/// Whether `xid` is this transaction's or one of its (live or released)
/// subtransactions'. Exact and never raises: no epoch arithmetic, no clog lookup;
/// special, frozen and other transactions' xids are false.
#[pg_extern(name = "__reflex_xid_is_current", stable, parallel_unsafe)]
fn reflex_xid_is_current(xid: pg_sys::TransactionId) -> bool {
    unsafe { pg_sys::TransactionIdIsCurrentTransactionId(xid) }
}

/// Predicate on a row: written by this transaction (or one of its
/// subtransactions); committed rows left by other sessions are not.
const WRITTEN_BY_THIS_XACT: &str = "public.__reflex_xid_is_current(xmin)";

/// Whether an IMV upstream of `imv_name` (transitively, any mode) may still
/// change in this transaction: a DEFERRED one with a delta this transaction
/// staged on a source it does not ignore (its flush, which empties that staging
/// table, is still to come), or one still waiting for its own TRUNCATE rebuild.
fn upstream_pending(
    client: &pgrx::spi::SpiClient<'_>,
    imv_name: &str,
    awaiting_rebuild: &[String],
) -> bool {
    let upstream: Vec<(String, Vec<String>)> = client
        .select(
            "WITH RECURSIVE up(name) AS ( \
               SELECT d FROM public.__reflex_ivm_reference r, unnest(r.depends_on) d \
               WHERE r.name = $1 \
               UNION \
               SELECT d FROM up JOIN public.__reflex_ivm_reference r ON r.name = up.name \
               CROSS JOIN LATERAL unnest(r.depends_on) d \
             ) \
             SELECT r.name::text AS name, \
                    CASE WHEN COALESCE(r.refresh_mode, 'IMMEDIATE') = 'DEFERRED' THEN \
                      ARRAY(SELECT d FROM unnest(r.depends_on) d \
                            WHERE NOT (COALESCE(r.ignored_sources, ARRAY[]::TEXT[]) \
                                       && ARRAY[d, regexp_replace(d, '^.*\\.', '')])) \
                    ELSE ARRAY[]::TEXT[] END AS sources \
             FROM up JOIN public.__reflex_ivm_reference r ON r.name = up.name \
             WHERE r.enabled AND r.name <> $1",
            None,
            &[unsafe {
                DatumWithOid::new(imv_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
            }],
        )
        .unwrap_or_report()
        .map(|row| {
            (
                row.get_by_name::<String, _>("name")
                    .unwrap_or(None)
                    .unwrap_or_default(),
                row.get_by_name::<Vec<String>, _>("sources")
                    .unwrap_or(None)
                    .unwrap_or_default(),
            )
        })
        .collect();
    upstream.iter().any(|(name, sources)| {
        awaiting_rebuild.contains(name)
            || sources
                .iter()
                .any(|source| staged_by_this_xact(client, &staging_delta_table_name(source)))
    })
}

fn staged_by_this_xact(client: &pgrx::spi::SpiClient<'_>, delta_tbl: &str) -> bool {
    let select_bool = |sql: &str| {
        client
            .select(sql, None, &[])
            .unwrap_or_report()
            .first()
            .get_one::<bool>()
            .unwrap_or(None)
            .unwrap_or(false)
    };
    select_bool(&format!(
        "SELECT to_regclass('{}') IS NOT NULL",
        delta_tbl.replace('\'', "''")
    )) && select_bool(&format!(
        "SELECT EXISTS (SELECT 1 FROM {delta_tbl} WHERE {WRITTEN_BY_THIS_XACT})"
    ))
}

/// Whether this transaction already requested a flush for `imv_name` that has
/// not run yet (a 'TRUNCATE' pending row on its observed source).
fn flush_request_outstanding(client: &pgrx::spi::SpiClient<'_>, imv_name: &str) -> bool {
    let Some(source) = observed_source(imv_name) else {
        return false;
    };
    client
        .select(
            &format!(
                "SELECT EXISTS (SELECT 1 FROM public.__reflex_deferred_pending \
                 WHERE source_table = $1 AND operation = 'TRUNCATE' AND {WRITTEN_BY_THIS_XACT})"
            ),
            None,
            &[unsafe { DatumWithOid::new(source, PgBuiltInOids::TEXTOID.oid().value()) }],
        )
        .unwrap_or_report()
        .first()
        .get_one::<bool>()
        .unwrap_or(None)
        .unwrap_or(false)
}

/// stale_reason of an IMV whose COMMIT-time rebuild is postponed; its rebuild clears it.
const POSTPONED_REASON: &str = "COMMIT-time rebuild postponed until upstream DEFERRED IMVs settle";

/// Rebuilds each IMV listed in `__reflex_deferred_rebuild` by a source TRUNCATE
/// (`reflex_build_truncate_sql`) or by the cross-source guard
/// (`flush_staged_deltas`; rebuilt by `rebuild_for_cross_source_guard`), and
/// records it in `__reflex_deferred_reconciled_batch` with its watermark, so the
/// deltas staged for it before the rebuild are skipped and the later ones applied
/// (a rebuild can run before the transaction's last write, e.g. under
/// `SET CONSTRAINTS ALL IMMEDIATE`). A rebuilt IMV is rebuilt again only when
/// re-armed: a second TRUNCATE, or two of its sources changed after the rebuild.
/// Runs around every flush, whatever its source: the rebuild must read every
/// upstream IMV in its final state, so while one may still change
/// ([`upstream_pending`]) it is postponed, and the flush that settles the
/// upstream runs this again. As a fallback a pending row is re-enqueued
/// (bounded; on exhaustion it is rebuilt anyway and marked stale). Nested calls
/// (a flush fired synchronously by a write of this pass, e.g. after
/// `SET CONSTRAINTS ALL IMMEDIATE`) return at once. The rebuild is the one an
/// IMMEDIATE dependent gets at TRUNCATE time, so its dependents receive a row
/// diff. A failure marks the IMV stale (or aborts under
/// `flush_failure_policy = error`).
fn rebuild_truncated_imvs(client: &mut pgrx::spi::SpiClient<'_>) -> usize {
    let nested = client
        .select(
            "SELECT current_setting('pg_reflex.truncate_rebuild_running', true) = 'on'",
            None,
            &[],
        )
        .unwrap_or_report()
        .first()
        .get_one::<bool>()
        .unwrap_or(None)
        .unwrap_or(false);
    if nested {
        return 0;
    }
    let mut listed = listed_for_rebuild(client);
    if listed.is_empty() {
        return 0;
    }
    client
        .update(
            "SELECT set_config('pg_reflex.truncate_rebuild_running', 'on', true)",
            None,
            &[],
        )
        .unwrap_or_report();
    let mut rebuilt = 0usize;
    // A flush nested in this pass (its own pass returns at once) may list a new
    // IMV, or re-list one this pass already rebuilt (dropping it from the marker:
    // rows staged after its rebuild on two sources, an ambiguous row, a second
    // TRUNCATE). This may be the transaction's last pass, so loop until every
    // listed IMV is rebuilt or postponed (postponed ones are flagged stale and
    // have a flush request queued). Bounded: what is still waiting after
    // TRUNCATE_REBUILD_MAX_DEFERRALS rounds is flagged stale.
    let mut rounds = 0;
    loop {
        let (count, postponed) = rebuild_listed(client, &listed);
        rebuilt += count;
        rounds += 1;
        let rebuilt_now = temp_table_names(client, "__reflex_deferred_reconciled_batch");
        listed = listed_for_rebuild(client);
        let waiting: Vec<String> = listed
            .iter()
            .filter(|name| !rebuilt_now.contains(name) && !postponed.contains(name))
            .cloned()
            .collect();
        if waiting.is_empty() {
            break;
        }
        if rounds >= TRUNCATE_REBUILD_MAX_DEFERRALS {
            for name in &waiting {
                pgrx::warning!(
                    "pg_reflex: IMV {} still awaits its COMMIT-time rebuild after {} rounds; \
                     marking it stale",
                    name,
                    TRUNCATE_REBUILD_MAX_DEFERRALS
                );
                client
                    .update(
                        "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, \
                           stale_reason = 'COMMIT-time rebuild re-listed too many times in one ' \
                             || 'rebuild pass; run reflex_reconcile(' || quote_literal($1) \
                             || ') to repair.', \
                           stale_since = now() \
                         WHERE name = $1",
                        None,
                        &[unsafe {
                            DatumWithOid::new(name.clone(), PgBuiltInOids::TEXTOID.oid().value())
                        }],
                    )
                    .unwrap_or_report();
            }
            break;
        }
    }
    client
        .update(
            "SELECT set_config('pg_reflex.truncate_rebuild_running', '', true)",
            None,
            &[],
        )
        .unwrap_or_report();
    rebuilt
}

/// `__reflex_deferred_rebuild` in graph-depth order (upstream IMVs first).
fn listed_for_rebuild(client: &pgrx::spi::SpiClient<'_>) -> Vec<String> {
    if temp_table_names(client, "__reflex_deferred_rebuild").is_empty() {
        return Vec::new();
    }
    client
        .select(
            "SELECT b.name FROM pg_temp.__reflex_deferred_rebuild b \
             LEFT JOIN public.__reflex_ivm_reference r ON r.name = b.name \
             ORDER BY r.graph_depth NULLS LAST, b.name",
            None,
            &[],
        )
        .unwrap_or_report()
        .filter_map(|row| row.get_by_name::<String, _>("name").unwrap_or(None))
        .collect()
}

/// Rebuilds the listed IMVs not yet in the marker; returns how many it rebuilt
/// and the ones it postponed.
fn rebuild_listed(
    client: &mut pgrx::spi::SpiClient<'_>,
    listed: &[String],
) -> (usize, Vec<String>) {
    let mut postponed = Vec::new();
    let mut already_rebuilt = temp_table_names(client, "__reflex_deferred_reconciled_batch");
    let fail_hard = flush_fail_hard(client);
    let mut rebuilt = 0usize;
    for imv_name in listed {
        if already_rebuilt.contains(imv_name) {
            continue;
        }
        let imv_esc = imv_name.replace('\'', "''");
        let awaiting_rebuild: Vec<String> = listed
            .iter()
            .filter(|n| !already_rebuilt.contains(n))
            .cloned()
            .collect();
        let upstream_moving = upstream_pending(client, imv_name, &awaiting_rebuild);
        if upstream_moving {
            let attempts = client
                .select(
                    &format!(
                        "SELECT attempts FROM pg_temp.__reflex_deferred_rebuild WHERE name = '{imv_esc}'"
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report()
                .first()
                .get_one::<i32>()
                .unwrap_or(None)
                .unwrap_or(0);
            if attempts < TRUNCATE_REBUILD_MAX_DEFERRALS {
                // Never end the transaction unrebuilt and unflagged: flagged until rebuilt.
                client
                    .update(
                        &format!(
                            "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, \
                               stale_reason = '{POSTPONED_REASON}', stale_since = now() \
                             WHERE name = '{imv_esc}' AND NOT known_stale"
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
                if !flush_request_outstanding(client, imv_name) {
                    if let Some(enqueue) = enqueue_truncate_flush_sql(imv_name) {
                        client
                            .update(
                                &format!(
                                    "UPDATE pg_temp.__reflex_deferred_rebuild \
                                     SET attempts = attempts + 1 WHERE name = '{imv_esc}'"
                                ),
                                None,
                                &[],
                            )
                            .unwrap_or_report();
                        client.update(&enqueue, None, &[]).unwrap_or_report();
                    }
                }
                postponed.push(imv_name.clone());
                continue;
            }
        }
        ensure_temp_table(
            client,
            "__reflex_deferred_reconciled_batch",
            RECONCILED_BATCH_TABLE_DDL,
        );
        // Taken just before the rebuild reads its sources.
        let watermark = Watermark::now();
        client
            .update(
                &format!(
                    "INSERT INTO __reflex_deferred_reconciled_batch \
                       (name, watermark, watermark_xid) \
                     VALUES ('{imv_esc}', {}, '{}'::xid) \
                     ON CONFLICT (name) DO UPDATE SET watermark = EXCLUDED.watermark, \
                       watermark_xid = EXCLUDED.watermark_xid",
                    watermark.command, watermark.next_xid
                ),
                None,
                &[],
            )
            .unwrap_or_report();
        already_rebuilt.push(imv_name.clone());
        let listed_by_guard = client
            .select(
                &format!(
                    "SELECT reconcile FROM pg_temp.__reflex_deferred_rebuild WHERE name = '{imv_esc}'"
                ),
                None,
                &[],
            )
            .unwrap_or_report()
            .first()
            .get_one::<bool>()
            .unwrap_or(None)
            .unwrap_or(false);
        if listed_by_guard {
            rebuild_for_cross_source_guard(client, imv_name, upstream_moving, fail_hard);
            rebuilt += 1;
            continue;
        }
        let mut stmts = match truncate_rebuild(imv_name) {
            TruncateRebuild::Stmts(stmts) => stmts,
            TruncateRebuild::Wrapper => continue,
            TruncateRebuild::Unreadable => {
                client
                    .update(&mark_stale_after_truncate_sql(imv_name), None, &[])
                    .unwrap_or_report();
                continue;
            }
        };
        stmts.push(format!(
            "UPDATE public.__reflex_ivm_reference \
               SET known_stale = FALSE, stale_reason = NULL, stale_since = NULL \
             WHERE name = '{imv_esc}' AND stale_reason = '{POSTPONED_REASON}'"
        ));
        if upstream_moving {
            pgrx::warning!(
                "pg_reflex: IMV {} rebuilt after a source TRUNCATE while upstream DEFERRED IMVs \
                 were still pending after {} deferrals; marking it stale",
                imv_name,
                TRUNCATE_REBUILD_MAX_DEFERRALS
            );
            stmts.push(format!(
                "UPDATE public.__reflex_ivm_reference SET known_stale = TRUE, \
                   stale_reason = 'rebuilt after a source TRUNCATE while upstream DEFERRED IMVs \
                     still had pending changes ({TRUNCATE_REBUILD_MAX_DEFERRALS} deferrals \
                     exhausted); run reflex_reconcile(''{imv_esc}'') to repair.', \
                   stale_since = now() \
                 WHERE name = '{imv_esc}'"
            ));
        }
        let body = stmts
            .iter()
            .map(|s| format!("{};", as_plpgsql_stmt(s)))
            .collect::<Vec<_>>()
            .join("\n");
        let exception_clause = if fail_hard {
            String::new()
        } else {
            format!(
                "EXCEPTION WHEN OTHERS THEN \
                   RAISE WARNING 'pg_reflex: IMV % rebuild after source TRUNCATE failed: % (SQLSTATE %)', \
                     '{imv_esc}', SQLERRM, SQLSTATE; \
                   UPDATE public.__reflex_ivm_reference \
                     SET known_stale = TRUE, \
                         last_error = LEFT(SQLERRM || ' (SQLSTATE ' || SQLSTATE || ')', 500), \
                         stale_reason = LEFT('rebuild after source TRUNCATE failed: ' || SQLERRM \
                                             || '; run reflex_reconcile(''{imv_esc}'') to repair.', 2000), \
                         stale_since = now() \
                     WHERE name = '{imv_esc}';"
            )
        };
        client
            .update(
                &format!(
                    "DO $_reflex_trunc_rb$ BEGIN \
                       PERFORM pg_advisory_xact_lock(hashtext('{imv_esc}'), hashtext(reverse('{imv_esc}'))); \
                       \n{body}\n \
                     {exception_clause} \
                     END $_reflex_trunc_rb$"
                ),
                None,
                &[],
            )
            .unwrap_or_report();
        rebuilt += 1;
    }
    (rebuilt, postponed)
}

/// Rebuilds an IMV the cross-source guard listed, through `reflex_reconcile`
/// (a generated sub-IMV through its propagation-safe variant), in its own
/// subtransaction: a raised error rolls back only this rebuild (or, under
/// `flush_failure_policy = error`, aborts the caller). A failure, raised or
/// returned as an `ERROR:` string, flags the IMV stale: the deltas staged for it
/// in this transaction are skipped, so nothing else would ever repair it.
///
/// Flagging is safe only because the IMV can never be a decomposed WRAPPER,
/// whose `reconcile_one` refusal is an `ERROR:` string nothing could ever clear.
/// The guard lists an IMV the flush selected by `= ANY(depends_on)`; a wrapper's
/// `depends_on` holds only its generated operands, which carry
/// `__reflex_union_mirror_*` triggers only — never the staging triggers that
/// write `__reflex_deferred_pending` (pinned by
/// `xsu_wrapper_operands_have_no_staging_triggers`).
fn rebuild_for_cross_source_guard(
    client: &mut pgrx::spi::SpiClient<'_>,
    imv_name: &str,
    upstream_moving: bool,
    fail_hard: bool,
) {
    let result = if fail_hard {
        crate::reconcile::reconcile_for_cross_source_guard(imv_name).to_string()
    } else {
        crate::reconcile::reconcile_isolated(imv_name)
    };
    let failed = result.starts_with("ERROR");
    let stale_reason = if failed {
        pgrx::warning!(
            "pg_reflex: IMV {} cross-source guard reconcile failed: {}",
            imv_name,
            result
        );
        format!("cross-source guard reconcile failed: {result}")
    } else if upstream_moving {
        pgrx::warning!(
            "pg_reflex: IMV {} rebuilt by the cross-source guard while upstream DEFERRED IMVs \
             were still pending after {} deferrals; marking it stale",
            imv_name,
            TRUNCATE_REBUILD_MAX_DEFERRALS
        );
        format!(
            "rebuilt by the cross-source guard while upstream DEFERRED IMVs still had pending \
             changes ({TRUNCATE_REBUILD_MAX_DEFERRALS} deferrals exhausted); run \
             reflex_reconcile('{imv_name}') to repair."
        )
    } else {
        return;
    };
    client
        .update(
            "UPDATE public.__reflex_ivm_reference \
             SET known_stale = TRUE, stale_reason = left($2, 2000), stale_since = now(), \
                 last_error = CASE WHEN $3 THEN left($4, 500) ELSE last_error END \
             WHERE name = $1",
            None,
            &[
                unsafe {
                    DatumWithOid::new(imv_name.to_string(), PgBuiltInOids::TEXTOID.oid().value())
                },
                unsafe { DatumWithOid::new(stale_reason, PgBuiltInOids::TEXTOID.oid().value()) },
                unsafe { DatumWithOid::new(failed, PgBuiltInOids::BOOLOID.oid().value()) },
                unsafe { DatumWithOid::new(result, PgBuiltInOids::TEXTOID.oid().value()) },
            ],
        )
        .unwrap_or_report();
}

/// Flushes all accumulated deferred deltas for a given source table.
///
/// Called by the deferred constraint trigger at COMMIT time.
/// Reads from the staging table (__reflex_delta_<source>), applies deltas
/// to each DEFERRED IMV, then cleans up staging and pending rows.
#[pg_extern]
pub fn reflex_flush_deferred(source_table: &str) -> String {
    // Before: a rebuilt IMV skips this flush's delta. After: this flush may have
    // settled an upstream IMV a postponed rebuild was waiting for.
    Spi::connect_mut(rebuild_truncated_imvs);
    let flushed = flush_staged_deltas(source_table);
    Spi::connect_mut(rebuild_truncated_imvs);
    flushed
}

fn flush_staged_deltas(source_table: &str) -> String {
    let delta_tbl = staging_delta_table_name(source_table);

    // Read all DEFERRED IMVs that depend on this source. IMVs that listed this
    // source in `ignore_sources` are excluded — the array-overlap check matches
    // both the qualified ($1) and bare ($2) forms, mirroring the trigger-body
    // runtime skip so the ignore contract holds on the deferred path too.
    let bare_source = source_table
        .split('.')
        .next_back()
        .unwrap_or(source_table)
        .to_string();
    let imvs: Vec<(String, String, String, String, Option<String>)> = Spi::connect(|client| {
        let args = [
            unsafe {
                DatumWithOid::new(
                    source_table.to_string(),
                    PgBuiltInOids::TEXTOID.oid().value(),
                )
            },
            unsafe { DatumWithOid::new(bare_source.clone(), PgBuiltInOids::TEXTOID.oid().value()) },
        ];
        client
            .select(
                "SELECT name, base_query, end_query, aggregations::text AS aggregations, \
                        where_predicate \
                 FROM public.__reflex_ivm_reference \
                 WHERE $1 = ANY(depends_on) AND enabled = TRUE \
                   AND COALESCE(refresh_mode, 'IMMEDIATE') = 'DEFERRED' \
                   AND NOT (COALESCE(ignored_sources, ARRAY[]::TEXT[]) && ARRAY[$1, $2]::TEXT[]) \
                 ORDER BY graph_depth, name",
                None,
                &args,
            )
            .unwrap_or_report()
            .map(|row| {
                (
                    row.get_by_name::<&str, _>("name")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    row.get_by_name::<&str, _>("base_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    row.get_by_name::<&str, _>("end_query")
                        .unwrap_or(None)
                        .unwrap_or("")
                        .to_string(),
                    row.get_by_name::<&str, _>("aggregations")
                        .unwrap_or(None)
                        .unwrap_or("{}")
                        .to_string(),
                    row.get_by_name::<&str, _>("where_predicate")
                        .unwrap_or(None)
                        .map(|s: &str| s.to_string()),
                )
            })
            .collect()
    });

    if imvs.is_empty() {
        return "NO DEFERRED IMVS".to_string();
    }

    let mut total_processed = 0usize;

    Spi::connect_mut(|client| {
        // 1.4.3 — Serialize flushes on the same source.
        //
        // ANALYZE (ShareUpdateExclusiveLock) + TRUNCATE (AccessExclusiveLock)
        // on the same staging-delta table inside the same transaction is a
        // classic deadlock antipattern when two sessions flush concurrently:
        // ShareUpdateExclusive is self-conflicting, so the second session
        // queues behind the first's ANALYZE. When the first then tries to
        // upgrade to AccessExclusive for the end-of-flush TRUNCATE, the lock
        // manager queues that request *behind* the second's pending
        // ShareUpdate request, and a cycle forms. Reproduced as a real
        // 42P40 deadlock under customer concurrency.
        //
        // The advisory lock is acquired before any table-level lock on the
        // staging delta, so the second session blocks here and the locks
        // inside execute in single-session order on each turn.
        let lock_key = format!("reflex_flush:{}", source_table).replace('\'', "''");
        client
            .update(
                &format!("SELECT pg_advisory_xact_lock(hashtext('{}'))", lock_key),
                None,
                &[],
            )
            .unwrap_or_report();

        // Check if staging table has any rows
        let has_rows = client
            .select(
                &format!("SELECT EXISTS(SELECT 1 FROM {} LIMIT 1) AS has", delta_tbl),
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .map(|row| {
                row.get_by_name::<bool, _>("has")
                    .unwrap_or(None)
                    .unwrap_or(false)
            })
            .unwrap_or(false);

        if !has_rows {
            // No deltas to process — clean up pending rows
            client
                .update(
                    &format!(
                        "DELETE FROM public.__reflex_deferred_pending \
                         WHERE source_table = '{}' AND operation <> 'TRUNCATE'",
                        source_table.replace("'", "''")
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
            return;
        }

        // Refresh planner stats on the staging delta so queries over it get correct
        // row estimates (TRUNCATE resets stats to zero; without ANALYZE the planner
        // assumes an empty table and may pick a bad plan).
        client
            .update(&format!("ANALYZE {}", delta_tbl), None, &[])
            .unwrap_or_report();

        // Passthrough INSERT/DELETE/UPDATE branches in reflex_build_delta_sql
        // reference the NEW/OLD transition tables literally — either directly
        // (pre-Phase-E paths) or via the Phase E per-(IMV, source) scratch
        // populate `INSERT INTO __reflex_pt_*_<v>_<s> SELECT * FROM __reflex_(new|old)_<s>`.
        // Those transition tables only exist inside an IMMEDIATE trigger's
        // REFERENCING scope; here we're at COMMIT, so stand both sides up as
        // temp views over the staging delta. The views must project the source
        // columns only (no `__reflex_op` metadata column) so downstream DML —
        // including `INSERT INTO pt_scratch SELECT * FROM view` where pt_scratch
        // is shaped `LIKE source` — sees the same column list as a real
        // transition table.
        // Fetch raw column NAME + TYPE-NAME together. The type name is
        // needed to cast `json` / `xml` to `text` in EXCEPT ALL projections
        // — those types lack an equality operator and crash the comparison
        // otherwise. The raw column projection (for the TEMP VIEW that
        // downstream incremental codegen reads) stays unchanged.
        //
        // Resolve via `to_regclass($1)`: this is the original schema-mismatch
        // bug site (pre-fix, an unwrap_or("public") fallback projected the
        // wrong homonym's columns when `source_table` arrived bare). Upstream
        // canonicalization at IMV-create time keeps `source_table` qualified
        // for non-public sources, but feeding the lookup through `to_regclass`
        // makes correctness independent of caller hygiene.
        let src_cols_with_types: Vec<(String, String)> = client
            .select(
                "SELECT a.attname::text AS rn, t.typname::text AS tn \
                 FROM pg_attribute a \
                 JOIN pg_type t ON t.oid = a.atttypid \
                 WHERE a.attrelid = to_regclass($1) \
                   AND a.attnum > 0 AND NOT a.attisdropped \
                 ORDER BY a.attnum",
                None,
                &[unsafe {
                    DatumWithOid::new(
                        source_table.to_string(),
                        PgBuiltInOids::TEXTOID.oid().value(),
                    )
                }],
            )
            .unwrap_or_report()
            .filter_map(|row| {
                let name = row
                    .get_by_name::<&str, _>("rn")
                    .unwrap_or(None)
                    .map(|s| s.to_string());
                let tn = row
                    .get_by_name::<&str, _>("tn")
                    .unwrap_or(None)
                    .map(|s| s.to_string());
                match (name, tn) {
                    (Some(n), Some(t)) => Some((n, t)),
                    _ => None,
                }
            })
            .collect();
        let quote_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let needs_text_cast = |t: &str| t == "json" || t == "xml";
        let src_cols: Vec<String> = src_cols_with_types
            .iter()
            .map(|(n, _)| quote_ident(n))
            .collect();
        // EXCEPT ALL comparison projection: cast types that lack an
        // equality operator to text. `json` and `xml` are the two stock
        // PG types in this category that real schemas commonly use.
        let cmp_cols: Vec<String> = src_cols_with_types
            .iter()
            .map(|(n, t)| {
                let q = quote_ident(n);
                if needs_text_cast(t) {
                    format!("{}::text", q)
                } else {
                    q
                }
            })
            .collect();
        let col_type_map: std::collections::HashMap<String, String> = src_cols_with_types
            .iter()
            .map(|(n, t)| (n.clone(), t.clone()))
            .collect();
        let projection = src_cols.join(", ");
        let new_view = transition_new_table_name(source_table);
        let old_view = transition_old_table_name(source_table);
        // The new-side (I, U_NEW) and old-side (D, U_OLD) of a batch can both
        // carry a row for the same key when one key is touched more than once
        // before the flush — e.g. INSERT k then UPDATE k stages I(v0), U_OLD(v0),
        // U_NEW(v1): the new side then holds BOTH v0 and v1 for k. Feeding that
        // straight into maintenance makes the passthrough INSERT add two rows for
        // k → "duplicate key value violates unique constraint __reflex_uk_*"
        // (docs/fuzz-findings.md finding #2).
        //
        // Net the two sides against each other (multiset difference): a row that
        // appears identically on both sides contributes zero net change, so it is
        // dropped from both. This telescopes any I→U→…→U chain down to the single
        // final row per key (v0 cancels, v1 survives) and is semantically a no-op
        // for every IMV shape — an old/new pair already nets to zero in both the
        // passthrough delete+insert and the aggregate decrement+increment.
        //
        // Matching uses `cmp_cols` (json/xml cast to text) so types without an
        // equality operator do not break the comparison. See
        // `build_netted_view_sql` for the two equivalent strategies (set-op vs
        // record-key anti-join) and why the choice hinges on `any_text_cast`.
        let cmp_csv = cmp_cols.join(", ");
        let any_text_cast = src_cols_with_types.iter().any(|(_, t)| needs_text_cast(t));
        // The views read `rel`: the staging table, or for an IMV rebuilt in this
        // transaction only the rows staged after its rebuild.
        let create_netted_views = |client: &mut pgrx::spi::SpiClient<'_>, rel: &str| {
            for (view, keep, drop) in [
                (&new_view, "'I', 'U_NEW'", "'D', 'U_OLD'"),
                (&old_view, "'D', 'U_OLD'", "'I', 'U_NEW'"),
            ] {
                client
                    .update(
                        &build_netted_view_sql(
                            view,
                            &projection,
                            &cmp_csv,
                            rel,
                            keep,
                            drop,
                            any_text_cast,
                        ),
                        None,
                        &[],
                    )
                    .unwrap_or_report();
            }
        };
        create_netted_views(client, &delta_tbl);
        let mut views_over = delta_tbl.clone();

        // 1.4.3 — Spurious-UPDATE short-circuit.
        //
        // If the staging delta contains only paired U_OLD/U_NEW rows whose
        // projections to the source columns are identical multisets (i.e.
        // every UPDATE was a no-op at the column level — e.g. `SET
        // status='validated'` on a row whose status is already 'validated'),
        // no IMV can observe a change. Skip every IMV body, clean up, return.
        //
        // EXCEPT ALL is multiset subtraction; if both directions are empty
        // and there are no INSERT/DELETE rows, U_OLD ≡ U_NEW.
        //
        // `cmp_cols` is non-empty for every real PG table (tables have at
        // least one column), so the run-the-EXCEPT branch always executes.
        let cols_csv = cmp_cols.join(", ");
        let is_spurious = {
            let sql = format!(
                "WITH \
                   has_id AS (SELECT 1 FROM {delta} WHERE __reflex_op IN ('I', 'D') LIMIT 1), \
                   only_old AS ( \
                     SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_OLD' \
                     EXCEPT ALL \
                     SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_NEW' \
                   ), \
                   only_new AS ( \
                     SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_NEW' \
                     EXCEPT ALL \
                     SELECT {cols} FROM {delta} WHERE __reflex_op = 'U_OLD' \
                   ) \
                 SELECT NOT EXISTS(SELECT 1 FROM has_id) \
                    AND NOT EXISTS(SELECT 1 FROM only_old) \
                    AND NOT EXISTS(SELECT 1 FROM only_new) AS sp",
                delta = delta_tbl,
                cols = cols_csv,
            );
            client
                .select(&sql, None, &[])
                .unwrap_or_report()
                .next()
                .map(|row| {
                    row.get_by_name::<bool, _>("sp")
                        .unwrap_or(None)
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        };

        // The marker (created below) survives across the batch's per-source
        // flush calls. A later flush sees a shrunken pending set (each flush
        // deletes its own pending rows), so `batch_has_multiple_sources` may
        // already read false by then — but if the marker exists, a
        // multi-source reconcile happened earlier in this batch and this flush
        // MUST still treat the rebuilt IMVs as rebuilt. `pg_my_temp_schema()` scopes the
        // lookup to this session's temp schema (0 ⇒ no temp schema yet).
        let marker_exists = client
            .select(
                "SELECT EXISTS(SELECT 1 FROM pg_class \
                   WHERE relname = '__reflex_deferred_reconciled_batch' \
                     AND relnamespace = pg_my_temp_schema()) AS e",
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .map(|row| {
                row.get_by_name::<bool, _>("e")
                    .unwrap_or(None)
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        // A delta that nets to nothing as a whole need not for an IMV rebuilt in
        // this transaction: it takes only the rows staged after its rebuild.
        let any_rebuilt_here = is_spurious
            && marker_exists
            && imvs
                .iter()
                .any(|imv| rebuild_watermark_of(client, &imv.0).is_some());
        if is_spurious && !any_rebuilt_here {
            // No IMV processing. Clean up the staging delta and pending rows.
            // DELETE (not TRUNCATE) — see end-of-function comment.
            client
                .update(&format!("DELETE FROM {}", delta_tbl), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!(
                        "DELETE FROM public.__reflex_deferred_pending \
                         WHERE source_table = '{}' AND operation <> 'TRUNCATE'",
                        source_table.replace("'", "''")
                    ),
                    None,
                    &[],
                )
                .unwrap_or_report();
            return;
        }

        // Cross-source consistency gate (deferred mode). When 2+ distinct
        // sources staged deltas in the same transaction, an IMV that joins two
        // of them would double-count the ΔA⋈ΔB cross product if each source's
        // net delta were applied independently: every per-source delta joins
        // against the OTHER sources' already-committed NEW state, so the cross
        // product is added once per mutated source instead of once total.
        // IMMEDIATE mode is immune (per-statement triggers apply deltas
        // sequentially, each seeing the correct intermediate state); the
        // hazard is unique to the commit-time batch flush. Detect the batch
        // shape once here — the per-IMV loop lists any affected IMV for one
        // COMMIT-time full rebuild (`rebuild_listed`), and every delta staged for
        // it before that rebuild is skipped. 'TRUNCATE' rows are
        // flush requests, not staged deltas, so they never count as a source.
        let batch_has_multiple_sources = client
            .select(
                "SELECT count(DISTINCT source_table) >= 2 AS m FROM public.__reflex_deferred_pending \
                 WHERE operation <> 'TRUNCATE'",
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .map(|row| row.get_by_name::<bool, _>("m").unwrap_or(None).unwrap_or(false))
            .unwrap_or(false);
        let engage_cross_source_guard = batch_has_multiple_sources || marker_exists;
        if engage_cross_source_guard && !marker_exists {
            // ON COMMIT DROP: one marker per transaction, shared across the
            // per-source flush calls (the constraint trigger flushes each
            // mutated source separately), auto-removed at commit. Records the
            // IMVs already full-reconciled in this batch so a later source's
            // flush skips them — its net delta would otherwise corrupt the
            // just-reconciled state.
            client
                .update(RECONCILED_BATCH_TABLE_DDL, None, &[])
                .unwrap_or_report();
        }

        // A5 — `pg_reflex.flush_failure_policy`. Default `warn`: a per-IMV flush
        // failure is caught, the IMV is marked known_stale, and the cascade
        // continues (existing behaviour, unchanged for every caller who never
        // sets this). Opt-in `error` drops the per-IMV EXCEPTION handler so the
        // failure propagates and aborts the whole caller transaction instead.
        // Unset or empty means `warn`. Any other unrecognised value also falls
        // back to `warn` and raises a WARNING naming it — the same contract as
        // `pg_reflex.alter_source_policy` (src/lib.rs, `__reflex_on_ddl_command_end`):
        // a typo must never silently select either mode.
        let fail_hard = flush_fail_hard(client);

        for (imv_name, base_query, end_query, agg_json, where_pred) in &imvs {
            let mut delta_rel = delta_tbl.clone();
            if engage_cross_source_guard {
                let imv_esc = imv_name.replace('\'', "''");
                if let Some(watermark) = rebuild_watermark_of(client, imv_name) {
                    // Rebuilt in this transaction: the deltas staged up to its
                    // rebuild are in it; the ones staged after are applied.
                    let after = watermark.after_rebuild_predicate();
                    if !delta_has_rows(client, &delta_tbl, &after) {
                        continue;
                    }
                    // A row whose staging cannot be placed relative to the
                    // rebuild is never applied incrementally; nor are deltas of
                    // two sources changed since it (applied one by one they
                    // would count their cross product twice). Rebuild it again,
                    // from the final state, after this flush.
                    let rebuild_again =
                        delta_has_rows(client, &delta_tbl, &watermark.ambiguous_predicate())
                            || observed_sources(client, imv_name).iter().any(|source| {
                                source != source_table
                                    && delta_has_rows(
                                        client,
                                        &staging_delta_table_name(source),
                                        &after,
                                    )
                            });
                    if rebuild_again {
                        ensure_temp_table(
                            client,
                            "__reflex_deferred_rebuild",
                            crate::trigger::DEFERRED_REBUILD_TABLE_DDL,
                        );
                        client
                            .update(
                                &format!(
                                    "DELETE FROM pg_temp.__reflex_deferred_reconciled_batch \
                                     WHERE name = '{imv_esc}'; \
                                     INSERT INTO pg_temp.__reflex_deferred_rebuild (name) \
                                     VALUES ('{imv_esc}') \
                                     ON CONFLICT (name) DO UPDATE SET attempts = 0"
                                ),
                                None,
                                &[],
                            )
                            .unwrap_or_report();
                        total_processed += 1;
                        continue;
                    }
                    delta_rel = delta_staged_after(&delta_tbl, watermark);
                } else {
                    if is_spurious {
                        continue;
                    }
                    if temp_table_names(client, "__reflex_deferred_rebuild").contains(imv_name) {
                        // Listed for a COMMIT-time rebuild (`rebuild_listed`)
                        // that has not run yet: it reads this delta's final state.
                        continue;
                    }
                    // Count this IMV's own sources that are pending in the batch.
                    // The first of the IMV's sources to be flushed still sees all
                    // of them pending (per-source cleanup only runs for sources
                    // already processed, none of which are this IMV's), so it
                    // reliably detects the multi-source shape and reconciles.
                    let imv_has_multiple_sources = client
                        .select(
                            &format!(
                                "SELECT count(DISTINCT p.source_table) >= 2 AS m \
                                 FROM public.__reflex_deferred_pending p \
                                 JOIN public.__reflex_ivm_reference r ON r.name = '{}' \
                                 WHERE p.source_table = ANY(r.depends_on) \
                                   AND p.operation <> 'TRUNCATE'",
                                imv_esc
                            ),
                            None,
                            &[],
                        )
                        .unwrap_or_report()
                        .next()
                        .map(|row| {
                            row.get_by_name::<bool, _>("m")
                                .unwrap_or(None)
                                .unwrap_or(false)
                        })
                        .unwrap_or(false);
                    if imv_has_multiple_sources {
                        // Listed for the COMMIT-time rebuild pass that follows this
                        // flush (`reflex_flush_deferred`) instead of reconciled here:
                        // the rebuild must read every upstream DEFERRED IMV in its
                        // final state, so it waits until none has a delta this
                        // transaction staged and still unflushed (`upstream_pending`),
                        // and runs in its own subtransaction, so a failure flags the
                        // IMV stale instead of aborting the COMMIT
                        // (`rebuild_for_cross_source_guard`).
                        ensure_temp_table(
                            client,
                            "__reflex_deferred_rebuild",
                            crate::trigger::DEFERRED_REBUILD_TABLE_DDL,
                        );
                        client
                            .update(
                                &format!(
                                    "INSERT INTO pg_temp.__reflex_deferred_rebuild (name, reconcile) \
                                     VALUES ('{imv_esc}', TRUE) \
                                     ON CONFLICT (name) DO UPDATE SET reconcile = TRUE"
                                ),
                                None,
                                &[],
                            )
                            .unwrap_or_report();
                        total_processed += 1;
                        continue;
                    }
                }
            }
            // 1.4.5 — Skip this IMV iff NO staged row matches the
            // predicate, on either side of the delta. The 1.4.4 check
            // looked only at NEW-state rows (`I` + `U_NEW`); that silently
            // dropped row-leaves-filter UPDATEs — when a row transitions
            // out of the IMV's WHERE the IMV must DELETE its
            // contribution, which requires the trigger body to run. The OR
            // below extends the gate to OLD-state passing rows so we no
            // longer mistake "no new row to add" for "no work at all".
            if let Some(pred) = where_pred {
                let pred_sql = format!(
                    "SELECT EXISTS( \
                        SELECT 1 FROM {delta} WHERE __reflex_op IN ('I', 'U_NEW') AND ({pred}) \
                     ) OR EXISTS( \
                        SELECT 1 FROM {delta} WHERE __reflex_op IN ('D', 'U_OLD') AND ({pred}) \
                     ) AS m",
                    delta = delta_rel,
                    pred = pred,
                );
                let matched = client
                    .select(&pred_sql, None, &[])
                    .unwrap_or_report()
                    .next()
                    .map(|row| {
                        row.get_by_name::<bool, _>("m")
                            .unwrap_or(None)
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if !matched {
                    continue;
                }
            }

            // 1.4.5 — Per-IMV filter-aware spurious-skip in DEFERRED mode.
            //
            // The 1.4.3 byte-identical multiset check above is a *source-wide*
            // gate — it fires only when EVERY column is byte-equal on every
            // U_OLD/U_NEW pair AND there are no INSERT/DELETE rows. That's
            // strong but rare. The check below is per-IMV and runs even when
            // the source-wide gate didn't fire:
            //
            //   * Read the IMV's `imv_relevant_columns[source_table]` — the
            //     columns the IMV actually projects / joins on / groups by.
            //     Filter-only columns (in WHERE only) are absent.
            //   * Read the source-restricted `imv_relevant_where[source]`
            //     — alias-stripped conjuncts that evaluate against the flat
            //     staging delta.
            //   * Compare multisets of (relevant_cols)-projected rows from
            //     old-state (U_OLD ∪ D) vs new-state (U_NEW ∪ I), each
            //     filtered by the per-source predicate.
            //
            // If multisets match in both directions, the IMV's output cannot
            // change for any group touched by this delta — skip it.
            //
            // Absent metadata (CTE IMVs, SELECT *, or IMVs created before
            // 1.4.5 metadata backfill) falls through to the existing path.
            let agg_jsonb: Result<serde_json::Value, _> = serde_json::from_str(agg_json);
            if let Ok(jv) = agg_jsonb {
                let cols_arr = jv
                    .get("imv_relevant_columns")
                    .and_then(|m| m.get(source_table))
                    .and_then(|a| a.as_array());
                if let Some(cols) = cols_arr {
                    // No runtime catalog filter — the analyzer (1.4.5) only
                    // attributes a column to a source when the reference is
                    // unambiguously resolvable, so every column listed here
                    // is guaranteed to exist on the source's transition /
                    // delta table.
                    // Cast json/xml to text (no equality operator). Drop
                    // columns the analyzer wrongly attributed to this
                    // source: pre-1.5.1 `create_ivm` only filtered the
                    // attribution catalog for aggregate IMVs, so
                    // passthrough IMVs created before that fix may have
                    // `imv_relevant_columns[source]` entries that don't
                    // exist on the source table. Selecting one would
                    // crash the EXCEPT ALL with `column "X" does not
                    // exist`. The `col_type_map` is populated from the
                    // source's catalog above; absence ⇒ not on the
                    // source ⇒ drop.
                    let cols_csv = cols
                        .iter()
                        .filter_map(|v| v.as_str())
                        .filter_map(|c| {
                            let q = format!("\"{}\"", c.replace('"', "\"\""));
                            match col_type_map.get(c) {
                                Some(t) if needs_text_cast(t) => Some(format!("{}::text", q)),
                                Some(_) => Some(q),
                                None => None,
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    if !cols_csv.is_empty() {
                        let pred = jv
                            .get("imv_relevant_where")
                            .and_then(|m| m.get(source_table))
                            .and_then(|s| s.as_str())
                            .filter(|s| !s.is_empty());
                        let old_filter = match pred {
                            Some(p) => format!("__reflex_op IN ('U_OLD', 'D') AND ({})", p),
                            None => "__reflex_op IN ('U_OLD', 'D')".to_string(),
                        };
                        let new_filter = match pred {
                            Some(p) => format!("__reflex_op IN ('U_NEW', 'I') AND ({})", p),
                            None => "__reflex_op IN ('U_NEW', 'I')".to_string(),
                        };
                        let sql = format!(
                            "WITH \
                               diff_o AS ( \
                                 SELECT {cols} FROM {delta} WHERE {of} \
                                 EXCEPT ALL \
                                 SELECT {cols} FROM {delta} WHERE {nf} \
                               ), \
                               diff_n AS ( \
                                 SELECT {cols} FROM {delta} WHERE {nf} \
                                 EXCEPT ALL \
                                 SELECT {cols} FROM {delta} WHERE {of} \
                               ) \
                             SELECT NOT EXISTS(SELECT 1 FROM diff_o) \
                                AND NOT EXISTS(SELECT 1 FROM diff_n) AS sp",
                            cols = cols_csv,
                            delta = delta_rel,
                            of = old_filter,
                            nf = new_filter,
                        );
                        let filter_skip = client
                            .select(&sql, None, &[])
                            .unwrap_or_report()
                            .next()
                            .map(|row| {
                                row.get_by_name::<bool, _>("sp")
                                    .unwrap_or(None)
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false);
                        if filter_skip {
                            continue;
                        }
                    }
                }
            }

            if views_over != delta_rel {
                create_netted_views(client, &delta_rel);
                views_over = delta_rel.clone();
            }

            // Collect every per-IMV statement into an ordered list; we emit them
            // inside a single PL/pgSQL DO block with EXCEPTION so one bad IMV
            // rolls back only its own subtransaction and lets the cascade continue.
            let mut imv_stmts: Vec<String> = Vec::new();

            imv_stmts.push(format!(
                "PERFORM pg_advisory_xact_lock(hashtext('{}'), hashtext(reverse('{}')))",
                imv_name.replace("'", "''"),
                imv_name.replace("'", "''")
            ));

            // 1.4.3 — Single op="UPDATE" call replaces the previous 4-way
            // dispatch (INSERT / DELETE / U_OLD-as-DELETE / U_NEW-as-INSERT).
            // The TEMP VIEWs created above (`__reflex_new_<src>` =
            // I + U_NEW, `__reflex_old_<src>` = D + U_OLD) act exactly like
            // the IMMEDIATE-mode transition tables, so `reflex_build_delta_sql`
            // routes through the normal UPDATE path and `build_net_delta_query`
            // fuses both halves into a single JOIN-scan instead of running
            // sub + add as two independent scans. Cuts per-flush JOIN cost ~2×
            // for real updates and exercises a single, well-tested code path.
            let upd_sql = reflex_build_delta_sql(
                imv_name,
                source_table,
                "UPDATE",
                base_query,
                end_query,
                Some(agg_json.as_str()),
                base_query,
            );
            let mut had_stmts = false;
            if !upd_sql.is_empty() {
                for stmt in upd_sql.split("\n--<<REFLEX_SEP>>--\n") {
                    if !stmt.is_empty() {
                        imv_stmts.push(stmt.to_string());
                        had_stmts = true;
                    }
                }
            }

            // Phase 3.4 — wrap per-IMV statements in a PL/pgSQL DO block. The
            // BEGIN…EXCEPTION…END creates an internal subtransaction: a single
            // bad IMV only rolls back its own work and logs a WARNING instead of
            // aborting the entire flush cascade.
            //
            // Theme 4 (observability): inside the same savepoint, record flush
            // timing + staged row count on success, clearing last_error only
            // when the IMV isn't already marked known_stale for some other
            // reason; on failure the EXCEPTION branch marks the IMV
            // known_stale with a repair-pointing stale_reason AND inserts a
            // durable row into public.__reflex_event_log (event = 'error')
            // carrying SQLERRM/SQLSTATE — the handler runs in the outer
            // transaction after its own subtransaction rolled back, so the
            // row survives even though the flush itself did not.
            //
            // The INSERT is wrapped in its own nested BEGIN…EXCEPTION WHEN
            // OTHERS THEN NULL — a logging side effect must never be able to
            // break the operation it observes. Without that nested handler, a
            // missing __reflex_event_log (e.g. an upgraded install whose
            // migration missed the table) would raise from INSIDE this
            // EXCEPTION branch, which is NOT caught by it: the caller would
            // see "relation does not exist" instead of the real failure, and
            // the whole cascade aborts, rolling back the known_stale UPDATE
            // two statements above — a contained per-IMV failure recording
            // LESS than before this observability existed.
            let body = imv_stmts
                .into_iter()
                .map(|s| format!("{};", as_plpgsql_stmt(&s)))
                .collect::<Vec<_>>()
                .join("\n");
            // 1.3.0 observability:
            //   * `flush_ms_history` ring buffer (size 64) collects recent flush
            //     wall times. `reflex_ivm_histogram(name)` reads it.
            //   * `application_name` is set to `reflex_flush:<view>` for the
            //     duration of this IMV's body so `pg_stat_statements` /
            //     `log_line_prefix` can correlate query rows back to the IMV.
            //
            // A5 — the success path (timing, row count, the registry UPDATE,
            // restoring application_name) is identical under `warn` and
            // `error`; only the EXCEPTION clause differs, so it is built once
            // as `success_body` and the two modes share it verbatim rather
            // than carrying two drifting copies of this SQL.
            let imv_name_esc = imv_name.replace("'", "''");
            let success_body = format!(
                "PERFORM set_config('application_name', 'reflex_flush:{imv_name_esc}', true); \
                 SELECT COUNT(*) INTO _rows FROM {delta_rel}; \
                 \n{body}\n \
                 _ms := (EXTRACT(EPOCH FROM (clock_timestamp() - _t0)) * 1000)::BIGINT; \
                 UPDATE public.__reflex_ivm_reference \
                   SET last_flush_ms = _ms, \
                       last_flush_rows = _rows, \
                       flush_count = COALESCE(flush_count, 0) + 1, \
                       last_error = CASE WHEN known_stale THEN last_error ELSE NULL END, \
                       flush_ms_history = (\
                           COALESCE(flush_ms_history, ARRAY[]::BIGINT[]) || _ms\
                       )[GREATEST(1, COALESCE(cardinality(flush_ms_history), 0) + 1 - 63):] \
                   WHERE name = '{imv_name_esc}'; \
                 PERFORM set_config('application_name', COALESCE(_prev_app, ''), true);",
                delta_rel = delta_rel,
                body = body,
                imv_name_esc = imv_name_esc,
            );
            // Under `error` there is deliberately no EXCEPTION clause at all:
            // the failure propagates and aborts the caller's transaction, so
            // the registry write and the failing IMV's own subtransaction both
            // roll back together with it. Nothing diverged, so nothing needs
            // marking — the client's exception plus the PG server log is the
            // durable trace. Under `warn` (default), the existing handler
            // catches the failure in its own subtransaction, marks the IMV
            // known_stale with a repair-pointing stale_reason, and records a
            // durable public.__reflex_event_log row — itself guarded by a
            // nested BEGIN…EXCEPTION WHEN OTHERS THEN NULL so a logging
            // failure can never mask the real error or abort the cascade.
            let exception_clause = if fail_hard {
                String::new()
            } else {
                format!(
                    "EXCEPTION WHEN OTHERS THEN \
                       PERFORM set_config('application_name', COALESCE(_prev_app, ''), true); \
                       RAISE WARNING 'pg_reflex: IMV % flush failed at cascade: % (SQLSTATE %)', \
                         '{imv_name_esc}', SQLERRM, SQLSTATE; \
                       UPDATE public.__reflex_ivm_reference \
                         SET last_error = LEFT(SQLERRM || ' (SQLSTATE ' || SQLSTATE || ')', 500), \
                             known_stale = TRUE, \
                             stale_reason = LEFT('deferred flush failed: ' || SQLERRM \
                                                 || ' (SQLSTATE ' || SQLSTATE || '). The staged ' \
                                                 || 'delta was discarded; run reflex_reconcile(' \
                                                 || '''{imv_name_esc}'') to repair.', 2000), \
                             stale_since = now(), \
                             flush_count = COALESCE(flush_count, 0) + 1 \
                         WHERE name = '{imv_name_esc}'; \
                       BEGIN \
                         INSERT INTO public.__reflex_event_log \
                           (imv_name, event, trigger_reason, detail, sqlstate) \
                           VALUES ('{imv_name_esc}', 'error', 'flush', \
                                   LEFT(SQLERRM, 2000), SQLSTATE); \
                       EXCEPTION WHEN OTHERS THEN NULL; \
                       END;",
                    imv_name_esc = imv_name_esc,
                )
            };
            let do_block = format!(
                "DO $_reflex_imv_sp$ \
                 DECLARE _t0 TIMESTAMP := clock_timestamp(); \
                         _rows BIGINT; \
                         _ms BIGINT; \
                         _prev_app TEXT := current_setting('application_name', true); \
                 BEGIN \
                   {success_body} \
                 {exception_clause} \
                 END $_reflex_imv_sp$",
                success_body = success_body,
                exception_clause = exception_clause,
            );
            client.update(&do_block, None, &[]).unwrap_or_report();

            if had_stmts {
                total_processed += 1;
            }
        }

        // 1.4.3 — DELETE (not TRUNCATE) for staging cleanup.
        //
        // TRUNCATE requires AccessExclusiveLock on the staging delta and
        // deadlocks against any concurrent session that holds a RowExclusive
        // on the same staging table from its earlier statement-level INSERT
        // and is now blocked at the COMMIT-time advisory lock. DELETE only
        // takes RowExclusive (no conflict at the table level), and MVCC
        // ensures we only remove rows visible to this transaction — i.e.
        // exactly the staged rows this flush just processed. Other
        // sessions' uncommitted staged rows remain for their own flush.
        //
        // The terminal DROP VIEW IF EXISTS calls are gone: the temp views
        // were redefined with CREATE OR REPLACE TEMP VIEW above and are
        // safe to leave for the next flush in the same session.
        client
            .update(&format!("DELETE FROM {}", delta_tbl), None, &[])
            .unwrap_or_report();
        client
            .update(
                &format!(
                    "DELETE FROM public.__reflex_deferred_pending \
                         WHERE source_table = '{}' AND operation <> 'TRUNCATE'",
                    source_table.replace("'", "''")
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    });

    format!("FLUSHED {} DEFERRED OPERATIONS", total_processed)
}
