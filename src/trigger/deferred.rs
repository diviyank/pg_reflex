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

/// Flushes all accumulated deferred deltas for a given source table.
///
/// Called by the deferred constraint trigger at COMMIT time.
/// Reads from the staging table (__reflex_delta_<source>), applies deltas
/// to each DEFERRED IMV, then cleans up staging and pending rows.
#[pg_extern]
pub fn reflex_flush_deferred(source_table: &str) -> String {
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
                        "DELETE FROM public.__reflex_deferred_pending WHERE source_table = '{}'",
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
        client
            .update(
                &build_netted_view_sql(
                    &new_view,
                    &projection,
                    &cmp_csv,
                    &delta_tbl,
                    "'I', 'U_NEW'",
                    "'D', 'U_OLD'",
                    any_text_cast,
                ),
                None,
                &[],
            )
            .unwrap_or_report();
        client
            .update(
                &build_netted_view_sql(
                    &old_view,
                    &projection,
                    &cmp_csv,
                    &delta_tbl,
                    "'D', 'U_OLD'",
                    "'I', 'U_NEW'",
                    any_text_cast,
                ),
                None,
                &[],
            )
            .unwrap_or_report();

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

        if is_spurious {
            // No IMV processing. Clean up the staging delta and pending rows.
            // DELETE (not TRUNCATE) — see end-of-function comment.
            client
                .update(&format!("DELETE FROM {}", delta_tbl), None, &[])
                .unwrap_or_report();
            client
                .update(
                    &format!(
                        "DELETE FROM public.__reflex_deferred_pending WHERE source_table = '{}'",
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
        // shape once here — the per-IMV loop full-reconciles any affected IMV
        // exactly once via the transaction-local marker below.
        let batch_has_multiple_sources = client
            .select(
                "SELECT count(DISTINCT source_table) >= 2 AS m FROM public.__reflex_deferred_pending",
                None,
                &[],
            )
            .unwrap_or_report()
            .next()
            .map(|row| row.get_by_name::<bool, _>("m").unwrap_or(None).unwrap_or(false))
            .unwrap_or(false);
        // The marker (created below) survives across the batch's per-source
        // flush calls. A later flush sees a shrunken pending set (each flush
        // deletes its own pending rows), so `batch_has_multiple_sources` may
        // already read false by then — but if the marker exists, a
        // multi-source reconcile happened earlier in this batch and this flush
        // MUST still skip the reconciled IMVs. `pg_my_temp_schema()` scopes the
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
        let engage_cross_source_guard = batch_has_multiple_sources || marker_exists;
        if engage_cross_source_guard {
            // ON COMMIT DROP: one marker per transaction, shared across the
            // per-source flush calls (the constraint trigger flushes each
            // mutated source separately), auto-removed at commit. Records the
            // IMVs already full-reconciled in this batch so a later source's
            // flush skips them — its net delta would otherwise corrupt the
            // just-reconciled state.
            client
                .update(
                    "CREATE TEMP TABLE IF NOT EXISTS __reflex_deferred_reconciled_batch \
                     (name TEXT PRIMARY KEY) ON COMMIT DROP",
                    None,
                    &[],
                )
                .unwrap_or_report();
        }

        for (imv_name, base_query, end_query, agg_json, where_pred) in &imvs {
            if engage_cross_source_guard {
                let imv_esc = imv_name.replace('\'', "''");
                let already_reconciled = client
                    .select(
                        &format!(
                            "SELECT EXISTS(SELECT 1 FROM __reflex_deferred_reconciled_batch \
                             WHERE name = '{}') AS e",
                            imv_esc
                        ),
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
                if already_reconciled {
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
                             WHERE p.source_table = ANY(r.depends_on)",
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
                    client
                        .update(
                            &format!(
                                "INSERT INTO __reflex_deferred_reconciled_batch (name) \
                                 VALUES ('{}') ON CONFLICT DO NOTHING",
                                imv_esc
                            ),
                            None,
                            &[],
                        )
                        .unwrap_or_report();
                    client
                        .update(
                            &format!("SELECT public.reflex_reconcile('{}')", imv_esc),
                            None,
                            &[],
                        )
                        .unwrap_or_report();
                    total_processed += 1;
                    continue;
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
                    delta = delta_tbl,
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
                            delta = delta_tbl,
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
            // timing + staged row count + clear last_error on success; on
            // failure the EXCEPTION branch captures SQLERRM into last_error.
            let body = imv_stmts
                .into_iter()
                .map(|s| format!("{};", s))
                .collect::<Vec<_>>()
                .join("\n");
            // 1.3.0 observability:
            //   * `flush_ms_history` ring buffer (size 64) collects recent flush
            //     wall times. `reflex_ivm_histogram(name)` reads it.
            //   * `application_name` is set to `reflex_flush:<view>` for the
            //     duration of this IMV's body so `pg_stat_statements` /
            //     `log_line_prefix` can correlate query rows back to the IMV.
            let do_block = format!(
                "DO $_reflex_imv_sp$ \
                 DECLARE _t0 TIMESTAMP := clock_timestamp(); \
                         _rows BIGINT; \
                         _ms BIGINT; \
                         _prev_app TEXT := current_setting('application_name', true); \
                 BEGIN \
                   PERFORM set_config('application_name', 'reflex_flush:{imv_name_esc}', true); \
                   SELECT COUNT(*) INTO _rows FROM {delta_tbl}; \
                   \n{body}\n \
                   _ms := (EXTRACT(EPOCH FROM (clock_timestamp() - _t0)) * 1000)::BIGINT; \
                   UPDATE public.__reflex_ivm_reference \
                     SET last_flush_ms = _ms, \
                         last_flush_rows = _rows, \
                         flush_count = COALESCE(flush_count, 0) + 1, \
                         last_error = NULL, \
                         flush_ms_history = (\
                             COALESCE(flush_ms_history, ARRAY[]::BIGINT[]) || _ms\
                         )[GREATEST(1, COALESCE(cardinality(flush_ms_history), 0) + 1 - 63):] \
                     WHERE name = '{imv_name_esc}'; \
                   PERFORM set_config('application_name', COALESCE(_prev_app, ''), true); \
                 EXCEPTION WHEN OTHERS THEN \
                   PERFORM set_config('application_name', COALESCE(_prev_app, ''), true); \
                   RAISE WARNING 'pg_reflex: IMV % flush failed at cascade: % (SQLSTATE %)', \
                     '{imv_name_esc}', SQLERRM, SQLSTATE; \
                   UPDATE public.__reflex_ivm_reference \
                     SET last_error = LEFT(SQLERRM || ' (SQLSTATE ' || SQLSTATE || ')', 500), \
                         flush_count = COALESCE(flush_count, 0) + 1 \
                     WHERE name = '{imv_name_esc}'; \
                 END $_reflex_imv_sp$",
                delta_tbl = delta_tbl,
                body = body,
                imv_name_esc = imv_name.replace("'", "''"),
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
                    "DELETE FROM public.__reflex_deferred_pending WHERE source_table = '{}'",
                    source_table.replace("'", "''")
                ),
                None,
                &[],
            )
            .unwrap_or_report();
    });

    format!("FLUSHED {} DEFERRED OPERATIONS", total_processed)
}
