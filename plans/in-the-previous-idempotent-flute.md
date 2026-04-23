# One-day sprint — pg_reflex quick wins (#3, #9, #11, #12)

## Context

The 2026-04-22 `db_clone` bench rerun on pg_reflex 1.1.2 (journal `journal/2026-04-22_optimization_ideas.md`) surfaced four one-hour quick wins: **#3** (skip target refresh when affected-groups is empty), **#9** (mark `reflex_build_delta_sql` PARALLEL SAFE), **#11** (ANALYZE staging delta inside flush), **#12** (advisory lock after the `where_predicate` filter). Goal: ship all four in one day as pg_reflex **1.1.3**. Stretch goals #5 and #1 stayed on the shelf.

Post-exploration adjustments to the journal's recipe:

- **#12 is partially already done.** The INSERT/DELETE trigger bodies (`schema_builder.rs:315-340` immediate, `schema_builder.rs:433-468` deferred) already check `where_predicate` before `pg_advisory_xact_lock`. Two *genuine* #12 gaps remain: (a) the deferred-mode UPDATE body (`schema_builder.rs:505-537`) omits the predicate check entirely, (b) `reflex_flush_deferred` (`trigger.rs:947-1169`) never loads `where_predicate` from the registry. Both are perf bugs (no correctness impact — missing filter means doing full delta work that nets to zero), and both land cleanly in this sprint. The TRUNCATE bodies (`schema_builder.rs:387-406`, `:549-570`) are N/A — TRUNCATE has no row-level filter.
- **#5** deferred: the `end_query_has_group_by` full-rebuild branch (`trigger.rs:872-876`) exists because the intermediate's grouping key can be finer than the target's output grouping. Converting it to targeted refresh needs a design pass.
- **#1** deferred: intermediate table ALTER + backfill + aggregations_json rewrite migration is real work.

## Files touched

| # | File | Location |
|---|---|---|
| #3 | `src/trigger.rs` | `:862-894` (replace the `else if let Some(ref cols) = grp_cols` branch) |
| #9 | `src/trigger.rs` | `:446` (attribute on `reflex_build_delta_sql`), `:902` (attribute on `reflex_build_truncate_sql`) |
| #11 | `src/trigger.rs` | insert after `:1014`, before the OLD temp view at `:1035` |
| #12a | `src/schema_builder.rs` | rewrite `:505-537` (deferred UPDATE body) |
| #12b | `src/trigger.rs` | `:947-1169` — add `where_predicate` to SELECT at `:961`, to the tuple at `:970-991`, and to the loop body at `:1050-1145` |
| migration | `sql/pg_reflex--1.1.2--1.1.3.sql` | new |
| version | `Cargo.toml` | `version = "1.1.3"` |

Tests: new cases in `src/tests/unit_trigger.rs` (SQL shape) and `src/tests/pg_test_deferred.rs` (behavior).

---

## #9 — PARALLEL SAFE

`src/trigger.rs:446`:
```rust
#[pg_extern(parallel_safe)]
pub fn reflex_build_delta_sql(
```
and `src/trigger.rs:902`:
```rust
#[pg_extern(parallel_safe)]
pub fn reflex_build_truncate_sql(view_name: &str) -> String {
```

Audit checklist (both functions): no SPI writes; only argument parsing + SQL-string construction + `pgrx::warning!` for invalid JSON (warning is parallel-safe). Leave `reflex_flush_deferred` at `:947` as-is — it executes SPI mutations.

Existing installations keep a `PARALLEL UNSAFE` row in `pg_proc` until the migration does `ALTER FUNCTION`. Fresh installs pick up the new annotation via pgrx-generated DDL.

**Expected runtime benefit:** ~0 on today's bench (plpgsql trigger loops aren't auto-parallelized by PG). The value is keeping the door open for #4-style breadth-first flush later.

---

## #11 — ANALYZE staging delta at flush start

Problem: `TRUNCATE` resets `pg_class.reltuples` to 0. The AFTER-statement triggers that insert into `__reflex_delta_<src>` don't update stats. At the next flush, `reflex_build_delta_sql`-generated queries read from the delta with zero-row estimates → nested-loop plans on large cascades.

Fix: one line inside `reflex_flush_deferred`, after the `has_rows` branch returns early on empty delta and before any query reads from the delta. Around `trigger.rs:1014-1016`, just before the `DROP VIEW / CREATE TEMP VIEW` block at `:1035-1048`, add:

```rust
client
    .update(&format!("ANALYZE {}", delta_tbl), None, &[])
    .unwrap_or_report();
```

ANALYZE on an UNLOGGED table with a few thousand rows is sub-millisecond; on larger stages it pays for itself many times over in the query plans that follow. No need to analyze after the trailing `TRUNCATE` at `:1154` — the next flush's ANALYZE is what matters, and analyzing an empty table to tell the planner "zero rows" is the opposite of useful.

---

## #12 — Missing `where_predicate` checks in two paths

### #12a — Deferred UPDATE trigger body (`schema_builder.rs:505-537`)

Current body declares no `_pred_match`, selects no `where_predicate`, and calls `reflex_build_delta_sql` unconditionally for IMMEDIATE-mode IMVs. Contrast the deferred INSERT/DELETE body (`:433-468`), which does both.

Replacement body (follow the `:433-468` template):

```rust
let upd_body = format!(
    "DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_deferred BOOLEAN := FALSE; _has_rows BOOLEAN; _pred_match BOOLEAN; \
     BEGIN \
       SELECT EXISTS(SELECT 1 FROM \"{ref_new}\" LIMIT 1) INTO _has_rows; \
       IF NOT _has_rows THEN RETURN NULL; END IF; \
       FOR _rec IN \
         SELECT name, base_query, end_query, aggregations::text AS aggregations, \
                COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, where_predicate \
         FROM public.__reflex_ivm_reference \
         WHERE '{source_table}' = ANY(depends_on) AND enabled = TRUE \
         ORDER BY graph_depth \
       LOOP \
         IF _rec.where_predicate IS NOT NULL THEN \
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '{ref_new}', _rec.where_predicate) INTO _pred_match; \
           IF NOT _pred_match THEN CONTINUE; END IF; \
         END IF; \
         IF _rec.refresh_mode = 'IMMEDIATE' THEN \
           PERFORM pg_advisory_xact_lock(hashtext(_rec.name)); \
           _sql := reflex_build_delta_sql(_rec.name, '{source_table}', 'UPDATE', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query); \
           IF _sql <> '' THEN \
             FOREACH _stmt IN ARRAY string_to_array(_sql, E'\\n--<<REFLEX_SEP>>--\\n') LOOP \
               IF _stmt <> '' THEN EXECUTE _stmt; END IF; \
             END LOOP; \
           END IF; \
         ELSE \
           _has_deferred := TRUE; \
         END IF; \
       END LOOP; \
       IF _has_deferred THEN \
         INSERT INTO {delta_tbl} SELECT 'U_OLD', * FROM \"{ref_old}\"; \
         INSERT INTO {delta_tbl} SELECT 'U_NEW', * FROM \"{ref_new}\"; \
         INSERT INTO public.__reflex_deferred_pending (source_table, operation) \
           VALUES ('{source_table}', 'UPDATE'); \
       END IF; \
       RETURN NULL; \
     END;"
);
```

The predicate runs against the NEW transition (`ref_new`). Deferred IMVs still get staged via `_has_deferred := TRUE`, regardless of whether the predicate would match for any single deferred IMV — the staging is all-or-nothing per transition (one transition feeds all deferred IMVs), and the predicate filter at flush time (12b below) decides per-IMV.

Predicate check must come before the advisory lock (the whole point of #12). The template above has that order.

### #12b — `reflex_flush_deferred` registry query (`trigger.rs:947-1169`)

Current SELECT at `:961-965`:
```sql
SELECT name, base_query, end_query, aggregations::text AS aggregations
FROM public.__reflex_ivm_reference
WHERE $1 = ANY(depends_on) AND enabled = TRUE
  AND COALESCE(refresh_mode, 'IMMEDIATE') = 'DEFERRED'
ORDER BY graph_depth
```

Change to `SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate` and widen the tuple in `imvs: Vec<(String, String, String, String)>` at `:952` to `Vec<(String, String, String, String, Option<String>)>`. Update the row-extraction closure at `:970-991` to include `where_predicate` (nullable). In the loop at `:1050-1145`, before `PERFORM pg_advisory_xact_lock` at `:1052-1061`, add:

```rust
if let Some(pred) = &where_predicate {
    let has_match = client
        .select(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {} WHERE {} LIMIT 1) AS m",
                delta_tbl, pred
            ),
            None,
            &[],
        )
        .unwrap_or_report()
        .next()
        .and_then(|r| r.get_by_name::<bool, _>("m").unwrap_or(None))
        .unwrap_or(false);
    if !has_match {
        continue;
    }
}
```

The predicate is evaluated against the staging delta table (same schema as source, plus `__reflex_op`), so existing WHERE expressions generated by `create_ivm` at `:1108-1119` still parse. If the flush-time EXISTS returns false, skip the entire IMV — no advisory lock, no INSERT/DELETE/UPDATE delta round-trip.

Run the same check once per IMV, not once per operation. Placing it at the top of the outer IMV loop covers INSERT, DELETE, and UPDATE branches.

**Bench note:** bench drivers that exercise many IMVs on a shared source (sop/supply schema) are the primary beneficiary — today every IMV's advisory lock fires even if its predicate prunes the delta to empty.

---

## #3 — Skip target refresh when affected-groups table is empty

Problem: `trigger.rs:877-888` emits unconditional DELETE+INSERT on the target, gated per-row by `EXISTS (SELECT 1 FROM "__reflex_affected_<view>" AS __a WHERE ...)`. When the MERGE RETURNING clause produces zero affected groups (net-zero delta — e.g., UPDATE that touches a column irrelevant to all aggregates), the target still gets a full scan for the DELETE and a full end_query evaluation for the INSERT. The planner *might* short-circuit on the EXISTS, but that depends on accurate stats (see #11).

Fix: wrap the two-or-three target-refresh statements in one `DO` block gated by a cheap `EXISTS` on the affected table. Replace `trigger.rs:877-888`:

```rust
} else if let Some(ref cols) = grp_cols {
    let qv = quote_identifier(view_name);
    let ns_in = null_safe_in(&affected_tbl, cols);
    let inner = if include_dead_cleanup {
        format!(
            "DELETE FROM {int} WHERE __ivm_count <= 0; \
             DELETE FROM {qv} WHERE {ns}; \
             INSERT INTO {qv} {eq} AND {ns};",
            int = intermediate_tbl, qv = qv, ns = ns_in, eq = end_query
        )
    } else {
        format!(
            "DELETE FROM {qv} WHERE {ns}; \
             INSERT INTO {qv} {eq} AND {ns};",
            qv = qv, ns = ns_in, eq = end_query
        )
    };
    stmts.push(format!(
        "DO $reflex_refresh$ BEGIN \
           IF EXISTS(SELECT 1 FROM \"{aff}\") THEN {body} END IF; \
         END $reflex_refresh$",
        aff = affected_tbl, body = inner
    ));
    stmts.push(metadata_sql);
}
```

Leave `trigger.rs:872-876` (the `end_query_has_group_by` full-rebuild branch) alone — that's #5 territory. Leave `trigger.rs:889-893` (sentinel, no affected table) alone — nothing to gate.

**Dollar-quoting:** grep the emitters of `end_query` and `ns_in` to confirm none use the `$reflex_refresh$` tag. Current code uses single-quoted strings and `IS NOT DISTINCT FROM`, so there's no collision. If a future helper introduces tagged dollar quotes, bump the tag.

**Why `DO` and not a plpgsql-level `IF` in `schema_builder.rs`:** keeping the gate inside `reflex_build_delta_sql`'s output means `schema_builder.rs` templates don't change — every caller of `reflex_build_delta_sql` (immediate triggers, deferred flush, truncate handler) gets the optimization for free. The `DO` block's plpgsql compile cost is paid once per query plan and cached.

**Metadata row stays outside the gate.** `last_update_date` bumps even on no-op flushes so the "last touched" bookkeeping tracks every fire, not just the effectful ones.

---

## Migration — `sql/pg_reflex--1.1.2--1.1.3.sql`

```sql
-- Migration: pg_reflex 1.1.2 -> 1.1.3
--
-- #9: mark reflex_build_delta_sql / reflex_build_truncate_sql PARALLEL SAFE.
-- #12a: replace the deferred-UPDATE trigger bodies in pg_proc to add the
--       missing where_predicate check (mirror of deferred INSERT/DELETE body).
-- #3 and #11 and #12b require no catalog patching — they are code-gen
-- (#3) or runtime (#11, #12b) changes that take effect after the shared
-- library is reloaded.

ALTER FUNCTION reflex_build_delta_sql(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT)
  PARALLEL SAFE;

ALTER FUNCTION reflex_build_truncate_sql(TEXT)
  PARALLEL SAFE;

-- #12a: patch every pre-existing deferred-UPDATE trigger body so it checks
-- where_predicate. Detect the old shape by two markers:
--   - proname matches '__reflex_upd_trigger_on_%'
--   - prosrc does NOT already contain 'where_predicate'
-- For each match, rebuild the body from schema_builder's current template by
-- re-running CREATE OR REPLACE via a per-source DO loop. The simplest path
-- here is to regenerate from build_deferred_trigger_ddls — but SQL-only
-- migrations can't call Rust code, so use a regex-patch approach like
-- 1.1.1->1.1.2 did:

DO $migration$
DECLARE
    _proc RECORD;
    _new_src TEXT;
    _patched INT := 0;
BEGIN
    FOR _proc IN
        SELECT oid, proname, prosrc
        FROM pg_proc
        WHERE proname LIKE '\_\_reflex\_upd\_trigger\_on\_%' ESCAPE '\'
          AND prosrc NOT LIKE '%where_predicate%'
          AND prosrc LIKE '%reflex_build_delta_sql%'
    LOOP
        -- Inject `, where_predicate` into the SELECT column list and a
        -- predicate gate before `IF _rec.refresh_mode = 'IMMEDIATE' THEN`.
        _new_src := regexp_replace(
            _proc.prosrc,
            '(COALESCE\(refresh_mode, ''IMMEDIATE''\) AS refresh_mode)',
            '\1, where_predicate',
            'g'
        );
        _new_src := regexp_replace(
            _new_src,
            '(IF _rec\.refresh_mode = ''IMMEDIATE'' THEN)',
            'IF _rec.where_predicate IS NOT NULL THEN ' ||
              'EXECUTE format(''SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)'', ' ||
                'TG_TABLE_NAME, _rec.where_predicate) INTO _pred_match; ' ||
              'IF NOT _pred_match THEN CONTINUE; END IF; ' ||
            'END IF; \1',
            'g'
        );
        -- Add _pred_match to DECLARE if missing
        IF _new_src NOT LIKE '%_pred_match%' THEN
            _new_src := regexp_replace(
                _new_src,
                '(DECLARE [^;]*?)(BEGIN)',
                '\1 _pred_match BOOLEAN; \2',
                'g'
            );
        END IF;
        IF _new_src IS DISTINCT FROM _proc.prosrc THEN
            UPDATE pg_proc SET prosrc = _new_src WHERE oid = _proc.oid;
            _patched := _patched + 1;
        END IF;
    END LOOP;
    RAISE NOTICE 'pg_reflex 1.1.2 -> 1.1.3: patched % deferred-UPDATE trigger bodies', _patched;
END;
$migration$;
```

**Important caveat on the regex patch:** the 1.1.1→1.1.2 migration used the same pattern and worked. If the regex ends up fragile across user schemas, fall back to a plain C-level `pgrx_sql_from_file` that rebuilds trigger DDLs by calling `build_deferred_trigger_ddls(source_table)` for each source in `__reflex_ivm_reference`. That needs one extension-level function added (e.g., `reflex_rebuild_triggers()`) and a final call inside the migration. Keep the regex-only approach as the default; if QA flags it, switch.

Also update `Cargo.toml`:
```toml
version = "1.1.3"
```
The `.control` file uses `@CARGO_VERSION@`, so nothing else to bump.

---

## Tests (write first; don't modify after — per CLAUDE.md)

### Unit tests — `src/tests/unit_trigger.rs`

1. `test_build_delta_sql_emits_do_block_gate_for_group_by_imv` — SUM+GROUP BY plan, `DELETE` op; assert output contains `DO $reflex_refresh$` and that the inner body has `EXISTS(SELECT 1 FROM "__reflex_affected_…")`.
2. `test_build_delta_sql_no_gate_for_end_query_group_by` — same plan but `end_query` contains `GROUP BY`; assert *no* `DO $reflex_refresh$` block (full-rebuild branch untouched).
3. `test_build_delta_sql_no_gate_for_sentinel_case` — no group columns; assert no DO block.
4. `test_build_delta_sql_do_block_includes_dead_cleanup_when_expected` — plan with `needs_ivm_count = true` + DELETE op; assert the `DELETE FROM intermediate WHERE __ivm_count <= 0` line is *inside* the DO block (not outside).

### Unit tests — `src/tests/unit_schema_builder.rs`

5. `test_deferred_upd_body_contains_where_predicate_check` — call `build_deferred_trigger_ddls("t")`, grab the UPDATE DDL (index 2), assert substring `_rec.where_predicate IS NOT NULL` and that `pg_advisory_xact_lock` appears *after* that check.
6. `test_deferred_upd_body_declares_pred_match` — same DDL; assert `_pred_match BOOLEAN` in the DECLARE.

### Integration tests — `src/tests/pg_test_deferred.rs`

7. `test_flush_is_noop_when_affected_empty` — seed grouped IMV (SUM on `(grp, val)`); run an UPDATE that changes only a non-aggregated column to the same value (net delta 0); flush; assert IMV matches a fresh re-execution. Secondary assertion: `pg_stat_user_tables` shows no `n_tup_ins` on the target since the last flush (or equivalent — `xact_commit` check). If that's too flaky in pgrx tests, drop the stats assertion and rely on the bench for perf evidence.
8. `test_flush_correct_after_empty_delta_gate_sequence` — INSERT → flush → verify; DELETE → flush → verify; INSERT-back → flush → verify. Exercises the gate across add/subtract/re-add.
9. `test_deferred_upd_respects_where_predicate` — create IMV with a `WHERE status = 'active'` clause (so `create_ivm` stores a predicate); UPDATE rows that are all `status = 'inactive'`; flush; assert the IMV is unchanged and (stretch) that no advisory lock was acquired. Parallel to the existing `test_deferred_bool_or_with_join_alias_recompute` test at `pg_test_deferred.rs:471`.
10. `test_flush_deferred_skips_imv_on_predicate_miss` — two IMVs on the same source, one with a predicate that matches everything, one that matches nothing. INSERT rows into source. Flush. Assert both IMVs correct and that the "matches nothing" IMV's `__ivm_count` didn't budge (no rows added, no rows subtracted).

### Migration smoke (bash)

11. Install 1.1.2 in a scratch DB; create two IMVs (one aggregate, one passthrough) on a source; `ALTER EXTENSION pg_reflex UPDATE TO '1.1.3'`; verify (a) `SELECT parallel FROM pg_proc WHERE proname = 'reflex_build_delta_sql'` is `'s'`, (b) at least one `__reflex_upd_trigger_on_*` function in `pg_proc` now contains `where_predicate`, (c) the aggregate IMV still updates correctly after a subsequent INSERT+flush.

---

## Implementation order

The changes are independent in scope — pick any order — but this sequence minimizes rebuild churn:

1. Write all unit + integration tests (11 total). Confirm they fail on 1.1.2 (expected: #3 tests fail on shape, #12 tests fail on the predicate-miss case, #9/#11 have no direct tests and rely on the bench).
2. Implement #9 (one-line attribute, two functions) + the `ALTER FUNCTION` stanza in the migration.
3. Implement #11 (one line in `reflex_flush_deferred`).
4. Implement #12a (rewrite `upd_body` in `schema_builder.rs`) + migration regex patch.
5. Implement #12b (widen registry tuple + predicate EXISTS check in `reflex_flush_deferred`).
6. Implement #3 (DO-block gate in `reflex_build_delta_sql`).
7. `cargo fmt && cargo clippy --all-targets --no-deps -- -D warnings`.
8. `cargo pgrx test` — expect all tests green.
9. Fresh install smoke: `DROP EXTENSION pg_reflex; CREATE EXTENSION pg_reflex;` in a scratch DB, run the smallest e2e scenario.
10. Upgrade smoke: install 1.1.2 first, create a mix of aggregate / passthrough / where-predicate IMVs, then `ALTER EXTENSION pg_reflex UPDATE TO '1.1.3'`. Confirm the migration notice reports a non-zero patched count for #12a and that all IMVs still update correctly.
11. **Benchmark**: rerun the driver at `/tmp/pg_reflex_bench_rerun/` on `db_clone` (see `memory/reference_benchmark_data.md`). Compare against the 1.1.2 numbers in `journal/2026-04-22_db_clone_benchmark_rerun.md`.

## Evaluation — per CLAUDE.md

Record results in a new `journal/2026-04-23_quick_wins_113.md`. For each of #3, #9, #11, #12, note wins/losses on the three clean wins (`sop_purchase_baseline_reflex` 2.35×, `sop_purchase_reflex` 3.75×, `forecast_stock_chart_weekly_reflex` 145×) and on the current disasters (`sop_last_forecast_reflex` 849× slower, sales_simulation cascade 3–6 min). If a change shows < 1 % impact anywhere it was supposed to help, consider reverting it pre-release — adding complexity without measurable gain fails the CLAUDE.md "worth the hassle" bar.

## Out of scope

- **#5** (end-query incremental UPDATE) — needs a design pass on intermediate-vs-output grouping mismatch.
- **#1** (BOOL_OR counter column) — requires intermediate DDL migration + backfill.
- The deferred-mode UPDATE trigger also misses the row-level `where_predicate` filter applied at *staging* time (we only fix the immediate-branch check inside the UPDATE body and the flush-time check in 12b). A full-filter staging path would require filtering the `INSERT INTO __reflex_delta_<src> SELECT 'U_OLD'…` lines per-IMV, which fights the "one delta table shared by N IMVs" design. Deferred.
