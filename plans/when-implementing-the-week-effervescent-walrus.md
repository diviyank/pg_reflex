# Plan: Eliminate SIGABRT in trigger-fired MERGE + targeted Rust defense-in-depth

## Context

Week 1 of the `pg_reflex 1.2.0` sprint (`plans/in-the-last-plans-lively-octopus.md`) is blocked by a SIGABRT that aborts the PostgreSQL backend during any trigger-fired flush. Findings from two investigation sessions are in `journal_2026_04_24_sigabrt_investigation.md`.

**Why this is urgent.** `pg_reflex` is production-oriented; a crash of the Postgres backend is the worst possible failure mode — it takes down other sessions, may leave shared state inconsistent, and destroys operator trust. Per CLAUDE.md priorities: correctness > simplicity > performance.

**What we know.** The committed `a5381cb` "dev:fixes" commit introduced a trigger path that issues `MERGE INTO __reflex_intermediate_<v> USING (SELECT … FROM "__reflex_new_<src>" …) AS d ON …` via `EXECUTE _stmt` inside a PL/pgSQL row-trigger body (`src/schema_builder.rs:315-340` → the `FOREACH _stmt … EXECUTE _stmt` loop). The USING subquery references an Ephemeral Named Relation (transition table). `pgrx-tests` builds against a cassert-enabled Postgres, and the combination trips an internal `Assert()`, firing `abort()` → SIGABRT.

**What "wrap in Rust errors" can and can't do.** `abort()` from PG's C code cannot be caught by Rust — `panic = "unwind"` is already set but only catches Rust panics (pgrx converts those to PG ERRORs via its FFI boundary). The only way to prevent SIGABRT is to stop emitting the SQL pattern that trips the assertion. Rust-level hardening is still worthwhile as defense-in-depth against non-SIGABRT failure modes (malformed input, bad catalog lookups, null plan rows).

**Chosen approach.** Materialize the grouped delta into a per-IMV UNLOGGED scratch table **before** the MERGE, so the MERGE's USING clause references a plain permanent table (not a transition table). Plus a targeted audit of `.unwrap()`/`.expect()` in trigger hot paths. Existing `__reflex_staging_delta_<src>` is source-shaped and used only by the deferred path; we need a new `__reflex_delta_<view>` shaped like the intermediate.

---

## Phase A — Confirm the hypothesis (one command, no code change)

Before touching code, reproduce the crash in isolation to prove the fix will actually help and rule out a different cause (e.g. something in `replace_source_with_transition`). Run against the pgrx test cluster (`cargo pgrx start pg17` → `cargo pgrx connect pg17`):

```sql
CREATE TABLE _t (city TEXT, amount INT);
CREATE UNLOGGED TABLE _agg (city TEXT PRIMARY KEY, total INT, __ivm_count BIGINT);

CREATE OR REPLACE FUNCTION _fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE _stmt TEXT;
BEGIN
  _stmt := 'MERGE INTO _agg AS t '
        || 'USING (SELECT city, SUM(amount) AS total, COUNT(*) AS __ivm_count '
        || '       FROM "__reflex_new__t" GROUP BY city) AS d '
        || 'ON t.city = d.city '
        || 'WHEN MATCHED THEN UPDATE SET total = t.total + d.total, __ivm_count = t.__ivm_count + d.__ivm_count '
        || 'WHEN NOT MATCHED THEN INSERT (city, total, __ivm_count) VALUES (d.city, d.total, d.__ivm_count)';
  EXECUTE _stmt;
  RETURN NULL;
END $$;

CREATE TRIGGER _tr AFTER INSERT ON _t REFERENCING NEW TABLE AS "__reflex_new__t"
  FOR EACH STATEMENT EXECUTE FUNCTION _fn();

INSERT INTO _t VALUES ('east', 5);   -- expect SIGABRT if hypothesis holds
```

If the hypothesis holds, also run a **counter-example** to confirm materialization is the fix:

```sql
-- Replace the MERGE with materialize-then-MERGE and re-insert
_stmt := 'CREATE TEMP TABLE _delta ON COMMIT DROP AS '
      || 'SELECT city, SUM(amount) AS total, COUNT(*) AS __ivm_count '
      || 'FROM "__reflex_new__t" GROUP BY city; '
      || 'MERGE INTO _agg AS t USING _delta AS d ON t.city = d.city …';
```

If the counter-example **succeeds** where the direct MERGE crashes → Phase B is unblocked. If it still crashes, revisit: the issue may be `CREATE TEMP TABLE … AS … FROM "__reflex_new_…"` itself, in which case we must `CREATE TABLE` upfront and `INSERT … SELECT` at trigger time.

**Owner action**: one manual session, ≤ 10 minutes. Document the outcome in `journal_2026_04_24_sigabrt_investigation.md` as "Phase A outcome".

---

## Phase B — Fix: per-IMV UNLOGGED delta scratch table

### B.1 New helper — `delta_scratch_table_name`

**File**: `src/query_decomposer.rs` (beside `staging_delta_table_name` at line 82)

```rust
pub fn delta_scratch_table_name(view_name: &str) -> String {
    safe_identifier(&format!("__reflex_delta_{}", split_qualified_name(view_name).1))
}
```

Reuses `safe_identifier` to respect Postgres' 63-char NAMEDATALEN (bug #1 is already landed for the existing helpers).

### B.2 New DDL emitter — `build_delta_scratch_table_ddl`

**File**: `src/schema_builder.rs` (beside `build_staging_table_ddl` at line 627)

```rust
pub fn build_delta_scratch_table_ddl(
    view_name: &str,
    plan: &AggregationPlan,
    column_types: &HashMap<String, String>,
) -> Option<String>
```

The scratch table has **the same shape as the intermediate** (group-by cols + distinct cols + aggregation cols + `__ivm_count`). Reuse `build_intermediate_table_ddl`'s column-assembly logic — factor it into a private `intermediate_column_spec(plan, column_types) -> Vec<(String, String)>` returning `(col_name, pg_type)` pairs. Both ddls call it.

Key differences vs intermediate:
- `UNLOGGED` (no WAL — scratch is always TRUNCATE'd at start of trigger)
- No indexes (linear scan at MERGE time is fine; maintaining an index per trigger fire costs more than it saves)
- No primary key
- Naming: `__reflex_delta_<view>`

### B.3 Lifecycle — create + drop

- `src/create_ivm.rs` around line 1029 (where `build_staging_table_ddl` is called): after creating intermediate + target, emit `build_delta_scratch_table_ddl` output and run it via `client.update(…)`. Use the same `.unwrap_or_report()` pattern as surrounding code.
- `src/drop_ivm.rs`: extend the `DROP TABLE` loop to also drop `__reflex_delta_<view>`. Current drops target `__reflex_intermediate_<v>`, `__reflex_target_<v>`, `__reflex_affected_<v>` — add the delta scratch.

### B.4 Rewrite `reflex_build_delta_sql` in `src/trigger.rs`

**Current** (HEAD): emits `MERGE INTO intermediate USING (<delta_new>) AS d …` where `<delta_new>` reads the transition table.

**After**: emits (for each op that previously went through MERGE):

```sql
TRUNCATE "__reflex_delta_<view>";
INSERT INTO "__reflex_delta_<view>" <delta_new>;       -- reads transition table
ANALYZE "__reflex_delta_<view>";                       -- cheap; helps planner
MERGE INTO "__reflex_intermediate_<view>" AS t
  USING "__reflex_delta_<view>" AS d
  ON <on_clause> WHEN MATCHED … WHEN NOT MATCHED …;
```

Each statement is emitted into the `stmts: Vec<String>` and joined with `--<<REFLEX_SEP>>--` (unchanged separator). The plpgsql body's `FOREACH _stmt EXECUTE _stmt` loop runs them in order — **the transition table is now referenced only in a plain INSERT, never in a MERGE USING subquery.**

Key edits in `trigger.rs`:
- Lines ~640–870 (`reflex_build_delta_sql` body): every call site of `build_merge_sql(…, delta_new, …)` and `build_merge_sql(…, delta_old, …)` becomes a 3-statement sequence `TRUNCATE + INSERT delta + MERGE from scratch`. Note both the **Add path** (INSERT delta_new into scratch) and **Subtract path** (INSERT delta_old into scratch) need the scratch cycle.
- Lines ~750–815 (`end_query_has_group_by` + `grp_cols.is_some()` paths that currently call `push_merge_and_affected`): the helper needs a new signature that takes a **scratch table name** and emits `TRUNCATE scratch; INSERT scratch; MERGE FROM scratch; INSERT INTO affected SELECT DISTINCT … FROM scratch`. Rename to `push_materialized_merge_and_affected` to make intent explicit.
- `build_net_delta_query` (line 160) is unchanged — it still produces the SELECT expression; that expression is now consumed by the INSERT-into-scratch statement rather than an inline USING subquery.

### B.5 Update the 4 failing unit tests

**File**: `src/tests/unit_trigger.rs` (lines ~501–591, per journal).

Assertions currently check `sql.contains("DO $reflex_refresh$")`. Under the materialize-first approach, the DO-block gate is equivalent to "skip refresh if scratch is empty" and can be simplified to:

```sql
IF EXISTS(SELECT 1 FROM "__reflex_delta_<v>") THEN
  DELETE FROM target WHERE …; INSERT INTO target SELECT …;
END IF;
```

Keep the DO block — it still serves its purpose of gating the target rewrite. Update the **tests that referenced the affected-table path** to instead check for `__reflex_delta_<v>` presence in the generated SQL. The 5 listed tests:

- `test_build_delta_sql_emits_do_block_gate_for_group_by_imv` → stays as-is (DO block still emitted).
- `test_build_delta_sql_emits_do_block_for_end_query_group_by` → stays.
- `test_build_delta_sql_no_gate_for_sentinel_case` → stays.
- `test_build_delta_sql_do_block_includes_dead_cleanup_when_expected` → stays.
- `test_build_delta_sql_splice_injects_filter_before_group_by` → update to assert the filter is on `__reflex_delta_<v>` not `__reflex_affected_<v>` IF we decide to replace the affected-groups table with the scratch (optional — see §B.6).
- `test_build_delta_sql_splice_uses_distinct_projection_for_compound_key` → same as above.

Also add **one new** assertion per relevant test:

```rust
assert!(sql.contains("\"__reflex_delta_test_view\""), "must materialize delta");
assert!(sql.contains("TRUNCATE"), "must TRUNCATE scratch first");
assert!(!regex::Regex::new(r#"MERGE\s+INTO\s+\S+\s+AS\s+t\s+USING\s*\(SELECT"#).unwrap().is_match(&sql),
        "MERGE must never use an inline subquery");
```

The last assertion is the **regression guard** — it fails loudly if any future change reintroduces the crashing pattern.

### B.6 Optional simplification — collapse affected-groups into scratch

Today there are two ephemeral tables per IMV: `__reflex_affected_<v>` (group keys touched by delta) and now `__reflex_delta_<v>` (materialized grouped delta). The affected-groups content is a projection of the scratch: `SELECT DISTINCT <group_cols> FROM __reflex_delta_<v>`. We **can** drop the affected-table and read its rows on demand from scratch.

**Defer** this simplification. Doing it in the same PR widens scope and touches the end-query targeted filter path (`inject_affected_filter_before_group_by`). Land B.1–B.5 first; if the scratch approach proves out, file a follow-up.

---

## Phase C — Targeted Rust defense-in-depth audit

**Scope** (per user): only `src/trigger.rs`, `src/schema_builder.rs`, `src/create_ivm.rs`. Skip test helpers (`src/lib.rs:198-199` `.expect()` calls are in `assert_imv_correct` — test-only).

### What to change

Audit every `.unwrap()`, `.expect()`, and implicit-panic path on the trigger hot path (code that runs on every row/statement trigger fire). For each:

| Location | Current | Target |
|---|---|---|
| `src/trigger.rs:785` `grp_cols.as_ref().unwrap()` | panics if `grp_cols` is `None` after an `is_some()` check on a clone | replace with a `let Some(cols) = grp_cols.as_ref() else { unreachable!("…") }` — or reshape the `if let Some(cols) = grp_cols.as_ref()` so the branch is tied to the presence. Doesn't change behavior, removes the literal `unwrap()`. |
| Any `.unwrap()` inside `build_merge_sql`, `build_net_delta_query`, `reflex_build_delta_sql`, `reflex_build_truncate_sql` | each should be audited | if the invariant is real (e.g. "plan always has at least one column"), replace with `.expect("invariant X: <explanation>")` so the panic message is actionable. If the case is actually reachable (malformed input from registry), return an empty string + `pgrx::warning!` — the plpgsql body handles empty `_sql` gracefully (line 332 `IF _sql <> '' THEN`). |
| `aggregations_json` parse failure already returns `""` with a warning (`trigger.rs:440-446`) — good pattern. Apply it to any other parse/catalog-lookup path. |

### What NOT to change

- `.unwrap_or_report()` is pgrx-idiomatic — it converts SPI errors to PG ERRORs (not panics). Leave as-is.
- Test helpers (`src/lib.rs` `.expect` in `assert_imv_correct`, unit test `unwrap()`s) — out of scope.
- `drop_ivm.rs`, `create_ivm.rs` creation paths — not trigger hot path. Skip for this PR.

### Input-validation hardening at the pg_extern boundary

Add explicit validation at the start of `reflex_build_delta_sql` (line 431):

```rust
match operation {
    "INSERT" | "UPDATE" | "DELETE" => {}
    other => {
        pgrx::warning!("pg_reflex: invalid operation '{}' for view '{}'", other, view_name);
        return String::new();
    }
}
```

Same pattern as the existing JSON parse failure at line 441. Returning `""` is the trigger body's graceful no-op path.

---

## Phase D — Regression test

**New file**: `src/tests/pg_test_no_sigabrt.rs`, included from `src/lib.rs` beside the other `include!("tests/pg_test_*.rs")` lines.

One integration test that drives the exact pattern that used to crash:

```rust
#[pg_test]
fn test_trigger_fired_merge_does_not_crash_backend() {
    Spi::run(r#"
        CREATE TABLE sigabrt_src (city TEXT, amount INT);
        SELECT create_reflex_ivm(
            'sigabrt_v',
            'SELECT city, SUM(amount) AS total FROM sigabrt_src GROUP BY city',
            'DEFERRED',
            NULL, NULL
        );
    "#).unwrap();
    // Each of these historically aborted the backend.
    Spi::run("INSERT INTO sigabrt_src VALUES ('east', 5)").expect("insert");
    Spi::run("UPDATE sigabrt_src SET amount = 10 WHERE city = 'east'").expect("update");
    Spi::run("UPDATE sigabrt_src SET city = 'north' WHERE city = 'east'").expect("group change");
    Spi::run("DELETE FROM sigabrt_src WHERE city = 'north'").expect("delete");
    // Oracle check: IMV matches a fresh aggregation.
    assert_imv_correct("sigabrt_v", "SELECT city, SUM(amount) AS total FROM sigabrt_src GROUP BY city");
}
```

This test crashes today (per journal). After the fix, it passes. If the test ever crashes in the future, CI stops the release.

Also keep the 6 targeted-refresh tests from `src/tests/pg_test_correctness.rs` — they become green after the fix and exercise broader surface.

---

## Files touched (summary)

| File | Change |
|---|---|
| `src/query_decomposer.rs` | new `delta_scratch_table_name(view)` helper (§B.1) |
| `src/schema_builder.rs` | new `build_delta_scratch_table_ddl` + factor out `intermediate_column_spec` (§B.2); ~5 LOC |
| `src/create_ivm.rs` | create scratch table after intermediate (§B.3); ~3 LOC |
| `src/drop_ivm.rs` | drop scratch in the drop loop (§B.3); ~2 LOC |
| `src/trigger.rs` | rewrite `push_merge_and_affected` → `push_materialized_merge_and_affected`; update every call site in `reflex_build_delta_sql` to go through TRUNCATE+INSERT+MERGE (§B.4); audit `.unwrap()` hot-path calls (§C); add input validation at `reflex_build_delta_sql` entry (§C) |
| `src/tests/unit_trigger.rs` | update 4–6 affected tests; add regression guard for inline-USING-subquery (§B.5) |
| `src/tests/pg_test_no_sigabrt.rs` (new) | one end-to-end test covering the 4 crashing operations (§D) |
| `src/lib.rs` | `include!("tests/pg_test_no_sigabrt.rs")` |
| `journal_2026_04_24_sigabrt_investigation.md` | append "Resolution" section once Phase A confirms + fix lands |

No changes to `Cargo.toml`, `.control`, migration SQL — this is a bugfix, not a version bump. Roll this into the 1.1.4 point release or fold into 1.2.0 depending on release schedule.

---

## Verification

Run in order:

1. `cargo fmt && cargo clippy --all-targets --no-deps -- -D warnings` — no new warnings.
2. `cargo pgrx test --features pg17` — **primary gate**: must be green including the new `test_trigger_fired_merge_does_not_crash_backend` and all 6 tests in `pg_test_correctness.rs` that currently crash.
3. `cargo pgrx test --features pg15 --features pg16 --features pg18` — extension still builds and tests pass on every supported PG major.
4. Manual smoke in `cargo pgrx connect pg17`: the Phase A reproducer — but with the fix applied, run it end-to-end through `create_reflex_ivm` + INSERT/UPDATE/DELETE. No SIGABRT, `SELECT * FROM __reflex_target_sigabrt_v` matches oracle.
5. Run the consolidated benchmark driver (`/tmp/pg_reflex_bench_rerun/run_flush.sh` per `memory/reference_benchmark_data.md`) on a subset of the 2026-04-22 IMVs. Expect ≤ 5 % regression from the extra TRUNCATE+INSERT step — if it's worse than 10 %, investigate (but don't block the fix; correctness first).

---

## Out of scope

- Collapsing `__reflex_affected_<v>` into the scratch table (§B.6).
- Restoring MERGE with transition tables once PG fixes the underlying assertion (would require a feature-detect + dual path). No evidence the PG-side fix is planned.
- Per-IMV SAVEPOINT wrapping of the flush cascade (tracked separately as Theme 3.4 of the 1.2.0 plan).
- Full-crate `.unwrap()` audit — `create_ivm.rs` / `drop_ivm.rs` still use `.unwrap_or_report()` but also have a couple of `.unwrap()` calls; those are setup-path-only, and their blast radius is bounded to the transaction that called `create_reflex_ivm`. Revisit for 1.3.0.

---

## Implementation status (2026-04-24)

### What was done (Phase B–D implemented)

Phase B was fully implemented:
- `delta_scratch_table_name` added to `query_decomposer.rs`
- `build_delta_scratch_table_ddl` + `intermediate_column_spec` added to `schema_builder.rs`
- Scratch table created in `create_ivm.rs`, dropped in `drop_ivm.rs`
- `reflex_build_delta_sql` rewritten: all MERGE paths now go through `push_materialized_merge` / `push_materialized_merge_and_affected` (TRUNCATE scratch → INSERT INTO scratch FROM transition_table → MERGE FROM scratch)
- Unit tests in `unit_trigger.rs` updated to reflect new statement structure
- `pg_test_no_sigabrt.rs` regression test added and included in `lib.rs`

Phase C (`.unwrap()` audit) and Phase D regression test were done.

### Current state

`test_trigger_fired_merge_does_not_crash_backend` **PASSES** — the simple single-IMV aggregate-GROUP-BY case no longer crashes.

`test_chain_aggregate_then_passthrough` **still SIGABRT-crashes** — this is a chained IMV scenario that was **not covered by the original plan**:
- `catp_src` → L1 `catp_l1` (aggregate, GROUP BY city) → L2 `catp_l2` (passthrough of L1)
- When `catp_src` is modified, L1's trigger fires. L1 uses the scratch table for MERGE — that part is safe.
- L1's trigger then modifies `catp_l1` via DELETE + INSERT (lines 935–936 of `reflex_build_delta_sql`). These modifications fire L2's trigger.
- L2 is a passthrough; its trigger does NOT use MERGE. Its generated SQL references transition tables inside EXECUTE dynamic SQL:
  - For DELETE: `DELETE FROM "catp_l2" WHERE ROW(...) IN (SELECT ... FROM "__reflex_old_catp_l1")`
  - For INSERT: `INSERT INTO "catp_l2" SELECT ... FROM "__reflex_new_catp_l1"`
- Despite these not being MERGE statements, a SIGABRT still occurs in the nested trigger execution context.

### Root cause hypothesis for chained case

The `Assert(!IsEphemeralRelation(sourceDesc))` in PostgreSQL's `nodeMerge.c` was the documented assertion for the MERGE case. The chained case doesn't use MERGE in L2, so a **different assertion** must be firing.

Two candidate hypotheses not yet confirmed:
1. **Nested trigger + EXECUTE + ephemeral relation**: When L2's trigger fires inside L1's trigger (nested execution), and L2's trigger body uses `EXECUTE` to run a statement that references an ephemeral transition table (`__reflex_old_catp_l1` or `__reflex_new_catp_l1`), PostgreSQL may hit an assertion about accessing ephemeral relations from a dynamically-executed statement inside a nested trigger context.
2. **Assertion in subquery executor path**: The `DELETE FROM … WHERE ROW(…) IN (SELECT … FROM ephemeral)` pattern executed via EXECUTE may hit an assertion in the tuple-comparison executor path that checks for ephemeral sources.

### What needs to be done next

The plan as written only addressed `MERGE USING (inline_subquery_reading_transition_table)`. The chained case reveals that **any dynamic SQL executed via `EXECUTE` inside a trigger body that references a transition table by name can crash in cassert builds**.

The fix must be extended to cover passthrough triggers too. Two approaches:

**Option 1 — Avoid transition tables in passthrough EXECUTE statements entirely.**
For the passthrough DELETE/INSERT path, instead of referencing `__reflex_old_catp_l1` / `__reflex_new_catp_l1` inside EXECUTE, do a full DELETE + INSERT from the base query (which reads the already-updated source table). This is already the fallback path (`mappings = None` branch, lines 689–692). The cost is a full scan of the source instead of a targeted delete — acceptable given correctness is paramount.

Implementation: in `reflex_build_delta_sql`, for `is_passthrough`, drop the `passthrough_key_mappings` branch entirely and always use the full DELETE + INSERT from `base_query`. This eliminates all transition-table references in passthrough EXECUTE statements.

**Option 2 — Materialize passthrough delta into a scratch table too.**
For passthrough INSERT, materialize the new rows into a scratch table first (like we do for aggregates). For passthrough DELETE, similar materialization. This preserves incrementality but requires a scratch table for passthrough IMVs as well.

Option 1 is simpler and safer. The passthrough case is typically reading from an aggregate IMV (a small table), so a full scan is cheap. Recommend Option 1.

### Updated file scope for next session

In `src/trigger.rs`, inside `reflex_build_delta_sql`, the `else if plan.is_passthrough` branch (line 664): change the DELETE and UPDATE paths so they never reference `old_tbl` / `new_tbl` inside the generated EXECUTE strings. Replace with full DELETE + INSERT from `base_query`. The INSERT path can similarly be simplified to DELETE + INSERT from base_query (or kept as-is if direct INSERT is safe — needs verification).
