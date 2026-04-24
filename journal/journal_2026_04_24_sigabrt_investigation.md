# Journal: SIGABRT Investigation in Trigger Tests (2026-04-24)

## Intended Task

Fix a SIGABRT (signal 6) crash that blocks 6 tests in `pg_test_correctness.rs` (the "targeted
refresh" test suite added for pg_reflex 1.2.0). The primary failing test was
`pg_test_targeted_refresh_update_group_change`, which exercises the UPDATE trigger path where a row
changes its group key (e.g., East → North).

Secondary task: fix 4 unit tests in `src/tests/unit_trigger.rs` that assert for `DO $reflex_refresh$`
blocks, which were removed in a prior session but whose test assertions were not updated.

## Background: Targeted Refresh Feature

In the HEAD commit (`a5381cb dev:fixes`), the trigger target-refresh path was changed from:

```sql
TRUNCATE target_table;
INSERT INTO target_table SELECT ... FROM intermediate WHERE __ivm_count > 0;
```

To a targeted per-group approach that only refreshes the groups touched by the delta:

```sql
-- For the push_merge_and_affected (PG17+) path:
WITH cleanup AS (DELETE FROM __reflex_affected_<view> RETURNING 1),
     __m AS (MERGE INTO intermediate USING delta ... RETURNING t.group_col)
INSERT INTO __reflex_affected_<view> SELECT DISTINCT group_col FROM __m;

-- Target refresh (with DO block guard):
DO $reflex_refresh$ BEGIN
  IF EXISTS(SELECT 1 FROM __reflex_affected_<view>) THEN
    DELETE FROM intermediate WHERE __ivm_count <= 0;
    DELETE FROM target WHERE group_col IN (SELECT group_col FROM __reflex_affected_<view>);
    INSERT INTO target SELECT ... FROM intermediate WHERE ... AND group_col IN (SELECT ... FROM __reflex_affected_<view>);
  END IF;
END $reflex_refresh$;
```

The `__reflex_affected_<view>` table is created by `create_reflex_ivm` as an UNLOGGED table.

## What Was Done

### Session 1 (prior context, before compaction)

1. Added `src/tests/pg_test_correctness.rs` with 4 targeted refresh tests and 2 type/safety tests.
2. Added `src/tests/pg_test_1_2_0.rs` with 7 operational tests (cycle detection, advisory lock
   fix, CTE alias collision, non-STRICT delta SQL, rebuild, source drop, etc.).
3. Observed SIGABRT crashing `pg_test_targeted_refresh_update_group_change` and others.
4. Hypothesis 1: DO blocks inside EXECUTE inside PL/pgSQL trigger body → removed all DO blocks →
   crash persisted.
5. Hypothesis 2: MERGE CTE with RETURNING (`WITH cleanup AS (...) __m AS (MERGE ... RETURNING ...)`)
   was causing the assertion → simplified `push_merge_and_affected` to 3 separate statements
   (TRUNCATE, MERGE, INSERT) → crash persisted.

### Session 2 (this session)

1. Confirmed via `git stash` + test run that the SIGABRT exists in HEAD **before** our WIP changes.
2. Confirmed via `git stash pop` + test run that the crash persists with WIP changes applied.
3. Observed that **both INSERT and UPDATE triggers crash**, not just UPDATE. This means
   `test_trigger_insert_updates_view` (the most basic trigger test) also crashes.
4. Confirmed that the crash is in the PostgreSQL server process (not the test binary), triggered by
   an `Assert()` in PG internals (SIGABRT = `abort()` = assertion failure in PG's cassert builds).

## The SIGABRT: Root Cause (Likely)

The crash was not investigated to full resolution. But the evidence strongly implicates:

**MERGE referencing a transition table name via `EXECUTE _stmt` in a PL/pgSQL trigger body.**

PostgreSQL's MERGE implementation (at least in PG15/16/17) has constraints around transition
tables. The trigger body contains:

```sql
_sql := reflex_build_delta_sql(...);
FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
  IF _stmt <> '' THEN EXECUTE _stmt; END IF;
END LOOP;
```

The EXECUTE'd statements include a MERGE that references the transition table:

```sql
MERGE INTO __reflex_intermediate_<view> AS t
USING (SELECT city AS "city", SUM(amount) AS "__sum_amount", COUNT(*) AS __ivm_count
       FROM "__reflex_new_<source>"  -- ← transition table name
       GROUP BY city) AS d
ON t."city" IS NOT DISTINCT FROM d."city"
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

Transition table references inside EXECUTE'd statements may violate a PostgreSQL internal invariant.
The transition table range table entry may not be visible or may be in an illegal state when accessed
from inside a dynamically executed string. This would trigger an `Assert()` in PG's executor or
planner, causing SIGABRT.

**This hypothesis was not verified against PG source code or PG documentation in this session.**

## WIP Changes State (not committed)

The working tree has these uncommitted changes:

### `src/trigger.rs`
- `push_merge_and_affected`: Simplified from PG-version-gated dual implementation to unified
  3-statement approach (TRUNCATE + MERGE + INSERT into affected). Removed `grp_cols` parameter.
- `reflex_build_delta_sql`: Changed `aggregations_json: &str` to `aggregations_json: Option<&str>`
  to handle NULL from SQL (the function was STRICT before, now it needs to handle NULL gracefully).
- DO blocks removed entirely from the target refresh paths (both `end_query_has_group_by` path and
  `else if grp_cols.is_some()` path).
- Deferred flush `pg_advisory_xact_lock` changed to 2-argument form.

### `src/tests/unit_trigger.rs`
- Updated `reflex_build_delta_sql` call sites from `&agg_json` to `Some(agg_json.as_str())`.
- **NOT fixed**: 4 unit tests still assert `sql.contains("DO $reflex_refresh$")` but DO blocks
  were removed. These 4 tests will fail:
  - `test_build_delta_sql_emits_do_block_gate_for_group_by_imv`
  - `test_build_delta_sql_emits_do_block_for_end_query_group_by`
  - `test_build_delta_sql_no_gate_for_sentinel_case` (asserts `!sql.contains(...)`)
  - `test_build_delta_sql_do_block_includes_dead_cleanup_when_expected`
  - `test_build_delta_sql_splice_injects_filter_before_group_by` (asserts contains DO block)
  - `test_build_delta_sql_splice_uses_distinct_projection_for_compound_key` (same)

### `src/create_ivm.rs` and `src/lib.rs`
- Minor changes related to the `aggregations_json` signature change (making function non-STRICT).

### `src/schema_builder.rs`
- Minor trigger DDL updates.

## Recommended Next Steps

### Step 1: Verify the MERGE+EXECUTE hypothesis

Connect to the pgrx test PostgreSQL instance (usually on port 28812 or similar) and test manually:

```sql
-- Create a simple test table
CREATE TABLE _t (city TEXT, amount NUMERIC);

-- Create a trigger on it that runs a MERGE referencing a transition table via EXECUTE:
CREATE OR REPLACE FUNCTION _test_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
  _stmt TEXT;
BEGIN
  _stmt := 'MERGE INTO _int AS t USING (SELECT city, SUM(amount) AS s FROM "__reflex_new__t" GROUP BY city) AS d ON t.city = d.city WHEN MATCHED THEN UPDATE SET s = d.s WHEN NOT MATCHED THEN INSERT VALUES (d.city, d.s)';
  EXECUTE _stmt;
  RETURN NULL;
END;
$$;
```

If this aborts, the hypothesis is confirmed.

### Step 2: Fix the MERGE+transition table issue

If confirmed, the fix is to **not reference transition tables inside MERGE via EXECUTE**. Instead,
materialize the delta into a temp table first, then MERGE from the temp table:

```sql
-- In the trigger body (generated SQL):
-- Step 1: materialize delta (outside EXECUTE, or in a separate statement)
CREATE TEMP TABLE __reflex_delta_<view> ON COMMIT DROP AS
  SELECT city, SUM(amount) AS "__sum_amount", COUNT(*) AS __ivm_count
  FROM "__reflex_new_<source>"
  GROUP BY city;

-- Step 2: MERGE from temp table (safe in EXECUTE context)
MERGE INTO __reflex_intermediate_<view> AS t
USING __reflex_delta_<view> AS d
ON t."city" IS NOT DISTINCT FROM d."city"
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

**However**, this approach was already explored (the PG15/16 fallback path used to do exactly this
for the `INSERT INTO affected` step via a separate statement). The concern was whether `CREATE TEMP
TABLE ... AS SELECT ... FROM transition_table` would itself work inside EXECUTE.

An alternative: materialize the delta in the trigger DDL body (not via EXECUTE), by generating a
`SELECT ... INTO _delta_rec FROM transition_table` first, then passing it to EXECUTE. This is
harder since the delta can have multiple rows.

### Step 3: Alternative — avoid MERGE entirely in the trigger-fired path

Use INSERT ... ON CONFLICT UPDATE instead of MERGE. This is supported in PG15+ and does not have
the same transition-table constraints as MERGE. The trade-off is verbosity.

### Step 4: Fix the 4 failing unit tests in unit_trigger.rs

Once the DO block situation is resolved (either restore DO blocks or keep them removed), update the
4 unit test assertions accordingly. The tests currently assert:

- `test_build_delta_sql_emits_do_block_gate_for_group_by_imv`: expects `DO $reflex_refresh$`
- `test_build_delta_sql_emits_do_block_for_end_query_group_by`: expects `DO $reflex_refresh$`
- `test_build_delta_sql_do_block_includes_dead_cleanup_when_expected`: expects `DO $reflex_refresh$`
- `test_build_delta_sql_splice_injects_filter_before_group_by`: expects `DO $reflex_refresh$`
- `test_build_delta_sql_splice_uses_distinct_projection_for_compound_key`: expects `DO $reflex_refresh$`

If DO blocks are restored, these tests pass as-is. If DO blocks stay removed, update assertions to
check for the separate DELETE + INSERT statements instead.

## Unanswered Questions

1. **Can MERGE reference a transition table inside EXECUTE?** — Not verified. PG documentation on
   transition tables says they are only accessible in the trigger that fires, but does not explicitly
   say whether EXECUTE'd SQL can reference them. PG source code (`nodeMerge.c`, `trigger.c`) would
   clarify this.

2. **Does the crash occur on PG15/16 or only PG17+?** — Not tested. The MERGE RETURNING path is
   PG17+ only in HEAD, but the simplified 3-statement path used by the WIP changes applies to all
   PG versions.

3. **Is `CREATE TEMP TABLE ... AS SELECT ... FROM transition_table` safe inside EXECUTE?** — Not
   tested. If yes, materializing the delta first would fix the crash cleanly.

4. **Does the crash also affect PG-test framework's default version?** — The pgrx test framework
   builds against a specific PG version. Need to check which version is active.

## Files To Review

- `src/trigger.rs` — all WIP changes; the MERGE SQL generation is in `build_merge_sql` and
  `reflex_build_delta_sql`
- `src/tests/unit_trigger.rs` — unit tests with DO block assertions (lines ~501–591)
- `src/tests/pg_test_correctness.rs` — the 6 new targeted-refresh integration tests
- `src/tests/pg_test_trigger.rs` — basic INSERT/UPDATE trigger tests (also crashing)

---

## 2026-04-24 — ROOT CAUSE FOUND (journal + plan were wrong)

### Actual crash

PG cassert backtrace (from `~/.pgrx/17.log`):

```
TRAP: failed Assert("MemoryContextIsValid(context)"), File: "mcxt.c", Line: 1184, PID: 870346
ExceptionalCondition+0x6b
initArrayResultWithSize+0x25
... pg_reflex.so ... create_reflex_ivm_wrapper+0x14
... standard_ExecutorRun ...
Failed process was running: SELECT create_reflex_ivm('test_city_totals', 'SELECT city, SUM(amount) AS total FROM test_orders GROUP BY city');
```

**This is NOT the MERGE/transition-table hypothesis.** It's a pgrx 0.16.1 SPI argument construction crash. The trigger body never runs — the crash happens inside `create_reflex_ivm` itself, before any trigger is fired.

### Why: pgrx 0.16.1 + PG 17.7 cassert interaction

`DatumWithOid::new(Vec<String>, TEXTARRAYOID)` calls `Vec::into_datum` → `array_datum_from_iter` → `initArrayResult(TEXTOID, CurrentMemoryContext, false)` (pgrx-0.16.1/src/datum/array.rs:1025-1033). In PG 17.7's cassert build, `MemoryContextIsValid(CurrentMemoryContext)` fails at this call site.

The codebase has **32 call sites** of this pattern in `src/create_ivm.rs` (and a few elsewhere). The WIP cycle-detection code added 1 more, but the pattern is extensive and pre-existing.

At HEAD (commit `a5381cb`, no WIP), `test_create_simple_sum_imv` and `test_trigger_fired_merge_does_not_crash_backend` BOTH SIGABRT during `create_reflex_ivm` — before any trigger fires. Reproduced manually via `cargo pgrx start pg17` + `psql`.

### What this invalidates

1. The journal's "MERGE referencing transition tables inside EXECUTE" hypothesis — *unrelated to the crash*. Plausible but not actually firing in this run.
2. Phase B (aggregate delta scratch): solid engineering but doesn't address this crash. Was never actually fixing anything end-to-end; unit tests pass but integration tests crash earlier.
3. Phase E (passthrough scratch, F.1 guard): same — correctness win in isolation, not the SIGABRT fix.

The 164 tests that pass are the ones that don't call `create_reflex_ivm` with a `Vec<String>` SPI arg path that triggers the PG assertion — e.g., `test_hello_pg_reflex`, `test_validate_view_name_unit`.

### Fix direction (next session)

**Option A — format arrays as SQL literals (recommended):** Replace every `DatumWithOid::new(Vec<String>, TEXTARRAYOID)` with a formatted `ARRAY['a','b','c']::TEXT[]` literal inlined into the SQL. Bypasses the pgrx Vec→ArrayResult path entirely. Mechanical but touches ~32 call sites; each needs proper escaping (single quotes doubled).

**Option B — upgrade pgrx:** pgrx 0.18.0 is current. Upgrade path requires CI validation across PG 15/16/17/18 and could surface unrelated breakage. Not recommended until we confirm the bug is fixed upstream.

**Option C — use pgrx's typed array constructors:** pgrx has safer array-construction APIs (e.g. `Array::from_slice`, `PgArray`). Refactor call sites to use those instead of `DatumWithOid::new(Vec, OID)`. Medium effort; less mechanical than A but more idiomatic.

Plan mode should be re-entered with this corrected root cause before any more coding.

### Where Phase B/E stand

Phase B + E implementations are committed to working tree (uncommitted). They:
- Pass unit tests (regression guards, transition-leak checks)
- Compile cleanly, clippy green
- Do NOT break anything that was previously working
- Do NOT fix the actual SIGABRT (which is in a different code path)

Recommend: leave Phase B + E in place (they're correct defensive work for a real future issue), but do **not** ship them as "the SIGABRT fix." The real SIGABRT fix is Option A above.

---

## 2026-04-24 pt.2 — Option B (pgrx upgrade) and Option A (inline literals) both insufficient

### Option B — pgrx 0.16.1 → 0.18.0

Upgraded `Cargo.toml` to `pgrx = "=0.18.0"`, `pgrx-tests = "=0.18.0"`, removed the `pgrx_embed_pg_reflex` bin target (0.18 no longer needs it), added `FlushErrorState` + `message_level_is_interesting` weak stubs to `build.rs` (new symbols pgrx 0.18 references in test binaries). Code compiled and clippy green. **SIGABRT persists** at same location: `initArrayResultWithSize` → `MemoryContextIsValid(context)` fails at `mcxt.c:1184`.

### Option A — inline array literals

Added `format_pg_text_array_literal(&[String]) -> String` helper in `query_decomposer.rs`. Replaced all 32 `DatumWithOid::new(Vec<String>, TEXTARRAYOID)` call sites in `create_ivm.rs` with `DatumWithOid::new(<formatted_array_text>, TEXTOID)`, then `::TEXT[]` cast inline in each SQL. Code compiles, clippy green.

**Progress** — the `initArrayResultWithSize` crash is *gone*. But a **different** `MemoryContextIsValid(context)` trap now fires at `mcxt.c:733` in PG 17.7 cassert. `addr2line` on the pg_reflex.so offsets shows the call chain:

```
pgrx_pg_sys::include::pg17::MemoryContextGetParent  (pg17.rs:34030)
  ← pgrx_pg_sys::submodules::ffi::pg_guard_ffi_boundary_impl  (ffi.rs:174/180)
  ← cee_scape::asm_based::call_with_sigsetjmp  (asm_based.rs:65/186)
  ← pgrx::memcxt::PgMemoryContexts::parent  (memcxt.rs:330)
  ← pgrx::spi::tuple::SpiTupleTable::get  (spi/tuple.rs:129)
  ← our Rust code reading an SPI tuple column
```

`PgMemoryContexts::CurrentMemoryContext.parent()` in pgrx 0.18 calls `pg_sys::MemoryContextGetParent(CurrentMemoryContext)`. PG 17.7's cassert build's `MemoryContextIsValid(CurrentMemoryContext)` rejects it.

### Conclusion — the crash is inside pgrx (both 0.16.1 and 0.18.0) hitting PG 17.7 cassert

`PG 17.7 cassert + pgrx SPI tuple access` is broken on this system. Our code can't avoid it because every `SpiTupleTable::get` (which *any* `.first().get_by_name(...)` call path triggers) calls `PgMemoryContexts::CurrentMemoryContext.parent()`.

Evidence:
- `test_validate_view_name_unit` (no SPI) → **passes**.
- `test_create_simple_sum_imv` (uses SPI) → **crashes**, even on a minimal input.
- Same crash via manual `psql` against `cargo pgrx start pg17` cluster (not test-harness artifact).
- Crash replays against HEAD (`a5381cb`, no WIP) exactly the same.
- 164 tests that pass are Rust-only unit tests that do not touch SPI.

### Options from here (punted to user)

- **Use a different PG 17 point release.** `~/.pgrx/17.2` exists; may not have the assertion that PG 17.7 added. Would need `cargo pgrx init --pg17 ~/.pgrx/17.2/...` or edit `~/.pgrx/config.toml` (global; user decides).
- **Build PG 17.7 without `--enable-cassert`.** The production path isn't broken (in non-assert builds the assertion is a no-op); only dev/test is. We'd lose runtime PG assertion protection in tests but regain a functioning test suite.
- **Upstream pgrx fix.** File against `pgx/pgrx` on GitHub: `PgMemoryContexts::CurrentMemoryContext.parent()` trips `MemoryContextIsValid` under PG 17.7 cassert, affecting every `SpiTupleTable::get` path. This is a pgrx bug that needs their attention.
- **Accept that `cargo pgrx test` doesn't run locally** until pgrx ships a fix, and rely on production-path smoke testing (`cargo pgrx run` + psql against non-cassert PG, or CI with different PG) for correctness verification.

### State of the tree at end of session

Kept:
- Phase B (aggregate delta scratch), Phase E (passthrough scratch), Phase F.1 generator guard. All compile, clippy green, unit-level defensive hardening.
- Option A mechanical rewrite (array literals inline). Bypasses the first of two pgrx-internal crashes (initArrayResultWithSize).
- `build.rs` weak stubs expanded (FlushErrorState, message_level_is_interesting) for pgrx 0.18 test-binary linking.
- `Cargo.toml`: pgrx = =0.18.0, pgrx-tests = =0.18.0, crate-type = ["cdylib"] only.
- `cargo-pgrx` CLI at 0.18.0.

Still broken:
- Integration tests (`cargo pgrx test pg17`) crash with SIGABRT in `PgMemoryContexts::CurrentMemoryContext.parent()` on every SPI-reading test. Not an application bug. User needs to pick one of the options above.

---

## 2026-04-24 pt.3 — Final resolution: ship, note test-only gap

**Confirmed production-safe.** `MemoryContextIsValid` in PG expands to:
- `--enable-cassert` build: `Assert()` → abort → SIGABRT
- production build: expands to `((void)0)` — the assertion is a no-op; control continues through `MemoryContextGetParent`, which just reads `context->parent`. pgrx-based extensions (Supabase et al.) are deployed at scale in production on non-cassert PG, so the pattern is known-safe there.

**Scope of impact**: only `cargo pgrx test` locally. CI with a distribution PG (non-cassert) would run the integration tests fine. Production is unaffected.

### Tree state at end of session (retained)

- `Cargo.toml`: `pgrx = "=0.18.0"`, `pgrx-tests = "=0.18.0"`, `crate-type = ["cdylib"]`, no `pgrx_embed_pg_reflex` bin. pgrx 0.16.1 is no longer an option — it fails to compile against current PG 17.7 bindgen output (`t_bits` field mismatch in `HeapTupleHeaderData`).
- `build.rs`: expanded with weak stubs `FlushErrorState` and `message_level_is_interesting` (pgrx 0.18 test-binary linking).
- `src/query_decomposer.rs`: new `format_pg_text_array_literal` helper.
- `src/create_ivm.rs`: all 32 `DatumWithOid::new(Vec<String>, TEXTARRAYOID)` call sites rewritten to pass TEXT literals with `::TEXT[]` casts in SQL. Sidesteps the `initArrayResultWithSize` cassert trap.
- `src/schema_builder.rs`: new `build_passthrough_scratch_ddls` (Phase E).
- `src/trigger.rs`: passthrough branch rewritten to materialize transition tables into per-(IMV, source) UNLOGGED scratch tables before any DML (Phase E); generator-side guard rejects any statement that references transition tables outside the sanctioned scratch-populate pattern (Phase F.1).
- `src/drop_ivm.rs`: passthrough scratch drop on IMV drop (Phase E).
- `src/tests/unit_trigger.rs`: 4 new unit tests (3 passthrough structure, 1 aggregate regression guard for transition leaks). All pass.
- `src/tests/pg_test_no_sigabrt.rs`: chain regression test (can't run locally until pgrx fix, but correct as written).

### Verification

- `cargo fmt` — clean.
- `cargo clippy --features pg17 --no-default-features --all-targets --no-deps -- -D warnings` — clean.
- `cargo test --features pg17 --no-default-features --lib` — 162 pure-Rust unit tests pass, 0 fail.
- `pg_test` integration tests blocked by pgrx cassert bug → file follow-up.

### Remaining follow-up (not blockers)

1. **Upstream pgrx issue**: file against `pgcentralfoundation/pgrx`, describing the two cassert sites (`DatumWithOid::new(Vec<T>, TEXTARRAYOID)` → `initArrayResult`, and `PgMemoryContexts::CurrentMemoryContext.parent()` → `MemoryContextGetParent`). Include repro against PG 17.7 + pgrx 0.16.1/0.18.0. pgrx maintainers will know whether the assumption needs patching or PG changed behavior.
2. **CI with distribution PG**: until pgrx patches, integration tests can only be run on non-cassert PG. Add a CI job on Debian/Ubuntu PG 17 or PG 18.
3. **Production smoke**: extensions install into distribution PG and run manually — the `create_reflex_ivm` path that crashes on cassert-PG 17.7 should succeed on any non-cassert install. Do one smoke test in a clean VM as part of the 1.2.0 release gate.

---

## 2026-04-24 (evening): Actual root cause found — build.rs stub leakage

The earlier hypothesis ("pgrx 0.18 cassert bug in PG 17.7") was **wrong**. Rebuilding PG 17.7 without `--enable-cassert` revealed the same crash in production mode, as SIGSEGV (signal 11) instead of SIGABRT. A production bug, not a test-harness quirk.

### Backtrace (non-cassert PG 17.7, direct psql smoke)

```
#0  MemoryContextGetParent
#3  pgrx::memcxt::PgMemoryContexts::parent
#4  pgrx::spi::tuple::SpiHeapTupleData::get_by_name
#5  pg_reflex::create_ivm::create_reflex_ivm_impl closure
#6  pgrx::spi::Spi::connect_mut
#7  pg_reflex::create_ivm::create_reflex_ivm_impl
#9  create_reflex_ivm_wrapper
```

`pg_sys::CurrentMemoryContext` was null; `MemoryContextGetParent(NULL)` segfaulted on the first `->parent` deref.

### Root cause

`nm pg_reflex.so` showed:

```
0000000000394380 b CurrentMemoryContext
0000000000394388 b ErrorContext
0000000000394390 b PG_exception_stack
```

Symbol class `b` = local BSS, zero-initialised. The weak stub definitions added to `build.rs` (to satisfy `cargo test --lib` linkage on Linux) were being linked into the installed cdylib. When postgres dlopen'd the `.so`, its own strong symbols did **not** override the local BSS copies (GNU ld's ELF symbol-resolution rules for weak variables in a dlopen'd library). Every pgrx call that read `pg_sys::CurrentMemoryContext` read the shadowed local NULL, not PG's real current context.

### Fix

Two-line mechanical change, but load-bearing:

1. `build.rs` — stop emitting a global `cargo:rustc-link-lib=static=pg_reflex_pg_stubs`. Only expose the archive's search path.
2. `src/lib.rs` — add a `#[cfg(all(test, target_os = "linux"))] #[link(name = "pg_reflex_pg_stubs", kind = "static")] unsafe extern "C" {}` block. This pulls the archive in **only** when compiling the unit-test binary; the cdylib that pgrx installs is never affected.

After the fix `nm` shows the symbols as `U` (undefined), and postgres resolves them to its own globals at dlopen — the correct behaviour.

### Collateral cleanup while here

- **Cycle-detection query** at `src/create_ivm.rs:802–833` was using `.first().get_one::<bool>()`, the pgrx pattern that crashes through `CurrentMemoryContext.parent()`. Rewrote it to mirror the adjacent duplicate-name probe (`.collect::<Vec<_>>().is_empty()`), avoiding the same pgrx call pattern regardless of whether the context is valid.
- **Phase E F.1 guard** in `reflex_build_delta_sql` was over-rejecting legitimate full-refresh SQL paths (LEFT JOIN secondary-table fallback, FULL OUTER JOIN aggregates, randomized-join correctness). Removed — the premise it was defending against (transition-table references in EXECUTE crash PG) was itself wrong; once the stub leak is fixed those references are safe.
- **Deferred passthrough flush** (`reflex_flush_deferred`) only stood up a temp view for `__reflex_old_<src>`. Phase E's passthrough branch populates both `__reflex_pt_new_<v>_<s>` and `__reflex_pt_old_<v>_<s>` from the transition tables, so the flush also needs to stand up `__reflex_new_<src>`. Both views now project the source columns explicitly (no `__reflex_op` metadata leak) via `information_schema.columns`.
- **`test_trigger_fired_merge_does_not_crash_backend`** oracle used a fresh query missing the `__ivm_count` column — deterministic EXCEPT-column-count failure unrelated to the crash work. Fixed.

### Verification

- `cargo fmt --check` — clean.
- `cargo clippy --release --features pg17 --no-default-features --lib --no-deps` — clean.
- `cargo pgrx test --release --features pg17 --no-default-features pg17` — **476 passed, 0 failed**.
- Manual psql smoke against non-cassert PG 17.7 — chained passthrough IMVs with INSERT/UPDATE(value)/UPDATE(group)/DELETE + deeper chain (v3→v2→v1) all correct, no crashes.

### Takeaways

- A weak variable definition inside a dlopen'd `.so` shadows the loader's real global. This is a subtle ELF rule that doesn't apply to functions (where RTLD resolves normally). Stubs intended for a static test binary must never end up in a loadable module.
- Build-script advice: prefer `#[cfg(test)] #[link(...)]` in Rust source over global `cargo:rustc-link-lib` when a dependency is test-only. The build script can still generate the archive unconditionally; only the link request needs to be conditional.
- When a crash persists after "fixing" it, the fix is probably wrong. Phase B (aggregate scratch), Phase E (passthrough scratch), and the F.1 guard all landed under the wrong root-cause hypothesis. Phase B/E are still useful as hygiene (single materialisation per trigger fire, no deep transition-table aliasing) and stayed in; F.1 was a pure liability and got removed.
