# 2026-05-14 — Four targeted optimizations: incremental trigger + reconcile

## Source

Findings in `journal/2026-05-14_sop_forecast_levers_probe.md`. The options
menu in `plans/2026-05-14-post-skip-filter-optimization-options.md`
selected items 2 (DISTINCT), 1 (Effective-INSERT shortcut), 7 (single-
direction delta), plus a new item 4 derived from the reconcile discussion:
**intermediate-only CTAS-and-swap**.

## Combined expected outcome (SOP-forecast shape, 1 M source, 760 K intermediate)

| Workload | 1.4.5 today | After items 1+2+3 | After items 1+2+3+4 |
|---|---:|---:|---:|
| Status pivot (no-op) | 1 ms | 1 ms | 1 ms |
| Out→in 20 K rows | 657 ms | ~430 ms | ~430 ms |
| Out→in 80 K rows | 2 719 ms | ~1 800 ms | ~1 800 ms |
| **Out→in 180 K rows** | **7 064 ms** | **~4 650 ms** | **~4 650 ms** |
| **Reconcile (≥30 % dispatch)** | **~14 000 ms** | ~14 000 ms | **~9 000 ms** |
| REFRESH MV baseline | 5 421 ms | 5 421 ms | 5 421 ms |

Items 1+2+3 fix the **incremental** path (the 24 % crossover). Item 4 fixes
the **dispatch/rebuild** path. They are independent.

## Ordering and dependencies

Items 1, 3, 4 are independent and can land in any order. Item 2 is the
biggest and most invasive, and benefits from being last so it can be
implemented against an already-trimmed `push_materialized_merge_and_affected`
(item 1 removes the redundant DISTINCT inside the helper).

Recommended sequence:

1. **Item 1** (DISTINCT, 30 min) — trivial, lands the helper change item 2 builds on.
2. **Item 3** (single-direction delta, half day) — independent codegen change.
3. **Item 4** (reconcile CTAS+swap, 1.5 days) — touches `reconcile.rs` only.
4. **Item 2** (effective-INSERT shortcut, 2 days) — biggest behavior change, lands last so we can isolate any regression to it.

Each item is shippable on its own; tests are independent.

---

## Item 1 — Drop redundant `SELECT DISTINCT` on the affected-groups insert

### Problem

`push_materialized_merge_and_affected` (`src/trigger.rs:1092-1118`) emits:

```rust
stmts.push(format!(
    "INSERT INTO {} SELECT DISTINCT {} FROM {} AS __d",
    affected_tbl, select_expr, scratch_tbl
));
```

The scratch table is built one step earlier (`:1106-1107`) by the
materialized merge as the result of a `GROUP BY` aggregation. So `scratch`
is already one row per group key. The `DISTINCT` is doing extra hashing/
sorting work for zero net effect.

Measured cost on the SOP-forecast bench:
- 20 K affected groups: 22 ms
- 180 K affected groups: 190 ms

### Change

In `src/trigger.rs:1114-1117`, replace `SELECT DISTINCT` with plain
`SELECT`:

```rust
stmts.push(format!(
    "INSERT INTO {} SELECT {} FROM {} AS __d",
    affected_tbl, select_expr, scratch_tbl
));
```

### What this does NOT touch

The outer-join-secondary branch at `src/trigger.rs:1285-1288` also uses
`SELECT DISTINCT`, but its source is `({delta_q}) AS __d` — a raw delta
query that is **not** pre-grouped (it's filtered group-key projection from
the transition table). Keep `DISTINCT` there.

Same caution: the global-COUNT(DISTINCT)-with-no-GROUP-BY branch.

### Tests

- `src/tests/unit_trigger.rs`: add a SQL-shape test asserting the generated
  affected-INSERT for a grouped IMV does NOT contain `SELECT DISTINCT`.
- All 528 existing tests must remain green. The behavior is identical (the
  rows are already distinct); only the SQL plan changes.

### Risk

Trivial. The invariant "scratch is one-row-per-group" is a property of the
materialized merge SQL emitted at `:1106-1107` and has held since 1.4.1.

### Effort

30 minutes. ~5 lines of code + 1 unit test.

---

## Item 3 — Single-direction delta for INSERT and DELETE source ops

### Problem

`build_net_delta_query` (`src/trigger.rs:292-368`) unconditionally emits
the `UNION ALL` form:

```sql
SELECT keys, SUM(CASE WHEN __reflex_sign = 1 THEN COALESCE(col, 0) ELSE -COALESCE(col, 0) END) AS col, ...
FROM (
    SELECT 1 AS __reflex_sign, __d.* FROM (delta_new) AS __d
    UNION ALL
    SELECT -1 AS __reflex_sign, __d.* FROM (delta_old) AS __d
) AS __net
GROUP BY keys
```

For source-table `INSERT` ops there's no `delta_old` (the trigger fires
with only `NEW`). For `DELETE` ops there's no `delta_new`. Today the
trigger only goes through this helper from the `UPDATE` branch
(`src/trigger.rs:1670-1696`), so the simple INSERT/DELETE branches at
`:1455-1509` already use single-direction `push_materialized_merge`
(`:970-986`).

What's missing: the `UPDATE` branch always uses the net-delta even when
the user's UPDATE doesn't actually change the filter-relevant columns on
some rows. There the `UNION ALL` form is correct, but it also runs for
UPDATEs where the filter-aware skip would have dropped half the
transition rows — both sides may not be empty in absolute terms, but the
double-scan-of-source overhead remains.

The single-direction simplification we can land:

When the trigger body is invoked with `operation = 'UPDATE'` AND the
analyzer/codegen can prove one of the deltas is empty post-filter (e.g.,
the IMV's `WHERE` clause rejects the OLD side: an `archived → validated`
flip in the SOP workload), short-circuit to a single-direction path. The
runtime check is cheap.

### Change

Add a runtime check after the transition tables are visible and before
the scratch INSERT in the UPDATE-with-grp_cols-no-min-max branch
(`src/trigger.rs:1670-1696`). The probe matches what the 1.4.5 spurious-
skip already does for filter-aware skipping:

```sql
-- For each side of the delta, check if any post-filter row exists.
SELECT
    EXISTS(SELECT 1 FROM __reflex_old_<source> WHERE <imv_relevant_where>) AS old_has_rows,
    EXISTS(SELECT 1 FROM __reflex_new_<source> WHERE <imv_relevant_where>) AS new_has_rows
```

Three runtime paths:
1. **Neither side has rows** — the spurious-skip already catches this at
   the trigger entry (1.4.5). The runtime check is redundant; rely on
   spurious-skip.
2. **Only NEW has rows** — emit `push_materialized_merge_and_affected`
   with `delta_new` only (sign +1). No UNION ALL. Equivalent to an INSERT-
   shape delta.
3. **Only OLD has rows** — emit single-direction subtract. Equivalent to
   a DELETE-shape delta.
4. **Both have rows** — keep today's `build_net_delta_query` UNION ALL
   form.

For our SOP workload (`archived → validated`): path 2 fires (OLD has no
post-filter rows, NEW has 20 K). The scratch INSERT drops from a
two-subquery UNION ALL aggregate to a single subquery aggregate. Plan
becomes simpler, planner inlines better, executor startup cost halves.

### Estimated win

Bench data (`profile_clean.log`):
- 20 K-row scratch INSERT today: 290 ms (UNION ALL, OLD side empty so PG fast-fails the inner subquery but planner+executor startup overhead remains)
- 20 K-row INSERT-only scratch (estimated from the captured INSERT branch shape): ~150-180 ms

Savings: ~100-150 ms per UPDATE on filter-flip workloads. Larger at
scale: ~600-900 ms on the 180 K-row case.

### Files

- `src/trigger.rs`:
  - Add a helper `build_single_direction_delta_query(delta_q, plan, op:
    DeltaOp)` that produces the SUM-only-from-one-side form (no
    `__reflex_sign` column, no UNION ALL).
  - In the UPDATE-with-grp_cols-no-min-max branch (`:1670-1696`): emit
    the runtime probe (single `SELECT EXISTS … AS old_has, EXISTS … AS
    new_has`), then a DO-block dispatch picking between three paths. The
    probe + dispatch shape mirrors the high-selectivity dispatch already
    present at `:1028-1089`.

### Tests

- `src/tests/unit_trigger.rs`: SQL-shape tests for the three runtime
  paths (assert the generated SQL contains `__reflex_sign` only in the
  both-sides path, etc.).
- `src/tests/pg_test_correctness.rs`: pgrx end-to-end tests for an UPDATE
  that flips a filter-driving column. Assert EXCEPT-ALL = 0 against
  REFRESH MATERIALIZED VIEW.

### Risk

Low-to-medium. The single-direction subtract path is equivalent to a
source-row DELETE — already exercised by 1.4.5's DELETE branch
(`:1482-1528`). The single-direction add path is equivalent to an
INSERT branch. Both are correctness-locked by existing test coverage;
we're just reaching them from a different trigger entry.

Edge case: a multi-row UPDATE where some rows flip filter-out and others
flip filter-in. Both sides have rows after filter; falls into path 4 (the
existing UNION ALL).

### Effort

Half a day. ~80 LOC + 4 tests.

---

## Item 4 — Reconcile: intermediate-only CTAS-and-swap

### Problem

`reflex_reconcile` (`src/reconcile.rs:128-277` for the aggregate branch)
rebuilds the intermediate via:

```sql
-- drop indexes
DROP INDEX ...;
-- TRUNCATE + INSERT INTO existing table
TRUNCATE __reflex_intermediate_<view>;
INSERT INTO __reflex_intermediate_<view> <base_query>;
-- recreate indexes
CREATE INDEX ...;
```

Measured: 9.5 s for the INSERT step on a 760 K-row intermediate.

Two structural costs:
1. `INSERT INTO existing_table SELECT …` is **not parallelized** by the
   PG planner (PG 14-17 reserve parallel INSERT for a narrow set of
   shapes; our `INSERT INTO unlogged_existing_table SELECT <5-way JOIN +
   GROUP BY>` doesn't qualify out of the box). The same `SELECT` body
   would run with parallel workers if invoked via `CREATE TABLE AS`.
2. The TRUNCATE+INSERT pattern holds AccessExclusive on the intermediate
   throughout the load. Concurrent triggers reading intermediate (for
   cascade-fed IMVs) block until reconcile finishes the INSERT.

Plus the unsymmetrical setup: even though reconcile drops indexes first
(`:184-192`), the bulk insert still does some per-tuple heap_insert
bookkeeping that CTAS' table-build path avoids.

### Why intermediate-only (not target too)

The target table may have user-attached objects: FKs referencing it,
views selecting from it, RLS policies, fillfactor settings, comments,
GRANT/REVOKE state, partitioning. PG's CTAS-and-rename pattern doesn't
preserve these (foreign keys reference by OID, view definitions capture
parsed dependencies that don't follow rename).

The `__reflex_intermediate_<view>` table is fully pg_reflex-managed:
- Indexes are recreated by `build_indexes_ddl()` (`reconcile.rs:254`).
- No external FK references.
- No user views (the intermediate is an implementation detail; user code
  reads the target).
- No user permissions or RLS.
- No triggers (only the per-source trigger functions, which are on the
  *source* tables, not on intermediate).

CTAS-and-rename on intermediate is safe.

For target, keep the existing TRUNCATE+INSERT pattern. The 3.6 s target
cost is acceptable; item 4 doesn't try to touch it.

### Change

Rewrite the aggregate-branch of `reflex_reconcile` (lines 128-277) so
that the intermediate rebuild is:

```rust
// 1. Drop the intermediate-side indexes the SAME WAY as today (keep this).
// 2. NEW: build a fresh heap.
let int_new = format!("{}_reconcile_new", int_bare);
let int_new_quoted = format!("\"{}\".\"{}\"", int_schema, int_new);

client.update(
    &format!(
        "CREATE UNLOGGED TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING STORAGE)",
        int_new_quoted, intermediate
    ), None, &[],
)?;
// Storage: copy fillfactor etc. The INCLUDING ALL variant would also
// copy indexes — we explicitly skip that; indexes get built post-load.

client.update(
    &format!("INSERT INTO {} {}", int_new_quoted, base_query),
    None, &[],
)?;

// 3. Build the intermediate indexes on the NEW table (with name suffix
//    to avoid collision; rename to canonical names at swap time).
for idx_ddl in build_indexes_ddl_for_table(&int_new_quoted, &plan) {
    client.update(&idx_ddl, None, &[])?;
}

// 4. Atomic swap.
client.update(&format!("DROP TABLE {}", intermediate), None, &[])?;
client.update(
    &format!(
        "ALTER TABLE {} RENAME TO {}",
        int_new_quoted, int_bare
    ), None, &[],
)?;
// Plus per-index renames if we used suffixed names.
```

### Why CTAS via `CREATE TABLE (LIKE …) + INSERT INTO` rather than
`CREATE TABLE … AS`

`CREATE TABLE AS` derives column types from the SELECT output and
doesn't honor existing constraints. We need the new heap to match the
intermediate's exact column shape (including `__ivm_count BIGINT NOT
NULL DEFAULT 0` and any column-level NOT NULL the data-probe added).
`(LIKE … INCLUDING DEFAULTS INCLUDING STORAGE)` gives us a structurally-
identical empty heap; the INSERT then populates it. The INSERT path
into an *empty* table inside a transaction is the bulk-load path that
parallelism CAN cover (and that PG14+ has explicit optimizations for
when wal_level=minimal).

If the parallel-INSERT-into-empty optimization doesn't fire on a given
PG version, fall back to `CREATE TABLE AS` with a post-CTAS ALTER TABLE
to add the constraints we want. Verify at code-write time which form
PG 15/16/17/18 actually parallelize on this workload.

### Caveat A — orphan table on crash

If the backend crashes between CTAS and RENAME (or if SPI errors out
before swap), the database is left with `__reflex_intermediate_<view>`
plus an orphan `__reflex_intermediate_<view>_reconcile_new`.

**Mitigation 1 — UUID suffix + startup cleanup**: name the new table
`__reflex_intermediate_<view>_reconcile_<uuid>`. Add a cleanup pass to
`drop_reflex_ivm` and a per-IMV "stale-temp-table" check at the start of
each `reflex_reconcile` invocation:

```sql
-- At reconcile start, before doing anything:
DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT relname FROM pg_class
             WHERE relname LIKE '__reflex_intermediate_<view>_reconcile_%'
             AND relnamespace = '<schema>'::regnamespace
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.relname);
    END LOOP;
END $$;
```

This makes reconcile self-healing: a previously crashed run's orphan is
swept on the next invocation.

**Mitigation 2 — wrap in a SAVEPOINT and clean on exception** (only
inside the SPI client, since the user's session is the parent xact):

```rust
client.update("SAVEPOINT reflex_ctas_sp", ...)?;
// ... CTAS + INSERT + index ...
match client.update(...) {
    Ok(_) => client.update("RELEASE SAVEPOINT reflex_ctas_sp", ...)?,
    Err(e) => {
        client.update("ROLLBACK TO SAVEPOINT reflex_ctas_sp", ...)?;
        // SAVEPOINT rollback unwinds the CTAS (drops the new table)
        return Err(e);
    }
}
```

The SAVEPOINT path is preferable — it makes the orphan impossible in
the normal error path. The UUID-suffix-and-startup-cleanup is a belt-
and-suspenders defense for true backend crashes (segfault, OOM kill)
where SAVEPOINT can't run rollback.

**Recommendation**: implement BOTH. SAVEPOINT-for-errors + UUID-suffix
+ startup-cleanup-for-crashes. Cost is ~30 LOC of cleanup logic; risk
reduction is high.

### Caveat B — index-name collisions

Indexes built on the new table must not collide with the existing
intermediate's index names (PG raises on duplicate names in the same
schema). Resolutions:

- Build new indexes with suffix names matching the table suffix
  (e.g. `idx__reflex_int_<view>_reconcile_<uuid>_uniq`).
- After RENAME of the table, run `ALTER INDEX … RENAME TO <canonical>`
  for each.
- This requires knowing the canonical names. They're derivable from the
  view name + `build_indexes_ddl` patterns.

Or: do the table swap with the new indexes carrying their suffix names,
and let pg_reflex's `build_indexes_ddl` always emit names with the
canonical pattern at trigger time. Since the trigger's MERGE/EXISTS SQL
references the intermediate by **table name** (not index name), the
indexes can be renamed lazily and the trigger still works. We just need
the rename for human-readability and for matching the create_ivm /
drop_reflex_ivm patterns that scan for `idx__reflex_int_<view>_*` by
name.

**Recommendation**: rename the indexes post-swap. ~20 LOC.

### Caveat C — concurrent triggers reading the intermediate

If a trigger on a *different* source table fires while reconcile is
mid-CTAS, the trigger reads the OLD intermediate (still in place). After
reconcile's RENAME, subsequent triggers see the NEW intermediate. The
window is fine — no row visibility tear, just a name-level swap at
RENAME commit time.

Concern: an in-flight trigger that already opened the OLD intermediate
heap will continue to read the OLD heap until that statement completes.
After the trigger's statement completes, the next statement in its
function body opens by NAME and sees the NEW table. As long as
reconcile holds AccessExclusive on the old table only at RENAME time
(brief), a long-running trigger in another session doesn't deadlock — it
queues for the AccessExclusive after its own statement releases.

This is **safer** than today's TRUNCATE+INSERT pattern, which holds
AccessExclusive throughout the 9.5 s INSERT.

### Caveat D — `pg_reflex` extension's own catalog tables

`__reflex_ivm_reference` and related extension tables are not touched
by reconcile beyond a `last_update_date` UPDATE. CTAS+swap on the
intermediate doesn't perturb them.

### Estimated win

Bench-derived:
- Step 3 (INSERT INTO intermediate): 9.5 s today → ~4-5 s with parallel
  CTAS (HashAgg + JOIN parallel with 4 workers).
- Index recreation: ~0.5 s, unchanged (already parallel via
  `max_parallel_maintenance_workers`).
- Swap (DROP + RENAME + index renames): ~10-50 ms.
- Net reconcile total: ~9-10 s vs today's ~14 s.

Still 1.7× REFRESH MV's 5.4 s — no miracle. The path to actually
**beating** REFRESH MV requires Option 5 from the menu (skip the
intermediate entirely for SUM-only IMVs), which is a separate, larger
change.

### Files

- `src/reconcile.rs`:
  - Replace the aggregate branch at `:128-277`.
  - Add SAVEPOINT-wrapping for the CTAS region.
  - Add a stale-temp-table sweep at function entry.
- `src/schema_builder.rs`:
  - Refactor `build_indexes_ddl` (or add `build_indexes_ddl_for_table`)
    to emit DDL for an arbitrary intermediate table name (parameterized
    on the table). This lets reconcile point the same DDL at the temp
    table.
- `src/drop_ivm.rs`:
  - Extend the cleanup pass to also drop any
    `__reflex_intermediate_<view>_reconcile_*` orphans.

### Tests

- `src/tests/pg_test_reconcile.rs` (or in `pg_test_basic.rs` if no
  dedicated file): test cases:
  - Reconcile on a populated IMV produces identical output (EXCEPT-ALL
    against REFRESH MATERIALIZED VIEW of an equivalent query) — already
    exists; just rerun.
  - Reconcile leaves no orphan `*_reconcile_*` tables on the happy path.
  - Simulated mid-reconcile abort (force a SQL error in a test hook)
    leaves no orphan and the original intermediate intact.
  - Concurrent SELECT from a different session against the target stays
    available throughout reconcile (no AccessExclusive on target).
- Benchmark: `benchmarks/bench_reconcile_ctas.sql` — measure reconcile
  wall-clock on the SOP-forecast shape before/after.

### Risk

Medium. SPI behavior across CTAS + RENAME + cross-session locking is
subtle. The SAVEPOINT-wrapping and the startup-cleanup defend against
the practical failure modes. Tests must cover:
- Happy path
- Failure mid-CTAS (rollback path)
- Failure between CTAS and RENAME (rollback path)
- Concurrent reader of target during reconcile (no blocking)
- Repeated reconcile calls (idempotent — no orphan accretion)

### Effort

1.5 days code + 1 day tests + 0.5 day bench. The bulk is in
`reconcile.rs`; `schema_builder.rs` and `drop_ivm.rs` are small
adjustments.

---

## Item 2 — Effective-INSERT shortcut for OUT→IN UPDATEs

(Implement last — biggest scope; benefits from item 1 having simplified
the helper it lives in.)

### Problem

When an UPDATE moves rows across the filter membership (e.g.,
`UPDATE demand_planning SET status='validated' WHERE id IN (5,6,7,…)`
flipping from `archived` → `validated`):

- `delta_old` is empty (rows didn't pass filter before).
- `delta_new` has rows.
- Scratch contains only positive `__ivm_count` contributions.
- Intermediate has no pre-existing rows for any affected key.
- Target has no pre-existing rows for any affected key.

Today's path runs anyway:
- MERGE intermediate (every row hits WHEN NOT MATCHED → INSERT — pays MERGE planner cost): 1 629 ms at 180 K rows.
- DELETE intermediate `WHERE __ivm_count <= 0 AND EXISTS (…affected)`: scans affected-subset, finds 0 dead rows, returns: 388 ms wasted.
- DELETE FROM target `WHERE EXISTS (…affected)`: scans target's affected-subset, finds 0 pre-existing rows, returns: 372 ms wasted.
- INSERT INTO target — necessary work, 1 291 ms.

Total wasted on OUT→IN at 180 K: **~2.4 s** (35 % of wall-clock).

### Change

After scratch is built and affected is populated, run a tiny probe:

```sql
SELECT bool_and(__ivm_count > 0) FROM __reflex_scratch_<view>
```

For OUT→IN: all scratch rows have positive `__ivm_count` → probe
returns `true`. Hot path. For IN→OUT: all rows have negative
`__ivm_count` (the subtract direction) → probe returns `false`.

Then a 3-way runtime dispatch inside the trigger DO block (like
`build_high_selectivity_dispatch_sql` does today):

1. **Pure-add path** (probe = true): emit
   - `INSERT INTO __reflex_intermediate SELECT * FROM __reflex_scratch`
     (plain INSERT, no MERGE).
   - Skip dead-cleanup DELETE entirely.
   - Skip target DELETE entirely.
   - `INSERT INTO target SELECT … FROM __reflex_intermediate WHERE
     __ivm_count > 0 AND EXISTS (…affected)` — keep this.

2. **Pure-subtract path** (probe = false AND all `__ivm_count < 0`):
   not a common case for non-OLTP shapes — DELETE branch already
   handles single-direction subtracts via `push_materialized_merge`. If
   we hit this from the UPDATE branch (would mean every group's net
   contribution is purely negative — the IN→OUT case), emit:
   - MERGE intermediate (subtract path) — keep.
   - DELETE intermediate WHERE __ivm_count <= 0 — keep (this case
     actually produces dead rows).
   - DELETE FROM target — keep.
   - Skip the target INSERT entirely (no surviving groups to insert).

3. **Mixed path** (probe = false AND scratch has both signs): today's
   path. Keep unchanged.

### Files

- `src/trigger.rs`:
  - In the UPDATE-with-grp_cols-no-min-max branch (`:1670-1696`), after
    the scratch+affected INSERT, emit a runtime probe + DO-block
    dispatch that picks among the three paths above.
  - Add `build_pure_add_intermediate_insert(scratch_tbl, intermediate_tbl, plan)` — a
    plain `INSERT INTO intermediate SELECT * FROM scratch` with column
    list matching the intermediate's columns.
  - Refactor `build_high_selectivity_dispatch_sql` to accept a list of
    alternative branch-bodies (it already accepts `&[&str]` per item 4
    in the menu — reuse).

### Why we don't extend this to the MIN/MAX/top-K path

MIN/MAX-bearing IMVs have heap state in the intermediate that the
"pure add" probe can't infer. Adding 20 K new groups with heap state
requires the MERGE WHEN NOT MATCHED path because the heap initialization
logic lives in `build_merge_using`'s INSERT VALUES (it copies the
delta's heap into the new row). Bypassing MERGE for MIN/MAX would
require re-implementing that logic inline; not worth it.

Gate the pure-add path on `!has_min_max && grp_cols.is_some()`.

### Estimated win

For the SOP-forecast workload (SUM + BOOL_OR, no MIN/MAX):

| Affected rows | Today | Item 2 |
|---|---:|---:|
| 20 K | 657 ms | ~450 ms |
| 80 K | 2 719 ms | ~1 900 ms |
| 180 K | 7 064 ms | **~4 650 ms** |

The 180 K case drops below REFRESH MV (5 421 ms): **pg_reflex back
ahead at 24 % selectivity**.

### Tests

Critical correctness tests (add to `src/tests/pg_test_correctness.rs`):

- **OUT→IN single dem_plan** flip on a SUM IMV. Assert EXCEPT-ALL = 0
  vs REFRESH MATERIALIZED VIEW. Snapshot ctid before/after; assert that
  the affected groups got freshly inserted (no incidental ctid stability
  on those rows).
- **IN→OUT single dem_plan** flip. Assert the IN→OUT path correctly
  hits the pure-subtract branch and removes the rows from target.
- **Mixed UPDATE** that flips some rows out and some in within one
  statement. Must hit the mixed path (today's behavior).
- **Multi-row INSERT** on the source while the source contains rows for
  groups already in the IMV. Some scratch rows are pure-add (new groups)
  and some hit existing groups (mixed). Must hit the mixed path.
- **OUT→IN with simultaneous data UPDATE**: the same statement both
  flips the filter AND changes a SUM-contributing column. Scratch has
  positive contributions only (the OLD didn't pass filter, so no
  subtract). Hits pure-add path. Output must match.
- **BOOL_OR on OUT→IN**: assert `BOOL_OR` aggregation is correct
  through the pure-add path (it's just SUM on `__true_count` and
  `__nonnull_count`, so should work, but explicit test).

Add a unit test in `unit_trigger.rs`:
- Generated SQL for OUT→IN-shaped UPDATE on a SUM IMV contains the
  pure-add dispatch (no MERGE on intermediate, no dead-cleanup DELETE,
  no target DELETE).
- Generated SQL for IN→OUT-shaped UPDATE keeps the existing path.

### Risk

Medium. The mathematical correctness of the pure-add path is clear (we
proved it above — every step we skip is provably a no-op when the probe
returns true). The implementation risk is in the dispatch wiring:
- The probe must run AFTER scratch+affected are built (`__ivm_count`
  must be populated).
- The pure-add INSERT must not conflict with the composite-unique index
  on intermediate (it won't, because the affected groups are by
  definition not yet in intermediate — but we should add a defensive
  test where a concurrent INSERT into the source pre-populates one of
  the affected groups; that case must fall to the mixed path).

The concurrent-pre-population case is the edge worth thinking about:
- Trigger fires for UPDATE T1.
- Between T1's scratch build and the probe, another session inserts a
  row on the source for one of T1's affected groups.
- T1's intermediate doesn't see the new row (we're in T1's snapshot),
  so the probe still returns `true` for "all positive".
- T1 takes the pure-add path: `INSERT INTO __reflex_intermediate SELECT
  * FROM __reflex_scratch`. The composite unique index catches the
  conflict if the affected group somehow exists.
  
Wait — under MVCC, T1's snapshot won't see another session's INSERT
unless they committed before T1's snapshot was taken. The trigger's
snapshot was taken at T1's statement start, which is before scratch is
built. So if the other session committed before T1's statement, T1's
scratch would have shown the post-insert state of __reflex_intermediate
(via the JOIN). If after T1's statement start, T1 doesn't see it.

So the snapshot ordering is consistent. The composite unique index on
intermediate (`UNIQUE NULLS NOT DISTINCT` on group keys) gives us a
correctness backstop: if two sessions race to insert the same group,
one will get a `unique_violation` and rollback. The pure-add path
inherits this safety from the standard INSERT.

Effort scope: ~2 days code + 2 days tests + bench. Highest-value item;
lands last so item 1+3+4 are stable when this is being benchmarked.

---

## Combined acceptance criteria

After all four items land:

- `cargo test --features pg17` green (528 + new tests).
- `cargo clippy --features pg17 --all-targets -- -D warnings` clean.
- `cargo fmt --check` clean.
- `benchmarks/bench_sop_forecast.sql` (to be written; mirrors
  `/tmp/pg_reflex_explore/recreate_and_bench.sql` from this session):
  - Status pivot (in-filter → in-filter): ≤ 2 ms.
  - OUT→IN 20 K rows: < 500 ms (down from 657 ms).
  - OUT→IN 180 K rows: < 5 000 ms (down from 7 064 ms; under REFRESH MV).
  - REFRESH MV baseline: ~5 400 ms (unchanged).
  - Reconcile total: < 11 000 ms (down from ~14 000 ms).
- No regression on the existing benchmarks under `benchmarks/`.

## Out of scope for this plan

- Reconcile fast-path that skips the intermediate (Option 5 from the
  menu). Larger redesign; separate plan if pursued.
- `INSERT … ON CONFLICT` for target sync (Option 3). Independent
  optimization; can layer on later.
- Drop `__nonnull_count_*` for NOT-NULL source columns (Option 6).
  Schema-level change with migration; separate plan.
- Multi-IMV cascade shared-scratch. Cross-IMV scope.
