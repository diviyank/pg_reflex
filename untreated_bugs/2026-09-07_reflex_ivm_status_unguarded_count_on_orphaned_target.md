# 2026-09-07 — `reflex_ivm_status` errors on any registry row whose target relation is gone

**Status: untreated.** Discovered while implementing Task 3 of the
2026-09-07-pg-reflex-silent-wipe plan (the durable maintenance event log), while
assessing whether widening `has_anomaly` (`src/introspect.rs`) to also consult
`__reflex_event_log` made this reachable in a new way. It does not — see "What was
ruled out" below — so it is filed here instead of folded into that work.

## The mechanism

`reflex_ivm_status`'s row-count pass (`src/introspect.rs`, the `map` over `rows` in
`reflex_ivm_status`) picks one of two queries per IMV depending on `has_anomaly`:

```sql
-- anomaly branch (exact count, no guard):
SELECT COUNT(*)::BIGINT AS c FROM {ident}

-- non-anomaly branch (looks guarded by to_regclass, but isn't):
SELECT CASE WHEN c.reltuples > 0 THEN c.reltuples::BIGINT
            ELSE (SELECT COUNT(*)::BIGINT FROM {ident}) END AS c,
       (c.reltuples > 0) AS is_estimate
FROM pg_class c WHERE c.oid = to_regclass('{name_lit}')
```

The doc comment above this code used to claim "Missing target → to_regclass NULL →
no row → the -1 sentinel", implying the second query degrades gracefully when the
target has been dropped out from under a still-registered `__reflex_ivm_reference`
row. That claim is false. Verified empirically with `psql`:

```sql
SELECT CASE WHEN c.reltuples > 0 THEN c.reltuples
            ELSE (SELECT count(*) FROM definitely_missing_xyz) END
FROM pg_class c WHERE c.oid = to_regclass('public.definitely_missing_xyz');
-- ERROR: relation "definitely_missing_xyz" does not exist
```

PostgreSQL resolves every relation reference in a query at parse-analysis time,
regardless of which `CASE` branch or subquery contains it and regardless of whether
that branch will actually execute for any row — the `to_regclass` check in the
`WHERE` clause never gets a chance to short-circuit the reference embedded in the
`SELECT` list, because parsing happens before any row is evaluated. **Both branches
raise identically** on a genuinely-dropped target; the "guard" on the non-anomaly
branch never worked.

## Why it matters

`reflex_ivm_status()` iterates every row of `__reflex_ivm_reference` in one call. If
even one registered IMV's target relation has been dropped (a manual `DROP TABLE`
bypassing `drop_reflex_ivm`, or an orphaned row from an incomplete drop) — regardless
of whether that row happens to carry `known_stale`/`last_error`, i.e. regardless of
which of the two branches above it takes — the whole `reflex_ivm_status()` call
errors instead of reporting a sentinel and continuing. One bad row takes down status
reporting for every IMV, including healthy ones.

## What was ruled out

Task 3 (2026-09-07-pg-reflex-silent-wipe) added a third `has_anomaly` term: an
unresolved `__reflex_event_log` entry (Task 3, refined in review to be
ANALYZE-recency-scoped for 'rebuild' rows and unconditional for 'error' rows). This
was checked, not assumed, to see whether it makes the above reachable in a way it
wasn't before: it does not. Since **both** branches already error identically on any
dropped target — independent of `has_anomaly`'s value — routing a few additional rows
(ones with an event-log entry but no `known_stale`/`last_error`) from the
"non-anomaly" branch to the "anomaly" branch changes nothing about whether the
overall call errors on an orphaned row. That was already guaranteed before Task 3,
for every orphaned row, via the non-anomaly branch alone.

## Reproduction sketch

```sql
SELECT create_reflex_ivm('t', 'SELECT 1 AS x', 'x');
DROP TABLE t;  -- bypasses drop_reflex_ivm; __reflex_ivm_reference row for 't' survives
SELECT * FROM reflex_ivm_status();
-- ERROR: relation "t" does not exist
```

Severity: medium. Requires an orphaned registry row (target dropped without going
through `drop_reflex_ivm`), which is already an unsupported/abnormal state this
project's audit checks (`checks_a_catastrophic.rs`, `checks_c_orphan.rs`) exist to
detect — but unlike those checks, `reflex_ivm_status()` has no defense of its own and
takes down status reporting for every OTHER (healthy) IMV in the same call, not just
the orphaned one.

## Fix direction

Guard both branches with an actual existence check that happens BEFORE the query
referencing `{ident}` is even built/executed — e.g. resolve `to_regclass(name)` in
Rust first (a separate `Spi::get_one::<i64>` returning the oid, or a boolean
existence flag) and short-circuit to the `-1` sentinel (with `is_estimate = false`)
without ever formatting `{ident}` into a query, for any row whose target does not
resolve. This mirrors how `count_rows`/`event_log_table_exists` in `src/partition.rs`
already check existence via a preceding query rather than relying on `to_regclass`
inside the same statement that references the identifier. Pin it with a test that
registers an IMV, drops its target directly (not via `drop_reflex_ivm`), and asserts
`reflex_ivm_status()` still returns a row for it (sentinel `-1`, `is_estimate =
false`) instead of erroring, and that OTHER healthy IMVs' rows are unaffected in the
same call.
