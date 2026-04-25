# pg_reflex 1.2.0 — production-readiness audit

**Date**: 2026-04-24
**Verdict (one line)**: **Ready for controlled production use** (known-shape IMVs, monitored rollout, oncall aware of fallback). **Not yet a drop-in replacement for `REFRESH MATERIALIZED VIEW`** across arbitrary workloads — several well-known perf cliffs remain for specific query shapes.

This audit covers what 1.2.0 is, what it is not, and where the risks are
for a team considering adopting it in a multi-tenant production Postgres
deployment. It deliberately prioritises *verified evidence* over
aspirational claims.

---

## Summary scorecard

| Dimension | Status | Notes |
|---|---|---|
| Correctness of supported query shapes | ✅ Strong | 487 tests; `assert_imv_correct` EXCEPT ALL oracle on every correctness test; fuzz/proptest coverage for aggregates. |
| Error handling in production hot path | ✅ Strong | 0 `.unwrap()` / `panic!` in `src/trigger.rs`, `create_ivm.rs`, `drop_ivm.rs`, `reconcile.rs`, `schema_builder.rs`, `aggregation.rs`, `query_decomposer.rs`, `introspect.rs`, `sql_analyzer.rs`, `window.rs`. Only 3 `.expect(` across all production files (1 in `trigger.rs`, 2 in `lib.rs`). |
| Concurrency (per-IMV flush) | ✅ Strong | Per-(IMV name) advisory-xact locks in both trigger bodies (`schema_builder.rs:362,428,478,550,590`) and flush (`trigger.rs:1340`); 1.1.3 migrated to 2-arg `(hashtext(name), hashtext(reverse(name)))` seeded keys to avoid collisions. Per-IMV savepoint around flush body so one bad IMV doesn't abort the cascade. |
| Concurrency (DDL path) | ⚠️ Partial | `create_reflex_ivm` / `drop_reflex_ivm` / `reflex_reconcile` take no explicit lock beyond Postgres' DDL locks. Two concurrent `create_reflex_ivm('v', ...)` callers will race on the registry `PRIMARY KEY(name)` — one wins, the other errors cleanly. Two concurrent `drop_reflex_ivm` on the same cascade parent have not been stress-tested. |
| Recovery (crash / drift) | ✅ Good | `reflex_reconcile(name)` rebuilds intermediate + target from the registered `sql_query` by scanning the source. `reflex_rebuild_imv` is a public alias. UNLOGGED intermediate is documented as rebuilt by reconcile on crash. |
| Observability | ✅ Good | `reflex_ivm_status()`, `reflex_ivm_stats(name)`, `reflex_explain_flush(name)` — all landed 1.2.0 (`src/introspect.rs`). Registry tracks `last_flush_ms`, `last_flush_rows`, `flush_count`, `last_error`, `last_update_date`. No `pg_stat_statements` integration. |
| Schema-change safety | ⚠️ Warn-only | `reflex_on_ddl_command_end` event trigger raises `WARNING` on source `ALTER TABLE` and tells the operator to run `reflex_rebuild_imv` (`lib.rs:210`). Does **not** block the ALTER; stale IMVs continue serving until manually rebuilt. |
| Source-drop safety | ⚠️ Partial | `reflex_on_sql_drop` deletes registry rows when source is dropped (`lib.rs:171-190`) but **does not drop the IMV's intermediate or target tables**. Orphaned artifacts remain until manually cleaned or dropped via `DROP TABLE`. |
| Migrations | ✅ Continuous | All 9 migration files present (`1.0.0→1.0.1→…→1.1.3→1.2.0`). `ALTER EXTENSION pg_reflex UPDATE` path verified through the chain. |
| TODO/FIXME/HACK hygiene | ✅ Clean | 0 `TODO`, `FIXME`, `XXX`, `HACK`, or `// BUG` comments in `src/**` (excluding tests). |
| Documentation | ⚠️ Partial | README covers API, installation, limitations, monitoring, operational notes. No troubleshooting runbook for "what to do when a flush loops" or "how to decide LOGGED vs UNLOGGED for a given blast radius". CLAUDE.md, journal/ and discussion*.md hold design history — valuable for contributors, not operators. |
| Perf characteristics documented | ⚠️ Partial | README has 1.0.4 bench numbers. 1.2.0 journal (`journal/2026-04-24_1_2_0_bench.md`) captures the scoped-recompute win but is not surfaced. No published per-query-shape SLO table. |

---

## Strengths — evidence

### Test coverage

- **487 tests** in the library test suite as of the 1.2.0 commit (see `journal/2026-04-24_1_2_0_bench.md`).
- **321 integration `#[pg_test]`s** across 15 categorized files (basic, trigger, passthrough, cte, set_ops, window, drop, reconcile, deferred, error, e2e, correctness, filter, distinct_on, no_sigabrt, 1_2_0).
- **152 pure-Rust unit `#[test]`s** across 6 unit files (sql_analyzer, aggregation, schema_builder, trigger, query_decomposer, proptest).
- Every correctness test uses `assert_imv_correct` (the `EXCEPT ALL` oracle), which compares IMV output row-by-row to a fresh direct query. This is as close to a "cannot lie" test as you can get in SQL.
- Proptest / fuzz coverage for aggregate semantics (`src/tests/unit_proptest.rs`, `pg_test_fuzz_min_max_extremum`, `pg_test_randomized_aggregate_correctness`).

### No panic paths in production code

```
src/trigger.rs               unwrap=0 expect=1 panic=0
src/create_ivm.rs            unwrap=0 expect=0 panic=0
src/drop_ivm.rs              unwrap=0 expect=0 panic=0
src/reconcile.rs             unwrap=0 expect=0 panic=0
src/lib.rs                   unwrap=0 expect=2 panic=0
src/schema_builder.rs        unwrap=0 expect=0 panic=0
src/aggregation.rs           unwrap=0 expect=0 panic=0
src/query_decomposer.rs      unwrap=0 expect=0 panic=0
src/introspect.rs            unwrap=0 expect=0 panic=0
src/sql_analyzer.rs          unwrap=0 expect=0 panic=0
src/window.rs                unwrap=0 expect=0 panic=0
```

The single `.expect(` in `trigger.rs` is an internal post-condition
(`grp_cols is Some — checked above`) guarded by the preceding if-let
binding. The two in `lib.rs` are in the `#[cfg(any(test, feature =
"pg_test"))]` helper block — not compiled into the shipping cdylib.

### Concurrency posture

- Per-IMV flush is serialized on a pair-keyed advisory lock derived from
  the IMV name. Two concurrent sessions flushing the same IMV serialize;
  two sessions flushing distinct IMVs do not.
- Each per-IMV flush body is wrapped in its own `SAVEPOINT` with an
  `EXCEPTION WHEN OTHERS` handler that records the SQLERRM into
  `__reflex_ivm_reference.last_error` and continues with the next IMV. A
  single bad IMV (e.g. stale trigger body after a source schema change)
  no longer aborts the cascade.
- Event triggers (`reflex_on_sql_drop`, `reflex_on_ddl_command_end`) run
  in the DDL transaction, so they see a consistent snapshot of the
  registry.

---

## Risks — evidence and mitigation

### R1 — Source DROP orphans IMV artifacts

`lib.rs:171-190` implements `__reflex_on_sql_drop`. It deletes registry
rows where `depends_on` matches the dropped object. It does **not** emit
`DROP TABLE` on the intermediate or target tables, nor clean up the
triggers on surviving sources.

**Impact**: After `DROP TABLE source`, the intermediate table
`__reflex_intermediate_<view>` and target table `<view>` remain in the
schema, empty and unreferenced. They must be cleaned up manually.

**Mitigation for operators**: include a post-drop check in your schema
migration pipeline — `SELECT tablename FROM pg_tables WHERE tablename
LIKE '__reflex_%' OR tablename IN (names-of-dropped-IMVs)` and drop
manually. Or call `drop_reflex_ivm('<view>', TRUE)` *before* the source
`DROP TABLE`.

**Fix budget**: small (~50 lines). Plausible 1.2.1 patch. The reason it
was not in 1.2.0 is conservative: auto-dropping user tables (even
pg_reflex-owned ones) from inside an event trigger has historically been
a source of cascading failure modes.

### R2 — Source ALTER TABLE warns but continues

`lib.rs:192-220` emits `RAISE WARNING 'pg_reflex: source table % was
altered; IMV % may be stale — run SELECT reflex_rebuild_imv(…)'`. The
ALTER proceeds. The stale IMV keeps serving.

**Impact**: reads from the IMV can diverge from reality until someone
runs `reflex_rebuild_imv`. No automated gate.

**Mitigation**: monitor `WARNING` logs, or add a hook in your DDL
deployment tooling that runs `reflex_rebuild_imv` on any alteration of a
tracked source. The registry's `depends_on` column lets you map
alter-source→affected-IMVs mechanically.

**Why not ERROR**: blocking ALTER from inside an event trigger is
possible (`ERROR` inside a `ddl_command_end` rolls the ALTER back) but
risky for operational workflows — legitimate renames/column drops would
fail, surprising operators. Keeping it WARNING is the conservative
default; teams that want strict mode can wrap their own event trigger.

### R3 — Top-K retraction cliff still present

Scoped recompute (1.2.0) turns a full source scan into an
affected-group-sized one. But when many groups are retracted at once
(e.g. daily price update hitting every stock in `stock_chart_*`), the
scan reduces to the full source anyway. The originally-planned bounded
top-K heap is deferred to 1.3.0.

**Impact**: three MIN/MAX-heavy views in the example `db_clone`
deployment (`stock_chart_weekly_reflex`, `stock_chart_monthly_reflex`,
`forecast_stock_chart_monthly_reflex`) remain MATERIALIZED VIEWs, not
IMVs. The supported-views inventory in
`journal/2026-04-22_unsupported_views.md` enumerates these.

**Mitigation**: don't migrate workloads whose retraction pattern
routinely hits >30 % of groups per flush. For such views, plain
`REFRESH MATERIALIZED VIEW` is the known-correct fallback.

### R4 — DEFERRED mode single-session flush

`reflex_flush_deferred(source)` processes one source's pending queue in a
single session (the one that fired `COMMIT`). For very wide cascades
(1000+ IMVs depending on one source), that can stretch the commit path.

**Impact**: commit latency spikes proportional to cascade width. No
observable hazard on correctness; just latency.

**Mitigation**: keep cascades narrow. `reflex_ivm_status()` +
`graph_depth` makes this auditable.

### R5 — Passthrough duplicate-row collapse

Documented limitation: passthrough IMVs use row-matching for incremental
DELETE/UPDATE. If the target view produces rows that are identical across
every column, a single-row source DELETE will remove every matching row.

**Impact**: correctness divergence on views with no de-duplicating key.

**Mitigation**: always include a PK or unique column in a passthrough
IMV's SELECT list. The docs flag this; `create_reflex_ivm` errors if
`unique_columns` is omitted on DELETE-capable passthroughs
(`create_ivm.rs:735-736`).

### R6 — No `pg_stat_statements` integration

`reflex_ivm_stats` returns table size, index count, trigger count, last
flush timing — but does not hook into `pg_stat_statements`. Per-query
flush latency histograms are unavailable.

**Impact**: operators have to rely on `last_flush_ms` (single data
point) rather than percentile distributions.

**Mitigation**: scrape `last_flush_ms` into your observability stack and
compute percentiles externally. 1.3.0 could add a histogram.

### R7 — No automated drift detection

`reflex_reconcile` rebuilds if you think the IMV has drifted. There is
no background task that detects drift. The extension trusts its own
invariants.

**Impact**: if a trigger fails silently (e.g. an UNLOGGED intermediate
table is truncated by a crash without reconcile running) reads return
stale data.

**Mitigation**: schedule `reflex_reconcile` via `pg_cron` on a cadence
matching your SLO. For UNLOGGED IMVs, run it after every crash recovery.

### R8 — Limited exposure to adversarial SQL in IMV definitions

`create_reflex_ivm` validates the view name (`validate_view_name` in
`lib.rs:68-86`) and parses the SQL via `sqlparser`. But the user-supplied
SQL body is eventually interpolated into trigger bodies. A SQL-injection
vector would require the IMV name to be callable by an untrusted user.

**Impact**: in a multi-tenant deployment where untrusted users can call
`create_reflex_ivm`, you must audit the validation path yourself. In
single-admin deployments, negligible.

**Mitigation**: the extension is designed to be called by admins, not
arbitrary tenants. Gate `create_reflex_ivm` / `drop_reflex_ivm` behind
your own RPC layer that sanitizes inputs.

---

## What would change the verdict from "controlled production use" to "drop-in REFRESH replacement"

1. **Top-K heap for MIN/MAX** — unblocks wide-retraction workloads (R3).
2. **Auto-drop intermediate + target on source drop** — closes R1.
3. **pg_stat_statements hook + per-IMV histogram** — closes R6.
4. **Background drift scanner** — closes R7.
5. **Runbook section in README** covering crash recovery, stuck flush
   diagnosis, cascade debugging. None of those exist in the current
   README.

Items 1-4 are ~3-week engineering. Item 5 is a doc sprint.

---

## Recommended deployment profile

**Green light** for:

- Analytical dashboards backed by SUM / COUNT / AVG / COUNT(DISTINCT) /
  BOOL_OR over append-mostly or narrowly-mutated sources.
- Low-hundreds-of-IMVs registries, cascade depth ≤ 3.
- DEFERRED mode where commit latency is acceptable at p99 = (single
  largest flush) × cascade width.
- Environments where schema changes are rare and operators can run
  `reflex_rebuild_imv` post-DDL as part of their change-control runbook.

**Yellow light** (use with caveats):

- MIN/MAX IMVs over large (>10 M row) sources where retraction is
  occasional (< 10 % of groups per flush). Above that threshold, the
  1.2.0 scoping doesn't help and plain REFRESH wins.
- Multi-session concurrent DDL on the same IMV graph. Tested but not
  stress-tested beyond 4 concurrent flush sessions.

**Red light** (do not deploy yet):

- Views relying on `WITH RECURSIVE`, `FULL OUTER JOIN` deltas, or
  `ARRAY_AGG` / `JSON_AGG` — structurally unsupported, see
  `journal/2026-04-22_unsupported_views.md`.
- Mission-critical read paths where stale-on-schema-change is worse than
  downtime. The WARN-only ALTER behaviour (R2) is not acceptable for
  that tier until it becomes configurable.
- Multi-tenant platforms where untrusted users can define IMV SQL
  (R8).

---

## Suggested 1.2.1 / 1.3.0 priority order (based on this audit)

1. **R1 — auto-drop intermediates on source drop** (small patch, big
   operator ergonomics win).
2. **R3 — top-K heap for MIN/MAX** (the headline perf gap).
3. **R7 — background drift scanner** (can be shipped as a pg_cron
   recipe in the docs, not a code change).
4. **R2 — configurable strict/warn mode for ALTER TABLE** (`GUC:
   pg_reflex.alter_source_policy = 'warn'|'error'`).
5. **R5 — accept `unique_columns` inferred from PK** so passthrough IMVs
   on well-keyed sources don't require manual setting.

Items outside this list (pg_stat_statements, runbook docs, extended
stress tests) remain valuable but are not on the critical path to
drop-in-REFRESH parity.
