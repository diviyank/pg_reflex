# pg_reflex 1.2.0 — production-ready release

## Context

`pg_reflex` 1.1.3 shipped the quick-wins sprint (`#3`, `#9`, `#11`, `#12`) and the `BOOL_OR` counter rewrite (`#1`). Two perf cliffs remain (`#2` MIN/MAX top-K, plus a handful of smaller ones from `journal/2026-04-22_optimization_ideas.md`), **seven bugs** from `journal/2026-04-22_bug_report.md` are still open, and the extension has no production-grade observability, operational-safety, or upgrade-migration tooling. This plan consolidates everything remaining into a single **1.2.0** release and cuts a CHANGELOG entry for it.

Verified against the current branch (2026-04-24):

| Item | Status | Evidence |
|---|---|---|
| #1 BOOL_OR algebraic counters | **landed** | `aggregation.rs:276-277, 592-593, 701-707` — two BIGINT counter cols |
| #3 empty-affected DO-block gate | **landed** | `trigger.rs:903-905, 941-943` |
| #5 end-query targeted splice | **landed** | `trigger.rs:332` `inject_affected_filter_before_group_by`, used at `:891` |
| #9 `parallel_safe` | **landed** | `trigger.rs:454, :961` |
| #11 ANALYZE staging delta | **landed** | `trigger.rs:1098` |
| #12 a/b where_predicate | **landed** | `schema_builder.rs:321-327, 440-446, 512-518`, `trigger.rs:1021-1121` |
| Bug #1 63-char identifier truncation | **landed** | `transition_new_table_name` / `transition_old_table_name` / `staging_delta_table_name` imported + used everywhere |
| #2 MIN/MAX top-K heap | **deferred** | no `src/topk.rs`; `build_min_max_recompute_sql` still full-scan |
| `reflex_rebuild_imv` SPI | **deferred** | grep negative; needed for #2 migration + manual recovery |
| Bugs #4–#7, #10–#13 | **deferred** | per `journal/2026-04-22_bug_report.md` resolution log |
| Optimizations #4, #6, #7, #8, #10 | **deferred** | per same journal |
| Observability SPI (`reflex_ivm_status`, `_stats`) | **missing** | none of these names appear in `lib.rs` |
| Source `DROP TABLE` / `ALTER TABLE` event triggers | **missing** | no `event_trigger` / `sql_drop` in code |
| GUC settings (`pg_reflex.*`) | **missing** | no `GucSetting` / `define_custom_*` |

The release goal is: **after 1.2.0, pg_reflex is safe to deploy into a multi-tenant production Postgres with only REFRESH as the known-slower fallback**. The CLAUDE.md priorities apply: correctness first, simplicity second, performance third.

---

## Scope — six themes

1. **#2 MIN/MAX top-K** (the last headline perf item)
2. **Correctness bugs** from the audit: #10 (transitive cycle), #7 (`resolve_column_type` silent TEXT), #6 (GROUP BY cast coerce), #11 (32-bit advisory lock), #4 (user-CTE alias collision), #13 (STRICT nullable), #5 (MERGE default-expr literal). Order: severity × fix cost.
3. **Operational safety**: `reflex_rebuild_imv` SPI, event trigger on source `DROP`/`ALTER`, per-IMV `SAVEPOINT` in cascade flush, concurrent-flush stress test.
4. **Observability**: `reflex_ivm_status()`, `reflex_ivm_stats()`, `reflex_explain_flush(view_name)`, per-IMV last-flush timing & row-count stored in registry.
5. **Perf quick wins** (carry-over): #4 topological skip, #8 lazy index maintenance on bulk rebuild, #10 streaming statement-split. #6/#7 (CTE dedup, template cache) remain deferred — documented in `journal/` as 1.3.0 backlog.
6. **Docs / packaging**: CHANGELOG 1.1.3 + 1.2.0 entries, README "Monitoring" + "Operational notes" sections, `COMMENT ON FUNCTION` for every `pg_extern`, `pg_reflex.control` kept at `@CARGO_VERSION@`, bump `Cargo.toml` to `1.2.0`, migration `sql/pg_reflex--1.1.3--1.2.0.sql`.

Each of the seven "theme-2 bugs" is small enough that one test + one fix lands in ≤ 1 day. #2 is the anchor week.

---

## Files touched (summary)

| Theme | File | Role |
|---|---|---|
| #2 | `src/topk.rs` (new) | pgrx aggregate `reflex_topk_min`/`_max`, scalar `reflex_topk_merge`/`_subtract`. |
| #2 | `src/aggregation.rs` | MIN/MAX arms → `MIN_TOPK`/`MAX_TOPK` intermediate + `(topk)[1]` end-query mapping. `:525-553, :637-651`. |
| #2 | `src/schema_builder.rs` | Resolve `MIN_TOPK`/`MAX_TOPK` to array types. `:58-85, 127-140`. |
| #2 | `src/query_decomposer.rs` | `generate_base_query` emits `reflex_topk_min(col, 16)`. `:403-412`. |
| #2 | `src/trigger.rs` | `build_topk_merge_sql` + `build_topk_subtract_sql`, replace `build_min_max_recompute_sql` with `build_topk_refill_sql` (empty-heap WHERE + affected-groups scope). `:60-84, :255-321, :694-850`. |
| #2 | `src/lib.rs` | Register topk aggregates. |
| op-safety | `src/create_ivm.rs` | `reflex_rebuild_imv(view_name)` SPI entrypoint — factor plan→DDL→repopulate into a reusable fn. |
| op-safety | `src/lib.rs` | `#[pg_extern]` for `reflex_rebuild_imv`. |
| op-safety | `src/trigger.rs` | Wrap per-IMV flush body in a `SAVEPOINT` so one bad IMV doesn't abort the whole cascade. `:1050-1145`. |
| op-safety | `src/drop_ivm.rs` + new event-trigger SQL | Register a `sql_drop` event trigger that calls `drop_reflex_ivm` when a source table/view is dropped; a `ddl_command_end` event trigger that warns (not errors) on source `ALTER TABLE … DROP COLUMN`. |
| bug #10 | `src/create_ivm.rs` | Transitive-closure cycle check before registering `depends_on`. |
| bug #7 | `src/schema_builder.rs` | `resolve_column_type` — WARNING + fallback to NUMERIC / explicit cast. `:638-659`. |
| bug #6 | `src/aggregation.rs` | Force canonical cast on re-extracted GROUP BY expression. `:713-736`. |
| bug #11 | `src/trigger.rs`, `src/schema_builder.rs` | Two-arg `pg_advisory_xact_lock(key1, key2)` seeded from a 64-bit hash helper. |
| bug #4 | `src/query_decomposer.rs` | Pre-scan AST for `__reflex_new_<src>` / `__reflex_old_<src>` CTE alias → error. |
| bug #13 | `src/trigger.rs` | Take `where_predicate` as `Option<&str>`; drop STRICT once any arg can be NULL. |
| bug #5 | `src/trigger.rs` | `build_merge_sql` consults `pg_attrdef.adbin` / `pg_get_expr` for DEFAULT expressions. |
| observability | `src/lib.rs` + new `src/introspect.rs` | `reflex_ivm_status`, `reflex_ivm_stats`, `reflex_explain_flush` pgrx SRFs. |
| observability | `src/lib.rs` migration | Extend `__reflex_ivm_reference` with `last_flush_ms BIGINT`, `last_flush_rows BIGINT`, `flush_count BIGINT`. |
| observability | `src/trigger.rs` | Update those columns at end of each flush (inside the savepoint). |
| perf #4 | `src/trigger.rs` | Extend deferred_pending with `affected_rows`; downstream IMV early-exits when all upstreams produced 0. |
| perf #8 | `src/trigger.rs` | When full-rebuild path fires (threshold: affected rows > 50 % of intermediate), `DROP INDEX IF EXISTS` → INSERT → `CREATE INDEX`. |
| perf #10 | `src/trigger.rs` | New pgrx helper `reflex_execute_separated(sql text)` replacing the `string_to_array` loop in `schema_builder`-generated trigger bodies. |
| packaging | `Cargo.toml` | `version = "1.2.0"`. |
| packaging | `sql/pg_reflex--1.1.3--1.2.0.sql` | Migration: schema extension, ALTER FUNCTION signatures for #11/#13/top-K, `reflex_rebuild_imv` dispatcher loop over BOOL_OR/MIN/MAX-carrying IMVs. |
| docs | `CHANGELOG.md` | 1.1.3 entry (backfill — never written) + 1.2.0 entry. |
| docs | `README.md` | New "Monitoring" section, "Operational notes" section, update Known Limitations (drop BOOL_OR/MIN/MAX caveats; add concurrency + source-schema-change guidance). |
| docs | `sql/pg_reflex--1.1.3--1.2.0.sql` head comment + each `#[pg_extern]` fn | `COMMENT ON FUNCTION …`. |
| bench | `benchmarks/bench_1_2_0.sql` (new) + `benchmarks/run_bench.sh` | Consolidated rerun (see §Benchmark). |

---

## Theme 1 — #2 MIN/MAX top-K heap (anchor)

Follow the design already in `plans/once-upon-an-algebraic-heap.md` §"#2". That design is **unchanged by this sprint** — no exploration surfaced new hazards. Summary:

- **Intermediate column**: `__min_<arg>_topk <elem_type>[]` (array of source element type). `source_aggregate = "MIN_TOPK"` (sibling: `"MAX_TOPK"`).
- **End-query mapping**: `(__min_<arg>_topk)[1]` (arrays are 1-indexed; empty array returns NULL, matching `MIN` over empty input).
- **base_query**: new aggregate `reflex_topk_min(col, 16)` / `reflex_topk_max`. K=16, compile-time. Registered in `src/topk.rs` as a pgrx `#[pg_aggregate]`.
- **Merge (Add)**: `__min_X_topk = reflex_topk_merge(t.__min_X_topk, d.__min_X_topk, 16, 'asc')` — union-sort-truncate.
- **Subtract**: `__min_X_topk = reflex_topk_subtract(t.__min_X_topk, d.__min_X_topk, 'asc')` — lockstep walk, one occurrence per element.
- **Empty-heap refill**: replaces `build_min_max_recompute_sql`. `WHERE array_length(t.__min_X_topk, 1) IS NULL AND __ivm_count > 0 AND grp IN (affected_tbl)` — only groups whose heap emptied get rescanned, scoped to affected rows. Same JOIN-alias wrapping as the current recompute (reuse the `(orig_base_query) AS __src` wrapper from the 2026-04-21 fix).

### Migration

- Needs `reflex_rebuild_imv(view_name)` (theme 3 delivers it anyway). Migration dispatcher iterates `__reflex_ivm_reference` rows whose `aggregations::text LIKE '%"MIN"%' OR '%"MAX"%'` and calls it.
- Savepoint per IMV. Report `_rebuilt` + `_skipped` with `RAISE NOTICE`.
- Document up-front that on 30 M-row sources this rebuild is minutes-per-IMV.

### Tests (write-first)

From `plans/once-upon-an-algebraic-heap.md` §Tests, tests 20–37. All land unmodified. Key correctness cases:

- Retraction past K → refill fires, MIN == K+1-th row.
- NULL values ignored (Postgres MIN semantics).
- Full group deletion → `__ivm_count = 0` gate removes row.
- TEXT-typed MIN (array-of-text).

### Out of scope for 1.2.0

Per-IMV K tuning (`WITH (topk_size = N)`), streaming low-water-mark refill, polymorphic-type edge cases (DOMAIN-wrapped numerics). Deferred to 1.3.0.

---

## Theme 2 — correctness bug fixes

For each bug: one failing test, one fix, one commit. In the order below.

### Bug #10 — transitive cycle detection (`create_reflex_ivm`)

- Before inserting into `__reflex_ivm_reference`, walk `depends_on` transitively via BFS across existing rows. If any visited node equals the new view name, error `ERROR: creating <name> would introduce cycle through <path>` and exit.
- Test: build A (depends on t), B (depends on A and a CTE that resolves back to A). Assert rejection.

### Bug #11 — 64-bit advisory lock

- Introduce `fn reflex_advisory_key(name: &str) -> (i32, i32)` that hashes the name to 64 bits (`seahash` or a plain `u64` from `sha2` truncated) and splits into two i32s.
- Use `pg_advisory_xact_lock(key1, key2)` instead of `pg_advisory_xact_lock(hashtext(name))` in both plpgsql trigger bodies (`schema_builder.rs`) and `reflex_flush_deferred` (`trigger.rs`).
- Test: two IMVs whose `hashtext` values would collide shouldn't deadlock / serialize under the new scheme. Crafting a collision pair is hard — substitute a unit test asserting the two-key form is emitted.

### Bug #7 — `resolve_column_type` silent TEXT

- In `schema_builder.rs:638-659`, on catalog lookup failure emit `pgrx::warning!` and default to `NUMERIC` (not TEXT). For obviously text-typed expressions this may produce a cast error at CREATE time — that's preferable to silent behavior drift.
- Test: IMV with `GROUP BY (col_a + col_b)` — assert WARNING is emitted and column type is `NUMERIC`.

### Bug #6 — GROUP BY expression cast coercion

- In `aggregation.rs:713-736`, when the extra column is an expression, wrap the re-extracted value in a cast to the intermediate column type. `IS NOT DISTINCT FROM` on mismatched types returns false; the cast prevents that.
- Test: `GROUP BY CAST(order_id AS TEXT)` — INSERT+flush, affected-groups filter matches intermediate row.

### Bug #4 — user CTE name collision

- In `query_decomposer.rs::replace_source_with_transition`, before the first substitution, walk the AST for any CTE aliased `__reflex_new_<src>` / `__reflex_old_<src>` / `__reflex_delta_<src>`. If found, `ereport ERROR: CTE alias %s collides with pg_reflex reserved name`.
- Test: CREATE IVM with a handcrafted colliding CTE. Assert error.

### Bug #13 — STRICT + future-nullable argument

- `reflex_build_delta_sql` — change last `where_predicate` argument to `Option<&str>` explicitly and annotate `#[pg_extern(parallel_safe, strict = false)]` (pgrx syntax — verify exact attribute; otherwise split into two overloads).
- Test: call with NULL `where_predicate` from plpgsql; assert non-NULL return. Today this already works because the arg isn't on the signature — the test is forward-looking.

### Bug #5 — MERGE INSERT defaults read catalog

- In `trigger.rs::build_merge_sql`, for each column not driven by the aggregation plan, consult `pg_attrdef.adbin` via `pg_get_expr(adbin, adrelid)` to get the real DEFAULT expression. Emit `DEFAULT` in the column list so Postgres substitutes it, rather than emitting a literal.
- Test: intermediate row has `DEFAULT now()` on a user-added column; assert the row's value is close-to-`now()`, not epoch.

---

## Theme 3 — operational safety

### 3.1 `reflex_rebuild_imv` SPI

Factor the "plan → DDL → repopulate" steps from `create_reflex_ivm_impl` into a reusable private fn (`fn rebuild_inner(view_name: &str, row: &IvmRow)`). Both `create_reflex_ivm` and the new `#[pg_extern] fn reflex_rebuild_imv(name: &str)` call it.

`reflex_rebuild_imv`:
1. Load registry row for `view_name`.
2. Re-run `plan_aggregation` + `generate_base_query` + `generate_end_query` on stored `sql_query`.
3. DROP intermediate + target, preserving index definitions for recreation.
4. CREATE fresh intermediate + target.
5. INSERT base_query into intermediate; INSERT end_query into target.
6. UPDATE registry row with new base/end/aggregations JSON.
7. Rebuild trigger DDLs on source tables.

Used by: the #2 migration dispatcher, future schema-change migrations, and manual recovery (drift, etc.).

### 3.2 Event trigger on source DROP

- `src/drop_ivm.rs` + migration SQL: register a `sql_drop` event trigger `__reflex_on_source_drop` that:
  - Iterates `pg_event_trigger_dropped_objects()` for tables/views.
  - For each dropped object, looks up any IMV in `__reflex_ivm_reference` where `dropped_name = ANY(depends_on)`.
  - Calls `drop_reflex_ivm(imv_name)` with `cascade=TRUE` to clean up.
  - Emits `RAISE NOTICE 'pg_reflex: auto-dropped IMV %s because source %s was dropped'`.
- Test: CREATE IMV on table `t`; `DROP TABLE t`; assert the IMV's intermediate + target + triggers are gone and the registry row is removed.

### 3.3 Source `ALTER TABLE … DROP COLUMN` warning

- `ddl_command_end` event trigger that walks `pg_event_trigger_ddl_commands()` for `ALTER TABLE` on source tables. If the dropped column is referenced by any IMV (check against `base_query` text or `parsed_sql_query`), emit `RAISE WARNING 'pg_reflex: dropping column %s from %s will leave IMV %s stale; call reflex_rebuild_imv() to recover'`. **Warning, not error** — users may be intentionally dropping and recreating.
- Test: skip in automated tests (DDL events are awkward to test in pgrx); cover via manual regression doc.

### 3.4 Per-IMV SAVEPOINT in cascade flush

In `reflex_flush_deferred` at `trigger.rs:1050-1145`, wrap each per-IMV body in a `SAVEPOINT __reflex_sp_<i>`; on error, `ROLLBACK TO SAVEPOINT`, log `pgrx::warning!`, continue to next IMV. Motivation: today one bad IMV aborts the whole cascade and rolls back upstream work.

- Test: two IMVs, second has a broken `base_query` (e.g., type mismatch). First flushes correctly; second logs warning; registry records first as updated, second as unchanged.

### 3.5 Concurrent-flush stress test

- Add `tests/concurrent_flush.sh` (bash + psql) that spawns 4 concurrent sessions each doing `INSERT + reflex_flush_deferred` on the same source, 1000 iterations. Assert no deadlocks, intermediate + target match oracle at end.
- Already have one concurrent test per CHANGELOG — extend it to cover the post-1.1.3 flush path with advisory-lock changes.

---

## Theme 4 — observability

### 4.1 Registry columns

Add `last_flush_ms BIGINT`, `last_flush_rows BIGINT`, `flush_count BIGINT`, `last_error TEXT` to `__reflex_ivm_reference`. Migration: `ALTER TABLE … ADD COLUMN … DEFAULT NULL`.

Update `reflex_flush_deferred` to record timing + row count inside the per-IMV savepoint; on error (from theme 3.4), stash the error message into `last_error`, otherwise clear it.

### 4.2 SPI functions (`src/introspect.rs`)

- `reflex_ivm_status() RETURNS TABLE(name TEXT, graph_depth INT, enabled BOOLEAN, refresh_mode TEXT, row_count BIGINT, last_flush_ms BIGINT, last_flush_rows BIGINT, flush_count BIGINT, last_error TEXT, last_update_date TIMESTAMP)` — summary per IMV. `row_count` is live — `SELECT count(*) FROM <target>`, cheap enough for operator use.
- `reflex_ivm_stats(view_name TEXT) RETURNS TABLE(metric TEXT, value TEXT)` — detailed: intermediate size in bytes, target size, index count, trigger count, last flush timing breakdown.
- `reflex_explain_flush(view_name TEXT) RETURNS TEXT` — calls `reflex_build_delta_sql` with a synthetic 1-row delta and `EXPLAIN (ANALYZE FALSE, VERBOSE)`s the output. Lets operators diagnose "what would the next flush do?" without actually firing one.

### 4.3 `COMMENT ON FUNCTION`

Add to every `#[pg_extern]`: one-sentence description. `sql/pg_reflex--1.1.3--1.2.0.sql` emits the `COMMENT ON FUNCTION` after the extension install.

### 4.4 Tests

- `test_ivm_status_reports_registered_imv` — create IMV, call `reflex_ivm_status`, assert row present with correct fields.
- `test_flush_records_timing_and_row_count` — insert, flush, check `last_flush_ms > 0` and `last_flush_rows == 1`.
- `test_explain_flush_contains_query_plan` — create IMV, call `reflex_explain_flush`, assert result contains `Seq Scan` or `Index Scan`.

---

## Theme 5 — perf carry-over (quick, selective)

Only include the items whose ROI is ≥ 5–10 %. #6 (CTE dedup, 1-week effort) and #7 (trigger template cache, 5-day effort) are **deferred to 1.3.0** per the effort-vs-gain ratio from `journal/2026-04-22_optimization_ideas.md`.

### 5.1 #4 topological cascade skip

- Extend `__reflex_deferred_pending` with per-IMV `affected_rows BIGINT`.
- After each IMV flushes, record its `affected_rows` count (size of `__reflex_affected_<view>` at end of delta).
- Before a downstream IMV processes, check: if **all** its upstreams in the current cascade produced `affected_rows = 0`, skip the IMV entirely.
- Test: 3-depth chain A→B→C. INSERT a row that's filtered out at A's WHERE — downstream B and C don't run.

### 5.2 #8 lazy index maintenance on bulk rebuild

- In `reflex_flush_deferred`, when a flush is about to write > 50 % of the intermediate (heuristic: `affected_rows / intermediate_size > 0.5`), emit `DROP INDEX IF EXISTS … ; INSERT …; CREATE INDEX …` instead of incremental maintenance.
- Threshold is a GUC: `pg_reflex.lazy_index_threshold` default 0.5. Add via `GucSetting<f64>` in `lib.rs` module init. If introspectability proves finicky in pgrx, pin to a constant 0.5 and defer the GUC.
- Test: seed 100K-row intermediate; delete 80 % in one transaction; flush; assert flush latency is within 30 % of a full REFRESH + `CREATE INDEX`.

### 5.3 #10 streaming statement split

- New `#[pg_extern]` `reflex_execute_separated(sql: &str)` in `trigger.rs` that walks the input for `--<<REFLEX_SEP>>--` markers and calls `Spi::run` on each slice. Replaces the `string_to_array` + `FOREACH` loop in the `schema_builder.rs` trigger bodies.
- Test: generate a 5-statement cascade; assert all 5 run.

---

## Theme 6 — docs / packaging

### 6.1 CHANGELOG

Backfill a **1.1.3** entry (currently missing) summarizing the quick-wins sprint + BOOL_OR rewrite. Then add a **1.2.0** entry with all six themes.

### 6.2 README

- **Drop** the "MIN/MAX/BOOL_OR on DELETE requires full group rescan" limitation.
- **Add** "Operational notes" section:
  - Concurrent-flush guidance (advisory locks, savepoint isolation).
  - Source schema change behavior (event triggers).
  - Upgrade path for MIN/MAX columns (rebuild scan cost).
  - Monitoring queries using `reflex_ivm_status`.
- **Add** "Troubleshooting" with common failure modes (drift → `reflex_reconcile`, schema change → `reflex_rebuild_imv`).

### 6.3 `COMMENT ON FUNCTION`

Emit from migration SQL for every `#[pg_extern]`:

```sql
COMMENT ON FUNCTION create_reflex_ivm(TEXT, TEXT, TEXT, TEXT, TEXT) IS
  'Create an incrementally-maintained view. See README.md#api.';
```

One-liner per function — 9 functions as of the 1.2.0 signature set.

### 6.4 Migration — `sql/pg_reflex--1.1.3--1.2.0.sql`

In order:
1. `ALTER TABLE __reflex_ivm_reference ADD COLUMN last_flush_ms BIGINT, …` (theme 4.1).
2. `CREATE EVENT TRIGGER __reflex_on_source_drop …` (theme 3.2).
3. `CREATE EVENT TRIGGER __reflex_on_source_alter …` (theme 3.3).
4. `ALTER FUNCTION` re-signatures for `reflex_build_delta_sql` if theme-2 bug #13 changes the arg list.
5. `DO $migration$ BEGIN FOR _rec IN … WITH MIN/MAX/BOOL_OR aggs LOOP PERFORM reflex_rebuild_imv(_rec.name); END LOOP; END; $migration$;` — theme 1 migration dispatcher.
6. `COMMENT ON FUNCTION …` for every exposed fn.

### 6.5 `Cargo.toml`

```toml
version = "1.2.0"
```

`.control` stays at `@CARGO_VERSION@`.

---

## Implementation order

Rough week-by-week — sprint scope is 3–4 weeks:

- **Week 1** — Theme 2 (correctness bugs) + theme 3 (op-safety). All tests-first. Lands: `reflex_rebuild_imv`, event triggers, per-IMV savepoint, 7 bug fixes. Every commit is one test + one fix.
- **Week 2** — Theme 4 (observability) + theme 5.3 (streaming exec) + theme 5.1 (topological skip). Registry schema migration + introspection SPI.
- **Weeks 3–4** — Theme 1 (MIN/MAX top-K). Longest item; integrates with the `reflex_rebuild_imv` from week 1.
- **Week 4 (end)** — Theme 5.2 (lazy index threshold), theme 6 (docs), consolidated benchmark.

Each commit runs `cargo fmt && cargo clippy --all-targets --no-deps -- -D warnings && cargo pgrx test` on PG 18 locally. CI also runs PG15/16/17 per the existing matrix.

---

## Benchmark (end-of-sprint, required)

Follow the methodology in `journal/2026-04-22_db_clone_benchmark_rerun.md`. Compare against the **1.1.3 baseline captured there** (same trial pattern: `BEGIN; UPDATE <src> LIMIT 1000; SELECT reflex_flush_deferred('<src>'); ROLLBACK;`).

### Driver

- Reuse `/tmp/pg_reflex_bench_rerun/run_flush.sh` and `run_refresh.sh` (see `memory/reference_benchmark_data.md`).
- Fresh scratch DB = `db_clone` snapshot (`/home/diviyan/fentech/algorithm/api/base-db-anchor-evm/base_db/sql/`).
- Install `1.1.3`, then `ALTER EXTENSION pg_reflex UPDATE TO '1.2.0'` so the migration rebuild path is exercised.

### IMVs measured

14 from the 2026-04-22 run, plus the **three currently matview-only views** that #2 unlocks:

- `stock_chart_weekly_reflex`
- `stock_chart_monthly_reflex`
- `forecast_stock_chart_monthly_reflex`

Convert those three to IMVs for this bench only (via `create_reflex_ivm` on their SQL).

### Success criteria

| Area | Target |
|---|---|
| #2 MIN/MAX (3 stock_chart views) | flush within 2× of REFRESH (today: would be 10–100× slower without top-K) |
| #4 topological skip | `sales_simulation` cascade when 0 rows changed: flush total < 2 s (today 3–6 min) |
| #8 lazy index | full-rebuild case within 30 % of fresh REFRESH + CREATE INDEX |
| #10 streaming exec | < 5 % regression on any existing flush (it's a no-op win on small cascades) |
| bugs #4–#13 | no correctness regression on existing 14 IMVs |
| observability | `reflex_ivm_status()` returns in < 50 ms on a registry with 20 IMVs |
| event triggers | `DROP TABLE <source>` succeeds and auto-removes 3 dependent IMVs |
| concurrent flush | 4 sessions × 1000 iters = 0 deadlocks, oracle passes |

### Regression gate

If any existing win from the 2026-04-22 run (`sop_purchase_baseline_reflex` 2.4×, `sop_purchase_reflex` 3.75×, `forecast_stock_chart_weekly_reflex` 145×) regresses by >10 %, investigate before release. Per CLAUDE.md: "is it worth the hassle". Revert the offending subfeature if the gain-elsewhere doesn't justify it.

### Output

`journal/<date>_1_2_0_bench.md`:
- Per-IMV flush ms / REFRESH ms / speedup vs 1.1.3 column.
- Separate commentary on the unlocked stock_chart views.
- Call out any surprises (cold-cache effects, planner-pick differences under PG18).
- Update `journal/2026-04-22_unsupported_views.md` §6 to move the 3 MIN/MAX views to "supported".

---

## Verification (end-to-end)

Before tagging 1.2.0:

1. `cargo pgrx test --features pg18` green.
2. `cargo clippy --all-targets --no-deps -- -D warnings` green.
3. Fresh install smoke: `DROP EXTENSION pg_reflex; CREATE EXTENSION pg_reflex; SELECT reflex_ivm_status();` returns empty.
4. Upgrade smoke: install 1.1.3 → create BOOL_OR + MIN + MAX + passthrough IMVs → `ALTER EXTENSION pg_reflex UPDATE TO '1.2.0'` → verify (a) migration notice counts are correct, (b) `reflex_ivm_status()` reports all 4 IMVs with correct `graph_depth`, (c) INSERT+flush still correct on all 4 (assert_imv_correct).
5. Event-trigger smoke: CREATE IMV on `t` → `DROP TABLE t CASCADE` → `SELECT name FROM __reflex_ivm_reference` returns 0 rows.
6. Concurrent-flush smoke: `tests/concurrent_flush.sh` ↯ 4 × 1000 iters green.
7. Benchmark results documented in `journal/`.

---

## Out of scope for 1.2.0 (moved to 1.3.0 backlog)

- #6 CTE dedup across sibling IMVs (1 week effort; 10–30 % gain on sop/supply).
- #7 Trigger template cache (5 days; 5–20 % gain on IMMEDIATE mode; users overwhelmingly run DEFERRED).
- Per-IMV K tuning for MIN/MAX (`WITH (topk_size = N)`).
- `FULL OUTER JOIN` delta maintenance (§4 of unsupported-views journal).
- `ARRAY_AGG` / `JSON_AGG` — structurally unalgebraic for UNORDERED, top-K-bound for ORDERED; no clear win.
- `WITH RECURSIVE` support (rejected by design).
- `UNION ALL` inside a CTE (§7 of unsupported-views journal — decomposer rewrite is its own sprint).
- Remote-pg-reflex support (cross-DB IMVs) — not on roadmap.
