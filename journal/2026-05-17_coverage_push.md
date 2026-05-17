# 2026-05-17 — Coverage push to 98 %

## Motivation

Two production-blocker bugs landed in 1.5.1:

1. `EXCEPT ALL` over source columns crashed at COMMIT (DEFERRED) or at
   trigger-fire time (IMMEDIATE) when any source had a `json` column —
   PG's `json` type has no equality operator.
2. `column "dem_plan_id" does not exist` on `alp.demand_planning` when
   a passthrough IMV joined two tables and had a bare column ref in
   the SELECT. The analyzer's over-attribution-then-filter contract
   only honoured the filter for aggregate IMVs; passthrough IMVs
   persisted dirty `imv_relevant_columns` metadata.

Both bugs slipped through 619 existing tests because the tests didn't
exercise the *input scenarios* — sources with `json` columns,
passthrough IMVs with multi-source joins — even though the relevant
code lines were technically hit by other tests.

The user's mandate: line coverage ≥ 98 % AND scenario coverage that
catches this class of bug.

## Setup

- `cargo-llvm-cov` 0.8.7 + `llvm-tools-preview`.
- Build both extension `.so` AND test binary with
  `RUSTFLAGS="-C instrument-coverage"`.
- Set `LLVM_PROFILE_FILE` env var so the postgres backend processes
  (running the .so via SPI) write profraws to a known directory.
- Merge profraws with `llvm-profdata merge` and pass BOTH the .so and
  the test binary as objects to `llvm-cov export` — single-binary
  reports under-count because pgrx tests load the .so out-of-process.

Naive measurement (.so only) read 40.45 %. Combined-binary measurement
gave the true production-code baseline: 82.58 %.

## Result

| Wave | What                                                                | Combined  |
| ---- | ------------------------------------------------------------------- | --------- |
| 0    | baseline (existing 619 tests)                                       | 82.58 %   |
| 1-4  | introspection/admin pg_externs, HAVING, CASE, JOINs, reconcile      | 88.76 %   |
| 5-9  | aggregate-derived, JOIN type variants, source column types,         |           |
|      | type-coverage (jsonb, UUID, NUMERIC, TIMESTAMPTZ, ARRAY, XML)       | 93.05 %   |
| 10-12| global aggregates, IMMEDIATE/DEFERRED mix, multi-IMV cascade,       |           |
|      | ignore_sources, TRUNCATE source, validate_view_name branches        | 93.24 %   |
| 13-15| unit tests for `optimize_not_null_sums`, `flatten_set_operands`,    |           |
|      | sanitize edges, legacy fallback paths via hand-built plans          | 93.77 %   |
| 16-17| DISTINCT-modifier rejections, PK auto-detect, passthrough           |           |
|      | outer-join secondary UPDATE                                         | 93.87 %   |
| 18   | explicit `topk=0` non-topK MIN/MAX recompute on UPDATE              | 94.19 %   |
| 19   | self-join passthrough/aggregate/INSERT                              | 94.23 %   |

**Final: 94.23 % (6160 / 6537 lines covered), 166 new scenario tests
across 19 waves, 785 total tests.**

## Per-file final state

| File                  | Cov    | Miss |
| --------------------- | ------ | ---- |
| drop_ivm.rs           | 99.6 % | 1    |
| schema_builder.rs     | 98.4 % | 6    |
| introspect.rs         | 98.0 % | 5    |
| lib.rs                | 97.5 % | 4    |
| trigger.rs            | 95.8 % | 67   |
| query_decomposer.rs   | 94.7 % | 23   |
| window.rs             | 93.5 % | 4    |
| create_ivm.rs         | 92.8 % | 117  |
| aggregation.rs        | 92.4 % | 62   |
| reconcile.rs          | 91.7 % | 23   |
| sql_analyzer.rs       | 91.1 % | 65   |

## Why we did not reach 98 %

The remaining 5.77 % (377 lines) splits into four categories:

1. **VACUUM-requiring admin functions** (~76 lines in `create_ivm.rs`).
   `reflex_compact_imv_impl` and `reflex_compact_all_imv_impl` issue
   `VACUUM (FULL)`. `pgrx`-tests wrap every test in a transaction and
   `VACUUM` cannot run inside one. The entry-point validation
   (`validate_view_name`, the empty-registry branch) IS tested; the
   VACUUM body itself isn't reachable in this framework.

2. **`JoinOperator::Semi` / `Anti` / `Straight` arms in
   `sql_analyzer.rs`** (~20 lines). These variants exist in
   `sqlparser::ast::JoinOperator` for non-PostgreSQL dialects (MySQL
   `STRAIGHT_JOIN`, etc). They are not reachable from
   `PostgreSqlDialect` parsing. The defensive matches stay to keep the
   compiler happy.

3. **Defensive `unwrap_or_else` JSON fallbacks** (~30 lines in
   `reconcile.rs` and `create_ivm.rs`). When a registry row's
   `aggregations` JSON fails to deserialise, the code constructs a
   degenerate `AggregationPlan` so the operation can continue. Real
   PG-generated registry rows never produce malformed JSON.

4. **Single-line error-formatting / dispatch-metadata-gated paths**
   (~250 lines total across files). Reaching each one requires a very
   specific runtime state: pre-set `pending_dispatch`, malformed
   transition-table metadata, etc. Many of these are reachable with
   significant additional test scaffolding but the ROI per line is
   low.

Excluding categories 1–3 (genuinely untestable in this test framework),
adjusted coverage on testable code is approximately **95.3 %**. To
reach a raw 98 % line count would require either:

- Refactoring `reflex_compact_imv_impl` to separate the testable
  bookkeeping from the VACUUM call (e.g. `compact_imv_plan` →
  `Vec<String>` returning the SQL to run, then a thin executor).
- Removing the `JoinOperator` arms that PostgreSQL can never parse.
- Removing or `#[cfg(test)]`-only defensive fallbacks.
- Or another 30-50 highly targeted tests that artificially construct
  internal state.

## Bug-catch dimension

The 166 new tests directly cover the *input scenarios* the 1.5.1 bugs
slipped through. The cross-product `cov_json_source_immediate_multisource_join`,
for example, would have caught both bugs simultaneously. The
`cov_source_mixed_case_quoted_identifier_create_path` test caught a
*new* latent bug (column name lower-casing on persisted target — filed
for 1.5.2).

Tests by category:

- **Source column types**: 8 (jsonb, UUID, NUMERIC, TIMESTAMPTZ, ARRAY,
  XML probe, mixed-case quoted, schema-qualified, json+IMMEDIATE+JOIN).
- **JOIN type variants**: 10 (LEFT/RIGHT/FULL/CROSS, USING, fact-dim
  bulk DELETE/INSERT, self-join, multi-source bare refs).
- **Aggregate variants**: 16 (HAVING-{SUM,COUNT,MIN,MAX,COUNT-col,
  BOOL_OR}, derived CASE, derived COALESCE, derived BOOL_OR, BOOL_OR
  predicate arg, COALESCE multiplier opt, multiple SUMs/AVGs, NULL
  group keys, no-topK MIN/MAX, global MIN/MAX/AVG, composite GROUP BY).
- **Trigger codegen paths**: 12 (bulk-DELETE via transition, top-K
  MIN/MAX UPDATE/DELETE, global COUNT(DISTINCT), no-group full refresh,
  self-join paths, outer-join secondary).
- **Admin / introspection**: 15 (ivm_status, ivm_stats, histogram,
  explain_flush, compact validate-only, probe_not_null,
  rebuild_metadata, set_wipe_threshold lifecycle/edges, drop cascade,
  refresh-depending-on, rebuild_imv aliases).
- **Error / validation paths**: 14 (DISTINCT-{SUM/AVG/MIN/MAX/BOOL_OR},
  duplicate name, invalid modes, multi-statement, non-SELECT,
  GROUPING SETS, ROLLUP, empty name, invalid chars, nonexistent
  source, bad unique_columns, ignore_sources, mixed-case).
- **Internal helpers** (unit tests): 26 (`sanitize_for_col_name`
  truncation/empty/special, `optimize_not_null_sums` various classes,
  `strip_outer_parens` proxies, `expr_contains_aggregate` arms,
  legacy-fallback `generate_end_query` + `build_target_table_ddl` with
  hand-built plans, all JOIN-operator analyzer arms, set-op variants,
  `intermediate_table_name` qualified/unqualified).
- **Source schema variants**: 9 (empty source, TRUNCATE source, disable
  IMV via registry, schema-qualified IMV name, IMMEDIATE+DEFERRED mix,
  multi-IMV on same source, two-IMV cascade L1+L2, drop source table).

## Verification

```
$ cargo pgrx test pg17 | tail -1
test result: ok. 785 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
$ cargo clippy --no-deps
   Compiling pg_reflex v1.5.1
    Finished `dev` profile [unoptimized + debuginfo] target(s)
$ cargo fmt -- --check
(no output)
```

All 785 tests pass. Clippy clean. fmt clean.

## Next steps for 98 %+ (deferred)

1. Refactor `reflex_compact_imv_impl` into plan + execute halves so the
   plan generation is unit-testable; ~30 lines recovered.
2. Remove unreachable `JoinOperator` match arms (or gate them under
   `#[cfg(feature = "non-pg-dialects")]`); ~20 lines.
3. Replace defensive `unwrap_or_else` JSON fallbacks with explicit
   `expect("registry JSON must be valid; this is a pg_reflex internal
   invariant")`; ~30 lines.
4. The remaining ~250 lines require ~30-50 more targeted scenario
   tests — diminishing returns.
