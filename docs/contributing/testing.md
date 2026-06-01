# Testing

```bash
# Full integration suite (1120 tests as of 1.7.6)
cargo pgrx test pg17

# Unit tests only — no Postgres needed
cargo test --lib -- --skip pg_test

# Specific test
cargo pgrx test pg17 -- pg_test_topk_min_basic

# Property-based tests
cargo test --lib -- proptest
```

## The EXCEPT-ALL oracle

Every correctness test calls:

```rust
fn assert_imv_correct(imv: &str, fresh_sql: &str) {
    // SELECT count(*) FROM (
    //   (SELECT * FROM imv EXCEPT ALL SELECT * FROM (fresh_sql))
    //   UNION ALL
    //   (SELECT * FROM (fresh_sql) EXCEPT ALL SELECT * FROM imv)
    // )
    // — must be 0
}
```

This is the strongest possible black-box test: it verifies the IMV's row set matches a fresh re-computation, exactly. If a single row diverges (in value or multiplicity), the test fails.

## Running benchmarks

```bash
cd benchmarks
./run_bench.sh bench_isolated.sql
```

The harness runs each scenario multiple times and reports variance. Setseed is used for reproducibility.

## Style

- `cargo fmt` before committing.
- `cargo clippy` clean (or with explicit `#[allow(clippy::…)]` annotations for justified cases).
- New aggregates: add unit test in `tests/unit_aggregation.rs`, integration test in `tests/pg_test_correctness.rs`, and a proptest case if the aggregate is non-trivially associative.
- New SQL clauses: add a unit test in `tests/unit_sql_analyzer.rs` and an integration test under the appropriate `pg_test_*.rs` file.

## CI

`.github/workflows/ci.yml` runs:

- `cargo fmt --check`
- `cargo clippy --features pg17`
- `cargo pgrx test pg17`
- `cargo pgrx test pg18`

…on every push to `main` and every pull request.

## Differential fuzzing

`fuzz_differential_exact` (in `src/tests/pg_test_fuzz.rs`) generates random query
shapes + data + DML, builds a pg_reflex IMV **and** an equivalent plain
`MATERIALIZED VIEW` from the same body, applies identical DML, refreshes/flushes,
and asserts their contents match (`SELECT * EXCEPT`, with a relative-epsilon
compare for `float8`/`AVG` columns). The MV is ground truth.

Run it:

```bash
cargo pgrx test pg17 fuzz_differential_exact
PG_REFLEX_FUZZ_CASES=200 cargo pgrx test pg17 fuzz_differential_exact   # deeper run
```

Default is 64 cases. Each case accumulates relations/locks in one test
transaction, so very large runs can still exhaust locks even with the raised
`max_locks_per_transaction` (see `pg_test::postgresql_conf_options`); a few
hundred cases is the practical ceiling per run.

Triage is automatic: a deliberate pg_reflex rejection RETURNS a string tagged
`[reflex-unsupported]` (skipped); a codegen defect RAISES a Postgres error
(caught by the oracle's PL/pgSQL `EXCEPTION` block and reported as a bug);
content divergence is a bug.

### When the fuzzer (or the sweep) finds a bug

Findings are catalogued in `docs/fuzz-findings.md` and frozen as `#[ignore]`'d
`#[pg_test]`s in `mod findings`. The shape that triggers an open finding is
"parked" out of `fuzz_case()` (commented), so the gate stays green (= no NEW,
uncatalogued bugs) while known bugs await a fix. To work a finding:

1. Reduce it to a minimal repro; add a Finding entry + an `#[ignore]`'d regression.
2. Fix the bug on the feature branch (TDD); remove `#[ignore]` and un-park the shape.
3. Never weaken the comparator or generator to make a finding disappear.

`scripts/imv_sweep.py` (see `scripts/README-imv-sweep.md`) runs the same
IMV-vs-MV diff against real views on a live database — an external, manual
complement to the in-CI fuzzer.

### Open follow-ups

- The mutation generator emits a fixed INSERT/UPDATE/DELETE per case; randomize
  to 2–5 statements and add low-probability TRUNCATE (mind PK collisions).
- The runner reports the first failing case, not the shrunk-minimal one.
- Partitioned-IMV variants are not yet generated (need partitioned base tables).
- LEFT-JOIN and DEFERRED shapes are parked pending findings #1 and #2.
