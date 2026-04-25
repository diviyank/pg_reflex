# Testing

```bash
# Full integration suite (504 tests as of 1.3.0)
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
