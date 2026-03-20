# pg_reflex Benchmarks

## Prerequisites

- PostgreSQL 17 with pg_reflex extension installed
- Start with: `cargo pgrx run pg17`
- Connect with the psql connection string shown by pgrx

## Running

```bash
# Setup (creates extension + source tables)
psql -f benchmarks/setup.sql

# Individual benchmarks
psql -f benchmarks/bench_sum.sql
psql -f benchmarks/bench_avg.sql
psql -f benchmarks/bench_count_distinct.sql
psql -f benchmarks/bench_join.sql
psql -f benchmarks/bench_cascade.sql
psql -f benchmarks/bench_cte.sql

# THE MAIN BENCHMARK: large batch operations vs REFRESH MATERIALIZED VIEW
psql -f benchmarks/bench_batch.sql

# Baseline comparison (standard REFRESH MATERIALIZED VIEW)
psql -f benchmarks/bench_baseline.sql

# Cleanup
psql -f benchmarks/teardown.sql
```

## What's Measured

Each benchmark script tests at 4 data scales: **1K, 10K, 100K, 1M** source rows.

For each scale:
| Metric | Description |
|--------|-------------|
| Initial materialization | Time to `create_reflex_ivm()` on an existing table |
| Batch INSERT (1K rows) | Trigger latency for a 1000-row batch insert |
| Single INSERT (1 row) | Trigger latency for a single-row insert |
| UPDATE (100 rows) | Trigger latency for updating 100 rows |
| DELETE (100 rows) | Trigger latency for deleting 100 rows |
| Correctness check | Verifies IMV matches a direct `SELECT ... GROUP BY` query |

## Benchmark Scripts

| Script | IMV Query | Focus |
|--------|-----------|-------|
| `bench_sum.sql` | `SELECT region, SUM(amount) GROUP BY region` | Core SUM aggregate |
| `bench_avg.sql` | `SELECT region, AVG(amount) GROUP BY region` | AVG decomposition (SUM+COUNT) |
| `bench_count_distinct.sql` | `SELECT DISTINCT region` | Reference counting for DISTINCT |
| `bench_join.sql` | `SELECT ... FROM orders JOIN products ... GROUP BY category` | JOIN-based IMV |
| `bench_cascade.sql` | L1 from source, L2 from L1 | Single-level propagation timing |
| `bench_cte.sql` | CTE decomposition (single, chained, JOIN) | CTE-based complex queries |
| **`bench_batch.sql`** | **SUM+COUNT on 1M base rows** | **Large batch INSERT/DELETE/UPDATE (10K-1M) vs REFRESH MATVIEW** |
| `bench_baseline.sql` | Same as bench_sum, using standard `MATERIALIZED VIEW` | Comparison baseline |

## Interpreting Results

The `\timing on` directive shows wall-clock time for each SQL statement. Compare:
- **pg_reflex trigger time** (batch INSERT/DELETE/UPDATE) vs **REFRESH MATERIALIZED VIEW** time from bench_baseline.sql
- At larger scales, pg_reflex should be significantly faster since it processes only the delta, not the entire dataset
