# pg_reflex Benchmarks

## Prerequisites

- PostgreSQL 17 with pg_reflex extension installed
- pgrx-managed PG17 on port 28817:
  ```bash
  cargo pgrx install --release --pg-config ~/.pgrx/17.7/pgrx-install/bin/pg_config
  ~/.pgrx/17.7/pgrx-install/bin/pg_ctl -D ~/.pgrx/data-17 -l ~/.pgrx/17.log start
  ```

## Running

```bash
PSQL="~/.pgrx/17.7/pgrx-install/bin/psql -h localhost -p 28817 -d bench_db"

# Main benchmarks
$PSQL -f benchmarks/bench_matrix.sql           # Fair comparison matrix (recommended)
$PSQL -f benchmarks/bench_mixed_operations.sql  # INSERT → UPDATE → DELETE sequence
$PSQL -f benchmarks/bench_oltp_single_row.sql   # Single-row insert latency

# Profiling
$PSQL -f benchmarks/bench_profile_v2.sql        # Per-step pipeline timing

# Legacy benchmarks (still work)
$PSQL -f benchmarks/setup.sql && $PSQL -f benchmarks/bench_batch.sql
```

## Performance Results (2026-03-21)

### Fair Comparison: pg_reflex vs bare INSERT + REFRESH MATERIALIZED VIEW

Source: 1M rows. All timings include INSERT cost on both sides.

**pg_reflex advantage (% faster than traditional approach):**

| Groups | 1K batch | 10K batch | 50K batch |
|--------|----------|-----------|-----------|
| **10** | +92% | +66% | +28% |
| **1K** | +86% | +63% | +26% |
| **10K** | +88% | +28% | +17% |
| **100K** | +96% | +78% | +10% |

pg_reflex is faster in every scenario. The advantage is largest at high cardinality with small batches — the typical real-time analytics pattern.

### Raw Timings (100K groups, 1 IMV)

| Operation | pg_reflex | bare INSERT + REFRESH | Speedup |
|-----------|-----------|----------------------|---------|
| INSERT 1K | 28 ms | 775 ms | **28x** |
| INSERT 10K | 184 ms | 827 ms | **4.5x** |
| INSERT 50K | 891 ms | 990 ms | **1.1x** |

### Mixed Operations (100K groups, covering index)

| Operation | pg_reflex | REFRESH |
|-----------|-----------|---------|
| INSERT 10K | 229 ms | 596 ms |
| UPDATE 5K | 323 ms | 598 ms |
| DELETE 2K | 33 ms | 605 ms |

### OLTP Single-Row Insert Latency

| Scenario | avg ms/insert |
|----------|---------------|
| 1 IMV (10 groups) | ~3.5 ms |
| 2 IMVs (10 + 10K groups) | ~7.5 ms |
| 3 IMVs | ~13.6 ms |
| bare INSERT (no trigger) | ~0.06 ms |

### Pipeline Profiling (100K groups, 100K batch)

Where time goes inside the trigger:

| Step | Time | % of total |
|------|------|-----------|
| MERGE into intermediate | 725 ms | 41% |
| INSERT affected into target | 368 ms | 21% |
| Framework overhead (plpgsql/FFI) | 319 ms | 18% |
| DELETE affected from target | 128 ms | 7% |
| Affected groups extraction | 94 ms | 5% |
| Cleanup + metadata | 1 ms | <1% |
| **Total** | **1,766 ms** | |

## Benchmark Scripts

| Script | What it tests |
|--------|---------------|
| **`bench_matrix.sql`** | **Fair comparison across 4 cardinalities × 3 batch sizes** |
| **`bench_mixed_operations.sql`** | **INSERT→UPDATE→DELETE sequence with correctness checks** |
| **`bench_oltp_single_row.sql`** | **Per-insert overhead with 1-3 IMVs** |
| **`bench_profile_v2.sql`** | **Per-step pipeline timing (find bottlenecks)** |
| `bench_highcard_multi_imv.sql` | 4 IMVs vs 4 MATVIEWs on 5M source, 1M target rows each |
| `bench_batch.sql` | Low-cardinality batch INSERT/DELETE/UPDATE (10 groups) |
| `bench_truncate_vs_delete.sql` | Isolated TRUNCATE vs DELETE on 1M-row targets |
| `bench_production.sql` | JOIN-based 3M orders × customers × products |
| `bench_sum.sql` | SUM aggregate at various scales |
| `bench_avg.sql` | AVG decomposition (SUM+COUNT) |
| `bench_join.sql` | JOIN-based IMV |
| `bench_cascade.sql` | Multi-level IMV propagation |

## Interpreting Results

- **pg_reflex time** includes both the INSERT and the trigger overhead
- **Baseline** = bare INSERT (no trigger) + REFRESH MATERIALIZED VIEW
- Positive advantage % = pg_reflex is faster
- pg_reflex advantage grows with: higher cardinality, smaller batch size, more IMVs on same source
- REFRESH advantage grows with: very large batches (>50K) on low-cardinality views
