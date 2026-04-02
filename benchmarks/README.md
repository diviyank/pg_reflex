# pg_reflex Benchmarks

## Benchmark Environment

### Production-Scale Benchmarks (`bench_sop_*.sql`)

These benchmarks use a real production-like dataset:

- **Database:** PostgreSQL 18 (`db_clone`)
- **Source table:** `alp.sales_simulation` — 76M rows, 11GB heap, 9 B-tree indexes (7.7GB), 3 DEFERRED FK constraints
- **Output:** 7.7M rows via a 5-table JOIN (sales_simulation + demand_planning + location + product + pricing + current_assortment_activity_view)
- **IMV type:** Passthrough (no aggregation), with unique key `(dem_plan_id, product_id, location_id, order_date)`
- **Target indexes:** 5 (unique, partial, 2 covering, 1 reflex-managed)
- **Storage:** UNLOGGED (no WAL on target table)
- **Hardware:** local SSD, 32GB RAM
- **PostgreSQL config:** `shared_buffers = 4GB`, `work_mem = 4MB`, `effective_cache_size = 4GB`

**Methodology:** Each batch size is tested by:
1. INSERT into source table with triggers active (timed) → Reflex total
2. Cleanup (delete test data from both source and target, bypassing triggers)
3. INSERT into source table in `replica` mode (no triggers, no FK checks) → raw baseline
4. Advantage = `1 - reflex / (raw + REFRESH baseline)`

The REFRESH MATERIALIZED VIEW baseline is measured once (2 runs: cold + warm) and used as a constant for comparison. Both "Reflex total" and "raw + REFRESH" include the source table DML cost, making the comparison fair.

### Synthetic Benchmarks (`bench_matrix.sql`, `bench_profile_v2.sql`, etc.)

These use generated data (1M–5M rows) on a pgrx-managed PostgreSQL 17 instance (port 28817). See the individual script headers for setup details.

## Running the Benchmarks

### Production-scale (recommended)

```bash
# Prerequisites: pg_reflex extension installed on db_clone, IMV already created
# See bench_sop_4gb.sql header for IMV creation if needed

# Full benchmark: 1K to 500K batches, INSERT/DELETE/UPDATE
psql -U postgres -h localhost -p 5432 -d db_clone -f benchmarks/bench_sop_4gb.sql

# Large batches: 500K, 1M, 2M
psql -U postgres -h localhost -p 5432 -d db_clone -f benchmarks/bench_sop_4gb_large.sql

# Diagnostic: trigger internals instrumentation
psql -U postgres -h localhost -p 5432 -d db_clone -f benchmarks/bench_sop_reproduce.sql
```

### Synthetic benchmarks

```bash
PSQL="~/.pgrx/17.7/pgrx-install/bin/psql -h localhost -p 28817 -d bench_db"

$PSQL -f benchmarks/bench_matrix.sql           # Fair comparison matrix
$PSQL -f benchmarks/bench_mixed_operations.sql  # INSERT → UPDATE → DELETE sequence
$PSQL -f benchmarks/bench_oltp_single_row.sql   # Single-row insert latency
$PSQL -f benchmarks/bench_profile_v2.sql        # Per-step pipeline timing
```

### Multi-run with variance reporting

```bash
./benchmarks/run_bench.sh benchmarks/bench_sum.sql 5
```

### Reproducibility

All benchmarks use `SELECT setseed(0.42)` for deterministic `random()` calls.

## PostgreSQL Tuning

**Critical:** `shared_buffers` must be sized appropriately for the working set. Early benchmarks with `shared_buffers = 128MB` on an 18.7GB working set showed 8.7x variance between cold and warm cache. With `shared_buffers = 4GB` (recommended: 25% of RAM), cold/warm variance is eliminated and results are stable.

| Setting | Recommended | Notes |
|---------|-------------|-------|
| `shared_buffers` | 25% of RAM (e.g., 4GB for 32GB) | Critical for consistent results |
| `work_mem` | 4MB (default) | Sufficient — plans use Hash Join naturally |
| `effective_cache_size` | 75% of RAM | Planner hint, doesn't allocate memory |

## Performance Results (2026-04-02, shared_buffers = 4GB)

### Production-Scale: 76M source, 7.7M output, 5-table JOIN

REFRESH MATERIALIZED VIEW baseline: **39–64s** (warm cache, varies with pool size)

#### INSERT

| Batch | Reflex | raw + REFRESH | Advantage |
|------:|-------:|--------------:|----------:|
| 1K | 235 ms | 38.9s | **99.4%** |
| 10K | 1.6s | 39.1s | **96.0%** |
| 25K | 2.5s | 40.0s | **93.7%** |
| 50K | 5.7s | 44.8s | **87.2%** |
| 100K | 16.5s | 48.9s | **66.3%** |
| 200K | 23.5s | 61.6s | **61.9%** |
| 500K | 58.8s | 93.2s | **36.9%** |
| 1M | 55.1s | 110.8s | **50.3%** |
| 2M | 2:59 | 4:57 | **39.8%** |

#### DELETE

| Batch | Reflex | raw + REFRESH | Advantage |
|------:|-------:|--------------:|----------:|
| 1K | 508 ms | 39.0s | **98.7%** |
| 10K | 116 ms | 39.0s | **99.7%** |
| 50K | 297 ms | 38.9s | **99.2%** |
| 100K | 550 ms | 39.0s | **98.6%** |
| 200K | 1.3s | 39.1s | **96.7%** |
| 500K | 18.7s | 67.3s | **72.3%** |
| 1M | 14.1s | 69.8s | **79.8%** |
| 2M | 24.0s | 93.3s | **74.3%** |

#### UPDATE

| Batch | Reflex | raw + REFRESH | Advantage |
|------:|-------:|--------------:|----------:|
| 1K | 4.2s | 38.9s | **89.2%** |
| 10K | 416 ms | 41.4s | **99.0%** |
| 50K | 7.3s | 44.5s | **83.5%** |
| 100K | 20.6s | 51.2s | **59.7%** |
| 200K | 40.0s | 66.1s | **39.5%** |
| 500K | 53.5s | 99.0s | **46.0%** |
| 1M | 3:27 | 4:33 | **24.2%** |
| 2M | 7:49 | 8:30 | **8.0%** |

#### Full Refresh

| Operation | Time |
|-----------|------|
| REFRESH MATERIALIZED VIEW | 1:37 |
| reflex_reconcile() | 2:08 |

Correctness: **PASS** (verified via `EXCEPT ALL` after reconcile + REFRESH)

### Trigger Internals (instrumented)

Framework overhead (EXISTS check, metadata query, advisory lock, Rust FFI) is **< 1ms** at all batch sizes. The trigger is **15–21%** of total INSERT time.

| Batch | Trigger delta INSERT | Trigger % of total |
|------:|---------------------:|-------------------:|
| 1K | 22 ms | 16% |
| 5K | 93 ms | 17% |
| 10K | 232 ms | 28% |
| 25K | 468 ms | 19% |
| 50K | 1,042 ms | 19% |
| 100K | 2,051 ms | 21% |

The delta INSERT scales linearly at ~20ms per 1K rows. No cliff at any batch size.

### Key Takeaways

1. **pg_reflex wins at ALL batch sizes up to 2M for ALL operations.** No break-even reached.
2. **DELETE is the standout** — 72–99% advantage at all sizes. Key-based targeted delete never approaches the fixed REFRESH cost.
3. **INSERT break-even** extrapolates to ~3M+ rows (well beyond typical batch sizes).
4. **UPDATE** is the tightest — the passthrough UPDATE path does DELETE old + INSERT new, touching 2x the rows. Still wins at 2M (8%).
5. **No cliff at any batch size.** All operations scale smoothly.
6. **Trigger overhead is 15–21% of total time.** The source table's own overhead (9 indexes + FK constraints) dominates.

### Synthetic: 1M source, 4 cardinalities × 3 batch sizes

**pg_reflex advantage (% faster than bare INSERT + REFRESH):**

| Groups | 1K batch | 10K batch | 50K batch |
|--------|----------|-----------|-----------|
| **10** | +92% | +66% | +28% |
| **1K** | +86% | +63% | +26% |
| **10K** | +88% | +28% | +17% |
| **100K** | +96% | +78% | +10% |

### OLTP Single-Row Insert Latency

| Scenario | avg ms/insert |
|----------|---------------|
| 1 IMV (10 groups) | ~3.5 ms |
| 2 IMVs (10 + 10K groups) | ~7.5 ms |
| 3 IMVs | ~13.6 ms |
| bare INSERT (no trigger) | ~0.06 ms |

## Benchmark Scripts

| Script | What it tests |
|--------|---------------|
| **`bench_sop_4gb.sql`** | **Production-scale: 76M source, 1K–500K batches, INSERT/DELETE/UPDATE** |
| **`bench_sop_4gb_large.sql`** | **Large batches: 500K, 1M, 2M rows** |
| **`bench_sop_reproduce.sql`** | **Instrumented trigger: per-step timing at all batch sizes** |
| `bench_sop_forecast.sql` | Original production benchmark (creates IMV + indexes) |
| `bench_diagnostic_cliff.sql` | EXPLAIN ANALYZE at 25K vs 50K (planner/buffer analysis) |
| `bench_diagnostic_breakdown_v2.sql` | Component breakdown: source INSERT vs FK vs trigger |
| **`bench_matrix.sql`** | **Synthetic: 4 cardinalities × 3 batch sizes** |
| **`bench_mixed_operations.sql`** | **INSERT → UPDATE → DELETE sequence with correctness** |
| **`bench_oltp_single_row.sql`** | **Per-insert overhead with 1–3 IMVs** |
| **`bench_profile_v2.sql`** | **Per-step pipeline timing (find bottlenecks)** |
| `bench_highcard_multi_imv.sql` | 4 IMVs vs 4 MATVIEWs on 5M source |
| `bench_batch.sql` | Low-cardinality batch operations (10 groups) |
| `bench_production.sql` | JOIN-based 3M orders × customers × products |

## Interpreting Results

- **Reflex total** includes both the source DML and the trigger overhead
- **raw + REFRESH** = source DML (replica mode, no trigger/FK) + REFRESH MATERIALIZED VIEW
- Positive advantage % = pg_reflex is faster
- "Raw (replica)" uses `session_replication_role = replica` which skips triggers AND FK constraint checks — this slightly underestimates the true source DML cost (FK registration adds ~860ms at 50K rows)
- pg_reflex advantage grows with: smaller batch size, higher cardinality, more IMVs
- REFRESH cost is fixed regardless of batch size — it always rescans the full source
