# Benchmarks

Two production-scale benchmarks live here: the **1.3.0 isolated bench** (10M
source rows, repeatable on a fresh DB) and the **legacy 76M-row reference**
(production clone, captured on the 1.1.x line and preserved for the
larger-scale shape comparison). Both run on PG 18 with
`shared_buffers = 4GB`, 32GB RAM, local SSD. The 1.3.0 numbers are
correctness-verified byte-for-byte via `EXCEPT ALL` against a fresh
`REFRESH MATERIALIZED VIEW` after every run.

## 1.3.0 — 10M-row source, 5-table JOIN passthrough (PG 18)

`benchmarks/bench_full_scale_1_3_0.sql`. Self-contained: builds dimensions
(product 100K, location 1K, calendar 3.3K, pricing 10M), 10M-row sales fact
table, matview baseline, and IMV in IMMEDIATE refresh mode.
**REFRESH MATERIALIZED VIEW baseline: 24,130 ms.**

### INSERT

| Batch | Reflex | Raw | Advantage vs raw + REFRESH |
|------:|-------:|----:|---------------------------:|
| 1K | 36 ms | 10 ms | **99.8%** |
| 10K | 362 ms | 62 ms | 98.5% |
| 100K | 2.4 s | 546 ms | 90.2% |
| 500K | 11.4 s | 3.0 s | 58.0% |
| 1M | 22.3 s | 6.8 s | 27.9% |

### DELETE — key-based targeting dominates

| Batch | Reflex | Raw | Advantage |
|------:|-------:|----:|----------:|
| 1K | 445 ms | 88 ms | 98.2% |
| 10K | 115 ms | 93 ms | **99.5%** |
| 100K | 357 ms | 141 ms | 98.5% |
| 500K | 1.7 s | 464 ms | 93.2% |
| 1M | 3.5 s | 721 ms | **86.0%** |

### UPDATE

| Batch | Reflex | Raw | Advantage |
|------:|-------:|----:|----------:|
| 1K | 363 ms | 92 ms | 98.5% |
| 10K | 414 ms | 195 ms | 98.3% |
| 100K | 3.4 s | 1.2 s | 86.5% |
| 500K | 17.1 s | 5.5 s | 42.2% |
| 1M | 32.3 s | 11.6 s | parity (REFRESH wins narrowly) |

**pg_reflex wins at every batch size up to 500K for every operation.** DELETE
is the standout — 1M deletions cost 3.5s vs 24s REFRESH (86% advantage). At
1M UPDATE the JOIN cost dominates and reaches REFRESH parity; this remains
the only case where REFRESH is competitive.

## 76M-row legacy reference (1.1.x, larger scale)

`benchmarks/bench_sop_4gb.sql` against a production-like dataset:
**76M source rows, 7.7M output rows, 5-table JOIN with LEFT JOINs,
9 source indexes, 5 target indexes.** REFRESH baseline 39–64s. Numbers
preserved for shape comparison; 1.3.0's passthrough algebra is unchanged
from 1.1.x, so per-row trigger costs scale similarly at the 76M scale.

### INSERT

| Batch | Reflex | raw + REFRESH | Advantage |
|------:|-------:|--------------:|----------:|
| 1K | 235 ms | 38.9s | **99.4%** |
| 10K | 1.6s | 39.1s | **96.0%** |
| 50K | 5.7s | 44.8s | **87.2%** |
| 100K | 16.5s | 48.9s | **66.3%** |
| 500K | 58.8s | 93.2s | **36.9%** |
| 1M | 55.1s | 110.8s | **50.3%** |
| 2M | 2:59 | 4:57 | **39.8%** |

### DELETE

| Batch | Reflex | raw + REFRESH | Advantage |
|------:|-------:|--------------:|----------:|
| 1K | 508 ms | 39.0s | **98.7%** |
| 50K | 297 ms | 38.9s | **99.2%** |
| 200K | 1.3s | 39.1s | **96.7%** |
| 500K | 18.7s | 67.3s | **72.3%** |
| 1M | 14.1s | 69.8s | **79.8%** |
| 2M | 24.0s | 93.3s | **74.3%** |

### UPDATE

| Batch | Reflex | raw + REFRESH | Advantage |
|------:|-------:|--------------:|----------:|
| 10K | 416 ms | 41.4s | **99.0%** |
| 50K | 7.3s | 44.5s | **83.5%** |
| 100K | 20.6s | 51.2s | **59.7%** |
| 500K | 53.5s | 99.0s | **46.0%** |
| 1M | 3:27 | 4:33 | **24.2%** |
| 2M | 7:49 | 8:30 | **8.0%** |

### Trigger internals (instrumented, per batch size)

Framework overhead (EXISTS check, metadata query, advisory lock, Rust FFI) is **< 1 ms** at all batch sizes. The trigger is 15–21% of total INSERT time; the rest is the source table's own overhead (9 indexes + FK constraints).

| Batch | Trigger delta INSERT | Trigger % of total |
|------:|---------------------:|-------------------:|
| 1K | 22 ms | 16% |
| 10K | 232 ms | 28% |
| 50K | 1,042 ms | 19% |
| 100K | 2,051 ms | 21% |

## Synthetic — 5M rows, 30K groups

Trigger overhead in isolation (GROUP BY + SUM/COUNT, no JOIN, no FKs).

| Batch | pg_reflex | pg_ivm | REFRESH MV |
|---:|---:|---:|---:|
| 1K | 36 ms | 42 ms | 463 ms |
| 10K | 62 ms | 3,012 ms | 470 ms |
| 50K | 221 ms | 26,668 ms | 463 ms |
| 100K | 29 ms | 25,686 ms | 476 ms |
| 500K | 78 ms | 27,510 ms | 526 ms |

## 1.3.0 top-K — MIN/MAX retraction

The audit's flagship perf gap (R3) was retraction on `MIN`/`MAX` over wide fact tables. Top-K (1.3.0, opt-in via `topk=K`) keeps the K extremum values per group and uses multi-set subtraction on retraction. The recompute path is gated on an `EXISTS` check over affected-group rows whose heap underflowed — when the heap survives, no source scan happens.

### 5M-row MIN/MAX, 5K groups (1000 rows/group avg) — PG18

| DELETE batch | REFRESH MV | IMV (no topk) | IMV (`topk=16`) | top-K vs no-topk | top-K vs REFRESH |
|---:|---:|---:|---:|---:|---:|
| 100 | 529 ms | 479 ms | **93 ms** | **5.1× faster** | **5.7× faster** |
| 1,000 | 529 ms | 1,551 ms | **556 ms** | **2.8× faster** | parity |
| 10,000 | 540 ms | 14,847 ms | **2,726 ms** | **5.4× faster** | 0.2× (REFRESH wins) |
| 50,000 | 540 ms | 14,888 ms | **2,908 ms** | **5.1× faster** | 0.2× (REFRESH wins) |

**Headline**: top-K turns the 1.2.0 retraction cliff into a flat curve — every batch size is 3-5× faster than the no-topk path. For small deltas (the operational common case: 100-1000 deletes per cron tick or per batch), top-K **also beats `REFRESH MV`** at the 5M-row scale.

For very large deltas (>10K rows on a 5M source), `REFRESH MV` reclaims the lead because a single sequential scan dominates. The crossover shifts higher as source size grows — at 50M+ rows (the audit's stock_chart scale), top-K should win across all delta sizes.

### 1M-row MIN/MAX, 1K groups (1000 rows/group) — PG18

| Op | REFRESH MV | IMV (no topk) | IMV (`topk=16`) |
|---|---:|---:|---:|
| INSERT 10,000 | — | 109 ms | 264 ms |
| DELETE 10,000 | 112 ms | 905 ms | **148 ms** |

Top-K's INSERT cost is ~2.5× higher than no-topk because the heap is maintained on every write; this is the price of the 6× retraction speedup. For workloads where retraction is rare, top-K is a net loss; for workloads where retraction is common (the audit's stock_chart pattern), it's a clean win.

### Algebraic aggregates (sanity check) — 1M-row source

| Operation | REFRESH MV | pg_reflex IMV |
|---|---:|---:|
| SUM/COUNT INSERT 10K | 101 ms | 99 ms |
| SUM/COUNT DELETE 10K | 102 ms | 121 ms |
| BOOL_OR DELETE 10K | 70 ms | 77 ms |

Algebraic aggregates (SUM, COUNT, BOOL_OR since 1.1.3) are at parity with `REFRESH MV` — neither side is meaningfully faster.

Reproduce: `benchmarks/bench_1_3_0_topk.sql` (1M rows) and `benchmarks/bench_1_3_0_topk_5m.sql` (5M rows).

## Reproduce

```bash
# 1.3.0 isolated full-scale (10M rows, self-contained)
createdb test_bench_1_3_0
psql -d test_bench_1_3_0 -c "CREATE EXTENSION pg_reflex"
psql -d test_bench_1_3_0 -f benchmarks/bench_full_scale_1_3_0.sql

# 76M-row legacy reference (requires production-like data)
psql -d db_clone -f benchmarks/bench_sop_4gb.sql
psql -d db_clone -f benchmarks/bench_sop_4gb_large.sql

# Synthetic GROUP BY
psql -d test -f benchmarks/bench_isolated.sql

# Top-K MIN/MAX
psql -d test -f benchmarks/bench_1_3_0_topk_5m.sql
```

See [`benchmarks/README.md`](https://github.com/diviyank/pg_reflex/blob/main/benchmarks/README.md) for setup details.

[Cost model :material-arrow-right-bold:](cost-model.md){ .md-button }
