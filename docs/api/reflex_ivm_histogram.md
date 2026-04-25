# `reflex_ivm_histogram`

(1.3.0+) Returns flush latency percentiles for an IMV computed from a 64-sample ring buffer of `last_flush_ms`.

## Signature

```sql
reflex_ivm_histogram(view_name TEXT) RETURNS TABLE(
    p50_ms DOUBLE PRECISION,
    p95_ms DOUBLE PRECISION,
    p99_ms DOUBLE PRECISION,
    max_ms BIGINT,
    samples BIGINT
)
```

## Example

```sql
SELECT * FROM reflex_ivm_histogram('sales_by_region');
```

| p50_ms | p95_ms | p99_ms | max_ms | samples |
|--:|--:|--:|--:|--:|
| 18.0 | 41.0 | 58.4 | 124 | 64 |

Returns one row with all NULLs and `samples = 0` when the IMV has not yet been flushed.

## How it's populated

Every per-IMV flush body inside `reflex_flush_deferred` appends its wall time to `__reflex_ivm_reference.flush_ms_history`, capping the array at the most recent 64 samples (a ring buffer). The SPI computes `percentile_cont(0.50/0.95/0.99)` over this array.

## Use cases

- Detect flush regressions after a schema change: compare `p99_ms` before and after.
- Identify latency outliers in a cascade: sort by `p99_ms` to find the bottleneck IMV.
- Capacity planning: track `samples` and `p99_ms` over time via your observability stack (scrape via `pg_cron` + push).
