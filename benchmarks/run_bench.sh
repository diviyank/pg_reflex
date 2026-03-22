#!/usr/bin/env bash
#
# Multi-run benchmark harness for pg_reflex
# Usage: ./benchmarks/run_bench.sh <benchmark.sql> [runs=5]
#
# Runs a benchmark SQL file multiple times, extracts timing data,
# and reports min/max/mean/median/stddev.
#
# Requires: psql, awk, sort
#
# Environment variables:
#   PSQL_CMD  - psql connection command (default: pgrx-managed PG17 on port 28817)
#   DB_NAME   - database name (default: bench_db)

set -euo pipefail

BENCH_FILE="${1:?Usage: $0 <benchmark.sql> [runs=5]}"
RUNS="${2:-5}"
DB_NAME="${DB_NAME:-bench_db}"
PGRX_PG="${HOME}/.pgrx/17.7/pgrx-install/bin"
PSQL_CMD="${PSQL_CMD:-${PGRX_PG}/psql -h localhost -p 28817 -d ${DB_NAME}}"

if [ ! -f "$BENCH_FILE" ]; then
    echo "Error: File not found: $BENCH_FILE"
    exit 1
fi

echo "============================================"
echo "  pg_reflex benchmark harness"
echo "  File:  $BENCH_FILE"
echo "  Runs:  $RUNS"
echo "============================================"
echo ""

TIMINGS_FILE=$(mktemp)
trap 'rm -f "$TIMINGS_FILE"' EXIT

for i in $(seq 1 "$RUNS"); do
    echo "--- Run $i/$RUNS ---"
    # Run benchmark, capture timing lines (pattern: "Time: 123.456 ms")
    $PSQL_CMD -f "$BENCH_FILE" 2>&1 \
        | grep -oP 'Time: \K[0-9]+(\.[0-9]+)?' \
        >> "$TIMINGS_FILE"
    echo ""
done

COUNT=$(wc -l < "$TIMINGS_FILE")
if [ "$COUNT" -eq 0 ]; then
    echo "No timing data captured. Make sure \\timing is enabled in the benchmark."
    exit 1
fi

echo "============================================"
echo "  Results ($COUNT timing samples across $RUNS runs)"
echo "============================================"
echo ""

sort -n "$TIMINGS_FILE" | awk '
{
    vals[NR] = $1
    sum += $1
    sumsq += $1 * $1
}
END {
    n = NR
    mean = sum / n
    variance = (sumsq / n) - (mean * mean)
    if (variance < 0) variance = 0
    stddev = sqrt(variance)

    # Median
    if (n % 2 == 1) {
        median = vals[int(n/2) + 1]
    } else {
        median = (vals[n/2] + vals[n/2 + 1]) / 2
    }

    # Percentiles
    p5_idx  = int(n * 0.05) + 1; if (p5_idx > n) p5_idx = n
    p95_idx = int(n * 0.95) + 1; if (p95_idx > n) p95_idx = n

    printf "  Min:      %10.2f ms\n", vals[1]
    printf "  P5:       %10.2f ms\n", vals[p5_idx]
    printf "  Median:   %10.2f ms\n", median
    printf "  Mean:     %10.2f ms\n", mean
    printf "  P95:      %10.2f ms\n", vals[p95_idx]
    printf "  Max:      %10.2f ms\n", vals[n]
    printf "  Stddev:   %10.2f ms\n", stddev
    printf "  CV:       %10.1f %%\n", (stddev / mean) * 100
    printf "  Samples:  %d\n", n
}
'
