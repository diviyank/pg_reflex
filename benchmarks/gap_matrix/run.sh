#!/usr/bin/env bash
# Synthetic gap-matrix run end-to-end against bench_gap_matrix.
# Usage:
#   ./run.sh smoke     # 100k base, MAXVOL=1000  (~1 min, exercises every path)
#   ./run.sh full      # 1M base, MAXVOL=100000, + scaling sweep (tens of minutes)
set -euo pipefail
cd "$(dirname "$0")"
PSQL="psql -U postgres -h localhost -d bench_gap_matrix -v ON_ERROR_STOP=1"
RUN_TS="$(date -u +'%Y-%m-%d %H:%M:%S+00')"
MODE="${1:-smoke}"
if [[ "$MODE" == "smoke" ]]; then BASE=100000; MAXVOL=1000; SCALING=""; else BASE=1000000; MAXVOL=100000; SCALING="-v SCALING=1"; fi

psql -U postgres -h localhost -tAc \
  "SELECT 1 FROM pg_database WHERE datname='bench_gap_matrix'" | grep -q 1 \
  || psql -U postgres -h localhost -c "CREATE DATABASE bench_gap_matrix"
$PSQL -c "CREATE EXTENSION IF NOT EXISTS pg_reflex"
$PSQL -f 00_harness.sql
$PSQL -v BASE_ROWS=$BASE -f 10_synthetic_setup.sql
$PSQL -v MAXVOL=$MAXVOL -v BASE_ROWS=$BASE -v RUN_TS="$RUN_TS" $SCALING -f 20_synthetic_driver.sql
$PSQL -v RUN_TS="$RUN_TS" -f 40_report.sql | tee "../../journal/2026-06-05_gap_matrix.md"
echo "RUN_TS=$RUN_TS"
