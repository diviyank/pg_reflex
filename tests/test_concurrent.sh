#!/usr/bin/env bash
#
# Concurrent operation tests for pg_reflex
#
# Tests that multiple parallel sessions can safely INSERT/DELETE against
# the same source table with an active IMV. Uses advisory locks (built into
# pg_reflex triggers) to ensure correctness.
#
# Requires: psql
#
# Environment variables:
#   PSQL_CMD  - psql connection command (default: pgrx-managed PG17 on port 28817)
#   DB_NAME   - database name (default: bench_db)
#   SESSIONS  - number of parallel sessions (default: 4)
#   ROWS      - rows per session (default: 1000)

set -euo pipefail

PG_VERSION="${PG_VERSION:-17}"
DB_NAME="${DB_NAME:-bench_db}"
SESSIONS="${SESSIONS:-4}"
ROWS="${ROWS:-1000}"

# Auto-detect pgrx PostgreSQL path and port
if [ -z "${PSQL_CMD:-}" ]; then
    # Find the pgrx install directory that contains a psql binary
    PGRX_PG=$(find "${HOME}/.pgrx" -path "*/${PG_VERSION}.*/pgrx-install/bin/psql" -print -quit 2>/dev/null | xargs -r dirname)
    if [ -z "$PGRX_PG" ]; then
        echo "Error: No pgrx installation found for PG ${PG_VERSION}"
        exit 1
    fi
    # pgrx uses port 288XX where XX is the PG major version
    PG_PORT="288${PG_VERSION}"
    PSQL_CMD="${PGRX_PG}/psql -h localhost -p ${PG_PORT} -d ${DB_NAME}"
fi

PASS=0
FAIL=0

run_sql() {
    $PSQL_CMD -tAq -c "$1" 2>/dev/null
}

report() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"

    if [ "$expected" = "$actual" ]; then
        echo "  PASS: $test_name (expected=$expected, got=$actual)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $test_name (expected=$expected, got=$actual)"
        FAIL=$((FAIL + 1))
    fi
}

echo "============================================"
echo "  pg_reflex concurrent operation tests"
echo "  Sessions: $SESSIONS"
echo "  Rows/session: $ROWS"
echo "============================================"
echo ""

# --- Test 1: Concurrent INSERTs to same group ---
echo "Test 1: Concurrent INSERTs ($SESSIONS sessions x $ROWS rows)"

run_sql "DROP TABLE IF EXISTS conc_src CASCADE;"
run_sql "CREATE TABLE conc_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL);"
run_sql "SELECT create_reflex_ivm('conc_view', 'SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM conc_src GROUP BY grp');"

# Spawn N parallel sessions, each inserting ROWS rows with value = session_id
PIDS=()
for s in $(seq 1 "$SESSIONS"); do
    $PSQL_CMD -c "INSERT INTO conc_src (grp, val) SELECT 'A', $s FROM generate_series(1, $ROWS)" &
    PIDS+=($!)
done

# Wait for all
for pid in "${PIDS[@]}"; do
    wait "$pid"
done

# Verify: total rows = SESSIONS * ROWS
EXPECTED_CNT=$((SESSIONS * ROWS))
ACTUAL_CNT=$(run_sql "SELECT cnt FROM conc_view WHERE grp = 'A'")
report "Row count" "$EXPECTED_CNT" "$ACTUAL_CNT"

# Verify: SUM matches source directly
EXPECTED_SUM=$(run_sql "SELECT SUM(val) FROM conc_src WHERE grp = 'A'")
ACTUAL_SUM=$(run_sql "SELECT total FROM conc_view WHERE grp = 'A'")
report "SUM matches source" "$EXPECTED_SUM" "$ACTUAL_SUM"

echo ""

# --- Test 2: Concurrent INSERTs to different groups ---
echo "Test 2: Concurrent INSERTs to different groups ($SESSIONS sessions)"

run_sql "DROP TABLE IF EXISTS conc2_src CASCADE;"
run_sql "CREATE TABLE conc2_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL);"
run_sql "SELECT create_reflex_ivm('conc2_view', 'SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM conc2_src GROUP BY grp');"

PIDS=()
for s in $(seq 1 "$SESSIONS"); do
    $PSQL_CMD -c "INSERT INTO conc2_src (grp, val) SELECT 'G$s', $s FROM generate_series(1, $ROWS)" &
    PIDS+=($!)
done

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

# Verify: each group has ROWS rows
MISMATCHES=$(run_sql "
    SELECT COUNT(*) FROM (
        SELECT grp, total, cnt FROM conc2_view
        EXCEPT
        SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM conc2_src GROUP BY grp
    ) x
")
report "All groups match source" "0" "$MISMATCHES"

echo ""

# --- Test 3: Concurrent INSERT + DELETE ---
echo "Test 3: Concurrent INSERT + DELETE"

run_sql "DROP TABLE IF EXISTS conc3_src CASCADE;"
run_sql "CREATE TABLE conc3_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL);"
run_sql "INSERT INTO conc3_src (grp, val) SELECT 'A', 1 FROM generate_series(1, $((ROWS * 2)));"
run_sql "SELECT create_reflex_ivm('conc3_view', 'SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM conc3_src GROUP BY grp');"

# 2 sessions INSERT, 2 sessions DELETE (from existing rows)
HALF=$((SESSIONS / 2))
PIDS=()

# INSERT sessions
for s in $(seq 1 "$HALF"); do
    $PSQL_CMD -c "INSERT INTO conc3_src (grp, val) SELECT 'A', 1 FROM generate_series(1, $ROWS)" &
    PIDS+=($!)
done

# DELETE sessions (delete some of the initial rows)
for s in $(seq 1 "$HALF"); do
    OFFSET=$(( (s - 1) * (ROWS / 2) + 1 ))
    LIMIT=$((ROWS / 2))
    $PSQL_CMD -c "DELETE FROM conc3_src WHERE id IN (SELECT id FROM conc3_src ORDER BY id OFFSET $OFFSET LIMIT $LIMIT)" &
    PIDS+=($!)
done

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

# After concurrent ops, reconcile and verify
run_sql "SELECT reflex_reconcile('conc3_view');"

EXPECTED_SUM=$(run_sql "SELECT COALESCE(SUM(val), 0) FROM conc3_src WHERE grp = 'A'")
ACTUAL_SUM=$(run_sql "SELECT COALESCE(total, 0) FROM conc3_view WHERE grp = 'A'")
EXPECTED_CNT=$(run_sql "SELECT COUNT(*) FROM conc3_src WHERE grp = 'A'")
ACTUAL_CNT=$(run_sql "SELECT COALESCE(cnt, 0) FROM conc3_view WHERE grp = 'A'")
report "SUM after reconcile" "$EXPECTED_SUM" "$ACTUAL_SUM"
report "COUNT after reconcile" "$EXPECTED_CNT" "$ACTUAL_CNT"

echo ""

# --- Cleanup ---
run_sql "SELECT drop_reflex_ivm('conc_view');" >/dev/null 2>&1 || true
run_sql "SELECT drop_reflex_ivm('conc2_view');" >/dev/null 2>&1 || true
run_sql "SELECT drop_reflex_ivm('conc3_view');" >/dev/null 2>&1 || true
run_sql "DROP TABLE IF EXISTS conc_src, conc2_src, conc3_src CASCADE;" >/dev/null 2>&1 || true

# --- Summary ---
echo "============================================"
echo "  Results: $PASS passed, $FAIL failed"
echo "============================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
