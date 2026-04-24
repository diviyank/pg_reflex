#!/usr/bin/env bash
#
# Concurrent DEFERRED flush tests for pg_reflex (Theme 3.5 of 1.2.0).
#
# Exercises reflex_flush_deferred under concurrent INSERT + flush pressure.
# Verifies (a) no deadlocks (b) oracle correctness after all sessions finish
# (c) the 2-arg pg_advisory_xact_lock (Bug #11) coordinates concurrent flushes.
#
# Requires: psql.
#
# Environment variables:
#   PG_VERSION - pgrx PostgreSQL major version (default: 17)
#   PSQL_CMD   - psql connection command (default: pgrx-managed PG on port 288XX)
#   DB_NAME    - database name (default: bench_db)
#   SESSIONS   - parallel sessions (default: 4)
#   ITERS      - iterations per session (default: 250)

set -euo pipefail

PG_VERSION="${PG_VERSION:-17}"
DB_NAME="${DB_NAME:-bench_db}"
SESSIONS="${SESSIONS:-4}"
ITERS="${ITERS:-250}"

if [ -z "${PSQL_CMD:-}" ]; then
    PGRX_PG=$(find "${HOME}/.pgrx" -path "*/${PG_VERSION}.*/pgrx-install/bin/psql" -print -quit 2>/dev/null | xargs -r dirname)
    if [ -z "$PGRX_PG" ]; then
        echo "Error: No pgrx installation found for PG ${PG_VERSION}"
        exit 1
    fi
    PG_PORT="288${PG_VERSION}"
    PSQL_CMD="${PGRX_PG}/psql -h localhost -p ${PG_PORT} -d ${DB_NAME}"
fi

PASS=0
FAIL=0

run_sql() {
    $PSQL_CMD -tAq -c "$1" 2>/dev/null
}

report() {
    local name="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo "  PASS: $name (expected=$expected, got=$actual)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $name (expected=$expected, got=$actual)"
        FAIL=$((FAIL + 1))
    fi
}

echo "============================================"
echo "  pg_reflex concurrent DEFERRED flush tests"
echo "  Sessions: $SESSIONS  Iterations: $ITERS"
echo "============================================"
echo ""

# --- Test 1: concurrent INSERT + reflex_flush_deferred on a DEFERRED IMV ---
echo "Test 1: Concurrent INSERT + flush on DEFERRED IMV"

run_sql "DROP TABLE IF EXISTS cf_src CASCADE;"
run_sql "CREATE TABLE cf_src (id SERIAL, grp TEXT NOT NULL, val NUMERIC NOT NULL);"
run_sql "SELECT create_reflex_ivm('cf_view', 'SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM cf_src GROUP BY grp', NULL, NULL, 'DEFERRED');"

PIDS=()
for s in $(seq 1 "$SESSIONS"); do
    (
        for i in $(seq 1 "$ITERS"); do
            $PSQL_CMD -c "INSERT INTO cf_src (grp, val) VALUES ('A', $s); SELECT reflex_flush_deferred('cf_src');" >/dev/null 2>&1 || true
        done
    ) &
    PIDS+=($!)
done

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

# Final flush to drain any residual staged deltas
run_sql "SELECT reflex_flush_deferred('cf_src');" >/dev/null

EXPECTED_SUM=$(run_sql "SELECT SUM(val) FROM cf_src WHERE grp = 'A'")
ACTUAL_SUM=$(run_sql "SELECT total FROM cf_view WHERE grp = 'A'")
EXPECTED_CNT=$(run_sql "SELECT COUNT(*) FROM cf_src WHERE grp = 'A'")
ACTUAL_CNT=$(run_sql "SELECT cnt FROM cf_view WHERE grp = 'A'")
report "SUM after concurrent flush" "$EXPECTED_SUM" "$ACTUAL_SUM"
report "COUNT after concurrent flush" "$EXPECTED_CNT" "$ACTUAL_CNT"

echo ""

# --- Test 2: concurrent flush on multiple sources (independent IMVs) ---
echo "Test 2: Concurrent flush on 2 independent IMVs"

run_sql "DROP TABLE IF EXISTS cf2a_src, cf2b_src CASCADE;"
run_sql "CREATE TABLE cf2a_src (id SERIAL, grp TEXT, val NUMERIC);"
run_sql "CREATE TABLE cf2b_src (id SERIAL, grp TEXT, val NUMERIC);"
run_sql "SELECT create_reflex_ivm('cf2a_view', 'SELECT grp, SUM(val) AS total FROM cf2a_src GROUP BY grp', NULL, NULL, 'DEFERRED');"
run_sql "SELECT create_reflex_ivm('cf2b_view', 'SELECT grp, SUM(val) AS total FROM cf2b_src GROUP BY grp', NULL, NULL, 'DEFERRED');"

PIDS=()
for s in $(seq 1 "$SESSIONS"); do
    TBL=$([ $((s % 2)) -eq 0 ] && echo "cf2a_src" || echo "cf2b_src")
    (
        for i in $(seq 1 "$ITERS"); do
            $PSQL_CMD -c "INSERT INTO $TBL (grp, val) VALUES ('G', $s); SELECT reflex_flush_deferred('$TBL');" >/dev/null 2>&1 || true
        done
    ) &
    PIDS+=($!)
done

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

run_sql "SELECT reflex_flush_deferred('cf2a_src');" >/dev/null
run_sql "SELECT reflex_flush_deferred('cf2b_src');" >/dev/null

A_MISMATCH=$(run_sql "
    SELECT COUNT(*) FROM (
        (SELECT grp, SUM(val) FROM cf2a_src GROUP BY grp EXCEPT ALL SELECT grp, total FROM cf2a_view)
        UNION ALL
        (SELECT grp, total FROM cf2a_view EXCEPT ALL SELECT grp, SUM(val) FROM cf2a_src GROUP BY grp)
    ) x
")
B_MISMATCH=$(run_sql "
    SELECT COUNT(*) FROM (
        (SELECT grp, SUM(val) FROM cf2b_src GROUP BY grp EXCEPT ALL SELECT grp, total FROM cf2b_view)
        UNION ALL
        (SELECT grp, total FROM cf2b_view EXCEPT ALL SELECT grp, SUM(val) FROM cf2b_src GROUP BY grp)
    ) x
")
report "cf2a_view oracle" "0" "$A_MISMATCH"
report "cf2b_view oracle" "0" "$B_MISMATCH"

echo ""

run_sql "SELECT drop_reflex_ivm('cf_view');" >/dev/null 2>&1 || true
run_sql "SELECT drop_reflex_ivm('cf2a_view');" >/dev/null 2>&1 || true
run_sql "SELECT drop_reflex_ivm('cf2b_view');" >/dev/null 2>&1 || true
run_sql "DROP TABLE IF EXISTS cf_src, cf2a_src, cf2b_src CASCADE;" >/dev/null 2>&1 || true

echo "============================================"
echo "  Results: $PASS passed, $FAIL failed"
echo "============================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
