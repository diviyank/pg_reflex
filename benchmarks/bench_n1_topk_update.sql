-- ==========================================================================
--  N1 — Heap-shrinkage-gated top-K UPDATE recompute
--  Measures UPDATE on a top-K MIN/MAX IMV where K << group_cardinality.
--  Most updates target rows whose value is NOT in the heap, so N1 should
--  short-circuit the forced recompute that 1.3.0 + 2026-04-26 introduced.
--
--  Shape: 5M-row source, ~500 groups × ~10K rows each, K=16 (default).
--    sales(id, product_id, qty)
--    IMV: SELECT product_id, MIN(qty) AS qmin, MAX(qty) AS qmax
--          FROM sales GROUP BY product_id;
--
--  Run:
--    ~/.pgrx/17.7/pgrx-install/bin/psql -h localhost -p 28817 -d bench_db \
--        -f benchmarks/bench_n1_topk_update.sql
-- ==========================================================================

\timing on
\pset pager off
SELECT setseed(0.42);

DROP SCHEMA IF EXISTS bench_n1 CASCADE;
CREATE SCHEMA bench_n1;
SET search_path = bench_n1, public;

\echo 'Building 5M-row source...'

CREATE TABLE sales (
    id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    qty INT NOT NULL
);

-- 500 groups × 10000 rows; qty uniform [1, 10000] so K=16 is far below
-- group cardinality. Most randomly-updated rows hold a non-heap-eligible
-- value, which is exactly the workload N1 targets.
INSERT INTO sales (product_id, qty)
SELECT (i % 500) + 1,
       (random() * 9999 + 1)::int
FROM generate_series(1, 5000000) AS i;

CREATE INDEX ix_sales_product ON sales(product_id);
ANALYZE sales;

\echo 'Building IMV (auto-topk K=16)...'

SELECT create_reflex_ivm(
    'bench_n1.sales_topk',
    'SELECT product_id, MIN(qty) AS qmin, MAX(qty) AS qmax
     FROM bench_n1.sales GROUP BY product_id'
);

ANALYZE sales_topk;
SELECT count(*) AS imv_rows FROM sales_topk;

\echo ''
\echo '--- Helpers ---'

CREATE OR REPLACE FUNCTION bench_n1_update_random_rows(p_batch INTEGER)
RETURNS TABLE(reflex_ms NUMERIC, raw_ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    rms NUMERIC; raw NUMERIC;
    upd_sql TEXT;
BEGIN
    -- Pick p_batch random rows (likely all hold non-heap qty values).
    -- Use a temp marker table so the SAME row set is hit twice, once with
    -- IMV triggers active, once with replication_role=replica (raw cost).
    CREATE TEMP TABLE IF NOT EXISTS _ids_n1 (id BIGINT PRIMARY KEY);
    TRUNCATE _ids_n1;
    INSERT INTO _ids_n1
    SELECT id FROM bench_n1.sales TABLESAMPLE SYSTEM (1) LIMIT p_batch;

    upd_sql := format('UPDATE bench_n1.sales SET qty = qty + 1 WHERE id IN (SELECT id FROM _ids_n1)');

    -- Run with triggers (reflex)
    t0 := clock_timestamp();
    EXECUTE upd_sql;
    t1 := clock_timestamp();
    rms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

    -- Revert change (raw, no trigger overhead)
    SET LOCAL session_replication_role = replica;
    EXECUTE format('UPDATE bench_n1.sales SET qty = qty - 1 WHERE id IN (SELECT id FROM _ids_n1)');
    SET LOCAL session_replication_role = DEFAULT;

    -- Run again, but raw this time
    SET LOCAL session_replication_role = replica;
    t0 := clock_timestamp();
    EXECUTE upd_sql;
    t1 := clock_timestamp();
    raw := EXTRACT(EPOCH FROM t1 - t0) * 1000;
    EXECUTE format('UPDATE bench_n1.sales SET qty = qty - 1 WHERE id IN (SELECT id FROM _ids_n1)');
    SET LOCAL session_replication_role = DEFAULT;

    reflex_ms := ROUND(rms, 0);
    raw_ms := ROUND(raw, 0);
    RETURN NEXT;
END $$ LANGUAGE plpgsql;

\echo ''
\echo '=== UPDATE bench (top-K MIN/MAX, K=16, ~10000 rows/group) ==='

DROP TABLE IF EXISTS _bench_n1_results;
CREATE TABLE _bench_n1_results (batch INT, reflex_ms NUMERIC, raw_ms NUMERIC, overhead_ratio NUMERIC);

INSERT INTO _bench_n1_results
SELECT 1000, reflex_ms, raw_ms, ROUND(reflex_ms::NUMERIC / NULLIF(raw_ms, 0), 2)
FROM bench_n1_update_random_rows(1000);

INSERT INTO _bench_n1_results
SELECT 10000, reflex_ms, raw_ms, ROUND(reflex_ms::NUMERIC / NULLIF(raw_ms, 0), 2)
FROM bench_n1_update_random_rows(10000);

INSERT INTO _bench_n1_results
SELECT 100000, reflex_ms, raw_ms, ROUND(reflex_ms::NUMERIC / NULLIF(raw_ms, 0), 2)
FROM bench_n1_update_random_rows(100000);

\echo ''
\echo '======================================================================'
\echo '  RESULTS — N1 top-K MIN/MAX UPDATE bench'
\echo '======================================================================'
SELECT batch,
       reflex_ms || ' ms' AS reflex,
       raw_ms || ' ms' AS raw,
       overhead_ratio || 'x' AS overhead
FROM _bench_n1_results
ORDER BY batch;

\echo ''
\echo '--- Correctness check ---'
SELECT reflex_reconcile('bench_n1.sales_topk');
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS correctness
FROM (
    SELECT * FROM sales_topk
    EXCEPT ALL
    SELECT product_id, MIN(qty), MAX(qty) FROM sales GROUP BY product_id
) diff;
