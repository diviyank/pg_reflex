-- ==========================================================================
--  Benchmark: pg_reflex IMV vs REFRESH MATERIALIZED VIEW — large batches
--  shared_buffers = 4GB | PG18 | 76M source rows | 7.7M output rows
--  Batch sizes: 500K, 1M, 2M
--
--  Connection: psql -U postgres -h localhost -p 5432 -d db_clone
-- ==========================================================================
\timing on
SELECT setseed(0.42);

\echo ''
\echo '================================================================'
\echo '  SOP FORECAST BENCHMARK — LARGE BATCHES (500K, 1M, 2M)'
\echo '  shared_buffers = 4GB'
\echo '================================================================'

SHOW shared_buffers;

-- Verify IMV
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.__reflex_ivm_reference WHERE name = 'alp.sop_forecast_reflex') THEN
        RAISE EXCEPTION 'IMV does not exist';
    END IF;
END $$;

-- ==========================================================================
-- REFRESH BASELINE (warm)
-- ==========================================================================
\echo ''
\echo '--- REFRESH MATERIALIZED VIEW baseline (2 runs) ---'

CREATE TEMP TABLE _baseline (run INT, refresh_ms NUMERIC);

DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
BEGIN
    t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW alp.sop_forecast_view;
    t1 := clock_timestamp();
    INSERT INTO _baseline VALUES (1, EXTRACT(EPOCH FROM t1 - t0) * 1000);

    t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW alp.sop_forecast_view;
    t1 := clock_timestamp();
    INSERT INTO _baseline VALUES (2, EXTRACT(EPOCH FROM t1 - t0) * 1000);
END $$;

SELECT run, ROUND(refresh_ms, 0) AS "REFRESH (ms)" FROM _baseline ORDER BY run;

-- ==========================================================================
-- POOL + HELPERS
-- ==========================================================================
-- Larger pool: all products × 4 locations to support 2M rows
CREATE TEMP TABLE _bench_pool AS
SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY product_id, location_id) AS rn
FROM (SELECT DISTINCT product_id FROM alp.sales_simulation WHERE dem_plan_id = 605) p
CROSS JOIN (VALUES (50), (51), (52), (53)) l(location_id);
SELECT COUNT(*) AS pool_size FROM _bench_pool;

CREATE OR REPLACE FUNCTION _gen_insert_sql(pool_sz INTEGER, batch_size INTEGER)
RETURNS TEXT AS $$
BEGIN
    RETURN format(
        'INSERT INTO alp.sales_simulation (dem_plan_id, product_id, location_id, order_date, year, month, week, isoyear, qty_sales, qty_sales_ub, qty_sales_lb, forecast_base)
         SELECT 605, product_id, location_id,
                ''2028-01-07''::timestamptz + (((rn_global - 1) / %s) * interval ''7 days''),
                2028, 1, 2 + ((rn_global - 1) / %s)::int, 2028,
                (random() * 100)::int, (random() * 120)::int, (random() * 80)::int, (random() * 100)::int
         FROM (
             SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY d.date_idx, bp.rn) AS rn_global
             FROM _bench_pool bp
             CROSS JOIN generate_series(1, GREATEST(1, CEIL(%s::float / %s)::int)) d(date_idx)
             LIMIT %s
         ) sub',
        pool_sz, pool_sz, batch_size, pool_sz, batch_size
    );
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _bench_cleanup() RETURNS VOID AS $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$ LANGUAGE plpgsql;

-- Warm up
INSERT INTO alp.sales_simulation (dem_plan_id, product_id, location_id, order_date, year, month, week, isoyear, qty_sales, qty_sales_ub, qty_sales_lb, forecast_base)
VALUES (605, 295, 50, '2029-01-07'::timestamptz, 2029, 1, 2, 2029, 10, 12, 8, 10);
DELETE FROM alp.sales_simulation WHERE order_date = '2029-01-07'::timestamptz AND dem_plan_id = 605 AND product_id = 295 AND location_id = 50;

-- ==========================================================================
-- BENCHMARK FUNCTIONS
-- ==========================================================================
CREATE OR REPLACE FUNCTION bench_insert(p_batch INTEGER)
RETURNS TABLE(metric TEXT, ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    reflex_ms NUMERIC; raw_ms NUMERIC; baseline_ms NUMERIC;
    pool_sz INTEGER; insert_sql TEXT;
BEGIN
    SELECT COUNT(*) INTO pool_sz FROM _bench_pool;
    SELECT refresh_ms INTO baseline_ms FROM _baseline WHERE run = 2;
    insert_sql := _gen_insert_sql(pool_sz, p_batch);

    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    reflex_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;
    PERFORM _bench_cleanup();

    SET LOCAL session_replication_role = replica;
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    raw_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;

    metric := 'reflex_total';         ms := ROUND(reflex_ms, 0);            RETURN NEXT;
    metric := 'raw_insert_replica';   ms := ROUND(raw_ms, 0);               RETURN NEXT;
    metric := 'refresh_baseline';     ms := ROUND(baseline_ms, 0);          RETURN NEXT;
    metric := 'raw+refresh';          ms := ROUND(raw_ms + baseline_ms, 0); RETURN NEXT;
    metric := 'advantage_%';          ms := ROUND(100.0 * (1.0 - reflex_ms / NULLIF(raw_ms + baseline_ms, 0)), 1); RETURN NEXT;
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bench_delete(p_batch INTEGER)
RETURNS TABLE(metric TEXT, ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    reflex_ms NUMERIC; raw_ms NUMERIC; baseline_ms NUMERIC;
    pool_sz INTEGER; insert_sql TEXT;
BEGIN
    SELECT COUNT(*) INTO pool_sz FROM _bench_pool;
    SELECT refresh_ms INTO baseline_ms FROM _baseline WHERE run = 2;
    insert_sql := _gen_insert_sql(pool_sz, p_batch);

    EXECUTE insert_sql;

    t0 := clock_timestamp();
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    t1 := clock_timestamp();
    reflex_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

    SET LOCAL session_replication_role = replica;
    EXECUTE insert_sql;
    t0 := clock_timestamp();
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    t1 := clock_timestamp();
    raw_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;
    SET LOCAL session_replication_role = DEFAULT;
    PERFORM _bench_cleanup();

    metric := 'reflex_total';         ms := ROUND(reflex_ms, 0);            RETURN NEXT;
    metric := 'raw_delete_replica';   ms := ROUND(raw_ms, 0);               RETURN NEXT;
    metric := 'refresh_baseline';     ms := ROUND(baseline_ms, 0);          RETURN NEXT;
    metric := 'raw+refresh';          ms := ROUND(raw_ms + baseline_ms, 0); RETURN NEXT;
    metric := 'advantage_%';          ms := ROUND(100.0 * (1.0 - reflex_ms / NULLIF(raw_ms + baseline_ms, 0)), 1); RETURN NEXT;
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bench_update(p_batch INTEGER)
RETURNS TABLE(metric TEXT, ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    reflex_ms NUMERIC; raw_ms NUMERIC; baseline_ms NUMERIC;
    pool_sz INTEGER; insert_sql TEXT;
BEGIN
    SELECT COUNT(*) INTO pool_sz FROM _bench_pool;
    SELECT refresh_ms INTO baseline_ms FROM _baseline WHERE run = 2;
    insert_sql := _gen_insert_sql(pool_sz, p_batch);

    EXECUTE insert_sql;

    t0 := clock_timestamp();
    UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1 WHERE order_date >= '2028-01-07'::timestamptz;
    t1 := clock_timestamp();
    reflex_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

    SET LOCAL session_replication_role = replica;
    t0 := clock_timestamp();
    UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1 WHERE order_date >= '2028-01-07'::timestamptz;
    t1 := clock_timestamp();
    raw_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;
    SET LOCAL session_replication_role = DEFAULT;
    PERFORM _bench_cleanup();

    metric := 'reflex_total';         ms := ROUND(reflex_ms, 0);            RETURN NEXT;
    metric := 'raw_update_replica';   ms := ROUND(raw_ms, 0);               RETURN NEXT;
    metric := 'refresh_baseline';     ms := ROUND(baseline_ms, 0);          RETURN NEXT;
    metric := 'raw+refresh';          ms := ROUND(raw_ms + baseline_ms, 0); RETURN NEXT;
    metric := 'advantage_%';          ms := ROUND(100.0 * (1.0 - reflex_ms / NULLIF(raw_ms + baseline_ms, 0)), 1); RETURN NEXT;
END $$ LANGUAGE plpgsql;

-- ==========================================================================
-- INSERT
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  INSERT BENCHMARK'
\echo '================================================================'

\echo '--- INSERT 500,000 ---'
SELECT * FROM bench_insert(500000);
\echo '--- INSERT 1,000,000 ---'
SELECT * FROM bench_insert(1000000);
\echo '--- INSERT 2,000,000 ---'
SELECT * FROM bench_insert(2000000);

-- ==========================================================================
-- DELETE
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  DELETE BENCHMARK'
\echo '================================================================'

\echo '--- DELETE 500,000 ---'
SELECT * FROM bench_delete(500000);
\echo '--- DELETE 1,000,000 ---'
SELECT * FROM bench_delete(1000000);
\echo '--- DELETE 2,000,000 ---'
SELECT * FROM bench_delete(2000000);

-- ==========================================================================
-- UPDATE
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  UPDATE BENCHMARK'
\echo '================================================================'

\echo '--- UPDATE 500,000 ---'
SELECT * FROM bench_update(500000);
\echo '--- UPDATE 1,000,000 ---'
SELECT * FROM bench_update(1000000);
\echo '--- UPDATE 2,000,000 ---'
SELECT * FROM bench_update(2000000);

-- ==========================================================================
-- CLEANUP
-- ==========================================================================
DROP FUNCTION IF EXISTS bench_insert(INTEGER);
DROP FUNCTION IF EXISTS bench_delete(INTEGER);
DROP FUNCTION IF EXISTS bench_update(INTEGER);
DROP FUNCTION IF EXISTS _gen_insert_sql(INTEGER, INTEGER);
DROP FUNCTION IF EXISTS _bench_cleanup();
DROP TABLE IF EXISTS _bench_pool;
DROP TABLE IF EXISTS _baseline;

\echo ''
\echo '================================================================'
\echo '  BENCHMARK COMPLETE — LARGE BATCHES'
\echo '================================================================'
