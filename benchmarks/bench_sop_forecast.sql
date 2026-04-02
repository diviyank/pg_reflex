-- ==========================================================================
--  Benchmark: pg_reflex IMV vs REFRESH MATERIALIZED VIEW
--  Target: alp.sop_forecast_view on db_clone
--  Source: 76M rows, Output: 7.7M rows, PostgreSQL 18
--  Batch sizes: 1K, 5K, 10K, 25K, 50K, 100K
--
--  REFRESH measured once as constant baseline. No reconcile per cell —
--  cleanup deletes directly from source + IMV target.
-- ==========================================================================
\timing on
SELECT setseed(0.42);

\echo ''
\echo '================================================================'
\echo '  SOP FORECAST VIEW BENCHMARK'
\echo '  pg_reflex v1.1.1 vs REFRESH MATERIALIZED VIEW'
\echo '  db_clone | PG18 | 76M source rows | 7.7M output rows'
\echo '================================================================'

-- ==========================================================================
-- SETUP
-- ==========================================================================
\echo ''
\echo '--- Creating pg_reflex IMV ---'

SELECT create_reflex_ivm(
    'alp.sop_forecast_reflex',
    $$
    SELECT
        sales_simulation.dem_plan_id,
        sales_simulation.week,
        sales_simulation.isoyear,
        sales_simulation.year,
        sales_simulation.month,
        sales_simulation.order_date,
        sales_simulation.product_id,
        sales_simulation.location_id,
        location.canal_id,
        sales_simulation.forecast_base::bigint AS forecast_base,
        sales_simulation.qty_sales::bigint AS quantity,
        sales_simulation.qty_sales_ub::bigint AS quantity_ub,
        sales_simulation.qty_sales_lb::bigint AS quantity_lb,
        sales_simulation.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
        sales_simulation.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
        sales_simulation.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
        sales_simulation.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
        caav.product_id IS NOT NULL AS in_current_assortment
    FROM alp.sales_simulation
        JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
        JOIN alp.location ON location.id = sales_simulation.location_id
        JOIN alp.product ON product.id = sales_simulation.product_id
        LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
            AND pricing.canal_id = location.canal_id
            AND sales_simulation.product_id = pricing.product_id
        LEFT JOIN alp.current_assortment_activity_view caav
            ON caav.product_id = sales_simulation.product_id
            AND caav.location_id = sales_simulation.location_id
    WHERE demand_planning.is_sent_to_sop
        AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]))
    $$,
    'dem_plan_id, product_id, location_id, order_date',
    'UNLOGGED'
);

\echo '--- Creating indexes on IMV ---'
CREATE INDEX IF NOT EXISTS idx_sop_forecast_reflex_assortment_true
    ON alp.sop_forecast_reflex (dem_plan_id) WHERE in_current_assortment = TRUE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_sop_forecast_reflex_main
    ON alp.sop_forecast_reflex (dem_plan_id, product_id, location_id, order_date);
CREATE INDEX IF NOT EXISTS idx_sop_forecast_reflex_monthly
    ON alp.sop_forecast_reflex (dem_plan_id, year, month)
    INCLUDE (quantity_ub, quantity, forecast_base, turnover, forecast_base_turnover, qty_sales_ub_turnover, order_date);
CREATE INDEX IF NOT EXISTS idx_sop_forecast_reflex_weekly
    ON alp.sop_forecast_reflex (dem_plan_id, isoyear, week)
    INCLUDE (quantity_ub, quantity, forecast_base, turnover, forecast_base_turnover, qty_sales_ub_turnover, order_date);

\echo '--- Verifying row counts ---'
SELECT COUNT(*) AS imv_rows FROM alp.sop_forecast_reflex;
SELECT COUNT(*) AS matview_rows FROM alp.sop_forecast_view;

-- ==========================================================================
-- REFRESH BASELINE (measured once)
-- ==========================================================================
\echo ''
\echo '--- REFRESH MATERIALIZED VIEW baseline ---'

CREATE TEMP TABLE _bench_baseline (refresh_ms NUMERIC);

DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
BEGIN
    t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW alp.sop_forecast_view;
    t1 := clock_timestamp();
    INSERT INTO _bench_baseline VALUES (EXTRACT(EPOCH FROM t1 - t0) * 1000);
END $$;

SELECT ROUND(refresh_ms, 0) AS "REFRESH baseline (ms)" FROM _bench_baseline;

-- ==========================================================================
-- POOL (~80K rows: 40K products × 2 locations)
-- ==========================================================================
CREATE TEMP TABLE _bench_pool AS
SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY product_id, location_id) AS rn
FROM (SELECT DISTINCT product_id FROM alp.sales_simulation WHERE dem_plan_id = 605) p
CROSS JOIN (VALUES (50), (51)) l(location_id);

SELECT COUNT(*) AS pool_size FROM _bench_pool;

-- Warm up
INSERT INTO alp.sales_simulation (dem_plan_id, product_id, location_id, order_date, year, month, week, isoyear, qty_sales, qty_sales_ub, qty_sales_lb, forecast_base)
VALUES (605, 295, 50, '2029-01-07'::timestamptz, 2029, 1, 2, 2029, 10, 12, 8, 10);
DELETE FROM alp.sales_simulation WHERE order_date = '2029-01-07'::timestamptz AND dem_plan_id = 605 AND product_id = 295 AND location_id = 50;

-- ==========================================================================
-- HELPERS
-- ==========================================================================
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

-- Fast cleanup: delete test data from both source and IMV directly (no reconcile)
CREATE OR REPLACE FUNCTION _bench_cleanup() RETURNS VOID AS $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$ LANGUAGE plpgsql;

-- ==========================================================================
-- BENCHMARK FUNCTIONS
-- ==========================================================================
CREATE OR REPLACE FUNCTION bench_insert(batch_size INTEGER)
RETURNS TABLE(metric TEXT, ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    reflex_ms NUMERIC; raw_ms NUMERIC; baseline_ms NUMERIC;
    pool_sz INTEGER; insert_sql TEXT;
BEGIN
    SELECT COUNT(*) INTO pool_sz FROM _bench_pool;
    SELECT refresh_ms INTO baseline_ms FROM _bench_baseline;
    insert_sql := _gen_insert_sql(pool_sz, batch_size);

    -- 1. pg_reflex: INSERT with trigger
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    reflex_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

    -- Cleanup
    PERFORM _bench_cleanup();

    -- 2. Raw INSERT (no trigger)
    SET LOCAL session_replication_role = replica;
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    raw_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

    -- Cleanup (still in replica mode)
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;

    metric := 'pg_reflex (INSERT+trigger)';    ms := ROUND(reflex_ms, 1);              RETURN NEXT;
    metric := 'bare INSERT (no trigger)';      ms := ROUND(raw_ms, 1);                 RETURN NEXT;
    metric := 'REFRESH MATVIEW (baseline)';    ms := ROUND(baseline_ms, 1);             RETURN NEXT;
    metric := 'fair baseline (bare+REFRESH)';  ms := ROUND(raw_ms + baseline_ms, 1);    RETURN NEXT;
    metric := 'pg_reflex advantage %';         ms := ROUND(100.0 * (1.0 - reflex_ms / NULLIF(raw_ms + baseline_ms, 0)), 1); RETURN NEXT;
END $$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bench_delete(batch_size INTEGER)
RETURNS TABLE(metric TEXT, ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    reflex_ms NUMERIC; raw_ms NUMERIC; baseline_ms NUMERIC;
    pool_sz INTEGER; insert_sql TEXT;
BEGIN
    SELECT COUNT(*) INTO pool_sz FROM _bench_pool;
    SELECT refresh_ms INTO baseline_ms FROM _bench_baseline;
    insert_sql := _gen_insert_sql(pool_sz, batch_size);

    -- Setup: insert with trigger so IMV has the rows
    EXECUTE insert_sql;

    -- 1. pg_reflex: DELETE with trigger
    t0 := clock_timestamp();
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    t1 := clock_timestamp();
    reflex_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

    -- Re-insert for raw DELETE (no trigger — IMV state irrelevant for raw measurement)
    SET LOCAL session_replication_role = replica;
    EXECUTE insert_sql;

    -- 2. Raw DELETE (no trigger)
    t0 := clock_timestamp();
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    t1 := clock_timestamp();
    raw_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;
    SET LOCAL session_replication_role = DEFAULT;

    -- Cleanup any leftover IMV rows
    PERFORM _bench_cleanup();

    metric := 'pg_reflex (DELETE+trigger)';    ms := ROUND(reflex_ms, 1);              RETURN NEXT;
    metric := 'bare DELETE (no trigger)';      ms := ROUND(raw_ms, 1);                 RETURN NEXT;
    metric := 'REFRESH MATVIEW (baseline)';    ms := ROUND(baseline_ms, 1);             RETURN NEXT;
    metric := 'fair baseline (bare+REFRESH)';  ms := ROUND(raw_ms + baseline_ms, 1);    RETURN NEXT;
    metric := 'pg_reflex advantage %';         ms := ROUND(100.0 * (1.0 - reflex_ms / NULLIF(raw_ms + baseline_ms, 0)), 1); RETURN NEXT;
END $$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bench_update(batch_size INTEGER)
RETURNS TABLE(metric TEXT, ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    reflex_ms NUMERIC; raw_ms NUMERIC; baseline_ms NUMERIC;
    pool_sz INTEGER; insert_sql TEXT;
BEGIN
    SELECT COUNT(*) INTO pool_sz FROM _bench_pool;
    SELECT refresh_ms INTO baseline_ms FROM _bench_baseline;
    insert_sql := _gen_insert_sql(pool_sz, batch_size);

    -- Setup: insert with trigger so IMV has the rows
    EXECUTE insert_sql;

    -- 1. pg_reflex: UPDATE with trigger
    t0 := clock_timestamp();
    UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1 WHERE order_date >= '2028-01-07'::timestamptz;
    t1 := clock_timestamp();
    reflex_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

    -- 2. Raw UPDATE (no trigger)
    SET LOCAL session_replication_role = replica;
    t0 := clock_timestamp();
    UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1 WHERE order_date >= '2028-01-07'::timestamptz;
    t1 := clock_timestamp();
    raw_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;
    SET LOCAL session_replication_role = DEFAULT;

    -- Cleanup
    PERFORM _bench_cleanup();

    metric := 'pg_reflex (UPDATE+trigger)';    ms := ROUND(reflex_ms, 1);              RETURN NEXT;
    metric := 'bare UPDATE (no trigger)';      ms := ROUND(raw_ms, 1);                 RETURN NEXT;
    metric := 'REFRESH MATVIEW (baseline)';    ms := ROUND(baseline_ms, 1);             RETURN NEXT;
    metric := 'fair baseline (bare+REFRESH)';  ms := ROUND(raw_ms + baseline_ms, 1);    RETURN NEXT;
    metric := 'pg_reflex advantage %';         ms := ROUND(100.0 * (1.0 - reflex_ms / NULLIF(raw_ms + baseline_ms, 0)), 1); RETURN NEXT;
END $$ LANGUAGE plpgsql;

-- ==========================================================================
-- INSERT BENCHMARK
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  INSERT BENCHMARK'
\echo '================================================================'

\echo '--- INSERT 1,000 rows ---'
SELECT * FROM bench_insert(1000);
\echo '--- INSERT 5,000 rows ---'
SELECT * FROM bench_insert(5000);
\echo '--- INSERT 10,000 rows ---'
SELECT * FROM bench_insert(10000);
\echo '--- INSERT 25,000 rows ---'
SELECT * FROM bench_insert(25000);
\echo '--- INSERT 50,000 rows ---'
SELECT * FROM bench_insert(50000);
\echo '--- INSERT 100,000 rows ---'
SELECT * FROM bench_insert(100000);

-- ==========================================================================
-- DELETE BENCHMARK
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  DELETE BENCHMARK'
\echo '================================================================'

\echo '--- DELETE 1,000 rows ---'
SELECT * FROM bench_delete(1000);
\echo '--- DELETE 5,000 rows ---'
SELECT * FROM bench_delete(5000);
\echo '--- DELETE 10,000 rows ---'
SELECT * FROM bench_delete(10000);
\echo '--- DELETE 25,000 rows ---'
SELECT * FROM bench_delete(25000);
\echo '--- DELETE 50,000 rows ---'
SELECT * FROM bench_delete(50000);
\echo '--- DELETE 100,000 rows ---'
SELECT * FROM bench_delete(100000);

-- ==========================================================================
-- UPDATE BENCHMARK
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  UPDATE BENCHMARK'
\echo '================================================================'

\echo '--- UPDATE 1,000 rows ---'
SELECT * FROM bench_update(1000);
\echo '--- UPDATE 5,000 rows ---'
SELECT * FROM bench_update(5000);
\echo '--- UPDATE 10,000 rows ---'
SELECT * FROM bench_update(10000);
\echo '--- UPDATE 25,000 rows ---'
SELECT * FROM bench_update(25000);
\echo '--- UPDATE 50,000 rows ---'
SELECT * FROM bench_update(50000);
\echo '--- UPDATE 100,000 rows ---'
SELECT * FROM bench_update(100000);

-- ==========================================================================
-- CORRECTNESS CHECK
-- ==========================================================================
\echo ''
\echo '--- Correctness check ---'

SELECT reflex_reconcile('alp.sop_forecast_reflex');
REFRESH MATERIALIZED VIEW alp.sop_forecast_view;

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) || ' mismatches' END AS correctness
FROM (
    SELECT * FROM alp.sop_forecast_reflex
    EXCEPT ALL
    SELECT * FROM alp.sop_forecast_view
) diff;

-- ==========================================================================
-- CLEANUP
-- ==========================================================================
\echo '--- Cleanup ---'
DROP FUNCTION IF EXISTS bench_insert(INTEGER);
DROP FUNCTION IF EXISTS bench_delete(INTEGER);
DROP FUNCTION IF EXISTS bench_update(INTEGER);
DROP FUNCTION IF EXISTS _gen_insert_sql(INTEGER, INTEGER);
DROP FUNCTION IF EXISTS _bench_cleanup();
DROP TABLE IF EXISTS _bench_pool;
DROP TABLE IF EXISTS _bench_baseline;
SELECT drop_reflex_ivm('alp.sop_forecast_reflex');

\echo ''
\echo '================================================================'
\echo '  BENCHMARK COMPLETE'
\echo '================================================================'
