-- ==========================================================================
--  Diagnostic: Why does pg_reflex trigger overhead cliff at 25K→50K?
--
--  This script captures EXPLAIN (ANALYZE, BUFFERS, WAL) for the exact
--  delta SQL that the trigger executes, at 25K and 50K batch sizes.
--
--  Run on db_clone with the IMV already created + indexes in place.
--  Prerequisite: run bench_sop_forecast.sql setup section first, or:
--    SELECT create_reflex_ivm('alp.sop_forecast_reflex', ...);
--    CREATE INDEX ... (4 indexes)
--
--  Usage: psql -d db_clone -f benchmarks/bench_diagnostic_cliff.sql
-- ==========================================================================
\timing on
SELECT setseed(0.42);

\echo ''
\echo '================================================================'
\echo '  DIAGNOSTIC: 25K vs 50K INSERT cliff analysis'
\echo '================================================================'

-- ==========================================================================
-- SETUP: Create IMV if not exists
-- ==========================================================================
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.__reflex_ivm_reference WHERE name = 'alp.sop_forecast_reflex') THEN
        RAISE NOTICE 'IMV does not exist — run bench_sop_forecast.sql setup first';
        RAISE EXCEPTION 'Missing IMV';
    END IF;
END $$;

-- Pool of (product_id, location_id) pairs for test data
CREATE TEMP TABLE _diag_pool AS
SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY product_id, location_id) AS rn
FROM (SELECT DISTINCT product_id FROM alp.sales_simulation WHERE dem_plan_id = 605) p
CROSS JOIN (VALUES (50), (51)) l(location_id);

SELECT COUNT(*) AS pool_size FROM _diag_pool;

-- Helper: generate INSERT SQL into a temp staging table (not the source)
CREATE OR REPLACE FUNCTION _diag_gen_rows(batch_size INTEGER)
RETURNS TEXT AS $$
DECLARE pool_sz INTEGER;
BEGIN
    SELECT COUNT(*) INTO pool_sz FROM _diag_pool;
    RETURN format(
        'CREATE TEMP TABLE _diag_delta AS
         SELECT 605 AS dem_plan_id, product_id, location_id,
                ''2028-01-07''::timestamptz + (((rn_global - 1) / %s) * interval ''7 days'') AS order_date,
                2028 AS year, 1 AS month, (2 + ((rn_global - 1) / %s)::int) AS week, 2028 AS isoyear,
                (random() * 100)::int AS qty_sales,
                (random() * 120)::int AS qty_sales_ub,
                (random() * 80)::int AS qty_sales_lb,
                (random() * 100)::int AS forecast_base
         FROM (
             SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY d.date_idx, bp.rn) AS rn_global
             FROM _diag_pool bp
             CROSS JOIN generate_series(1, GREATEST(1, CEIL(%s::float / %s)::int)) d(date_idx)
             LIMIT %s
         ) sub',
        pool_sz, pool_sz, batch_size, pool_sz, batch_size
    );
END $$ LANGUAGE plpgsql;


-- ==========================================================================
-- TEST 1: EXPLAIN ANALYZE the delta INSERT at 25K rows
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  TEST 1: 25K rows — EXPLAIN ANALYZE of delta INSERT'
\echo '================================================================'

-- Generate 25K test rows in a temp table
SELECT _diag_gen_rows(25000) AS sql \gset
:sql;
SELECT COUNT(*) AS "delta rows (25K)" FROM _diag_delta;

-- EXPLAIN the exact query the trigger would execute:
-- INSERT INTO target SELECT ... FROM _diag_delta (as transition table) JOIN ...
\echo ''
\echo '--- EXPLAIN (ANALYZE, BUFFERS) INSERT into target from 25K delta ---'

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO alp.sop_forecast_reflex
SELECT
    _diag_delta.dem_plan_id,
    _diag_delta.week,
    _diag_delta.isoyear,
    _diag_delta.year,
    _diag_delta.month,
    _diag_delta.order_date,
    _diag_delta.product_id,
    _diag_delta.location_id,
    location.canal_id,
    _diag_delta.forecast_base::bigint AS forecast_base,
    _diag_delta.qty_sales::bigint AS quantity,
    _diag_delta.qty_sales_ub::bigint AS quantity_ub,
    _diag_delta.qty_sales_lb::bigint AS quantity_lb,
    _diag_delta.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
    _diag_delta.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
    _diag_delta.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
    _diag_delta.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
    caav.product_id IS NOT NULL AS in_current_assortment
FROM _diag_delta
    JOIN alp.demand_planning ON demand_planning.id = _diag_delta.dem_plan_id
    JOIN alp.location ON location.id = _diag_delta.location_id
    JOIN alp.product ON product.id = _diag_delta.product_id
    LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
        AND pricing.canal_id = location.canal_id
        AND _diag_delta.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_view caav
        ON caav.product_id = _diag_delta.product_id
        AND caav.location_id = _diag_delta.location_id
WHERE demand_planning.is_sent_to_sop
    AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]));

-- Time the same INSERT without EXPLAIN (actual execution)
\echo ''
\echo '--- Timed INSERT (25K, no EXPLAIN overhead) ---'
DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;

INSERT INTO alp.sop_forecast_reflex
SELECT
    _diag_delta.dem_plan_id,
    _diag_delta.week,
    _diag_delta.isoyear,
    _diag_delta.year,
    _diag_delta.month,
    _diag_delta.order_date,
    _diag_delta.product_id,
    _diag_delta.location_id,
    location.canal_id,
    _diag_delta.forecast_base::bigint AS forecast_base,
    _diag_delta.qty_sales::bigint AS quantity,
    _diag_delta.qty_sales_ub::bigint AS quantity_ub,
    _diag_delta.qty_sales_lb::bigint AS quantity_lb,
    _diag_delta.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
    _diag_delta.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
    _diag_delta.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
    _diag_delta.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
    caav.product_id IS NOT NULL AS in_current_assortment
FROM _diag_delta
    JOIN alp.demand_planning ON demand_planning.id = _diag_delta.dem_plan_id
    JOIN alp.location ON location.id = _diag_delta.location_id
    JOIN alp.product ON product.id = _diag_delta.product_id
    LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
        AND pricing.canal_id = location.canal_id
        AND _diag_delta.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_view caav
        ON caav.product_id = _diag_delta.product_id
        AND caav.location_id = _diag_delta.location_id
WHERE demand_planning.is_sent_to_sop
    AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]));

-- Cleanup 25K
DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
DROP TABLE _diag_delta;


-- ==========================================================================
-- TEST 2: EXPLAIN ANALYZE the delta INSERT at 50K rows
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  TEST 2: 50K rows — EXPLAIN ANALYZE of delta INSERT'
\echo '================================================================'

SELECT _diag_gen_rows(50000) AS sql \gset
:sql;
SELECT COUNT(*) AS "delta rows (50K)" FROM _diag_delta;

\echo ''
\echo '--- EXPLAIN (ANALYZE, BUFFERS) INSERT into target from 50K delta ---'

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO alp.sop_forecast_reflex
SELECT
    _diag_delta.dem_plan_id,
    _diag_delta.week,
    _diag_delta.isoyear,
    _diag_delta.year,
    _diag_delta.month,
    _diag_delta.order_date,
    _diag_delta.product_id,
    _diag_delta.location_id,
    location.canal_id,
    _diag_delta.forecast_base::bigint AS forecast_base,
    _diag_delta.qty_sales::bigint AS quantity,
    _diag_delta.qty_sales_ub::bigint AS quantity_ub,
    _diag_delta.qty_sales_lb::bigint AS quantity_lb,
    _diag_delta.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
    _diag_delta.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
    _diag_delta.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
    _diag_delta.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
    caav.product_id IS NOT NULL AS in_current_assortment
FROM _diag_delta
    JOIN alp.demand_planning ON demand_planning.id = _diag_delta.dem_plan_id
    JOIN alp.location ON location.id = _diag_delta.location_id
    JOIN alp.product ON product.id = _diag_delta.product_id
    LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
        AND pricing.canal_id = location.canal_id
        AND _diag_delta.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_view caav
        ON caav.product_id = _diag_delta.product_id
        AND caav.location_id = _diag_delta.location_id
WHERE demand_planning.is_sent_to_sop
    AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]));

-- Timed INSERT (50K)
\echo ''
\echo '--- Timed INSERT (50K, no EXPLAIN overhead) ---'
DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;

INSERT INTO alp.sop_forecast_reflex
SELECT
    _diag_delta.dem_plan_id,
    _diag_delta.week,
    _diag_delta.isoyear,
    _diag_delta.year,
    _diag_delta.month,
    _diag_delta.order_date,
    _diag_delta.product_id,
    _diag_delta.location_id,
    location.canal_id,
    _diag_delta.forecast_base::bigint AS forecast_base,
    _diag_delta.qty_sales::bigint AS quantity,
    _diag_delta.qty_sales_ub::bigint AS quantity_ub,
    _diag_delta.qty_sales_lb::bigint AS quantity_lb,
    _diag_delta.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
    _diag_delta.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
    _diag_delta.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
    _diag_delta.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
    caav.product_id IS NOT NULL AS in_current_assortment
FROM _diag_delta
    JOIN alp.demand_planning ON demand_planning.id = _diag_delta.dem_plan_id
    JOIN alp.location ON location.id = _diag_delta.location_id
    JOIN alp.product ON product.id = _diag_delta.product_id
    LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
        AND pricing.canal_id = location.canal_id
        AND _diag_delta.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_view caav
        ON caav.product_id = _diag_delta.product_id
        AND caav.location_id = _diag_delta.location_id
WHERE demand_planning.is_sent_to_sop
    AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]));

-- Cleanup 50K
DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
DROP TABLE _diag_delta;


-- ==========================================================================
-- TEST 3: SELECT-only (no INSERT) to isolate JOIN cost from index maintenance
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  TEST 3: SELECT-only (no INSERT) — isolate JOIN cost'
\echo '================================================================'

-- 25K SELECT only
SELECT _diag_gen_rows(25000) AS sql \gset
:sql;

\echo '--- 25K SELECT only (no write) ---'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    _diag_delta.dem_plan_id,
    _diag_delta.week,
    _diag_delta.isoyear,
    _diag_delta.year,
    _diag_delta.month,
    _diag_delta.order_date,
    _diag_delta.product_id,
    _diag_delta.location_id,
    location.canal_id,
    _diag_delta.forecast_base::bigint AS forecast_base,
    _diag_delta.qty_sales::bigint AS quantity,
    _diag_delta.qty_sales_ub::bigint AS quantity_ub,
    _diag_delta.qty_sales_lb::bigint AS quantity_lb,
    _diag_delta.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
    _diag_delta.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
    _diag_delta.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
    _diag_delta.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
    caav.product_id IS NOT NULL AS in_current_assortment
FROM _diag_delta
    JOIN alp.demand_planning ON demand_planning.id = _diag_delta.dem_plan_id
    JOIN alp.location ON location.id = _diag_delta.location_id
    JOIN alp.product ON product.id = _diag_delta.product_id
    LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
        AND pricing.canal_id = location.canal_id
        AND _diag_delta.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_view caav
        ON caav.product_id = _diag_delta.product_id
        AND caav.location_id = _diag_delta.location_id
WHERE demand_planning.is_sent_to_sop
    AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]));

DROP TABLE _diag_delta;

-- 50K SELECT only
SELECT _diag_gen_rows(50000) AS sql \gset
:sql;

\echo ''
\echo '--- 50K SELECT only (no write) ---'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    _diag_delta.dem_plan_id,
    _diag_delta.week,
    _diag_delta.isoyear,
    _diag_delta.year,
    _diag_delta.month,
    _diag_delta.order_date,
    _diag_delta.product_id,
    _diag_delta.location_id,
    location.canal_id,
    _diag_delta.forecast_base::bigint AS forecast_base,
    _diag_delta.qty_sales::bigint AS quantity,
    _diag_delta.qty_sales_ub::bigint AS quantity_ub,
    _diag_delta.qty_sales_lb::bigint AS quantity_lb,
    _diag_delta.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
    _diag_delta.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
    _diag_delta.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
    _diag_delta.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
    caav.product_id IS NOT NULL AS in_current_assortment
FROM _diag_delta
    JOIN alp.demand_planning ON demand_planning.id = _diag_delta.dem_plan_id
    JOIN alp.location ON location.id = _diag_delta.location_id
    JOIN alp.product ON product.id = _diag_delta.product_id
    LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
        AND pricing.canal_id = location.canal_id
        AND _diag_delta.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_view caav
        ON caav.product_id = _diag_delta.product_id
        AND caav.location_id = _diag_delta.location_id
WHERE demand_planning.is_sent_to_sop
    AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]));

DROP TABLE _diag_delta;


-- ==========================================================================
-- TEST 4: INSERT without indexes — isolate index maintenance cost
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  TEST 4: INSERT without target indexes — isolate index cost'
\echo '================================================================'

-- Save and drop all indexes on the IMV target
CREATE TEMP TABLE _saved_indexes AS
SELECT indexname, indexdef FROM pg_indexes
WHERE schemaname = 'alp' AND tablename = 'sop_forecast_reflex';

SELECT COUNT(*) AS "indexes to drop" FROM _saved_indexes;

DO $$ DECLARE r RECORD; BEGIN
    FOR r IN SELECT indexname FROM pg_indexes WHERE schemaname = 'alp' AND tablename = 'sop_forecast_reflex' LOOP
        EXECUTE format('DROP INDEX IF EXISTS alp.%I', r.indexname);
    END LOOP;
END $$;

-- 50K INSERT without indexes
SELECT _diag_gen_rows(50000) AS sql \gset
:sql;

\echo '--- 50K INSERT into target WITHOUT indexes ---'

INSERT INTO alp.sop_forecast_reflex
SELECT
    _diag_delta.dem_plan_id,
    _diag_delta.week,
    _diag_delta.isoyear,
    _diag_delta.year,
    _diag_delta.month,
    _diag_delta.order_date,
    _diag_delta.product_id,
    _diag_delta.location_id,
    location.canal_id,
    _diag_delta.forecast_base::bigint AS forecast_base,
    _diag_delta.qty_sales::bigint AS quantity,
    _diag_delta.qty_sales_ub::bigint AS quantity_ub,
    _diag_delta.qty_sales_lb::bigint AS quantity_lb,
    _diag_delta.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
    _diag_delta.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
    _diag_delta.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
    _diag_delta.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
    caav.product_id IS NOT NULL AS in_current_assortment
FROM _diag_delta
    JOIN alp.demand_planning ON demand_planning.id = _diag_delta.dem_plan_id
    JOIN alp.location ON location.id = _diag_delta.location_id
    JOIN alp.product ON product.id = _diag_delta.product_id
    LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
        AND pricing.canal_id = location.canal_id
        AND _diag_delta.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_view caav
        ON caav.product_id = _diag_delta.product_id
        AND caav.location_id = _diag_delta.location_id
WHERE demand_planning.is_sent_to_sop
    AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]));

-- Cleanup inserted rows
DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
DROP TABLE _diag_delta;

-- Recreate indexes
\echo '--- Recreating indexes (time each one) ---'
DO $$ DECLARE r RECORD; BEGIN
    FOR r IN SELECT indexdef FROM _saved_indexes LOOP
        EXECUTE r.indexdef;
    END LOOP;
END $$;

DROP TABLE _saved_indexes;


-- ==========================================================================
-- TEST 5: 50K INSERT with work_mem = 256MB
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  TEST 5: 50K INSERT with work_mem = 256MB'
\echo '================================================================'

SELECT _diag_gen_rows(50000) AS sql \gset
:sql;

SET LOCAL work_mem = '256MB';
SET LOCAL maintenance_work_mem = '512MB';

\echo '--- 50K INSERT with boosted work_mem ---'

INSERT INTO alp.sop_forecast_reflex
SELECT
    _diag_delta.dem_plan_id,
    _diag_delta.week,
    _diag_delta.isoyear,
    _diag_delta.year,
    _diag_delta.month,
    _diag_delta.order_date,
    _diag_delta.product_id,
    _diag_delta.location_id,
    location.canal_id,
    _diag_delta.forecast_base::bigint AS forecast_base,
    _diag_delta.qty_sales::bigint AS quantity,
    _diag_delta.qty_sales_ub::bigint AS quantity_ub,
    _diag_delta.qty_sales_lb::bigint AS quantity_lb,
    _diag_delta.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
    _diag_delta.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
    _diag_delta.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
    _diag_delta.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
    caav.product_id IS NOT NULL AS in_current_assortment
FROM _diag_delta
    JOIN alp.demand_planning ON demand_planning.id = _diag_delta.dem_plan_id
    JOIN alp.location ON location.id = _diag_delta.location_id
    JOIN alp.product ON product.id = _diag_delta.product_id
    LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
        AND pricing.canal_id = location.canal_id
        AND _diag_delta.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_view caav
        ON caav.product_id = _diag_delta.product_id
        AND caav.location_id = _diag_delta.location_id
WHERE demand_planning.is_sent_to_sop
    AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]));

RESET work_mem;
RESET maintenance_work_mem;

-- Cleanup
DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
DROP TABLE _diag_delta;


-- ==========================================================================
-- CLEANUP
-- ==========================================================================
DROP FUNCTION IF EXISTS _diag_gen_rows(INTEGER);
DROP TABLE IF EXISTS _diag_pool;

\echo ''
\echo '================================================================'
\echo '  DIAGNOSTIC COMPLETE'
\echo '  Compare:'
\echo '    TEST 1 vs 2: plan differences at 25K vs 50K (planner tipping point?)'
\echo '    TEST 2 vs 3: JOIN cost vs total INSERT cost (index overhead isolation)'
\echo '    TEST 2 vs 4: with vs without indexes (index maintenance cost)'
\echo '    TEST 2 vs 5: default vs boosted work_mem (memory pressure?)'
\echo '================================================================'
