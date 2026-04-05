-- =============================================================================
-- Test script: Create reflex IMVs for alp schema views and validate correctness
-- Run with: psql -U postgres -d db_clone -f benchmarks/test_alp_imvs.sql
-- =============================================================================

\timing on
\set ON_ERROR_STOP off

-- =============================================================================
-- CLEANUP: Drop any previous test IMVs
-- =============================================================================
DO $$
DECLARE
    imv_name TEXT;
BEGIN
    FOR imv_name IN
        SELECT name FROM public.__reflex_ivm_reference
        WHERE name LIKE 'alp.%_reflex'
        AND name != 'alp.sop_forecast_reflex'  -- keep existing one
    LOOP
        RAISE NOTICE 'Dropping existing test IMV: %', imv_name;
        PERFORM drop_reflex_ivm(imv_name, TRUE);
    END LOOP;
END $$;

-- =============================================================================
-- Helper: validation function
-- Compares an IMV target table against the original matview
-- =============================================================================
CREATE OR REPLACE FUNCTION alp.validate_imv(
    matview_name TEXT,
    imv_name TEXT
) RETURNS TABLE(
    test_name TEXT,
    matview_rows BIGINT,
    imv_rows BIGINT,
    extra_in_imv BIGINT,
    missing_from_imv BIGINT,
    status TEXT
) AS $$
DECLARE
    mv_count BIGINT;
    iv_count BIGINT;
    extra BIGINT;
    missing BIGINT;
BEGIN
    -- Count rows
    EXECUTE format('SELECT count(*) FROM %I.%I', 'alp', matview_name) INTO mv_count;
    EXECUTE format('SELECT count(*) FROM %I.%I', 'alp', imv_name) INTO iv_count;

    -- Check extra rows in IMV (rows in IMV but not in matview)
    EXECUTE format(
        'SELECT count(*) FROM (SELECT * FROM %I.%I EXCEPT ALL SELECT * FROM %I.%I) x',
        'alp', imv_name, 'alp', matview_name
    ) INTO extra;

    -- Check missing rows from IMV (rows in matview but not in IMV)
    EXECUTE format(
        'SELECT count(*) FROM (SELECT * FROM %I.%I EXCEPT ALL SELECT * FROM %I.%I) x',
        'alp', matview_name, 'alp', imv_name
    ) INTO missing;

    test_name := matview_name;
    matview_rows := mv_count;
    imv_rows := iv_count;
    extra_in_imv := extra;
    missing_from_imv := missing;

    IF extra = 0 AND missing = 0 THEN
        status := 'PASS';
    ELSE
        status := 'FAIL';
    END IF;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Results table
-- =============================================================================
DROP TABLE IF EXISTS alp._imv_test_results;
CREATE TABLE alp._imv_test_results (
    view_name TEXT,
    action TEXT,
    duration_ms NUMERIC,
    matview_rows BIGINT,
    imv_rows BIGINT,
    extra_in_imv BIGINT,
    missing_from_imv BIGINT,
    status TEXT,
    error_message TEXT,
    tested_at TIMESTAMP DEFAULT now()
);

-- =============================================================================
-- 1. zscore_view - Simple SUM GROUP BY on sales_simulation
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing zscore_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.zscore_view_reflex', '
            SELECT product_id,
                location_id,
                dem_plan_id,
                CASE
                    WHEN SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) = 0
                        AND SUM(ABS(qty_sales)) = 0 THEN 0::numeric
                    WHEN SUM(ABS(qty_sales)) = 0 THEN 2::numeric
                    ELSE 0.5 * SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales))::numeric
                END AS zscore_num,
                CASE
                    WHEN SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) = 0
                        AND SUM(ABS(qty_sales)) = 0 THEN 1::bigint
                    WHEN SUM(ABS(qty_sales)) = 0 THEN 1::bigint
                    WHEN SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) < 0 THEN 1::bigint
                    ELSE SUM(ABS(qty_sales))
                END AS zscore_den,
                CASE
                    WHEN SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) = 0
                        AND SUM(ABS(qty_sales)) = 0 THEN 0::numeric
                    WHEN SUM(ABS(qty_sales)) = 0
                        AND SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) <> 0 THEN 2::numeric
                    ELSE 0.5 * SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales))::numeric
                        / SUM(ABS(qty_sales))::numeric
                END AS zscore
            FROM alp.sales_simulation
            GROUP BY product_id, location_id, dem_plan_id
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('zscore_view', 'zscore_view_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('zscore_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'zscore_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('zscore_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'zscore_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 2. sop_forecast_history_view - Passthrough JOIN (no aggregation)
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing sop_forecast_history_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.sop_forecast_history_reflex', '
            SELECT
                dem_plan_id,
                week,
                isoyear,
                year,
                month,
                order_date,
                sales_simulation.product_id,
                sales_simulation.location_id,
                location.canal_id,
                forecast_base AS forecast_base,
                qty_sales AS quantity,
                qty_sales_ub AS quantity_ub,
                qty_sales_lb AS quantity_lb,
                qty_sales * COALESCE(pricing.base_price, 0) AS turnover,
                forecast_base * COALESCE(pricing.base_price, 0) AS forecast_base_turnover,
                qty_sales_ub * COALESCE(pricing.base_price, 0) AS qty_sales_ub_turnover,
                qty_sales_lb * COALESCE(pricing.base_price, 0) AS qty_sales_lb_turnover,
                (caav.product_id IS NOT NULL) AS in_current_assortment
            FROM
                alp.sales_simulation
                INNER JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
                INNER JOIN alp.location ON location.id = sales_simulation.location_id
                LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
                    AND pricing.canal_id = location.canal_id
                    AND sales_simulation.product_id = pricing.product_id
                LEFT JOIN alp.current_assortment_activity_view caav
                    ON caav.product_id = sales_simulation.product_id
                    AND caav.location_id = sales_simulation.location_id
            WHERE
                demand_planning.status = ''archive''
        ', 'dem_plan_id, product_id, location_id, order_date');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('sop_forecast_history_view', 'sop_forecast_history_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('sop_forecast_history_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'sop_forecast_history_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('sop_forecast_history_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'sop_forecast_history_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 3. sop_last_forecast_view - SUM + BOOL_OR GROUP BY
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing sop_last_forecast_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.sop_last_forecast_reflex', '
            SELECT
                week,
                isoyear,
                year,
                month,
                order_date,
                last_sales_simulation.product_id,
                last_sales_simulation.location_id,
                location.canal_id,
                SUM(forecast_base)::BIGINT AS forecast_base,
                SUM(qty_sales)::BIGINT AS quantity,
                SUM(qty_sales_ub)::BIGINT AS quantity_ub,
                SUM(qty_sales_lb)::BIGINT AS quantity_lb,
                SUM(qty_sales * COALESCE(latest_price_view.base_price, 0)) AS turnover,
                SUM(forecast_base * COALESCE(latest_price_view.base_price, 0)) AS forecast_base_turnover,
                SUM(qty_sales_ub * COALESCE(latest_price_view.base_price, 0)) AS qty_sales_ub_turnover,
                SUM(qty_sales_lb * COALESCE(latest_price_view.base_price, 0)) AS qty_sales_lb_turnover,
                bool_or(caav.product_id IS NOT NULL) AS in_current_assortment
            FROM
                alp.last_sales_simulation
                INNER JOIN alp.location ON location.id = last_sales_simulation.location_id
                INNER JOIN alp.product ON product.id = last_sales_simulation.product_id
                LEFT JOIN alp.latest_price_view ON latest_price_view.canal_id = location.canal_id
                    AND last_sales_simulation.product_id = latest_price_view.product_id
                LEFT JOIN alp.current_assortment_activity_view caav
                    ON caav.product_id = last_sales_simulation.product_id
                    AND caav.location_id = last_sales_simulation.location_id
            GROUP BY
                year, month, week, isoyear, order_date,
                last_sales_simulation.product_id,
                last_sales_simulation.location_id,
                location.canal_id
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('sop_last_forecast_view', 'sop_last_forecast_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('sop_last_forecast_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'sop_last_forecast_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('sop_last_forecast_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'sop_last_forecast_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 4. unsent_sop_forecast_view - SUM + BOOL_OR GROUP BY
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing unsent_sop_forecast_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.unsent_sop_forecast_reflex', '
            SELECT
                dem_plan_id,
                week,
                isoyear,
                year,
                month,
                order_date,
                sales_simulation.product_id,
                sales_simulation.location_id,
                location.canal_id,
                SUM(forecast_base)::BIGINT AS forecast_base,
                SUM(qty_sales)::BIGINT AS quantity,
                SUM(qty_sales_ub)::BIGINT AS quantity_ub,
                SUM(qty_sales_lb)::BIGINT AS quantity_lb,
                SUM(qty_sales * COALESCE(pricing.base_price, 0)) AS turnover,
                SUM(forecast_base * COALESCE(pricing.base_price, 0)) AS forecast_base_turnover,
                SUM(qty_sales_ub * COALESCE(pricing.base_price, 0)) AS qty_sales_ub_turnover,
                SUM(qty_sales_lb * COALESCE(pricing.base_price, 0)) AS qty_sales_lb_turnover,
                bool_or(caav.product_id IS NOT NULL) AS in_current_assortment
            FROM
                alp.sales_simulation
                INNER JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
                INNER JOIN alp.location ON location.id = sales_simulation.location_id
                LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
                    AND pricing.canal_id = location.canal_id
                    AND sales_simulation.product_id = pricing.product_id
                LEFT JOIN alp.current_assortment_activity_view caav
                    ON caav.product_id = sales_simulation.product_id
                    AND caav.location_id = sales_simulation.location_id
            WHERE
                NOT demand_planning.is_sent_to_sop
                AND NOT demand_planning.is_draft
                AND demand_planning.status NOT IN (''archive'', ''archiving'')
            GROUP BY
                dem_plan_id,
                year, month, week, isoyear, order_date,
                sales_simulation.product_id,
                sales_simulation.location_id,
                location.canal_id
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('unsent_sop_forecast_view', 'unsent_sop_forecast_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('unsent_sop_forecast_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'unsent_sop_forecast_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('unsent_sop_forecast_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'unsent_sop_forecast_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 5. stock_transfer_baseline_view - SUM + BOOL_OR GROUP BY
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing stock_transfer_baseline_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.stock_transfer_baseline_reflex', '
            SELECT
                supply_plan_id,
                EXTRACT(week FROM transfer_date) AS week,
                EXTRACT(MONTH FROM transfer_date) AS month,
                EXTRACT(YEAR FROM transfer_date) AS year,
                EXTRACT(ISOYEAR FROM transfer_date) AS isoyear,
                stock_transfer_baseline.transfer_date,
                stock_transfer_baseline.product_id,
                stock_transfer_baseline.to_location_id AS location_id,
                stock_transfer_baseline.from_location_id AS warehouse_id,
                SUM(quantity_kept)::BIGINT AS quantity,
                SUM(quantity_kept * COALESCE(unit_pricing.unit_price, 0)) AS turnover,
                bool_or(caav.product_id IS NOT NULL) AS in_current_assortment
            FROM
                alp.stock_transfer_baseline
                INNER JOIN alp.supply_plan ON supply_plan.id = stock_transfer_baseline.supply_plan_id
                LEFT OUTER JOIN alp.unit_pricing ON unit_pricing.product_id = stock_transfer_baseline.product_id
                LEFT JOIN alp.current_assortment_activity_view caav
                    ON caav.product_id = stock_transfer_baseline.product_id
                    AND caav.location_id = stock_transfer_baseline.to_location_id
            GROUP BY
                EXTRACT(MONTH FROM transfer_date),
                EXTRACT(YEAR FROM transfer_date),
                stock_transfer_baseline.product_id,
                to_location_id,
                transfer_date,
                stock_transfer_baseline.supply_plan_id,
                stock_transfer_baseline.from_location_id
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('stock_transfer_baseline_view', 'stock_transfer_baseline_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('stock_transfer_baseline_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'stock_transfer_baseline_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('stock_transfer_baseline_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'stock_transfer_baseline_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 6. stock_transfer_view - SUM + BOOL_OR GROUP BY
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing stock_transfer_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.stock_transfer_reflex', '
            SELECT
                EXTRACT(week FROM transfer_date) AS week,
                EXTRACT(MONTH FROM transfer_date) AS month,
                EXTRACT(YEAR FROM transfer_date) AS year,
                EXTRACT(ISOYEAR FROM transfer_date) AS isoyear,
                stock_transfer.transfer_date,
                stock_transfer.product_id,
                stock_transfer.to_location_id AS location_id,
                stock_transfer.from_location_id AS warehouse_id,
                SUM(quantity)::BIGINT AS quantity,
                SUM(quantity * COALESCE(unit_pricing.unit_price, 0)) AS turnover,
                bool_or(caav.product_id IS NOT NULL) AS in_current_assortment
            FROM
                alp.stock_transfer
                LEFT OUTER JOIN alp.unit_pricing ON unit_pricing.product_id = stock_transfer.product_id
                LEFT JOIN alp.current_assortment_activity_view caav
                    ON caav.product_id = stock_transfer.product_id
                    AND caav.location_id = stock_transfer.to_location_id
            WHERE
                EXTRACT(YEAR FROM stock_transfer.transfer_date)::INTEGER >= (
                    SELECT year FROM alp.max_order_date_view
                ) - 2
            GROUP BY
                EXTRACT(MONTH FROM transfer_date),
                EXTRACT(YEAR FROM transfer_date),
                stock_transfer.product_id,
                to_location_id,
                transfer_date,
                stock_transfer.from_location_id
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('stock_transfer_view', 'stock_transfer_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('stock_transfer_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'stock_transfer_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('stock_transfer_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'stock_transfer_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 7. sop_purchase_view - SUM + EXISTS GROUP BY
-- Note: EXISTS subquery for in_current_assortment - testing if pg_reflex handles it
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing sop_purchase_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.sop_purchase_reflex', '
            SELECT
                EXTRACT(WEEK FROM DATE_TRUNC(''day'', po.order_date)) AS week,
                EXTRACT(MONTH FROM DATE_TRUNC(''day'', po.order_date)) AS month,
                EXTRACT(YEAR FROM DATE_TRUNC(''day'', po.order_date)) AS year,
                EXTRACT(ISOYEAR FROM DATE_TRUNC(''day'', po.order_date)) AS isoyear,
                pol.product_id,
                po.location_id,
                location.canal_id,
                DATE_TRUNC(''day'', po.order_date) AS purchase_date,
                SUM(ordered_quantity)::BIGINT AS quantity,
                SUM(ordered_quantity * COALESCE(unit_pricing.unit_price, 0)) AS turnover,
                EXISTS (
                    SELECT 1 FROM alp.current_assortment_activity_view caav
                    WHERE caav.product_id = pol.product_id
                ) AS in_current_assortment
            FROM
                alp.purchase_order_line AS pol
                INNER JOIN alp.purchase_order AS po ON po.id = pol.po_id
                INNER JOIN alp.location ON po.location_id = location.id
                LEFT OUTER JOIN alp.unit_pricing ON unit_pricing.product_id = pol.product_id
            WHERE
                EXTRACT(YEAR FROM po.order_date)::INTEGER >= (
                    SELECT year FROM alp.max_order_date_view
                ) - 2
            GROUP BY
                pol.product_id,
                location_id,
                canal_id,
                DATE_TRUNC(''day'', po.order_date)
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('sop_purchase_view', 'sop_purchase_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('sop_purchase_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'sop_purchase_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('sop_purchase_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'sop_purchase_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 8. sop_purchase_baseline_view - SUM + EXISTS GROUP BY
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing sop_purchase_baseline_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.sop_purchase_baseline_reflex', '
            SELECT
                supply_plan_id,
                DATE_TRUNC(''day'', order_date) AS purchase_date,
                EXTRACT(WEEK FROM DATE_TRUNC(''day'', order_date)) AS week,
                EXTRACT(MONTH FROM DATE_TRUNC(''day'', order_date)) AS month,
                EXTRACT(YEAR FROM DATE_TRUNC(''day'', order_date)) AS year,
                EXTRACT(ISOYEAR FROM DATE_TRUNC(''day'', order_date)) AS isoyear,
                purchase_baseline.product_id,
                purchase_baseline.location_id,
                location.canal_id,
                SUM(ordered_quantity_kept)::BIGINT AS quantity,
                SUM(ordered_quantity_kept * COALESCE(unit_pricing.unit_price, 0)) AS turnover,
                EXISTS (
                    SELECT 1 FROM alp.current_assortment_activity_view caav
                    WHERE caav.product_id = purchase_baseline.product_id
                ) AS in_current_assortment
            FROM
                alp.purchase_baseline
                INNER JOIN alp.supply_plan ON supply_plan.id = purchase_baseline.supply_plan_id
                INNER JOIN alp.location ON purchase_baseline.location_id = location.id
                LEFT OUTER JOIN alp.unit_pricing ON unit_pricing.product_id = purchase_baseline.product_id
            WHERE
                purchase_baseline.state != ''rejected''
            GROUP BY
                purchase_baseline.product_id,
                location_id,
                canal_id,
                DATE_TRUNC(''day'', order_date),
                purchase_baseline.supply_plan_id
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('sop_purchase_baseline_view', 'sop_purchase_baseline_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('sop_purchase_baseline_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'sop_purchase_baseline_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('sop_purchase_baseline_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'sop_purchase_baseline_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 9. history_sales_view - CTE with SUM GROUP BY + JOIN
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing history_sales_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.history_sales_reflex', '
            WITH historical_orders AS (
                SELECT
                    DATE_TRUNC(''day'', order_line.order_date) AS order_date,
                    "order".location_id,
                    order_line.product_id,
                    sum(order_line.quantity) AS quantity,
                    sum(order_line.sub_total) AS turnover,
                    location.canal_id
                FROM
                    alp."order"
                    JOIN alp.order_line ON order_line.order_id = "order".id
                    JOIN alp.location ON "order".location_id = location.id
                WHERE
                    EXTRACT(YEAR FROM order_line.order_date)::INTEGER >= (
                        SELECT year FROM alp.max_order_date_view
                    ) - 2
                GROUP BY
                    "order".location_id,
                    order_line.product_id,
                    location.canal_id,
                    DATE_TRUNC(''day'', order_line.order_date)
            )
            SELECT
                product.sku,
                product.description,
                product.image_url,
                EXTRACT(ISOYEAR FROM historical_orders.order_date)::INTEGER AS isoyear,
                EXTRACT(YEAR FROM historical_orders.order_date)::INTEGER AS year,
                EXTRACT(MONTH FROM historical_orders.order_date)::INTEGER AS month,
                EXTRACT(WEEK FROM historical_orders.order_date)::INTEGER AS week,
                historical_orders.order_date,
                historical_orders.location_id,
                historical_orders.product_id,
                historical_orders.quantity,
                historical_orders.turnover,
                historical_orders.canal_id,
                (caav.product_id IS NOT NULL) AS in_current_assortment
            FROM
                historical_orders
                JOIN alp.product ON historical_orders.product_id = product.id
                LEFT JOIN alp.current_assortment_activity_view caav
                    ON caav.product_id = historical_orders.product_id
                    AND caav.location_id = historical_orders.location_id
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('history_sales_view', 'history_sales_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('history_sales_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'history_sales_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('history_sales_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'history_sales_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 10. last_month_sales_view - CTE with SUM CASE GROUP BY (CROSS JOIN to matview)
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing last_month_sales_view ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.last_month_sales_reflex', '
            WITH order_data_with_windows AS (
                SELECT
                    o.location_id,
                    ol.product_id,
                    ol.quantity,
                    ol.sub_total,
                    ol.sub_total_tax,
                    CASE
                        WHEN m.order_date - INTERVAL ''28 days'' < o.order_date THEN ''last_28_days''
                        WHEN m.order_date - INTERVAL ''56 days'' < o.order_date
                            AND m.order_date - INTERVAL ''28 days'' >= o.order_date THEN ''prev_28_days''
                        WHEN m.order_date - INTERVAL ''84 days'' < o.order_date
                            AND m.order_date - INTERVAL ''56 days'' >= o.order_date THEN ''prev_56_days''
                        ELSE NULL
                    END AS time_window
                FROM
                    alp."order" AS o
                    CROSS JOIN alp.max_order_date_view m
                    JOIN alp.order_line AS ol ON ol.order_id = o.id
                WHERE
                    o.order_date >= m.order_date - INTERVAL ''90 days''
            )
            SELECT
                product_id,
                location_id,
                SUM(CASE WHEN time_window = ''last_28_days'' THEN quantity ELSE 0 END) AS quantity,
                SUM(CASE WHEN time_window = ''last_28_days'' THEN sub_total ELSE 0 END) AS value,
                SUM(CASE WHEN time_window = ''last_28_days'' THEN sub_total_tax ELSE 0 END) AS value_tax,
                SUM(CASE WHEN time_window = ''prev_28_days'' THEN quantity ELSE 0 END) AS l2m_quantity,
                SUM(CASE WHEN time_window = ''prev_28_days'' THEN sub_total ELSE 0 END) AS l2m_value,
                SUM(CASE WHEN time_window = ''prev_28_days'' THEN sub_total_tax ELSE 0 END) AS l2m_value_tax,
                SUM(CASE WHEN time_window = ''prev_56_days'' THEN quantity ELSE 0 END) AS l3m_quantity,
                SUM(CASE WHEN time_window = ''prev_56_days'' THEN sub_total ELSE 0 END) AS l3m_value,
                SUM(CASE WHEN time_window = ''prev_56_days'' THEN sub_total_tax ELSE 0 END) AS l3m_value_tax
            FROM
                order_data_with_windows
            GROUP BY
                location_id,
                product_id
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('last_month_sales_view', 'last_month_sales_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('last_month_sales_view', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'last_month_sales_view: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('last_month_sales_view', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'last_month_sales_view: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 11. event_demand_planning_sales - SUM GROUP BY (CROSS JOIN + subquery WHERE)
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Testing event_demand_planning_sales ===';
    t0 := clock_timestamp();
    BEGIN
        PERFORM create_reflex_ivm('alp.event_demand_planning_sales_reflex', '
            SELECT
                event_demand_planning.event_id,
                sales_simulation.product_id,
                sales_simulation.location_id,
                sales_simulation.order_date AS sale_date,
                SUM(sales_simulation.qty_sales) AS planned_qty,
                COALESCE(SUM(history_sales_view.quantity), 0) AS sum_sales_qty
            FROM
                alp.sales_simulation
                CROSS JOIN alp.event_demand_planning
                JOIN alp.event ON event_demand_planning.event_id = event.id
                LEFT JOIN alp.history_sales_view ON
                    history_sales_view.order_date BETWEEN event.start_date AND event.end_date
                    AND history_sales_view.order_date = sales_simulation.order_date
                    AND history_sales_view.product_id = sales_simulation.product_id
                    AND history_sales_view.location_id = sales_simulation.location_id
            WHERE
                sales_simulation.order_date BETWEEN event.start_date AND event.end_date
                AND sales_simulation.dem_plan_id = (SELECT dem_plan_id FROM alp.sop_current_view)
                AND event_demand_planning.dem_plan_id = (SELECT dem_plan_id FROM alp.sop_current_view)
            GROUP BY
                event_demand_planning.event_id,
                sales_simulation.product_id,
                sales_simulation.location_id,
                sales_simulation.order_date
        ');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

        SELECT * INTO r FROM alp.validate_imv('event_demand_planning_sales', 'event_demand_planning_sales_reflex');
        INSERT INTO alp._imv_test_results VALUES
            ('event_demand_planning_sales', 'create', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'event_demand_planning_sales: % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('event_demand_planning_sales', 'create', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'event_demand_planning_sales: ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- 12. Validate existing sop_forecast_reflex against sop_forecast_view
-- =============================================================================
DO $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    r RECORD;
BEGIN
    RAISE NOTICE '=== Validating existing sop_forecast_reflex ===';
    t0 := clock_timestamp();
    BEGIN
        SELECT * INTO r FROM alp.validate_imv('sop_forecast_view', 'sop_forecast_reflex');
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('sop_forecast_view (existing)', 'validate', dur, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv, r.status, NULL);
        RAISE NOTICE 'sop_forecast_view (existing): % (matview=%, imv=%, extra=%, missing=%)',
            r.status, r.matview_rows, r.imv_rows, r.extra_in_imv, r.missing_from_imv;
    EXCEPTION WHEN OTHERS THEN
        dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
        INSERT INTO alp._imv_test_results VALUES
            ('sop_forecast_view (existing)', 'validate', dur, NULL, NULL, NULL, NULL, 'ERROR', SQLERRM);
        RAISE NOTICE 'sop_forecast_view (existing): ERROR - %', SQLERRM;
    END;
END $$;

-- =============================================================================
-- FINAL REPORT
-- =============================================================================
\echo ''
\echo '============================================='
\echo '  IMV TEST RESULTS SUMMARY'
\echo '============================================='

SELECT
    view_name,
    action,
    status,
    matview_rows,
    imv_rows,
    extra_in_imv,
    missing_from_imv,
    ROUND(duration_ms) || ' ms' AS duration,
    COALESCE(LEFT(error_message, 80), '') AS error
FROM alp._imv_test_results
ORDER BY tested_at;

\echo ''
\echo '============================================='
\echo '  VIEWS NOT TESTED (Incompatible with IMVs)'
\echo '============================================='
\echo '  max_order_date_view         - MAX only, scalar (1 row)'
\echo '  sop_current_view            - ORDER BY + LIMIT 1'
\echo '  current_assortment_activity - DISTINCT + WHERE subquery'
\echo '  latest_price_view           - DISTINCT ON + ORDER BY'
\echo '  latest_inventory_view       - ROW_NUMBER + WHERE rn=1'
\echo '  incoming_stock_view         - FULL JOIN'
\echo '  inventory_detail_view       - FULL JOIN + complex CTEs'
\echo '  forecast_analysis_view      - FULL JOIN + UNION'
\echo '  latest_inventory_repartition- SUM OVER() window function'
\echo '  last_month_pdm              - SUM OVER() window function'
\echo '  assortment_orders_view      - ROW_NUMBER + EXISTS + CROSS JOIN'
\echo '  assortment_characteristics  - DISTINCT ON subqueries + AVG'
\echo '  product_computed_features   - DISTINCT ON + LEFT JOIN'
\echo '  next_month_sales_view       - SUM FILTER + CTE'
\echo '  stock_chart_weekly_view     - MIN + MAX + BOOL_OR (MIN on delete = rescan)'
\echo '  stock_chart_monthly_view    - MIN + MAX + BOOL_OR (MIN on delete = rescan)'
\echo '  forecast_stock_chart_monthly- MIN + MAX + BOOL_OR (MIN on delete = rescan)'
\echo '  demand_planning_chars_view  - ANY_VALUE + complex subqueries'
\echo '  assortment_details_view     - Passthrough on matview dependencies'
\echo '  appro_summary_view          - Complex CTEs referencing matviews'
\echo '  allocation_summary_view     - Passthrough (could work, low priority)'
\echo '  sop_incoming_stock_view     - UNION ALL in CTE (testing separately)'
\echo '  sop_incoming_stock_baseline - UNION ALL in CTE'
\echo '  sop_received_stock_view     - UNION ALL in CTE'
\echo '  tcd_schema                  - JSON aggregation, schema introspection'
\echo '  sop_kind_info_view          - 10+ CTEs, UNION ALL hierarchy'
\echo ''

-- Count the IMVs now registered
SELECT count(*) AS total_imvs,
       count(*) FILTER (WHERE enabled) AS enabled_imvs
FROM public.__reflex_ivm_reference;
