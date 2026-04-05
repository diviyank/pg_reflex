\timing on
\set ON_ERROR_STOP off

-- Results table
DROP TABLE IF EXISTS alp._imv_test_results_v2;
CREATE TABLE alp._imv_test_results_v2 (
    view_name TEXT,
    status TEXT,
    matview_rows BIGINT,
    imv_rows BIGINT,
    mismatches BIGINT,
    duration_ms NUMERIC,
    error_message TEXT,
    tested_at TIMESTAMP DEFAULT now()
);

-- Helper: create IMV, validate against matview, record result
CREATE OR REPLACE FUNCTION alp.test_imv(
    p_view_name TEXT,
    p_imv_name TEXT,
    p_sql TEXT,
    p_unique_cols TEXT DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    t0 TIMESTAMP;
    dur NUMERIC;
    mv_count BIGINT;
    iv_count BIGINT;
    mismatch_count BIGINT;
BEGIN
    RAISE NOTICE '=== Testing % ===', p_view_name;
    t0 := clock_timestamp();

    -- Create IMV
    IF p_unique_cols IS NOT NULL THEN
        PERFORM create_reflex_ivm(p_imv_name, p_sql, p_unique_cols);
    ELSE
        PERFORM create_reflex_ivm(p_imv_name, p_sql);
    END IF;

    dur := extract(epoch from (clock_timestamp() - t0)) * 1000;

    -- Refresh matview for accurate comparison
    EXECUTE format('REFRESH MATERIALIZED VIEW alp.%I', p_view_name);

    -- Count rows
    EXECUTE format('SELECT count(*) FROM alp.%I', p_view_name) INTO mv_count;
    EXECUTE format('SELECT count(*) FROM alp.%I', split_part(p_imv_name, '.', 2)) INTO iv_count;

    -- EXCEPT ALL check (column-count-safe: use explicit column list from IMV)
    BEGIN
        EXECUTE format(
            'SELECT count(*) FROM (
                (SELECT * FROM alp.%I EXCEPT ALL SELECT * FROM alp.%I)
                UNION ALL
                (SELECT * FROM alp.%I EXCEPT ALL SELECT * FROM alp.%I)
            ) x',
            split_part(p_imv_name, '.', 2), p_view_name,
            p_view_name, split_part(p_imv_name, '.', 2)
        ) INTO mismatch_count;
    EXCEPTION WHEN OTHERS THEN
        -- Type mismatch in EXCEPT ALL — try with ::text cast on all columns
        mismatch_count := -1;
    END;

    IF mismatch_count = 0 THEN
        INSERT INTO alp._imv_test_results_v2 VALUES
            (p_view_name, 'PASS', mv_count, iv_count, 0, dur, NULL);
        RAISE NOTICE '% PASS (rows=%, time=% ms)', p_view_name, mv_count, round(dur);
    ELSIF mismatch_count = -1 THEN
        INSERT INTO alp._imv_test_results_v2 VALUES
            (p_view_name, 'TYPE_MISMATCH', mv_count, iv_count, NULL, dur, 'EXCEPT ALL type mismatch');
        RAISE NOTICE '% TYPE_MISMATCH (mv=%, imv=%)', p_view_name, mv_count, iv_count;
    ELSE
        INSERT INTO alp._imv_test_results_v2 VALUES
            (p_view_name, 'FAIL', mv_count, iv_count, mismatch_count, dur, NULL);
        RAISE NOTICE '% FAIL (mv=%, imv=%, mismatches=%)', p_view_name, mv_count, iv_count, mismatch_count;
    END IF;

EXCEPTION WHEN OTHERS THEN
    dur := extract(epoch from (clock_timestamp() - t0)) * 1000;
    INSERT INTO alp._imv_test_results_v2 VALUES
        (p_view_name, 'ERROR', NULL, NULL, NULL, dur, SQLERRM);
    RAISE NOTICE '% ERROR: %', p_view_name, SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 1. zscore_view — CASE + SUM (Bug #3 fix)
-- ============================================================
SELECT alp.test_imv('zscore_view', 'alp.zscore_reflex', '
    SELECT product_id, location_id, dem_plan_id,
        CASE WHEN SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) = 0
                AND SUM(ABS(qty_sales)) = 0 THEN 0::numeric
            WHEN SUM(ABS(qty_sales)) = 0 THEN 2::numeric
            ELSE 0.5 * SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales))::numeric
        END AS zscore_num,
        CASE WHEN SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) = 0
                AND SUM(ABS(qty_sales)) = 0 THEN 1::bigint
            WHEN SUM(ABS(qty_sales)) = 0 THEN 1::bigint
            WHEN SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) < 0 THEN 1::bigint
            ELSE SUM(ABS(qty_sales))
        END AS zscore_den,
        CASE WHEN SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) = 0
                AND SUM(ABS(qty_sales)) = 0 THEN 0::numeric
            WHEN SUM(ABS(qty_sales)) = 0
                AND SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales)) <> 0 THEN 2::numeric
            ELSE 0.5 * SUM(ABS(qty_sales_ub - qty_sales) + ABS(qty_sales_lb - qty_sales))::numeric
                / SUM(ABS(qty_sales))::numeric
        END AS zscore
    FROM alp.sales_simulation
    GROUP BY product_id, location_id, dem_plan_id
');

-- ============================================================
-- 2. sop_forecast_view — passthrough JOIN (previously working)
-- ============================================================
SELECT alp.test_imv('sop_forecast_view', 'alp.sop_forecast_reflex', '
    SELECT
        sales_simulation.dem_plan_id, sales_simulation.week, sales_simulation.isoyear,
        sales_simulation.year, sales_simulation.month, sales_simulation.order_date,
        sales_simulation.product_id, sales_simulation.location_id, location.canal_id,
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
            AND pricing.canal_id = location.canal_id AND sales_simulation.product_id = pricing.product_id
        LEFT JOIN alp.current_assortment_activity_view caav
            ON caav.product_id = sales_simulation.product_id AND caav.location_id = sales_simulation.location_id
    WHERE demand_planning.is_sent_to_sop
        AND (demand_planning.status::text <> ALL (ARRAY[''archive'',''archiving'',''error'']::text[]))
', 'dem_plan_id, product_id, location_id, order_date');

-- ============================================================
-- 3. sop_forecast_history_view — passthrough JOIN
-- ============================================================
SELECT alp.test_imv('sop_forecast_history_view', 'alp.sop_forecast_history_reflex', '
    SELECT dem_plan_id, week, isoyear, year, month, order_date,
        sales_simulation.product_id, sales_simulation.location_id, location.canal_id,
        forecast_base, qty_sales AS quantity, qty_sales_ub AS quantity_ub, qty_sales_lb AS quantity_lb,
        qty_sales * COALESCE(pricing.base_price, 0) AS turnover,
        forecast_base * COALESCE(pricing.base_price, 0) AS forecast_base_turnover,
        qty_sales_ub * COALESCE(pricing.base_price, 0) AS qty_sales_ub_turnover,
        qty_sales_lb * COALESCE(pricing.base_price, 0) AS qty_sales_lb_turnover,
        (caav.product_id IS NOT NULL) AS in_current_assortment
    FROM alp.sales_simulation
        INNER JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
        INNER JOIN alp.location ON location.id = sales_simulation.location_id
        LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
            AND pricing.canal_id = location.canal_id AND sales_simulation.product_id = pricing.product_id
        LEFT JOIN alp.current_assortment_activity_view caav
            ON caav.product_id = sales_simulation.product_id AND caav.location_id = sales_simulation.location_id
    WHERE demand_planning.status = ''archive''
', 'dem_plan_id, product_id, location_id, order_date');

-- ============================================================
-- 4. sop_last_forecast_view — SUM + BOOL_OR
-- ============================================================
SELECT alp.test_imv('sop_last_forecast_view', 'alp.sop_last_forecast_reflex', '
    SELECT week, isoyear, year, month, order_date,
        last_sales_simulation.product_id, last_sales_simulation.location_id, location.canal_id,
        SUM(forecast_base)::BIGINT AS forecast_base,
        SUM(qty_sales)::BIGINT AS quantity,
        SUM(qty_sales_ub)::BIGINT AS quantity_ub,
        SUM(qty_sales_lb)::BIGINT AS quantity_lb,
        SUM(qty_sales * COALESCE(latest_price_view.base_price, 0)) AS turnover,
        SUM(forecast_base * COALESCE(latest_price_view.base_price, 0)) AS forecast_base_turnover,
        SUM(qty_sales_ub * COALESCE(latest_price_view.base_price, 0)) AS qty_sales_ub_turnover,
        SUM(qty_sales_lb * COALESCE(latest_price_view.base_price, 0)) AS qty_sales_lb_turnover,
        bool_or(caav.product_id IS NOT NULL) AS in_current_assortment
    FROM alp.last_sales_simulation
        INNER JOIN alp.location ON location.id = last_sales_simulation.location_id
        INNER JOIN alp.product ON product.id = last_sales_simulation.product_id
        LEFT JOIN alp.latest_price_view ON latest_price_view.canal_id = location.canal_id
            AND last_sales_simulation.product_id = latest_price_view.product_id
        LEFT JOIN alp.current_assortment_activity_view caav
            ON caav.product_id = last_sales_simulation.product_id AND caav.location_id = last_sales_simulation.location_id
    GROUP BY year, month, week, isoyear, order_date,
        last_sales_simulation.product_id, last_sales_simulation.location_id, location.canal_id
');

-- ============================================================
-- 5. unsent_sop_forecast_view — SUM + BOOL_OR (largest)
-- ============================================================
SELECT alp.test_imv('unsent_sop_forecast_view', 'alp.unsent_sop_forecast_reflex', '
    SELECT dem_plan_id, week, isoyear, year, month, order_date,
        sales_simulation.product_id, sales_simulation.location_id, location.canal_id,
        SUM(forecast_base)::BIGINT AS forecast_base,
        SUM(qty_sales)::BIGINT AS quantity,
        SUM(qty_sales_ub)::BIGINT AS quantity_ub,
        SUM(qty_sales_lb)::BIGINT AS quantity_lb,
        SUM(qty_sales * COALESCE(pricing.base_price, 0)) AS turnover,
        SUM(forecast_base * COALESCE(pricing.base_price, 0)) AS forecast_base_turnover,
        SUM(qty_sales_ub * COALESCE(pricing.base_price, 0)) AS qty_sales_ub_turnover,
        SUM(qty_sales_lb * COALESCE(pricing.base_price, 0)) AS qty_sales_lb_turnover,
        bool_or(caav.product_id IS NOT NULL) AS in_current_assortment
    FROM alp.sales_simulation
        INNER JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
        INNER JOIN alp.location ON location.id = sales_simulation.location_id
        LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
            AND pricing.canal_id = location.canal_id AND sales_simulation.product_id = pricing.product_id
        LEFT JOIN alp.current_assortment_activity_view caav
            ON caav.product_id = sales_simulation.product_id AND caav.location_id = sales_simulation.location_id
    WHERE NOT demand_planning.is_sent_to_sop
        AND NOT demand_planning.is_draft
        AND demand_planning.status NOT IN (''archive'', ''archiving'')
    GROUP BY dem_plan_id, year, month, week, isoyear, order_date,
        sales_simulation.product_id, sales_simulation.location_id, location.canal_id
');

-- ============================================================
-- 6. history_sales_view — CTE + "order" table (Bug #1 fix)
-- ============================================================
SELECT alp.test_imv('history_sales_view', 'alp.history_sales_reflex', '
    WITH historical_orders AS (
        SELECT
            DATE_TRUNC(''day'', order_line.order_date) AS order_date,
            o.location_id, order_line.product_id,
            sum(order_line.quantity) AS quantity,
            sum(order_line.sub_total) AS turnover,
            location.canal_id
        FROM alp.order_line
            JOIN alp."order" o ON order_line.order_id = o.id
            JOIN alp.location ON o.location_id = location.id
        GROUP BY o.location_id, order_line.product_id, location.canal_id,
            DATE_TRUNC(''day'', order_line.order_date)
    )
    SELECT product.sku, product.description, product.image_url,
        EXTRACT(ISOYEAR FROM historical_orders.order_date)::INTEGER AS isoyear,
        EXTRACT(YEAR FROM historical_orders.order_date)::INTEGER AS year,
        EXTRACT(MONTH FROM historical_orders.order_date)::INTEGER AS month,
        EXTRACT(WEEK FROM historical_orders.order_date)::INTEGER AS week,
        historical_orders.order_date, historical_orders.location_id,
        historical_orders.product_id, historical_orders.quantity,
        historical_orders.turnover, historical_orders.canal_id,
        (caav.product_id IS NOT NULL) AS in_current_assortment
    FROM historical_orders
        JOIN alp.product ON historical_orders.product_id = product.id
        LEFT JOIN alp.current_assortment_activity_view caav
            ON caav.product_id = historical_orders.product_id
            AND caav.location_id = historical_orders.location_id
');

-- ============================================================
-- 7. stock_transfer_baseline_view — EXTRACT not in GROUP BY (Bug #2 fix)
-- ============================================================
SELECT alp.test_imv('stock_transfer_baseline_view', 'alp.stock_transfer_baseline_reflex', '
    SELECT supply_plan_id,
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
    FROM alp.stock_transfer_baseline
        INNER JOIN alp.supply_plan ON supply_plan.id = stock_transfer_baseline.supply_plan_id
        LEFT OUTER JOIN alp.unit_pricing ON unit_pricing.product_id = stock_transfer_baseline.product_id
        LEFT JOIN alp.current_assortment_activity_view caav
            ON caav.product_id = stock_transfer_baseline.product_id
            AND caav.location_id = stock_transfer_baseline.to_location_id
    GROUP BY EXTRACT(MONTH FROM transfer_date), EXTRACT(YEAR FROM transfer_date),
        stock_transfer_baseline.product_id, to_location_id, transfer_date,
        stock_transfer_baseline.supply_plan_id, stock_transfer_baseline.from_location_id
');

-- ============================================================
-- 8. sop_purchase_baseline_view — EXISTS→LEFT JOIN, EXTRACT
-- ============================================================
SELECT alp.test_imv('sop_purchase_baseline_view', 'alp.sop_purchase_baseline_reflex', '
    WITH pb_base AS (
        SELECT supply_plan_id,
            DATE_TRUNC(''day'', order_date) AS purchase_date,
            EXTRACT(WEEK FROM DATE_TRUNC(''day'', order_date))::double precision AS week,
            EXTRACT(MONTH FROM DATE_TRUNC(''day'', order_date))::double precision AS month,
            EXTRACT(YEAR FROM DATE_TRUNC(''day'', order_date))::double precision AS year,
            EXTRACT(ISOYEAR FROM DATE_TRUNC(''day'', order_date))::double precision AS isoyear,
            purchase_baseline.product_id, purchase_baseline.location_id, location.canal_id,
            ordered_quantity_kept,
            ordered_quantity_kept * COALESCE(unit_pricing.unit_price, 0) AS turnover_val,
            caav.product_id IS NOT NULL AS in_assortment
        FROM alp.purchase_baseline
            INNER JOIN alp.supply_plan ON supply_plan.id = purchase_baseline.supply_plan_id
            INNER JOIN alp.location ON purchase_baseline.location_id = location.id
            LEFT OUTER JOIN alp.unit_pricing ON unit_pricing.product_id = purchase_baseline.product_id
            LEFT JOIN (SELECT DISTINCT product_id FROM alp.current_assortment_activity_view) caav
                ON caav.product_id = purchase_baseline.product_id
        WHERE purchase_baseline.state != ''rejected''
    )
    SELECT supply_plan_id, purchase_date, week, month, year, isoyear,
        product_id, location_id, canal_id,
        SUM(ordered_quantity_kept)::BIGINT AS quantity,
        SUM(turnover_val) AS turnover,
        bool_or(in_assortment) AS in_current_assortment
    FROM pb_base
    GROUP BY supply_plan_id, purchase_date, week, month, year, isoyear,
        product_id, location_id, canal_id
');

-- ============================================================
-- 9. sop_purchase_view — EXISTS→LEFT JOIN, EXTRACT
-- ============================================================
SELECT alp.test_imv('sop_purchase_view', 'alp.sop_purchase_reflex', '
    WITH po_base AS (
        SELECT
            EXTRACT(WEEK FROM DATE_TRUNC(''day'', po.order_date))::double precision AS week,
            EXTRACT(MONTH FROM DATE_TRUNC(''day'', po.order_date))::double precision AS month,
            EXTRACT(YEAR FROM DATE_TRUNC(''day'', po.order_date))::double precision AS year,
            EXTRACT(ISOYEAR FROM DATE_TRUNC(''day'', po.order_date))::double precision AS isoyear,
            pol.product_id, po.location_id, location.canal_id,
            DATE_TRUNC(''day'', po.order_date) AS purchase_date,
            ordered_quantity,
            ordered_quantity * COALESCE(unit_pricing.unit_price, 0) AS turnover_val,
            caav.product_id IS NOT NULL AS in_assortment
        FROM alp.purchase_order_line AS pol
            INNER JOIN alp.purchase_order AS po ON po.id = pol.po_id
            INNER JOIN alp.location ON po.location_id = location.id
            LEFT OUTER JOIN alp.unit_pricing ON unit_pricing.product_id = pol.product_id
            LEFT JOIN (SELECT DISTINCT product_id FROM alp.current_assortment_activity_view) caav
                ON caav.product_id = pol.product_id
    )
    SELECT week, month, year, isoyear, product_id, location_id, canal_id, purchase_date,
        SUM(ordered_quantity)::BIGINT AS quantity,
        SUM(turnover_val) AS turnover,
        bool_or(in_assortment) AS in_current_assortment
    FROM po_base
    GROUP BY week, month, year, isoyear, product_id, location_id, canal_id, purchase_date
');

-- ============================================================
-- 10. event_demand_planning_sales — SUM + CROSS JOIN + subquery WHERE
-- ============================================================
SELECT alp.test_imv('event_demand_planning_sales', 'alp.event_dp_sales_reflex', '
    SELECT event_demand_planning.event_id,
        sales_simulation.product_id, sales_simulation.location_id,
        sales_simulation.order_date AS sale_date,
        SUM(sales_simulation.qty_sales) AS planned_qty,
        COALESCE(SUM(history_sales_view.quantity), 0) AS sum_sales_qty
    FROM alp.sales_simulation
        CROSS JOIN alp.event_demand_planning
        JOIN alp.event ON event_demand_planning.event_id = event.id
        LEFT JOIN alp.history_sales_view ON
            history_sales_view.order_date BETWEEN event.start_date AND event.end_date
            AND history_sales_view.order_date = sales_simulation.order_date
            AND history_sales_view.product_id = sales_simulation.product_id
            AND history_sales_view.location_id = sales_simulation.location_id
    WHERE sales_simulation.order_date BETWEEN event.start_date AND event.end_date
        AND sales_simulation.dem_plan_id = (SELECT dem_plan_id FROM alp.sop_current_view)
        AND event_demand_planning.dem_plan_id = (SELECT dem_plan_id FROM alp.sop_current_view)
    GROUP BY event_demand_planning.event_id, sales_simulation.product_id,
        sales_simulation.location_id, sales_simulation.order_date
');

-- ============================================================
-- 11. stock_transfer_view — SUM + BOOL_OR (no WHERE subquery version)
-- ============================================================
SELECT alp.test_imv('stock_transfer_view', 'alp.stock_transfer_reflex', '
    SELECT EXTRACT(week FROM transfer_date) AS week,
        EXTRACT(MONTH FROM transfer_date) AS month,
        EXTRACT(YEAR FROM transfer_date) AS year,
        EXTRACT(ISOYEAR FROM transfer_date) AS isoyear,
        stock_transfer.transfer_date, stock_transfer.product_id,
        stock_transfer.to_location_id AS location_id,
        stock_transfer.from_location_id AS warehouse_id,
        SUM(quantity)::BIGINT AS quantity,
        SUM(quantity * COALESCE(unit_pricing.unit_price, 0)) AS turnover,
        bool_or(caav.product_id IS NOT NULL) AS in_current_assortment
    FROM alp.stock_transfer
        LEFT OUTER JOIN alp.unit_pricing ON unit_pricing.product_id = stock_transfer.product_id
        LEFT JOIN alp.current_assortment_activity_view caav
            ON caav.product_id = stock_transfer.product_id
            AND caav.location_id = stock_transfer.to_location_id
    GROUP BY EXTRACT(MONTH FROM transfer_date), EXTRACT(YEAR FROM transfer_date),
        stock_transfer.product_id, to_location_id, transfer_date, stock_transfer.from_location_id
');

-- ============================================================
-- 12. demand_planning_characteristics_view — ANY_VALUE + complex
-- ============================================================
SELECT alp.test_imv('demand_planning_characteristics_view', 'alp.demand_planning_chars_reflex', '
    WITH n_events AS (
        SELECT COUNT(event_id) AS n_events, dem_plan_id
        FROM alp.event_demand_planning GROUP BY dem_plan_id
    ),
    sales_stats AS (
        SELECT sum(qty_sales) AS qty_sales, dem_plan_id, product_id, location_id
        FROM alp.sales_simulation
            INNER JOIN alp.product ON sales_simulation.product_id = product.id
        WHERE product.is_active
        GROUP BY product_id, dem_plan_id, location_id
    )
    SELECT demand_planning.id AS dem_plan_id,
        SUM(sales_stats.qty_sales) AS quantity,
        SUM(sales_stats.qty_sales * pricing.base_price) AS turnover,
        SUM(sales_stats.qty_sales * GREATEST(pricing.base_price - unit_pricing.unit_price, 0)) AS net_margin,
        COALESCE(MAX(n_events.n_events), 0) AS n_events
    FROM alp.demand_planning
        LEFT JOIN n_events ON demand_planning.id = n_events.dem_plan_id
        JOIN sales_stats ON sales_stats.dem_plan_id = demand_planning.id
        JOIN alp.location ON sales_stats.location_id = location.id
        JOIN alp.pricing ON sales_stats.product_id = pricing.product_id
            AND demand_planning.assortment_id = pricing.assortment_id
            AND pricing.canal_id = location.canal_id
        JOIN alp.unit_pricing ON unit_pricing.product_id = sales_stats.product_id
    WHERE NOT (demand_planning.is_draft OR demand_planning.status = ''archive'')
    GROUP BY demand_planning.id
');

-- ============================================================
-- FINAL REPORT
-- ============================================================
\echo ''
\echo '============================================='
\echo '  IMV TEST RESULTS v2'
\echo '============================================='

SELECT view_name, status, matview_rows, imv_rows, mismatches,
       ROUND(duration_ms) || ' ms' AS duration,
       COALESCE(LEFT(error_message, 100), '') AS error
FROM alp._imv_test_results_v2
ORDER BY tested_at;

SELECT count(*) FILTER (WHERE status = 'PASS') AS pass,
       count(*) FILTER (WHERE status = 'FAIL') AS fail,
       count(*) FILTER (WHERE status = 'ERROR') AS error,
       count(*) FILTER (WHERE status = 'TYPE_MISMATCH') AS type_mismatch,
       count(*) AS total
FROM alp._imv_test_results_v2;
