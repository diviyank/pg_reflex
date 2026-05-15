-- Benchmark: validate 1.4.5/1.4.6 perf bumps on db_clone using the user's
-- exact aggregating SOP-forecast query, on both alp and yse schemas.
--
-- Workloads (chosen because the data shapes in db_clone are real-prod, not synthetic):
--   * Pure data UPDATE: bump qty_sales on N source rows
--   * OUT→IN bulk: flip status of K plans from non-whitelist to 'current'
--   * IN→OUT bulk: flip status of K plans from 'current' back to 'ready_for_sop'
--   * Mixed UPDATE: flip 2 plans IN, 2 plans OUT in one UPDATE
--
-- Comparison: full REFRESH MATERIALIZED VIEW on an equivalent regular MV.
--
-- Run with:  psql -h localhost -U postgres -d db_clone -f bench_user_query_1_4_6.sql

\timing on
\pset border 2

SET work_mem = '256MB';
SET maintenance_work_mem = '2GB';
SET reflex.wipe_threshold = 1.0;  -- 1.4.6 default; explicit for safety

\echo
\echo === Step 1: bootstrap IMVs and MVs (one-time setup) ===
\echo

-- Drop prior bench artifacts
DROP TABLE IF EXISTS alp.bench_user_imv CASCADE;
DROP TABLE IF EXISTS yse.bench_user_imv CASCADE;
DROP MATERIALIZED VIEW IF EXISTS alp.bench_user_mv;
DROP MATERIALIZED VIEW IF EXISTS yse.bench_user_mv;
SELECT public.drop_reflex_ivm('alp.bench_user_imv') FROM (VALUES (1)) v WHERE EXISTS (SELECT 1 FROM public.__reflex_ivm_reference WHERE name='alp.bench_user_imv');
SELECT public.drop_reflex_ivm('yse.bench_user_imv') FROM (VALUES (1)) v WHERE EXISTS (SELECT 1 FROM public.__reflex_ivm_reference WHERE name='yse.bench_user_imv');

\echo --- creating alp.bench_user_imv (pg_reflex 1.4.6) ---
SELECT public.create_reflex_ivm(
  'alp.bench_user_imv',
  $$
    SELECT
      dem_plan_id,
      week,
      isoyear,
      year,
      month,
      order_date,
      sales_simulation.product_id,
      sales_simulation.location_id,
      SUM(forecast_base)::BIGINT AS forecast_base,
      SUM(qty_sales)::BIGINT AS quantity,
      SUM(qty_sales_ub)::BIGINT AS quantity_ub,
      SUM(qty_sales_lb)::BIGINT AS quantity_lb,
      SUM(qty_sales * COALESCE(pricing.base_price, 0)) AS turnover,
      SUM(forecast_base * COALESCE(pricing.base_price, 0)) AS forecast_base_turnover,
      SUM(qty_sales_ub * COALESCE(pricing.base_price, 0)) AS qty_sales_ub_turnover,
      SUM(qty_sales_lb * COALESCE(pricing.base_price, 0)) AS qty_sales_lb_turnover,
      BOOL_OR(caav.product_id IS NOT NULL) AS in_current_assortment
    FROM alp.sales_simulation
    INNER JOIN alp.demand_planning
      ON demand_planning.id = sales_simulation.dem_plan_id
    LEFT JOIN alp.pricing
      ON demand_planning.assortment_id = pricing.assortment_id
      AND sales_simulation.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_reflex caav
      ON caav.product_id = sales_simulation.product_id
      AND caav.location_id = sales_simulation.location_id
    WHERE demand_planning.status IN (
      'creating_supply_plan','running_optimizer','refreshing_views_sp',
      'sent_to_sop','validated','current'
    )
    GROUP BY
      dem_plan_id, week, isoyear, year, month, order_date,
      sales_simulation.product_id, sales_simulation.location_id
  $$
);

\echo --- creating alp.bench_user_mv (regular MV, REFRESH baseline) ---
CREATE MATERIALIZED VIEW alp.bench_user_mv AS
  SELECT
    dem_plan_id, week, isoyear, year, month, order_date,
    sales_simulation.product_id, sales_simulation.location_id,
    SUM(forecast_base)::BIGINT AS forecast_base,
    SUM(qty_sales)::BIGINT AS quantity,
    SUM(qty_sales_ub)::BIGINT AS quantity_ub,
    SUM(qty_sales_lb)::BIGINT AS quantity_lb,
    SUM(qty_sales * COALESCE(pricing.base_price, 0)) AS turnover,
    SUM(forecast_base * COALESCE(pricing.base_price, 0)) AS forecast_base_turnover,
    SUM(qty_sales_ub * COALESCE(pricing.base_price, 0)) AS qty_sales_ub_turnover,
    SUM(qty_sales_lb * COALESCE(pricing.base_price, 0)) AS qty_sales_lb_turnover,
    BOOL_OR(caav.product_id IS NOT NULL) AS in_current_assortment
  FROM alp.sales_simulation
  INNER JOIN alp.demand_planning
    ON demand_planning.id = sales_simulation.dem_plan_id
  LEFT JOIN alp.pricing
    ON demand_planning.assortment_id = pricing.assortment_id
    AND sales_simulation.product_id = pricing.product_id
  LEFT JOIN alp.current_assortment_activity_reflex caav
    ON caav.product_id = sales_simulation.product_id
    AND caav.location_id = sales_simulation.location_id
  WHERE demand_planning.status IN (
    'creating_supply_plan','running_optimizer','refreshing_views_sp',
    'sent_to_sop','validated','current'
  )
  GROUP BY
    dem_plan_id, week, isoyear, year, month, order_date,
    sales_simulation.product_id, sales_simulation.location_id
WITH NO DATA;

\echo --- creating yse.bench_user_imv (pg_reflex 1.4.6) ---
SELECT public.create_reflex_ivm(
  'yse.bench_user_imv',
  $$
    SELECT
      dem_plan_id, week, isoyear, year, month, order_date,
      sales_simulation.product_id, sales_simulation.location_id,
      SUM(forecast_base)::BIGINT AS forecast_base,
      SUM(qty_sales)::BIGINT AS quantity,
      SUM(qty_sales_ub)::BIGINT AS quantity_ub,
      SUM(qty_sales_lb)::BIGINT AS quantity_lb,
      SUM(qty_sales * COALESCE(pricing.base_price, 0)) AS turnover,
      SUM(forecast_base * COALESCE(pricing.base_price, 0)) AS forecast_base_turnover,
      SUM(qty_sales_ub * COALESCE(pricing.base_price, 0)) AS qty_sales_ub_turnover,
      SUM(qty_sales_lb * COALESCE(pricing.base_price, 0)) AS qty_sales_lb_turnover,
      BOOL_OR(caav.product_id IS NOT NULL) AS in_current_assortment
    FROM yse.sales_simulation
    INNER JOIN yse.demand_planning
      ON demand_planning.id = sales_simulation.dem_plan_id
    LEFT JOIN yse.pricing
      ON demand_planning.assortment_id = pricing.assortment_id
      AND sales_simulation.product_id = pricing.product_id
    LEFT JOIN yse.current_assortment_activity_view caav
      ON caav.product_id = sales_simulation.product_id
      AND caav.location_id = sales_simulation.location_id
    WHERE demand_planning.status IN (
      'creating_supply_plan','running_optimizer','refreshing_views_sp',
      'sent_to_sop','validated','current'
    )
    GROUP BY
      dem_plan_id, week, isoyear, year, month, order_date,
      sales_simulation.product_id, sales_simulation.location_id
  $$
);

\echo --- creating yse.bench_user_mv (regular MV) ---
CREATE MATERIALIZED VIEW yse.bench_user_mv AS
  SELECT
    dem_plan_id, week, isoyear, year, month, order_date,
    sales_simulation.product_id, sales_simulation.location_id,
    SUM(forecast_base)::BIGINT AS forecast_base,
    SUM(qty_sales)::BIGINT AS quantity,
    SUM(qty_sales_ub)::BIGINT AS quantity_ub,
    SUM(qty_sales_lb)::BIGINT AS quantity_lb,
    SUM(qty_sales * COALESCE(pricing.base_price, 0)) AS turnover,
    SUM(forecast_base * COALESCE(pricing.base_price, 0)) AS forecast_base_turnover,
    SUM(qty_sales_ub * COALESCE(pricing.base_price, 0)) AS qty_sales_ub_turnover,
    SUM(qty_sales_lb * COALESCE(pricing.base_price, 0)) AS qty_sales_lb_turnover,
    BOOL_OR(caav.product_id IS NOT NULL) AS in_current_assortment
  FROM yse.sales_simulation
  INNER JOIN yse.demand_planning
    ON demand_planning.id = sales_simulation.dem_plan_id
  LEFT JOIN yse.pricing
    ON demand_planning.assortment_id = pricing.assortment_id
    AND sales_simulation.product_id = pricing.product_id
  LEFT JOIN yse.current_assortment_activity_view caav
    ON caav.product_id = sales_simulation.product_id
    AND caav.location_id = sales_simulation.location_id
  WHERE demand_planning.status IN (
    'creating_supply_plan','running_optimizer','refreshing_views_sp',
    'sent_to_sop','validated','current'
  )
  GROUP BY
    dem_plan_id, week, isoyear, year, month, order_date,
    sales_simulation.product_id, sales_simulation.location_id
WITH NO DATA;

\echo --- sizes after creation ---
SELECT 'alp.bench_user_imv' AS rel, count(*) AS rows FROM alp.bench_user_imv
UNION ALL SELECT 'yse.bench_user_imv', count(*) FROM yse.bench_user_imv;
