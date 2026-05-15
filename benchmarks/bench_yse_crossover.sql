-- Crossover sweep on yse.bench_user_imv (small, 419 K rows, ~3 plans).
-- Drives the open question: where does REFRESH MV become faster than
-- 1.4.6 incremental as the affected fraction grows?

\timing on
\pset border 2
SET search_path TO yse, public;
SET work_mem = '256MB';
SET maintenance_work_mem = '2GB';
SET reflex.wipe_threshold = 1.0;

-- yse has plan 172 ('current', 421K rows) and plan 176 ('ready_for_sop', 148K rows).
-- IMV currently contains plan 172 only (whitelist match).

\echo === BASELINE: REFRESH MATERIALIZED VIEW (warm) ===
REFRESH MATERIALIZED VIEW yse.bench_user_mv;
REFRESH MATERIALIZED VIEW yse.bench_user_mv;   -- warm

\echo
\echo === Small data UPDATE: 100 rows on plan 172 (in-filter) ===
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 100)
UPDATE yse.sales_simulation SET qty_sales = qty_sales + 1 WHERE id IN (SELECT id FROM ids);
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 100)
UPDATE yse.sales_simulation SET qty_sales = qty_sales - 1 WHERE id IN (SELECT id FROM ids);

\echo
\echo === Data UPDATE: 1K rows ===
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 1000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales + 1 WHERE id IN (SELECT id FROM ids);
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 1000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales - 1 WHERE id IN (SELECT id FROM ids);

\echo
\echo === Data UPDATE: 10K rows ===
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 10000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales + 1 WHERE id IN (SELECT id FROM ids);
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 10000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales - 1 WHERE id IN (SELECT id FROM ids);

\echo
\echo === Data UPDATE: 50K rows ===
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 50000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales + 1 WHERE id IN (SELECT id FROM ids);
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 50000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales - 1 WHERE id IN (SELECT id FROM ids);

\echo
\echo === Data UPDATE: 100K rows ===
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 100000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales + 1 WHERE id IN (SELECT id FROM ids);
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id=172 LIMIT 100000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales - 1 WHERE id IN (SELECT id FROM ids);

\echo
\echo === OUT→IN single plan (176, 148K rows = 35% of IMV) ===
UPDATE yse.demand_planning SET status='current' WHERE id=176;

\echo === reflex_reconcile() right after the flip ===
SELECT public.reflex_reconcile('yse.bench_user_imv');

\echo === REFRESH MV right after the flip (for comparison) ===
REFRESH MATERIALIZED VIEW yse.bench_user_mv;

\echo === Correctness verify (post-flip state) ===
SELECT (SELECT count(*) FROM (SELECT * FROM yse.bench_user_imv EXCEPT ALL SELECT * FROM yse.bench_user_mv) x) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM yse.bench_user_mv EXCEPT ALL SELECT * FROM yse.bench_user_imv) x) AS mv_extra;

\echo === Flip back ===
UPDATE yse.demand_planning SET status='ready_for_sop' WHERE id=176;
