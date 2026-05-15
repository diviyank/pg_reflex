-- v2: SET search_path so the 1.4.4 unqualified delta-INSERT in legacy
-- triggers resolves to alp.__reflex_delta_*. Otherwise every demand_planning
-- UPDATE rolls back via "relation __reflex_delta_demand_planning does not
-- exist" — which is what poisoned v1.

\timing on
\pset border 2
SET search_path TO alp, public;
SET work_mem = '256MB';
SET maintenance_work_mem = '2GB';
SET reflex.wipe_threshold = 1.0;

\echo
\echo === Capture initial IMV / MV state ===
SELECT (SELECT count(*) FROM alp.bench_user_imv) AS imv_alp,
       (SELECT count(*) FROM alp.bench_user_mv)  AS mv_alp,
       (SELECT count(*) FROM yse.bench_user_imv) AS imv_yse,
       (SELECT count(*) FROM yse.bench_user_mv)  AS mv_yse;

\echo
\echo === Warmup: touch tables ===
SELECT count(*) FROM alp.sales_simulation WHERE dem_plan_id IN (605, 622, 661, 644, 645, 623, 660);
SELECT count(*) FROM alp.bench_user_imv;
SELECT count(*) FROM alp.__reflex_intermediate_bench_user_imv;

\echo
\echo === A1: pure data UPDATE 1K rows on the in-filter plan 605 ===
WITH ids AS (SELECT id FROM alp.sales_simulation WHERE dem_plan_id = 605 LIMIT 1000)
UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1
WHERE id IN (SELECT id FROM ids);

\echo
\echo === A2: pure data UPDATE 10K rows on plan 605 ===
WITH ids AS (SELECT id FROM alp.sales_simulation WHERE dem_plan_id = 605 OFFSET 1000 LIMIT 10000)
UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1
WHERE id IN (SELECT id FROM ids);

\echo
\echo === A3: OUT→IN single small plan (622, 2.5M rows) ===
UPDATE alp.demand_planning SET status = 'current' WHERE id = 622;

\echo === A3b: IN→OUT same plan back (622) ===
UPDATE alp.demand_planning SET status = 'ready_for_sop' WHERE id = 622;

\echo
\echo === A4: OUT→IN single large plan (661, 8.9M rows) ===
UPDATE alp.demand_planning SET status = 'current' WHERE id = 661;

\echo === A4b: IN→OUT large plan back (661) ===
UPDATE alp.demand_planning SET status = 'custom' WHERE id = 661;

\echo
\echo === A5: REFRESH MV (warm baseline after work) ===
REFRESH MATERIALIZED VIEW alp.bench_user_mv;

\echo
\echo === A6: admin reflex_reconcile() ===
SELECT public.reflex_reconcile('alp.bench_user_imv');

\echo === Correctness verify (must be 0 each side) ===
SELECT (SELECT count(*) FROM (SELECT * FROM alp.bench_user_imv EXCEPT ALL SELECT * FROM alp.bench_user_mv) x) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM alp.bench_user_mv EXCEPT ALL SELECT * FROM alp.bench_user_imv) x) AS mv_extra;

\echo
\echo ============================================================
\echo  YSE workloads
\echo ============================================================
SET search_path TO yse, public;
SELECT (SELECT count(*) FROM yse.sales_simulation) AS rows_yse,
       (SELECT count(*) FROM yse.bench_user_imv)   AS imv_yse;

\echo
\echo === Y1: pure data UPDATE 1K rows on plan 172 ===
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id = 172 LIMIT 1000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales + 1
WHERE id IN (SELECT id FROM ids);

\echo === Y1b: revert same rows ===
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id = 172 LIMIT 1000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales - 1
WHERE id IN (SELECT id FROM ids);

\echo
\echo === Y2: OUT→IN single plan (176, 148K rows) ===
UPDATE yse.demand_planning SET status = 'current' WHERE id = 176;
\echo === Y2b: IN→OUT plan back ===
UPDATE yse.demand_planning SET status = 'ready_for_sop' WHERE id = 176;

\echo
\echo === Y3: REFRESH MV (yse) ===
REFRESH MATERIALIZED VIEW yse.bench_user_mv;

\echo === Y4: admin reflex_reconcile (yse) ===
SELECT public.reflex_reconcile('yse.bench_user_imv');

\echo === Correctness verify yse ===
SELECT (SELECT count(*) FROM (SELECT * FROM yse.bench_user_imv EXCEPT ALL SELECT * FROM yse.bench_user_mv) x) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM yse.bench_user_mv EXCEPT ALL SELECT * FROM yse.bench_user_imv) x) AS mv_extra;
