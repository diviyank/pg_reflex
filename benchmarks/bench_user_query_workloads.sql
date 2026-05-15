-- Workload bench for 1.4.6 vs REFRESH MV using the user's exact query.
-- Pre-req: bench_user_query_1_4_6.sql already ran (IMVs + MVs exist, MV refreshed).

\timing on
\pset border 2
SET work_mem = '256MB';
SET maintenance_work_mem = '2GB';
SET reflex.wipe_threshold = 1.0;
SET reflex.observability = 'OFF';

\echo
\echo ============================================================
\echo  ALP workloads (76M source × 28 dem_plans; IMV starts 7.7M rows)
\echo ============================================================
\echo

\echo --- warmup: read both relations to warm cache ---
SELECT count(*) FROM alp.sales_simulation WHERE dem_plan_id IN (605, 622, 661, 663, 664, 665);
SELECT count(*) FROM alp.bench_user_imv;

\echo
\echo === A0: no-op pivot (UPDATE that keeps both sides outside whitelist) ===
\echo Plan 668 (archive, 0 rows) → flip to archive (same), trigger fires but nothing visible
UPDATE alp.demand_planning SET modified_date = now() WHERE id = 668;

\echo
\echo === A1: pure data UPDATE 1K source rows on the in-filter plan 605 ===
WITH ids AS (
  SELECT id FROM alp.sales_simulation WHERE dem_plan_id = 605 LIMIT 1000
)
UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1
WHERE id IN (SELECT id FROM ids);

\echo
\echo === A2: pure data UPDATE 10K rows on the in-filter plan 605 ===
WITH ids AS (
  SELECT id FROM alp.sales_simulation WHERE dem_plan_id = 605 OFFSET 1000 LIMIT 10000
)
UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1
WHERE id IN (SELECT id FROM ids);

\echo
\echo === A3: OUT→IN single small plan (plan 622, 2.5M rows) ===
UPDATE alp.demand_planning SET status = 'current' WHERE id = 622;

\echo
\echo === A3b: IN→OUT same plan back (plan 622) ===
UPDATE alp.demand_planning SET status = 'ready_for_sop' WHERE id = 622;

\echo
\echo === A4: OUT→IN single large plan (plan 661, 8.9M rows) ===
UPDATE alp.demand_planning SET status = 'current' WHERE id = 661;

\echo
\echo === A4b: IN→OUT same large plan back (plan 661) ===
UPDATE alp.demand_planning SET status = 'custom' WHERE id = 661;

\echo
\echo === A5: OUT→IN 5 plans bulk (623, 645, 622, 644, 643 — ~16M source rows) ===
UPDATE alp.demand_planning SET status = 'current' WHERE id IN (623, 645, 622, 644, 643);

\echo
\echo === A5b: IN→OUT 5 plans bulk back ===
UPDATE alp.demand_planning SET status = 'ready_for_sop' WHERE id IN (623, 645, 622, 644, 643);

\echo
\echo === A6: mixed UPDATE (some IN, some OUT in single UPDATE) ===
-- Flip 660 'custom' → 'current' (IN) and 605 'current' → 'ready_for_sop' (OUT)
-- in a single UPDATE statement.
UPDATE alp.demand_planning
SET status = CASE id WHEN 660 THEN 'current' WHEN 605 THEN 'ready_for_sop' END
WHERE id IN (660, 605);

\echo === A6b: restore (660 back to custom, 605 back to current) ===
UPDATE alp.demand_planning
SET status = CASE id WHEN 660 THEN 'custom' WHEN 605 THEN 'current' END
WHERE id IN (660, 605);

\echo
\echo === A7: admin reflex_reconcile() ===
SELECT public.reflex_reconcile('alp.bench_user_imv');

\echo
\echo === A8: REFRESH MATERIALIZED VIEW for reference (warm cache) ===
REFRESH MATERIALIZED VIEW alp.bench_user_mv;

\echo
\echo === Correctness check after the sequence ===
SELECT 'alp imv vs mv' AS lbl, count(*) AS imv_minus_mv FROM (SELECT * FROM alp.bench_user_imv EXCEPT ALL SELECT * FROM alp.bench_user_mv) x;
SELECT 'alp mv vs imv' AS lbl, count(*) AS mv_minus_imv FROM (SELECT * FROM alp.bench_user_mv EXCEPT ALL SELECT * FROM alp.bench_user_imv) x;

\echo
\echo ============================================================
\echo  YSE workloads (569K source × 3 dem_plans; IMV starts 419K rows)
\echo ============================================================
\echo

SELECT count(*) FROM yse.sales_simulation WHERE dem_plan_id IN (172, 176);
SELECT count(*) FROM yse.bench_user_imv;

\echo
\echo === Y1: pure data UPDATE 1K rows on in-filter plan 172 ===
WITH ids AS (SELECT id FROM yse.sales_simulation WHERE dem_plan_id = 172 LIMIT 1000)
UPDATE yse.sales_simulation SET qty_sales = qty_sales + 1 WHERE id IN (SELECT id FROM ids);

\echo
\echo === Y2: OUT→IN single plan (176, 148K rows) ===
UPDATE yse.demand_planning SET status = 'current' WHERE id = 176;

\echo
\echo === Y2b: IN→OUT same plan back ===
UPDATE yse.demand_planning SET status = 'ready_for_sop' WHERE id = 176;

\echo
\echo === Y3: REFRESH MV (yse) ===
REFRESH MATERIALIZED VIEW yse.bench_user_mv;

\echo
\echo === Correctness check yse ===
SELECT 'yse imv vs mv' AS lbl, count(*) AS imv_minus_mv FROM (SELECT * FROM yse.bench_user_imv EXCEPT ALL SELECT * FROM yse.bench_user_mv) x;
SELECT 'yse mv vs imv' AS lbl, count(*) AS mv_minus_imv FROM (SELECT * FROM yse.bench_user_mv EXCEPT ALL SELECT * FROM yse.bench_user_imv) x;
