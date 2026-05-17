-- Aggregated bench_user_imv vs REFRESH MV — apples-to-apples per operation.
-- For each operation, time the IMV trigger (transparent), then time
-- the equivalent REFRESH MV on a matching post-state.
--
-- Initial state assumption:
--   605 IN-filter ('current'), 622 OUT-filter ('ready_for_sop'),
--   661 OUT-filter ('custom').
--
\timing on
\pset border 2
SET search_path TO alp, public;
SET work_mem='256MB';
SET maintenance_work_mem='2GB';

\echo === Initial state ===
SELECT id, status FROM alp.demand_planning WHERE id IN (605, 622, 661) ORDER BY id;
SELECT (SELECT count(*) FROM alp.bench_user_imv) AS imv_rows,
       (SELECT count(*) FROM alp.bench_user_mv) AS mv_rows;

\echo
\echo === A1: pure-data UPDATE 1K rows in-filter plan 605 ===
WITH ids AS (SELECT id FROM alp.sales_simulation WHERE dem_plan_id = 605 LIMIT 1000)
UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1
WHERE id IN (SELECT id FROM ids);

\echo --- A1-MV: REFRESH MV after equivalent state change ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;
\echo --- A1: correctness ---
SELECT (SELECT count(*) FROM (SELECT * FROM alp.bench_user_imv EXCEPT SELECT * FROM alp.bench_user_mv) e) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM alp.bench_user_mv EXCEPT SELECT * FROM alp.bench_user_imv) e) AS mv_extra;

\echo
\echo === A2: pure-data UPDATE 10K rows plan 605 ===
WITH ids AS (SELECT id FROM alp.sales_simulation WHERE dem_plan_id = 605 OFFSET 1000 LIMIT 10000)
UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1
WHERE id IN (SELECT id FROM ids);

\echo --- A2-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;

\echo
\echo === A3: OUT→IN single small plan (622, 2.5M rows) ===
UPDATE alp.demand_planning SET status = 'current' WHERE id = 622;

\echo --- A3-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;

\echo --- A3: correctness ---
SELECT (SELECT count(*) FROM (SELECT * FROM alp.bench_user_imv EXCEPT SELECT * FROM alp.bench_user_mv) e) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM alp.bench_user_mv EXCEPT SELECT * FROM alp.bench_user_imv) e) AS mv_extra,
       (SELECT count(*) FROM alp.bench_user_imv) AS imv_rows;

\echo
\echo === A3b: IN→OUT plan back (622) ===
UPDATE alp.demand_planning SET status = 'ready_for_sop' WHERE id = 622;

\echo --- A3b-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;

\echo
\echo === A4: OUT→IN single large plan (661, 8.9M rows) ===
UPDATE alp.demand_planning SET status = 'current' WHERE id = 661;

\echo --- A4-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;

\echo --- A4: correctness ---
SELECT (SELECT count(*) FROM (SELECT * FROM alp.bench_user_imv EXCEPT SELECT * FROM alp.bench_user_mv) e) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM alp.bench_user_mv EXCEPT SELECT * FROM alp.bench_user_imv) e) AS mv_extra,
       (SELECT count(*) FROM alp.bench_user_imv) AS imv_rows;

\echo
\echo === A4b: IN→OUT large plan back (661) ===
UPDATE alp.demand_planning SET status = 'custom' WHERE id = 661;

\echo --- A4b-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;

\echo --- Final correctness ---
SELECT (SELECT count(*) FROM (SELECT * FROM alp.bench_user_imv EXCEPT SELECT * FROM alp.bench_user_mv) e) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM alp.bench_user_mv EXCEPT SELECT * FROM alp.bench_user_imv) e) AS mv_extra,
       (SELECT count(*) FROM alp.bench_user_imv) AS imv_rows;
