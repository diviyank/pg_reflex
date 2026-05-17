\timing on
\pset border 2
SET search_path TO alp, public;
SET work_mem='256MB';
SET maintenance_work_mem='2GB';

\echo === Disable autovacuum on relevant tables ===
ALTER TABLE alp.bench_user_imv SET (autovacuum_enabled = false);
ALTER TABLE alp.bench_user_mv SET (autovacuum_enabled = false);
ALTER TABLE alp.__reflex_intermediate_bench_user_imv SET (autovacuum_enabled = false);
ALTER TABLE alp.sales_simulation SET (autovacuum_enabled = false);
ALTER TABLE alp.demand_planning SET (autovacuum_enabled = false);

\echo === Reset to clean state ===
UPDATE alp.demand_planning SET status='custom' WHERE id=661;
UPDATE alp.demand_planning SET status='ready_for_sop' WHERE id=622;
SELECT public.reflex_reconcile('alp.bench_user_imv');
REFRESH MATERIALIZED VIEW alp.bench_user_mv;
VACUUM alp.bench_user_imv;
VACUUM alp.bench_user_mv;
VACUUM alp.__reflex_intermediate_bench_user_imv;

\echo === Initial state ===
SELECT id, status FROM alp.demand_planning WHERE id IN (605, 622, 661) ORDER BY id;
SELECT (SELECT count(*) FROM alp.bench_user_imv) AS imv_rows,
       (SELECT count(*) FROM alp.bench_user_mv) AS mv_rows;

\echo
\echo === A1: pure-data UPDATE 1K rows in-filter plan 605 ===
WITH ids AS (SELECT id FROM alp.sales_simulation WHERE dem_plan_id = 605 LIMIT 1000)
UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1
WHERE id IN (SELECT id FROM ids);
VACUUM alp.__reflex_intermediate_bench_user_imv;
VACUUM alp.bench_user_imv;

\echo --- A1-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;
VACUUM alp.bench_user_mv;

\echo === A2: pure-data UPDATE 10K rows plan 605 ===
WITH ids AS (SELECT id FROM alp.sales_simulation WHERE dem_plan_id = 605 OFFSET 1000 LIMIT 10000)
UPDATE alp.sales_simulation SET qty_sales = qty_sales + 1
WHERE id IN (SELECT id FROM ids);
VACUUM alp.__reflex_intermediate_bench_user_imv;
VACUUM alp.bench_user_imv;

\echo --- A2-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;
VACUUM alp.bench_user_mv;

\echo === A3: OUT→IN single small plan (622, 2.5M rows) ===
UPDATE alp.demand_planning SET status = 'current' WHERE id = 622;
VACUUM alp.__reflex_intermediate_bench_user_imv;
VACUUM alp.bench_user_imv;

\echo --- A3-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;
VACUUM alp.bench_user_mv;

\echo === A3b: IN→OUT plan back (622) ===
UPDATE alp.demand_planning SET status = 'ready_for_sop' WHERE id = 622;
VACUUM alp.__reflex_intermediate_bench_user_imv;
VACUUM alp.bench_user_imv;

\echo --- A3b-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;
VACUUM alp.bench_user_mv;

\echo === A4: OUT→IN single large plan (661, 8.9M rows) ===
UPDATE alp.demand_planning SET status = 'current' WHERE id = 661;
VACUUM alp.__reflex_intermediate_bench_user_imv;
VACUUM alp.bench_user_imv;

\echo --- A4-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;
VACUUM alp.bench_user_mv;

\echo === A4b: IN→OUT large plan back (661) ===
UPDATE alp.demand_planning SET status = 'custom' WHERE id = 661;
VACUUM alp.__reflex_intermediate_bench_user_imv;
VACUUM alp.bench_user_imv;

\echo --- A4b-MV ---
REFRESH MATERIALIZED VIEW alp.bench_user_mv;
VACUUM alp.bench_user_mv;

\echo === Final correctness ===
SELECT (SELECT count(*) FROM (SELECT * FROM alp.bench_user_imv EXCEPT SELECT * FROM alp.bench_user_mv) e) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM alp.bench_user_mv EXCEPT SELECT * FROM alp.bench_user_imv) e) AS mv_extra,
       (SELECT count(*) FROM alp.bench_user_imv) AS imv_rows;

\echo === Re-enable autovacuum ===
ALTER TABLE alp.bench_user_imv RESET (autovacuum_enabled);
ALTER TABLE alp.bench_user_mv RESET (autovacuum_enabled);
ALTER TABLE alp.__reflex_intermediate_bench_user_imv RESET (autovacuum_enabled);
ALTER TABLE alp.sales_simulation RESET (autovacuum_enabled);
ALTER TABLE alp.demand_planning RESET (autovacuum_enabled);
