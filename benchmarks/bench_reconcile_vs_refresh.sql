\timing on
\pset border 2
SET work_mem='256MB';
SET maintenance_work_mem='2GB';

\echo === Pre-state ===
SELECT (SELECT COUNT(*) FROM alp.bench_user_imv) AS imv_rows,
       (SELECT COUNT(*) FROM alp.bench_user_mv) AS mv_rows,
       (SELECT COUNT(*) FROM alp.__reflex_intermediate_bench_user_imv) AS int_rows;

\echo
\echo === Reconcile (full rebuild of intermediate + target) ===
SELECT public.reflex_reconcile('alp.bench_user_imv');

\echo
\echo === REFRESH MV ===
REFRESH MATERIALIZED VIEW alp.bench_user_mv;

\echo
\echo === Reconcile again (to see warm cache) ===
SELECT public.reflex_reconcile('alp.bench_user_imv');

\echo
\echo === REFRESH MV again (warm) ===
REFRESH MATERIALIZED VIEW alp.bench_user_mv;

\echo === Correctness ===
SELECT (SELECT count(*) FROM (SELECT * FROM alp.bench_user_imv EXCEPT SELECT * FROM alp.bench_user_mv) e) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM alp.bench_user_mv EXCEPT SELECT * FROM alp.bench_user_imv) e) AS mv_extra;
