-- Focused bench: reconcile alone on alp + yse IMVs.
-- Compares against HEAD (TRUNCATE+INSERT both intermediate and target)
-- the LIKE+rename swap of the intermediate (target stays TRUNCATE+INSERT).

\timing on
\pset border 2
SET work_mem = '256MB';
SET maintenance_work_mem = '2GB';

\echo
\echo === Initial state ===
SELECT (SELECT count(*) FROM alp.bench_user_imv) AS imv_alp,
       (SELECT count(*) FROM yse.bench_user_imv) AS imv_yse;

\echo
\echo === A6a — reflex_reconcile alp (cold cache) ===
SELECT public.reflex_reconcile('alp.bench_user_imv');

\echo === A6b — reflex_reconcile alp (warm cache, 2nd run) ===
SELECT public.reflex_reconcile('alp.bench_user_imv');

\echo === A6c — reflex_reconcile alp (warm cache, 3rd run) ===
SELECT public.reflex_reconcile('alp.bench_user_imv');

\echo === Correctness verify (both columns must be 0) ===
SELECT (SELECT count(*) FROM (SELECT * FROM alp.bench_user_imv EXCEPT ALL SELECT * FROM alp.bench_user_mv) x) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM alp.bench_user_mv EXCEPT ALL SELECT * FROM alp.bench_user_imv) x) AS mv_extra;

\echo
\echo === Y4a — reflex_reconcile yse (cold cache) ===
SELECT public.reflex_reconcile('yse.bench_user_imv');

\echo === Y4b — reflex_reconcile yse (warm cache, 2nd run) ===
SELECT public.reflex_reconcile('yse.bench_user_imv');

\echo === Y4c — reflex_reconcile yse (warm cache, 3rd run) ===
SELECT public.reflex_reconcile('yse.bench_user_imv');

\echo === Correctness verify yse ===
SELECT (SELECT count(*) FROM (SELECT * FROM yse.bench_user_imv EXCEPT ALL SELECT * FROM yse.bench_user_mv) x) AS imv_extra,
       (SELECT count(*) FROM (SELECT * FROM yse.bench_user_mv EXCEPT ALL SELECT * FROM yse.bench_user_imv) x) AS mv_extra;
