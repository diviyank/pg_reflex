-- ============================================================================
-- Plan #10 (1.4.6) — bigger-scale comparison benchmark
--
-- Two shapes:
--   A. Large target (1M groups, 20M source rows, 20 rows/group, clustered)
--   B. Customer-scale-ish target (500K groups, 10M source rows)
--
-- For each, measure trigger fire wall-clock at multiple low selectivities
-- (the band where #10's MERGE-on-target replaces the legacy DELETE+INSERT).
-- Above 30% the high-selectivity dispatch fires and #10 doesn't apply.
--
-- Compares: pg_reflex IMV update (triggered) vs UPDATE-only + REFRESH MAT VIEW.
-- ============================================================================

\timing on
SELECT setseed(0.42);

DROP TABLE IF EXISTS bm CASCADE;
CREATE TABLE bm (
    id      BIGSERIAL PRIMARY KEY,
    grp     INT NOT NULL,
    qty     INT NOT NULL,
    amount  NUMERIC NOT NULL
);

\echo '--- Seeding 10M source / 500K groups (CLUSTERED) ---'
INSERT INTO bm (grp, qty, amount)
SELECT
    ((i - 1) / 20)::INT,
    (random() * 1000)::INT,
    (random() * 1000)::NUMERIC(10,2)
FROM generate_series(1, 10000000) AS i;
ANALYZE bm;

\echo ''
SELECT public.create_reflex_ivm(
    'bm_v',
    'SELECT grp, SUM(qty) AS total_qty, SUM(amount) AS total_amount, COUNT(*) AS cnt
     FROM bm GROUP BY grp',
    NULL, NULL, 'IMMEDIATE', NULL
);
ANALYZE bm_v;

DROP MATERIALIZED VIEW IF EXISTS bm_matview;
CREATE MATERIALIZED VIEW bm_matview AS
SELECT grp, SUM(qty) AS total_qty, SUM(amount) AS total_amount, COUNT(*) AS cnt
FROM bm GROUP BY grp;
ANALYZE bm_matview;

SELECT 'sizes' AS x, pg_size_pretty(pg_relation_size('bm')) AS src,
       pg_size_pretty(pg_relation_size('bm_v')) AS tgt,
       pg_size_pretty(pg_relation_size('__reflex_intermediate_bm_v')) AS imm,
       (SELECT count(*) FROM bm_v) AS target_rows;

\echo ''
\echo '--- Warm-up ---'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id <= 200; ROLLBACK;

\echo ''
\echo '=== 0.001% sel — UPDATE 200 rows / 10 groups ==='
\echo '[pg_reflex]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 200; ROLLBACK;
\echo '[REFRESH MAT VIEW]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 200; REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '=== 0.1% sel — UPDATE 10 K rows / 500 groups ==='
\echo '[pg_reflex]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 10000; ROLLBACK;
\echo '[REFRESH MAT VIEW]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 10000; REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '=== 1% sel — UPDATE 100 K rows / 5 K groups ==='
\echo '[pg_reflex]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 100000; ROLLBACK;
\echo '[REFRESH MAT VIEW]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 100000; REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '=== 5% sel — UPDATE 500 K rows / 25 K groups ==='
\echo '[pg_reflex]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 500000; ROLLBACK;
\echo '[REFRESH MAT VIEW]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 500000; REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '=== 15% sel — UPDATE 1.5 M rows / 75 K groups ==='
\echo '[pg_reflex]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 1500000; ROLLBACK;
\echo '[REFRESH MAT VIEW]'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id BETWEEN 1 AND 1500000; REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '--- Cleanup ---'
SELECT public.drop_reflex_ivm('bm_v');
DROP MATERIALIZED VIEW bm_matview;
DROP TABLE bm CASCADE;
