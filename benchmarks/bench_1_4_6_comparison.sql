-- ============================================================================
-- Plan #10 (1.4.6) — comprehensive comparison benchmark
--
-- Compares incremental UPDATE costs at multiple selectivities against a
-- baseline of REFRESH MATERIALIZED VIEW. Run twice: once against the
-- post-#10 binary, once against the pre-#10 binary (`git stash` the
-- restructure and re-install).
--
-- All workloads use NOT NULL group cols so the #10 codepath is exercised.
-- ============================================================================

\timing on
SELECT setseed(0.42);

DROP TABLE IF EXISTS bm_src CASCADE;
CREATE TABLE bm_src (
    id      BIGSERIAL PRIMARY KEY,
    grp     INT NOT NULL,
    region  TEXT NOT NULL,
    qty     INT NOT NULL,
    amount  NUMERIC NOT NULL
);

\echo '--- Seeding 2M source rows / 100K groups (20 rows/group, CLUSTERED by grp) ---'
-- Clustered layout: rows 1..20 → grp 0, 21..40 → grp 1, etc. This lets
-- range UPDATEs hit a contiguous slice of groups (true low-selectivity)
-- instead of touching every group like a `grp = id % N` shape would.
INSERT INTO bm_src (grp, region, qty, amount)
SELECT
    ((i - 1) / 20)::INT,
    (ARRAY['us-e','us-w','eu','apac','latam'])[1 + (i % 5)],
    (random() * 1000)::INT,
    (random() * 1000)::NUMERIC(10,2)
FROM generate_series(1, 2000000) AS i;
ANALYZE bm_src;

\echo ''
\echo '=============================================================='
\echo '  Workload A: Simple SUM IMV, varying UPDATE selectivities'
\echo '=============================================================='

SELECT public.create_reflex_ivm(
    'bm_v',
    'SELECT grp, region, SUM(qty) AS total_qty, SUM(amount) AS total_amount, COUNT(*) AS cnt
     FROM bm_src GROUP BY grp, region',
    NULL, NULL, 'IMMEDIATE', NULL
);
ANALYZE bm_v;

-- Baseline: REFRESH MATERIALIZED VIEW for the same shape
DROP MATERIALIZED VIEW IF EXISTS bm_matview;
CREATE MATERIALIZED VIEW bm_matview AS
SELECT grp, region, SUM(qty) AS total_qty, SUM(amount) AS total_amount, COUNT(*) AS cnt
FROM bm_src GROUP BY grp, region;
ANALYZE bm_matview;

\echo ''
\echo '--- Warm-up ---'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id <= 500; ROLLBACK;

\echo ''
\echo '=== 0.1% sel — UPDATE 2 K rows ==='
\echo '[pg_reflex IMV]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 2000; ROLLBACK;
\echo '[REFRESH MATERIALIZED VIEW (no IMV update)]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 2000;
       REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '=== 1% sel — UPDATE 20 K rows ==='
\echo '[pg_reflex IMV]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 20000; ROLLBACK;
\echo '[REFRESH MATERIALIZED VIEW]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 20000;
       REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '=== 5% sel — UPDATE 100 K rows ==='
\echo '[pg_reflex IMV]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 100000; ROLLBACK;
\echo '[REFRESH MATERIALIZED VIEW]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 100000;
       REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '=== 10% sel — UPDATE 200 K rows ==='
\echo '[pg_reflex IMV]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 200000; ROLLBACK;
\echo '[REFRESH MATERIALIZED VIEW]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 200000;
       REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '=== 25% sel — UPDATE 500 K rows (still below 30% dispatch threshold) ==='
\echo '[pg_reflex IMV]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 500000; ROLLBACK;
\echo '[REFRESH MATERIALIZED VIEW]'
BEGIN; UPDATE bm_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 500000;
       REFRESH MATERIALIZED VIEW bm_matview; ROLLBACK;

\echo ''
\echo '--- Cleanup ---'
SELECT public.drop_reflex_ivm('bm_v');
DROP MATERIALIZED VIEW bm_matview;
DROP TABLE bm_src CASCADE;
