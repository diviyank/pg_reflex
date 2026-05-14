-- ============================================================================
-- Plan #10 (1.4.6) — MERGE-on-target benchmark
--
-- Goal: measure UPDATE wall-clock on an aggregate IMV with NOT NULL group
--   cols at selectivities BELOW the dispatch threshold (0.3) so the
--   MERGE-on-target codepath is the one exercised, not the high-selectivity
--   reconcile.
--
-- Workload: 5 M source rows / 500 K groups (10 rows per group, NOT NULL grp).
--   UPDATE varying counts → 1%, 10%, 25% group selectivity. All stay
--   below the wipe threshold so #10's single-MERGE replaces the legacy
--   DELETE+INSERT on the target.
--
-- Connection: psql -h localhost -p 28817 -d bench_db -f <this>
-- ============================================================================

\timing on
SELECT setseed(0.42);

DROP TABLE IF EXISTS tmrg_src CASCADE;
CREATE TABLE tmrg_src (
    id      BIGSERIAL PRIMARY KEY,
    grp     INT NOT NULL,
    qty     INT NOT NULL
);

\echo ''
\echo '--- Seeding 5M source rows / 500K groups (10 rows/group) ---'
INSERT INTO tmrg_src (grp, qty)
SELECT
    (i % 500000)::INT AS grp,
    (random() * 1000)::INT AS qty
FROM generate_series(1, 5000000) AS i;

ANALYZE tmrg_src;

\echo ''
\echo '--- Creating IMV (grouped on NOT NULL `grp`) ---'
SELECT public.create_reflex_ivm(
    'tmrg_v',
    'SELECT grp, SUM(qty) AS total, COUNT(*) AS cnt FROM tmrg_src GROUP BY grp',
    NULL, NULL, 'IMMEDIATE', NULL
);

ANALYZE tmrg_v;

-- Warm-up so the buffer cache + plan cache are populated.
\echo ''
\echo '--- Warm-up (UPDATE 500 rows = ~500 affected groups, 0.1%) ---'
BEGIN; UPDATE tmrg_src SET qty = qty + 1 WHERE id <= 500; ROLLBACK;

\echo ''
\echo '=========================================='
\echo '  1% selectivity — UPDATE 5 K rows / ~5 K groups'
\echo '=========================================='
BEGIN; UPDATE tmrg_src SET qty = qty + 1 WHERE id BETWEEN 1 AND 5000; ROLLBACK;
\echo '--- Run 2 ---'
BEGIN; UPDATE tmrg_src SET qty = qty + 1 WHERE id BETWEEN 5001 AND 10000; ROLLBACK;
\echo '--- Run 3 ---'
BEGIN; UPDATE tmrg_src SET qty = qty + 1 WHERE id BETWEEN 10001 AND 15000; ROLLBACK;

\echo ''
\echo '=========================================='
\echo '  10% selectivity — UPDATE 50 K rows / ~50 K groups'
\echo '=========================================='
BEGIN; UPDATE tmrg_src SET qty = qty + 1 WHERE id BETWEEN 15001 AND 65000; ROLLBACK;
\echo '--- Run 2 ---'
BEGIN; UPDATE tmrg_src SET qty = qty + 1 WHERE id BETWEEN 65001 AND 115000; ROLLBACK;
\echo '--- Run 3 ---'
BEGIN; UPDATE tmrg_src SET qty = qty + 1 WHERE id BETWEEN 115001 AND 165000; ROLLBACK;

\echo ''
\echo '=========================================='
\echo '  25% selectivity — UPDATE 125 K rows / ~125 K groups'
\echo '=========================================='
BEGIN; UPDATE tmrg_src SET qty = qty + 1 WHERE id BETWEEN 165001 AND 290000; ROLLBACK;
\echo '--- Run 2 ---'
BEGIN; UPDATE tmrg_src SET qty = qty + 1 WHERE id BETWEEN 290001 AND 415000; ROLLBACK;

\echo ''
\echo '--- Verify dispatch path: low-selectivity must use incremental ---'
\echo '(Look for `RAISE DEBUG` `incremental` in the log — but DEBUG is off by default.)'
\echo '(If `reconciled IMV` notices appear above, the dispatch fired and #10 was NOT exercised.)'

\echo ''
\echo '--- Cleanup ---'
SELECT public.drop_reflex_ivm('tmrg_v');
DROP TABLE tmrg_src CASCADE;
