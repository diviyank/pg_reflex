-- ============================================================================
-- 1.12.5 — COMMIT-time flush of a reader of a source and of a DEFERRED IMV over it
--
-- Shape (prod: sop_forecast_view = sop_current_view ⋈ current_assortment_activity_view):
--   mf_x  source, 200 K rows, 2 K (product, location) groups of 100 rows
--   mf_u  DEFERRED aggregate over mf_x
--   mf_v  DEFERRED keyed passthrough mf_x ⋈ mf_u
--
-- Each write is its own transaction; the COMMIT line's \timing is the flush cost.
-- Rows are committed, so rerun from the top for each build.
-- Connection: psql -X -h localhost -p 28816 -d bench_db -f <this>
-- ============================================================================

\set ON_ERROR_STOP on
SET client_min_messages = warning;

SELECT drop_reflex_ivm(name) FROM public.__reflex_ivm_reference
 WHERE name IN ('mf_v', 'mf_u') ORDER BY graph_depth DESC;
DROP TABLE IF EXISTS mf_x CASCADE;

CREATE TABLE mf_x (
    id          INT PRIMARY KEY,
    product_id  INT NOT NULL,
    location_id INT NOT NULL,
    qty         INT NOT NULL
);
INSERT INTO mf_x SELECT g, g % 2000, g % 50, g % 997 FROM generate_series(1, 200000) g;
ANALYZE mf_x;

SELECT create_reflex_ivm('mf_u',
    'SELECT product_id, location_id, SUM(qty) AS total, COUNT(*) AS n
       FROM mf_x GROUP BY product_id, location_id',
    NULL, NULL, 'DEFERRED');
SELECT create_reflex_ivm('mf_v',
    'SELECT x.id, x.product_id, x.location_id, x.qty, u.total
       FROM mf_x x JOIN mf_u u ON u.product_id = x.product_id AND u.location_id = x.location_id',
    'id', NULL, 'DEFERRED');
ANALYZE mf_u;
ANALYZE mf_v;

-- Warm-up
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id = 1; COMMIT;
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id BETWEEN 1 AND 1000; COMMIT;

\timing on
\echo '== one_row'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id = 101; COMMIT;
\echo '== one_row'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id = 202; COMMIT;
\echo '== one_row'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id = 303; COMMIT;
\echo '== one_row'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id = 404; COMMIT;
\echo '== one_row'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id = 505; COMMIT;
\echo '== one_row'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id = 606; COMMIT;
\echo '== one_row'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id = 707; COMMIT;
\echo '== thousand_rows'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id BETWEEN 10001 AND 11000; COMMIT;
\echo '== thousand_rows'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id BETWEEN 20001 AND 21000; COMMIT;
\echo '== thousand_rows'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id BETWEEN 30001 AND 31000; COMMIT;
\echo '== thousand_rows'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id BETWEEN 40001 AND 41000; COMMIT;
\echo '== thousand_rows'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id BETWEEN 50001 AND 51000; COMMIT;
\echo '== thousand_rows'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id BETWEEN 60001 AND 61000; COMMIT;
\echo '== thousand_rows'
BEGIN; UPDATE mf_x SET qty = qty + 1 WHERE id BETWEEN 70001 AND 71000; COMMIT;
\timing off

\echo '== mismatches (mf_u, mf_v)'
SELECT count(*) FROM (
    (SELECT * FROM mf_u EXCEPT ALL
     SELECT product_id, location_id, SUM(qty), COUNT(*) FROM mf_x GROUP BY 1, 2)
    UNION ALL
    (SELECT product_id, location_id, SUM(qty), COUNT(*) FROM mf_x GROUP BY 1, 2
     EXCEPT ALL SELECT * FROM mf_u)) d;
SELECT count(*) FROM (
    (SELECT * FROM mf_v EXCEPT ALL
     SELECT x.id, x.product_id, x.location_id, x.qty, u.total FROM mf_x x
       JOIN (SELECT product_id, location_id, SUM(qty) AS total FROM mf_x GROUP BY 1, 2) u
         USING (product_id, location_id))
    UNION ALL
    (SELECT x.id, x.product_id, x.location_id, x.qty, u.total FROM mf_x x
       JOIN (SELECT product_id, location_id, SUM(qty) AS total FROM mf_x GROUP BY 1, 2) u
         USING (product_id, location_id)
     EXCEPT ALL SELECT * FROM mf_v)) d;
