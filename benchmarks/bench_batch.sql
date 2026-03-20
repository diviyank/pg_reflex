-- pg_reflex benchmark: BATCH UPDATES (the core use case)
--
-- Measures the real-world latency of large batch INSERT/DELETE/UPDATE operations.
-- The AFTER STATEMENT trigger fires ONCE per SQL statement, processing the entire
-- batch in a single pass. Compare with REFRESH MATERIALIZED VIEW (the traditional approach).
--
-- Base table: 1M rows. Batch sizes: 10K, 50K, 100K, 500K, 1M rows.

\timing on
\echo ''
\echo '================================================================'
\echo '  BENCHMARK: BATCH UPDATES — pg_reflex vs REFRESH MATERIALIZED VIEW'
\echo '  Base table: 1M rows, 10 regions'
\echo '  Query: SELECT region, SUM(amount) AS total, COUNT(*) AS cnt'
\echo '         FROM bench_orders GROUP BY region'
\echo '================================================================'

-- ============================================================
-- SETUP: 1M row base table + IMV + MATERIALIZED VIEW
-- ============================================================
\echo ''
\echo '--- Setup: seeding 1M rows ---'
SELECT bench_seed_orders(1000000);

SELECT bench_cleanup_imv('bench_batch_view');

\echo 'Creating pg_reflex IMV...'
SELECT create_reflex_ivm('bench_batch_view',
    'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region');

\echo 'Creating standard MATERIALIZED VIEW...'
DROP MATERIALIZED VIEW IF EXISTS bench_batch_matview;
CREATE MATERIALIZED VIEW bench_batch_matview AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region;

\echo ''
\echo '================================================================'
\echo '  BATCH INSERT'
\echo '================================================================'

-- --- 10K INSERT ---
\echo ''
\echo '--- Batch INSERT: 10,000 rows ---'

\echo '[pg_reflex]  INSERT 10K rows (trigger processes entire batch):'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'BatchCity', ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 10000) AS i;

\echo '[baseline]   REFRESH MATERIALIZED VIEW (full re-scan):'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness] IMV vs direct query:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) || ' mismatches' END AS result
FROM (
    SELECT r.region FROM bench_batch_view r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

-- --- 50K INSERT ---
\echo ''
\echo '--- Batch INSERT: 50,000 rows ---'

\echo '[pg_reflex]  INSERT 50K rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'BatchCity', ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 50000) AS i;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 100K INSERT ---
\echo ''
\echo '--- Batch INSERT: 100,000 rows ---'

\echo '[pg_reflex]  INSERT 100K rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'BatchCity', ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 100000) AS i;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 500K INSERT ---
\echo ''
\echo '--- Batch INSERT: 500,000 rows ---'

\echo '[pg_reflex]  INSERT 500K rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'BatchCity', ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 500000) AS i;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 1M INSERT ---
\echo ''
\echo '--- Batch INSERT: 1,000,000 rows ---'

\echo '[pg_reflex]  INSERT 1M rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'BatchCity', ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

\echo ''
\echo '================================================================'
\echo '  BATCH DELETE'
\echo '  (Resetting to 1M base rows first)'
\echo '================================================================'

-- Reset to clean 1M
SELECT bench_cleanup_imv('bench_batch_view');
DROP MATERIALIZED VIEW IF EXISTS bench_batch_matview;
SELECT bench_seed_orders(1000000);
SELECT create_reflex_ivm('bench_batch_view',
    'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region');
CREATE MATERIALIZED VIEW bench_batch_matview AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region;

-- --- 10K DELETE ---
\echo ''
\echo '--- Batch DELETE: 10,000 rows ---'

\echo '[pg_reflex]  DELETE 10K rows (trigger processes entire batch):'
DELETE FROM bench_orders WHERE id <= 10000;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 50K DELETE ---
\echo ''
\echo '--- Batch DELETE: 50,000 rows ---'

\echo '[pg_reflex]  DELETE 50K rows:'
DELETE FROM bench_orders WHERE id > 10000 AND id <= 60000;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 100K DELETE ---
\echo ''
\echo '--- Batch DELETE: 100,000 rows ---'

\echo '[pg_reflex]  DELETE 100K rows:'
DELETE FROM bench_orders WHERE id > 60000 AND id <= 160000;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

\echo ''
\echo '================================================================'
\echo '  BATCH UPDATE'
\echo '  (Resetting to 1M base rows first)'
\echo '================================================================'

-- Reset to clean 1M
SELECT bench_cleanup_imv('bench_batch_view');
DROP MATERIALIZED VIEW IF EXISTS bench_batch_matview;
SELECT bench_seed_orders(1000000);
SELECT create_reflex_ivm('bench_batch_view',
    'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region');
CREATE MATERIALIZED VIEW bench_batch_matview AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region;

-- --- 10K UPDATE ---
\echo ''
\echo '--- Batch UPDATE: 10,000 rows ---'

\echo '[pg_reflex]  UPDATE 10K rows (trigger processes entire batch):'
UPDATE bench_orders SET amount = amount + 1 WHERE id <= 10000;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 50K UPDATE ---
\echo ''
\echo '--- Batch UPDATE: 50,000 rows ---'

\echo '[pg_reflex]  UPDATE 50K rows:'
UPDATE bench_orders SET amount = amount + 1 WHERE id <= 50000;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 100K UPDATE ---
\echo ''
\echo '--- Batch UPDATE: 100,000 rows ---'

\echo '[pg_reflex]  UPDATE 100K rows:'
UPDATE bench_orders SET amount = amount + 1 WHERE id <= 100000;

\echo '[baseline]   REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_batch_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (SELECT r.region FROM bench_batch_view r FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region WHERE r.total IS DISTINCT FROM d.total) diff;

-- ============================================================
-- CLEANUP
-- ============================================================
\echo ''
SELECT bench_cleanup_imv('bench_batch_view');
DROP MATERIALIZED VIEW IF EXISTS bench_batch_matview;

\echo ''
\echo '================================================================'
\echo '  BATCH BENCHMARK COMPLETE'
\echo '  Compare [pg_reflex] times vs [baseline] times above'
\echo '================================================================'
