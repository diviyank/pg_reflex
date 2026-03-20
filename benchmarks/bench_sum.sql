-- pg_reflex benchmark: SUM aggregate
-- Measures: SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region

\timing on
\echo ''
\echo '=========================================='
\echo '  BENCHMARK: SUM Aggregate'
\echo '=========================================='

-- Run for each data size
DO $$
DECLARE
    sizes INT[] := ARRAY[1000, 10000, 100000, 1000000];
    n INT;
BEGIN
    FOREACH n IN ARRAY sizes LOOP
        RAISE NOTICE '--- Scale: % rows ---', n;
    END LOOP;
END $$;

-- === 1K rows ===
\echo ''
\echo '--- Scale: 1,000 rows ---'
SELECT bench_seed_orders(1000);
SELECT bench_cleanup_imv('bench_sum_view');

\echo '[1K] Initial materialization:'
SELECT create_reflex_ivm('bench_sum_view', 'SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region');

\echo '[1K] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT
    (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
    'BenchCity',
    ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[1K] Single INSERT:'
INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'BenchCity', 42.00);

\echo '[1K] UPDATE 100 rows:'
UPDATE bench_orders SET amount = amount + 1 WHERE id <= 100;

\echo '[1K] DELETE 100 rows:'
DELETE FROM bench_orders WHERE id <= 100;

\echo '[1K] Correctness check:'
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS: all regions match'
         ELSE 'FAIL: ' || COUNT(*) || ' mismatches'
    END AS result
FROM (
    SELECT r.region, r.total AS imv_total, d.total AS direct_total
    FROM bench_sum_view r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d
        ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

SELECT bench_cleanup_imv('bench_sum_view');

-- === 10K rows ===
\echo ''
\echo '--- Scale: 10,000 rows ---'
SELECT bench_seed_orders(10000);

\echo '[10K] Initial materialization:'
SELECT create_reflex_ivm('bench_sum_view', 'SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region');

\echo '[10K] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT
    (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
    'BenchCity',
    ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[10K] Single INSERT:'
INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'BenchCity', 42.00);

\echo '[10K] UPDATE 100 rows:'
UPDATE bench_orders SET amount = amount + 1 WHERE id <= 100;

\echo '[10K] DELETE 100 rows:'
DELETE FROM bench_orders WHERE id <= 100;

\echo '[10K] Correctness check:'
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS: all regions match'
         ELSE 'FAIL: ' || COUNT(*) || ' mismatches'
    END AS result
FROM (
    SELECT r.region, r.total AS imv_total, d.total AS direct_total
    FROM bench_sum_view r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d
        ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

SELECT bench_cleanup_imv('bench_sum_view');

-- === 100K rows ===
\echo ''
\echo '--- Scale: 100,000 rows ---'
SELECT bench_seed_orders(100000);

\echo '[100K] Initial materialization:'
SELECT create_reflex_ivm('bench_sum_view', 'SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region');

\echo '[100K] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT
    (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
    'BenchCity',
    ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[100K] Single INSERT:'
INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'BenchCity', 42.00);

\echo '[100K] UPDATE 100 rows:'
UPDATE bench_orders SET amount = amount + 1 WHERE id <= 100;

\echo '[100K] DELETE 100 rows:'
DELETE FROM bench_orders WHERE id <= 100;

\echo '[100K] Correctness check:'
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS: all regions match'
         ELSE 'FAIL: ' || COUNT(*) || ' mismatches'
    END AS result
FROM (
    SELECT r.region, r.total AS imv_total, d.total AS direct_total
    FROM bench_sum_view r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d
        ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

SELECT bench_cleanup_imv('bench_sum_view');

-- === 1M rows ===
\echo ''
\echo '--- Scale: 1,000,000 rows ---'
SELECT bench_seed_orders(1000000);

\echo '[1M] Initial materialization:'
SELECT create_reflex_ivm('bench_sum_view', 'SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region');

\echo '[1M] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT
    (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
    'BenchCity',
    ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[1M] Single INSERT:'
INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'BenchCity', 42.00);

\echo '[1M] UPDATE 100 rows:'
UPDATE bench_orders SET amount = amount + 1 WHERE id <= 100;

\echo '[1M] DELETE 100 rows:'
DELETE FROM bench_orders WHERE id <= 100;

\echo '[1M] Correctness check:'
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS: all regions match'
         ELSE 'FAIL: ' || COUNT(*) || ' mismatches'
    END AS result
FROM (
    SELECT r.region, r.total AS imv_total, d.total AS direct_total
    FROM bench_sum_view r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d
        ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

SELECT bench_cleanup_imv('bench_sum_view');

\echo ''
\echo '=== SUM benchmark complete ==='
