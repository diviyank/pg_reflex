-- pg_reflex benchmark: CTE (Common Table Expression) decomposition
-- Tests complex queries with CTEs at 4 data scales

\timing on
\echo ''
\echo '=========================================='
\echo '  BENCHMARK: CTE Decomposition'
\echo '=========================================='

-----------------------------------------------------------
-- Query 1: Single CTE with SUM aggregation
-- CTE does GROUP BY + SUM, main body is a passthrough VIEW
-----------------------------------------------------------

\echo ''
\echo '=========================================='
\echo '  Query 1: Single CTE Aggregation'
\echo '=========================================='

-- 1K
\echo '--- Scale: 1,000 rows ---'
SELECT bench_seed_orders(1000);
SELECT bench_cleanup_imv('q1_cte_view');
SELECT bench_cleanup_imv('q1_cte_view__cte_regional');

\echo '[1K] Create CTE IMV:'
SELECT create_reflex_ivm('q1_cte_view',
    'WITH regional AS (
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
        FROM bench_orders GROUP BY region
    ) SELECT region, total, cnt FROM regional WHERE total > 0');

\echo '[1K] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
       'BenchCity', ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[1K] Single INSERT:'
INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'Bench', 42.00);

\echo '[1K] DELETE 100 rows:'
DELETE FROM bench_orders WHERE id <= 100;

\echo '[1K] Correctness check:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) || ' mismatches' END AS result
FROM (
    SELECT r.region, r.total AS imv, d.total AS direct
    FROM q1_cte_view r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region HAVING SUM(amount) > 0) d ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

SELECT bench_cleanup_imv('q1_cte_view');
SELECT bench_cleanup_imv('q1_cte_view__cte_regional');
DROP VIEW IF EXISTS "q1_cte_view";

-- 10K
\echo '--- Scale: 10,000 rows ---'
SELECT bench_seed_orders(10000);

\echo '[10K] Create CTE IMV:'
SELECT create_reflex_ivm('q1_cte_view',
    'WITH regional AS (
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
        FROM bench_orders GROUP BY region
    ) SELECT region, total, cnt FROM regional WHERE total > 0');

\echo '[10K] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
       'BenchCity', ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[10K] Single INSERT:'
INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'Bench', 42.00);

\echo '[10K] DELETE 100 rows:'
DELETE FROM bench_orders WHERE id <= 100;

\echo '[10K] Correctness check:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) || ' mismatches' END AS result
FROM (
    SELECT r.region, r.total AS imv, d.total AS direct
    FROM q1_cte_view r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region HAVING SUM(amount) > 0) d ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

SELECT bench_cleanup_imv('q1_cte_view');
SELECT bench_cleanup_imv('q1_cte_view__cte_regional');
DROP VIEW IF EXISTS "q1_cte_view";

-- 100K
\echo '--- Scale: 100,000 rows ---'
SELECT bench_seed_orders(100000);

\echo '[100K] Create CTE IMV:'
SELECT create_reflex_ivm('q1_cte_view',
    'WITH regional AS (
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
        FROM bench_orders GROUP BY region
    ) SELECT region, total, cnt FROM regional WHERE total > 0');

\echo '[100K] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
       'BenchCity', ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[100K] Single INSERT:'
INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'Bench', 42.00);

\echo '[100K] DELETE 100 rows:'
DELETE FROM bench_orders WHERE id <= 100;

\echo '[100K] Correctness check:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) || ' mismatches' END AS result
FROM (
    SELECT r.region, r.total AS imv, d.total AS direct
    FROM q1_cte_view r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region HAVING SUM(amount) > 0) d ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

SELECT bench_cleanup_imv('q1_cte_view');
SELECT bench_cleanup_imv('q1_cte_view__cte_regional');
DROP VIEW IF EXISTS "q1_cte_view";

-- 1M
\echo '--- Scale: 1,000,000 rows ---'
SELECT bench_seed_orders(1000000);

\echo '[1M] Create CTE IMV:'
SELECT create_reflex_ivm('q1_cte_view',
    'WITH regional AS (
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
        FROM bench_orders GROUP BY region
    ) SELECT region, total, cnt FROM regional WHERE total > 0');

\echo '[1M] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
       'BenchCity', ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[1M] Single INSERT:'
INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'Bench', 42.00);

\echo '[1M] DELETE 100 rows:'
DELETE FROM bench_orders WHERE id <= 100;

\echo '[1M] Correctness check:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) || ' mismatches' END AS result
FROM (
    SELECT r.region, r.total AS imv, d.total AS direct
    FROM q1_cte_view r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region HAVING SUM(amount) > 0) d ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

SELECT bench_cleanup_imv('q1_cte_view');
SELECT bench_cleanup_imv('q1_cte_view__cte_regional');
DROP VIEW IF EXISTS "q1_cte_view";

-----------------------------------------------------------
-- Query 2: Multi-level CTE (chained: city -> region)
-----------------------------------------------------------

\echo ''
\echo '=========================================='
\echo '  Query 2: Multi-Level Chained CTE'
\echo '=========================================='

-- 1K
\echo '--- Scale: 1,000 rows ---'
SELECT bench_seed_orders(1000);

\echo '[1K] Create chained CTE IMV:'
SELECT create_reflex_ivm('q2_chain',
    'WITH by_city AS (
        SELECT region, city, SUM(amount) AS city_total, COUNT(*) AS city_cnt
        FROM bench_orders GROUP BY region, city
    ), by_region AS (
        SELECT region, SUM(city_total) AS total, SUM(city_cnt) AS cnt
        FROM by_city GROUP BY region
    ) SELECT region, total, cnt FROM by_region');

\echo '[1K] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West'])[1 + (i % 3)],
       (ARRAY['CityA','CityB','CityC'])[1 + (i % 3)],
       ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[1K] Correctness check (sub-IMV by_city):'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT region, city FROM q2_chain__cte_by_city
    EXCEPT
    SELECT region, city FROM (SELECT region, city, SUM(amount) FROM bench_orders GROUP BY region, city) d
) diff;

SELECT bench_cleanup_imv('q2_chain');
SELECT bench_cleanup_imv('q2_chain__cte_by_city');
SELECT bench_cleanup_imv('q2_chain__cte_by_region');
DROP VIEW IF EXISTS "q2_chain";

-- 100K
\echo '--- Scale: 100,000 rows ---'
SELECT bench_seed_orders(100000);

\echo '[100K] Create chained CTE IMV:'
SELECT create_reflex_ivm('q2_chain',
    'WITH by_city AS (
        SELECT region, city, SUM(amount) AS city_total, COUNT(*) AS city_cnt
        FROM bench_orders GROUP BY region, city
    ), by_region AS (
        SELECT region, SUM(city_total) AS total, SUM(city_cnt) AS cnt
        FROM by_city GROUP BY region
    ) SELECT region, total, cnt FROM by_region');

\echo '[100K] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West'])[1 + (i % 3)],
       (ARRAY['CityA','CityB','CityC'])[1 + (i % 3)],
       ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[100K] Correctness check (sub-IMV by_city):'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT region, city FROM q2_chain__cte_by_city
    EXCEPT
    SELECT region, city FROM (SELECT region, city, SUM(amount) FROM bench_orders GROUP BY region, city) d
) diff;

SELECT bench_cleanup_imv('q2_chain');
SELECT bench_cleanup_imv('q2_chain__cte_by_city');
SELECT bench_cleanup_imv('q2_chain__cte_by_region');
DROP VIEW IF EXISTS "q2_chain";

-- 1M
\echo '--- Scale: 1,000,000 rows ---'
SELECT bench_seed_orders(1000000);

\echo '[1M] Create chained CTE IMV:'
SELECT create_reflex_ivm('q2_chain',
    'WITH by_city AS (
        SELECT region, city, SUM(amount) AS city_total, COUNT(*) AS city_cnt
        FROM bench_orders GROUP BY region, city
    ), by_region AS (
        SELECT region, SUM(city_total) AS total, SUM(city_cnt) AS cnt
        FROM by_city GROUP BY region
    ) SELECT region, total, cnt FROM by_region');

\echo '[1M] Batch INSERT 1000 rows:'
INSERT INTO bench_orders (region, city, amount)
SELECT (ARRAY['US-East','US-West','EU-West'])[1 + (i % 3)],
       (ARRAY['CityA','CityB','CityC'])[1 + (i % 3)],
       ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[1M] Correctness check (sub-IMV by_city):'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT region, city FROM q2_chain__cte_by_city
    EXCEPT
    SELECT region, city FROM (SELECT region, city, SUM(amount) FROM bench_orders GROUP BY region, city) d
) diff;

SELECT bench_cleanup_imv('q2_chain');
SELECT bench_cleanup_imv('q2_chain__cte_by_city');
SELECT bench_cleanup_imv('q2_chain__cte_by_region');
DROP VIEW IF EXISTS "q2_chain";

-----------------------------------------------------------
-- Query 3: CTE with JOIN
-----------------------------------------------------------

\echo ''
\echo '=========================================='
\echo '  Query 3: CTE with JOIN'
\echo '=========================================='

-- 1K
\echo '--- Scale: 1,000 rows ---'
SELECT bench_seed_orders(1000);
SELECT bench_seed_order_items(1000);

\echo '[1K] Create CTE+JOIN IMV:'
SELECT create_reflex_ivm('q3_cte_join',
    'WITH product_revenue AS (
        SELECT p.category, SUM(oi.price * oi.quantity) AS revenue, COUNT(*) AS order_count
        FROM bench_order_items oi JOIN bench_products p ON oi.product_id = p.id
        GROUP BY p.category
    ) SELECT category, revenue, order_count FROM product_revenue');

\echo '[1K] Batch INSERT 1000 order_items:'
INSERT INTO bench_order_items (order_id, product_id, quantity, price)
SELECT 1 + (i % GREATEST((SELECT COUNT(*) FROM bench_orders)::int, 1)),
       1 + (i % 100), 1 + (i % 5), ROUND((random() * 200)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[1K] Correctness check:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT r.category, r.revenue
    FROM q3_cte_join r
    FULL OUTER JOIN (
        SELECT p.category, SUM(oi.price * oi.quantity) AS revenue
        FROM bench_order_items oi JOIN bench_products p ON oi.product_id = p.id
        GROUP BY p.category
    ) d ON r.category = d.category
    WHERE r.revenue IS DISTINCT FROM d.revenue
) diff;

SELECT bench_cleanup_imv('q3_cte_join');
SELECT bench_cleanup_imv('q3_cte_join__cte_product_revenue');
DROP VIEW IF EXISTS "q3_cte_join";

-- 100K
\echo '--- Scale: 100,000 rows ---'
SELECT bench_seed_orders(10000);
SELECT bench_seed_order_items(100000);

\echo '[100K] Create CTE+JOIN IMV:'
SELECT create_reflex_ivm('q3_cte_join',
    'WITH product_revenue AS (
        SELECT p.category, SUM(oi.price * oi.quantity) AS revenue, COUNT(*) AS order_count
        FROM bench_order_items oi JOIN bench_products p ON oi.product_id = p.id
        GROUP BY p.category
    ) SELECT category, revenue, order_count FROM product_revenue');

\echo '[100K] Batch INSERT 1000 order_items:'
INSERT INTO bench_order_items (order_id, product_id, quantity, price)
SELECT 1 + (i % GREATEST((SELECT COUNT(*) FROM bench_orders)::int, 1)),
       1 + (i % 100), 1 + (i % 5), ROUND((random() * 200)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[100K] Correctness check:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT r.category, r.revenue
    FROM q3_cte_join r
    FULL OUTER JOIN (
        SELECT p.category, SUM(oi.price * oi.quantity) AS revenue
        FROM bench_order_items oi JOIN bench_products p ON oi.product_id = p.id
        GROUP BY p.category
    ) d ON r.category = d.category
    WHERE r.revenue IS DISTINCT FROM d.revenue
) diff;

SELECT bench_cleanup_imv('q3_cte_join');
SELECT bench_cleanup_imv('q3_cte_join__cte_product_revenue');
DROP VIEW IF EXISTS "q3_cte_join";

-- 1M
\echo '--- Scale: 1,000,000 rows ---'
SELECT bench_seed_orders(100000);
SELECT bench_seed_order_items(1000000);

\echo '[1M] Create CTE+JOIN IMV:'
SELECT create_reflex_ivm('q3_cte_join',
    'WITH product_revenue AS (
        SELECT p.category, SUM(oi.price * oi.quantity) AS revenue, COUNT(*) AS order_count
        FROM bench_order_items oi JOIN bench_products p ON oi.product_id = p.id
        GROUP BY p.category
    ) SELECT category, revenue, order_count FROM product_revenue');

\echo '[1M] Batch INSERT 1000 order_items:'
INSERT INTO bench_order_items (order_id, product_id, quantity, price)
SELECT 1 + (i % GREATEST((SELECT COUNT(*) FROM bench_orders)::int, 1)),
       1 + (i % 100), 1 + (i % 5), ROUND((random() * 200)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo '[1M] Correctness check:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT r.category, r.revenue
    FROM q3_cte_join r
    FULL OUTER JOIN (
        SELECT p.category, SUM(oi.price * oi.quantity) AS revenue
        FROM bench_order_items oi JOIN bench_products p ON oi.product_id = p.id
        GROUP BY p.category
    ) d ON r.category = d.category
    WHERE r.revenue IS DISTINCT FROM d.revenue
) diff;

SELECT bench_cleanup_imv('q3_cte_join');
SELECT bench_cleanup_imv('q3_cte_join__cte_product_revenue');
DROP VIEW IF EXISTS "q3_cte_join";

\echo ''
\echo '=== CTE benchmark complete ==='
