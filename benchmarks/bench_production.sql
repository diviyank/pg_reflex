-- pg_reflex benchmark: PRODUCTION-LIKE SCENARIO
--
-- Simulates a real analytics workload:
-- - 3M rows in a fact table (orders)
-- - JOINed with 2 dimension tables (customers, products)
-- - GROUP BY with multiple aggregates (SUM, COUNT, AVG)
-- - REFRESH MATERIALIZED VIEW takes ~3s
--
-- Measures pg_reflex trigger latency for batch INSERT/DELETE/UPDATE
-- compared with REFRESH MATERIALIZED VIEW.

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  PRODUCTION BENCHMARK'
\echo '  3M orders × customers × products'
\echo '  Query: revenue by customer segment + product category'
\echo '================================================================'

-- ============================================================
-- SETUP: Create dimension + fact tables
-- ============================================================
\echo ''
\echo '--- Setting up tables ---'

DROP TABLE IF EXISTS prod_orders CASCADE;
DROP TABLE IF EXISTS prod_customers CASCADE;
DROP TABLE IF EXISTS prod_products CASCADE;

-- Dimension: 1K customers across 5 segments
CREATE TABLE prod_customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    segment TEXT NOT NULL  -- 'Enterprise', 'SMB', 'Startup', 'Government', 'Education'
);
INSERT INTO prod_customers (name, segment)
SELECT
    'Customer_' || i,
    (ARRAY['Enterprise', 'SMB', 'Startup', 'Government', 'Education'])[1 + (i % 5)]
FROM generate_series(1, 1000) AS i;

-- Dimension: 500 products across 10 categories
CREATE TABLE prod_products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL
);
INSERT INTO prod_products (name, category)
SELECT
    'Product_' || i,
    (ARRAY['Electronics', 'Clothing', 'Food', 'Books', 'Sports',
           'Home', 'Garden', 'Automotive', 'Health', 'Toys'])[1 + (i % 10)]
FROM generate_series(1, 500) AS i;

-- Fact: 3M orders
CREATE TABLE prod_orders (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES prod_customers(id),
    product_id INT NOT NULL REFERENCES prod_products(id),
    quantity INT NOT NULL,
    unit_price NUMERIC NOT NULL,
    order_date DATE NOT NULL
);

\echo 'Seeding 3M orders (this takes ~20s)...'
INSERT INTO prod_orders (customer_id, product_id, quantity, unit_price, order_date)
SELECT
    1 + (i % 1000),           -- customer_id (1-1000)
    1 + (i % 500),            -- product_id (1-500)
    1 + (i % 10),             -- quantity (1-10)
    ROUND((10 + random() * 490)::numeric, 2),  -- unit_price ($10-$500)
    DATE '2023-01-01' + (i % 730)              -- order_date (2 years)
FROM generate_series(1, 3000000) AS i;

CREATE INDEX idx_prod_orders_customer ON prod_orders(customer_id);
CREATE INDEX idx_prod_orders_product ON prod_orders(product_id);
ANALYZE prod_orders;
ANALYZE prod_customers;
ANALYZE prod_products;

-- The query: revenue by segment × category
-- This is the view we'll maintain incrementally
\echo ''
\echo 'Query: SELECT c.segment, p.category,'
\echo '              SUM(o.quantity * o.unit_price) AS revenue,'
\echo '              COUNT(*) AS order_count,'
\echo '              AVG(o.unit_price) AS avg_price'
\echo '       FROM prod_orders o'
\echo '       JOIN prod_customers c ON o.customer_id = c.id'
\echo '       JOIN prod_products p ON o.product_id = p.id'
\echo '       GROUP BY c.segment, p.category'

-- ============================================================
-- BASELINE: How long does REFRESH MATERIALIZED VIEW take?
-- ============================================================
\echo ''
\echo '--- Baseline: REFRESH MATERIALIZED VIEW ---'

DROP MATERIALIZED VIEW IF EXISTS prod_matview;
\echo 'CREATE MATERIALIZED VIEW (initial):'
CREATE MATERIALIZED VIEW prod_matview AS
    SELECT c.segment, p.category,
           SUM(o.quantity * o.unit_price) AS revenue,
           COUNT(*) AS order_count,
           AVG(o.unit_price) AS avg_price
    FROM prod_orders o
    JOIN prod_customers c ON o.customer_id = c.id
    JOIN prod_products p ON o.product_id = p.id
    GROUP BY c.segment, p.category;

\echo 'Row count in matview:'
SELECT COUNT(*) AS groups FROM prod_matview;

\echo 'REFRESH MATERIALIZED VIEW (baseline time):'
REFRESH MATERIALIZED VIEW prod_matview;

\echo 'REFRESH again (warm cache):'
REFRESH MATERIALIZED VIEW prod_matview;

-- ============================================================
-- PG_REFLEX: Create IMV
-- ============================================================
\echo ''
\echo '--- pg_reflex: CREATE IMV ---'

\echo 'create_reflex_ivm (initial materialization):'
SELECT create_reflex_ivm('prod_imv',
    'SELECT c.segment, p.category,
            SUM(o.quantity * o.unit_price) AS revenue,
            COUNT(*) AS order_count,
            AVG(o.unit_price) AS avg_price
     FROM prod_orders o
     JOIN prod_customers c ON o.customer_id = c.id
     JOIN prod_products p ON o.product_id = p.id
     GROUP BY c.segment, p.category');

\echo 'IMV row count:'
SELECT COUNT(*) AS groups FROM prod_imv;

-- ============================================================
-- BENCHMARK: Batch INSERT
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  BATCH INSERT (into prod_orders, triggers update prod_imv)'
\echo '================================================================'

-- 1K INSERT
\echo ''
\echo '--- Batch INSERT: 1,000 rows ---'
\echo '[pg_reflex]:'
INSERT INTO prod_orders (customer_id, product_id, quantity, unit_price, order_date)
SELECT 1 + (i % 1000), 1 + (i % 500), 1 + (i % 5),
       ROUND((50 + random() * 200)::numeric, 2), DATE '2025-01-01'
FROM generate_series(1, 1000) AS i;

\echo '[baseline] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW prod_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (
    SELECT r.segment, r.category FROM prod_imv r
    FULL OUTER JOIN (
        SELECT c.segment, p.category, SUM(o.quantity * o.unit_price) AS revenue
        FROM prod_orders o JOIN prod_customers c ON o.customer_id = c.id
        JOIN prod_products p ON o.product_id = p.id GROUP BY c.segment, p.category
    ) d ON r.segment = d.segment AND r.category = d.category
    WHERE r.revenue IS DISTINCT FROM d.revenue
) diff;

-- 10K INSERT
\echo ''
\echo '--- Batch INSERT: 10,000 rows ---'
\echo '[pg_reflex]:'
INSERT INTO prod_orders (customer_id, product_id, quantity, unit_price, order_date)
SELECT 1 + (i % 1000), 1 + (i % 500), 1 + (i % 5),
       ROUND((50 + random() * 200)::numeric, 2), DATE '2025-02-01'
FROM generate_series(1, 10000) AS i;

\echo '[baseline] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW prod_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (
    SELECT r.segment, r.category FROM prod_imv r
    FULL OUTER JOIN (
        SELECT c.segment, p.category, SUM(o.quantity * o.unit_price) AS revenue
        FROM prod_orders o JOIN prod_customers c ON o.customer_id = c.id
        JOIN prod_products p ON o.product_id = p.id GROUP BY c.segment, p.category
    ) d ON r.segment = d.segment AND r.category = d.category
    WHERE r.revenue IS DISTINCT FROM d.revenue
) diff;

-- 50K INSERT
\echo ''
\echo '--- Batch INSERT: 50,000 rows ---'
\echo '[pg_reflex]:'
INSERT INTO prod_orders (customer_id, product_id, quantity, unit_price, order_date)
SELECT 1 + (i % 1000), 1 + (i % 500), 1 + (i % 5),
       ROUND((50 + random() * 200)::numeric, 2), DATE '2025-03-01'
FROM generate_series(1, 50000) AS i;

\echo '[baseline] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW prod_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (
    SELECT r.segment, r.category FROM prod_imv r
    FULL OUTER JOIN (
        SELECT c.segment, p.category, SUM(o.quantity * o.unit_price) AS revenue
        FROM prod_orders o JOIN prod_customers c ON o.customer_id = c.id
        JOIN prod_products p ON o.product_id = p.id GROUP BY c.segment, p.category
    ) d ON r.segment = d.segment AND r.category = d.category
    WHERE r.revenue IS DISTINCT FROM d.revenue
) diff;

-- 100K INSERT
\echo ''
\echo '--- Batch INSERT: 100,000 rows ---'
\echo '[pg_reflex]:'
INSERT INTO prod_orders (customer_id, product_id, quantity, unit_price, order_date)
SELECT 1 + (i % 1000), 1 + (i % 500), 1 + (i % 5),
       ROUND((50 + random() * 200)::numeric, 2), DATE '2025-04-01'
FROM generate_series(1, 100000) AS i;

\echo '[baseline] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW prod_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (
    SELECT r.segment, r.category FROM prod_imv r
    FULL OUTER JOIN (
        SELECT c.segment, p.category, SUM(o.quantity * o.unit_price) AS revenue
        FROM prod_orders o JOIN prod_customers c ON o.customer_id = c.id
        JOIN prod_products p ON o.product_id = p.id GROUP BY c.segment, p.category
    ) d ON r.segment = d.segment AND r.category = d.category
    WHERE r.revenue IS DISTINCT FROM d.revenue
) diff;

-- ============================================================
-- BENCHMARK: Batch DELETE
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  BATCH DELETE'
\echo '================================================================'

-- 10K DELETE
\echo ''
\echo '--- Batch DELETE: 10,000 rows ---'
\echo '[pg_reflex]:'
DELETE FROM prod_orders WHERE id <= 10000;

\echo '[baseline] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW prod_matview;

\echo '[correctness]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (
    SELECT r.segment, r.category FROM prod_imv r
    FULL OUTER JOIN (
        SELECT c.segment, p.category, SUM(o.quantity * o.unit_price) AS revenue
        FROM prod_orders o JOIN prod_customers c ON o.customer_id = c.id
        JOIN prod_products p ON o.product_id = p.id GROUP BY c.segment, p.category
    ) d ON r.segment = d.segment AND r.category = d.category
    WHERE r.revenue IS DISTINCT FROM d.revenue
) diff;

-- ============================================================
-- CLEANUP
-- ============================================================
\echo ''
DROP MATERIALIZED VIEW IF EXISTS prod_matview;

\echo '================================================================'
\echo '  PRODUCTION BENCHMARK COMPLETE'
\echo '================================================================'
