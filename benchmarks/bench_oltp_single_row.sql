-- pg_reflex benchmark: SINGLE-ROW OLTP INSERT LATENCY
--
-- The most common production pattern: individual row inserts.
-- Measures per-insert trigger overhead for 1-3 IMVs.
-- Source: 1M rows, varying group cardinality.

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  BENCHMARK: SINGLE-ROW OLTP INSERT LATENCY'
\echo '  Source: 1M rows, 1-3 IMVs, per-insert overhead'
\echo '================================================================'

-- ============================================================
-- SETUP
-- ============================================================
\echo ''
\echo '--- Setup ---'

DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;

DROP TABLE IF EXISTS oltp_src CASCADE;
CREATE TABLE oltp_src (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    amount NUMERIC NOT NULL
);

\echo 'Seeding 1M rows...'
INSERT INTO oltp_src (customer_id, category, amount)
SELECT
    1 + (i % 10000),
    (ARRAY['Electronics','Clothing','Food','Books','Sports','Home','Garden','Auto','Health','Toys'])[1 + (i % 10)],
    ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000000) AS i;

CREATE INDEX idx_oltp_customer ON oltp_src(customer_id);
ANALYZE oltp_src;

-- ============================================================
-- SCENARIO 1: 1 IMV, low cardinality (10 groups)
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  SCENARIO 1: 1 IMV, 10 groups (by category)'
\echo '================================================================'

SELECT create_reflex_ivm('oltp_by_cat',
    'SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM oltp_src GROUP BY category');

\echo 'IMV rows:'
SELECT COUNT(*) AS groups FROM oltp_by_cat;

-- Warm up
INSERT INTO oltp_src (customer_id, category, amount) VALUES (1, 'Electronics', 10.00);

-- Measure 100 individual inserts
\echo ''
\echo '--- 100 individual single-row INSERTs (1 IMV, 10 groups) ---'
\echo '[pg_reflex] 100 single-row inserts:'
DO $$
DECLARE
    _start TIMESTAMPTZ;
    _end TIMESTAMPTZ;
    _cats TEXT[] := ARRAY['Electronics','Clothing','Food','Books','Sports','Home','Garden','Auto','Health','Toys'];
BEGIN
    _start := clock_timestamp();
    FOR i IN 1..100 LOOP
        INSERT INTO oltp_src (customer_id, category, amount)
        VALUES (i, _cats[1 + (i % 10)], ROUND((random() * 100)::numeric, 2));
    END LOOP;
    _end := clock_timestamp();
    RAISE NOTICE '100 inserts in % ms (avg % ms/insert)',
        EXTRACT(MILLISECONDS FROM _end - _start)::integer,
        ROUND(EXTRACT(MILLISECONDS FROM _end - _start) / 100, 2);
END $$;

-- Baseline: measure raw INSERT cost on an identical table without triggers
CREATE TEMP TABLE oltp_baseline (LIKE oltp_src INCLUDING DEFAULTS);

\echo '[no trigger] 100 single-row inserts:'
DO $$
DECLARE
    _start TIMESTAMPTZ;
    _end TIMESTAMPTZ;
    _cats TEXT[] := ARRAY['Electronics','Clothing','Food','Books','Sports','Home','Garden','Auto','Health','Toys'];
BEGIN
    _start := clock_timestamp();
    FOR i IN 1..100 LOOP
        INSERT INTO oltp_baseline (customer_id, category, amount)
        VALUES (i, _cats[1 + (i % 10)], ROUND((random() * 100)::numeric, 2));
    END LOOP;
    _end := clock_timestamp();
    RAISE NOTICE '100 inserts in % ms (avg % ms/insert)',
        EXTRACT(MILLISECONDS FROM _end - _start)::integer,
        ROUND(EXTRACT(MILLISECONDS FROM _end - _start) / 100, 2);
END $$;

DROP TABLE oltp_baseline;

-- ============================================================
-- SCENARIO 2: 1 IMV, high cardinality (10K groups)
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  SCENARIO 2: 1 IMV, 10K groups (by customer_id)'
\echo '================================================================'

SELECT create_reflex_ivm('oltp_by_cust',
    'SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt FROM oltp_src GROUP BY customer_id');

\echo 'IMV rows:'
SELECT COUNT(*) AS groups FROM oltp_by_cust;

INSERT INTO oltp_src (customer_id, category, amount) VALUES (1, 'Food', 10.00);

\echo ''
\echo '--- 100 individual single-row INSERTs (2 IMVs: 10 groups + 10K groups) ---'
\echo '[pg_reflex] 100 single-row inserts (2 IMVs fire):'
DO $$
DECLARE
    _start TIMESTAMPTZ;
    _end TIMESTAMPTZ;
    _cats TEXT[] := ARRAY['Electronics','Clothing','Food','Books','Sports','Home','Garden','Auto','Health','Toys'];
BEGIN
    _start := clock_timestamp();
    FOR i IN 1..100 LOOP
        INSERT INTO oltp_src (customer_id, category, amount)
        VALUES (1 + (i % 10000), _cats[1 + (i % 10)], ROUND((random() * 100)::numeric, 2));
    END LOOP;
    _end := clock_timestamp();
    RAISE NOTICE '100 inserts in % ms (avg % ms/insert)',
        EXTRACT(MILLISECONDS FROM _end - _start)::integer,
        ROUND(EXTRACT(MILLISECONDS FROM _end - _start) / 100, 2);
END $$;

-- ============================================================
-- SCENARIO 3: 3 IMVs on same source
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  SCENARIO 3: 3 IMVs on same source'
\echo '================================================================'

SELECT create_reflex_ivm('oltp_by_cat_cust',
    'SELECT category, customer_id, SUM(amount) AS total FROM oltp_src GROUP BY category, customer_id');

\echo 'IMV rows (category x customer):'
SELECT COUNT(*) AS groups FROM oltp_by_cat_cust;

INSERT INTO oltp_src (customer_id, category, amount) VALUES (1, 'Food', 10.00);

\echo ''
\echo '--- 100 individual single-row INSERTs (3 IMVs fire) ---'
\echo '[pg_reflex] 100 single-row inserts (3 IMVs):'
DO $$
DECLARE
    _start TIMESTAMPTZ;
    _end TIMESTAMPTZ;
    _cats TEXT[] := ARRAY['Electronics','Clothing','Food','Books','Sports','Home','Garden','Auto','Health','Toys'];
BEGIN
    _start := clock_timestamp();
    FOR i IN 1..100 LOOP
        INSERT INTO oltp_src (customer_id, category, amount)
        VALUES (1 + (i % 10000), _cats[1 + (i % 10)], ROUND((random() * 100)::numeric, 2));
    END LOOP;
    _end := clock_timestamp();
    RAISE NOTICE '100 inserts in % ms (avg % ms/insert)',
        EXTRACT(MILLISECONDS FROM _end - _start)::integer,
        ROUND(EXTRACT(MILLISECONDS FROM _end - _start) / 100, 2);
END $$;

-- Correctness
\echo ''
\echo '--- Correctness ---'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS oltp_by_cat
FROM (SELECT r.category FROM oltp_by_cat r
      FULL OUTER JOIN (SELECT category, SUM(amount) AS total FROM oltp_src GROUP BY category) d
      ON r.category = d.category WHERE r.total IS DISTINCT FROM d.total) x;

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS oltp_by_cust
FROM (SELECT r.customer_id FROM oltp_by_cust r
      FULL OUTER JOIN (SELECT customer_id, SUM(amount) AS total FROM oltp_src GROUP BY customer_id) d
      ON r.customer_id = d.customer_id WHERE r.total IS DISTINCT FROM d.total) x;

-- ============================================================
-- CLEANUP
-- ============================================================
\echo ''
\echo '--- Cleanup ---'
SELECT drop_reflex_ivm('oltp_by_cat_cust');
SELECT drop_reflex_ivm('oltp_by_cust');
SELECT drop_reflex_ivm('oltp_by_cat');
DROP TABLE IF EXISTS oltp_src CASCADE;

\echo ''
\echo '================================================================'
\echo '  OLTP BENCHMARK COMPLETE'
\echo '  Compare [pg_reflex] vs [no trigger] avg ms/insert'
\echo '================================================================'
