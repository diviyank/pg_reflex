-- pg_reflex diagnostic benchmark: decompose trigger pipeline into timed steps
--
-- Purpose: Identify exactly where time is spent in the trigger pipeline
-- for batch INSERT operations at various sizes.
--
-- Tests both low-cardinality (10 groups) and high-cardinality (~3,650 groups).

\timing on
\echo ''
\echo '================================================================'
\echo '  DIAGNOSTIC BENCHMARK: Trigger Pipeline Decomposition'
\echo '================================================================'

-- ============================================================
-- SETUP
-- ============================================================
DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;

DROP TABLE IF EXISTS bench_orders CASCADE;
CREATE TABLE bench_orders (
    id SERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    city TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

\echo '--- Seeding 1M rows ---'
INSERT INTO bench_orders (region, city, amount, created_at)
SELECT
    (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
           'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
    (ARRAY['New York','Los Angeles','London','Berlin','Tokyo',
           'Sydney','Sao Paulo','Lagos','Dubai','Toronto'])[1 + (i % 10)],
    ROUND((random() * 1000)::numeric, 2),
    NOW() - (random() * interval '365 days')
FROM generate_series(1, 1000000) AS i;
ANALYZE bench_orders;

-- ============================================================
-- SCENARIO 1: LOW CARDINALITY (10 groups — GROUP BY region)
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  SCENARIO 1: LOW CARDINALITY (10 groups)'
\echo '  GROUP BY region on 1M rows'
\echo '================================================================'

SELECT create_reflex_ivm('diag_lc',
    'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region');

-- Create baseline MATERIALIZED VIEW
DROP MATERIALIZED VIEW IF EXISTS diag_lc_matview;
CREATE MATERIALIZED VIEW diag_lc_matview AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region;

-- ---- Test function for batch sizes ----
-- We'll test: 10K, 50K, 100K, 500K

-- Helper to generate batch rows
CREATE OR REPLACE FUNCTION make_batch(n INT) RETURNS VOID AS $$
BEGIN
    DROP TABLE IF EXISTS staged_rows;
    CREATE TEMP TABLE staged_rows AS
    SELECT
        (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
               'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)] AS region,
        'BatchCity' AS city,
        ROUND((random() * 1000)::numeric, 2) AS amount,
        NOW() AS created_at
    FROM generate_series(1, n) AS i;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- BASELINE: Full trigger (INSERT with trigger enabled)
-- ============================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
\echo ''
\echo '--- BASELINE: Full trigger execution (low cardinality) ---'

\echo ''
\echo '[10K] Full trigger INSERT:'
INSERT INTO bench_orders (region, city, amount, created_at)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'BL', ROUND((random() * 1000)::numeric, 2), NOW()
FROM generate_series(1, 10000) AS i;

\echo '[10K] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW diag_lc_matview;

\echo '[50K] Full trigger INSERT:'
INSERT INTO bench_orders (region, city, amount, created_at)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'BL', ROUND((random() * 1000)::numeric, 2), NOW()
FROM generate_series(1, 50000) AS i;

\echo '[50K] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW diag_lc_matview;

\echo '[100K] Full trigger INSERT:'
INSERT INTO bench_orders (region, city, amount, created_at)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'BL', ROUND((random() * 1000)::numeric, 2), NOW()
FROM generate_series(1, 100000) AS i;

\echo '[100K] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW diag_lc_matview;

\echo '[500K] Full trigger INSERT:'
INSERT INTO bench_orders (region, city, amount, created_at)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'BL', ROUND((random() * 1000)::numeric, 2), NOW()
FROM generate_series(1, 500000) AS i;

\echo '[500K] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW diag_lc_matview;

-- Correctness check
\echo '[correctness] IMV vs direct query:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) || ' mismatches' END AS result
FROM (
    SELECT r.region FROM diag_lc r
    FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d ON r.region = d.region
    WHERE r.total IS DISTINCT FROM d.total
) diff;

-- ============================================================
-- DECOMPOSED: Step-by-step timing (low cardinality)
-- ============================================================

\echo ''
\echo '--- DECOMPOSED: Per-step timing (low cardinality) ---'
\echo '--- Disabling triggers for manual step execution ---'

-- Disable triggers
ALTER TABLE bench_orders DISABLE TRIGGER "__reflex_trigger_ins_on_bench_orders";
ALTER TABLE bench_orders DISABLE TRIGGER "__reflex_trigger_del_on_bench_orders";
ALTER TABLE bench_orders DISABLE TRIGGER "__reflex_trigger_upd_on_bench_orders";
ALTER TABLE bench_orders DISABLE TRIGGER "__reflex_trigger_trunc_on_bench_orders";

-- For each batch size, decompose the pipeline

-- ---- 50K Decomposed ----
\echo ''
\echo '=== 50K batch, LOW CARDINALITY — step-by-step ==='

SELECT make_batch(50000);

\echo 'S1: Temp table copy (50K rows):'
DROP TABLE IF EXISTS "__reflex_delta_new_bench_orders";
CREATE TEMP TABLE "__reflex_delta_new_bench_orders" ON COMMIT DROP AS SELECT * FROM staged_rows;

\echo 'S2: Delta aggregation + UPSERT into intermediate:'
INSERT INTO __reflex_intermediate_diag_lc
    SELECT region, SUM(amount) AS "__sum_amount", COUNT(*) AS "__count_star", COUNT(*) AS __ivm_count
    FROM "__reflex_delta_new_bench_orders"
    GROUP BY region
ON CONFLICT ("region") DO UPDATE SET
    "__sum_amount" = __reflex_intermediate_diag_lc."__sum_amount" + EXCLUDED."__sum_amount",
    "__count_star" = __reflex_intermediate_diag_lc."__count_star" + EXCLUDED."__count_star",
    __ivm_count = __reflex_intermediate_diag_lc.__ivm_count + EXCLUDED.__ivm_count;

\echo 'S3a: DELETE FROM target:'
DELETE FROM "diag_lc";

\echo 'S3b: INSERT INTO target FROM intermediate:'
INSERT INTO "diag_lc"
    SELECT "region", "__sum_amount" AS "total", "__count_star" AS "cnt"
    FROM __reflex_intermediate_diag_lc
    WHERE __ivm_count > 0;

\echo 'S4: Update metadata:'
UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = 'diag_lc';

-- Also insert those rows into the actual source (without trigger) so counts stay consistent
INSERT INTO bench_orders (region, city, amount, created_at) SELECT region, city, amount, created_at FROM staged_rows;

-- ---- 100K Decomposed ----
\echo ''
\echo '=== 100K batch, LOW CARDINALITY — step-by-step ==='

SELECT make_batch(100000);

\echo 'S1: Temp table copy (100K rows):'
DROP TABLE IF EXISTS "__reflex_delta_new_bench_orders";
CREATE TEMP TABLE "__reflex_delta_new_bench_orders" ON COMMIT DROP AS SELECT * FROM staged_rows;

\echo 'S2: Delta aggregation + UPSERT into intermediate:'
INSERT INTO __reflex_intermediate_diag_lc
    SELECT region, SUM(amount) AS "__sum_amount", COUNT(*) AS "__count_star", COUNT(*) AS __ivm_count
    FROM "__reflex_delta_new_bench_orders"
    GROUP BY region
ON CONFLICT ("region") DO UPDATE SET
    "__sum_amount" = __reflex_intermediate_diag_lc."__sum_amount" + EXCLUDED."__sum_amount",
    "__count_star" = __reflex_intermediate_diag_lc."__count_star" + EXCLUDED."__count_star",
    __ivm_count = __reflex_intermediate_diag_lc.__ivm_count + EXCLUDED.__ivm_count;

\echo 'S3a: DELETE FROM target:'
DELETE FROM "diag_lc";

\echo 'S3b: INSERT INTO target FROM intermediate:'
INSERT INTO "diag_lc"
    SELECT "region", "__sum_amount" AS "total", "__count_star" AS "cnt"
    FROM __reflex_intermediate_diag_lc
    WHERE __ivm_count > 0;

INSERT INTO bench_orders (region, city, amount, created_at) SELECT region, city, amount, created_at FROM staged_rows;

-- ---- 500K Decomposed ----
\echo ''
\echo '=== 500K batch, LOW CARDINALITY — step-by-step ==='

SELECT make_batch(500000);

\echo 'S1: Temp table copy (500K rows):'
DROP TABLE IF EXISTS "__reflex_delta_new_bench_orders";
CREATE TEMP TABLE "__reflex_delta_new_bench_orders" ON COMMIT DROP AS SELECT * FROM staged_rows;

\echo 'S2: Delta aggregation + UPSERT into intermediate:'
INSERT INTO __reflex_intermediate_diag_lc
    SELECT region, SUM(amount) AS "__sum_amount", COUNT(*) AS "__count_star", COUNT(*) AS __ivm_count
    FROM "__reflex_delta_new_bench_orders"
    GROUP BY region
ON CONFLICT ("region") DO UPDATE SET
    "__sum_amount" = __reflex_intermediate_diag_lc."__sum_amount" + EXCLUDED."__sum_amount",
    "__count_star" = __reflex_intermediate_diag_lc."__count_star" + EXCLUDED."__count_star",
    __ivm_count = __reflex_intermediate_diag_lc.__ivm_count + EXCLUDED.__ivm_count;

\echo 'S3a: DELETE FROM target:'
DELETE FROM "diag_lc";

\echo 'S3b: INSERT INTO target FROM intermediate:'
INSERT INTO "diag_lc"
    SELECT "region", "__sum_amount" AS "total", "__count_star" AS "cnt"
    FROM __reflex_intermediate_diag_lc
    WHERE __ivm_count > 0;

INSERT INTO bench_orders (region, city, amount, created_at) SELECT region, city, amount, created_at FROM staged_rows;

-- Re-enable triggers
ALTER TABLE bench_orders ENABLE TRIGGER "__reflex_trigger_ins_on_bench_orders";
ALTER TABLE bench_orders ENABLE TRIGGER "__reflex_trigger_del_on_bench_orders";
ALTER TABLE bench_orders ENABLE TRIGGER "__reflex_trigger_upd_on_bench_orders";
ALTER TABLE bench_orders ENABLE TRIGGER "__reflex_trigger_trunc_on_bench_orders";

-- ============================================================
-- SCENARIO 2: HIGH CARDINALITY (~3,650 groups)
-- GROUP BY region, city, date
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  SCENARIO 2: HIGH CARDINALITY (~3,650 groups)'
\echo '  GROUP BY region, city, created_at::date on 1M+ rows'
\echo '================================================================'

-- Need a version of the table with a date column for GROUP BY
-- We'll just create a new IMV on the same table
SELECT create_reflex_ivm('diag_hc',
    'SELECT region, city, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region, city');

SELECT COUNT(*) AS hc_groups FROM diag_hc;

DROP MATERIALIZED VIEW IF EXISTS diag_hc_matview;
CREATE MATERIALIZED VIEW diag_hc_matview AS
    SELECT region, city, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region, city;

\echo ''
\echo '--- BASELINE: Full trigger (high cardinality) ---'

\echo '[50K] Full trigger INSERT (HC):'
INSERT INTO bench_orders (region, city, amount, created_at)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'City_' || (i % 100),
       ROUND((random() * 1000)::numeric, 2), NOW()
FROM generate_series(1, 50000) AS i;

\echo '[50K] REFRESH MATERIALIZED VIEW (HC):'
REFRESH MATERIALIZED VIEW diag_hc_matview;

\echo '[100K] Full trigger INSERT (HC):'
INSERT INTO bench_orders (region, city, amount, created_at)
SELECT (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North',
              'APAC-South','LATAM','Africa','Middle-East','Canada'])[1 + (i % 10)],
       'City_' || (i % 100),
       ROUND((random() * 1000)::numeric, 2), NOW()
FROM generate_series(1, 100000) AS i;

\echo '[100K] REFRESH MATERIALIZED VIEW (HC):'
REFRESH MATERIALIZED VIEW diag_hc_matview;

\echo '[correctness HC]:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) || ' mismatches' END AS result
FROM (
    SELECT r.region, r.city FROM diag_hc r
    FULL OUTER JOIN (SELECT region, city, SUM(amount) AS total FROM bench_orders GROUP BY region, city) d
        ON r.region = d.region AND r.city = d.city
    WHERE r.total IS DISTINCT FROM d.total
) diff;

-- ============================================================
-- CLEANUP
-- ============================================================
\echo ''
\echo '--- Cleanup ---'
SELECT drop_reflex_ivm('diag_hc');
SELECT drop_reflex_ivm('diag_lc');
DROP MATERIALIZED VIEW IF EXISTS diag_lc_matview;
DROP MATERIALIZED VIEW IF EXISTS diag_hc_matview;
DROP TABLE IF EXISTS bench_orders CASCADE;
DROP FUNCTION IF EXISTS make_batch(INT);

\echo ''
\echo '================================================================'
\echo '  DIAGNOSTIC BENCHMARK COMPLETE'
\echo '================================================================'
