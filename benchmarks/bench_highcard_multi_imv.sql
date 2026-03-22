-- pg_reflex benchmark: HIGH-CARDINALITY MULTI-IMV (4 IMVs vs 4 MATVIEWs)
--
-- Tests the realistic production scenario:
--   - Single source table with 5M rows
--   - 4 IMVs and 4 MATVIEWs, each producing ~1M target rows
--   - Covering indexes on all targets
--   - Batch INSERT/DELETE at 10K, 50K, 100K
--
-- Fair comparison: pg_reflex (INSERT + trigger overhead for 4 IMVs)
--                  vs traditional (INSERT + 4x REFRESH MATERIALIZED VIEW)

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  BENCHMARK: HIGH-CARDINALITY MULTI-IMV'
\echo '  Source: 5M rows, 4 IMVs + 4 MATVIEWs (~1M target rows each)'
\echo '  Covering indexes on all targets'
\echo '================================================================'

-- ============================================================
-- SETUP
-- ============================================================
\echo ''
\echo '--- Setup ---'

DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;

DROP TABLE IF EXISTS bench_source CASCADE;
CREATE TABLE bench_source (
    id SERIAL PRIMARY KEY,
    key_a INTEGER NOT NULL,
    key_b INTEGER NOT NULL,
    key_c INTEGER NOT NULL,
    key_d INTEGER NOT NULL,
    val1 NUMERIC NOT NULL,
    val2 NUMERIC NOT NULL
);

\echo 'Seeding 5M rows (this takes ~30-60s)...'
INSERT INTO bench_source (key_a, key_b, key_c, key_d, val1, val2)
SELECT
    (i % 1000000),                      -- key_a: 0..999999 (1M distinct)
    ((i * 7 + 13) % 1000000),           -- key_b: permuted, 1M distinct
    ((i * 31 + 97) % 1000000),          -- key_c: permuted, 1M distinct
    ((i * 127 + 251) % 1000000),        -- key_d: permuted, 1M distinct
    ROUND((random() * 1000)::numeric, 2),
    ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 5000000) AS i;

CREATE INDEX idx_bench_source_a ON bench_source (key_a);
CREATE INDEX idx_bench_source_b ON bench_source (key_b);
CREATE INDEX idx_bench_source_c ON bench_source (key_c);
CREATE INDEX idx_bench_source_d ON bench_source (key_d);
ANALYZE bench_source;

\echo 'Source row count:'
SELECT COUNT(*) AS source_rows FROM bench_source;

-- ============================================================
-- CREATE 4 IMVs
-- ============================================================
\echo ''
\echo '--- Creating 4 pg_reflex IMVs ---'

\echo 'IMV A: GROUP BY key_a, SUM(val1), COUNT(*)'
SELECT create_reflex_ivm('bench_hc_a',
    'SELECT key_a, SUM(val1) AS total, COUNT(*) AS cnt FROM bench_source GROUP BY key_a');

\echo 'IMV B: GROUP BY key_b, SUM(val2), COUNT(*)'
SELECT create_reflex_ivm('bench_hc_b',
    'SELECT key_b, SUM(val2) AS total, COUNT(*) AS cnt FROM bench_source GROUP BY key_b');

\echo 'IMV C: GROUP BY key_c, SUM(val1), SUM(val2), COUNT(*)'
SELECT create_reflex_ivm('bench_hc_c',
    'SELECT key_c, SUM(val1) AS sum1, SUM(val2) AS sum2, COUNT(*) AS cnt FROM bench_source GROUP BY key_c');

\echo 'IMV D: GROUP BY key_d, SUM(val1), AVG(val2), COUNT(*)'
SELECT create_reflex_ivm('bench_hc_d',
    'SELECT key_d, SUM(val1) AS sum1, AVG(val2) AS avg2, COUNT(*) AS cnt FROM bench_source GROUP BY key_d');

\echo 'IMV row counts:'
SELECT 'bench_hc_a' AS imv, COUNT(*) AS rows FROM bench_hc_a
UNION ALL SELECT 'bench_hc_b', COUNT(*) FROM bench_hc_b
UNION ALL SELECT 'bench_hc_c', COUNT(*) FROM bench_hc_c
UNION ALL SELECT 'bench_hc_d', COUNT(*) FROM bench_hc_d;

-- ============================================================
-- CREATE 4 MATVIEWs (matching queries)
-- ============================================================
\echo ''
\echo '--- Creating 4 MATERIALIZED VIEWs ---'

\echo 'MATVIEW A:'
CREATE MATERIALIZED VIEW bench_hc_mat_a AS
    SELECT key_a, SUM(val1) AS total, COUNT(*) AS cnt FROM bench_source GROUP BY key_a;

\echo 'MATVIEW B:'
CREATE MATERIALIZED VIEW bench_hc_mat_b AS
    SELECT key_b, SUM(val2) AS total, COUNT(*) AS cnt FROM bench_source GROUP BY key_b;

\echo 'MATVIEW C:'
CREATE MATERIALIZED VIEW bench_hc_mat_c AS
    SELECT key_c, SUM(val1) AS sum1, SUM(val2) AS sum2, COUNT(*) AS cnt FROM bench_source GROUP BY key_c;

\echo 'MATVIEW D:'
CREATE MATERIALIZED VIEW bench_hc_mat_d AS
    SELECT key_d, SUM(val1) AS sum1, AVG(val2) AS avg2, COUNT(*) AS cnt FROM bench_source GROUP BY key_d;

-- ============================================================
-- CREATE COVERING INDEXES on all targets
-- ============================================================
\echo ''
\echo '--- Creating covering indexes ---'

-- IMV covering indexes
CREATE INDEX idx_hc_a_cover ON bench_hc_a (key_a) INCLUDE (total, cnt);
CREATE INDEX idx_hc_b_cover ON bench_hc_b (key_b) INCLUDE (total, cnt);
CREATE INDEX idx_hc_c_cover ON bench_hc_c (key_c) INCLUDE (sum1, sum2, cnt);
CREATE INDEX idx_hc_d_cover ON bench_hc_d (key_d) INCLUDE (sum1, avg2, cnt);

-- MATVIEW covering indexes
CREATE INDEX idx_hc_mat_a_cover ON bench_hc_mat_a (key_a) INCLUDE (total, cnt);
CREATE INDEX idx_hc_mat_b_cover ON bench_hc_mat_b (key_b) INCLUDE (total, cnt);
CREATE INDEX idx_hc_mat_c_cover ON bench_hc_mat_c (key_c) INCLUDE (sum1, sum2, cnt);
CREATE INDEX idx_hc_mat_d_cover ON bench_hc_mat_d (key_d) INCLUDE (sum1, avg2, cnt);

ANALYZE bench_hc_a; ANALYZE bench_hc_b; ANALYZE bench_hc_c; ANALYZE bench_hc_d;
ANALYZE bench_hc_mat_a; ANALYZE bench_hc_mat_b; ANALYZE bench_hc_mat_c; ANALYZE bench_hc_mat_d;

\echo 'Setup complete.'

-- ============================================================
-- WARM-UP (1K rows — primes buffer cache)
-- ============================================================
\echo ''
\echo '--- Warm-up: 1K INSERT ---'
INSERT INTO bench_source (key_a, key_b, key_c, key_d, val1, val2)
SELECT (i % 1000000), ((i*7+13) % 1000000), ((i*31+97) % 1000000), ((i*127+251) % 1000000),
       ROUND((random() * 1000)::numeric, 2), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;
REFRESH MATERIALIZED VIEW bench_hc_mat_a;
REFRESH MATERIALIZED VIEW bench_hc_mat_b;
REFRESH MATERIALIZED VIEW bench_hc_mat_c;
REFRESH MATERIALIZED VIEW bench_hc_mat_d;

-- ============================================================
-- BATCH INSERT BENCHMARK
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  BATCH INSERT — pg_reflex (4 IMVs) vs 4x REFRESH'
\echo '================================================================'

-- --- 10K INSERT ---
\echo ''
\echo '--- Batch INSERT: 10,000 rows ---'

\echo '[pg_reflex]  INSERT 10K rows (triggers maintain 4 IMVs):'
INSERT INTO bench_source (key_a, key_b, key_c, key_d, val1, val2)
SELECT (i % 1000000), ((i*7+13) % 1000000), ((i*31+97) % 1000000), ((i*127+251) % 1000000),
       ROUND((random() * 1000)::numeric, 2), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 10000) AS i;

\echo '[baseline]   4x REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_hc_mat_a;
REFRESH MATERIALIZED VIEW bench_hc_mat_b;
REFRESH MATERIALIZED VIEW bench_hc_mat_c;
REFRESH MATERIALIZED VIEW bench_hc_mat_d;

\echo '[correctness] IMV A:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.key_a FROM bench_hc_a r FULL OUTER JOIN
      (SELECT key_a, SUM(val1) AS total FROM bench_source GROUP BY key_a) d
      ON r.key_a = d.key_a WHERE r.total IS DISTINCT FROM d.total) diff;

\echo '[correctness] IMV D (AVG):'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.key_d FROM bench_hc_d r FULL OUTER JOIN
      (SELECT key_d, SUM(val1) AS sum1, AVG(val2) AS avg2 FROM bench_source GROUP BY key_d) d
      ON r.key_d = d.key_d WHERE r.sum1 IS DISTINCT FROM d.sum1 OR r.avg2 IS DISTINCT FROM d.avg2) diff;

-- --- 50K INSERT ---
\echo ''
\echo '--- Batch INSERT: 50,000 rows ---'

\echo '[pg_reflex]  INSERT 50K rows (triggers maintain 4 IMVs):'
INSERT INTO bench_source (key_a, key_b, key_c, key_d, val1, val2)
SELECT (i % 1000000), ((i*7+13) % 1000000), ((i*31+97) % 1000000), ((i*127+251) % 1000000),
       ROUND((random() * 1000)::numeric, 2), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 50000) AS i;

\echo '[baseline]   4x REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_hc_mat_a;
REFRESH MATERIALIZED VIEW bench_hc_mat_b;
REFRESH MATERIALIZED VIEW bench_hc_mat_c;
REFRESH MATERIALIZED VIEW bench_hc_mat_d;

\echo '[correctness] IMV A:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.key_a FROM bench_hc_a r FULL OUTER JOIN
      (SELECT key_a, SUM(val1) AS total FROM bench_source GROUP BY key_a) d
      ON r.key_a = d.key_a WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 100K INSERT ---
\echo ''
\echo '--- Batch INSERT: 100,000 rows ---'

\echo '[pg_reflex]  INSERT 100K rows (triggers maintain 4 IMVs):'
INSERT INTO bench_source (key_a, key_b, key_c, key_d, val1, val2)
SELECT (i % 1000000), ((i*7+13) % 1000000), ((i*31+97) % 1000000), ((i*127+251) % 1000000),
       ROUND((random() * 1000)::numeric, 2), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 100000) AS i;

\echo '[baseline]   4x REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_hc_mat_a;
REFRESH MATERIALIZED VIEW bench_hc_mat_b;
REFRESH MATERIALIZED VIEW bench_hc_mat_c;
REFRESH MATERIALIZED VIEW bench_hc_mat_d;

\echo '[correctness] IMV A:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.key_a FROM bench_hc_a r FULL OUTER JOIN
      (SELECT key_a, SUM(val1) AS total FROM bench_source GROUP BY key_a) d
      ON r.key_a = d.key_a WHERE r.total IS DISTINCT FROM d.total) diff;

-- ============================================================
-- BARE INSERT MEASUREMENT (triggers disabled)
-- This isolates the raw INSERT cost so we can compute:
--   pg_reflex_total vs (bare_insert + 4x_refresh)
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  BARE INSERT (triggers disabled) — isolate INSERT cost'
\echo '================================================================'

ALTER TABLE bench_source DISABLE TRIGGER ALL;

\echo '[bare INSERT] 10K rows (no triggers):'
INSERT INTO bench_source (key_a, key_b, key_c, key_d, val1, val2)
SELECT (i % 1000000), ((i*7+13) % 1000000), ((i*31+97) % 1000000), ((i*127+251) % 1000000),
       ROUND((random() * 1000)::numeric, 2), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 10000) AS i;

\echo '[bare INSERT] 50K rows (no triggers):'
INSERT INTO bench_source (key_a, key_b, key_c, key_d, val1, val2)
SELECT (i % 1000000), ((i*7+13) % 1000000), ((i*31+97) % 1000000), ((i*127+251) % 1000000),
       ROUND((random() * 1000)::numeric, 2), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 50000) AS i;

\echo '[bare INSERT] 100K rows (no triggers):'
INSERT INTO bench_source (key_a, key_b, key_c, key_d, val1, val2)
SELECT (i % 1000000), ((i*7+13) % 1000000), ((i*31+97) % 1000000), ((i*127+251) % 1000000),
       ROUND((random() * 1000)::numeric, 2), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 100000) AS i;

ALTER TABLE bench_source ENABLE TRIGGER ALL;

-- Reconcile IMVs after the bare inserts (triggers were disabled)
SELECT reflex_reconcile('bench_hc_a');
SELECT reflex_reconcile('bench_hc_b');
SELECT reflex_reconcile('bench_hc_c');
SELECT reflex_reconcile('bench_hc_d');

-- ============================================================
-- BATCH DELETE BENCHMARK
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  BATCH DELETE — pg_reflex (4 IMVs) vs 4x REFRESH'
\echo '================================================================'

-- --- 10K DELETE ---
\echo ''
\echo '--- Batch DELETE: 10,000 rows ---'

\echo '[pg_reflex]  DELETE 10K rows (triggers maintain 4 IMVs):'
DELETE FROM bench_source WHERE id <= 10000;

\echo '[baseline]   4x REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_hc_mat_a;
REFRESH MATERIALIZED VIEW bench_hc_mat_b;
REFRESH MATERIALIZED VIEW bench_hc_mat_c;
REFRESH MATERIALIZED VIEW bench_hc_mat_d;

\echo '[correctness] IMV A:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.key_a FROM bench_hc_a r FULL OUTER JOIN
      (SELECT key_a, SUM(val1) AS total FROM bench_source GROUP BY key_a) d
      ON r.key_a = d.key_a WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 50K DELETE ---
\echo ''
\echo '--- Batch DELETE: 50,000 rows ---'

\echo '[pg_reflex]  DELETE 50K rows (triggers maintain 4 IMVs):'
DELETE FROM bench_source WHERE id > 10000 AND id <= 60000;

\echo '[baseline]   4x REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_hc_mat_a;
REFRESH MATERIALIZED VIEW bench_hc_mat_b;
REFRESH MATERIALIZED VIEW bench_hc_mat_c;
REFRESH MATERIALIZED VIEW bench_hc_mat_d;

\echo '[correctness] IMV A:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.key_a FROM bench_hc_a r FULL OUTER JOIN
      (SELECT key_a, SUM(val1) AS total FROM bench_source GROUP BY key_a) d
      ON r.key_a = d.key_a WHERE r.total IS DISTINCT FROM d.total) diff;

-- --- 100K DELETE ---
\echo ''
\echo '--- Batch DELETE: 100,000 rows ---'

\echo '[pg_reflex]  DELETE 100K rows (triggers maintain 4 IMVs):'
DELETE FROM bench_source WHERE id > 60000 AND id <= 160000;

\echo '[baseline]   4x REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW bench_hc_mat_a;
REFRESH MATERIALIZED VIEW bench_hc_mat_b;
REFRESH MATERIALIZED VIEW bench_hc_mat_c;
REFRESH MATERIALIZED VIEW bench_hc_mat_d;

\echo '[correctness] IMV A:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.key_a FROM bench_hc_a r FULL OUTER JOIN
      (SELECT key_a, SUM(val1) AS total FROM bench_source GROUP BY key_a) d
      ON r.key_a = d.key_a WHERE r.total IS DISTINCT FROM d.total) diff;

-- ============================================================
-- SUMMARY TABLE
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  HOW TO READ RESULTS'
\echo '================================================================'
\echo ''
\echo '  pg_reflex total  = [pg_reflex] INSERT time (includes trigger overhead for 4 IMVs)'
\echo '  baseline total   = [bare INSERT] time + sum of 4x [REFRESH] times'
\echo ''
\echo '  pg_reflex advantage = baseline_total - pg_reflex_total'
\echo '  (positive = pg_reflex is faster)'
\echo ''

-- ============================================================
-- CLEANUP
-- ============================================================
\echo ''
\echo '--- Cleanup ---'
SELECT drop_reflex_ivm('bench_hc_d');
SELECT drop_reflex_ivm('bench_hc_c');
SELECT drop_reflex_ivm('bench_hc_b');
SELECT drop_reflex_ivm('bench_hc_a');
DROP MATERIALIZED VIEW IF EXISTS bench_hc_mat_a;
DROP MATERIALIZED VIEW IF EXISTS bench_hc_mat_b;
DROP MATERIALIZED VIEW IF EXISTS bench_hc_mat_c;
DROP MATERIALIZED VIEW IF EXISTS bench_hc_mat_d;
DROP TABLE IF EXISTS bench_source CASCADE;

\echo ''
\echo '================================================================'
\echo '  HIGH-CARDINALITY MULTI-IMV BENCHMARK COMPLETE'
\echo '================================================================'
