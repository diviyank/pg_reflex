-- pg_reflex benchmark: MIXED OPERATIONS (INSERT → UPDATE → DELETE)
--
-- Tests realistic workload: sequential INSERT, UPDATE, DELETE operations.
-- Verifies correctness after each step.
-- Source: 1M rows, 100K groups, 1 IMV with covering index.

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  BENCHMARK: MIXED OPERATIONS (INSERT → UPDATE → DELETE)'
\echo '  Source: 1M rows, 100K groups, covering index'
\echo '================================================================'

-- ============================================================
-- SETUP
-- ============================================================
\echo ''
\echo '--- Setup ---'

DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;

DROP TABLE IF EXISTS mix_src CASCADE;
CREATE TABLE mix_src (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    amount NUMERIC NOT NULL
);

\echo 'Seeding 1M rows (100K groups)...'
INSERT INTO mix_src (account_id, amount)
SELECT 1 + (i % 100000), ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;

CREATE INDEX idx_mix_account ON mix_src(account_id);
ANALYZE mix_src;

SELECT create_reflex_ivm('mix_view',
    'SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM mix_src GROUP BY account_id');

CREATE INDEX idx_mix_view_cover ON mix_view (account_id) INCLUDE (total, cnt);
ANALYZE mix_view;

DROP MATERIALIZED VIEW IF EXISTS mix_matview;
CREATE MATERIALIZED VIEW mix_matview AS
    SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM mix_src GROUP BY account_id;
CREATE INDEX idx_mix_matview_cover ON mix_matview (account_id) INCLUDE (total, cnt);

\echo 'IMV groups:'
SELECT COUNT(*) AS groups FROM mix_view;

-- ============================================================
-- STEP 1: INSERT 10K rows
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  STEP 1: INSERT 10,000 rows'
\echo '================================================================'

\echo '[pg_reflex] INSERT 10K:'
INSERT INTO mix_src (account_id, amount)
SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 10000) AS i;

\echo '[baseline] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW mix_matview;

\echo '[correctness] after INSERT:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.account_id FROM mix_view r
      FULL OUTER JOIN (SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM mix_src GROUP BY account_id) d
      ON r.account_id = d.account_id
      WHERE r.total IS DISTINCT FROM d.total OR r.cnt IS DISTINCT FROM d.cnt) x;

-- ============================================================
-- STEP 2: UPDATE 5K rows (change amounts)
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  STEP 2: UPDATE 5,000 rows (change amounts, same groups)'
\echo '================================================================'

\echo '[pg_reflex] UPDATE 5K rows:'
UPDATE mix_src SET amount = amount + 100 WHERE id <= 5000;

\echo '[baseline] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW mix_matview;

\echo '[correctness] after UPDATE:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.account_id FROM mix_view r
      FULL OUTER JOIN (SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM mix_src GROUP BY account_id) d
      ON r.account_id = d.account_id
      WHERE r.total IS DISTINCT FROM d.total OR r.cnt IS DISTINCT FROM d.cnt) x;

-- ============================================================
-- STEP 3: DELETE 2K rows
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  STEP 3: DELETE 2,000 rows'
\echo '================================================================'

\echo '[pg_reflex] DELETE 2K rows:'
DELETE FROM mix_src WHERE id > 5000 AND id <= 7000;

\echo '[baseline] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW mix_matview;

\echo '[correctness] after DELETE:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.account_id FROM mix_view r
      FULL OUTER JOIN (SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM mix_src GROUP BY account_id) d
      ON r.account_id = d.account_id
      WHERE r.total IS DISTINCT FROM d.total OR r.cnt IS DISTINCT FROM d.cnt) x;

-- ============================================================
-- STEP 4: Large batch INSERT 50K
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  STEP 4: INSERT 50,000 rows (some new groups 100001+)'
\echo '================================================================'

\echo '[pg_reflex] INSERT 50K rows:'
INSERT INTO mix_src (account_id, amount)
SELECT
    CASE WHEN i <= 45000 THEN 1 + (i % 100000)
         ELSE 100000 + i - 44999 END,
    ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 50000) AS i;

\echo '[baseline] REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW mix_matview;

\echo '[correctness] after large INSERT:'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.account_id FROM mix_view r
      FULL OUTER JOIN (SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM mix_src GROUP BY account_id) d
      ON r.account_id = d.account_id
      WHERE r.total IS DISTINCT FROM d.total OR r.cnt IS DISTINCT FROM d.cnt) x;

\echo 'Group count:'
SELECT COUNT(*) AS imv_groups FROM mix_view;
SELECT COUNT(DISTINCT account_id) AS src_groups FROM mix_src;

-- ============================================================
-- CLEANUP
-- ============================================================
\echo ''
\echo '--- Cleanup ---'
SELECT drop_reflex_ivm('mix_view');
DROP MATERIALIZED VIEW IF EXISTS mix_matview;
DROP TABLE IF EXISTS mix_src CASCADE;

\echo ''
\echo '================================================================'
\echo '  MIXED OPERATIONS BENCHMARK COMPLETE'
\echo '================================================================'
