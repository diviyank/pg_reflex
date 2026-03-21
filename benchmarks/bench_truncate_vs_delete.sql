-- pg_reflex benchmark: TRUNCATE vs DELETE on 1M-row targets
--
-- Isolates the target refresh step (DELETE FROM target + INSERT INTO target)
-- and compares it against TRUNCATE + INSERT, with ~1M rows in the target.
--
-- Tests with and without covering indexes, 3 runs each.
-- VACUUM FULL between DELETE runs to avoid dead-tuple bias.
--
-- Self-contained: creates its own setup, does not require other benchmarks.

\timing on
\echo ''
\echo '================================================================'
\echo '  BENCHMARK: TRUNCATE vs DELETE on 1M-row targets'
\echo '  Isolates the target refresh step of the trigger pipeline'
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
    val1 NUMERIC NOT NULL,
    val2 NUMERIC NOT NULL
);

\echo 'Seeding 5M rows...'
INSERT INTO bench_source (key_a, val1, val2)
SELECT
    (i % 1000000),
    ROUND((random() * 1000)::numeric, 2),
    ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 5000000) AS i;

CREATE INDEX idx_bench_source_key ON bench_source (key_a);
ANALYZE bench_source;

\echo 'Creating IMV (1M target rows)...'
SELECT create_reflex_ivm('bench_trunc_test',
    'SELECT key_a, SUM(val1) AS total, COUNT(*) AS cnt FROM bench_source GROUP BY key_a');

\echo 'Target row count:'
SELECT COUNT(*) AS target_rows FROM bench_trunc_test;

-- Print the end_query for reference
\echo 'End query stored in reference table:'
SELECT end_query FROM public.__reflex_ivm_reference WHERE name = 'bench_trunc_test';

-- Disable all triggers — we are manually controlling the target refresh
ALTER TABLE bench_source DISABLE TRIGGER ALL;

-- Helper functions that read end_query from the reference table
CREATE OR REPLACE FUNCTION bench_delete_refresh(vname TEXT) RETURNS VOID AS $$
DECLARE eq TEXT;
BEGIN
    SELECT end_query INTO eq FROM public.__reflex_ivm_reference WHERE name = vname;
    EXECUTE format('DELETE FROM %I', vname);
    EXECUTE format('INSERT INTO %I %s', vname, eq);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bench_truncate_refresh(vname TEXT) RETURNS VOID AS $$
DECLARE eq TEXT;
BEGIN
    SELECT end_query INTO eq FROM public.__reflex_ivm_reference WHERE name = vname;
    EXECUTE format('TRUNCATE %I', vname);
    EXECUTE format('INSERT INTO %I %s', vname, eq);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bench_reinsert(vname TEXT) RETURNS VOID AS $$
DECLARE eq TEXT;
BEGIN
    SELECT end_query INTO eq FROM public.__reflex_ivm_reference WHERE name = vname;
    EXECUTE format('INSERT INTO %I %s', vname, eq);
END;
$$ LANGUAGE plpgsql;

\echo 'Setup complete.'
\echo ''

-- ============================================================
-- PHASE 1: WITHOUT COVERING INDEX
-- ============================================================
\echo '================================================================'
\echo '  PHASE 1: WITHOUT COVERING INDEX (1M rows in target)'
\echo '================================================================'

-- --- DELETE + re-INSERT (3 runs) ---
\echo ''
\echo '--- DELETE FROM + INSERT (no covering index) ---'

\echo 'Run 1: DELETE + INSERT:'
SELECT bench_delete_refresh('bench_trunc_test');
\echo 'Verify row count:'
SELECT COUNT(*) AS rows FROM bench_trunc_test;

VACUUM FULL bench_trunc_test;

\echo 'Run 2: DELETE + INSERT:'
SELECT bench_delete_refresh('bench_trunc_test');

VACUUM FULL bench_trunc_test;

\echo 'Run 3: DELETE + INSERT:'
SELECT bench_delete_refresh('bench_trunc_test');

VACUUM FULL bench_trunc_test;

-- --- TRUNCATE + re-INSERT (3 runs) ---
\echo ''
\echo '--- TRUNCATE + INSERT (no covering index) ---'

\echo 'Run 1: TRUNCATE + INSERT:'
SELECT bench_truncate_refresh('bench_trunc_test');
\echo 'Verify row count:'
SELECT COUNT(*) AS rows FROM bench_trunc_test;

\echo 'Run 2: TRUNCATE + INSERT:'
SELECT bench_truncate_refresh('bench_trunc_test');

\echo 'Run 3: TRUNCATE + INSERT:'
SELECT bench_truncate_refresh('bench_trunc_test');

-- --- Isolated: just DELETE or just TRUNCATE (no re-INSERT) ---
\echo ''
\echo '--- Isolated: DELETE-only vs TRUNCATE-only (no re-INSERT) ---'

\echo 'DELETE alone (1M rows):'
DELETE FROM bench_trunc_test;
\echo 'Re-INSERT:'
SELECT bench_reinsert('bench_trunc_test');

VACUUM FULL bench_trunc_test;

\echo 'DELETE alone (1M rows) run 2:'
DELETE FROM bench_trunc_test;
\echo 'Re-INSERT:'
SELECT bench_reinsert('bench_trunc_test');

VACUUM FULL bench_trunc_test;

\echo 'TRUNCATE alone (1M rows):'
TRUNCATE bench_trunc_test;
\echo 'Re-INSERT:'
SELECT bench_reinsert('bench_trunc_test');

\echo 'TRUNCATE alone (1M rows) run 2:'
TRUNCATE bench_trunc_test;
\echo 'Re-INSERT:'
SELECT bench_reinsert('bench_trunc_test');

-- ============================================================
-- PHASE 2: WITH COVERING INDEX
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  PHASE 2: WITH COVERING INDEX (1M rows in target)'
\echo '================================================================'

\echo 'Creating covering index...'
CREATE INDEX idx_trunc_test_cover ON bench_trunc_test (key_a) INCLUDE (total, cnt);
ANALYZE bench_trunc_test;

-- --- DELETE + re-INSERT with index (3 runs) ---
\echo ''
\echo '--- DELETE FROM + INSERT (WITH covering index) ---'

\echo 'Run 1: DELETE + INSERT:'
SELECT bench_delete_refresh('bench_trunc_test');
\echo 'Verify row count:'
SELECT COUNT(*) AS rows FROM bench_trunc_test;

VACUUM FULL bench_trunc_test;

\echo 'Run 2: DELETE + INSERT:'
SELECT bench_delete_refresh('bench_trunc_test');

VACUUM FULL bench_trunc_test;

\echo 'Run 3: DELETE + INSERT:'
SELECT bench_delete_refresh('bench_trunc_test');

VACUUM FULL bench_trunc_test;

-- --- TRUNCATE + re-INSERT with index (3 runs) ---
\echo ''
\echo '--- TRUNCATE + INSERT (WITH covering index) ---'

\echo 'Run 1: TRUNCATE + INSERT:'
SELECT bench_truncate_refresh('bench_trunc_test');
\echo 'Verify row count:'
SELECT COUNT(*) AS rows FROM bench_trunc_test;

\echo 'Run 2: TRUNCATE + INSERT:'
SELECT bench_truncate_refresh('bench_trunc_test');

\echo 'Run 3: TRUNCATE + INSERT:'
SELECT bench_truncate_refresh('bench_trunc_test');

-- --- Isolated with index ---
\echo ''
\echo '--- Isolated: DELETE-only vs TRUNCATE-only (WITH covering index) ---'

\echo 'DELETE alone (1M rows + index):'
DELETE FROM bench_trunc_test;
\echo 'Re-INSERT:'
SELECT bench_reinsert('bench_trunc_test');

VACUUM FULL bench_trunc_test;

\echo 'DELETE alone (1M rows + index) run 2:'
DELETE FROM bench_trunc_test;
\echo 'Re-INSERT:'
SELECT bench_reinsert('bench_trunc_test');

VACUUM FULL bench_trunc_test;

\echo 'TRUNCATE alone (1M rows + index):'
TRUNCATE bench_trunc_test;
\echo 'Re-INSERT:'
SELECT bench_reinsert('bench_trunc_test');

\echo 'TRUNCATE alone (1M rows + index) run 2:'
TRUNCATE bench_trunc_test;
\echo 'Re-INSERT:'
SELECT bench_reinsert('bench_trunc_test');

-- ============================================================
-- CORRECTNESS
-- ============================================================
\echo ''
\echo '--- Final correctness check ---'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (
    SELECT r.key_a FROM bench_trunc_test r
    FULL OUTER JOIN (SELECT key_a, SUM(val1) AS total FROM bench_source GROUP BY key_a) d
        ON r.key_a = d.key_a
    WHERE r.total IS DISTINCT FROM d.total
) diff;

-- ============================================================
-- CLEANUP
-- ============================================================
\echo ''
\echo '--- Cleanup ---'
DROP FUNCTION IF EXISTS bench_delete_refresh(TEXT);
DROP FUNCTION IF EXISTS bench_truncate_refresh(TEXT);
DROP FUNCTION IF EXISTS bench_reinsert(TEXT);
SELECT drop_reflex_ivm('bench_trunc_test');
DROP TABLE IF EXISTS bench_source CASCADE;

\echo ''
\echo '================================================================'
\echo '  TRUNCATE vs DELETE BENCHMARK COMPLETE'
\echo '================================================================'
\echo ''
\echo '  Expected result at 1M rows:'
\echo '  - DELETE: O(N) scan + dead tuples + index maintenance'
\echo '  - TRUNCATE: O(1) file reset, no dead tuples'
\echo '  - The difference should be dramatic with covering indexes'
\echo '================================================================'
