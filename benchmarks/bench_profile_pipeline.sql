-- pg_reflex pipeline profiling: WHERE DOES THE TIME GO?
--
-- Decomposes the trigger pipeline into individually timed steps.
-- Compares sum-of-steps vs full trigger to isolate framework overhead.
-- Tests at 1K, 10K, 50K batch sizes with 1 IMV, 100K groups, covering index.

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  PIPELINE PROFILING: Step-by-Step Timing'
\echo '  Source: 1M rows, 100K groups, 1 IMV, covering index'
\echo '================================================================'

-- ============================================================
-- SETUP
-- ============================================================
\echo ''
\echo '--- Setup ---'

DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;

DROP TABLE IF EXISTS prof_src CASCADE;
CREATE TABLE prof_src (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    amount NUMERIC NOT NULL
);

\echo 'Seeding 1M rows (100K groups)...'
INSERT INTO prof_src (account_id, amount)
SELECT 1 + (i % 100000), ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;

CREATE INDEX idx_prof_account ON prof_src(account_id);
ANALYZE prof_src;

SELECT create_reflex_ivm('prof_view',
    'SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM prof_src GROUP BY account_id');

CREATE INDEX idx_prof_view_cover ON prof_view (account_id) INCLUDE (total, cnt);
ANALYZE prof_view;

DROP MATERIALIZED VIEW IF EXISTS prof_matview;
CREATE MATERIALIZED VIEW prof_matview AS
    SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM prof_src GROUP BY account_id;

\echo 'Setup complete. IMV rows:'
SELECT COUNT(*) AS groups FROM prof_view;

-- Fetch the stored queries for manual replay
\echo ''
\echo '--- Stored queries ---'
SELECT
    'base_query: ' || base_query AS info FROM __reflex_ivm_reference WHERE name = 'prof_view'
UNION ALL
SELECT
    'end_query: ' || end_query FROM __reflex_ivm_reference WHERE name = 'prof_view';

-- ============================================================
-- PROFILING FUNCTION
-- ============================================================

-- Profiling function: runs each pipeline step individually, reports timing
CREATE OR REPLACE FUNCTION profile_insert_pipeline(batch_size INTEGER) RETURNS TABLE(
    step TEXT, duration_ms NUMERIC
) AS $$
DECLARE
    t0 TIMESTAMPTZ;
    t1 TIMESTAMPTZ;
    base_q TEXT;
    end_q TEXT;
    delta_q TEXT;
    interm TEXT := '__reflex_intermediate_prof_view';
    affected TEXT := '__reflex_affected_prof_view';
BEGIN
    -- Get stored queries
    SELECT base_query, end_query INTO base_q, end_q
    FROM __reflex_ivm_reference WHERE name = 'prof_view';

    -- Build delta query (replace source table with staged_batch)
    delta_q := replace(base_q, 'prof_src', 'staged_batch');

    -- Stage the batch rows
    t0 := clock_timestamp();
    EXECUTE format('DROP TABLE IF EXISTS staged_batch');
    EXECUTE format(
        'CREATE TEMP TABLE staged_batch AS
         SELECT (1 + (i %% 100000))::integer AS account_id, ROUND((random() * 500)::numeric, 2) AS amount
         FROM generate_series(1, %s) AS i', batch_size);
    t1 := clock_timestamp();
    step := 'S0: Stage batch'; duration_ms := EXTRACT(MILLISECONDS FROM t1 - t0); RETURN NEXT;

    -- S2: Create affected groups temp table
    t0 := clock_timestamp();
    EXECUTE format('DROP TABLE IF EXISTS "%s"', affected);
    EXECUTE format('CREATE TEMP TABLE "%s" AS SELECT DISTINCT "account_id" FROM (%s) _d', affected, delta_q);
    t1 := clock_timestamp();
    step := 'S2: Affected groups (delta agg #1)'; duration_ms := EXTRACT(MILLISECONDS FROM t1 - t0); RETURN NEXT;

    -- S3: UPSERT into intermediate
    t0 := clock_timestamp();
    EXECUTE format(
        'INSERT INTO %s %s ON CONFLICT ("account_id") DO UPDATE SET
         "__sum_amount" = %s."__sum_amount" + EXCLUDED."__sum_amount",
         "__count_star" = %s."__count_star" + EXCLUDED."__count_star",
         __ivm_count = %s.__ivm_count + EXCLUDED.__ivm_count',
        interm, delta_q, interm, interm, interm);
    t1 := clock_timestamp();
    step := 'S3: UPSERT intermediate (delta agg #2)'; duration_ms := EXTRACT(MILLISECONDS FROM t1 - t0); RETURN NEXT;

    -- S4: Targeted DELETE from target
    t0 := clock_timestamp();
    EXECUTE format(
        'DELETE FROM "prof_view" WHERE "account_id" IN (SELECT "account_id" FROM "%s")', affected);
    t1 := clock_timestamp();
    step := 'S4: DELETE affected from target'; duration_ms := EXTRACT(MILLISECONDS FROM t1 - t0); RETURN NEXT;

    -- S5: Targeted INSERT into target
    t0 := clock_timestamp();
    EXECUTE format(
        'INSERT INTO "prof_view" %s AND "account_id" IN (SELECT "account_id" FROM "%s")', end_q, affected);
    t1 := clock_timestamp();
    step := 'S5: INSERT affected into target'; duration_ms := EXTRACT(MILLISECONDS FROM t1 - t0); RETURN NEXT;

    -- S6: Cleanup
    t0 := clock_timestamp();
    EXECUTE format('DROP TABLE IF EXISTS "%s"', affected);
    t1 := clock_timestamp();
    step := 'S6: DROP affected temp'; duration_ms := EXTRACT(MILLISECONDS FROM t1 - t0); RETURN NEXT;

    -- S7: Metadata update
    t0 := clock_timestamp();
    UPDATE __reflex_ivm_reference SET last_update_date = NOW() WHERE name = 'prof_view';
    t1 := clock_timestamp();
    step := 'S7: UPDATE metadata'; duration_ms := EXTRACT(MILLISECONDS FROM t1 - t0); RETURN NEXT;

    -- Also insert the batch into the real source table (without trigger) so data stays consistent
    EXECUTE format('INSERT INTO prof_src (account_id, amount) SELECT account_id, amount FROM staged_batch');
    DROP TABLE IF EXISTS staged_batch;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- WARM-UP
-- ============================================================
\echo ''
\echo '--- Warm-up ---'
INSERT INTO prof_src (account_id, amount) VALUES (1, 1.00);

-- ============================================================
-- PROFILE: 1K batch
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  PROFILE: 1,000 row INSERT'
\echo '================================================================'

ALTER TABLE prof_src DISABLE TRIGGER ALL;

\echo '--- Step-by-step (triggers disabled) ---'
SELECT * FROM profile_insert_pipeline(1000);

\echo '--- Full trigger (triggers enabled) ---'
ALTER TABLE prof_src ENABLE TRIGGER ALL;

\echo 'Full trigger INSERT 1K:'
INSERT INTO prof_src (account_id, amount)
SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 1000) AS i;

\echo 'REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW prof_matview;

-- ============================================================
-- PROFILE: 10K batch
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  PROFILE: 10,000 row INSERT'
\echo '================================================================'

ALTER TABLE prof_src DISABLE TRIGGER ALL;

\echo '--- Step-by-step (triggers disabled) ---'
SELECT * FROM profile_insert_pipeline(10000);

\echo '--- Full trigger (triggers enabled) ---'
ALTER TABLE prof_src ENABLE TRIGGER ALL;

\echo 'Full trigger INSERT 10K:'
INSERT INTO prof_src (account_id, amount)
SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 10000) AS i;

\echo 'REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW prof_matview;

-- ============================================================
-- PROFILE: 50K batch
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  PROFILE: 50,000 row INSERT'
\echo '================================================================'

ALTER TABLE prof_src DISABLE TRIGGER ALL;

\echo '--- Step-by-step (triggers disabled) ---'
SELECT * FROM profile_insert_pipeline(50000);

\echo '--- Full trigger (triggers enabled) ---'
ALTER TABLE prof_src ENABLE TRIGGER ALL;

\echo 'Full trigger INSERT 50K:'
INSERT INTO prof_src (account_id, amount)
SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 50000) AS i;

\echo 'REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW prof_matview;

-- ============================================================
-- PROFILE: 100K batch
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  PROFILE: 100,000 row INSERT'
\echo '================================================================'

ALTER TABLE prof_src DISABLE TRIGGER ALL;

\echo '--- Step-by-step (triggers disabled) ---'
SELECT * FROM profile_insert_pipeline(100000);

\echo '--- Full trigger (triggers enabled) ---'
ALTER TABLE prof_src ENABLE TRIGGER ALL;

\echo 'Full trigger INSERT 100K:'
INSERT INTO prof_src (account_id, amount)
SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2)
FROM generate_series(1, 100000) AS i;

\echo 'REFRESH MATERIALIZED VIEW:'
REFRESH MATERIALIZED VIEW prof_matview;

-- ============================================================
-- CORRECTNESS
-- ============================================================
\echo ''
\echo '--- Correctness ---'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.account_id FROM prof_view r
      FULL OUTER JOIN (SELECT account_id, SUM(amount) AS total FROM prof_src GROUP BY account_id) d
      ON r.account_id = d.account_id WHERE r.total IS DISTINCT FROM d.total) x;

-- ============================================================
-- CLEANUP
-- ============================================================
\echo ''
DROP FUNCTION IF EXISTS profile_insert_pipeline(INTEGER);
SELECT drop_reflex_ivm('prof_view');
DROP MATERIALIZED VIEW IF EXISTS prof_matview;
DROP TABLE IF EXISTS prof_src CASCADE;

\echo ''
\echo '================================================================'
\echo '  PROFILING COMPLETE'
\echo '  Compare: sum of S2-S7 vs Full trigger → difference = framework overhead'
\echo '  Compare: S2 vs S3 → delta query duplication cost'
\echo '  Compare: S4+S5 → targeted refresh cost (index maintenance)'
\echo '================================================================'
