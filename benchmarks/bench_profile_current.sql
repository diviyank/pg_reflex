-- pg_reflex pipeline profiling: CURRENT (MERGE-based) implementation
-- Decomposes the trigger pipeline into individually timed steps.
-- Compares sum-of-steps vs full trigger to isolate framework overhead.
-- Tests at 1K, 10K, 50K, 100K batch sizes with 1 IMV, 100K groups.

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  PIPELINE PROFILING: CURRENT (MERGE-based)'
\echo '  Source: 1M rows, 100K groups, 1 IMV'
\echo '================================================================'

-- ============================================================
-- SETUP
-- ============================================================
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
ANALYZE prof_src;

SELECT create_reflex_ivm('prof_view',
    'SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM prof_src GROUP BY account_id');

\echo 'IMV rows:'
SELECT COUNT(*) AS groups FROM prof_view;

DROP MATERIALIZED VIEW IF EXISTS prof_matview;
CREATE MATERIALIZED VIEW prof_matview AS
    SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM prof_src GROUP BY account_id;

-- ============================================================
-- PROFILING FUNCTION (using MERGE, matching current trigger code)
-- ============================================================
CREATE OR REPLACE FUNCTION profile_pipeline_current(batch_size INTEGER) RETURNS TABLE(
    step TEXT, duration_ms NUMERIC
) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT; agg_json TEXT;
    delta_q TEXT;
    interm TEXT := '__reflex_intermediate_prof_view';
    affected TEXT := '__reflex_affected_prof_view';
    built_sql TEXT;
    stmt TEXT;
BEGIN
    -- S0: Stage batch (simulates transition table)
    t0 := clock_timestamp();
    EXECUTE 'DROP TABLE IF EXISTS staged_batch';
    EXECUTE format(
        'CREATE TEMP TABLE staged_batch AS
         SELECT (1 + (i %% 100000))::integer AS account_id,
                ROUND((random() * 500)::numeric, 2) AS amount
         FROM generate_series(1, %s) AS i', batch_size);
    t1 := clock_timestamp();
    step := 'S0: Stage batch'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S1: Query reference table (framework overhead)
    t0 := clock_timestamp();
    SELECT r.base_query, r.end_query, r.aggregations::text
    INTO base_q, end_q, agg_json
    FROM __reflex_ivm_reference r WHERE r.name = 'prof_view';
    t1 := clock_timestamp();
    step := 'S1: Query reference table'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S2: Build delta SQL via Rust FFI (framework overhead)
    t0 := clock_timestamp();
    -- Replace source with staged_batch in the Rust-generated SQL
    built_sql := reflex_build_delta_sql('prof_view', 'prof_src', 'INSERT',
        replace(base_q, 'prof_src', 'staged_batch'),
        end_q,
        agg_json);
    t1 := clock_timestamp();
    step := 'S2: reflex_build_delta_sql (Rust FFI)'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- Now build the actual SQL we'd execute (matching trigger behavior)
    delta_q := replace(base_q, 'prof_src', 'staged_batch');

    -- S3: Affected groups temp table
    t0 := clock_timestamp();
    EXECUTE format('DROP TABLE IF EXISTS "%s"', affected);
    EXECUTE format('CREATE TEMP TABLE "%s" AS SELECT DISTINCT "account_id" FROM (%s) _d', affected, delta_q);
    t1 := clock_timestamp();
    step := 'S3: Affected groups (CREATE TEMP)'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S4: MERGE into intermediate (this is the actual current implementation)
    t0 := clock_timestamp();
    EXECUTE format(
        'MERGE INTO %s AS t USING (%s) AS d ON t."account_id" = d."account_id" '
        'WHEN MATCHED THEN UPDATE SET '
        '"__sum_amount" = t."__sum_amount" + d."__sum_amount", '
        '"__count_star" = t."__count_star" + d."__count_star", '
        '__ivm_count = t.__ivm_count + d.__ivm_count '
        'WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count) '
        'VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count)',
        interm, delta_q);
    t1 := clock_timestamp();
    step := 'S4: MERGE intermediate'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S5: Targeted DELETE from target
    t0 := clock_timestamp();
    EXECUTE format(
        'DELETE FROM "prof_view" WHERE "account_id" IN (SELECT "account_id" FROM "%s")', affected);
    t1 := clock_timestamp();
    step := 'S5: DELETE affected from target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S6: Targeted INSERT into target from intermediate
    t0 := clock_timestamp();
    EXECUTE format(
        'INSERT INTO "prof_view" %s AND "account_id" IN (SELECT "account_id" FROM "%s")', end_q, affected);
    t1 := clock_timestamp();
    step := 'S6: INSERT affected into target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S7: Cleanup + metadata
    t0 := clock_timestamp();
    EXECUTE format('DROP TABLE IF EXISTS "%s"', affected);
    UPDATE __reflex_ivm_reference SET last_update_date = NOW() WHERE name = 'prof_view';
    t1 := clock_timestamp();
    step := 'S7: Cleanup + metadata'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- Insert batch into real source (without trigger) so data stays consistent
    EXECUTE 'INSERT INTO prof_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    EXECUTE 'DROP TABLE IF EXISTS staged_batch';
END $$ LANGUAGE plpgsql;

-- ============================================================
-- WARM-UP
-- ============================================================
\echo ''
\echo '--- Warm-up ---'
ALTER TABLE prof_src DISABLE TRIGGER ALL;
SELECT * FROM profile_pipeline_current(100);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
INSERT INTO prof_src (account_id, amount) VALUES (1, 1.00);

-- ============================================================
-- PROFILE: 1K batch
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  PROFILE: 1,000 row INSERT'
\echo '================================================================'
ALTER TABLE prof_src DISABLE TRIGGER ALL;
\echo '--- Step-by-step ---'
SELECT * FROM profile_pipeline_current(1000);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
\echo 'Full trigger 1K:'
INSERT INTO prof_src (account_id, amount) SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2) FROM generate_series(1, 1000) AS i;
\echo 'REFRESH MATVIEW:'
REFRESH MATERIALIZED VIEW prof_matview;

-- ============================================================
-- PROFILE: 10K batch
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  PROFILE: 10,000 row INSERT'
\echo '================================================================'
ALTER TABLE prof_src DISABLE TRIGGER ALL;
\echo '--- Step-by-step ---'
SELECT * FROM profile_pipeline_current(10000);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
\echo 'Full trigger 10K:'
INSERT INTO prof_src (account_id, amount) SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2) FROM generate_series(1, 10000) AS i;
\echo 'REFRESH MATVIEW:'
REFRESH MATERIALIZED VIEW prof_matview;

-- ============================================================
-- PROFILE: 50K batch
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  PROFILE: 50,000 row INSERT'
\echo '================================================================'
ALTER TABLE prof_src DISABLE TRIGGER ALL;
\echo '--- Step-by-step ---'
SELECT * FROM profile_pipeline_current(50000);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
\echo 'Full trigger 50K:'
INSERT INTO prof_src (account_id, amount) SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2) FROM generate_series(1, 50000) AS i;
\echo 'REFRESH MATVIEW:'
REFRESH MATERIALIZED VIEW prof_matview;

-- ============================================================
-- PROFILE: 100K batch
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  PROFILE: 100,000 row INSERT'
\echo '================================================================'
ALTER TABLE prof_src DISABLE TRIGGER ALL;
\echo '--- Step-by-step ---'
SELECT * FROM profile_pipeline_current(100000);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
\echo 'Full trigger 100K:'
INSERT INTO prof_src (account_id, amount) SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2) FROM generate_series(1, 100000) AS i;
\echo 'REFRESH MATVIEW:'
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
DROP FUNCTION IF EXISTS profile_pipeline_current(INTEGER);
SELECT drop_reflex_ivm('prof_view');
DROP MATERIALIZED VIEW IF EXISTS prof_matview;
DROP TABLE IF EXISTS prof_src CASCADE;

\echo ''
\echo '================================================================'
\echo '  PROFILING COMPLETE'
\echo '  S3 = delta agg for affected groups (runs delta query once)'
\echo '  S4 = MERGE into intermediate (runs delta query AGAIN)'
\echo '  S3+S4 = total delta query cost (runs 2x)'
\echo '  S5+S6 = targeted refresh (DELETE+INSERT on target)'
\echo '  S1+S2 = framework overhead (ref table + Rust FFI)'
\echo '  Full trigger - sum(S3..S7) = plpgsql overhead + advisory lock'
\echo '================================================================'
