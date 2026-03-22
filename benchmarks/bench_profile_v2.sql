-- pg_reflex pipeline profiling V2 (with MERGE)
-- Decomposes the trigger pipeline into individually timed steps.

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  PIPELINE PROFILING V2 (MERGE-based)'
\echo '  Source: 1M rows, 100K groups, 1 IMV'
\echo '================================================================'

-- ============================================================
-- SETUP
-- ============================================================
DROP TABLE IF EXISTS prof_view CASCADE;
DROP TABLE IF EXISTS __reflex_intermediate_prof_view CASCADE;
DROP TABLE IF EXISTS prof_src CASCADE;
DELETE FROM __reflex_ivm_reference WHERE name = 'prof_view';

DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;

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

-- ============================================================
-- PROFILING FUNCTION (using MERGE syntax)
-- ============================================================
CREATE OR REPLACE FUNCTION profile_pipeline(batch_size INTEGER) RETURNS TABLE(
    step TEXT, duration_ms NUMERIC
) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT;
    delta_q TEXT;
    interm TEXT := '__reflex_intermediate_prof_view';
    affected TEXT := '__reflex_affected_prof_view';
BEGIN
    SELECT base_query, end_query INTO base_q, end_q
    FROM __reflex_ivm_reference WHERE name = 'prof_view';

    delta_q := replace(base_q, 'prof_src', 'staged_batch');

    -- S0: Stage batch
    t0 := clock_timestamp();
    EXECUTE 'DROP TABLE IF EXISTS staged_batch';
    EXECUTE format(
        'CREATE TEMP TABLE staged_batch AS
         SELECT (1 + (i %% 100000))::integer AS account_id,
                ROUND((random() * 500)::numeric, 2) AS amount
         FROM generate_series(1, %s) AS i', batch_size);
    t1 := clock_timestamp();
    step := 'S0: Stage batch'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S2: Affected groups temp table (delta agg)
    t0 := clock_timestamp();
    EXECUTE format('DROP TABLE IF EXISTS "%s"', affected);
    EXECUTE format('CREATE TEMP TABLE "%s" AS SELECT DISTINCT "account_id" FROM (%s) _d', affected, delta_q);
    t1 := clock_timestamp();
    step := 'S2: Affected groups (delta agg)'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S3: MERGE into intermediate
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
    step := 'S3: MERGE intermediate'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S4: Targeted DELETE from target
    t0 := clock_timestamp();
    EXECUTE format(
        'DELETE FROM "prof_view" WHERE "account_id" IN (SELECT "account_id" FROM "%s")', affected);
    t1 := clock_timestamp();
    step := 'S4: DELETE affected from target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S5: Targeted INSERT into target
    t0 := clock_timestamp();
    EXECUTE format(
        'INSERT INTO "prof_view" %s AND "account_id" IN (SELECT "account_id" FROM "%s")', end_q, affected);
    t1 := clock_timestamp();
    step := 'S5: INSERT affected into target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S6+S7: Cleanup + metadata
    t0 := clock_timestamp();
    EXECUTE format('DROP TABLE IF EXISTS "%s"', affected);
    UPDATE __reflex_ivm_reference SET last_update_date = NOW() WHERE name = 'prof_view';
    t1 := clock_timestamp();
    step := 'S6+S7: Cleanup + metadata'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- Insert batch into real source (without trigger)
    EXECUTE 'INSERT INTO prof_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    EXECUTE 'DROP TABLE IF EXISTS staged_batch';
END $$ LANGUAGE plpgsql;

-- ============================================================
-- PROFILING + FULL TRIGGER COMPARISON
-- ============================================================

-- Warm up
ALTER TABLE prof_src DISABLE TRIGGER ALL;
SELECT * FROM profile_pipeline(100);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
INSERT INTO prof_src (account_id, amount) VALUES (1, 1.00);

\echo ''
\echo '=== 1K batch ==='
ALTER TABLE prof_src DISABLE TRIGGER ALL;
SELECT * FROM profile_pipeline(1000);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
\echo 'Full trigger 1K:'
INSERT INTO prof_src (account_id, amount) SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2) FROM generate_series(1, 1000) AS i;
\echo 'REFRESH:'
REFRESH MATERIALIZED VIEW prof_matview;

\echo ''
\echo '=== 10K batch ==='
ALTER TABLE prof_src DISABLE TRIGGER ALL;
SELECT * FROM profile_pipeline(10000);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
\echo 'Full trigger 10K:'
INSERT INTO prof_src (account_id, amount) SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2) FROM generate_series(1, 10000) AS i;

\echo ''
\echo '=== 50K batch ==='
ALTER TABLE prof_src DISABLE TRIGGER ALL;
SELECT * FROM profile_pipeline(50000);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
\echo 'Full trigger 50K:'
INSERT INTO prof_src (account_id, amount) SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2) FROM generate_series(1, 50000) AS i;

\echo ''
\echo '=== 100K batch ==='
ALTER TABLE prof_src DISABLE TRIGGER ALL;
SELECT * FROM profile_pipeline(100000);
ALTER TABLE prof_src ENABLE TRIGGER ALL;
\echo 'Full trigger 100K:'
INSERT INTO prof_src (account_id, amount) SELECT 1 + (i % 100000), ROUND((random() * 500)::numeric, 2) FROM generate_series(1, 100000) AS i;

-- ============================================================
-- CLEANUP
-- ============================================================
DROP FUNCTION IF EXISTS profile_pipeline(INTEGER);
SELECT drop_reflex_ivm('prof_view');
DROP TABLE IF EXISTS prof_src CASCADE;

\echo ''
\echo '================================================================'
\echo '  PROFILING V2 COMPLETE'
\echo '================================================================'
