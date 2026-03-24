-- Investigate: Can we speed up MERGE by tuning planner/memory settings?
-- Also test: hash index on intermediate instead of B-tree PK
\timing on

DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;
DROP TABLE IF EXISTS inv_src CASCADE;
CREATE TABLE inv_src (id SERIAL PRIMARY KEY, account_id INTEGER NOT NULL, amount NUMERIC NOT NULL);
INSERT INTO inv_src (account_id, amount) SELECT 1 + (i % 100000), ROUND((random() * 1000)::numeric, 2) FROM generate_series(1, 1000000) AS i;
ANALYZE inv_src;
SELECT create_reflex_ivm('inv_view', 'SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM inv_src GROUP BY account_id');

ALTER TABLE inv_src DISABLE TRIGGER ALL;

-- Helper function to time the full pipeline
CREATE OR REPLACE FUNCTION bench_pipeline(batch_size INT) RETURNS TABLE(step TEXT, duration_ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT; delta_q TEXT;
    total_start TIMESTAMPTZ;
BEGIN
    SELECT base_query, end_query INTO base_q, end_q FROM __reflex_ivm_reference WHERE name = 'inv_view';
    delta_q := replace(base_q, 'inv_src', 'staged_batch');

    EXECUTE 'DROP TABLE IF EXISTS staged_batch';
    EXECUTE format('CREATE TEMP TABLE staged_batch AS SELECT (1 + (i %% 100000))::integer AS account_id, ROUND((random() * 500)::numeric, 2) AS amount FROM generate_series(1, %s) AS i', batch_size);

    total_start := clock_timestamp();

    -- Empty affected table
    t0 := clock_timestamp();
    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_inv_view"';
    EXECUTE format('CREATE TEMP TABLE "__reflex_affected_inv_view" AS SELECT "account_id" FROM __reflex_intermediate_inv_view WHERE FALSE');
    t1 := clock_timestamp();
    step := 'create affected'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- MERGE + RETURNING
    t0 := clock_timestamp();
    EXECUTE format(
        'WITH __m AS (MERGE INTO __reflex_intermediate_inv_view AS t USING (%s) AS d ON t."account_id" = d."account_id" '
        'WHEN MATCHED THEN UPDATE SET "__sum_amount" = t."__sum_amount" + d."__sum_amount", "__count_star" = t."__count_star" + d."__count_star", __ivm_count = t.__ivm_count + d.__ivm_count '
        'WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count) VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count) '
        'RETURNING t."account_id") INSERT INTO "__reflex_affected_inv_view" SELECT DISTINCT "account_id" FROM __m', delta_q);
    t1 := clock_timestamp();
    step := 'MERGE+RETURNING'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- DELETE from target
    t0 := clock_timestamp();
    EXECUTE 'DELETE FROM "inv_view" WHERE "account_id" IN (SELECT "account_id" FROM "__reflex_affected_inv_view")';
    t1 := clock_timestamp();
    step := 'DELETE target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- INSERT into target
    t0 := clock_timestamp();
    EXECUTE format('INSERT INTO "inv_view" %s AND "account_id" IN (SELECT "account_id" FROM "__reflex_affected_inv_view")', end_q);
    t1 := clock_timestamp();
    step := 'INSERT target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_inv_view"';
    step := '** TOTAL'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM clock_timestamp() - total_start)::numeric, 1); RETURN NEXT;

    EXECUTE 'INSERT INTO inv_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    DROP TABLE IF EXISTS staged_batch;
END $$ LANGUAGE plpgsql;

-- Warmup
SELECT * FROM bench_pipeline(100);

\echo ''
\echo '=== 100K — Default settings (work_mem=4MB, shared_buffers=128MB) ==='
SELECT * FROM bench_pipeline(100000);

\echo ''
\echo '=== 100K — High work_mem (64MB) ==='
SET work_mem = '64MB';
SELECT * FROM bench_pipeline(100000);
RESET work_mem;

\echo ''
\echo '=== 100K — Very high work_mem (256MB) ==='
SET work_mem = '256MB';
SELECT * FROM bench_pipeline(100000);
RESET work_mem;

\echo ''
\echo '=== Now test: add hash index on intermediate alongside PK ==='
CREATE INDEX __reflex_hash_inv_view ON __reflex_intermediate_inv_view USING hash ("account_id");

\echo '=== 100K — With hash index + default work_mem ==='
SELECT * FROM bench_pipeline(100000);

\echo '=== 100K — With hash index + high work_mem ==='
SET work_mem = '64MB';
SELECT * FROM bench_pipeline(100000);
RESET work_mem;

\echo ''
\echo '=== Now test: drop B-tree PK, keep only hash index ==='
ALTER TABLE __reflex_intermediate_inv_view DROP CONSTRAINT __reflex_intermediate_inv_view_pkey;
-- Add back a unique btree for correctness
-- Actually hash doesn't support unique, keep PK. Let me just test without PK.

\echo '=== 100K — Hash index only (no PK B-tree) ==='
SELECT * FROM bench_pipeline(100000);

\echo '=== 100K — Hash index only + high work_mem ==='
SET work_mem = '64MB';
SELECT * FROM bench_pipeline(100000);
RESET work_mem;

-- Correctness
\echo ''
\echo '--- Correctness ---'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.account_id FROM inv_view r
      FULL OUTER JOIN (SELECT account_id, SUM(amount) AS total FROM inv_src GROUP BY account_id) d
      ON r.account_id = d.account_id WHERE r.total IS DISTINCT FROM d.total) x;

-- Cleanup
DROP INDEX IF EXISTS __reflex_hash_inv_view;
DROP FUNCTION IF EXISTS bench_pipeline(INT);
ALTER TABLE inv_src ENABLE TRIGGER ALL;
SELECT drop_reflex_ivm('inv_view');
DROP TABLE IF EXISTS inv_src CASCADE;
