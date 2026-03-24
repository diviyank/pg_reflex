-- pg_reflex optimization experiment V2
-- Fix CTE dedup (use WITH clause, not subquery)
-- Add VIEW-based approach (eliminate target table entirely)
-- Add DELETE/UPDATE operation tests

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  OPTIMIZATION EXPERIMENTS V2'
\echo '  Source: 1M rows, 100K groups'
\echo '================================================================'

-- ============================================================
-- SETUP
-- ============================================================
DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;

DROP TABLE IF EXISTS opt_src CASCADE;
CREATE TABLE opt_src (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    amount NUMERIC NOT NULL
);

\echo 'Seeding 1M rows (100K groups)...'
INSERT INTO opt_src (account_id, amount)
SELECT 1 + (i % 100000), ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;
ANALYZE opt_src;

SELECT create_reflex_ivm('opt_view',
    'SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM opt_src GROUP BY account_id');

\echo 'IMV rows:'
SELECT COUNT(*) AS groups FROM opt_view;

ALTER TABLE opt_src DISABLE TRIGGER ALL;

-- Helper
CREATE OR REPLACE FUNCTION make_opt_batch(n INT) RETURNS VOID AS $$
BEGIN
    DROP TABLE IF EXISTS staged_batch;
    CREATE TEMP TABLE staged_batch AS
    SELECT (1 + (i % 100000))::integer AS account_id,
           ROUND((random() * 500)::numeric, 2) AS amount
    FROM generate_series(1, n) AS i;
END $$ LANGUAGE plpgsql;

-- ============================================================
-- A) Current approach (baseline)
-- ============================================================
CREATE OR REPLACE FUNCTION bench_current(batch_size INT) RETURNS TABLE(step TEXT, duration_ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT; delta_q TEXT;
    total_start TIMESTAMPTZ; total_end TIMESTAMPTZ;
BEGIN
    SELECT base_query, end_query INTO base_q, end_q
    FROM __reflex_ivm_reference WHERE name = 'opt_view';
    delta_q := replace(base_q, 'opt_src', 'staged_batch');
    PERFORM make_opt_batch(batch_size);
    total_start := clock_timestamp();

    t0 := clock_timestamp();
    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_opt_view"';
    EXECUTE format('CREATE TEMP TABLE "__reflex_affected_opt_view" AS SELECT DISTINCT "account_id" FROM (%s) _d', delta_q);
    t1 := clock_timestamp();
    step := 'affected groups'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    t0 := clock_timestamp();
    EXECUTE format(
        'MERGE INTO __reflex_intermediate_opt_view AS t USING (%s) AS d ON t."account_id" = d."account_id" '
        'WHEN MATCHED THEN UPDATE SET '
        '"__sum_amount" = t."__sum_amount" + d."__sum_amount", '
        '"__count_star" = t."__count_star" + d."__count_star", '
        '__ivm_count = t.__ivm_count + d.__ivm_count '
        'WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count) '
        'VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count)',
        delta_q);
    t1 := clock_timestamp();
    step := 'MERGE intermediate'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    t0 := clock_timestamp();
    EXECUTE 'DELETE FROM "opt_view" WHERE "account_id" IN (SELECT "account_id" FROM "__reflex_affected_opt_view")';
    t1 := clock_timestamp();
    step := 'DELETE from target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    t0 := clock_timestamp();
    EXECUTE format('INSERT INTO "opt_view" %s AND "account_id" IN (SELECT "account_id" FROM "__reflex_affected_opt_view")', end_q);
    t1 := clock_timestamp();
    step := 'INSERT into target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_opt_view"';
    total_end := clock_timestamp();
    step := '** TOTAL'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM total_end - total_start)::numeric, 1); RETURN NEXT;

    EXECUTE 'INSERT INTO opt_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    DROP TABLE IF EXISTS staged_batch;
END $$ LANGUAGE plpgsql;

-- ============================================================
-- C2) CTE dedup FIXED: MERGE in WITH clause (not subquery)
-- ============================================================
CREATE OR REPLACE FUNCTION bench_cte_dedup_v2(batch_size INT) RETURNS TABLE(step TEXT, duration_ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT; delta_q TEXT;
    total_start TIMESTAMPTZ; total_end TIMESTAMPTZ;
BEGIN
    SELECT base_query, end_query INTO base_q, end_q
    FROM __reflex_ivm_reference WHERE name = 'opt_view';
    delta_q := replace(base_q, 'opt_src', 'staged_batch');
    PERFORM make_opt_batch(batch_size);
    total_start := clock_timestamp();

    -- COMBINED: MERGE intermediate + capture affected groups in one statement
    -- Uses WITH clause (not subquery) for MERGE ... RETURNING
    t0 := clock_timestamp();
    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_opt_view"';
    EXECUTE format(
        'CREATE TEMP TABLE "__reflex_affected_opt_view" AS '
        'WITH merged AS ('
        '  MERGE INTO __reflex_intermediate_opt_view AS t '
        '  USING (%s) AS d ON t."account_id" = d."account_id" '
        '  WHEN MATCHED THEN UPDATE SET '
        '    "__sum_amount" = t."__sum_amount" + d."__sum_amount", '
        '    "__count_star" = t."__count_star" + d."__count_star", '
        '    __ivm_count = t.__ivm_count + d.__ivm_count '
        '  WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count) '
        '    VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count) '
        '  RETURNING "account_id"'
        ') SELECT DISTINCT "account_id" FROM merged',
        delta_q);
    t1 := clock_timestamp();
    step := 'CTE MERGE+affected'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- DELETE from target
    t0 := clock_timestamp();
    EXECUTE 'DELETE FROM "opt_view" WHERE "account_id" IN (SELECT "account_id" FROM "__reflex_affected_opt_view")';
    t1 := clock_timestamp();
    step := 'DELETE from target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- INSERT into target
    t0 := clock_timestamp();
    EXECUTE format('INSERT INTO "opt_view" %s AND "account_id" IN (SELECT "account_id" FROM "__reflex_affected_opt_view")', end_q);
    t1 := clock_timestamp();
    step := 'INSERT into target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_opt_view"';
    total_end := clock_timestamp();
    step := '** TOTAL'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM total_end - total_start)::numeric, 1); RETURN NEXT;

    EXECUTE 'INSERT INTO opt_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    DROP TABLE IF EXISTS staged_batch;
END $$ LANGUAGE plpgsql;

-- ============================================================
-- E) VIEW approach: eliminate target table, use VIEW on intermediate
-- Only MERGE into intermediate, no targeted refresh needed
-- ============================================================
CREATE OR REPLACE FUNCTION bench_view_approach(batch_size INT) RETURNS TABLE(step TEXT, duration_ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT; delta_q TEXT;
    total_start TIMESTAMPTZ; total_end TIMESTAMPTZ;
BEGIN
    SELECT base_query, end_query INTO base_q, end_q
    FROM __reflex_ivm_reference WHERE name = 'opt_view';
    delta_q := replace(base_q, 'opt_src', 'staged_batch');
    PERFORM make_opt_batch(batch_size);
    total_start := clock_timestamp();

    -- Only step: MERGE into intermediate. No targeted refresh needed.
    -- (A VIEW on the intermediate would serve the data directly)
    t0 := clock_timestamp();
    EXECUTE format(
        'MERGE INTO __reflex_intermediate_opt_view AS t USING (%s) AS d ON t."account_id" = d."account_id" '
        'WHEN MATCHED THEN UPDATE SET '
        '"__sum_amount" = t."__sum_amount" + d."__sum_amount", '
        '"__count_star" = t."__count_star" + d."__count_star", '
        '__ivm_count = t.__ivm_count + d.__ivm_count '
        'WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count) '
        'VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count)',
        delta_q);
    t1 := clock_timestamp();
    step := 'MERGE intermediate only'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    total_end := clock_timestamp();
    step := '** TOTAL'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM total_end - total_start)::numeric, 1); RETURN NEXT;

    EXECUTE 'INSERT INTO opt_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    DROP TABLE IF EXISTS staged_batch;
END $$ LANGUAGE plpgsql;

-- ============================================================
-- F) CTE dedup + VIEW approach (best of both: single statement, no target refresh)
-- ============================================================
-- This is the same as E since without target refresh we don't need affected groups.
-- The VIEW approach already implies the MERGE is the only step.

-- ============================================================
-- RUN EXPERIMENTS — INSERT
-- ============================================================

-- Warm up
SELECT * FROM bench_current(100);
SELECT * FROM bench_cte_dedup_v2(100);
SELECT * FROM bench_view_approach(100);

\echo ''
\echo '================================================================'
\echo '  10K batch — INSERT'
\echo '================================================================'
\echo 'A) Current:'
SELECT * FROM bench_current(10000);
\echo 'C2) CTE dedup (fixed WITH clause):'
SELECT * FROM bench_cte_dedup_v2(10000);
\echo 'E) VIEW approach (MERGE only, no target refresh):'
SELECT * FROM bench_view_approach(10000);

\echo ''
\echo '================================================================'
\echo '  50K batch — INSERT'
\echo '================================================================'
\echo 'A) Current:'
SELECT * FROM bench_current(50000);
\echo 'C2) CTE dedup:'
SELECT * FROM bench_cte_dedup_v2(50000);
\echo 'E) VIEW approach:'
SELECT * FROM bench_view_approach(50000);

\echo ''
\echo '================================================================'
\echo '  100K batch — INSERT'
\echo '================================================================'
\echo 'A) Current:'
SELECT * FROM bench_current(100000);
\echo 'C2) CTE dedup:'
SELECT * FROM bench_cte_dedup_v2(100000);
\echo 'E) VIEW approach:'
SELECT * FROM bench_view_approach(100000);

-- ============================================================
-- QUERY PERFORMANCE: Compare target table vs VIEW on intermediate
-- ============================================================
\echo ''
\echo '================================================================'
\echo '  QUERY PERFORMANCE: target table vs VIEW on intermediate'
\echo '================================================================'

-- Create a VIEW on the intermediate for comparison
CREATE OR REPLACE VIEW opt_view_via_intermediate AS
SELECT "account_id", "__sum_amount" AS "total", "__count_star" AS "cnt"
FROM __reflex_intermediate_opt_view
WHERE __ivm_count > 0;

\echo 'Full scan — target table:'
SELECT COUNT(*), SUM(total) FROM opt_view;
\echo 'Full scan — VIEW on intermediate:'
SELECT COUNT(*), SUM(total) FROM opt_view_via_intermediate;

\echo 'Point lookup — target table (10 lookups):'
SELECT total FROM opt_view WHERE account_id = 42;
SELECT total FROM opt_view WHERE account_id = 42;
SELECT total FROM opt_view WHERE account_id = 42;
SELECT total FROM opt_view WHERE account_id = 42;
SELECT total FROM opt_view WHERE account_id = 42;
SELECT total FROM opt_view WHERE account_id = 42;
SELECT total FROM opt_view WHERE account_id = 42;
SELECT total FROM opt_view WHERE account_id = 42;
SELECT total FROM opt_view WHERE account_id = 42;
SELECT total FROM opt_view WHERE account_id = 42;

\echo 'Point lookup — VIEW on intermediate (10 lookups):'
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;
SELECT total FROM opt_view_via_intermediate WHERE account_id = 42;

-- ============================================================
-- CORRECTNESS CHECK
-- ============================================================
\echo ''
\echo '--- Correctness (target table) ---'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.account_id FROM opt_view r
      FULL OUTER JOIN (SELECT account_id, SUM(amount) AS total FROM opt_src GROUP BY account_id) d
      ON r.account_id = d.account_id WHERE r.total IS DISTINCT FROM d.total) x;

\echo '--- Correctness (VIEW on intermediate) ---'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.account_id FROM opt_view_via_intermediate r
      FULL OUTER JOIN (SELECT account_id, SUM(amount) AS total FROM opt_src GROUP BY account_id) d
      ON r.account_id = d.account_id WHERE r.total IS DISTINCT FROM d.total) x;

-- ============================================================
-- CLEANUP
-- ============================================================
ALTER TABLE opt_src ENABLE TRIGGER ALL;
DROP VIEW IF EXISTS opt_view_via_intermediate;
DROP FUNCTION IF EXISTS bench_current(INT);
DROP FUNCTION IF EXISTS bench_cte_dedup_v2(INT);
DROP FUNCTION IF EXISTS bench_view_approach(INT);
DROP FUNCTION IF EXISTS make_opt_batch(INT);
SELECT drop_reflex_ivm('opt_view');
DROP TABLE IF EXISTS opt_src CASCADE;

\echo ''
\echo '================================================================'
\echo '  EXPERIMENTS V2 COMPLETE'
\echo '================================================================'
