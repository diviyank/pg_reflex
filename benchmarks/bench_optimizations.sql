-- pg_reflex optimization experiment: test proposed optimizations
-- Tests on 1M source, 100K groups, 1 IMV
-- Compares current approach vs optimized approaches

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  OPTIMIZATION EXPERIMENTS'
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

-- Disable triggers for manual experiments
ALTER TABLE opt_src DISABLE TRIGGER ALL;

-- ============================================================
-- Helper: create batch and prepare delta query
-- ============================================================
CREATE OR REPLACE FUNCTION make_opt_batch(n INT) RETURNS VOID AS $$
BEGIN
    DROP TABLE IF EXISTS staged_batch;
    CREATE TEMP TABLE staged_batch AS
    SELECT (1 + (i % 100000))::integer AS account_id,
           ROUND((random() * 500)::numeric, 2) AS amount
    FROM generate_series(1, n) AS i;
END $$ LANGUAGE plpgsql;

-- ============================================================
-- EXPERIMENT A: Current approach (affected groups + MERGE intermediate + DELETE/INSERT target)
-- ============================================================
CREATE OR REPLACE FUNCTION bench_current(batch_size INT) RETURNS TABLE(step TEXT, duration_ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT;
    delta_q TEXT;
    total_start TIMESTAMPTZ; total_end TIMESTAMPTZ;
BEGIN
    SELECT base_query, end_query INTO base_q, end_q
    FROM __reflex_ivm_reference WHERE name = 'opt_view';
    delta_q := replace(base_q, 'opt_src', 'staged_batch');

    PERFORM make_opt_batch(batch_size);

    total_start := clock_timestamp();

    -- S3: affected groups
    t0 := clock_timestamp();
    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_opt_view"';
    EXECUTE format('CREATE TEMP TABLE "__reflex_affected_opt_view" AS SELECT DISTINCT "account_id" FROM (%s) _d', delta_q);
    t1 := clock_timestamp();
    step := 'affected groups'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S4: MERGE intermediate
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

    -- S5: DELETE from target
    t0 := clock_timestamp();
    EXECUTE 'DELETE FROM "opt_view" WHERE "account_id" IN (SELECT "account_id" FROM "__reflex_affected_opt_view")';
    t1 := clock_timestamp();
    step := 'DELETE from target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S6: INSERT into target
    t0 := clock_timestamp();
    EXECUTE format('INSERT INTO "opt_view" %s AND "account_id" IN (SELECT "account_id" FROM "__reflex_affected_opt_view")', end_q);
    t1 := clock_timestamp();
    step := 'INSERT into target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- Cleanup
    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_opt_view"';

    total_end := clock_timestamp();
    step := '** TOTAL'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM total_end - total_start)::numeric, 1); RETURN NEXT;

    -- Keep source consistent
    EXECUTE 'INSERT INTO opt_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    DROP TABLE IF EXISTS staged_batch;
END $$ LANGUAGE plpgsql;

-- ============================================================
-- EXPERIMENT B: MERGE for target refresh (replace DELETE+INSERT with MERGE on target)
-- ============================================================
CREATE OR REPLACE FUNCTION bench_merge_target(batch_size INT) RETURNS TABLE(step TEXT, duration_ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT;
    delta_q TEXT;
    total_start TIMESTAMPTZ; total_end TIMESTAMPTZ;
BEGIN
    SELECT base_query, end_query INTO base_q, end_q
    FROM __reflex_ivm_reference WHERE name = 'opt_view';
    delta_q := replace(base_q, 'opt_src', 'staged_batch');

    PERFORM make_opt_batch(batch_size);

    total_start := clock_timestamp();

    -- S3: affected groups (still needed for scoping)
    t0 := clock_timestamp();
    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_opt_view"';
    EXECUTE format('CREATE TEMP TABLE "__reflex_affected_opt_view" AS SELECT DISTINCT "account_id" FROM (%s) _d', delta_q);
    t1 := clock_timestamp();
    step := 'affected groups'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S4: MERGE intermediate (same as current)
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

    -- S5+S6 COMBINED: MERGE into target from intermediate (replaces DELETE+INSERT)
    t0 := clock_timestamp();
    EXECUTE format(
        'MERGE INTO "opt_view" AS t '
        'USING (SELECT "account_id", "__sum_amount" AS "total", "__count_star" AS "cnt" '
        '       FROM __reflex_intermediate_opt_view '
        '       WHERE __ivm_count > 0 AND "account_id" IN (SELECT "account_id" FROM "__reflex_affected_opt_view")) AS d '
        'ON t."account_id" = d."account_id" '
        'WHEN MATCHED THEN UPDATE SET "total" = d."total", "cnt" = d."cnt" '
        'WHEN NOT MATCHED THEN INSERT ("account_id", "total", "cnt") VALUES (d."account_id", d."total", d."cnt")');
    t1 := clock_timestamp();
    step := 'MERGE into target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- DELETE groups that went to zero (for INSERT ops this is a no-op)
    t0 := clock_timestamp();
    EXECUTE 'DELETE FROM "opt_view" WHERE "account_id" IN (
        SELECT a."account_id" FROM "__reflex_affected_opt_view" a
        LEFT JOIN __reflex_intermediate_opt_view i ON a."account_id" = i."account_id"
        WHERE i.__ivm_count IS NULL OR i.__ivm_count <= 0)';
    t1 := clock_timestamp();
    step := 'DELETE zero-count'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_opt_view"';

    total_end := clock_timestamp();
    step := '** TOTAL'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM total_end - total_start)::numeric, 1); RETURN NEXT;

    EXECUTE 'INSERT INTO opt_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    DROP TABLE IF EXISTS staged_batch;
END $$ LANGUAGE plpgsql;

-- ============================================================
-- EXPERIMENT C: CTE deduplication (single delta query execution)
-- Uses CTE to run delta query once, feeding both MERGE and affected groups.
-- ============================================================
CREATE OR REPLACE FUNCTION bench_cte_dedup(batch_size INT) RETURNS TABLE(step TEXT, duration_ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT;
    delta_q TEXT;
    total_start TIMESTAMPTZ; total_end TIMESTAMPTZ;
BEGIN
    SELECT base_query, end_query INTO base_q, end_q
    FROM __reflex_ivm_reference WHERE name = 'opt_view';
    delta_q := replace(base_q, 'opt_src', 'staged_batch');

    PERFORM make_opt_batch(batch_size);

    total_start := clock_timestamp();

    -- COMBINED S3+S4: Use CTE to compute delta once, then MERGE + capture affected
    -- MERGE with RETURNING to get affected groups
    t0 := clock_timestamp();
    EXECUTE format(
        'DROP TABLE IF EXISTS "__reflex_affected_opt_view"');
    EXECUTE format(
        'CREATE TEMP TABLE "__reflex_affected_opt_view" AS '
        'WITH delta AS (%s) '
        'SELECT "account_id" FROM ('
        '  MERGE INTO __reflex_intermediate_opt_view AS t '
        '  USING delta AS d ON t."account_id" = d."account_id" '
        '  WHEN MATCHED THEN UPDATE SET '
        '    "__sum_amount" = t."__sum_amount" + d."__sum_amount", '
        '    "__count_star" = t."__count_star" + d."__count_star", '
        '    __ivm_count = t.__ivm_count + d.__ivm_count '
        '  WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count) '
        '    VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count) '
        '  RETURNING "account_id"'
        ') AS merged', delta_q);
    t1 := clock_timestamp();
    step := 'CTE+MERGE+RETURNING'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- S5+S6: MERGE into target (same as Experiment B)
    t0 := clock_timestamp();
    EXECUTE format(
        'MERGE INTO "opt_view" AS t '
        'USING (SELECT "account_id", "__sum_amount" AS "total", "__count_star" AS "cnt" '
        '       FROM __reflex_intermediate_opt_view '
        '       WHERE __ivm_count > 0 AND "account_id" IN (SELECT "account_id" FROM "__reflex_affected_opt_view")) AS d '
        'ON t."account_id" = d."account_id" '
        'WHEN MATCHED THEN UPDATE SET "total" = d."total", "cnt" = d."cnt" '
        'WHEN NOT MATCHED THEN INSERT ("account_id", "total", "cnt") VALUES (d."account_id", d."total", d."cnt")');
    t1 := clock_timestamp();
    step := 'MERGE into target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    -- DELETE zero-count
    t0 := clock_timestamp();
    EXECUTE 'DELETE FROM "opt_view" WHERE "account_id" IN (
        SELECT a."account_id" FROM "__reflex_affected_opt_view" a
        LEFT JOIN __reflex_intermediate_opt_view i ON a."account_id" = i."account_id"
        WHERE i.__ivm_count IS NULL OR i.__ivm_count <= 0)';
    t1 := clock_timestamp();
    step := 'DELETE zero-count'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    EXECUTE 'DROP TABLE IF EXISTS "__reflex_affected_opt_view"';

    total_end := clock_timestamp();
    step := '** TOTAL'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM total_end - total_start)::numeric, 1); RETURN NEXT;

    EXECUTE 'INSERT INTO opt_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    DROP TABLE IF EXISTS staged_batch;
END $$ LANGUAGE plpgsql;


-- ============================================================
-- EXPERIMENT D: Skip intermediate entirely, MERGE delta directly into target
-- Only works for simple SUM/COUNT (no AVG/HAVING)
-- ============================================================
CREATE OR REPLACE FUNCTION bench_direct_target(batch_size INT) RETURNS TABLE(step TEXT, duration_ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    base_q TEXT; end_q TEXT;
    delta_q TEXT;
    total_start TIMESTAMPTZ; total_end TIMESTAMPTZ;
BEGIN
    SELECT base_query, end_query INTO base_q, end_q
    FROM __reflex_ivm_reference WHERE name = 'opt_view';
    delta_q := replace(base_q, 'opt_src', 'staged_batch');

    PERFORM make_opt_batch(batch_size);

    total_start := clock_timestamp();

    -- MERGE intermediate (still needed to maintain state for future operations)
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

    -- MERGE directly into target using the delta (add to existing, or insert new)
    -- For INSERT operations: we can add delta directly to existing target rows
    t0 := clock_timestamp();
    EXECUTE format(
        'MERGE INTO "opt_view" AS t '
        'USING (%s) AS d '
        'ON t."account_id" = d."account_id" '
        'WHEN MATCHED THEN UPDATE SET '
        '  "total" = t."total" + d."__sum_amount", '
        '  "cnt" = t."cnt" + d."__count_star" '
        'WHEN NOT MATCHED THEN INSERT ("account_id", "total", "cnt") '
        '  VALUES (d."account_id", d."__sum_amount", d."__count_star")',
        delta_q);
    t1 := clock_timestamp();
    step := 'MERGE delta into target'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1); RETURN NEXT;

    total_end := clock_timestamp();
    step := '** TOTAL'; duration_ms := ROUND(EXTRACT(MILLISECONDS FROM total_end - total_start)::numeric, 1); RETURN NEXT;

    EXECUTE 'INSERT INTO opt_src (account_id, amount) SELECT account_id, amount FROM staged_batch';
    DROP TABLE IF EXISTS staged_batch;
END $$ LANGUAGE plpgsql;


-- ============================================================
-- RUN EXPERIMENTS
-- ============================================================

-- Warm up
SELECT * FROM bench_current(100);
SELECT * FROM bench_merge_target(100);
SELECT * FROM bench_cte_dedup(100);
SELECT * FROM bench_direct_target(100);

\echo ''
\echo '================================================================'
\echo '  10K batch — INSERT'
\echo '================================================================'
\echo 'A) Current (affected + MERGE intermediate + DELETE/INSERT target):'
SELECT * FROM bench_current(10000);
\echo 'B) MERGE target (replace DELETE+INSERT with MERGE):'
SELECT * FROM bench_merge_target(10000);
\echo 'C) CTE dedup (single delta query + MERGE RETURNING):'
SELECT * FROM bench_cte_dedup(10000);
\echo 'D) Direct delta to target (skip affected groups entirely):'
SELECT * FROM bench_direct_target(10000);

\echo ''
\echo '================================================================'
\echo '  50K batch — INSERT'
\echo '================================================================'
\echo 'A) Current:'
SELECT * FROM bench_current(50000);
\echo 'B) MERGE target:'
SELECT * FROM bench_merge_target(50000);
\echo 'C) CTE dedup:'
SELECT * FROM bench_cte_dedup(50000);
\echo 'D) Direct delta to target:'
SELECT * FROM bench_direct_target(50000);

\echo ''
\echo '================================================================'
\echo '  100K batch — INSERT'
\echo '================================================================'
\echo 'A) Current:'
SELECT * FROM bench_current(100000);
\echo 'B) MERGE target:'
SELECT * FROM bench_merge_target(100000);
\echo 'C) CTE dedup:'
SELECT * FROM bench_cte_dedup(100000);
\echo 'D) Direct delta to target:'
SELECT * FROM bench_direct_target(100000);

-- ============================================================
-- CORRECTNESS CHECK
-- ============================================================
\echo ''
\echo '--- Correctness ---'
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.account_id FROM opt_view r
      FULL OUTER JOIN (SELECT account_id, SUM(amount) AS total FROM opt_src GROUP BY account_id) d
      ON r.account_id = d.account_id WHERE r.total IS DISTINCT FROM d.total) x;

-- ============================================================
-- CLEANUP
-- ============================================================
ALTER TABLE opt_src ENABLE TRIGGER ALL;
DROP FUNCTION IF EXISTS bench_current(INT);
DROP FUNCTION IF EXISTS bench_merge_target(INT);
DROP FUNCTION IF EXISTS bench_cte_dedup(INT);
DROP FUNCTION IF EXISTS bench_direct_target(INT);
DROP FUNCTION IF EXISTS make_opt_batch(INT);
SELECT drop_reflex_ivm('opt_view');
DROP TABLE IF EXISTS opt_src CASCADE;

\echo ''
\echo '================================================================'
\echo '  OPTIMIZATION EXPERIMENTS COMPLETE'
\echo '================================================================'
