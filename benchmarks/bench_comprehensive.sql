-- pg_reflex comprehensive benchmark
-- Tests all IMV types against MATERIALIZED VIEW baseline
-- Separates trigger overhead from bare DML cost
--
-- Run: psql -f benchmarks/setup.sql && psql -f benchmarks/bench_comprehensive.sql

\pset pager off
\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pg_reflex;
SELECT setseed(0.42);

\echo ''
\echo '============================================================'
\echo '  pg_reflex comprehensive benchmark'
\echo '  Source: 1M rows, 1K city groups, 10 regions, 100 categories'
\echo '============================================================'
\echo ''

-- ============================================================
-- 1. SOURCE DATA
-- ============================================================

DROP TABLE IF EXISTS bsrc CASCADE;
DROP TABLE IF EXISTS bdim CASCADE;

CREATE TABLE bdim (
    category TEXT PRIMARY KEY,
    label TEXT NOT NULL
);
INSERT INTO bdim (category, label)
SELECT 'cat_' || i, 'Label for category ' || i
FROM generate_series(0, 99) AS i;

CREATE TABLE bsrc (
    id SERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    city TEXT NOT NULL,
    category TEXT NOT NULL,
    amount NUMERIC NOT NULL
);

\echo 'Seeding 1M rows...'
INSERT INTO bsrc (region, city, category, amount)
SELECT
    'R' || (i % 10),
    'city_' || (i % 1000),
    'cat_' || (i % 100),
    ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;

CREATE INDEX idx_bsrc_city ON bsrc(city);
CREATE INDEX idx_bsrc_region ON bsrc(region);
CREATE INDEX idx_bsrc_category ON bsrc(category);
ANALYZE bsrc;
ANALYZE bdim;

\echo 'Source data ready: ' || (SELECT COUNT(*)::text FROM bsrc) || ' rows'

-- ============================================================
-- 2. CREATE IMVs
-- ============================================================

\echo ''
\echo '--- Creating IMVs ---'

-- IMV 1: GROUP BY aggregate (1K groups)
SELECT create_reflex_ivm('bv_groupby',
    'SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM bsrc GROUP BY city');

-- IMV 2: Passthrough JOIN (1M rows)
SELECT create_reflex_ivm('bv_passthrough',
    'SELECT s.id, s.city, s.amount, d.label FROM bsrc s JOIN bdim d ON s.category = d.category',
    'id');

-- IMV 3: WINDOW (GROUP BY + RANK over 1K groups) -- this creates bv_window__base + VIEW
SELECT create_reflex_ivm('bv_window',
    'SELECT city, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM bsrc GROUP BY city');

-- IMV 4: UNION ALL (two region slices)
SELECT create_reflex_ivm('bv_union_all',
    'SELECT city, amount FROM bsrc WHERE region = ''R0'' UNION ALL SELECT city, amount FROM bsrc WHERE region = ''R1''');

-- IMV 5: UNION dedup
SELECT create_reflex_ivm('bv_union',
    'SELECT city FROM bsrc WHERE region = ''R0'' UNION SELECT city FROM bsrc WHERE region = ''R1''');

-- Create indexes on IMV targets
CREATE INDEX IF NOT EXISTS idx_bv_groupby_city ON bv_groupby(city);
CREATE INDEX IF NOT EXISTS idx_bv_passthrough_id ON bv_passthrough(id);
-- bv_window is a VIEW, index is on bv_window__base
CREATE INDEX IF NOT EXISTS idx_bv_window_base_city ON bv_window__base(city);
-- UNION views read from sub-IMVs; index the sub-IMV targets
CREATE INDEX IF NOT EXISTS idx_bv_ua0_city ON bv_union_all__union_0(city);
CREATE INDEX IF NOT EXISTS idx_bv_ua1_city ON bv_union_all__union_1(city);

ANALYZE bv_groupby;
ANALYZE bv_passthrough;
ANALYZE bv_window__base;

-- ============================================================
-- 3. CREATE MATERIALIZED VIEWS (baseline)
-- ============================================================

\echo ''
\echo '--- Creating Materialized Views (baseline) ---'

CREATE MATERIALIZED VIEW mv_groupby AS
    SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM bsrc GROUP BY city;
CREATE INDEX idx_mv_groupby_city ON mv_groupby(city);

CREATE MATERIALIZED VIEW mv_passthrough AS
    SELECT s.id, s.city, s.amount, d.label FROM bsrc s JOIN bdim d ON s.category = d.category;
CREATE INDEX idx_mv_passthrough_id ON mv_passthrough(id);

CREATE MATERIALIZED VIEW mv_window AS
    SELECT city, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM bsrc GROUP BY city;
CREATE INDEX idx_mv_window_city ON mv_window(city);

CREATE MATERIALIZED VIEW mv_union_all AS
    SELECT city, amount FROM bsrc WHERE region = 'R0' UNION ALL SELECT city, amount FROM bsrc WHERE region = 'R1';
CREATE INDEX idx_mv_ua_city ON mv_union_all(city);

CREATE MATERIALIZED VIEW mv_union AS
    SELECT city FROM bsrc WHERE region = 'R0' UNION SELECT city FROM bsrc WHERE region = 'R1';
CREATE INDEX idx_mv_union_city ON mv_union(city);

ANALYZE mv_groupby;
ANALYZE mv_passthrough;
ANALYZE mv_window;
ANALYZE mv_union_all;

-- ============================================================
-- 4. RESULTS TABLE
-- ============================================================

DROP TABLE IF EXISTS bench_results;
CREATE TABLE bench_results (
    imv_type TEXT,
    operation TEXT,
    batch INT,
    bare_ms NUMERIC,
    reflex_ms NUMERIC,
    trigger_ms NUMERIC,
    refresh_ms NUMERIC,
    advantage_pct NUMERIC
);

-- ============================================================
-- 5. BENCHMARK FUNCTION
-- ============================================================

-- Benchmark DML approach: We can't use SAVEPOINT in plpgsql, so we use
-- a two-pass approach:
--   Pass 1: bare DML (triggers disabled), then undo with reverse DML
--   Pass 2: reflex DML (triggers enabled), then reconcile
--   Pass 3: bare DML + REFRESH MATERIALIZED VIEW, then undo
-- For INSERT: undo = DELETE the inserted rows (they have id > max_id)
-- For UPDATE: undo = reverse UPDATE (amount - 1 instead of + 1)
-- For DELETE: undo = re-INSERT from a saved temp table (impractical)
--
-- Simpler: just measure the full operation including both bare and triggered,
-- and use separate runs (disable/enable triggers between runs).
-- Since each operation is idempotent at the measurement level, small state
-- changes between runs are acceptable for timing purposes.

CREATE OR REPLACE FUNCTION run_bench_insert(
    p_imv_type TEXT,
    p_batch INT,
    p_mv_name TEXT
) RETURNS VOID AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    bare NUMERIC; reflex NUMERIC; refresh NUMERIC; trig NUMERIC; adv NUMERIC;
    max_id BIGINT;
    insert_sql TEXT;
BEGIN
    insert_sql := format(
        'INSERT INTO bsrc (region, city, category, amount)
         SELECT ''R'' || (i %% 10), ''city_'' || (i %% 1000), ''cat_'' || (i %% 100),
                ROUND((random() * 1000)::numeric, 2)
         FROM generate_series(1, %s) AS i', p_batch);

    -- Pass 1: bare INSERT (no triggers)
    SELECT MAX(id) INTO max_id FROM bsrc;
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    bare := EXTRACT(MILLISECONDS FROM t1 - t0);
    DELETE FROM bsrc WHERE id > max_id;  -- undo
    ALTER TABLE bsrc ENABLE TRIGGER ALL;

    -- Pass 2: pg_reflex INSERT (triggers fire)
    SELECT MAX(id) INTO max_id FROM bsrc;
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    reflex := EXTRACT(MILLISECONDS FROM t1 - t0);
    -- Undo: disable triggers to avoid double-firing on cleanup
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    DELETE FROM bsrc WHERE id > max_id;
    ALTER TABLE bsrc ENABLE TRIGGER ALL;
    -- Reconcile affected IMVs to restore clean state
    PERFORM reflex_reconcile(name) FROM public.__reflex_ivm_reference
        WHERE 'bsrc' = ANY(depends_on) AND enabled = TRUE AND name LIKE p_imv_type || '%'
        ORDER BY graph_depth;

    -- Pass 3: REFRESH MATERIALIZED VIEW (baseline)
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    EXECUTE insert_sql;
    ALTER TABLE bsrc ENABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE format('REFRESH MATERIALIZED VIEW %I', p_mv_name);
    t1 := clock_timestamp();
    refresh := EXTRACT(MILLISECONDS FROM t1 - t0);
    -- Undo
    SELECT MAX(id) - p_batch INTO max_id FROM bsrc;
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    DELETE FROM bsrc WHERE id > max_id;
    ALTER TABLE bsrc ENABLE TRIGGER ALL;

    trig := GREATEST(reflex - bare, 0);
    adv := CASE WHEN refresh > 0 THEN ROUND((refresh - trig) / refresh * 100, 1) ELSE 0 END;

    INSERT INTO bench_results VALUES (
        p_imv_type, 'INSERT', p_batch,
        ROUND(bare, 1), ROUND(reflex, 1), ROUND(trig, 1), ROUND(refresh, 1), adv);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION run_bench_update(
    p_imv_type TEXT,
    p_batch INT,
    p_mv_name TEXT
) RETURNS VOID AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    bare NUMERIC; reflex NUMERIC; refresh NUMERIC; trig NUMERIC; adv NUMERIC;
    update_sql TEXT;
    undo_sql TEXT;
BEGIN
    update_sql := format('UPDATE bsrc SET amount = amount + 1 WHERE id <= %s', p_batch);
    undo_sql := format('UPDATE bsrc SET amount = amount - 1 WHERE id <= %s', p_batch);

    -- Pass 1: bare UPDATE
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE update_sql;
    t1 := clock_timestamp();
    bare := EXTRACT(MILLISECONDS FROM t1 - t0);
    EXECUTE undo_sql;
    ALTER TABLE bsrc ENABLE TRIGGER ALL;

    -- Pass 2: pg_reflex UPDATE
    t0 := clock_timestamp();
    EXECUTE update_sql;
    t1 := clock_timestamp();
    reflex := EXTRACT(MILLISECONDS FROM t1 - t0);
    -- Undo
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    EXECUTE undo_sql;
    ALTER TABLE bsrc ENABLE TRIGGER ALL;
    PERFORM reflex_reconcile(name) FROM public.__reflex_ivm_reference
        WHERE 'bsrc' = ANY(depends_on) AND enabled = TRUE AND name LIKE p_imv_type || '%'
        ORDER BY graph_depth;

    -- Pass 3: REFRESH MATERIALIZED VIEW
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    EXECUTE update_sql;
    ALTER TABLE bsrc ENABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE format('REFRESH MATERIALIZED VIEW %I', p_mv_name);
    t1 := clock_timestamp();
    refresh := EXTRACT(MILLISECONDS FROM t1 - t0);
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    EXECUTE undo_sql;
    ALTER TABLE bsrc ENABLE TRIGGER ALL;

    trig := GREATEST(reflex - bare, 0);
    adv := CASE WHEN refresh > 0 THEN ROUND((refresh - trig) / refresh * 100, 1) ELSE 0 END;

    INSERT INTO bench_results VALUES (
        p_imv_type, 'UPDATE', p_batch,
        ROUND(bare, 1), ROUND(reflex, 1), ROUND(trig, 1), ROUND(refresh, 1), adv);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION run_bench_delete(
    p_imv_type TEXT,
    p_batch INT,
    p_mv_name TEXT
) RETURNS VOID AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    bare NUMERIC; reflex NUMERIC; refresh NUMERIC; trig NUMERIC; adv NUMERIC;
    delete_sql TEXT;
    max_id BIGINT;
BEGIN
    SELECT MAX(id) INTO max_id FROM bsrc;
    delete_sql := format('DELETE FROM bsrc WHERE id > %s', max_id - p_batch);

    -- Save rows to restore later
    CREATE TEMP TABLE _bench_deleted AS SELECT * FROM bsrc WHERE id > max_id - p_batch;

    -- Pass 1: bare DELETE
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE delete_sql;
    t1 := clock_timestamp();
    bare := EXTRACT(MILLISECONDS FROM t1 - t0);
    INSERT INTO bsrc SELECT * FROM _bench_deleted;  -- restore
    ALTER TABLE bsrc ENABLE TRIGGER ALL;

    -- Pass 2: pg_reflex DELETE
    t0 := clock_timestamp();
    EXECUTE delete_sql;
    t1 := clock_timestamp();
    reflex := EXTRACT(MILLISECONDS FROM t1 - t0);
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    INSERT INTO bsrc SELECT * FROM _bench_deleted;  -- restore
    ALTER TABLE bsrc ENABLE TRIGGER ALL;
    PERFORM reflex_reconcile(name) FROM public.__reflex_ivm_reference
        WHERE 'bsrc' = ANY(depends_on) AND enabled = TRUE AND name LIKE p_imv_type || '%'
        ORDER BY graph_depth;

    -- Pass 3: REFRESH MATERIALIZED VIEW
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    EXECUTE delete_sql;
    ALTER TABLE bsrc ENABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE format('REFRESH MATERIALIZED VIEW %I', p_mv_name);
    t1 := clock_timestamp();
    refresh := EXTRACT(MILLISECONDS FROM t1 - t0);
    ALTER TABLE bsrc DISABLE TRIGGER ALL;
    INSERT INTO bsrc SELECT * FROM _bench_deleted;
    ALTER TABLE bsrc ENABLE TRIGGER ALL;

    DROP TABLE _bench_deleted;

    trig := GREATEST(reflex - bare, 0);
    adv := CASE WHEN refresh > 0 THEN ROUND((refresh - trig) / refresh * 100, 1) ELSE 0 END;

    INSERT INTO bench_results VALUES (
        p_imv_type, 'DELETE', p_batch,
        ROUND(bare, 1), ROUND(reflex, 1), ROUND(trig, 1), ROUND(refresh, 1), adv);
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 6. POINT READ BENCHMARK
-- ============================================================

CREATE OR REPLACE FUNCTION run_bench_read(
    p_imv_type TEXT,
    p_imv_table TEXT,
    p_mv_table TEXT,
    p_read_sql_imv TEXT,
    p_read_sql_mv TEXT
) RETURNS VOID AS $$
DECLARE
    t0 TIMESTAMPTZ;
    t1 TIMESTAMPTZ;
    imv_ms NUMERIC;
    mv_ms NUMERIC;
    iterations INT := 100;
    dummy BIGINT;
BEGIN
    -- Warm up
    EXECUTE p_read_sql_imv INTO dummy;
    EXECUTE p_read_sql_mv INTO dummy;

    -- Measure IMV point read (average over iterations)
    t0 := clock_timestamp();
    FOR i IN 1..iterations LOOP
        EXECUTE p_read_sql_imv INTO dummy;
    END LOOP;
    t1 := clock_timestamp();
    imv_ms := EXTRACT(MILLISECONDS FROM t1 - t0) / iterations;

    -- Measure MATVIEW point read
    t0 := clock_timestamp();
    FOR i IN 1..iterations LOOP
        EXECUTE p_read_sql_mv INTO dummy;
    END LOOP;
    t1 := clock_timestamp();
    mv_ms := EXTRACT(MILLISECONDS FROM t1 - t0) / iterations;

    INSERT INTO bench_results VALUES (
        p_imv_type, 'POINT READ', 1,
        ROUND(imv_ms, 3), ROUND(imv_ms, 3), 0, ROUND(mv_ms, 3), 0
    );
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 7. RUN BENCHMARKS
-- ============================================================

\echo ''
\echo '=== Running benchmarks ==='

-- ---- POINT READS ----
\echo ''
\echo '--- Point Reads (indexed, avg of 100 iterations) ---'

SELECT run_bench_read('GROUP BY', 'bv_groupby', 'mv_groupby',
    $$SELECT total FROM bv_groupby WHERE city = 'city_42'$$,
    $$SELECT total FROM mv_groupby WHERE city = 'city_42'$$);

SELECT run_bench_read('PASSTHROUGH', 'bv_passthrough', 'mv_passthrough',
    $$SELECT amount FROM bv_passthrough WHERE id = 500000$$,
    $$SELECT amount FROM mv_passthrough WHERE id = 500000$$);

SELECT run_bench_read('WINDOW', 'bv_window', 'mv_window',
    $$SELECT rnk FROM bv_window WHERE city = 'city_42'$$,
    $$SELECT rnk FROM mv_window WHERE city = 'city_42'$$);

-- ---- GROUP BY: INSERT/UPDATE/DELETE ----
\echo ''
\echo '--- GROUP BY (1K groups from 1M rows) ---'

DO $$ BEGIN
    PERFORM run_bench_insert('bv_groupby', 1000, 'mv_groupby');
    PERFORM run_bench_insert('bv_groupby', 10000, 'mv_groupby');
    PERFORM run_bench_insert('bv_groupby', 50000, 'mv_groupby');
    PERFORM run_bench_insert('bv_groupby', 100000, 'mv_groupby');
    PERFORM run_bench_update('bv_groupby', 100, 'mv_groupby');
    PERFORM run_bench_update('bv_groupby', 1000, 'mv_groupby');
    PERFORM run_bench_update('bv_groupby', 10000, 'mv_groupby');
    PERFORM run_bench_delete('bv_groupby', 100, 'mv_groupby');
    PERFORM run_bench_delete('bv_groupby', 1000, 'mv_groupby');
    PERFORM run_bench_delete('bv_groupby', 10000, 'mv_groupby');
END $$;

-- ---- PASSTHROUGH JOIN ----
\echo ''
\echo '--- PASSTHROUGH JOIN (1M rows) ---'

DO $$ BEGIN
    PERFORM run_bench_insert('bv_passthrough', 1000, 'mv_passthrough');
    PERFORM run_bench_insert('bv_passthrough', 10000, 'mv_passthrough');
    PERFORM run_bench_insert('bv_passthrough', 50000, 'mv_passthrough');
    PERFORM run_bench_insert('bv_passthrough', 100000, 'mv_passthrough');
    PERFORM run_bench_update('bv_passthrough', 100, 'mv_passthrough');
    PERFORM run_bench_update('bv_passthrough', 1000, 'mv_passthrough');
    PERFORM run_bench_update('bv_passthrough', 10000, 'mv_passthrough');
    PERFORM run_bench_delete('bv_passthrough', 100, 'mv_passthrough');
    PERFORM run_bench_delete('bv_passthrough', 1000, 'mv_passthrough');
    PERFORM run_bench_delete('bv_passthrough', 10000, 'mv_passthrough');
END $$;

-- ---- WINDOW (GROUP BY + RANK) ----
\echo ''
\echo '--- WINDOW (GROUP BY + RANK, 1K groups) ---'
-- Note: bv_window is a VIEW over bv_window__base (GROUP BY sub-IMV).
-- The sub-IMV bv_window__base is the one that depends on bsrc.

DO $$ BEGIN
    PERFORM run_bench_insert('bv_window__base', 1000, 'mv_window');
    PERFORM run_bench_insert('bv_window__base', 10000, 'mv_window');
    PERFORM run_bench_insert('bv_window__base', 50000, 'mv_window');
    PERFORM run_bench_insert('bv_window__base', 100000, 'mv_window');
    PERFORM run_bench_update('bv_window__base', 100, 'mv_window');
    PERFORM run_bench_update('bv_window__base', 1000, 'mv_window');
    PERFORM run_bench_update('bv_window__base', 10000, 'mv_window');
    PERFORM run_bench_delete('bv_window__base', 100, 'mv_window');
    PERFORM run_bench_delete('bv_window__base', 1000, 'mv_window');
    PERFORM run_bench_delete('bv_window__base', 10000, 'mv_window');
END $$;

-- ---- UNION ALL ----
\echo ''
\echo '--- UNION ALL (two region slices, ~200K rows) ---'
-- UNION sub-IMVs depend on bsrc directly

DO $$ BEGIN
    PERFORM run_bench_insert('bv_union_all__union_0', 1000, 'mv_union_all');
    PERFORM run_bench_insert('bv_union_all__union_0', 10000, 'mv_union_all');
    PERFORM run_bench_insert('bv_union_all__union_0', 50000, 'mv_union_all');
    PERFORM run_bench_insert('bv_union_all__union_0', 100000, 'mv_union_all');
    PERFORM run_bench_delete('bv_union_all__union_0', 100, 'mv_union_all');
    PERFORM run_bench_delete('bv_union_all__union_0', 1000, 'mv_union_all');
    PERFORM run_bench_delete('bv_union_all__union_0', 10000, 'mv_union_all');
END $$;

-- ---- UNION dedup ----
\echo ''
\echo '--- UNION dedup (1K distinct cities) ---'

DO $$ BEGIN
    PERFORM run_bench_insert('bv_union__union_0', 1000, 'mv_union');
    PERFORM run_bench_insert('bv_union__union_0', 10000, 'mv_union');
    PERFORM run_bench_insert('bv_union__union_0', 50000, 'mv_union');
END $$;

-- ============================================================
-- 8. RESULTS
-- ============================================================

\echo ''
\echo '============================================================'
\echo '  RESULTS'
\echo '============================================================'
\echo ''

-- Friendly name mapping
CREATE OR REPLACE FUNCTION friendly_name(raw TEXT) RETURNS TEXT AS $$
BEGIN
    RETURN CASE
        WHEN raw LIKE '%groupby%' THEN 'GROUP BY'
        WHEN raw LIKE '%passthrough%' THEN 'PASSTHROUGH'
        WHEN raw LIKE '%window%' THEN 'WINDOW'
        WHEN raw LIKE '%union_all%' THEN 'UNION ALL'
        WHEN raw LIKE '%union__union%' THEN 'UNION'
        ELSE raw
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

\echo '--- Point Reads (ms per query, avg of 100 iterations) ---'
SELECT friendly_name(imv_type) AS type,
       bare_ms AS "imv_ms",
       refresh_ms AS "matview_ms"
FROM bench_results
WHERE operation = 'POINT READ'
ORDER BY imv_type;

\echo ''
\echo '--- INSERT Performance ---'
SELECT friendly_name(imv_type) AS type,
       batch,
       bare_ms AS "bare_dml_ms",
       reflex_ms AS "with_trigger_ms",
       trigger_ms AS "trigger_only_ms",
       refresh_ms AS "matview_refresh_ms",
       advantage_pct || '%' AS "advantage"
FROM bench_results
WHERE operation = 'INSERT'
ORDER BY imv_type, batch;

\echo ''
\echo '--- UPDATE Performance ---'
SELECT friendly_name(imv_type) AS type,
       batch,
       bare_ms AS "bare_dml_ms",
       reflex_ms AS "with_trigger_ms",
       trigger_ms AS "trigger_only_ms",
       refresh_ms AS "matview_refresh_ms",
       advantage_pct || '%' AS "advantage"
FROM bench_results
WHERE operation = 'UPDATE'
ORDER BY imv_type, batch;

\echo ''
\echo '--- DELETE Performance ---'
SELECT friendly_name(imv_type) AS type,
       batch,
       bare_ms AS "bare_dml_ms",
       reflex_ms AS "with_trigger_ms",
       trigger_ms AS "trigger_only_ms",
       refresh_ms AS "matview_refresh_ms",
       advantage_pct || '%' AS "advantage"
FROM bench_results
WHERE operation = 'DELETE'
ORDER BY imv_type, batch;

\echo ''
\echo '--- Crossover Analysis (where does MATVIEW REFRESH become cheaper?) ---'
SELECT friendly_name(imv_type) AS type,
       operation, batch,
       trigger_ms || ' ms' AS "trigger_cost",
       refresh_ms || ' ms' AS "refresh_cost",
       CASE WHEN trigger_ms > refresh_ms THEN 'MATVIEW WINS'
            ELSE 'pg_reflex WINS' END AS winner
FROM bench_results
WHERE operation != 'POINT READ'
ORDER BY imv_type, operation, batch;

\echo ''
\echo '--- Summary by IMV type ---'
SELECT friendly_name(imv_type) AS type,
       ROUND(AVG(advantage_pct), 1) || '%' AS avg_advantage,
       COUNT(*) AS measurements,
       SUM(CASE WHEN advantage_pct > 0 THEN 1 ELSE 0 END) || '/' || COUNT(*) AS "wins/total"
FROM bench_results
WHERE operation != 'POINT READ'
GROUP BY imv_type
ORDER BY AVG(advantage_pct) DESC;

DROP FUNCTION friendly_name;

-- ============================================================
-- 9. CLEANUP
-- ============================================================

DROP FUNCTION IF EXISTS run_bench_insert;
DROP FUNCTION IF EXISTS run_bench_update;
DROP FUNCTION IF EXISTS run_bench_delete;
DROP FUNCTION IF EXISTS run_bench_read;

\echo ''
\echo '=== Benchmark complete ==='
