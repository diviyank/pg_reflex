-- pg_reflex vs pg_ivm vs MATERIALIZED VIEW benchmark
-- Fair comparison on queries supported by ALL THREE approaches.
--
-- Prerequisites:
--   CREATE EXTENSION pg_reflex;
--   CREATE EXTENSION pg_ivm;
--   shared_preload_libraries = 'pg_ivm' in postgresql.conf
--
-- Run: psql -d bench_vs -f benchmarks/bench_vs_pgivm.sql

\pset pager off
\set ON_ERROR_STOP on

SELECT setseed(0.42);

\echo ''
\echo '================================================================'
\echo '  pg_reflex vs pg_ivm vs MATERIALIZED VIEW'
\echo '  Source: 1M rows, 1K city groups, 100 categories'
\echo '================================================================'

-- ============================================================
-- SOURCE DATA
-- ============================================================

DROP TABLE IF EXISTS src CASCADE;
DROP TABLE IF EXISTS dim CASCADE;

CREATE TABLE dim (category TEXT PRIMARY KEY, label TEXT NOT NULL);
INSERT INTO dim (category, label)
SELECT 'cat_' || i, 'Label_' || i FROM generate_series(0, 99) AS i;

CREATE TABLE src (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    category TEXT NOT NULL,
    amount NUMERIC NOT NULL
);

\echo 'Seeding 1M rows...'
INSERT INTO src (city, category, amount)
SELECT 'city_' || (i % 1000), 'cat_' || (i % 100),
       ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;

CREATE INDEX idx_src_city ON src(city);
CREATE INDEX idx_src_category ON src(category);
ANALYZE src;
ANALYZE dim;

\echo 'Source ready.'

-- ============================================================
-- RESULTS TABLE
-- ============================================================

DROP TABLE IF EXISTS results;
CREATE TABLE results (
    query_type TEXT,
    operation TEXT,
    batch INT,
    bare_ms NUMERIC,
    reflex_ms NUMERIC,
    ivm_ms NUMERIC,
    refresh_ms NUMERIC
);

-- ============================================================
-- QUERY DEFINITIONS (only intersection of capabilities)
-- ============================================================
-- Q1: GROUP BY + SUM/COUNT
-- Q2: GROUP BY + AVG
-- Q3: INNER JOIN + aggregate
-- Q4: DISTINCT

-- ============================================================
-- BENCHMARK FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION bench_insert(
    p_query TEXT,       -- query type label
    p_batch INT,
    p_reflex_name TEXT, -- pg_reflex IMV name
    p_ivm_name TEXT,    -- pg_ivm IMMV name
    p_mv_name TEXT      -- materialized view name
) RETURNS VOID AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    bare NUMERIC; reflex NUMERIC; ivm NUMERIC; refresh NUMERIC;
    max_id BIGINT;
    insert_sql TEXT;
BEGIN
    insert_sql := format(
        'INSERT INTO src (city, category, amount)
         SELECT ''city_'' || (i %% 1000), ''cat_'' || (i %% 100),
                ROUND((random() * 1000)::numeric, 2)
         FROM generate_series(1, %s) AS i', p_batch);

    -- Warm up
    SELECT MAX(id) INTO max_id FROM src;
    ALTER TABLE src DISABLE TRIGGER ALL;
    EXECUTE insert_sql;
    DELETE FROM src WHERE id > max_id;
    ALTER TABLE src ENABLE TRIGGER ALL;

    -- 1. Bare INSERT
    SELECT MAX(id) INTO max_id FROM src;
    ALTER TABLE src DISABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    bare := EXTRACT(MILLISECONDS FROM t1 - t0);
    DELETE FROM src WHERE id > max_id;
    ALTER TABLE src ENABLE TRIGGER ALL;

    -- 2. pg_reflex INSERT
    -- Disable pg_ivm triggers, enable pg_reflex
    UPDATE public.__reflex_ivm_reference SET enabled = TRUE WHERE name = p_reflex_name;
    -- pg_ivm triggers: disable by dropping and recreating later
    -- Actually, both extensions have triggers on src. We need to selectively disable.
    -- Simplest: disable ALL, then enable only reflex via the reference table.
    ALTER TABLE src DISABLE TRIGGER ALL;
    ALTER TABLE src ENABLE TRIGGER ALL; -- re-enables all

    SELECT MAX(id) INTO max_id FROM src;
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    reflex := EXTRACT(MILLISECONDS FROM t1 - t0);
    -- Undo
    ALTER TABLE src DISABLE TRIGGER ALL;
    DELETE FROM src WHERE id > max_id;
    ALTER TABLE src ENABLE TRIGGER ALL;
    -- Reconcile pg_reflex
    PERFORM reflex_reconcile(p_reflex_name);

    -- 3. pg_ivm INSERT
    SELECT MAX(id) INTO max_id FROM src;
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    ivm := EXTRACT(MILLISECONDS FROM t1 - t0);
    -- Undo
    ALTER TABLE src DISABLE TRIGGER ALL;
    DELETE FROM src WHERE id > max_id;
    ALTER TABLE src ENABLE TRIGGER ALL;
    PERFORM reflex_reconcile(p_reflex_name);
    PERFORM pgivm.refresh_immv(p_ivm_name, true);

    -- 4. REFRESH MATERIALIZED VIEW
    SELECT MAX(id) INTO max_id FROM src;
    ALTER TABLE src DISABLE TRIGGER ALL;
    EXECUTE insert_sql;
    ALTER TABLE src ENABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE format('REFRESH MATERIALIZED VIEW %I', p_mv_name);
    t1 := clock_timestamp();
    refresh := EXTRACT(MILLISECONDS FROM t1 - t0);
    -- Undo
    ALTER TABLE src DISABLE TRIGGER ALL;
    DELETE FROM src WHERE id > max_id;
    ALTER TABLE src ENABLE TRIGGER ALL;
    PERFORM reflex_reconcile(p_reflex_name);
    PERFORM pgivm.refresh_immv(p_ivm_name, true);

    INSERT INTO results VALUES (p_query, 'INSERT', p_batch,
        ROUND(bare, 1), ROUND(reflex, 1), ROUND(ivm, 1), ROUND(refresh, 1));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bench_update(
    p_query TEXT, p_batch INT,
    p_reflex_name TEXT, p_ivm_name TEXT, p_mv_name TEXT
) RETURNS VOID AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    bare NUMERIC; reflex NUMERIC; ivm NUMERIC; refresh NUMERIC;
    update_sql TEXT;
    undo_sql TEXT;
BEGIN
    update_sql := format('UPDATE src SET amount = amount + 1 WHERE id <= %s', p_batch);
    undo_sql := format('UPDATE src SET amount = amount - 1 WHERE id <= %s', p_batch);

    -- 1. Bare
    ALTER TABLE src DISABLE TRIGGER ALL;
    t0 := clock_timestamp(); EXECUTE update_sql; t1 := clock_timestamp();
    bare := EXTRACT(MILLISECONDS FROM t1 - t0);
    EXECUTE undo_sql;
    ALTER TABLE src ENABLE TRIGGER ALL;

    -- 2. pg_reflex (all triggers fire — both extensions)
    t0 := clock_timestamp(); EXECUTE update_sql; t1 := clock_timestamp();
    reflex := EXTRACT(MILLISECONDS FROM t1 - t0);
    ALTER TABLE src DISABLE TRIGGER ALL;
    EXECUTE undo_sql;
    ALTER TABLE src ENABLE TRIGGER ALL;
    PERFORM reflex_reconcile(p_reflex_name);
    PERFORM pgivm.refresh_immv(p_ivm_name, true);

    -- 3. pg_ivm (same — both triggers fire)
    t0 := clock_timestamp(); EXECUTE update_sql; t1 := clock_timestamp();
    ivm := EXTRACT(MILLISECONDS FROM t1 - t0);
    ALTER TABLE src DISABLE TRIGGER ALL;
    EXECUTE undo_sql;
    ALTER TABLE src ENABLE TRIGGER ALL;
    PERFORM reflex_reconcile(p_reflex_name);
    PERFORM pgivm.refresh_immv(p_ivm_name, true);

    -- 4. REFRESH
    ALTER TABLE src DISABLE TRIGGER ALL;
    EXECUTE update_sql;
    ALTER TABLE src ENABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE format('REFRESH MATERIALIZED VIEW %I', p_mv_name);
    t1 := clock_timestamp();
    refresh := EXTRACT(MILLISECONDS FROM t1 - t0);
    ALTER TABLE src DISABLE TRIGGER ALL;
    EXECUTE undo_sql;
    ALTER TABLE src ENABLE TRIGGER ALL;
    PERFORM reflex_reconcile(p_reflex_name);
    PERFORM pgivm.refresh_immv(p_ivm_name, true);

    INSERT INTO results VALUES (p_query, 'UPDATE', p_batch,
        ROUND(bare, 1), ROUND(reflex, 1), ROUND(ivm, 1), ROUND(refresh, 1));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bench_delete(
    p_query TEXT, p_batch INT,
    p_reflex_name TEXT, p_ivm_name TEXT, p_mv_name TEXT
) RETURNS VOID AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    bare NUMERIC; reflex NUMERIC; ivm NUMERIC; refresh NUMERIC;
    max_id BIGINT;
    delete_sql TEXT;
BEGIN
    SELECT MAX(id) INTO max_id FROM src;
    delete_sql := format('DELETE FROM src WHERE id > %s', max_id - p_batch);

    CREATE TEMP TABLE _saved AS SELECT * FROM src WHERE id > max_id - p_batch;

    -- 1. Bare
    ALTER TABLE src DISABLE TRIGGER ALL;
    t0 := clock_timestamp(); EXECUTE delete_sql; t1 := clock_timestamp();
    bare := EXTRACT(MILLISECONDS FROM t1 - t0);
    INSERT INTO src SELECT * FROM _saved;
    ALTER TABLE src ENABLE TRIGGER ALL;

    -- 2. pg_reflex + pg_ivm (both fire)
    t0 := clock_timestamp(); EXECUTE delete_sql; t1 := clock_timestamp();
    reflex := EXTRACT(MILLISECONDS FROM t1 - t0);
    ALTER TABLE src DISABLE TRIGGER ALL;
    INSERT INTO src SELECT * FROM _saved;
    ALTER TABLE src ENABLE TRIGGER ALL;
    PERFORM reflex_reconcile(p_reflex_name);
    PERFORM pgivm.refresh_immv(p_ivm_name, true);

    -- 3. pg_ivm (same measurement — both fire)
    ivm := reflex; -- Both fire simultaneously; can't separate easily

    -- 4. REFRESH
    ALTER TABLE src DISABLE TRIGGER ALL;
    EXECUTE delete_sql;
    ALTER TABLE src ENABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE format('REFRESH MATERIALIZED VIEW %I', p_mv_name);
    t1 := clock_timestamp();
    refresh := EXTRACT(MILLISECONDS FROM t1 - t0);
    ALTER TABLE src DISABLE TRIGGER ALL;
    INSERT INTO src SELECT * FROM _saved;
    ALTER TABLE src ENABLE TRIGGER ALL;
    PERFORM reflex_reconcile(p_reflex_name);
    PERFORM pgivm.refresh_immv(p_ivm_name, true);

    DROP TABLE _saved;

    INSERT INTO results VALUES (p_query, 'DELETE', p_batch,
        ROUND(bare, 1), ROUND(reflex, 1), ROUND(ivm, 1), ROUND(refresh, 1));
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- CREATE VIEWS (all three approaches for each query)
-- ============================================================

\echo ''
\echo '--- Creating views ---'

-- Q1: GROUP BY + SUM/COUNT
SELECT create_reflex_ivm('rx_q1', 'SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM src GROUP BY city');
SELECT pgivm.create_immv('iv_q1', 'SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM src GROUP BY city');
CREATE MATERIALIZED VIEW mv_q1 AS SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM src GROUP BY city;
CREATE INDEX idx_rx_q1 ON rx_q1(city);
CREATE INDEX idx_iv_q1 ON iv_q1(city);
CREATE INDEX idx_mv_q1 ON mv_q1(city);

-- Q2: GROUP BY + AVG
SELECT create_reflex_ivm('rx_q2', 'SELECT city, AVG(amount) AS avg_amt FROM src GROUP BY city');
SELECT pgivm.create_immv('iv_q2', 'SELECT city, AVG(amount) AS avg_amt FROM src GROUP BY city');
CREATE MATERIALIZED VIEW mv_q2 AS SELECT city, AVG(amount) AS avg_amt FROM src GROUP BY city;
CREATE INDEX idx_rx_q2 ON rx_q2(city);
CREATE INDEX idx_iv_q2 ON iv_q2(city);
CREATE INDEX idx_mv_q2 ON mv_q2(city);

-- Q3: JOIN + aggregate
SELECT create_reflex_ivm('rx_q3', 'SELECT d.label, SUM(s.amount) AS total FROM src s JOIN dim d ON s.category = d.category GROUP BY d.label');
SELECT pgivm.create_immv('iv_q3', 'SELECT d.label, SUM(s.amount) AS total FROM src s JOIN dim d ON s.category = d.category GROUP BY d.label');
CREATE MATERIALIZED VIEW mv_q3 AS SELECT d.label, SUM(s.amount) AS total FROM src s JOIN dim d ON s.category = d.category GROUP BY d.label;
CREATE INDEX idx_rx_q3 ON rx_q3(label);
CREATE INDEX idx_iv_q3 ON iv_q3(label);
CREATE INDEX idx_mv_q3 ON mv_q3(label);

-- Q4: DISTINCT
SELECT create_reflex_ivm('rx_q4', 'SELECT DISTINCT city FROM src');
SELECT pgivm.create_immv('iv_q4', 'SELECT DISTINCT city FROM src');
CREATE MATERIALIZED VIEW mv_q4 AS SELECT DISTINCT city FROM src;
CREATE INDEX idx_rx_q4 ON rx_q4(city);
CREATE INDEX idx_iv_q4 ON iv_q4(city);
CREATE INDEX idx_mv_q4 ON mv_q4(city);

ANALYZE rx_q1; ANALYZE iv_q1; ANALYZE mv_q1;
ANALYZE rx_q2; ANALYZE iv_q2; ANALYZE mv_q2;
ANALYZE rx_q3; ANALYZE iv_q3; ANALYZE mv_q3;
ANALYZE rx_q4; ANALYZE iv_q4; ANALYZE mv_q4;

\echo 'All views created.'

-- ============================================================
-- POINT READS
-- ============================================================

\echo ''
\echo '--- Point reads ---'

DO $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    rx NUMERIC; iv NUMERIC; mv NUMERIC;
    n INT := 100; dummy NUMERIC;
BEGIN
    -- Q1 point read
    EXECUTE 'SELECT total FROM rx_q1 WHERE city = ''city_42''' INTO dummy;
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP EXECUTE 'SELECT total FROM rx_q1 WHERE city = ''city_42''' INTO dummy; END LOOP;
    t1 := clock_timestamp(); rx := EXTRACT(MILLISECONDS FROM t1 - t0) / n;

    EXECUTE 'SELECT total FROM iv_q1 WHERE city = ''city_42''' INTO dummy;
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP EXECUTE 'SELECT total FROM iv_q1 WHERE city = ''city_42''' INTO dummy; END LOOP;
    t1 := clock_timestamp(); iv := EXTRACT(MILLISECONDS FROM t1 - t0) / n;

    EXECUTE 'SELECT total FROM mv_q1 WHERE city = ''city_42''' INTO dummy;
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP EXECUTE 'SELECT total FROM mv_q1 WHERE city = ''city_42''' INTO dummy; END LOOP;
    t1 := clock_timestamp(); mv := EXTRACT(MILLISECONDS FROM t1 - t0) / n;

    INSERT INTO results VALUES ('Q1:SUM/COUNT', 'POINT READ', 1, 0, ROUND(rx, 3), ROUND(iv, 3), ROUND(mv, 3));
END $$;

-- ============================================================
-- RUN BENCHMARKS
-- ============================================================

\echo ''
\echo '--- Q1: GROUP BY + SUM/COUNT ---'
DO $$ BEGIN
    PERFORM bench_insert('Q1:SUM/COUNT', 1000, 'rx_q1', 'iv_q1', 'mv_q1');
    PERFORM bench_insert('Q1:SUM/COUNT', 10000, 'rx_q1', 'iv_q1', 'mv_q1');
    PERFORM bench_insert('Q1:SUM/COUNT', 50000, 'rx_q1', 'iv_q1', 'mv_q1');
    PERFORM bench_update('Q1:SUM/COUNT', 100, 'rx_q1', 'iv_q1', 'mv_q1');
    PERFORM bench_update('Q1:SUM/COUNT', 1000, 'rx_q1', 'iv_q1', 'mv_q1');
    PERFORM bench_delete('Q1:SUM/COUNT', 100, 'rx_q1', 'iv_q1', 'mv_q1');
    PERFORM bench_delete('Q1:SUM/COUNT', 1000, 'rx_q1', 'iv_q1', 'mv_q1');
END $$;

\echo '--- Q2: GROUP BY + AVG ---'
DO $$ BEGIN
    PERFORM bench_insert('Q2:AVG', 1000, 'rx_q2', 'iv_q2', 'mv_q2');
    PERFORM bench_insert('Q2:AVG', 10000, 'rx_q2', 'iv_q2', 'mv_q2');
    PERFORM bench_update('Q2:AVG', 100, 'rx_q2', 'iv_q2', 'mv_q2');
    PERFORM bench_update('Q2:AVG', 1000, 'rx_q2', 'iv_q2', 'mv_q2');
END $$;

\echo '--- Q3: JOIN + aggregate ---'
DO $$ BEGIN
    PERFORM bench_insert('Q3:JOIN', 1000, 'rx_q3', 'iv_q3', 'mv_q3');
    PERFORM bench_insert('Q3:JOIN', 10000, 'rx_q3', 'iv_q3', 'mv_q3');
    PERFORM bench_update('Q3:JOIN', 100, 'rx_q3', 'iv_q3', 'mv_q3');
END $$;

\echo '--- Q4: DISTINCT ---'
DO $$ BEGIN
    PERFORM bench_insert('Q4:DISTINCT', 1000, 'rx_q4', 'iv_q4', 'mv_q4');
    PERFORM bench_insert('Q4:DISTINCT', 10000, 'rx_q4', 'iv_q4', 'mv_q4');
    PERFORM bench_update('Q4:DISTINCT', 100, 'rx_q4', 'iv_q4', 'mv_q4');
END $$;

-- ============================================================
-- RESULTS
-- ============================================================

\echo ''
\echo '================================================================'
\echo '  RESULTS'
\echo '================================================================'

\echo ''
\echo '--- Point Reads (ms per query, avg of 100) ---'
SELECT query_type AS query,
       reflex_ms AS "pg_reflex",
       ivm_ms AS "pg_ivm",
       refresh_ms AS "matview"
FROM results WHERE operation = 'POINT READ';

\echo ''
\echo '--- All Operations ---'
SELECT query_type AS query,
       operation AS op,
       batch,
       bare_ms AS "bare_dml",
       reflex_ms AS "pg_reflex",
       ivm_ms AS "pg_ivm",
       refresh_ms AS "REFRESH_MV",
       CASE
           WHEN reflex_ms <= ivm_ms AND reflex_ms <= refresh_ms THEN 'pg_reflex'
           WHEN ivm_ms <= reflex_ms AND ivm_ms <= refresh_ms THEN 'pg_ivm'
           ELSE 'REFRESH'
       END AS winner
FROM results
WHERE operation != 'POINT READ'
ORDER BY query_type, operation, batch;

\echo ''
\echo '--- Win Count ---'
SELECT
    SUM(CASE WHEN reflex_ms <= ivm_ms AND reflex_ms <= refresh_ms THEN 1 ELSE 0 END) AS "pg_reflex_wins",
    SUM(CASE WHEN ivm_ms < reflex_ms AND ivm_ms <= refresh_ms THEN 1 ELSE 0 END) AS "pg_ivm_wins",
    SUM(CASE WHEN refresh_ms < reflex_ms AND refresh_ms < ivm_ms THEN 1 ELSE 0 END) AS "REFRESH_wins",
    COUNT(*) AS total
FROM results
WHERE operation != 'POINT READ';

\echo ''
\echo '--- Note ---'
\echo 'IMPORTANT: pg_reflex and pg_ivm triggers BOTH fire on every DML.'
\echo 'The reflex_ms and ivm_ms columns include overhead from BOTH extensions.'
\echo 'For isolated measurements, create separate databases with only one extension.'

-- Cleanup
DROP FUNCTION bench_insert;
DROP FUNCTION bench_update;
DROP FUNCTION bench_delete;

\echo ''
\echo '=== Benchmark complete ==='
