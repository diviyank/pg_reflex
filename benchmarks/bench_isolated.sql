\pset pager off
SELECT setseed(0.42);

DROP TABLE IF EXISTS src CASCADE;
CREATE TABLE src (id SERIAL PRIMARY KEY, city TEXT NOT NULL, category TEXT NOT NULL, amount NUMERIC NOT NULL);

\echo 'Seeding 5M rows, 30K cities...'
INSERT INTO src (city, category, amount)
SELECT 'city_' || (i % 30000), 'cat_' || (i % 100), ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 5000000) AS i;
CREATE INDEX ON src(city);
ANALYZE src;

\echo 'Source: 5M rows, 30K groups'

DROP TABLE IF EXISTS iso_results;
CREATE TABLE iso_results (ext TEXT, op TEXT, batch INT, bare_ms NUMERIC, triggered_ms NUMERIC, trigger_overhead NUMERIC, refresh_ms NUMERIC);

-- Create views
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_reflex') THEN
        PERFORM create_reflex_ivm('q1_view', 'SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM src GROUP BY city');
        EXECUTE 'CREATE INDEX ON q1_view(city)';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_ivm') THEN
        PERFORM pgivm.create_immv('q1_view', 'SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM src GROUP BY city');
        EXECUTE 'CREATE INDEX ON q1_view(city)';
    END IF;
END $$;

CREATE MATERIALIZED VIEW mv_q1 AS SELECT city, SUM(amount) AS total, COUNT(*) AS cnt FROM src GROUP BY city;
CREATE INDEX ON mv_q1(city);
ANALYZE q1_view; ANALYZE mv_q1;

-- Identify which extension
DO $$ BEGIN RAISE NOTICE 'Extension: %', (SELECT extname FROM pg_extension WHERE extname IN ('pg_reflex','pg_ivm') LIMIT 1); END $$;

-- ============================================================
-- POINT READ benchmark
-- ============================================================
DO $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    imv_ms NUMERIC; mv_ms NUMERIC;
    n INT := 500; dummy NUMERIC;
BEGIN
    -- Warm
    EXECUTE 'SELECT total FROM q1_view WHERE city = ''city_42''' INTO dummy;
    EXECUTE 'SELECT total FROM mv_q1 WHERE city = ''city_42''' INTO dummy;

    t0 := clock_timestamp();
    FOR i IN 1..n LOOP EXECUTE 'SELECT total FROM q1_view WHERE city = ''city_'' || ($1 % 30000)' USING i INTO dummy; END LOOP;
    t1 := clock_timestamp();
    imv_ms := EXTRACT(MILLISECONDS FROM t1 - t0) / n;

    t0 := clock_timestamp();
    FOR i IN 1..n LOOP EXECUTE 'SELECT total FROM mv_q1 WHERE city = ''city_'' || ($1 % 30000)' USING i INTO dummy; END LOOP;
    t1 := clock_timestamp();
    mv_ms := EXTRACT(MILLISECONDS FROM t1 - t0) / n;

    INSERT INTO iso_results VALUES (
        (SELECT extname FROM pg_extension WHERE extname IN ('pg_reflex','pg_ivm') LIMIT 1),
        'READ', 1, ROUND(mv_ms, 3), ROUND(imv_ms, 3), 0, ROUND(mv_ms, 3));
END $$;

-- ============================================================
-- SCAN benchmark (count all rows — full table scan)
-- ============================================================
DO $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    imv_ms NUMERIC; mv_ms NUMERIC;
    n INT := 10; dummy BIGINT;
BEGIN
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP EXECUTE 'SELECT COUNT(*) FROM q1_view' INTO dummy; END LOOP;
    t1 := clock_timestamp();
    imv_ms := EXTRACT(MILLISECONDS FROM t1 - t0) / n;

    t0 := clock_timestamp();
    FOR i IN 1..n LOOP EXECUTE 'SELECT COUNT(*) FROM mv_q1' INTO dummy; END LOOP;
    t1 := clock_timestamp();
    mv_ms := EXTRACT(MILLISECONDS FROM t1 - t0) / n;

    INSERT INTO iso_results VALUES (
        (SELECT extname FROM pg_extension WHERE extname IN ('pg_reflex','pg_ivm') LIMIT 1),
        'SCAN', 30000, ROUND(mv_ms, 1), ROUND(imv_ms, 1), 0, ROUND(mv_ms, 1));
END $$;

-- ============================================================
-- DML benchmarks
-- ============================================================

CREATE OR REPLACE FUNCTION iso_bench(p_op TEXT, p_batch INT) RETURNS VOID AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    bare NUMERIC; triggered NUMERIC; refresh NUMERIC; overhead NUMERIC;
    max_id BIGINT; sql_txt TEXT; ext TEXT;
BEGIN
    ext := (SELECT extname FROM pg_extension WHERE extname IN ('pg_reflex','pg_ivm') LIMIT 1);

    IF p_op = 'INSERT' THEN
        sql_txt := format('INSERT INTO src (city, category, amount) SELECT ''city_'' || (i %% 30000), ''cat_'' || (i %% 100), ROUND((random()*1000)::numeric,2) FROM generate_series(1,%s) i', p_batch);

        -- Warm
        SELECT MAX(id) INTO max_id FROM src;
        ALTER TABLE src DISABLE TRIGGER ALL;
        EXECUTE sql_txt; DELETE FROM src WHERE id > max_id;
        ALTER TABLE src ENABLE TRIGGER ALL;

        -- Bare
        SELECT MAX(id) INTO max_id FROM src;
        ALTER TABLE src DISABLE TRIGGER ALL;
        t0 := clock_timestamp(); EXECUTE sql_txt; t1 := clock_timestamp();
        bare := EXTRACT(MILLISECONDS FROM t1 - t0);
        DELETE FROM src WHERE id > max_id;
        ALTER TABLE src ENABLE TRIGGER ALL;

        -- Triggered
        SELECT MAX(id) INTO max_id FROM src;
        t0 := clock_timestamp(); EXECUTE sql_txt; t1 := clock_timestamp();
        triggered := EXTRACT(MILLISECONDS FROM t1 - t0);
        ALTER TABLE src DISABLE TRIGGER ALL;
        DELETE FROM src WHERE id > max_id;
        ALTER TABLE src ENABLE TRIGGER ALL;
        IF ext = 'pg_reflex' THEN PERFORM reflex_reconcile('q1_view');
        ELSE PERFORM pgivm.refresh_immv('q1_view', true); END IF;

        -- Refresh MV
        SELECT MAX(id) INTO max_id FROM src;
        ALTER TABLE src DISABLE TRIGGER ALL; EXECUTE sql_txt; ALTER TABLE src ENABLE TRIGGER ALL;
        t0 := clock_timestamp(); REFRESH MATERIALIZED VIEW mv_q1; t1 := clock_timestamp();
        refresh := EXTRACT(MILLISECONDS FROM t1 - t0);
        ALTER TABLE src DISABLE TRIGGER ALL; DELETE FROM src WHERE id > max_id; ALTER TABLE src ENABLE TRIGGER ALL;

    ELSIF p_op = 'UPDATE' THEN
        sql_txt := format('UPDATE src SET amount = amount + 1 WHERE id <= %s', p_batch);

        ALTER TABLE src DISABLE TRIGGER ALL;
        t0 := clock_timestamp(); EXECUTE sql_txt; t1 := clock_timestamp();
        bare := EXTRACT(MILLISECONDS FROM t1 - t0);
        EXECUTE format('UPDATE src SET amount = amount - 1 WHERE id <= %s', p_batch);
        ALTER TABLE src ENABLE TRIGGER ALL;

        t0 := clock_timestamp(); EXECUTE sql_txt; t1 := clock_timestamp();
        triggered := EXTRACT(MILLISECONDS FROM t1 - t0);
        ALTER TABLE src DISABLE TRIGGER ALL;
        EXECUTE format('UPDATE src SET amount = amount - 1 WHERE id <= %s', p_batch);
        ALTER TABLE src ENABLE TRIGGER ALL;
        IF ext = 'pg_reflex' THEN PERFORM reflex_reconcile('q1_view');
        ELSE PERFORM pgivm.refresh_immv('q1_view', true); END IF;

        ALTER TABLE src DISABLE TRIGGER ALL; EXECUTE sql_txt; ALTER TABLE src ENABLE TRIGGER ALL;
        t0 := clock_timestamp(); REFRESH MATERIALIZED VIEW mv_q1; t1 := clock_timestamp();
        refresh := EXTRACT(MILLISECONDS FROM t1 - t0);
        ALTER TABLE src DISABLE TRIGGER ALL;
        EXECUTE format('UPDATE src SET amount = amount - 1 WHERE id <= %s', p_batch);
        ALTER TABLE src ENABLE TRIGGER ALL;

    ELSIF p_op = 'DELETE' THEN
        SELECT MAX(id) INTO max_id FROM src;
        sql_txt := format('DELETE FROM src WHERE id > %s', max_id - p_batch);
        CREATE TEMP TABLE _sv AS SELECT * FROM src WHERE id > max_id - p_batch;

        ALTER TABLE src DISABLE TRIGGER ALL;
        t0 := clock_timestamp(); EXECUTE sql_txt; t1 := clock_timestamp();
        bare := EXTRACT(MILLISECONDS FROM t1 - t0);
        INSERT INTO src SELECT * FROM _sv;
        ALTER TABLE src ENABLE TRIGGER ALL;

        t0 := clock_timestamp(); EXECUTE sql_txt; t1 := clock_timestamp();
        triggered := EXTRACT(MILLISECONDS FROM t1 - t0);
        ALTER TABLE src DISABLE TRIGGER ALL;
        INSERT INTO src SELECT * FROM _sv;
        ALTER TABLE src ENABLE TRIGGER ALL;
        IF ext = 'pg_reflex' THEN PERFORM reflex_reconcile('q1_view');
        ELSE PERFORM pgivm.refresh_immv('q1_view', true); END IF;

        ALTER TABLE src DISABLE TRIGGER ALL; EXECUTE sql_txt; ALTER TABLE src ENABLE TRIGGER ALL;
        t0 := clock_timestamp(); REFRESH MATERIALIZED VIEW mv_q1; t1 := clock_timestamp();
        refresh := EXTRACT(MILLISECONDS FROM t1 - t0);
        ALTER TABLE src DISABLE TRIGGER ALL;
        INSERT INTO src SELECT * FROM _sv;
        ALTER TABLE src ENABLE TRIGGER ALL;

        DROP TABLE _sv;
    END IF;

    overhead := GREATEST(triggered - bare, 0);
    INSERT INTO iso_results VALUES (ext, p_op, p_batch,
        ROUND(bare,1), ROUND(triggered,1), ROUND(overhead,1), ROUND(refresh,1));
END $$ LANGUAGE plpgsql;

\echo 'Running benchmarks...'
DO $$ BEGIN
    PERFORM iso_bench('INSERT', 1000);
    PERFORM iso_bench('INSERT', 10000);
    PERFORM iso_bench('INSERT', 50000);
    PERFORM iso_bench('INSERT', 100000);
    PERFORM iso_bench('INSERT', 500000);
    PERFORM iso_bench('UPDATE', 100);
    PERFORM iso_bench('UPDATE', 1000);
    PERFORM iso_bench('UPDATE', 10000);
    PERFORM iso_bench('UPDATE', 100000);
    PERFORM iso_bench('DELETE', 100);
    PERFORM iso_bench('DELETE', 1000);
    PERFORM iso_bench('DELETE', 10000);
    PERFORM iso_bench('DELETE', 100000);
END $$;

\echo ''
SELECT ext, op, batch,
       bare_ms AS bare,
       triggered_ms AS total,
       trigger_overhead AS overhead,
       refresh_ms AS refresh,
       CASE WHEN op IN ('READ','SCAN') THEN NULL
            WHEN trigger_overhead < refresh_ms THEN ROUND((refresh_ms - trigger_overhead) / NULLIF(refresh_ms,0) * 100, 1) || '% faster'
            ELSE ROUND((trigger_overhead - refresh_ms) / NULLIF(refresh_ms,0) * 100, 1) || '% slower'
       END AS vs_refresh
FROM iso_results ORDER BY
    CASE op WHEN 'READ' THEN 1 WHEN 'SCAN' THEN 2 WHEN 'INSERT' THEN 3 WHEN 'UPDATE' THEN 4 WHEN 'DELETE' THEN 5 END,
    batch;

DROP FUNCTION iso_bench;
