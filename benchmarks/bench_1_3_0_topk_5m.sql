-- bench_1_3_0_topk_5m.sql — 5M-row top-K headline benchmark
--
-- Source: 5M rows, 5K groups (1000 rows/group avg).
-- Compares REFRESH MV vs MIN/MAX (no topk) vs MIN/MAX (topk=16) on a 50K-row
-- DELETE — the audit's R3 "wide retraction" cliff.

\pset pager off
SELECT setseed(0.42);

DROP TABLE IF EXISTS mm5_src CASCADE;
CREATE TABLE mm5_src (id SERIAL PRIMARY KEY, grp INT NOT NULL, val NUMERIC NOT NULL);

\echo 'Seeding 5M rows, 5K groups...'
INSERT INTO mm5_src (grp, val)
SELECT (i % 5000), ROUND((random() * 100000)::numeric, 2)
FROM generate_series(1, 5000000) AS i;
ANALYZE mm5_src;

DROP TABLE IF EXISTS mm5_results;
CREATE TABLE mm5_results (variant TEXT, op TEXT, batch INT, ms NUMERIC);

-- ---------------------------------------------------------------------
-- A. plain MATERIALIZED VIEW + REFRESH baseline
-- ---------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mm5_mv;
CREATE MATERIALIZED VIEW mm5_mv AS
    SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mm5_src GROUP BY grp;
CREATE INDEX ON mm5_mv(grp);
ANALYZE mm5_mv;

DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    -- Warm
    REFRESH MATERIALIZED VIEW mm5_mv;
    -- Measure
    t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW mm5_mv;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO mm5_results VALUES ('REFRESH MV', 'rebuild', 5000000, ms);
END $$;

-- ---------------------------------------------------------------------
-- B. pg_reflex IMV WITHOUT top-K (1.2.0 scoped recompute)
-- ---------------------------------------------------------------------
SELECT create_reflex_ivm('mm5_no_topk',
    'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mm5_src GROUP BY grp');
ANALYZE mm5_no_topk;

DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    -- INSERT 10K: algebraic, fast on both
    t0 := clock_timestamp();
    INSERT INTO mm5_src (grp, val)
        SELECT (i % 5000), random() * 100000 FROM generate_series(1, 10000) AS i;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO mm5_results VALUES ('IMV (no topk)', 'INSERT', 10000, ms);

    -- DELETE 10K: the cliff
    t0 := clock_timestamp();
    DELETE FROM mm5_src WHERE id <= 10000;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO mm5_results VALUES ('IMV (no topk)', 'DELETE', 10000, ms);

    -- DELETE 50K: wider retraction
    t0 := clock_timestamp();
    DELETE FROM mm5_src WHERE id BETWEEN 100000 AND 149999;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO mm5_results VALUES ('IMV (no topk)', 'DELETE', 50000, ms);
END $$;

SELECT drop_reflex_ivm('mm5_no_topk');

-- ---------------------------------------------------------------------
-- C. pg_reflex IMV WITH top-K (1.3.0)
-- ---------------------------------------------------------------------
-- Reset source for fair comparison
DROP TABLE IF EXISTS mm5_src CASCADE;
CREATE TABLE mm5_src (id SERIAL PRIMARY KEY, grp INT NOT NULL, val NUMERIC NOT NULL);
INSERT INTO mm5_src (grp, val)
SELECT (i % 5000), ROUND((random() * 100000)::numeric, 2)
FROM generate_series(1, 5000000) AS i;
ANALYZE mm5_src;

SELECT create_reflex_ivm('mm5_topk',
    'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mm5_src GROUP BY grp',
    NULL, NULL, NULL,
    16);
ANALYZE mm5_topk;

DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    t0 := clock_timestamp();
    INSERT INTO mm5_src (grp, val)
        SELECT (i % 5000), random() * 100000 FROM generate_series(1, 10000) AS i;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO mm5_results VALUES ('IMV (topk=16)', 'INSERT', 10000, ms);

    t0 := clock_timestamp();
    DELETE FROM mm5_src WHERE id <= 10000;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO mm5_results VALUES ('IMV (topk=16)', 'DELETE', 10000, ms);

    t0 := clock_timestamp();
    DELETE FROM mm5_src WHERE id BETWEEN 100000 AND 149999;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO mm5_results VALUES ('IMV (topk=16)', 'DELETE', 50000, ms);
END $$;

\echo ''
\echo '======================================================================='
\echo '5M-row MIN/MAX benchmark — 1.3.0'
\echo '======================================================================='
SELECT variant,
       op,
       batch,
       ms || ' ms' AS time
FROM mm5_results
ORDER BY op, batch, variant;
