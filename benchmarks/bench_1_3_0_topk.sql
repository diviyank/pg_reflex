-- bench_1_3_0_topk.sql — focused 1.3.0 benchmark
--
-- Compares 1.3.0 top-K MIN/MAX retraction against the 1.2.0 scoped-recompute
-- baseline, plus a clean incremental INSERT/DELETE measurement on a SUM/COUNT
-- workload. Each measurement is a single op against a fresh 1M-row source —
-- no ALTER TABLE / DISABLE TRIGGER cycles to skew the timing.
--
-- Run via:
--   PGRX_BIN=/home/diviyan/.pgrx/18.3/pgrx-install/bin
--   $PGRX_BIN/psql -h /home/diviyan/.pgrx -p 28818 -d bench130 \
--                  -f benchmarks/bench_1_3_0_topk.sql

\pset pager off
SELECT setseed(0.42);

DROP TABLE IF EXISTS bench_results CASCADE;
CREATE TABLE bench_results (
    scenario TEXT, batch INT, op TEXT, ms NUMERIC, mode TEXT
);

\echo ''
\echo '======================================================================='
\echo 'pg_reflex 1.3.0 — focused benchmark'
\echo '======================================================================='
\echo ''

-- =====================================================================
-- Scenario A: SUM/COUNT — baseline algebraic aggregates (already fast)
-- =====================================================================

DROP TABLE IF EXISTS sum_src CASCADE;
CREATE TABLE sum_src (id SERIAL PRIMARY KEY, grp INT NOT NULL, amount NUMERIC NOT NULL);

\echo 'A. SUM/COUNT — seeding 1M rows, 1K groups...'
INSERT INTO sum_src (grp, amount)
SELECT (i % 1000), ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;
ANALYZE sum_src;

SELECT create_reflex_ivm('sum_view',
    'SELECT grp, SUM(amount) AS total, COUNT(*) AS cnt FROM sum_src GROUP BY grp');

DROP MATERIALIZED VIEW IF EXISTS sum_mv;
CREATE MATERIALIZED VIEW sum_mv AS
    SELECT grp, SUM(amount) AS total, COUNT(*) AS cnt FROM sum_src GROUP BY grp;
CREATE INDEX ON sum_mv(grp);
ANALYZE sum_view; ANALYZE sum_mv;

-- A1: INSERT 10K rows
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    t0 := clock_timestamp();
    INSERT INTO sum_src (grp, amount)
        SELECT (i % 1000), 100 FROM generate_series(1, 10000) AS i;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('SUM/COUNT', 10000, 'INSERT (incremental)', ms, '');

    t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW sum_mv;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('SUM/COUNT', 10000, 'REFRESH MV', ms, '');
END $$;

-- A2: DELETE 10K rows
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    t0 := clock_timestamp();
    DELETE FROM sum_src WHERE id <= 10000;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('SUM/COUNT', 10000, 'DELETE (incremental)', ms, '');

    t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW sum_mv;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('SUM/COUNT', 10000, 'REFRESH MV', ms, '');
END $$;

-- =====================================================================
-- Scenario B: MIN/MAX retraction WITHOUT top-K (1.2.0 scoped recompute)
-- =====================================================================

DROP TABLE IF EXISTS mm_src CASCADE;
CREATE TABLE mm_src (id SERIAL PRIMARY KEY, grp INT NOT NULL, val NUMERIC NOT NULL);
\echo ''
\echo 'B. MIN/MAX retraction — seeding 1M rows, 1K groups...'
INSERT INTO mm_src (grp, val)
SELECT (i % 1000), ROUND((random() * 10000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;
ANALYZE mm_src;

SELECT create_reflex_ivm('mm_view_no_topk',
    'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mm_src GROUP BY grp');

DROP MATERIALIZED VIEW IF EXISTS mm_mv;
CREATE MATERIALIZED VIEW mm_mv AS
    SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mm_src GROUP BY grp;
CREATE INDEX ON mm_mv(grp);
ANALYZE mm_view_no_topk; ANALYZE mm_mv;

-- B1: INSERT 10K rows (algebraic — should be fast on both versions)
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    t0 := clock_timestamp();
    INSERT INTO mm_src (grp, val)
        SELECT (i % 1000), random() * 10000 FROM generate_series(1, 10000) AS i;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('MIN/MAX (no topk)', 10000, 'INSERT (incremental)', ms, '');
END $$;

-- B2: DELETE 10K rows (the headline 1.2.0 → 1.3.0 case)
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    t0 := clock_timestamp();
    DELETE FROM mm_src WHERE id <= 10000;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('MIN/MAX (no topk)', 10000, 'DELETE (incremental)', ms, 'scoped recompute');

    t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW mm_mv;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('MIN/MAX', 10000, 'REFRESH MV', ms, '');
END $$;

-- =====================================================================
-- Scenario C: MIN/MAX retraction WITH top-K (1.3.0 — opt-in topk=16)
-- =====================================================================

DROP TABLE IF EXISTS mm_src2 CASCADE;
CREATE TABLE mm_src2 (id SERIAL PRIMARY KEY, grp INT NOT NULL, val NUMERIC NOT NULL);
\echo ''
\echo 'C. MIN/MAX retraction with top-K — seeding 1M rows, 1K groups...'
INSERT INTO mm_src2 (grp, val)
SELECT (i % 1000), ROUND((random() * 10000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;
ANALYZE mm_src2;

SELECT create_reflex_ivm(
    'mm_view_topk',
    'SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM mm_src2 GROUP BY grp',
    NULL, NULL, NULL,
    16  -- topk=16
);

ANALYZE mm_view_topk;

-- C1: INSERT 10K rows
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    t0 := clock_timestamp();
    INSERT INTO mm_src2 (grp, val)
        SELECT (i % 1000), random() * 10000 FROM generate_series(1, 10000) AS i;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('MIN/MAX (topk=16)', 10000, 'INSERT (incremental)', ms, 'top-K merge');
END $$;

-- C2: DELETE 10K rows — the 1.3.0 headline win
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    t0 := clock_timestamp();
    DELETE FROM mm_src2 WHERE id <= 10000;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('MIN/MAX (topk=16)', 10000, 'DELETE (incremental)', ms, 'multiset subtract');
END $$;

-- C3: DELETE 1K rows (small batch — should be much faster with top-K)
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    t0 := clock_timestamp();
    DELETE FROM mm_src2 WHERE id BETWEEN 100000 AND 100999;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('MIN/MAX (topk=16)', 1000, 'DELETE (incremental)', ms, 'multiset subtract');
END $$;

-- =====================================================================
-- Scenario D: BOOL_OR (algebraic since 1.1.3) — sanity check
-- =====================================================================
DROP TABLE IF EXISTS bo_src CASCADE;
CREATE TABLE bo_src (id SERIAL PRIMARY KEY, grp INT NOT NULL, flag BOOL);
\echo ''
\echo 'D. BOOL_OR — seeding 1M rows, 1K groups...'
INSERT INTO bo_src (grp, flag)
SELECT (i % 1000), (random() > 0.5) FROM generate_series(1, 1000000) AS i;
ANALYZE bo_src;

SELECT create_reflex_ivm('bo_view',
    'SELECT grp, BOOL_OR(flag) AS any_flag FROM bo_src GROUP BY grp');

DROP MATERIALIZED VIEW IF EXISTS bo_mv;
CREATE MATERIALIZED VIEW bo_mv AS
    SELECT grp, BOOL_OR(flag) AS any_flag FROM bo_src GROUP BY grp;
ANALYZE bo_view; ANALYZE bo_mv;

DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; ms NUMERIC;
BEGIN
    t0 := clock_timestamp();
    DELETE FROM bo_src WHERE id <= 10000;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('BOOL_OR', 10000, 'DELETE (incremental)', ms, 'algebraic');

    t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW bo_mv;
    t1 := clock_timestamp();
    ms := ROUND(EXTRACT(MILLISECONDS FROM t1 - t0)::numeric, 1);
    INSERT INTO bench_results VALUES ('BOOL_OR', 10000, 'REFRESH MV', ms, '');
END $$;

\echo ''
\echo '======================================================================='
\echo 'Results'
\echo '======================================================================='
SELECT scenario,
       batch,
       op,
       ms || ' ms' AS time,
       COALESCE(NULLIF(mode, ''), '-') AS path
FROM bench_results
ORDER BY scenario, batch, op;
