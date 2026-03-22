-- pg_reflex FAIR comparison matrix: cardinality × batch size
--
-- Tests pg_reflex vs (bare INSERT + REFRESH MATERIALIZED VIEW)
-- across 4 cardinalities and 3 batch sizes.
-- ALL timings include INSERT cost for fair comparison.

\timing on
SELECT setseed(0.42);
\echo ''
\echo '================================================================'
\echo '  FAIR BENCHMARK MATRIX'
\echo '  pg_reflex (INSERT+trigger) vs (bare INSERT + REFRESH)'
\echo '  Source: 1M rows | Cards: 10, 1K, 10K, 100K | Batches: 1K, 10K, 50K'
\echo '================================================================'

-- ============================================================
-- SETUP
-- ============================================================
DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;

DROP TABLE IF EXISTS matrix_src CASCADE;
CREATE TABLE matrix_src (
    id SERIAL PRIMARY KEY,
    grp_10 INTEGER NOT NULL,
    grp_1k INTEGER NOT NULL,
    grp_10k INTEGER NOT NULL,
    grp_100k INTEGER NOT NULL,
    amount NUMERIC NOT NULL
);

\echo 'Seeding 1M rows...'
INSERT INTO matrix_src (grp_10, grp_1k, grp_10k, grp_100k, amount)
SELECT
    (i % 10),
    (i % 1000),
    (i % 10000),
    (i % 100000),
    ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;
ANALYZE matrix_src;

-- Create 4 IMVs (one per cardinality)
SELECT create_reflex_ivm('mx_10',    'SELECT grp_10,   SUM(amount) AS total, COUNT(*) AS cnt FROM matrix_src GROUP BY grp_10');
SELECT create_reflex_ivm('mx_1k',    'SELECT grp_1k,   SUM(amount) AS total, COUNT(*) AS cnt FROM matrix_src GROUP BY grp_1k');
SELECT create_reflex_ivm('mx_10k',   'SELECT grp_10k,  SUM(amount) AS total, COUNT(*) AS cnt FROM matrix_src GROUP BY grp_10k');
SELECT create_reflex_ivm('mx_100k',  'SELECT grp_100k, SUM(amount) AS total, COUNT(*) AS cnt FROM matrix_src GROUP BY grp_100k');

-- Create 4 matching MATVIEWs
CREATE MATERIALIZED VIEW mx_mat_10   AS SELECT grp_10,   SUM(amount) AS total, COUNT(*) AS cnt FROM matrix_src GROUP BY grp_10;
CREATE MATERIALIZED VIEW mx_mat_1k   AS SELECT grp_1k,   SUM(amount) AS total, COUNT(*) AS cnt FROM matrix_src GROUP BY grp_1k;
CREATE MATERIALIZED VIEW mx_mat_10k  AS SELECT grp_10k,  SUM(amount) AS total, COUNT(*) AS cnt FROM matrix_src GROUP BY grp_10k;
CREATE MATERIALIZED VIEW mx_mat_100k AS SELECT grp_100k, SUM(amount) AS total, COUNT(*) AS cnt FROM matrix_src GROUP BY grp_100k;

\echo 'IMV group counts:'
SELECT 'mx_10' AS imv, COUNT(*) FROM mx_10
UNION ALL SELECT 'mx_1k', COUNT(*) FROM mx_1k
UNION ALL SELECT 'mx_10k', COUNT(*) FROM mx_10k
UNION ALL SELECT 'mx_100k', COUNT(*) FROM mx_100k;

-- Warm up (primes cache and triggers)
INSERT INTO matrix_src (grp_10, grp_1k, grp_10k, grp_100k, amount)
VALUES (0, 0, 0, 0, 1.00);

-- ============================================================
-- BENCHMARK FUNCTION
-- ============================================================
-- Each IMV fires independently on the same INSERT since they share
-- the same source table via consolidated triggers.
-- We benchmark ONE IMV at a time by disabling all except the target.

CREATE OR REPLACE FUNCTION bench_cell(
    imv_name TEXT, mat_name TEXT, batch_size INTEGER, grp_col TEXT, grp_mod INTEGER
) RETURNS TABLE(metric TEXT, ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    reflex_ms NUMERIC; bare_ms NUMERIC; refresh_ms NUMERIC;
BEGIN
    -- Disable ALL IMV triggers, then re-enable only the target
    UPDATE __reflex_ivm_reference SET enabled = FALSE;
    UPDATE __reflex_ivm_reference SET enabled = TRUE WHERE name = imv_name;

    -- 1. pg_reflex: INSERT with trigger
    t0 := clock_timestamp();
    EXECUTE format(
        'INSERT INTO matrix_src (grp_10, grp_1k, grp_10k, grp_100k, amount) '
        'SELECT (i %% 10), (i %% 1000), (i %% 10000), (i %% 100000), '
        'ROUND((random() * 500)::numeric, 2) FROM generate_series(1, %s) AS i',
        batch_size);
    t1 := clock_timestamp();
    reflex_ms := EXTRACT(MILLISECONDS FROM t1 - t0);

    -- 2. bare INSERT (no triggers)
    ALTER TABLE matrix_src DISABLE TRIGGER ALL;
    t0 := clock_timestamp();
    EXECUTE format(
        'INSERT INTO matrix_src (grp_10, grp_1k, grp_10k, grp_100k, amount) '
        'SELECT (i %% 10), (i %% 1000), (i %% 10000), (i %% 100000), '
        'ROUND((random() * 500)::numeric, 2) FROM generate_series(1, %s) AS i',
        batch_size);
    t1 := clock_timestamp();
    bare_ms := EXTRACT(MILLISECONDS FROM t1 - t0);
    ALTER TABLE matrix_src ENABLE TRIGGER ALL;

    -- 3. REFRESH (of the corresponding MATVIEW)
    t0 := clock_timestamp();
    EXECUTE format('REFRESH MATERIALIZED VIEW %I', mat_name);
    t1 := clock_timestamp();
    refresh_ms := EXTRACT(MILLISECONDS FROM t1 - t0);

    -- Re-enable all IMVs
    UPDATE __reflex_ivm_reference SET enabled = TRUE;

    -- Reconcile the IMV (bare insert didn't fire triggers)
    PERFORM reflex_reconcile(imv_name);

    metric := 'pg_reflex (INSERT+trigger)'; ms := ROUND(reflex_ms, 1); RETURN NEXT;
    metric := 'bare INSERT (no trigger)';   ms := ROUND(bare_ms, 1);   RETURN NEXT;
    metric := 'REFRESH MATVIEW';            ms := ROUND(refresh_ms, 1); RETURN NEXT;
    metric := 'fair baseline (bare+REFRESH)'; ms := ROUND(bare_ms + refresh_ms, 1); RETURN NEXT;
    metric := 'pg_reflex advantage %';      ms := ROUND(100.0 * (1.0 - reflex_ms / NULLIF(bare_ms + refresh_ms, 0)), 1); RETURN NEXT;
END $$ LANGUAGE plpgsql;

-- ============================================================
-- RUN MATRIX
-- ============================================================

\echo ''
\echo '================================================================'
\echo '  10 groups × 1K batch'
\echo '================================================================'
SELECT * FROM bench_cell('mx_10', 'mx_mat_10', 1000, 'grp_10', 10);

\echo '  10 groups × 10K batch'
SELECT * FROM bench_cell('mx_10', 'mx_mat_10', 10000, 'grp_10', 10);

\echo '  10 groups × 50K batch'
SELECT * FROM bench_cell('mx_10', 'mx_mat_10', 50000, 'grp_10', 10);

\echo ''
\echo '================================================================'
\echo '  1K groups × 1K batch'
\echo '================================================================'
SELECT * FROM bench_cell('mx_1k', 'mx_mat_1k', 1000, 'grp_1k', 1000);

\echo '  1K groups × 10K batch'
SELECT * FROM bench_cell('mx_1k', 'mx_mat_1k', 10000, 'grp_1k', 1000);

\echo '  1K groups × 50K batch'
SELECT * FROM bench_cell('mx_1k', 'mx_mat_1k', 50000, 'grp_1k', 1000);

\echo ''
\echo '================================================================'
\echo '  10K groups × 1K batch'
\echo '================================================================'
SELECT * FROM bench_cell('mx_10k', 'mx_mat_10k', 1000, 'grp_10k', 10000);

\echo '  10K groups × 10K batch'
SELECT * FROM bench_cell('mx_10k', 'mx_mat_10k', 10000, 'grp_10k', 10000);

\echo '  10K groups × 50K batch'
SELECT * FROM bench_cell('mx_10k', 'mx_mat_10k', 50000, 'grp_10k', 10000);

\echo ''
\echo '================================================================'
\echo '  100K groups × 1K batch'
\echo '================================================================'
SELECT * FROM bench_cell('mx_100k', 'mx_mat_100k', 1000, 'grp_100k', 100000);

\echo '  100K groups × 10K batch'
SELECT * FROM bench_cell('mx_100k', 'mx_mat_100k', 10000, 'grp_100k', 100000);

\echo '  100K groups × 50K batch'
SELECT * FROM bench_cell('mx_100k', 'mx_mat_100k', 50000, 'grp_100k', 100000);

-- ============================================================
-- CORRECTNESS CHECK (all 4 IMVs)
-- ============================================================
\echo ''
\echo '--- Correctness ---'
SELECT 'mx_10' AS imv,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS result
FROM (SELECT r.grp_10 FROM mx_10 r FULL OUTER JOIN
      (SELECT grp_10, SUM(amount) AS total FROM matrix_src GROUP BY grp_10) d
      ON r.grp_10 = d.grp_10 WHERE r.total IS DISTINCT FROM d.total) x
UNION ALL
SELECT 'mx_1k',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END
FROM (SELECT r.grp_1k FROM mx_1k r FULL OUTER JOIN
      (SELECT grp_1k, SUM(amount) AS total FROM matrix_src GROUP BY grp_1k) d
      ON r.grp_1k = d.grp_1k WHERE r.total IS DISTINCT FROM d.total) x
UNION ALL
SELECT 'mx_10k',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END
FROM (SELECT r.grp_10k FROM mx_10k r FULL OUTER JOIN
      (SELECT grp_10k, SUM(amount) AS total FROM matrix_src GROUP BY grp_10k) d
      ON r.grp_10k = d.grp_10k WHERE r.total IS DISTINCT FROM d.total) x
UNION ALL
SELECT 'mx_100k',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END
FROM (SELECT r.grp_100k FROM mx_100k r FULL OUTER JOIN
      (SELECT grp_100k, SUM(amount) AS total FROM matrix_src GROUP BY grp_100k) d
      ON r.grp_100k = d.grp_100k WHERE r.total IS DISTINCT FROM d.total) x;

-- ============================================================
-- CLEANUP
-- ============================================================
DROP FUNCTION IF EXISTS bench_cell(TEXT, TEXT, INTEGER, TEXT, INTEGER);
SELECT drop_reflex_ivm('mx_100k');
SELECT drop_reflex_ivm('mx_10k');
SELECT drop_reflex_ivm('mx_1k');
SELECT drop_reflex_ivm('mx_10');
DROP MATERIALIZED VIEW IF EXISTS mx_mat_10, mx_mat_1k, mx_mat_10k, mx_mat_100k;
DROP TABLE IF EXISTS matrix_src CASCADE;

\echo ''
\echo '================================================================'
\echo '  MATRIX BENCHMARK COMPLETE'
\echo '  Positive advantage % = pg_reflex faster'
\echo '  Negative advantage % = REFRESH faster'
\echo '================================================================'
