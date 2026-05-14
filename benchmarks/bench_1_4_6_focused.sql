-- Focused micro-benchmark for #10 — multiple iterations per workload to
-- average out noise. Designed to be diffed before/after the patch.
\timing on
SELECT setseed(0.42);

DROP TABLE IF EXISTS bm CASCADE;
CREATE TABLE bm (id BIGSERIAL PRIMARY KEY, grp INT NOT NULL, qty INT NOT NULL);

\echo '--- Seeding 10M rows / 500K groups (clustered) ---'
INSERT INTO bm (grp, qty)
SELECT ((i - 1) / 20)::INT, (random() * 1000)::INT
FROM generate_series(1, 10000000) AS i;
ANALYZE bm;

SELECT public.create_reflex_ivm(
    'bm_v',
    'SELECT grp, SUM(qty) AS total, COUNT(*) AS cnt FROM bm GROUP BY grp',
    NULL, NULL, 'IMMEDIATE', NULL
);
ANALYZE bm_v;

\echo ''
\echo '--- Warm-up ---'
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id <= 500; ROLLBACK;
BEGIN; UPDATE bm SET qty = qty + 1 WHERE id <= 500; ROLLBACK;

\echo ''
\echo '=== 0.1% sel (10K rows / 500 groups) — 5 iters ==='
DO $$
DECLARE _r INT; _t0 TIMESTAMPTZ; _ms NUMERIC; _base INT;
BEGIN
    FOR _r IN 1..5 LOOP
        _base := (_r - 1) * 12000;
        _t0 := clock_timestamp();
        UPDATE bm SET qty = qty + 1 WHERE id BETWEEN _base + 1 AND _base + 10000;
        _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
        RAISE NOTICE 'iter % : % ms', _r, ROUND(_ms, 2);
    END LOOP;
END $$;

\echo ''
\echo '=== 1% sel (100K rows / 5K groups) — 5 iters ==='
DO $$
DECLARE _r INT; _t0 TIMESTAMPTZ; _ms NUMERIC; _base INT;
BEGIN
    FOR _r IN 1..5 LOOP
        _base := 100000 + (_r - 1) * 120000;
        _t0 := clock_timestamp();
        UPDATE bm SET qty = qty + 1 WHERE id BETWEEN _base + 1 AND _base + 100000;
        _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
        RAISE NOTICE 'iter % : % ms', _r, ROUND(_ms, 2);
    END LOOP;
END $$;

\echo ''
\echo '=== 5% sel (500K rows / 25K groups) — 3 iters ==='
DO $$
DECLARE _r INT; _t0 TIMESTAMPTZ; _ms NUMERIC; _base INT;
BEGIN
    FOR _r IN 1..3 LOOP
        _base := 2000000 + (_r - 1) * 600000;
        _t0 := clock_timestamp();
        UPDATE bm SET qty = qty + 1 WHERE id BETWEEN _base + 1 AND _base + 500000;
        _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
        RAISE NOTICE 'iter % : % ms', _r, ROUND(_ms, 2);
    END LOOP;
END $$;

\echo ''
\echo '--- Cleanup ---'
SELECT public.drop_reflex_ivm('bm_v');
DROP TABLE bm CASCADE;
