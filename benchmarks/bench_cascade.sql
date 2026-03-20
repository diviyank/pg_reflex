-- pg_reflex benchmark: Single-level propagation timing
-- Measures how fast a trigger update propagates from source to IMV
-- (Multi-level cascading is a known v1 limitation)

\timing on
\echo ''
\echo '=========================================='
\echo '  BENCHMARK: Single-Level Propagation'
\echo '=========================================='

DO $sizes$
DECLARE
    sizes INT[] := ARRAY[1000, 10000, 100000, 1000000];
    n INT;
    label TEXT;
BEGIN
    FOREACH n IN ARRAY sizes LOOP
        label := CASE n WHEN 1000 THEN '1K' WHEN 10000 THEN '10K' WHEN 100000 THEN '100K' WHEN 1000000 THEN '1M' END;
        RAISE NOTICE '--- Scale: % rows ---', n;
        PERFORM bench_seed_orders(n);
        PERFORM bench_cleanup_imv('bench_cascade_l1');

        -- Create L1: SUM + COUNT + AVG (multiple aggregates in one view)
        RAISE NOTICE '[%] Create L1 IMV (SUM + COUNT)', label;
        PERFORM create_reflex_ivm('bench_cascade_l1',
            'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM bench_orders GROUP BY region');

        -- Batch INSERT: time full propagation from source to L1
        RAISE NOTICE '[%] Batch INSERT 1000 rows (propagation to L1)', label;
        INSERT INTO bench_orders (region, city, amount)
        SELECT
            (ARRAY['US-East','US-West','EU-West'])[1 + (i % 3)],
            'CascadeCity',
            ROUND((random() * 1000)::numeric, 2)
        FROM generate_series(1, 1000) AS i;

        -- Batch INSERT 10K: stress test
        RAISE NOTICE '[%] Batch INSERT 10000 rows (larger batch)', label;
        INSERT INTO bench_orders (region, city, amount)
        SELECT
            (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
            'CascadeCity',
            ROUND((random() * 1000)::numeric, 2)
        FROM generate_series(1, 10000) AS i;

        -- DELETE batch
        RAISE NOTICE '[%] Batch DELETE 1000 rows (propagation to L1)', label;
        DELETE FROM bench_orders WHERE city = 'CascadeCity' AND id <= (SELECT MIN(id) + 999 FROM bench_orders WHERE city = 'CascadeCity');

        -- Correctness
        PERFORM 1 FROM (
            SELECT r.region, r.total, d.total
            FROM bench_cascade_l1 r
            FULL OUTER JOIN (SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region) d
                ON r.region = d.region
            WHERE r.total IS DISTINCT FROM d.total
        ) diff LIMIT 1;
        IF FOUND THEN
            RAISE WARNING '[%] Correctness: FAIL', label;
        ELSE
            RAISE NOTICE '[%] Correctness: PASS', label;
        END IF;

        PERFORM bench_cleanup_imv('bench_cascade_l1');
    END LOOP;
END $sizes$;

\echo ''
\echo '=== Cascade benchmark complete ==='
