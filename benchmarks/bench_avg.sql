-- pg_reflex benchmark: AVG aggregate (decomposes to SUM + COUNT)
-- Measures: SELECT region, AVG(amount) AS avg_amount FROM bench_orders GROUP BY region

\timing on
\echo ''
\echo '=========================================='
\echo '  BENCHMARK: AVG Aggregate'
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
        PERFORM bench_cleanup_imv('bench_avg_view');

        -- Initial materialization
        RAISE NOTICE '[%] Initial materialization', label;
        PERFORM create_reflex_ivm('bench_avg_view',
            'SELECT region, AVG(amount) AS avg_amount FROM bench_orders GROUP BY region');

        -- Batch INSERT
        RAISE NOTICE '[%] Batch INSERT 1000 rows', label;
        INSERT INTO bench_orders (region, city, amount)
        SELECT
            (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
            'BenchCity', ROUND((random() * 500)::numeric, 2)
        FROM generate_series(1, 1000) AS i;

        -- Single INSERT
        RAISE NOTICE '[%] Single INSERT', label;
        INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'Bench', 42.00);

        -- UPDATE
        RAISE NOTICE '[%] UPDATE 100 rows', label;
        UPDATE bench_orders SET amount = amount + 1 WHERE id <= 100;

        -- DELETE
        RAISE NOTICE '[%] DELETE 100 rows', label;
        DELETE FROM bench_orders WHERE id <= 100;

        -- Correctness check
        PERFORM 1 FROM (
            SELECT r.region
            FROM bench_avg_view r
            FULL OUTER JOIN (SELECT region, AVG(amount) AS avg_amount FROM bench_orders GROUP BY region) d
                ON r.region = d.region
            WHERE ROUND(r.avg_amount::numeric, 4) IS DISTINCT FROM ROUND(d.avg_amount::numeric, 4)
        ) diff
        LIMIT 1;
        IF FOUND THEN
            RAISE WARNING '[%] Correctness: FAIL', label;
        ELSE
            RAISE NOTICE '[%] Correctness: PASS', label;
        END IF;

        PERFORM bench_cleanup_imv('bench_avg_view');
    END LOOP;
END $sizes$;

\echo ''
\echo '=== AVG benchmark complete ==='
