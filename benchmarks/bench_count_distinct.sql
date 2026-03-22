-- pg_reflex benchmark: COUNT DISTINCT (reference counting)
-- Measures: SELECT DISTINCT region FROM bench_orders

\timing on
SELECT setseed(0.42);
\echo ''
\echo '=========================================='
\echo '  BENCHMARK: DISTINCT (ref counting)'
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
        PERFORM bench_cleanup_imv('bench_distinct_view');

        -- Initial materialization
        RAISE NOTICE '[%] Initial materialization', label;
        PERFORM create_reflex_ivm('bench_distinct_view',
            'SELECT DISTINCT region FROM bench_orders');

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

        -- DELETE
        RAISE NOTICE '[%] DELETE 100 rows', label;
        DELETE FROM bench_orders WHERE id <= 100;

        -- Correctness check
        PERFORM 1 FROM (
            SELECT region FROM bench_distinct_view
            EXCEPT
            SELECT DISTINCT region FROM bench_orders
        ) diff
        LIMIT 1;
        IF FOUND THEN
            RAISE WARNING '[%] Correctness: FAIL', label;
        ELSE
            RAISE NOTICE '[%] Correctness: PASS', label;
        END IF;

        PERFORM bench_cleanup_imv('bench_distinct_view');
    END LOOP;
END $sizes$;

\echo ''
\echo '=== DISTINCT benchmark complete ==='
