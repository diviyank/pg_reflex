-- pg_reflex benchmark: BASELINE comparison
-- Standard PostgreSQL MATERIALIZED VIEW with REFRESH for comparison
-- Same query as bench_sum.sql: SELECT region, SUM(amount) FROM bench_orders GROUP BY region

\timing on
SELECT setseed(0.42);
\echo ''
\echo '=========================================='
\echo '  BASELINE: Standard MATERIALIZED VIEW'
\echo '  (REFRESH after each batch of changes)'
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

        DROP MATERIALIZED VIEW IF EXISTS bench_matview;

        -- Create materialized view
        RAISE NOTICE '[%] CREATE MATERIALIZED VIEW', label;
        CREATE MATERIALIZED VIEW bench_matview AS
            SELECT region, SUM(amount) AS total FROM bench_orders GROUP BY region;

        -- Batch INSERT 1000 rows + REFRESH
        RAISE NOTICE '[%] Batch INSERT 1000 rows', label;
        INSERT INTO bench_orders (region, city, amount)
        SELECT
            (ARRAY['US-East','US-West','EU-West','EU-East','APAC-North'])[1 + (i % 5)],
            'BenchCity', ROUND((random() * 500)::numeric, 2)
        FROM generate_series(1, 1000) AS i;

        RAISE NOTICE '[%] REFRESH MATERIALIZED VIEW (after INSERT)', label;
        REFRESH MATERIALIZED VIEW bench_matview;

        -- Single INSERT + REFRESH
        RAISE NOTICE '[%] Single INSERT', label;
        INSERT INTO bench_orders (region, city, amount) VALUES ('US-East', 'BenchCity', 42.00);
        RAISE NOTICE '[%] REFRESH MATERIALIZED VIEW (after single INSERT)', label;
        REFRESH MATERIALIZED VIEW bench_matview;

        -- UPDATE 100 rows + REFRESH
        RAISE NOTICE '[%] UPDATE 100 rows', label;
        UPDATE bench_orders SET amount = amount + 1 WHERE id <= 100;
        RAISE NOTICE '[%] REFRESH MATERIALIZED VIEW (after UPDATE)', label;
        REFRESH MATERIALIZED VIEW bench_matview;

        -- DELETE 100 rows + REFRESH
        RAISE NOTICE '[%] DELETE 100 rows', label;
        DELETE FROM bench_orders WHERE id <= 100;
        RAISE NOTICE '[%] REFRESH MATERIALIZED VIEW (after DELETE)', label;
        REFRESH MATERIALIZED VIEW bench_matview;

        DROP MATERIALIZED VIEW IF EXISTS bench_matview;
    END LOOP;
END $sizes$;

\echo ''
\echo '=== Baseline benchmark complete ==='
\echo 'Compare REFRESH times above with pg_reflex trigger times from bench_sum.sql'
