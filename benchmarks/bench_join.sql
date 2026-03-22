-- pg_reflex benchmark: JOIN-based IMV
-- Measures: SELECT p.category, SUM(oi.price * oi.quantity) AS revenue
--           FROM bench_order_items oi JOIN bench_products p ON oi.product_id = p.id
--           GROUP BY p.category

\timing on
SELECT setseed(0.42);
\echo ''
\echo '=========================================='
\echo '  BENCHMARK: JOIN Aggregate'
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

        -- Need some orders for FKs
        PERFORM bench_seed_orders(GREATEST(n / 10, 1000));
        PERFORM bench_seed_order_items(n);
        PERFORM bench_cleanup_imv('bench_join_view');

        -- Initial materialization
        RAISE NOTICE '[%] Initial materialization', label;
        PERFORM create_reflex_ivm('bench_join_view',
            'SELECT p.category, SUM(oi.price * oi.quantity) AS revenue FROM bench_order_items oi JOIN bench_products p ON oi.product_id = p.id GROUP BY p.category');

        -- Batch INSERT into order_items (trigger fires on this table)
        RAISE NOTICE '[%] Batch INSERT 1000 rows into order_items', label;
        INSERT INTO bench_order_items (order_id, product_id, quantity, price)
        SELECT
            1 + (i % GREATEST((SELECT COUNT(*) FROM bench_orders)::int, 1)),
            1 + (i % 100),
            1 + (i % 5),
            ROUND((random() * 200)::numeric, 2)
        FROM generate_series(1, 1000) AS i;

        -- Single INSERT
        RAISE NOTICE '[%] Single INSERT', label;
        INSERT INTO bench_order_items (order_id, product_id, quantity, price) VALUES (1, 1, 1, 99.99);

        -- DELETE
        RAISE NOTICE '[%] DELETE 100 rows', label;
        DELETE FROM bench_order_items WHERE id <= 100;

        -- Correctness check
        PERFORM 1 FROM (
            SELECT r.category, r.revenue AS imv, d.revenue AS direct
            FROM bench_join_view r
            FULL OUTER JOIN (
                SELECT p.category, SUM(oi.price * oi.quantity) AS revenue
                FROM bench_order_items oi JOIN bench_products p ON oi.product_id = p.id
                GROUP BY p.category
            ) d ON r.category = d.category
            WHERE r.revenue IS DISTINCT FROM d.revenue
        ) diff
        LIMIT 1;
        IF FOUND THEN
            RAISE WARNING '[%] Correctness: FAIL', label;
        ELSE
            RAISE NOTICE '[%] Correctness: PASS', label;
        END IF;

        PERFORM bench_cleanup_imv('bench_join_view');
    END LOOP;
END $sizes$;

\echo ''
\echo '=== JOIN benchmark complete ==='
