-- pg_reflex benchmark teardown
-- Removes all benchmark artifacts

DROP MATERIALIZED VIEW IF EXISTS bench_matview;

SELECT bench_cleanup_imv('bench_sum_view');
SELECT bench_cleanup_imv('bench_avg_view');
SELECT bench_cleanup_imv('bench_distinct_view');
SELECT bench_cleanup_imv('bench_join_view');
SELECT bench_cleanup_imv('bench_cascade_l1');

DROP TABLE IF EXISTS bench_order_items CASCADE;
DROP TABLE IF EXISTS bench_products CASCADE;
DROP TABLE IF EXISTS bench_orders CASCADE;

DROP FUNCTION IF EXISTS bench_seed_orders(INT);
DROP FUNCTION IF EXISTS bench_seed_order_items(INT);
DROP FUNCTION IF EXISTS bench_cleanup_imv(TEXT);

\echo '=== pg_reflex benchmark teardown complete ==='
