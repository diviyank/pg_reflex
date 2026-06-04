-- Benchmark for the passthrough partition-aware (#2) + keyed-secondary (#3)
-- maintenance branch. Self-contained synthetic workload that mirrors the audit's
-- sop_forecast_view shape (LIST-partitioned source, passthrough IMV with a
-- LEFT JOIN secondary), so it runs against any DB with pg_reflex installed:
--
--     psql -U postgres -d <db> -f benchmarks/bench_passthrough_partition_keyed.sql
--
-- Run it on the PRE-branch build and on this branch and diff the timings.
-- For the production-scale numbers, point the same two operations at the real
-- alp.sop_forecast_view / alp.sales_simulation in db_clone instead of the
-- synthetic tables below.
\timing on
SELECT setseed(0.42);

DROP TABLE IF EXISTS bpk_sales CASCADE;
DROP TABLE IF EXISTS bpk_assort CASCADE;

-- Anchor: LIST-partitioned on dem_plan_id (the audit's partition key), 20 leaves.
CREATE TABLE bpk_sales (
    dem_plan_id INT  NOT NULL,
    product_id  INT  NOT NULL,
    location_id INT  NOT NULL,
    order_date  DATE NOT NULL,
    qty         INT  NOT NULL
) PARTITION BY LIST (dem_plan_id);
DO $$
BEGIN
    FOR p IN 1..20 LOOP
        EXECUTE format('CREATE TABLE bpk_sales_%s PARTITION OF bpk_sales FOR VALUES IN (%s)', p, p);
    END LOOP;
END $$;

-- Secondary ("assortment activity"): keyed on (product_id, location_id).
CREATE TABLE bpk_assort (
    product_id  INT NOT NULL,
    location_id INT NOT NULL,
    is_active   BOOL NOT NULL
);

\echo '--- Seeding 2M anchor rows across 20 partitions + 50K assortment rows ---'
INSERT INTO bpk_sales (dem_plan_id, product_id, location_id, order_date, qty)
SELECT ((i % 20) + 1),
       (i % 5000),
       (i % 200),
       DATE '2026-01-01' + (i % 90),
       (random() * 100)::INT
FROM generate_series(1, 2000000) AS i;

INSERT INTO bpk_assort (product_id, location_id, is_active)
SELECT (i % 5000), (i % 200), TRUE
FROM generate_series(1, 50000) AS i
ON CONFLICT DO NOTHING;
ANALYZE bpk_sales;
ANALYZE bpk_assort;

-- Passthrough IMV: one row per sale, projected through a LEFT JOIN secondary,
-- partitioned by dem_plan_id. Unique key includes the secondary join cols.
SELECT public.create_reflex_ivm(
    'bpk_view',
    'SELECT s.dem_plan_id, s.product_id, s.location_id, s.order_date, s.qty, '
    || 'COALESCE(a.is_active, FALSE) AS in_assortment '
    || 'FROM bpk_sales s LEFT JOIN bpk_assort a '
    || 'ON a.product_id = s.product_id AND a.location_id = s.location_id',
    'dem_plan_id,product_id,location_id,order_date',
    NULL, 'DEFERRED', NULL,
    ARRAY['dem_plan_id']
);
ANALYZE bpk_view;

\echo ''
\echo '=== Audit #2 — primary UPDATE bounded to ONE partition (dem_plan_id = 7) ==='
\echo 'Expect: DELETE/INSERT pruned to the touched leaf, not all 20 leaves.'
DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    _t0 := clock_timestamp();
    UPDATE bpk_sales SET qty = qty + 1 WHERE dem_plan_id = 7;
    PERFORM reflex_flush_deferred('bpk_sales');
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE 'single-partition UPDATE + flush: % ms', ROUND(_ms, 2);
END $$;

\echo ''
\echo '=== Audit #3 — secondary edit touching a small key set (250 keys) ==='
\echo 'Expect: keyed delete/reinsert ~ O(changed keys), NOT a full rebuild of the IMV.'
DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    _t0 := clock_timestamp();
    UPDATE bpk_assort SET is_active = NOT is_active
        WHERE product_id < 50;   -- ~250 (product_id, location_id) keys
    PERFORM reflex_flush_deferred('bpk_assort');
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE 'secondary edit (~250 keys) + flush: % ms', ROUND(_ms, 2);
END $$;

\echo ''
\echo '--- Correctness cross-check: IMV must equal a fresh recompute ---'
SELECT count(*) AS mismatches FROM (
    (SELECT * FROM bpk_view
     EXCEPT ALL
     SELECT s.dem_plan_id, s.product_id, s.location_id, s.order_date, s.qty,
            COALESCE(a.is_active, FALSE)
     FROM bpk_sales s LEFT JOIN bpk_assort a
       ON a.product_id = s.product_id AND a.location_id = s.location_id)
    UNION ALL
    (SELECT s.dem_plan_id, s.product_id, s.location_id, s.order_date, s.qty,
            COALESCE(a.is_active, FALSE)
     FROM bpk_sales s LEFT JOIN bpk_assort a
       ON a.product_id = s.product_id AND a.location_id = s.location_id
     EXCEPT ALL
     SELECT * FROM bpk_view)
) d;

\echo ''
\echo '--- Plan for the secondary-edit DELETE (should be keyed, not seq-scan-all) ---'
\echo '(inspect with reflex_explain_flush in a real run; synthetic check above asserts correctness)'

DROP TABLE IF EXISTS bpk_sales CASCADE;
DROP TABLE IF EXISTS bpk_assort CASCADE;
