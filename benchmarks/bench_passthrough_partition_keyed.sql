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
\echo '--- pg_reflex version under test (must be the build you intend to bench) ---'
SELECT extversion AS pg_reflex_version FROM pg_extension WHERE extname = 'pg_reflex';
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

\echo '--- Seeding 500K anchor rows across 20 partitions + ~12.5K assortment rows ---'
-- Decompose i so (dem_plan_id, product_id, location_id, order_date) is UNIQUE:
-- 20 partitions x 25000 rows; within a partition (product_id 0..249) x
-- (location_id 0..99) = 25000 distinct combos.
INSERT INTO bpk_sales (dem_plan_id, product_id, location_id, order_date, qty)
SELECT (((i - 1) / 25000) + 1),
       (((i - 1) % 25000) / 100),
       ((i - 1) % 100),
       DATE '2026-01-01' + (((i - 1) % 25000) % 90),
       (random() * 100)::INT
FROM generate_series(1, 500000) AS i;

INSERT INTO bpk_assort (product_id, location_id, is_active)
SELECT p, l, TRUE
FROM generate_series(0, 249) p, generate_series(0, 99) l
WHERE (p * 100 + l) % 2 = 0;   -- ~12500 of the 25000 (product, location) keys
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
\echo '=== Audit #2a — SPARSE primary UPDATE within ONE partition (dem_plan_id = 7) ==='
\echo 'Realistic bounded edit (~200 rows, cold) → keyed delete/insert pruned to the leaf.'
DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    _t0 := clock_timestamp();
    UPDATE bpk_sales SET qty = qty + 1 WHERE dem_plan_id = 7 AND product_id < 2;  -- ~200 rows
    PERFORM reflex_flush_deferred('bpk_sales');
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE 'SPARSE single-partition UPDATE + flush: % ms', ROUND(_ms, 2);
END $$;

\echo ''
\echo '=== Audit #2b — DENSE UPDATE of a whole partition (dem_plan_id = 8) ==='
\echo 'Hot leaf (100%% dirty) → atomic swap rebuild of that one leaf.'
DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    _t0 := clock_timestamp();
    UPDATE bpk_sales SET qty = qty + 1 WHERE dem_plan_id = 8;  -- all 100k rows of the leaf
    PERFORM reflex_flush_deferred('bpk_sales');
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE 'DENSE whole-partition UPDATE + flush: % ms', ROUND(_ms, 2);
END $$;

\echo ''
\echo '=== Audit #3 — secondary edit touching a small key set (~250 keys) ==='
\echo 'Expect: keyed delete/reinsert ~ O(changed keys), NOT a full rebuild of the IMV.'
DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    _t0 := clock_timestamp();
    UPDATE bpk_assort SET is_active = NOT is_active
        WHERE product_id < 5;   -- ~250 (product_id, location_id) keys
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
\echo '=== EXPLAIN ANALYZE — Audit #2: target DELETE bounded to one partition ==='
\echo 'Expect: only ONE leaf (bpk_view_7) in the plan, not all 20.'
BEGIN;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM bpk_view WHERE dem_plan_id = 7;
ROLLBACK;

\echo ''
\echo '=== EXPLAIN ANALYZE — Audit #3: keyed secondary DELETE ==='
\echo 'Expect: Index Scan via __reflex_skidx_* on (product_id, location_id), not a full seq scan of all leaves.'
BEGIN;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM bpk_view
WHERE (product_id, location_id) IN (
    SELECT product_id, location_id FROM bpk_assort WHERE product_id < 50
);
ROLLBACK;

\echo ''
\echo '=== Contrast — unpruned full-table DELETE (what the old passthrough emitted) ==='
\echo 'Expect: ALL 20 leaves appear (the cost #2 removes).'
BEGIN;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM bpk_view WHERE qty = -999999;  -- matches nothing, but planner opens every leaf
ROLLBACK;

DROP TABLE IF EXISTS bpk_sales CASCADE;
DROP TABLE IF EXISTS bpk_assort CASCADE;
