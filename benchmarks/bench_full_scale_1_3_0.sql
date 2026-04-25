-- ==========================================================================
--  Full-scale 1.3.0 INSERT/DELETE/UPDATE bench vs REFRESH MV
--  Self-contained: builds its own dataset on a fresh schema.
--
--  Shape (mimics the production 5-table-JOIN passthrough):
--    sales (10M rows) JOIN product, location, calendar, pricing
--    Output: passthrough projection (no aggregation)
--
--  Run: psql -U postgres -h localhost -d test_bench_1_3_0 -f /tmp/bench_1_3_0_full_scale.sql
-- ==========================================================================

\timing on
\pset pager off
SELECT setseed(0.42);

DROP SCHEMA IF EXISTS bench_1_3_0 CASCADE;
CREATE SCHEMA bench_1_3_0;
SET search_path = bench_1_3_0, public;

-- ---------------------------------------------------------------------
-- Build dimensions
-- ---------------------------------------------------------------------
\echo 'Building dimensions...'

CREATE TABLE product (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    category_id BIGINT NOT NULL,
    cost NUMERIC(10,2) NOT NULL
);
INSERT INTO product
SELECT i, 'product_' || i, (i % 100) + 1, ROUND((random() * 100 + 1)::numeric, 2)
FROM generate_series(1, 100000) AS i;

CREATE TABLE location (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    country_id BIGINT NOT NULL
);
INSERT INTO location
SELECT i, 'loc_' || i, (i % 50) + 1
FROM generate_series(1, 1000) AS i;

CREATE TABLE pricing (
    product_id BIGINT NOT NULL,
    location_id BIGINT NOT NULL,
    base_price NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (product_id, location_id)
);
INSERT INTO pricing
SELECT p.id, l.id, ROUND((random() * 50 + 10)::numeric, 2)
FROM product p
CROSS JOIN (SELECT id FROM location LIMIT 100) l;

CREATE TABLE calendar (
    order_date DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    week INT NOT NULL,
    isoyear INT NOT NULL
);
INSERT INTO calendar
SELECT d::date,
       EXTRACT(YEAR FROM d)::int,
       EXTRACT(MONTH FROM d)::int,
       EXTRACT(WEEK FROM d)::int,
       EXTRACT(ISOYEAR FROM d)::int
FROM generate_series('2020-01-01'::date, '2028-12-31'::date, '1 day') AS d;

-- ---------------------------------------------------------------------
-- Build fact (10M rows, with FK to all dims)
-- ---------------------------------------------------------------------
\echo 'Building 10M-row fact table...'

CREATE TABLE sales (
    id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES product(id),
    location_id BIGINT NOT NULL REFERENCES location(id),
    order_date DATE NOT NULL REFERENCES calendar(order_date),
    qty INT NOT NULL,
    qty_ub INT NOT NULL,
    qty_lb INT NOT NULL
);

INSERT INTO sales (product_id, location_id, order_date, qty, qty_ub, qty_lb)
SELECT
    (random() * 99999 + 1)::bigint,
    (random() * 99 + 1)::bigint,
    '2024-01-01'::date + ((random() * 1000)::int || ' days')::interval,
    (random() * 100)::int,
    (random() * 120)::int,
    (random() * 80)::int
FROM generate_series(1, 10000000);

CREATE INDEX ix_sales_product ON sales(product_id);
CREATE INDEX ix_sales_location ON sales(location_id);
CREATE INDEX ix_sales_order_date ON sales(order_date);

ANALYZE product;
ANALYZE location;
ANALYZE pricing;
ANALYZE calendar;
ANALYZE sales;

\echo 'Building IMV + matview baseline...'

-- Matview baseline
CREATE MATERIALIZED VIEW sales_view AS
SELECT
    s.id,
    s.product_id,
    s.location_id,
    s.order_date,
    c.year,
    c.month,
    c.week,
    p.category_id,
    l.country_id,
    s.qty,
    s.qty * COALESCE(pr.base_price, 0) AS turnover,
    s.qty_ub,
    s.qty_lb
FROM sales s
JOIN product p ON p.id = s.product_id
JOIN location l ON l.id = s.location_id
JOIN calendar c ON c.order_date = s.order_date
LEFT JOIN pricing pr ON pr.product_id = s.product_id AND pr.location_id = s.location_id;
ANALYZE sales_view;
SELECT count(*) AS matview_rows FROM sales_view;

-- IMV (IMMEDIATE mode for direct comparison)
SELECT create_reflex_ivm(
    'bench_1_3_0.sales_reflex',
    'SELECT
        s.id,
        s.product_id,
        s.location_id,
        s.order_date,
        c.year,
        c.month,
        c.week,
        p.category_id,
        l.country_id,
        s.qty,
        s.qty * COALESCE(pr.base_price, 0) AS turnover,
        s.qty_ub,
        s.qty_lb
    FROM sales s
    JOIN product p ON p.id = s.product_id
    JOIN location l ON l.id = s.location_id
    JOIN calendar c ON c.order_date = s.order_date
    LEFT JOIN pricing pr ON pr.product_id = s.product_id AND pr.location_id = s.location_id',
    'id'
);
ANALYZE sales_reflex;
SELECT count(*) AS imv_rows FROM sales_reflex;

-- ---------------------------------------------------------------------
-- REFRESH baseline
-- ---------------------------------------------------------------------
\echo ''
\echo '--- REFRESH baseline ---'

CREATE TEMP TABLE _baseline (refresh_ms NUMERIC);
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
BEGIN
    REFRESH MATERIALIZED VIEW sales_view;  -- warm
    t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW sales_view;
    t1 := clock_timestamp();
    INSERT INTO _baseline VALUES (EXTRACT(EPOCH FROM t1 - t0) * 1000);
END $$;
SELECT ROUND(refresh_ms, 0) || ' ms' AS refresh_baseline FROM _baseline;

-- ---------------------------------------------------------------------
-- Bench helpers
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _gen_insert_sql(batch_size INTEGER)
RETURNS TEXT AS $$
BEGIN
    RETURN format(
        'INSERT INTO sales (product_id, location_id, order_date, qty, qty_ub, qty_lb)
         SELECT
             (random() * 99999 + 1)::bigint,
             (random() * 99 + 1)::bigint,
             ''2029-01-01''::date,
             (random() * 100)::int,
             (random() * 120)::int,
             (random() * 80)::int
         FROM generate_series(1, %s)',
        batch_size
    );
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _cleanup() RETURNS VOID AS $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM sales WHERE order_date = '2029-01-01'::date;
    DELETE FROM sales_reflex WHERE order_date = '2029-01-01'::date;
    SET LOCAL session_replication_role = DEFAULT;
END $$ LANGUAGE plpgsql;

-- Pre-create 2029-01-01 in calendar so FK insert succeeds
INSERT INTO calendar VALUES ('2029-01-01', 2029, 1, 1, 2029) ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION bench_op(p_op TEXT, p_batch INTEGER)
RETURNS TABLE(operation TEXT, batch INT, reflex_ms NUMERIC, raw_ms NUMERIC, refresh_ms NUMERIC, advantage_pct NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    rms NUMERIC; raw NUMERIC; baseline NUMERIC;
    insert_sql TEXT;
BEGIN
    SELECT _baseline.refresh_ms INTO baseline FROM _baseline LIMIT 1;
    insert_sql := _gen_insert_sql(p_batch);

    IF p_op = 'INSERT' THEN
        t0 := clock_timestamp();
        EXECUTE insert_sql;
        t1 := clock_timestamp();
        rms := EXTRACT(EPOCH FROM t1 - t0) * 1000;
        PERFORM _cleanup();

        SET LOCAL session_replication_role = replica;
        t0 := clock_timestamp();
        EXECUTE insert_sql;
        t1 := clock_timestamp();
        raw := EXTRACT(EPOCH FROM t1 - t0) * 1000;
        DELETE FROM sales WHERE order_date = '2029-01-01'::date;
        SET LOCAL session_replication_role = DEFAULT;
    ELSIF p_op = 'DELETE' THEN
        EXECUTE insert_sql;
        t0 := clock_timestamp();
        DELETE FROM sales WHERE order_date = '2029-01-01'::date;
        t1 := clock_timestamp();
        rms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

        SET LOCAL session_replication_role = replica;
        EXECUTE insert_sql;
        t0 := clock_timestamp();
        DELETE FROM sales WHERE order_date = '2029-01-01'::date;
        t1 := clock_timestamp();
        raw := EXTRACT(EPOCH FROM t1 - t0) * 1000;
        SET LOCAL session_replication_role = DEFAULT;
        PERFORM _cleanup();
    ELSE  -- UPDATE
        EXECUTE insert_sql;
        t0 := clock_timestamp();
        UPDATE sales SET qty = qty + 1 WHERE order_date = '2029-01-01'::date;
        t1 := clock_timestamp();
        rms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

        SET LOCAL session_replication_role = replica;
        t0 := clock_timestamp();
        UPDATE sales SET qty = qty + 1 WHERE order_date = '2029-01-01'::date;
        t1 := clock_timestamp();
        raw := EXTRACT(EPOCH FROM t1 - t0) * 1000;
        SET LOCAL session_replication_role = DEFAULT;
        PERFORM _cleanup();
    END IF;

    operation := p_op;
    batch := p_batch;
    reflex_ms := ROUND(rms, 0);
    raw_ms := ROUND(raw, 0);
    refresh_ms := ROUND(baseline, 0);
    advantage_pct := ROUND(100.0 * (1.0 - rms / NULLIF(raw + baseline, 0)), 1);
    RETURN NEXT;
END $$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------
-- Run benchmark
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS _bench_results;
CREATE TABLE _bench_results (operation TEXT, batch INT, reflex_ms NUMERIC, raw_ms NUMERIC, refresh_ms NUMERIC, advantage_pct NUMERIC);

\echo ''
\echo '=== INSERT bench ==='
INSERT INTO _bench_results SELECT * FROM bench_op('INSERT', 1000);
INSERT INTO _bench_results SELECT * FROM bench_op('INSERT', 10000);
INSERT INTO _bench_results SELECT * FROM bench_op('INSERT', 100000);
INSERT INTO _bench_results SELECT * FROM bench_op('INSERT', 500000);
INSERT INTO _bench_results SELECT * FROM bench_op('INSERT', 1000000);

\echo ''
\echo '=== DELETE bench ==='
INSERT INTO _bench_results SELECT * FROM bench_op('DELETE', 1000);
INSERT INTO _bench_results SELECT * FROM bench_op('DELETE', 10000);
INSERT INTO _bench_results SELECT * FROM bench_op('DELETE', 100000);
INSERT INTO _bench_results SELECT * FROM bench_op('DELETE', 500000);
INSERT INTO _bench_results SELECT * FROM bench_op('DELETE', 1000000);

\echo ''
\echo '=== UPDATE bench ==='
INSERT INTO _bench_results SELECT * FROM bench_op('UPDATE', 1000);
INSERT INTO _bench_results SELECT * FROM bench_op('UPDATE', 10000);
INSERT INTO _bench_results SELECT * FROM bench_op('UPDATE', 100000);
INSERT INTO _bench_results SELECT * FROM bench_op('UPDATE', 500000);
INSERT INTO _bench_results SELECT * FROM bench_op('UPDATE', 1000000);

-- ---------------------------------------------------------------------
-- Print results
-- ---------------------------------------------------------------------
\echo ''
\echo '======================================================================'
\echo '  RESULTS — pg_reflex 1.3.0 (10M-row 5-table-JOIN passthrough)'
\echo '======================================================================'
SELECT operation, batch, reflex_ms || ' ms' AS reflex, raw_ms || ' ms' AS raw, refresh_ms || ' ms' AS refresh, advantage_pct || '%' AS advantage
FROM _bench_results
ORDER BY CASE operation WHEN 'INSERT' THEN 1 WHEN 'DELETE' THEN 2 WHEN 'UPDATE' THEN 3 END, batch;

-- ---------------------------------------------------------------------
-- Correctness check
-- ---------------------------------------------------------------------
\echo ''
\echo '--- Correctness check ---'
SELECT reflex_reconcile('bench_1_3_0.sales_reflex');
REFRESH MATERIALIZED VIEW sales_view;

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: ' || COUNT(*) END AS correctness
FROM (
    SELECT * FROM sales_reflex EXCEPT ALL SELECT * FROM sales_view
) diff;
