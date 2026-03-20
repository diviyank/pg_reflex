-- pg_reflex benchmark setup
-- Run once before any benchmark scripts

CREATE EXTENSION IF NOT EXISTS pg_reflex;

-- Source table for single-table benchmarks (SUM, AVG, COUNT, DISTINCT)
DROP TABLE IF EXISTS bench_orders CASCADE;
CREATE TABLE bench_orders (
    id SERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    city TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Source tables for JOIN benchmarks
DROP TABLE IF EXISTS bench_products CASCADE;
CREATE TABLE bench_products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL
);

DROP TABLE IF EXISTS bench_order_items CASCADE;
CREATE TABLE bench_order_items (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT REFERENCES bench_products(id),
    quantity INT NOT NULL,
    price NUMERIC NOT NULL
);

-- Seed products (static dimension table)
INSERT INTO bench_products (name, category)
SELECT
    'Product_' || i,
    (ARRAY['Electronics', 'Clothing', 'Food', 'Books', 'Sports'])[1 + (i % 5)]
FROM generate_series(1, 100) AS i;

-- Helper: seed bench_orders with N rows across ~10 regions
CREATE OR REPLACE FUNCTION bench_seed_orders(n INT) RETURNS VOID AS $$
DECLARE
    regions TEXT[] := ARRAY['US-East', 'US-West', 'EU-West', 'EU-East', 'APAC-North',
                            'APAC-South', 'LATAM', 'Africa', 'Middle-East', 'Canada'];
    cities TEXT[] := ARRAY['New York', 'Los Angeles', 'London', 'Berlin', 'Tokyo',
                           'Sydney', 'Sao Paulo', 'Lagos', 'Dubai', 'Toronto'];
BEGIN
    TRUNCATE bench_orders RESTART IDENTITY CASCADE;
    INSERT INTO bench_orders (region, city, amount, created_at)
    SELECT
        regions[1 + (i % 10)],
        cities[1 + (i % 10)],
        ROUND((random() * 1000)::numeric, 2),
        NOW() - (random() * interval '365 days')
    FROM generate_series(1, n) AS i;
END;
$$ LANGUAGE plpgsql;

-- Helper: seed bench_order_items with N rows referencing orders and products
CREATE OR REPLACE FUNCTION bench_seed_order_items(n INT) RETURNS VOID AS $$
BEGIN
    TRUNCATE bench_order_items RESTART IDENTITY;
    INSERT INTO bench_order_items (order_id, product_id, quantity, price)
    SELECT
        1 + (i % GREATEST((SELECT COUNT(*) FROM bench_orders)::int, 1)),
        1 + (i % 100),
        1 + (i % 10),
        ROUND((random() * 500)::numeric, 2)
    FROM generate_series(1, n) AS i;
END;
$$ LANGUAGE plpgsql;

-- Helper: clean up IMV artifacts for a given view name
CREATE OR REPLACE FUNCTION bench_cleanup_imv(vname TEXT) RETURNS VOID AS $$
BEGIN
    -- Drop triggers on source tables
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', vname);
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', '__reflex_intermediate_' || vname);
    DELETE FROM public.__reflex_ivm_reference WHERE name = vname;
    -- Drop trigger functions (ignore errors for missing functions)
    BEGIN
        EXECUTE format('DROP FUNCTION IF EXISTS __reflex_ins_trigger_%s_bench_orders() CASCADE', vname);
        EXECUTE format('DROP FUNCTION IF EXISTS __reflex_del_trigger_%s_bench_orders() CASCADE', vname);
        EXECUTE format('DROP FUNCTION IF EXISTS __reflex_upd_trigger_%s_bench_orders() CASCADE', vname);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
END;
$$ LANGUAGE plpgsql;

\echo '=== pg_reflex benchmark setup complete ==='
