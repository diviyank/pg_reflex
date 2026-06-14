-- Benchmark: attaching a new top-level demand-plan branch whose monthly
-- sub-partitions are mostly EMPTY. Mirrors the production sop_forecast_view
-- shape (48-month window, only a few months populated).
\set ON_ERROR_STOP on

DROP TABLE IF EXISTS sim CASCADE;
SELECT drop_reflex_ivm('simv');

CREATE TABLE sim (
    dem_plan_id BIGINT NOT NULL,
    order_date  DATE   NOT NULL,
    product_id  BIGINT,
    qty         INT
) PARTITION BY LIST (dem_plan_id);

-- Existing, populated branch.
CREATE TABLE sim_172 PARTITION OF sim FOR VALUES IN (172) PARTITION BY RANGE (order_date);
CREATE TABLE sim_172_2025_01 PARTITION OF sim_172 FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
INSERT INTO sim VALUES (172, '2025-01-15', 5, 10);

SELECT create_reflex_ivm(
    'simv',
    'SELECT dem_plan_id, order_date, product_id, qty FROM sim',
    'dem_plan_id,product_id,order_date', NULL, NULL, NULL,
    ARRAY['dem_plan_id','order_date']
);

-- Build a brand-new branch (dem_plan_id = 173) with a 48-month window of
-- sub-partitions. Populate only 4 of them; the other 44 stay empty.
CREATE TABLE sim_173 (LIKE sim INCLUDING ALL) PARTITION BY RANGE (order_date);
DO $$
DECLARE
    m   date := '2025-01-01';
    lo  date;
    hi  date;
    nm  text;
    i   int := 0;
BEGIN
    WHILE i < 48 LOOP
        lo := m;
        hi := (m + interval '1 month')::date;
        nm := 'sim_173_' || to_char(lo, 'YYYY_MM');
        EXECUTE format('CREATE TABLE %I PARTITION OF sim_173 FOR VALUES FROM (%L) TO (%L)', nm, lo, hi);
        -- Populate only every 12th month -> 4 populated, 44 empty.
        IF i % 12 = 0 THEN
            EXECUTE format('INSERT INTO sim_173 VALUES (173, %L, 5, 7)', lo + 14);
        END IF;
        m := hi;
        i := i + 1;
    END LOOP;
END $$;

\echo '=== Attaching new 48-month branch (44 empty months) and flushing ==='
\timing on
ALTER TABLE sim ATTACH PARTITION sim_173 FOR VALUES IN (173);
SELECT reflex_flush_partition_source('public.sim');
\timing off

-- Correctness: 4 populated months present, 44 empty months mirrored but empty.
SELECT count(*) AS populated_rows FROM simv WHERE dem_plan_id = 173;
SELECT count(*) AS mirror_partitions
FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
JOIN pg_class c ON c.oid = i.inhrelid
WHERE p.relname = 'simv_sim_173';
