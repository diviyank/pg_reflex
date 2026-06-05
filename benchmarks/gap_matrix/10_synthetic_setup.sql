\set ON_ERROR_STOP on
\timing off
\if :{?BASE_ROWS}
\else
\set BASE_ROWS 1000000
\endif
SELECT setseed(0.13);
SET bench.base_rows = :BASE_ROWS;

-- Shape registry. body_tmpl uses %1$s for the source table name.
-- unique_cols / part_by depend on partitioning (region must lead the key when LIST).
DROP TABLE IF EXISTS gap_shape;
CREATE TABLE gap_shape (
    skey       text PRIMARY KEY,   -- e.g. 'p_nc_np'
    shape      text,               -- passthrough | classic
    cte        boolean,
    partitioned text,              -- none | LIST
    body_tmpl  text,               -- %1$s = source table
    unique_cols text,              -- NULL for aggregate
    part_by    text                -- NULL or 'region'
);

INSERT INTO gap_shape VALUES
 ('p_nc_np','passthrough',false,'none',
   'SELECT id, region, grp, qty FROM %1$s WHERE qty > 0','id',NULL),
 ('p_nc_lp','passthrough',false,'LIST',
   'SELECT id, region, grp, qty FROM %1$s WHERE qty > 0','region,id','region'),
 ('p_ct_np','passthrough',true,'none',
   'WITH f AS (SELECT id, region, grp, qty FROM %1$s WHERE qty > 0) SELECT id, region, grp, qty FROM f','id',NULL),
 ('p_ct_lp','passthrough',true,'LIST',
   'WITH f AS (SELECT id, region, grp, qty FROM %1$s WHERE qty > 0) SELECT id, region, grp, qty FROM f','region,id','region'),
 ('c_nc_np','classic',false,'none',
   'SELECT region, grp, sum(qty) AS s, count(*) AS c FROM %1$s WHERE qty > 0 GROUP BY region, grp',NULL,NULL),
 ('c_nc_lp','classic',false,'LIST',
   'SELECT region, grp, sum(qty) AS s, count(*) AS c FROM %1$s WHERE qty > 0 GROUP BY region, grp',NULL,'region'),
 ('c_ct_np','classic',true,'none',
   'WITH f AS (SELECT region, grp, qty FROM %1$s WHERE qty > 0) SELECT region, grp, sum(qty) AS s, count(*) AS c FROM f GROUP BY region, grp',NULL,NULL),
 ('c_ct_lp','classic',true,'LIST',
   'WITH f AS (SELECT region, grp, qty FROM %1$s WHERE qty > 0) SELECT region, grp, sum(qty) AS s, count(*) AS c FROM f GROUP BY region, grp',NULL,'region');

-- Build *_imv and *_base sources (+ partitions) and mv_<skey>. qty in [1,100]
-- so every seeded row is IN the WHERE qty>0 filter (FLIP_OUT moves them out).
DO $$
DECLARE
    s gap_shape; n bigint := current_setting('bench.base_rows')::bigint;
    src text; tbl text; p int;
BEGIN
    FOR s IN SELECT * FROM gap_shape LOOP
        FOREACH src IN ARRAY ARRAY['imv','base'] LOOP
            tbl := format('%s_%s', s.skey, src);
            EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', tbl);
            IF s.partitioned = 'LIST' THEN
                EXECUTE format(
                  'CREATE TABLE %I (id int NOT NULL, region int NOT NULL, grp int NOT NULL, qty int) PARTITION BY LIST (region)', tbl);
                FOR p IN 0..19 LOOP
                    EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%s)', tbl||'_'||p, tbl, p);
                END LOOP;
            ELSE
                EXECUTE format(
                  'CREATE TABLE %I (id int PRIMARY KEY, region int NOT NULL, grp int NOT NULL, qty int)', tbl);
            END IF;
            EXECUTE format(
              'INSERT INTO %I SELECT i, i%%20, i%%1000, (random()*99)::int + 1 FROM generate_series(1,%s) i', tbl, n);
            EXECUTE format('ANALYZE %I', tbl);
        END LOOP;
        -- MV over *_base
        EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %I', 'mv_'||s.skey);
        EXECUTE format('CREATE MATERIALIZED VIEW %I AS %s',
                       'mv_'||s.skey, format(s.body_tmpl, s.skey||'_base'));
        EXECUTE format('ANALYZE %I', 'mv_'||s.skey);
    END LOOP;
END $$;

\echo '=== synthetic setup complete ==='
SELECT skey, shape, cte, partitioned FROM gap_shape ORDER BY skey;
