-- pg_reflex maintenance benchmark matrix — feature dimensions, with/without,
-- and combinations, across edit volumes 100 .. 1,000,000.
--
-- Dimensions (each present AND absent, and combined):
--   shape:        passthrough | aggregate | CTE | passthrough+LEFT-JOIN-secondary
--   partitioning: none | LIST | RANGE
--   mode:         IMMEDIATE | DEFERRED
--   edit volume:  100, 1k, 10k, 100k, 1M (disjoint id ranges; no reset needed)
--
-- Every case prints "RESULT | <label> | <ms> | mismatches=N". N MUST be 0
-- (two-way EXCEPT ALL vs a fresh recompute). Does NOT drop tables at the end so
-- a 1M follow-up can reuse them; run benchmarks/bench_features_cleanup.sql after.
--
--   psql -h localhost -p <port> -d <db> -v MAXVOL=100000 -f benchmarks/bench_features_matrix.sql
--
\set ON_ERROR_STOP on
\timing off
\echo '=== pg_reflex version under test (must be the intended build) ==='
SELECT extversion FROM pg_extension WHERE extname = 'pg_reflex';
\if :{?MAXVOL}
\else
\set MAXVOL 1000000
\endif
SELECT setseed(0.13);

CREATE OR REPLACE FUNCTION bench_run(_label text, _edit text, _flush_source text, _imv text, _fresh text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE _t0 timestamptz; _ms numeric; _mm bigint; _src text;
BEGIN
    _t0 := clock_timestamp();
    EXECUTE _edit;
    -- _flush_source may be a comma list: flush in dependency order so a chained
    -- IMV (e.g. a CTE's inner IMV) is maintained after its base source. NULL =>
    -- IMMEDIATE (already applied by the per-statement trigger).
    IF _flush_source IS NOT NULL THEN
        FOREACH _src IN ARRAY string_to_array(_flush_source, ',') LOOP
            EXECUTE format('SELECT reflex_flush_deferred(%L)', trim(_src));
        END LOOP;
    END IF;
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    EXECUTE format(
        'SELECT count(*) FROM ( (SELECT * FROM %1$s EXCEPT ALL (%2$s)) '
        || 'UNION ALL ((%2$s) EXCEPT ALL SELECT * FROM %1$s) ) d',
        _imv, _fresh) INTO _mm;
    RAISE NOTICE 'RESULT | % | % ms | mismatches=%',
        rpad(_label, 32), lpad(to_char(_ms, 'FM9999990.0'), 9), _mm;
END $$;

\echo ''
\echo '=== seeding sources (≈15M rows total; takes a minute) ==='
-- 2M-row sources (sweep to 1M). region=id%20 (LIST), grp=id%1000 (GROUP BY).
CREATE TABLE s_ptnp (id INT PRIMARY KEY, region INT NOT NULL, grp INT NOT NULL, qty INT);
INSERT INTO s_ptnp SELECT i, i%20, i%1000, (random()*100)::INT FROM generate_series(1,2000000) i;
ANALYZE s_ptnp;

CREATE TABLE s_ptl (id INT NOT NULL, region INT NOT NULL, grp INT NOT NULL, qty INT) PARTITION BY LIST (region);
DO $$ BEGIN FOR p IN 0..19 LOOP EXECUTE format('CREATE TABLE s_ptl_%s PARTITION OF s_ptl FOR VALUES IN (%s)', p, p); END LOOP; END $$;
INSERT INTO s_ptl SELECT i, i%20, i%1000, (random()*100)::INT FROM generate_series(1,2000000) i;
ANALYZE s_ptl;

CREATE TABLE s_agnp (id INT PRIMARY KEY, grp INT NOT NULL, qty INT);
INSERT INTO s_agnp SELECT i, i%1000, (random()*100)::INT FROM generate_series(1,2000000) i;
ANALYZE s_agnp;

CREATE TABLE s_agl (id INT NOT NULL, region INT NOT NULL, qty INT) PARTITION BY LIST (region);
DO $$ BEGIN FOR p IN 0..19 LOOP EXECUTE format('CREATE TABLE s_agl_%s PARTITION OF s_agl FOR VALUES IN (%s)', p, p); END LOOP; END $$;
INSERT INTO s_agl SELECT i, i%20, (random()*100)::INT FROM generate_series(1,2000000) i;
ANALYZE s_agl;

CREATE TABLE s_cte (id INT PRIMARY KEY, grp INT NOT NULL, qty INT, flag BOOL);
INSERT INTO s_cte SELECT i, i%1000, (random()*100)::INT, (i%2=0) FROM generate_series(1,2000000) i;
ANALYZE s_cte;

CREATE TABLE s_ctel (id INT NOT NULL, region INT NOT NULL, grp INT NOT NULL, qty INT, flag BOOL) PARTITION BY LIST (region);
DO $$ BEGIN FOR p IN 0..19 LOOP EXECUTE format('CREATE TABLE s_ctel_%s PARTITION OF s_ctel FOR VALUES IN (%s)', p, p); END LOOP; END $$;
INSERT INTO s_ctel SELECT i, i%20, i%1000, (random()*100)::INT, (i%2=0) FROM generate_series(1,2000000) i;
ANALYZE s_ctel;

-- passthrough + secondary: non-partitioned anchor (G) and LIST anchor (I)
CREATE TABLE s_anch (id INT PRIMARY KEY, product_id INT NOT NULL, location_id INT NOT NULL, qty INT);
INSERT INTO s_anch SELECT i, i%2000, i%500, (random()*100)::INT FROM generate_series(1,2000000) i;
CREATE TABLE s_sec (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL);
INSERT INTO s_sec SELECT p, l, TRUE FROM generate_series(0,1999) p, generate_series(0,499) l WHERE (p*500+l)%97 = 0;
ANALYZE s_anch; ANALYZE s_sec;

CREATE TABLE s_anchp (id INT NOT NULL, region INT NOT NULL, product_id INT NOT NULL, location_id INT NOT NULL, qty INT) PARTITION BY LIST (region);
DO $$ BEGIN FOR p IN 0..19 LOOP EXECUTE format('CREATE TABLE s_anchp_%s PARTITION OF s_anchp FOR VALUES IN (%s)', p, p); END LOOP; END $$;
INSERT INTO s_anchp SELECT i, i%20, i%2000, i%500, (random()*100)::INT FROM generate_series(1,2000000) i;
CREATE TABLE s_secp (product_id INT NOT NULL, location_id INT NOT NULL, is_active BOOL);
INSERT INTO s_secp SELECT p, l, TRUE FROM generate_series(0,1999) p, generate_series(0,499) l WHERE (p*500+l)%97 = 0;
ANALYZE s_anchp; ANALYZE s_secp;

-- RANGE + IMMEDIATE sources (smaller; capped at 100k)
CREATE TABLE s_agr (id INT NOT NULL, d DATE NOT NULL, qty INT) PARTITION BY RANGE (d);
DO $$ DECLARE q INT; lo DATE; BEGIN FOR q IN 0..7 LOOP lo := DATE '2026-01-01' + (q*90);
  EXECUTE format('CREATE TABLE s_agr_%s PARTITION OF s_agr FOR VALUES FROM (%L) TO (%L)', q, lo, lo+90); END LOOP; END $$;
INSERT INTO s_agr SELECT i, DATE '2026-01-01' + (i % 720), (random()*100)::INT FROM generate_series(1,800000) i;
ANALYZE s_agr;

CREATE TABLE s_pti (id INT NOT NULL, region INT NOT NULL, grp INT NOT NULL, qty INT) PARTITION BY LIST (region);
DO $$ BEGIN FOR p IN 0..19 LOOP EXECUTE format('CREATE TABLE s_pti_%s PARTITION OF s_pti FOR VALUES IN (%s)', p, p); END LOOP; END $$;
INSERT INTO s_pti SELECT i, i%20, i%1000, (random()*100)::INT FROM generate_series(1,400000) i;
ANALYZE s_pti;

\echo ''
\echo '=== creating IMVs (with/without features + combinations) ==='
SELECT create_reflex_ivm('a_ptnp','SELECT id, region, grp, qty FROM s_ptnp','id',NULL,'DEFERRED',NULL);
SELECT create_reflex_ivm('b_ptl','SELECT id, region, grp, qty FROM s_ptl','region,id',NULL,'DEFERRED',NULL,ARRAY['region']);
SELECT create_reflex_ivm('d_agnp','SELECT grp, sum(qty) AS s, count(*) AS c FROM s_agnp GROUP BY grp',NULL,NULL,'DEFERRED',NULL);
SELECT create_reflex_ivm('e_agl','SELECT region, sum(qty) AS s, count(*) AS c FROM s_agl GROUP BY region',NULL,NULL,'DEFERRED',NULL,ARRAY['region']);
SELECT create_reflex_ivm('h_cte','WITH f AS (SELECT id, grp, qty FROM s_cte WHERE flag) SELECT id, grp, qty FROM f','id',NULL,'DEFERRED',NULL);
SELECT create_reflex_ivm('j_ctel','WITH f AS (SELECT id, region, grp, qty FROM s_ctel WHERE flag) SELECT id, region, grp, qty FROM f','region,id',NULL,'DEFERRED',NULL,ARRAY['region']);
SELECT create_reflex_ivm('g_sec','SELECT a.id, a.product_id, a.location_id, a.qty, COALESCE(x.is_active,FALSE) AS act FROM s_anch a LEFT JOIN s_sec x ON x.product_id=a.product_id AND x.location_id=a.location_id','product_id,location_id,id',NULL,'DEFERRED',NULL);
SELECT create_reflex_ivm('i_secl','SELECT a.region, a.product_id, a.location_id, a.id, a.qty, COALESCE(x.is_active,FALSE) AS act FROM s_anchp a LEFT JOIN s_secp x ON x.product_id=a.product_id AND x.location_id=a.location_id','region,product_id,location_id,id',NULL,'DEFERRED',NULL,ARRAY['region']);
SELECT create_reflex_ivm('f_agr','SELECT d, sum(qty) AS s, count(*) AS c FROM s_agr GROUP BY d',NULL,NULL,'DEFERRED',NULL,ARRAY['d']);
SELECT create_reflex_ivm('c_pti','SELECT id, region, grp, qty FROM s_pti','region,id',NULL,'IMMEDIATE',NULL,ARRAY['region']);
ANALYZE a_ptnp; ANALYZE b_ptl; ANALYZE d_agnp; ANALYZE e_agl; ANALYZE h_cte; ANALYZE j_ctel; ANALYZE g_sec; ANALYZE i_secl; ANALYZE f_agr; ANALYZE c_pti;

\echo ''
\echo '======================================================================'
\echo 'RESULTS  (label | flush ms | mismatches)   — mismatches MUST be 0'
\echo '======================================================================'
SET bench.maxvol = :MAXVOL;

DO $$
DECLARE
    vols BIGINT[] := ARRAY[100, 1000, 10000, 100000, 1000000];
    v BIGINT; lo BIGINT; hi BIGINT; maxv BIGINT := current_setting('bench.maxvol')::bigint;
BEGIN
    FOREACH v IN ARRAY vols LOOP
        IF v > maxv THEN CONTINUE; END IF;
        lo := v + 1; hi := 2*v;
        PERFORM bench_run(format('A pt/none/DEF      v=%s', v),
            format('UPDATE s_ptnp SET qty=qty+1 WHERE id BETWEEN %s AND %s', lo, hi),
            's_ptnp','a_ptnp','SELECT id, region, grp, qty FROM s_ptnp');
        PERFORM bench_run(format('B pt/LIST/DEF      v=%s', v),
            format('UPDATE s_ptl SET qty=qty+1 WHERE id BETWEEN %s AND %s', lo, hi),
            's_ptl','b_ptl','SELECT id, region, grp, qty FROM s_ptl');
        PERFORM bench_run(format('D agg/none/DEF     v=%s', v),
            format('UPDATE s_agnp SET qty=qty+1 WHERE id BETWEEN %s AND %s', lo, hi),
            's_agnp','d_agnp','SELECT grp, sum(qty) AS s, count(*) AS c FROM s_agnp GROUP BY grp');
        PERFORM bench_run(format('E agg/LIST/DEF     v=%s', v),
            format('UPDATE s_agl SET qty=qty+1 WHERE id BETWEEN %s AND %s', lo, hi),
            's_agl','e_agl','SELECT region, sum(qty) AS s, count(*) AS c FROM s_agl GROUP BY region');
        -- NOTE: CTE IMVs (H, J) are chained (an inner __cte_<alias> IMV feeds the
        -- outer). Their cascade resolves at COMMIT, which this in-transaction
        -- driver cannot trigger, so they are measured in the autocommit section
        -- below instead of here.
        PERFORM bench_run(format('I pt+sec/LIST/DEF  v=%s', v),
            format('UPDATE s_anchp SET qty=qty+1 WHERE id BETWEEN %s AND %s', lo, hi),
            's_anchp','i_secl','SELECT a.region, a.product_id, a.location_id, a.id, a.qty, COALESCE(x.is_active,FALSE) AS act FROM s_anchp a LEFT JOIN s_secp x ON x.product_id=a.product_id AND x.location_id=a.location_id');
        PERFORM bench_run(format('G pt+sec/none/DEF  v=%s', v),
            format('UPDATE s_anch SET qty=qty+1 WHERE id BETWEEN %s AND %s', lo, hi),
            's_anch','g_sec','SELECT a.id, a.product_id, a.location_id, a.qty, COALESCE(x.is_active,FALSE) AS act FROM s_anch a LEFT JOIN s_sec x ON x.product_id=a.product_id AND x.location_id=a.location_id');
    END LOOP;

    -- capped at 100k: RANGE aggregate, and IMMEDIATE passthrough
    FOREACH v IN ARRAY vols LOOP
        IF v > LEAST(maxv, 100000) THEN CONTINUE; END IF;
        lo := v + 1; hi := 2*v;
        PERFORM bench_run(format('F agg/RANGE/DEF    v=%s', v),
            format('UPDATE s_agr SET qty=qty+1 WHERE id BETWEEN %s AND %s', lo, hi),
            's_agr','f_agr','SELECT d, sum(qty) AS s, count(*) AS c FROM s_agr GROUP BY d');
        PERFORM bench_run(format('C pt/LIST/IMMEDIATE v=%s', v),
            format('UPDATE s_pti SET qty=qty+3 WHERE id BETWEEN %s AND %s', lo, hi),
            NULL,'c_pti','SELECT id, region, grp, qty FROM s_pti');
    END LOOP;
END $$;

-- Secondary-source edits at increasing key volumes (audit #3), non-part + LIST.
DO $$
DECLARE ks INT[] := ARRAY[2, 20, 200, 2000]; k INT;
BEGIN
    FOREACH k IN ARRAY ks LOOP
        PERFORM bench_run(format('G sec-edit/none    prod<%s', k),
            format('UPDATE s_sec SET is_active = NOT is_active WHERE product_id < %s', k),
            's_sec','g_sec',
            'SELECT a.id, a.product_id, a.location_id, a.qty, COALESCE(x.is_active,FALSE) AS act FROM s_anch a LEFT JOIN s_sec x ON x.product_id=a.product_id AND x.location_id=a.location_id');
        PERFORM bench_run(format('I sec-edit/LIST    prod<%s', k),
            format('UPDATE s_secp SET is_active = NOT is_active WHERE product_id < %s', k),
            's_secp','i_secl',
            'SELECT a.region, a.product_id, a.location_id, a.id, a.qty, COALESCE(x.is_active,FALSE) AS act FROM s_anchp a LEFT JOIN s_secp x ON x.product_id=a.product_id AND x.location_id=a.location_id');
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- CTE IMVs (chained: inner __cte_<alias> IMV -> outer). The full chain is
-- maintained at COMMIT, so each edit is its own autocommitted statement here;
-- \timing on the UPDATE includes the COMMIT-time cascade. Correctness checked
-- after. (Disjoint id ranges so this composes with the sweep above.)
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== CTE IMVs (autocommit; UPDATE time includes COMMIT-cascade) ==='
\timing on
UPDATE s_cte  SET qty = qty + 1 WHERE id BETWEEN 1500001 AND 1510000 AND flag;   -- H cte/none ~10k
SELECT 'H cte/none mismatches' AS k, count(*) FROM ((SELECT * FROM h_cte EXCEPT ALL (WITH f AS (SELECT id,grp,qty FROM s_cte WHERE flag) SELECT id,grp,qty FROM f)) UNION ALL ((WITH f AS (SELECT id,grp,qty FROM s_cte WHERE flag) SELECT id,grp,qty FROM f) EXCEPT ALL SELECT * FROM h_cte)) d;
UPDATE s_ctel SET qty = qty + 1 WHERE id BETWEEN 1500001 AND 1510000 AND flag;   -- J cte/LIST ~10k
SELECT 'J cte/LIST mismatches' AS k, count(*) FROM ((SELECT * FROM j_ctel EXCEPT ALL (WITH f AS (SELECT id,region,grp,qty FROM s_ctel WHERE flag) SELECT id,region,grp,qty FROM f)) UNION ALL ((WITH f AS (SELECT id,region,grp,qty FROM s_ctel WHERE flag) SELECT id,region,grp,qty FROM f) EXCEPT ALL SELECT * FROM j_ctel)) d;
\timing off

\echo ''
\echo '=== matrix done (tables kept; run bench_features_cleanup.sql to drop) ==='
