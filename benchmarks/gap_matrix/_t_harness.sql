\set ON_ERROR_STOP on
-- Minimal fixture: one passthrough IMMEDIATE cell, tiny data.
DROP TABLE IF EXISTS t_src_imv, t_src_base CASCADE;
CALL gap_drop_imv('t_imv');
DROP MATERIALIZED VIEW IF EXISTS t_mv;

CREATE TABLE t_src_imv  (id int primary key, region int, grp int, qty int);
CREATE TABLE t_src_base (id int primary key, region int, grp int, qty int);
INSERT INTO t_src_imv  SELECT i, i%4, i%10, (i%50)+1 FROM generate_series(1,1000) i;
INSERT INTO t_src_base SELECT i, i%4, i%10, (i%50)+1 FROM generate_series(1,1000) i;
SELECT create_reflex_ivm('t_imv','SELECT id, region, grp, qty FROM t_src_imv WHERE qty > 0','id',NULL,'IMMEDIATE',NULL);
CREATE MATERIALIZED VIEW t_mv AS SELECT id, region, grp, qty FROM t_src_base WHERE qty > 0;

CALL gap_measure(
    run_ts        => '2026-06-05 00:00:00+00',
    layer         => 'synthetic', cell_label => 'TEST', shape => 'passthrough',
    cte           => false, p_mode => 'IMMEDIATE', partitioned => 'none', operation => 'UPDATE',
    base_rows     => 1000, edit_rows => 100,
    edit_imv_sql  => 'UPDATE t_src_imv  SET qty=qty+1 WHERE id BETWEEN 1 AND 100',
    edit_bare_sql => 'UPDATE t_src_base SET qty=qty+1 WHERE id BETWEEN 1 AND 100',
    flush_source  => 't_src_imv', imv_target => 't_imv', mv_name => 't_mv',
    fresh_sql     => 'SELECT id, region, grp, qty FROM t_src_imv WHERE qty > 0'
);

DO $$
DECLARE r bench_gap_results;
BEGIN
    SELECT * INTO r FROM bench_gap_results WHERE cell_label='TEST';
    IF r.mismatches <> 0 THEN RAISE EXCEPTION 'CORRECTNESS FAIL: mismatches=%', r.mismatches; END IF;
    IF r.imv_ms IS NULL OR r.refresh_ms IS NULL THEN RAISE EXCEPTION 'timing not recorded'; END IF;
    RAISE NOTICE 'SMOKE OK: imv=% ms bare=% ms refresh=% ms adv=%%%',
        r.imv_ms, r.bare_ms, r.refresh_ms, r.advantage_pct;
END $$;

-- DEFERRED passthrough cell
CALL gap_drop_imv('t_imv_def');
SELECT create_reflex_ivm('t_imv_def','SELECT id, region, grp, qty FROM t_src_imv WHERE qty > 0','id',NULL,'DEFERRED',NULL);
CALL gap_measure('2026-06-05 00:00:00+00','synthetic','TEST_DEF','passthrough',false,'DEFERRED','none','UPDATE',
    1000,100,
    'UPDATE t_src_imv  SET qty=qty+1 WHERE id BETWEEN 101 AND 200',
    'UPDATE t_src_base SET qty=qty+1 WHERE id BETWEEN 101 AND 200',
    't_src_imv','t_imv_def','t_mv',
    'SELECT id, region, grp, qty FROM t_src_imv WHERE qty > 0');

-- CTE passthrough cell (chain resolves at COMMIT; flush_source NULL)
CALL gap_drop_imv('t_imv_cte');
SELECT create_reflex_ivm('t_imv_cte','WITH f AS (SELECT id, region, grp, qty FROM t_src_imv WHERE qty > 0) SELECT id, region, grp, qty FROM f','id',NULL,'DEFERRED',NULL);
CALL gap_measure('2026-06-05 00:00:00+00','synthetic','TEST_CTE','passthrough',true,'DEFERRED','none','UPDATE',
    1000,100,
    'UPDATE t_src_imv  SET qty=qty+1 WHERE id BETWEEN 201 AND 300',
    'UPDATE t_src_base SET qty=qty+1 WHERE id BETWEEN 201 AND 300',
    NULL,'t_imv_cte','t_mv',
    'WITH f AS (SELECT id, region, grp, qty FROM t_src_imv WHERE qty > 0) SELECT id, region, grp, qty FROM f');

DO $$
DECLARE bad int;
BEGIN
    SELECT count(*) INTO bad FROM bench_gap_results WHERE cell_label LIKE 'TEST%' AND mismatches <> 0;
    IF bad > 0 THEN RAISE EXCEPTION '% invalid cells', bad; END IF;
    RAISE NOTICE 'ALL SMOKE CELLS CORRECT';
END $$;
