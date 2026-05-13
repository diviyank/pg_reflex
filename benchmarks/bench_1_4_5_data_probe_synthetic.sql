-- ==========================================================================
-- Bench: 1.4.5 data-probe + REFRESH comparison on production-scale synthetic
--
-- Goal: prove on synthetic data that the 1.4.5 data-probe converts the
-- 405-s yse regression class into normal incremental-update times.
-- Compare against REFRESH MATERIALIZED VIEW as the reference baseline.
--
-- Production-scale (per customer feedback): 2M source rows, ~1.5M intermediate
-- under the "dominant" parent, ~500K under the secondary. Each UPDATE on the
-- dominant parent touches ~75% of the intermediate.
--
-- Three phases, each with its own VACUUM FULL between to eliminate dead-tuple
-- bloat carry-over (otherwise AFTER timings drift up across iterations and
-- mask the BEFORE/AFTER signal):
--
--   1. AFTER  — 1.4.5 default. Probe ON, fk_id promoted, MERGE uses `=`.
--   2. BEFORE — 1.4.4 regression. Probe-promoted columns stripped from
--               not_null_columns, MERGE falls back to IS NOT DISTINCT FROM.
--   3. REFRESH — baseline. REFRESH MATERIALIZED VIEW on a sibling matview
--                with the same query body.
--
-- Run via: psql -h <host> -U <user> -d <isolated_test_db> -f <this>
--          (must NOT be run on the customer DB)
-- ==========================================================================
\timing on
\set ON_ERROR_STOP on

DROP SCHEMA IF EXISTS bench_probe CASCADE;
CREATE SCHEMA bench_probe;

CREATE TABLE bench_probe.parent (
    id     BIGINT NOT NULL PRIMARY KEY,
    label  TEXT   NOT NULL,
    status TEXT   NOT NULL
);

-- child.fk_id is CATALOG-NULLABLE but the INNER JOIN forces non-NULL.
CREATE TABLE bench_probe.child (
    id     BIGINT NOT NULL PRIMARY KEY,
    fk_id  BIGINT,
    grp_a  INT    NOT NULL,
    grp_b  INT    NOT NULL,
    qty    INT    NOT NULL
);

INSERT INTO bench_probe.parent VALUES (1, 'p1', 'validated'), (2, 'p2', 'draft');

-- 1.5M under fk_id=1, 500K under fk_id=2 → 2M total, 75% concentration on dp=1.
\echo '=== Loading 2M child rows ==='
INSERT INTO bench_probe.child (id, fk_id, grp_a, grp_b, qty)
SELECT g, 1, g, (g / 1000), 1 + (g % 100)
FROM generate_series(1, 1500000) g;
INSERT INTO bench_probe.child (id, fk_id, grp_a, grp_b, qty)
SELECT 1500000 + g, 2, 1500000 + g, ((1500000 + g) / 1000), 1 + (g % 100)
FROM generate_series(1, 500000) g;

CREATE INDEX ix_child_fk ON bench_probe.child(fk_id);
ANALYZE bench_probe.parent;
ANALYZE bench_probe.child;

-- ----------------------------------------------------------------------
-- Reference: a regular materialized view with the same query.
-- REFRESH on this is the "best we could do without incremental".
-- ----------------------------------------------------------------------
\echo ''
\echo '=== Creating reference REGULAR MATERIALIZED VIEW for REFRESH bench ==='
CREATE MATERIALIZED VIEW bench_probe.kv_ref AS
SELECT c.fk_id, c.grp_a, c.grp_b, SUM(c.qty) AS total
FROM bench_probe.child c
INNER JOIN bench_probe.parent p ON p.id = c.fk_id
GROUP BY c.fk_id, c.grp_a, c.grp_b;
ANALYZE bench_probe.kv_ref;

-- ----------------------------------------------------------------------
-- pg_reflex IMV (will trigger the 1.4.5 data-probe)
-- ----------------------------------------------------------------------
\echo ''
\echo '=== Creating pg_reflex IMV (probe runs automatically at create time) ==='
SELECT public.create_reflex_ivm(
    'bench_probe.kv',
    'SELECT c.fk_id, c.grp_a, c.grp_b, SUM(c.qty) AS total
     FROM bench_probe.child c
     INNER JOIN bench_probe.parent p ON p.id = c.fk_id
     GROUP BY c.fk_id, c.grp_a, c.grp_b',
    NULL,        -- unique_columns
    'UNLOGGED',  -- storage
    'IMMEDIATE'  -- mode
);

\echo ''
\echo '=== Probe outcome ==='
SELECT (aggregations::jsonb->'not_null_columns')::text AS not_null_cols
FROM public.__reflex_ivm_reference WHERE name = 'bench_probe.kv';

\echo ''
\echo '=== Sizes ==='
SELECT
  (SELECT count(*) FROM bench_probe.child)                          AS child_rows,
  (SELECT count(*) FROM bench_probe.__reflex_intermediate_kv)       AS intermediate_rows,
  (SELECT count(*) FROM bench_probe.kv)                             AS target_rows,
  (SELECT count(*) FROM bench_probe.kv_ref)                         AS ref_matview_rows;

-- ----------------------------------------------------------------------
-- Bench helper: one UPDATE iteration with wall-clock capture, then
-- VACUUM both IMV tables to start the next iteration in a clean state.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bench_probe._run_update_iter(phase TEXT, iter INT)
RETURNS NUMERIC AS $$
DECLARE
    _t0 TIMESTAMPTZ;
    _t1 TIMESTAMPTZ;
    _ms NUMERIC;
BEGIN
    _t0 := clock_timestamp();
    UPDATE bench_probe.parent SET status = 'validated' WHERE id = 1;
    _t1 := clock_timestamp();
    _ms := EXTRACT(EPOCH FROM _t1 - _t0) * 1000;
    RAISE NOTICE '% iter %: % ms', phase, iter, ROUND(_ms, 1);
    RETURN _ms;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE bench_probe._timings (phase TEXT, iter INT, ms NUMERIC);

-- Helper: run one UPDATE and INSERT the timing, no VACUUM (VACUUM can't
-- run inside a function or transaction block, so we wrap it at psql level).
CREATE OR REPLACE FUNCTION bench_probe._do_iter(phase TEXT, iter INT)
RETURNS NUMERIC AS $$
DECLARE
    _t0 TIMESTAMPTZ;
    _t1 TIMESTAMPTZ;
    _ms NUMERIC;
BEGIN
    _t0 := clock_timestamp();
    UPDATE bench_probe.parent SET status = 'validated' WHERE id = 1;
    _t1 := clock_timestamp();
    _ms := EXTRACT(EPOCH FROM _t1 - _t0) * 1000;
    INSERT INTO bench_probe._timings VALUES (phase, iter, _ms);
    RAISE NOTICE '% iter %: % ms', phase, iter, ROUND(_ms, 1);
    RETURN _ms;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------
-- PHASE 1: AFTER (1.4.5 default — probe ON, fk_id is NOT NULL)
-- ----------------------------------------------------------------------
\echo ''
\echo '=== PHASE 1: AFTER (probe ON, MERGE uses `=` on fk_id) ==='

-- Warm-up (not recorded)
SELECT bench_probe._run_update_iter('warmup_AFTER', 0);
VACUUM FULL bench_probe.__reflex_intermediate_kv;
VACUUM FULL bench_probe.kv;

SELECT bench_probe._do_iter('AFTER', 1);
VACUUM FULL bench_probe.__reflex_intermediate_kv;
VACUUM FULL bench_probe.kv;

SELECT bench_probe._do_iter('AFTER', 2);
VACUUM FULL bench_probe.__reflex_intermediate_kv;
VACUUM FULL bench_probe.kv;

-- ----------------------------------------------------------------------
-- PHASE 2: BEFORE (simulate pre-1.4.5 — strip fk_id from not_null_columns)
-- ----------------------------------------------------------------------
\echo ''
\echo '=== PHASE 2: BEFORE (probe-promoted fk_id stripped; IS NOT DISTINCT FROM) ==='

UPDATE public.__reflex_ivm_reference
SET aggregations = jsonb_set(
    aggregations::jsonb,
    '{not_null_columns}',
    (
      SELECT COALESCE(jsonb_agg(col), '[]'::jsonb)
      FROM jsonb_array_elements_text((aggregations::jsonb)->'not_null_columns') AS col
      WHERE col <> 'fk_id'
    )
)::json
WHERE name = 'bench_probe.kv';

SELECT bench_probe._run_update_iter('warmup_BEFORE', 0);
VACUUM FULL bench_probe.__reflex_intermediate_kv;
VACUUM FULL bench_probe.kv;

SELECT bench_probe._do_iter('BEFORE', 1);
VACUUM FULL bench_probe.__reflex_intermediate_kv;
VACUUM FULL bench_probe.kv;

SELECT bench_probe._do_iter('BEFORE', 2);
VACUUM FULL bench_probe.__reflex_intermediate_kv;
VACUUM FULL bench_probe.kv;

-- Restore probe-promoted state
\echo ''
\echo '=== Restoring not_null_columns to include fk_id ==='
SELECT public.reflex_probe_not_null_columns('bench_probe.kv');

-- ----------------------------------------------------------------------
-- PHASE 3: REFRESH MATERIALIZED VIEW (reference baseline)
-- ----------------------------------------------------------------------
\echo ''
\echo '=== PHASE 3: REFRESH MATERIALIZED VIEW (reference baseline) ==='

CREATE OR REPLACE FUNCTION bench_probe._do_refresh_iter(iter INT)
RETURNS NUMERIC AS $$
DECLARE
    _t0 TIMESTAMPTZ;
    _t1 TIMESTAMPTZ;
    _ms NUMERIC;
BEGIN
    UPDATE bench_probe.parent SET status = 'validated' WHERE id = 1;
    _t0 := clock_timestamp();
    REFRESH MATERIALIZED VIEW bench_probe.kv_ref;
    _t1 := clock_timestamp();
    _ms := EXTRACT(EPOCH FROM _t1 - _t0) * 1000;
    INSERT INTO bench_probe._timings VALUES ('REFRESH', iter, _ms);
    RAISE NOTICE 'REFRESH iter %: % ms', iter, ROUND(_ms, 1);
    RETURN _ms;
END;
$$ LANGUAGE plpgsql;

SELECT bench_probe._do_refresh_iter(0);  -- warmup, not recorded below
DELETE FROM bench_probe._timings WHERE phase = 'REFRESH' AND iter = 0;
SELECT bench_probe._do_refresh_iter(1);
SELECT bench_probe._do_refresh_iter(2);

-- ----------------------------------------------------------------------
-- Report
-- ----------------------------------------------------------------------
\echo ''
\echo '=== Summary ==='
SELECT phase,
       count(*)                                 AS iters,
       ROUND(AVG(ms)::numeric, 1)               AS avg_ms,
       ROUND(MIN(ms)::numeric, 1)               AS min_ms,
       ROUND(MAX(ms)::numeric, 1)               AS max_ms
FROM bench_probe._timings
GROUP BY phase
ORDER BY
  CASE phase WHEN 'AFTER' THEN 1 WHEN 'BEFORE' THEN 2 WHEN 'REFRESH' THEN 3 END;

\echo ''
\echo '=== Ratios (versus AFTER 1.4.5) ==='
WITH avgs AS (
    SELECT phase, AVG(ms) AS avg_ms FROM bench_probe._timings GROUP BY phase
)
SELECT
    ROUND((SELECT avg_ms FROM avgs WHERE phase='BEFORE') /
          NULLIF((SELECT avg_ms FROM avgs WHERE phase='AFTER'), 0), 2)
        AS before_vs_after,
    ROUND((SELECT avg_ms FROM avgs WHERE phase='AFTER') /
          NULLIF((SELECT avg_ms FROM avgs WHERE phase='REFRESH'), 0), 2)
        AS after_vs_refresh,
    ROUND((SELECT avg_ms FROM avgs WHERE phase='BEFORE') /
          NULLIF((SELECT avg_ms FROM avgs WHERE phase='REFRESH'), 0), 2)
        AS before_vs_refresh;

-- ----------------------------------------------------------------------
-- Cleanup
-- ----------------------------------------------------------------------
SELECT public.drop_reflex_ivm('bench_probe.kv');
DROP MATERIALIZED VIEW bench_probe.kv_ref;
DROP SCHEMA bench_probe CASCADE;
