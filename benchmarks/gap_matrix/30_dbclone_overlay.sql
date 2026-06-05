-- Gap-matrix real-data overlay (Layer 2). Runs against db_clone.
--
-- SCOPE NOTE: the plan envisioned ~6 real views. The production views in
-- base_db/sql/views are deeply interdependent (view-on-view chains, scalar
-- subqueries) which makes faithful per-view scratch adaptation high-effort for
-- low marginal value once the synthetic matrix already covers the orthogonal
-- feature space. This overlay is therefore focused on ONE representative real
-- view shape that is self-contained AND exercises the highest-value path: a
-- DEFERRED filtered passthrough (current_assortment_activity_view), whose
-- FLIP_OUT op drives a row OUT of the WHERE filter — the exact class of bug the
-- matrix surfaced and that was fixed in this branch. It validates both real-data
-- correctness and real-data perf, non-destructively (only gap_scratch.* is ever
-- written; alp.* is never mutated).
--
-- Prereq: load 00_harness.sql into db_clone first (provides bench_gap_results +
-- gap_measure + gap_drop_imv).
--   psql -U postgres -h localhost -d db_clone -f benchmarks/gap_matrix/00_harness.sql
--   psql -U postgres -h localhost -d db_clone -v RUN_TS="<ts>" -f benchmarks/gap_matrix/30_dbclone_overlay.sql
\set ON_ERROR_STOP on
\timing off
\if :{?RUN_TS}
\else
\set RUN_TS '2026-06-05 13:00:00+00'
\endif
\if :{?ASSORT}
\else
\set ASSORT 99
\endif

CREATE SCHEMA IF NOT EXISTS gap_scratch;

-- ---------------------------------------------------------------------------
-- current_assortment_activity_view shape:
--   SELECT product_id, location_id, is_active
--   FROM assortment_activity_relation WHERE assortment_id = <current>
-- Driving source cloned in full (~208k rows, 7 assortments). The scalar-subquery
-- filter is pinned to a concrete, populated assortment so the clone is
-- self-contained (no dependency on sop_current_view / max_order_date_view).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS gap_scratch.aar_imv, gap_scratch.aar_base CASCADE;
CREATE TABLE gap_scratch.aar_imv  AS TABLE alp.assortment_activity_relation;
CREATE TABLE gap_scratch.aar_base AS TABLE alp.assortment_activity_relation;
CREATE INDEX ON gap_scratch.aar_imv  (assortment_id, id);
CREATE INDEX ON gap_scratch.aar_base (assortment_id, id);
ANALYZE gap_scratch.aar_imv;
ANALYZE gap_scratch.aar_base;

-- Bridge psql vars into the DO block (plpgsql) via session GUCs.
SET gap.assort = :ASSORT;
SET gap.run_ts = :'RUN_TS';

\set BODY 'SELECT product_id, location_id, is_active FROM gap_scratch.aar_imv WHERE assortment_id = ' :ASSORT

CALL gap_drop_imv('gap_scratch.aar_v');
SELECT create_reflex_ivm('gap_scratch.aar_v', :'BODY', 'product_id,location_id', NULL, 'DEFERRED', NULL);
DROP MATERIALIZED VIEW IF EXISTS gap_scratch.aar_mv;
CREATE MATERIALIZED VIEW gap_scratch.aar_mv AS SELECT product_id, location_id, is_active FROM gap_scratch.aar_base WHERE assortment_id = :ASSORT;
ANALYZE gap_scratch.aar_v;
ANALYZE gap_scratch.aar_mv;

-- Disjoint row windows via id%15 buckets within the filtered assortment (~30k
-- rows ⇒ ~2k per bucket), so ops never touch the same rows.
-- UPDATE (in-filter): toggle is_active. DELETE: remove rows. FLIP_OUT: move rows
-- OUT of the filter (assortment_id := -1) — must be DELETEd from the IMV.
-- FLIP_IN: bring the flipped-out rows back.
DO $$
DECLARE _base bigint;
BEGIN
    SELECT count(*) INTO _base FROM gap_scratch.aar_imv WHERE assortment_id = current_setting('gap.assort')::bigint;

    CALL gap_measure(current_setting('gap.run_ts')::timestamptz,'dbclone',
        'caav/passthrough/DEFERRED/UPDATE','passthrough',false,'DEFERRED','none','UPDATE',_base,NULL,
        format('UPDATE gap_scratch.aar_imv  SET is_active = NOT is_active WHERE assortment_id=%1$s AND id %% 15 = 0', current_setting('gap.assort')),
        format('UPDATE gap_scratch.aar_base SET is_active = NOT is_active WHERE assortment_id=%1$s AND id %% 15 = 0', current_setting('gap.assort')),
        'gap_scratch.aar_imv','gap_scratch.aar_v','gap_scratch.aar_mv',
        format('SELECT product_id, location_id, is_active FROM gap_scratch.aar_imv WHERE assortment_id = %s', current_setting('gap.assort')));

    CALL gap_measure(current_setting('gap.run_ts')::timestamptz,'dbclone',
        'caav/passthrough/DEFERRED/DELETE','passthrough',false,'DEFERRED','none','DELETE',_base,NULL,
        format('DELETE FROM gap_scratch.aar_imv  WHERE assortment_id=%1$s AND id %% 15 = 5', current_setting('gap.assort')),
        format('DELETE FROM gap_scratch.aar_base WHERE assortment_id=%1$s AND id %% 15 = 5', current_setting('gap.assort')),
        'gap_scratch.aar_imv','gap_scratch.aar_v','gap_scratch.aar_mv',
        format('SELECT product_id, location_id, is_active FROM gap_scratch.aar_imv WHERE assortment_id = %s', current_setting('gap.assort')));

    -- FLIP_OUT: predicate exit (the just-fixed path). Rows leave assortment 99.
    CALL gap_measure(current_setting('gap.run_ts')::timestamptz,'dbclone',
        'caav/passthrough/DEFERRED/FLIP_OUT','passthrough',false,'DEFERRED','none','FLIP_OUT',_base,NULL,
        format('UPDATE gap_scratch.aar_imv  SET assortment_id=-1 WHERE assortment_id=%1$s AND id %% 15 = 10', current_setting('gap.assort')),
        format('UPDATE gap_scratch.aar_base SET assortment_id=-1 WHERE assortment_id=%1$s AND id %% 15 = 10', current_setting('gap.assort')),
        'gap_scratch.aar_imv','gap_scratch.aar_v','gap_scratch.aar_mv',
        format('SELECT product_id, location_id, is_active FROM gap_scratch.aar_imv WHERE assortment_id = %s', current_setting('gap.assort')));

    -- FLIP_IN: the rows parked at -1 re-enter the filter.
    CALL gap_measure(current_setting('gap.run_ts')::timestamptz,'dbclone',
        'caav/passthrough/DEFERRED/FLIP_IN','passthrough',false,'DEFERRED','none','FLIP_IN',_base,NULL,
        format('UPDATE gap_scratch.aar_imv  SET assortment_id=%1$s WHERE assortment_id=-1', current_setting('gap.assort')),
        format('UPDATE gap_scratch.aar_base SET assortment_id=%1$s WHERE assortment_id=-1', current_setting('gap.assort')),
        'gap_scratch.aar_imv','gap_scratch.aar_v','gap_scratch.aar_mv',
        format('SELECT product_id, location_id, is_active FROM gap_scratch.aar_imv WHERE assortment_id = %s', current_setting('gap.assort')));
END $$;

\echo '=== db_clone overlay done ==='
SELECT cell_label, operation, imv_ms, bare_ms, refresh_ms, advantage_pct, mismatches
FROM bench_gap_results WHERE layer='dbclone' AND run_ts = :'RUN_TS'::timestamptz ORDER BY cell_label;
