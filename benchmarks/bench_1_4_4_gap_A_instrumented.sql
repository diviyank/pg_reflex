-- ==========================================================================
-- Probe A: instrumented UPDATE trigger on rb.demand_planning
--
-- Goal: attribute the ~2.3 s gap between per-step measured (~1.4 s) and
-- wall-clock UPDATE (~3.7 s) reported in
-- journal/2026-05-12_1_4_3_and_1_4_4_customer_unblock.md
--
-- Method: hot-swap public.__reflex_upd_trigger_on_rb_demand_planning() with
-- an instrumented body that logs clock_timestamp() deltas around every step
-- (exists check, ref-table SELECT, FFI to reflex_build_delta_sql, every
-- EXECUTE _stmt). Restore the original body in a finally-style block.
--
-- Connection: psql -h localhost -U postgres -d db_clone -f <this>
-- ==========================================================================
\timing on
\set ON_ERROR_STOP off

-- =========================================================
-- 1. Capture the original trigger body so we can restore it
-- =========================================================
DROP TABLE IF EXISTS _saved_upd_trigger;
CREATE TEMP TABLE _saved_upd_trigger AS
SELECT pg_get_functiondef(p.oid) AS def
FROM pg_proc p
JOIN pg_namespace n ON n.oid=p.pronamespace
WHERE p.proname='__reflex_upd_trigger_on_rb_demand_planning'
  AND n.nspname='public';

SELECT COUNT(*) AS saved FROM _saved_upd_trigger;

-- =========================================================
-- 2. Timings log
-- =========================================================
DROP TABLE IF EXISTS _gap_timings;
CREATE TABLE _gap_timings (
    run_label TEXT,
    step_idx INT,
    step_name TEXT,
    stmt_excerpt TEXT,
    ms NUMERIC,
    ts TIMESTAMPTZ DEFAULT clock_timestamp()
);

-- =========================================================
-- 3. Install instrumented trigger
-- =========================================================
CREATE OR REPLACE FUNCTION public.__reflex_upd_trigger_on_rb_demand_planning()
RETURNS trigger AS $function$
DECLARE
    _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
    _t0 TIMESTAMPTZ; _t_step TIMESTAMPTZ; _t_trigger TIMESTAMPTZ; _stmt_idx INT := 0;
    _label TEXT := current_setting('reflex.gap_run_label', true);
BEGIN
    _t_trigger := clock_timestamp();
    _t_step := _t_trigger;

    SELECT EXISTS(SELECT 1 FROM "__reflex_new_rb_demand_planning" LIMIT 1) INTO _has_rows;
    INSERT INTO _gap_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
        VALUES (_label, 1, '1_exists_check', NULL,
            EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
    IF NOT _has_rows THEN RETURN NULL; END IF;

    _t_step := clock_timestamp();
    FOR _rec IN
        SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate
        FROM public.__reflex_ivm_reference
        WHERE 'rb.demand_planning' = ANY(depends_on) AND enabled = TRUE
        ORDER BY graph_depth, name
    LOOP
        INSERT INTO _gap_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
            VALUES (_label, 2, '2_ref_query', _rec.name,
                EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        IF _rec.where_predicate IS NOT NULL THEN
            _t_step := clock_timestamp();
            EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)',
                '__reflex_new_rb_demand_planning', _rec.where_predicate) INTO _pred_match;
            INSERT INTO _gap_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
                VALUES (_label, 3, '3_pred_check', _rec.where_predicate,
                    EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
            IF NOT _pred_match THEN CONTINUE; END IF;
        END IF;

        _t_step := clock_timestamp();
        PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
        INSERT INTO _gap_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
            VALUES (_label, 4, '4_advisory_lock', _rec.name,
                EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        _t_step := clock_timestamp();
        _sql := public.reflex_build_delta_sql(
            _rec.name, 'rb.demand_planning', 'UPDATE',
            _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
        INSERT INTO _gap_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
            VALUES (_label, 5, '5_rust_ffi_build_delta_sql', NULL,
                EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        IF _sql <> '' THEN
            FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
                IF _stmt <> '' THEN
                    _stmt_idx := _stmt_idx + 1;
                    _t_step := clock_timestamp();
                    EXECUTE _stmt;
                    INSERT INTO _gap_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
                        VALUES (_label, 6, '6_execute_stmt_' || lpad(_stmt_idx::text, 2, '0'),
                            left(_stmt, 120),
                            EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
                END IF;
            END LOOP;
        END IF;
    END LOOP;

    INSERT INTO _gap_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
        VALUES (_label, 99, '99_trigger_internal_total', NULL,
            EXTRACT(EPOCH FROM clock_timestamp() - _t_trigger) * 1000);
    RETURN NULL;
END;
$function$ LANGUAGE plpgsql;

-- =========================================================
-- 4. Warm-up run
-- =========================================================
SET reflex.gap_run_label = 'warm0';
BEGIN;
UPDATE rb.demand_planning SET status = 'validated' WHERE id = 1;
ROLLBACK;
DELETE FROM _gap_timings WHERE run_label = 'warm0';

-- =========================================================
-- 5. Three repeats — capture wall-clock + trigger-internal-total
-- =========================================================
DROP TABLE IF EXISTS _wallclock_timings;
CREATE TABLE _wallclock_timings (run_label TEXT, wall_ms NUMERIC);

DO $$
DECLARE _r INT; _t0 TIMESTAMPTZ; _t1 TIMESTAMPTZ; _lbl TEXT;
BEGIN
    FOR _r IN 1..3 LOOP
        _lbl := 'run_' || _r;
        PERFORM set_config('reflex.gap_run_label', _lbl, false);
        _t0 := clock_timestamp();
        UPDATE rb.demand_planning SET status = 'validated' WHERE id = 1;
        _t1 := clock_timestamp();
        INSERT INTO _wallclock_timings VALUES (_lbl, EXTRACT(EPOCH FROM _t1 - _t0) * 1000);
    END LOOP;
END $$;

-- =========================================================
-- 6. Restore original trigger body
-- =========================================================
DO $$
DECLARE _def TEXT;
BEGIN
    SELECT def INTO _def FROM _saved_upd_trigger;
    EXECUTE _def;
END $$;

-- =========================================================
-- 7. Report
-- =========================================================
\echo ''
\echo '=== Per-step trigger timings (average across runs) ==='
SELECT step_name,
       ROUND(AVG(ms)::numeric, 2) AS avg_ms,
       ROUND(MIN(ms)::numeric, 2) AS min_ms,
       ROUND(MAX(ms)::numeric, 2) AS max_ms,
       COUNT(*) AS n_runs
FROM _gap_timings
WHERE run_label LIKE 'run_%'
GROUP BY step_idx, step_name
ORDER BY step_idx, step_name;

\echo ''
\echo '=== Wall-clock vs trigger-internal-total (per run) ==='
SELECT w.run_label,
       ROUND(w.wall_ms::numeric, 2) AS wall_ms,
       ROUND(t.ms::numeric, 2) AS trigger_internal_ms,
       ROUND((w.wall_ms - t.ms)::numeric, 2) AS gap_ms
FROM _wallclock_timings w
LEFT JOIN _gap_timings t ON t.run_label = w.run_label AND t.step_name = '99_trigger_internal_total'
ORDER BY w.run_label;

\echo ''
\echo '=== Per-statement summary ==='
SELECT step_name,
       ROUND(AVG(ms)::numeric, 2) AS avg_ms,
       MAX(left(stmt_excerpt, 80)) AS stmt
FROM _gap_timings
WHERE run_label LIKE 'run_%' AND step_idx = 6
GROUP BY step_idx, step_name
ORDER BY step_name;
