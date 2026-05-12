-- ==========================================================================
-- Probe G: test the stale-stats-on-affected hypothesis
--
-- Probe A revealed: trigger-internal time matches wall-clock (~3.95 s);
-- the journal's "2.3 s gap" came from manual measurements that included
-- ANALYZE between INSERT scratch and the DELETE/INSERT target steps.
--
-- Hypothesis: inside the trigger, __reflex_affected_fcast has empty pg_stats
-- after its INSERT. The planner of DELETE FROM target WHERE EXISTS (SELECT
-- ... FROM affected) and INSERT INTO target SELECT ... WHERE EXISTS picks
-- a bad plan (no row-count estimate ⇒ likely Hash Join / Seq Scan instead
-- of Bitmap Index Scan on the target's composite index).
--
-- This probe re-fires the same trigger but with explicit ANALYZE calls
-- between each step. If the gap closes, ANALYZE is the fix.
-- ==========================================================================
\timing on
\set ON_ERROR_STOP off

DROP TABLE IF EXISTS _saved_upd_trigger_G;
CREATE TEMP TABLE _saved_upd_trigger_G AS
SELECT pg_get_functiondef(p.oid) AS def
FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
WHERE p.proname='__reflex_upd_trigger_on_rb_demand_planning' AND n.nspname='public';

DROP TABLE IF EXISTS _g_timings;
CREATE TABLE _g_timings (
    run_label TEXT, step_idx INT, step_name TEXT, stmt_excerpt TEXT,
    ms NUMERIC, ts TIMESTAMPTZ DEFAULT clock_timestamp()
);

-- Instrumented trigger WITH ANALYZE after each INSERT into a __reflex_* table
CREATE OR REPLACE FUNCTION public.__reflex_upd_trigger_on_rb_demand_planning()
RETURNS trigger AS $function$
DECLARE
    _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
    _t0 TIMESTAMPTZ; _t_step TIMESTAMPTZ; _t_trigger TIMESTAMPTZ; _stmt_idx INT := 0;
    _label TEXT := current_setting('reflex.gap_run_label', true);
    _is_populate BOOLEAN; _target_tbl TEXT;
BEGIN
    _t_trigger := clock_timestamp();

    SELECT EXISTS(SELECT 1 FROM "__reflex_new_rb_demand_planning" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;

    FOR _rec IN
        SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate
        FROM public.__reflex_ivm_reference
        WHERE 'rb.demand_planning' = ANY(depends_on) AND enabled = TRUE
        ORDER BY graph_depth, name
    LOOP
        IF _rec.where_predicate IS NOT NULL THEN
            EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)',
                '__reflex_new_rb_demand_planning', _rec.where_predicate) INTO _pred_match;
            IF NOT _pred_match THEN CONTINUE; END IF;
        END IF;

        PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));

        _sql := public.reflex_build_delta_sql(
            _rec.name, 'rb.demand_planning', 'UPDATE',
            _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);

        IF _sql <> '' THEN
            FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
                IF _stmt <> '' THEN
                    _stmt_idx := _stmt_idx + 1;
                    _t_step := clock_timestamp();
                    EXECUTE _stmt;
                    INSERT INTO _g_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
                        VALUES (_label, _stmt_idx, 'stmt_' || lpad(_stmt_idx::text, 2, '0'),
                                left(_stmt, 100),
                                EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

                    -- After every INSERT into a __reflex_* helper table, run ANALYZE.
                    -- These are the writes that feed the planner of subsequent EXISTS
                    -- subqueries (affected, scratch).
                    _is_populate := _stmt ~* 'INSERT INTO\s+"?[a-z_]*"?\."?(__reflex_(scratch|affected|shrunk)_)';
                    IF _is_populate THEN
                        _target_tbl := substring(_stmt FROM 'INSERT INTO\s+("?[a-z_]+"?\."?__reflex_[a-z_]+"?)');
                        IF _target_tbl IS NOT NULL THEN
                            _t_step := clock_timestamp();
                            EXECUTE 'ANALYZE ' || _target_tbl;
                            INSERT INTO _g_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
                                VALUES (_label, _stmt_idx, 'analyze_after_stmt_' || lpad(_stmt_idx::text, 2, '0'),
                                        'ANALYZE ' || _target_tbl,
                                        EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
                        END IF;
                    END IF;
                END IF;
            END LOOP;
        END IF;
    END LOOP;

    INSERT INTO _g_timings(run_label, step_idx, step_name, stmt_excerpt, ms)
        VALUES (_label, 99, 'trigger_internal_total', NULL,
            EXTRACT(EPOCH FROM clock_timestamp() - _t_trigger) * 1000);
    RETURN NULL;
END;
$function$ LANGUAGE plpgsql;

-- Warm-up
SET reflex.gap_run_label = 'warm0';
BEGIN; UPDATE rb.demand_planning SET status = 'validated' WHERE id = 1; ROLLBACK;
DELETE FROM _g_timings WHERE run_label = 'warm0';

-- 3 runs
DROP TABLE IF EXISTS _g_wallclock;
CREATE TABLE _g_wallclock (run_label TEXT, wall_ms NUMERIC);

DO $$
DECLARE _r INT; _t0 TIMESTAMPTZ; _t1 TIMESTAMPTZ; _lbl TEXT;
BEGIN
    FOR _r IN 1..3 LOOP
        _lbl := 'run_' || _r;
        PERFORM set_config('reflex.gap_run_label', _lbl, false);
        _t0 := clock_timestamp();
        UPDATE rb.demand_planning SET status = 'validated' WHERE id = 1;
        _t1 := clock_timestamp();
        INSERT INTO _g_wallclock VALUES (_lbl, EXTRACT(EPOCH FROM _t1 - _t0) * 1000);
    END LOOP;
END $$;

-- Restore
DO $$ DECLARE _def TEXT; BEGIN SELECT def INTO _def FROM _saved_upd_trigger_G; EXECUTE _def; END $$;

\echo ''
\echo '=== Per-step trigger timings WITH ANALYZE after populate ==='
SELECT step_name,
       MAX(left(stmt_excerpt, 80)) AS stmt,
       ROUND(AVG(ms)::numeric, 2) AS avg_ms
FROM _g_timings
WHERE run_label LIKE 'run_%' AND step_name <> 'trigger_internal_total'
GROUP BY step_idx, step_name
ORDER BY step_idx, step_name;

\echo ''
\echo '=== Wall-clock vs trigger-internal-total ==='
SELECT w.run_label,
       ROUND(w.wall_ms::numeric, 2) AS wall_ms,
       ROUND(t.ms::numeric, 2) AS trigger_internal_ms
FROM _g_wallclock w
LEFT JOIN _g_timings t ON t.run_label = w.run_label AND t.step_name = 'trigger_internal_total'
ORDER BY w.run_label;
