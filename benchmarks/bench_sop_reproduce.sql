-- ==========================================================================
--  Reproduce bench_sop_forecast.sql with detailed internal trigger timing
--
--  Same methodology as the original:
--    1. INSERT with trigger (timed)
--    2. Cleanup
--    3. INSERT replica mode (timed)
--    4. Cleanup
--
--  PLUS: instrumented trigger that logs per-step timings
--
--  Prerequisite: IMV alp.sop_forecast_reflex with indexes must exist
--  Connection: psql -U postgres -h localhost -p 5432 -d db_clone
-- ==========================================================================
\timing on
SELECT setseed(0.42);

\echo ''
\echo '================================================================'
\echo '  REPRODUCE: bench_sop_forecast + detailed trigger timing'
\echo '  db_clone | PG18 | 76M source rows | 7.7M output rows'
\echo '  shared_buffers = 128MB'
\echo '================================================================'

-- Verify
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.__reflex_ivm_reference WHERE name = 'alp.sop_forecast_reflex') THEN
        RAISE EXCEPTION 'IMV does not exist';
    END IF;
END $$;

-- Show current index count on target
SELECT COUNT(*) AS target_indexes FROM pg_indexes WHERE schemaname = 'alp' AND tablename = 'sop_forecast_reflex';

-- Pool (same as original)
CREATE TEMP TABLE _bench_pool AS
SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY product_id, location_id) AS rn
FROM (SELECT DISTINCT product_id FROM alp.sales_simulation WHERE dem_plan_id = 605) p
CROSS JOIN (VALUES (50), (51)) l(location_id);
SELECT COUNT(*) AS pool_size FROM _bench_pool;

-- Timing log for trigger internals
CREATE TEMP TABLE _trigger_timings (
    batch_size INT,
    run_type TEXT,   -- 'original' or 'instrumented'
    step_name TEXT,
    ms NUMERIC,
    ts TIMESTAMPTZ DEFAULT clock_timestamp()
);

-- ==========================================================================
-- HELPERS (same as original benchmark)
-- ==========================================================================
CREATE OR REPLACE FUNCTION _gen_insert_sql(pool_sz INTEGER, batch_size INTEGER)
RETURNS TEXT AS $$
BEGIN
    RETURN format(
        'INSERT INTO alp.sales_simulation (dem_plan_id, product_id, location_id, order_date, year, month, week, isoyear, qty_sales, qty_sales_ub, qty_sales_lb, forecast_base)
         SELECT 605, product_id, location_id,
                ''2028-01-07''::timestamptz + (((rn_global - 1) / %s) * interval ''7 days''),
                2028, 1, 2 + ((rn_global - 1) / %s)::int, 2028,
                (random() * 100)::int, (random() * 120)::int, (random() * 80)::int, (random() * 100)::int
         FROM (
             SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY d.date_idx, bp.rn) AS rn_global
             FROM _bench_pool bp
             CROSS JOIN generate_series(1, GREATEST(1, CEIL(%s::float / %s)::int)) d(date_idx)
             LIMIT %s
         ) sub',
        pool_sz, pool_sz, batch_size, pool_sz, batch_size
    );
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _bench_cleanup() RETURNS VOID AS $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$ LANGUAGE plpgsql;

-- Warm up
INSERT INTO alp.sales_simulation (dem_plan_id, product_id, location_id, order_date, year, month, week, isoyear, qty_sales, qty_sales_ub, qty_sales_lb, forecast_base)
VALUES (605, 295, 50, '2029-01-07'::timestamptz, 2029, 1, 2, 2029, 10, 12, 8, 10);
DELETE FROM alp.sales_simulation WHERE order_date = '2029-01-07'::timestamptz AND dem_plan_id = 605 AND product_id = 295 AND location_id = 50;

-- ==========================================================================
-- INSTRUMENTED TRIGGER FUNCTION
-- ==========================================================================
-- Save original
CREATE TEMP TABLE _saved_trigger_fn AS
SELECT prosrc FROM pg_proc WHERE proname = '__reflex_ins_trigger_on_alp_sales_simulation';

CREATE OR REPLACE FUNCTION __reflex_ins_trigger_on_alp_sales_simulation() RETURNS TRIGGER AS $fn$
DECLARE
    _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
    _t0 TIMESTAMPTZ; _t_step TIMESTAMPTZ; _stmt_idx INT := 0;
BEGIN
    _t0 := clock_timestamp();

    _t_step := clock_timestamp();
    SELECT EXISTS(SELECT 1 FROM "__reflex_new_alp_sales_simulation" LIMIT 1) INTO _has_rows;
    INSERT INTO _trigger_timings (batch_size, run_type, step_name, ms) VALUES
        (0, 'instrumented', '1_exists_check', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
    IF NOT _has_rows THEN RETURN NULL; END IF;

    _t_step := clock_timestamp();
    FOR _rec IN
        SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate
        FROM public.__reflex_ivm_reference
        WHERE 'alp.sales_simulation' = ANY(depends_on) AND enabled = TRUE
        ORDER BY graph_depth
    LOOP
        INSERT INTO _trigger_timings (batch_size, run_type, step_name, ms) VALUES
            (0, 'instrumented', '2_ref_query', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        IF _rec.where_predicate IS NOT NULL THEN
            _t_step := clock_timestamp();
            EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)',
                '__reflex_new_alp_sales_simulation', _rec.where_predicate) INTO _pred_match;
            INSERT INTO _trigger_timings (batch_size, run_type, step_name, ms) VALUES
                (0, 'instrumented', '3_pred_check', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
            IF NOT _pred_match THEN CONTINUE; END IF;
        END IF;

        _t_step := clock_timestamp();
        PERFORM pg_advisory_xact_lock(hashtext(_rec.name));
        INSERT INTO _trigger_timings (batch_size, run_type, step_name, ms) VALUES
            (0, 'instrumented', '4_advisory_lock', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        _t_step := clock_timestamp();
        _sql := reflex_build_delta_sql(_rec.name, 'alp.sales_simulation', 'INSERT',
                    _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
        INSERT INTO _trigger_timings (batch_size, run_type, step_name, ms) VALUES
            (0, 'instrumented', '5_rust_ffi', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        IF _sql <> '' THEN
            FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
                IF _stmt <> '' THEN
                    _stmt_idx := _stmt_idx + 1;
                    _t_step := clock_timestamp();
                    EXECUTE _stmt;
                    INSERT INTO _trigger_timings (batch_size, run_type, step_name, ms) VALUES
                        (0, 'instrumented', '6_execute_stmt_' || _stmt_idx,
                         EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
                END IF;
            END LOOP;
        END IF;
    END LOOP;

    INSERT INTO _trigger_timings (batch_size, run_type, step_name, ms) VALUES
        (0, 'instrumented', '7_trigger_total', EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000);
    RETURN NULL;
END;
$fn$ LANGUAGE plpgsql;

-- ==========================================================================
-- BENCHMARK FUNCTION: reproduces original + captures trigger timings
-- ==========================================================================
CREATE OR REPLACE FUNCTION bench_insert_detailed(batch_size INTEGER)
RETURNS TABLE(metric TEXT, ms NUMERIC) AS $$
DECLARE
    t0 TIMESTAMPTZ; t1 TIMESTAMPTZ;
    reflex_ms NUMERIC; raw_ms NUMERIC;
    no_trig_ms NUMERIC;
    pool_sz INTEGER; insert_sql TEXT;
    trig_delta_ms NUMERIC; trig_total_ms NUMERIC;
BEGIN
    SELECT COUNT(*) INTO pool_sz FROM _bench_pool;
    insert_sql := _gen_insert_sql(pool_sz, batch_size);

    -- 1. pg_reflex: INSERT with instrumented trigger
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    reflex_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;

    -- Tag the trigger timings with this batch size
    UPDATE _trigger_timings SET batch_size = bench_insert_detailed.batch_size WHERE _trigger_timings.batch_size = 0;

    -- Read trigger internals
    SELECT _trigger_timings.ms INTO trig_delta_ms FROM _trigger_timings
        WHERE _trigger_timings.batch_size = bench_insert_detailed.batch_size AND _trigger_timings.step_name = '6_execute_stmt_1';
    SELECT _trigger_timings.ms INTO trig_total_ms FROM _trigger_timings
        WHERE _trigger_timings.batch_size = bench_insert_detailed.batch_size AND _trigger_timings.step_name = '7_trigger_total';

    -- Cleanup
    PERFORM _bench_cleanup();

    -- 2. Raw INSERT (no trigger) — replica mode (same as original benchmark)
    SET LOCAL session_replication_role = replica;
    t0 := clock_timestamp();
    EXECUTE insert_sql;
    t1 := clock_timestamp();
    raw_ms := EXTRACT(EPOCH FROM t1 - t0) * 1000;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;

    metric := 'reflex_total';                  ms := ROUND(reflex_ms, 1);                    RETURN NEXT;
    metric := 'raw_insert_replica';            ms := ROUND(raw_ms, 1);                       RETURN NEXT;
    metric := 'overhead_total';                ms := ROUND(reflex_ms - raw_ms, 1);            RETURN NEXT;
    metric := 'trigger_delta_insert';          ms := ROUND(COALESCE(trig_delta_ms, -1), 1);   RETURN NEXT;
    metric := 'trigger_total_internal';        ms := ROUND(COALESCE(trig_total_ms, -1), 1);   RETURN NEXT;
    metric := 'source_insert_cost';            ms := ROUND(reflex_ms - COALESCE(trig_total_ms, 0), 1); RETURN NEXT;
    metric := 'trigger_pct_of_total';          ms := ROUND(100.0 * COALESCE(trig_total_ms, 0) / NULLIF(reflex_ms, 0), 1); RETURN NEXT;
END $$ LANGUAGE plpgsql;


-- ==========================================================================
-- RUN BENCHMARKS
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  INSERT BENCHMARK (all batch sizes)'
\echo '================================================================'

\echo ''
\echo '--- INSERT 1,000 rows ---'
SELECT * FROM bench_insert_detailed(1000);

\echo ''
\echo '--- INSERT 5,000 rows ---'
SELECT * FROM bench_insert_detailed(5000);

\echo ''
\echo '--- INSERT 10,000 rows ---'
SELECT * FROM bench_insert_detailed(10000);

\echo ''
\echo '--- INSERT 25,000 rows ---'
SELECT * FROM bench_insert_detailed(25000);

\echo ''
\echo '--- INSERT 50,000 rows ---'
SELECT * FROM bench_insert_detailed(50000);

\echo ''
\echo '--- INSERT 100,000 rows ---'
SELECT * FROM bench_insert_detailed(100000);


-- ==========================================================================
-- DETAILED TRIGGER TIMING REPORT
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  TRIGGER INTERNALS PER BATCH SIZE'
\echo '================================================================'

SELECT batch_size, step_name, ROUND(ms, 1) AS ms
FROM _trigger_timings
ORDER BY batch_size, ts;

\echo ''
\echo '--- Trigger delta INSERT scaling ---'
SELECT
    batch_size,
    ROUND(MAX(CASE WHEN step_name = '6_execute_stmt_1' THEN ms END), 1) AS delta_insert_ms,
    ROUND(MAX(CASE WHEN step_name = '7_trigger_total' THEN ms END), 1) AS trigger_total_ms
FROM _trigger_timings
GROUP BY batch_size
ORDER BY batch_size;


-- ==========================================================================
-- RESTORE & CLEANUP
-- ==========================================================================
DO $$
DECLARE orig TEXT;
BEGIN
    SELECT prosrc INTO orig FROM _saved_trigger_fn;
    EXECUTE format('CREATE OR REPLACE FUNCTION __reflex_ins_trigger_on_alp_sales_simulation() RETURNS TRIGGER AS $fn$ %s $fn$ LANGUAGE plpgsql', orig);
END $$;

DROP TABLE _saved_trigger_fn;
DROP FUNCTION IF EXISTS bench_insert_detailed(INTEGER);
DROP FUNCTION IF EXISTS _gen_insert_sql(INTEGER, INTEGER);
DROP FUNCTION IF EXISTS _bench_cleanup();
DROP TABLE IF EXISTS _bench_pool;
DROP TABLE IF EXISTS _trigger_timings;

\echo ''
\echo '================================================================'
\echo '  BENCHMARK COMPLETE'
\echo '================================================================'
