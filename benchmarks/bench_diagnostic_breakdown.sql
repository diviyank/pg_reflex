-- ==========================================================================
--  Diagnostic: Break down the 50K INSERT time into individual components
--
--  Goal: understand WHY the trigger overhead is 34.7s when the same
--  delta INSERT takes 946ms standalone. Measure each component separately.
--
--  Prerequisite: IMV alp.sop_forecast_reflex must exist with indexes.
--  Connection: psql -U postgres -h localhost -p 5432 -d db_clone
-- ==========================================================================
\timing on
SELECT setseed(0.42);

\echo ''
\echo '================================================================'
\echo '  DIAGNOSTIC BREAKDOWN: Where does the 50K INSERT time go?'
\echo '================================================================'

-- Verify IMV exists
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.__reflex_ivm_reference WHERE name = 'alp.sop_forecast_reflex') THEN
        RAISE EXCEPTION 'IMV alp.sop_forecast_reflex does not exist';
    END IF;
END $$;

-- Pool for test data generation
CREATE TEMP TABLE _diag_pool AS
SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY product_id, location_id) AS rn
FROM (SELECT DISTINCT product_id FROM alp.sales_simulation WHERE dem_plan_id = 605) p
CROSS JOIN (VALUES (50), (51)) l(location_id);

-- Generate 50K INSERT SQL (into source table)
CREATE OR REPLACE FUNCTION _diag_source_insert_sql()
RETURNS TEXT AS $$
DECLARE pool_sz INTEGER;
BEGIN
    SELECT COUNT(*) INTO pool_sz FROM _diag_pool;
    RETURN format(
        'INSERT INTO alp.sales_simulation (dem_plan_id, product_id, location_id, order_date, year, month, week, isoyear, qty_sales, qty_sales_ub, qty_sales_lb, forecast_base)
         SELECT 605, product_id, location_id,
                ''2028-01-07''::timestamptz + (((rn_global - 1) / %s) * interval ''7 days''),
                2028, 1, 2 + ((rn_global - 1) / %s)::int, 2028,
                (random() * 100)::int, (random() * 120)::int, (random() * 80)::int, (random() * 100)::int
         FROM (
             SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY d.date_idx, bp.rn) AS rn_global
             FROM _diag_pool bp
             CROSS JOIN generate_series(1, GREATEST(1, CEIL(%s::float / %s)::int)) d(date_idx)
             LIMIT %s
         ) sub',
        pool_sz, pool_sz, 50000, pool_sz, 50000
    );
END $$ LANGUAGE plpgsql;

-- Cleanup helper (bypasses triggers)
CREATE OR REPLACE FUNCTION _diag_cleanup() RETURNS VOID AS $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$ LANGUAGE plpgsql;

-- Timing log table
CREATE TEMP TABLE _timings (
    test_name TEXT,
    step_name TEXT,
    ms NUMERIC,
    ts TIMESTAMPTZ DEFAULT clock_timestamp()
);


-- ==========================================================================
-- TEST 1: Full INSERT with trigger (the real benchmark scenario)
-- ==========================================================================
\echo ''
\echo '--- TEST 1: Full 50K INSERT with trigger (total) ---'
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    SELECT _diag_source_insert_sql() INTO sql;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings (test_name, step_name, ms) VALUES
        ('T1_full_with_trigger', 'total', EXTRACT(EPOCH FROM t1 - t0) * 1000);
    PERFORM _diag_cleanup();
END $$;


-- ==========================================================================
-- TEST 2: Source INSERT only — triggers disabled, FK still active
--         (ALTER TABLE DISABLE TRIGGER, not replica mode)
-- ==========================================================================
\echo ''
\echo '--- TEST 2: 50K source INSERT, triggers disabled, FK active ---'
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    -- Disable only the reflex triggers
    ALTER TABLE alp.sales_simulation DISABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";
    SELECT _diag_source_insert_sql() INTO sql;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings (test_name, step_name, ms) VALUES
        ('T2_no_trigger_fk_active', 'total', EXTRACT(EPOCH FROM t1 - t0) * 1000);
    ALTER TABLE alp.sales_simulation ENABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";
    PERFORM _diag_cleanup();
END $$;


-- ==========================================================================
-- TEST 3: Source INSERT only — replica mode (no triggers, no FK, no checks)
-- ==========================================================================
\echo ''
\echo '--- TEST 3: 50K source INSERT, replica mode (no trigger, no FK) ---'
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    SELECT _diag_source_insert_sql() INTO sql;
    SET LOCAL session_replication_role = replica;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    SET LOCAL session_replication_role = DEFAULT;
    INSERT INTO _timings (test_name, step_name, ms) VALUES
        ('T3_replica_mode', 'total', EXTRACT(EPOCH FROM t1 - t0) * 1000);
    PERFORM _diag_cleanup();
END $$;


-- ==========================================================================
-- TEST 4: Instrumented trigger — replace with timing version
-- ==========================================================================
\echo ''
\echo '--- TEST 4: Instrumented trigger (step-by-step timing) ---'

-- Save original trigger function source
CREATE TEMP TABLE _saved_trigger_fn AS
SELECT prosrc FROM pg_proc WHERE proname = '__reflex_ins_trigger_on_alp_sales_simulation';

-- Create instrumented version
CREATE OR REPLACE FUNCTION __reflex_ins_trigger_on_alp_sales_simulation() RETURNS TRIGGER AS $fn$
DECLARE
    _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
    _t0 TIMESTAMPTZ; _t1 TIMESTAMPTZ; _t_step TIMESTAMPTZ;
    _stmt_idx INT := 0;
BEGIN
    _t0 := clock_timestamp();

    -- Step 1: EXISTS check on transition table
    _t_step := clock_timestamp();
    SELECT EXISTS(SELECT 1 FROM "__reflex_new_alp_sales_simulation" LIMIT 1) INTO _has_rows;
    INSERT INTO _timings (test_name, step_name, ms) VALUES
        ('T4_instrumented', 'exists_check', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
    IF NOT _has_rows THEN RETURN NULL; END IF;

    -- Step 2: Query __reflex_ivm_reference
    _t_step := clock_timestamp();
    FOR _rec IN
        SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate
        FROM public.__reflex_ivm_reference
        WHERE 'alp.sales_simulation' = ANY(depends_on) AND enabled = TRUE
        ORDER BY graph_depth
    LOOP
        INSERT INTO _timings (test_name, step_name, ms) VALUES
            ('T4_instrumented', 'ref_query', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        -- Step 3: WHERE predicate check
        IF _rec.where_predicate IS NOT NULL THEN
            _t_step := clock_timestamp();
            EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)',
                '__reflex_new_alp_sales_simulation', _rec.where_predicate) INTO _pred_match;
            INSERT INTO _timings (test_name, step_name, ms) VALUES
                ('T4_instrumented', 'pred_check', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
            IF NOT _pred_match THEN CONTINUE; END IF;
        END IF;

        -- Step 4: Advisory lock
        _t_step := clock_timestamp();
        PERFORM pg_advisory_xact_lock(hashtext(_rec.name));
        INSERT INTO _timings (test_name, step_name, ms) VALUES
            ('T4_instrumented', 'advisory_lock', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        -- Step 5: Rust FFI call (build delta SQL)
        _t_step := clock_timestamp();
        _sql := reflex_build_delta_sql(_rec.name, 'alp.sales_simulation', 'INSERT',
                    _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
        INSERT INTO _timings (test_name, step_name, ms) VALUES
            ('T4_instrumented', 'rust_ffi_build_sql', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        -- Log the generated SQL for inspection
        INSERT INTO _timings (test_name, step_name, ms) VALUES
            ('T4_instrumented', 'generated_sql_len=' || length(_sql), 0);

        -- Step 6: Execute each statement
        IF _sql <> '' THEN
            FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
                IF _stmt <> '' THEN
                    _stmt_idx := _stmt_idx + 1;
                    _t_step := clock_timestamp();
                    EXECUTE _stmt;
                    INSERT INTO _timings (test_name, step_name, ms) VALUES
                        ('T4_instrumented', 'execute_stmt_' || _stmt_idx || '_(' || left(_stmt, 60) || '...)',
                         EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
                END IF;
            END LOOP;
        END IF;
    END LOOP;

    _t1 := clock_timestamp();
    INSERT INTO _timings (test_name, step_name, ms) VALUES
        ('T4_instrumented', 'trigger_total', EXTRACT(EPOCH FROM _t1 - _t0) * 1000);
    RETURN NULL;
END;
$fn$ LANGUAGE plpgsql;

-- Run the INSERT with the instrumented trigger
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    SELECT _diag_source_insert_sql() INTO sql;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings (test_name, step_name, ms) VALUES
        ('T4_instrumented', 'wall_clock_total', EXTRACT(EPOCH FROM t1 - t0) * 1000);
    PERFORM _diag_cleanup();
END $$;

-- Restore original trigger function
DO $$
DECLARE orig TEXT;
BEGIN
    SELECT prosrc INTO orig FROM _saved_trigger_fn;
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION __reflex_ins_trigger_on_alp_sales_simulation() RETURNS TRIGGER AS $fn$ %s $fn$ LANGUAGE plpgsql',
        orig
    );
END $$;
DROP TABLE _saved_trigger_fn;


-- ==========================================================================
-- TEST 5: Same delta INSERT but from transition table context
--         (simulate trigger conditions without source INSERT overhead)
--
-- Insert 50K rows with trigger disabled, then manually fire the delta SQL
-- using the same transition table mechanism via a custom trigger
-- ==========================================================================
\echo ''
\echo '--- TEST 5: Delta INSERT from fresh temp table (simulating transition table, after source INSERT) ---'

-- Step 1: Do the source INSERT with triggers disabled
DO $$
DECLARE sql TEXT;
BEGIN
    ALTER TABLE alp.sales_simulation DISABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";
    SELECT _diag_source_insert_sql() INTO sql;
    EXECUTE sql;
    ALTER TABLE alp.sales_simulation ENABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";
END $$;

-- Step 2: Create a temp table mimicking the transition table content
CREATE TEMP TABLE _diag_transition AS
SELECT * FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
SELECT COUNT(*) AS "transition rows" FROM _diag_transition;

-- Step 3: Get the generated delta SQL
DO $$
DECLARE _sql TEXT; _stmt TEXT; _t0 TIMESTAMPTZ; _t1 TIMESTAMPTZ; _rec RECORD;
BEGIN
    SELECT name, base_query, end_query, aggregations::text AS aggregations
    INTO _rec
    FROM public.__reflex_ivm_reference
    WHERE name = 'alp.sop_forecast_reflex';

    _sql := reflex_build_delta_sql(_rec.name, 'alp.sales_simulation', 'INSERT',
                _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);

    -- Log the SQL
    RAISE NOTICE 'Delta SQL: %', left(_sql, 200);

    -- Replace the transition table reference with our temp table
    _sql := replace(_sql, '"__reflex_new_alp_sales_simulation"', '_diag_transition');

    -- Execute and time it (this runs AFTER the source INSERT has polluted the buffer pool)
    _t0 := clock_timestamp();
    FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
        IF _stmt <> '' THEN
            _stmt := replace(_stmt, '"__reflex_new_alp_sales_simulation"', '_diag_transition');
            EXECUTE _stmt;
        END IF;
    END LOOP;
    _t1 := clock_timestamp();

    INSERT INTO _timings (test_name, step_name, ms) VALUES
        ('T5_post_source_insert', 'delta_insert_only', EXTRACT(EPOCH FROM _t1 - _t0) * 1000);
END $$;

DROP TABLE _diag_transition;
PERFORM _diag_cleanup();


-- ==========================================================================
-- TEST 6: Same as TEST 5 but with EXPLAIN ANALYZE to see the plan
--         when the buffer pool is "warm" from the source INSERT
-- ==========================================================================
\echo ''
\echo '--- TEST 6: Delta INSERT after source INSERT (EXPLAIN ANALYZE, dirty buffers) ---'

DO $$
DECLARE sql TEXT;
BEGIN
    ALTER TABLE alp.sales_simulation DISABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";
    SELECT _diag_source_insert_sql() INTO sql;
    EXECUTE sql;
    ALTER TABLE alp.sales_simulation ENABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";
END $$;

CREATE TEMP TABLE _diag_transition2 AS
SELECT * FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO alp.sop_forecast_reflex
SELECT
    _diag_transition2.dem_plan_id,
    _diag_transition2.week,
    _diag_transition2.isoyear,
    _diag_transition2.year,
    _diag_transition2.month,
    _diag_transition2.order_date,
    _diag_transition2.product_id,
    _diag_transition2.location_id,
    location.canal_id,
    _diag_transition2.forecast_base::bigint AS forecast_base,
    _diag_transition2.qty_sales::bigint AS quantity,
    _diag_transition2.qty_sales_ub::bigint AS quantity_ub,
    _diag_transition2.qty_sales_lb::bigint AS quantity_lb,
    _diag_transition2.qty_sales::double precision * COALESCE(pricing.base_price, 0::double precision) AS turnover,
    _diag_transition2.forecast_base::double precision * COALESCE(pricing.base_price, 0::double precision) AS forecast_base_turnover,
    _diag_transition2.qty_sales_ub::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_ub_turnover,
    _diag_transition2.qty_sales_lb::double precision * COALESCE(pricing.base_price, 0::double precision) AS qty_sales_lb_turnover,
    caav.product_id IS NOT NULL AS in_current_assortment
FROM _diag_transition2
    JOIN alp.demand_planning ON demand_planning.id = _diag_transition2.dem_plan_id
    JOIN alp.location ON location.id = _diag_transition2.location_id
    JOIN alp.product ON product.id = _diag_transition2.product_id
    LEFT JOIN alp.pricing ON demand_planning.assortment_id = pricing.assortment_id
        AND pricing.canal_id = location.canal_id
        AND _diag_transition2.product_id = pricing.product_id
    LEFT JOIN alp.current_assortment_activity_view caav
        ON caav.product_id = _diag_transition2.product_id
        AND caav.location_id = _diag_transition2.location_id
WHERE demand_planning.is_sent_to_sop
    AND (demand_planning.status::text <> ALL (ARRAY['archive','archiving','error']::text[]));

DROP TABLE _diag_transition2;

-- Cleanup
SELECT _diag_cleanup();


-- ==========================================================================
-- RESULTS
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  RESULTS SUMMARY'
\echo '================================================================'
\echo ''

SELECT test_name, step_name, ROUND(ms, 1) AS ms
FROM _timings
ORDER BY ts;

\echo ''
\echo '--- Computed breakdown ---'
SELECT
    'Source INSERT (no trigger, with FK)' AS component,
    ROUND((SELECT ms FROM _timings WHERE test_name = 'T2_no_trigger_fk_active'), 0) AS ms
UNION ALL SELECT
    'Source INSERT (replica, no FK)',
    ROUND((SELECT ms FROM _timings WHERE test_name = 'T3_replica_mode'), 0)
UNION ALL SELECT
    'FK overhead (T2 - T3)',
    ROUND((SELECT ms FROM _timings WHERE test_name = 'T2_no_trigger_fk_active') -
          (SELECT ms FROM _timings WHERE test_name = 'T3_replica_mode'), 0)
UNION ALL SELECT
    'Full INSERT with trigger (T1)',
    ROUND((SELECT ms FROM _timings WHERE test_name = 'T1_full_with_trigger'), 0)
UNION ALL SELECT
    'Trigger overhead (T1 - T2)',
    ROUND((SELECT ms FROM _timings WHERE test_name = 'T1_full_with_trigger') -
          (SELECT ms FROM _timings WHERE test_name = 'T2_no_trigger_fk_active'), 0)
UNION ALL SELECT
    'Delta INSERT after source (T5, dirty buffers)',
    ROUND((SELECT ms FROM _timings WHERE test_name = 'T5_post_source_insert'), 0)
UNION ALL SELECT
    'Trigger internal total (T4)',
    ROUND((SELECT ms FROM _timings WHERE test_name = 'T4_instrumented' AND step_name = 'trigger_total'), 0)
UNION ALL SELECT
    'Gap: wall_clock - trigger_internal (T4)',
    ROUND((SELECT ms FROM _timings WHERE test_name = 'T4_instrumented' AND step_name = 'wall_clock_total') -
          (SELECT ms FROM _timings WHERE test_name = 'T2_no_trigger_fk_active') -
          COALESCE((SELECT ms FROM _timings WHERE test_name = 'T4_instrumented' AND step_name = 'trigger_total'), 0), 0);


-- ==========================================================================
-- CLEANUP
-- ==========================================================================
DROP FUNCTION IF EXISTS _diag_source_insert_sql();
DROP FUNCTION IF EXISTS _diag_cleanup();
DROP TABLE IF EXISTS _diag_pool;
DROP TABLE IF EXISTS _timings;

\echo ''
\echo '================================================================'
\echo '  DIAGNOSTIC COMPLETE'
\echo ''
\echo '  Key comparisons:'
\echo '  T1 vs T2: trigger overhead (isolates trigger from source INSERT)'
\echo '  T2 vs T3: FK constraint overhead (deferred, should be ~0)'
\echo '  T4 steps: where time goes INSIDE the trigger'
\echo '  T5 vs T4: transition table overhead (T5 uses temp table after source INSERT)'
\echo '  T6: EXPLAIN with dirty buffer pool (vs diagnostic_cliff TEST 2 with clean pool)'
\echo '================================================================'
