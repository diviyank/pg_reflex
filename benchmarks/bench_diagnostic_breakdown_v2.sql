-- ==========================================================================
--  Diagnostic v2: Break down the 50K INSERT time — fixed tests
--
--  Findings from v1:
--    - T4 instrumented trigger: delta INSERT = 830ms, trigger total = 832ms
--    - T4 wall clock: 4,268ms (warm cache)
--    - T3 source INSERT replica: 2,928ms (warm cache)
--    - Original benchmark: 48.7s total (cold cache)
--    - shared_buffers = 128MB, source+indexes = 18.7GB, target+indexes = ~2-3GB
--
--  THIS SCRIPT: fixes T2/T5/T6 failures, adds cold cache tests, adds
--  pg_stat_bgwriter buffer stats, isolates each component properly.
--
--  Connection: psql -U postgres -h localhost -p 5432 -d db_clone
-- ==========================================================================
\timing on
SELECT setseed(0.42);

-- Reset buffer stats for tracking
SELECT pg_stat_reset_shared('bgwriter');

\echo ''
\echo '================================================================'
\echo '  DIAGNOSTIC v2: 50K INSERT component breakdown'
\echo '  shared_buffers=128MB | source=18.7GB | target=~2-3GB'
\echo '================================================================'

-- Verify IMV exists
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.__reflex_ivm_reference WHERE name = 'alp.sop_forecast_reflex') THEN
        RAISE EXCEPTION 'IMV alp.sop_forecast_reflex does not exist';
    END IF;
END $$;

-- Pool for test data
CREATE TEMP TABLE _diag_pool AS
SELECT product_id, location_id, ROW_NUMBER() OVER (ORDER BY product_id, location_id) AS rn
FROM (SELECT DISTINCT product_id FROM alp.sales_simulation WHERE dem_plan_id = 605) p
CROSS JOIN (VALUES (50), (51)) l(location_id);

CREATE TEMP TABLE _timings (
    test_name TEXT,
    step_name TEXT,
    ms NUMERIC,
    ts TIMESTAMPTZ DEFAULT clock_timestamp()
);

-- Helper: generate 50K INSERT SQL
CREATE OR REPLACE FUNCTION _diag_insert_sql()
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
             CROSS JOIN generate_series(1, GREATEST(1, CEIL(50000::float / %s)::int)) d(date_idx)
             LIMIT 50000
         ) sub',
        pool_sz, pool_sz, pool_sz
    );
END $$ LANGUAGE plpgsql;


-- ==========================================================================
-- TEST A: Source INSERT, replica mode (no trigger, no FK, no checks)
-- ==========================================================================
\echo ''
\echo '--- TEST A: Source INSERT, replica mode ---'
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    SELECT _diag_insert_sql() INTO sql;
    SET LOCAL session_replication_role = replica;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings VALUES ('A_replica', 'source_insert', EXTRACT(EPOCH FROM t1 - t0) * 1000);
    -- cleanup still in replica mode
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$;


-- ==========================================================================
-- TEST B: Source INSERT, trigger disabled (FK still active but DEFERRED)
--         ALTER TABLE must be OUTSIDE the INSERT transaction
-- ==========================================================================
\echo ''
\echo '--- TEST B: Source INSERT, trigger disabled, FK active ---'
ALTER TABLE alp.sales_simulation DISABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";

DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    SELECT _diag_insert_sql() INTO sql;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings VALUES ('B_no_trigger_fk', 'source_insert', EXTRACT(EPOCH FROM t1 - t0) * 1000);
END $$;

-- Cleanup (still no trigger)
DO $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$;

ALTER TABLE alp.sales_simulation ENABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";


-- ==========================================================================
-- TEST C: Full INSERT with trigger
-- ==========================================================================
\echo ''
\echo '--- TEST C: Full 50K INSERT with trigger ---'
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    SELECT _diag_insert_sql() INTO sql;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings VALUES ('C_with_trigger', 'total', EXTRACT(EPOCH FROM t1 - t0) * 1000);
END $$;

-- Cleanup
DO $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$;


-- ==========================================================================
-- TEST D: Instrumented trigger (step-by-step timing)
-- ==========================================================================
\echo ''
\echo '--- TEST D: Instrumented trigger ---'

CREATE TEMP TABLE _saved_fn AS SELECT prosrc FROM pg_proc WHERE proname = '__reflex_ins_trigger_on_alp_sales_simulation';

CREATE OR REPLACE FUNCTION __reflex_ins_trigger_on_alp_sales_simulation() RETURNS TRIGGER AS $fn$
DECLARE
    _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
    _t0 TIMESTAMPTZ; _t_step TIMESTAMPTZ; _stmt_idx INT := 0;
BEGIN
    _t0 := clock_timestamp();

    _t_step := clock_timestamp();
    SELECT EXISTS(SELECT 1 FROM "__reflex_new_alp_sales_simulation" LIMIT 1) INTO _has_rows;
    INSERT INTO _timings VALUES ('D_instrumented', 'exists_check', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
    IF NOT _has_rows THEN RETURN NULL; END IF;

    _t_step := clock_timestamp();
    FOR _rec IN
        SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate
        FROM public.__reflex_ivm_reference
        WHERE 'alp.sales_simulation' = ANY(depends_on) AND enabled = TRUE
        ORDER BY graph_depth
    LOOP
        INSERT INTO _timings VALUES ('D_instrumented', 'ref_query', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        IF _rec.where_predicate IS NOT NULL THEN
            _t_step := clock_timestamp();
            EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)',
                '__reflex_new_alp_sales_simulation', _rec.where_predicate) INTO _pred_match;
            INSERT INTO _timings VALUES ('D_instrumented', 'pred_check', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
            IF NOT _pred_match THEN CONTINUE; END IF;
        END IF;

        _t_step := clock_timestamp();
        PERFORM pg_advisory_xact_lock(hashtext(_rec.name));
        INSERT INTO _timings VALUES ('D_instrumented', 'advisory_lock', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        _t_step := clock_timestamp();
        _sql := reflex_build_delta_sql(_rec.name, 'alp.sales_simulation', 'INSERT',
                    _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
        INSERT INTO _timings VALUES ('D_instrumented', 'rust_ffi', EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);

        IF _sql <> '' THEN
            FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
                IF _stmt <> '' THEN
                    _stmt_idx := _stmt_idx + 1;
                    _t_step := clock_timestamp();
                    EXECUTE _stmt;
                    INSERT INTO _timings VALUES ('D_instrumented',
                        'execute_stmt_' || _stmt_idx, EXTRACT(EPOCH FROM clock_timestamp() - _t_step) * 1000);
                END IF;
            END LOOP;
        END IF;
    END LOOP;

    INSERT INTO _timings VALUES ('D_instrumented', 'trigger_total', EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000);
    RETURN NULL;
END;
$fn$ LANGUAGE plpgsql;

DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    SELECT _diag_insert_sql() INTO sql;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings VALUES ('D_instrumented', 'wall_clock_total', EXTRACT(EPOCH FROM t1 - t0) * 1000);
END $$;

-- Restore original trigger
DO $$
DECLARE orig TEXT;
BEGIN
    SELECT prosrc INTO orig FROM _saved_fn;
    EXECUTE format('CREATE OR REPLACE FUNCTION __reflex_ins_trigger_on_alp_sales_simulation() RETURNS TRIGGER AS $fn$ %s $fn$ LANGUAGE plpgsql', orig);
END $$;
DROP TABLE _saved_fn;

-- Cleanup
DO $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$;


-- ==========================================================================
-- TEST E: Delta INSERT AFTER source INSERT (measures buffer contention)
--         Source INSERT with trigger disabled → then manual delta INSERT
-- ==========================================================================
\echo ''
\echo '--- TEST E: Delta INSERT after source INSERT (dirty buffer pool) ---'
ALTER TABLE alp.sales_simulation DISABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";

DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT; delta_sql TEXT; _stmt TEXT; _rec RECORD;
BEGIN
    SELECT _diag_insert_sql() INTO sql;

    -- Source INSERT (pollutes buffer pool)
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings VALUES ('E_post_source', 'source_insert_no_trig', EXTRACT(EPOCH FROM t1 - t0) * 1000);

    -- Build delta SQL
    SELECT name, base_query, end_query, aggregations::text AS aggregations
    INTO _rec FROM public.__reflex_ivm_reference WHERE name = 'alp.sop_forecast_reflex';
    delta_sql := reflex_build_delta_sql(_rec.name, 'alp.sales_simulation', 'INSERT',
                     _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);

    -- Create temp table mimicking transition table
    CREATE TEMP TABLE _diag_trans ON COMMIT DROP AS
    SELECT * FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;

    -- Replace transition table name with temp table
    delta_sql := replace(delta_sql, '"__reflex_new_alp_sales_simulation"', '_diag_trans');

    -- Execute delta INSERT (buffer pool is dirty from source INSERT)
    t0 := clock_timestamp();
    FOREACH _stmt IN ARRAY string_to_array(delta_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
        IF _stmt <> '' THEN
            _stmt := replace(_stmt, '"__reflex_new_alp_sales_simulation"', '_diag_trans');
            EXECUTE _stmt;
        END IF;
    END LOOP;
    t1 := clock_timestamp();
    INSERT INTO _timings VALUES ('E_post_source', 'delta_insert_dirty_buffers', EXTRACT(EPOCH FROM t1 - t0) * 1000);
END $$;

ALTER TABLE alp.sales_simulation ENABLE TRIGGER "__reflex_trigger_ins_on_alp_sales_simulation";

-- Cleanup
DO $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$;


-- ==========================================================================
-- TEST F: Cold-cache simulation — flush OS cache and repeat
--         We use pg_prewarm's reverse: drop caches via SQL
-- ==========================================================================
\echo ''
\echo '--- TEST F: After dropping from shared buffers (pg_buffercache eviction) ---'

-- Evict target table + indexes from shared buffers by reading a large unrelated table
-- (This is a best-effort simulation — we can't SYNC_FILE_RANGE from SQL)

-- First: force a checkpoint to flush dirty pages
CHECKPOINT;

-- Now evict buffers by scanning a large table (fills shared_buffers with other data)
DO $$
DECLARE cnt BIGINT;
BEGIN
    -- Scan the entire source table to evict target table pages from shared_buffers
    SELECT COUNT(*) INTO cnt FROM alp.sales_simulation;
    INSERT INTO _timings VALUES ('F_cold_sim', 'eviction_scan_count', cnt);
END $$;

-- Now run the full INSERT with trigger (target pages evicted)
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    SELECT _diag_insert_sql() INTO sql;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings VALUES ('F_cold_sim', 'full_insert_cold_target', EXTRACT(EPOCH FROM t1 - t0) * 1000);
END $$;

-- Cleanup
DO $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$;


-- ==========================================================================
-- TEST G: Repeat full INSERT (warm cache now — compare with F)
-- ==========================================================================
\echo ''
\echo '--- TEST G: Repeat full INSERT (warm cache, compare with F) ---'
DO $$
DECLARE t0 TIMESTAMPTZ; t1 TIMESTAMPTZ; sql TEXT;
BEGIN
    SELECT _diag_insert_sql() INTO sql;
    t0 := clock_timestamp();
    EXECUTE sql;
    t1 := clock_timestamp();
    INSERT INTO _timings VALUES ('G_warm_repeat', 'full_insert_warm', EXTRACT(EPOCH FROM t1 - t0) * 1000);
END $$;

-- Cleanup
DO $$
BEGIN
    SET LOCAL session_replication_role = replica;
    DELETE FROM alp.sales_simulation WHERE order_date >= '2028-01-07'::timestamptz;
    DELETE FROM alp.sop_forecast_reflex WHERE order_date >= '2028-01-07'::timestamptz;
    SET LOCAL session_replication_role = DEFAULT;
END $$;


-- ==========================================================================
-- BUFFER STATS
-- ==========================================================================
\echo ''
\echo '--- Buffer statistics since start of diagnostic ---'
SELECT
    buffers_checkpoint,
    buffers_clean,
    buffers_backend,
    buffers_alloc
FROM pg_stat_bgwriter;


-- ==========================================================================
-- RESULTS
-- ==========================================================================
\echo ''
\echo '================================================================'
\echo '  RESULTS SUMMARY'
\echo '================================================================'

SELECT test_name, step_name, ROUND(ms, 1) AS ms FROM _timings ORDER BY ts;

\echo ''
\echo '--- Computed breakdown ---'
WITH vals AS (
    SELECT test_name, step_name, ms FROM _timings
)
SELECT component, ms FROM (VALUES
    ('A: Source INSERT (replica, no trigger/FK)',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'A_replica')),
    ('B: Source INSERT (no trigger, FK active)',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'B_no_trigger_fk')),
    ('B-A: FK registration overhead',
     (SELECT ROUND((SELECT ms FROM vals WHERE test_name = 'B_no_trigger_fk') -
                    (SELECT ms FROM vals WHERE test_name = 'A_replica')))),
    ('C: Full INSERT with trigger',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'C_with_trigger')),
    ('C-B: Trigger overhead (total)',
     (SELECT ROUND((SELECT ms FROM vals WHERE test_name = 'C_with_trigger') -
                    (SELECT ms FROM vals WHERE test_name = 'B_no_trigger_fk')))),
    ('D: Trigger internal (delta EXECUTE)',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'D_instrumented' AND step_name = 'execute_stmt_1')),
    ('D: Trigger internal total',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'D_instrumented' AND step_name = 'trigger_total')),
    ('D: Wall clock (source + trigger)',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'D_instrumented' AND step_name = 'wall_clock_total')),
    ('E: Source INSERT alone (no trigger)',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'E_post_source' AND step_name = 'source_insert_no_trig')),
    ('E: Delta INSERT after source (dirty buffers)',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'E_post_source' AND step_name = 'delta_insert_dirty_buffers')),
    ('F: Full INSERT cold target (after buffer eviction)',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'F_cold_sim' AND step_name = 'full_insert_cold_target')),
    ('G: Full INSERT warm (repeat)',
     (SELECT ROUND(ms) FROM vals WHERE test_name = 'G_warm_repeat'))
) AS t(component, ms);


-- ==========================================================================
-- CLEANUP
-- ==========================================================================
DROP FUNCTION IF EXISTS _diag_insert_sql();
DROP TABLE IF EXISTS _diag_pool;
DROP TABLE IF EXISTS _timings;

\echo ''
\echo '================================================================'
\echo '  DIAGNOSTIC v2 COMPLETE'
\echo ''
\echo '  shared_buffers = 128MB'
\echo '  source table + indexes = ~18.7GB'
\echo '  target table + indexes = ~2-3GB'
\echo ''
\echo '  Key questions answered:'
\echo '  A vs B: FK overhead (should be ~0, deferred)'
\echo '  C vs B: pure trigger overhead'
\echo '  D steps: where time goes inside trigger'
\echo '  E: does dirty buffer pool slow the delta INSERT?'
\echo '  F vs G: cold vs warm cache impact'
\echo '================================================================'
