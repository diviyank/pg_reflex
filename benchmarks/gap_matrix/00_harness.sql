-- pg_reflex gap-matrix harness: results sink + measurement procedure.
-- Loaded first by every gap-matrix run. Safe to re-run (idempotent).
\set ON_ERROR_STOP on
\timing off

DO $$
BEGIN
    IF (SELECT extversion FROM pg_extension WHERE extname='pg_reflex') IS NULL THEN
        RAISE EXCEPTION 'pg_reflex not installed in this database';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS bench_gap_results (
    run_ts        timestamptz NOT NULL,
    layer         text NOT NULL,            -- 'synthetic' | 'dbclone'
    cell_label    text NOT NULL,
    shape         text,                     -- 'passthrough' | 'classic'
    cte           boolean,
    mode          text,                     -- 'IMMEDIATE' | 'DEFERRED'
    partitioned   text,                     -- 'none' | 'LIST'
    operation     text,                     -- INSERT|UPDATE|DELETE|FLIP_OUT|FLIP_IN|CASCADE
    base_rows     bigint,
    edit_rows     bigint,
    imv_ms        numeric,
    bare_ms       numeric,
    refresh_ms    numeric,
    advantage_pct numeric,
    mismatches    bigint,
    note          text
);

-- Drop an IMV if it exists, swallowing "not found" so setup is idempotent.
CREATE OR REPLACE PROCEDURE gap_drop_imv(_view text) LANGUAGE plpgsql AS $$
BEGIN
    PERFORM drop_reflex_ivm(_view);
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- One fair-baseline measurement of a single matrix cell. Commits per pass so
-- CTE chained IMVs (maintained at COMMIT) are exercised uniformly. Inserts one
-- row into bench_gap_results. Timing spans edit -> [flush] -> COMMIT.
CREATE OR REPLACE PROCEDURE gap_measure(
    run_ts        timestamptz,
    layer         text,
    cell_label    text,
    shape         text,
    cte           boolean,
    p_mode        text,
    partitioned   text,
    operation     text,
    base_rows     bigint,
    edit_rows     bigint,
    edit_imv_sql  text,
    edit_bare_sql text,
    flush_source  text,     -- NULL for IMMEDIATE / CTE
    imv_target    text,
    mv_name       text,
    fresh_sql     text
) LANGUAGE plpgsql AS $$
DECLARE
    t0 timestamptz; imv_ms numeric; bare_ms numeric; refresh_ms numeric;
    mm bigint; adv numeric;
BEGIN
    -- Pass A: IMV maintenance. For DEFERRED non-CTE, flush explicitly; for CTE,
    -- rely on the COMMIT below (do NOT also flush, to avoid double-maintenance).
    t0 := clock_timestamp();
    EXECUTE edit_imv_sql;
    IF p_mode = 'DEFERRED' AND NOT cte AND flush_source IS NOT NULL THEN
        EXECUTE format('SELECT reflex_flush_deferred(%L)', flush_source);
    END IF;
    COMMIT;
    imv_ms := EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000;

    -- Pass B: bare DML on the triggerless twin.
    t0 := clock_timestamp();
    EXECUTE edit_bare_sql;
    COMMIT;
    bare_ms := EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000;

    -- Pass C: full REFRESH of the matching MV.
    t0 := clock_timestamp();
    EXECUTE format('REFRESH MATERIALIZED VIEW %s', mv_name);
    COMMIT;
    refresh_ms := EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000;

    -- Correctness: IMV target vs fresh recompute of the body over *_imv.
    EXECUTE format(
        'SELECT count(*) FROM ( (SELECT * FROM %1$s EXCEPT ALL (%2$s)) '
        || 'UNION ALL ((%2$s) EXCEPT ALL SELECT * FROM %1$s) ) d',
        imv_target, fresh_sql) INTO mm;

    adv := CASE WHEN (bare_ms + refresh_ms) > 0
                THEN (1 - imv_ms / (bare_ms + refresh_ms)) * 100 ELSE NULL END;

    INSERT INTO bench_gap_results VALUES (
        run_ts, layer, cell_label, shape, cte, p_mode, partitioned, operation,
        base_rows, edit_rows, round(imv_ms,1), round(bare_ms,1), round(refresh_ms,1),
        round(adv,1), mm,
        CASE WHEN mm <> 0 THEN 'INVALID: mismatch' ELSE NULL END);
    COMMIT;
END $$;
