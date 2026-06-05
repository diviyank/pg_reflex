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
