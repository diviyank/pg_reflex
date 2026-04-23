-- Migration: pg_reflex 1.1.2 -> 1.1.3
--
-- Changes:
--   #3  Targeted refresh (group-by IMVs) now wrapped in a DO block that skips
--       the DELETE+INSERT when the affected-groups staging table is empty.
--       This avoids a full target-table scan on transactions that produce no
--       matching groups.
--   #9  reflex_build_delta_sql and reflex_build_truncate_sql annotated
--       PARALLEL SAFE (they read no shared state and produce deterministic SQL).
--   #11 reflex_flush_deferred now ANALYZEs the staging delta table before
--       processing, so the planner gets correct row estimates after the
--       TRUNCATE that reset stats to zero.
--   #12a Deferred UPDATE trigger bodies: add _pred_match BOOLEAN, include
--       where_predicate in the IMV registry SELECT, and check the predicate
--       before acquiring the advisory lock.
--   #12b reflex_flush_deferred: include where_predicate in the DEFERRED IMV
--       query and skip IMVs whose predicate matches no staged row.
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.1.3';

-- === #9: PARALLEL SAFE annotations ===

ALTER FUNCTION reflex_build_delta_sql(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT)
    PARALLEL SAFE;

ALTER FUNCTION reflex_build_truncate_sql(TEXT)
    PARALLEL SAFE;

-- === #12a: Patch stored deferred UPDATE trigger bodies ===
-- Add _pred_match BOOLEAN to DECLARE, add where_predicate to the IMV SELECT,
-- and insert a predicate check before pg_advisory_xact_lock.
DO $migration$
DECLARE
    _proc RECORD;
    _new_src TEXT;
    _patched INT := 0;
BEGIN
    FOR _proc IN
        SELECT oid, proname, prosrc
        FROM pg_proc
        WHERE prosrc LIKE '%_has_deferred BOOLEAN := FALSE%'
          AND prosrc LIKE '%COALESCE(refresh_mode%'
          AND prosrc NOT LIKE '%where_predicate%'
    LOOP
        _new_src := _proc.prosrc;

        -- 1. Add _pred_match BOOLEAN to DECLARE section
        _new_src := replace(
            _new_src,
            '_has_rows BOOLEAN;',
            '_has_rows BOOLEAN; _pred_match BOOLEAN;'
        );

        -- 2. Add where_predicate to the IMV SELECT
        _new_src := replace(
            _new_src,
            'COALESCE(refresh_mode, ''IMMEDIATE'') AS refresh_mode',
            'COALESCE(refresh_mode, ''IMMEDIATE'') AS refresh_mode, where_predicate'
        );

        -- 3. Insert predicate check before the advisory lock acquisition
        _new_src := replace(
            _new_src,
            'IF _rec.refresh_mode = ''IMMEDIATE'' THEN PERFORM pg_advisory_xact_lock',
            'IF _rec.where_predicate IS NOT NULL THEN '
            'EXECUTE format(''SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)'', '
            'TG_TABLE_NAME, _rec.where_predicate) INTO _pred_match; '
            'IF NOT _pred_match THEN CONTINUE; END IF; END IF; '
            'IF _rec.refresh_mode = ''IMMEDIATE'' THEN PERFORM pg_advisory_xact_lock'
        );

        IF _new_src IS DISTINCT FROM _proc.prosrc THEN
            UPDATE pg_proc SET prosrc = _new_src WHERE oid = _proc.oid;
            _patched := _patched + 1;
        END IF;
    END LOOP;
    RAISE NOTICE 'pg_reflex 1.1.2 -> 1.1.3: patched % deferred UPDATE trigger bodies', _patched;
END;
$migration$;
