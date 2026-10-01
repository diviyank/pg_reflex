-- Migration: pg_reflex 1.11.4 → 1.11.5
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.11.5';
--
-- Rebuilds of an IMV that has dependents hand those dependents a row diff
-- instead of clearing or fully rebuilding them, after a field incident
-- (2026-09-30) where a TRUNCATE + INSERT rebuild of one IMV wiped every row of
-- a 123 M-row IMV that LEFT JOINs it. Most of the change is Rust-side; this
-- file carries the installed-SQL part:
--
--   1. New Rust-backed functions: `reflex_rebuild_target_rows` (the
--      dependent-safe target rebuild), `__reflex_target_propagates` and
--      `__reflex_rebuild_cost_rows` (volume dispatch: higher threshold with
--      dependents, two-level partitions sized by their leaves), and
--      `__reflex_xid_is_current` (exact "row written by this transaction"
--      test used by the COMMIT-time rebuild of DEFERRED IMVs) and
--      `__reflex_xid_precedes` (wraparound-safe xid order, used to place a
--      staged row relative to that rebuild).
--
--   2. `__reflex_deferred_flush_fn` deletes its own 'TRUNCATE' request row
--      (`reflex_build_truncate_sql` enqueues one per DEFERRED IMV whose source
--      was truncated) before flushing, so the row lives exactly as long as
--      its queued event.
--
--   3. The deferred TRUNCATE trigger body of every existing source now keeps
--      the 'TRUNCATE' request rows when it clears the source's pending rows;
--      its one changed statement is rewritten in place. A 1.11.4 body deletes
--      them early, so a COMMIT that postpones a rebuild can take a request
--      whose event is still queued for one already fired, and mark the IMV
--      stale needlessly. No other per-source trigger body changed in 1.11.5.
--
--   4. 'TRUNCATE' request rows committed by a 1.11.5 library running with the
--      1.11.4 flush function (between the library install and this update)
--      are deleted: their events have fired, and nothing else removes them.
--
-- The function DDL below is the pgrx-generated SQL (`cargo pgrx schema`) and
-- the runtime DDL of `schema_builder::build_deferred_flush_ddl` verbatim apart
-- from indentation, so fresh installs and upgrades converge. If you edit one,
-- edit both.
--
-- Step 3 takes no lock on the sources. A body that cannot be rewritten is
-- reported by a WARNING and skipped without aborting the upgrade; run
-- `SELECT reflex_rebuild_triggers('<source>');` for it afterwards.
--
-- Install the 1.11.5 library and run this update together: until the update,
-- a 1.11.5 library over the 1.11.4 catalog cannot rebuild after a TRUNCATE
-- (`reflex_rebuild_target_rows` is missing) and marks the IMV stale instead.

-- === New functions ===

CREATE FUNCTION "reflex_rebuild_target_rows"(
	"view_name" TEXT,
	"rebuild_sql" TEXT
) RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_rebuild_target_rows_wrapper';

CREATE FUNCTION "__reflex_target_propagates"(
	"view_name" TEXT
) RETURNS bool
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', '__reflex_target_propagates_wrapper';

CREATE FUNCTION "__reflex_rebuild_cost_rows"(
	"view_name" TEXT,
	"child" oid
) RETURNS double precision
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', '__reflex_rebuild_cost_rows_wrapper';

CREATE FUNCTION "__reflex_xid_is_current"(
	"xid" xid
) RETURNS bool
STRICT STABLE PARALLEL UNSAFE
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_xid_is_current_wrapper';

CREATE FUNCTION "__reflex_xid_precedes"(
	"a" xid,
	"b" xid
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_xid_precedes_wrapper';

-- === Deferred flush: a 'TRUNCATE' request row is removed by its own event ===
-- Created by the first DEFERRED `create_reflex_ivm`, so it may be absent or,
-- on an install predating 1.8.0's adoption, not yet an extension member, in
-- which case CREATE OR REPLACE is refused during ALTER EXTENSION: adopt first.

DO $adopt$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
               WHERE p.proname = '__reflex_deferred_flush_fn' AND n.nspname = 'public')
       AND NOT EXISTS (
           SELECT 1 FROM pg_depend d
           JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'pg_reflex'
           JOIN pg_proc p ON p.oid = d.objid
           JOIN pg_namespace n ON n.oid = p.pronamespace
           WHERE d.deptype = 'e' AND p.proname = '__reflex_deferred_flush_fn' AND n.nspname = 'public')
    THEN
        ALTER EXTENSION pg_reflex ADD FUNCTION public.__reflex_deferred_flush_fn();
    END IF;
END
$adopt$;

CREATE OR REPLACE FUNCTION public.__reflex_deferred_flush_fn() RETURNS TRIGGER AS $fn$
BEGIN
    IF NEW.operation = 'TRUNCATE' THEN
        DELETE FROM public.__reflex_deferred_pending WHERE id = NEW.id;
    END IF;
    PERFORM public.reflex_flush_deferred(NEW.source_table);
    RETURN NULL;
END;
$fn$ LANGUAGE plpgsql;

-- === Regenerate the deferred TRUNCATE bodies of existing sources ===
-- The only per-source body that changed is the deferred TRUNCATE one, and only
-- in its pending-row DELETE (sql/deferred_trigger_truncate_body.plpgsql.in), so
-- that statement is rewritten in place in each installed body; a 1.11.4 body
-- becomes what 1.11.5 renders for its source. `reflex_rebuild_triggers` is not used:
-- inside ALTER EXTENSION its passthrough scratch heal (`CREATE TABLE IF NOT
-- EXISTS` on tables that are not extension members) is refused, and it would
-- re-create every trigger, locking every source. Replacing a function body
-- takes no lock on the source.

DO $regen$
DECLARE
    _fn RECORD;
BEGIN
    FOR _fn IN
        SELECT p.oid::regprocedure AS fn,
               substring(p.prosrc FROM 'DELETE FROM public\.__reflex_deferred_pending WHERE source_table = ''([^'']*)''') AS src
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.prorettype = 'trigger'::regtype
          AND p.prosrc ~ 'DELETE FROM public\.__reflex_deferred_pending WHERE source_table = ''[^'']*'';'
        ORDER BY p.proname
    LOOP
        BEGIN
            EXECUTE regexp_replace(
                pg_get_functiondef(_fn.fn),
                '(DELETE FROM public\.__reflex_deferred_pending WHERE source_table = ''[^'']*'');',
                '\1 AND operation <> ''TRUNCATE'';');
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '%', format('pg_reflex 1.11.5: could not update %s: %s — run SELECT reflex_rebuild_triggers(%L) after the upgrade',
                _fn.fn, SQLERRM, _fn.src);
        END;
    END LOOP;
END
$regen$;

-- === Leftover truncate flush requests ===

DO $leftover$
BEGIN
    IF to_regclass('public.__reflex_deferred_pending') IS NOT NULL THEN
        DELETE FROM public.__reflex_deferred_pending WHERE operation = 'TRUNCATE';
    END IF;
END
$leftover$;
