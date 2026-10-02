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
--      stale needlessly.
--
--   5. The IMMEDIATE INSERT / DELETE / UPDATE trigger bodies of every existing
--      source lose their pre-scratch "Path B" block, which rebuilt the IMV
--      (`reflex_reconcile`) when the statement changed a large share of the
--      source. Another trigger of the same statement (an upsert's INSERT after
--      its UPDATE, MERGE, a writable CTE) then applied its delta on top of a
--      rebuild that already read it: a silent double count for an aggregate, a
--      23505 for a passthrough. The block is cut out of each installed body
--      in place. No other per-source trigger body changed in 1.11.5.
--
--   6. `__reflex_partition_child_for_key` resolves a NULL key to the child
--      holding NULL (DEFAULT, or a LIST child listing NULL) instead of to no
--      child, so the DEFERRED flush dispatch rebuilds a hot DEFAULT child's
--      NULL rows once instead of also merging them cold.
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
-- Steps 3 and 5 take no lock on the sources. It reports how many deferred TRUNCATE
-- bodies it found and rewrote (INFO); a body it cannot rewrite is listed in a
-- WARNING with its remedy and skipped without aborting the upgrade.
--
-- UPGRADE WINDOW — writes fail until this update runs. The 1.11.5 library
-- generates SQL that calls the five functions above, so between installing
-- the library and running this update in a database:
--   * the flush of a DEFERRED grouped aggregate or partitioned passthrough
--     IMV fails (volume dispatch: `__reflex_target_propagates`,
--     `__reflex_rebuild_cost_rows`);
--   * a TRUNCATE of a source of an IMMEDIATE IMV and the trigger-side full
--     refreshes (including every write to a source a passthrough IMMEDIATE
--     IMV has no key mapping for) fail (`reflex_rebuild_target_rows`);
--   * a COMMIT that leaves a DEFERRED IMV to rebuild (e.g. two of its sources
--     written) aborts (`__reflex_xid_is_current`, `__reflex_xid_precedes`);
--   * a TRUNCATE of a source of a DEFERRED IMV commits and marks the IMV
--     known_stale.
-- The failures are loud — the write is rolled back, no IMV silently diverges.
-- Operator sequence, in a quiet window: install the library; immediately run
-- `ALTER EXTENSION pg_reflex UPDATE TO '1.11.5';` in every database that has
-- the extension; recycle connection pools; `reflex_reconcile` any IMV left
-- known_stale.

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
    _found INT := 0;
    _rewritten INT := 0;
    _current INT := 0;
    _skipped TEXT[] := ARRAY[]::TEXT[];
BEGIN
    FOR _fn IN
        SELECT p.oid::regprocedure AS fn,
               substring(p.prosrc FROM 'source_table = ''([^'']*)''') AS src,
               p.prosrc ~ 'DELETE FROM public\.__reflex_deferred_pending WHERE source_table = ''[^'']*'' AND operation <> ''TRUNCATE'';' AS is_current,
               p.prosrc ~ 'DELETE FROM public\.__reflex_deferred_pending WHERE source_table = ''[^'']*'';' AS is_rewritable
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.prorettype = 'trigger'::regtype
          AND p.proname LIKE '\_\_reflex\_trunc\_trigger\_on\_%'
          AND strpos(p.prosrc, '__reflex_deferred_pending') > 0
        ORDER BY p.proname
    LOOP
        _found := _found + 1;
        IF _fn.is_current THEN
            _current := _current + 1;
            CONTINUE;
        END IF;
        IF NOT _fn.is_rewritable THEN
            _skipped := _skipped || format('%s (source %s): unrecognised body', _fn.fn, COALESCE(_fn.src, '?'));
            CONTINUE;
        END IF;
        BEGIN
            EXECUTE regexp_replace(
                pg_get_functiondef(_fn.fn),
                '(DELETE FROM public\.__reflex_deferred_pending WHERE source_table = ''[^'']*'');',
                '\1 AND operation <> ''TRUNCATE'';');
            _rewritten := _rewritten + 1;
        EXCEPTION WHEN OTHERS THEN
            _skipped := _skipped || format('%s (source %s): %s', _fn.fn, COALESCE(_fn.src, '?'), SQLERRM);
        END;
    END LOOP;
    -- INFO, not NOTICE: an extension script runs with client_min_messages raised to WARNING.
    RAISE INFO 'pg_reflex 1.11.5: rewrote % of % deferred TRUNCATE trigger bodies found (% already current, % skipped)',
        _rewritten, _found, _current, COALESCE(array_length(_skipped, 1), 0);
    IF COALESCE(array_length(_skipped, 1), 0) > 0 THEN
        RAISE WARNING '%', format('pg_reflex 1.11.5: %s deferred TRUNCATE trigger bodies were NOT rewritten: %s. '
            'Until repaired, a TRUNCATE of such a source can mark its DEFERRED IMVs stale needlessly (reflex_reconcile clears it). '
            'Remedy, after the upgrade: create, then drop, a throwaway DEFERRED IMV over each source listed, spelled as listed '
            '(create_reflex_ivm re-renders the source''s trigger bodies; reflex_rebuild_triggers does not reach the live triggers of a bare-named source).',
            array_length(_skipped, 1), array_to_string(_skipped, '; '));
    END IF;
END
$regen$;

-- === Statement-trigger bodies no longer rebuild (step 5) ===
-- The Path B block and its DECLARE line are rendered verbatim from
-- sql/trigger_body.plpgsql.in (unchanged from 1.4.6 to 1.11.4) and contain only
-- the source-name slot, so they are cut by position: a 1.11.4 body becomes what
-- 1.11.5 renders for its source.

DO $stmt_triggers$
DECLARE
    _fn RECORD;
    _def TEXT;
    _start INT;
    _len INT;
    _found INT := 0;
    _rewritten INT := 0;
    _skipped TEXT[] := ARRAY[]::TEXT[];
    _decl CONSTANT TEXT := E'        _pre_trans_count BIGINT; _pre_src_total BIGINT; _pre_thr NUMERIC; _pre_per_imv NUMERIC; _pre_ratio NUMERIC;\n';
    _head CONSTANT TEXT := E'      BEGIN\n        SELECT reltuples::BIGINT INTO _pre_src_total FROM pg_class WHERE oid = ';
    _tail CONSTANT TEXT := E'      EXCEPTION WHEN OTHERS THEN NULL; END;\n';
BEGIN
    FOR _fn IN
        SELECT p.oid::regprocedure AS fn,
               substring(p.prosrc FROM 'WHERE ''([^'']*)'' = ANY\(depends_on\)') AS src
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.prorettype = 'trigger'::regtype
          AND strpos(p.prosrc, 'pg_reflex Path B: dispatching') > 0
        ORDER BY p.proname
    LOOP
        _found := _found + 1;
        _def := pg_get_functiondef(_fn.fn);
        _start := strpos(_def, _head);
        _len := CASE WHEN _start > 0 THEN strpos(substr(_def, _start), _tail) ELSE 0 END;
        IF _start = 0 OR _len = 0 OR strpos(_def, _decl) = 0
           OR strpos(substr(_def, _start, _len), 'PERFORM public.reflex_reconcile(_rec.name);') = 0 THEN
            _skipped := _skipped || format('%s (source %s): unrecognised body', _fn.fn, COALESCE(_fn.src, '?'));
            CONTINUE;
        END IF;
        _def := substr(_def, 1, _start - 1) || substr(_def, _start + _len - 1 + length(_tail));
        _def := replace(_def, _decl, '');
        BEGIN
            EXECUTE _def;
            _rewritten := _rewritten + 1;
        EXCEPTION WHEN OTHERS THEN
            _skipped := _skipped || format('%s (source %s): %s', _fn.fn, COALESCE(_fn.src, '?'), SQLERRM);
        END;
    END LOOP;
    RAISE INFO 'pg_reflex 1.11.5: removed the rebuild (Path B) from % of % statement-trigger bodies found (% skipped)',
        _rewritten, _found, COALESCE(array_length(_skipped, 1), 0);
    IF COALESCE(array_length(_skipped, 1), 0) > 0 THEN
        RAISE WARNING '%', format('pg_reflex 1.11.5: %s statement-trigger bodies still rebuild the IMV on a large statement: %s. '
            'Until repaired, a statement that both updates and inserts (upsert, MERGE, writable CTE) a large share of such a source '
            'can double count its aggregate IMVs. Remedy, after the upgrade: create, then drop, a throwaway DEFERRED IMV over each source listed, '
            'spelled as listed (create_reflex_ivm re-renders the source''s trigger bodies; reflex_rebuild_triggers does not reach '
            'the live triggers of a bare-named source), then reflex_reconcile its IMVs.',
            array_length(_skipped, 1), array_to_string(_skipped, '; '));
    END IF;
END
$stmt_triggers$;

-- === A NULL partition key resolves to its DEFAULT child (step 6) ===
-- Same body as the extension_sql! in src/lib.rs.

CREATE OR REPLACE FUNCTION public.__reflex_partition_child_for_key(
    parent regclass, part_col TEXT, k TEXT
) RETURNS regclass
LANGUAGE plpgsql STABLE AS $REFLEX$
DECLARE
    _r RECORD;
    _expr TEXT;
    _match BOOLEAN;
    _ident_re TEXT;
BEGIN
    IF parent IS NULL OR part_col IS NULL THEN
        RETURN NULL;
    END IF;
    _ident_re := '\m(?:' || regexp_replace(part_col, '([\\.+*?^$()\[\]{}|])', '\\\1', 'g')
                 || ')\M';
    FOR _r IN
        SELECT c.oid::regclass AS rc,
               pg_get_partition_constraintdef(c.oid) AS def
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = parent
    LOOP
        IF _r.def IS NULL OR _r.def = '' THEN CONTINUE; END IF;
        _expr := regexp_replace(_r.def, _ident_re, COALESCE(quote_literal(k), 'NULL'), 'gi');
        BEGIN
            EXECUTE 'SELECT (' || _expr || ')::boolean' INTO _match;
        EXCEPTION WHEN OTHERS THEN
            _match := FALSE;
        END;
        IF _match THEN
            RETURN _r.rc;
        END IF;
    END LOOP;
    RETURN NULL;
END;
$REFLEX$;

-- === Leftover truncate flush requests ===

DO $leftover$
BEGIN
    IF to_regclass('public.__reflex_deferred_pending') IS NOT NULL THEN
        DELETE FROM public.__reflex_deferred_pending WHERE operation = 'TRUNCATE';
    END IF;
END
$leftover$;
