-- Migration: pg_reflex 1.12.4 → 1.12.5
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.12.5';
--
-- A correctness release: a deadlock between operator calls and the COMMIT
-- flush, the double count of the joint term ΔA ⋈ ΔB when two sources of one
-- join IMV change together, and IMVs left stale by a DROP TABLE of a source
-- partition. Most of it is in the compiled library. The installed-SQL part:
--
--   1. New Rust-backed functions, called by the 1.12.5 statement-trigger
--      bodies: `reflex_build_delta_sql_resolving` (the delta of one source,
--      corrected for the delta of another source of the same statement),
--      `reflex_joint_resolved_delta_sql`, `reflex_joint_skipped_delta_sql`
--      and `reflex_joint_delta_may_be_skipped`.
--
--   2. New SQL functions `reflex_joint_stash_text(anyarray)` and
--      `reflex_joint_stashed_rows(text, anyarray)`: they write and read the
--      rows one source's delta keeps for the other's, under pinned settings.
--
--   3. `public.__reflex_on_sql_drop()` (the `sql_drop` event trigger
--      function) is replaced: a DROP TABLE of a partition of a source queues
--      its root for the COMMIT-time partition flush, so the IMVs over the
--      source lose the dropped rows.
--
--   4. The per-source trigger functions of every existing source are
--      rewritten in place to what 1.12.5 renders for that source:
--        * every delta body (IMMEDIATE and DEFERRED flavours) loops over the
--          IMVs of the source readers first (`ORDER BY graph_depth DESC`), so
--          an IMV joining a source and an IMV over that source counts the
--          joint delta once;
--        * the IMMEDIATE statement bodies call
--          `reflex_build_delta_sql_resolving`, run the correction it hands
--          back, and record the rows of each delta they skip.
--      `reflex_rebuild_triggers` is not used: inside ALTER EXTENSION it is
--      refused or silently reaches no live trigger (see
--      untreated_bugs/2026-10-01_migration_trigger_regeneration_silently_noop.md).
--      Replacing a function body takes no lock on the source. The TRUNCATE
--      bodies are unchanged.
--
-- Each new or replaced function below is the pgrx-generated SQL
-- (`cargo pgrx schema`) or the bootstrap text of `src/lib.rs`, byte for byte.
-- No registry column, table, index, trigger or event trigger changes.
--
-- Step 4 reports how many bodies it rewrote (INFO). A body it cannot match
-- to the 1.12.4 text is left as it is, listed in a WARNING with its source,
-- and the upgrade goes on. Such a body never double counts a two-source
-- statement (it recomputes the IMV instead), but a DEFERRED-flavour body
-- left in ascending order keeps the joint double count of an IMMEDIATE IMV
-- that joins a source and another IMMEDIATE IMV over it. The WARNING gives
-- the remedy.
--
-- UPGRADE WINDOW. Old bodies stay correct with the 1.12.5 library (they keep
-- no stash and recompute a two-source statement). But an IMV created between
-- installing the library and running this update renders bodies that call the
-- functions of step 1, so writes to its sources fail until the update runs.
-- Run the update right after replacing the library, before creating IMVs.

-- === 1. New Rust-backed functions ===

CREATE  FUNCTION "reflex_build_delta_sql_resolving"(
	"view_name" TEXT, /* & str */
	"source_table" TEXT, /* & str */
	"operation" TEXT, /* & str */
	"base_query" TEXT, /* & str */
	"end_query" TEXT, /* & str */
	"aggregations_json" TEXT, /* Option < & str > */
	"orig_base_query" TEXT /* & str */
) RETURNS TEXT /* String */
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_build_delta_sql_resolving_wrapper';

CREATE  FUNCTION "reflex_joint_delta_may_be_skipped"(
	"view_name" TEXT, /* & str */
	"source_table" TEXT, /* & str */
	"base_query" TEXT /* & str */
) RETURNS bool /* bool */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_joint_delta_may_be_skipped_wrapper';

CREATE  FUNCTION "reflex_joint_resolved_delta_sql"(
	"view_name" TEXT, /* & str */
	"source_table" TEXT, /* & str */
	"operation" TEXT, /* & str */
	"counted" bigint, /* i64 */
	"group_missing" bool /* bool */
) RETURNS TEXT /* String */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_joint_resolved_delta_sql_wrapper';

CREATE  FUNCTION "reflex_joint_skipped_delta_sql"(
	"view_name" TEXT, /* & str */
	"source_table" TEXT, /* & str */
	"base_query" TEXT, /* & str */
	"transition_table" TEXT /* & str */
) RETURNS TEXT /* String */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_joint_skipped_delta_sql_wrapper';

-- === 2. Stash text functions (bootstrap text of src/lib.rs) ===

    CREATE OR REPLACE FUNCTION public.reflex_joint_stash_text(stashed anyarray)
    RETURNS text LANGUAGE sql STABLE
    SET extra_float_digits = 3 SET DateStyle = 'ISO, YMD' SET IntervalStyle = 'postgres'
    SET TimeZone = 'UTC' SET bytea_output = 'hex'
    AS 'SELECT $1::text';

    CREATE OR REPLACE FUNCTION public.reflex_joint_stashed_rows(stashed text, row_type anyarray)
    RETURNS anyarray LANGUAGE plpgsql STABLE
    SET extra_float_digits = 3 SET DateStyle = 'ISO, YMD' SET IntervalStyle = 'postgres'
    SET TimeZone = 'UTC' SET bytea_output = 'hex'
    AS $fn$ DECLARE parsed ALIAS FOR $0; BEGIN parsed := stashed; RETURN parsed; END $fn$;

-- === 3. sql_drop event trigger function (bootstrap text of src/lib.rs) ===

    CREATE OR REPLACE FUNCTION public.__reflex_on_sql_drop()
    RETURNS event_trigger LANGUAGE plpgsql AS $$
    DECLARE
        _obj RECORD;
        _imv RECORD;
        _stale_root TEXT;
    BEGIN
        FOR _obj IN
            SELECT objid, object_identity, schema_name, object_name,
                   (schema_name || object_name) !~ '[."A-Z]' AS reads_back_unquoted
            FROM pg_event_trigger_dropped_objects()
            WHERE object_type = 'table'
        LOOP
            -- A dropped partition of a tracked source: the table is gone, so its
            -- root comes from the snapshot (matched by oid, never by name) and is
            -- queued for the COMMIT-time partition flush, which classifies the
            -- vanished child as a detach. Never queued for a root that is an IMV
            -- (pg_reflex's own swap drops IMV partitions; the cascade is
            -- code-driven). A root dropped in the same statement is reported
            -- before its partitions, so its IMVs are already torn down by the
            -- source branch below and no enabled reader is left to match.
            -- A role that cannot read the snapshot cannot tell a tracked partition
            -- from any other table, so it is skipped: reading it would raise on
            -- every unrelated DROP. A role that finds a tracked partition but
            -- cannot write the queue (INSERT .. ON CONFLICT DO UPDATE needs
            -- SELECT, INSERT and UPDATE on it) is told loudly instead.
            IF has_table_privilege('public.__reflex_source_partition_snapshot', 'SELECT') THEN
                FOR _stale_root IN
                    SELECT DISTINCT s.source_root
                    FROM public.__reflex_source_partition_snapshot s
                    WHERE s.child_oid = _obj.objid::bigint
                      AND s.source_root NOT LIKE '%\_\_reflex\_%'
                      AND NOT EXISTS (
                          SELECT 1 FROM public.__reflex_ivm_reference r
                          WHERE public.__reflex_named_relation(
                                    CASE WHEN r.target_schema IS NOT NULL AND r.name NOT LIKE '%.%'
                                         THEN quote_ident(r.target_schema) || '.' || r.name
                                         ELSE r.name END)
                                = public.__reflex_named_relation(s.source_root))
                      AND EXISTS (
                          SELECT 1 FROM public.__reflex_ivm_reference r
                          WHERE r.enabled
                            AND public.__reflex_reads_relation(r.depends_on, s.source_root))
                LOOP
                    IF has_table_privilege('public.__reflex_partition_pending', 'SELECT')
                       AND has_table_privilege('public.__reflex_partition_pending', 'INSERT')
                       AND has_table_privilege('public.__reflex_partition_pending', 'UPDATE') THEN
                        INSERT INTO public.__reflex_partition_pending (source_root)
                        VALUES (_stale_root)
                        ON CONFLICT (source_root)
                        DO UPDATE SET enqueued_at = statement_timestamp(),
                                      attempts    = public.__reflex_partition_pending.attempts + 1;
                    ELSE
                        RAISE WARNING 'pg_reflex: dropped partition % of source %, but this role cannot write the partition queue; the IMVs over it are stale — run SELECT reflex_flush_partition_source(''%'') as a role that can',
                            _obj.object_identity, _stale_root, _stale_root;
                    END IF;
                END LOOP;
            END IF;

            -- A source is registered without a schema only when it is a table of
            -- public: a table of another schema with the same name is another table.
            -- A name PostgreSQL prints quoted but reads back bare (a non-ASCII
            -- letter, a `$`) is registered bare.
            FOR _imv IN
                SELECT name
                FROM public.__reflex_ivm_reference
                WHERE depends_on @> ARRAY[_obj.object_identity]
                   OR (_obj.schema_name = 'public'
                       AND depends_on @> ARRAY[split_part(_obj.object_identity, '.', 2)])
                   OR (_obj.reads_back_unquoted
                       AND (depends_on @> ARRAY[_obj.schema_name || '.' || _obj.object_name]
                            OR (_obj.schema_name = 'public'
                                AND depends_on @> ARRAY[_obj.object_name])))
                ORDER BY graph_depth DESC, name DESC
            LOOP
                BEGIN
                    PERFORM public.drop_reflex_ivm(_imv.name, TRUE);
                    RAISE NOTICE 'pg_reflex: dropped IMV % (source % was dropped)', _imv.name, _obj.object_identity;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'pg_reflex: failed to drop IMV % after source % drop: %',
                        _imv.name, _obj.object_identity, SQLERRM;
                    DELETE FROM public.__reflex_ivm_reference WHERE name = _imv.name;
                END;
            END LOOP;

            -- An IMV whose own *target* table was dropped (e.g. DROP SCHEMA …
            -- CASCADE, or a stray DROP TABLE) must also be torn down, otherwise
            -- the registry row orphans, pointing at a relation that no longer
            -- exists. The source branch above never catches this when the
            -- source is a view or lives outside the dropped scope. Match on the
            -- EXACT target identity (target_schema + bare name) — never a prefix
            -- — so a partition swap dropping child / __reflex_swap_* tables can
            -- never be mistaken for the registered target.
            FOR _imv IN
                SELECT name
                FROM public.__reflex_ivm_reference
                WHERE COALESCE(target_schema, 'public') || '.'
                      || (regexp_match(name, '([^.]+)$'))[1] = _obj.object_identity
                ORDER BY graph_depth DESC, name DESC
            LOOP
                BEGIN
                    PERFORM public.drop_reflex_ivm(_imv.name, TRUE);
                    RAISE NOTICE 'pg_reflex: dropped IMV % (target % was dropped)', _imv.name, _obj.object_identity;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'pg_reflex: failed to drop IMV % after target % drop: %',
                        _imv.name, _obj.object_identity, SQLERRM;
                    DELETE FROM public.__reflex_ivm_reference WHERE name = _imv.name;
                END;
            END LOOP;
        END LOOP;
    END;
    $$;

-- === 4. Rewrite the per-source delta trigger bodies in place ===
-- The 1.12.4 bodies are rendered from sql/trigger_body.plpgsql.in and
-- sql/deferred_trigger{,_update}_body.plpgsql.in, whose changed lines hold no
-- slot but the source name and the transition table, both read back from the
-- body. Each change below is an exact text replacement of what 1.12.4
-- renders by what 1.12.5 renders; a rewritten body is then checked as a whole
-- (every CONTINUE records its skipped rows, no 1.12.4 call or loop is left)
-- before it is installed. Re-running this block is harmless: a current body
-- is counted and left alone.

DO $regen$
DECLARE
    _fn RECORD;
    _src TEXT;
    _new TEXT;
    _skipped_hook TEXT;
    _may_skip TEXT;
    _found INT := 0;
    _rewritten INT := 0;
    _current INT := 0;
    _skipped TEXT[] := ARRAY[]::TEXT[];
    _old_declare CONSTANT TEXT := $t$_directional_op TEXT;
BEGIN$t$;
    _new_declare CONSTANT TEXT := $t$_directional_op TEXT; _skipped_sql TEXT; _then TEXT;
BEGIN$t$;
    _old_order CONSTANT TEXT := 'ORDER BY graph_depth, name';
    _new_order CONSTANT TEXT := 'ORDER BY graph_depth DESC, name';
    _old_ignored CONSTANT TEXT := $t$ = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
$t$;
    _old_pred CONSTANT TEXT := 'IF NOT _pred_match THEN CONTINUE; END IF;';
    _old_filter CONSTANT TEXT := 'IF _filter_skip THEN CONTINUE; END IF;';
    _old_path_c CONSTANT TEXT := $t$WHERE name = _rec.name;
                   CONTINUE;$t$;
    _old_call CONSTANT TEXT := '_sql := public.reflex_build_delta_sql(';
    _new_call CONSTANT TEXT := '_sql := public.reflex_build_delta_sql_resolving(';
    _old_apply CONSTANT TEXT := $t$      IF _sql <> '' THEN
        FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
          IF _stmt <> '' THEN EXECUTE _stmt; END IF;
        END LOOP;
      END IF;
$t$;
    _new_apply CONSTANT TEXT := $t$      WHILE _sql <> '' LOOP
        _then := '';
        FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
          IF starts_with(_stmt, '--<<REFLEX_THEN>>--') THEN EXECUTE _stmt INTO _then;
          ELSIF _stmt <> '' THEN EXECUTE _stmt;
          END IF;
        END LOOP;
        _sql := _then;
      END LOOP;
$t$;
BEGIN
    FOR _fn IN
        SELECT p.oid::regprocedure AS fn,
               p.prosrc AS body,
               substring(p.prosrc FROM 'WHERE ''([^'']*)'' = ANY\(depends_on\)') AS src,
               substring(p.prosrc FROM 'SELECT EXISTS\(SELECT 1 FROM "([^"]*)" LIMIT 1\) INTO _has_rows;') AS transition,
               strpos(p.prosrc, '_has_deferred') > 0 AS is_deferred
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.prorettype = 'trigger'::regtype
          AND strpos(p.prosrc, '= ANY(depends_on) AND enabled = TRUE') > 0
          AND strpos(p.prosrc, 'public.reflex_build_delta_sql') > 0
        ORDER BY p.proname
    LOOP
        _found := _found + 1;
        _src := COALESCE(_fn.src, '?');
        IF _fn.src IS NULL OR _fn.transition IS NULL THEN
            _skipped := _skipped || format('%s (source %s): unrecognised body', _fn.fn, _src);
            CONTINUE;
        END IF;
        _new := replace(_fn.body, _old_order, _new_order);
        IF NOT _fn.is_deferred THEN
            _skipped_hook := format(
                '_skipped_sql := public.reflex_joint_skipped_delta_sql(_rec.name, ''%s'', _rec.base_query, ''%s''); IF _skipped_sql <> '''' THEN EXECUTE _skipped_sql; END IF;',
                _fn.src, _fn.transition);
            _may_skip := format('public.reflex_joint_delta_may_be_skipped(_rec.name, ''%s'', _rec.base_query)', _fn.src);
            _new := replace(_new, _old_declare, _new_declare);
            _new := replace(_new, _old_ignored,
                ' = ANY(_rec.ignored_sources)) THEN' || chr(10)
                || '        ' || _skipped_hook || chr(10)
                || '        CONTINUE;' || chr(10)
                || '      END IF;' || chr(10));
            _new := replace(_new, _old_pred,
                'IF NOT _pred_match AND ' || _may_skip || ' THEN ' || _skipped_hook || ' CONTINUE; END IF;');
            _new := replace(_new, _old_filter,
                'IF _filter_skip AND ' || _may_skip || ' THEN ' || _skipped_hook || ' CONTINUE; END IF;');
            _new := replace(_new, _old_path_c,
                'WHERE name = _rec.name;' || chr(10)
                || '                   ' || _skipped_hook || chr(10)
                || '                   CONTINUE;');
            _new := replace(_new, _old_call, _new_call);
            _new := replace(_new, _old_apply, _new_apply);
        END IF;
        -- The rewritten body, checked as a whole.
        IF (length(_new) - length(replace(_new, _new_order, ''))) / length(_new_order) <> 1
           OR strpos(_new, _old_order) > 0
           OR (NOT _fn.is_deferred AND (
                  strpos(_new, _new_declare) = 0
               OR strpos(_new, 'public.reflex_build_delta_sql(') > 0
               OR (length(_new) - length(replace(_new, _new_call, ''))) / length(_new_call) <> 1
               OR (length(_new) - length(replace(_new, _new_apply, ''))) / length(_new_apply) <> 1
               OR strpos(_new, 'IF _sql <> '''' THEN') > 0
               OR strpos(_new, _old_pred) > 0
               OR strpos(_new, _old_filter) > 0
               OR (length(_new) - length(replace(_new, 'CONTINUE;', ''))) / length('CONTINUE;')
                  <> (length(_new) - length(replace(_new, _skipped_hook, ''))) / length(_skipped_hook)))
        THEN
            _skipped := _skipped || format('%s (source %s): unrecognised body', _fn.fn, _src);
            CONTINUE;
        END IF;
        IF _new = _fn.body THEN
            _current := _current + 1;
            CONTINUE;
        END IF;
        BEGIN
            EXECUTE replace(pg_get_functiondef(_fn.fn), _fn.body, _new);
            _rewritten := _rewritten + 1;
        EXCEPTION WHEN OTHERS THEN
            _skipped := _skipped || format('%s (source %s): %s', _fn.fn, _src, SQLERRM);
        END;
    END LOOP;
    -- INFO, not NOTICE: an extension script runs with client_min_messages raised to WARNING.
    RAISE INFO 'pg_reflex 1.12.5: rewrote % of % delta trigger bodies found (% already current, % skipped)',
        _rewritten, _found, _current, COALESCE(array_length(_skipped, 1), 0);
    IF COALESCE(array_length(_skipped, 1), 0) > 0 THEN
        RAISE WARNING '%', format('pg_reflex 1.12.5: %s delta trigger bodies were NOT rewritten: %s. '
            'Until repaired, a DEFERRED-flavour body among them can double count the joint delta of an IMMEDIATE IMV '
            'that joins its source and another IMMEDIATE IMV over that source, and an IMMEDIATE body among them recomputes '
            'its join IMVs on every statement that writes two of their sources. '
            'Remedy, after the upgrade, in a normal session as a superuser or the owner of the functions: '
            're-run step 4 (the DO block) of sql/pg_reflex--1.12.4--1.12.5.sql (it retries a body refused here); '
            'a body it still reports as unrecognised predates 1.12.0: drop and recreate the IMVs over that source '
            '(dropping the last one removes its trigger functions and the next create renders them fresh), '
            'then reflex_reconcile them.',
            array_length(_skipped, 1), array_to_string(_skipped, '; '));
    END IF;
END
$regen$;
