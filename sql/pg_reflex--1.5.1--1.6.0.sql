-- Migration: pg_reflex 1.5.1 → 1.6.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.6.0';
--
-- 1.6.0 bundles three previously-unreleased deltas:
--   (a) mixed-case quoted column-name correctness fix (was tagged 1.5.2 internally),
--   (b) declarative partitioning Phase 1 (plans/partitioning_2.md): opt-in
--       `partition_by`, `reflex_sync_partitions`, `reflex_reconcile_partition`,
--       auto-mirror,
--   (c) declarative partitioning Phase 2 (plans/partitioning_3.md): atomic
--       DETACH/ATTACH swap, per-partition trigger dispatch, bare-column-ref
--       validation, Tier 2 (JOIN-derived) partition metadata, and the
--       event-trigger-driven auto-sync of IMV partitions when the source's
--       partition tree changes.
--
-- Migration steps:
--   1. Add new catalog columns (idempotent).
--   2. Install the SQL helper `__reflex_partition_child_for_key`.
--   3. Replace the `__reflex_on_ddl_command_end` event-trigger function with
--      the 1.6.0 body (auto-sync on ATTACH/DETACH PARTITION + CREATE TABLE
--      ... PARTITION OF for sources of partitioned IMVs).
--   4. Re-create the `reflex_on_ddl_command_end` event trigger with the
--      widened `WHEN TAG IN ('ALTER TABLE', 'CREATE TABLE')` clause.
--   5. Re-emit trigger function bodies (mixed-case fix carry-over from 1.5.2).
--
-- Partitioning is opt-in. Non-partitioned IMVs need no operator action.
-- Operators with mixed-case quoted source columns should DROP + recreate
-- the affected IMVs to get case-preserved target column names.

-- ---------------------------------------------------------------------------
-- 1. New catalog columns (Phase 1 + Phase 2)
-- ---------------------------------------------------------------------------

ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS partition_columns TEXT[];
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS partition_strategy TEXT;
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS wipe_floor_rows BIGINT;
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS partition_dispatch_cost_cap BIGINT;

-- ---------------------------------------------------------------------------
-- 2. SQL helper: __reflex_partition_child_for_key
-- ---------------------------------------------------------------------------

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
    IF parent IS NULL OR part_col IS NULL OR k IS NULL THEN
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
        _expr := regexp_replace(_r.def, _ident_re, quote_literal(k), 'gi');
        BEGIN
            EXECUTE 'SELECT (' || _expr || ')' INTO _match;
            IF _match THEN
                RETURN _r.rc;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            CONTINUE;
        END;
    END LOOP;
    RETURN NULL;
END;
$REFLEX$;

-- ---------------------------------------------------------------------------
-- 3. + 4. Replace __reflex_on_ddl_command_end and its event trigger
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.__reflex_on_ddl_command_end()
RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    _cmd RECORD;
    _imv RECORD;
    _src TEXT;
    _parent TEXT;
    _policy TEXT;
    _affected TEXT[] := ARRAY[]::TEXT[];
    _synced_keys TEXT[] := ARRAY[]::TEXT[];
    _sync_key TEXT;
BEGIN
    _policy := lower(COALESCE(NULLIF(current_setting('pg_reflex.alter_source_policy', true), ''), 'warn'));
    IF _policy NOT IN ('warn', 'error') THEN
        RAISE WARNING 'pg_reflex: invalid pg_reflex.alter_source_policy=%, falling back to ''warn''', _policy;
        _policy := 'warn';
    END IF;

    FOR _cmd IN
        SELECT object_identity, object_type, command_tag
        FROM pg_event_trigger_ddl_commands()
        WHERE command_tag IN ('ALTER TABLE', 'CREATE TABLE')
    LOOP
        _parent := NULL;
        IF _cmd.command_tag = 'ALTER TABLE' THEN
            _parent := _cmd.object_identity;
        ELSIF _cmd.command_tag = 'CREATE TABLE' THEN
            BEGIN
                SELECT n.nspname || '.' || c.relname INTO _parent
                FROM pg_inherits i
                JOIN pg_class c   ON c.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.inhrelid = _cmd.object_identity::regclass;
            EXCEPTION WHEN OTHERS THEN
                _parent := NULL;
            END;
        END IF;

        IF _parent IS NOT NULL THEN
            FOR _imv IN
                SELECT name FROM public.__reflex_ivm_reference
                WHERE partition_columns IS NOT NULL
                  AND array_length(partition_columns, 1) > 0
                  AND (depends_on @> ARRAY[_parent]
                       OR depends_on @> ARRAY[split_part(_parent, '.', 2)])
            LOOP
                _sync_key := _parent || '|' || _imv.name;
                IF _sync_key = ANY(_synced_keys) THEN
                    CONTINUE;
                END IF;
                _synced_keys := _synced_keys || _sync_key;
                BEGIN
                    PERFORM public.reflex_sync_partitions(_imv.name, FALSE);
                    RAISE NOTICE 'pg_reflex: auto-synced partitions for IMV % (source %)',
                        _imv.name, _parent;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'pg_reflex: auto-sync of IMV % failed after source % partition change: % — run SELECT reflex_sync_partitions(''%'') manually',
                        _imv.name, _parent, SQLERRM, _imv.name;
                END;
            END LOOP;
        END IF;
    END LOOP;

    FOR _cmd IN
        SELECT object_identity, command_tag
        FROM pg_event_trigger_ddl_commands()
        WHERE command_tag = 'ALTER TABLE'
    LOOP
        _src := _cmd.object_identity;
        FOR _imv IN
            SELECT name FROM public.__reflex_ivm_reference
            WHERE depends_on @> ARRAY[_src]
               OR depends_on @> ARRAY[split_part(_src, '.', 2)]
        LOOP
            _affected := _affected || (_src || ' -> ' || _imv.name);
            IF _policy = 'warn' THEN
                RAISE WARNING 'pg_reflex: source table % was altered; IMV % may be stale — run SELECT reflex_rebuild_imv(''%'') to recover',
                    _src, _imv.name, _imv.name;
            END IF;
        END LOOP;
    END LOOP;

    IF _policy = 'error' AND array_length(_affected, 1) > 0 THEN
        RAISE EXCEPTION 'pg_reflex: ALTER blocked by pg_reflex.alter_source_policy=''error'' on tracked source(s); affected: %',
            array_to_string(_affected, ', ')
            USING HINT = 'Set pg_reflex.alter_source_policy = ''warn'' (default) or drop_reflex_ivm() first.';
    END IF;
END;
$$;

DROP EVENT TRIGGER IF EXISTS reflex_on_ddl_command_end;
CREATE EVENT TRIGGER reflex_on_ddl_command_end
    ON ddl_command_end
    WHEN TAG IN ('ALTER TABLE', 'CREATE TABLE')
    EXECUTE FUNCTION public.__reflex_on_ddl_command_end();

-- ---------------------------------------------------------------------------
-- 5. Re-emit trigger function bodies (mixed-case fix carry-over)
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    src TEXT;
    res TEXT;
BEGIN
    FOR src IN
        SELECT DISTINCT unnest(depends_on)
        FROM public.__reflex_ivm_reference
        WHERE enabled = TRUE
    LOOP
        BEGIN
            res := public.reflex_rebuild_triggers(src);
            IF res LIKE 'ERROR:%' THEN
                RAISE NOTICE 'pg_reflex 1.6.0 migration: %', res;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pg_reflex 1.6.0 migration: could not rebuild triggers for %: %', src, SQLERRM;
        END;
    END LOOP;
END $$;
