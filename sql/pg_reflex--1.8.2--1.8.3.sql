-- Migration: pg_reflex 1.8.2 → 1.8.3
--
-- Unpartitioned IMV on a partitioned source (plans/2026-06-03-imv-partition-depth.md
-- follow-on). An empty partition_by array (ARRAY[]::text[]) now forces an
-- UNPARTITIONED target IMV instead of auto-mirroring. Source partition swaps
-- (DETACH/ATTACH) are captured for such IMVs by:
--   * relaxing the ddl_command_end event trigger to enqueue the source root for
--     ANY enabled dependent IMV (not just partitioned ones), and
--   * having reflex_flush_partitions full-reconcile unpartitioned dependents of
--     a dirty root (logic ships in the .so).
--
-- This script redefines only the event-trigger function (its WHERE/EXISTS body
-- changed). The flush behavior is in the .so — replace it before running
-- `ALTER EXTENSION pg_reflex UPDATE TO '1.8.3';`. Idempotent (CREATE OR REPLACE).

CREATE OR REPLACE FUNCTION public.__reflex_on_ddl_command_end()
    RETURNS event_trigger LANGUAGE plpgsql AS $$
    DECLARE
        _cmd RECORD;
        _imv RECORD;
        _src TEXT;
        _parent TEXT;
        _part_root TEXT;
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

        -- 1.6.0: auto-sync IMV partitions when a source's partition tree changes.
        --
        -- Two trigger surfaces matter:
        --   (a) ALTER TABLE parent ATTACH/DETACH PARTITION child
        --       → pg_event_trigger_ddl_commands() returns object_identity = parent,
        --         command_tag = 'ALTER TABLE'.
        --   (b) CREATE TABLE child PARTITION OF parent FOR VALUES ...
        --       → command_tag = 'CREATE TABLE'; object_identity = child;
        --         the parent must be looked up via pg_inherits.
        --
        -- For every command we resolve a candidate parent table name, then for
        -- each partitioned IMV depending on that parent we call
        -- reflex_sync_partitions(view, drop_orphans=>FALSE) — orphan deletion is
        -- never automatic (IMV data is the user's, and a DETACH on the source
        -- side is not a delete signal). reflex_sync_partitions is idempotent
        -- and advisory-lock protected, so duplicate fires inside one
        -- transaction collapse harmlessly.
        --
        -- The previous (1.5.x) warn/error contract for non-partition ALTERs
        -- (column add/drop on a tracked source) is preserved below.

        FOR _cmd IN
            SELECT object_identity, object_type, command_tag
            FROM pg_event_trigger_ddl_commands()
            WHERE command_tag IN ('ALTER TABLE', 'CREATE TABLE')
        LOOP
            -- Resolve the parent table for partition-tree changes. NULL for
            -- non-partition events (regular ALTER TABLE on a leaf table).
            _parent := NULL;
            IF _cmd.command_tag = 'ALTER TABLE' THEN
                -- ATTACH / DETACH PARTITION: object_identity is the parent.
                -- Other ALTER variants (ADD COLUMN, …) also land here with
                -- object_identity = the altered table — we sync anyway iff
                -- that table is a partitioned source of a partitioned IMV.
                _parent := _cmd.object_identity;
            ELSIF _cmd.command_tag = 'CREATE TABLE' THEN
                -- CREATE TABLE … PARTITION OF parent: look up parent via
                -- pg_inherits regardless of `object_type` (PG reports
                -- 'table' or 'table partition' depending on version).
                -- Empty result = the new table isn't a partition; _parent
                -- stays NULL and the branch below skips.
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
                -- Capture for flush: resolve the partition ROOT (a multi-level
                -- attach reports an intermediate level as _parent, but IMVs depend
                -- on the top-level source) and enqueue it, unless it is
                -- pg_reflex-owned (our own atomic swap ATTACH/DETACHes IMV
                -- partitions; reacting to those would race the code-driven cascade).
                BEGIN
                    SELECT n.nspname || '.' || c.relname
                      INTO _part_root
                      FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE c.oid = pg_partition_root(_parent::regclass);
                EXCEPTION WHEN OTHERS THEN
                    _part_root := NULL;
                END;

                IF _part_root IS NOT NULL
                   AND _part_root NOT LIKE '%\_\_reflex\_%'
                   AND NOT EXISTS (
                       SELECT 1 FROM public.__reflex_ivm_reference r
                       WHERE r.name = _part_root OR r.name = split_part(_part_root, '.', 2)
                   )
                   AND EXISTS (
                       -- Enqueue for ANY enabled IMV depending on this root:
                       -- partitioned IMVs reconcile per-partition; unpartitioned
                       -- IMVs get a full reconcile (flush handles both). Without
                       -- this, an unpartitioned IMV on a swap source goes stale.
                       SELECT 1 FROM public.__reflex_ivm_reference r
                       WHERE r.enabled
                         AND (r.depends_on @> ARRAY[_part_root]
                              OR r.depends_on @> ARRAY[split_part(_part_root, '.', 2)])
                   )
                THEN
                    INSERT INTO public.__reflex_partition_pending (source_root)
                    VALUES (_part_root)
                    ON CONFLICT (source_root) DO NOTHING;
                END IF;

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

        -- Warn/error policy for non-partition ALTERs on tracked sources.
        -- This branch is unchanged from 1.5.x except that auto-sync above may
        -- have already healed pure partition-tree changes; the warning still
        -- fires (column shape may have changed) so the operator knows to
        -- inspect.
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
