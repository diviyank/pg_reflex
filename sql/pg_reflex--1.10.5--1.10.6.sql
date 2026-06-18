-- Migration: pg_reflex 1.10.5 → 1.10.6
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.10.6';
--
-- 1.10.6 fixes orphaned registry rows when an IMV's own *target* table is
-- dropped. The `sql_drop` event trigger only reacted to a *source* table drop:
-- it matched each dropped table against every IMV's `depends_on`. An IMV whose
-- source was a VIEW (views are object_type='view', which the trigger's
-- `object_type='table'` filter skips), or whose target lived in a schema
-- dropped via `DROP SCHEMA … CASCADE` while its sources did not, had its target
-- table vaporized but its `__reflex_ivm_reference` row left behind — pointing at
-- a relation that no longer exists. Observed as 8 surviving `yse.*` rows after
-- `DROP SCHEMA yse CASCADE` (their sources were views).
--
-- The fix adds a second branch to `__reflex_on_sql_drop`: for each dropped
-- table it also tears down any IMV whose registered target table IS that table.
-- The match is on the EXACT target identity (target_schema + bare name), never a
-- prefix, so a partition-swap maintenance cycle dropping child / __reflex_swap_*
-- tables can never be mistaken for the registered target.
--
-- This is a plpgsql function-body change only (no signature change, no schema
-- change), so a single CREATE OR REPLACE FUNCTION suffices — the existing
-- `reflex_on_sql_drop` event trigger keeps pointing at it. Replace the `.so`
-- BEFORE running `ALTER EXTENSION … UPDATE` as usual.

CREATE OR REPLACE FUNCTION public.__reflex_on_sql_drop()
RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    _obj RECORD;
    _imv RECORD;
BEGIN
    FOR _obj IN
        SELECT object_identity
        FROM pg_event_trigger_dropped_objects()
        WHERE object_type = 'table'
    LOOP
        FOR _imv IN
            SELECT name
            FROM public.__reflex_ivm_reference
            WHERE depends_on @> ARRAY[_obj.object_identity]
               OR depends_on @> ARRAY[split_part(_obj.object_identity, '.', 2)]
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
