-- Migration: pg_reflex 1.1.3 → 1.2.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.2.0';
--
-- Changes in 1.2.0:
--
-- Theme 1 — MIN/MAX retraction
--   The recompute path that fires on DELETE/UPDATE-old for MIN/MAX aggregates
--   is now scoped to the affected-groups table (__reflex_affected_<view>)
--   rather than re-aggregating the full source. Code-gen only — the emitted
--   SQL changes shape but existing IMVs need no schema migration.
--
-- Theme 2 — correctness bug fixes
--   Transitive cycle detection in create_reflex_ivm, catalog-lookup WARNING
--   in resolve_column_type, 64-bit advisory-lock keys, STRICT-safe
--   reflex_build_delta_sql signature, and other bug fixes from
--   journal/2026-04-22_bug_report.md.
--
-- Theme 3 — operational safety
--   reflex_rebuild_imv SPI (exposed as an alias over reflex_reconcile) to
--   rebuild an IMV from scratch. sql_drop event trigger auto-drops IMVs
--   whose source table is dropped. ddl_command_end event trigger raises a
--   WARNING on ALTER TABLE of a tracked source so operators can rerun
--   reflex_rebuild_imv. Per-IMV SAVEPOINT around each flush so one bad IMV
--   doesn't abort a cascade.
--
-- Theme 4 — observability
--   __reflex_ivm_reference gains four columns: last_flush_ms, last_flush_rows,
--   flush_count, last_error. These are populated by reflex_flush_deferred.
--   New SPIs: reflex_ivm_status(), reflex_ivm_stats(view_name),
--   reflex_explain_flush(view_name).

-- === Registry columns (Theme 4.1) ===
--
-- 1.1.3 shipped without these columns; 1.2.0 tables declare them in the
-- bootstrap DDL, but existing installations need ALTERs so upgraded
-- installations match a fresh install. IF NOT EXISTS keeps this idempotent.
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS last_flush_ms BIGINT,
    ADD COLUMN IF NOT EXISTS last_flush_rows BIGINT,
    ADD COLUMN IF NOT EXISTS flush_count BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

-- === Function signature updates (Theme 2 / bug #13) ===
--
-- Any signature renegotiation of reflex_build_delta_sql lands here in a
-- future migration. 1.2.0 keeps the 1.1.3 signature unchanged (seven TEXT
-- args) — no ALTER needed.

-- Nothing else to do: Theme 1's change is code-gen only (emitted SQL shape),
-- so installed triggers pick up the new behavior on the next flush; Theme 3
-- event triggers are installed by extension_sql in lib.rs during UPDATE;
-- Theme 4 SPIs are new #[pg_extern]s auto-registered by the upgrade.

-- === Source DROP cascading cleanup (Theme 3, R1) ===
--
-- 1.2.0 ships an event trigger that not only deletes the registry row but also
-- drops every artifact owned by the IMV (target, intermediate, affected,
-- delta-scratch, passthrough-scratch tables and the standalone trigger
-- functions). The body delegates to public.drop_reflex_ivm(name, TRUE) — which
-- itself was made resilient to a missing source table in 1.2.0.
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
            ORDER BY graph_depth DESC
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
    END LOOP;
END;
$$;
