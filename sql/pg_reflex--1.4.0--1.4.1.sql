-- Migration: pg_reflex 1.4.0 → 1.4.1
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.4.1';
--
-- Bug fix release. No additive features.
--
-- Fixed: internal reflex tables (delta scratch, staging delta, passthrough
-- scratch, affected-groups, shrunk-groups) were created with unqualified
-- names and so lived in whichever schema topped the creating session's
-- `search_path`. Trigger bodies and generated MERGE SQL then referenced
-- them by bare name and were resolved against the *firing* session's
-- search_path — application sessions that ran `SET search_path = '<schema>'`
-- (excluding public) hit "relation does not exist" on every DML, e.g.:
--
--     ERROR:  relation "__reflex_delta_alp_demand_planning" does not exist
--     [SQL: UPDATE alp.demand_planning SET ... ]
--
-- 1.4.1 co-locates each internal table in the schema of its owning IMV
-- (per-IMV artefacts) or source table (staging delta), and emits fully
-- qualified references in every generated trigger body and SPI query. The
-- in-process reflex_* SPIs are now qualified as `public.reflex_*` in trigger
-- bodies for the same reason (functions inherit caller search_path; the
-- extension lives in public by convention).
--
-- Existing IMVs created under 1.4.0 (or older) still have the OLD bare-name
-- trigger function bodies and OLD bare-name internal tables in postgres'
-- catalog — the extension upgrade does NOT rewrite them. To pick up the fix
-- you must drop and recreate each affected IMV:
--
--     SELECT drop_reflex_ivm('<schema>.<view>');
--     SELECT create_reflex_ivm('<schema>.<view>', '<SELECT …>', …);
--
-- The block below emits a per-IMV NOTICE so you can see what to rebuild.

DO $REFLEX_MIG_141$
DECLARE
    rec RECORD;
    needs_rebuild INT := 0;
BEGIN
    FOR rec IN
        SELECT name FROM public.__reflex_ivm_reference ORDER BY graph_depth, name
    LOOP
        needs_rebuild := needs_rebuild + 1;
        RAISE NOTICE
            'pg_reflex 1.4.1: IMV % was created before the search_path fix — drop and recreate it to pick up the new qualified trigger bodies and table layout.',
            rec.name;
    END LOOP;

    IF needs_rebuild > 0 THEN
        RAISE NOTICE
            'pg_reflex 1.4.1: % existing IMV(s) listed above. Until rebuilt, DML on their source tables under a non-public `search_path` will continue to fail.',
            needs_rebuild;
    END IF;
END;
$REFLEX_MIG_141$;
