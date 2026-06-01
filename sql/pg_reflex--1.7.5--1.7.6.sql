-- Migration: pg_reflex 1.7.5 → 1.7.6
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.7.6';
--
-- 1.7.6 is a correctness release for the `ignore_sources` contract on the
-- DEFERRED trigger path.
--
-- Background: `ignore_sources` excludes an IMV from being maintained by DML on
-- the named source. Pre-1.7.6 this was honored only by the IMMEDIATE trigger
-- body. The DEFERRED path never consulted `ignored_sources` — neither the three
-- deferred trigger bodies (INSERT/DELETE, UPDATE, TRUNCATE) nor the commit-time
-- `reflex_flush_deferred`. So whenever a source's trigger was the *deferred
-- flavour* (installed because some sibling IMV on that source is DEFERRED), an
-- IMV that had ignored that source was still maintained — both inline (for
-- IMMEDIATE IMVs processed within the deferred trigger) and at flush (for
-- DEFERRED IMVs). This silently broke the documented ignore contract.
--
-- The fix lives in two places, both already compiled into the new `.so`:
--   (1) the deferred trigger-body codegen now emits the same
--       `ignored_sources` ANY()-skip guard as the immediate body, and
--   (2) `reflex_flush_deferred` now excludes IMVs whose `ignored_sources`
--       overlaps the (qualified, bare) source name.
--
-- There is NO catalog schema change. But (1) is baked into the per-source
-- trigger *function bodies* at install time, so existing deployments keep the
-- old, guard-less bodies until their triggers are rebuilt. This migration
-- therefore rebuilds every enabled source's triggers via
-- `reflex_rebuild_triggers` (which picks the correct immediate/deferred
-- flavour), mirroring the 1.6.2 trigger-codegen migration. (2) takes effect
-- immediately with the new `.so` — no rebuild needed for the flush path.

DO $$
DECLARE
    src TEXT;
    res TEXT;
BEGIN
    FOR src IN
        SELECT DISTINCT depended
        FROM (
            SELECT unnest(depends_on) AS depended
            FROM public.__reflex_ivm_reference
            WHERE enabled = TRUE
        ) d
        WHERE depended IS NOT NULL AND depended NOT LIKE '<%'
    LOOP
        BEGIN
            res := public.reflex_rebuild_triggers(src);
            IF res LIKE 'ERROR:%' THEN
                RAISE NOTICE 'pg_reflex 1.7.6 migration: %', res;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- A single source failing to rebuild (e.g. its table was dropped,
            -- leaving an orphaned reference row, or a bare name is ambiguous)
            -- must not abort the upgrade. Log and continue.
            RAISE NOTICE 'pg_reflex 1.7.6 migration: could not rebuild triggers for %: %', src, SQLERRM;
        END;
    END LOOP;
END $$;

DO $migrate$
BEGIN
    RAISE NOTICE 'pg_reflex 1.7.6: ignore_sources is now honored on the DEFERRED trigger path (bodies rebuilt) and in reflex_flush_deferred. No catalog change.';
END
$migrate$;
