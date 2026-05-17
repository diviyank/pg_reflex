-- Migration: pg_reflex 1.5.0 → 1.5.2
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.5.2';
--
-- 1.5.2 fixes a bug where quoted mixed-case column names in IMV source queries
-- (e.g. SELECT "Grp", SUM(v) FROM t GROUP BY "Grp") were silently lowercased.
-- The target table was created with column `grp` instead of `"Grp"`, causing
-- queries against the IMV with the original quoted name to fail.
--
-- Root cause: `normalized_column_name` unconditionally lowercased all column
-- names, including those the user explicitly quoted to preserve case. The fix
-- matches PostgreSQL's own folding rule:
--   - Quoted identifiers: strip outer quotes, preserve case verbatim.
--   - Unquoted identifiers: fold to lowercase (unchanged behavior).
--
-- Migration scope:
--
--   Re-emit trigger function bodies for every source so the new codegen takes
--   effect for source-side column reads. IMVs that were created with
--   mixed-case quoted columns under 1.5.0/1.5.1 still work as-is (their
--   target was built lowercase, and the trigger SQL references the same
--   lowercase names — internally consistent). To get the case-preserved target
--   shape, DROP and CREATE the IMV fresh after upgrading.
--
-- Operator action required for affected IMVs:
--   DROP the IMV and recreate it with CREATE REFLEX INCREMENTAL VIEW to get
--   columns exposed under the user's quoted names.

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
                RAISE NOTICE 'pg_reflex 1.5.2 migration: %', res;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Don't abort the migration if one source's trigger rebuild
            -- fails (e.g. the source table was dropped). Log and continue.
            RAISE NOTICE 'pg_reflex 1.5.2 migration: could not rebuild triggers for %: %', src, SQLERRM;
        END;
    END LOOP;
END $$;
