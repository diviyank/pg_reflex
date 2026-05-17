-- Migration: pg_reflex 1.5.0 → 1.5.1
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.5.1';
--
-- 1.5.1 is a correctness hotfix for two distinct crashes that made
-- pg_reflex unusable on real customer schemas (forecast-factory hit
-- both within one transaction). Both are root-caused and fixed; the
-- migration's only job is to re-emit trigger function bodies so the
-- IMMEDIATE-mode UPDATE trigger picks up the new filter_skip_block
-- PL/pgSQL (which now JOIN-defends against pre-1.5.1 metadata).
--
-- Bug A — `could not identify an equality operator for type json`
-- at COMMIT (DEFERRED mode) or at UPDATE fire-time (IMMEDIATE mode).
--
--   The spurious-UPDATE short-circuit and the per-IMV filter-aware
--   skip both project source columns into `EXCEPT ALL`. PG's `json`
--   type (unlike `jsonb`) has no `=` operator, so any source with a
--   `json` column blew up the moment an UPDATE on it reached the
--   skip check. Repro is trivial: an IMV over `(id INT, meta json)`
--   plus a single UPDATE.
--
--   Fixes:
--     * `reflex_flush_deferred` now fetches each source column's
--       typname and casts `json` / `xml` columns to `text` in the
--       EXCEPT ALL projection only (TEMP VIEW for downstream IMV
--       codegen still projects the raw column).
--     * The IMMEDIATE-mode UPDATE trigger's filter_skip_block now
--       builds `_skip_cols` via a JOIN to `pg_attribute` / `pg_type`
--       so json/xml columns get the same cast at runtime.
--
-- Bug B — `column "X" does not exist` on the wrong source table at
-- IMMEDIATE-mode UPDATE fire-time. Repro: a passthrough IMV with a
-- multi-source JOIN and a bare column ref in the SELECT — exactly
-- the alp.sop_forecast_view shape (`SELECT dem_plan_id, ... FROM
-- sales_simulation JOIN demand_planning ON demand_planning.id =
-- sales_simulation.dem_plan_id`). An UPDATE on `demand_planning`
-- fired the trigger and crashed with `column "dem_plan_id" does
-- not exist`.
--
--   Root cause was in `create_ivm.rs`: the analyzer intentionally
--   over-attributes bare column refs to every real source (a safe
--   over-set with a comment promising the catalog filter would
--   drop bogus entries). The filter only ran inside the AGGREGATE
--   branch of the create-IMV flow; passthrough IMVs persisted the
--   dirty JSON, and the IMMEDIATE-mode UPDATE trigger then
--   referenced columns that didn't exist on the source.
--
--   Fix: hoist the per-source catalog filter so it runs for BOTH
--   passthrough and aggregate IMVs. The IMMEDIATE-mode trigger
--   `_skip_cols` builder now also JOINs `pg_attribute` as a runtime
--   defense, so IMVs created with dirty pre-1.5.1 metadata don't
--   crash — they just skip the optimisation (safe). DEFERRED-mode
--   per-IMV skip drops absent columns the same way.
--
-- Migration scope:
--
--   1. Re-emit triggers on every distinct source so the IMMEDIATE
--      UPDATE trigger function body picks up the new pg_attribute
--      JOIN in filter_skip_block.
--   2. No DDL changes to user-visible state. No persisted JSON
--      rewrites. (Existing IMVs continue to work; recreate them at
--      your convenience to drop the dirty `imv_relevant_columns`
--      entries and let the skip optimisation fire.)

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
                RAISE NOTICE 'pg_reflex 1.5.1 migration: %', res;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Don't abort the migration if one source's trigger rebuild
            -- fails (e.g. the source table was dropped). Log and continue.
            RAISE NOTICE 'pg_reflex 1.5.1 migration: could not rebuild triggers for %: %', src, SQLERRM;
        END;
    END LOOP;
END $$;
