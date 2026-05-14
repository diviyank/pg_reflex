-- Migration: pg_reflex 1.4.5 → 1.4.6
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.4.6';
--
-- 1.4.6 ships Item α (directional UPDATE dispatch) + the ANALYZE plan-guard
-- fix it surfaced + WIPE_THRESHOLD_DEFAULT raised to 1.0.
--
-- Changes:
--
-- 1. **Directional UPDATE dispatch**: the UPDATE trigger function body now
--    contains a probe that reads the OLD and NEW transition tables (gated
--    on `imv_relevant_columns[source]` non-empty) and routes to
--    `reflex_build_delta_sql` with a *promoted* op:
--      * OLD empty post-filter, NEW has rows → 'INSERT'
--      * OLD has rows, NEW empty post-filter → 'DELETE'
--      * both have rows → 'UPDATE' (today's UNION ALL path)
--    For OUT→IN flips on filter columns (e.g., the SOP-forecast
--    customer's hot path), the promotion drops the UNION ALL/outer-
--    GROUP-BY scratch wrapper and the wasted dead-cleanup DELETE.
--
-- 2. **ANALYZE plan-guard**: after the MERGE and the affected INSERT,
--    `pg_class.reltuples` on the intermediate and affected tables is stale.
--    The downstream dead-cleanup DELETE and target sync EXISTS lookups can
--    pick pathological NestedLoop+SeqScan plans (measured 12+ minutes on
--    100K affected groups). Trigger codegen now emits ANALYZE on both
--    tables at the right points; cost is ~200 ms total, restores Hash
--    semi-join / Index Scan plans.
--
-- 3. **WIPE_THRESHOLD_DEFAULT 0.3 → 1.0**: post-Item α, incremental wins
--    over reconcile at every reachable selectivity on the SOP-forecast
--    shape (11 %→78 % swept, incremental 0.6 s→2.9 s vs reconcile ~17 s).
--    Auto-dispatch to reconcile is effectively disabled. Operators with
--    workloads where reconcile genuinely wins (e.g. the rb.fcast shape
--    from the 1.4.4 journal) can re-enable via
--    `SET reflex.wipe_threshold = 0.3` at session scope.
--
-- All three changes are code-only (live entirely in the trigger function
-- bodies + the C dylib's `reflex_build_delta_sql`); no schema changes.
--
-- Existing IMVs need their trigger functions re-emitted to pick up the
-- directional probe (item 1) and the new ANALYZE statements (item 2). The
-- migration calls `reflex_rebuild_triggers` for each unique source table.

DO $$
DECLARE
    src TEXT;
BEGIN
    FOR src IN
        SELECT DISTINCT unnest(depends_on)
        FROM public.__reflex_ivm_reference
        WHERE enabled = TRUE
    LOOP
        BEGIN
            PERFORM public.reflex_rebuild_triggers(src);
        EXCEPTION WHEN OTHERS THEN
            -- Don't abort the migration if one source's trigger rebuild
            -- fails (e.g. the source table was dropped). Log and continue.
            RAISE NOTICE 'pg_reflex 1.4.6 migration: could not rebuild triggers for %: %', src, SQLERRM;
        END;
    END LOOP;
END $$;
