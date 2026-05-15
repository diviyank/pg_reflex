-- Migration: pg_reflex 1.4.5 → 1.4.6
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.4.6';
--
-- 1.4.6 ships Item α (directional UPDATE dispatch) + the ANALYZE plan-guard
-- fix it surfaced + per-IMV `wipe_threshold` override + lowered default
-- threshold (0.50) + a schema-resolving `reflex_rebuild_triggers` + a
-- reconcile speedup (target ANALYZE removed). An attempt to also wire
-- dispatch into the INSERT/DELETE paths was reverted after db_clone bench
-- showed it regresses the bulk-flip case by ~70 % — see
-- `journal/2026-05-15_dispatch_wiring_revert.md`.
--
-- Changes:
--
-- 1. **Directional UPDATE dispatch (Item α)**: the UPDATE trigger function
--    body now probes the OLD and NEW transition tables (gated on
--    `imv_relevant_columns[source]` non-empty) and routes to
--    `reflex_build_delta_sql` with a *promoted* op:
--      * OLD empty post-filter, NEW has rows → 'INSERT'
--      * OLD has rows, NEW empty post-filter → 'DELETE'
--      * both have rows → 'UPDATE' (today's UNION ALL path)
--    For OUT→IN flips on filter columns the promotion drops the UNION ALL /
--    outer-GROUP-BY scratch wrapper and the wasted dead-cleanup DELETE.
--
-- 2. **ANALYZE plan-guard**: trigger codegen now emits ANALYZE on the
--    intermediate (after MERGE) and on the affected groups table (after the
--    scratch-fed INSERT). Restores Hash semi-join / Index Scan plans for
--    the downstream dead-cleanup + target sync (without these, the planner
--    picked NestedLoop + SeqScan and timed out at 100K+ affected groups).
--
-- 3. **`WIPE_THRESHOLD_DEFAULT` 0.3 → 0.50**: a compromise between the
--    synthetic-shape extreme (small flips, incremental wins up to 78 %) and
--    the production-shape extreme (multi-M-row flips, reconcile wins from
--    ~50 %). Affects the UPDATE-path dispatch only. Operators with
--    shape-divergent IMVs override per-IMV via the new column (item 4) or
--    per-session via `SET reflex.wipe_threshold = X`.
--
-- 4. **Per-IMV `wipe_threshold` column** in `__reflex_ivm_reference`. The
--    UPDATE-path dispatch DO block consults this first, then the GUC, then
--    the compiled default. Operators set it via
--    `SELECT reflex_set_wipe_threshold('schema.imv', value::NUMERIC)`
--    (pass `NULL::NUMERIC` to clear).
--
-- 5. **Schema-resolving `reflex_rebuild_triggers`**. When called with an
--    unqualified source name (the format pre-1.4.6 IMVs stored in
--    `depends_on`), the function now consults `pg_class` to pin the actual
--    schema instead of letting the DDL fall through to the caller's
--    `search_path`. Multiple matches → explicit error rather than silent
--    wrong-table attachment. This fixes the customer-reported migration
--    breakage on `alp.*` / `yse.*` source tables.
--
-- 6. **Reconcile speedup**: post-reconcile `ANALYZE` on the target table
--    removed (intermediate ANALYZE drives all pg_reflex-internal planning;
--    user analytics get autovacuum stats refresh on the usual cadence).
--
-- All trigger function bodies are re-emitted to pick up items 1, 2, and 4.

-- Item 4 — add the per-IMV override column.
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS wipe_threshold NUMERIC;

-- 1.4.6 — backfill source_join_keys metadata on every IMV. This unlocks
-- bulk-INSERT (OUT→IN), bulk-DELETE (IN→OUT), and Path B pre-scratch
-- dispatch for IMVs created on 1.4.5 or earlier.
DO $$
DECLARE
    imv TEXT;
    res TEXT;
BEGIN
    FOR imv IN SELECT name FROM public.__reflex_ivm_reference WHERE enabled = TRUE LOOP
        BEGIN
            res := public.reflex_rebuild_imv_metadata(imv);
            IF res LIKE 'ERROR:%' THEN
                RAISE NOTICE 'pg_reflex 1.4.6 migration: %', res;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pg_reflex 1.4.6 migration: could not rebuild metadata for %: %', imv, SQLERRM;
        END;
    END LOOP;
END $$;

-- Items 1, 2, 4: re-emit triggers for every source. The schema-resolving
-- rebuild from item 5 means unqualified `depends_on` entries are now safe.
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
                RAISE NOTICE 'pg_reflex 1.4.6 migration: %', res;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Don't abort the migration if one source's trigger rebuild
            -- fails (e.g. the source table was dropped). Log and continue.
            RAISE NOTICE 'pg_reflex 1.4.6 migration: could not rebuild triggers for %: %', src, SQLERRM;
        END;
    END LOOP;
END $$;
