-- Migration: pg_reflex 1.4.6 → 1.5.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.5.0';
--
-- 1.5.0 closes the bulk-flip gap on the aggregate-IMV shape that
-- previously lost to REFRESH MATERIALIZED VIEW. The headline change:
-- Path C (the EXPLAIN-based pre-scratch dispatch for Item α
-- INSERT_PROMOTED) now executes an inline **smart bulk-INSERT**
-- instead of dispatching to `reflex_reconcile`.
--
-- The smart bulk-INSERT exploits the Item α `INSERT_PROMOTED`
-- precondition (OLD-side filter-rejected ⇒ intermediate has zero rows
-- for the affected group keys) to do a surgical add — only the new
-- keys go through scratch + bulk INSERT (no per-row UNIQUE-index
-- probe) + target projection from scratch (no intermediate re-read):
--
--   1. scratch fill (base_query with source → transition_new)
--   2. DROP intermediate UNIQUE index
--   3. INSERT INTO intermediate SELECT * FROM scratch
--   4. CREATE intermediate UNIQUE index back
--   5. INSERT INTO target FROM (end_query with intermediate → scratch)
--   6. ANALYZE intermediate
--
-- Reconcile would have rebuilt all post-state rows (including the
-- unchanged survivors); smart bulk-INSERT touches only the new keys.
-- On db_clone alp.bench_user_imv 8.9 M-row OUT→IN flip: 175 s
-- reconcile → ~90 s smart path, beating REFRESH MV (~160 s).
--
-- Companion fixes shipped in 1.5.0:
--
-- - **Passthrough trigger codegen** (`trigger.rs`): the passthrough
--   match arm and scratch-fill gate predated Item α and silently
--   emitted nothing for `INSERT_PROMOTED` / `DELETE_PROMOTED`. With
--   the fix, bulk OUT→IN/IN→OUT on passthrough IMVs (e.g. the
--   alp.sop_forecast_view shape) now beats REFRESH MV in every tested
--   case. Also: Path C couldn't size passthrough IMVs (no
--   intermediate to read reltuples from) — fixed by falling back to
--   the target table's reltuples.
--
-- - **Reconcile drop-indexes step** (`reconcile.rs`): `pg_indexes.indexname`
--   is `name`, not `text`. SPI read via `get_by_name::<&str,_>` silently
--   returned None, the `DROP INDEX IF EXISTS` loop ran zero iterations,
--   and `CREATE INDEX IF NOT EXISTS` then no-op'd. ~30 s of stale-index
--   maintenance per 100 M-row IMV was being paid silently.
--
-- - **Reconcile SPI aggregations cast** (`reconcile.rs`):
--   `__reflex_ivm_reference.aggregations` is `jsonb`. SPI's
--   `get_by_name::<&str,_>` silently returned None for that column,
--   the plan deserialised from `{}`, and reconcile fell into a
--   no-group-by code path that failed silently on aggregate IMVs.
--   Fix: `aggregations::text AS aggregations` in the catalog query.
--
-- All trigger function bodies must be re-emitted to pick up the smart
-- bulk-INSERT codegen and the passthrough fixes.

-- Re-emit triggers for every source. The schema-resolving
-- `reflex_rebuild_triggers` from 1.4.6 means unqualified `depends_on`
-- entries are safe.
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
                RAISE NOTICE 'pg_reflex 1.5.0 migration: %', res;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Don't abort the migration if one source's trigger rebuild
            -- fails (e.g. the source table was dropped). Log and continue.
            RAISE NOTICE 'pg_reflex 1.5.0 migration: could not rebuild triggers for %: %', src, SQLERRM;
        END;
    END LOOP;
END $$;
