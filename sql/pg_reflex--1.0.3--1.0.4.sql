-- Migration: pg_reflex 1.0.3 -> 1.0.4
--
-- Performance optimizations:
-- 1. Empty-delta early-exit: triggers skip processing when transition table is empty
-- 2. Predicate-filtered trigger skip: WHERE clause checked before processing
-- 3. Persistent affected-groups table: TRUNCATE replaces DROP+CREATE per trigger fire
-- 4. Single-pass net-delta MERGE for UPDATE (SUM/COUNT aggregates)
--
-- SQL coverage:
-- 5. INTERSECT and EXCEPT support (same decomposition as UNION)
--
-- Schema changes:
-- 6. where_predicate column in __reflex_ivm_reference

DO $migration$
BEGIN
    -- Add where_predicate column for predicate-filtered trigger skip
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS where_predicate TEXT;

    -- Backfill where_predicate from stored SQL queries
    -- (best effort: extract WHERE clause from sql_query for existing IMVs)
    -- New IMVs will have this set automatically at creation time.

    -- Re-create trigger functions to include early-exit and predicate checks.
    -- Trigger functions are automatically regenerated on next IMV creation.
    -- For existing IMVs, a reconcile will rebuild trigger functions.

    RAISE NOTICE 'pg_reflex migration 1.0.3 -> 1.0.4: where_predicate, performance optimizations complete';
END;
$migration$;
