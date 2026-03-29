-- Migration: pg_reflex 1.0.4 -> 1.1.0
--
-- Fixes:
-- 1. DROP CASCADE: drop_reflex_ivm(name, true) now issues DROP TABLE ... CASCADE
--    on target, intermediate, and affected-groups tables, removing any dependent
--    PostgreSQL views or objects.
--
-- Internal:
-- 2. Codebase restructured into focused modules (no user-facing change)
-- 3. Tests reorganized into categorized files (no user-facing change)
--
-- No schema changes required.

DO $migration$
BEGIN
    RAISE NOTICE 'pg_reflex migration 1.0.4 -> 1.1.0: DROP CASCADE fix, codebase restructuring complete';
END;
$migration$;
