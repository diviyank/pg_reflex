-- Migration: pg_reflex 1.1.0 -> 1.1.1
--
-- New features:
-- 1. FILTER clause support: SUM(x) FILTER (WHERE cond) and variants
-- 2. DISTINCT ON support: decomposed into passthrough sub-IMV + ROW_NUMBER VIEW
--
-- Fixes:
-- 3. DROP CASCADE: drop_reflex_ivm(name, true) now issues DROP TABLE/VIEW ... CASCADE
-- 4. DROP VIEW detection: correctly handles VIEW targets from window/DISTINCT ON decompositions
--
-- No schema changes required.

DO $migration$
BEGIN
    RAISE NOTICE 'pg_reflex migration 1.1.0 -> 1.1.1: FILTER clause, DISTINCT ON support, DROP fixes';
END;
$migration$;
