-- Migration: pg_reflex 1.7.6 → 1.7.7
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.7.7';
--
-- 1.7.7 is a concurrency-isolation fix for DEFERRED-mode maintenance across
-- independent schemas.
--
-- Background: the COMMIT-time dispatcher `__reflex_deferred_flush_fn` is a
-- FOR EACH ROW constraint trigger on the shared `public.__reflex_deferred_pending`
-- queue. Its body looped `SELECT DISTINCT source_table FROM
-- public.__reflex_deferred_pending` and flushed *every* pending source, not just
-- the source the committing transaction had staged. Because the queue is global
-- to the extension, a commit touching a source in schema A could pick up a
-- pending row for a source in schema B and call `reflex_flush_deferred` on it —
-- acquiring schema B's per-source advisory lock (`reflex_flush:<src>`) and the
-- AccessExclusiveLock that the flush's staging-table TRUNCATE takes. Two
-- otherwise-independent schemas therefore serialized against each other at
-- COMMIT.
--
-- Fix: the trigger is FOR EACH ROW, so `NEW.source_table` is exactly the pending
-- row this transaction inserted. The dispatcher now flushes only that source.
-- Maintenance becomes transaction-local: a commit in one schema only touches the
-- sources it itself staged. The per-source advisory lock inside
-- `reflex_flush_deferred` still collapses the redundant fires for a source
-- touched by several statements into a single effective flush, and the
-- cross-source double-count guard is unaffected — at COMMIT every pending row a
-- transaction staged is present before any flush deletes its own, so the first
-- flush of a multi-source IMV still observes all of that IMV's mutated sources.
--
-- This replaces only the dispatcher function; the constraint trigger definition
-- is unchanged (it already binds the function by name), so the new body takes
-- effect immediately with no trigger rebuild.

CREATE OR REPLACE FUNCTION __reflex_deferred_flush_fn() RETURNS TRIGGER AS $fn$
BEGIN
    PERFORM public.reflex_flush_deferred(NEW.source_table);
    RETURN NULL;
END;
$fn$ LANGUAGE plpgsql;

DO $migrate$
BEGIN
    RAISE NOTICE 'pg_reflex 1.7.7: DEFERRED flush is now transaction-local (flushes only NEW.source_table); independent schemas no longer serialize at COMMIT. No catalog change.';
END
$migrate$;
