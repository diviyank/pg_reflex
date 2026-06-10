-- Migration: pg_reflex 1.9.2 → 1.10.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.10.0';
--
-- 1.10.0 fixes the long-standing hole where `ALTER TABLE source ATTACH
-- PARTITION child_with_data` created the IMV partition structure but never
-- synced its data (the IMV partition stayed empty, row count 0). Three
-- changes land together:
--
--   * Per-root isolation in the partition pending-queue drain (Rust): each
--     source root is now reconciled + snapshot-refreshed + drained inside its
--     own subtransaction, so one broken root can no longer abort the whole
--     flush and wedge the queue for every other source.
--   * Shape-drift heal in sync (Rust): when a source child is rebuilt
--     partitioned under the same name (gaining a sub-level), the mirrored IMV
--     child — previously left a plain table by `CREATE … IF NOT EXISTS` and
--     thus fatal to reconcile ("… is not partitioned") — is now dropped and
--     rebuilt with the correct shape.
--   * Commit-time auto-drain (DDL below): a DEFERRABLE INITIALLY DEFERRED
--     constraint trigger on `__reflex_partition_pending` runs a scoped flush
--     of each enqueued root at COMMIT, so ATTACH-with-data syncs automatically
--     with no manual `reflex_flush_partitions()` call.
--
-- The two Rust fixes need no migration DDL — they are recompiled into the
-- module; replace the `.so` BEFORE running `ALTER EXTENSION … UPDATE` so the
-- corrected paths are loaded. The DDL below installs the one new SPI and the
-- auto-drain trigger on existing databases, then drains any queue rows left
-- pending by pre-1.10.0 ATTACHes that never had a commit-time drain.

-- === New SPI: SQL-callable snapshot refresh (used by the per-root flush DO block) ===
CREATE OR REPLACE FUNCTION public."__reflex_refresh_partition_snapshot"(
    "source_root" TEXT
) RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_refresh_partition_snapshot_wrapper';

-- === Commit-time auto-drain of the partition pending queue ===
-- Mirrors the deferred-DML flush mechanism. Scoped per root (not the all-roots
-- reflex_flush_partitions) so each root drains independently; combined with the
-- per-root subtransaction inside reflex_flush_partition_source, one broken root
-- cannot wedge the queue. Recursion is bounded: the flush's own ATTACH/DETACH on
-- __reflex_-owned tables is ignored by the ddl_command_end enqueue guard
-- (NOT LIKE '%__reflex_%'), so no new pending rows are produced.
CREATE OR REPLACE FUNCTION public.__reflex_partition_flush_fn()
RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM public.reflex_flush_partition_source(NEW.source_root);
    RETURN NULL;
END;
$fn$;

DROP TRIGGER IF EXISTS __reflex_partition_flush_trigger
    ON public.__reflex_partition_pending;

CREATE CONSTRAINT TRIGGER __reflex_partition_flush_trigger
    AFTER INSERT ON public.__reflex_partition_pending
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION public.__reflex_partition_flush_fn();

-- === One-time drain of pre-1.10.0 wedged queue rows ===
-- Rows enqueued by ATTACHes before this upgrade never fired a commit-time
-- trigger and may have been blocked by the (now-fixed) all-or-nothing drain.
-- This scoped, per-root, isolated flush heals them; a root that still fails
-- emits a WARNING and stays pending for inspection rather than aborting.
SELECT public.reflex_flush_partitions();
