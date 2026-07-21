-- Migration: pg_reflex 1.10.9 → 1.10.10
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.10.10';
--
-- Replace the module (.so) BEFORE running this.
--
-- Partition-flush deadlock release. Four independent fixes:
--
--   * The flush trigger is scoped to `UPDATE OF enqueued_at`. Previously the
--     flush's own EXCEPTION handler wrote last_error to the same table under a
--     bare `AFTER INSERT OR UPDATE` trigger, re-arming itself into an unbounded
--     retry loop that hung committing backends indefinitely.
--   * A per-root `failures` counter caps retries at 5 (on the queue-drain and
--     the single-root flush paths); capped roots keep their pending row and
--     last_error for reflex_doctor, and dependents stay known_stale.
--   * Syncing an IMV now drains every DEFAULT partition in the tree into a
--     holding table, builds the missing leaves, and re-inserts by routing
--     (moving, never deleting), fixing the circular state where rows sat in the
--     default because the leaf did not exist and the leaf could not be created
--     because the rows sat in the default (SQLSTATE 23514) — at any partition
--     depth, multi-level trees included.
--   * reflex_rebuild_chain refuses when dependent IMVs would be CASCADE-dropped;
--     pass cascade => TRUE to drop and recreate every transitive dependent in
--     dependency order.
--   * The archive_residue audit check reports "cannot confirm" instead of a false
--     residue warning for any multi-source IMV, whose per-partition counts are
--     not comparable to a source partition's.

ALTER TABLE public.__reflex_partition_pending
    ADD COLUMN IF NOT EXISTS failures INT NOT NULL DEFAULT 0;

DROP TRIGGER IF EXISTS __reflex_partition_flush_trigger
    ON public.__reflex_partition_pending;

CREATE CONSTRAINT TRIGGER __reflex_partition_flush_trigger
    AFTER INSERT OR UPDATE OF enqueued_at ON public.__reflex_partition_pending
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION public.__reflex_partition_flush_fn();

-- reflex_rebuild_chain gains a `cascade` argument. The old 1-arg signature must
-- be dropped and the 2-arg version recreated here: ALTER EXTENSION UPDATE runs
-- only this delta script, not the full generated schema, so without this CREATE
-- the function would be uninstalled by the upgrade.
DROP FUNCTION IF EXISTS public.reflex_rebuild_chain(TEXT);

CREATE OR REPLACE FUNCTION "reflex_rebuild_chain"(
	"view_name" TEXT, /* &str */
	"cascade" bool DEFAULT FALSE /* bool */
) RETURNS TEXT /* String */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_rebuild_chain_wrapper';

DO $migrate$ BEGIN
    RAISE NOTICE 'pg_reflex 1.10.10: partition-flush deadlock fixes. Roots stuck in __reflex_partition_pending should be re-checked with reflex_doctor.';
END $migrate$;
