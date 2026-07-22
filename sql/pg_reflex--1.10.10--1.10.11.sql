-- Migration: pg_reflex 1.10.10 → 1.10.11
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.10.11';
--
-- Replace the module (.so) BEFORE running this.
--
-- Two independent fixes, both delivered entirely in the module — no SQL
-- signatures, tables, or triggers change, so this delta carries no DDL:
--
--   * reflex_reconcile / reflex_sync_partitions no longer set the superuser-only
--     `session_replication_role` GUC when relocating default-partition rows.
--     Trigger suppression during the relocation now uses ownership-scoped
--     `ALTER TABLE ... DISABLE/ENABLE TRIGGER USER` on the relocation roots, so
--     a non-superuser table owner can run reconcile. Every root disabled is
--     re-enabled on all exit paths, including a partial-failure bailout.
--   * The archive_residue audit check now VERIFIES residue on multi-source
--     (join) and aggregate IMVs instead of reporting "cannot confirm". For each
--     empty IMV partition it probes the IMV's own definition (base_query) scoped
--     to that partition and reports confirmed residue only when the definition
--     would have produced rows; a failed or timed-out probe degrades to a
--     prose advisory. Correctly-empty partitions (filtered out, or no join
--     match) are no longer flagged.

SELECT 1 WHERE FALSE;

DO $migrate$ BEGIN
    RAISE NOTICE 'pg_reflex 1.10.11: non-superuser reconcile + multi-source archive_residue verification. Re-run reflex_doctor() to re-check any partitions previously reported as "cannot confirm".';
END $migrate$;
