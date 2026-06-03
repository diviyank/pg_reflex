-- Migration: pg_reflex 1.8.1 → 1.8.2
--
-- Shallow partition mirroring (plans/2026-06-03-imv-partition-depth.md).
-- A partitioned IMV may now mirror its source at a depth <= the source's
-- partition depth. Explicit `partition_by` is authoritative for the IMV's
-- depth; auto-mirror prunes to the deepest level whose column is a bare
-- projected output column. Capture (sync / reconcile / flush / audit) is
-- depth-aware: a source sub-partition change reconciles up to the IMV's
-- mirror-depth partition.
--
-- This script is additive and NON-BREAKING — it only adds two nullable
-- columns. No function signatures changed; the new depth-aware behavior ships
-- in the .so, so replace it before running
-- `ALTER EXTENSION pg_reflex UPDATE TO '1.8.2';`.
--
-- Idempotent and safe to re-run.

-- The IMV's partition mirror depth: how many source partition levels it
-- mirrors. NULL = mirror the FULL source depth (the prior behavior), so every
-- existing partitioned IMV keeps working with no recreate.
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS partition_depth INT;

-- Per source leaf, the root-first list of its ancestor bare-names. Lets the
-- flush map a swapped-out (vanished) leaf up to its mirror-depth ancestor even
-- though it is no longer in the live partition tree.
ALTER TABLE public.__reflex_source_partition_snapshot
    ADD COLUMN IF NOT EXISTS ancestors TEXT[];
