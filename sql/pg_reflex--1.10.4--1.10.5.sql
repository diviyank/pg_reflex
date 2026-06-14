-- Migration: pg_reflex 1.10.4 → 1.10.5
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.10.5';
--
-- 1.10.5 makes attaching a new top-level partition with many EMPTY
-- sub-partitions cheap. A demand-plan macro-partition attaches a fixed window
-- of monthly sub-partitions (e.g. 48 months) even though only a handful hold
-- data; the partition flush reconciled every leaf, and each reconcile re-synced
-- the whole tree AND paid the fixed per-leaf swap DDL (CREATE … LIKE + fill +
-- ANALYZE + DETACH/ATTACH/DROP — a cost independent of row count). The flush now
-- syncs each IMV's tree ONCE up front, then drives the per-leaf reconciles with
-- skip_sync => true, and skips outright any brand-new (AttachNew) leaf whose
-- source is empty (a provable empty→empty no-op: the up-front sync already
-- created its empty mirror partition, and there is no prior target data to
-- clear). SwapFill and surviving-ancestor refills always fill, and any
-- source-probe failure falls back to filling — the skip never trades
-- correctness for speed.
--
-- The flush wiring is pure Rust (src/partition.rs); the only schema change is
-- the new skip_sync argument on reflex_reconcile_partition below. Replace the
-- `.so` BEFORE running `ALTER EXTENSION … UPDATE`.

-- === reflex_reconcile_partition gains a 4th `skip_sync` argument ===
-- The 3-arg form (1.8.1) and the 4-arg form are distinct SQL signatures, so
-- CREATE OR REPLACE cannot upgrade in place — drop the old 3-arg function and
-- create the 4-arg one (the new wrapper symbol reads four args; leaving the old
-- 3-arg declaration would invoke the wrapper with the wrong arity). The
-- DEFAULT false keeps existing 2-/3-arg call sites working unchanged.
DROP FUNCTION IF EXISTS public.reflex_reconcile_partition(TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION public.reflex_reconcile_partition(
    view_name TEXT,
    partition_keys TEXT,
    source_partition TEXT DEFAULT '',
    skip_sync bool DEFAULT FALSE
)
    RETURNS TEXT
    STRICT
    LANGUAGE c
    AS 'MODULE_PATHNAME', 'reflex_reconcile_partition_wrapper';
