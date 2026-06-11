-- Migration: pg_reflex 1.10.3 → 1.10.4
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.10.4';
--
-- 1.10.4 lands two follow-up fixes to 1.10.x. Replace the `.so` BEFORE running
-- `ALTER EXTENSION … UPDATE`.
--
--   * Partition attach/detach no-op skip now covers DETACH-then-DROP. The
--     1.10.3 incremental partition-delta path skips updating a dependent
--     unpartitioned IMV when an attached/detached partition holds no rows the
--     IMV's WHERE filter keeps — by probing the partition child's rows. The
--     flush is a DEFERRED trigger that fires at COMMIT, so when a partition is
--     DETACHed and DROPped in the SAME transaction (the common migration-tool
--     pattern) the child is already gone by flush time, the probe is impossible,
--     and the IMV force-reconciled (full TRUNCATE + rebuild + downstream
--     cascade) — exactly the cost the optimization removes. This was a
--     performance regression only; the reconcile produced correct data.
--
--     The fix proves the dropped partition was irrelevant from its captured
--     LIST bound instead of its (now-gone) rows. `refresh_source_snapshot`
--     (Rust) now records each leaf's `FOR VALUES …` bound in the existing
--     `__reflex_source_partition_snapshot.bound` column, and the new
--     `reflex_partition_drop_maybe_skip` SQL function (installed below) probes
--     the IMV filter against that bound. It is sound by construction: the probe
--     relation exposes ONLY the partition key column, so a predicate touching
--     any non-key column raises an error that is trapped → reconcile; RANGE/HASH
--     bounds, multi-key sources, no-filter IMVs, and any inconclusive branch all
--     fall back to `reflex_reconcile`. Only a clean "no partition value passes
--     the filter" proves the no-op and skips. The flush wiring is in Rust
--     (`src/partition.rs`); existing IMVs benefit once their snapshot is
--     re-seeded by the next flush (the first detach-drop after upgrade still
--     reconciles).
--
--   * `drop_reflex_ivm` no longer leaks the per-source DEFERRED staging delta
--     table (`__reflex_delta_<source>`). When the last DEFERRED IMV on a source
--     is dropped, its shared staging delta is now dropped too. This is a pure
--     Rust change to `drop_reflex_ivm` (a C-language function whose CREATE
--     statement is unchanged) — it needs NO migration DDL, only the recompiled
--     module. The `OrphanStaging` audit check (`reflex_audit`) flagged these
--     leaked tables; pre-1.10.4 orphans are also cleaned at create time by
--     `ensure_staging_matches_source`.

-- === New SQL helper: detach-then-drop no-op proof. For a DROP whose child is
-- === gone by flush time, prove the partition was irrelevant to an unpartitioned
-- === IMV's filter from its captured LIST bound (the rows are gone, the bound is
-- === not). The synthetic probe relation exposes only the partition key column,
-- === so a non-key-column predicate raises "column does not exist" → trapped →
-- === reconcile. Postgres parses the bound value list itself (`unnest(ARRAY[…])`),
-- === so there is no fragile text splitting. Every inconclusive branch falls back
-- === to reflex_reconcile (always correct).
CREATE OR REPLACE FUNCTION public.reflex_partition_drop_maybe_skip(
    _imv TEXT, _keycol TEXT, _bound_inner TEXT
) RETURNS TEXT LANGUAGE plpgsql AS $fn$
DECLARE
    _wp TEXT;
    _hit BOOLEAN;
BEGIN
    SELECT where_predicate INTO _wp
      FROM public.__reflex_ivm_reference
     WHERE name = _imv AND enabled = TRUE;
    IF NOT FOUND THEN RETURN 'SKIPPED (imv not found)'; END IF;

    PERFORM pg_advisory_xact_lock(hashtext(_imv), hashtext(reverse(_imv)));

    -- No filter → every dropped partition's rows were in the IMV; we cannot
    -- prove a no-op, so reconcile.
    IF _wp IS NULL OR _wp = '' THEN
        PERFORM public.reflex_reconcile(_imv);
        RETURN 'RECONCILED (no predicate)';
    END IF;

    BEGIN
        EXECUTE format(
            'SELECT bool_or(%s) FROM (SELECT unnest(ARRAY[%s]) AS %I) AS s',
            _wp, _bound_inner, _keycol
        ) INTO _hit;
    EXCEPTION WHEN OTHERS THEN
        PERFORM public.reflex_reconcile(_imv);
        RETURN 'RECONCILED (probe inconclusive)';
    END;

    -- bool_or IS NOT TRUE  ⇔  no partition value passes the filter  ⇔  the
    -- partition never contributed a row to the IMV  ⇔  its removal is a no-op.
    IF _hit IS NOT TRUE THEN
        RETURN 'SKIPPED (bound excluded by filter)';
    END IF;

    -- A value passes the filter, so the partition may have held IMV rows; they
    -- are gone with the dropped child, so a DELETE delta is impossible.
    PERFORM public.reflex_reconcile(_imv);
    RETURN 'RECONCILED (bound relevant)';
END;
$fn$;
