-- Migration: pg_reflex 1.10.2 → 1.10.3
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.10.3';
--
-- 1.10.3 lands two independent changes:
--
--   * Correctness + performance fix for IMVs whose FROM clause contains a
--     `UNION ALL` subquery. A mutation to one operand was maintained as if the
--     whole subquery were the delta, re-counting the unchanged sibling operands
--     (silent wrong SUM in overlapping groups) and full-scanning the base
--     (O(base) — a 1-row delta took ~18 min on the production
--     `sop_incoming_stock_baseline_view`). This is a pure Rust trigger-codegen
--     fix — it needs NO migration DDL; the corrected delta SQL is generated at
--     trigger time from the unchanged stored `base_query`, so existing IMVs are
--     fixed automatically once the recompiled module is loaded. Replace the
--     `.so` BEFORE running `ALTER EXTENSION … UPDATE`.
--
--   * Incremental partition delta for unpartitioned IMVs
--     (plans/2026-06-11): attaching/detaching a partition on a partitioned
--     source no longer forces a full `reflex_reconcile` (TRUNCATE + rebuild) of
--     every dependent unpartitioned IMV. The partition child is applied through
--     the normal incremental INSERT/DELETE maintenance path instead, so a
--     net-zero change (e.g. attaching a non-current LIST assortment) skips the
--     downstream cascade entirely. This adds one new SQL function, installed
--     below for existing databases.

-- === New SPI helper: apply an attached/detached partition child to an
-- === UNPARTITIONED IMV as the bulk INSERT/DELETE it semantically is, instead
-- === of a full TRUNCATE+rebuild reconcile. Mirrors the INSERT/DELETE trigger
-- === body pipeline (pred-check skip → Path B ratio dispatch →
-- === reflex_build_delta_sql → execute), parameterized at runtime. `_trans` is
-- === the conventional transition-table name the caller computes via
-- === transition_{new,old}_table_name(_source) that reflex_build_delta_sql
-- === reads from. Every uncertain branch falls back to reflex_reconcile
-- === (always correct).
CREATE OR REPLACE FUNCTION public.reflex_apply_partition_delta(
    _imv TEXT, _source TEXT, _op TEXT, _child TEXT, _trans TEXT
) RETURNS TEXT LANGUAGE plpgsql AS $fn$
DECLARE
    _rec RECORD;
    _sql TEXT;
    _no_pass BOOLEAN;
    _src_total BIGINT;
    _trans_count BIGINT;
    _thr NUMERIC;
BEGIN
    SELECT base_query, end_query, aggregations::text AS aggregations,
           where_predicate, wipe_threshold
      INTO _rec
      FROM public.__reflex_ivm_reference
     WHERE name = _imv AND enabled = TRUE;
    IF NOT FOUND THEN RETURN 'SKIPPED (imv not found)'; END IF;

    PERFORM pg_advisory_xact_lock(hashtext(_imv), hashtext(reverse(_imv)));

    -- No-op short-circuit FIRST, probing the child directly so a filtered-out
    -- partition is skipped in O(1) (the planner evaluates the WHERE against the
    -- partition's constant key) without materializing the transition at all.
    -- where_predicate is the bare-column form, which evaluates against the
    -- child the same as against the flat transition table.
    IF _rec.where_predicate IS NOT NULL AND _rec.where_predicate <> '' THEN
        EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM %s WHERE %s LIMIT 1)',
                       _child, _rec.where_predicate) INTO _no_pass;
        IF _no_pass THEN
            RETURN 'SKIPPED (no rows pass filter)';
        END IF;
    END IF;

    -- Materialize the partition child as the conventional transition table
    -- reflex_build_delta_sql reads from.
    EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', _trans);
    EXECUTE format('CREATE TEMP TABLE %I ON COMMIT DROP AS SELECT * FROM %s', _trans, _child);

    -- Path B: a bulk change large relative to the source is cheaper to
    -- reconcile than to delta (same decision a real bulk INSERT makes).
    BEGIN
        SELECT reltuples::BIGINT INTO _src_total FROM pg_class WHERE oid = _source::regclass;
        IF _src_total IS NOT NULL AND _src_total >= 1000 THEN
            EXECUTE format('SELECT count(*) FROM %I', _trans) INTO _trans_count;
            _thr := COALESCE(_rec.wipe_threshold,
                             current_setting('reflex.wipe_threshold', true)::NUMERIC, 0.5);
            IF _trans_count::NUMERIC / _src_total >= _thr THEN
                EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', _trans);
                PERFORM public.reflex_reconcile(_imv);
                RETURN 'RECONCILED (path B)';
            END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN NULL; END;

    -- Incremental delta — the exact pipeline the INSERT/DELETE triggers run.
    _sql := public.reflex_build_delta_sql(_imv, _source, _op,
                _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
    IF _sql IS NULL OR _sql = '' THEN
        EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', _trans);
        PERFORM public.reflex_reconcile(_imv);
        RETURN 'RECONCILED (no incremental delta)';
    END IF;
    PERFORM public.reflex_execute_separated(_sql);
    EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I', _trans);
    RETURN 'DELTA';
END;
$fn$;
