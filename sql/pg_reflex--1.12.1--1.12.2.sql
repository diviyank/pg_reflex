-- Migration: pg_reflex 1.12.1 → 1.12.2
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.12.2';
--
-- A performance release for DEFERRED chains, with one correctness fix.
-- A partition push refreshes the DEFERRED aggregates that read the rebuilt
-- IMV by group instead of leaving them to the COMMIT delta flush; the
-- MIN/MAX recompute of shrunk groups prunes partitions; a large DEFERRED
-- top-K MIN/MAX delta replaces its groups from the base query; and an UPDATE
-- of a DEFERRED source that leaves every column its IMVs read unchanged
-- costs nothing at COMMIT. Those changes are Rust-side or in trigger bodies
-- rendered by the library; existing sources pick up the UPDATE skip once
-- their triggers are re-rendered with SELECT reflex_rebuild_triggers('<source>');
--
-- The fix: a reflex.* setting that reads back as '' (after SET LOCAL, or SET
-- then RESET) raised 22P02 in the DEFERRED flush, which discarded the delta.
-- This file adds public.__reflex_setting(), which reads a setting with a
-- default for an empty or unparsable value, and replaces
-- public.reflex_apply_partition_delta() to use it. Both statements are
-- byte-identical to the bootstrap text. No registry column, table, index,
-- trigger, event trigger or function signature changes; no IMV needs to be
-- recreated.

    CREATE OR REPLACE FUNCTION public.__reflex_setting(_setting TEXT, _default anyelement)
    RETURNS anyelement LANGUAGE plpgsql STABLE AS $fn$
    DECLARE
        _raw TEXT := NULLIF(btrim(current_setting(_setting, true)), '');
        _value _default%TYPE;
    BEGIN
        IF _raw IS NULL THEN
            RETURN _default;
        END IF;
        BEGIN
            _value := _raw;
        EXCEPTION WHEN data_exception THEN
            RAISE WARNING 'pg_reflex: setting % = % is not a valid %; using the default %',
                _setting, quote_literal(_raw), pg_typeof(_default), _default;
            RETURN _default;
        END;
        RETURN _value;
    END;
    $fn$;

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
                                 public.__reflex_setting('reflex.wipe_threshold', 0.5::NUMERIC));
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
