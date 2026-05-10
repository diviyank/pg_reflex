-- Migration: pg_reflex 1.3.0 → 1.4.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.4.0';
--
-- Changes in 1.4.0:
--
-- A — Top-K MIN/MAX is auto-enabled (K=16) on freshly created IMVs.
--     Behaviour change for `create_reflex_ivm` only — existing IMVs are
--     untouched. To retrofit top-K onto an in-flight IMV, drop and
--     recreate it.
--
-- B — N1 heap-shrinkage gate.
--     UPDATEs on top-K MIN/MAX IMVs now scope the forced source-scan
--     recompute to a new persistent capture table `__reflex_shrunk_<view>`
--     populated post-Sub. Only groups whose heap shrank below K trigger
--     the recompute; groups whose heap stayed at K rely on the algebraic
--     Sub+Add merge alone. The capture table mirrors the column shape of
--     the existing `__reflex_affected_<view>` table.
--
--     This migration provisions the new capture table for every IMV in
--     the registry that already has a top-K column (i.e. was created in
--     1.3.0 with an explicit `topk = K` parameter). Newly created 1.4.0
--     IMVs provision it at create time; this block is purely for
--     in-flight upgrades.
--
-- C — Top-K MIN/MAX over non-NUMERIC source columns + UPDATE staleness fix.
--     No DDL change — the fixes live in trigger MERGE codegen and in
--     `create_reflex_ivm_impl` post-catalog-resolution. Existing 1.3.0
--     top-K IMVs over TEXT / DATE / TIMESTAMP columns continue to fire
--     their already-installed (broken) trigger bodies; rebuild them via
--     `reflex_rebuild_imv('<name>')` to pick up the corrected codegen.
--
-- D — O2 per-backend delta-SQL template cache.
--     In-process only, no schema impact.
--
-- E — Non-deterministic-function rejection message clarified to be
--     query-wide. No DDL change — analyzer-only.

-- === 1.4.0-B: provision __reflex_shrunk_<view> for existing top-K IMVs ===
DO $REFLEX_MIG_140$
DECLARE
    rec RECORD;
    affected_short TEXT;
    shrunk_short TEXT;
    bare_view TEXT;
    affected_schema TEXT;
BEGIN
    FOR rec IN
        SELECT name, aggregations
        FROM public.__reflex_ivm_reference
        WHERE aggregations IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM jsonb_array_elements(
                       (aggregations::jsonb) -> 'intermediate_columns'
                   ) AS ic
              WHERE (ic ->> 'topk_k') IS NOT NULL
          )
    LOOP
        bare_view := split_part(rec.name, '.', 2);
        IF bare_view = '' THEN
            bare_view := rec.name;
        END IF;

        affected_short := format('__reflex_affected_%s', bare_view);
        shrunk_short := format('__reflex_shrunk_%s', bare_view);

        SELECT n.nspname INTO affected_schema
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = affected_short AND c.relkind = 'r'
        LIMIT 1;

        IF affected_schema IS NULL THEN
            RAISE NOTICE 'pg_reflex 1.4.0: % has top-K but % not found — skipping shrunk-table provision (run reflex_rebuild_imv to recover)',
                rec.name, affected_short;
            CONTINUE;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = shrunk_short
              AND n.nspname = affected_schema
              AND c.relkind = 'r'
        ) THEN
            EXECUTE format(
                'CREATE UNLOGGED TABLE IF NOT EXISTS %I.%I (LIKE %I.%I)',
                affected_schema, shrunk_short,
                affected_schema, affected_short
            );
            RAISE NOTICE 'pg_reflex 1.4.0: provisioned %.% for top-K IMV %',
                affected_schema, shrunk_short, rec.name;
        END IF;
    END LOOP;
END;
$REFLEX_MIG_140$;

-- create_reflex_ivm signature is unchanged at the SQL surface; the new
-- DEFAULT_TOPK_K is supplied internally by the Rust function body.
-- All other SPIs (reflex_flush_deferred, reflex_reconcile, etc.) are
-- pgrx-regenerated at install time — no manual CREATE OR REPLACE needed.
