-- Migration: pg_reflex 1.7.1 → 1.7.2
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.7.2';
--
-- 1.7.2 is a correctness release fixing `drop_reflex_ivm`, which silently
-- orphaned the target + auxiliary tables of any IMV created with a BARE
-- (unqualified) name while the session `search_path` pointed at a non-public
-- schema.
--
-- Root cause: every teardown statement derived its relation names from the
-- stored (bare) `name` and resolved the target via `to_regclass(name)`, both
-- of which honour the session `search_path` AT DROP TIME. An IMV created while
-- `search_path = alp` landed its objects in `alp`, but a later
-- `drop_reflex_ivm` run under a different `search_path` issued unqualified
-- `DROP TABLE IF EXISTS …` that resolved against the wrong schema, skipped
-- every real object, deleted only the catalog row, and left the target plus
-- `__reflex_intermediate_*` / `__reflex_affected_*` / `__reflex_uk_*` behind.
-- A same-named decoy relation of a different relkind in the `search_path`
-- (e.g. a materialized view) could be hit instead, surfacing as
-- `ERROR: "<name>" is not a table`.
--
-- Fix: creation now records the schema the objects were created in
-- (`current_schema()` for bare names, the explicit schema for qualified
-- names) in the new `target_schema` column, and `drop_reflex_ivm`
-- re-qualifies all teardown DDL with it — making cleanup independent of the
-- session `search_path`. Rows that predate this column stay NULL and drop
-- falls back to the prior `search_path` resolution, preserving old behaviour.
--
-- Migration step: add the nullable column. No data backfill — existing rows
-- keep NULL (legacy fallback). Newly created IMVs populate it automatically.

ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS target_schema TEXT;

DO $migrate$
BEGIN
    RAISE NOTICE 'pg_reflex 1.7.2: drop_reflex_ivm teardown is now search_path-independent for IMVs created after this upgrade (target_schema recorded at create). Pre-existing rows keep NULL target_schema and use the legacy search_path fallback.';
END
$migrate$;
