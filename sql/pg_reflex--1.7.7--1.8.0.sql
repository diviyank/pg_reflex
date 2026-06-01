-- Migration: pg_reflex 1.7.7 → 1.8.0
--
-- Schema-qualify all trigger functions to public and register them as
-- extension members. Eliminates the per-schema duplicate copies that were
-- created when IMVs were built under a non-public search_path, and re-points
-- the shared deferred-flush trigger at the canonical public copy.
--
-- Idempotent and safe to re-run; per-source rebuild is exception-isolated so a
-- single bad source cannot abort the upgrade.

-- 1. Adopt-if-orphan so step 2's CREATE OR REPLACE is permitted on an existing
--    non-member public copy.
DO $heal$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE p.proname = '__reflex_deferred_flush_fn' AND n.nspname = 'public')
     AND NOT EXISTS (
       SELECT 1 FROM pg_depend d
       JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'pg_reflex'
       JOIN pg_proc p ON p.oid = d.objid
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE d.deptype = 'e' AND p.proname = '__reflex_deferred_flush_fn' AND n.nspname = 'public')
  THEN
    ALTER EXTENSION pg_reflex ADD FUNCTION public.__reflex_deferred_flush_fn();
  END IF;
END
$heal$;

-- 2. Canonical public flush fn (transaction-local body; auto-members if created fresh here).
CREATE OR REPLACE FUNCTION public.__reflex_deferred_flush_fn() RETURNS TRIGGER AS $fn$
BEGIN
  PERFORM public.reflex_flush_deferred(NEW.source_table);
  RETURN NULL;
END;
$fn$ LANGUAGE plpgsql;

-- 3. Re-point the single live trigger at the public copy.
DROP TRIGGER IF EXISTS __reflex_deferred_flush_trigger ON public.__reflex_deferred_pending;
CREATE CONSTRAINT TRIGGER __reflex_deferred_flush_trigger
  AFTER INSERT ON public.__reflex_deferred_pending
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION public.__reflex_deferred_flush_fn();

-- 4. Rebuild every source's triggers with the fixed (qualified+member) codegen.
--    Per-source exception isolation. Skip synthetic depends_on entries (<subquery:…>).
DO $rebuild$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT DISTINCT src
    FROM public.__reflex_ivm_reference, unnest(depends_on) AS src
    WHERE enabled AND src NOT LIKE '<%'
  LOOP
    BEGIN
      PERFORM reflex_rebuild_triggers(r.src);
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'pg_reflex 1.8.0: reflex_rebuild_triggers(%) failed: %', r.src, SQLERRM;
    END;
  END LOOP;
END
$rebuild$;

-- 5. Orphan sweep: drop every __reflex_* function in a non-public schema bound
--    to no live trigger (the now-unbound per-schema copies). Runs after re-point/rebuild.
DO $sweep$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT n.nspname AS schema, p.proname AS fn
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE p.proname LIKE '__reflex\_%'
      AND n.nspname <> 'public'
      AND NOT EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgfoid = p.oid AND NOT t.tgisinternal)
  LOOP
    EXECUTE format('DROP FUNCTION IF EXISTS %I.%I()', r.schema, r.fn);
  END LOOP;
END
$sweep$;

DO $note$
BEGIN
  RAISE NOTICE 'pg_reflex 1.8.0: trigger functions consolidated to public and registered as extension members.';
END
$note$;
