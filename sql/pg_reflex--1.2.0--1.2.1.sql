-- Migration: pg_reflex 1.2.0 → 1.2.1
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.2.1';
--
-- Changes in 1.2.1:
--
-- Theme A — pg_reflex.alter_source_policy GUC (R2)
--   The __reflex_on_ddl_command_end event trigger now reads
--   pg_reflex.alter_source_policy. The default 'warn' preserves the 1.2.0
--   behaviour (RAISE WARNING, ALTER proceeds). Setting the GUC to 'error'
--   makes the trigger RAISE EXCEPTION, rolling back the ALTER. Custom
--   namespaced GUCs work without explicit registration on PG 9.2+.
--
-- Theme B — Passthrough PK auto-detection UX (R5)
--   create_reflex_ivm gains a clearer info message when a single-source
--   passthrough has a PK that is not in the SELECT list. Code-only change;
--   no DDL impact.
--
-- Theme C — Scheduled reconciliation (R7)
--   New SPI reflex_scheduled_reconcile(max_age_minutes INTEGER DEFAULT 60)
--   returns one row per attempted IMV. Designed for pg_cron. Function is
--   auto-registered by pgrx schema generation; no manual SQL needed.

-- === Updated event trigger body (Theme A) ===
CREATE OR REPLACE FUNCTION public.__reflex_on_ddl_command_end()
RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    _cmd RECORD;
    _imv RECORD;
    _src TEXT;
    _policy TEXT;
    _affected TEXT[] := ARRAY[]::TEXT[];
BEGIN
    _policy := lower(COALESCE(NULLIF(current_setting('pg_reflex.alter_source_policy', true), ''), 'warn'));
    IF _policy NOT IN ('warn', 'error') THEN
        RAISE WARNING 'pg_reflex: invalid pg_reflex.alter_source_policy=%, falling back to ''warn''', _policy;
        _policy := 'warn';
    END IF;

    FOR _cmd IN
        SELECT object_identity, command_tag
        FROM pg_event_trigger_ddl_commands()
        WHERE command_tag = 'ALTER TABLE'
    LOOP
        _src := _cmd.object_identity;
        FOR _imv IN
            SELECT name FROM public.__reflex_ivm_reference
            WHERE depends_on @> ARRAY[_src]
               OR depends_on @> ARRAY[split_part(_src, '.', 2)]
        LOOP
            _affected := _affected || (_src || ' -> ' || _imv.name);
            IF _policy = 'warn' THEN
                RAISE WARNING 'pg_reflex: source table % was altered; IMV % may be stale — run SELECT reflex_rebuild_imv(''%'') to recover',
                    _src, _imv.name, _imv.name;
            END IF;
        END LOOP;
    END LOOP;

    IF _policy = 'error' AND array_length(_affected, 1) > 0 THEN
        RAISE EXCEPTION 'pg_reflex: ALTER blocked by pg_reflex.alter_source_policy=''error'' on tracked source(s); affected: %',
            array_to_string(_affected, ', ')
            USING HINT = 'Set pg_reflex.alter_source_policy = ''warn'' (default) or drop_reflex_ivm() first.';
    END IF;
END;
$$;
