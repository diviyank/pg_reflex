-- Migration: pg_reflex 1.0.2 -> 1.0.3
--
-- New features:
-- 1. Storage mode: LOGGED/UNLOGGED option for crash safety vs performance tradeoff
-- 2. Refresh mode: IMMEDIATE/DEFERRED option for batch coalescing at COMMIT
-- 3. Materialized view auto-refresh via event trigger
--
-- Existing IMVs default to UNLOGGED + IMMEDIATE (preserving current behavior).

DO $migration$
BEGIN
    -- 1. Add storage_mode column
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS storage_mode TEXT DEFAULT 'UNLOGGED';

    -- 2. Add refresh_mode column
    ALTER TABLE public.__reflex_ivm_reference
        ADD COLUMN IF NOT EXISTS refresh_mode TEXT DEFAULT 'IMMEDIATE';

    -- 3. Backfill existing IMVs (preserves current behavior)
    UPDATE public.__reflex_ivm_reference
    SET storage_mode = 'UNLOGGED',
        refresh_mode = 'IMMEDIATE'
    WHERE storage_mode IS NULL OR refresh_mode IS NULL;

    -- 4. Create deferred processing infrastructure
    CREATE TABLE IF NOT EXISTS public.__reflex_deferred_pending (
        id BIGSERIAL,
        source_table TEXT NOT NULL,
        operation TEXT NOT NULL,
        batch_ts TIMESTAMPTZ DEFAULT now()
    );

    -- 5. Deferred flush constraint trigger function
    CREATE OR REPLACE FUNCTION __reflex_deferred_flush_fn() RETURNS TRIGGER AS $fn$
    DECLARE _src RECORD;
    BEGIN
        FOR _src IN
            SELECT DISTINCT source_table FROM public.__reflex_deferred_pending
        LOOP
            PERFORM reflex_flush_deferred(_src.source_table);
        END LOOP;
        RETURN NULL;
    END;
    $fn$ LANGUAGE plpgsql;

    -- 6. Constraint trigger fires at COMMIT for deferred delta processing
    -- Drop if exists to allow re-creation with updated definition
    BEGIN
        DROP TRIGGER IF EXISTS __reflex_deferred_flush_trigger ON public.__reflex_deferred_pending;
    EXCEPTION WHEN OTHERS THEN
        NULL; -- ignore if table didn't exist before
    END;

    CREATE CONSTRAINT TRIGGER __reflex_deferred_flush_trigger
        AFTER INSERT ON public.__reflex_deferred_pending
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION __reflex_deferred_flush_fn();

    RAISE NOTICE 'pg_reflex migration 1.0.2 -> 1.0.3: storage_mode, refresh_mode, deferred infrastructure complete';
END;
$migration$;

-- 7. Materialized view auto-refresh event trigger
-- When REFRESH MATERIALIZED VIEW is executed, automatically cascade to
-- any pg_reflex IMVs that depend on that materialized view.
CREATE OR REPLACE FUNCTION __reflex_matview_refresh_handler() RETURNS event_trigger AS $$
DECLARE
    obj RECORD;
    _src TEXT;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF obj.command_tag = 'REFRESH MATERIALIZED VIEW' THEN
            _src := obj.object_identity;
            -- Check if any IMV depends on this materialized view
            IF EXISTS (
                SELECT 1 FROM public.__reflex_ivm_reference
                WHERE _src = ANY(depends_on)
            ) THEN
                PERFORM refresh_imv_depending_on(_src);
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Drop if exists to allow clean re-creation
DROP EVENT TRIGGER IF EXISTS __reflex_matview_refresh;

CREATE EVENT TRIGGER __reflex_matview_refresh
    ON ddl_command_end
    WHEN TAG IN ('REFRESH MATERIALIZED VIEW')
    EXECUTE FUNCTION __reflex_matview_refresh_handler();
