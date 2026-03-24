-- Migration: pg_reflex 1.0.1 → 1.0.2
--
-- Performance optimizations:
-- 1. Target tables converted from LOGGED to UNLOGGED (matches intermediate)
-- 2. Intermediate B-tree PK replaced with hash index (single-col) or plain B-tree (multi-col)
-- 3. Trigger SQL regenerated automatically (MERGE RETURNING pattern) — no migration needed
--
-- NOTE: Converting LOGGED → UNLOGGED requires recreating the table.
-- We use reflex_reconcile() which does TRUNCATE + re-INSERT + index rebuild.
-- For large IMVs this may take a while.

DO $migration$
DECLARE
    _rec RECORD;
    _intermediate TEXT;
    _pk_name TEXT;
    _idx_name TEXT;
    _group_cols TEXT[];
    _col TEXT;
    _has_intermediate BOOLEAN;
BEGIN
    FOR _rec IN
        SELECT name, aggregations::text AS agg_json, index_columns
        FROM public.__reflex_ivm_reference
        ORDER BY graph_depth
    LOOP
        -- Check if this is an aggregate IMV (has intermediate table)
        _intermediate := '__reflex_intermediate_' || _rec.name;
        SELECT EXISTS(
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = _intermediate
        ) INTO _has_intermediate;

        IF NOT _has_intermediate THEN
            -- Passthrough IMV: no intermediate, but convert target to UNLOGGED
            -- (passthrough targets are now also UNLOGGED)
            BEGIN
                EXECUTE format('ALTER TABLE %I SET UNLOGGED', _rec.name);
                RAISE NOTICE 'pg_reflex migration: converted target "%" to UNLOGGED', _rec.name;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'pg_reflex migration: could not convert "%" to UNLOGGED: %', _rec.name, SQLERRM;
            END;
            CONTINUE;
        END IF;

        -- Aggregate IMV: convert target to UNLOGGED
        BEGIN
            EXECUTE format('ALTER TABLE %I SET UNLOGGED', _rec.name);
            RAISE NOTICE 'pg_reflex migration: converted target "%" to UNLOGGED', _rec.name;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pg_reflex migration: could not convert "%" to UNLOGGED: %', _rec.name, SQLERRM;
        END;

        -- Drop the B-tree PK on intermediate (replaced by hash/btree index)
        _pk_name := _intermediate || '_pkey';
        BEGIN
            EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT IF EXISTS %I', _intermediate, _pk_name);
            RAISE NOTICE 'pg_reflex migration: dropped PK on "%"', _intermediate;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pg_reflex migration: no PK to drop on "%": %', _intermediate, SQLERRM;
        END;

        -- Create replacement index (hash for single-col, btree for multi-col)
        _group_cols := _rec.index_columns;
        IF _group_cols IS NOT NULL AND array_length(_group_cols, 1) > 0 THEN
            _idx_name := 'idx__reflex_int_' || _rec.name;

            -- Drop if exists (idempotent)
            EXECUTE format('DROP INDEX IF EXISTS public.%I', _idx_name);

            IF array_length(_group_cols, 1) = 1 THEN
                -- Single column: hash index
                EXECUTE format(
                    'CREATE INDEX %I ON public.%I USING hash (%I)',
                    _idx_name, _intermediate, _group_cols[1]
                );
                RAISE NOTICE 'pg_reflex migration: created hash index on "%.%"', _intermediate, _group_cols[1];
            ELSE
                -- Multi-column: B-tree index
                EXECUTE format(
                    'CREATE INDEX %I ON public.%I (%s)',
                    _idx_name, _intermediate,
                    (SELECT string_agg(format('%I', col), ', ') FROM unnest(_group_cols) AS col)
                );
                RAISE NOTICE 'pg_reflex migration: created B-tree index on "%"', _intermediate;
            END IF;
        END IF;

    END LOOP;

    RAISE NOTICE 'pg_reflex migration 1.0.1 → 1.0.2 complete';
END;
$migration$;
