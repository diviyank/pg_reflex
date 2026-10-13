-- Migration: pg_reflex 1.12.0 → 1.12.1
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.12.1';
--
-- A partition swap of a source no longer refreshes, at COMMIT, the IMVs that
-- list the rebuilt partition-mirroring IMV in their ignored_sources: their
-- affected partitions are queued in __reflex_heal_pending (a whole-IMV entry
-- uses the partition_key '(whole IMV)') and healed by
-- reflex_heal_ignored_sources / reflex_scheduled_reconcile / reflex_doctor.
-- FULL JOIN passthrough deltas are scoped to the changed join keys, and an
-- aggregate target without a unique index is diffed by its groups. Those
-- changes are Rust-side.
--
-- This file replaces the one installed-SQL object that changed, byte-identical
-- to the bootstrap text: public.__reflex_heal_on_ignored_change() skips the
-- readers the partition flush already queued directly. No registry column,
-- table, index, trigger, event trigger or function signature changes; no IMV
-- needs to be recreated.

    CREATE OR REPLACE FUNCTION public.__reflex_heal_on_ignored_change()
    RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = pg_catalog, pg_temp SET extra_float_digits = 3 AS $fn$
    DECLARE
        _h RECORD;
        _key TEXT;
        _keys TEXT;
        _cols TEXT;
        _covered JSONB := NULLIF(current_setting('pg_reflex.heal_covered_by_rebuild', true), '')::jsonb;
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            UPDATE public.__reflex_ivm_reference r
               SET known_stale = TRUE,
                   stale_since = COALESCE(r.stale_since, now()),
                   stale_reason = CASE WHEN COALESCE(r.known_stale, FALSE) AND COALESCE(r.stale_reason, '') <> ''
                                       THEN r.stale_reason || ' | ' || m.reason
                                       ELSE m.reason END
              FROM (SELECT DISTINCT i.name,
                           format('ignored source %s was truncated, which no heal can scope. '
                                  || 'Run SELECT reflex_reconcile(%L);', TG_RELID::regclass, i.name) AS reason
                      FROM public.__reflex_ivm_reference i
                     CROSS JOIN LATERAL jsonb_each(COALESCE(i.aggregations->'ignore_heal_keys', '{}'::jsonb)) k
                     WHERE COALESCE(i.enabled, TRUE)
                       AND to_regclass(k.value->>'relation') = TG_RELID) m
             WHERE r.name = m.name
               AND NOT (COALESCE(r.known_stale, FALSE)
                        AND position(m.reason IN COALESCE(r.stale_reason, '')) > 0);
            RETURN NULL;
        END IF;
        FOR _h IN
            SELECT r.name, k.key AS source, k.value->>'source_column' AS source_column,
                   ARRAY(SELECT jsonb_array_elements_text(
                             COALESCE(k.value->'watched_columns', '[]'::jsonb))) AS watched
              FROM public.__reflex_ivm_reference r
             CROSS JOIN LATERAL jsonb_each(COALESCE(r.aggregations->'ignore_heal_keys', '{}'::jsonb)) k
             WHERE COALESCE(r.enabled, TRUE)
               AND to_regclass(k.value->>'relation') = TG_RELID
        LOOP
            CONTINUE WHEN (_covered->>'relid')::oid = TG_RELID AND _covered->'imvs' ? _h.name;
            CONTINUE WHEN EXISTS (
                SELECT 1 FROM unnest(_h.watched || _h.source_column) AS c
                 WHERE NOT EXISTS (SELECT 1 FROM pg_attribute a
                                    WHERE a.attrelid = TG_RELID AND a.attname = c
                                      AND a.attnum > 0 AND NOT a.attisdropped));
            CONTINUE WHEN NOT EXISTS (
                SELECT 1 FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
                 WHERE a.attrelid = TG_RELID AND a.attname = _h.source_column
                   AND t.typnamespace = 'pg_catalog'::regnamespace);
            _key := format('to_jsonb(%I) #>> ''{}''', _h.source_column);
            IF TG_OP = 'INSERT' THEN
                _keys := format('SELECT %s FROM __reflex_heal_new', _key);
            ELSIF TG_OP = 'DELETE' THEN
                _keys := format('SELECT %s FROM __reflex_heal_old', _key);
            ELSIF cardinality(_h.watched) = 0 THEN
                _keys := format('SELECT %1$s FROM __reflex_heal_old UNION SELECT %1$s FROM __reflex_heal_new', _key);
            ELSE
                SELECT string_agg(format('(%1$I IS NULL), format(''%%s'', %1$I)', c), ', ') INTO _cols
                  FROM unnest(_h.watched) AS c;
                _keys := format(
                    'SELECT d.__reflex_heal_key FROM ('
                    || '(SELECT %1$s AS __reflex_heal_key, %2$s FROM __reflex_heal_old '
                    || 'EXCEPT SELECT %1$s, %2$s FROM __reflex_heal_new) UNION ALL '
                    || '(SELECT %1$s, %2$s FROM __reflex_heal_new '
                    || 'EXCEPT SELECT %1$s, %2$s FROM __reflex_heal_old)) d',
                    _key, _cols);
            END IF;
            EXECUTE format(
                'INSERT INTO public.__reflex_heal_pending (imv_name, partition_key, source) '
                || 'SELECT DISTINCT $1, k, $2 FROM (%s) s(k) WHERE k IS NOT NULL '
                || 'ON CONFLICT (imv_name, partition_key) DO UPDATE '
                || 'SET enqueued_at = clock_timestamp(), source = EXCLUDED.source, last_error = NULL',
                _keys)
            USING _h.name, _h.source;
        END LOOP;
        RETURN NULL;
    END;
    $fn$;
