-- Migration: pg_reflex 1.1.1 -> 1.1.2
--
-- Bug fixes (all 2026-04-21):
--   1. BOOL_OR / MIN / MAX recompute now respects JOIN aliases in the stored
--      base_query. `build_min_max_recompute_sql` emits UPDATE ... FROM
--      (orig_base_query) AS __src instead of a scalar subquery restricted to
--      the bare source table.
--   2. `replace_source_with_delta` pass 2 consumes an existing user alias
--      (`AS <id>` or bare `<id>`) so the substituted subquery adopts the
--      user's alias instead of appending `AS __dt` on top — eliminates the
--      `AS __dt AS ol` / `AS __dt lib` / `AS __dt AS pol` syntax errors.
--
-- Function-signature change:
--   `reflex_build_delta_sql` gains a 7th TEXT argument `orig_base_query`.
--   All pgrx-generated trigger bodies stored in the catalog from prior
--   versions pass 6 arguments; we patch each trigger body in-place so the
--   stored `_rec.base_query` is also passed as the 7th argument (for plain
--   source tables this is equivalent to the old behavior).
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.1.2';

-- === 1. Replace reflex_build_delta_sql with the 7-arg version ===
DROP FUNCTION IF EXISTS reflex_build_delta_sql(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT);

CREATE FUNCTION reflex_build_delta_sql(
    "view_name" TEXT,
    "source_table" TEXT,
    "operation" TEXT,
    "base_query" TEXT,
    "end_query" TEXT,
    "aggregations_json" TEXT,
    "orig_base_query" TEXT
) RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_build_delta_sql_wrapper';

-- === 2. Patch stored trigger bodies to pass a 7th argument ===
-- Matches the exact call shape emitted by schema_builder.rs (both
-- refresh_mode-aware and pre-refresh_mode trigger bodies).
DO $migration$
DECLARE
    _proc RECORD;
    _new_src TEXT;
    _patched INT := 0;
BEGIN
    FOR _proc IN
        SELECT oid, proname, prosrc
        FROM pg_proc
        WHERE prosrc LIKE '%reflex_build_delta_sql(_rec.name%'
          AND prosrc NOT LIKE '%_rec.aggregations, _rec.base_query)%'
    LOOP
        _new_src := regexp_replace(
            _proc.prosrc,
            '(reflex_build_delta_sql\([^;]*?_rec\.aggregations)\)',
            '\1, _rec.base_query)',
            'g'
        );
        IF _new_src IS DISTINCT FROM _proc.prosrc THEN
            UPDATE pg_proc SET prosrc = _new_src WHERE oid = _proc.oid;
            _patched := _patched + 1;
        END IF;
    END LOOP;
    RAISE NOTICE 'pg_reflex 1.1.1 -> 1.1.2: patched % trigger function bodies', _patched;
END;
$migration$;
