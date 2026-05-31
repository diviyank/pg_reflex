-- Migration: pg_reflex 1.7.0 → 1.7.1
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.7.1';
--
-- 1.7.1 is a correctness release fixing Path C (the INSERT_PROMOTED smart
-- bulk-INSERT dispatch fired only by UPDATE triggers). Two compounding
-- defects:
--
--   (1) The Path C plpgsql block built the intermediate / scratch / target
--       relation names by parsing the IMV name with `split_part(name, '.', 1|2)`
--       and concatenating `'"<schema>"."__reflex_intermediate_<view>"'` by
--       hand. That bypassed `safe_identifier`'s 63-char truncation+hash
--       (every other call site routes through it). Two symptoms:
--
--         * Bare-name IMVs (no `schema.` prefix): the second `split_part`
--           returned the empty string, so the constructed
--           `"foo"."__reflex_intermediate_"` was not a real relation. The
--           outer EXCEPTION block caught the error and degraded Path C to
--           a silent fall-through into MERGE — correctness preserved by
--           accident, but the optimisation was disabled for every IMV in
--           the default schema.
--
--         * Long IMV names: when `__reflex_intermediate_<bare>` exceeded
--           PG's 63-char NAMEDATALEN, the real relation got the 8-hex
--           `safe()` hash suffix while the Path C concat did not. The
--           constructed name (un-hashed) referenced a relation that did
--           not exist, surfacing during UPDATE as
--           `ERROR: relation "<...>" does not exist`. The outer
--           EXCEPTION caught the error so the UPDATE itself succeeded
--           (with Path C disabled for that IMV), but operators saw the
--           accompanying `WARNING: pg_reflex Path C smart bulk-INSERT
--           failed for <imv>: relation "<...>" does not exist`.
--
--   (2) Path C's bulk-INSERT skips MERGE on the assumption that the
--       affected slice of intermediate group keys cannot already be
--       populated. That assumption only holds when the source's identity
--       uniquely determines its slice of keys — i.e., when the analyser
--       captured a `source_join_keys` entry for the trigger source. For
--       single-source aggregates one source row can feed many group
--       keys, and other rows (filter-passed before this UPDATE) may
--       already be contributing to those groups; bulk-INSERT then
--       duplicates rows in the intermediate / target and silently
--       wrong-answers. Pre-1.7.1 this was unreachable for bare-name
--       IMVs (the concat bug aborted Path C before any DML), but
--       schema-qualified single-source aggregates with long-enough
--       intermediates passed the `_pc_ratio >= _pc_thr` gate and hit
--       the duplicate-rows path.
--
-- What changed in the recompiled module:
--
--   * Three new SQL-callable helpers expose the canonical Rust name
--     builders that every other call site uses:
--       reflex_intermediate_table_name(name TEXT) RETURNS TEXT
--       reflex_delta_scratch_table_name(name TEXT) RETURNS TEXT
--       reflex_quote_identifier(name TEXT) RETURNS TEXT
--     They route through `split_qualified_name` + `safe_identifier` so
--     bare names and long names are handled uniformly.
--
--   * `sql/trigger_path_c_block.plpgsql.in` calls these instead of
--     `split_part` + concat. The unique-index lookup is rewritten to
--     join via `to_regclass(_pc_int_tbl)::regclass` and `pg_index.indisunique`
--     (the previous `pg_indexes.indexdef ILIKE '%UNIQUE%'` form
--     false-positives on comments / column names).
--
--   * `reflex_build_path_c_explain_sql` now returns the empty string when
--     the plan has no `source_join_keys` entry for the trigger source
--     — matching the Rust-side `aggregate_insert_stmts` gate. The trigger
--     body's `IF _pc_sql <> ''` check turns that into a Path C skip →
--     standard MERGE path.
--
-- Migration steps performed by this script:
--
--   (1) Register the three new SQL-callable helpers.
--   (2) Rebuild every IMV's source triggers via `reflex_rebuild_triggers`
--       so the new Path C body lands in every existing trigger function
--       without operator intervention.

CREATE OR REPLACE FUNCTION "reflex_intermediate_table_name"(
    "view_name" TEXT
) RETURNS TEXT
STRICT PARALLEL SAFE IMMUTABLE
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_intermediate_table_name_wrapper';

CREATE OR REPLACE FUNCTION "reflex_delta_scratch_table_name"(
    "view_name" TEXT
) RETURNS TEXT
STRICT PARALLEL SAFE IMMUTABLE
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_delta_scratch_table_name_wrapper';

CREATE OR REPLACE FUNCTION "reflex_quote_identifier"(
    "name" TEXT
) RETURNS TEXT
STRICT PARALLEL SAFE IMMUTABLE
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_quote_identifier_wrapper';

-- Rebuild the immediate / deferred trigger functions for every distinct
-- source referenced by an enabled IMV.  The trigger body templates are
-- baked into `reflex_rebuild_triggers` at extension build time, so
-- re-running it picks up the new Path C body.  Errors from individual
-- sources (e.g., a `depends_on` entry that no longer resolves) are
-- demoted to WARNINGs so a stale row cannot block the rest of the
-- migration; the unreached IMV's trigger keeps its 1.7.0 body and will
-- continue to fall through to MERGE (still correct, just less optimal
-- on schema-qualified joined aggregates with a populated
-- `source_join_keys`).
--
-- We deduplicate via `SELECT DISTINCT` and drop sub-IMV refs (entries
-- prefixed with `<` mark CTE / set-op decompositions that are not
-- materialised as real relations).
DO $migrate$
DECLARE
    _src TEXT;
    _msg TEXT;
    _ok INT := 0;
    _err INT := 0;
BEGIN
    FOR _src IN
        SELECT DISTINCT s
        FROM public.__reflex_ivm_reference, unnest(depends_on) AS s
        WHERE enabled = TRUE
          AND s NOT LIKE '<%'
        ORDER BY 1
    LOOP
        BEGIN
            _msg := public.reflex_rebuild_triggers(_src);
            IF _msg LIKE 'ERROR:%' THEN
                RAISE WARNING 'pg_reflex 1.7.1 migration: reflex_rebuild_triggers(%) returned: %', quote_literal(_src), _msg;
                _err := _err + 1;
            ELSE
                _ok := _ok + 1;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'pg_reflex 1.7.1 migration: reflex_rebuild_triggers(%) raised: %', quote_literal(_src), SQLERRM;
            _err := _err + 1;
        END;
    END LOOP;
    RAISE NOTICE 'pg_reflex 1.7.1 migration: rebuilt triggers for % source(s); % skipped', _ok, _err;
END
$migrate$;
