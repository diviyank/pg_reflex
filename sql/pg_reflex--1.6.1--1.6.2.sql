-- Migration: pg_reflex 1.6.1 → 1.6.2
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.6.2';
--
-- 1.6.2 fixes a class of catastrophic deferred-trigger failures.  The
-- per-source staging delta `__reflex_delta_<src>` outlived its source
-- across DDL changes (an IMV drop, a source DROP/CREATE — the latter is
-- unavoidable when partitioning an existing table on PG ≤ 17 since you
-- cannot ALTER a regular table to be partitioned).  The deferred trigger
-- body used to do `INSERT INTO staging SELECT '<op>', * FROM transition`,
-- a positional INSERT.  When the staging columns no longer aligned with
-- the source/transition column order, the trigger died with
-- `column "X" is of type Y but expression is of type Z` and every DML on
-- the source thereafter failed until the trigger was disabled.
--
-- Changes:
--
-- 1. **Named-column INSERT in deferred trigger bodies**.  The trigger
--    DDL now embeds the live source column list, so the staging INSERT
--    binds rows by column NAME rather than position.  Column ORDER drift
--    no longer breaks the trigger.
--
-- 2. **Staging shape guard at create_ivm**.  When a DEFERRED IMV is
--    created on a source whose staging exists with a different column
--    SET (renames / adds / drops), the create path drops+recreates the
--    staging if empty, or raises a clear error directing the operator to
--    `reflex_flush_deferred()` first if pending deferred rows would be
--    lost.
--
-- 3. **`reflex_rebuild_triggers` is now deferred-aware**.  It picks the
--    deferred or immediate trigger body based on whether any enabled IMV
--    depending on the source is DEFERRED; pre-1.6.2 it always emitted
--    the immediate body, silently breaking deferred IMVs whose triggers
--    had been rebuilt via this entry point.
--
-- Migration steps:
--
-- A. For each source with at least one enabled DEFERRED IMV: validate
--    `__reflex_delta_<src>`'s column NAME set against the source's
--    current shape.  If they differ, drop the staging (CASCADE — the
--    per-session TEMP VIEWs `__reflex_new_<src>` / `__reflex_old_<src>`
--    created by a prior `reflex_flush_deferred` call could otherwise
--    block it).  The new trigger DDL emitted in step B will recreate the
--    staging from the current source shape on its first INSERT path
--    only if needed; we recreate it explicitly below to keep the
--    invariant that the staging table is in sync after the migration.
--
--    Non-empty stale staging is also dropped — pre-drift rows reference
--    an older column layout and cannot be replayed safely against the
--    current IMV.  Operators with critical pending state should flush
--    BEFORE running this migration.  A NOTICE is emitted in that case.
--
-- B. Re-emit the trigger function bodies for every source (now
--    deferred-aware), so they pick up the named-column INSERT codegen.

-- Step A — staging shape validation and recreate for DEFERRED sources.
--
-- Staging naming follows `query_decomposer::staging_delta_table_name`:
--
--   * `schema.tbl`  → `schema.__reflex_delta_schema_tbl`
--   * `tbl` (bare)  → `__reflex_delta_tbl` in the source's resolved schema
--
-- The Rust builder additionally truncates names > 63 chars with a hash
-- suffix (`safe_identifier`).  Sources whose staging name would truncate
-- are an edge case (sanitized suffix > 48 chars); the migration skips
-- those with a NOTICE rather than risk matching the wrong table.
DO $$
DECLARE
    src                 TEXT;
    src_schema          TEXT;
    src_local           TEXT;
    staging_schema      TEXT;
    staging_local       TEXT;
    staging_qual        TEXT;
    source_oid          OID;
    staging_oid         OID;
    source_col_set      TEXT[];
    staging_col_set     TEXT[];
    staging_row_count   BIGINT;
BEGIN
    FOR src IN
        SELECT DISTINCT depended
        FROM (
            SELECT unnest(depends_on) AS depended
            FROM public.__reflex_ivm_reference
            WHERE enabled = TRUE
              AND COALESCE(refresh_mode, 'IMMEDIATE') = 'DEFERRED'
        ) d
        WHERE depended IS NOT NULL AND depended NOT LIKE '<%'
    LOOP
        -- Resolve the source's regclass; skip if it no longer exists.
        BEGIN
            source_oid := src::regclass;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pg_reflex 1.6.2 migration: source ''%'' not found — skipping staging fix', src;
            CONTINUE;
        END;
        SELECT n.nspname, c.relname INTO src_schema, src_local
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = source_oid;

        -- Compute the expected staging name.  Mirrors the Rust builder:
        -- qualified → "<src_schema>"."__reflex_delta_<src_schema>_<src_local>",
        -- bare      → "__reflex_delta_<src_local>" (no schema prefix).
        IF position('.' IN src) > 0 THEN
            staging_schema := src_schema;
            staging_local  := '__reflex_delta_' || src_schema || '_' || src_local;
        ELSE
            staging_schema := src_schema;
            staging_local  := '__reflex_delta_' || src_local;
        END IF;

        IF length(staging_local) > 63 THEN
            RAISE NOTICE 'pg_reflex 1.6.2 migration: staging name for ''%'' would truncate (>63 chars) — skipping. Drop+recreate %.% manually if drift suspected.',
                src, staging_schema, staging_local;
            CONTINUE;
        END IF;

        SELECT c.oid INTO staging_oid
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = staging_schema AND c.relname = staging_local;
        IF staging_oid IS NULL THEN
            -- No staging yet; create_ivm will materialize one on next run.
            CONTINUE;
        END IF;

        SELECT array_agg(attname ORDER BY attname) INTO source_col_set
        FROM pg_attribute
        WHERE attrelid = source_oid AND attnum > 0 AND NOT attisdropped;
        SELECT array_agg(attname ORDER BY attname) INTO staging_col_set
        FROM pg_attribute
        WHERE attrelid = staging_oid AND attnum > 0 AND NOT attisdropped
          AND attname <> '__reflex_op';

        IF source_col_set IS DISTINCT FROM staging_col_set THEN
            staging_qual := quote_ident(staging_schema) || '.' || quote_ident(staging_local);
            EXECUTE format('SELECT count(*) FROM %s', staging_qual) INTO staging_row_count;
            IF staging_row_count > 0 THEN
                RAISE NOTICE 'pg_reflex 1.6.2 migration: dropping stale staging delta % with % pending row(s) — they reference an older source column layout and cannot be replayed safely. Flush BEFORE upgrading next time.',
                    staging_qual, staging_row_count;
            END IF;
            EXECUTE format('DROP TABLE %s CASCADE', staging_qual);
            EXECUTE format(
                'CREATE UNLOGGED TABLE %s (__reflex_op TEXT NOT NULL, LIKE %I.%I INCLUDING DEFAULTS)',
                staging_qual, src_schema, src_local
            );
            RAISE NOTICE 'pg_reflex 1.6.2 migration: recreated staging delta % to match current shape of %.%',
                staging_qual, src_schema, src_local;
        END IF;
    END LOOP;
END $$;

-- Step B — rebuild all trigger functions (now deferred-aware) to pick up
-- the named-column INSERT codegen.
DO $$
DECLARE
    src TEXT;
    res TEXT;
BEGIN
    FOR src IN
        SELECT DISTINCT depended
        FROM (
            SELECT unnest(depends_on) AS depended
            FROM public.__reflex_ivm_reference
            WHERE enabled = TRUE
        ) d
        WHERE depended IS NOT NULL AND depended NOT LIKE '<%'
    LOOP
        BEGIN
            res := public.reflex_rebuild_triggers(src);
            IF res LIKE 'ERROR:%' THEN
                RAISE NOTICE 'pg_reflex 1.6.2 migration: %', res;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Don't abort the migration if one source's trigger rebuild
            -- fails (e.g. the source table was dropped, leaving an
            -- orphaned IMV reference row).  Log and continue.
            RAISE NOTICE 'pg_reflex 1.6.2 migration: could not rebuild triggers for %: %', src, SQLERRM;
        END;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 1.6.2 (audit): reflex_audit() operator entry points.
--
-- Two overloads, both backed by pgrx-generated C wrappers in
-- src/lib.rs (#[pg_extern(name = "reflex_audit")] on reflex_audit_all
-- and reflex_audit_one).  The pgrx-generated install file for fresh
-- CREATE EXTENSION installs already declares these; this block adds
-- them on top of an existing 1.6.1 install.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.reflex_audit()
RETURNS TEXT
AS 'MODULE_PATHNAME', 'reflex_audit_all_wrapper'
LANGUAGE c STRICT;

CREATE OR REPLACE FUNCTION public.reflex_audit(view_name TEXT)
RETURNS TEXT
AS 'MODULE_PATHNAME', 'reflex_audit_one_wrapper'
LANGUAGE c STRICT;

COMMENT ON FUNCTION public.reflex_audit() IS
  'Audit pg_reflex invariants against the live catalog. Returns a multi-line text report with severity-tagged findings and suggested fixes. Read-only, safe during DML.';

COMMENT ON FUNCTION public.reflex_audit(TEXT) IS
  'Scoped reflex_audit() — checks only the named IMV. Skips orphan-artifact checks. Raises ERROR if the IMV is not registered.';
