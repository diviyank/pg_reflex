-- Migration: pg_reflex 1.4.4 → 1.4.5
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.4.5';
--
-- 1.4.5 ships the data-probe pass for `not_null_columns`. The pure
-- catalog heuristic introduced in 1.4.3 (and unconditionally backfilled in
-- 1.4.4's migration Part 1) reads `is_nullable` from
-- `information_schema.columns` and unions across `depends_on` source tables.
-- That heuristic is correct as far as the schema goes, but blind to query
-- semantics: a column declared NULLable can still be effectively NOT NULL on
-- the IMV's output when the base_query's INNER JOIN keys or filter
-- predicates exclude NULLs.
--
-- Customer-reported regression (yse.ivm_sop_forecast_view, 1.4.4):
--   * Catalog declares yse.sales_simulation.dem_plan_id NULLable.
--   * Base query INNER JOINs sales_simulation ON dem_plan_id =
--     demand_planning.id → join output column dem_plan_id is non-NULL.
--   * Migration's union missed it → trigger MERGE codegen emitted
--     `IS NOT DISTINCT FROM` on the composite index's leading column →
--     planner couldn't use the index → 405 s UPDATE on a 1-row source change.
--
-- The probe scans the populated intermediate per group-by column with an
-- EXISTS-IS-NULL check. NULL-free columns are added to `not_null_columns`.
-- The trigger then emits `=` (sargable) for those columns, restoring
-- composite-index range-scan probing.
--
-- This migration calls `public.reflex_probe_not_null_columns(name)` once per
-- existing aggregated IMV. The cdylib drives the probe via SPI from Rust
-- (the normalized intermediate column names live there). For new IMVs
-- created post-1.4.5, the probe runs automatically at the end of
-- `create_reflex_ivm` (right after ANALYZE).

-- New SQL-callable functions (Rust-backed via pgrx).
CREATE FUNCTION "reflex_probe_not_null_columns"(
    "view_name" TEXT
) RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_probe_not_null_columns_wrapper';

CREATE FUNCTION "reflex_compact_imv"(
    "view_name" TEXT
) RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_compact_imv_wrapper';

CREATE FUNCTION "reflex_compact_all_imv"()
RETURNS TEXT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_compact_all_imv_wrapper';

DO $REFLEX_MIG_145_PART1$
DECLARE
    rec     RECORD;
    msg     TEXT;
    t0      TIMESTAMPTZ;
    elapsed BIGINT;
BEGIN
    FOR rec IN
        SELECT name
        FROM public.__reflex_ivm_reference
        WHERE enabled = TRUE
        ORDER BY graph_depth, name
    LOOP
        t0 := clock_timestamp();
        BEGIN
            msg := public.reflex_probe_not_null_columns(rec.name);
            elapsed := (EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000)::BIGINT;
            RAISE NOTICE 'pg_reflex 1.4.5: % (% ms)', msg, elapsed;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'pg_reflex 1.4.5: probe failed on % — %',
                rec.name, SQLERRM;
        END;
    END LOOP;
END
$REFLEX_MIG_145_PART1$;

-- ----------------------------------------------------------------------
-- Part 2: fillfactor-rewrite guidance for existing IMVs.
--
-- The 1.4.3→1.4.4 migration set fillfactor=70 on intermediate and target
-- tables via ALTER TABLE … SET — a catalog-only change. Existing pages
-- stay packed (fillfactor=100), so HOT updates can't fire until pages get
-- rewritten naturally (slow) or manually via VACUUM FULL.
--
-- 1.4.5 ships `reflex_compact_imv(view_name)` to do the VACUUM FULL on
-- both the intermediate and target tables (HOLDING AccessExclusive on
-- both — schedule during a maintenance window).
--
-- This part of the migration emits a NOTICE listing every IMV that has
-- fillfactor=70 set but pages that haven't been rewritten, so operators
-- can act on it. We DO NOT auto-run VACUUM FULL here because it requires
-- maintenance-window-grade locks.
-- ----------------------------------------------------------------------
DO $REFLEX_MIG_145_PART2$
DECLARE
    n_imv INTEGER;
BEGIN
    SELECT count(*) INTO n_imv FROM public.__reflex_ivm_reference WHERE enabled = TRUE;
    IF n_imv > 0 THEN
        RAISE NOTICE 'pg_reflex 1.4.5: % enabled IMV(s) in this database. The 1.4.3→1.4.4 migration set fillfactor=70 on intermediate+target tables but did NOT rewrite existing pages, so HOT updates do not fire until pages naturally churn. To rewrite pages immediately (HOT-eligible workloads benefit ~5×), schedule a maintenance window and run public.reflex_compact_imv(name) per IMV. Lists IMVs: SELECT name FROM public.__reflex_ivm_reference WHERE enabled = TRUE;', n_imv;
    END IF;
END
$REFLEX_MIG_145_PART2$;

-- ----------------------------------------------------------------------
-- Part 3: migrate `aggregations` column from json → jsonb.
--
-- json stores the original whitespace and re-parses on every read; jsonb
-- is binary, indexable, and faster for the lookups the trigger codegen
-- does (jsonb_set, ->'key', etc). The earlier 1.4.4 customer fix workflow
-- hit a usability papercut: `jsonb_array_elements_text(json)` errors
-- because jsonb fns don't accept json. After this migration, no
-- ::jsonb casts are needed at the read site.
--
-- ALTER COLUMN TYPE rewrites the column in place. For 6-20 IMVs each
-- holding a few KB of JSON, the cost is < 100 ms. No application
-- downtime.
-- ----------------------------------------------------------------------
DO $REFLEX_MIG_145_PART3$
DECLARE
    col_type TEXT;
BEGIN
    SELECT data_type INTO col_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = '__reflex_ivm_reference'
      AND column_name = 'aggregations';
    IF col_type = 'json' THEN
        ALTER TABLE public.__reflex_ivm_reference
            ALTER COLUMN aggregations TYPE JSONB USING aggregations::JSONB;
        RAISE NOTICE 'pg_reflex 1.4.5: migrated aggregations column json → jsonb';
    ELSE
        RAISE NOTICE 'pg_reflex 1.4.5: aggregations column already %, skipping migration', col_type;
    END IF;
END
$REFLEX_MIG_145_PART3$;

-- ----------------------------------------------------------------------
-- Part 4: add `ignored_sources` column + extend create_reflex_ivm with
-- an optional `ignore_sources` parameter.
--
-- New in 1.4.5: operators can pass an `ignore_sources` parameter to
-- `create_reflex_ivm(...)` to suppress trigger installation on specific
-- sources. The list is persisted in
-- `__reflex_ivm_reference.ignored_sources`; the trigger body installed
-- by sibling IMVs reads the column and skips this IMV when fired by an
-- ignored source.
--
-- Because adding a parameter changes the function signature, we drop the
-- old 5-arg signatures and recreate the 6-arg ones. Existing calls with
-- 2-5 arguments continue to work via PostgreSQL default arguments.
-- ----------------------------------------------------------------------
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS ignored_sources TEXT[] DEFAULT ARRAY[]::TEXT[];

DROP FUNCTION IF EXISTS public.create_reflex_ivm(text, text, text, text, text);
DROP FUNCTION IF EXISTS public.create_reflex_ivm(text, text, text, text, text, int4);
DROP FUNCTION IF EXISTS public.create_reflex_ivm_if_not_exists(text, text, text, text, text);

CREATE FUNCTION "create_reflex_ivm"(
    "view_name" TEXT,
    "sql" TEXT,
    "unique_columns" TEXT DEFAULT NULL,
    "storage" TEXT DEFAULT 'UNLOGGED',
    "mode" TEXT DEFAULT 'IMMEDIATE',
    "ignore_sources" TEXT DEFAULT NULL
) RETURNS TEXT
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_reflex_ivm_wrapper';

CREATE FUNCTION "create_reflex_ivm"(
    "view_name" TEXT,
    "sql" TEXT,
    "unique_columns" TEXT,
    "storage" TEXT,
    "mode" TEXT,
    "topk" INT4,
    "ignore_sources" TEXT DEFAULT NULL
) RETURNS TEXT
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_reflex_ivm_with_topk_wrapper';

CREATE FUNCTION "create_reflex_ivm_if_not_exists"(
    "view_name" TEXT,
    "sql" TEXT,
    "unique_columns" TEXT DEFAULT NULL,
    "storage" TEXT DEFAULT 'UNLOGGED',
    "mode" TEXT DEFAULT 'IMMEDIATE',
    "ignore_sources" TEXT DEFAULT NULL
) RETURNS TEXT
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_reflex_ivm_if_not_exists_wrapper';
