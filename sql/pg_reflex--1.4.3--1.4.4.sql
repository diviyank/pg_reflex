-- Migration: pg_reflex 1.4.3 → 1.4.4
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.4.4';
--
-- Bug fix + performance release. Catalog rewrite: rebuilds the composite
-- index on each intermediate table as `UNIQUE … NULLS NOT DISTINCT`.
--
-- Fixed: IMMEDIATE-mode `MERGE INTO __reflex_intermediate_<view>` hung for
-- 20+ minutes on customer dev (reproduced on a 352 MB / 867 K-row intermediate
-- with 8 group columns). Two issues compounded:
--
--   1. `build_merge_using` emitted `t.col IS NOT DISTINCT FROM d.col` for
--      every group column unconditionally. `IS NOT DISTINCT FROM` is not
--      index-usable on a plain btree, so the planner fell back to hash join
--      or nested-loop-no-index — both seq-scan the intermediate per scratch
--      row. With a moderately-large scratch (the customer's JOIN against
--      `sales_simulation` aggregated to tens of thousands of distinct group
--      tuples), this turned a millisecond MERGE into a minutes-long hang.
--
--      Fix: `build_merge_using` now reads `pg_attribute.attnotnull` for each
--      group column of the intermediate and emits `t.col = d.col` for
--      NOT NULL columns (index-usable) while keeping
--      `t.col IS NOT DISTINCT FROM d.col` for NULLable ones (semantically
--      required). The lookup is cached per backend.
--
--   2. The existing composite index on intermediate's group columns was
--      non-UNIQUE. It worked correctly but the MERGE planner gives stronger
--      preference to a UNIQUE index when matching one row per probe. Worse,
--      it provided no defensive enforcement of the
--      one-row-per-group invariant the MERGE codegen has always relied on.
--
--      Fix: new IMVs emit `CREATE UNIQUE INDEX … NULLS NOT DISTINCT` (PG 15+)
--      for multi-column groups. Single-column groups stay non-unique-hash
--      (hash indexes don't support uniqueness).
--
-- Migration:
--
-- This script iterates every existing intermediate table with a multi-column
-- composite group index and rebuilds the index as UNIQUE NULLS NOT DISTINCT.
-- If the existing intermediate has duplicate rows for some group key (which
-- should not happen but could from a prior MERGE bug), the unique build
-- fails with `unique_violation` and we fall back to recreating the non-unique
-- index — the migration emits a WARNING listing the affected IMV so the
-- operator can decide whether to drop and recreate it.
--
-- Backends connected before the upgrade will continue to serve cached MERGE
-- SQL with the old `IS NOT DISTINCT FROM` clauses. Reconnect to pick up the
-- fix.

-- ----------------------------------------------------------------------
-- Part 1: backfill `not_null_columns` in stored aggregations JSON.
--
-- Pre-1.4.4 the MERGE codegen always emitted `IS NOT DISTINCT FROM` for
-- every group column, so the field went unused and was only set
-- opportunistically (when the SUM companion-column rewrite happened to
-- fire). Now that `build_merge_using` reads it to choose
-- `=` vs `IS NOT DISTINCT FROM`, every existing IMV needs the catalog's
-- NOT NULL columns recorded — otherwise the fix is a no-op on already-
-- deployed IMVs.
--
-- For each IMV, union the NOT NULL bare column names across every
-- `depends_on` source table (information_schema.columns is search-path
-- independent and treats `bench_imm.ss` / unqualified `orders` uniformly).
-- ----------------------------------------------------------------------
DO $REFLEX_MIG_144_PART1$
DECLARE
    rec        RECORD;
    nn_set     JSONB;
BEGIN
    FOR rec IN
        SELECT name, depends_on, aggregations
        FROM public.__reflex_ivm_reference
    LOOP
        SELECT COALESCE(jsonb_agg(DISTINCT col.column_name), '[]'::jsonb)
        INTO nn_set
        FROM unnest(rec.depends_on) AS dep,
        LATERAL (
            SELECT c.column_name
            FROM information_schema.columns c
            WHERE c.is_nullable = 'NO'
              AND (
                   /* schema-qualified `schema.table` form */
                   (position('.' in dep) > 0
                    AND c.table_schema = split_part(dep, '.', 1)
                    AND c.table_name   = split_part(dep, '.', 2))
                OR /* unqualified — match across all schemas */
                   (position('.' in dep) = 0
                    AND c.table_name = dep)
              )
        ) AS col
        WHERE NOT (dep LIKE '<%>' OR dep LIKE '"%"'); -- skip sub-IMV CTE refs

        UPDATE public.__reflex_ivm_reference
        SET aggregations = jsonb_set(
            aggregations::jsonb,
            '{not_null_columns}',
            nn_set
        )
        WHERE name = rec.name;

        RAISE NOTICE 'pg_reflex 1.4.4: backfilled not_null_columns on % (% cols)',
            rec.name, jsonb_array_length(nn_set);
    END LOOP;
END
$REFLEX_MIG_144_PART1$;

-- ----------------------------------------------------------------------
-- Part 2: rebuild every multi-column intermediate composite index as
-- UNIQUE NULLS NOT DISTINCT. Single-column indexes (hash) are left alone.
-- ----------------------------------------------------------------------
DO $REFLEX_MIG_144$
DECLARE
    rec      RECORD;
    new_ddl  TEXT;
    fb_ddl   TEXT;
    drop_ddl TEXT;
    t0       TIMESTAMPTZ;
    elapsed  BIGINT;
BEGIN
    FOR rec IN
        SELECT
            cl.relname  AS index_name,
            n.nspname   AS schema_name,
            t.relname   AS table_name,
            (
                SELECT string_agg(
                           quote_ident(a.attname),
                           ', '
                           ORDER BY u.ord
                       )
                FROM unnest(ix.indkey::int[]) WITH ORDINALITY u(attnum, ord)
                JOIN pg_attribute a
                  ON a.attrelid = t.oid AND a.attnum = u.attnum
            )           AS cols_csv,
            ix.indisunique AS is_unique
        FROM pg_index ix
        JOIN pg_class cl ON cl.oid = ix.indexrelid
        JOIN pg_class t  ON t.oid  = ix.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE cl.relname LIKE 'idx\_\_reflex\_int\_%' ESCAPE '\'
          AND array_length(ix.indkey::int[], 1) > 1
          AND NOT ix.indisunique
    LOOP
        new_ddl  := format(
            'CREATE UNIQUE INDEX %I ON %I.%I (%s) NULLS NOT DISTINCT',
            rec.index_name, rec.schema_name, rec.table_name, rec.cols_csv
        );
        fb_ddl   := format(
            'CREATE INDEX %I ON %I.%I (%s)',
            rec.index_name, rec.schema_name, rec.table_name, rec.cols_csv
        );
        drop_ddl := format(
            'DROP INDEX %I.%I',
            rec.schema_name, rec.index_name
        );

        t0 := clock_timestamp();
        EXECUTE drop_ddl;
        BEGIN
            EXECUTE new_ddl;
            elapsed := (EXTRACT(EPOCH FROM (clock_timestamp() - t0)) * 1000)::BIGINT;
            RAISE NOTICE 'pg_reflex 1.4.4: rebuilt % on %.% as UNIQUE NULLS NOT DISTINCT (% ms)',
                rec.index_name, rec.schema_name, rec.table_name, elapsed;
        EXCEPTION
            WHEN unique_violation THEN
                EXECUTE fb_ddl;
                RAISE WARNING
                    'pg_reflex 1.4.4: % on %.% has duplicate group keys — kept non-unique. '
                    'Drop and recreate the IMV (or de-duplicate the intermediate manually) '
                    'before this constraint can be enforced.',
                    rec.index_name, rec.schema_name, rec.table_name;
            WHEN OTHERS THEN
                EXECUTE fb_ddl;
                RAISE WARNING
                    'pg_reflex 1.4.4: index build failed on %.% (%) — restored non-unique form',
                    rec.schema_name, rec.table_name, SQLERRM;
        END;
    END LOOP;
END
$REFLEX_MIG_144$;

-- ----------------------------------------------------------------------
-- Part 3: drop per-column intermediate indexes (idx__reflex_<view>_<n>)
-- that pre-1.4.4 emitted alongside the composite. They were never used
-- by any pg_reflex query path (every code path probes the full
-- composite key), but added ~480ms of index maintenance per 47K-row
-- UPDATE cycle on rb.fcast (perftest bench A→B: 691ms → 208ms).
--
-- Pattern match: `idx__reflex_<bare_view>_<digit>` on a table named
-- `__reflex_intermediate_*`. The composite index is named
-- `idx__reflex_int_<bare_view>` — explicitly excluded by the
-- "_<digit>$" suffix filter.
-- ----------------------------------------------------------------------
DO $REFLEX_MIG_144_PART3$
DECLARE
    rec     RECORD;
    dropped INTEGER := 0;
BEGIN
    FOR rec IN
        SELECT n.nspname AS schema_name,
               cl.relname AS index_name,
               t.relname AS table_name
        FROM pg_index ix
        JOIN pg_class cl ON cl.oid = ix.indexrelid
        JOIN pg_class t  ON t.oid  = ix.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE cl.relname LIKE 'idx\_\_reflex\_%' ESCAPE '\'
          AND cl.relname ~ '_[0-9]+$'
          AND t.relname LIKE '\_\_reflex\_intermediate\_%' ESCAPE '\'
          AND array_length(ix.indkey::int[], 1) = 1
    LOOP
        EXECUTE format('DROP INDEX %I.%I', rec.schema_name, rec.index_name);
        dropped := dropped + 1;
        RAISE NOTICE 'pg_reflex 1.4.4: dropped vestigial per-column index %.%',
            rec.schema_name, rec.index_name;
    END LOOP;
    IF dropped > 0 THEN
        RAISE NOTICE 'pg_reflex 1.4.4: dropped % vestigial per-column intermediate indexes', dropped;
    END IF;
END
$REFLEX_MIG_144_PART3$;

-- ----------------------------------------------------------------------
-- Part 4: set fillfactor=70 on intermediate and target tables so MERGE
-- WHEN MATCHED UPDATE (touching only non-indexed aggregate columns) can
-- HOT-update in place. Bench (perftest C vs B, 47K rows): 208ms → 75ms
-- on intermediate, 169ms → 64ms on target. Verified via pg_stat_user_tables
-- n_tup_hot_upd: 100% of WHEN MATCHED UPDATEs become HOT.
--
-- ALTER TABLE … SET (fillfactor) is a catalog-only change. Existing pages
-- are NOT rewritten — disk size stays the same immediately after the
-- migration. New rows and HOT updates will gradually bring pages to the
-- new fillfactor. Operators wanting an immediate rewrite can VACUUM FULL
-- the IMV's intermediate and target tables during a maintenance window.
-- ----------------------------------------------------------------------
DO $REFLEX_MIG_144_PART4$
DECLARE
    rec      RECORD;
    altered  INTEGER := 0;
BEGIN
    FOR rec IN
        SELECT view_schema,
               imv_schema,
               imv_name,
               view_schema || '.' || quote_ident(view_name)        AS target_qualified,
               imv_schema  || '.' || quote_ident(imv_name)         AS intermediate_qualified
        FROM (
            SELECT
                COALESCE(NULLIF(split_part(r.name, '.', 2), ''), r.name)::text AS view_name,
                COALESCE(NULLIF(split_part(r.name, '.', 1), ''), 'public')::text AS view_schema,
                n.nspname::text AS imv_schema,
                cl.relname::text AS imv_name
            FROM public.__reflex_ivm_reference r
            JOIN pg_class cl ON cl.relname = '__reflex_intermediate_'
                || COALESCE(NULLIF(split_part(r.name, '.', 2), ''), r.name)
            JOIN pg_namespace n ON n.oid = cl.relnamespace
            WHERE n.nspname = COALESCE(NULLIF(split_part(r.name, '.', 1), ''), 'public')
        ) sub
    LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %s SET (fillfactor = 70)', rec.intermediate_qualified);
            EXECUTE format('ALTER TABLE %s SET (fillfactor = 70)', rec.target_qualified);
            altered := altered + 1;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'pg_reflex 1.4.4: failed to set fillfactor on % / % (%)',
                    rec.intermediate_qualified, rec.target_qualified, SQLERRM;
        END;
    END LOOP;
    IF altered > 0 THEN
        RAISE NOTICE 'pg_reflex 1.4.4: set fillfactor=70 on % intermediate+target table pairs', altered;
    END IF;
END
$REFLEX_MIG_144_PART4$;
