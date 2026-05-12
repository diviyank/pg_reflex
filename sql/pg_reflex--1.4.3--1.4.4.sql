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
