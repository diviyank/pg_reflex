-- Migration: pg_reflex 1.8.0 → 1.8.1
--
-- Multi-level (sub-partition) source support (plans/sub_partitioning.md).
-- An IMV whose source is partitioned more than one level deep (e.g.
-- LIST(dem_plan_id) → RANGE(order_date)) now mirrors the full source partition
-- hierarchy and reconciles at any level. Partition DETACH/ATTACH swaps — which
-- fire no DML trigger — are captured by the ddl_command_end event trigger
-- (enqueue) and applied by the new reflex_flush_partitions() (snapshot oid-diff).
--
-- This script:
--   1. Adds the two capture catalog tables.
--   2. Adds the flush C functions.
--   3. Replaces reflex_reconcile_partition with the 3-arg (source_partition) form.
--   4. Replaces the ddl_command_end event-trigger function with the enqueue body.
--   5. Seeds the partition snapshot for existing partitioned IMVs so the first
--      post-upgrade swap is incremental (not a one-time full rebuild).
--
-- Idempotent and safe to re-run. Replace the .so before running
-- `ALTER EXTENSION pg_reflex UPDATE TO '1.8.1';`.

-- 1. Capture catalog tables ------------------------------------------------

-- Snapshot of each tracked source root's recursive LEAF set, keyed by
-- (source_root, child). reflex_flush_partitions oid-diffs the live leaf set
-- against this to classify attach (new) / swap (oid changed) / detach (gone).
CREATE TABLE IF NOT EXISTS public.__reflex_source_partition_snapshot (
    source_root TEXT   NOT NULL,
    child_name  TEXT   NOT NULL,
    child_oid   BIGINT NOT NULL,
    bound       TEXT,
    PRIMARY KEY (source_root, child_name)
);

-- Roots enqueued by the DDL event trigger when a source partition is
-- attached/detached; drained by reflex_flush_partitions.
CREATE TABLE IF NOT EXISTS public.__reflex_partition_pending (
    source_root TEXT NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_root)
);

-- 2. Flush C functions -----------------------------------------------------

CREATE OR REPLACE FUNCTION public.reflex_flush_partitions()
    RETURNS TEXT
    STRICT
    LANGUAGE c
    AS 'MODULE_PATHNAME', 'reflex_flush_partitions_wrapper';

CREATE OR REPLACE FUNCTION public.reflex_flush_partition_source(source_root TEXT)
    RETURNS TEXT
    STRICT
    LANGUAGE c
    AS 'MODULE_PATHNAME', 'reflex_flush_partition_source_wrapper';

-- 3. reflex_reconcile_partition gains a third `source_partition` argument ---
--    The 2-arg form (1.6.0) and the 3-arg form are distinct SQL signatures, so
--    CREATE OR REPLACE cannot upgrade in place — drop the old 2-arg function and
--    create the 3-arg one (the new wrapper symbol reads three args; leaving the
--    old 2-arg declaration would invoke the wrapper with the wrong arity). The
--    DEFAULT '' keeps existing 2-arg call sites working unchanged.
DROP FUNCTION IF EXISTS public.reflex_reconcile_partition(TEXT, TEXT);

CREATE OR REPLACE FUNCTION public.reflex_reconcile_partition(
    view_name TEXT,
    partition_keys TEXT,
    source_partition TEXT DEFAULT ''
)
    RETURNS TEXT
    STRICT
    LANGUAGE c
    AS 'MODULE_PATHNAME', 'reflex_reconcile_partition_wrapper';

-- 4. Event-trigger function: enqueue the source partition root on ATTACH/DETACH
--    (the CREATE EVENT TRIGGER itself is unchanged and stays bound to this fn).
CREATE OR REPLACE FUNCTION public.__reflex_on_ddl_command_end()
RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    _cmd RECORD;
    _imv RECORD;
    _src TEXT;
    _parent TEXT;
    _part_root TEXT;
    _policy TEXT;
    _affected TEXT[] := ARRAY[]::TEXT[];
    _synced_keys TEXT[] := ARRAY[]::TEXT[];
    _sync_key TEXT;
BEGIN
    _policy := lower(COALESCE(NULLIF(current_setting('pg_reflex.alter_source_policy', true), ''), 'warn'));
    IF _policy NOT IN ('warn', 'error') THEN
        RAISE WARNING 'pg_reflex: invalid pg_reflex.alter_source_policy=%, falling back to ''warn''', _policy;
        _policy := 'warn';
    END IF;

    FOR _cmd IN
        SELECT object_identity, object_type, command_tag
        FROM pg_event_trigger_ddl_commands()
        WHERE command_tag IN ('ALTER TABLE', 'CREATE TABLE')
    LOOP
        _parent := NULL;
        IF _cmd.command_tag = 'ALTER TABLE' THEN
            _parent := _cmd.object_identity;
        ELSIF _cmd.command_tag = 'CREATE TABLE' THEN
            BEGIN
                SELECT n.nspname || '.' || c.relname INTO _parent
                FROM pg_inherits i
                JOIN pg_class c   ON c.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.inhrelid = _cmd.object_identity::regclass;
            EXCEPTION WHEN OTHERS THEN
                _parent := NULL;
            END;
        END IF;

        IF _parent IS NOT NULL THEN
            -- Capture for flush: resolve the partition ROOT (a multi-level
            -- attach reports an intermediate level as _parent, but IMVs depend
            -- on the top-level source) and enqueue it, unless it is
            -- pg_reflex-owned (our own atomic swap ATTACH/DETACHes IMV
            -- partitions; reacting to those would race the code-driven cascade).
            BEGIN
                SELECT n.nspname || '.' || c.relname
                  INTO _part_root
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.oid = pg_partition_root(_parent::regclass);
            EXCEPTION WHEN OTHERS THEN
                _part_root := NULL;
            END;

            IF _part_root IS NOT NULL
               AND _part_root NOT LIKE '%\_\_reflex\_%'
               AND NOT EXISTS (
                   SELECT 1 FROM public.__reflex_ivm_reference r
                   WHERE r.name = _part_root OR r.name = split_part(_part_root, '.', 2)
               )
               AND EXISTS (
                   SELECT 1 FROM public.__reflex_ivm_reference r
                   WHERE r.partition_columns IS NOT NULL
                     AND array_length(r.partition_columns, 1) > 0
                     AND (r.depends_on @> ARRAY[_part_root]
                          OR r.depends_on @> ARRAY[split_part(_part_root, '.', 2)])
               )
            THEN
                INSERT INTO public.__reflex_partition_pending (source_root)
                VALUES (_part_root)
                ON CONFLICT (source_root) DO NOTHING;
            END IF;

            FOR _imv IN
                SELECT name FROM public.__reflex_ivm_reference
                WHERE partition_columns IS NOT NULL
                  AND array_length(partition_columns, 1) > 0
                  AND (depends_on @> ARRAY[_parent]
                       OR depends_on @> ARRAY[split_part(_parent, '.', 2)])
            LOOP
                _sync_key := _parent || '|' || _imv.name;
                IF _sync_key = ANY(_synced_keys) THEN
                    CONTINUE;
                END IF;
                _synced_keys := _synced_keys || _sync_key;
                BEGIN
                    PERFORM public.reflex_sync_partitions(_imv.name, FALSE);
                    RAISE NOTICE 'pg_reflex: auto-synced partitions for IMV % (source %)',
                        _imv.name, _parent;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'pg_reflex: auto-sync of IMV % failed after source % partition change: % — run SELECT reflex_sync_partitions(''%'') manually',
                        _imv.name, _parent, SQLERRM, _imv.name;
                END;
            END LOOP;
        END IF;
    END LOOP;

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

-- 5. Seed the snapshot for existing partitioned IMVs ----------------------
--    Without a baseline, the first post-upgrade swap would classify every leaf
--    as attach-new and rebuild the whole source root once. Seed the canonical
--    (schema-qualified) root key with its recursive leaf set + oids now, so the
--    first flush is incremental. Per-IMV exception isolation; an IMV whose
--    anchor cannot be resolved is left unseeded (it self-heals on first flush).
DO $seed$
DECLARE
    imv      RECORD;
    root_oid OID;
    root_key TEXT;
BEGIN
    FOR imv IN
        SELECT name, depends_on, partition_columns
        FROM public.__reflex_ivm_reference
        WHERE partition_columns IS NOT NULL
          AND array_length(partition_columns, 1) > 0
    LOOP
        BEGIN
            -- Anchor = a source in depends_on that is a partitioned table
            -- partitioned on the IMV's first partition column.
            SELECT c.oid INTO root_oid
            FROM unnest(imv.depends_on) AS src
            JOIN pg_class c ON c.oid = to_regclass(src)
            JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
            WHERE EXISTS (
                SELECT 1
                FROM unnest(string_to_array(pt.partattrs::text, ' ')::int[]) AS k(attnum)
                JOIN pg_attribute a ON a.attrelid = pt.partrelid
                                   AND a.attnum = k.attnum::smallint
                WHERE lower(a.attname) = lower(imv.partition_columns[1])
            )
            LIMIT 1;

            IF root_oid IS NULL THEN
                CONTINUE;
            END IF;

            SELECT n.nspname || '.' || c.relname INTO root_key
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.oid = root_oid;

            DELETE FROM public.__reflex_source_partition_snapshot WHERE source_root = root_key;

            INSERT INTO public.__reflex_source_partition_snapshot
                (source_root, child_name, child_oid, bound)
            WITH RECURSIVE tree AS (
                SELECT inhrelid AS coid
                FROM pg_inherits WHERE inhparent = root_oid
              UNION ALL
                SELECT i.inhrelid
                FROM pg_inherits i JOIN tree t ON i.inhparent = t.coid
            )
            SELECT root_key, c.relname, c.oid::bigint, NULL
            FROM tree t JOIN pg_class c ON c.oid = t.coid
            WHERE NOT EXISTS (
                SELECT 1 FROM pg_partitioned_table pt WHERE pt.partrelid = c.oid
            );  -- leaves only
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'pg_reflex 1.8.1: snapshot seed for IMV % skipped: %', imv.name, SQLERRM;
        END;
    END LOOP;
END
$seed$;

DO $note$
BEGIN
    RAISE NOTICE 'pg_reflex 1.8.1: multi-level (sub-partition) source support installed. Capture swaps with SELECT reflex_flush_partitions() after DETACH/ATTACH.';
END
$note$;
