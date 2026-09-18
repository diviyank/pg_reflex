-- Migration: pg_reflex 1.11.3 → 1.11.4
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.11.4';
--
-- Silent-wipe observability and prevention, after a field incident where a
-- 1.86 M-row forecast IMV showed 2 161 rows while `reflex_ivm_status()` reported
-- `known_stale = false`, `last_error = NULL` and the pre-wipe row count.
--
--   1. A caught deferred-flush failure now marks the IMV `known_stale` with a
--      `stale_reason` naming the failure and the repair, and `last_error`
--      survives later successful flushes while the IMV is stale. Rust-side.
--
--   2. `reflex_ivm_status()` counts an anomalous IMV exactly, reports
--      `is_estimate`, and reports every IMV whose partition source root is at
--      the flush failure cap — directly or through other IMVs — as stale. A
--      capped root is skipped by every flush, and before this release a full
--      reconcile cleared `known_stale` while the root stayed capped.
--
--   3. New durable maintenance table `__reflex_event_log` (caught flush
--      failures and slice-changing rebuilds) and `reflex_prune_event_log`.
--
--   4. New GUC `pg_reflex.flush_failure_policy`: `warn` (default, the
--      pre-1.11.4 behaviour plus item 1) or `error` (abort the caller).
--
--   5. `create_reflex_ivm` REFUSES an `ignore_sources` entry the query depends
--      on (WHERE / HAVING / JOIN ON / JOIN USING / inner join, or an
--      unattributable query), unless acknowledged with a `'!'` prefix or
--      `reflex_ack_ignore_source(imv, source)`. `reflex_audit` and
--      `reflex_doctor` (F13) report existing unacknowledged ones.
--
--   6. Healing after an ignored source changes. When an ignored source joins
--      onto a partitioned IMV's first partition column, statement triggers on
--      it queue the affected keys into `__reflex_heal_pending`; the IMV
--      reports `known_stale` until `reflex_heal_ignored_sources`,
--      `reflex_scheduled_reconcile` or `reflex_doctor(fix => TRUE)` (F14)
--      rebuilds those partitions. The trigger function is SECURITY DEFINER,
--      so a writer needs no pg_reflex grant nor USAGE on `public`, and it
--      evaluates no user-defined code. The last statement of this file
--      installs the triggers on existing IMVs via
--      `reflex_rebuild_imv_metadata`.
--
--   7. A partitioned IMV's `row_count` estimate is the sum over its leaves,
--      and a partition swap no longer leaves `reflex_ivm_status` counting the
--      IMV exactly until the root is analyzed. Rust-side.
--
--   8. `__reflex_on_ddl_command_end` no longer reports the trigger toggle
--      `reflex_sync_partitions` wraps around a partition relocation as a
--      change to the IMV's source. It warned every dependent of a
--      partitioned IMV on each reconcile, and under
--      `alter_source_policy = 'error'` aborted it. The reconcile also no
--      longer re-issues `CREATE TABLE IF NOT EXISTS` for existing children
--      (Rust-side).
--
-- The table, column and function DDL below is the bootstrap DDL from
-- `src/lib.rs` verbatim apart from indentation, so fresh installs and upgrades
-- converge. If you edit one, edit both.
--
-- Installing the heal triggers takes a SHARE ROW EXCLUSIVE lock on each
-- mapped ignored source, blocking writes to it and waiting behind any open
-- transaction on it. Upgrade in a quiet window, with `lock_timeout` set.
--
-- Operationally, after upgrading and BEFORE recreating any IMV (including via
-- `reflex_rebuild_chain`, which replays the create; `reflex_rebuild_imv` is a
-- reconcile and is not affected):
--
--   SELECT * FROM reflex_audit() WHERE category = 'ignore-soundness';
--   SELECT reflex_ack_ignore_source('<imv>', '<source>');  -- per accepted risk
--
-- Otherwise the replay of an IMV with an unsound ignore is refused.

-- === Registry: acknowledged unsound ignores ===

ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS ignore_ack TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];

-- === Maintenance event log ===

CREATE TABLE IF NOT EXISTS public.__reflex_event_log (
    id             BIGSERIAL PRIMARY KEY,
    at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    imv_name       TEXT NOT NULL,
    event          TEXT NOT NULL,
    trigger_reason TEXT,
    slice          TEXT,
    rows_before    BIGINT,
    rows_after     BIGINT,
    detail         TEXT,
    sqlstate       TEXT
);
CREATE INDEX IF NOT EXISTS __reflex_event_log_imv_at
    ON public.__reflex_event_log (imv_name, at DESC);

ALTER TABLE public.__reflex_event_log
    ALTER COLUMN at SET DEFAULT clock_timestamp();

CREATE OR REPLACE FUNCTION public.reflex_prune_event_log(_older_than INTERVAL)
RETURNS BIGINT LANGUAGE plpgsql AS $fn$
DECLARE _n BIGINT;
BEGIN
    DELETE FROM public.__reflex_event_log WHERE at < now() - _older_than;
    GET DIAGNOSTICS _n = ROW_COUNT;
    RETURN _n;
END;
$fn$;

-- === Heal queue for changes to ignored sources ===

CREATE TABLE IF NOT EXISTS public.__reflex_heal_pending (
    imv_name      TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    source        TEXT NOT NULL,
    enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_error    TEXT,
    PRIMARY KEY (imv_name, partition_key)
);

-- Statement trigger on an ignored source (src/heal.rs). Queues the partition
-- keys of the changed rows for every enabled IMV whose `ignore_heal_keys`
-- names this relation; an UPDATE queues only keys whose watched columns
-- changed. A TRUNCATE cannot be scoped to keys, so it marks those IMVs
-- known_stale, keeping an earlier reason and stale_since.
--
-- SECURITY DEFINER so a writer needs no pg_reflex grant nor USAGE on
-- public; a trigger function cannot be called directly, so no role can
-- point it at a relation of its choosing. It evaluates no user-defined
-- code: watched columns are compared through their types' output functions
-- (a NULL flag plus `format('%s', col)`, never a cast), and a key is
-- rendered by `to_jsonb` only while its type is built in. It opens no
-- subtransaction and writes nothing when a mapped or watched column is
-- gone or the key was retyped; reflex_ivm_status derives that instead,
-- since every writer of the source would contend on the registry row.
CREATE OR REPLACE FUNCTION public.__reflex_heal_on_ignored_change()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp SET extra_float_digits = 3 AS $fn$
DECLARE
    _h RECORD;
    _key TEXT;
    _keys TEXT;
    _cols TEXT;
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

CREATE FUNCTION "reflex_heal_ignored_sources"(
	"imv" TEXT DEFAULT NULL
) RETURNS TEXT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_heal_ignored_sources_wrapper';

-- === reflex_ivm_status() gains is_estimate ===
-- A RETURNS TABLE shape change: DROP + CREATE, not CREATE OR REPLACE, matching
-- sql/pg_reflex--1.11.0--1.11.1.sql. ALTER EXTENSION UPDATE does not re-derive
-- the catalog signature from the module on its own.

DROP FUNCTION IF EXISTS public.reflex_ivm_status();
CREATE FUNCTION "reflex_ivm_status"() RETURNS TABLE (
	"name" TEXT,
	"graph_depth" INT,
	"enabled" bool,
	"refresh_mode" TEXT,
	"row_count" bigint,
	"last_flush_ms" bigint,
	"last_flush_rows" bigint,
	"flush_count" bigint,
	"last_error" TEXT,
	"last_update_date" timestamp,
	"known_stale" bool,
	"stale_reason" TEXT,
	"requires_explicit_refresh" bool,
	"rebuild_count" bigint,
	"last_rebuild_at" timestamp with time zone,
	"is_estimate" bool
)
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_ivm_status_wrapper';

-- === New: acknowledge an unsound ignore_sources entry ===

CREATE FUNCTION "reflex_ack_ignore_source"(
	"imv" TEXT,
	"source" TEXT
) RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_ack_ignore_source_wrapper';

-- === Alter-source alarm: skip pg_reflex's own relocation trigger toggle ===

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
    _reconcile_root TEXT;
    _swap_root TEXT;
    _toggle_root TEXT;
BEGIN
    -- pg_reflex's own atomic partition swap (partition.rs
    -- `execute_partition_swap_for_child`) publishes the IMV it is rebuilding
    -- here for the duration of its DETACH/ATTACH sequence. Nothing else runs
    -- inside that window, so every DDL command reaching this trigger while
    -- the GUC is set is ours, and NONE of it is a source change:
    --
    --   * the parent's child set is TRANSIENT mid-swap. A dependent that
    --     re-mirrors it adopts a `<dep>___reflex_swap_tgt_*` child and drops
    --     its real one as a bound-collision orphan; the closing RENAME then
    --     never revisits the dependent, leaving it EMPTY with a mirror of a
    --     relation that no longer exists.
    --   * the swap changes no column shape, so the alter-source alarm has
    --     nothing to report — and under `alter_source_policy = 'error'` it
    --     would abort the very reconcile that repairs the IMV.
    --
    -- Dependents are refreshed explicitly once the swap is complete
    -- (reconcile.rs `cascade_partitioned_rebuild_to_dependents`), which is
    -- what makes skipping here safe rather than merely quiet.
    _swap_root := NULLIF(current_setting('pg_reflex.internal_swap_root', true), '');
    IF _swap_root IS NOT NULL THEN
        RETURN;
    END IF;

    _policy := lower(COALESCE(NULLIF(current_setting('pg_reflex.alter_source_policy', true), ''), 'warn'));
    IF _policy NOT IN ('warn', 'error') THEN
        RAISE WARNING 'pg_reflex: invalid pg_reflex.alter_source_policy=%, falling back to ''warn''', _policy;
        _policy := 'warn';
    END IF;

    -- Root whose chain reflex_reconcile is currently rebuilding, set by that
    -- function around its DISABLE/ENABLE TRIGGER of each generated sub-IMV.
    -- Those internal ALTERs are on tracked sources (a generated child sits in
    -- its parent's depends_on), so the warn/error branch below would fire a
    -- spurious "run reflex_rebuild_imv" for a rebuild already in flight and,
    -- under 'error' policy, abort the reconcile outright. Suppressed for the
    -- nodes of the active chain only — a DIFFERENT root that reads the same
    -- node still warns, because that consumer really did miss the refresh.
    _reconcile_root := NULLIF(current_setting('pg_reflex.internal_reconcile_root', true), '');

    -- Relation whose triggers reflex_sync_partitions is toggling around a
    -- partition relocation, set for that one ALTER only
    -- (partition.rs `toggle_relocation_triggers`). The toggle changes no
    -- column and is always undone, so it is not reported as a source change.
    _toggle_root := NULLIF(current_setting('pg_reflex.internal_trigger_toggle_root', true), '');

    -- 1.6.0: auto-sync IMV partitions when a source's partition tree changes.
    --
    -- Two trigger surfaces matter:
    --   (a) ALTER TABLE parent ATTACH/DETACH PARTITION child
    --       → pg_event_trigger_ddl_commands() returns object_identity = parent,
    --         command_tag = 'ALTER TABLE'.
    --   (b) CREATE TABLE child PARTITION OF parent FOR VALUES ...
    --       → command_tag = 'CREATE TABLE'; object_identity = child;
    --         the parent must be looked up via pg_inherits.
    --
    -- For every command we resolve a candidate parent table name, then for
    -- each partitioned IMV depending on that parent we call
    -- reflex_sync_partitions(view, drop_orphans=>FALSE) — orphan deletion is
    -- never automatic (IMV data is the user's, and a DETACH on the source
    -- side is not a delete signal). reflex_sync_partitions is idempotent
    -- and advisory-lock protected, so duplicate fires inside one
    -- transaction collapse harmlessly.
    --
    -- The previous (1.5.x) warn/error contract for non-partition ALTERs
    -- (column add/drop on a tracked source) is preserved below.

    FOR _cmd IN
        SELECT object_identity, object_type, command_tag
        FROM pg_event_trigger_ddl_commands()
        WHERE command_tag IN ('ALTER TABLE', 'CREATE TABLE')
    LOOP
        -- Resolve the parent table for partition-tree changes. NULL for
        -- non-partition events (regular ALTER TABLE on a leaf table).
        _parent := NULL;
        IF _cmd.command_tag = 'ALTER TABLE' THEN
            -- ATTACH / DETACH PARTITION: object_identity is the parent.
            -- Other ALTER variants (ADD COLUMN, …) also land here with
            -- object_identity = the altered table — we sync anyway iff
            -- that table is a partitioned source of a partitioned IMV.
            _parent := _cmd.object_identity;
        ELSIF _cmd.command_tag = 'CREATE TABLE' THEN
            -- CREATE TABLE … PARTITION OF parent: look up parent via
            -- pg_inherits regardless of `object_type` (PG reports
            -- 'table' or 'table partition' depending on version).
            -- Empty result = the new table isn't a partition; _parent
            -- stays NULL and the branch below skips.
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
                   -- Enqueue for ANY enabled IMV depending on this root:
                   -- partitioned IMVs reconcile per-partition; unpartitioned
                   -- IMVs get a full reconcile (flush handles both). Without
                   -- this, an unpartitioned IMV on a swap source goes stale.
                   SELECT 1 FROM public.__reflex_ivm_reference r
                   WHERE r.enabled
                     AND (r.depends_on @> ARRAY[_part_root]
                          OR r.depends_on @> ARRAY[split_part(_part_root, '.', 2)])
               )
            THEN
                INSERT INTO public.__reflex_partition_pending (source_root)
                VALUES (_part_root)
                ON CONFLICT (source_root)
                DO UPDATE SET enqueued_at = statement_timestamp(),
                              attempts    = public.__reflex_partition_pending.attempts + 1;
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
                    UPDATE public.__reflex_ivm_reference
                       SET known_stale = TRUE, stale_reason = left(SQLERRM, 2000), stale_since = now()
                     WHERE name = _imv.name;
                    RAISE WARNING 'pg_reflex: auto-sync of IMV % failed after source % partition change: % — run SELECT reflex_sync_partitions(''%'') manually',
                        _imv.name, _parent, SQLERRM, _imv.name;
                END;
            END LOOP;
        END IF;
    END LOOP;

    -- Warn/error policy for non-partition ALTERs on tracked sources.
    -- This branch is unchanged from 1.5.x except that auto-sync above may
    -- have already healed pure partition-tree changes; the warning still
    -- fires (column shape may have changed) so the operator knows to
    -- inspect.
    FOR _cmd IN
        SELECT object_identity, command_tag
        FROM pg_event_trigger_ddl_commands()
        WHERE command_tag = 'ALTER TABLE'
    LOOP
        _src := _cmd.object_identity;
        CONTINUE WHEN _toggle_root IS NOT NULL
                  AND to_regclass(_src) = to_regclass(_toggle_root);
        FOR _imv IN
            SELECT name FROM public.__reflex_ivm_reference
            WHERE depends_on @> ARRAY[_src]
               OR depends_on @> ARRAY[split_part(_src, '.', 2)]
        LOOP
            -- Skip pg_reflex's own DISABLE/ENABLE TRIGGER on a generated
            -- sub-IMV of the chain being reconciled: the consumer named here
            -- is that same chain's root or an intermediate generated node of
            -- it, and it is about to be rebuilt. A consumer on a DIFFERENT
            -- root does not match this prefix, so its legitimate stale signal
            -- still fires.
            IF _reconcile_root IS NOT NULL
               AND ( _imv.name = _reconcile_root
                     OR split_part(_imv.name, '.', 2)
                        = split_part(_reconcile_root, '.', 2)
                     OR split_part(_imv.name, '.', 2)
                        LIKE split_part(_reconcile_root, '.', 2) || '\_\_%' )
            THEN
                CONTINUE;
            END IF;
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

-- === Install heal triggers on existing IMVs ===
-- Recomputes each partitioned IMV's metadata, which now includes
-- `ignore_heal_keys`, and installs the triggers on its mappable ignored
-- sources. Kept last: it needs the heal function and table above.

SELECT reflex_rebuild_imv_metadata(name)
  FROM public.__reflex_ivm_reference
 WHERE enabled = TRUE
   AND cardinality(COALESCE(ignored_sources, ARRAY[]::TEXT[])) > 0
   AND cardinality(COALESCE(partition_columns, ARRAY[]::TEXT[])) > 0
 ORDER BY graph_depth, name;
