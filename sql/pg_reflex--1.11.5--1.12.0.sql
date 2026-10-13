-- Migration: pg_reflex 1.11.5 → 1.12.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.12.0';
--
-- 1.12.0 hardens what `create_reflex_ivm` accepts, how the generated
-- maintenance SQL is assembled, and how partitioned sources are followed.
-- Most of it is in the compiled library. The installed-SQL part is five
-- functions, created or replaced below. No registry column, no table, no
-- index, no trigger, no event trigger and no `#[pg_extern]` signature
-- changes.
--
-- THE 1.12.0 LIBRARY REQUIRES THIS FILE. The partition flush of the new
-- library calls `public.__reflex_reads_relation`, which this file creates.
-- Between installing the new library and running `ALTER EXTENSION pg_reflex
-- UPDATE TO '1.12.0'`, every partition flush raises `function
-- public.__reflex_reads_relation(text[], text) does not exist`: a
-- transaction that creates, attaches or detaches a partition of a source
-- fails at COMMIT and is rolled back, and `reflex_flush_partition_source()`
-- and `reflex_flush_partitions()` (when a root is pending) raise. Run the
-- update right after replacing the library, before any partition DDL on a
-- source.
--
-- What this file does, in this order (a function is created before the
-- functions that call it):
--
--   1. `public.__reflex_on_sql_drop()` (the `sql_drop` event trigger
--      function) is replaced. A dropped table is matched to the IMVs that
--      read it:
--
--        * by its quoted identity (`schema.name` as PostgreSQL prints it),
--          as before;
--        * by its bare name only when the table is in `public`. 1.11.5
--          matched the bare name whatever the schema, so `DROP TABLE other.t`
--          dropped the IMVs of `public.t`;
--        * by its unquoted `schema.name` (and, in `public`, its unquoted bare
--          name) when that text holds no dot, double quote or upper-case
--          ASCII letter. This is how 1.12.0 registers a name that PostgreSQL
--          prints quoted but reads back bare (a non-ASCII letter, a `$`), so
--          that `DROP TABLE é_t` takes the IMVs of `é_t` with it.
--
--   2. `public.__reflex_named_relation(TEXT)` (new): the relation a
--      registered spelling names, as ARRAY[schema, name], folded the way
--      PostgreSQL folds identifiers. A spelling without a schema names a
--      relation of `public`.
--
--   3. `public.__reflex_reads_relation(TEXT[], TEXT)` (new): true when one of
--      the registered spellings names the given relation. `ta.t` is read by
--      an IMV registered with `ta.t` or `"ta"."t"`, never by one registered
--      with `t`, which is `public.t`.
--
--   4. `public.__reflex_on_ddl_command_end()` (the `ddl_command_end` event
--      trigger function) is replaced. Partition DDL on a source, and ALTER
--      TABLE of a source, now reach only the IMVs that read that relation:
--      1.11.5 also matched IMVs registered with the bare name of a namesake
--      in another schema, so partition DDL on `tb.t` queued, synced and
--      locked the IMVs of `public.t`, and the change of `tb.t` could go
--      unqueued when an IMV of `public` was named like its root.
--
--   5. `public.__reflex_partition_flush_fn()` (the function of the deferred
--      trigger on `__reflex_partition_pending`) is replaced. Every partition
--      DDL command of a transaction queues one event for its root; only the
--      event of the latest enqueue flushes, so a root is flushed once per
--      COMMIT instead of once per command.
--
--   Each body is byte-identical to the bootstrap text in `src/lib.rs`. The
--   existing event triggers and the `__reflex_partition_flush_trigger` keep
--   pointing at the replaced functions.
--
-- What this file does NOT do, and what follows operationally:
--
--   1. Per-source trigger functions are generated when an IMV is created and
--      by `reflex_rebuild_triggers('<source>')`. This file does not rewrite
--      the installed ones. The TRUNCATE trigger bodies changed in 1.12.0 (one
--      rebuild per TRUNCATE statement over a partition tree; the source is
--      marked as written for the COMMIT-time partition flush), and a
--      partitioned source now carries triggers on every table of its
--      partition tree. A source keeps its 1.11.5 functions and its 1.11.5
--      trigger set until `SELECT reflex_rebuild_triggers('<source>')` runs or
--      its IMVs are recreated.
--
--   2. The stored query, the registered source spellings (`depends_on`), the
--      aggregation plan, the column types and the unique key of an IMV are
--      computed at create time. The fixes to them (identifier folding, one
--      spelling per relation, nested aggregates, column types read from
--      PostgreSQL, key inference) reach an IMV only when it is recreated.
--
--   3. `create_reflex_ivm` refuses definitions that 1.11.5 created. An
--      existing IMV of such a definition keeps being maintained as before;
--      recreating it (drop + create, `reflex_rebuild_imv` or
--      `reflex_rebuild_chain`) is refused with a message that carries the
--      rewrite, and a refused rebuild keeps the IMV.
--
--   This release therefore expects every IMV to be dropped and recreated
--   after the update. See docs/getting-started/upgrading.md, "Upgrading to
--   1.12.0", for the steps, the refused definitions and their remedies.
--
-- Replace the library BEFORE running `ALTER EXTENSION … UPDATE`, as usual.

    CREATE OR REPLACE FUNCTION public.__reflex_on_sql_drop()
    RETURNS event_trigger LANGUAGE plpgsql AS $$
    DECLARE
        _obj RECORD;
        _imv RECORD;
    BEGIN
        FOR _obj IN
            SELECT object_identity, schema_name, object_name,
                   (schema_name || object_name) !~ '[."A-Z]' AS reads_back_unquoted
            FROM pg_event_trigger_dropped_objects()
            WHERE object_type = 'table'
        LOOP
            -- A source is registered without a schema only when it is a table of
            -- public: a table of another schema with the same name is another table.
            -- A name PostgreSQL prints quoted but reads back bare (a non-ASCII
            -- letter, a `$`) is registered bare.
            FOR _imv IN
                SELECT name
                FROM public.__reflex_ivm_reference
                WHERE depends_on @> ARRAY[_obj.object_identity]
                   OR (_obj.schema_name = 'public'
                       AND depends_on @> ARRAY[split_part(_obj.object_identity, '.', 2)])
                   OR (_obj.reads_back_unquoted
                       AND (depends_on @> ARRAY[_obj.schema_name || '.' || _obj.object_name]
                            OR (_obj.schema_name = 'public'
                                AND depends_on @> ARRAY[_obj.object_name])))
                ORDER BY graph_depth DESC, name DESC
            LOOP
                BEGIN
                    PERFORM public.drop_reflex_ivm(_imv.name, TRUE);
                    RAISE NOTICE 'pg_reflex: dropped IMV % (source % was dropped)', _imv.name, _obj.object_identity;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'pg_reflex: failed to drop IMV % after source % drop: %',
                        _imv.name, _obj.object_identity, SQLERRM;
                    DELETE FROM public.__reflex_ivm_reference WHERE name = _imv.name;
                END;
            END LOOP;

            -- An IMV whose own *target* table was dropped (e.g. DROP SCHEMA …
            -- CASCADE, or a stray DROP TABLE) must also be torn down, otherwise
            -- the registry row orphans, pointing at a relation that no longer
            -- exists. The source branch above never catches this when the
            -- source is a view or lives outside the dropped scope. Match on the
            -- EXACT target identity (target_schema + bare name) — never a prefix
            -- — so a partition swap dropping child / __reflex_swap_* tables can
            -- never be mistaken for the registered target.
            FOR _imv IN
                SELECT name
                FROM public.__reflex_ivm_reference
                WHERE COALESCE(target_schema, 'public') || '.'
                      || (regexp_match(name, '([^.]+)$'))[1] = _obj.object_identity
                ORDER BY graph_depth DESC, name DESC
            LOOP
                BEGIN
                    PERFORM public.drop_reflex_ivm(_imv.name, TRUE);
                    RAISE NOTICE 'pg_reflex: dropped IMV % (target % was dropped)', _imv.name, _obj.object_identity;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'pg_reflex: failed to drop IMV % after target % drop: %',
                        _imv.name, _obj.object_identity, SQLERRM;
                    DELETE FROM public.__reflex_ivm_reference WHERE name = _imv.name;
                END;
            END LOOP;
        END LOOP;
    END;
    $$;

    CREATE OR REPLACE FUNCTION public.__reflex_named_relation(spelling TEXT)
    RETURNS TEXT[] LANGUAGE plpgsql IMMUTABLE AS $fn$
    DECLARE
        part TEXT[] := regexp_match(
            spelling, '^("(?:[^"]|"")*"|[^."]+)(?:\.("(?:[^"]|"")*"|[^."]+))?$');
        first_part TEXT;
    BEGIN
        IF part IS NULL THEN
            RETURN ARRAY[spelling];
        END IF;
        first_part := CASE WHEN part[1] LIKE '"%"'
                           THEN replace(substr(part[1], 2, length(part[1]) - 2), '""', '"')
                           ELSE translate(part[1], 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')
                      END;
        IF part[2] IS NULL THEN
            RETURN ARRAY['public', first_part];
        END IF;
        RETURN ARRAY[first_part,
                     CASE WHEN part[2] LIKE '"%"'
                          THEN replace(substr(part[2], 2, length(part[2]) - 2), '""', '"')
                          ELSE translate(part[2], 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')
                     END];
    END;
    $fn$;

    CREATE OR REPLACE FUNCTION public.__reflex_reads_relation(registered TEXT[], relation TEXT)
    RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $fn$
    DECLARE
        named TEXT[] := public.__reflex_named_relation(relation);
        spelling TEXT;
    BEGIN
        FOREACH spelling IN ARRAY COALESCE(registered, ARRAY[]::TEXT[]) LOOP
            IF public.__reflex_named_relation(spelling) = named THEN
                RETURN TRUE;
            END IF;
        END LOOP;
        RETURN FALSE;
    END;
    $fn$;

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
        -- (reconcile.rs `cascade_partitioned_rebuild_to_dependents` for a
        -- partitioned root; `ignoring_dependents` for ignoring consumers of
        -- an unpartitioned one, which see none of the rebuild's DML), which
        -- is what makes skipping here safe rather than merely quiet.
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
                -- pg_reflex's own trigger toggle around a relocation changes no
                -- partition: syncing the readers of the toggled IMV re-enters
                -- this trigger through their own toggles, down the whole chain.
                IF _toggle_root IS NOT NULL
                   AND to_regclass(_parent) = to_regclass(_toggle_root) THEN
                    _parent := NULL;
                END IF;
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
                       WHERE public.__reflex_named_relation(
                                 CASE WHEN r.target_schema IS NOT NULL AND r.name NOT LIKE '%.%'
                                      THEN quote_ident(r.target_schema) || '.' || r.name
                                      ELSE r.name END)
                             = public.__reflex_named_relation(_part_root)
                   )
                   AND EXISTS (
                       -- Enqueue for ANY enabled IMV depending on this root:
                       -- partitioned IMVs reconcile per-partition; unpartitioned
                       -- IMVs get a full reconcile (flush handles both). Without
                       -- this, an unpartitioned IMV on a swap source goes stale.
                       SELECT 1 FROM public.__reflex_ivm_reference r
                       WHERE r.enabled
                         AND public.__reflex_reads_relation(r.depends_on, _part_root)
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
                      AND public.__reflex_reads_relation(depends_on, _parent)
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
                WHERE public.__reflex_reads_relation(depends_on, _src)
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

    CREATE OR REPLACE FUNCTION public.__reflex_partition_flush_fn()
    RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
    BEGIN
        -- Every partition DDL command of a transaction queues one event for its
        -- root; the event of the latest enqueue flushes them all, once.
        IF EXISTS (SELECT 1 FROM public.__reflex_partition_pending
                    WHERE source_root = NEW.source_root AND attempts = NEW.attempts) THEN
            PERFORM public.reflex_flush_partition_source(NEW.source_root);
        END IF;
        RETURN NULL;
    END;
    $fn$;
