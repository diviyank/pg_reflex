-- Migration: pg_reflex 1.11.2 → 1.11.3
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.11.3';
--
-- Four correctness fixes on the partitioned-IMV maintenance paths. Three of
-- them are silent: they produced wrong or absent data with no error, so an
-- upgraded cluster shows no symptom until someone reads the result.
--
--   1. `reflex_reconcile_partition` committed the destructive DDL of its
--      pre-sync even when it reported failure, and a raised error unwinding
--      out of the reconcile could abort the backend. Both are Rust-side.
--
--   2. Adding a source partition held AccessExclusive on the live IMV root
--      for the whole COMMIT-time reconcile, blocking every reader of the IMV
--      — including readers of unrelated partitions. Rust-side.
--
--   3. The partition swap replaced a sub-partitioned mirror child with a
--      plain table (`LIKE ... INCLUDING ALL` cannot carry partitioning), and
--      the next partition sync then dropped and recreated those children
--      EMPTY. Rust-side.
--
--   4. `reflex_reconcile` on a partitioned IMV DESTROYED its dependent IMVs:
--      each swap's `ALTER TABLE` fired this event trigger, whose dependent
--      auto-sync re-mirrored the parent's TRANSIENT mid-swap child set and
--      dropped the real child as a bound-collision orphan.
--
-- Only (4) needs SQL here, and it is the one that MUST NOT be skipped: the
-- fix is split between the Rust swap primitive (which now publishes
-- `pg_reflex.internal_swap_root` for the duration of its DETACH/ATTACH) and
-- this trigger (which returns immediately while that GUC is set). Shipping
-- the new library against the OLD trigger body pairs an explicit dependent
-- cascade with a trigger that still corrupts dependents mid-swap — worse
-- than either half alone. The function body below is the bootstrap DDL from
-- `src/lib.rs` verbatim apart from indentation, so fresh installs and
-- upgrades converge on the same definition. If you edit one, edit both.
--
-- No registry columns were added or changed by this release.
--
-- Operationally, after upgrading: an IMV whose mirror was already flattened
-- by (3) still holds correct data but is armed — the next partition sync
-- empties it. Repair such an IMV with `SELECT reflex_reconcile('<imv>');`
-- and do NOT run `reflex_sync_partitions` on it first. A dependent already
-- emptied by (4) is repaired with `SELECT reflex_rebuild_imv('<dependent>');`

-- === The dependent auto-sync trigger, replayed with the mid-swap guard ===
-- The CREATE EVENT TRIGGER binding is unchanged and is not recreated here.

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
