-- Migration: pg_reflex 1.10.7 → 1.10.8
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.10.8';
-- Replace the module (.so) BEFORE running this — the Rust-side fixes (F1
-- flush-failure recording, F3 orphan-overlap swap heal, F4 known_stale
-- writes on the flush path, F5 doc, F6/F8 audit checks, F7 divergence
-- detector) are recompiled into the module; the DDL below installs the
-- catalog + SQL-callable surface on an existing database.
--
-- 1.10.8 is the "untreated findings" remediation release. It closes the
-- silent failure modes seen in the 2026-07-02 multi-tenant incident and adds
-- a single operator entrypoint (reflex_doctor) for diagnosis + safe repair.
--
--   F1  Pending-queue re-arm. The DDL enqueue is now ON CONFLICT DO UPDATE
--       and the drain constraint trigger fires AFTER INSERT OR UPDATE, so a
--       root that failed one flush is retried on the next partition DDL
--       instead of being silently ignored forever. New columns
--       __reflex_partition_pending.attempts / .last_error surface the retry.
--   F4  Durable staleness. __reflex_ivm_reference gains known_stale /
--       stale_reason / stale_since, set on caught cascade/flush failures and
--       cleared on a successful reconcile; surfaced in reflex_ivm_status().
--   F2  reflex_partition_pending_status() — read-only pending-queue backlog.
--   F9  REFLEX-DBG resolve_anchor NOTICEs gated behind GUC
--       pg_reflex.debug_resolve_anchor (default off) — Rust-only.
--   F3  Orphan-overlap swap heal — Rust-only (partition swap).
--   F5  reflex_rebuild_imv documented as an anchor-scoped alias of
--       reflex_reconcile — docs-only.
--   F6  Archive-residue audit check; F8 bare-name-ambiguity audit check —
--       Rust-only (reflex_audit).
--   F7  reflex_refresh_partition_snapshot_if_diverged() — on-demand snapshot
--       self-heal.
--   F4b reflex_rebuild_chain() — atomic CASCADE drop + recreate of a
--       decomposed IMV chain from stored create_args (new
--       __reflex_ivm_reference.create_args column).
--   F10 reflex_doctor(target, fix, drop_orphans, max_attempts) — dry-run by
--       default; applies only non-breaking repairs under fix => TRUE.
--
-- NOTE: like 1.10.0, this migration does NOT drain the pending queue (ALTER
-- EXTENSION UPDATE runs in creating_extension mode where DDL on the runtime
-- partition children is rejected, SQLSTATE 55000). Drain any backlog AFTER
-- the upgrade as a normal statement: SELECT public.reflex_flush_partitions();

-- === F1 / F4 — new catalog columns (idempotent) ===
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS known_stale BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS stale_reason TEXT;
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS stale_since TIMESTAMPTZ;
-- F4b — captured create-time args for reflex_rebuild_chain (NULL for legacy rows)
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS create_args TEXT;
ALTER TABLE public.__reflex_partition_pending
    ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
ALTER TABLE public.__reflex_partition_pending
    ADD COLUMN IF NOT EXISTS last_error TEXT;

-- === F1 (re-arm enqueue) + F4 (known_stale on auto-sync failure): re-emit the
--     ddl_command_end handler with the new body ===
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
    -- The reflex_on_ddl_command_end EVENT TRIGGER itself is unchanged and
    -- already exists on 1.10.7; CREATE OR REPLACE of its function above is
    -- sufficient (event triggers bind by function name).

-- === F1 — drain trigger now fires AFTER INSERT OR UPDATE ===
    DROP TRIGGER IF EXISTS __reflex_partition_flush_trigger
        ON public.__reflex_partition_pending;

    CREATE CONSTRAINT TRIGGER __reflex_partition_flush_trigger
        AFTER INSERT OR UPDATE ON public.__reflex_partition_pending
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION public.__reflex_partition_flush_fn();

-- === F10 — repair-isolation helper (savepoint-returning) ===
    CREATE OR REPLACE FUNCTION public.__reflex_doctor_try_repair(_sql TEXT)
    RETURNS TEXT LANGUAGE plpgsql AS $fn$
    BEGIN
        EXECUTE _sql;
        RETURN 'fixed';
    EXCEPTION WHEN OTHERS THEN
        RETURN 'failed:' || left(SQLERRM, 400);
    END;
    $fn$;

-- === F4 — reflex_ivm_status() gains known_stale / stale_reason.
--     RETURNS TABLE shape changed, so DROP + CREATE (not CREATE OR REPLACE). ===
DROP FUNCTION IF EXISTS public.reflex_ivm_status();
CREATE  FUNCTION "reflex_ivm_status"() RETURNS TABLE (
	"name" TEXT,  /* String */
	"graph_depth" INT,  /* i32 */
	"enabled" bool,  /* bool */
	"refresh_mode" TEXT,  /* String */
	"row_count" bigint,  /* i64 */
	"last_flush_ms" bigint,  /* Option < i64 > */
	"last_flush_rows" bigint,  /* Option < i64 > */
	"flush_count" bigint,  /* i64 */
	"last_error" TEXT,  /* Option < String > */
	"last_update_date" timestamp,  /* Option < pgrx :: datum :: Timestamp > */
	"known_stale" bool,  /* bool */
	"stale_reason" TEXT  /* Option < String > */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_ivm_status_wrapper';

-- === F2 — reflex_partition_pending_status() ===
CREATE  FUNCTION "reflex_partition_pending_status"() RETURNS TABLE (
	"source_root" TEXT,  /* String */
	"enqueued_at" timestamp with time zone,  /* pgrx :: datum :: TimestampWithTimeZone */
	"age_seconds" bigint,  /* i64 */
	"attempts" INT,  /* i32 */
	"last_error" TEXT  /* Option < String > */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_partition_pending_status_wrapper';

-- === F10 — reflex_doctor() ===
CREATE  FUNCTION "reflex_doctor"(
	"target" TEXT DEFAULT NULL, /* Option < & str > */
	"fix" bool DEFAULT FALSE, /* bool */
	"drop_orphans" bool DEFAULT FALSE, /* bool */
	"max_attempts" INT DEFAULT 3 /* i32 */
) RETURNS TABLE (
	"check_id" TEXT,  /* String */
	"severity" TEXT,  /* String */
	"object" TEXT,  /* String */
	"finding" TEXT,  /* String */
	"action" TEXT,  /* String */
	"outcome" TEXT  /* String */
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_doctor_wrapper';

-- === F4b — reflex_rebuild_chain() ===
CREATE  FUNCTION "reflex_rebuild_chain"(
	"view_name" TEXT /* & str */
) RETURNS TEXT /* String */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_rebuild_chain_wrapper';

-- === F7 — reflex_refresh_partition_snapshot_if_diverged() ===
CREATE  FUNCTION "reflex_refresh_partition_snapshot_if_diverged"(
	"source_root" TEXT /* & str */
) RETURNS TEXT /* String */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_refresh_partition_snapshot_if_diverged_wrapper';

DO $migrate$ BEGIN RAISE NOTICE 'pg_reflex 1.10.8: reflex_doctor() installed (dry-run by default). Run SELECT * FROM reflex_doctor(); to diagnose. Drain any partition backlog with SELECT public.reflex_flush_partitions(); as a separate statement.'; END $migrate$;
