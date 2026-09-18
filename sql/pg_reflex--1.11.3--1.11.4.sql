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
--      rebuilds those partitions. The trigger runs as the writer; three
--      SECURITY DEFINER helpers write the pg_reflex tables. The last
--      statement of this file installs the triggers on existing IMVs via
--      `reflex_rebuild_imv_metadata`.
--
--   7. A partitioned IMV's `row_count` estimate is the sum over its leaves,
--      and a partition swap no longer leaves `reflex_ivm_status` counting the
--      IMV exactly until the root is analyzed. Rust-side.
--
-- The table, column and function DDL below is the bootstrap DDL from
-- `src/lib.rs` verbatim apart from indentation, so fresh installs and upgrades
-- converge. If you edit one, edit both.
--
-- Operationally, after upgrading and BEFORE recreating any IMV (including via
-- `reflex_rebuild_imv`, which replays the create):
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

-- The IMVs whose ignore_heal_keys name `_relid`.
CREATE OR REPLACE FUNCTION public.__reflex_heal_targets(_relid regclass)
RETURNS TABLE (imv_name TEXT, source TEXT, source_column TEXT, watched_columns TEXT[])
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
    SELECT r.name, k.key, k.value->>'source_column',
           ARRAY(SELECT jsonb_array_elements_text(
                     COALESCE(k.value->'watched_columns', '[]'::jsonb)))
      FROM public.__reflex_ivm_reference r
     CROSS JOIN LATERAL jsonb_each(COALESCE(r.aggregations->'ignore_heal_keys', '{}'::jsonb)) k
     WHERE COALESCE(r.enabled, TRUE)
       AND to_regclass(k.value->>'relation') = _relid
$fn$;

-- Queue text keys for one IMV mapped to `_relid`. Re-queuing a key
-- re-stamps it, so a heal that read the older stamp leaves it queued.
CREATE OR REPLACE FUNCTION public.__reflex_heal_enqueue(_relid regclass, _imv TEXT, _source TEXT, _keys TEXT[])
RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
    INSERT INTO public.__reflex_heal_pending (imv_name, partition_key, source)
    SELECT DISTINCT t.imv_name, q.key, t.source
      FROM public.__reflex_heal_targets(_relid) t
     CROSS JOIN unnest(_keys) AS q(key)
     WHERE t.imv_name = _imv AND t.source = _source AND q.key IS NOT NULL
    ON CONFLICT (imv_name, partition_key) DO UPDATE
       SET enqueued_at = clock_timestamp(), source = EXCLUDED.source, last_error = NULL
$fn$;

-- A TRUNCATE cannot be scoped to keys: mark every IMV mapped to `_relid`
-- known_stale, keeping an earlier reason and stale_since. Anyone may call
-- this helper, so it acts only on a source that really is empty.
CREATE OR REPLACE FUNCTION public.__reflex_heal_mark_truncated(_relid regclass)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
    _emptied BOOLEAN;
BEGIN
    EXECUTE format('SELECT NOT EXISTS (SELECT 1 FROM %s)', _relid) INTO _emptied;
    IF NOT _emptied THEN
        RETURN;
    END IF;
    UPDATE public.__reflex_ivm_reference r
       SET known_stale = TRUE,
           stale_since = COALESCE(r.stale_since, now()),
           stale_reason = CASE WHEN COALESCE(r.known_stale, FALSE) AND COALESCE(r.stale_reason, '') <> ''
                               THEN r.stale_reason || ' | ' || m.reason
                               ELSE m.reason END
      FROM (SELECT DISTINCT t.imv_name,
                   format('ignored source %s was truncated, which no heal can scope. '
                          || 'Run SELECT reflex_reconcile(%L);', _relid, t.imv_name) AS reason
              FROM public.__reflex_heal_targets(_relid) t) m
     WHERE r.name = m.imv_name
       AND NOT (COALESCE(r.known_stale, FALSE)
                AND position(m.reason IN COALESCE(r.stale_reason, '')) > 0);
END;
$fn$;

CREATE OR REPLACE FUNCTION public.__reflex_heal_on_ignored_change()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp SET extra_float_digits = 3 AS $fn$
DECLARE
    _h RECORD;
    _key TEXT;
    _keys TEXT;
    _cols TEXT;
    _batch TEXT[];
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        PERFORM public.__reflex_heal_mark_truncated(TG_RELID::regclass);
        RETURN NULL;
    END IF;
    FOR _h IN SELECT * FROM public.__reflex_heal_targets(TG_RELID::regclass) LOOP
        CONTINUE WHEN EXISTS (
            SELECT 1 FROM unnest(_h.watched_columns || _h.source_column) AS c
             WHERE NOT EXISTS (SELECT 1 FROM pg_attribute a
                                WHERE a.attrelid = TG_RELID AND a.attname = c
                                  AND a.attnum > 0 AND NOT a.attisdropped));
        _key := format('to_jsonb(%I) #>> ''{}''', _h.source_column);
        IF TG_OP = 'INSERT' THEN
            _keys := format('SELECT %s FROM __reflex_heal_new', _key);
        ELSIF TG_OP = 'DELETE' THEN
            _keys := format('SELECT %s FROM __reflex_heal_old', _key);
        ELSIF cardinality(_h.watched_columns) = 0 THEN
            _keys := format('SELECT %1$s FROM __reflex_heal_old UNION SELECT %1$s FROM __reflex_heal_new', _key);
        ELSE
            SELECT string_agg(format('format(''%%s'', %I)', c), ', ') INTO _cols
              FROM unnest(_h.watched_columns) AS c;
            _keys := format(
                'SELECT d.__reflex_heal_key FROM ('
                || '(SELECT %1$s AS __reflex_heal_key, %2$s FROM __reflex_heal_old '
                || 'EXCEPT SELECT %1$s, %2$s FROM __reflex_heal_new) UNION ALL '
                || '(SELECT %1$s, %2$s FROM __reflex_heal_new '
                || 'EXCEPT SELECT %1$s, %2$s FROM __reflex_heal_old)) d',
                _key, _cols);
        END IF;
        EXECUTE format('SELECT array_agg(DISTINCT k) FROM (%s) s(k) WHERE k IS NOT NULL', _keys)
           INTO _batch;
        IF _batch IS NOT NULL THEN
            PERFORM public.__reflex_heal_enqueue(TG_RELID::regclass, _h.imv_name, _h.source, _batch);
        END IF;
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
