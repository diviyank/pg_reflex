-- Migration: pg_reflex 1.11.0 → 1.11.1
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.11.1';
--
-- Replace the module (.so) BEFORE running this.
--
-- Ten fixes plus one new function. Seven of the fixes are delivered entirely
-- in the module; the eighth (targeted-recovery observability) adds two
-- nullable/defaulted columns to the registry, so this delta carries DDL at
-- the end (a fast-default ADD COLUMN, no table rewrite) plus one corrective
-- metadata backfill. The ninth fix and the new `reflex_rebuild_union_mirror`
-- function (CREATE FUNCTION below) are the materialised UNION-ALL wrapper
-- mirror-trigger work; the tenth is a further, module-only fix in that same
-- area (mirror-trigger FUNCTION naming, not the trigger naming the ninth
-- fix covers).
--
--   * (SILENT WRONG RESULT) A MIN/MAX aggregate IMV over a nullable group key returned
--     a stale extremum forever after a retraction: the recompute scoped its rescan
--     with `(cols) IN (SELECT … FROM affected)`, and `(NULL) IN (…)` is NULL, so the
--     NULL group was never re-derived. Now NULL-safe (`EXISTS … IS NOT DISTINCT FROM`).
--
--   * (SILENT WRONG RESULT) The same NULL-unsafe skip in the top-K scalar-refresh path
--     left a NULL group's MIN/MAX wrong after a middle-ranked UPDATE (shrinking neither
--     heap). Now NULL-safe, gated so the common path keeps its sargable index scan.
--
--   * (TX ABORT / SILENT COLUMN-SHIFT) reflex_reconcile on a decomposed wrapper IMV
--     (top-level UNION/UNION ALL/INTERSECT/EXCEPT, DISTINCT ON, window) either raised
--     `"<view>" is not a table` (aborting the caller's transaction) or column-shifted a
--     materialised wrapper. reconcile_one now refuses a wrapper with a clean error
--     string before any DDL; reconcile the parent or the operands, not the wrapper.
--
--   * reflex_scheduled_reconcile no longer dies on decomposed set-op / DISTINCT ON /
--     window wrapper IMVs. Reconcile cannot operate on a wrapper (it is a view,
--     maintained through its generated sub-IMVs), so the sweep raised
--     `"<view>" is not a table` and returned NOTHING — no IMV in the database was
--     swept. Wrapper rows were permanent candidates because nothing advances their
--     `last_update_date`. The candidate query now skips planless rows. If you have
--     any UNION / UNION ALL / DISTINCT ON / window IMV, your scheduled reconcile was
--     a no-op before this release.
--
--   * reflex_reconcile (and its alias reflex_rebuild_imv) now recreates a dropped
--     `__reflex_intermediate_<view>`, its indexes and its group-capture tables from
--     the registry row, then lets the existing partition sync mirror the children.
--     Previously that DDL existed only at create time, so an intermediate lost to a
--     `DROP … CASCADE` left the IMV unrepairable by any exposed primitive while the
--     audit reported it at Error severity with a remedy that could not work. The
--     heal runs under the IMV's advisory lock, re-probes after acquiring it, and
--     refuses to build a shape whose group-key types it cannot resolve.
--
--   * internal-tables-exist and trigger-attached no longer false-positive at Error
--     severity on decomposed wrapper IMVs, demanding internal tables and consolidated
--     triggers the wrapper does not own. trigger-attached's remedy was harmful:
--     rebuilding triggers on a sub-IMV installed four junk triggers per retry without
--     clearing the finding. If you ran it, drop any
--     `__reflex_trigger_{ins,del,upd,trunc}_on_<sub-imv>` triggers it left behind.
--
--   * New repair primitive `reflex_rebuild_union_mirror(wrapper TEXT)`: a
--     materialised UNION-ALL wrapper (built when a CTE feeding a set-op is
--     consumed by an aggregate) is kept in sync by
--     `__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>` triggers on each
--     operand. Previously nothing could repair a dropped mirror trigger.
--     Refuses cleanly on a VIEW wrapper (no operand triggers by design) or a
--     non-wrapper IMV; restores future maintenance only, does not backfill
--     deltas missed while the trigger was absent.
--
--   * trigger-attached's decomposed-wrapper skip above was unconditional, which
--     also silenced the real check for a materialised wrapper's mirror
--     triggers. It now checks a materialised (TABLE) wrapper's operands and
--     reports any missing mirror trigger, naming `reflex_rebuild_union_mirror`;
--     a VIEW wrapper stays silent, unchanged. The expected trigger name is
--     compared against PostgreSQL's own NAMEDATALEN-truncated form, so a
--     wrapper whose generated trigger name exceeds 63 bytes is not
--     permanently misreported as broken.
--
--   * (SILENT WRONG RESULT) A materialised UNION-ALL wrapper's per-operand mirror
--     trigger FUNCTIONS shared one unbounded name; a wrapper name >=38 bytes
--     truncated away the discriminator between the ins/del/upd functions (or, at a
--     longer threshold, between two operands' same-kind function), so PostgreSQL's
--     NAMEDATALEN truncation collapsed distinct CREATE OR REPLACE FUNCTIONs onto one
--     proname and the last one issued silently overwrote the others' bodies while
--     already-bound triggers kept executing them — an INSERT on one operand could
--     run another DML kind's body (erroring) or mis-tag the row with the wrong
--     operand index (no error, wrong data). Fixed by hashing the full name through
--     safe_identifier so both discriminators survive regardless of wrapper length;
--     reflex_rebuild_union_mirror (above) repairs an already-collided wrapper by
--     hand, but this fix does not detect or auto-heal one — see
--     untreated_bugs/2026-07-25_union_mirror_function_name_collision.md.
--
--   * (DDL) `reflex_rebuild_imv` / targeted `reflex_reconcile` retries are now
--     observable: `__reflex_ivm_reference` gains `rebuild_count` and
--     `last_rebuild_at`, incremented only by direct operator recovery (not by
--     trigger-fired maintenance or the scheduled sweep), and surfaced by
--     `reflex_ivm_status`. When a rebuild targets an IMV it cannot converge (a
--     matview source, or an `ignore_sources`-fed / anchor-empty partition) it now
--     WARNs, naming the primitive that can (`refresh_imv_depending_on` /
--     `reflex_reconcile_partition`), instead of returning a bare success. The
--     counter also increments on `reflex_doctor(fix => true)` recoveries.
--
--   * The `partition-mirror` audit check (surfaced by reflex_doctor as F3) no
--     longer reports phantom intermediate-partition drift on passthrough IMVs.
--     A passthrough IMV owns no `__reflex_intermediate_<view>` table at all, so
--     the check diffed the anchor source's child set against an absent parent's
--     necessarily-empty child list and reported every child as missing — then
--     prescribed `reflex_sync_partitions`, which gates its intermediate-child
--     DDL on the same relation's existence and therefore could never create
--     any of them. The finding survived its own remedy indefinitely (field:
--     42 IMVs across 5 tenants, one of them reporting 17 phantom children while
--     sync returned `+0 intermediate`).
--
--     The intermediate half of the comparison now runs only when an
--     intermediate is both expected (non-empty `end_query`) and present. The
--     target half always runs, so target-tree drift is still reported when the
--     intermediate parent is absent. A genuinely missing intermediate parent
--     remains reported by the `internal-tables-exist` check, which already
--     covers relation absence at Error severity.
--
-- No action is required beyond replacing the module and running this update.
-- Existing F3 `partition-mirror` findings on passthrough IMVs disappear from the
-- next `reflex_doctor()` / `reflex_audit()` run; nothing needs to be reconciled
-- to clear them, because nothing was ever wrong with those IMVs.
--
-- One EXCEPTION: if you have a materialised UNION-ALL wrapper (a CTE feeding a
-- set-op consumed by an aggregate) whose name is >=38 bytes, its mirror trigger
-- functions may already be collided under the pre-1.11.1 naming bug above. This
-- update does not detect or repair that automatically — run
-- `SELECT reflex_rebuild_union_mirror('<wrapper_name>')` by hand for any such
-- wrapper after replacing the module.

-- New SQL-callable function (Rust-backed via pgrx).
CREATE FUNCTION "reflex_rebuild_union_mirror"(
    "wrapper" TEXT
) RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_rebuild_union_mirror_wrapper';

-- --------------------------------------------------------------------------
-- DDL: targeted-recovery observability columns. Identical to the bootstrap DDL
-- in src/lib.rs (fresh installs get these from the bootstrap; upgrades from here).
-- `NOT NULL DEFAULT 0` is a PG11+ fast default — metadata-only, no table rewrite.
-- --------------------------------------------------------------------------
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS rebuild_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS last_rebuild_at TIMESTAMPTZ;

-- reflex_ivm_status() gains rebuild_count / last_rebuild_at (and, cumulatively,
-- the 1.11.0 requires_explicit_refresh column that never got its own redeclare
-- either — this is a RETURNS TABLE shape change, so DROP + CREATE, not CREATE
-- OR REPLACE, matching the precedent in sql/pg_reflex--1.10.7--1.10.8.sql.
-- ALTER EXTENSION UPDATE does not re-derive a function's catalog signature from
-- the module's pgrx annotations on its own; skipping this step leaves an
-- upgraded install's reflex_ivm_status() at its pre-1.11.0 (12-column) shape
-- forever while a fresh 1.11.1 install gets the current 15-column one.
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
	"last_rebuild_at" timestamp with time zone
)
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_ivm_status_wrapper';

-- --------------------------------------------------------------------------
-- Corrective backfill (PS-14 Part B): the 1.10.11→1.11.0 PS-3 backfill matched
-- ignored sources by exact string only, so an IMV that ignores a real table by
-- BARE name while `depends_on` stores it QUALIFIED was under-flagged and stayed
-- invisible. Widen to bare-OR-qualified, mirroring create-time
-- (all_real_sources_are_matviews). Only ever flips FALSE→TRUE — it excludes no
-- more sources than create-time, so it cannot over-flag a maintainable IMV.
-- --------------------------------------------------------------------------
WITH real_source AS (
    SELECT r.name, s AS src
      FROM public.__reflex_ivm_reference r,
           LATERAL unnest(COALESCE(r.depends_on, ARRAY[]::TEXT[])) AS s
     WHERE s NOT LIKE '<%'
       AND NOT (s = ANY(COALESCE(r.ignored_sources, ARRAY[]::TEXT[]))
                OR split_part(s, '.', 2) = ANY(COALESCE(r.ignored_sources, ARRAY[]::TEXT[])))
),
verdict AS (
    SELECT name,
           bool_and(EXISTS (SELECT 1 FROM pg_class c
                             WHERE c.oid = to_regclass(src) AND c.relkind = 'm')) AS all_matviews
      FROM real_source
     GROUP BY name
)
UPDATE public.__reflex_ivm_reference r
   SET requires_explicit_refresh = TRUE
  FROM verdict v
 WHERE r.name = v.name
   AND v.all_matviews
   AND COALESCE(r.requires_explicit_refresh, FALSE) = FALSE;
