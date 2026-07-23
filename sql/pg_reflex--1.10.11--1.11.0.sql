-- Migration: pg_reflex 1.10.11 → 1.11.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.11.0';
--
-- Replace the module (.so) BEFORE running this.
--
-- 1.11.0 collects several independent fixes. Each owns one clearly fenced
-- section below; sections do not depend on each other and may be applied in any
-- order.

SELECT 1 WHERE FALSE;


-- === PS-1: decomposed-chain correctness (N1 + B1) ==========================
--
-- A CTE-decomposed IMV's registry row never recorded the edge to the sub-IMV
-- pg_reflex generated for it. `resolve_existing_imv_deps` matched `depends_on`
-- against the registry `name` by exact string equality, but the decomposer
-- persists a sub-IMV source double-quoted ("schema"."view__cte_x") to preserve
-- identifier case. The match never fired, so for every decomposed IMV:
--
--   * `depends_on_imv` was empty and the generated child's `graph_child` was
--     empty, so reflex_rebuild_chain(<generated child>) saw no dependents — it
--     dropped the child's table, taking the parent's triggers with it, and
--     recreated only the child, silently ending the parent's maintenance.
--   * `graph_depth` was collapsed (a CTE parent got 1 instead of 2), so
--     ORDER BY graph_depth no longer ordered children before parents in
--     reflex_scheduled_reconcile, refresh_imv_depending_on and reflex_doctor.
--   * reflex_reconcile / reflex_rebuild_imv / refresh_reflex_imv on the parent
--     re-aggregated a stale child snapshot and returned RECONCILED. On a chain
--     whose generated child reads a MATERIALIZED VIEW — which cannot carry
--     triggers, so the child is frozen at create time — that meant every
--     operator-facing recovery primitive silently served stale data.
--
-- 1.11.0 canonicalises the dependency probe, records the generated node
-- explicitly, and makes reflex_reconcile rebuild a decomposed IMV's generated
-- sub-IMVs bottom-up (with their triggers suppressed, so nothing double-counts)
-- before rebuilding the IMV itself. A user-declared IMV dependency keeps the
-- old non-recursive behaviour.
--
-- Three operator-visible behaviour changes:
--
--   * reflex_rebuild_chain(<generated sub-IMV>) and a NON-cascade
--     drop_reflex_ivm(<generated sub-IMV>) now REFUSE, because the parent is
--     finally a registered dependent. That replaces silently ending the
--     parent's maintenance.
--   * graph_depth changes value for every decomposed IMV — up for CTE parents
--     (collapsed -> real), down for set-op wrappers (which used operand count
--     plus one). Every consumer orders by it ascending and every one of them
--     wants the corrected order.
--   * reflex_reconcile of a decomposed IMV does strictly more work: one extra
--     full rebuild per generated sub-IMV in the chain.

ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS is_generated_sub_imv BOOLEAN NOT NULL DEFAULT FALSE;

-- ALTER EXTENSION UPDATE runs only this delta script, not the full generated
-- schema, so a new function must be declared here or the upgrade leaves it
-- uninstalled.
CREATE OR REPLACE FUNCTION "reflex_repair_dependency_graph"() RETURNS TEXT /* String */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_repair_dependency_graph_wrapper';

-- New two-argument overload of reflex_reconcile. The one-argument form (unchanged)
-- keeps drop_orphans => TRUE, its 1.10.11 behaviour; this overload lets a caller
-- decline orphan-partition deletion — reflex_doctor uses it so an F4 reconcile no
-- longer drops a partition the operator refused to authorise at the F3 step.
CREATE OR REPLACE FUNCTION "reflex_reconcile"(
    "view_name" TEXT, /* &str */
    "drop_orphans" bool /* bool */
) RETURNS TEXT /* String */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reflex_reconcile_scoped_wrapper';

-- The backfill is NOT run inline here. `CREATE OR REPLACE FUNCTION … MODULE_PATHNAME`
-- followed by an immediate call needs `dlsym` of a brand-new symbol, which fails
-- when the backend running ALTER EXTENSION already has the previous `.so` mapped.
-- The operator runs it in a fresh session after the upgrade — see the NOTICE.
--
-- What it does when run: everything is derived from `depends_on`, the one
-- registry column no bug has corrupted.
--
-- * `depends_on_imv` and `graph_child` are repaired ADDITIVELY: an edge is only
--   ever added, never removed. Removal is the only operation that could make the
--   graph worse than it already is, and every spurious extra edge fails in the
--   safe direction (reflex_rebuild_chain and non-cascade drop_reflex_ivm refuse
--   rather than destroying or orphaning a dependent).
-- * `is_generated_sub_imv` is backfilled from the `__cte_` / `__union_<n>` /
--   `__base` name suffix AND the requirement that some other registry row depends
--   on the name. The suffix is a heuristic, acceptable here because this is a
--   one-shot repair of rows created before the flag existed and no better
--   evidence survives; the runtime reconcile decision reads the column instead. A
--   false positive needs a user to have named an IMV `foo__cte_bar` and to have
--   `foo` read from it; the node then gets rebuilt on `foo`'s reconcile, which
--   never silently changes another IMV's values but does suppress propagation to
--   any OTHER consumer of it for that rebuild.
-- * `graph_depth` is recomputed to a fixpoint of
--   depth(v) = max(depth(d) for d in depends_on_imv(v)) + 1, the same expression
--   create-time now uses. Idempotent on an acyclic graph; on a registry holding a
--   dependency cycle it cannot converge and returns a WARNING instead of REPAIRED.

DO $ps1$ BEGIN
    RAISE NOTICE 'pg_reflex 1.11.0 (PS-1): decomposed-chain correctness. AFTER this upgrade completes, in a NEW session, run: SELECT public.reflex_repair_dependency_graph(); to repair depends_on_imv / graph_child / graph_depth / is_generated_sub_imv on existing rows. Then re-run reflex_doctor(). Note: reflex_rebuild_imv on a decomposed IMV now reconciles its generated sub-IMVs first; reflex_rebuild_chain and non-cascade drop_reflex_ivm on a generated sub-IMV now refuse.';
END $ps1$;
-- === end PS-1 ==============================================================


-- === PS-4: reflex_doctor truthfulness ======================================
--
-- __reflex_partition_pending.last_attempt_at — stamped by the drain so a
-- finding can be dated. `enqueued_at` is reset by every re-enqueue and
-- `attempts` counts enqueues (not drain attempts), so before this column
-- nothing in the queue could date a drain failure: reflex_doctor classified
-- F1/F2 on `attempts` and reported an arbitrarily old `last_error` next to a
-- freshly reset age. It now classifies on `failures` and dates on
-- `last_attempt_at`.

ALTER TABLE public.__reflex_partition_pending
    ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ;

-- __reflex_doctor_try_repair now captures its statement's return value.
-- CONTRACT: _sql must be a statement that RETURNS a value (all callers pass
-- `SELECT reflex_*(...)`); `EXECUTE ... INTO` rejects one that returns no data.
-- reflex_reconcile / reflex_sync_partitions / drop_reflex_ivm signal some
-- failures by RETURNING an 'ERROR: …' string rather than raising; the old helper
-- discarded the result and reported those repairs as 'fixed'.
CREATE OR REPLACE FUNCTION public.__reflex_doctor_try_repair(_sql TEXT)
RETURNS TEXT LANGUAGE plpgsql AS $fn$
DECLARE
    _res TEXT;
BEGIN
    EXECUTE _sql INTO _res;
    IF _res IS NOT NULL AND upper(_res) LIKE 'ERROR%' THEN
        RETURN 'failed:' || left(_res, 400);
    END IF;
    RETURN 'fixed';
EXCEPTION WHEN OTHERS THEN
    RETURN 'failed:' || left(SQLERRM, 400);
END;
$fn$;

-- reflex_reset_partition_failures — re-arm pending roots the failure cap has
-- given up on. A root at PARTITION_FLUSH_FAILURE_CAP (5) is skipped by BOTH
-- reflex_flush_partitions() and reflex_flush_partition_source(root), and the
-- counter is cleared only by the DELETE a *successful* drain performs — so no
-- exposed primitive could move a capped root, while the skip warning told the
-- operator to "reset failures" and provided no way to do it. reflex_doctor(fix =>
-- TRUE) now re-arms each capped root once per invocation before the flush it
-- prescribes, and capped rows report as F2b rather than F2 so "wedged and
-- retrying" is distinguishable from "wedged and given up on".
CREATE OR REPLACE FUNCTION "reflex_reset_partition_failures"(
    "source_root" TEXT DEFAULT NULL
) RETURNS bigint
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_reset_partition_failures_wrapper';

DO $ps4$ BEGIN
    RAISE NOTICE 'pg_reflex 1.11.0 (PS-4): reflex_doctor classifies the pending queue on drain failures (F2b when the failure cap has been reached), dates findings from last_attempt_at, and re-arms capped roots via the new reflex_reset_partition_failures() before flushing. Existing pending rows have last_attempt_at NULL until their next drain attempt.';
END $ps4$;
-- === end PS-4 ==============================================================


-- === PS-3: unmaintainable-source visibility ================================
--
-- An IMV whose only real sources are materialized views cannot self-maintain:
-- PG fires no trigger on a matview, so the node is a snapshot frozen at create
-- time. Before this column pg_reflex recorded it indistinguishably from a
-- maintainable IMV, so every health surface read clean and reflex_doctor (which
-- keys off known_stale) could not see it — in prod 40/40 generated sub-IMVs sat
-- more than two days stale, unflagged.
--
-- requires_explicit_refresh records that structurally and PERMANENTLY. It is
-- kept strictly distinct from known_stale: known_stale means "a flush failed"
-- and is the authority PS-4's verify_stale_cleared uses to confirm a repair, so
-- reusing it for a by-design-unmaintainable node would make every
-- reflex_doctor(fix => TRUE) report a permanent failure. The new column is
-- surfaced by reflex_ivm_status and as reflex_doctor finding F7, whose action is
-- refresh_imv_depending_on('<mv>') (which cascades the whole chain) rather than
-- reflex_reconcile (which only fixes one level). No reconcile or verify path
-- ever clears it.
--
-- BLAST RADIUS: the backfill below flags existing matview-only IMVs. Any
-- monitoring that alerts on the new column (or the F7 finding count) will begin
-- firing for them. That is correct — they genuinely cannot self-maintain — but
-- it is a visible change on upgrade. Mixed-source and normally-maintainable IMVs
-- are never flagged.

ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS requires_explicit_refresh BOOLEAN NOT NULL DEFAULT FALSE;

-- Backfill: reproduce the create-time per-node verdict from depends_on +
-- ignored_sources. Flag a row iff it has at least one real source (not a
-- <subquery:>/<function:> placeholder, not ignored) AND every real source is a
-- materialized view. Pure SQL (no MODULE_PATHNAME call), so — unlike PS-1's
-- backfill — it runs inline here without a dlsym of a brand-new symbol.
WITH real_source AS (
    SELECT r.name, s AS src
      FROM public.__reflex_ivm_reference r,
           LATERAL unnest(COALESCE(r.depends_on, ARRAY[]::TEXT[])) AS s
     WHERE s NOT LIKE '<%'
       AND NOT (s = ANY(COALESCE(r.ignored_sources, ARRAY[]::TEXT[])))
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

DO $ps3$
DECLARE
    _n BIGINT;
BEGIN
    SELECT count(*) INTO _n
      FROM public.__reflex_ivm_reference
     WHERE requires_explicit_refresh = TRUE;
    RAISE NOTICE 'pg_reflex 1.11.0 (PS-3): % IMV(s) flagged requires_explicit_refresh (all sources are materialized views; cannot self-maintain). reflex_ivm_status and reflex_doctor (finding F7) now surface them; the remedy is SELECT refresh_imv_depending_on(''<matview>'') after each REFRESH MATERIALIZED VIEW. The flag is permanent and is never cleared by reconcile or reflex_doctor.', _n;
END $ps3$;
-- === end PS-3 ==============================================================
