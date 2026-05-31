-- Migration: pg_reflex 1.7.3 → 1.7.4
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.7.4';
--
-- 1.7.4 is a correctness release for partitioned IMV CREATION. The fix lives
-- entirely in the compiled extension (the partition-anchor resolution); there
-- is NO catalog schema change and NO change to any SQL function signature, so
-- this migration only bumps the installed version. Replacing the `.so` is what
-- actually ships the fix.
--
-- It refines `resolve_anchor_source` (the 1.7.3 anchor fix) along two axes:
--
-- (1) A candidate anchor must be partitioned ON the partition column itself.
--     A source partitioned on a *different* column is no longer mistaken for a
--     candidate (new `source_partitioned_on` helper, replacing the looser
--     "partitioned at all" check).
--
-- (2) Several sources co-partitioned on the SAME column are no longer
--     "ambiguous". When a JOIN key IS the partition column, every co-owner's
--     partition layout aligns, so any of them is a sound anchor for the child
--     DDL — including the case where ALL owners are reflex intermediates and
--     there is no base owner (the forecast_analysis_view shape:
--     `__cte_forecast_sales FULL JOIN __cte_history_sales ON dem_plan_id`).
--     Base owners are still preferred when present; otherwise the choice is
--     made deterministically (lexicographically) for stability across rebuilds.
--     Non-anchor co-owners own the column natively and fall through to Path B.
--     The error now fires only when NO source is partitioned on the column.
--
-- No data backfill, no DDL. Existing IMVs are unaffected.

DO $migrate$
BEGIN
    RAISE NOTICE 'pg_reflex 1.7.4: partition-anchor resolution now accepts sources co-partitioned on the join key (incl. all-intermediate FULL JOINs like forecast_analysis_view) and ignores sources partitioned on a different column. No catalog change.';
END
$migrate$;
