-- Migration: pg_reflex 1.7.2 → 1.7.3
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.7.3';
--
-- 1.7.3 is a correctness release for IMV CREATION. Both fixes live entirely in
-- the compiled extension (the create/decompose + partition-anchor logic); there
-- is NO catalog schema change and NO change to any SQL function signature, so
-- this migration only bumps the installed version. Replacing the `.so` is what
-- actually ships the fixes.
--
-- (1) Failed decomposed-IMV creation no longer orphans its sub-IMVs.
--     Creation rejections are returned as `"ERROR…"` strings, so the function
--     returns normally and the transaction is NOT aborted. A query that
--     decomposes into several sub-IMVs (CTE / UNION-ALL set-op) materialises
--     them one at a time; when a LATER operand/CTE or the final body soft-
--     rejected, the already-created sub-IMVs were committed and left behind,
--     polluting the IMV space. Every such soft-reject path now rolls back the
--     sub-IMVs it had already created (cascade, reverse creation order).
--
-- (2) Partition-anchor disambiguation prefers the base source over derived
--     intermediates. A decomposed query can have two PARTITIONED owners of the
--     partition column — a base partitioned table AND a partition-inheriting
--     reflex sub-IMV (`__cte_`/`__union_`/`__base`). `resolve_anchor_source`
--     previously errored "multiple sources own partition column … ambiguous",
--     blocking creation. It now prefers the sole base (non-intermediate)
--     partitioned owner — the table whose partition children we physically
--     mirror — and only errors when the choice is still genuinely ambiguous.
--
-- No data backfill, no DDL. Existing IMVs are unaffected.

DO $migrate$
BEGIN
    RAISE NOTICE 'pg_reflex 1.7.3: failed decomposed-IMV creation now rolls back its partial sub-IMVs, and partition-anchor resolution prefers the base source over reflex-generated intermediates. No catalog change.';
END
$migrate$;
