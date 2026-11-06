-- Migration: pg_reflex 1.12.3 → 1.12.4
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.12.4';
--
-- A bug-fix release. The join-key-scoped recompute of an aggregate IMV after
-- a write to a LEFT JOIN secondary now deletes from the target by the GROUP BY
-- column's projection alias instead of its intermediate name. The fix is
-- Rust-side codegen. No registry column, table, index, trigger, event trigger
-- or function changes; no IMV needs to be recreated. IMVs already marked
-- known_stale by the bug need one reflex_reconcile() each.

SELECT 1 WHERE FALSE;
