-- Migration: pg_reflex 1.7.4 → 1.7.5
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.7.5';
--
-- 1.7.5 widens CTE/JOIN passthrough unique-key inference (to-one + to-many
-- INNER joins, CROSS-to-single-row, mixed equi+range). One catalog change: a
-- new `max_one_row` flag used to classify a CROSS JOIN to an ungrouped
-- aggregate sub-IMV as to-one. Existing rows default to FALSE (the prior
-- behaviour: such joins simply weren't inferred). No data backfill required —
-- inference re-runs at create time, and existing IMVs keep their stored keys.

ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS max_one_row BOOLEAN DEFAULT FALSE;

DO $migrate$
BEGIN
    RAISE NOTICE 'pg_reflex 1.7.5: added __reflex_ivm_reference.max_one_row; widened JOIN unique-key inference.';
END
$migrate$;
