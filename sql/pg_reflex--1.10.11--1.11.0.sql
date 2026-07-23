-- Migration: pg_reflex 1.10.11 → 1.11.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.11.0';
--
-- Replace the module (.so) BEFORE running this.

SELECT 1 WHERE FALSE;

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

DO $ps4$ BEGIN
    RAISE NOTICE 'pg_reflex 1.11.0 (PS-4): reflex_doctor classifies the pending queue on drain failures and dates findings from last_attempt_at. Existing pending rows have last_attempt_at NULL until their next drain attempt.';
END $ps4$;
-- === end PS-4 ==============================================================
