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

-- __reflex_doctor_try_repair now captures its statement's return value.
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

DO $ps4$ BEGIN
    RAISE NOTICE 'pg_reflex 1.11.0 (PS-4): reflex_doctor classifies the pending queue on drain failures and dates findings from last_attempt_at. Existing pending rows have last_attempt_at NULL until their next drain attempt.';
END $ps4$;
-- === end PS-4 ==============================================================
