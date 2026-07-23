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
CREATE OR REPLACE FUNCTION public."reflex_reset_partition_failures"(
    "source_root" TEXT DEFAULT NULL
) RETURNS bigint
LANGUAGE c
AS 'MODULE_PATHNAME', 'reflex_reset_partition_failures_wrapper';

DO $ps4$ BEGIN
    RAISE NOTICE 'pg_reflex 1.11.0 (PS-4): reflex_doctor classifies the pending queue on drain failures (F2b when the failure cap has been reached), dates findings from last_attempt_at, and re-arms capped roots via the new reflex_reset_partition_failures() before flushing. Existing pending rows have last_attempt_at NULL until their next drain attempt.';
END $ps4$;
-- === end PS-4 ==============================================================
