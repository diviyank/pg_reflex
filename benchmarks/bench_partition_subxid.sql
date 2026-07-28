-- ============================================================================
-- pg_reflex — subtransaction-XID consumption of a multi-root partition flush
-- ============================================================================
--
-- WHY THIS IS A SEPARATE METRIC
--
-- Wall clock cannot see this one.  PostgreSQL caches at most
-- PGPROC_MAX_CACHED_SUBXIDS = 64 subtransaction XIDs per backend in shared
-- memory.  Past 64 the backend's subxidStatus.overflowed flag is set and every
-- OTHER backend's visibility check against this transaction's tuples falls back
-- to pg_subtrans lookups for the rest of the transaction.  That is a cliff, and
-- it lands on the readers, not on the writer being benchmarked — so a
-- single-session timing benchmark is structurally blind to it.
--
-- What we can measure cheaply is the input to the cliff: how many XIDs one
-- flush transaction consumes per pending root.  1 per root puts the overflow
-- at 65 roots; 2 per root moves it to 33.
--
-- Both flush entry points run in a single transaction, so this is the live
-- production shape: reflex_flush_partitions() drains every pending root in one
-- Spi::connect_mut, and the DEFERRED __reflex_partition_flush_trigger fires
-- once per pending row inside the committing transaction.
--
-- METHOD
--
--   txn A:  INSERT pg_current_xact_id()          -> x0
--   txn B:  SELECT reflex_flush_partitions()     <- the transaction under test
--   txn C:  UPDATE .. pg_current_xact_id()       -> x1
--   XIDs consumed by txn B = x1 - x0 - 1
--
-- The pending queue is filled first with the COMMIT-time flush trigger
-- DISABLED, so all N roots accumulate and are drained by one explicit call.
--
-- Concurrent backends assigning XIDs would inflate the count; run this on an
-- otherwise idle cluster.  The signal being looked for (1 vs 2 per root) is
-- large compared with the idle-cluster noise floor, and the driver prints the
-- per-root figure so a stray XID is visibly a rounding error.
--
-- HOW TO RUN
--
--     ./benchmarks/bench_partition_scaling.sh --label $(git rev-parse --short HEAD)
--
-- or standalone, against a database with pg_reflex installed:
--
--     psql -d rfxbench -v roots=40 -f benchmarks/bench_partition_subxid.sql
--
-- Output lines the driver parses:
--     NOTICE:  RFXSUBXID|<roots>|<xids_consumed>
-- ============================================================================

\set ON_ERROR_STOP on
\timing off
SET client_min_messages TO NOTICE;

\if :{?roots} \else \set roots 40 \endif

DROP TABLE IF EXISTS rfx_subxid_cfg;
CREATE TABLE rfx_subxid_cfg AS SELECT :roots::INT AS roots;

DROP TABLE IF EXISTS rfx_subxid_probe;
CREATE TABLE rfx_subxid_probe (x0 XID8, x1 XID8);

-- ---------------------------------------------------------------------------
-- N independent partitioned roots, one partitioned IMV each.  Deliberately
-- tiny: the metric is XIDs per root, not the cost of the work per root.
-- ---------------------------------------------------------------------------
DO $$
DECLARE _roots INT;
BEGIN
    SELECT roots INTO _roots FROM rfx_subxid_cfg;
    FOR k IN 1.._roots LOOP
        EXECUTE format('DROP TABLE IF EXISTS rfxsx_v_%s CASCADE', k);
        EXECUTE format('DROP TABLE IF EXISTS rfxsx_src_%s CASCADE', k);
        EXECUTE format(
            'CREATE TABLE rfxsx_src_%s (id BIGINT NOT NULL, bucket INT NOT NULL, '
            || 'amount NUMERIC NOT NULL) PARTITION BY LIST (bucket)', k);
        EXECUTE format(
            'CREATE TABLE rfxsx_src_%s_p0 PARTITION OF rfxsx_src_%s FOR VALUES IN (0)', k, k);
        EXECUTE format(
            'INSERT INTO rfxsx_src_%s (id, bucket, amount) '
            || 'SELECT i, 0, i FROM generate_series(1, 50) AS i', k);
        EXECUTE format(
            'SELECT public.create_reflex_ivm(%L, %L, NULL, ''UNLOGGED'', ''IMMEDIATE'', NULL, ARRAY[''bucket''])',
            'rfxsx_v_' || k,
            format('SELECT bucket, SUM(amount) AS total, COUNT(*) AS cnt FROM rfxsx_src_%s GROUP BY bucket', k));
    END LOOP;
END $$;

-- Accumulate the whole queue instead of letting each ATTACH drain at COMMIT,
-- so one explicit flush covers all N roots — the shape a migration that
-- attaches across many roots in one transaction produces.
ALTER TABLE public.__reflex_partition_pending
    DISABLE TRIGGER __reflex_partition_flush_trigger;

DO $$
DECLARE _roots INT;
BEGIN
    SELECT roots INTO _roots FROM rfx_subxid_cfg;
    FOR k IN 1.._roots LOOP
        EXECUTE format(
            'CREATE TABLE rfxsx_src_%s_p1 PARTITION OF rfxsx_src_%s FOR VALUES IN (1)', k, k);
        EXECUTE format(
            'INSERT INTO rfxsx_src_%s (id, bucket, amount) '
            || 'SELECT 1000 + i, 1, i FROM generate_series(1, 50) AS i', k);
    END LOOP;
END $$;

SELECT count(*) AS pending_roots FROM public.__reflex_partition_pending;

-- txn A
INSERT INTO rfx_subxid_probe (x0, x1) VALUES (pg_current_xact_id(), NULL);

-- txn B — the transaction under measurement
SELECT left(public.reflex_flush_partitions(), 60) AS flush_result;

-- txn C
UPDATE rfx_subxid_probe SET x1 = pg_current_xact_id();

ALTER TABLE public.__reflex_partition_pending
    ENABLE TRIGGER __reflex_partition_flush_trigger;

DO $$
DECLARE _roots INT; _gap NUMERIC;
BEGIN
    SELECT roots INTO _roots FROM rfx_subxid_cfg;
    SELECT (x1::TEXT::NUMERIC - x0::TEXT::NUMERIC - 1) INTO _gap FROM rfx_subxid_probe;
    RAISE NOTICE 'RFXSUBXID|%|%', _roots, _gap;
END $$;
