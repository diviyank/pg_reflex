-- ============================================================================
-- pg_reflex — partition-count scaling benchmark (one N per invocation)
-- ============================================================================
--
-- WHAT IT MEASURES
--
-- Every metric below is designed so that the amount of *data* is held constant
-- while N — the number of partitions — varies.  Total row count, rows touched
-- per change, and the number of leaves a correct implementation must visit are
-- all independent of N.  Therefore any growth in a timing as N grows is
-- per-child overhead, and the shape of that growth (flat / linear / quadratic)
-- is read directly off the log-log slope the driver prints.
--
-- This is the point of the harness: raw totals cannot distinguish "the fixture
-- got bigger" from "the code got worse".  A constant-data sweep can.
--
--   flush_deferred           reflex_flush_deferred() after a routine one-leaf
--                            UPDATE on a DEFERRED partitioned PASSTHROUGH IMV
--   flush_txn                the same change as a whole transaction, i.e.
--                            including the COMMIT-time maintenance
--   reconcile_partition_one  reflex_reconcile_partition() for ONE leaf
--   sync_partitions          reflex_sync_partitions() on an already-in-sync tree
--   sync_partitions_pt       the same, on the passthrough IMV
--   reconcile_full           reflex_reconcile() on a partitioned aggregate IMV
--   attach_txn               BEGIN; CREATE TABLE .. PARTITION OF ..; INSERT; COMMIT
--                            measured ACROSS the COMMIT, where the reconcile lands
--   attach_txn_pt            the same, on the passthrough source
--
-- EXPECTED SHAPES
--
--   flush_deferred, flush_txn                 FLAT.  The change is confined to
--       one leaf and is the same size at every N, so a build that prunes
--       correctly does the same work at N=10 and N=200.  These are the
--       1.11.1-shaped metrics: an unprunable membership predicate turns them
--       linear in N, which is exactly the regression that shipped.
--   sync_partitions, reconcile_full, attach_*  LINEAR.  These legitimately
--       visit every child; the question is only whether the per-child constant
--       is bounded, i.e. whether the slope stays near 1 and never reaches 2.
--   reconcile_partition_one                   LINEAR at worst, and only
--       because of the O(tree) pre-sync — the leaf's own work SHRINKS with N.
--
-- ORDERING IS LOAD-BEARING
--
-- The attach metrics are destructive: they add partitions and leave rows in
-- __reflex_partition_pending / __reflex_source_partition_snapshot.  They run
-- LAST so they cannot contaminate the flush metrics, which are the most
-- sensitive.  Every metric discards a warm-up repetition, because the first
-- call of each kind pays one-off plan-cache and catalog-cache costs that are
-- not a function of N and would otherwise be attributed to it.
--
-- HOW TO RUN
--
-- Normally you do not invoke this file directly — the driver sweeps N for you
-- and does the linear/superlinear arithmetic:
--
--     ./benchmarks/bench_partition_scaling.sh --label $(git rev-parse --short HEAD)
--
-- To run a single N by hand, against a database with pg_reflex installed:
--
--     psql -d rfxbench \
--          -v n=50 -v total_rows=40000 -v reps=5 \
--          -f benchmarks/bench_partition_scaling.sql
--
-- Rebuild and reinstall the extension between commits under test, and always
-- record which commit the loaded .so was built from — the driver writes that
-- into the results file for you.
--
-- Output lines the driver parses look like:
--     NOTICE:  RFXBENCH|<metric>|<n>|<rep>|<milliseconds>
-- ============================================================================

\set ON_ERROR_STOP on
\timing off
SET client_min_messages TO NOTICE;

\if :{?n}          \else \set n 10           \endif
\if :{?total_rows} \else \set total_rows 40000 \endif
\if :{?reps}       \else \set reps 5         \endif

\echo '--- pg_reflex version under test ---'
SELECT extversion AS pg_reflex_version FROM pg_extension WHERE extname = 'pg_reflex';

-- ---------------------------------------------------------------------------
-- Config + emit plumbing.  psql does not interpolate :vars inside dollar-quoted
-- bodies, so N reaches the PL/pgSQL blocks through this table instead.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS rfx_bench_cfg;
CREATE TABLE rfx_bench_cfg AS
    SELECT :n::INT AS n, :total_rows::INT AS total_rows, :reps::INT AS reps;

DROP TABLE IF EXISTS rfx_bench_clock;
CREATE TABLE rfx_bench_clock (t TIMESTAMPTZ);

CREATE OR REPLACE FUNCTION rfx_bench_emit(_metric TEXT, _rep INT, _ms NUMERIC)
RETURNS VOID LANGUAGE plpgsql AS $fn$
DECLARE _n INT;
BEGIN
    -- rep 0 is the discarded warm-up
    IF _rep < 1 THEN RETURN; END IF;
    SELECT n INTO _n FROM rfx_bench_cfg;
    RAISE NOTICE 'RFXBENCH|%|%|%|%', _metric, _n, _rep, round(_ms, 3);
END $fn$;

-- Cross-transaction stopwatch: mark() commits t0 in its own transaction, so a
-- later lap() picks up everything the measured transaction did INCLUDING its
-- COMMIT.  A DO block cannot see its own COMMIT, which is exactly where the
-- DEFERRED maintenance runs, so the timing has to leave the transaction.
CREATE OR REPLACE FUNCTION rfx_bench_mark() RETURNS VOID LANGUAGE plpgsql AS $fn$
BEGIN
    DELETE FROM rfx_bench_clock;
    INSERT INTO rfx_bench_clock VALUES (clock_timestamp());
END $fn$;

CREATE OR REPLACE FUNCTION rfx_bench_lap(_metric TEXT, _rep INT)
RETURNS VOID LANGUAGE plpgsql AS $fn$
DECLARE _ms NUMERIC;
BEGIN
    SELECT EXTRACT(EPOCH FROM clock_timestamp() - t) * 1000 INTO _ms FROM rfx_bench_clock;
    PERFORM rfx_bench_emit(_metric, _rep, _ms);
END $fn$;

-- One-leaf UPDATE of a fixed 100 rows at every N: bucket 0 holds ids
-- 1, n+1, 2n+1, ... so `id <= 100 * n` is exactly 100 rows in one leaf.
CREATE OR REPLACE FUNCTION rfx_bench_touch_one_leaf() RETURNS VOID LANGUAGE plpgsql AS $fn$
BEGIN
    UPDATE rfxsc_ptsrc SET amount = amount + 1
     WHERE bucket = 0 AND id <= 100 * (SELECT n FROM rfx_bench_cfg);
END $fn$;

-- ---------------------------------------------------------------------------
-- Fixture.  Two sources so the aggregate (IMMEDIATE) and passthrough (DEFERRED)
-- paths are attributable separately.  Each carries :total_rows rows spread
-- round-robin over N leaves, so total data is constant across the sweep and
-- per-leaf data shrinks as N grows.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS rfxsc_agg CASCADE;
DROP TABLE IF EXISTS rfxsc_pt CASCADE;
DROP TABLE IF EXISTS rfxsc_src CASCADE;
DROP TABLE IF EXISTS rfxsc_ptsrc CASCADE;

SELECT setseed(0.42);

CREATE TABLE rfxsc_src (
    id     BIGINT  NOT NULL,
    bucket INT     NOT NULL,
    region TEXT,
    amount NUMERIC NOT NULL
) PARTITION BY LIST (bucket);

CREATE TABLE rfxsc_ptsrc (
    id     BIGINT  NOT NULL,
    bucket INT     NOT NULL,
    region TEXT,
    amount NUMERIC NOT NULL
) PARTITION BY LIST (bucket);

DO $$
DECLARE _n INT;
BEGIN
    SELECT n INTO _n FROM rfx_bench_cfg;
    FOR p IN 0.._n - 1 LOOP
        EXECUTE format('CREATE TABLE rfxsc_src_%s PARTITION OF rfxsc_src '
                       || 'FOR VALUES IN (%s) WITH (autovacuum_enabled = false)', p, p);
        EXECUTE format('CREATE TABLE rfxsc_ptsrc_%s PARTITION OF rfxsc_ptsrc '
                       || 'FOR VALUES IN (%s) WITH (autovacuum_enabled = false)', p, p);
    END LOOP;
END $$;

INSERT INTO rfxsc_src (id, bucket, region, amount)
SELECT i, (i - 1) % (SELECT n FROM rfx_bench_cfg),
       CASE WHEN i % 7 = 0 THEN NULL ELSE 'r' || (i % 5) END,
       (random() * 1000)::NUMERIC(10, 2)
FROM generate_series(1, (SELECT total_rows FROM rfx_bench_cfg)) AS i;

INSERT INTO rfxsc_ptsrc (id, bucket, region, amount)
SELECT i, (i - 1) % (SELECT n FROM rfx_bench_cfg),
       CASE WHEN i % 7 = 0 THEN NULL ELSE 'r' || (i % 5) END,
       (random() * 1000)::NUMERIC(10, 2)
FROM generate_series(1, (SELECT total_rows FROM rfx_bench_cfg)) AS i;

ANALYZE rfxsc_src;
ANALYZE rfxsc_ptsrc;

SELECT public.create_reflex_ivm(
    'rfxsc_agg',
    'SELECT bucket, SUM(amount) AS total, COUNT(*) AS cnt FROM rfxsc_src GROUP BY bucket',
    NULL, 'UNLOGGED', 'IMMEDIATE', NULL,
    ARRAY['bucket']
);

SELECT public.create_reflex_ivm(
    'rfxsc_pt',
    'SELECT id, bucket, region, amount FROM rfxsc_ptsrc',
    'id,bucket', 'UNLOGGED', 'DEFERRED', NULL,
    ARRAY['bucket']
);

ANALYZE rfxsc_agg;
ANALYZE rfxsc_pt;

\echo '--- fixture built ---'
SELECT (SELECT n FROM rfx_bench_cfg)                              AS n_partitions,
       (SELECT count(*) FROM rfxsc_src)                           AS src_rows,
       (SELECT count(*) FROM rfxsc_agg)                           AS agg_rows,
       (SELECT count(*) FROM rfxsc_pt)                            AS pt_rows,
       (SELECT count(*) FROM pg_inherits
         WHERE inhparent = 'rfxsc_agg'::REGCLASS)                 AS agg_leaves,
       (SELECT count(*) FROM pg_inherits
         WHERE inhparent = 'rfxsc_pt'::REGCLASS)                  AS pt_leaves;

-- ===========================================================================
-- M1 flush_deferred — routine one-leaf change, maintenance measured alone.
--
-- 100 rows change, in one leaf, at every N.  A build that prunes to that leaf
-- is FLAT in N.  The 1.11.1 cold-DELETE regression made the predicate
-- unprunable, so the flush executed every leaf of the IMV: linear in N.
-- ===========================================================================
DO $$
DECLARE _t0 TIMESTAMPTZ; _reps INT;
BEGIN
    SELECT reps INTO _reps FROM rfx_bench_cfg;
    FOR r IN 0.._reps LOOP
        PERFORM rfx_bench_touch_one_leaf();
        _t0 := clock_timestamp();
        PERFORM public.reflex_flush_deferred('rfxsc_ptsrc');
        PERFORM rfx_bench_emit('flush_deferred', r,
                               EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000);
    END LOOP;
END $$;

-- ===========================================================================
-- M2 flush_txn — the same one-leaf change as a whole transaction, so the
-- COMMIT-time maintenance is inside the measurement.  Also FLAT in N.
-- Unrolled at psql level: a DO block cannot span its own COMMIT.
-- ===========================================================================
SELECT rfx_bench_mark();
BEGIN;
SELECT rfx_bench_touch_one_leaf();
COMMIT;
SELECT rfx_bench_lap('flush_txn', 0);

SELECT rfx_bench_mark();
BEGIN;
SELECT rfx_bench_touch_one_leaf();
COMMIT;
SELECT rfx_bench_lap('flush_txn', 1);

SELECT rfx_bench_mark();
BEGIN;
SELECT rfx_bench_touch_one_leaf();
COMMIT;
SELECT rfx_bench_lap('flush_txn', 2);

SELECT rfx_bench_mark();
BEGIN;
SELECT rfx_bench_touch_one_leaf();
COMMIT;
SELECT rfx_bench_lap('flush_txn', 3);

SELECT rfx_bench_mark();
BEGIN;
SELECT rfx_bench_touch_one_leaf();
COMMIT;
SELECT rfx_bench_lap('flush_txn', 4);

SELECT rfx_bench_mark();
BEGIN;
SELECT rfx_bench_touch_one_leaf();
COMMIT;
SELECT rfx_bench_lap('flush_txn', 5);

-- ===========================================================================
-- M3 reconcile_partition_one — ONE leaf.  The work on the leaf itself shrinks
-- as N grows (constant total data), so anything but a falling curve is
-- per-child overhead in the pre-sync / swap-set derivation.
-- ===========================================================================
DO $$
DECLARE _t0 TIMESTAMPTZ; _reps INT;
BEGIN
    SELECT reps INTO _reps FROM rfx_bench_cfg;
    FOR r IN 0.._reps LOOP
        _t0 := clock_timestamp();
        PERFORM public.reflex_reconcile_partition('rfxsc_agg', '0');
        PERFORM rfx_bench_emit('reconcile_partition_one', r,
                               EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000);
    END LOOP;
END $$;

-- ===========================================================================
-- M4 sync_partitions — tree already in sync, so this is pure probe cost
-- ===========================================================================
DO $$
DECLARE _t0 TIMESTAMPTZ; _reps INT;
BEGIN
    SELECT reps INTO _reps FROM rfx_bench_cfg;
    FOR r IN 0.._reps LOOP
        _t0 := clock_timestamp();
        PERFORM public.reflex_sync_partitions('rfxsc_agg');
        PERFORM rfx_bench_emit('sync_partitions', r,
                               EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000);
    END LOOP;
END $$;

DO $$
DECLARE _t0 TIMESTAMPTZ; _reps INT;
BEGIN
    SELECT reps INTO _reps FROM rfx_bench_cfg;
    FOR r IN 0.._reps LOOP
        _t0 := clock_timestamp();
        PERFORM public.reflex_sync_partitions('rfxsc_pt');
        PERFORM rfx_bench_emit('sync_partitions_pt', r,
                               EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000);
    END LOOP;
END $$;

-- ===========================================================================
-- M5 reconcile_full — full reflex_reconcile on the partitioned aggregate IMV
-- ===========================================================================
-- Capped at 3 measured reps: this is by far the most expensive metric and at
-- N=200 a full sweep would otherwise dominate the run time.
DO $$
DECLARE _t0 TIMESTAMPTZ; _reps INT;
BEGIN
    SELECT LEAST(reps, 3) INTO _reps FROM rfx_bench_cfg;
    FOR r IN 0.._reps LOOP
        _t0 := clock_timestamp();
        PERFORM public.reflex_reconcile('rfxsc_agg');
        PERFORM rfx_bench_emit('reconcile_full', r,
                               EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000);
    END LOOP;
END $$;

-- ===========================================================================
-- M6 attach_txn — attach one new source partition and load it, measured over
-- the whole transaction because the reconcile lands in the COMMIT.
-- Rows loaded are constant (200), so only the per-child cost varies with N.
--
-- DESTRUCTIVE, and therefore last: each rep raises the partition count by one
-- and leaves pending/snapshot rows behind.
-- ===========================================================================
SELECT rfx_bench_mark();
BEGIN;
CREATE TABLE rfxsc_src_new0 PARTITION OF rfxsc_src FOR VALUES IN (900000);
INSERT INTO rfxsc_src_new0 (id, bucket, region, amount)
SELECT 90000000 + i, 900000, 'r1', i FROM generate_series(1, 200) AS i;
COMMIT;
SELECT rfx_bench_lap('attach_txn', 0);

SELECT rfx_bench_mark();
BEGIN;
CREATE TABLE rfxsc_src_new1 PARTITION OF rfxsc_src FOR VALUES IN (900001);
INSERT INTO rfxsc_src_new1 (id, bucket, region, amount)
SELECT 90000100 + i, 900001, 'r1', i FROM generate_series(1, 200) AS i;
COMMIT;
SELECT rfx_bench_lap('attach_txn', 1);

SELECT rfx_bench_mark();
BEGIN;
CREATE TABLE rfxsc_src_new2 PARTITION OF rfxsc_src FOR VALUES IN (900002);
INSERT INTO rfxsc_src_new2 (id, bucket, region, amount)
SELECT 90000200 + i, 900002, 'r1', i FROM generate_series(1, 200) AS i;
COMMIT;
SELECT rfx_bench_lap('attach_txn', 2);

SELECT rfx_bench_mark();
BEGIN;
CREATE TABLE rfxsc_src_new3 PARTITION OF rfxsc_src FOR VALUES IN (900003);
INSERT INTO rfxsc_src_new3 (id, bucket, region, amount)
SELECT 90000300 + i, 900003, 'r1', i FROM generate_series(1, 200) AS i;
COMMIT;
SELECT rfx_bench_lap('attach_txn', 3);

-- ===========================================================================
-- M7 attach_txn_pt — same, on the DEFERRED passthrough source
-- ===========================================================================
SELECT rfx_bench_mark();
BEGIN;
CREATE TABLE rfxsc_ptsrc_new0 PARTITION OF rfxsc_ptsrc FOR VALUES IN (900000);
INSERT INTO rfxsc_ptsrc_new0 (id, bucket, region, amount)
SELECT 90000000 + i, 900000, 'r1', i FROM generate_series(1, 200) AS i;
COMMIT;
SELECT rfx_bench_lap('attach_txn_pt', 0);

SELECT rfx_bench_mark();
BEGIN;
CREATE TABLE rfxsc_ptsrc_new1 PARTITION OF rfxsc_ptsrc FOR VALUES IN (900001);
INSERT INTO rfxsc_ptsrc_new1 (id, bucket, region, amount)
SELECT 90000100 + i, 900001, 'r1', i FROM generate_series(1, 200) AS i;
COMMIT;
SELECT rfx_bench_lap('attach_txn_pt', 1);

SELECT rfx_bench_mark();
BEGIN;
CREATE TABLE rfxsc_ptsrc_new2 PARTITION OF rfxsc_ptsrc FOR VALUES IN (900002);
INSERT INTO rfxsc_ptsrc_new2 (id, bucket, region, amount)
SELECT 90000200 + i, 900002, 'r1', i FROM generate_series(1, 200) AS i;
COMMIT;
SELECT rfx_bench_lap('attach_txn_pt', 2);

SELECT rfx_bench_mark();
BEGIN;
CREATE TABLE rfxsc_ptsrc_new3 PARTITION OF rfxsc_ptsrc FOR VALUES IN (900003);
INSERT INTO rfxsc_ptsrc_new3 (id, bucket, region, amount)
SELECT 90000300 + i, 900003, 'r1', i FROM generate_series(1, 200) AS i;
COMMIT;
SELECT rfx_bench_lap('attach_txn_pt', 3);

-- ===========================================================================
-- Correctness oracle.  A benchmark that blessed a wrong-but-fast build would
-- be worse than no benchmark: bidirectional EXCEPT ALL against the base query.
-- ===========================================================================
DO $$
DECLARE _diff BIGINT;
BEGIN
    SELECT count(*) INTO _diff FROM (
        (SELECT bucket, SUM(amount) AS total, COUNT(*) AS cnt FROM rfxsc_src GROUP BY bucket
         EXCEPT ALL
         SELECT bucket, total, cnt FROM rfxsc_agg)
        UNION ALL
        (SELECT bucket, total, cnt FROM rfxsc_agg
         EXCEPT ALL
         SELECT bucket, SUM(amount) AS total, COUNT(*) AS cnt FROM rfxsc_src GROUP BY bucket)
    ) d;
    RAISE NOTICE 'RFXCHECK|rfxsc_agg|%', _diff;

    SELECT count(*) INTO _diff FROM (
        (SELECT id, bucket, region, amount FROM rfxsc_ptsrc
         EXCEPT ALL
         SELECT id, bucket, region, amount FROM rfxsc_pt)
        UNION ALL
        (SELECT id, bucket, region, amount FROM rfxsc_pt
         EXCEPT ALL
         SELECT id, bucket, region, amount FROM rfxsc_ptsrc)
    ) d;
    RAISE NOTICE 'RFXCHECK|rfxsc_pt|%', _diff;
END $$;
