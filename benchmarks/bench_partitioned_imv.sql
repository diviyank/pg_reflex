-- Bench harness for the partitioned-IMV pipeline introduced in
-- plans/partitioning_3.md (1.5.3).
--
-- Six scenarios driving a 4-way LIST-partitioned source.  Each scenario
-- reports wall-clock for the trigger-driven path and (where applicable)
-- the equivalent global-reconcile baseline.
--
-- Run from psql:
--     \i benchmarks/bench_partitioned_imv.sql

\timing off
SET client_min_messages TO 'NOTICE';

DROP TABLE IF EXISTS part_bench CASCADE;
DROP TABLE IF EXISTS part_bench_fact2 CASCADE;
DROP TABLE IF EXISTS part_bench_dim2 CASCADE;

CREATE TABLE part_bench (
    id     BIGINT NOT NULL,
    region TEXT   NOT NULL,
    amount NUMERIC
) PARTITION BY LIST (region);

CREATE TABLE part_bench_a PARTITION OF part_bench FOR VALUES IN ('A');
CREATE TABLE part_bench_b PARTITION OF part_bench FOR VALUES IN ('B');
CREATE TABLE part_bench_c PARTITION OF part_bench FOR VALUES IN ('C');
CREATE TABLE part_bench_d PARTITION OF part_bench FOR VALUES IN ('D');

\echo '--- Seeding 10M rows / 4 partitions (clustered, 2.5M each) ---'
INSERT INTO part_bench (id, region, amount)
SELECT
    i,
    CASE ((i - 1) % 4)
        WHEN 0 THEN 'A'
        WHEN 1 THEN 'B'
        WHEN 2 THEN 'C'
        ELSE        'D'
    END,
    (random() * 1000)::NUMERIC(10, 2)
FROM generate_series(1, 10000000) AS i;

ANALYZE part_bench;

SELECT public.create_reflex_ivm(
    'part_bench_v',
    'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM part_bench GROUP BY region',
    NULL, 'UNLOGGED', 'IMMEDIATE', NULL,
    ARRAY['region']
);
ANALYZE part_bench_v;
ANALYZE "__reflex_intermediate_part_bench_v";

\echo ''
\echo '=== Scenario 1: manual reflex_reconcile_partition vs reflex_reconcile ==='
\echo 'Phase A win — swap-based partition reconcile vs global reconcile.'
DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    _t0 := clock_timestamp();
    PERFORM public.reflex_reconcile_partition('part_bench_v', 'A');
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE '[S1a] partition reconcile (A):  % ms', round(_ms, 1);

    _t0 := clock_timestamp();
    PERFORM public.reflex_reconcile('part_bench_v');
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE '[S1b] global reconcile (all):   % ms', round(_ms, 1);
END $$;

\echo ''
\echo '=== Scenario 2: bulk UPDATE concentrated in one partition ==='
\echo 'wipe_threshold > scenario ratio → Path B is skipped; we hit the'
\echo 'post-scratch partition-aware dispatch.  Reports the trigger time.'
DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    -- Raise threshold above the source-level ratio (0.25) so Path B does
    -- NOT fire — the post-scratch dispatch then evaluates partition-by-
    -- partition.
    PERFORM public.reflex_set_wipe_threshold('part_bench_v', 0.30);
    -- And drop the floor so a 4-group IMV can still trip the per-
    -- partition ratio (1 affected group / 2.5M reltuples per partition
    -- = tiny without aggressive floor).
    PERFORM public.reflex_set_wipe_floor_rows('part_bench_v', 1);

    _t0 := clock_timestamp();
    UPDATE part_bench SET amount = amount + 1 WHERE region = 'A';
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE '[S2] UPDATE all-of-A (trigger fire): % ms', round(_ms, 1);

    PERFORM public.reflex_set_wipe_threshold('part_bench_v', NULL);
    PERFORM public.reflex_set_wipe_floor_rows('part_bench_v', NULL);
END $$;

\echo ''
\echo '=== Scenario 2b: high-cardinality IMV — partition dispatch should fire ==='
\echo 'The 4-region IMV in S2 has too few groups for the dispatch to fire.  Here'
\echo 'we group by (region, sub_grp) to give each partition 1000 groups, so a'
\echo 'concentrated UPDATE on one partition produces ~1000 dirty group keys'
\echo 'against 1000 reltuples-per-partition = ratio 1.0 → HOT → partition reconcile.'
DROP TABLE IF EXISTS part_bench_hc CASCADE;
CREATE TABLE part_bench_hc (
    id     BIGINT NOT NULL,
    region TEXT   NOT NULL,
    sub_grp INT   NOT NULL,
    amount NUMERIC
) PARTITION BY LIST (region);
CREATE TABLE part_bench_hc_a PARTITION OF part_bench_hc FOR VALUES IN ('A');
CREATE TABLE part_bench_hc_b PARTITION OF part_bench_hc FOR VALUES IN ('B');
CREATE TABLE part_bench_hc_c PARTITION OF part_bench_hc FOR VALUES IN ('C');
CREATE TABLE part_bench_hc_d PARTITION OF part_bench_hc FOR VALUES IN ('D');
INSERT INTO part_bench_hc (id, region, sub_grp, amount)
SELECT
    i,
    CASE ((i - 1) % 4) WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' ELSE 'D' END,
    (i % 1000),
    (random() * 1000)::NUMERIC(10, 2)
FROM generate_series(1, 4000000) AS i;
ANALYZE part_bench_hc;
SELECT public.create_reflex_ivm(
    'part_bench_hc_v',
    'SELECT region, sub_grp, SUM(amount) AS total FROM part_bench_hc GROUP BY region, sub_grp',
    NULL, 'UNLOGGED', 'IMMEDIATE', NULL,
    ARRAY['region']
);
ANALYZE part_bench_hc_v;
ANALYZE "__reflex_intermediate_part_bench_hc_v";

DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    -- Variant A: threshold = 2.0 → never trips → standard incremental MERGE
    -- (this is what the dispatch chooses on cold partitions, and what the
    -- legacy code would do without per-partition routing).
    PERFORM public.reflex_set_wipe_threshold('part_bench_hc_v', 2.0);
    _t0 := clock_timestamp();
    UPDATE part_bench_hc SET amount = amount + 1 WHERE region = 'A';
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE '[S2b-cold] high-card UPDATE (no dispatch, standard MERGE): % ms', round(_ms, 1);

    -- Reset target so the next UPDATE has the same starting state.
    PERFORM public.reflex_reconcile('part_bench_hc_v');

    -- Variant B: default threshold 0.5 → A trips → partition_reconcile.
    PERFORM public.reflex_set_wipe_threshold('part_bench_hc_v', NULL);
    _t0 := clock_timestamp();
    UPDATE part_bench_hc SET amount = amount + 1 WHERE region = 'A';
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE '[S2b-hot] high-card UPDATE (partition dispatch fires): % ms', round(_ms, 1);
END $$;
SELECT public.drop_reflex_ivm('part_bench_hc_v', TRUE);
DROP TABLE part_bench_hc CASCADE;

\echo ''
\echo '=== Scenario 3: bulk UPDATE on a JOIN-secondary source (Tier 2) ==='
\echo 'Setup: fact JOINs to partitioned dim.  Trigger fires on fact.'
CREATE TABLE part_bench_dim2 (
    region TEXT NOT NULL,
    label  TEXT,
    PRIMARY KEY (region)
) PARTITION BY LIST (region);
CREATE TABLE part_bench_dim2_a PARTITION OF part_bench_dim2 FOR VALUES IN ('A');
CREATE TABLE part_bench_dim2_b PARTITION OF part_bench_dim2 FOR VALUES IN ('B');
CREATE TABLE part_bench_dim2_c PARTITION OF part_bench_dim2 FOR VALUES IN ('C');
CREATE TABLE part_bench_dim2_d PARTITION OF part_bench_dim2 FOR VALUES IN ('D');
INSERT INTO part_bench_dim2 VALUES ('A', 'A-label'), ('B', 'B-label'),
                                   ('C', 'C-label'), ('D', 'D-label');
CREATE TABLE part_bench_fact2 (
    id  BIGINT NOT NULL,
    src_region TEXT NOT NULL,
    qty NUMERIC,
    PRIMARY KEY (id, src_region)
);
INSERT INTO part_bench_fact2 (id, src_region, qty)
SELECT
    i,
    CASE ((i - 1) % 4)
        WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' ELSE 'D'
    END,
    (random() * 100)::NUMERIC(10, 2)
FROM generate_series(1, 1000000) AS i;
ANALYZE part_bench_fact2;
ANALYZE part_bench_dim2;
SELECT public.create_reflex_ivm(
    'part_bench_t2_v',
    'SELECT d.region AS region, SUM(f.qty) AS total FROM part_bench_fact2 f JOIN part_bench_dim2 d ON f.src_region = d.region GROUP BY d.region',
    NULL, 'UNLOGGED', 'IMMEDIATE', NULL,
    ARRAY['region']
);
ANALYZE part_bench_t2_v;
DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    _t0 := clock_timestamp();
    UPDATE part_bench_fact2 SET qty = qty + 1 WHERE src_region = 'A';
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE '[S3] tier-2 UPDATE on A (250k rows): % ms', round(_ms, 1);
END $$;

\echo ''
\echo '=== Scenario 5: all partitions hot (trip-cap fallback) ==='
\echo 'wipe_threshold = 0.01 + low floor → every partition trips → trip-cap'
\echo 'falls back to global reflex_reconcile.'
DO $$
DECLARE _t0 TIMESTAMPTZ; _ms NUMERIC;
BEGIN
    PERFORM public.reflex_reconcile('part_bench_v');
    PERFORM public.reflex_set_wipe_threshold('part_bench_v', 0.01);
    PERFORM public.reflex_set_wipe_floor_rows('part_bench_v', 1);

    _t0 := clock_timestamp();
    UPDATE part_bench SET amount = amount + 1;
    _ms := EXTRACT(EPOCH FROM clock_timestamp() - _t0) * 1000;
    RAISE NOTICE '[S5] UPDATE all rows (trip-cap): % ms', round(_ms, 1);

    PERFORM public.reflex_set_wipe_threshold('part_bench_v', NULL);
    PERFORM public.reflex_set_wipe_floor_rows('part_bench_v', NULL);
END $$;

\echo ''
\echo '=== Scenario 6: cascade post-swap (correctness check only) ==='
SELECT public.create_reflex_ivm(
    'part_bench_csw_c',
    'SELECT region, SUM(total) AS doubled FROM part_bench_v GROUP BY region',
    NULL, 'UNLOGGED', 'IMMEDIATE', NULL,
    ARRAY['region']
);
DO $$
DECLARE _ok BOOLEAN;
BEGIN
    UPDATE part_bench_v SET total = 999 WHERE region = 'A';
    UPDATE part_bench_csw_c SET doubled = 9999 WHERE region = 'A';
    PERFORM public.reflex_reconcile_partition('part_bench_v', 'A');
    SELECT (parent.total = child.doubled) INTO _ok
    FROM part_bench_v parent
    JOIN part_bench_csw_c child ON parent.region = child.region
    WHERE parent.region = 'A';
    IF _ok IS NOT TRUE THEN
        RAISE EXCEPTION '[S6] cascade did NOT propagate to child IMV for A';
    END IF;
    RAISE NOTICE '[S6] cascade post-swap: parent and child agree';
END $$;

\echo ''
\echo '=== Cleanup ==='
SELECT public.drop_reflex_ivm('part_bench_csw_c', TRUE);
SELECT public.drop_reflex_ivm('part_bench_v', TRUE);
SELECT public.drop_reflex_ivm('part_bench_t2_v', TRUE);
DROP TABLE IF EXISTS part_bench_fact2 CASCADE;
DROP TABLE IF EXISTS part_bench_dim2 CASCADE;
DROP TABLE IF EXISTS part_bench CASCADE;

\echo ''
\echo 'Bench complete.  See plans/partitioning_3.md §5 for interpretation.'
