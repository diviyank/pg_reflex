\set ON_ERROR_STOP on
\if :{?RUN_TS}
\else
\set RUN_TS '2026-06-05 12:00:00+00'
\endif

\echo '### Invalid cells (correctness failures — must be empty) ###'
SELECT layer, cell_label, mismatches FROM bench_gap_results
 WHERE mismatches IS NOT NULL AND mismatches <> 0 ORDER BY 1,2;

\echo ''
\echo '### Ranked gaps (advantage_pct < 0: IMV slower than bare+REFRESH) ###'
SELECT
    round(advantage_pct,1) AS adv_pct,
    layer, operation, shape,
    CASE WHEN cte THEN 'CTE' ELSE '-' END AS cte,
    mode, partitioned, base_rows, edit_rows,
    imv_ms, bare_ms, refresh_ms,
    CASE
        WHEN operation IN ('FLIP_OUT','FLIP_IN') THEN 'filter-flip full recompute (BOOL_OR/MIN/MAX or wipe path)'
        WHEN shape='classic' AND edit_rows >= 50000 THEN 'large-batch MERGE crossover'
        WHEN partitioned='LIST' THEN 'dual-table / per-partition dispatch overhead'
        WHEN cte THEN 'CTE-chain COMMIT-cascade flush cost'
        WHEN operation='INSERT' AND edit_rows >= 50000 THEN 'large-batch INSERT crossover'
        ELSE 'unclassified — investigate'
    END AS hypothesis
FROM bench_gap_results
WHERE advantage_pct < 0 AND (mismatches IS NULL OR mismatches = 0)
ORDER BY advantage_pct ASC;

\echo ''
\echo '### Raw matrix (all cells) ###'
SELECT layer, operation, shape, cte, mode, partitioned, base_rows, edit_rows,
       imv_ms, bare_ms, refresh_ms, advantage_pct, mismatches, note
FROM bench_gap_results ORDER BY layer, shape, cte, mode, partitioned, operation, edit_rows;
