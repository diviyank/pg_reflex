# Passthrough UPDATE dispatch collapses the dirty count to distinct partition values — S3 (perf)

`src/trigger/ops.rs:1211` builds the partitioned passthrough UPDATE `affected` set as
`SELECT {sc}::text AS pkey FROM {old} UNION SELECT {sc}::text AS pkey FROM {new}`. `UNION`
deduplicates, so the dispatch's `per_val` CTE (`count(*) ... GROUP BY pkey`) sees one row per
distinct partition value instead of one per changed row. `dirty` is therefore ~1 per partition,
`dirty / GREATEST(cost_rows, wipe_floor_rows)` never reaches the threshold, and a partitioned
passthrough UPDATE can never go hot, however large (e.g. 80% of one plan). The DELETE arm counts
`pt_old` rows directly and is not affected.

Correctness is unaffected (the cold path is the incremental one); the cost is that a bulk UPDATE
of a partition runs keyed delete + insert instead of the cheaper swap rebuild.

Pinned by the ignored test `pg_rco_large_update_goes_hot_without_dependent_passthrough`
(src/tests/pg_test_rebuild_cost.rs), RED today.

## Fix direction
Count dirty rows per partition value: e.g. `UNION ALL` of old and new keyed rows, or a COUNT per
value, without double-counting the old+new image of the same row (an UPDATE touching N rows
should contribute N, not 2N, unless the threshold is recalibrated for it). Remove the `#[ignore]`.
