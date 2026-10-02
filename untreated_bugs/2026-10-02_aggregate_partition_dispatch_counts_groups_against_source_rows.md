# Aggregate partition dispatch compares dirty GROUPS against source ROWS — S3 (perf, to evaluate)

Found while fixing the passthrough UPDATE dirty count (1.11.5). Not the same bug: there is
no UNION collapse here, the units differ.

`build_partition_aware_dispatch_sql_strategy` (src/trigger/dispatch.rs) counts `dirty` per
child as rows of `__reflex_affected_<view>`, which the aggregate UPDATE arm
(`aggregate_update_stmts`, src/trigger/ops.rs) fills from the netted scratch: ONE ROW PER
AFFECTED GROUP. The denominator `__reflex_rebuild_cost_rows` sizes an aggregate IMV's child by
the matching anchor SOURCE child (rows; new in 1.11.5, commit 02c603e — 1.11.4 divided by
the intermediate child's `reltuples`, i.e. groups). So the ratio is now
`groups changed / source rows`, and a plan with many source rows per group never goes hot:
e.g. 80k of 100k groups over 1M source rows was hot in 1.11.4 (0.8) and is cold now (0.08).

## Reproduction (pg17, branch sdd/r5-update-dirty)
Five plans of `rcu_source` (src/tests/pg_test_rebuild_cost.rs: LIST plan -> RANGE month,
2000 rows per plan, analyzed), IMV `SELECT plan, m, SUM(v) AS s FROM src GROUP BY plan, m`
(12 groups per plan). `UPDATE src SET v = v + 1 WHERE plan = 1` (100% of the plan): the
plan's target leaf OIDs are unchanged (cold), ratio 12 / 2000 = 0.006. The result is correct.
(This small fixture is cold in 1.11.4 too, through the 1000-row floor: 12 / 1000; the
regression shows only on partitions with more than `wipe_floor_rows` groups.)

## To evaluate before fixing
After the scratch fill the cold MERGE touches only the changed groups, so for an aggregate
the hot rebuild may rarely pay off once the scratch is built — the right fix may be to count
source rows (transition-table rows per partition, as the passthrough arm now does) or to size
the denominator in groups, or to document that aggregate dispatch only goes hot for
near-row-grained groups. Benchmark first.
