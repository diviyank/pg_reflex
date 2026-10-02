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

## Review assessment (r5 fix round 1)
- Cost: a plan that should have gone hot is maintained incrementally instead; estimated 1-3x the
  hot path's time for such a flush. Perf only, results stay correct.
- Two-level partitions are a net improvement over 1.11.4 (whose plan-level child was sized by its
  own `reltuples` of 0 / -1, i.e. always the 1000-row floor), so the 1.11.5 sizing is kept.
- Do NOT switch the numerator to dirty SOURCE rows (as the passthrough arm now does) while a
  rebuild can be double-applied; with IMMEDIATE dispatch now always cold (1.11.5) that risk is
  confined to the DEFERRED flush, which nets first, but re-check before changing.
- Alternative: size aggregate children by their own intermediate leaf sum (groups), matching the
  numerator's unit.

## To evaluate before fixing
Only the DEFERRED flush dispatches since 1.11.5 (statement triggers stay incremental).
After the scratch fill the cold MERGE touches only the changed groups, so for an aggregate
the hot rebuild may rarely pay off once the scratch is built — the right fix may be to count
source rows (transition-table rows per partition, as the passthrough arm now does) or to size
the denominator in groups, or to document that aggregate dispatch only goes hot for
near-row-grained groups. Benchmark first.
