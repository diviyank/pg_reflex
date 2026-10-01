# Hot/cold dispatch classifies a two-level mirror at the plan level, not the leaf — S3 (perf)

Narrowed in 1.11.5. The sizing half is fixed: the classifier's denominator is
`__reflex_rebuild_cost_rows(view, child)` (src/rebuild_diff.rs:430), the sum of the leaf
`reltuples` under the child (file-size fallback when unanalyzed), instead of the partitioned
child's own `reltuples` (0 / -1, so always the 1000-row floor). A small change to a large plan
now stays cold (`pg_rco_small_delete_of_two_level_plan_stays_cold`).

## Residual
`build_partition_aware_dispatch_sql_strategy` (aggregate, src/trigger/dispatch.rs:325) and
`build_passthrough_partition_dispatch_sql` (src/trigger/dispatch.rs:557) still resolve each dirty row to a child through
`__reflex_partition_child_for_key(parent, <first partition column>, key)`. On a two-level mirror
(e.g. `sop_forecast_view` partitioned `[dem_plan_id, order_date]`) that child is the plan-level
node, so the unit of classification — and of a hot rebuild — is the whole plan (~10M rows in
prod). A change concentrated in one plan-month, large relative to that leaf but small relative
to the plan, stays cold (incremental), and one large enough relative to the plan rebuilds every
month of it.

Correctness is unaffected either way (cold is the incremental path; hot rebuilds the plan, as a
leaf diff when it has dependents).

## Fix direction
Resolve the dirty rows to leaf partitions (all partition columns) and classify each leaf by its
own `reltuples`, so a change confined to one plan-month rebuilds only that leaf. Must fail toward
doing the full work when a leaf cannot be resolved.
