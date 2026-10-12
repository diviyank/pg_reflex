# A one-leaf source partition swap fully reconciles the whole ignoring-dependent chain, repeatedly — S2 (perf cliff, aborts the client COMMIT)

Found in the field on 1.11.5 (PostgreSQL 17, dev database upgraded 2026-10-02). Results are
believed correct; the cost is what breaks: a push that committed in ~80 s on 1.11.4 no longer
fits a 200 s client timeout, so its COMMIT is cancelled and the push is lost every time.

## What happens

Source `sales_simulation` is LIST(`dem_plan_id`) × RANGE(month). The application replaces a
plan's months in one transaction (per month: build detached, `DETACH` old / `ATTACH` new /
`DROP` old / `RENAME` to the canonical name), so each leaf classifies as `SwapFill`.

`sop_forecast_view` (DEFERRED passthrough, two-level mirror, ~60 M rows, 34 plans) is rebuilt leaf
by leaf: 20 leaves of ~370 k rows, ~4.2 s each. That part is unchanged from 1.11.4.

Its dependents are `forecast_analysis_view__cte_forecast_bounds`,
`forecast_analysis_view__cte_forecast_sales` (generated sub-IMVs of `forecast_analysis_view`) and
`forecast_dp_year_agg`. After the leaf rebuild the swap cascades to them
(`reflex_reconcile_partition_impl`, src/partition.rs:2211-2365). The dependents did not take the
scoped branch: the session shows full reconciles of the chain, several nodes more than once,
for ONE swapped leaf:

```
reconciled IMV '…__cte_forecast_bounds' (partitioned, 34 children swapped)   x3
reconciled IMV '…__cte_date_limits'     (partitioned, 34 children swapped)   x2
reconciled IMV '…__cte_forecast_sales'  (partitioned, 34 children swapped)   x2
reconciled IMV '…__cte_history_bounds'                                       x2
reconciled IMV '…__cte_history_sales'   (partitioned, 35 children swapped)
reconciled IMV '…__cte_history_buckets'
reconciled IMV '…__cte_active_assortment'
reconciled IMV 'forecast_analysis_view' (partitioned, 34 children swapped)
reconciled IMV 'analysis_dp_year_agg'
```

34 children is every plan, not the one swapped. Three separate costs stack:

1. **Scope is lost.** One month of one plan changed; every plan of every chain node is rebuilt.
   Either the same-column call (`reflex_reconcile_partition(child, '', <rebuilt leaves>)`,
   partition.rs:2296) did not return `RECONCILED` for a depth-2 source leaf feeding a dependent
   partitioned by plan only, or the dependents are not registered as same-column; both fall to
   `SELECT public.reflex_reconcile(child)` (partition.rs:2360). Which of the two is not established.
2. **No de-duplication across the cascade.** A full reconcile of a partitioned IMV refreshes its
   own dependents with full reconciles (`cascade_partitioned_rebuild_to_dependents`,
   src/reconcile.rs:1115-1131). The chain is a DAG with shared nodes, so a node reachable by
   several paths is rebuilt once per path.
3. **Per leaf.** The cascade runs inside each leaf's reconcile, so a 20-leaf push pays it up to 20
   times in one transaction.

## Measurements (one tenant schema, dev)

| | |
|---|---|
| `reflex_flush_partition_source(root)` with nothing swapped | 0 change(s), 0.4 s |
| same call after swapping ONE leaf (379,591 rows, every `qty_sales` changed), in `BEGIN … ROLLBACK` | 1 change(s), **564.98 s** |
| whole push COMMIT on 1.11.4 (20 leaves, event log, 2026-09-25 … 10-01) | 75–86 s, no rebuild event on the `forecast_analysis` chain |
| whole push COMMIT on 1.11.5 (20 leaves, 2026-10-12) | cancelled by the client at 200 s |

On 1.11.4 the `forecast_analysis` chain was refreshed ~17 min after each push by a separate,
plan-scoped `reflex_reconcile_partition` from the application (14 rebuild events, ~80 s).

## What changed

The cascade and its "anything else → full reconcile" branch date from 2026-06-23 and are in
1.11.4. 1.11.5 (`d13f5e3`, `e6a9e96`) made populated-leaf rebuilds a row diff and added the explicit
refresh of dependents that list the view in `ignored_sources`
(`ignoring_dependents`, reconcile.rs:880). The chain's sub-IMVs were not rebuilt by a push before
that; they are now.

## Ruled out

- Leftover queue work: the root was clean (0 changes) before the test.
- Stale flags: the `ignored source(s) … changed` notices on these IMVs are healed only by
  `reflex_heal_ignored_sources`, not by the flush.
- A general 1.11.5 slowdown of the leaf rebuild: smaller tenants keep 1.1–1.5 s per leaf after
  the upgrade.

## Not established

- The test called the flush explicitly; the real path is the deferred constraint trigger at
  COMMIT. Related: `2026-10-02_in_trigger_scoped_reconcile_escalates_to_full_commit_rebuild.md`
  (a scoped reconcile queued for COMMIT becomes a full one). Whether the field failure goes
  through that pass or through the immediate cascade above is not known.
- Whether an unchanged leaf (empty diff) cascades the same way: only the all-rows-changed variant
  was run.
- Smaller tenants with the same graph still commit after the upgrade; assumed to be size (fewer
  plans, smaller leaves), not measured.
- `partition_columns` / `ignored_sources` of the chain nodes were not read.

## Reproduction (field, rolls back)

```sql
BEGIN;
CREATE TABLE s.zz_swap_test (LIKE s.sales_simulation_p_<plan>_2027_03 INCLUDING ALL);
INSERT INTO s.zz_swap_test SELECT * FROM s.sales_simulation_p_<plan>_2027_03;
UPDATE s.zz_swap_test SET qty_sales = qty_sales + 1;
ALTER TABLE s.sales_simulation_p_<plan> DETACH PARTITION s.sales_simulation_p_<plan>_2027_03;
ALTER TABLE s.sales_simulation_p_<plan> ATTACH PARTITION s.zz_swap_test
  FOR VALUES FROM ('2027-03-01') TO ('2027-04-01');
DROP TABLE s.sales_simulation_p_<plan>_2027_03;
ALTER TABLE s.zz_swap_test RENAME TO sales_simulation_p_<plan>_2027_03;
SELECT reflex_flush_partition_source('s.sales_simulation');   -- 565 s
ROLLBACK;
```

A pgrx test still has to be written: LIST × RANGE source, a two-level passthrough IMV, a
dependent partitioned by the LIST key only that lists it in `ignore_sources`, a second-level
dependent reachable by two paths; swap one leaf and assert on the number of reconciles and on the
plans touched (the cmin-based rewritten-rows metric of `0b7aaf4` fits).

## Fix direction

1. Keep the scope: a swap of leaves under plan P refreshes a plan-partitioned dependent for P only,
   whatever the source leaf's depth. If the scoped call cannot serve the dependent, say why
   (refuse loudly) rather than silently widening to the whole IMV.
2. Refresh each dependent once per flush: collect the affected (dependent, keys) pairs across all
   swapped leaves and all cascade paths, then run them once in dependency order.
3. Correctness bias stays asymmetric: when the affected keys cannot be proven, do the full work.

## Contrast with a smaller tenant (server log, same day)

For another schema the same cascade DID take the scoped branch:
`reflex_reconcile_partition('<schema>.forecast_analysis_view__cte_forecast_bounds', '', '<8 sop_forecast_view leaves of one plan>')`.
So the same-column call is issued with depth-2 source leaves and can be served. Why the large
tenant's cascade ended in full reconciles instead is the open point of item 1 above (candidates:
its chain flagged stale by an ignored-source change since 2026-10-08, an orphan target partition
reported during the run, or the explicit-call path of the test).

## Related

That tenant's flush then failed on a deadlock with a third tenant: tracked separately in
`2026-10-12_default_drain_catalog_query_locks_every_partition_in_the_database.md`.
