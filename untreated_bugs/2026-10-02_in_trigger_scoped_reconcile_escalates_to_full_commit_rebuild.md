# In-trigger scoped reconcile of a DEFERRED IMV escalates to a full COMMIT-time rebuild — S3 (perf)

Found in the 1.11.5 release-candidate round-3 review. Correct results; cost only. Parked: no
fix in 1.11.5.

## What happens

A partition- or key-scoped rebuild of a DEFERRED IMV reached from inside a statement's
trigger (before that statement stages its delta) cannot take a watermark: it would read the
write whose delta is staged after it, and the flush would apply that delta on top. 1.11.5
therefore sends it to the COMMIT-time pass (`scoped_rebuild_waits_for_commit`,
src/trigger/deferred.rs:195), but that pass only knows full reconciles:

- `reflex_reconcile_partition_impl` (src/partition.rs:1910) lists the IMV with
  `list_for_commit_reconcile` and returns `RECONCILE QUEUED FOR COMMIT`;
- the cascade of a partition reconcile into a DEFERRED dependent does the same for a
  same-column partitioned dependent (through the call above) and for a key-scoped
  aggregate dependent (src/partition.rs:2334).

At COMMIT, `rebuild_listed` (src/trigger/deferred.rs:961) runs `reflex_reconcile` on the
whole IMV: a one-slice reconcile becomes a rebuild of every slice from the base query, diffed
into its dependents. The common field shape is a hot-partition cascade from an IMMEDIATE
upstream into a DEFERRED same-column dependent (one plan written, every plan rebuilt).

Ruled out: correctness. The full rebuild runs after every statement staged its delta and
takes a whole-IMV watermark, so the result is exact (pg_drc_partition_reconcile_inside_trigger_before_staging).

## Reproduction

`pg_drc_partition_reconcile_inside_trigger_before_staging` (src/tests/pg_test_deferred_reconcile.rs):
a DEFERRED IMV partitioned by `plan`, a user statement trigger sorting before `__reflex_*`
that calls `reflex_reconcile_partition('drp4_v', '2')`. After the INSERT the IMV is in
`pg_temp.__reflex_deferred_rebuild`, and the COMMIT-time pass rebuilds plans 1, 2 and 3.

## Fix direction

Queue a scoped rebuild instead of a full one: record (IMV, rebuilt keys or source
partitions) in a transaction-local list, and in the COMMIT pass run
`reflex_reconcile_partition` (or the key-scoped cascade) for those slices, through the same
slice-record path as an out-of-trigger scoped reconcile. Several requests for one IMV merge
into one call; an IMV also listed for a full reconcile takes only the full one. Measure
first: a one-plan cascade into a large DEFERRED dependent (e.g. `alp.sop_forecast_view`
shape on db_clone), COMMIT time before and after.
