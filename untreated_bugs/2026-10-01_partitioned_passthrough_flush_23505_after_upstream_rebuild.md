# Partitioned passthrough DEFERRED flush fails with 23505 after an upstream rebuild — S1 (latent)

**Field:** prod 2026-09-22 07:00 and 2026-09-30 06:10:19, pg_reflex 1.11.4,
`alp.sop_forecast_view` (passthrough, DEFERRED, UNLOGGED, partitioned
`[dem_plan_id, order_date]`, `unique_key [dem_plan_id, product_id, location_id, order_date, canal]`,
LEFT JOIN `alp.current_assortment_activity_view`).

```
deferred flush failed: duplicate key value violates unique constraint
"sop_forecast_view_sales_sim_dem_plan_id_product_id_loca_idx1188" (SQLSTATE 23505)
```
(`idx1188` is on leaf `sop_forecast_view_sales_simulation_p_958_2026_09`.)

## Why it mattered
Combined with the TRUNCATE-clears-dependent bug (fixed in 1.11.5), the discarded flush made a
full wipe of every plan permanent. Since 1.11.5 an upstream rebuild reaches the dependent as a
row diff and a source TRUNCATE never clears it, so a discarded flush no longer loses rows
wholesale — but the IMV is still flagged `known_stale` and any rows the discarded delta should
have changed are stale until a reconcile.

## Ruled out
- An unpartitioned DEFERRED LEFT JOIN dependent with an incremental change followed by a
  `reflex_reconcile` of the upstream in the same transaction (the same key staged twice):
  `pg_toj_incremental_then_rebuild_of_left_joined_imv_keeps_dependent` flushes cleanly.

## Still to reproduce
The partitioned passthrough dispatch (`build_passthrough_partition_dispatch_sql`) on a
two-level mirror (LIST plan → RANGE month), with the upstream change arriving through the
outer-join-secondary path (`outer_join_secondary_stmts`) inside the job's single transaction
(bulk `INSERT … ON CONFLICT DO UPDATE` on `assortment_activity_relation` → wipe dispatch on
the upstream IMV → TRUNCATE + INSERT).
