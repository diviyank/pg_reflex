# `reflex_explain_flush`

(1.2.0+) Returns the `EXPLAIN (VERBOSE, COSTS ON)` of the SQL the next flush would execute, without actually running it.

## Signature

```sql
reflex_explain_flush(view_name TEXT) RETURNS TEXT
```

Returns a multi-line string (the EXPLAIN output of the IMV's `base_query`).

## Example

```sql
SELECT reflex_explain_flush('sales_by_region');
```

```
HashAggregate  (cost=210.00..212.50 rows=200 width=40)
  Group Key: sales.region
  ->  Seq Scan on public.sales  (cost=0.00..150.00 rows=10000 width=12)
```

## Use cases

- Debug a slow IMV without firing a flush: spot full scans, missing source indexes, planner regressions.
- Validate a `reflex_rebuild_imv` plan before running it on a large source.
- Compare plans before and after `ANALYZE` on the source.

The EXPLAIN runs on the registered `base_query`, not the trigger-side delta SQL — so it shows the cost of a full rebuild, not the cost of a per-statement delta.
