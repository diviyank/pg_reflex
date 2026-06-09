# `reflex_set_partition_dispatch_cost_cap`

(1.6.0+) Sets or clears the per-IMV `partition_dispatch_cost_cap`.

!!! warning "Reserved — not yet wired"
    The metadata column and this setter exist, but the value is **not consulted at runtime** in the current release. Setting it has no effect today. It is documented for forward compatibility; behaviour below describes the intended design.

## Signature

```sql
reflex_set_partition_dispatch_cost_cap(view_name TEXT, value BIGINT)
RETURNS TEXT
```

Pass `value = NULL` to inherit the GUC / compiled default.

## Intended behaviour

For the Tier 2 (JOIN-secondary) per-partition dispatch gate: when such a source trigger fires on a partitioned IMV, the dispatch JOINs to the anchor source to derive partition keys. If the planner's estimated row count of that JOIN exceeds this cap, per-partition dispatch is skipped in favour of the global Path B flush.

## Resolution order

1. The per-IMV `partition_dispatch_cost_cap` column (set here).
2. The GUC `reflex.partition_dispatch_cost_cap`.
3. The compiled default (`100000`).

## Example

```sql
SELECT reflex_set_partition_dispatch_cost_cap('sales_by_region', 250000);
```
