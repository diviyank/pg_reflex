# CTE

Each CTE in a `WITH` clause becomes its own sub-IMV. The outer query becomes either an IMV (if it has its own aggregation) or a passthrough VIEW (if it's a projection / filter / join over the CTE results).

## Example

```sql
SELECT create_reflex_ivm('top_regions',
    'WITH regional AS (
        SELECT region, SUM(amount) AS total FROM orders GROUP BY region
    )
    SELECT region, total FROM regional WHERE total > 1000');
```

Creates:

- `top_regions__cte_regional` (sub-IMV with intermediate + target)
- `top_regions` (VIEW: `SELECT region, total FROM top_regions__cte_regional WHERE total > 1000`)

## Multi-level CTE

```sql
SELECT create_reflex_ivm('region_summary',
    'WITH by_city AS (
        SELECT region, city, SUM(amount) AS city_total
        FROM orders GROUP BY region, city
    ),
    by_region AS (
        SELECT region, SUM(city_total) AS total, COUNT(*) AS num_cities
        FROM by_city GROUP BY region
    )
    SELECT region, total, num_cities FROM by_region');
```

Creates:

- `region_summary__cte_by_city` (depth 1)
- `region_summary__cte_by_region` (depth 2, depends on `by_city`)
- `region_summary` (VIEW)

## Recursive CTE

`WITH RECURSIVE` is **not supported** — recursion can't be statically decomposed into IMV layers. Use a plain `MATERIALIZED VIEW` and refresh on a schedule.

[Decomposition concepts :material-arrow-right-bold:](../concepts/decomposition.md){ .md-button }
