# CTE

Each CTE in a `WITH` clause becomes its own sub-IMV. The outer query becomes either an IMV (if it has its own aggregation) or a passthrough VIEW (if it's a projection / filter / join over the CTE results).

## Window functions and DISTINCT ON in CTEs

Window functions or `DISTINCT ON` **inside a CTE** that is then referenced or joined by an outer query are **not supported** and are rejected with a clear error at IMV creation time.

**Why**: A CTE with a window function or `DISTINCT ON` materializes as a read-time VIEW (these operations cannot be incrementally maintained inline). An IMV cannot install row-level triggers on a VIEW, so parent-level incremental maintenance is not possible.

**Solutions**:
- Move the window function or `DISTINCT ON` to the outermost `SELECT` — windows/DISTINCT ON in the top-level SELECT over CTEs are fully supported (see [Window functions](./window-functions.md) for examples).
- Define the CTE's view separately as `kind: mv` (a plain `MATERIALIZED VIEW`), then reference it from the IMV query.

**Example of unsupported pattern**:

```sql
-- ❌ Rejected: window inside CTE, referenced by outer query
SELECT create_reflex_ivm('my_ivm',
    'WITH ranked_data AS (
        SELECT id, amount,
               ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
        FROM orders
    )
    SELECT id, amount FROM ranked_data WHERE rn = 1');
```

**Example of supported pattern**:

```sql
-- ✅ Supported: window in outermost SELECT
SELECT create_reflex_ivm('my_ivm',
    'WITH order_data AS (
        SELECT id, amount FROM orders
    )
    SELECT id, amount,
           ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
    FROM order_data');
```

## Partition propagation to CTE sub-IMVs

When a partitioned IMV is built from a query with CTEs, the parent's `partition_by` columns automatically propagate to each CTE sub-IMV — but only if that partition column appears in the CTE's output.

```sql
SELECT create_reflex_ivm('region_summary',
    'WITH by_city AS (
        SELECT region, city, SUM(amount) AS city_total
        FROM orders GROUP BY region, city
    )
    SELECT region, SUM(city_total) AS total
    FROM by_city GROUP BY region',
    partition_by => ARRAY['region']);
```

In this example, `region` appears in `by_city`'s output, so the sub-IMV `region_summary__cte_by_city` inherits `partition_by => ARRAY['region']`, and `region_summary` itself is also partitioned by `region`.

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
