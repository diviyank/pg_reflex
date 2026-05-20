# Window functions

Window functions are supported **only in the top-level SELECT** of an IMV query and are decomposed into a **base sub-IMV** (incrementally maintained) plus a **VIEW** that applies the window function at read time.

Window functions nested in subqueries, derived tables, or inside a referenced CTE are not supported and are rejected with a clear error at IMV creation time.

## GROUP BY + RANK / DENSE_RANK / ROW_NUMBER

```sql
SELECT create_reflex_ivm('ranked_regions',
    'SELECT region, SUM(amount) AS total,
            RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
     FROM orders GROUP BY region');
```

Creates:

- `ranked_regions__base` (sub-IMV: `region, total` aggregated incrementally)
- `ranked_regions` (VIEW: `SELECT *, RANK() OVER (ORDER BY total DESC) AS rnk FROM ranked_regions__base`)

The window applies only to the small group-summary rows — cheap.

## Passthrough + LAG / LEAD

```sql
SELECT create_reflex_ivm('time_series',
    'SELECT ts, value, LAG(value) OVER (ORDER BY ts) AS prev_value
     FROM measurements');
```

The base is a passthrough sub-IMV; the VIEW applies `LAG` at read time.

## Window functions over CTEs

Window functions in the top-level SELECT work correctly over CTEs. The CTE decomposition runs first, so sibling CTEs are preserved and available in the window scope.

```sql
SELECT create_reflex_ivm('region_ranking',
    'WITH regional_totals AS (
        SELECT region, SUM(amount) AS total FROM orders GROUP BY region
    ),
    regional_counts AS (
        SELECT region, COUNT(*) AS num_orders FROM orders GROUP BY region
    )
    SELECT t.region, t.total, c.num_orders,
           RANK() OVER (ORDER BY t.total DESC) AS rank
    FROM regional_totals t
    JOIN regional_counts c ON t.region = c.region');
```

Both `regional_totals` and `regional_counts` are preserved as sub-IMVs, and the window function rank applies at read time.

## Supported window functions

`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, `NTH_VALUE()`, `NTILE()`, plus any ordinary aggregate as a window (`SUM(x) OVER (...)`, `AVG(x) OVER (...)`, …).

## Limitation — window aggregates in SELECT (no GROUP BY)

`SUM(x) OVER (PARTITION BY g)` over a passthrough query (no GROUP BY) is not incrementally maintainable: any new row can change the partition total for unrelated rows, so the affected set is the whole partition. See [unsupported shapes §5](../limitations/unsupported-shapes.md#window-functions-in-select).
