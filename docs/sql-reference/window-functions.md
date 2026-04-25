# Window functions

Window functions are decomposed into a **base sub-IMV** (incrementally maintained) plus a **VIEW** that applies the window function at read time.

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

## Supported window functions

`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, `NTH_VALUE()`, `NTILE()`, plus any ordinary aggregate as a window (`SUM(x) OVER (...)`, `AVG(x) OVER (...)`, …).

## Limitation — window aggregates in SELECT (no GROUP BY)

`SUM(x) OVER (PARTITION BY g)` over a passthrough query (no GROUP BY) is not incrementally maintainable: any new row can change the partition total for unrelated rows, so the affected set is the whole partition. See [unsupported shapes §5](../limitations/unsupported-shapes.md#window-functions-in-select).
