# FILTER clause

`AGG(x) FILTER (WHERE cond)` is supported (1.1.1+) for all algebraic-or-near-algebraic aggregates: SUM, COUNT, COUNT(*), AVG, MIN, MAX, BOOL_OR.

## How it's rewritten

Internally pg_reflex rewrites `AGG(x) FILTER (WHERE cond)` to `AGG(CASE WHEN cond THEN x END)`. So all the existing trigger / MERGE / delta machinery applies transparently.

## Example: mixed filtered + unfiltered

```sql
SELECT create_reflex_ivm('order_metrics',
    'SELECT region,
            SUM(amount) AS total,
            SUM(amount) FILTER (WHERE type = ''refund'') AS refunds,
            COUNT(*) AS cnt,
            COUNT(*) FILTER (WHERE amount > 100) AS big_orders,
            AVG(amount) FILTER (WHERE region = ''US'') AS us_avg
     FROM orders GROUP BY region');
```

Each filtered aggregate gets its own intermediate column (`__sum_amount_refunds`, `__count_amount_big_orders`, etc.).

## Limitations

- `FILTER` cannot reference subqueries or non-deterministic functions (`NOW()`, `RANDOM()`).
- `COUNT(DISTINCT x) FILTER (WHERE cond)` is not yet supported.
