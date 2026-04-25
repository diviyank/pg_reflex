# Set operations

`UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` are decomposed into per-operand sub-IMVs plus a parent VIEW.

## UNION ALL

Each operand becomes a sub-IMV. The parent is a zero-cost VIEW.

```sql
SELECT create_reflex_ivm('all_orders',
    'SELECT region, amount FROM domestic_orders
     UNION ALL
     SELECT region, amount FROM international_orders');
```

Creates:

- `all_orders__union_0` (sub-IMV)
- `all_orders__union_1` (sub-IMV)
- `all_orders` (VIEW: `SELECT * FROM all_orders__union_0 UNION ALL SELECT * FROM all_orders__union_1`)

## UNION (deduplicating)

Same decomposition; the parent VIEW does `UNION` (not `UNION ALL`) so Postgres deduplicates at read time.

## INTERSECT / EXCEPT

Same pattern. Sub-IMV per operand, parent VIEW with `INTERSECT` / `EXCEPT`.

## Operands can have aggregates

```sql
SELECT create_reflex_ivm('combined_totals',
    'SELECT region, SUM(amount) AS total FROM domestic_orders GROUP BY region
     UNION ALL
     SELECT region, SUM(amount) AS total FROM international_orders GROUP BY region');
```

Each operand sub-IMV has its own intermediate + target. The parent VIEW unions the targets.

## Limitation — UNION ALL inside CTEs

Set operations **inside CTE bodies** are not decomposed (the CTE stays as a single block, breaking delta substitution). Workaround: lift the set operation to the top level.

```sql
-- Doesn't work as you'd expect:
WITH x AS (SELECT a FROM t1 UNION ALL SELECT a FROM t2)
SELECT a, COUNT(*) FROM x GROUP BY a;

-- Works:
SELECT a, COUNT(*) FROM (
    SELECT a FROM t1 UNION ALL SELECT a FROM t2
) AS x GROUP BY a;
```

See [unsupported shapes §7](../limitations/unsupported-shapes.md#union-all-inside-a-cte).
