# Decomposition

pg_reflex breaks complex queries into composable pieces:

| User wrote | pg_reflex creates |
|---|---|
| Simple `GROUP BY` aggregate | One intermediate + one target |
| `WITH` (CTE) | Sub-IMV per CTE + target IMV/VIEW for the main body |
| `UNION ALL` | Sub-IMV per operand + zero-cost VIEW that unions sub-IMV targets |
| `UNION` (dedup) | Same as UNION ALL + Postgres-native deduplication at read time |
| `INTERSECT` / `EXCEPT` | Sub-IMV per operand + VIEW |
| Window function (`RANK`, `LAG`, `SUM OVER`) | Base sub-IMV + VIEW that applies the window at read time |
| `DISTINCT ON` | Passthrough sub-IMV + VIEW with `ROW_NUMBER() OVER (... ORDER BY ...)` |

## Why a sub-IMV per CTE?

Each CTE body is a single SELECT. By treating it as its own IMV, pg_reflex can:

- Maintain the CTE's result incrementally (its own intermediate).
- Use it as a dependency for downstream IMVs that reference it.
- Skip re-computing it when only an unrelated CTE changes.

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

- `region_summary__cte_by_city` (sub-IMV, depth 1)
- `region_summary__cte_by_region` (sub-IMV, depth 2, depends on by_city)
- `region_summary` (VIEW over by_region's target — passthrough)

## Why a VIEW for the outer body?

If the outer body is **passthrough** (no aggregation), it's just a column projection / filter / join over already-incremental sub-IMVs. A plain `CREATE VIEW` reads from those sub-IMVs and is always up-to-date — no second incremental layer needed.

If the outer body has **its own aggregation**, it becomes another IMV: another intermediate + target.

## Why decompose UNION ALL?

`UNION ALL` is a row-level concatenation. Each operand can be maintained independently — there's no cross-operand dependency. So pg_reflex builds:

- `<view>__union_0`, `<view>__union_1`, … (sub-IMVs)
- `<view>` as a `CREATE OR REPLACE VIEW` that does `SELECT * FROM <view>__union_0 UNION ALL SELECT * FROM <view>__union_1 …`

Zero overhead: the VIEW is rewritten by Postgres' planner.

## Why a VIEW for window functions?

Window functions like `RANK()` and `LAG()` are not associative across disjoint row sets — a new row can change ranks for unrelated rows. So:

- The **base** of the window query (the SELECT without the window functions) becomes a sub-IMV, maintained incrementally.
- The window functions are applied at **read time** by a VIEW over the sub-IMV's target.

For `GROUP BY + RANK`, the VIEW only ranks the small group-summary table, which is cheap.

[Architecture diagram :material-arrow-right-bold:](architecture.md){ .md-button }
