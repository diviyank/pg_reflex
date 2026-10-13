# Clauses

| Clause | Supported? | Notes |
|---|:---:|---|
| `GROUP BY` | ✅ | Columns become intermediate-table keys |
| `WHERE` | ✅ | Static filters only — no clock, session or `VOLATILE` function ([list](../limitations/unsupported-shapes.md#non-deterministic-functions)) |
| `INNER JOIN` | ✅ | Triggers installed on each source |
| `LEFT JOIN` | ✅ | Same trigger mechanism |
| `RIGHT JOIN` | ✅ | Recomputed from the query on every write to any source (1.12.0). Written as a `LEFT JOIN` with the tables swapped, writes to the preserved table are incremental |
| `FULL OUTER JOIN` | ✅ | Recomputed from the query on every write to either side ([details](../limitations/unsupported-shapes.md#full-outer-join)) |
| `HAVING` | ✅ | Rewritten to filter on intermediate columns; aggregates not in SELECT are auto-added |
| `DISTINCT` | ✅ | Without GROUP BY, columns become implicit group keys + `__ivm_count` |
| `DISTINCT ON` (1.1.1+) | ✅ | Decomposed into passthrough sub-IMV + `ROW_NUMBER` VIEW |
| `WITH` (CTE) | ✅ | Each CTE becomes a sub-IMV |
| `WITH RECURSIVE` | ❌ | Cannot decompose into static IMV layers |
| `LIMIT` | ❌ | Rejected — not meaningful for materialized state |
| `ORDER BY` | ❌ | Rejected — target tables are unordered |

## Passthrough queries

Queries with no aggregation and no DISTINCT are **passthrough**: pg_reflex skips the intermediate table and applies the delta directly to the target.

```sql
SELECT create_reflex_ivm('active_orders',
    'SELECT o.id, o.amount, p.name AS product_name
     FROM orders o JOIN products p ON o.product_id = p.id
     WHERE o.status = ''active''',
    'id'  -- unique_columns for incremental DELETE/UPDATE
);
```

Passthrough INSERT is incremental (`O(delta)`). DELETE/UPDATE are incremental **when** `unique_columns` is provided or auto-inferred from a single-source PK (1.1.x+); otherwise they fall back to a full refresh from source.

## Deterministic functions only

The IMV definition must be static. `WHERE date > NOW()` is rejected because the predicate's truth value changes over time without a corresponding source mutation — pg_reflex would silently drift. The rule holds anywhere in the query, CTEs and subqueries included, and covers every function PostgreSQL labels `VOLATILE`: see [Non-deterministic functions](../limitations/unsupported-shapes.md#non-deterministic-functions).

If you need a moving-window IMV, keep the window boundary in a table and move it with an UPDATE:

```sql
-- Rejected:
--   ... WHERE created > NOW() - INTERVAL '7 days'

CREATE TABLE report_bounds (lo TIMESTAMPTZ NOT NULL);
INSERT INTO report_bounds VALUES (NOW() - INTERVAL '7 days');

SELECT create_reflex_ivm('recent_orders',
    'SELECT o.id, o.amount
     FROM orders o
     WHERE o.created > (SELECT lo FROM report_bounds)',
    'id');

-- on a schedule:
UPDATE report_bounds SET lo = NOW() - INTERVAL '7 days';
```

The UPDATE of `report_bounds` is a source write like any other: it recomputes `recent_orders` from its query. Writes to `orders` stay incremental because the IMV has a key (the third argument; a passthrough that reads a second table through a subquery infers none).
