# Clauses

| Clause | Supported? | Notes |
|---|:---:|---|
| `GROUP BY` | ✅ | Columns become intermediate-table keys |
| `WHERE` | ✅ | Static filters only — no `NOW()`, `RANDOM()`, `CURRENT_DATE` |
| `INNER JOIN` | ✅ | Triggers installed on each source |
| `LEFT JOIN` / `RIGHT JOIN` | ✅ | Same trigger mechanism |
| `FULL OUTER JOIN` | ❌ | Cannot reason about MATCH↔NULL transitions on both sides |
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

## Deterministic functions only in WHERE

The IMV definition must be static. `WHERE date > NOW()` is rejected because the predicate's truth value changes over time without a corresponding source mutation — pg_reflex would silently drift.

If you need a moving-window IMV, materialise the window boundary as a column:

```sql
-- Don't:
WHERE date > NOW() - INTERVAL '7 days'

-- Do:
WITH bounds AS (SELECT NOW() - INTERVAL '7 days' AS lo)
SELECT ... WHERE date > (SELECT lo FROM bounds)
-- ...and reconcile on a schedule via reflex_scheduled_reconcile
```
