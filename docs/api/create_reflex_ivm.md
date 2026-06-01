# `create_reflex_ivm`

Creates an incremental materialized view from a SELECT query.

## Signature

```sql
create_reflex_ivm(
    view_name        TEXT,
    sql              TEXT,
    unique_columns   TEXT     DEFAULT NULL,
    storage          TEXT     DEFAULT 'UNLOGGED',
    mode             TEXT     DEFAULT 'IMMEDIATE',
    ignore_sources   TEXT     DEFAULT NULL    -- 1.4.5+
) RETURNS TEXT
```

Returns `'CREATE REFLEX INCREMENTAL VIEW'` on success, `'ERROR: …'` on failure.

### Overloads

`create_reflex_ivm` is overloaded; pick the form that carries the extra
argument you need:

```sql
-- Top-K MIN/MAX (1.3.0+): topk in position 6, ignore_sources moves to 7
create_reflex_ivm(view_name, sql, unique_columns, storage, mode,
                  topk INTEGER, ignore_sources TEXT DEFAULT NULL)

-- Partitioned (1.5.0+): partition_by is the trailing argument
create_reflex_ivm(view_name, sql, unique_columns, storage, mode,
                  ignore_sources, partition_by TEXT[])
create_reflex_ivm(view_name, sql, unique_columns, storage, mode,
                  topk INTEGER, ignore_sources, partition_by TEXT[])
```

## Parameters

| Parameter | Description |
|---|---|
| `view_name` | Target IMV name (alphanumeric, underscores, periods for schema qualification) |
| `sql` | A `SELECT` query — the IMV definition |
| `unique_columns` | Comma-separated unique key columns for passthrough IMVs. Auto-inferred from source PK in 1.2.1+ for single-source passthroughs. |
| `storage` | `'UNLOGGED'` (default, max perf) or `'LOGGED'` (WAL-logged, crash-safe) |
| `mode` | `'IMMEDIATE'` (default, per-statement flush) or `'DEFERRED'` (flush at COMMIT) |
| `ignore_sources` | 1.4.5+. Comma-separated source list to exclude from maintenance. DML on listed sources will NOT refresh this IMV — use `reflex_reconcile` or periodic refresh instead. Schema-qualified (`alp.product`) or bare (`product`) names both accepted. Honored on the IMMEDIATE and (since 1.7.6) DEFERRED trigger paths. |
| `topk` | 1.3.0+ (overload). Integer K. When > 0, MIN/MAX columns maintain a sibling top-K array. Disabled by default. |
| `partition_by` | 1.5.0+ (overload). `TEXT[]` of output column names to partition the target on; see the partitioning guide. |

## What gets created

| Query type | Objects created |
|---|---|
| GROUP BY + aggregates | Intermediate UNLOGGED + target table + per-source triggers |
| Passthrough (no agg) | Target table + per-source triggers |
| WITH (CTE) | Sub-IMV per CTE + IMV/VIEW for the main body |
| UNION / INTERSECT / EXCEPT | Sub-IMV per operand + VIEW |
| WINDOW functions | Base sub-IMV + VIEW that applies the window at read time |
| DISTINCT ON | Passthrough sub-IMV + ROW_NUMBER VIEW |

## Examples

```sql
-- Basic (2 args)
SELECT create_reflex_ivm('sales_by_region',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region');

-- Passthrough with explicit unique key
SELECT create_reflex_ivm('active_orders',
    'SELECT o.id, o.amount, p.name FROM orders o JOIN products p ON o.product_id = p.id',
    'id');

-- Crash-safe (LOGGED)
SELECT create_reflex_ivm('critical_view',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region',
    NULL, 'LOGGED');

-- Deferred mode for bulk loads
SELECT create_reflex_ivm('batch_view',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region',
    NULL, 'UNLOGGED', 'DEFERRED');

-- Top-K MIN/MAX (1.3.0)
SELECT create_reflex_ivm('stock_chart',
    'SELECT product_id, MIN(price) AS lo, MAX(price) AS hi
     FROM stock_history GROUP BY product_id',
    NULL, 'UNLOGGED', 'IMMEDIATE', 16);
```

## Idempotent variant

```sql
create_reflex_ivm_if_not_exists(view_name, sql [, unique_columns [, storage [, mode [, ignore_sources]]]]) RETURNS TEXT
```

Mirrors `create_reflex_ivm`'s argument list (including the `topk` and
`partition_by` overloads).

Returns `'REFLEX INCREMENTAL VIEW ALREADY EXISTS (skipped)'` instead of erroring when `view_name` is already registered.
