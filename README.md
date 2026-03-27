# pg_reflex

**Incremental View Maintenance for PostgreSQL** -- keep your aggregated views fresh in real-time, without full refreshes.

pg_reflex is a PostgreSQL extension that maintains materialized views incrementally. When source data changes (INSERT, UPDATE, DELETE), only the affected groups are recomputed -- not the entire dataset. This turns O(N) `REFRESH MATERIALIZED VIEW` into O(delta) trigger-based updates.

## Installation

### Option A: Pre-built package (recommended)

Download and install the `.deb` package for your PostgreSQL version:

```bash
# Download the package (replace VERSION and pg17 with your version)
wget https://github.com/diviyank/pg_reflex/releases/download/VERSION/pg-reflex-VERSION-pg17-amd64.deb

# Install it
sudo dpkg -i pg-reflex-VERSION-pg17-amd64.deb
```

Then enable the extension in your database:

```sql
-- Connect to your database and run:
CREATE EXTENSION pg_reflex;
```

### Option B: From source

```bash
# 1. Clone the repository
git clone https://github.com/diviyank/pg_reflex.git
cd pg_reflex

# 2. Install the Rust toolchain (skip if you already have Rust)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# 3. Install cargo-pgrx (the PostgreSQL extension build tool)
cargo install cargo-pgrx --version '=0.16.1' --locked

# 4. Initialize pgrx with your PostgreSQL version (downloads PG headers)
cargo pgrx init --pg17 download    # adjust pg17 to your version (pg15, pg16, pg18)

# 5. Build and install the extension into your PostgreSQL instance
./install.sh --release --pg-config $(which pg_config)
```

> **Note:** If you get a "Permission denied" error, the PostgreSQL extension directories need to be writable by your user. On a dev machine:
> ```bash
> sudo chown -R $USER /usr/share/postgresql/extension/ /usr/lib/postgresql/*/lib/
> ```

Then enable the extension in your database:

```sql
-- Connect to your database and run:
CREATE EXTENSION pg_reflex;
```

## Upgrading

### From pre-built package

```bash
# Install the new version (replaces the .so and .sql files on disk)
sudo dpkg -i pg-reflex-NEW_VERSION-pg17-amd64.deb

# Update the extension in each database that uses it
psql -d mydb -c "ALTER EXTENSION pg_reflex UPDATE TO 'NEW_VERSION';"
```

### From source

```bash
cd pg_reflex
git pull
./install.sh --release --pg-config $(which pg_config)

# Update the extension in each database that uses it
psql -d mydb -c "ALTER EXTENSION pg_reflex UPDATE;"
```

Existing IMVs, triggers, and data are preserved across upgrades. No need to recreate views.

## Quick Start

```sql
-- Create a source table
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    region TEXT,
    amount NUMERIC
);
INSERT INTO sales (region, amount) VALUES
    ('US', 100), ('US', 200), ('EU', 150);

-- Create an incremental materialized view
SELECT create_reflex_ivm(
    'sales_by_region',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region'
);

-- Query the view (it's a regular table)
SELECT * FROM sales_by_region;
--  region | total
-- --------+-------
--  US     |   300
--  EU     |   150

-- Insert new data -- the view updates automatically via triggers
INSERT INTO sales (region, amount) VALUES ('US', 50), ('EU', 200);

SELECT * FROM sales_by_region;
--  region | total
-- --------+-------
--  US     |   350
--  EU     |   350

-- Deletes and updates also propagate
DELETE FROM sales WHERE amount = 100;
SELECT * FROM sales_by_region;
--  region | total
-- --------+-------
--  US     |   250
--  EU     |   350
```

## Usage Examples

```sql
-- Passthrough (no aggregation) — complex JOINs/filters kept fresh via triggers
SELECT create_reflex_ivm('active_orders',
    'SELECT o.id, o.amount, p.name AS product_name
     FROM orders o JOIN products p ON o.product_id = p.id
     WHERE o.status = ''active''');

-- Simple aggregation
SELECT create_reflex_ivm('daily_totals',
    'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
     FROM orders GROUP BY region');

-- Average (decomposed into SUM + COUNT internally)
SELECT create_reflex_ivm('avg_salaries',
    'SELECT dept, AVG(salary) AS avg_sal FROM employees GROUP BY dept');

-- DISTINCT with reference counting
SELECT create_reflex_ivm('unique_regions',
    'SELECT DISTINCT region FROM orders');

-- JOIN-based view
SELECT create_reflex_ivm('dept_revenue',
    'SELECT d.name, SUM(o.amount) AS revenue
     FROM orders o JOIN departments d ON o.dept_id = d.id
     GROUP BY d.name');

-- MIN/MAX
SELECT create_reflex_ivm('price_range',
    'SELECT category, MIN(price) AS lo, MAX(price) AS hi
     FROM products GROUP BY category');

-- Multiple aggregates in one view
SELECT create_reflex_ivm('full_stats',
    'SELECT region,
            SUM(amount) AS total,
            COUNT(*) AS cnt,
            AVG(amount) AS avg_amount,
            MIN(amount) AS min_amount,
            MAX(amount) AS max_amount
     FROM orders GROUP BY region');

-- BOOL_OR with cast
SELECT create_reflex_ivm('product_flags',
    'SELECT product_id,
            SUM(qty)::BIGINT AS total_qty,
            bool_or(is_promotional) AS has_promo
     FROM order_lines GROUP BY product_id');

-- CTE: each WITH clause becomes its own sub-IMV automatically
SELECT create_reflex_ivm('top_regions',
    'WITH regional AS (
        SELECT region, SUM(amount) AS total FROM orders GROUP BY region
    )
    SELECT region, total FROM regional WHERE total > 1000');
-- Creates: sub-IMV "top_regions__cte_regional" + VIEW "top_regions"

-- Multi-level CTE (chained)
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
-- Creates: sub-IMV "region_summary__cte_by_city",
--          sub-IMV "region_summary__cte_by_region" (depends on by_city),
--          VIEW "region_summary"

-- UNION ALL: each operand becomes a sub-IMV
SELECT create_reflex_ivm('all_orders',
    'SELECT region, amount FROM domestic_orders
     UNION ALL
     SELECT region, amount FROM international_orders');
-- Creates: sub-IMV "all_orders__union_0", sub-IMV "all_orders__union_1",
--          VIEW "all_orders" (zero overhead, reads from sub-IMVs)

-- UNION (dedup)
SELECT create_reflex_ivm('active_regions',
    'SELECT region FROM domestic_orders
     UNION
     SELECT region FROM international_orders');

-- WINDOW: GROUP BY + ranking
SELECT create_reflex_ivm('ranked_regions',
    'SELECT region, SUM(amount) AS total,
            RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
     FROM orders GROUP BY region');
-- Creates: sub-IMV "ranked_regions__base" (incremental aggregates),
--          VIEW "ranked_regions" (applies RANK at read time)

-- WINDOW: passthrough with LAG/LEAD
SELECT create_reflex_ivm('time_series',
    'SELECT ts, value, LAG(value) OVER (ORDER BY ts) AS prev_value
     FROM measurements');

-- Crash-safe mode: WAL-logged tables
SELECT create_reflex_ivm('critical_view',
    'SELECT region, SUM(amount) AS total FROM orders GROUP BY region',
    NULL,                -- unique_columns
    'LOGGED'             -- storage: crash-safe WAL-logged tables
);

-- Deferred mode: batch coalescing at COMMIT
SELECT create_reflex_ivm('batch_view',
    'SELECT region, SUM(amount) AS total FROM orders GROUP BY region',
    NULL,                -- unique_columns
    'UNLOGGED',          -- storage
    'DEFERRED'           -- mode: accumulate deltas, flush at COMMIT
);
```

## Supported SQL Features

### Aggregate Functions

| Function | Incremental on INSERT | Incremental on DELETE | Notes |
|----------|:---:|:---:|-------|
| `SUM(x)` | Yes | Yes | Additive -- stores running sum |
| `COUNT(x)` | Yes | Yes | Additive -- stores running count |
| `COUNT(*)` | Yes | Yes | Additive -- stores running count |
| `AVG(x)` | Yes | Yes | Decomposed to SUM + COUNT, recomputed as SUM/COUNT |
| `MIN(x)` | Yes | Recomputes group | Uses LEAST on insert; full group rescan on delete |
| `MAX(x)` | Yes | Recomputes group | Uses GREATEST on insert; full group rescan on delete |
| `BOOL_OR(x)` | Yes | Recomputes group | Uses OR on insert; full group rescan on delete |
| `DISTINCT` | Yes | Yes | Reference counting (`__ivm_count`) tracks multiplicity |

### Type Casts

Aggregate expressions with casts are supported. The cast is applied when materializing from the intermediate table to the target table:

```sql
-- Target column "total" will be BIGINT, not NUMERIC
SELECT create_reflex_ivm('typed_totals',
    'SELECT region, SUM(amount)::BIGINT AS total FROM orders GROUP BY region');
```

### Query Clauses

| Clause | Supported | Notes |
|--------|:---------:|-------|
| Passthrough (no agg) | Yes | Complex JOINs/filters without GROUP BY. INSERT is incremental (O(delta)); DELETE/UPDATE do full refresh from source |
| `GROUP BY` | Yes | Required for aggregate queries |
| `WHERE` | Yes | Static filters only (no `NOW()` or `RANDOM()`) |
| `JOIN` (INNER) | Yes | Triggers created on each source table |
| `LEFT/RIGHT JOIN` | Yes | Parsed and used, same trigger mechanism |
| `HAVING` | Yes | Rewritten to filter on intermediate columns; aggregates not in SELECT are auto-added |
| `DISTINCT` | Yes | Without GROUP BY, columns become implicit group keys |
| `CTE (WITH)` | Yes | Each CTE becomes a sub-IMV; passthrough main body becomes a VIEW |
| `UNION ALL` | Yes | Each operand becomes a sub-IMV; parent is a VIEW |
| `UNION` (dedup) | Yes | Same decomposition; PostgreSQL handles dedup at read time |
| `WINDOW functions` | Yes | Decomposed into base sub-IMV + VIEW that applies window at read time |
| `WITH RECURSIVE` | No | Recursive CTEs cannot be decomposed into static IMV layers |
| `INTERSECT` | Yes | Each operand becomes a sub-IMV; parent is a VIEW |
| `EXCEPT` | Yes | Each operand becomes a sub-IMV; parent is a VIEW |
| `LIMIT` | No | Rejected -- not meaningful for materialized views |
| `ORDER BY` | No | Rejected -- the target table is unordered |
| `Subqueries in FROM` | Parsed | Tracked as `<subquery:alias>` |

## How It Works

### Architecture

```
Source Table(s)
    |
    | AFTER INSERT/UPDATE/DELETE triggers
    v
+-------------------+     +--------------------+
| Intermediate Table| --> | Target Table       |
| (UNLOGGED)        |     | (UNLOGGED)         |
|                   |     |                    |
| Stores partial    |     | Final query result |
| aggregates:       |     | users SELECT from  |
| __sum_x, __count_x|     | this table         |
| __ivm_count       |     |                    |
+-------------------+     +--------------------+
```

### Delta Processing

When you INSERT, UPDATE, or DELETE rows in a source table:

1. **Statement-level triggers** fire (one per operation type, shared across all IMVs on the same source)
2. A Rust function generates **MERGE SQL** from the stored `base_query`, replacing the source table reference with the transition table (new/old rows)
3. The MERGE applies the delta to the **intermediate table** and captures affected group keys via `RETURNING`:
   - For INSERT: `__sum_x = intermediate.__sum_x + delta.__sum_x`
   - For DELETE: `__sum_x = intermediate.__sum_x - delta.__sum_x`
   - For UPDATE: subtract old values, then add new values (two-phase)
4. **Targeted refresh**: only the affected groups (captured from MERGE RETURNING) are deleted and re-inserted into the **target table** from the intermediate table
5. `__ivm_count` tracks how many source rows contribute to each group -- groups with `__ivm_count = 0` are excluded from the target

### Sufficient Statistics

Instead of storing raw data, pg_reflex stores the minimum state needed to maintain each aggregate:

| User writes | Intermediate stores | Target computes |
|---|---|---|
| `AVG(salary)` | `__sum_salary` + `__count_salary` | `__sum_salary / __count_salary` |
| `COUNT(*)` | `__count_star` | `__count_star` |
| `DISTINCT col` | `col` + `__ivm_count` | `col WHERE __ivm_count > 0` |
| `BOOL_OR(active)` | `__bool_or_active` | `__bool_or_active` |
| `SUM(x)::BIGINT` | `__sum_x` (NUMERIC) | `__sum_x::BIGINT` |

### Dependency Graph

IMVs can depend on other IMVs. pg_reflex tracks this via `graph_depth` and `graph_child` in the metadata table:

```sql
-- L1: depends on base table (depth = 1)
SELECT create_reflex_ivm('daily_totals',
    'SELECT date, SUM(amount) AS total FROM sales GROUP BY date');

-- L2: depends on L1 (depth = 2)
SELECT create_reflex_ivm('monthly_totals',
    'SELECT date_trunc(''month'', date) AS month, SUM(total) AS grand_total
     FROM daily_totals GROUP BY date_trunc(''month'', date)');
```

Multi-level cascading propagation works automatically -- when L1 updates its target table, PostgreSQL fires L2's triggers, which process their own delta. This works to arbitrary depth.

## Metadata & Introspection

All IMV metadata is stored in `public.__reflex_ivm_reference`:

```sql
SELECT name, graph_depth, depends_on, enabled, last_update_date
FROM public.__reflex_ivm_reference;

--        name       | graph_depth |  depends_on   | enabled |     last_update_date
-- ------------------+-------------+---------------+---------+--------------------------
--  sales_by_region  |           1 | {sales}       | t       | 2025-01-15 10:30:00
--  dept_revenue     |           1 | {orders,departments} | t | 2025-01-15 10:31:00
```

Useful columns:
- `base_query` -- the query that computes partial aggregates from source data
- `end_query` -- the query that computes final results from the intermediate table
- `aggregations` -- JSON describing the aggregation plan (column mappings, types)
- `depends_on` -- source tables this view reads from
- `depends_on_imv` -- other IMVs this view depends on
- `graph_child` -- downstream IMVs that depend on this one

## API Reference

### `create_reflex_ivm(view_name, sql [, unique_columns [, storage [, mode]]]) -> TEXT`

Creates an incremental materialized view from a SELECT query. Returns `'CREATE REFLEX INCREMENTAL VIEW'` on success, or `'ERROR: ...'` on failure.

**Parameters:**
| Parameter | Type | Default | Description |
|---|---|---|---|
| `view_name` | TEXT | required | Name for the IMV (alphanumeric, underscores, periods) |
| `sql` | TEXT | required | SELECT query to maintain incrementally |
| `unique_columns` | TEXT | NULL | Comma-separated unique key columns for passthrough IMVs |
| `storage` | TEXT | `'UNLOGGED'` | `'LOGGED'` for crash-safe WAL-logged tables, `'UNLOGGED'` for max performance |
| `mode` | TEXT | `'IMMEDIATE'` | `'DEFERRED'` to batch deltas until COMMIT, `'IMMEDIATE'` for per-statement updates |

**What it creates** depends on the query type:

| Query type | Creates |
|---|---|
| GROUP BY + aggregates | Intermediate table + target table + triggers |
| Passthrough (no agg) | Target table + triggers |
| CTE (WITH) | Sub-IMV per CTE + VIEW or IMV for main body |
| UNION ALL / UNION | Sub-IMV per operand + VIEW |
| WINDOW functions | Base sub-IMV + VIEW (window applied at read time) |

```sql
-- Basic (2 args)
SELECT create_reflex_ivm('sales_by_region',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region');

-- With unique key (3 args)
SELECT create_reflex_ivm('active_orders',
    'SELECT o.id, o.amount, p.name FROM orders o JOIN products p ON o.product_id = p.id',
    'id');

-- Crash-safe (4 args)
SELECT create_reflex_ivm('critical_view',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region',
    NULL, 'LOGGED');

-- Full (5 args)
SELECT create_reflex_ivm('batch_view',
    'SELECT region, SUM(amount) AS total FROM sales GROUP BY region',
    NULL, 'UNLOGGED', 'DEFERRED');
```

### `create_reflex_ivm_if_not_exists(view_name, sql [, unique_columns [, storage [, mode]]]) -> TEXT`

Same as `create_reflex_ivm`, but returns `'REFLEX INCREMENTAL VIEW ALREADY EXISTS (skipped)'` instead of an error if the view already exists. Same parameters.

### `drop_reflex_ivm(view_name TEXT) -> TEXT`

Drops an IMV and all its artifacts (target table, intermediate table, triggers, metadata). Refuses if the IMV has child dependencies.

### `drop_reflex_ivm(view_name TEXT, cascade BOOLEAN) -> TEXT`

Same as above, but with `cascade = true` drops all child IMVs first.

```sql
SELECT drop_reflex_ivm('monthly_totals');           -- fails if children exist
SELECT drop_reflex_ivm('daily_totals', true);       -- drops children first
```

### `reflex_reconcile(view_name TEXT) -> TEXT`

Rebuilds the intermediate and target tables from the source data. Use to fix drift or as a periodic safety net via `pg_cron`. Returns `'RECONCILED'` on success.

For performance, reconcile drops all indexes (including user-created), does a bulk TRUNCATE + INSERT, then recreates all indexes and runs ANALYZE. User-created index definitions are saved and restored faithfully.

```sql
SELECT reflex_reconcile('sales_by_region');
```

### `refresh_reflex_imv(view_name TEXT) -> TEXT`

Alias for `reflex_reconcile`. Provided for naming consistency with PostgreSQL's `REFRESH MATERIALIZED VIEW`.

```sql
SELECT refresh_reflex_imv('sales_by_region');
```

### `refresh_imv_depending_on(source TEXT) -> TEXT`

Refreshes all IMVs that depend on the given source table or materialized view. Useful after bulk-loading data with triggers disabled, or after refreshing a source materialized view.

```sql
-- Refresh all IMVs that read from the 'orders' table
SELECT refresh_imv_depending_on('orders');
```

## Testing

```bash
# Run all tests (unit + integration)
cargo test

# Run only unit tests (no PostgreSQL required)
cargo test --lib -- --skip pg_test

# Run integration tests against PostgreSQL 17
cargo pgrx test pg17
```

## Benchmarks

Benchmark scripts are in `benchmarks/`. See [`benchmarks/README.md`](benchmarks/README.md) for details.

```bash
cargo pgrx run pg17
# In another terminal:
psql -f benchmarks/setup.sql
psql -f benchmarks/bench_sum.sql
psql -f benchmarks/bench_baseline.sql
psql -f benchmarks/teardown.sql
```

### Performance Summary (single IMV, PostgreSQL 17)

Trigger overhead only (total DML time minus bare DML time), measured in isolation.

**1M source rows, 1K groups:**

| IMV Type | INSERT 1K | UPDATE 100 | vs REFRESH MATVIEW |
|---|---:|---:|---|
| GROUP BY (SUM/COUNT) | 36 ms | 3 ms | REFRESH: 52 ms |
| Passthrough JOIN | 10 ms | 3 ms | REFRESH: 2,500 ms (**250x faster**) |
| WINDOW (GROUP BY + RANK) | 41 ms | similar | REFRESH: 57 ms |
| UNION ALL (2 operands) | 16 ms | n/a | REFRESH: 410 ms (**25x faster**) |

Key: trigger overhead is O(delta), not O(source). With multiple IMVs on the same source, overhead scales linearly per IMV.

### Comparison: pg_reflex vs pg_ivm vs REFRESH MATERIALIZED VIEW

Measured in isolated databases (one extension per database) on the same hardware.
Source: **5M rows, 30K groups, GROUP BY + SUM/COUNT**.

#### INSERT — trigger overhead

| Batch | pg_reflex | pg_ivm | REFRESH MV |
|---:|---:|---:|---:|
| 1K | 36 ms | 42 ms | 463 ms |
| 10K | 62 ms | 3,012 ms | 470 ms |
| 50K | 221 ms | 26,668 ms | 463 ms |
| 100K | 29 ms | 25,686 ms | 476 ms |
| 500K | 78 ms | 27,510 ms | 526 ms |

#### UPDATE — trigger overhead

| Batch | pg_reflex | pg_ivm | REFRESH MV |
|---:|---:|---:|---:|
| 100 | 9 ms | 8 ms | 462 ms |
| 1K | ~0 ms | 46 ms | 762 ms |
| 10K | ~0 ms | 2,978 ms | 483 ms |

#### DELETE — trigger overhead

| Batch | pg_reflex | pg_ivm | REFRESH MV |
|---:|---:|---:|---:|
| 100 | 24 ms | 22 ms | 470 ms |
| 1K | ~0 ms | 22 ms | 489 ms |
| 10K | 58 ms | 91 ms | 1,321 ms |
| 100K | 193 ms | 551 ms | 467 ms |

#### Read performance

| Operation | pg_reflex | pg_ivm | MATVIEW |
|---|---:|---:|---:|
| Point read (indexed) | 0.014 ms | 0.043 ms | 0.026 ms |
| Full scan (30K rows) | 0.8 ms | 0.7 ms | 0.7 ms |

**Key observations:**
- Both IVM extensions significantly outperform `REFRESH MATERIALIZED VIEW` for small-to-medium batches (1K–10K rows)
- pg_reflex uses MERGE-based batch delta processing, which maintains consistent performance across batch sizes
- pg_ivm performs well on small batches but has higher overhead at larger batch sizes (10K+ rows), likely due to its per-row counting algorithm
- `REFRESH MATERIALIZED VIEW` has constant cost (~470ms) regardless of batch size — it always rescans the full source
- For very large batches (500K rows = 10% of source), `REFRESH` can be more efficient than incremental maintenance

> **Reproduce these benchmarks:** `benchmarks/bench_isolated.sql` (single-extension isolated test) and `benchmarks/bench_vs_pgivm.sql` (side-by-side). Run each extension in its own database for accurate isolated measurements.

## Known Limitations

- **Passthrough DELETE/UPDATE with exact duplicates:** Passthrough IMVs use row-matching for incremental DELETE/UPDATE. If the view contains rows that are identical across ALL columns (exact duplicates), a single-row delete may remove multiple matching rows. This is rare in practice (most queries include a PK or unique column).
- **Recursive CTEs:** `WITH RECURSIVE` is not supported.
- **MIN/MAX/BOOL_OR on DELETE:** Requires full group rescan from the source table (no algebraic inverse for extrema or boolean OR).
- **Non-deterministic functions:** `NOW()`, `RANDOM()`, `CURRENT_DATE` in WHERE clauses are not supported -- the view definition must be static.
- **Subqueries with aggregation in FROM:** `SELECT ... FROM (SELECT SUM(x) ... GROUP BY y) AS sub` is not supported — use a CTE (`WITH sub AS (...)`) instead, which pg_reflex decomposes into sub-IMVs automatically. Simple subqueries without aggregation (e.g., WHERE filters) work correctly.

## Project Structure

```
src/
  lib.rs               -- Extension entry point, create_reflex_ivm, integration tests
  sql_analyzer.rs       -- SQL parsing: extracts GROUP BY, aggregates, JOINs, WHERE
  aggregation.rs        -- Maps user aggregates to sufficient statistics
  query_decomposer.rs   -- Generates base_query (source->intermediate) and end_query (intermediate->target)
  schema_builder.rs     -- DDL generation: tables, indexes, triggers
  trigger.rs            -- Delta processing: MERGE-based delta application, targeted refresh
  bin/pgrx_embed.rs     -- pgrx binary entry point
benchmarks/             -- SQL benchmark scripts (1K to 1M rows)
tests/pg_regress/       -- PostgreSQL regression tests
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
