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

### Production-Scale Benchmark: Passthrough IMV (5-table JOIN)

Two benchmarks covering different scales. The 1.3.0 number set is the freshest;
the 76M-row dataset is preserved for the high-end shape comparison.

#### 1.3.0 — 10M-row source, 5-table JOIN, IMMEDIATE (PG 18, shared_buffers = 4GB)

`benchmarks/bench_full_scale_1_3_0.sql`. Source: 10M rows, 5-table JOIN
(sales × product 100K × location 1K × calendar 3.3K × pricing 10M).
**REFRESH MATERIALIZED VIEW baseline: 24,130 ms.** Correctness verified
byte-for-byte via `EXCEPT ALL` against fresh REFRESH MV after every run.

| Op | Batch | Reflex | Raw | Advantage vs raw + REFRESH |
|---|---:|---:|---:|---:|
| INSERT | 1K | 36 ms | 10 ms | **99.8%** |
| INSERT | 10K | 362 ms | 62 ms | 98.5% |
| INSERT | 100K | 2.4 s | 546 ms | 90.2% |
| INSERT | 500K | 11.4 s | 3.0 s | 58.0% |
| INSERT | 1M | 22.3 s | 6.8 s | 27.9% |
| DELETE | 1K | 445 ms | 88 ms | 98.2% |
| DELETE | 10K | 115 ms | 93 ms | **99.5%** |
| DELETE | 100K | 357 ms | 141 ms | 98.5% |
| DELETE | 500K | 1.7 s | 464 ms | 93.2% |
| DELETE | 1M | 3.5 s | 721 ms | **86.0%** |
| UPDATE | 1K | 363 ms | 92 ms | 98.5% |
| UPDATE | 10K | 414 ms | 195 ms | 98.3% |
| UPDATE | 100K | 3.4 s | 1.2 s | 86.5% |
| UPDATE | 500K | 17.1 s | 5.5 s | 42.2% |
| UPDATE | 1M | 32.3 s | 11.6 s | parity (≈ REFRESH) |

**pg_reflex wins at every batch size up to 500K rows for all operations.**
DELETE remains the standout — even 1M-row deletions cost 3.5s vs 24s REFRESH
(86% advantage). The lone case where pg_reflex matches REFRESH is UPDATE 1M;
the JOIN cost dominates at that scale. The optimization roadmap (with
attempted/landed/deferred status) lives at
[`journal/2026-04-25_full_scale_bench_and_optimizations.md`](journal/2026-04-25_full_scale_bench_and_optimizations.md).

#### 1.1.x — 76M-row source production reference

The legacy production-scale dataset: **76M source rows, 7.7M output rows,
5-table JOIN with LEFT JOINs, 9 source indexes, 5 target indexes.**
shared_buffers = 4GB, 32GB RAM, local SSD. REFRESH MATERIALIZED VIEW
baseline: **39–64s** (depending on pool size).

#### INSERT — pg_reflex advantage (% faster than raw DML + REFRESH)

| Batch | Reflex | raw + REFRESH | Advantage |
|------:|-------:|--------------:|----------:|
| 1K | 235 ms | 38.9s | **99.4%** |
| 10K | 1.6s | 39.1s | **96.0%** |
| 50K | 5.7s | 44.8s | **87.2%** |
| 100K | 16.5s | 48.9s | **66.3%** |
| 500K | 58.8s | 93.2s | **36.9%** |
| 1M | 55.1s | 110.8s | **50.3%** |
| 2M | 2:59 | 4:57 | **39.8%** |

#### DELETE — key-based targeting dominates at all sizes

| Batch | Reflex | raw + REFRESH | Advantage |
|------:|-------:|--------------:|----------:|
| 1K | 508 ms | 39.0s | **98.7%** |
| 50K | 297 ms | 38.9s | **99.2%** |
| 200K | 1.3s | 39.1s | **96.7%** |
| 500K | 18.7s | 67.3s | **72.3%** |
| 1M | 14.1s | 69.8s | **79.8%** |
| 2M | 24.0s | 93.3s | **74.3%** |

#### UPDATE — competitive even at extreme batch sizes

| Batch | Reflex | raw + REFRESH | Advantage |
|------:|-------:|--------------:|----------:|
| 10K | 416 ms | 41.4s | **99.0%** |
| 50K | 7.3s | 44.5s | **83.5%** |
| 100K | 20.6s | 51.2s | **59.7%** |
| 500K | 53.5s | 99.0s | **46.0%** |
| 1M | 3:27 | 4:33 | **24.2%** |
| 2M | 7:49 | 8:30 | **8.0%** |

**pg_reflex wins at every batch size up to 2M rows for all operations.** No break-even reached. DELETE is the standout — key-based targeted deletion scales linearly and never approaches the fixed REFRESH cost.

#### Trigger internals (instrumented, per batch size)

Framework overhead (EXISTS check, metadata query, advisory lock, Rust FFI) is **< 1ms** at all batch sizes. The trigger is 15–21% of total INSERT time — the rest is the source table's own overhead (9 indexes + FK constraints).

| Batch | Trigger delta INSERT | Trigger % of total |
|------:|---------------------:|-------------------:|
| 1K | 22 ms | 16% |
| 10K | 232 ms | 28% |
| 50K | 1,042 ms | 19% |
| 100K | 2,051 ms | 21% |

### 1.3.0 — Top-K MIN/MAX retraction (audit R3 closed)

The opt-in `topk=K` parameter on `create_reflex_ivm` keeps the K extremum
values per group and uses multi-set subtraction on retraction, falling back
to the existing scoped recompute only when the heap underflows. The recompute
path is gated by an `EXISTS` check so the source scan is skipped when no
group needs it.

**5M-row source, 5K groups, MIN/MAX IMV — PG 18:**

| DELETE batch | REFRESH MV | IMV (no topk) | IMV (`topk=16`) | top-K vs no-topk | top-K vs REFRESH |
|---:|---:|---:|---:|---:|---:|
| 100   | 529 ms | 479 ms     | **93 ms**  | **5.1× faster** | **5.7× faster** |
| 1,000 | 529 ms | 1,551 ms   | **556 ms** | **2.8× faster** | parity |
| 10,000| 540 ms | 14,847 ms  | **2,726 ms** | **5.4× faster** | 0.2× (REFRESH wins) |
| 50,000| 540 ms | 14,888 ms  | **2,908 ms** | **5.1× faster** | 0.2× (REFRESH wins) |

INSERT cost is ~2.5× higher with top-K than without (264 ms vs 109 ms on
1M rows × 10K inserts) — the price of maintaining the heap on every write.
For workloads where retraction is common, top-K is a clean win.

> **Reproduce:** `benchmarks/bench_1_3_0_topk.sql` (1M rows) and
> `benchmarks/bench_1_3_0_topk_5m.sql` (5M rows). The full breakdown lives
> in [`docs/performance/benchmarks.md`](docs/performance/benchmarks.md).

### Synthetic Benchmarks (5M rows, 30K groups)

Trigger overhead only (GROUP BY + SUM/COUNT), measured in isolation.

#### pg_reflex vs pg_ivm vs REFRESH MATERIALIZED VIEW

| Batch | pg_reflex | pg_ivm | REFRESH MV |
|---:|---:|---:|---:|
| 1K | 36 ms | 42 ms | 463 ms |
| 10K | 62 ms | 3,012 ms | 470 ms |
| 50K | 221 ms | 26,668 ms | 463 ms |
| 100K | 29 ms | 25,686 ms | 476 ms |
| 500K | 78 ms | 27,510 ms | 526 ms |

> **Reproduce:** `benchmarks/bench_sop_4gb.sql` (production-scale), `benchmarks/bench_sop_4gb_large.sql` (1M/2M batches), `benchmarks/bench_isolated.sql` (synthetic). See `benchmarks/README.md` for setup details.

## Known Limitations

- **Passthrough DELETE/UPDATE with exact duplicates:** Passthrough IMVs use row-matching for incremental DELETE/UPDATE. If the view contains rows that are identical across ALL columns (exact duplicates), a single-row delete may remove multiple matching rows. This is rare in practice (most queries include a PK or unique column).
- **Recursive CTEs:** `WITH RECURSIVE` is not supported.
- **MIN/MAX on DELETE:** the stored extremum is nulled on every retraction and re-derived from the source, scoped to the groups affected by the delta (not the full table). Workloads that retract from the same hot group repeatedly may still re-scan that group's rows on each flush. `BOOL_OR` is algebraic since 1.1.3 and carries no retraction cost.
- **Non-deterministic functions:** `NOW()`, `RANDOM()`, `CURRENT_DATE` in WHERE clauses are not supported -- the view definition must be static.
- **Subqueries with aggregation in FROM:** `SELECT ... FROM (SELECT SUM(x) ... GROUP BY y) AS sub` is not supported — use a CTE (`WITH sub AS (...)`) instead, which pg_reflex decomposes into sub-IMVs automatically. Simple subqueries without aggregation (e.g., WHERE filters) work correctly.

## Monitoring

Since 1.2.0 pg_reflex exposes three read-only SPIs for operational visibility:

- `reflex_ivm_status()` — one row per registered IMV with `graph_depth`, `refresh_mode`, live `row_count`, `last_flush_ms`, `last_flush_rows`, `flush_count`, `last_error`, `last_update_date`.
- `reflex_ivm_stats(view_name)` — intermediate/target byte sizes, index count, trigger count, last flush timing breakdown.
- `reflex_explain_flush(view_name)` — `EXPLAIN (ANALYZE FALSE, VERBOSE)` of the SQL that the next flush would emit, without actually running it.

Each per-IMV flush body is wrapped in a `SAVEPOINT`. A failing IMV logs a `WARNING`, records `last_error`, and does not abort the rest of the cascade — cascade correctness is a per-IMV property, not a per-transaction one.

## Operational notes

- **Source `DROP TABLE`** — the `reflex_on_sql_drop` event trigger auto-drops every artifact owned by the IMV (target, intermediate, affected-groups, delta, and passthrough scratch tables), removes its registry row, and cascades to child IMVs in `graph_child` order. A `NOTICE` is emitted for each IMV cleaned up. To preserve an IMV across a recreated source, drop the source with `drop_reflex_ivm` *first* so you can re-`create_reflex_ivm` against the new table.
- **Source `ALTER TABLE`** — the `reflex_on_ddl_command_end` trigger raises a `WARNING` when a tracked source is altered, with guidance to run `SELECT reflex_rebuild_imv('<name>')`. The extension will not block the ALTER.
- **Concurrent flushes** — the per-(view, source) advisory lock keys are derived from a 64-bit hash joined into two i32s (`pg_advisory_xact_lock(key1, key2)`), so two sessions flushing distinct IMVs on the same source no longer serialize on the same integer.

## Troubleshooting

A short operator runbook covering the cases that show up in production. The full guide — including the LOGGED-vs-UNLOGGED decision matrix, top-K caveats, and pg_cron recipes — is on the [docs site](https://diviyank.github.io/pg_reflex/operations/runbook/).

### Flush keeps failing on one IMV

```sql
-- Find the bad IMV
SELECT name, last_error, flush_count, last_flush_ms
FROM public.__reflex_ivm_reference
WHERE last_error IS NOT NULL;

-- Inspect what the next flush would run
SELECT reflex_explain_flush('<name>');

-- If the source schema changed, rebuild from scratch
SELECT reflex_rebuild_imv('<name>');
```

A failing IMV no longer aborts the cascade — its `last_error` is recorded and the next IMV runs normally (per-IMV SAVEPOINT, since 1.2.0).

### IMV drifted after a crash

UNLOGGED intermediates are truncated on crash recovery. Run `reflex_rebuild_imv('<name>')` to rebuild from the source, or schedule it via pg_cron (see [`pg-cron` recipes in the docs](#documentation-site)).

### Source `ALTER TABLE` warning emitted

The IMV may now reference dropped or renamed columns. Run `reflex_rebuild_imv('<name>')` as part of your DDL change-control runbook. From 1.2.1 onward, set `pg_reflex.alter_source_policy = 'error'` to reject the ALTER instead.

### Cascade is slow

```sql
-- Sort by depth, then by last flush latency
SELECT name, graph_depth, last_flush_ms, last_flush_rows, flush_count
FROM reflex_ivm_status()
ORDER BY graph_depth, last_flush_ms DESC NULLS LAST;
```

If one IMV dominates the latency budget, check `reflex_explain_flush(name)` for an unexpected sequential scan on the source. MIN/MAX-heavy IMVs over wide source tables are a known sharp edge — see the "MIN/MAX retraction" entry in the limitations matrix.

### The IMV was created but DELETE on the source fails

Passthrough IMVs that handle DELETE require a unique key. From 1.2.1 the extension auto-infers `unique_columns` from the source PK; before that, pass `unique_columns` explicitly:

```sql
SELECT create_reflex_ivm('v', 'SELECT id, name FROM src', 'id');
```

### Flush is looping or stuck

```sql
SELECT pid, NOW() - query_start AS elapsed, query
FROM pg_stat_activity
WHERE application_name LIKE 'reflex_flush:%'
ORDER BY elapsed DESC NULLS LAST;
```

If one IMV is consistently slow, check `reflex_explain_flush(name)` for an unexpected source seq-scan. If genuinely stuck, `pg_cancel_backend(<pid>)` then `reflex_rebuild_imv('<name>')` — the per-IMV SAVEPOINT keeps the cascade consistent.

### Top-K is auto-enabled for MIN/MAX

`create_reflex_ivm` auto-enables top-K (K=16) on MIN/MAX intermediate columns. The parameter is a no-op for SUM/COUNT/AVG/BOOL_OR. To disable for append-only MIN/MAX workloads, call the 6-arg overload with `topk=0`.

### Picking LOGGED vs UNLOGGED for an IMV

UNLOGGED is the default and gives 2-4× lower flush latency. Use LOGGED for IMVs that back SLA-bound reads where post-crash drift is unacceptable, or for very small IMVs where WAL overhead is negligible. Decision matrix: [docs/operations/crash-recovery](https://diviyank.github.io/pg_reflex/operations/crash-recovery/#picking-logged-vs-unlogged-decision-guide).

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
