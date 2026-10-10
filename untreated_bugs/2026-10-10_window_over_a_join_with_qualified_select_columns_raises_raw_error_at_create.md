# A window over a join whose SELECT list holds qualified columns raises a raw error at create

**Severity:** S3 (raw PostgreSQL error at create, nothing created). **Versions:** observed on
1.12.0 pre-release; not checked on 1.11.5.

## Reproduction

```sql
CREATE TABLE orders (id SERIAL PRIMARY KEY, region TEXT, amount NUMERIC);
SELECT create_reflex_ivm('region_ranking',
    'WITH regional_totals AS (
        SELECT region, SUM(amount) AS total FROM orders GROUP BY region
    ),
    regional_counts AS (
        SELECT region, COUNT(*) AS num_orders FROM orders GROUP BY region
    )
    SELECT t.region, t.total, c.num_orders,
           RANK() OVER (ORDER BY total DESC) AS rank
    FROM regional_totals t
    JOIN regional_counts c ON c.region = t.region');
-- ERROR: missing FROM-clause entry for table "t"
```

Expected: the IMV is created, or the definition is refused with a tagged
`[reflex-unsupported]` message naming the remedy.

## What was ruled out

The same query with bare column names and `JOIN … USING (region)` is created and stays correct
(this is the example now in `docs/sql-reference/window-functions.md`). The window layer is
computed over the base sub-IMV's output columns, where the alias `t` no longer exists.

## Fix direction (hypothesis)

Strip or re-map table qualifiers in the SELECT list and window clauses of the view layer to the
base sub-IMV's output columns, or reject qualified references there before any DDL.
