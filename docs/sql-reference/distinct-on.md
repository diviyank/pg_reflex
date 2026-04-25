# DISTINCT ON

`SELECT DISTINCT ON (cols) … ORDER BY …` (1.1.1+) is decomposed into a **passthrough sub-IMV** plus a **VIEW** with `ROW_NUMBER() OVER (PARTITION BY cols ORDER BY ...) WHERE rn = 1`.

```sql
SELECT create_reflex_ivm('latest_price_per_product',
    'SELECT DISTINCT ON (product_id) product_id, price, recorded_at
     FROM price_history
     ORDER BY product_id, recorded_at DESC');
```

Creates:

- `latest_price_per_product__base` (passthrough sub-IMV)
- `latest_price_per_product` (VIEW: `SELECT product_id, price, recorded_at FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY recorded_at DESC) AS rn FROM latest_price_per_product__base) WHERE rn = 1`)

## What's incremental

INSERT/DELETE/UPDATE on `price_history` propagates to the base passthrough sub-IMV in `O(delta)`. The VIEW applies `ROW_NUMBER` at read time over the full base table.

## What's not

The "winner" selection happens on every read — Postgres doesn't memoise the partition. For large bases with many partitions, the VIEW read can be expensive. If reads dominate, consider materialising the result with a scheduled `reflex_reconcile`.

For DELETE of a row that was the current winner: the base is updated, then the next read re-computes the winner. Correctness is preserved; perf depends on the partition size at read time.
