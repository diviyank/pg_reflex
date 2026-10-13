# Ungrouped `MAX(date) - MIN(date)` reads 0 when the source is emptied — S1 (silent, IMMEDIATE)

Pre-existing. Versions: 1.11.5 (6946400) and the 1.12.0 pre-release (0324f47), pg17. Found by the non-regression review of 1.12.0 (full-review-r1, 650 definitions x IMMEDIATE / DEFERRED, every DML step its own committed transaction); same outcome on both trees, so not a regression.

## Reproduction (probe fixture)
```sql
CREATE TABLE orders (id INT PRIMARY KEY, order_date DATE /* other probe columns omitted */);
-- seed some rows, then:
SELECT create_reflex_ivm('r_y09', 'SELECT MAX(order_date) - MIN(order_date) AS span FROM orders');
DELETE FROM orders;
SELECT * FROM r_y09;     -- IMV
SELECT MAX(order_date) - MIN(order_date) AS span FROM orders;   -- query
```

## Observed vs expected
Observed (IMMEDIATE, step "orders delall"): `imv has: (0) || query has: ()` — the IMV holds one row `0`, the query yields NULL. Expected: the IMV equals the query (NULL), as an ungrouped aggregate over no rows yields one NULL row.

## What was ruled out
- Not a regression (identical on 6946400).
- Correct again after the next insert, and correct after "orders truncate" in the same run.
- DEFERRED is correct (`r_y09=ok` at delall).
- The numeric form is correct (y12 `SELECT MAX(amount) - MIN(amount) AS spread, COUNT(*) AS n FROM orders`), as is the grouped form (y13 `SELECT region, MAX(order_date) - MIN(order_date) AS span FROM orders GROUP BY region`).
- Column type is `integer` on both the IMV and the query.

## Fix direction (hypothesis to falsify)
Date minus date is an integer expression over MIN/MAX; the empty-group handling for an ungrouped MIN/MAX expression apparently coalesces the NULL difference to 0 (the date operands become NULL, the integer result becomes 0), where the numeric form keeps NULL. Compare the two paths' empty-source SQL (not read in this review). Test: y09 with delete-all in IMMEDIATE, using the oracle, plus a date-valued `MAX(order_date)` ungrouped.

## Seed rows and steps of the probe (added 2026-10-12, from `full-review-r1-probes.rs` in the ledger)

`orders` and its 60-row seed: as printed in
`2026-10-10_select_distinct_expression_alias_is_lost.md`. The IMV is created over the seeded
table; each step below is its own committed transaction, and the wrong row is read after the
last one. Copied from the probe source; whether `DELETE FROM orders` straight after the create
is enough was not tried.

```sql
-- "orders ins": the seed statement with generate_series(61, 66)
UPDATE orders SET amount = amount + 1000.25, qty = qty + 1, discount = discount + 0.5, note = 'edited',
  is_gift = NOT is_gift, created_at = created_at + INTERVAL '3 days' WHERE id IN (1, 2, 3, 30);
UPDATE orders SET region = CASE id WHEN 4 THEN 'APAC' WHEN 5 THEN NULL ELSE 'EU' END, customer_id = 1 + id % 3,
  product_id = 2, dept_id = 1, store_id = 2, status = 'closed', type = 'refund', order_date = order_date + 45,
  city = 'city0', state = 'cancelled', priority = 2 WHERE id IN (4, 5, 6, 31);
DELETE FROM orders WHERE id IN (7, 8, 9) OR region = 'LATAM';
INSERT INTO orders (id, customer_id, product_id, dept_id, store_id, region, city, status, type, amount, qty,
  order_date, created_at, discount) VALUES
  (10, 2, 3, 1, 1, 'US', 'city1', 'active', 'sale', 77.5, 2, DATE '2024-03-03', TIMESTAMPTZ '2024-03-03 10:00:00+00', 0.5),
  (9001, 3, 4, 2, 2, 'EU', 'city2', 'open', 'sale', 12.5, 1, DATE '2024-03-04', TIMESTAMPTZ '2024-03-04 10:00:00+00', 0)
  ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount, region = EXCLUDED.region, status = EXCLUDED.status;
DELETE FROM orders;            -- "orders delall": IMV (0), query NULL
```
