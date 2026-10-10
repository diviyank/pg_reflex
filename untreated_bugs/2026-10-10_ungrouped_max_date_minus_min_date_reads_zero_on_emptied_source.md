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
