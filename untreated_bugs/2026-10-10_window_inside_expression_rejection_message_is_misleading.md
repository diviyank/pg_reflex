# A window function inside an expression is rejected with "only supported in the top-level SELECT" although it is in the top-level SELECT — S3 (misleading message)

Pre-existing. Versions: 1.11.5 (6946400) and the 1.12.0 pre-release (0324f47), pg17. Found by the non-regression review of 1.12.0 (full-review-r1, 650 definitions x IMMEDIATE / DEFERRED, every DML step its own committed transaction); same outcome on both trees, so not a regression.

## Reproduction (probe fixture: `orders`, `measurements (id INT, ts TIMESTAMPTZ, value FLOAT8, sensor TEXT)`)
```sql
SELECT create_reflex_ivm('r_w05', 'SELECT region, SUM(amount) AS total, SUM(amount) / SUM(SUM(amount)) OVER () AS share FROM orders GROUP BY region');
SELECT create_reflex_ivm('r_w08', 'SELECT id, region, amount, AVG(amount) OVER (PARTITION BY region) AS region_avg, amount - AVG(amount) OVER (PARTITION BY region) AS diff FROM orders');
SELECT create_reflex_ivm('r_w15', 'SELECT id, ts, value, LEAD(value, 1, 0) OVER (ORDER BY ts) - value AS delta FROM measurements');
-- x62: SELECT region, SUM(amount) AS total, RANK() OVER (ORDER BY SUM(amount) DESC) AS rk, SUM(amount) / SUM(SUM(amount)) OVER () AS share FROM orders GROUP BY region
```

## Observed vs expected
All four, both modes: `[reflex-unsupported] Window functions are only supported in the top-level SELECT. A window function inside a subquery or derived table cannot be incrementally maintained — move it to the outermost SELECT, or define this view with kind: mv.`
The window is already in the outermost SELECT; it sits inside an arithmetic expression. The remedy cannot be followed. Expected: a message naming the real restriction (a window function must be a whole select item, not an operand of an expression) with the remedy of computing the window in an inner query or CTE and combining it in an outer one, or support.

## What was ruled out
- Not a regression (identical on 6946400).
- The rejection itself is not asserted wrong: the review does not show that these expressions could be maintained, only that the message misdescribes the cause.
- Docs: `docs/sql-reference/window-functions.md:46` (`region_ranking`, `RANK() OVER (ORDER BY t.total DESC)`) is presented as supported but fails with a raw `missing FROM-clause entry for table "t"` (the review's M2; related raw-error report `2026-10-07_window_query_qualified_select_item_or_named_window_raw_error.md`).

## Fix direction (hypothesis)
Distinguish "window inside a subquery / derived table" from "window nested in an expression" in the rejection (the detector finds a window anywhere below the select item, then emits the subquery text). Test: the message for w05, w08, w15, x62 does not say "subquery or derived table" and names the nested expression.
