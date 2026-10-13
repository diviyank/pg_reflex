# UPDATE of a column the view does not read, on a table the view reads twice, now recomputes the view in full — perf (new in 1.12.0, correctness unaffected)

Review finding M3. Versions: 1.12.0 pre-release (0324f47) only; 6946400 (1.11.5) skipped these updates. Result is correct on both; the cost is not.

## Observed (from the review, section 4, cause 3b)
Commit 6e7dc16 "keep the UPDATE skip off for a source read more than once". An UPDATE that changes no column the IMV reads used to be skipped; for a source read more than once (self-join, two aliases of one dimension, a table also read by a subquery) it now runs the full recompute that these shapes already had for real changes. Rows of the IMV rewritten per salary / unrelated update, 6946400 -> 0324f47:

| Def | Shape | rewritten / total |
|---|---|---|
| j26 | employee / manager self LEFT JOIN, salary update | 0 -> 22 of 22 |
| j43 | two joins to `customers` | 0 -> 60 of 60 |
| j50, r14, s08 | (definitions not quoted in the review) | same direction |
| r01 | transfers with two account roles | 0 -> 40 of 40 |

Measured by the `xmin` of the registered tables' rows newer than the step. The SQL of j26 is not printed in the review beyond "employee / manager self LEFT JOIN"; no minimal reproduction is available from it. Reproduce by self-joining `employees` on `manager_id` and running `UPDATE employees SET <column the view does not read> = ...`, then comparing row `xmin`.

## Status
Documented only in commit 6e7dc16: not in any user-facing page (the review asks for release-notes text, M3). This report is the tracking record.

## Fix direction (hypothesis)
The skip is unsound for these shapes only if the unread column can still change what the second read sees; if the set of columns read through every alias of the table is collected once, the skip is sound whenever the changed columns are outside that union. Falsify with the oracle on j26/j43 for UPDATE of an unread column and of a column read through only one alias.

## Definitions and seed rows (added 2026-10-12, from `full-review-r1-probes.rs` in the ledger)

The definitions the review did not quote, copied from the probe source (not re-run as a
standalone script):

- j26 (key `id`): `SELECT e.id, e.name, m.name AS manager FROM employees e LEFT JOIN employees m ON m.id = e.manager_id`
- j43 (key `id`): `SELECT o.id, c1.name AS buyer, c2.name AS other FROM orders o LEFT JOIN customers c1 ON c1.id = o.customer_id LEFT JOIN customers c2 ON c2.id = o.store_id`
- j50: `SELECT p.id, COUNT(*) AS cheaper FROM products p JOIN products q ON q.price < p.price GROUP BY p.id`
- s08 (key `id`): `SELECT id, amount FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)`
- r01: `SELECT t.id, a1.owner AS src_owner, a2.owner AS dst_owner FROM app.transfers t JOIN app.accounts a1 ON a1.id = t.account_id JOIN app.accounts a2 ON a2.id = 1 + t.id % 12`
- r14: `SELECT e.id, e.name, d.name AS dept, m.name AS manager FROM employees e JOIN departments d ON d.id = e.dept_id LEFT JOIN employees m ON m.id = e.manager_id`

j26, the smallest:

```sql
CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, dept_id INT, manager_id INT,
  salary NUMERIC(10,2), region TEXT, hired DATE);
INSERT INTO employees SELECT g, 'emp' || g, (ARRAY['eng','ops','sales',NULL])[1 + g % 4], 1 + g % 5,
  CASE WHEN g <= 2 THEN NULL ELSE 1 + g % 4 END, (1000 + g * 150.5)::numeric(10,2),
  (ARRAY['US','EU','ASIA'])[1 + g % 3], DATE '2020-01-01' + g * 30 FROM generate_series(1, 16) g;
-- create j26, then "employees ins" (the seed with generate_series(17, 22)), then the measured step:
UPDATE employees SET salary = salary + 500 WHERE id IN (1, 2, 17);   -- salary is not read by j26
-- rows of the IMV with an xmin newer than the step: 0 of 22 on 6946400, 22 of 22 on 0324f47
```
