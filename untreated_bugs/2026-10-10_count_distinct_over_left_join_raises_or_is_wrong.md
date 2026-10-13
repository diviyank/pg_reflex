# `COUNT(DISTINCT e.id)` over a LEFT JOIN blocks writes (IMMEDIATE) or goes silently wrong (DEFERRED) — S1 / S2

Pre-existing. Versions: 1.11.5 (6946400) and the 1.12.0 pre-release (0324f47), pg17. Found by the non-regression review of 1.12.0 (full-review-r1, 650 definitions x IMMEDIATE / DEFERRED, every DML step its own committed transaction); same outcome on both trees, so not a regression.

## Reproduction (probe fixture)
```sql
CREATE TABLE departments (id INT PRIMARY KEY, name TEXT, region TEXT);
CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, dept_id INT, manager_id INT,
  salary NUMERIC(10,2), region TEXT, hired DATE);
-- seed both (probe seed not printed in the review), then:
SELECT create_reflex_ivm('r_r15',
  'SELECT d.name, COUNT(DISTINCT e.id) AS staff FROM departments d LEFT JOIN employees e ON e.dept_id = d.id GROUP BY d.name');
INSERT INTO employees ...;
```

## Observed vs expected
- IMMEDIATE, step "employees ins": `ERROR: column r_r15.id does not exist LINE 1: ...r15" AS __a WHERE "r_r15"."name" = __a."name" AND "r_r15"."i...  QUERY: DELETE FROM "r_...` — the write is refused.
- DEFERRED, same step: no error, the IMV is silently wrong (4 rows differ): imv has `(hr,3) ; (sales,3) ; (rd,4)`, query has `(hr,5) ; (sales,4) ; (ops,4)`. It stays wrong at every later step (e.g. after delete-all the IMV still holds those rows, the query `(rd,0) ; (ops,0) ; (hr,0)`).
- Creation succeeds in both modes and the state at create is correct.
- Expected: the IMV equals the query after every write.

## What was ruled out
- Not a regression (identical on 6946400).
- The INNER JOIN form is correct (y17 `SELECT d.name, COUNT(DISTINCT e.id) AS staff FROM departments d JOIN employees e ON e.dept_id = d.id GROUP BY d.name`).
- Writes to `departments` (the left table) pass in both modes; only writes to `employees` (the nullable side) fail.

## Mechanism (hypothesis)
The IMMEDIATE error shows the generated `DELETE` joins the target to a delta on columns named `id`, a name the target `r_r15` does not have (its columns are `name`, `staff`): the COUNT(DISTINCT) value-tracking structure for the nullable side of a LEFT JOIN looks up the aggregated argument `e.id` in the wrong relation. Not located in source.

## Fix direction (hypothesis to falsify)
Either fix the delta for COUNT(DISTINCT) on the nullable side of an outer join or refuse it at create with a tagged rejection and remedy (compute the distinct count in a CTE, as the 1.12.0 COUNT(DISTINCT) rejections already advise). Given the DEFERRED silent-wrong result the refusal is the safe floor. Tests: r15 and y17 with the oracle, both modes, writes to both tables.

## Seed rows and steps of the probe (added 2026-10-12, from `full-review-r1-probes.rs` in the ledger)

Copied from the probe source; not re-run as a standalone script. Each statement is its own
committed transaction. The probe runs the nine `departments` steps before the first `employees`
step, so the failing "employees ins" comes tenth; whether it fails straight after the create was
not tried.

```sql
INSERT INTO departments SELECT g, (ARRAY['sales','ops','rd','hr'])[1 + g % 4], (ARRAY['US','EU'])[1 + g % 2]
  FROM generate_series(1, 4) g;
INSERT INTO employees SELECT g, 'emp' || g, (ARRAY['eng','ops','sales',NULL])[1 + g % 4], 1 + g % 5,
  CASE WHEN g <= 2 THEN NULL ELSE 1 + g % 4 END, (1000 + g * 150.5)::numeric(10,2),
  (ARRAY['US','EU','ASIA'])[1 + g % 3], DATE '2020-01-01' + g * 30 FROM generate_series(1, 16) g;
-- create r_r15, then the departments steps:
--   the departments seed with generate_series(5, 10)
UPDATE departments SET region = 'ASIA' WHERE id IN (1, 5);
UPDATE departments SET name = CASE id WHEN 2 THEN 'legal' ELSE 'ops' END WHERE id IN (2, 3);
DELETE FROM departments WHERE id IN (4, 6);
INSERT INTO departments VALUES (1, 'sales2', 'EU'), (9004, 'newd', 'US')
  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, region = EXCLUDED.region;
DELETE FROM departments;
--   the departments seed with generate_series(1, 2)
TRUNCATE departments;
--   the departments seed with generate_series(1, 4)
-- "employees ins": the employees seed with generate_series(17, 22)
--   IMMEDIATE: ERROR column r_r15.id does not exist; DEFERRED: 4 rows differ
```
