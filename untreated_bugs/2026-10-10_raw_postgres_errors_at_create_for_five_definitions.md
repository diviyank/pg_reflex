# Five definitions fail at create with a raw PostgreSQL error instead of a tagged rejection — S3 (loud at create)

Pre-existing. Versions: 1.11.5 (6946400) and the 1.12.0 pre-release (0324f47), pg17. Found by the non-regression review of 1.12.0 (full-review-r1, 650 definitions x IMMEDIATE / DEFERRED, every DML step its own committed transaction); same outcome on both trees, so not a regression.

Choice: one report. The review lists these as one finding, the symptom and the contract fix (a tagged `[reflex-unsupported]` rejection with a remedy, or support) are the same, but the causes are distinct (four mechanisms, table below), so each row can be closed on its own. Same shape as `2026-10-07_five_untagged_raw_errors_at_create_found_by_g4_review.md`. The review's own count says three, its text lists five (z04, o06, c13, x68 and x69); all five are reported here.

Probe fixture: `measurements (id INT, ts TIMESTAMPTZ, value FLOAT8, sensor TEXT)`, `orders`, `domestic_orders (id INT PRIMARY KEY, region TEXT, amount NUMERIC)`, `international_orders` (same columns).

| Id | Definition | Error at create (head output) |
|---|---|---|
| z04 | `create_reflex_ivm('all_sales', 'SELECT region, amount FROM domestic_orders UNION ALL SELECT region, amount FROM international_orders')` succeeds, then `create_reflex_ivm('all_sales_by_region', 'SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM all_sales GROUP BY region')` | `"all_sales" is a view DETAIL: Triggers on views cannot have transition tables.` (the first IMV's registry rows are `all_sales{IMMEDIATE,2} all_sales__union_0 all_sales__union_1`) |
| o06 | `SELECT DISTINCT ON (date_trunc('day', ts)) date_trunc('day', ts) AS day, id, value FROM measurements ORDER BY date_trunc('day', ts), ts DESC` | `syntax error at or near "ts"` |
| c13 | `WITH t(r, s) AS (SELECT region, SUM(amount) FROM orders GROUP BY region) SELECT r, s FROM t` | `column "r" does not exist` |
| x68 | `create_reflex_ivm('r_x68', 'SELECT id, region FROM orders', 'region', 'UNLOGGED', '{M}')` (declared key is not unique in the data) | `could not create unique index "__reflex_uk_r_x68" DETAIL: Key (region)=(EU) is duplicated.` |
| x69 | same with key `'nope'` (names no column) | `column "nope" does not exist` |

Expected: a tagged rejection naming the construct and a remedy, or support. The same outcome in both modes.

## What was ruled out
- Not a regression (identical on 6946400).
- `q03 BETWEEN SYMMETRIC` is already a tagged parse failure (not part of this report).
- A UNION ALL IMV used as a source is accepted by a nearby path (`z08` is a UNION ALL CTE and works); z04 differs by reading the registered UNION ALL IMV, which is a view.
- **Not checked:** whether any of the five failures leaves relations or registry rows behind. The review did not look.

## Fix direction (hypotheses to falsify, one per row)
- z04: a UNION ALL IMV is a view over per-operand sub-IMVs; an IMV over it must attach triggers to the operands (or refuse with a remedy to read the sub-IMVs' union). Related: `2026-10-08_union_all_subquery_operands_with_different_column_names_raise_on_dml.md`.
- o06: DISTINCT ON with a function-call expression is rewritten by text and cuts at the `ts`; accept or refuse expression keys.
- c13: a CTE column list is not applied before the CTE body is rewritten; refuse it with the remedy "rename in the CTE's select list".
- x68/x69: validate the declared key (exists in the select list, unique in the current data) before creating objects, and return a tagged error.
