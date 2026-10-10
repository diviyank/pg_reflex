# `SELECT DISTINCT <expr> AS alias` loses its alias — S3 (wrong column name, contents correct)

Pre-existing. Versions: 1.11.5 (6946400) and the 1.12.0 pre-release (0324f47), pg17. Found by the non-regression review of 1.12.0 (full-review-r1, 650 definitions x IMMEDIATE / DEFERRED, every DML step its own committed transaction); same outcome on both trees, so not a regression.

## Reproduction (probe fixture; `orders` as in the fixture, columns `region TEXT, order_date DATE, created_at TIMESTAMPTZ` used)
```sql
SELECT create_reflex_ivm('r_g36', 'SELECT DISTINCT date_trunc(''month'', order_date)::date AS month FROM orders');
SELECT create_reflex_ivm('r_y19', 'SELECT DISTINCT upper(region) AS region_u FROM orders');
SELECT create_reflex_ivm('r_y20', 'SELECT DISTINCT created_at::date AS day FROM orders');
```

## Observed vs expected
| Definition | PostgreSQL's column | IMV column |
|---|---|---|
| g36 | `month:date` | `date_trunc__month___order_date___date:date` |
| y19 | `region_u:text` | `upper_region:text` |
| y20 | `day:date` | `created_at::date:date` (a column literally named so) |

Contents are correct at every step; only the name differs. Expected: the alias the user wrote. A reader selecting `month` from the IMV fails.

## What was ruled out
- Not a regression (identical on 6946400).
- A plain column or a multi-column DISTINCT (a53, g35) keeps its names.
- Unaliased aggregates are also named `<agg>_<arg>` (`max_seen_at` where PostgreSQL says `max`) on both trees (d21, d23, d38): a separate, related naming difference the review lists but does not detail.

## Fix direction (hypothesis to falsify)
The DISTINCT path names output columns from the expression text instead of the alias, in the code that normalises a SELECT DISTINCT into the grouped form. Use the alias when present. Test: assert the IMV's column names and types equal PostgreSQL's for the query (the review's `P`/`T` comparison), for the three definitions above.
