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

## Seed rows of the probe (added 2026-10-12, from `full-review-r1-probes.rs` in the ledger)

The outcome is at create, so the fixture and the seed are enough. Copied from the probe source;
not re-run as a standalone script.

```sql
CREATE TYPE order_state AS ENUM ('new', 'paid', 'shipped', 'cancelled');
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, product_id INT, dept_id INT, store_id INT,
  region TEXT, city TEXT, status TEXT, type TEXT, amount NUMERIC, qty INT, order_date DATE,
  created_at TIMESTAMPTZ, discount FLOAT8, note VARCHAR(40), meta JSONB, ref UUID, tags TEXT[],
  state order_state, priority SMALLINT, is_gift BOOLEAN, big BIGINT);
INSERT INTO orders SELECT g, CASE WHEN g % 13 = 0 THEN NULL ELSE 1 + g % 11 END, 1 + g % 13,
  1 + g % 5, 1 + g % 5, (ARRAY['US','EU','ASIA','LATAM','US','EU',NULL])[1 + g % 7],
  'city' || g % 9, (ARRAY['active','open','closed','cancelled'])[1 + g % 4],
  (ARRAY['sale','refund','sale'])[1 + g % 3],
  CASE WHEN g % 53 = 17 THEN NULL ELSE (g * 37 % 500) + 0.5 * (g % 2) END, g % 6,
  DATE '2024-01-01' + (g * 5) % 200, TIMESTAMPTZ '2024-01-01 00:00:00+00' + g * INTERVAL '31 hours',
  (g % 5) * 0.25, CASE WHEN g % 4 = 0 THEN NULL ELSE 'note ' || g % 6 END,
  jsonb_build_object('src', (ARRAY['web','app'])[1 + g % 2], 'n', g % 4), md5((g % 20)::text)::uuid,
  ARRAY['t' || g % 3, 'u' || g % 2], (ARRAY['new','paid','shipped','cancelled'])[1 + g % 4]::order_state,
  (g % 3)::smallint, g % 5 = 0, g::bigint * 1000000000 FROM generate_series(1, 60) g;
```
