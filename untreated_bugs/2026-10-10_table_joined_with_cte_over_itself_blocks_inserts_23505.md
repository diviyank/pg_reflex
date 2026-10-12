# A table joined with a CTE built over the same table blocks its own inserts (23505) — S2 (write-blocking, IMMEDIATE)

Pre-existing. Versions: 1.11.5 (6946400) and the 1.12.0 pre-release (0324f47), pg17. Found by the non-regression review of 1.12.0 (full-review-r1, 650 definitions x IMMEDIATE / DEFERRED, every DML step its own committed transaction); same outcome on both trees, so not a regression.

## Reproduction (probe fixture `R1_FIXTURE`; row values are the probe's seed, the review does not print them)
```sql
CREATE TABLE products (id INT PRIMARY KEY, name TEXT NOT NULL, sku VARCHAR(20) UNIQUE, category TEXT,
  price NUMERIC(10,2), weight FLOAT8, active BOOLEAN, launched DATE);
-- seed some rows, then (IMMEDIATE, unique key inferred from the primary key):
SELECT create_reflex_ivm('r_c17',
  'WITH avgp AS (SELECT category, AVG(price) AS ap FROM products GROUP BY category)
   SELECT p.id, p.name, p.price, a.ap FROM products p JOIN avgp a ON a.category = p.category WHERE p.price > a.ap');
INSERT INTO products ...;   -- 6 rows, one statement
```
Registry (review row `R`): `r_c17{IMMEDIATE,2} r_c17__cte_avgp{IMMEDIATE,1}`; both read `products`.

Other definitions that fail the same way (all IMMEDIATE, all raise on `INSERT INTO <table>`):
- z09 `WITH latest AS (SELECT customer_id, MAX(created_at) AS last_at FROM orders GROUP BY customer_id) SELECT o.id, o.customer_id, o.amount FROM orders o JOIN latest l ON l.customer_id = o.customer_id AND l.last_at = o.created_at`, key `'id'`
- y15 (key `id`) and y16 (no key, key inferred): `WITH m AS (SELECT region, MAX(amount) AS hi FROM orders GROUP BY region) SELECT o.id, o.amount FROM orders o JOIN m ON m.region = o.region AND m.hi = o.amount`

## Observed vs expected
Observed (c17, step "products ins"): `ERROR: duplicate key value violates unique constraint "__reflex_uk_r_c17" DETAIL: Key (id)=(13) already exists. CONTEXT: SQL statement "INSERT INTO "r_c17" SELECT p.id, p.name, p.price, a.ap FROM ...`; the INSERT is refused (`r_c17` 0/3 rows rewritten). y16: same on `orders ins` (Key (id)=(65)). Same raise on the refill and reseed steps after a delete-all. UPDATE, DELETE, upsert and TRUNCATE steps pass and the IMV is correct. DEFERRED is correct at every step.
Expected: the insert commits and the IMV equals its query.

## What was ruled out
- Not a regression: identical on 6946400.
- Not DEFERRED: the flush nets the transaction into one pass.
- Not the declared key: y16 has none (the unique index is inferred) and fails the same way.

## Mechanism (hypothesis, not reproduced under a debugger)
One statement changes two inputs of the join at once: the table and its CTE sub-IMV. The outer IMV's insert delta is applied once for the table's trigger and once more for the sub-IMV's change, and the second application inserts rows the first already inserted, hitting the unique index. This is the class of `2026-10-02_single_statement_two_source_write_double_counts_join_delta.md`, which does not name this one-statement, one-table reproduction (the second source is reached through the CTE, not written by the user).

## Fix direction (hypothesis to falsify)
Treat this shape as that report's case: when the same statement already merged a delta through another input of the IMV, fall back to a reconcile of the IMV for the statement. Test: c17 and y16 with the `assert_imv_correct` oracle after INSERT, refill and reseed in IMMEDIATE.

## Seed rows and the failing statement (added 2026-10-12, from `full-review-r1-probes.rs` in the ledger)

Copied from the probe source; not re-run as a standalone script. c17: the table above seeded
with 12 rows, the IMV created, then the first step ("products ins") is the same statement for
ids 13 to 18 and raises `Key (id)=(13) already exists`.

```sql
INSERT INTO products SELECT g, 'prod' || g, 'SKU-' || g, (ARRAY['toys','books','food',NULL])[1 + g % 4],
  (g * 2.5)::numeric(10,2), g * 0.5, g % 3 <> 0, DATE '2023-01-01' + g * 10 FROM generate_series(1, 12) g;
-- create r_c17, then:
INSERT INTO products SELECT g, 'prod' || g, 'SKU-' || g, (ARRAY['toys','books','food',NULL])[1 + g % 4],
  (g * 2.5)::numeric(10,2), g * 0.5, g % 3 <> 0, DATE '2023-01-01' + g * 10 FROM generate_series(13, 18) g;
```

z09, y15, y16: `orders` and its 60-row seed as printed in
`2026-10-10_select_distinct_expression_alias_is_lost.md`; the failing step is the same seed
statement with `generate_series(61, 66)` (y16: `Key (id)=(65)`).
