# A statement writing two sources of a join IMV whose shape is outside the exact correction recomputes the IMV — perf (correct, not incremental)

Residual of the 1.12.5 two-source fix (branch `fix/two-source-statement-recompute`). Since 1.12.5 a
statement that writes two sources A and B of an IMMEDIATE join IMV (writable CTE, MERGE or upsert,
a user trigger on A writing B, an `ON DELETE` / `ON UPDATE CASCADE` or `SET NULL` foreign key) is
corrected exactly — the second delta subtracts ΔA ⋈ ΔB — only when:

- the IMV is SUM / COUNT (AVG is stored as both) aggregates without DISTINCT, or a passthrough, over
  joins with no outer join, LATERAL, set operation, window, LIMIT, grouping sets or subquery
  (`corrects_exactly`, src/trigger/joint.rs), and each source has a scoped delta; and
- **every column of the source whose transition rows are stashed** (the source applied first that
  sees the other one pending) — read by the IMV or not — has a type in
  `EXACT_TEXT_TYPES` (src/trigger/joint.rs: bool, integers, float, numeric, text types, uuid,
  date/time types, interval, bytea, json, jsonb). The stash keeps whole rows as text.

Everything else is recomputed at the second source's delta (correct, cost = a full rebuild of the
IMV for every such statement):

- MIN / MAX, outer joins, keyless passthrough, subqueries, three or more written sources;
- a stashed source holding any column of an **enum, domain, money, reg\*, array-of-those or
  user-defined type**, even one the IMV does not read, on an otherwise exact shape: no stash is kept, because the
  text form of those types is not proven exact under every session setting;
- the cascade shapes above on any of these IMVs. The batch's earlier ruling accepted "an IMMEDIATE
  cascade-shaped IMV pays a reconcile per parent delete"; the exact redesign removed that cost for
  the whitelisted shapes, and this report keeps the residual.

## Reproduction (verified on 1.12.5, pg17)

```sql
CREATE TYPE mood AS ENUM ('a','b');
CREATE TABLE fa (k INT PRIMARY KEY, g mood NOT NULL, a INT NOT NULL);
CREATE TABLE fb (k INT PRIMARY KEY, b INT NOT NULL, tag mood NOT NULL DEFAULT 'a');
INSERT INTO fa SELECT g, 'a', g FROM generate_series(1,1000) g;
INSERT INTO fb SELECT g, g FROM generate_series(1,1000) g;
SELECT create_reflex_ivm('f_sum',
  'SELECT fa.k % 10 AS m, SUM(fb.b) AS s, COUNT(*) AS n FROM fa JOIN fb ON fb.k = fa.k GROUP BY fa.k % 10',
  NULL, NULL, 'IMMEDIATE');
WITH x AS (UPDATE fa SET a = a + 1 WHERE k = 1 RETURNING 1) UPDATE fb SET b = b + 1 WHERE k = 1;
-- correct; pg_stat_user_tables.seq_tup_read grows by 1000 on fa and on fb per statement
-- (a recompute). The same tables with `g TEXT` and no `tag` column read 0 rows (corrected).
```

With the enum only on `fa` (the side that was not stashed) the statement was corrected: which
source's types matter depends on the trigger firing order.

## Severity

Performance only. The cost is a full IMV recompute per two-source statement on these shapes; zero
for single-source writes. Not on a known production path (db_dev's two-source shapes are SUM/COUNT
over integer and text keys).

## Fix direction

Per shape, only when a field IMV needs it: extend the whitelist with a proof that the text form
round-trips under every DateStyle / IntervalStyle / extra_float_digits / lc_monetary setting the
stash tests cover (enum and domain-over-whitelisted are likely easy; money depends on lc_monetary);
MIN / MAX would need the joint term's retraction semantics, which the existing MIN/MAX recompute
path does not offer.
