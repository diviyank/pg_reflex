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
- an IMMEDIATE IMV over a source that **also has a DEFERRED IMV**, whatever its shape (see below);
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

## An IMMEDIATE IMV over a source that also has a DEFERRED IMV (found by the 1.12.5 final review)

A source read by a DEFERRED IMV gets the DEFERRED-flavour trigger bodies
(`sql/deferred_trigger_body.plpgsql.in:18`, `sql/deferred_trigger_update_body.plpgsql.in:23`).
They run the source's IMMEDIATE IMVs through `reflex_build_delta_sql`, which does not resolve a
correction. Whichever trigger fires first, the IMMEDIATE IMV is recomputed:

- the DEFERRED-flavour body of X fires first: it poisons the stash, and the other source's delta
  recomputes;
- the other source's normal body fires first: it stashes, and X's non-resolving delta recomputes.

The DEFERRED-flavour bodies' predicate skip and ignored-source skip also do not call the skip hooks
(`reflex_joint_skipped_delta_sql`); that leaves an excess, so a recompute, never a wrong result.

Reproduction (pg16, the 1.12.5 final-review probe `pg_finalrev_deferred_flavour_source_is_not_corrected`,
two dblink sessions):

```sql
CREATE TABLE fd_x (k INT PRIMARY KEY, v INT NOT NULL);
CREATE TABLE fd_y (k INT PRIMARY KEY, g INT NOT NULL);
INSERT INTO fd_x SELECT g, g FROM generate_series(1, 100) g;
INSERT INTO fd_y SELECT g, g % 5 FROM generate_series(1, 100) g;
SELECT create_reflex_ivm('fd_v',
  'SELECT fd_y.g, COUNT(*) AS n, SUM(fd_x.v) AS s FROM fd_x JOIN fd_y ON fd_y.k = fd_x.k GROUP BY fd_y.g',
  NULL, NULL, 'IMMEDIATE');
-- add this line to see the recompute:
SELECT create_reflex_ivm('fd_dx', 'SELECT k % 3 AS b, SUM(v) AS s FROM fd_x GROUP BY k % 3',
  NULL, NULL, 'DEFERRED');
BEGIN;
WITH a AS (INSERT INTO fd_x VALUES (500, 7) RETURNING 1) INSERT INTO fd_y VALUES (500, 2);
-- corrections counted in the transaction-local setting `joint::corrections_setting('fd_v')`:
-- 1 without fd_dx, 0 with it; fd_v matches its query in both cases.
COMMIT;
```

Fix direction: render the DEFERRED-flavour bodies with `reflex_build_delta_sql_resolving` and the
`--<<REFLEX_THEN>>--` loop for their IMMEDIATE IMVs, and call the skip hooks from their skips. This
changes rendered bodies, so the release migration's body rewrite and `pg_test_upgrade_1_12_5` must
follow.

## Severity

Performance only. The cost is a full IMV recompute per two-source statement on these shapes; zero
for single-source writes. Not on a known production path (db_dev's two-source shapes are SUM/COUNT
over integer and text keys). The DEFERRED-IMV case was not checked against db_dev: a source there
with both a DEFERRED IMV and an IMMEDIATE join IMV written together in one statement would pay it.

## Fix direction

Per shape, only when a field IMV needs it: extend the whitelist with a proof that the text form
round-trips under every DateStyle / IntervalStyle / extra_float_digits / lc_monetary setting the
stash tests cover (enum and domain-over-whitelisted are likely easy; money depends on lc_monetary);
MIN / MAX would need the joint term's retraction semantics, which the existing MIN/MAX recompute
path does not offer.
