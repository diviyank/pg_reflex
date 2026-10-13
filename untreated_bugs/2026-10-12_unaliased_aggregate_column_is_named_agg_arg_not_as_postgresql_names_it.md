# An unaliased aggregate is stored under `<agg>_<arg>` (`max_seen_at`, `sum_charged`) where PostgreSQL names the column `max`, `sum` — S3 / limitation (column name differs from the query, contents correct; pre-existing)

Found by the non-regression review of 1.12.0 (`full-review-r1.md` §5, pg17; identical on
`6946400` and `0324f47`) in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`. Until now only mentioned inside
`2026-10-10_select_distinct_expression_alias_is_lost.md`.

## What the source gives

> **No column name changed** between the trees. Names that differ from PostgreSQL's on both
> trees (pre-existing): an unaliased aggregate is named `<agg>_<arg>` (`max_seen_at`,
> `sum_charged` where PostgreSQL says `max`, `sum`; d21, d23, d38), and
> `SELECT DISTINCT <expr> AS alias` loses its alias.

The definitions d21, d23 and d38 are not printed in the review. From the names, they are
aggregates over `events.seen_at` and `invoices.charged` of the probe fixture
(`CREATE TABLE events (id BIGINT, grp TEXT, seen_at TIMESTAMPTZ, region TEXT, revenue NUMERIC,
kind TEXT, user_id INT, payload JSONB)`,
`CREATE TABLE invoices (id INT PRIMARY KEY, customer_id INT, charged NUMERIC(12,2), paid BOOLEAN, issued DATE)`).
The shape, not a copied script:

```sql
SELECT create_reflex_ivm('x', 'SELECT grp, MAX(seen_at) FROM events GROUP BY grp');
-- IMV columns: grp, max_seen_at        the query's columns: grp, max
```

## Expected vs actual

Expected: the IMV has the column names of its query, so that it can replace a materialized view
of the same definition. Actual: a reader selecting `max` fails; `SELECT *` consumers see another
name. The contents are correct at every step.

## What is established and what is not

Established: the naming on both trees for three definitions. Not established: the rule for
expressions (`SUM(a + b)`), for `COUNT(*)`, for two unaliased aggregates of the same function
(which PostgreSQL names identically and cannot store either: 1.12.0 refuses two output columns
of one name with a tagged message). Whether any deployed reader depends on the `<agg>_<arg>`
names is unknown, and that is the cost of changing them.

## Fix direction (hypothesis to falsify)

Not obviously worth changing: the pg_reflex name is deterministic, and PostgreSQL's would collide
as soon as a query has two aggregates of one function. Minimum: document the rule next to the
aggregates reference and say "alias every aggregate". If changed, it must be opt-in or it breaks
readers of existing IMVs on recreate.
