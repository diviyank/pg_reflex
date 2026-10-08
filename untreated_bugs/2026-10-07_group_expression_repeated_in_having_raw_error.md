# A GROUP BY expression repeated in HAVING fails at create with `column "g" does not exist` — S3 (loud, at create)

Pre-existing: same raw error on commit 8ef9743 and on the `fix/fold-g2-having` head, pg16. Found by
the adversarial review of the HAVING fold (its P3); not re-run when this report was written.

## Reproduction

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT);
INSERT INTO s(g, v) VALUES ('a', 1), ('b', 5);
SELECT create_reflex_ivm('h',
  'SELECT upper(g) AS ug, SUM(v) AS s FROM s GROUP BY upper(g) HAVING upper(g) <> ''A'' AND SUM(v) > 2');
-- ERROR:  column "g" does not exist
```

## Expected vs actual

Expected: an IMV, or a tagged rejection. Actual: raw PostgreSQL error at create; nothing is left
behind.

## What is established

The HAVING predicate is evaluated over the intermediate table, where the group expression is one
stored column. The rewrite replaces aggregate calls only and prints everything else as written, so
`upper(g)` is re-evaluated over a column the intermediate does not have. Sibling of
`2026-10-07_qualified_group_column_in_having_raw_error.md` (a qualified group column): one mapping
from HAVING sub-expressions to group columns closes both.

## Fix direction (hypothesis to falsify)

Replace each HAVING sub-expression that equals a GROUP BY expression by its intermediate column
before printing, or reject with a tagged message whose remedy is a CTE that names the expression.
