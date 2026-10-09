# A table-qualified group column in HAVING fails at create with `missing FROM-clause entry` — S3 (loud, at create)

Pre-existing: same raw error on commit 8ef9743 and on the `fix/fold-g2-having` head, pg17.

## Reproduction

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, v INT);
INSERT INTO t(g, v) VALUES ('a', 1), ('b', 5);
SELECT create_reflex_ivm('h',
  'SELECT t.g, COUNT(*) AS c FROM t GROUP BY t.g HAVING t.g <> ''a'' AND SUM(t.v) > 2');
-- ERROR:  missing FROM-clause entry for table "t"
```

## Expected vs actual

Expected: an IMV, or a tagged rejection. Actual: raw PostgreSQL error at create; nothing is left
behind.

## What is established

The HAVING predicate is evaluated over the intermediate table, where a group column is stored
under its bare normalized name. The rewrite replaces aggregate calls only; a column reference is
printed as written, so `t.g` keeps a qualifier the end query has no relation for. The unqualified
form (`HAVING g <> 'a' AND SUM(v) > 4`) works and is pinned (`v112_hv_group_column`), as does a
qualified aggregate argument (`SUM(t.v)`). A group column selected under an alias
(`SELECT g AS grp ... HAVING g <> 'a'`) also works.

## Fix direction (hypothesis to falsify)

Map each HAVING column reference that names a GROUP BY expression to its intermediate column, the
way the SELECT list resolves group columns, or reject a qualified column in HAVING with a tagged
message whose remedy is the unqualified name.
