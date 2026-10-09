# DEFERRED IMV over a derived table that holds a subquery (or a UNION ALL): COMMIT raises `relation "__reflex_delta_<subquery:x>" does not exist` — S2 (LOUD, blocks writes)

Pre-existing: same outcome on `8ef9743` and on the 1.12.0 fold branches. Found by the adversarial
review of the G4 fold (cells `bd_derived`, `bd_uaagg`, real COMMIT, pg18).

## Reproduction (pg18)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT, w INT);
INSERT INTO s(g,v,w) VALUES ('a',1,2),('b',5,NULL);
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, label TEXT, v INT);
INSERT INTO t(g,label,v) VALUES ('a','A',1),('b','B',2);
SELECT create_reflex_ivm('v', 'SELECT x.g, SUM(x.v) AS sv, COUNT(*) AS c
  FROM (SELECT g, v FROM s WHERE g IN (SELECT g FROM t)) x GROUP BY x.g',
  NULL, 'UNLOGGED', 'DEFERRED');
INSERT INTO s(g,v,w) VALUES ('a',10,1);
-- ERROR at COMMIT: relation "__reflex_delta_<subquery:x>" does not exist
```

The `UNION ALL` derived form raises the same with `<subquery:q>`.

## Expected vs actual

Expected: the write commits and the IMV follows. Actual: the COMMIT of any write to `s` is
aborted.

## What is established and what is not

Established: the error at a real COMMIT. `dim LEFT JOIN (SELECT … FROM fact WHERE v > 0) x`
commits fine. Calling `reflex_flush_deferred` by hand inside one transaction does not raise,
which is why no `#[pg_test]` saw it.

Not established: which statement of the flush builds the name.

## Fix direction (hypothesis to falsify)

The placeholder source of the derived table (`<subquery:x>`) is stored in `depends_on`, and the
COMMIT-time flush derives a staging-table name from every entry. Skip placeholder sources there
(`create_ivm` already skips them when it installs triggers), and cover it with a test that
commits through a second connection.
