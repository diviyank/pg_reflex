# A window query with a qualified select item, or with a named WINDOW clause, fails at create with a raw error — S3 (loud, at create)

Pre-existing. Found on pg15.19 while fixing "window PARTITION BY column not in SELECT" on the
1.12.0 branch (fix/fold-g5-classify). That fix rejects a window expression reading something the
base sub-IMV does not project; these two shapes fail for another reason and are left raw.

## Reproduction (pg15.19)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT);
SELECT create_reflex_ivm('a',
  'SELECT t.id, t.g, t.v, SUM(v) OVER (PARTITION BY g) AS w FROM s t');
-- ERROR:  missing FROM-clause entry for table "t"
SELECT create_reflex_ivm('b',
  'SELECT id, g, v, SUM(v) OVER w AS x FROM s WINDOW w AS (PARTITION BY g)');
-- ERROR:  window "w" does not exist
```

`SELECT id, g, v, SUM(v) OVER (PARTITION BY g) AS w FROM s` is created and correct.

## Expected vs actual

Expected: an IMV or a tagged rejection. Actual: raw PostgreSQL errors from the generated view.

## What is known about the mechanism

`decompose_window_query` (`src/window.rs`) builds the window view's select list from the text of
each select item: an unaliased `t.g` is written as `t.g` over the base sub-IMV, which has no
`t`; and the WINDOW clause is not carried into the view.

## Fix direction

Write an unaliased qualified item by its bare column name in the view, and either carry the
WINDOW clause or reject a named window with a tagged message (remedy: write the window inline).
