# A function called on the whole row of a source (`f(t)`) is created, then every write to the source raises — S2 (LOUD, blocks writes)

Pre-existing. Found on pg15.19 while fixing the attribute-notation call (`t.f`) on the 1.12.0
branch (fix/fold-g5-classify), which rejects `t.f` / `(t).f` at create. The function-call
spelling of the same thing is not rejected.

## Reproduction (pg15.19)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, v INT);
INSERT INTO s(v) VALUES (1);
CREATE FUNCTION f(s) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1.v + 1';
SELECT create_reflex_ivm('x', 'SELECT id, f(t) AS x FROM s t');   -- created, IMV = query
INSERT INTO s(v) VALUES (5);
-- ERROR:  function f(__reflex_pt_new_x_s) does not exist
```

## Expected vs actual

Expected: the create is refused, or writes succeed. Actual: created, and the first write fails.

## What is known about the mechanism

By the error text: the keyed passthrough delta reads the query over the transition table
(`__reflex_pt_new_x_s`), whose row type is not the source's, so `f(<row>)` does not resolve.

## Fix direction (hypothesis to falsify)

Reject at create a function whose argument is a whole-row reference to a source (an identifier
naming a FROM alias or table used as a value), with the remedy the attribute-notation rejection
prints (a column of the source, generated when the function is immutable); or cast the transition
row to the source's row type in the delta query.
