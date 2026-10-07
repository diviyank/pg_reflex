# Another schema's quoted table with the same bare name breaks DML on the source — S3 (loud)

Pre-existing (identical on the base). Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400; Task 1b probe branch based on c71c65d), on pg15.
Found by the Task 1b adversarial reviewer (P4). The unquoted form of the same query is correct.

## Reproduction (pg15)

The reviewer recorded the shape `FROM f JOIN "sa".dim a ON … JOIN sb."dim" b ON …` with fixture
"fact + `sa.dim` + `sb.dim` (same bare name), INNER and LEFT, DML on all three". The join
conditions and select list are not recorded; the SQL below is a sketch of that shape with
illustrative `ON` conditions and select list.

```sql
CREATE SCHEMA sa; CREATE SCHEMA sb;
CREATE TABLE sa.dim(kind TEXT PRIMARY KEY, label TEXT);
CREATE TABLE sb.dim(kind TEXT PRIMARY KEY, label TEXT);
CREATE TABLE f(id SERIAL PRIMARY KEY, kind TEXT, v INT);
SELECT create_reflex_ivm('v', 'SELECT a.label, COUNT(*) AS c FROM f
  JOIN "sa".dim a ON a.kind = f.kind JOIN sb."dim" b ON b.kind = f.kind GROUP BY a.label');
INSERT INTO sa.dim VALUES ('k','K');
-- ERROR:  zero-length delimited identifier at or near """"
```

Recorded: DML on `sa.dim` fails with `zero-length delimited identifier at or near """"`. The same
fixture with unquoted `sa.dim` and `sb.dim` is correct (green on base and head).

## Expected vs actual

Expected: a write to `sa.dim` rewrites only the `sa.dim` reference. Actual: the rewrite produces
invalid SQL.

## What is known about the mechanism

By the reviewer's reading (`src/sql_writer/identifier.rs`, about lines 313-318): the bare-name
pass for `sa.dim` matches the bare word `dim` inside `sb."dim"` and replaces it, leaving a stray
pair of quotes. Related minor note from the same review: the quoted-schema spelling pass can also
match inside a schema name that itself contains a double quote (`"a""s".t` matches `"s".t`); not
worth a guard on its own.

## Fix direction (hypothesis)

Make the bare-name pass refuse a match whose neighbour is a quote or a dot on either side, so a
reference qualified with a different schema is never taken for the bare name; add oracle tests
for the quoted and unquoted forms with two schemas sharing a table name, DML on each.
