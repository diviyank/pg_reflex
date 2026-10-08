# An unqualified column whose name is the bare name of a schema-qualified source raises on a write to that source — S3 (loud)

Pre-existing (same on `903efbc` and on the G1 head `8ef9743`). Found by the G1 (joins)
adversarial review, probe cell `snbarecol`, pg17.

## Reproduction (pg17)

```sql
CREATE SCHEMA a;
CREATE TABLE a.dim(id SERIAL PRIMARY KEY, season TEXT, v INT);
CREATE TABLE fact(id SERIAL PRIMARY KEY, season TEXT, v INT, dim INT);
INSERT INTO a.dim(season, v) VALUES ('e',1),('y',2);
INSERT INTO fact(season, v, dim) VALUES ('e',1,1),('y',2,NULL);
SELECT create_reflex_ivm('v', 'SELECT f.season, COUNT(dim) AS c, SUM(a.v) AS s
  FROM fact f JOIN a.dim a ON a.id = f.id GROUP BY f.season');
INSERT INTO a.dim(season, v) VALUES ('n',3);
-- ERROR:  column "__reflex_new_a_dim" does not exist
```

Writes to `fact` are correct. The same query with the column qualified (`COUNT(f.dim)`) is
correct, and with the column quoted (`COUNT(f."dim")`) it is correct since 1.12.0 (recomputed in
full: `source_rewrite_breaks_quoted_identifier`). The passthrough form
(`SELECT f.id, dim IS NULL AS isnull, a.v FROM …`) is correct (`snbarecolpt`).

## Expected vs actual

Expected: the column `dim` of `fact` is not a reference to the table `a.dim`. Actual: the
transition-table rewrite replaces the bare word `dim` wherever it stands, the column included.

## What is known about the mechanism

`rewritten_source_spellings` (`src/sql_writer/identifier.rs`) ends with the bare name of a
schema-qualified source, so that a column qualifier `dim.col` is rewritten; the replacement has
no way to tell a qualifier from a column of the same name. A match followed by `.` is a
qualifier; one that is not is either the table in a FROM clause (already consumed by the
qualified spellings) or something else.

## Fix direction (hypothesis)

Restrict the bare-name pass to matches followed by a dot. Check it against the legacy-differential
unit tests in `src/sql_writer/tests.rs` first: they pin the current output on generated text and
may have to change on purpose. Until then, the full recompute is available as a fallback: have
`has_no_scoped_delta` detect a bare-name match that is not followed by a dot. Add oracle tests
for the aggregate and passthrough forms, the column in the SELECT list, WHERE and GROUP BY.
