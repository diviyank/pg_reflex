# A GROUP BY key of type varchar(n), interval, time or any type outside the built-in list is stored as text — S3 (type difference)

Pre-existing. Measured on pg17 while fixing the result-type reports of the 1.12.0 fold (group G3),
which resolved the types of aggregate and aggregate-derived result columns only.

## Reproduction (pg17)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, vc VARCHAR(10), iv INTERVAL);
SELECT create_reflex_ivm('x', 'SELECT vc, COUNT(*) AS r FROM s GROUP BY vc');
-- x.vc is text; the query gives character varying(10). Data equal.
SELECT create_reflex_ivm('y', 'SELECT iv, COUNT(*) AS r FROM s GROUP BY iv');
-- y.iv is text; the query gives interval. `SELECT * FROM y EXCEPT ALL <query>` raises
-- "EXCEPT types text and interval cannot be matched".
```

## Expected vs actual

Expected: the key column of the IMV has the query's type. Actual: `text`, in the target and in the
intermediate table, for every type `map_information_schema_type` does not list (it lists integer,
bigint, smallint, numeric, real, double precision, boolean, date, timestamp, timestamptz, text, uuid,
json, jsonb; character types map to TEXT; everything else falls to TEXT).

## What is established and what is not

Established: the two type differences above. Not established: whether a key stored as text groups
differently from the query. Two values that are equal in the type but spell differently as text, or
the reverse, would split or merge groups (`interval '24 hours'` and `interval '1 day'` are distinct
intervals but compare equal; a `citext` key; a `numeric` key is not affected, it is mapped). That
would be a silent wrong result and is worth one oracle test before anything else.

## Fix direction (hypothesis to falsify)

Type the key columns from the described base query / user query with PostgreSQL's own spelling
(`describe_query_columns`, `result_column_type` in `src/create_ivm/admin.rs` already do this for
aggregate columns) instead of `map_information_schema_type`. The key type feeds the MERGE join, the
affected-groups tables and the target index, so the change needs the full suite and the partition
tests.
