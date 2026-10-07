# A varchar(n) result column is stored as text in the IMV — S3 (type difference, data correct)

Pre-existing: identical on the 1.11.5 base and on the 1.12.0 branch. Recorded by the Task 4b
author and confirmed by the Task 4b re-reviewer.

## Reproduction

Shapes recorded, no full script: `(COUNT(*) + 1)::varchar(10)` gives an IMV column of type `text`
where the query gives `character varying(10)`; data correct. `(SUM(v) / 2)::varchar` shows the
same type difference. A top-level `MAX(t)::varchar` keeps `character varying` on both.

## Expected vs actual

Expected: the IMV column has the query's type (`character varying(10)`). Actual: `text`.

## What is established and what is not

Established: the observation on base and head. Not established: where the type is derived; the
re-reviewer only notes that a nested derived (non-top-level-aggregate) expression is the case and
a top-level aggregate cast is not affected.

## Fix direction (hypothesis to falsify)

Take the column types from the query's own result types (catalog) rather than a text-based
derivation, then compare `pg_typeof` of the IMV and the query in the oracle. Limitation is
probably acceptable if no consumer depends on the length.
