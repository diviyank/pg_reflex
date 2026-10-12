# `FROM ONLY t` raises `relation "only" does not exist` — S3 (LOUD raw error at create)

Pre-existing (same on `d0a3e06`). Reported by the adversarial review of the 1.12.0 create
refusals (pg18); not re-run here.

## Reproduction

```sql
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SELECT create_reflex_ivm('x', 'SELECT id, v FROM ONLY t');
-- ERROR: relation "only" does not exist
```

## Expected vs actual

Expected: created, or refused with a tagged message. Actual: a raw PostgreSQL error; nothing is
created. `ONLY` matters for a table with inheritance children, which `create_reflex_ivm` now
refuses as a source, so the remedy of that refusal cannot be `FROM ONLY parent`.

## What is established and what is not

Established by the review: the error above, identical on base and head. Not established: where
`only` is taken for a relation name (the parser, or the source collection of the analysis);
`t *` and `TABLE t`, which the review saw refused or raising as well.

## Fix direction (hypothesis to falsify)

Refuse `ONLY` with a tagged message naming it, before the source names are resolved.
