# Passthrough key auto-detection matches a SELECT alias to a primary-key column by name only — S2 (LOUD on write; SILENT in a keyed aggregate wrapper)

Pre-existing. Found by the re-review of the 1.12.0 final fix wave (pg17, 1.12.0 branch); the
mechanism was read in `resolve_unique_columns` (`src/create_ivm/mod.rs`).

## Reproduction

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT);
INSERT INTO s(g, v) VALUES ('a', 1), ('a', 1);
SELECT create_reflex_ivm('x', 'SELECT v AS id, g FROM s WHERE v IS NOT NULL');
-- registered with unique_columns = {id}
INSERT INTO s(g, v) VALUES ('b', 1);
-- ERROR: duplicate key value violates unique constraint "__reflex_uk_…"
```

`SELECT v + 1 AS id FROM s` fails at create with `could not create unique index` when the data
has duplicates.

Related, silent: a recognised aggregate the planner does not decompose, created with a declared
key, is wrong and then raises (`SELECT COUNT(DISTINCT v) AS m FROM s` keyed on `m`); see
`2026-10-07_ungrouped_aggregate_wrapper_with_unique_key_duplicate_key_on_write.md`.

## Expected vs actual

Expected: an output column counts as the source's primary key only when its expression is that
column. Actual: any output column whose alias equals a primary-key column name is taken as the
key, whatever its expression.

## Fix direction (hypothesis to falsify)

In the auto-detection, require the SELECT item to be a bare (optionally qualified) reference to
the primary-key column, resolved from the AST. Existing IMVs keep their registered key; decide
whether a rebuild should re-detect.
