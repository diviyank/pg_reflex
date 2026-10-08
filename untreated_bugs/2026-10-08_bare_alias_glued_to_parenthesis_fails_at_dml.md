# `"s".t AS t)x`: a source aliased with its own bare name, the alias glued to a parenthesis, raises on the first write — S3 (loud)

Pre-existing (same on `903efbc` and on the G1 head `8ef9743`). Found by the G1 (joins)
adversarial review, probe cell `b4tight`, pg17. It is the residual of
`2026-10-07_quoted_schema_with_redundant_bare_alias_fails_at_dml.md`: that report's shape is
correct today only because the alias is a whitespace-delimited token.

## Reproduction (pg17)

```sql
CREATE SCHEMA s;
CREATE TABLE s.t(id SERIAL PRIMARY KEY, season TEXT, v INT);
INSERT INTO s.t(season, v) VALUES ('e',1),('y',2),('e',3);
SELECT create_reflex_ivm('v', 'SELECT x.id, x.season, x.v
  FROM (SELECT t.id, t.season, t.v FROM "s".t AS t)x WHERE x.v > 0');
INSERT INTO s.t(season, v) VALUES ('e',10);
-- ERROR:  invalid reference to FROM-clause entry for table "__reflex_pt_new_v_s_t"
```

With a space before `x` (`… AS t) x`) the same query is correct.

## Expected vs actual

Expected: the write maintains the IMV. Actual: the delta query reads the scratch copy of the
transition table under the alias `t` and still refers to it by the scratch table's own name.

## What is known about the mechanism

- `source_reference_count` (`src/trigger/mod.rs`) counts whitespace-delimited tokens that name the
  source. `t)x` is one token and does not name `s.t`, so the count is 1, the write is not treated
  as a self-join and the transition-table rewrite runs.
- `strip_redundant_bare_alias` (`src/sql_writer/identifier.rs`) only matches the spelling the
  source is registered under (`s.t`), not `"s".t`, so the alias stays and the bare-name pass
  rewrites `t.id` to the transition table's name, which the alias hides.
- Since 1.12.0 the parsed query is available at this point (`outer_joins`,
  `src/trigger/outer_join.rs`, counts table references from the AST); it does not see aliases.

## Fix direction (hypothesis)

Have `strip_redundant_bare_alias` iterate `rewritten_source_spellings`, which removes the need
for the token count to catch this shape at all. The legacy-differential unit test
`source_rewrite_matches_legacy_for_unquoted_fully_quoted_and_bare_references`
(`src/sql_writer/tests.rs`) pins the current output for `"alp"."product" AS product` by reading,
so it has to change on purpose. As a fallback, read the alias from the AST: a table factor whose
alias equals the bare name of the source it names is recomputed in full.
