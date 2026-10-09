# A source aliased with its own bare name (`s.t AS t`) is recomputed in full on every write — perf (correct, slow)

Narrowed by the 1.12.0 bug-fold work (group G1, joins). The original report (Task 1b, pg15, probe
branch based on c71c65d) was a loud failure at the first DML for `"s".t AS t`:
`invalid reference to FROM-clause entry for table "__reflex_new_s_t"`. That failure does not
reproduce on the 1.12.0 line on pg17 (the `v112_j_ba_*` tests were green before any G1 change,
the `v112_j_filter_entering_update_*` ones with the G1 bulk-path guard removed); what remains is
the cost below and a latent gap in one helper.

## What was run (pg17, all correct)

Schema `s`, table `s.t(id SERIAL PRIMARY KEY, season TEXT, v INT)`. For each FROM spelling
`"s".t AS t`, `"s"."t" AS t`, `s."t" AS t`, `s.t AS t`, `"s".t t`, `"s"."t" t`, `"s".t as t`, and
the controls `"s".t AS x`, `"s"."t" AS x`:

- aggregate `SELECT t.season, SUM(t.v) AS s FROM … WHERE t.v > 0 GROUP BY t.season` and
  passthrough `SELECT t.id, t.season, t.v FROM … WHERE t.v > 0`, IMMEDIATE (and DEFERRED for the
  first two spellings), through INSERT, UPDATE of `v`, UPDATE of `season`, DELETE;
- the same alias in a join (`"s".t AS t JOIN o …`), DML on both tables;
- an UPDATE that brings a row into the WHERE filter of a join where `s.t` is the joined table
  (`o JOIN "s".t AS t ON o.v = t.id WHERE t.v > 0 GROUP BY t.id`), the route that plans the
  rewritten query before it builds the delta.

Tests: `v112_j_ba_*`, `v112_j_bare_alias_in_a_join_stays_correct`,
`v112_j_filter_entering_update_*` in `src/tests/pg_test_v112_joins.rs`.

## Why it does not fail

The self-join guard (`source_reference_count`, `src/trigger/mod.rs`) counts every
whitespace-delimited token that names the source, and the alias token `t` equals the bare name
of `s.t`. The count is 2, the write is treated as a self-join and the IMV is recomputed from its
query, so the transition-table rewrite never runs for this shape. Since G1 the same guard also
keeps the bulk path of the UPDATE trigger (`reflex_build_path_c_explain_sql`) from rewriting such
a query.

## Residual

- Cost: every write to `s.t` recomputes the IMV in full. Workaround: drop the alias or use a
  different one (`"s".t AS x` is not counted as a second reference and is correct, tests
  `v112_j_ba_*_other_alias`).
- Latent: `strip_redundant_bare_alias` (`src/sql_writer/identifier.rs`) still only matches the
  spelling the source is registered under, so `replace_source_with_transition` returns
  `FROM "__reflex_new_s_t" AS t WHERE "__reflex_new_s_t".v > 0` for `"s".t AS t`. No caller
  reaches it today because of the guard above. By reading, the legacy-differential unit test
  `source_rewrite_matches_legacy_for_unquoted_fully_quoted_and_bare_references`
  (`src/sql_writer/tests.rs`) pins that output for `"alp"."product" AS product` (its `REFERENCES`
  and `ALIASES` tables hold that combination); not confirmed by changing the helper.

## Fix direction (hypothesis)

Have the self-join guard skip a token that is the alias of the reference just before it, and
have `strip_redundant_bare_alias` iterate `rewritten_source_spellings`; both together, since the
second is what makes the first safe. The pinned differential test has to be updated on purpose in
the same change.
