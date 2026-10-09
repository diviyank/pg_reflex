# A quoted identifier equal to a source's bare name, belonging to another relation, makes the IMV recompute in full on every write to that source — perf (correct, slow)

Narrowed by the 1.12.0 bug-fold work (group G1, joins). The original report (Task 1b review, P4)
was a loud failure: DML on `sa.dim` raised `zero-length delimited identifier at or near """"` for
`FROM f JOIN "sa".dim a ON … JOIN sb."dim" b ON …`. Reproduced on pg17 (INNER and LEFT, aggregate
and passthrough, IMMEDIATE and DEFERRED, and for a `public` table beside `other."same_name"`),
and closed by a guard: the write is recomputed from the query instead of going through the
transition-table rewrite. What remains is the cost and the rewrite itself.

## Residual

`replace_source_with_transition` (`src/sql_writer/identifier.rs`) still replaces the bare name
inside the quotes of `sb."dim"` and returns `sb.""__reflex_new_sa_dim""`. Nothing executes that
text any more: `has_no_scoped_delta` (`src/trigger/mod.rs`) asks
`source_rewrite_breaks_quoted_identifier` first and recomputes the IMV in full for the affected
source (`sa.dim` above; `sb.dim` and `f` keep their scoped deltas). The same applies to a quoted
column named like the source's table (`x."dim"`).

Workaround: write the other relation unquoted (`sb.dim`): the guard does not fire and the result
is correct (test `v112_j_sn_inner_unquoted`).

## Why the rewrite was not changed

The fix is small: in the source rewrite, refuse a match with a double quote directly before or
after it (it is part of another identifier). It was written, made the oracle tests green, and
reverted, because two existing legacy-differential unit tests pin the current output for exactly
this input class:

- `source_rewrite_matches_legacy_for_unquoted_fully_quoted_and_bare_references`
  (`src/sql_writer/tests.rs`): for `SELECT "alp"."product".v FROM product`, source `product`, it
  expects `SELECT "alp".""__reflex_new_alp_product"".v FROM …`.
- `literal_aware_rewrite_matches_legacy_when_query_has_no_literal`: random concatenations such as
  `p"product"`.

## Fix direction (hypothesis)

Apply the quote-neighbour rule in the source rewrite, update the two differential tests on
purpose (skip the inputs where the legacy output holds `""`), then remove
`source_rewrite_breaks_quoted_identifier` and its use. The oracle tests `v112_j_sn_*` and
`v112_j_public_table_beside_quoted_same_name_in_other_schema` must stay green throughout.
