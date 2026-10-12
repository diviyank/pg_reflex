# `SUM(v ORDER BY - -v)`, `array_agg(v ORDER BY 0x1F)`: a form the parser prints back differently is refused inside an aggregate's ORDER BY, where it was maintained correctly before — limitation (new in 1.12.0, the one deliberate correct-to-refused change of that guard)

New in 1.12.0 (`935362b`, `8f61245`, the guard against expressions the parser reprints with
another meaning). Source: `p3-a-report.md` rounds 2 and 3 in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`; pg17, base `d0a3e06`.

## What the source gives

No script and no fixture are printed. The statements:

- the guard refuses, with a tagged message and a remedy, the forms that sqlparser prints back
  with another meaning (two consecutive prefix operators such as `- -v`, `@ -v`; a hexadecimal /
  octal / binary integer) wherever pg_reflex writes the expression back from the parse;
- from the list "Correct before, refused now" (against `d0a3e06`): "Inside an aggregate's
  `ORDER BY` (`SUM(v ORDER BY - -v)`, `array_agg(v ORDER BY 0x1F)`), every refused form: two
  prefix operators in a row that print glued (`- -v`, `- - -v`, `@ -v`, `~ -v`, `- @v`, `- ~v`,
  `@ ~v` and the like — not `- +v`/`+ -v`), and a lower-case hexadecimal integer `0x…`. A bare
  aggregate never had this clause printed, so those definitions worked." For `SUM` the analyzer
  drops an ORDER BY inside an order-insensitive aggregate, so the cell was `ok` before. Recorded
  in `progress.md` as an outcome change to document.

Reason given: for a select item that holds an aggregate inside an expression
(`SUM(v ORDER BY - -v) + 1`), `plan_aggregation` takes the whole printed item, parses it again
(`rewrite_aggregate_derived_expr`, `src/aggregation.rs`) and, when that parse fails, puts the
printed text into the end query unchanged; HAVING goes through the same reader. Exempting
`ORDER BY` inside a bare aggregate would need the guard to reproduce the planner's choice of
which aggregates are bare, so the author kept the default refusal.

## Expected vs actual

Expected: a definition that was maintained correctly stays accepted. Actual: refused at create
with the remedy "write one sign, or parenthesise the operand" (decimal for the integer forms).
The remedy is performed in tests. An IMV created before 1.12.0 with such an item cannot
be recreated as written.

## What is established and what is not

Established by the author's tests: the refusal and its remedy. Not established: which
aggregates accept an ORDER BY at all on the head (the report names `SUM` and `array_agg`
without showing the `array_agg` cell); the outcome on
1.11.5 (the base of the comparison is `d0a3e06`, not `6946400`).

## Fix direction (hypothesis to falsify)

Have the planner tell the guard which aggregates are bare (kept as the caller's text) and skip
the check for those; or make `rewrite_aggregate_derived_expr` fail closed, after which the guard
is not needed for this position. Related:
`2026-10-12_minus_plus_prefix_pair_is_not_read_back_by_the_parser.md`,
`2026-10-12_non_decimal_integer_before_more_tokens_is_a_parse_error_with_no_remedy.md`.
