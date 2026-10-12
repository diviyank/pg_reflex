# Valid PostgreSQL forms the parser (or the analyzer) refuses at create with a tagged error that names no remedy: `v ISNULL`, `BETWEEN SYMMETRIC`, `VARIADIC`, `IS JSON`, `xmlelement`, `t*`, `ROWS FROM`, `USING … AS j`, `TABLE t`, `GROUP BY 1` — S3 / limitation (loud at create, nothing left behind)

Met by several reviews of 1.12.0 and filed nowhere. Sources, in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`: `full-review-r2-fix-report.md`
("Met on the way", PostgreSQL 18), `fold-g1b-review.md` ((b) Parser fallback, pg17, head
`b9af686`, base `8ef9743`), `fold-g5-review.md` (pre-existing item 8, base `e01231f`). Grouped
because the contract gap is one: the 1.12.0 create contract is "a tagged rejection **with a
remedy**", and these are tagged (`Failed to parse SQL` or an analyzer message) without one. The
neighbours that already have a report are listed at the end and are not repeated here.

## What the sources give

No fixture is needed: each is a refusal at create whatever the rows. The texts of the messages
are not printed in the sources beyond what is quoted.

| Form | Source | Outcome reported |
|---|---|---|
| `v ISNULL` in WHERE, or followed by `AS x` (`NOTNULL` listed beside it) | R2 fix report | "the parser refuses them, tagged. Loud, valid PostgreSQL, not filed" |
| `VARIADIC`, `BETWEEN SYMMETRIC`, `IS JSON`, `xmlelement`, `t*`, `ROWS FROM`, `USING … AS j`, `TABLE t`, `TABLESAMPLE`, `LATERAL` | G1b review | "the 19 [of 83 syntax cells] the parser or the analyzer refuses are refused at create"; the list is the review's, ending with "…" |
| `GROUP BY 1, 2` and `GROUP BY <alias>` | G5 review | rejected, tagged, on both trees: `GROUP BY key '1' is not projected`. "the brief lists positional and alias group keys as legal forms, but they were not accepted before this branch either" |
| `GROUP BY 1` over an expression (`m39`), `COLLATE` in a group key (`m34`), `COLLATE` inside a cast argument, `BOOL_AND` | R2 review §4 | "not created … loud on both trees" (`6946400` and `0324f47`); whether tagged or raw is not said |

## Expected vs actual

Expected: either support, or a `[reflex-unsupported]` message that names the form and an
equivalent pg_reflex accepts (`v IS NULL`; `BETWEEN LEAST(a, b) AND GREATEST(a, b)`; the column
written out in `GROUP BY`; `SELECT * FROM t` for `TABLE t`). Actual: a parse failure or an
analyzer message with no instruction.

## What is established and what is not

Established: that these forms are refused at create, on the trees named. `LATERAL` and
`TABLESAMPLE` are documented as unsupported; they are in the table only because the review lists
them. **Contradicted, not resolved:** the positional group key. The
reproduction of `2026-10-01_mixed_case_names_rejected_by_create_reflex_ivm.md` (1.11.5, pg17)
creates `SELECT product_id, location_id, COUNT(*) AS n FROM rbq.rel GROUP BY 1, 2` and reports no
error for that statement, while the G5 review says `GROUP BY 1, 2` is rejected on both of its
trees. The two were not run side by side; the G5 corpus statement is not printed. Not
established: the exact message of each; which of them 1.11.5 refused the same way
(the G1b and G5 bases are fold bases, not `6946400`; only the R2 §4 row compares with it);
whether the release head still refuses each (none was re-run after the reviews). The note in
`2026-10-10_raw_postgres_errors_at_create_for_five_definitions.md` already says that
`BETWEEN SYMMETRIC` is a tagged parse failure.

## Already filed, not part of this report

`FROM ONLY t` (`2026-10-12_from_only_is_read_as_a_relation_named_only.md`), `0o17` / `0b101`
followed by more tokens
(`2026-10-12_non_decimal_integer_before_more_tokens_is_a_parse_error_with_no_remedy.md`), `- +v`
(`2026-10-12_minus_plus_prefix_pair_is_not_read_back_by_the_parser.md`).

## Fix direction (hypothesis to falsify)

One catch-all at the place the parse error is tagged: match the offending token against a short
table of known forms and append the equivalent. `ISNULL` / `NOTNULL` can be rewritten before the
parse only if the rewrite is token-aware (a column may be named `isnull`); a remedy in the
message is the safe floor. Positional and alias group keys: resolve them against the select list
before the "not projected" check. Test: one cell per form asserting the tag and that the printed
equivalent is created and correct.
