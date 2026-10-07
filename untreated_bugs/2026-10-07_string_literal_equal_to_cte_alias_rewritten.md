# A string literal equal to a CTE alias is rewritten: the IMV is wrong right after create — S1 (silent wrong result)

Pre-existing (identical on the base). Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400; Task 1b probe branch based on c71c65d), on pg15.
Reported by the Task 1b implementer (concern 4) and confirmed by the Task 1b reviewer (P3). The
1.12.0 branch made the delta rewrite of a source literal-aware; the two CTE call sites below were
deliberately left out and still rewrite literals.

## Reproduction (pg15)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, season TEXT, v INT);
INSERT INTO t(season, v) VALUES ('e',1),('y',2),('e',3);
SELECT create_reflex_ivm('v', $$WITH e AS (SELECT season, SUM(v) AS s FROM t GROUP BY season)
  SELECT season, s FROM e WHERE season <> 'e'$$);
```

Recorded result, right after create (before any DML): IMV `(y,2),(e,4)` against a fresh run
`(y,2)`; 1 oracle mismatch in the implementer's run. The control with `<> 'x'` is correct. The
stored query reads `... WHERE season <> '"<view>__cte_e"'`: the literal `'e'` became the quoted
name of the CTE's sub-IMV.

## Expected vs actual

Expected: the literal is left alone and the IMV equals the query. Actual: every occurrence of the
CTE alias as a whole word, including inside a string literal, is replaced by the sub-IMV's quoted
name, so the filter compares against the wrong text and rows leak through.

## What is known about the mechanism

- Two call sites of the plain `replace_identifier` in `src/create_ivm/decompose.rs` (lines 1013
  and 1070 on the reviewed tree) replace a CTE alias by the quoted sub-IMV name in a later CTE's
  text and in the main query body. They do not skip string literals.
- `replace_identifier_outside_literals` (added by the 1.12.0 source-rewrite work) already skips
  literals; the 1.12.0 brief reserved the CTE call sites for their own report. The implementer
  expects that function to fix this, which was not tried here.
- Not investigated: dollar-quoted literals and non-ASCII literals in the CTE case (the 1.12.0
  literal scanner handles them, so they would be covered by the same switch).

## Fix direction (hypothesis)

Switch both `decompose.rs` call sites to `replace_identifier_outside_literals`. Add oracle tests:
a literal equal to the alias in a later CTE body and in the main query, a literal embedding the
alias (`'a-e-b'`), and a dollar-quoted literal, each checked right after create and after DML.
