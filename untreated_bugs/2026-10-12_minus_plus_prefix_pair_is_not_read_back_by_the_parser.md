# `- +v` is printed `-+v`, which the SQL parser pg_reflex uses does not read back — S3 (LOUD at create: one raw error, two tagged parse errors)

Pre-existing. Met on pg17 while adding the create-time refusal of the expressions pg_reflex
writes back with another meaning (`src/create_ivm/reprinted_expressions.rs`). That refusal
leaves `- +v` alone on purpose: PostgreSQL reads the printed `-+v` as `-` then `+`, so the
maintenance SQL is right, and the definitions below that work today must keep working.

## Reproduction (pg17, IMMEDIATE, table `t(id INT PRIMARY KEY, u INT, v INT, b BOOLEAN, s TEXT)` with rows)

| Query | Outcome |
|---|---|
| `SELECT b, COUNT(*) AS c FROM t GROUP BY b HAVING SUM(- +v) > 1` | raw error at create: `column "v" does not exist` |
| `WITH c AS (SELECT id, - +v AS x FROM t) SELECT id, x FROM c` | `[reflex-unsupported] Failed to parse SQL: sql parser error: Expected: an expression, found: -+` |
| `SELECT id, - +v AS x FROM t UNION ALL SELECT id, u FROM t` | same tagged message |
| `- +v` as a group key, an aggregate argument, in WHERE, FILTER, CASE, JOIN ON, `SELECT DISTINCT`, a passthrough select item, a derived table | created and correct after INSERT / UPDATE / DELETE |

`+ -v` (printed `+-v`) is created and correct in every one of these positions.

## Expected vs actual

Expected: the three definitions are created, as their `-v` and `+ -v` forms are, or all three
are refused with a tagged message that gives the rewrite. Actual: one raw PostgreSQL error and
two tagged messages that name no remedy. Nothing is created in any of the three, and no wrong
result was seen.

## What is established and what is not

Established: the outcomes above (cells of a temporary probe over 18 clause positions, pg17).
The parser reads `-+` as one operator token where an expression starts, so text that holds the
printed form of `- +v` and is parsed again (the query of a CTE sub-IMV, the operand of a set
operation, the HAVING rewrite) fails. Not established: DISTINCT ON and window queries, which
also print and re-parse; other sign pairs behind a third sign (`- + -v`); pg15, pg16, pg18.

## Fix direction (hypothesis to falsify)

Remedy for the user today: write `-(+v)` or `-v`. In the package: print a space (or
parentheses) between two consecutive prefix operators wherever a clause is printed and parsed
again, or refuse `- +v` in the three shapes above with a tagged message carrying that rewrite.
Refusing it in every rewritten clause would turn away definitions that are correct today.
