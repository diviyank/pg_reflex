# An expression that pg_reflex prints back from the parsed query can change meaning: `- -v`, `0x1F` — S1 (SILENT wrong result in a group key), LOUD elsewhere

Pre-existing (same on 1.11.5). Met by the 1.12.0 full re-review, seat R2 (finding P3). The
1.12.0 fix that makes PostgreSQL run the caller's text for the stored query (`sql_query`, and
the passthrough select list) does not reach this: the group keys, the aggregate arguments and
the WHERE clause of the maintenance SQL are still printed from the sqlparser AST, clause by
clause.

## Reproduction (pg18, IMMEDIATE, table `t(id INT PRIMARY KEY, u INT, v INT, b BOOLEAN, s TEXT)` with rows)

| Query | Outcome |
|---|---|
| `SELECT - -v AS x, COUNT(*) AS c FROM t GROUP BY - -v` | created; the IMV is empty, the query is not (`base_query` is `SELECT --v AS "--v", …`: PostgreSQL reads `--v …` as a comment). **Silent.** |
| `SELECT id, v FROM t WHERE - -v > 1` | created and correct; every write to `t` raises `syntax error at end of input` |
| `SELECT id, v FROM t WHERE 0x1F > v` | created and correct; every write raises `operator does not exist: bit > integer` |
| `SELECT b, SUM(- -v) AS s, COUNT(*) AS c FROM t GROUP BY b` | raw error at create: `syntax error at end of input` |
| `SELECT b, SUM(0x1F + v) AS s, COUNT(*) AS c FROM t GROUP BY b` | raw error at create: `operator does not exist: bit + integer` |
| `SELECT 0x1F + v AS x, COUNT(*) AS c FROM t GROUP BY 0x1F + v` | raw error at create, same message |

The same expressions as a select item of a passthrough (`SELECT id, - -v AS x FROM t`,
`SELECT id, 0x1F + v AS x FROM t`) are maintained correctly on 1.12.0, whatever the case of
the identifiers around them (`v112_sr_text_select_items`).

## Expected vs actual

Expected: the IMV equals its query, or the create is refused with a tagged message. Actual:
one silent empty IMV, two IMVs that block every write to their source, three raw errors.

## What is established and what is not

Established: the six outcomes above (a temporary probe on the 1.12.0 fix branch, pg18). The
cause is `Display` of sqlparser 0.60: a unary minus is printed with no space before its
operand, so two of them print as `--`; a `0x…` integer is lexed as a hex *string* and printed
`X'1F'`, a bit string. Not established: the full list of constructs the printer changes. The
re-review printed 434 expression, predicate and query templates back with the same meaning
(`E''`, `U&''`, dollar quotes, typed literals, casts with typmods, intervals, `AT TIME ZONE`,
`EXTRACT`, `SUBSTRING … FROM`, arrays, JSON operators, row comparisons, windows, set operations,
CTEs, comments); these two are the ones it found. `v ISNULL` and `0o17` / `0b101` literals are
refused by the parser (`[reflex-unsupported] Failed to parse SQL`), which is loud.

## Fix direction (hypothesis to falsify)

Either cut each clause out of the caller's text at the span of its AST node instead of printing
the node (the spans are there: `Expr::span()`), which is the same rule the stored query now
follows; or, as a stop-gap, describe the maintenance SQL at create and refuse with a tagged
message when PostgreSQL does not accept it, and print a space (or parentheses) between two
consecutive unary signs. The stop-gap closes the write-blocking and raw-error faces but only the
`- -v` instance of the silent one.
