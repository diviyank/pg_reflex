# `0X1F`, `0o17`, `0b101` followed by more tokens: tagged parse error that names no remedy — S3 (LOUD)

Pre-existing. Residual of the removed report on expressions the parser prints back with another
meaning. The silent face is closed: such a literal ending an unaliased select item, which the
parser reads as `0 AS x1f`, is refused wherever pg_reflex writes the item back, with the decimal
value, and lower-case `0x1F` is refused in every rewritten clause.

## Reproduction (PostgreSQL 16 or later)

```sql
SELECT create_reflex_ivm('x', 'SELECT id, v FROM t WHERE v > 0b101 AND id > 0');
SELECT create_reflex_ivm('y', 'SELECT b, SUM(v) + 0o17 AS s FROM t GROUP BY b');
-- ERROR: [reflex-unsupported] Failed to parse SQL: sql parser error: ...
```

## Expected vs actual

Expected: created (PostgreSQL reads one integer), or refused with the decimal remedy. Actual:
refused with the parser's message only. Nothing is created; no wrong result.

## What is established and what is not

Established: the two outcomes above belong to cells of the 1.12.0 review corpus (tagged
refusal on base and head) and to `v112_sr_text_select_items`, which pins the parser's message
for `0o17 + v AS x` and `0b101 + v AS x`. Not established: every position.

## Fix direction (hypothesis to falsify)

Look for a number directly followed by a name in the token stream before parsing, and return
the decimal remedy in place of the parser's message. The test above pins the current message.
