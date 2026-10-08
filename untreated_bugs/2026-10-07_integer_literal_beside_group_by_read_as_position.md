# An unsigned integer literal in the SELECT list beside GROUP BY is read as a GROUP BY position — S3 (loud at create, or works by position)

Residual of the "SELECT-list literal beside GROUP BY" report. Closed on the 1.12.0 branch: a
string, NULL, boolean, non-integer or signed numeric literal beside GROUP BY or DISTINCT is
rejected at create with a tagged message instead of the raw `non-integer constant in GROUP BY` /
`GROUP BY position -1 is not in select list`; the remedy it prints (cast the constant:
`'plain'::text`, `NULL::text`, `TRUE::boolean`, `3.5::numeric`) is oracle-proven
(`v112_cl_cast_constant_beside_group_by_is_maintained`).

## Reproduction (pg15, 1.12.0 branch)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, g TEXT, v INT);
SELECT create_reflex_ivm('a', 'SELECT g, 2 AS d, SUM(v) AS s FROM t GROUP BY g');  -- created, correct
SELECT create_reflex_ivm('b', 'SELECT g, 3 AS d, SUM(v) AS s FROM t GROUP BY g');
-- ERROR:  aggregate functions are not allowed in GROUP BY
```

## Expected vs actual

Expected: a constant output column, whatever its value. Actual: the literal is emitted into the
generated `GROUP BY`, where PostgreSQL reads an integer as a select-list position. `2` names the
literal itself and `1` names `g`, so both happen to group correctly (oracle-checked for `2`
through INSERT / UPDATE / DELETE); `3` names an aggregate and raises; a larger value raises
`GROUP BY position N is not in select list`. No wrong result was found: a position either names
a group key, the literal, or raises.

## Why it was left

Rejecting every integer literal would refuse definitions that are created and correct today.
The cast form (`2::int`) always works.

## Fix direction

Emit a constant select item only in the end query, or cast an integer literal in the generated
GROUP BY (`2::int`), so its value never reads as a position.
