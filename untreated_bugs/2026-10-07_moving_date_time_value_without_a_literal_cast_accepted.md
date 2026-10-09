# A moving date/time value that is not a typed string literal is accepted — limitation

Residual of "the volatile / clock function check does not see literal casts such as
'now'::timestamptz". Closed on the 1.12.0 branch: a string literal holding `now`, `today`,
`tomorrow` or `yesterday` (any case, surrounding spaces, beside other words) that is given a
built-in date/time type is rejected like `now()`: `'now'::timestamptz`, `timestamptz 'now'`,
`DATE 'today'`, `CAST(' Tomorrow ' AS timestamp)`, `date('yesterday')`, `'now'::time`,
`'{now}'::timestamptz[]`, `'now'::pg_catalog.timestamptz`, anywhere in the query
(`is_moving_date_time_literal`, `src/sql_analyzer.rs`). `'now'::text`, `'2020-01-01'::date`,
`'epoch'::timestamptz`, `'infinity'::date` and a column named `now` are still accepted.

## What remains (pg15, 1.12.0 branch)

```sql
-- table t(id serial primary key, ts timestamptz, s text, v int)
SELECT id, v FROM t WHERE ts < 'now'             -- created: the literal takes the type of ts
SELECT id, s::timestamptz AS x FROM t            -- created: s may hold 'now'
SELECT id, 'now'::text::timestamptz AS x FROM t  -- created: the cast operand is not a literal
SELECT id, 'today'::my_date_domain AS x FROM t   -- created: the type name is not a built-in one
```

The first form was executed (created). The others are read from the code: the check needs a
string literal directly under a cast to a type named in `DATE_TIME_TYPES`.

## Expected vs actual

Expected: rejected like `now()`. Actual: created; the value is read again at every maintenance,
so the IMV drifts.

## Why it was left

Each needs the type PostgreSQL resolves for an expression, which the analyzer does not have: it
works on the parsed text and on catalog lookups by name. Rejecting every string literal `'now'`
would refuse `WHERE status = 'now'` on a text column.

## Fix direction

Resolve expression types through PostgreSQL (plan the query and walk the planned expressions for
constants of a date/time type folded from a special input, or for casts from text to a date/time
type), or document the limitation in the function-contract notes.
