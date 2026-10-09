# `daterange('2024-01-01', 'today')` is accepted: a moving date/time word passed to a constructor — limitation

Residual of "the volatile / clock function check does not see literal casts such as
'now'::timestamptz". The check closed on the 1.12.0 branch covers a string literal under a cast
to a built-in date/time type and the one-argument function form (`date('today')`). A literal
passed to a function of several arguments is not inspected.

## Reproduction (pg15, executed by the adversarial review of the branch)

```sql
-- t(id serial primary key, v int)
SELECT create_reflex_ivm('r', 'SELECT id, daterange(''2024-01-01'', ''today'') AS dr FROM t');
-- CREATE REFLEX INCREMENTAL VIEW, on 1.11.5 and on the branch
```

`'[today,)'::daterange` is rejected on the branch. The same gap is expected, not executed, for
`tstzrange('now', NULL)`, `make_timestamptz`-style helpers given a literal, and any user function
with a date/time parameter.

## Expected vs actual

Expected: rejected like `'today'::date`. Actual: created; `today` is read again by every
maintenance statement, so rows written on different days hold different upper bounds and the IMV
drifts from its own query. Silent.

## Fix direction

The parameter type of the function decides whether `'today'` is a date, and the analyzer has no
types. Same direction as
`2026-10-07_moving_date_time_value_without_a_literal_cast_accepted.md`: resolve expression
types through PostgreSQL (plan the query and look for date/time constants folded from a special
input). A name list of range constructors would close the reported spelling only.
