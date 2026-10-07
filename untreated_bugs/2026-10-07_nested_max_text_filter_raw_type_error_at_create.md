# Nested MAX(text) FILTER (...) raises a raw type error at create (was silently wrong data on 1.11.5) — S3 (loud, at create)

Found by the Task 4b re-reviewer (finding N1). Introduced in kind by commit `9528197` of the
1.12.0 branch (a FILTER on a nested aggregate now reaches the aggregate's argument). On the 1.11.5
base the same shape was created with wrong data (filter ignored). Left unfixed by the fix round.

## Reproduction (pg17, from the re-review)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT, t TEXT);
INSERT INTO s(g, v, t) VALUES ('a',1,'x'),('a',2,'y');
SELECT create_reflex_ivm('x', $$SELECT g, COALESCE(MAX(t) FILTER (WHERE v > 1), '-') AS r FROM s GROUP BY g$$);
-- head: ERROR column "__max_case_when_v_1_then_t_end" is of type numeric but expression is of type text
-- base: created, wrong data
```

## Expected vs actual

Expected: an IMV equal to the query (`r = 'y'`), or a tagged rejection. Actual on the branch: raw
PostgreSQL error, nothing created. The re-reviewer judged it the safe direction (loud, replaces
silent wrong data), and notes the top-level form has always raised the same raw error; the brief
asked for wrong-to-correct or wrong-to-rejected, and this is wrong-to-raw-error.

## What is established and what is not

Established: both outcomes above. Not established: a reason beyond the shared intermediate-column
typing family; the source does not trace it.

## Related reports

- `2026-10-07_min_max_filter_over_text_raw_type_error.md` (top-level form)
- `2026-10-07_max_over_text_function_expression_raw_type_error.md`

## Fix direction (hypothesis to falsify)

Fixing the intermediate-column typing for MIN / MAX over an expression argument should clear this
and the related reports together; alternatively reject the shape with a tagged message while
that is open. Pin with the reproduction above through INSERT, UPDATE and DELETE.
