# Unqualified self-join written once quoted and once unquoted: UPDATE silently not applied — S1 (silent wrong aggregate)

Pre-existing (equally wrong on the base). Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (Task 1b probe branch based on c71c65d, itself on the 1.12.0 line from
base commit 6946400), on pg15. Found by the Task 1b adversarial reviewer (P1).

## Reproduction (pg15)

```sql
CREATE TABLE t(id SERIAL PRIMARY KEY, season TEXT, v INT);
INSERT INTO t(season, v) VALUES ('e',1),('y',2),('e',3);
SELECT create_reflex_ivm('v', 'SELECT a.season, COUNT(*) AS c, SUM(b.v) AS s
  FROM "t" a JOIN t b ON a.season = b.season GROUP BY a.season');
INSERT INTO t(season, v) VALUES ('e',10),('y',20);   -- head: correct   base: wrong
UPDATE t SET v = v + 100 WHERE id = 1;               -- head and base: IMV (e,9,42), fresh (e,9,342)
```

The same with the quoting reversed, `t a JOIN "t" b`. `"t" a JOIN "t" b` and `t a JOIN t b` are
correct.

## Expected vs actual

Expected: after the UPDATE the IMV equals a fresh run. Actual: the UPDATE does not reach the IMV
(`(e,9,42)` against `(e,9,342)`). The INSERT in the same sequence is correct on the 1.12.0 head.

## What is known about the mechanism

- The registry holds the table twice: `depends_on = {"\"t\"", t}`.
- On the 1.12.0 head both self-join occurrence guards count 2 and select the full refresh, and
  the INSERT is correct; the UPDATE nevertheless does not reach the IMV. The mechanism is not
  investigated; the reviewer states it is not in the guards.
- The branch's unit matrix (`src/tests/unit_trigger_source_guards.rs`, about line 110) pairs each
  registered spelling with one referenced spelling used on both sides, so this shape is covered
  nowhere. The Task 1b report's statement "self-join in every spelling" must not be read as
  covering it.

## Fix direction (hypothesis)

Canonicalise the table name before it enters `depends_on` (one entry per relation, regardless of
quoting), then re-run the self-join guards and the UPDATE path against the single entry. Add oracle
tests for INSERT, UPDATE and DELETE on both mixed spellings, and a registry assertion that
`depends_on` holds the table once.
