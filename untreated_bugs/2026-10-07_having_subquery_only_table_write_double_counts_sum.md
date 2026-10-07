# A write to a table referenced only in a HAVING scalar subquery double-counts SUM — S1 (silent wrong aggregate)

Pre-existing (not introduced by 1.12.0). Observed during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400), on pg17. Found by the Task 3 implementer while
pinning the HAVING-subquery fix and confirmed by the Task 3 re-reviewer (Risk 5).

## Reproduction (pg17)

```sql
CREATE TABLE s(id SERIAL PRIMARY KEY, g TEXT, v INT);
INSERT INTO s(g, v) VALUES ('a', 1), ('a', 2), ('b', 10);
CREATE TABLE o(k INT);
INSERT INTO o VALUES (1);
SELECT create_reflex_ivm('imv',
  'SELECT g, SUM(v) AS s FROM s GROUP BY g HAVING SUM(v) > (SELECT COUNT(*) FROM o)');
-- imv: a=3, b=10
INSERT INTO o VALUES (2);
-- imv: a=6, b=20     (correct: a=3, b=10)
DELETE FROM o WHERE k = 2;
-- imv: a=3, b=10
```

Observed with `create_reflex_ivm(..., NULL, NULL, NULL, NULL)` (default topk) inside a
`#[pg_test]`. There is no `GROUP BY` in the subquery and the topk is the default, so the
"changed flag" touched by the 1.12.0 keyword-scan work is not involved.

## Expected vs actual

Expected: `imv` equals a fresh run of its query after the INSERT into `o` (a=3, b=10, since the
subquery value 2 is still below both sums). Actual: every group's sum is doubled (a=6, b=20). The
following DELETE from `o` brings it back to a=3, b=10, so the error tracks the write rather than
accumulating permanently in this sequence.

## What is known about the mechanism

- `o` is registered as a source of the IMV (it gets maintenance triggers and appears in
  `depends_on`), but the stored `base_query` does not mention `o`: the `HAVING` lives only in
  `end_query`. Likely cause, by the implementer's reading and not traced: the delta query for a
  write to `o` is therefore the whole base aggregate, which is merged in again, so `SUM` is added
  twice. `MAX` survives because re-merging the same maximum is idempotent; the committed 1.12.0
  test for the MAX variant of this query shape writes to `o` and is green.
- Not investigated further by either the implementer or the re-reviewer.
- Consequence for tests: the two `SUM` variants of the HAVING-subquery oracle tests
  (`src/tests/pg_test_v112_keyword_literals.rs`, around lines 295 and 305) cannot enable the
  "write to the subquery's table" variant until this is fixed (re-review note N2).

## Fix direction (hypothesis)

A source that appears only in `end_query` (HAVING, or any end-query-only subquery) must not take
the "delta is the whole base aggregate" path. Either (1) a write to such a source triggers a
re-evaluation of the end query only (the intermediate table is unchanged; recompute the target
from it), or (2) the create-time analysis refuses, with a tagged `[reflex-unsupported]` message,
a HAVING subquery over a table that is not in the base query. Add oracle tests for INSERT,
UPDATE and DELETE on `o` with `SUM`, `COUNT` and `MIN`/`MAX` aggregates.
