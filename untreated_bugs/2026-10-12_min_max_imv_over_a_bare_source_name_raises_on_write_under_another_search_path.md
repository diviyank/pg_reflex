# A MIN / MAX IMV created over a bare source name under a non-default `search_path` raises `relation "s" does not exist` on writes made under another path — S2 (write-blocking, loud; pre-existing)

Met by the review of the result-types work of 1.12.0 (`fold-g3-review.md`, "Pre-existing, out of
scope (same on base)", cell X.sp6; pg17, base `c3d4866`, head `2b74d4f`) in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`. Marked "Unfiled" there. Not run
on 1.11.5 nor on the release head.

## What the source gives

The whole evidence is this entry:

> An IMV with MIN / MAX created with a bare source name under a non-default `search_path`
> (`SET search_path = app, public; … FROM s …`): INSERT / UPDATE / emptying DELETE under another
> path raise `relation "s" does not exist` (X.sp6; a SUM / COUNT-only IMV is fine, X.sp5).

The shape it describes (the table, the rows and the IMV definition of the cell are not printed;
the statements below are the reading of that sentence, not a copied script):

```sql
CREATE SCHEMA app;
SET search_path = app, public;
CREATE TABLE s(id INT PRIMARY KEY, g TEXT, v INT);
SELECT create_reflex_ivm('x', 'SELECT g, MAX(v) AS hi FROM s GROUP BY g');   -- bare source name
SET search_path = public;
INSERT INTO app.s VALUES (1, 'a', 1);
-- reported: ERROR:  relation "s" does not exist
```

## Expected vs actual

Expected: a write to the source maintains the IMV whatever the writer's `search_path`, as it
does for the SUM / COUNT-only IMV of the same cell family. Actual: the write is refused.

## What is established and what is not

Established: the outcome for INSERT, UPDATE and a DELETE that empties a group, with MIN / MAX,
and that the SUM / COUNT-only IMV is not affected. Not established: which statement of the
MIN / MAX path carries the bare name (the recompute of a retracted extremum reads the source
again, which is the first suspect: it would run the stored query text under the writer's path);
DEFERRED; top-K; whether `SET search_path` in the trigger functions or a schema-qualified
registration at create would cure it; the release head.

Not the same as `2026-09-18_partitioned_bare_name_imv_under_tenant_search_path.md`: that one is
a create failure of a partitioned IMV (`relation "public.p_imv" does not exist`), this one is a
write failure of an unpartitioned MIN / MAX IMV. Same family as
`2026-10-12_remaining_matches_of_a_relation_by_name_without_schema.md`.

## Fix direction (hypothesis to falsify)

Store and use the schema-qualified spelling of every source in the SQL the MIN / MAX recompute
runs (create resolves the bare name once, with the creator's path). Test: the fixture above, the
three writes under `search_path = public` and under a path where another table named `s`
exists, both modes, with the oracle; the second path is the dangerous one, because there the
statement would not raise but read the wrong table.
